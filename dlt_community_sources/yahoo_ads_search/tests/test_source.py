"""Tests for Yahoo Ads Search source."""

from unittest.mock import MagicMock, patch

import pytest

from dlt_community_sources.yahoo_ads_common.auth import (
    YAHOO_TOKEN_URL,
    refresh_access_token,
)
from dlt_community_sources.yahoo_ads_common.helpers import (
    download_report,
    get_entities,
    make_client,
    poll_report,
    post_rpc,
    safe_get_entities,
    submit_report,
)
from dlt_community_sources.yahoo_ads_search.source import (
    _ENTITY_RESOURCES,
    BASE_URL,
    REPORT_FIELDS,
    REPORT_TYPES,
    _convert_report_types,
    _derive_primary_key,
)


def _mock_response(json_data, status_code=200):
    mock = MagicMock()
    mock.json.return_value = json_data
    mock.raise_for_status.return_value = None
    mock.status_code = status_code
    return mock


# ---------------------------------------------------------------------------
# Auth tests
# ---------------------------------------------------------------------------


class TestRefreshAccessToken:
    @patch("dlt_community_sources.yahoo_ads_common.auth.requests.post")
    def test_success(self, mock_post):
        mock_post.return_value = _mock_response(
            {"access_token": "new_at", "token_type": "Bearer", "expires_in": 3600}
        )
        result = refresh_access_token("cid", "cs", "rt")
        assert result["access_token"] == "new_at"
        call_kwargs = mock_post.call_args
        assert call_kwargs[1]["data"]["grant_type"] == "refresh_token"
        assert call_kwargs[1]["data"]["client_id"] == "cid"

    @patch("dlt_community_sources.yahoo_ads_common.auth.requests.post")
    def test_error_raises(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = Exception("HTTP 400")
        mock_post.return_value = mock_resp
        with pytest.raises(Exception, match="HTTP 400"):
            refresh_access_token("cid", "cs", "bad_rt")


def test_token_url():
    assert "biz-oauth.yahoo.co.jp" in YAHOO_TOKEN_URL


# ---------------------------------------------------------------------------
# Helper tests
# ---------------------------------------------------------------------------


class TestMakeClient:
    def test_creates_client_with_headers(self):
        client = make_client("token123", "acct456")
        assert client.session.headers["Authorization"] == "Bearer token123"
        assert client.session.headers["x-z-base-account-id"] == "acct456"


class TestPostRpc:
    def test_returns_json(self):
        client = MagicMock()
        client.post.return_value = _mock_response({"rval": {"values": []}})
        result = post_rpc(client, "https://example.com/api", {"key": "val"})
        assert result == {"rval": {"values": []}}


class TestGetEntities:
    def test_single_page(self):
        client = MagicMock()
        client.post.return_value = _mock_response(
            {
                "rval": {
                    "totalNumEntries": 2,
                    "values": [
                        {"operationSucceeded": True, "campaign": {"campaignId": 1}},
                        {"operationSucceeded": True, "campaign": {"campaignId": 2}},
                    ],
                }
            }
        )
        results = list(get_entities(client, "https://api/get", "123"))
        assert len(results) == 2
        assert results[0]["campaignId"] == 1

    def test_skips_failed_operations(self):
        client = MagicMock()
        client.post.return_value = _mock_response(
            {
                "rval": {
                    "totalNumEntries": 2,
                    "values": [
                        {"operationSucceeded": True, "campaign": {"campaignId": 1}},
                        {
                            "operationSucceeded": False,
                            "errors": [{"code": "V0001"}],
                        },
                    ],
                }
            }
        )
        results = list(get_entities(client, "https://api/get", "123"))
        assert len(results) == 1

    def test_pagination(self):
        page1 = _mock_response(
            {
                "rval": {
                    "totalNumEntries": 3,
                    "values": [
                        {"operationSucceeded": True, "item": {"id": 1}},
                        {"operationSucceeded": True, "item": {"id": 2}},
                    ],
                }
            }
        )
        page2 = _mock_response(
            {
                "rval": {
                    "totalNumEntries": 3,
                    "values": [
                        {"operationSucceeded": True, "item": {"id": 3}},
                    ],
                }
            }
        )
        client = MagicMock()
        client.post.side_effect = [page1, page2]
        results = list(get_entities(client, "https://api/get", "123", page_size=2))
        assert len(results) == 3

    def test_empty_response(self):
        client = MagicMock()
        client.post.return_value = _mock_response(
            {"rval": {"totalNumEntries": 0, "values": []}}
        )
        results = list(get_entities(client, "https://api/get", "123"))
        assert results == []


class TestSafeGetEntities:
    def test_handles_403(self):
        from dlt.sources.helpers.requests import HTTPError

        client = MagicMock()
        resp = MagicMock()
        resp.status_code = 403
        client.post.side_effect = HTTPError(response=resp)
        results = list(safe_get_entities(client, "https://api/get", "123"))
        assert results == []

    def test_raises_on_500(self):
        from dlt.sources.helpers.requests import HTTPError

        client = MagicMock()
        resp = MagicMock()
        resp.status_code = 500
        client.post.side_effect = HTTPError(response=resp)
        with pytest.raises(HTTPError):
            list(safe_get_entities(client, "https://api/get", "123"))


# ---------------------------------------------------------------------------
# Report tests
# ---------------------------------------------------------------------------


class TestSubmitReport:
    def test_returns_job_id(self):
        client = MagicMock()
        client.post.return_value = _mock_response(
            {
                "rval": {
                    "values": [
                        {
                            "reportDefinition": {
                                "reportJobId": 12345,
                                "reportJobStatus": "WAIT",
                            }
                        }
                    ]
                }
            }
        )
        job_id = submit_report(
            client,
            "https://api",
            "123",
            "CAMPAIGN",
            ["DAY", "IMPS"],
            "2026-01-01",
            "2026-01-31",
        )
        assert job_id == 12345
        call_body = client.post.call_args[1]["json"]
        assert call_body["accountId"] == 123
        assert call_body["operand"][0]["reportType"] == "CAMPAIGN"
        # Date format: YYYYMMDD (hyphens removed)
        assert call_body["operand"][0]["dateRange"]["startDate"] == "20260101"

    def test_returns_none_on_empty(self):
        client = MagicMock()
        client.post.return_value = _mock_response({"rval": {"values": []}})
        job_id = submit_report(
            client,
            "https://api",
            "123",
            "CAMPAIGN",
            ["DAY"],
            "2026-01-01",
            "2026-01-31",
        )
        assert job_id is None


class TestPollReport:
    def test_completed(self):
        client = MagicMock()
        client.post.return_value = _mock_response(
            {
                "rval": {
                    "values": [
                        {
                            "reportDefinition": {
                                "reportJobId": 123,
                                "reportJobStatus": "COMPLETED",
                            }
                        }
                    ]
                }
            }
        )
        status = poll_report(client, "https://api", "12345", 123)
        assert status == "COMPLETED"

    def test_failed_returns_none(self):
        client = MagicMock()
        client.post.return_value = _mock_response(
            {
                "rval": {
                    "values": [
                        {
                            "reportDefinition": {
                                "reportJobId": 123,
                                "reportJobStatus": "FAILED",
                            }
                        }
                    ]
                }
            }
        )
        status = poll_report(client, "https://api", "12345", 123)
        assert status is None

    @patch("dlt_community_sources.yahoo_ads_common.helpers.time.sleep")
    @patch("dlt_community_sources.yahoo_ads_common.helpers.POLL_MAX_WAIT_SECONDS", 20)
    @patch("dlt_community_sources.yahoo_ads_common.helpers.POLL_INTERVAL_SECONDS", 10)
    def test_timeout(self, mock_sleep):
        client = MagicMock()
        client.post.return_value = _mock_response(
            {
                "rval": {
                    "values": [
                        {
                            "reportDefinition": {
                                "reportJobId": 123,
                                "reportJobStatus": "IN_PROGRESS",
                            }
                        }
                    ]
                }
            }
        )
        status = poll_report(client, "https://api", "12345", 123)
        assert status is None
        assert mock_sleep.call_count == 2


class TestDownloadReport:
    def test_parses_csv(self):
        client = MagicMock()
        resp = MagicMock()
        resp.text = "DAY,IMPS,COST\n2026-01-01,100,12.34\n"
        resp.raise_for_status.return_value = None
        client.post.return_value = resp
        rows = list(download_report(client, "https://api", "12345", 123))
        assert len(rows) == 1
        assert rows[0]["DAY"] == "2026-01-01"
        assert rows[0]["IMPS"] == "100"


# ---------------------------------------------------------------------------
# Source config tests
# ---------------------------------------------------------------------------


class TestSourceConfig:
    def test_base_url(self):
        assert "ads-search" in BASE_URL
        assert "v19" in BASE_URL

    def test_entity_resources_count(self):
        assert len(_ENTITY_RESOURCES) >= 25

    def test_entity_resources_have_required_fields(self):
        for name, path, disposition, pk in _ENTITY_RESOURCES:
            assert name, "resource name is empty"
            assert "/" in path, f"{name}: path missing /"
            assert disposition in ("merge", "replace", "append")
            assert pk, f"{name}: primary_key is empty"

    def test_report_types(self):
        assert "CAMPAIGN" in REPORT_TYPES
        assert "AD" in REPORT_TYPES
        assert "KEYWORDS" in REPORT_TYPES

    def test_report_fields_have_day(self):
        for rt, fields in REPORT_FIELDS.items():
            assert "DAY" in fields, f"{rt}: missing DAY field"

    def test_convert_report_types_int(self):
        row = {"IMPS": "1,234", "CLICKS": "56", "DAY": "2026-01-01"}
        result = _convert_report_types(row)
        assert result["IMPS"] == 1234
        assert result["CLICKS"] == 56
        assert result["DAY"] == "2026-01-01"

    def test_convert_report_types_float(self):
        row = {"COST": "1,234.56", "AVG_CPC": "12.34"}
        result = _convert_report_types(row)
        assert result["COST"] == 1234.56
        assert result["AVG_CPC"] == 12.34

    def test_convert_report_types_dash(self):
        row = {"IMPS": "--", "COST": ""}
        result = _convert_report_types(row)
        assert result["IMPS"] is None
        assert result["COST"] is None


class TestDerivePrimaryKey:
    def test_campaign_report(self):
        fields = [
            "DAY",
            "ACCOUNT_ID",
            "CAMPAIGN_ID",
            "CAMPAIGN_NAME",
            "IMPS",
            "CLICKS",
            "COST",
        ]
        pk = _derive_primary_key(fields)
        assert pk == ["DAY", "ACCOUNT_ID", "CAMPAIGN_ID", "CAMPAIGN_NAME"]

    def test_excludes_all_metrics(self):
        fields = [
            "DAY",
            "IMPS",
            "CLICKS",
            "CLICK_RATE",
            "AVG_CPC",
            "COST",
            "CONVERSIONS",
            "CONV_RATE",
            "CONV_VALUE",
        ]
        pk = _derive_primary_key(fields)
        assert pk == ["DAY"]


class TestSourceFunction:
    @patch("dlt_community_sources.yahoo_ads_search.source.refresh_access_token")
    def test_returns_source_with_resources(self, mock_refresh):
        mock_refresh.return_value = {"access_token": "at"}
        from dlt_community_sources.yahoo_ads_search.source import (
            yahoo_ads_search_source,
        )

        source = yahoo_ads_search_source(
            client_id="cid",
            client_secret="cs",
            refresh_token="rt",
            account_id="123",
        )
        assert "report" in source.resources
        assert "campaigns" in source.resources

    @patch("dlt_community_sources.yahoo_ads_search.source.refresh_access_token")
    def test_filter_resources(self, mock_refresh):
        mock_refresh.return_value = {"access_token": "at"}
        from dlt_community_sources.yahoo_ads_search.source import (
            yahoo_ads_search_source,
        )

        source = yahoo_ads_search_source(
            client_id="cid",
            client_secret="cs",
            refresh_token="rt",
            account_id="123",
            resources=["campaigns", "report"],
        )
        resource_names = list(source.resources.keys())
        assert "campaigns" in resource_names
        assert "report" in resource_names
        assert "ads" not in resource_names

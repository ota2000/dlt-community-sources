"""Tests for Microsoft Ads source configuration and helpers."""

from unittest.mock import MagicMock

from dlt_community_sources.microsoft_ads.source import (
    CAMPAIGN_MGMT_URL,
    CUSTOMER_MGMT_URL,
    REPORT_COLUMNS,
    REPORT_FLOAT_FIELDS,
    REPORT_INT_FIELDS,
    REPORT_TYPES,
    REPORTING_URL,
    _build_headers,
    _convert_report_types,
    _get_entities_paginated,
    _make_client,
    _post_rpc,
    _safe_rpc,
    _submit_report,
)


class TestBuildHeaders:
    """Test _build_headers()."""

    def test_includes_all_required_headers(self):
        headers = _build_headers("token", "dev", "cust", "acct")
        assert headers["Authorization"] == "Bearer token"
        assert headers["DeveloperToken"] == "dev"
        assert headers["CustomerId"] == "cust"
        assert headers["AccountId"] == "acct"

    def test_content_type(self):
        headers = _build_headers("t", "d", "c", "a")
        assert headers["Content-Type"] == "application/json"


class TestMakeClient:
    """Test _make_client()."""

    def test_sets_headers(self):
        client = _make_client("token", "dev", "cust", "acct")
        assert client.session.headers["Authorization"] == "Bearer token"
        assert client.session.headers["DeveloperToken"] == "dev"
        assert client.session.headers["CustomerId"] == "cust"
        assert client.session.headers["AccountId"] == "acct"


class TestConstants:
    """Test URL and config constants."""

    def test_campaign_mgmt_url(self):
        assert "campaign.api.bingads.microsoft.com" in CAMPAIGN_MGMT_URL
        assert "/v13" in CAMPAIGN_MGMT_URL

    def test_reporting_url(self):
        assert "reporting.api.bingads.microsoft.com" in REPORTING_URL
        assert "/v13" in REPORTING_URL

    def test_customer_mgmt_url(self):
        assert "clientcenter.api.bingads.microsoft.com" in CUSTOMER_MGMT_URL

    def test_report_types_include_basics(self):
        assert "CampaignPerformanceReportRequest" in REPORT_TYPES
        assert "AdGroupPerformanceReportRequest" in REPORT_TYPES
        assert "AdPerformanceReportRequest" in REPORT_TYPES
        assert "KeywordPerformanceReportRequest" in REPORT_TYPES

    def test_report_columns_include_basics(self):
        cols = REPORT_COLUMNS["CampaignPerformanceReportRequest"]
        for field in ["TimePeriod", "CampaignName", "Impressions", "Clicks", "Spend"]:
            assert field in cols

    def test_all_report_columns_have_time_period(self):
        for rt, cols in REPORT_COLUMNS.items():
            assert "TimePeriod" in cols, f"{rt} missing TimePeriod"

    def test_int_and_float_fields_disjoint(self):
        assert REPORT_INT_FIELDS.isdisjoint(REPORT_FLOAT_FIELDS)


class TestPostRpc:
    """Test _post_rpc()."""

    def test_returns_json(self):
        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"Campaigns": [{"Id": 1}]}
        mock_resp.raise_for_status.return_value = None
        mock_client.post.return_value = mock_resp
        result = _post_rpc(mock_client, "http://test", {"key": "val"})
        assert result == {"Campaigns": [{"Id": 1}]}


class TestSafeRpc:
    """Test _safe_rpc()."""

    def test_success(self):
        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"Items": [{"Id": 1}]}
        mock_resp.raise_for_status.return_value = None
        mock_client.post.return_value = mock_resp
        result = _safe_rpc(mock_client, "http://test", {}, "Items")
        assert result == [{"Id": 1}]

    def test_404_returns_empty(self):
        from dlt.sources.helpers import requests as dlt_req

        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        error = dlt_req.HTTPError(response=mock_resp)
        mock_client.post.side_effect = error
        result = _safe_rpc(mock_client, "http://test", {}, "Items")
        assert result == []

    def test_missing_key_returns_empty(self):
        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"Other": []}
        mock_resp.raise_for_status.return_value = None
        mock_client.post.return_value = mock_resp
        result = _safe_rpc(mock_client, "http://test", {}, "Items")
        assert result == []


class TestGetEntitiesPaginated:
    """Test _get_entities_paginated()."""

    def test_single_page(self):
        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"Items": [{"Id": 1}, {"Id": 2}]}
        mock_resp.raise_for_status.return_value = None
        mock_client.post.return_value = mock_resp
        result = list(
            _get_entities_paginated(
                mock_client, "http://test", {}, "Items", page_size=10
            )
        )
        assert len(result) == 2

    def test_empty_response(self):
        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"Items": []}
        mock_resp.raise_for_status.return_value = None
        mock_client.post.return_value = mock_resp
        result = list(_get_entities_paginated(mock_client, "http://test", {}, "Items"))
        assert result == []

    def test_pagination_stops_on_partial_page(self):
        mock_client = MagicMock()
        page1 = MagicMock()
        page1.json.return_value = {"Items": [{"Id": i} for i in range(3)]}
        page1.raise_for_status.return_value = None
        page2 = MagicMock()
        page2.json.return_value = {"Items": [{"Id": 3}]}
        page2.raise_for_status.return_value = None
        mock_client.post.side_effect = [page1, page2]
        result = list(
            _get_entities_paginated(
                mock_client, "http://test", {}, "Items", page_size=3
            )
        )
        assert len(result) == 4


class TestConvertReportTypes:
    """Test _convert_report_types()."""

    def test_converts_int_fields(self):
        row = {"Impressions": "1234", "Clicks": "56"}
        result = _convert_report_types(row)
        assert result["Impressions"] == 1234
        assert result["Clicks"] == 56

    def test_converts_float_fields(self):
        row = {"Spend": "12.34", "Ctr": "0.03"}
        result = _convert_report_types(row)
        assert result["Spend"] == 12.34
        assert isinstance(result["Ctr"], float)

    def test_handles_none(self):
        row = {"Impressions": None}
        result = _convert_report_types(row)
        assert result["Impressions"] is None

    def test_handles_invalid(self):
        row = {"Impressions": "--"}
        result = _convert_report_types(row)
        assert result["Impressions"] == "--"

    def test_preserves_non_numeric(self):
        row = {"CampaignName": "Test", "Impressions": "100"}
        result = _convert_report_types(row)
        assert result["CampaignName"] == "Test"
        assert result["Impressions"] == 100


class TestSubmitReport:
    """Test _submit_report()."""

    def test_returns_request_id(self):
        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"ReportRequestId": "abc123"}
        mock_resp.raise_for_status.return_value = None
        mock_client.post.return_value = mock_resp
        result = _submit_report(
            mock_client,
            "CampaignPerformanceReportRequest",
            "12345",
            ["TimePeriod", "Impressions"],
            "2026-01-01",
            "2026-01-31",
        )
        assert result == "abc123"

    def test_returns_none_on_missing_id(self):
        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {}
        mock_resp.raise_for_status.return_value = None
        mock_client.post.return_value = mock_resp
        result = _submit_report(
            mock_client,
            "CampaignPerformanceReportRequest",
            "12345",
            ["TimePeriod"],
            "2026-01-01",
            "2026-01-31",
        )
        assert result is None

    def test_date_parsing(self):
        """Verify date is parsed into Day/Month/Year structure."""
        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"ReportRequestId": "x"}
        mock_resp.raise_for_status.return_value = None
        mock_client.post.return_value = mock_resp
        _submit_report(
            mock_client,
            "CampaignPerformanceReportRequest",
            "12345",
            ["TimePeriod"],
            "2026-03-15",
            "2026-04-01",
        )
        call_body = mock_client.post.call_args[1]["json"]
        time_config = call_body["ReportRequest"]["Time"]
        assert time_config["CustomDateRangeStart"] == {
            "Day": 15,
            "Month": 3,
            "Year": 2026,
        }
        assert time_config["CustomDateRangeEnd"] == {"Day": 1, "Month": 4, "Year": 2026}

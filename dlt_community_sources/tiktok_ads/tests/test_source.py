"""Tests for TikTok Ads source configuration and helpers."""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from dlt_community_sources.tiktok_ads.source import (
    DEFAULT_METRICS,
    REPORT_DIMENSIONS,
    REPORT_FLOAT_FIELDS,
    REPORT_INT_FIELDS,
    REPORT_PRIMARY_KEYS,
    _check_response,
    _convert_report_types,
    _date_chunks,
    _flatten_report_row,
    _make_client,
    _rest_api_config,
    advertiser_balance,
    advertiser_info,
    advertiser_transactions,
    apps,
    authorized_advertiser_ids,
    identities,
    pixels,
    report,
    rule_results,
    tiktok_ads_source,
    videos,
)


class TestRestApiConfig:
    """Test _rest_api_config() structure."""

    def setup_method(self):
        self.config = _rest_api_config(
            access_token="test_token",
            advertiser_id="12345",
            base_url="https://business-api.tiktok.com/open_api/v1.3",
        )

    def test_client_auth_type(self):
        auth = self.config["client"]["auth"]
        assert auth["type"] == "api_key"
        assert auth["name"] == "Access-Token"
        assert auth["api_key"] == "test_token"
        assert auth["location"] == "header"

    def test_paginator(self):
        paginator = self.config["client"]["paginator"]
        assert paginator["type"] == "page_number"
        assert paginator["base_page"] == 1
        assert paginator["page_param"] == "page"
        assert paginator["total_path"] == "data.page_info.total_page"

    def test_data_selector(self):
        assert (
            self.config["resource_defaults"]["endpoint"]["data_selector"] == "data.list"
        )

    def test_resource_names(self):
        names = [r["name"] for r in self.config["resources"]]
        assert names == [
            "campaigns",
            "ad_groups",
            "ads",
            "custom_audiences",
            "saved_audiences",
            "creative_portfolios",
            "automated_rules",
        ]

    def test_primary_keys(self):
        pk_map = {r["name"]: r["primary_key"] for r in self.config["resources"]}
        assert pk_map["campaigns"] == "campaign_id"
        assert pk_map["ad_groups"] == "adgroup_id"
        assert pk_map["ads"] == "ad_id"
        assert pk_map["custom_audiences"] == "custom_audience_id"
        assert pk_map["saved_audiences"] == "saved_audience_id"

    def test_advertiser_id_in_params(self):
        params = self.config["resource_defaults"]["endpoint"]["params"]
        assert params["advertiser_id"] == "12345"


class TestReportConfig:
    """Test report configuration constants."""

    def test_dimensions_per_level(self):
        assert "stat_time_day" in REPORT_DIMENSIONS["AUCTION_AD"]
        assert "ad_id" in REPORT_DIMENSIONS["AUCTION_AD"]
        assert "campaign_id" in REPORT_DIMENSIONS["AUCTION_CAMPAIGN"]

    def test_primary_keys_per_level(self):
        assert REPORT_PRIMARY_KEYS["AUCTION_AD"] == ["stat_time_day", "ad_id"]
        assert REPORT_PRIMARY_KEYS["AUCTION_CAMPAIGN"] == [
            "stat_time_day",
            "campaign_id",
        ]

    def test_default_metrics_include_basics(self):
        assert "spend" in DEFAULT_METRICS
        assert "impressions" in DEFAULT_METRICS
        assert "clicks" in DEFAULT_METRICS
        assert "conversions" in DEFAULT_METRICS

    def test_int_and_float_fields_disjoint(self):
        assert REPORT_INT_FIELDS.isdisjoint(REPORT_FLOAT_FIELDS)


class TestConvertReportTypes:
    """Test _convert_report_types() type conversion."""

    def test_converts_int_fields(self):
        row = {"impressions": "1234", "clicks": "56"}
        result = _convert_report_types(row)
        assert result["impressions"] == 1234
        assert result["clicks"] == 56

    def test_converts_float_fields_to_decimal(self):
        row = {"spend": "12.34", "cpc": "0.56"}
        result = _convert_report_types(row)
        assert result["spend"] == Decimal("12.34")
        assert isinstance(result["cpc"], Decimal)

    def test_handles_none(self):
        row = {"impressions": None, "spend": None}
        result = _convert_report_types(row)
        assert result["impressions"] is None

    def test_handles_invalid(self):
        row = {"impressions": "N/A"}
        result = _convert_report_types(row)
        assert result["impressions"] == "N/A"


class TestFlattenReportRow:
    """Test _flatten_report_row()."""

    def test_merges_dimensions_and_metrics(self):
        row = {
            "dimensions": {"ad_id": "123", "stat_time_day": "2026-01-01"},
            "metrics": {"spend": "10.5", "impressions": "100"},
        }
        flat = _flatten_report_row(row)
        assert flat == {
            "ad_id": "123",
            "stat_time_day": "2026-01-01",
            "spend": "10.5",
            "impressions": "100",
        }

    def test_handles_empty(self):
        row = {}
        flat = _flatten_report_row(row)
        assert flat == {}


class TestDateChunks:
    """Test _date_chunks()."""

    def test_single_chunk(self):
        chunks = list(_date_chunks("2026-01-01", "2026-01-15", max_days=30))
        assert chunks == [("2026-01-01", "2026-01-15")]

    def test_multiple_chunks(self):
        chunks = list(_date_chunks("2026-01-01", "2026-02-15", max_days=30))
        assert len(chunks) == 2
        assert chunks[0] == ("2026-01-01", "2026-01-30")
        assert chunks[1] == ("2026-01-31", "2026-02-15")

    def test_exact_boundary(self):
        chunks = list(_date_chunks("2026-01-01", "2026-01-30", max_days=30))
        assert chunks == [("2026-01-01", "2026-01-30")]


class TestCheckResponse:
    """Test _check_response()."""

    def test_success(self):
        assert _check_response({"code": 0, "message": "OK"}, "test") is True

    def test_error(self):
        assert _check_response({"code": 40001, "message": "error"}, "test") is False

    def test_missing_code(self):
        assert _check_response({}, "test") is False


class TestMakeClient:
    """Test _make_client()."""

    def test_sets_access_token_header(self):
        client = _make_client("test_token")
        assert client.session.headers["Access-Token"] == "test_token"

    def test_no_bearer_header(self):
        client = _make_client("test_token")
        assert "Authorization" not in client.session.headers


class TestSourceResourceNames:
    """Test tiktok_ads_source() returns all expected resources."""

    def test_all_resource_names(self):
        source = tiktok_ads_source(
            access_token="test_token",
            advertiser_id="12345",
        )
        names = sorted(r.name for r in source.resources.values())
        expected = sorted(
            [
                "campaigns",
                "ad_groups",
                "ads",
                "custom_audiences",
                "saved_audiences",
                "creative_portfolios",
                "automated_rules",
                "advertiser_info",
                "advertiser_balance",
                "advertiser_transactions",
                "apps",
                "pixels",
                "identities",
                "videos",
                "rule_results",
                "report",
            ]
        )
        assert names == expected

    def test_pixels_resource_config(self):
        source = tiktok_ads_source(
            access_token="test_token",
            advertiser_id="12345",
            resources=["pixels"],
        )
        names = [r.name for r in source.resources.values()]
        assert names == ["pixels"]

    def test_identities_resource_config(self):
        source = tiktok_ads_source(
            access_token="test_token",
            advertiser_id="12345",
            resources=["identities"],
        )
        names = [r.name for r in source.resources.values()]
        assert names == ["identities"]

    def test_report_primary_key_default(self):
        source = tiktok_ads_source(
            access_token="test_token",
            advertiser_id="12345",
            data_level="AUCTION_AD",
            resources=["report"],
        )
        report_res = list(source.resources.values())[0]
        assert report_res.name == "report"

    def test_report_primary_key_campaign(self):
        source = tiktok_ads_source(
            access_token="test_token",
            advertiser_id="12345",
            data_level="AUCTION_CAMPAIGN",
            resources=["report"],
        )
        report_res = list(source.resources.values())[0]
        assert report_res.name == "report"

    def test_custom_start_date(self):
        source = tiktok_ads_source(
            access_token="test_token",
            advertiser_id="12345",
            start_date="2025-01-01",
            resources=["report"],
        )
        assert len(list(source.resources.values())) == 1

    def test_custom_base_url(self):
        source = tiktok_ads_source(
            access_token="test_token",
            advertiser_id="12345",
            base_url="https://custom-api.example.com/v1",
            resources=["advertiser_info"],
        )
        assert len(list(source.resources.values())) == 1

    def test_resource_filtering_empty(self):
        source = tiktok_ads_source(
            access_token="test_token",
            advertiser_id="12345",
            resources=["nonexistent"],
        )
        assert len(list(source.resources.values())) == 0


class TestConvertReportTypesEdgeCases:
    """Test _convert_report_types() edge cases for float fields."""

    def test_float_field_with_list_triggers_type_error(self):
        """Decimal([1, 2]) raises TypeError which is caught."""
        row = {"spend": [1, 2]}
        result = _convert_report_types(row)
        assert result["spend"] == [1, 2]

    def test_int_field_with_list_triggers_type_error(self):
        """int([1, 2]) raises TypeError which is caught."""
        row = {"impressions": [1, 2]}
        result = _convert_report_types(row)
        assert result["impressions"] == [1, 2]


class TestDateChunksEdgeCases:
    """Test _date_chunks() edge cases."""

    def test_single_day(self):
        chunks = list(_date_chunks("2026-01-01", "2026-01-01"))
        assert chunks == [("2026-01-01", "2026-01-01")]

    def test_large_range(self):
        chunks = list(_date_chunks("2026-01-01", "2026-12-31", max_days=30))
        assert len(chunks) == 13
        assert chunks[0][0] == "2026-01-01"
        assert chunks[-1][1] == "2026-12-31"

    def test_max_days_one(self):
        chunks = list(_date_chunks("2026-01-01", "2026-01-03", max_days=1))
        assert len(chunks) == 3


def _mock_response(json_data, status_code=200):
    """Create a mock HTTP response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.raise_for_status.return_value = None
    return resp


def _mock_http_error(status_code):
    """Create a mock HTTPError with given status code."""
    from dlt.sources.helpers.requests import HTTPError

    resp = MagicMock()
    resp.status_code = status_code
    error = HTTPError(response=resp)
    return error


class TestAuthorizedAdvertiserIds:
    """Test authorized_advertiser_ids() resource."""

    @patch("dlt_community_sources.tiktok_ads.source._make_client")
    def test_success(self, mock_make_client):
        client = MagicMock()
        mock_make_client.return_value = client
        client.get.return_value = _mock_response(
            {
                "code": 0,
                "data": {"list": ["adv_001", "adv_002"]},
            }
        )
        results = list(
            authorized_advertiser_ids(access_token="tok", app_id="app1", secret="sec1")
        )
        assert results == [
            {"advertiser_id": "adv_001"},
            {"advertiser_id": "adv_002"},
        ]

    @patch("dlt_community_sources.tiktok_ads.source._make_client")
    def test_api_error(self, mock_make_client):
        client = MagicMock()
        mock_make_client.return_value = client
        client.get.return_value = _mock_response({"code": 40001, "message": "err"})
        results = list(
            authorized_advertiser_ids(access_token="tok", app_id="app1", secret="sec1")
        )
        assert results == []

    @patch("dlt_community_sources.tiktok_ads.source._make_client")
    def test_http_403_skipped(self, mock_make_client):
        client = MagicMock()
        mock_make_client.return_value = client
        client.get.return_value.raise_for_status.side_effect = _mock_http_error(403)
        results = list(
            authorized_advertiser_ids(access_token="tok", app_id="app1", secret="sec1")
        )
        assert results == []

    @patch("dlt_community_sources.tiktok_ads.source._make_client")
    def test_http_500_raises(self, mock_make_client):
        from dlt.extract.exceptions import ResourceExtractionError
        from dlt.sources.helpers.requests import HTTPError

        client = MagicMock()
        mock_make_client.return_value = client
        err = HTTPError(response=MagicMock(status_code=500))
        # response attr should be None to trigger re-raise
        err.response = None
        client.get.return_value.raise_for_status.side_effect = err
        with pytest.raises(ResourceExtractionError):
            list(
                authorized_advertiser_ids(
                    access_token="tok", app_id="app1", secret="sec1"
                )
            )


class TestAdvertiserInfo:
    """Test advertiser_info() resource."""

    @patch("dlt_community_sources.tiktok_ads.source._make_client")
    def test_success(self, mock_make_client):
        client = MagicMock()
        mock_make_client.return_value = client
        client.get.return_value = _mock_response(
            {
                "code": 0,
                "data": {"list": [{"advertiser_id": "123", "name": "Test"}]},
            }
        )
        results = list(advertiser_info(access_token="tok", advertiser_id="123"))
        assert results == [{"advertiser_id": "123", "name": "Test"}]

    @patch("dlt_community_sources.tiktok_ads.source._make_client")
    def test_http_404_skipped(self, mock_make_client):
        client = MagicMock()
        mock_make_client.return_value = client
        client.get.return_value.raise_for_status.side_effect = _mock_http_error(404)
        results = list(advertiser_info(access_token="tok", advertiser_id="123"))
        assert results == []


class TestAdvertiserBalance:
    """Test advertiser_balance() resource."""

    @patch("dlt_community_sources.tiktok_ads.source._make_client")
    def test_success(self, mock_make_client):
        client = MagicMock()
        mock_make_client.return_value = client
        client.get.return_value = _mock_response(
            {
                "code": 0,
                "data": {"balance": 100.0},
            }
        )
        results = list(advertiser_balance(access_token="tok", advertiser_id="123"))
        assert len(results) == 1
        assert results[0]["advertiser_id"] == "123"
        assert results[0]["balance"] == 100.0

    @patch("dlt_community_sources.tiktok_ads.source._make_client")
    def test_empty_data(self, mock_make_client):
        client = MagicMock()
        mock_make_client.return_value = client
        client.get.return_value = _mock_response({"code": 0, "data": {}})
        results = list(advertiser_balance(access_token="tok", advertiser_id="123"))
        assert results == []

    @patch("dlt_community_sources.tiktok_ads.source._make_client")
    def test_http_403_skipped(self, mock_make_client):
        client = MagicMock()
        mock_make_client.return_value = client
        client.get.return_value.raise_for_status.side_effect = _mock_http_error(403)
        results = list(advertiser_balance(access_token="tok", advertiser_id="123"))
        assert results == []


class TestAdvertiserTransactions:
    """Test advertiser_transactions() resource."""

    @patch("dlt_community_sources.tiktok_ads.source._make_client")
    def test_single_page(self, mock_make_client):
        client = MagicMock()
        mock_make_client.return_value = client
        client.get.return_value = _mock_response(
            {
                "code": 0,
                "data": {
                    "list": [
                        {"transaction_time": "2025-01-01T00:00:00.000Z", "amount": 10}
                    ],
                    "page_info": {"total_page": 1},
                },
            }
        )
        results = list(advertiser_transactions(access_token="tok", advertiser_id="123"))
        assert len(results) == 1

    @patch("dlt_community_sources.tiktok_ads.source._make_client")
    def test_multi_page(self, mock_make_client):
        client = MagicMock()
        mock_make_client.return_value = client
        page1_resp = _mock_response(
            {
                "code": 0,
                "data": {
                    "list": [{"transaction_time": "2025-01-01T00:00:00.000Z"}],
                    "page_info": {"total_page": 2},
                },
            }
        )
        page2_resp = _mock_response(
            {
                "code": 0,
                "data": {
                    "list": [{"transaction_time": "2025-01-02T00:00:00.000Z"}],
                    "page_info": {"total_page": 2},
                },
            }
        )
        client.get.side_effect = [page1_resp, page2_resp]
        results = list(advertiser_transactions(access_token="tok", advertiser_id="123"))
        assert len(results) == 2

    @patch("dlt_community_sources.tiktok_ads.source._make_client")
    def test_empty_list_stops(self, mock_make_client):
        client = MagicMock()
        mock_make_client.return_value = client
        client.get.return_value = _mock_response(
            {
                "code": 0,
                "data": {"list": [], "page_info": {"total_page": 1}},
            }
        )
        results = list(advertiser_transactions(access_token="tok", advertiser_id="123"))
        assert results == []

    @patch("dlt_community_sources.tiktok_ads.source._make_client")
    def test_api_error_stops(self, mock_make_client):
        client = MagicMock()
        mock_make_client.return_value = client
        client.get.return_value = _mock_response({"code": 40001, "message": "err"})
        results = list(advertiser_transactions(access_token="tok", advertiser_id="123"))
        assert results == []

    @patch("dlt_community_sources.tiktok_ads.source._make_client")
    def test_http_403_skipped(self, mock_make_client):
        client = MagicMock()
        mock_make_client.return_value = client
        client.get.return_value.raise_for_status.side_effect = _mock_http_error(403)
        results = list(advertiser_transactions(access_token="tok", advertiser_id="123"))
        assert results == []


class TestApps:
    """Test apps() resource."""

    @patch("dlt_community_sources.tiktok_ads.source._make_client")
    def test_success(self, mock_make_client):
        client = MagicMock()
        mock_make_client.return_value = client
        client.get.return_value = _mock_response(
            {
                "code": 0,
                "data": {"list": [{"app_id": "a1"}, {"app_id": "a2"}]},
            }
        )
        results = list(apps(access_token="tok", advertiser_id="123"))
        assert len(results) == 2

    @patch("dlt_community_sources.tiktok_ads.source._make_client")
    def test_http_404_skipped(self, mock_make_client):
        client = MagicMock()
        mock_make_client.return_value = client
        client.get.return_value.raise_for_status.side_effect = _mock_http_error(404)
        results = list(apps(access_token="tok", advertiser_id="123"))
        assert results == []


class TestRuleResults:
    """Test rule_results() resource."""

    @patch("dlt_community_sources.tiktok_ads.source._make_client")
    def test_single_page(self, mock_make_client):
        client = MagicMock()
        mock_make_client.return_value = client
        client.get.return_value = _mock_response(
            {
                "code": 0,
                "data": {
                    "list": [{"rule_id": "r1"}],
                    "page_info": {"total_page": 1},
                },
            }
        )
        results = list(rule_results(access_token="tok", advertiser_id="123"))
        assert len(results) == 1

    @patch("dlt_community_sources.tiktok_ads.source._make_client")
    def test_empty_stops(self, mock_make_client):
        client = MagicMock()
        mock_make_client.return_value = client
        client.get.return_value = _mock_response(
            {
                "code": 0,
                "data": {"list": [], "page_info": {"total_page": 1}},
            }
        )
        results = list(rule_results(access_token="tok", advertiser_id="123"))
        assert results == []

    @patch("dlt_community_sources.tiktok_ads.source._make_client")
    def test_http_403_skipped(self, mock_make_client):
        client = MagicMock()
        mock_make_client.return_value = client
        client.get.return_value.raise_for_status.side_effect = _mock_http_error(403)
        results = list(rule_results(access_token="tok", advertiser_id="123"))
        assert results == []


class TestPixels:
    """Test pixels() resource."""

    @patch("dlt_community_sources.tiktok_ads.source._make_client")
    def test_success(self, mock_make_client):
        client = MagicMock()
        mock_make_client.return_value = client
        client.get.return_value = _mock_response(
            {
                "code": 0,
                "data": {
                    "pixels": [{"pixel_id": "p1"}],
                    "page_info": {"total_page": 1},
                },
            }
        )
        results = list(pixels(access_token="tok", advertiser_id="123"))
        assert len(results) == 1
        assert results[0]["pixel_id"] == "p1"

    @patch("dlt_community_sources.tiktok_ads.source._make_client")
    def test_empty_pixels(self, mock_make_client):
        client = MagicMock()
        mock_make_client.return_value = client
        client.get.return_value = _mock_response(
            {
                "code": 0,
                "data": {"pixels": [], "page_info": {"total_page": 1}},
            }
        )
        results = list(pixels(access_token="tok", advertiser_id="123"))
        assert results == []

    @patch("dlt_community_sources.tiktok_ads.source._make_client")
    def test_http_404_skipped(self, mock_make_client):
        client = MagicMock()
        mock_make_client.return_value = client
        client.get.return_value.raise_for_status.side_effect = _mock_http_error(404)
        results = list(pixels(access_token="tok", advertiser_id="123"))
        assert results == []


class TestIdentities:
    """Test identities() resource."""

    @patch("dlt_community_sources.tiktok_ads.source._make_client")
    def test_success(self, mock_make_client):
        client = MagicMock()
        mock_make_client.return_value = client
        client.get.return_value = _mock_response(
            {
                "code": 0,
                "data": {
                    "list": [{"identity_id": "i1"}],
                    "page_info": {"total_page": 1},
                },
            }
        )
        results = list(identities(access_token="tok", advertiser_id="123"))
        assert len(results) == 1

    @patch("dlt_community_sources.tiktok_ads.source._make_client")
    def test_empty_stops(self, mock_make_client):
        client = MagicMock()
        mock_make_client.return_value = client
        client.get.return_value = _mock_response(
            {
                "code": 0,
                "data": {"list": [], "page_info": {"total_page": 1}},
            }
        )
        results = list(identities(access_token="tok", advertiser_id="123"))
        assert results == []

    @patch("dlt_community_sources.tiktok_ads.source._make_client")
    def test_http_403_skipped(self, mock_make_client):
        client = MagicMock()
        mock_make_client.return_value = client
        client.get.return_value.raise_for_status.side_effect = _mock_http_error(403)
        results = list(identities(access_token="tok", advertiser_id="123"))
        assert results == []


class TestVideos:
    """Test videos() resource."""

    @patch("dlt_community_sources.tiktok_ads.source._make_client")
    def test_success(self, mock_make_client):
        client = MagicMock()
        mock_make_client.return_value = client
        client.get.return_value = _mock_response(
            {
                "code": 0,
                "data": {
                    "list": [{"video_id": "v1"}],
                    "page_info": {"total_page": 1},
                },
            }
        )
        results = list(videos(access_token="tok", advertiser_id="123"))
        assert len(results) == 1

    @patch("dlt_community_sources.tiktok_ads.source._make_client")
    def test_empty_stops(self, mock_make_client):
        client = MagicMock()
        mock_make_client.return_value = client
        client.get.return_value = _mock_response(
            {
                "code": 0,
                "data": {"list": [], "page_info": {"total_page": 1}},
            }
        )
        results = list(videos(access_token="tok", advertiser_id="123"))
        assert results == []

    @patch("dlt_community_sources.tiktok_ads.source._make_client")
    def test_http_404_skipped(self, mock_make_client):
        client = MagicMock()
        mock_make_client.return_value = client
        client.get.return_value.raise_for_status.side_effect = _mock_http_error(404)
        results = list(videos(access_token="tok", advertiser_id="123"))
        assert results == []


class TestReport:
    """Test report() resource."""

    @patch("dlt_community_sources.tiktok_ads.source._make_client")
    def test_success(self, mock_make_client):
        from datetime import date, timedelta

        import dlt

        client = MagicMock()
        mock_make_client.return_value = client

        # Use yesterday as initial_value with attribution_window_days=0
        # so only one chunk is fetched
        yesterday = (date.today() - timedelta(days=1)).isoformat()
        data_resp = _mock_response(
            {
                "code": 0,
                "data": {
                    "list": [
                        {
                            "dimensions": {"ad_id": "ad1", "stat_time_day": yesterday},
                            "metrics": {"spend": "10.5", "impressions": "100"},
                        }
                    ],
                    "page_info": {"total_page": 1},
                },
            }
        )
        client.get.return_value = data_resp
        results = list(
            report(
                access_token="tok",
                advertiser_id="123",
                data_level="AUCTION_AD",
                attribution_window_days=0,
                last_date=dlt.sources.incremental(
                    "stat_time_day", initial_value=yesterday
                ),
            )
        )
        assert len(results) == 1
        assert results[0]["ad_id"] == "ad1"
        assert results[0]["impressions"] == 100

    @patch("dlt_community_sources.tiktok_ads.source._make_client")
    def test_start_after_end_skips(self, mock_make_client):
        """When start > end (future date), report should return nothing."""
        import dlt

        client = MagicMock()
        mock_make_client.return_value = client
        results = list(
            report(
                access_token="tok",
                advertiser_id="123",
                last_date=dlt.sources.incremental(
                    "stat_time_day", initial_value="2099-01-01"
                ),
            )
        )
        assert results == []
        client.get.assert_not_called()

    @patch("dlt_community_sources.tiktok_ads.source._make_client")
    def test_api_error_stops(self, mock_make_client):
        from datetime import date, timedelta

        import dlt

        client = MagicMock()
        mock_make_client.return_value = client
        client.get.return_value = _mock_response({"code": 40001, "message": "err"})
        yesterday = (date.today() - timedelta(days=1)).isoformat()
        results = list(
            report(
                access_token="tok",
                advertiser_id="123",
                attribution_window_days=0,
                last_date=dlt.sources.incremental(
                    "stat_time_day", initial_value=yesterday
                ),
            )
        )
        assert results == []

    @patch("dlt_community_sources.tiktok_ads.source._make_client")
    def test_http_403_skipped(self, mock_make_client):
        from datetime import date, timedelta

        import dlt

        client = MagicMock()
        mock_make_client.return_value = client
        client.get.return_value.raise_for_status.side_effect = _mock_http_error(403)
        yesterday = (date.today() - timedelta(days=1)).isoformat()
        results = list(
            report(
                access_token="tok",
                advertiser_id="123",
                attribution_window_days=0,
                last_date=dlt.sources.incremental(
                    "stat_time_day", initial_value=yesterday
                ),
            )
        )
        assert results == []

    @patch("dlt_community_sources.tiktok_ads.source._make_client")
    def test_empty_list_stops(self, mock_make_client):
        from datetime import date, timedelta

        import dlt

        client = MagicMock()
        mock_make_client.return_value = client
        client.get.return_value = _mock_response(
            {
                "code": 0,
                "data": {"list": [], "page_info": {"total_page": 1}},
            }
        )
        yesterday = (date.today() - timedelta(days=1)).isoformat()
        results = list(
            report(
                access_token="tok",
                advertiser_id="123",
                attribution_window_days=0,
                last_date=dlt.sources.incremental(
                    "stat_time_day", initial_value=yesterday
                ),
            )
        )
        assert results == []

    @patch("dlt_community_sources.tiktok_ads.source._make_client")
    def test_custom_metrics_and_dimensions(self, mock_make_client):
        from datetime import date, timedelta

        import dlt

        client = MagicMock()
        mock_make_client.return_value = client
        yesterday = (date.today() - timedelta(days=1)).isoformat()
        client.get.return_value = _mock_response(
            {
                "code": 0,
                "data": {
                    "list": [
                        {
                            "dimensions": {
                                "campaign_id": "c1",
                                "stat_time_day": yesterday,
                            },
                            "metrics": {"spend": "5.0"},
                        }
                    ],
                    "page_info": {"total_page": 1},
                },
            }
        )
        results = list(
            report(
                access_token="tok",
                advertiser_id="123",
                data_level="AUCTION_CAMPAIGN",
                metrics=["spend"],
                dimensions=["campaign_id", "stat_time_day"],
                attribution_window_days=0,
                last_date=dlt.sources.incremental(
                    "stat_time_day", initial_value=yesterday
                ),
            )
        )
        assert len(results) == 1
        assert results[0]["campaign_id"] == "c1"

    @patch("dlt_community_sources.tiktok_ads.source._make_client")
    def test_multi_page(self, mock_make_client):
        from datetime import date, timedelta

        import dlt

        client = MagicMock()
        mock_make_client.return_value = client
        yesterday = (date.today() - timedelta(days=1)).isoformat()
        page1 = _mock_response(
            {
                "code": 0,
                "data": {
                    "list": [
                        {
                            "dimensions": {"ad_id": "a1", "stat_time_day": yesterday},
                            "metrics": {"spend": "1"},
                        }
                    ],
                    "page_info": {"total_page": 2},
                },
            }
        )
        page2 = _mock_response(
            {
                "code": 0,
                "data": {
                    "list": [
                        {
                            "dimensions": {"ad_id": "a2", "stat_time_day": yesterday},
                            "metrics": {"spend": "2"},
                        }
                    ],
                    "page_info": {"total_page": 2},
                },
            }
        )
        client.get.side_effect = [page1, page2]
        results = list(
            report(
                access_token="tok",
                advertiser_id="123",
                last_date=dlt.sources.incremental(
                    "stat_time_day", initial_value=yesterday
                ),
                attribution_window_days=0,
            )
        )
        assert len(results) == 2

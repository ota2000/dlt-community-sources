"""Tests for TikTok Ads source configuration and helpers."""

from decimal import Decimal

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

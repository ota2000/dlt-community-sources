"""Tests for Meta Ads source configuration and helpers."""

from decimal import Decimal

from dlt_community_sources.meta_ads.source import (
    DEFAULT_FIELDS,
    DEFAULT_INSIGHT_FIELDS,
    INSIGHT_FLOAT_FIELDS,
    INSIGHT_INT_FIELDS,
    INSIGHTS_PRIMARY_KEYS,
    _convert_insight_types,
    _rest_api_config,
)


class TestRestApiConfig:
    """Test _rest_api_config() structure."""

    def setup_method(self):
        self.config = _rest_api_config(
            access_token="test_token",
            account_id="123456",
            base_url="https://graph.facebook.com/v25.0",
        )

    def test_client_base_url(self):
        assert self.config["client"]["base_url"] == "https://graph.facebook.com/v25.0/"

    def test_client_auth(self):
        auth = self.config["client"]["auth"]
        assert auth["type"] == "bearer"
        assert auth["token"] == "test_token"

    def test_paginator(self):
        paginator = self.config["client"]["paginator"]
        assert paginator["type"] == "json_link"
        assert paginator["next_url_path"] == "paging.next"

    def test_resource_defaults(self):
        defaults = self.config["resource_defaults"]
        assert defaults["primary_key"] == "id"
        assert defaults["write_disposition"] == "merge"

    def test_resource_names(self):
        names = [r["name"] for r in self.config["resources"]]
        assert names == [
            "campaigns",
            "ad_sets",
            "ads",
            "ad_creatives",
            "custom_audiences",
            "custom_conversions",
            "ad_images",
            "ad_videos",
            "activities",
            "saved_audiences",
        ]

    def test_account_id_prefix(self):
        config = _rest_api_config("token", "123456", "https://example.com")
        paths = [r["endpoint"]["path"] for r in config["resources"]]
        assert all(p.startswith("act_123456/") for p in paths)

    def test_account_id_already_prefixed(self):
        config = _rest_api_config("token", "act_123456", "https://example.com")
        paths = [r["endpoint"]["path"] for r in config["resources"]]
        assert all(p.startswith("act_123456/") for p in paths)
        assert not any("act_act_" in p for p in paths)

    def test_campaign_fields(self):
        campaigns = self.config["resources"][0]
        fields_param = campaigns["endpoint"]["params"]["fields"]
        for field in DEFAULT_FIELDS["campaigns"]:
            assert field in fields_param

    def test_response_actions(self):
        actions = self.config["resource_defaults"]["endpoint"]["response_actions"]
        status_codes = [a["status_code"] for a in actions]
        assert 400 in status_codes
        assert 403 in status_codes
        assert 404 in status_codes

    def test_ad_images_primary_key(self):
        ad_images = [r for r in self.config["resources"] if r["name"] == "ad_images"][0]
        assert ad_images["primary_key"] == "hash"

    def test_activities_write_disposition(self):
        activities = [r for r in self.config["resources"] if r["name"] == "activities"][
            0
        ]
        assert activities["write_disposition"] == "append"

    def test_activities_no_primary_key(self):
        activities = [r for r in self.config["resources"] if r["name"] == "activities"][
            0
        ]
        assert activities["primary_key"] == ""

    def test_custom_fields_override(self):
        config = _rest_api_config(
            "token",
            "123",
            "https://example.com",
            custom_fields={"campaigns": ["id", "name"]},
        )
        campaigns = config["resources"][0]
        assert campaigns["endpoint"]["params"]["fields"] == "id,name"


class TestDefaultFields:
    """Test default field definitions."""

    def test_all_field_sets_exist(self):
        expected = [
            "campaigns",
            "ad_sets",
            "ads",
            "ad_creatives",
            "ad_leads",
            "custom_audiences",
            "custom_conversions",
            "ad_images",
            "ad_videos",
            "activities",
            "saved_audiences",
        ]
        for name in expected:
            assert name in DEFAULT_FIELDS, f"Missing field set: {name}"

    def test_insight_fields_include_basics(self):
        assert "impressions" in DEFAULT_INSIGHT_FIELDS
        assert "clicks" in DEFAULT_INSIGHT_FIELDS
        assert "spend" in DEFAULT_INSIGHT_FIELDS

    def test_insight_fields_include_ids(self):
        assert "campaign_id" in DEFAULT_INSIGHT_FIELDS
        assert "adset_id" in DEFAULT_INSIGHT_FIELDS
        assert "ad_id" in DEFAULT_INSIGHT_FIELDS

    def test_insight_fields_include_dates(self):
        assert "date_start" in DEFAULT_INSIGHT_FIELDS
        assert "date_stop" in DEFAULT_INSIGHT_FIELDS


class TestInsightsPrimaryKeys:
    """Test insights primary key mapping by level."""

    def test_account_level(self):
        assert INSIGHTS_PRIMARY_KEYS["account"] == ["date_start", "date_stop"]

    def test_campaign_level(self):
        assert INSIGHTS_PRIMARY_KEYS["campaign"] == [
            "date_start",
            "date_stop",
            "campaign_id",
        ]

    def test_adset_level(self):
        assert INSIGHTS_PRIMARY_KEYS["adset"] == [
            "date_start",
            "date_stop",
            "adset_id",
        ]

    def test_ad_level(self):
        assert INSIGHTS_PRIMARY_KEYS["ad"] == ["date_start", "date_stop", "ad_id"]


class TestConvertInsightTypes:
    """Test _convert_insight_types() type conversion."""

    def test_converts_int_fields(self):
        row = {"impressions": "1234", "clicks": "56", "reach": "789"}
        result = _convert_insight_types(row)
        assert result["impressions"] == 1234
        assert result["clicks"] == 56
        assert result["reach"] == 789

    def test_converts_float_fields_to_decimal(self):
        row = {"spend": "12.34", "cpc": "0.56", "cpm": "7.89", "ctr": "0.03"}
        result = _convert_insight_types(row)
        assert result["spend"] == Decimal("12.34")
        assert result["cpc"] == Decimal("0.56")
        assert isinstance(result["spend"], Decimal)

    def test_preserves_non_numeric_fields(self):
        row = {"campaign_name": "Test", "impressions": "100"}
        result = _convert_insight_types(row)
        assert result["campaign_name"] == "Test"
        assert result["impressions"] == 100

    def test_handles_none_values(self):
        row = {"impressions": None, "spend": None}
        result = _convert_insight_types(row)
        assert result["impressions"] is None
        assert result["spend"] is None

    def test_handles_missing_fields(self):
        row = {"campaign_name": "Test"}
        result = _convert_insight_types(row)
        assert result == {"campaign_name": "Test"}

    def test_handles_invalid_values(self):
        row = {"impressions": "N/A", "spend": "N/A"}
        result = _convert_insight_types(row)
        assert result["impressions"] == "N/A"
        assert result["spend"] == "N/A"

    def test_int_and_float_field_sets_are_disjoint(self):
        assert INSIGHT_INT_FIELDS.isdisjoint(INSIGHT_FLOAT_FIELDS)

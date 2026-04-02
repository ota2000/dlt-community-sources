"""Tests for Yahoo Ads Display source."""

from decimal import Decimal
from unittest.mock import patch

from dlt_community_sources.yahoo_ads_common.helpers import convert_report_types
from dlt_community_sources.yahoo_ads_display.source import (
    _ENTITY_RESOURCES,
    BASE_URL,
    REPORT_FIELDS,
    REPORT_TYPES,
)


class TestSourceConfig:
    def test_base_url(self):
        assert "ads-display" in BASE_URL
        assert "v19" in BASE_URL

    def test_entity_resources_count(self):
        assert len(_ENTITY_RESOURCES) >= 25

    def test_entity_resources_have_required_fields(self):
        for name, path, disposition, pk in _ENTITY_RESOURCES:
            assert name, "resource name is empty"
            assert "/" in path, f"{name}: path missing /"
            assert disposition in ("merge", "replace", "append")
            assert pk, f"{name}: primary_key is empty"

    def test_display_specific_resources(self):
        names = [r[0] for r in _ENTITY_RESOURCES]
        # YDA-specific resources not in SS
        assert "media" in names
        assert "videos" in names
        assert "feeds" in names
        assert "ad_group_targets" in names
        assert "placement_url_lists" in names
        assert "guaranteed_campaigns" in names
        assert "brand_lift" in names

    def test_report_types(self):
        assert "AD" in REPORT_TYPES
        assert "PLACEMENT_TARGET" in REPORT_TYPES
        assert "AUDIENCE_LIST_TARGET" in REPORT_TYPES

    def test_report_fields_have_day(self):
        for rt, fields in REPORT_FIELDS.items():
            assert "DAY" in fields, f"{rt}: missing DAY field"

    def test_placement_report_fields(self):
        fields = REPORT_FIELDS["PLACEMENT_TARGET"]
        assert "PLACEMENT_URL_LIST_NAME" in fields
        assert "PLACEMENT_URL_LIST_TYPE" in fields

    def test_convert_report_types(self):
        row = {"IMPS": "1,000", "COST": "500.50", "DAY": "2026-01-01", "CLICKS": "--"}
        result = convert_report_types(row)
        assert result["IMPS"] == 1000
        assert result["COST"] == Decimal("500.50")
        assert result["DAY"] == "2026-01-01"
        assert result["CLICKS"] is None


class TestSourceFunction:
    @patch("dlt_community_sources.yahoo_ads_display.source.refresh_access_token")
    def test_returns_source_with_resources(self, mock_refresh):
        mock_refresh.return_value = {"access_token": "at"}
        from dlt_community_sources.yahoo_ads_display.source import (
            yahoo_ads_display_source,
        )

        source = yahoo_ads_display_source(
            client_id="cid",
            client_secret="cs",
            refresh_token="rt",
            account_id="123",
        )
        assert "report" in source.resources
        assert "campaigns" in source.resources
        assert "media" in source.resources

    @patch("dlt_community_sources.yahoo_ads_display.source.refresh_access_token")
    def test_filter_resources(self, mock_refresh):
        mock_refresh.return_value = {"access_token": "at"}
        from dlt_community_sources.yahoo_ads_display.source import (
            yahoo_ads_display_source,
        )

        source = yahoo_ads_display_source(
            client_id="cid",
            client_secret="cs",
            refresh_token="rt",
            account_id="123",
            resources=["report", "media"],
        )
        resource_names = list(source.resources.keys())
        assert "report" in resource_names
        assert "media" in resource_names
        assert "campaigns" not in resource_names

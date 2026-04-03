"""Tests for Yahoo Ads Display source."""

from decimal import Decimal
from unittest.mock import MagicMock, patch

from dlt_community_sources.yahoo_ads_common.helpers import (
    convert_report_types,
    get_report_fields,
)
from dlt_community_sources.yahoo_ads_display.source import (
    _ENTITY_RESOURCES,
    BASE_URL,
    REPORT_TYPES,
)


class TestSourceConfig:
    def test_base_url(self):
        assert "ads-display" in BASE_URL
        assert "v19" in BASE_URL

    def test_entity_resources_count(self):
        assert len(_ENTITY_RESOURCES) >= 27

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

    def test_new_entity_resources_present(self):
        names = [r[0] for r in _ENTITY_RESOURCES]
        assert "account_links" in names
        assert "app_links" in names

    def test_report_types(self):
        assert "AD" in REPORT_TYPES
        assert "PLACEMENT_TARGET" in REPORT_TYPES
        assert "AUDIENCE_LIST_TARGET" in REPORT_TYPES

    def test_convert_report_types(self):
        row = {"IMPS": "1,000", "COST": "500.50", "DAY": "2026-01-01", "CLICKS": "--"}
        result = convert_report_types(row)
        assert result["IMPS"] == 1000
        assert result["COST"] == Decimal("500.50")
        assert result["DAY"] == "2026-01-01"
        assert result["CLICKS"] is None


def _mock_response(json_data, status_code=200):
    mock = MagicMock()
    mock.json.return_value = json_data
    mock.raise_for_status.return_value = None
    mock.status_code = status_code
    return mock


class TestGetReportFields:
    def test_returns_field_names(self):
        client = MagicMock()
        client.post.return_value = _mock_response(
            {
                "rval": {
                    "fields": [
                        {"fieldName": "DAY", "displayFieldNameEN": "Day"},
                        {"fieldName": "IMPS", "displayFieldNameEN": "Impressions"},
                    ]
                }
            }
        )
        result = get_report_fields(client, "https://api", "AD")
        assert result == ["DAY", "IMPS"]
        call_body = client.post.call_args[1]["json"]
        assert call_body["reportType"] == "AD"


class TestSourceFunction:
    @patch("dlt_community_sources.yahoo_ads_display.source.get_report_fields")
    @patch("dlt_community_sources.yahoo_ads_display.source.refresh_access_token")
    def test_returns_source_with_account_id(self, mock_refresh, mock_get_fields):
        """When account_id is specified, use that single account."""
        mock_refresh.return_value = {"access_token": "at"}
        mock_get_fields.return_value = ["DAY", "ACCOUNT_ID", "CAMPAIGN_ID", "IMPS"]
        from dlt_community_sources.yahoo_ads_display.source import (
            yahoo_ads_display_source,
        )

        source = yahoo_ads_display_source(
            client_id="cid",
            client_secret="cs",
            refresh_token="rt",
            base_account_id="mcc_456",
            account_id="123",
        )
        assert "report" in source.resources
        assert "campaigns" in source.resources
        assert "media" in source.resources
        mock_get_fields.assert_called_once()

    @patch("dlt_community_sources.yahoo_ads_display.source.get_report_fields")
    @patch("dlt_community_sources.yahoo_ads_display.source.discover_accounts")
    @patch("dlt_community_sources.yahoo_ads_display.source.refresh_access_token")
    def test_auto_discovers_accounts_when_account_id_none(
        self, mock_refresh, mock_discover, mock_get_fields
    ):
        """When account_id is None, discover_accounts is called."""
        mock_refresh.return_value = {"access_token": "at"}
        mock_discover.return_value = ["111", "222"]
        mock_get_fields.return_value = ["DAY", "ACCOUNT_ID", "CAMPAIGN_ID", "IMPS"]
        from dlt_community_sources.yahoo_ads_display.source import (
            yahoo_ads_display_source,
        )

        source = yahoo_ads_display_source(
            client_id="cid",
            client_secret="cs",
            refresh_token="rt",
            base_account_id="mcc_456",
        )
        mock_discover.assert_called_once()
        mock_get_fields.assert_called_once()
        assert "report" in source.resources
        assert "campaigns" in source.resources

    @patch("dlt_community_sources.yahoo_ads_display.source.get_report_fields")
    @patch("dlt_community_sources.yahoo_ads_display.source.refresh_access_token")
    def test_filter_resources(self, mock_refresh, mock_get_fields):
        mock_refresh.return_value = {"access_token": "at"}
        mock_get_fields.return_value = ["DAY", "ACCOUNT_ID", "CAMPAIGN_ID", "IMPS"]
        from dlt_community_sources.yahoo_ads_display.source import (
            yahoo_ads_display_source,
        )

        source = yahoo_ads_display_source(
            client_id="cid",
            client_secret="cs",
            refresh_token="rt",
            base_account_id="mcc_456",
            account_id="123",
            resources=["report", "media"],
        )
        resource_names = list(source.resources.keys())
        assert "report" in resource_names
        assert "media" in resource_names
        assert "campaigns" not in resource_names

    @patch("dlt_community_sources.yahoo_ads_display.source.refresh_access_token")
    def test_skips_dynamic_fetch_when_report_fields_provided(self, mock_refresh):
        """When report_fields is explicitly provided, get_report_fields is not called."""
        mock_refresh.return_value = {"access_token": "at"}
        from dlt_community_sources.yahoo_ads_display.source import (
            yahoo_ads_display_source,
        )

        with patch(
            "dlt_community_sources.yahoo_ads_display.source.get_report_fields"
        ) as mock_get_fields:
            source = yahoo_ads_display_source(
                client_id="cid",
                client_secret="cs",
                refresh_token="rt",
                base_account_id="mcc_456",
                account_id="123",
                report_fields=["DAY", "IMPS", "CLICKS"],
            )
            mock_get_fields.assert_not_called()
            assert "report" in source.resources

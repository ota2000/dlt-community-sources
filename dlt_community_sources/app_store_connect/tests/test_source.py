"""Tests for dlt source definition."""

from unittest.mock import MagicMock

from dlt_community_sources.app_store_connect import source as mod
from dlt_community_sources.app_store_connect.auth import AppStoreConnectAuth
from dlt_community_sources.app_store_connect.source import _rest_api_config

REST_API_RESOURCE_NAMES = [
    "apps",
    "app_store_versions",
    "builds",
    "beta_testers",
    "beta_groups",
    "bundle_ids",
    "certificates",
    "devices",
    "in_app_purchases",
    "subscriptions",
    "subscription_groups",
    "users",
    "user_invitations",
    "app_categories",
    "territories",
    "pre_release_versions",
    "beta_app_review_submissions",
    "beta_build_localizations",
    "beta_app_localizations",
    "beta_license_agreements",
    "build_beta_details",
    "app_encryption_declarations",
    "provisioning_profiles",
    "review_submissions",
]

REPORT_RESOURCE_NAMES = [
    "sales_reports",
    "finance_reports",
    "analytics_reports",
]


def test_rest_api_config_has_all_resources():
    """Verify the REST API config dict contains all expected resources."""
    mock_auth = MagicMock(spec=AppStoreConnectAuth)
    config = _rest_api_config(mock_auth)

    resource_names = []
    for r in config["resources"]:
        if isinstance(r, str):
            resource_names.append(r)
        else:
            resource_names.append(r["name"])

    for name in REST_API_RESOURCE_NAMES:
        assert name in resource_names, f"Missing REST API resource: {name}"


def test_rest_api_config_defaults():
    """Verify resource defaults are correctly set."""
    mock_auth = MagicMock(spec=AppStoreConnectAuth)
    config = _rest_api_config(mock_auth)

    assert config["resource_defaults"]["primary_key"] == "id"
    assert config["resource_defaults"]["write_disposition"] == "merge"
    assert config["client"]["paginator"]["type"] == "json_link"


def test_rest_api_config_replace_disposition():
    """Verify app_categories and territories use replace disposition."""
    mock_auth = MagicMock(spec=AppStoreConnectAuth)
    config = _rest_api_config(mock_auth)

    replace_resources = {
        r["name"]: r
        for r in config["resources"]
        if isinstance(r, dict) and r.get("write_disposition") == "replace"
    }
    assert "app_categories" in replace_resources
    assert "territories" in replace_resources


def test_rest_api_config_child_resources():
    """Verify parent-child resource relationships are defined."""
    mock_auth = MagicMock(spec=AppStoreConnectAuth)
    config = _rest_api_config(mock_auth)

    resources_by_name = {
        r["name"]: r for r in config["resources"] if isinstance(r, dict)
    }

    # app_store_versions depends on apps
    assert (
        "resources.apps.id"
        in resources_by_name["app_store_versions"]["endpoint"]["path"]
    )
    # in_app_purchases depends on apps
    assert (
        "resources.apps.id" in resources_by_name["in_app_purchases"]["endpoint"]["path"]
    )
    # subscriptions depends on subscription_groups
    assert (
        "resources.subscription_groups.id"
        in resources_by_name["subscriptions"]["endpoint"]["path"]
    )


def test_report_resource_functions_exist():
    """Verify report resource functions are defined."""
    for name in REPORT_RESOURCE_NAMES:
        assert hasattr(mod, name), f"Missing report resource function: {name}"


def test_sales_reports_skips_without_vendor():
    mock_auth = MagicMock()
    result = list(mod.sales_reports(mock_auth, vendor_number=""))
    assert result == []


def test_finance_reports_skips_without_vendor():
    mock_auth = MagicMock()
    result = list(mod.finance_reports(mock_auth, vendor_number=""))
    assert result == []


def test_date_range():
    dates = list(mod._date_range("2026-03-25", "2026-03-27"))
    assert dates == ["2026-03-25", "2026-03-26", "2026-03-27"]


def test_date_range_single_day():
    dates = list(mod._date_range("2026-03-27", "2026-03-27"))
    assert dates == ["2026-03-27"]


def test_date_range_empty():
    dates = list(mod._date_range("2026-03-28", "2026-03-27"))
    assert dates == []


def test_month_range():
    months = list(mod._month_range("2026-01", "2026-03"))
    assert months == ["2026-01", "2026-02", "2026-03"]


def test_month_range_year_boundary():
    months = list(mod._month_range("2025-11", "2026-02"))
    assert months == ["2025-11", "2025-12", "2026-01", "2026-02"]


def test_month_range_single():
    months = list(mod._month_range("2026-03", "2026-03"))
    assert months == ["2026-03"]


def test_month_range_empty():
    months = list(mod._month_range("2026-04", "2026-03"))
    assert months == []

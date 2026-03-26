"""Tests for dlt source definition."""

from unittest.mock import MagicMock, patch

from dlt_community_sources.app_store_connect import source as mod
from dlt_community_sources.app_store_connect.source import app_store_connect_source


def test_source_has_all_resources():
    """Verify all expected resources are defined."""
    expected = [
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
        "sales_reports",
        "finance_reports",
        "analytics_reports",
    ]
    for name in expected:
        assert hasattr(mod, name), f"Missing resource function: {name}"


def test_resource_filtering():
    """Verify resource filtering by name works."""
    with patch(
        "dlt_community_sources.app_store_connect.source.AppStoreConnectClient"
    ) as MockClient:
        mock_client = MagicMock()
        MockClient.return_value = mock_client
        mock_client.get_paginated.return_value = iter([])

        source = app_store_connect_source(
            key_id="TEST",
            issuer_id="TEST",
            private_key="TEST",
            resources=["apps", "builds"],
        )
        resource_names = [r.name for r in source.resources.values()]
        assert "apps" in resource_names
        assert "builds" in resource_names
        assert "users" not in resource_names


def test_apps_resource():
    mock_client = MagicMock()
    mock_client.get_paginated.return_value = iter(
        [{"id": "1", "type": "apps", "attributes": {"name": "Test"}}]
    )
    result = list(mod.apps(mock_client))
    assert len(result) == 1
    assert result[0]["id"] == "1"


def test_app_store_versions_resource():
    mock_client = MagicMock()
    mock_client.get_paginated.side_effect = [
        iter([{"id": "app1"}]),
        iter([{"id": "v1", "attributes": {"versionString": "1.0"}}]),
    ]
    result = list(mod.app_store_versions(mock_client))
    assert len(result) == 1
    assert result[0]["id"] == "v1"


def test_users_resource():
    mock_client = MagicMock()
    mock_client.get_paginated.return_value = iter(
        [{"id": "u1", "attributes": {"username": "test"}}]
    )
    result = list(mod.users(mock_client))
    assert len(result) == 1


def test_sales_reports_skips_without_vendor():
    mock_client = MagicMock()
    result = list(mod.sales_reports(mock_client, vendor_number=""))
    assert result == []
    mock_client.download_tsv.assert_not_called()


def test_finance_reports_skips_without_vendor():
    mock_client = MagicMock()
    result = list(mod.finance_reports(mock_client, vendor_number=""))
    assert result == []
    mock_client.download_tsv.assert_not_called()


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

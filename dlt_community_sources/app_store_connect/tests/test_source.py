"""Tests for dlt source definition."""

from dlt_community_sources.app_store_connect.source import app_store_connect_source


def test_source_has_all_resources():
    """Verify all expected resources are defined."""
    from dlt_community_sources.app_store_connect import source as mod

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
        "sales_reports",
        "finance_reports",
        "analytics_reports",
    ]

    # Check that all resource functions exist
    for name in expected:
        assert hasattr(mod, name), f"Missing resource function: {name}"


def test_resource_filtering():
    """Verify resource filtering by name works."""
    from unittest.mock import MagicMock, patch

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

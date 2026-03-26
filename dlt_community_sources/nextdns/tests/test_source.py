"""Tests for NextDNS dlt source."""

from dlt_community_sources.nextdns import source as mod


def test_source_has_all_resources():
    expected = [
        "profiles",
        "logs",
        "analytics_status",
        "analytics_domains",
        "analytics_blocked_domains",
        "analytics_reasons",
        "analytics_devices",
        "analytics_protocols",
        "analytics_destinations",
    ]
    for name in expected:
        assert hasattr(mod, name), f"Missing resource function: {name}"


def test_resource_filtering():
    from unittest.mock import MagicMock, patch

    with patch("dlt_community_sources.nextdns.source.NextDNSClient") as MockClient:
        mock_client = MagicMock()
        MockClient.return_value = mock_client
        mock_client.get_paginated.return_value = iter([])

        source = mod.nextdns_source(
            api_key="TEST",
            profile_id="abc",
            resources=["profiles", "logs"],
        )
        resource_names = [r.name for r in source.resources.values()]
        assert "profiles" in resource_names
        assert "logs" in resource_names
        assert "analytics_status" not in resource_names

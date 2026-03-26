"""Tests for NextDNS dlt source."""

from unittest.mock import MagicMock, patch

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
        "analytics_ips",
        "analytics_query_types",
        "analytics_ip_versions",
        "analytics_dnssec",
        "analytics_encryption",
    ]
    for name in expected:
        assert hasattr(mod, name), f"Missing resource function: {name}"


def test_resource_filtering():
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


def test_auto_discover_profiles():
    with patch("dlt_community_sources.nextdns.source.NextDNSClient") as MockClient:
        mock_client = MagicMock()
        MockClient.return_value = mock_client
        mock_client.get_paginated.return_value = iter(
            [{"id": "p1", "name": "Profile 1"}]
        )

        source = mod.nextdns_source(
            api_key="TEST",
            resources=["profiles"],
        )
        resource_names = [r.name for r in source.resources.values()]
        assert "profiles" in resource_names


def test_iso_to_unix_ms():
    result = mod._iso_to_unix_ms("2026-03-27T00:00:00.000Z")
    assert isinstance(result, int)
    assert result > 0
    result2 = mod._iso_to_unix_ms("2020-01-01T00:00:00.000Z")
    assert result2 > 0
    assert result > result2


def test_iso_to_unix_ms_invalid():
    assert mod._iso_to_unix_ms("not-a-date") == 0
    assert mod._iso_to_unix_ms("") == 0

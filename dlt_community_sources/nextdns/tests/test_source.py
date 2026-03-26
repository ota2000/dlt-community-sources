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


def test_profiles_resource():
    mock_client = MagicMock()
    mock_client.get_paginated.return_value = iter([{"id": "p1", "name": "Test"}])
    result = list(mod.profiles(mock_client))
    assert len(result) == 1
    assert result[0]["id"] == "p1"


def test_logs_resource():
    mock_client = MagicMock()
    mock_client.get_paginated.return_value = iter(
        [{"timestamp": "2026-03-27T00:00:00.000Z", "domain": "example.com"}]
    )
    result = list(mod.logs(mock_client, profile_ids=["p1"]))
    assert len(result) == 1
    assert result[0]["_profile_id"] == "p1"
    assert result[0]["domain"] == "example.com"


def test_analytics_status_resource():
    mock_client = MagicMock()
    mock_client.get_paginated.return_value = iter([{"id": "default", "queries": 100}])
    result = list(mod.analytics_status(mock_client, profile_ids=["p1"]))
    assert len(result) == 1
    assert result[0]["_profile_id"] == "p1"


def test_analytics_blocked_domains_passes_status_param():
    mock_client = MagicMock()
    mock_client.get_paginated.return_value = iter([])
    list(mod.analytics_blocked_domains(mock_client, profile_ids=["p1"]))
    mock_client.get_paginated.assert_called_with(
        "profiles/p1/analytics/domains", params={"status": "blocked"}
    )


def test_analytics_destinations_passes_type_param():
    mock_client = MagicMock()
    mock_client.get_paginated.return_value = iter([])
    list(mod.analytics_destinations(mock_client, profile_ids=["p1"]))
    mock_client.get_paginated.assert_called_with(
        "profiles/p1/analytics/destinations", params={"type": "countries"}
    )


def test_iso_to_unix_ms():
    result = mod._iso_to_unix_ms("2026-03-27T00:00:00.000Z")
    assert isinstance(result, int)
    assert result > 0
    # Verify round-trip
    result2 = mod._iso_to_unix_ms("2020-01-01T00:00:00.000Z")
    assert result2 > 0
    assert result > result2  # 2026 > 2020


def test_iso_to_unix_ms_invalid():
    assert mod._iso_to_unix_ms("not-a-date") == 0
    assert mod._iso_to_unix_ms("") == 0


def test_empty_profile_ids():
    mock_client = MagicMock()
    result = list(mod.analytics_status(mock_client, profile_ids=None))
    assert result == []
    mock_client.get_paginated.assert_not_called()


def _make_analytics_test(resource_fn):
    mock_client = MagicMock()
    mock_client.get_paginated.return_value = iter([{"id": "1", "queries": 10}])
    result = list(resource_fn(mock_client, profile_ids=["p1"]))
    assert len(result) == 1
    assert result[0]["_profile_id"] == "p1"


def test_analytics_domains():
    _make_analytics_test(mod.analytics_domains)


def test_analytics_reasons():
    _make_analytics_test(mod.analytics_reasons)


def test_analytics_devices():
    _make_analytics_test(mod.analytics_devices)


def test_analytics_protocols():
    _make_analytics_test(mod.analytics_protocols)


def test_analytics_destinations():
    _make_analytics_test(mod.analytics_destinations)


def test_analytics_ips():
    _make_analytics_test(mod.analytics_ips)


def test_analytics_query_types():
    _make_analytics_test(mod.analytics_query_types)


def test_analytics_ip_versions():
    _make_analytics_test(mod.analytics_ip_versions)


def test_analytics_dnssec():
    _make_analytics_test(mod.analytics_dnssec)


def test_analytics_encryption():
    _make_analytics_test(mod.analytics_encryption)

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
        "analytics_status_series",
        "analytics_domains_series",
        "analytics_devices_series",
        "analytics_protocols_series",
        "analytics_destinations_series",
        "analytics_encryption_series",
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


def test_flatten_series():
    mock_client = MagicMock()
    mock_client.get.return_value = {
        "data": [
            {"id": "default", "queries": [10, 20, 30]},
            {"id": "blocked", "queries": [1, 2, 3]},
        ],
        "meta": {
            "series": {
                "times": [
                    "2026-03-25T00:00:00Z",
                    "2026-03-26T00:00:00Z",
                    "2026-03-27T00:00:00Z",
                ],
                "interval": 86400,
            },
            "pagination": {"cursor": None},
        },
    }
    rows = list(mod._flatten_series(mock_client, "profiles/p1/analytics/status;series"))
    assert len(rows) == 6
    assert rows[0] == {
        "id": "default",
        "timestamp": "2026-03-25T00:00:00Z",
        "queries": 10,
    }
    assert rows[3] == {
        "id": "blocked",
        "timestamp": "2026-03-25T00:00:00Z",
        "queries": 1,
    }


def test_flatten_series_empty():
    mock_client = MagicMock()
    mock_client.get.return_value = {
        "data": [],
        "meta": {"series": {"times": []}, "pagination": {"cursor": None}},
    }
    rows = list(mod._flatten_series(mock_client, "profiles/p1/analytics/status;series"))
    assert rows == []


def test_iso_to_unix_ms_invalid():
    assert mod._iso_to_unix_ms("not-a-date") == 0
    assert mod._iso_to_unix_ms("") == 0

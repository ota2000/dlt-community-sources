"""Tests for NextDNS dlt source."""

from unittest.mock import MagicMock

from dlt_community_sources.nextdns import source as mod
from dlt_community_sources.nextdns.source import _rest_api_config

REST_API_RESOURCE_NAMES = [
    "profiles",
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

CUSTOM_RESOURCE_NAMES = [
    "logs",
    "analytics_status_series",
    "analytics_domains_series",
    "analytics_devices_series",
    "analytics_protocols_series",
    "analytics_destinations_series",
    "analytics_encryption_series",
]


def test_rest_api_config_has_all_resources():
    """Verify the REST API config dict contains all expected resources."""
    config = _rest_api_config("TEST_KEY", mod.DEFAULT_BASE_URL)

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
    config = _rest_api_config("TEST_KEY", mod.DEFAULT_BASE_URL)

    assert config["resource_defaults"]["write_disposition"] == "replace"
    assert config["client"]["paginator"]["type"] == "cursor"
    assert config["client"]["auth"]["type"] == "api_key"
    assert config["client"]["auth"]["name"] == "X-Api-Key"


def test_rest_api_config_profiles_merge():
    """Verify profiles uses merge disposition."""
    config = _rest_api_config("TEST_KEY", mod.DEFAULT_BASE_URL)

    profiles = next(
        r
        for r in config["resources"]
        if isinstance(r, dict) and r["name"] == "profiles"
    )
    assert profiles["write_disposition"] == "merge"
    assert profiles["primary_key"] == "id"


def _get_profiles_resource(config):
    return next(
        r
        for r in config["resources"]
        if isinstance(r, dict) and r["name"] == "profiles"
    )


def test_rest_api_config_no_profile_id_has_no_filter():
    """Without profile_id, profiles is not filtered (all profiles loaded)."""
    config = _rest_api_config("TEST_KEY", mod.DEFAULT_BASE_URL)

    profiles = _get_profiles_resource(config)
    assert "processing_steps" not in profiles


def test_rest_api_config_profile_id_filters_profiles():
    """With profile_id, profiles is scoped to that id so child analytics inherit it."""
    config = _rest_api_config("TEST_KEY", mod.DEFAULT_BASE_URL, profile_id="abc123")

    profiles = _get_profiles_resource(config)
    steps = profiles["processing_steps"]
    assert len(steps) == 1

    filter_fn = steps[0]["filter"]
    assert filter_fn({"id": "abc123"}) is True
    assert filter_fn({"id": "other"}) is False
    assert filter_fn({}) is False


def test_rest_api_config_child_resources():
    """Verify analytics resources depend on profiles."""
    config = _rest_api_config("TEST_KEY", mod.DEFAULT_BASE_URL)

    for r in config["resources"]:
        if isinstance(r, dict) and r["name"].startswith("analytics_"):
            assert "resources.profiles.id" in r["endpoint"]["path"], (
                f"{r['name']} should reference profiles"
            )


def test_rest_api_config_blocked_domains_params():
    """Verify analytics_blocked_domains has status=blocked param."""
    config = _rest_api_config("TEST_KEY", mod.DEFAULT_BASE_URL)

    blocked = next(
        r
        for r in config["resources"]
        if isinstance(r, dict) and r["name"] == "analytics_blocked_domains"
    )
    assert blocked["endpoint"]["params"]["status"] == "blocked"


def test_custom_resource_functions_exist():
    """Verify custom resource functions are defined."""
    for name in CUSTOM_RESOURCE_NAMES:
        assert hasattr(mod, name), f"Missing custom resource function: {name}"


def test_iso_to_unix_ms():
    result = mod._iso_to_unix_ms("2026-03-27T00:00:00.000Z")
    assert isinstance(result, int)
    assert result > 0
    result2 = mod._iso_to_unix_ms("2020-01-01T00:00:00.000Z")
    assert result2 > 0
    assert result > result2


def test_flatten_series():
    session = MagicMock()
    resp = MagicMock()
    resp.json.return_value = {
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
    resp.raise_for_status = MagicMock()
    session.get.return_value = resp

    rows = list(mod._flatten_series(session, "profiles/p1/analytics/status;series"))
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
    session = MagicMock()
    resp = MagicMock()
    resp.json.return_value = {
        "data": [],
        "meta": {"series": {"times": []}, "pagination": {"cursor": None}},
    }
    resp.raise_for_status = MagicMock()
    session.get.return_value = resp

    rows = list(mod._flatten_series(session, "profiles/p1/analytics/status;series"))
    assert rows == []


def test_flatten_series_mismatched_lengths():
    """When queries array is shorter than times array, missing values default to 0."""
    session = MagicMock()
    resp = MagicMock()
    resp.json.return_value = {
        "data": [
            {"id": "default", "queries": [10]},
        ],
        "meta": {
            "series": {
                "times": [
                    "2026-03-25T00:00:00Z",
                    "2026-03-26T00:00:00Z",
                    "2026-03-27T00:00:00Z",
                ],
            },
        },
    }
    resp.raise_for_status = MagicMock()
    session.get.return_value = resp

    rows = list(mod._flatten_series(session, "profiles/p1/analytics/status;series"))
    assert len(rows) == 3
    assert rows[0]["queries"] == 10
    assert rows[1]["queries"] == 0
    assert rows[2]["queries"] == 0


def test_iso_to_unix_ms_invalid():
    assert mod._iso_to_unix_ms("not-a-date") == 0
    assert mod._iso_to_unix_ms("") == 0

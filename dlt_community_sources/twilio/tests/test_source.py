"""Tests for Twilio dlt source."""

from unittest.mock import patch

from dlt_community_sources.twilio import source as mod
from dlt_community_sources.twilio.source import _rest_api_config

REST_API_RESOURCE_NAMES = [
    "transcriptions",
    "queues",
    "incoming_phone_numbers",
    "addresses",
    "keys",
    "outgoing_caller_ids",
    "applications",
    "connect_apps",
    "sip_domains",
    "sip_ip_access_control_lists",
    "sip_credential_lists",
]

CUSTOM_RESOURCE_NAMES = [
    "messages",
    "calls",
    "accounts",
    "usage_records",
    "recordings",
    "conferences",
    "notifications",
    "available_phone_numbers",
]


def test_rest_api_config_has_all_resources():
    """Verify the REST API config dict contains all expected resources."""
    config = _rest_api_config("AC_TEST", "user", "pass")

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
    config = _rest_api_config("AC_TEST", "user", "pass")

    assert config["resource_defaults"]["primary_key"] == "sid"
    assert config["resource_defaults"]["write_disposition"] == "merge"
    assert config["client"]["auth"]["type"] == "http_basic"


def test_rest_api_config_base_url():
    """Verify base URL includes account SID."""
    config = _rest_api_config("AC_TEST", "user", "pass")
    assert "AC_TEST" in config["client"]["base_url"]


def test_custom_resource_functions_exist():
    """Verify custom resource functions are defined."""
    # accounts is exposed as accounts_resource function but named "accounts"
    for name in CUSTOM_RESOURCE_NAMES:
        if name == "accounts":
            assert hasattr(mod, "accounts_resource")
        else:
            assert hasattr(mod, name), f"Missing custom resource function: {name}"


def test_resource_filtering():
    with patch("dlt_community_sources.twilio.source.rest_api_resources") as mock_rest:
        mock_rest.return_value = {}

        source = mod.twilio_source(
            account_sid="TEST",
            auth_token="TEST",
            resources=["messages", "calls"],
        )
        resource_names = [r.name for r in source.resources.values()]
        assert "messages" in resource_names
        assert "calls" in resource_names
        assert "accounts" not in resource_names


def test_auth_requires_credentials():
    import pytest

    with pytest.raises(ValueError, match="auth_token"):
        mod.twilio_source(account_sid="TEST")


def test_rfc2822_to_iso():
    assert (
        mod._rfc2822_to_iso("Thu, 27 Mar 2026 15:30:00 +0000")
        == "2026-03-27T15:30:00+0000"
    )


def test_rfc2822_to_iso_sorts_correctly():
    dates_rfc = [
        "Wed, 01 Feb 2024 00:00:00 +0000",
        "Mon, 01 Jan 2024 00:00:00 +0000",
        "Thu, 01 Mar 2025 00:00:00 +0000",
        "Tue, 02 Jan 2024 00:00:00 +0000",
    ]
    converted = [mod._rfc2822_to_iso(d) for d in dates_rfc]
    assert max(converted) == "2025-03-01T00:00:00+0000"


def test_rfc2822_to_iso_fallback():
    assert mod._rfc2822_to_iso("not-a-date") == "not-a-date"
    assert mod._rfc2822_to_iso("") == ""

"""Tests for Twilio dlt source."""

from unittest.mock import MagicMock, patch

from dlt_community_sources.twilio import source as mod


def test_source_has_all_resources():
    expected = [
        "messages",
        "calls",
        "accounts",
        "usage_records",
        "recordings",
        "transcriptions",
        "conferences",
        "queues",
        "incoming_phone_numbers",
        "available_phone_numbers",
        "addresses",
        "keys",
        "outgoing_caller_ids",
        "applications",
        "connect_apps",
        "notifications",
        "sip_domains",
        "sip_ip_access_control_lists",
        "sip_credential_lists",
    ]
    for name in expected:
        assert hasattr(mod, name), f"Missing resource function: {name}"


def test_resource_filtering():
    with patch("dlt_community_sources.twilio.source.TwilioClient") as MockClient:
        mock_client = MagicMock()
        MockClient.return_value = mock_client
        mock_client.get_paginated.return_value = iter([])

        source = mod.twilio_source(
            account_sid="TEST",
            auth_token="TEST",
            resources=["messages", "calls"],
        )
        resource_names = [r.name for r in source.resources.values()]
        assert "messages" in resource_names
        assert "calls" in resource_names
        assert "accounts" not in resource_names


def test_resource_filtering_with_api_key():
    with patch("dlt_community_sources.twilio.source.TwilioClient") as MockClient:
        mock_client = MagicMock()
        MockClient.return_value = mock_client
        mock_client.get_paginated.return_value = iter([])

        mod.twilio_source(
            account_sid="TEST",
            api_key_sid="SK_TEST",
            api_key_secret="SECRET",
            resources=["messages"],
        )
        MockClient.assert_called_once_with(
            "TEST",
            auth_token=None,
            api_key_sid="SK_TEST",
            api_key_secret="SECRET",
        )


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

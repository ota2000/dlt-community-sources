"""Tests for Twilio dlt source."""

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
    ]
    for name in expected:
        assert hasattr(mod, name), f"Missing resource function: {name}"


def test_resource_filtering():
    from unittest.mock import MagicMock, patch

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
    from unittest.mock import MagicMock, patch

    with patch("dlt_community_sources.twilio.source.TwilioClient") as MockClient:
        mock_client = MagicMock()
        MockClient.return_value = mock_client
        mock_client.get_paginated.return_value = iter([])

        source = mod.twilio_source(
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
        resource_names = [r.name for r in source.resources.values()]
        assert "messages" in resource_names

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


def test_messages_resource():
    mock_client = MagicMock()
    mock_client.get_paginated.return_value = iter(
        [{"sid": "SM1", "date_sent": "Thu, 27 Mar 2026 00:00:00 +0000"}]
    )
    result = list(mod.messages(mock_client))
    assert len(result) == 1
    assert result[0]["sid"] == "SM1"
    assert result[0]["_cursor"] == "2026-03-27T00:00:00+0000"


def test_calls_resource():
    mock_client = MagicMock()
    mock_client.get_paginated.return_value = iter(
        [{"sid": "CA1", "start_time": "Thu, 27 Mar 2026 10:00:00 +0000"}]
    )
    result = list(mod.calls(mock_client))
    assert len(result) == 1
    assert result[0]["_cursor"] == "2026-03-27T10:00:00+0000"


def test_accounts_resource():
    mock_client = MagicMock()
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"sid": "AC_TEST", "friendly_name": "Test"}
    mock_client._request.return_value = mock_resp
    mock_client.account_sid = "AC_TEST"
    result = list(mod.accounts(mock_client))
    assert len(result) == 1
    assert result[0]["sid"] == "AC_TEST"


def test_usage_records_resource():
    mock_client = MagicMock()
    mock_client.get_paginated.return_value = iter(
        [{"start_date": "2026-03-27", "category": "sms", "usage": "10"}]
    )
    result = list(mod.usage_records(mock_client))
    assert len(result) == 1
    assert result[0]["category"] == "sms"


def test_available_phone_numbers_resource():
    mock_client = MagicMock()
    mock_client.get.return_value = {
        "available_phone_numbers": [{"phone_number": "+1234567890"}]
    }
    result = list(mod.available_phone_numbers(mock_client))
    assert len(result) == 1
    assert result[0]["phone_number"] == "+1234567890"


def test_notifications_resource():
    mock_client = MagicMock()
    mock_client.get_paginated.return_value = iter(
        [{"sid": "NO1", "message_date": "Thu, 27 Mar 2026 00:00:00 +0000"}]
    )
    result = list(mod.notifications(mock_client))
    assert len(result) == 1
    assert result[0]["_cursor"] == "2026-03-27T00:00:00+0000"


def _make_paginated_test(resource_fn):
    mock_client = MagicMock()
    mock_client.get_paginated.return_value = iter([{"sid": "X1"}])
    result = list(resource_fn(mock_client))
    assert len(result) == 1
    assert result[0]["sid"] == "X1"


def test_recordings_resource():
    mock_client = MagicMock()
    mock_client.get_paginated.return_value = iter(
        [{"sid": "RE1", "date_created": "Thu, 27 Mar 2026 00:00:00 +0000"}]
    )
    result = list(mod.recordings(mock_client))
    assert len(result) == 1
    assert result[0]["_cursor"] == "2026-03-27T00:00:00+0000"


def test_conferences_resource():
    mock_client = MagicMock()
    mock_client.get_paginated.return_value = iter(
        [{"sid": "CF1", "date_created": "Thu, 27 Mar 2026 00:00:00 +0000"}]
    )
    result = list(mod.conferences(mock_client))
    assert len(result) == 1
    assert result[0]["_cursor"] == "2026-03-27T00:00:00+0000"


def test_transcriptions_resource():
    _make_paginated_test(mod.transcriptions)


def test_queues_resource():
    _make_paginated_test(mod.queues)


def test_incoming_phone_numbers_resource():
    _make_paginated_test(mod.incoming_phone_numbers)


def test_addresses_resource():
    _make_paginated_test(mod.addresses)


def test_keys_resource():
    _make_paginated_test(mod.keys)


def test_outgoing_caller_ids_resource():
    _make_paginated_test(mod.outgoing_caller_ids)


def test_applications_resource():
    _make_paginated_test(mod.applications)


def test_connect_apps_resource():
    _make_paginated_test(mod.connect_apps)

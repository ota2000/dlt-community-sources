"""Tests for Twilio API client."""

from unittest.mock import MagicMock, patch

import pytest
import requests

from dlt_community_sources.twilio.client import TwilioClient
from dlt_community_sources.twilio.source import _rfc2822_to_iso


@pytest.fixture
def client():
    with patch.object(TwilioClient, "__init__", lambda self, *args, **kwargs: None):
        c = TwilioClient.__new__(TwilioClient)
        c.account_sid = "AC_TEST"
        c._session = MagicMock()
        return c


def _mock_response(data, resource_key="messages", next_page_uri=None, status_code=200):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = {
        resource_key: data,
        "next_page_uri": next_page_uri,
    }
    resp.raise_for_status = MagicMock()
    return resp


def test_get_paginated_single_page(client):
    items = [{"sid": "SM1"}, {"sid": "SM2"}]
    client._session.request = MagicMock(return_value=_mock_response(items))

    result = list(client.get_paginated("Messages", "messages"))
    assert len(result) == 2
    assert result[0]["sid"] == "SM1"


def test_get_paginated_multiple_pages(client):
    page1 = _mock_response(
        [{"sid": "SM1"}],
        next_page_uri="/2010-04-01/Accounts/AC_TEST/Messages.json?Page=1",
    )
    page2 = _mock_response([{"sid": "SM2"}])

    client._session.request = MagicMock(side_effect=[page1, page2])

    result = list(client.get_paginated("Messages", "messages"))
    assert len(result) == 2


@patch("dlt_community_sources.twilio.client.time.sleep")
def test_retry_on_429(mock_sleep, client):
    rate_limited = MagicMock()
    rate_limited.status_code = 429
    rate_limited.headers = {}

    success = _mock_response([{"sid": "SM1"}])

    client._session.request = MagicMock(side_effect=[rate_limited, success])

    result = list(client.get_paginated("Messages", "messages"))
    assert len(result) == 1
    mock_sleep.assert_called_once()


def test_rfc2822_to_iso_conversion():
    assert _rfc2822_to_iso("Thu, 27 Mar 2026 15:30:00 +0000") == "2026-03-27T15:30:00+0000"
    assert _rfc2822_to_iso("Mon, 01 Jan 2024 00:00:00 +0000") == "2024-01-01T00:00:00+0000"


def test_rfc2822_to_iso_sorts_correctly():
    dates_rfc = [
        "Wed, 01 Feb 2024 00:00:00 +0000",  # Wed
        "Mon, 01 Jan 2024 00:00:00 +0000",  # Mon
        "Thu, 01 Mar 2025 00:00:00 +0000",  # Thu
        "Tue, 02 Jan 2024 00:00:00 +0000",  # Tue
    ]
    converted = [_rfc2822_to_iso(d) for d in dates_rfc]
    assert max(converted) == "2025-03-01T00:00:00+0000"
    assert sorted(converted) == [
        "2024-01-01T00:00:00+0000",
        "2024-01-02T00:00:00+0000",
        "2024-02-01T00:00:00+0000",
        "2025-03-01T00:00:00+0000",
    ]


def test_rfc2822_to_iso_fallback():
    assert _rfc2822_to_iso("not-a-date") == "not-a-date"
    assert _rfc2822_to_iso("") == ""


def test_403_graceful_skip(client):
    error_resp = MagicMock()
    error_resp.status_code = 403
    error_resp.raise_for_status.side_effect = requests.exceptions.HTTPError(
        response=error_resp
    )

    client._session.request = MagicMock(return_value=error_resp)

    result = list(client.get_paginated("Messages", "messages"))
    assert result == []

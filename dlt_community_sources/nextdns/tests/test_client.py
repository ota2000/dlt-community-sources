"""Tests for NextDNS API client."""

from unittest.mock import MagicMock, patch

import pytest
import requests

from dlt_community_sources.nextdns.client import NextDNSClient


@pytest.fixture
def client():
    with patch.object(NextDNSClient, "__init__", lambda self, *args, **kwargs: None):
        c = NextDNSClient.__new__(NextDNSClient)
        c.api_key = "TEST_KEY"
        c._session = MagicMock()
        return c


def _mock_response(data, cursor=None, status_code=200):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = {
        "data": data,
        "meta": {"pagination": {"cursor": cursor}},
    }
    resp.raise_for_status = MagicMock()
    return resp


def test_get_paginated_single_page(client):
    items = [{"id": "1"}, {"id": "2"}]
    client._session.request = MagicMock(return_value=_mock_response(items))

    result = list(client.get_paginated("profiles"))
    assert len(result) == 2
    assert result[0]["id"] == "1"


def test_get_paginated_multiple_pages(client):
    page1 = _mock_response([{"id": "1"}], cursor="abc123")
    page2 = _mock_response([{"id": "2"}])

    client._session.request = MagicMock(side_effect=[page1, page2])

    result = list(client.get_paginated("profiles"))
    assert len(result) == 2


@patch("dlt_community_sources.nextdns.client.time.sleep")
def test_retry_on_429(mock_sleep, client):
    rate_limited = MagicMock()
    rate_limited.status_code = 429

    success = _mock_response([{"id": "1"}])

    client._session.request = MagicMock(side_effect=[rate_limited, success])

    result = list(client.get_paginated("profiles"))
    assert len(result) == 1
    mock_sleep.assert_called_once()


def test_403_graceful_skip(client):
    error_resp = MagicMock()
    error_resp.status_code = 403
    error_resp.raise_for_status.side_effect = requests.exceptions.HTTPError(
        response=error_resp
    )

    client._session.request = MagicMock(return_value=error_resp)

    result = list(client.get_paginated("profiles/abc/logs"))
    assert result == []

"""Tests for App Store Connect API client."""

import gzip
from unittest.mock import MagicMock, patch

import pytest
import requests

from dlt_community_sources.app_store_connect.client import AppStoreConnectClient

TEST_PRIVATE_KEY = """-----BEGIN EC PRIVATE KEY-----
MHcCAQEEILhwBLXcPIjna02ld7Ifk8poVFmhbD5gGIQfuanlitFnoAoGCCqGSM49
AwEHoUQDQgAEWDusakKGGVeANoNlC2U4QdOst3IkbxoIdq736rFAP9x9IpyR+Gs7
oP4O5IpOPmrqV/5E47OntNnClkTQ+GZWIw==
-----END EC PRIVATE KEY-----"""


@pytest.fixture
def client():
    with patch.object(AppStoreConnectClient, "_refresh_token"):
        c = AppStoreConnectClient(
            key_id="TEST",
            issuer_id="TEST",
            private_key=TEST_PRIVATE_KEY,
        )
        return c


def _mock_response(data, status_code=200, next_url=None):
    resp = MagicMock()
    resp.status_code = status_code
    links = {"next": next_url} if next_url else {}
    resp.json.return_value = {"data": data, "links": links}
    resp.raise_for_status = MagicMock()
    return resp


def test_get_paginated_single_page(client):
    items = [{"id": "1", "type": "apps"}, {"id": "2", "type": "apps"}]
    client._session.request = MagicMock(return_value=_mock_response(items))

    result = list(client.get_paginated("apps"))
    assert len(result) == 2
    assert result[0]["id"] == "1"


def test_get_paginated_multiple_pages(client):
    page1 = _mock_response(
        [{"id": "1"}],
        next_url="https://api.appstoreconnect.apple.com/v1/apps?cursor=abc",
    )
    page2 = _mock_response([{"id": "2"}])

    client._session.request = MagicMock(side_effect=[page1, page2])

    result = list(client.get_paginated("apps"))
    assert len(result) == 2
    assert result[0]["id"] == "1"
    assert result[1]["id"] == "2"


def test_token_refresh_on_401(client):
    unauthorized = MagicMock()
    unauthorized.status_code = 401

    success = _mock_response({"key": "value"})

    client._session.request = MagicMock(side_effect=[unauthorized, success])
    client._refresh_token = MagicMock()

    result = client.get("apps")
    assert result == {"data": {"key": "value"}, "links": {}}
    client._refresh_token.assert_called_once()


@patch("dlt_community_sources.app_store_connect.client.time.sleep")
def test_retry_on_429(mock_sleep, client):
    rate_limited = MagicMock()
    rate_limited.status_code = 429

    success = _mock_response([{"id": "1"}])

    client._session.request = MagicMock(side_effect=[rate_limited, success])

    result = list(client.get_paginated("apps"))
    assert len(result) == 1
    mock_sleep.assert_called_once()


@patch("dlt_community_sources.app_store_connect.client.time.sleep")
def test_retry_backoff_increases(mock_sleep, client):
    rate_limited = MagicMock()
    rate_limited.status_code = 429

    success = _mock_response([{"id": "1"}])

    client._session.request = MagicMock(
        side_effect=[rate_limited, rate_limited, rate_limited, success]
    )

    result = list(client.get_paginated("apps"))
    assert len(result) == 1
    # Backoff: 1.0, 2.0, 4.0
    assert mock_sleep.call_count == 3
    calls = [c.args[0] for c in mock_sleep.call_args_list]
    assert calls == [1.0, 2.0, 4.0]


def test_download_tsv_parses_tab_separated(client):
    tsv_content = b"Provider\tSKU\tUnits\nAPPLE\tcom.example\t10\nAPPLE\tcom.other\t5\n"
    resp = MagicMock()
    resp.status_code = 200
    resp.content = tsv_content
    resp.raise_for_status = MagicMock()
    client._session.request = MagicMock(return_value=resp)

    result = client.download_tsv("https://example.com/report.tsv")
    assert len(result) == 2
    assert result[0]["Provider"] == "APPLE"
    assert result[0]["Units"] == "10"
    assert result[1]["SKU"] == "com.other"


def test_download_tsv_handles_gzip(client):
    tsv_content = b"Provider\tSKU\nAPPLE\tcom.example\n"
    compressed = gzip.compress(tsv_content)
    resp = MagicMock()
    resp.status_code = 200
    resp.content = compressed
    resp.raise_for_status = MagicMock()
    client._session.request = MagicMock(return_value=resp)

    result = client.download_tsv("https://example.com/report.tsv.gz")
    assert len(result) == 1
    assert result[0]["Provider"] == "APPLE"


def test_download_tsv_returns_empty_on_404(client):
    error_resp = MagicMock()
    error_resp.status_code = 404
    exc = requests.exceptions.HTTPError(response=error_resp)
    resp = MagicMock()
    resp.raise_for_status.side_effect = exc
    resp.status_code = 404
    client._session.request = MagicMock(return_value=resp)

    result = client.download_tsv("https://example.com/report.tsv")
    assert result == []


def test_download_gzip_tsv_parses_correctly(client):
    tsv_content = b"Date\tAmount\n2026-01-01\t100\n"
    compressed = gzip.compress(tsv_content)
    resp = MagicMock()
    resp.status_code = 200
    resp.content = compressed
    resp.raise_for_status = MagicMock()
    client._session.request = MagicMock(return_value=resp)

    result = client.download_gzip_tsv("https://example.com/report.tsv.gz")
    assert len(result) == 1
    assert result[0]["Date"] == "2026-01-01"
    assert result[0]["Amount"] == "100"

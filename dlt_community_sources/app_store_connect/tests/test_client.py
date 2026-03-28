"""Tests for report download helpers."""

import gzip
from unittest.mock import MagicMock, patch

import requests

from dlt_community_sources.app_store_connect.source import (
    _download_gzip_tsv,
    _download_tsv,
    _make_client,
)


def _mock_response(content, status_code=200):
    resp = MagicMock()
    resp.status_code = status_code
    resp.content = content
    resp.raise_for_status = MagicMock()
    return resp


def test_make_client_sets_auth():
    mock_auth = MagicMock()
    client = _make_client(mock_auth)
    assert client.session.auth is mock_auth


@patch("dlt_community_sources.app_store_connect.source.req")
def test_download_tsv_parses_tab_separated(mock_req):
    tsv_content = b"Provider\tSKU\tUnits\nAPPLE\tcom.example\t10\nAPPLE\tcom.other\t5\n"
    session = MagicMock()
    session.get.return_value = _mock_response(tsv_content)

    result = _download_tsv(session, "https://example.com/report.tsv")
    assert len(result) == 2
    assert result[0]["Provider"] == "APPLE"
    assert result[0]["Units"] == "10"
    assert result[1]["SKU"] == "com.other"


@patch("dlt_community_sources.app_store_connect.source.req")
def test_download_tsv_handles_gzip(mock_req):
    tsv_content = b"Provider\tSKU\nAPPLE\tcom.example\n"
    compressed = gzip.compress(tsv_content)
    session = MagicMock()
    session.get.return_value = _mock_response(compressed)

    result = _download_tsv(session, "https://example.com/report.tsv.gz")
    assert len(result) == 1
    assert result[0]["Provider"] == "APPLE"


@patch("dlt_community_sources.app_store_connect.source.req")
def test_download_tsv_returns_empty_on_404(mock_req):
    error_resp = MagicMock()
    error_resp.status_code = 404
    exc = requests.exceptions.HTTPError(response=error_resp)
    session = MagicMock()
    resp = MagicMock()
    resp.raise_for_status.side_effect = exc
    session.get.return_value = resp
    mock_req.HTTPError = requests.exceptions.HTTPError

    result = _download_tsv(session, "https://example.com/report.tsv")
    assert result == []


@patch("dlt_community_sources.app_store_connect.source.req")
def test_download_gzip_tsv_parses_correctly(mock_req):
    tsv_content = b"Date\tAmount\n2026-01-01\t100\n"
    compressed = gzip.compress(tsv_content)
    session = MagicMock()
    session.get.return_value = _mock_response(compressed)

    result = _download_gzip_tsv(session, "https://example.com/report.tsv.gz")
    assert len(result) == 1
    assert result[0]["Date"] == "2026-01-01"
    assert result[0]["Amount"] == "100"

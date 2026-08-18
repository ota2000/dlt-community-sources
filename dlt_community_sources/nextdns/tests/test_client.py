"""Tests for NextDNS helper functions."""

from unittest.mock import MagicMock

import pytest
import requests

from dlt_community_sources.nextdns.source import _get_paginated, _make_client


def test_make_client_sets_headers():
    client = _make_client("TEST_KEY")
    assert client.session.headers["X-Api-Key"] == "TEST_KEY"
    assert client.session.headers["Accept"] == "application/json"


def _mock_response(data, cursor=None, status_code=200):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = {
        "data": data,
        "meta": {"pagination": {"cursor": cursor}},
    }
    resp.raise_for_status = MagicMock()
    return resp


def test_get_paginated_single_page():
    session = MagicMock()
    session.get.return_value = _mock_response([{"id": "1"}, {"id": "2"}])

    result = list(_get_paginated(session, "profiles"))
    assert len(result) == 2
    assert result[0]["id"] == "1"


def test_get_paginated_multiple_pages():
    session = MagicMock()
    session.get.side_effect = [
        _mock_response([{"id": "1"}], cursor="abc123"),
        _mock_response([{"id": "2"}]),
    ]

    result = list(_get_paginated(session, "profiles"))
    assert len(result) == 2


def test_get_paginated_403_skip():
    # dlt クライアントは .get() 内部で raise する
    session = MagicMock()
    error_resp = MagicMock()
    error_resp.status_code = 403
    session.get.side_effect = requests.exceptions.HTTPError(response=error_resp)

    result = list(_get_paginated(session, "profiles/abc/logs"))
    assert result == []


def test_get_paginated_403_raises_when_skip_disabled():
    session = MagicMock()
    error_resp = MagicMock()
    error_resp.status_code = 403
    session.get.side_effect = requests.exceptions.HTTPError(response=error_resp)

    with pytest.raises(requests.exceptions.HTTPError):
        list(_get_paginated(session, "profiles", skip_client_errors=False))

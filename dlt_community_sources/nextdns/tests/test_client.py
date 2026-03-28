"""Tests for NextDNS helper functions."""

from unittest.mock import MagicMock

import requests

from dlt_community_sources.nextdns.source import _get_paginated, _make_session


def test_make_session_sets_headers():
    session = _make_session("TEST_KEY")
    assert session.headers["X-Api-Key"] == "TEST_KEY"
    assert session.headers["Accept"] == "application/json"


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
    session = MagicMock()
    error_resp = MagicMock()
    error_resp.status_code = 403
    error_resp.raise_for_status.side_effect = requests.exceptions.HTTPError(
        response=error_resp
    )
    session.get.return_value = error_resp

    result = list(_get_paginated(session, "profiles/abc/logs"))
    assert result == []

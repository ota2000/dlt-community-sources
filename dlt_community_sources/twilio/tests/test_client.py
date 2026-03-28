"""Tests for Twilio helper functions."""

from unittest.mock import MagicMock

import requests

from dlt_community_sources.twilio.source import _get_paginated, _make_session


def test_make_session_sets_auth():
    session = _make_session("user", "pass")
    assert session.auth == ("user", "pass")
    assert session.headers["Accept"] == "application/json"


def _mock_response(data, resource_key="items", next_uri=None, status_code=200):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = {
        resource_key: data,
        "next_page_uri": next_uri,
    }
    resp.raise_for_status = MagicMock()
    return resp


def test_get_paginated_single_page():
    session = MagicMock()
    session.get.return_value = _mock_response([{"sid": "1"}, {"sid": "2"}], "messages")

    result = list(
        _get_paginated(
            session,
            "https://api.twilio.com/2010-04-01/Accounts/AC/Messages.json",
            "messages",
        )
    )
    assert len(result) == 2
    assert result[0]["sid"] == "1"


def test_get_paginated_multiple_pages():
    session = MagicMock()
    session.get.side_effect = [
        _mock_response(
            [{"sid": "1"}],
            "messages",
            next_uri="/2010-04-01/Accounts/AC/Messages.json?Page=1",
        ),
        _mock_response([{"sid": "2"}], "messages"),
    ]

    result = list(
        _get_paginated(
            session,
            "https://api.twilio.com/2010-04-01/Accounts/AC/Messages.json",
            "messages",
        )
    )
    assert len(result) == 2


def test_get_paginated_403_skip():
    session = MagicMock()
    error_resp = MagicMock()
    error_resp.status_code = 403
    error_resp.raise_for_status.side_effect = requests.exceptions.HTTPError(
        response=error_resp
    )
    session.get.return_value = error_resp

    result = list(
        _get_paginated(
            session,
            "https://api.twilio.com/2010-04-01/Accounts/AC/Messages.json",
            "messages",
        )
    )
    assert result == []

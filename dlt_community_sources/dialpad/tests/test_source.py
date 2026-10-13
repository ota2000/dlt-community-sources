"""Tests for Dialpad dlt source."""

from unittest.mock import MagicMock, patch

import dlt
import pytest
from dlt.sources.helpers.requests import HTTPError

from dlt_community_sources._utils import (
    PrimaryResourceError,
    PrimaryResourceTerminalError,
    PrimaryResourceTransientError,
    contains_terminal_exception,
)
from dlt_community_sources.dialpad import source as mod
from dlt_community_sources.dialpad.source import (
    _call_cursor,
    _get_paginated,
    _iso_to_unix_ms,
    _make_client,
    _rest_api_config,
    _unix_ms_to_iso,
    calls,
)

REST_API_RESOURCE_NAMES = ["users", "offices", "departments", "call_centers"]

CUSTOM_RESOURCE_NAMES = ["calls"]


def _paged_client(*pages):
    """Client stub whose .get() returns the given page bodies in order.

    Request params are copied as they are seen: the helper reuses one dict
    across pages, so the recorded call args would otherwise all show the
    params of the final request.
    """
    client = MagicMock()
    client.sent_params = []
    responses = [MagicMock(**{"json.return_value": p}) for p in pages]

    def get(url, params=None):
        client.sent_params.append(dict(params or {}))
        return responses[len(client.sent_params) - 1]

    client.get.side_effect = get
    return client


def _http_error(status_code: int) -> HTTPError:
    response = MagicMock()
    response.status_code = status_code
    response.text = '{"error":"detail"}'
    return HTTPError(f"{status_code} Error", response=response)


# --- Config ---


def test_rest_api_config_has_all_resources():
    config = _rest_api_config("TEST_KEY", mod.DEFAULT_BASE_URL)

    names = [r["name"] for r in config["resources"]]
    for name in REST_API_RESOURCE_NAMES:
        assert name in names, f"Missing REST API resource: {name}"


def test_rest_api_config_defaults():
    config = _rest_api_config("TEST_KEY", mod.DEFAULT_BASE_URL)

    assert config["client"]["auth"] == {"type": "bearer", "token": "TEST_KEY"}
    assert config["client"]["paginator"]["type"] == "cursor"
    assert config["client"]["paginator"]["cursor_path"] == "cursor"
    assert config["client"]["paginator"]["cursor_param"] == "cursor"
    assert config["resource_defaults"]["write_disposition"] == "merge"
    assert config["resource_defaults"]["primary_key"] == "id"
    assert config["resource_defaults"]["endpoint"]["data_selector"] == "items"


def test_rest_api_config_skips_client_errors_on_auxiliary_resources():
    config = _rest_api_config("TEST_KEY", mod.DEFAULT_BASE_URL)

    actions = config["resource_defaults"]["endpoint"]["response_actions"]
    assert {a["status_code"] for a in actions} == {400, 403, 404}
    assert all(a["action"] == "ignore" for a in actions)


def test_rest_api_config_base_url():
    config = _rest_api_config("TEST_KEY", "https://sandbox.dialpad.com")

    assert config["client"]["base_url"] == "https://sandbox.dialpad.com/api/v2/"


def test_custom_resource_functions_exist():
    for name in CUSTOM_RESOURCE_NAMES:
        assert hasattr(mod, name), f"Missing custom resource function: {name}"


# --- Source assembly ---


def test_resource_filtering():
    with patch("dlt_community_sources.dialpad.source.rest_api_resources") as mock_rest:
        mock_rest.return_value = []

        source = mod.dialpad_source(api_key="TEST", resources=["calls"])

        assert [r.name for r in source.resources.values()] == ["calls"]


def test_sandbox_selects_sandbox_host():
    with patch("dlt_community_sources.dialpad.source.rest_api_resources") as mock_rest:
        mock_rest.return_value = []

        mod.dialpad_source(api_key="TEST", sandbox=True)

        config = mock_rest.call_args[0][0]
        assert config["client"]["base_url"].startswith(mod.SANDBOX_BASE_URL)


def test_base_url_overrides_sandbox():
    with patch("dlt_community_sources.dialpad.source.rest_api_resources") as mock_rest:
        mock_rest.return_value = []

        mod.dialpad_source(
            api_key="TEST", sandbox=True, base_url="https://example.test/"
        )

        config = mock_rest.call_args[0][0]
        assert config["client"]["base_url"] == "https://example.test/api/v2/"


# --- Cursor conversion ---


def test_unix_ms_to_iso():
    assert _unix_ms_to_iso(1789379387000) == "2026-09-14T09:49:47+00:00"


def test_iso_to_unix_ms_round_trip():
    assert _iso_to_unix_ms(_unix_ms_to_iso(1789379387000)) == 1789379387000


def test_iso_to_unix_ms_treats_naive_timestamp_as_utc():
    assert _iso_to_unix_ms("2026-09-14") == _iso_to_unix_ms("2026-09-14T00:00:00+00:00")


def test_cursor_sorts_correctly_as_string():
    """Epoch milliseconds do not sort as strings; the ISO cursor must."""
    earlier = _unix_ms_to_iso(999999999000)
    later = _unix_ms_to_iso(1789379387000)

    assert earlier < later


def test_call_cursor_uses_date_started():
    assert _call_cursor({"call_id": 1, "date_started": 1789379387000}) == (
        "2026-09-14T09:49:47+00:00"
    )


def test_call_cursor_reads_date_started_sent_as_a_string():
    """The API sends 64-bit timestamps as JSON strings, unlike its reference."""
    assert _call_cursor({"call_id": "1", "date_started": "1789379387000"}) == (
        "2026-09-14T09:49:47+00:00"
    )


@pytest.mark.parametrize("raw", [None, "", "  ", "not-a-number", True, [], {}])
def test_call_cursor_without_usable_date_started_fails_loudly(raw):
    """calls is primary data: an unpositionable row must not load silently."""
    with pytest.raises(PrimaryResourceTerminalError, match="date_started"):
        _call_cursor({"call_id": 1, "date_started": raw})


# --- Client ---


def test_make_client_sends_bearer_token():
    client = _make_client("TEST_KEY")

    assert client.session.headers["Authorization"] == "Bearer TEST_KEY"
    assert client.session.headers["Accept"] == "application/json"


# --- Pagination ---


def test_get_paginated_follows_cursor():
    client = _paged_client(
        {"items": [{"call_id": 1}], "cursor": "page2"},
        {"items": [{"call_id": 2}]},
    )

    rows = list(_get_paginated(client, "call", params={"started_after": 1}))

    assert [r["call_id"] for r in rows] == [1, 2]
    assert client.sent_params[0] == {"started_after": 1}
    assert client.sent_params[1]["cursor"] == "page2"


def test_get_paginated_handles_null_items():
    client = _paged_client({"items": None})

    assert list(_get_paginated(client, "call")) == []


def test_get_paginated_does_not_mutate_caller_params():
    client = _paged_client(
        {"items": [], "cursor": "page2"},
        {"items": []},
    )
    params = {"started_after": 1}

    list(_get_paginated(client, "call", params=params))

    assert params == {"started_after": 1}


# --- calls resource ---


@patch("dlt_community_sources.dialpad.source._make_client")
def test_calls_requests_window_from_incremental_start_value(mock_make_client):
    mock_make_client.return_value = _paged_client({"items": []})

    list(
        calls(
            "TEST_KEY",
            last_started=dlt.sources.incremental(
                "_cursor", initial_value="2026-09-01T00:00:00+00:00"
            ),
        )
    )

    assert mock_make_client.return_value.sent_params == [
        {"started_after": _iso_to_unix_ms("2026-09-01T00:00:00+00:00")}
    ]


@patch("dlt_community_sources.dialpad.source._make_client")
def test_calls_adds_cursor_field(mock_make_client):
    mock_make_client.return_value = _paged_client(
        {"items": [{"call_id": "1", "date_started": "1789379387000"}]}
    )

    rows = list(
        calls(
            "TEST_KEY",
            last_started=dlt.sources.incremental(
                "_cursor", initial_value="2026-09-01T00:00:00+00:00"
            ),
        )
    )

    assert rows[0]["_cursor"] == "2026-09-14T09:49:47+00:00"


@patch("dlt_community_sources.dialpad.source._make_client")
def test_calls_passes_target_filter(mock_make_client):
    mock_make_client.return_value = _paged_client({"items": []})

    list(
        calls(
            "TEST_KEY",
            last_started=dlt.sources.incremental(
                "_cursor", initial_value="2026-09-01T00:00:00+00:00"
            ),
            target_id=123,
            target_type="department",
        )
    )

    params = mock_make_client.return_value.sent_params[0]
    assert params["target_id"] == 123
    assert params["target_type"] == "department"


def _primary_error_from(exc: BaseException) -> PrimaryResourceError:
    """Find the PrimaryResourceError dlt wrapped in its extraction error."""
    seen: set[int] = set()
    current: "BaseException | None" = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, PrimaryResourceError):
            return current
        current = current.__cause__ or current.__context__
    raise AssertionError(f"no PrimaryResourceError in the chain of {exc!r}")


@patch("dlt_community_sources.dialpad.source._make_client")
def test_calls_client_error_is_terminal(mock_make_client):
    client = MagicMock()
    client.get.side_effect = _http_error(403)
    mock_make_client.return_value = client

    with pytest.raises(Exception) as excinfo:
        list(calls("TEST_KEY"))

    error = _primary_error_from(excinfo.value)
    assert isinstance(error, PrimaryResourceTerminalError)
    assert "403" in str(error)
    # dlt wraps resource exceptions, so retry predicates must walk the chain
    assert contains_terminal_exception(excinfo.value)


@pytest.mark.parametrize("status_code", [429, 500])
@patch("dlt_community_sources.dialpad.source._make_client")
def test_calls_retryable_error_is_transient(mock_make_client, status_code):
    client = MagicMock()
    client.get.side_effect = _http_error(status_code)
    mock_make_client.return_value = client

    with pytest.raises(Exception) as excinfo:
        list(calls("TEST_KEY"))

    error = _primary_error_from(excinfo.value)
    assert isinstance(error, PrimaryResourceTransientError)
    assert str(status_code) in str(error)
    assert not contains_terminal_exception(excinfo.value)

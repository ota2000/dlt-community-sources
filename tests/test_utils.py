"""Unit tests for shared _utils helpers (error-handling policy)."""

from unittest.mock import MagicMock

import pytest
from dlt.common.exceptions import TerminalException, TransientException
from dlt.sources.helpers.requests import HTTPError

from dlt_community_sources._utils import (
    PrimaryResourceError,
    PrimaryResourceTerminalError,
    PrimaryResourceTransientError,
    primary_error_from_http,
    response_snippet,
    skip_or_raise,
)


def _http_error(status_code: int, body: str = "") -> HTTPError:
    response = MagicMock()
    response.status_code = status_code
    response.text = body
    return HTTPError(f"{status_code} Client Error", response=response)


class TestResponseSnippet:
    def test_none_response(self):
        assert response_snippet(None) == ""

    def test_truncates_long_body(self):
        response = MagicMock()
        response.text = "x" * 1000
        assert len(response_snippet(response)) == 500

    def test_short_body_unchanged(self):
        response = MagicMock()
        response.text = '{"Errors":[{"Code":2004}]}'
        assert response_snippet(response) == '{"Errors":[{"Code":2004}]}'


class TestPrimaryErrorTaxonomy:
    """PrimaryResourceError subclasses integrate with dlt's retry taxonomy."""

    def test_terminal_error_is_dlt_terminal(self):
        assert issubclass(PrimaryResourceTerminalError, PrimaryResourceError)
        assert issubclass(PrimaryResourceTerminalError, TerminalException)

    def test_transient_error_is_dlt_transient(self):
        assert issubclass(PrimaryResourceTransientError, PrimaryResourceError)
        assert issubclass(PrimaryResourceTransientError, TransientException)

    def test_base_class_catches_both(self):
        with pytest.raises(PrimaryResourceError):
            raise PrimaryResourceTerminalError("x")
        with pytest.raises(PrimaryResourceError):
            raise PrimaryResourceTransientError("x")


class TestPrimaryErrorFromHttp:
    @pytest.mark.parametrize("status", [400, 403, 404, 422])
    def test_client_errors_are_terminal(self, status):
        err = primary_error_from_http(_http_error(status, "detail"), "ctx")
        assert isinstance(err, PrimaryResourceTerminalError)
        assert "detail" in str(err)
        assert str(status) in str(err)

    @pytest.mark.parametrize("status", [500, 502, 503])
    def test_server_errors_are_transient(self, status):
        err = primary_error_from_http(_http_error(status), "ctx")
        assert isinstance(err, PrimaryResourceTransientError)

    def test_no_response_is_transient(self):
        err = primary_error_from_http(HTTPError(), "ctx")
        assert isinstance(err, PrimaryResourceTransientError)


class TestSkipOrRaise:
    @pytest.mark.parametrize("status", [400, 403, 404])
    def test_client_errors_skipped_with_body(self, status, caplog):
        with caplog.at_level("WARNING"):
            skip_or_raise(_http_error(status, '{"error":"detail"}'), "ctx")
        assert '{"error":"detail"}' in caplog.text

    @pytest.mark.parametrize("status", [429, 500, 503])
    def test_other_errors_raised(self, status):
        with pytest.raises(HTTPError):
            skip_or_raise(_http_error(status), "ctx")

    def test_no_response_raised(self):
        with pytest.raises(HTTPError):
            skip_or_raise(HTTPError(), "ctx")

    def test_primary_resource_error_is_not_an_http_error(self):
        # skip helpers only catch HTTPError: PrimaryResourceError must never
        # be swallowed by an auxiliary-resource catch block.
        assert not issubclass(PrimaryResourceError, HTTPError)


class TestContainsTerminalException:
    def test_detects_terminal_through_wrapping_chain(self):
        from dlt_community_sources._utils import contains_terminal_exception

        try:
            try:
                raise PrimaryResourceTerminalError("inner")
            except PrimaryResourceTerminalError as inner:
                raise RuntimeError("wrapper") from inner
        except RuntimeError as wrapped:
            assert contains_terminal_exception(wrapped)

    def test_transient_chain_is_not_terminal(self):
        from dlt_community_sources._utils import contains_terminal_exception

        try:
            try:
                raise PrimaryResourceTransientError("inner")
            except PrimaryResourceTransientError as inner:
                raise RuntimeError("wrapper") from inner
        except RuntimeError as wrapped:
            assert not contains_terminal_exception(wrapped)

    def test_plain_exception_is_not_terminal(self):
        from dlt_community_sources._utils import contains_terminal_exception

        assert not contains_terminal_exception(RuntimeError("x"))


class TestPrimaryErrorFromRequest:
    def test_http_error_classifies_by_status(self):
        from dlt_community_sources._utils import primary_error_from_request

        err = primary_error_from_request(_http_error(403, "denied"), "ctx")
        assert isinstance(err, PrimaryResourceTerminalError)

    def test_connection_error_is_transient(self):
        from dlt.sources.helpers.requests import ConnectionError as ReqConnectionError

        from dlt_community_sources._utils import primary_error_from_request

        err = primary_error_from_request(
            ReqConnectionError("[Errno 113] No route to host"), "ctx"
        )
        assert isinstance(err, PrimaryResourceTransientError)
        assert "No route to host" in str(err)

    def test_timeout_is_transient(self):
        from dlt.sources.helpers.requests import Timeout

        from dlt_community_sources._utils import primary_error_from_request

        err = primary_error_from_request(Timeout("timed out"), "ctx")
        assert isinstance(err, PrimaryResourceTransientError)

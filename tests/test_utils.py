"""Unit tests for shared _utils helpers (error-handling policy)."""

from unittest.mock import MagicMock

import dlt
import pytest
from dlt.sources.helpers.requests import HTTPError

from dlt_community_sources._utils import (
    PrimaryResourceError,
    response_snippet,
    skip_or_raise,
    wrap_resources_safe,
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


class TestWrapResourcesSafe:
    def _resource(self, gen_fn, name: str):
        return dlt.resource(gen_fn, name=name)

    def test_auxiliary_resource_skips_client_error(self):
        def failing():
            yield {"id": 1}
            raise _http_error(404)

        (wrapped,) = wrap_resources_safe([self._resource(failing, "aux")])
        assert list(wrapped) == [{"id": 1}]

    def test_auxiliary_resource_raises_server_error(self):
        def failing():
            yield {"id": 1}
            raise _http_error(500)

        (wrapped,) = wrap_resources_safe([self._resource(failing, "aux")])
        with pytest.raises(Exception, match="500"):
            list(wrapped)

    def test_critical_resource_propagates_client_error(self):
        def failing():
            yield {"id": 1}
            raise _http_error(400, '{"Errors":[]}')

        (wrapped,) = wrap_resources_safe(
            [self._resource(failing, "report")], critical=("report",)
        )
        with pytest.raises(Exception, match="400"):
            list(wrapped)

    def test_primary_resource_error_passes_through_wrapper(self):
        # PrimaryResourceError is not an HTTPError: even a wrapped
        # (non-critical) resource must propagate it.
        def failing():
            yield {"id": 1}
            raise PrimaryResourceError("report submit failed")

        (wrapped,) = wrap_resources_safe([self._resource(failing, "aux")])
        with pytest.raises(Exception, match="report submit failed"):
            list(wrapped)

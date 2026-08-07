"""Shared utilities for dlt-community-sources.

Error-handling policy (see also the Error Handling section in .ai/rules.md):

- Auxiliary resources (metadata: campaigns, ads, audiences, ...) declare
  expected client errors (400/403/404) at each call site with
  ``skip_or_raise`` — mirroring the ``response_actions`` "ignore" pattern of
  dlt's declarative rest_api sources. Everything else propagates.
- Primary resources (the fact data a source exists for: ``report``,
  ``insights``) never skip: failures raise ``PrimaryResourceError``
  subclasses, which integrate with dlt's terminal/transient exception
  taxonomy so ``dlt.pipeline.helpers.retry_load`` can make correct retry
  decisions.
"""

import logging

from dlt.common.exceptions import TerminalException, TransientException
from dlt.sources.helpers.requests import HTTPError, Response

logger = logging.getLogger(__name__)

# HTTP status codes that indicate "no data" (not an error worth retrying)
_SKIP_STATUS_CODES = {400, 403, 404}

# Max length of the response body included in skip/error logs
_BODY_SNIPPET_LEN = 500


class PrimaryResourceError(Exception):
    """A primary data resource failed to fetch its data.

    Primary resources carry the main fact data of a source (e.g. ``report``,
    ``insights``). Unlike auxiliary metadata resources, their failures must
    fail the pipeline loudly instead of being skipped: a silent skip means
    the load "succeeds" with missing data, which downstream consumers cannot
    detect.

    Deliberately not a subclass of ``HTTPError`` so that per-endpoint skip
    helpers never swallow it. Raise one of the concrete subclasses so dlt's
    retry helpers can classify the failure.
    """


class PrimaryResourceTerminalError(PrimaryResourceError, TerminalException):
    """Primary data fetch failed in a way that retrying will not fix.

    Examples: the API rejected the request (4xx), or returned a
    well-formed response without the expected payload. dlt's
    ``retry_load`` will not retry these.
    """


class PrimaryResourceTransientError(PrimaryResourceError, TransientException):
    """Primary data fetch failed in a way that may succeed on retry.

    Examples: report job timed out, the provider reported a server-side
    job failure, or the API returned 5xx after built-in retries.
    """


def response_snippet(response: "Response | None") -> str:
    """Return a truncated response body for skip/error logs.

    API error bodies (e.g. the Microsoft Ads ``Errors`` array) are the only
    way to diagnose a 4xx after the fact — always include them in logs.
    """
    if response is None:
        return ""
    try:
        return response.text[:_BODY_SNIPPET_LEN]
    except Exception:  # noqa: BLE001 - body may be unreadable/detached
        return ""


def primary_error_from_http(e: HTTPError, message: str) -> PrimaryResourceError:
    """Build the right PrimaryResourceError subclass for an HTTP failure.

    Client errors (4xx) are terminal — the request itself is rejected and
    repeating it changes nothing. Anything else (5xx after built-in
    retries, connection resets surfaced as HTTPError) is transient.
    The response body is appended for diagnosability.
    """
    status = e.response.status_code if e.response is not None else None
    detail = f"{message}: HTTP {status or '?'} body={response_snippet(e.response)}"
    if status is not None and 400 <= status < 500:
        return PrimaryResourceTerminalError(detail)
    return PrimaryResourceTransientError(detail)


def skip_or_raise(e: HTTPError, context: str) -> None:
    """Log-and-skip 400/403/404 client errors; re-raise everything else.

    Shared helper for auxiliary-resource fetchers: some APIs return 4xx for
    valid requests on accounts without certain features, which should not
    stop the pipeline. The response body is logged so the cause remains
    diagnosable. Never use this on primary resources.
    """
    if e.response is not None and e.response.status_code in _SKIP_STATUS_CODES:
        logger.warning(
            "Skipping %s: %d body=%s",
            context,
            e.response.status_code,
            response_snippet(e.response),
        )
        return
    raise e

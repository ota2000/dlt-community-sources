"""Shared utilities for dlt-community-sources."""

import logging

from dlt.sources import DltResource
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

    Deliberately not a subclass of ``HTTPError`` so that it propagates
    through ``wrap_resources_safe`` untouched.
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


def skip_or_raise(e: HTTPError, context: str) -> None:
    """Log-and-skip 400/403/404 client errors; re-raise everything else.

    Shared helper for auxiliary-resource fetchers: some APIs return 4xx for
    valid requests on accounts without certain features, which should not
    stop the pipeline. The response body is logged so the cause remains
    diagnosable.
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


def wrap_resources_safe(
    resources: list[DltResource],
    critical: tuple[str, ...] = (),
) -> list[DltResource]:
    """Wrap each resource's generator to catch and log expected errors.

    Only skips resources that fail with HTTP 400/403/404 (no data, no
    permission, not found). All other errors (429 after retries, 5xx,
    connection errors) are raised to stop the pipeline, since they
    indicate a real problem that should be investigated.

    Resources named in *critical* are returned unwrapped: they carry the
    primary data of the source, so even client errors must propagate and
    fail the pipeline instead of producing a silent partial load.

    Raises ValueError when a *critical* name matches no resource: a typo or
    a renamed resource would otherwise silently re-enable the skip behavior
    for the primary data.
    """
    names = {r.name for r in resources}
    missing = set(critical) - names
    if missing:
        raise ValueError(f"critical resource(s) not found in source: {sorted(missing)}")
    for r in resources:
        if r.name in critical:
            continue
        gen = r._pipe.gen
        if callable(gen):
            resource_name = r.name

            def _make_wrapper(gen_fn, name):
                def wrapper(*args, **kwargs):
                    try:
                        yield from gen_fn(*args, **kwargs)
                    except HTTPError as e:
                        skip_or_raise(e, name)

                return wrapper

            r._pipe.replace_gen(_make_wrapper(gen, resource_name))
    return resources

"""dlt source for Dialpad API."""

import logging
from collections.abc import Generator
from datetime import datetime, timezone
from typing import Optional, Sequence

import dlt
from dlt.sources import DltResource
from dlt.sources.helpers import requests as req
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig

from dlt_community_sources._utils import (
    PrimaryResourceTerminalError,
    primary_error_from_request,
)

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://dialpad.com"
SANDBOX_BASE_URL = "https://sandbox.dialpad.com"

# How far back to re-request calls on each run, in seconds.
#
# The Call List API only returns calls that have already concluded, but orders
# and filters them by ``date_started``. A call that was still ringing or
# connected when a run finished is therefore invisible to that run, yet its
# start time is already behind the cursor the run recorded — without an
# overlap it would never be picked up again. One day covers any realistic
# call; re-fetched rows merge on ``call_id`` instead of duplicating.
DEFAULT_LOOKBACK_SECONDS = 24 * 60 * 60

DEFAULT_START_DATE = "2020-01-01T00:00:00+00:00"


def _rest_api_config(api_key: str, base_url: str) -> RESTAPIConfig:
    """Build the REST API config for standard Dialpad endpoints.

    Covers the auxiliary metadata resources only. ``calls`` needs the
    millisecond-epoch cursor conversion in :func:`calls` and is added
    separately by :func:`dialpad_source`.
    """
    return {
        "client": {
            "base_url": f"{base_url}/api/v2/",
            "auth": {"type": "bearer", "token": api_key},
            "paginator": {
                "type": "cursor",
                "cursor_path": "cursor",
                "cursor_param": "cursor",
            },
        },
        "resource_defaults": {
            "primary_key": "id",
            "write_disposition": "merge",
            "endpoint": {
                "data_selector": "items",
                "response_actions": [
                    {"status_code": 400, "action": "ignore"},
                    {"status_code": 403, "action": "ignore"},
                    {"status_code": 404, "action": "ignore"},
                ],
            },
        },
        "resources": [
            {"name": "users", "endpoint": {"path": "users"}},
            {"name": "offices", "endpoint": {"path": "offices"}},
            {"name": "departments", "endpoint": {"path": "departments"}},
            {"name": "call_centers", "endpoint": {"path": "callcenters"}},
        ],
    }


@dlt.source(name="dialpad")
def dialpad_source(
    api_key: str = dlt.secrets.value,
    resources: Optional[Sequence[str]] = None,
    base_url: Optional[str] = None,
    sandbox: bool = False,
    start_date: Optional[str] = None,
    lookback_seconds: int = DEFAULT_LOOKBACK_SECONDS,
    target_id: Optional[int] = None,
    target_type: Optional[str] = None,
) -> list[DltResource]:
    """A dlt source for Dialpad API.

    Args:
        api_key: Dialpad API key. ``calls`` requires a company admin key
            with the ``calls:list`` scope.
        resources: List of resource names to load. None for all.
        base_url: Override the API base URL. Useful for testing.
        sandbox: Route requests to the Dialpad sandbox host. Ignored when
            ``base_url`` is given.
        start_date: Override the incremental start date for ``calls``
            (ISO 8601, e.g. "2026-09-01" or "2026-09-01T00:00:00+00:00").
        lookback_seconds: How far back to re-request calls on each run, so
            calls that were still in progress during the previous run are
            not missed. See :data:`DEFAULT_LOOKBACK_SECONDS`.
        target_id: Restrict ``calls`` to one target (department, user, ...).
        target_type: Type of ``target_id`` (e.g. "department", "user").

    Returns:
        List of dlt resources.
    """
    url = (base_url or (SANDBOX_BASE_URL if sandbox else DEFAULT_BASE_URL)).rstrip("/")

    # REST API resources (declarative)
    config = _rest_api_config(api_key, url)
    rest_resources = rest_api_resources(config)

    # Custom resources (can't be done via rest_api)
    custom_resources = [
        calls(
            api_key,
            last_started=dlt.sources.incremental(
                "_cursor",
                initial_value=start_date or DEFAULT_START_DATE,
                lag=lookback_seconds,
            ),
            base_url=url,
            target_id=target_id,
            target_type=target_type,
        ),
    ]

    all_resources: list[DltResource] = rest_resources + custom_resources

    if resources:
        return [r for r in all_resources if r.name in resources]
    return all_resources


# --- Helpers ---


def _make_client(api_key: str) -> req.Client:
    """Create a dlt HTTP client with Bearer auth and automatic retry."""
    client = req.Client()
    client.session.headers.update(
        {"Authorization": f"Bearer {api_key}", "Accept": "application/json"}
    )
    return client


def _get_paginated(
    client: req.Client,
    path: str,
    params: Optional[dict] = None,
    base_url: str = DEFAULT_BASE_URL,
) -> Generator[dict, None, None]:
    """Fetch all pages of a Dialpad list endpoint using cursor pagination.

    Used for primary data only, so client errors propagate instead of being
    skipped — the caller classifies them via ``primary_error_from_request``.
    """
    params = dict(params or {})
    url = f"{base_url}/api/v2/{path}"
    while True:
        # dlt's Client raises inside .get() (raise_for_status=True default),
        # so the call itself must be inside the try block.
        response = client.get(url, params=params)
        data = response.json()
        yield from (data.get("items") or [])
        cursor = data.get("cursor")
        if not cursor:
            break
        params["cursor"] = cursor


def _iso_to_unix_ms(iso_timestamp: str) -> int:
    """Convert an ISO 8601 timestamp to Unix milliseconds.

    Naive timestamps are read as UTC, matching how the API reports times.
    """
    parsed = datetime.fromisoformat(iso_timestamp)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


def _unix_ms_to_iso(value: int) -> str:
    """Convert Unix milliseconds to an ISO 8601 timestamp.

    The API reports times as millisecond epochs, which do not compare
    correctly as strings; the cursor stores the ISO form instead.
    """
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc).isoformat()


def _as_unix_ms(raw: object) -> Optional[int]:
    """Read a millisecond epoch the API may send as a number or a string.

    Dialpad returns 64-bit ids and timestamps as JSON strings so they survive
    JavaScript number precision, even though the API reference types them as
    integers. Returns None when the value cannot be read as one.
    """
    if isinstance(raw, bool):
        return None
    if isinstance(raw, (int, float)):
        return int(raw)
    if isinstance(raw, str) and raw.strip().lstrip("-").isdigit():
        return int(raw.strip())
    return None


def _call_cursor(item: dict) -> str:
    """Return the incremental cursor for one call record.

    ``date_started`` positions every call in the incremental window, so a
    record without a usable one cannot be loaded correctly. Calls are the
    primary data of this source, so that fails the load rather than
    silently shifting or dropping the row.
    """
    started = _as_unix_ms(item.get("date_started"))
    if started is None:
        raise PrimaryResourceTerminalError(
            f"call {item.get('call_id')!r} has no usable date_started: "
            f"{item.get('date_started')!r}"
        )
    return _unix_ms_to_iso(started)


# --- Custom resources (incremental or non-standard) ---


@dlt.resource(name="calls", write_disposition="merge", primary_key="call_id")
def calls(
    api_key: str,
    last_started=dlt.sources.incremental(
        "_cursor", initial_value=DEFAULT_START_DATE, lag=DEFAULT_LOOKBACK_SECONDS
    ),
    base_url: str = DEFAULT_BASE_URL,
    target_id: Optional[int] = None,
    target_type: Optional[str] = None,
):
    """Concluded calls, in reverse-chronological order by start time.

    Requires a company admin API key with the ``calls:list`` scope.
    """
    client = _make_client(api_key)
    # start_value already has the lookback applied, so the request window and
    # dlt's own incremental filter agree on the same lower bound.
    params: dict = {"started_after": _iso_to_unix_ms(last_started.start_value)}
    if target_id is not None:
        params["target_id"] = target_id
        params["target_type"] = target_type

    try:
        for item in _get_paginated(client, "call", params=params, base_url=base_url):
            item["_cursor"] = _call_cursor(item)
            yield item
    except req.RequestException as e:
        raise primary_error_from_request(e, "call list failed") from e

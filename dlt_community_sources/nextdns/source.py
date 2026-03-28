"""dlt source for NextDNS API."""

import logging
from collections.abc import Generator
from datetime import datetime
from typing import Optional, Sequence

import dlt
import requests as req
from dlt.sources import DltResource
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig

logger = logging.getLogger(__name__)

BASE_URL = "https://api.nextdns.io"


def _rest_api_config(api_key: str) -> RESTAPIConfig:
    """Build the REST API config for standard NextDNS endpoints."""
    return {
        "client": {
            "base_url": f"{BASE_URL}/",
            "auth": {
                "type": "api_key",
                "name": "X-Api-Key",
                "api_key": api_key,
                "location": "header",
            },
            "paginator": {
                "type": "cursor",
                "cursor_path": "meta.pagination.cursor",
                "cursor_param": "cursor",
            },
        },
        "resource_defaults": {
            "write_disposition": "replace",
            "endpoint": {
                "data_selector": "data",
                "response_actions": [
                    {"status_code": 403, "action": "ignore"},
                    {"status_code": 404, "action": "ignore"},
                ],
            },
        },
        "resources": [
            {
                "name": "profiles",
                "primary_key": "id",
                "write_disposition": "merge",
                "endpoint": {"path": "profiles"},
            },
            {
                "name": "analytics_status",
                "endpoint": {
                    "path": "profiles/{resources.profiles.id}/analytics/status",
                },
                "include_from_parent": ["id"],
            },
            {
                "name": "analytics_domains",
                "endpoint": {
                    "path": "profiles/{resources.profiles.id}/analytics/domains",
                },
                "include_from_parent": ["id"],
            },
            {
                "name": "analytics_blocked_domains",
                "endpoint": {
                    "path": "profiles/{resources.profiles.id}/analytics/domains",
                    "params": {"status": "blocked"},
                },
                "include_from_parent": ["id"],
            },
            {
                "name": "analytics_reasons",
                "endpoint": {
                    "path": "profiles/{resources.profiles.id}/analytics/reasons",
                },
                "include_from_parent": ["id"],
            },
            {
                "name": "analytics_devices",
                "endpoint": {
                    "path": "profiles/{resources.profiles.id}/analytics/devices",
                },
                "include_from_parent": ["id"],
            },
            {
                "name": "analytics_protocols",
                "endpoint": {
                    "path": "profiles/{resources.profiles.id}/analytics/protocols",
                },
                "include_from_parent": ["id"],
            },
            {
                "name": "analytics_destinations",
                "endpoint": {
                    "path": "profiles/{resources.profiles.id}/analytics/destinations",
                    "params": {"type": "countries"},
                },
                "include_from_parent": ["id"],
            },
            {
                "name": "analytics_ips",
                "endpoint": {
                    "path": "profiles/{resources.profiles.id}/analytics/ips",
                },
                "include_from_parent": ["id"],
            },
            {
                "name": "analytics_query_types",
                "endpoint": {
                    "path": "profiles/{resources.profiles.id}/analytics/queryTypes",
                },
                "include_from_parent": ["id"],
            },
            {
                "name": "analytics_ip_versions",
                "endpoint": {
                    "path": "profiles/{resources.profiles.id}/analytics/ipVersions",
                },
                "include_from_parent": ["id"],
            },
            {
                "name": "analytics_dnssec",
                "endpoint": {
                    "path": "profiles/{resources.profiles.id}/analytics/dnssec",
                },
                "include_from_parent": ["id"],
            },
            {
                "name": "analytics_encryption",
                "endpoint": {
                    "path": "profiles/{resources.profiles.id}/analytics/encryption",
                },
                "include_from_parent": ["id"],
            },
        ],
    }


@dlt.source(name="nextdns")
def nextdns_source(
    api_key: str = dlt.secrets.value,
    profile_id: Optional[str] = None,
    resources: Optional[Sequence[str]] = None,
) -> list[DltResource]:
    """A dlt source for NextDNS API.

    Args:
        api_key: NextDNS API key from https://my.nextdns.io/account.
        profile_id: NextDNS profile ID. If None, fetches all profiles.
        resources: List of resource names to load. None for all.

    Returns:
        List of dlt resources.
    """
    # REST API resources (declarative)
    config = _rest_api_config(api_key)
    rest_resources = rest_api_resources(config)

    # Discover profile IDs for custom resources
    profile_ids = []
    if profile_id:
        profile_ids = [profile_id]
    else:
        session = _make_session(api_key)
        for p in _get_paginated(session, "profiles"):
            profile_ids.append(p["id"])

    # Custom resources (can't be done via rest_api)
    custom_resources = [
        logs(api_key, profile_ids=profile_ids),
        analytics_status_series(api_key, profile_ids=profile_ids),
        analytics_domains_series(api_key, profile_ids=profile_ids),
        analytics_devices_series(api_key, profile_ids=profile_ids),
        analytics_protocols_series(api_key, profile_ids=profile_ids),
        analytics_destinations_series(api_key, profile_ids=profile_ids),
        analytics_encryption_series(api_key, profile_ids=profile_ids),
    ]

    all_resources: list[DltResource] = rest_resources + custom_resources

    if resources:
        return [r for r in all_resources if r.name in resources]
    return all_resources


# --- Helpers ---


def _make_session(api_key: str) -> req.Session:
    """Create a requests Session with API key auth."""
    session = req.Session()
    session.headers.update({"X-Api-Key": api_key, "Accept": "application/json"})
    return session


def _get_paginated(
    session: req.Session, path: str, params: Optional[dict] = None
) -> Generator[dict, None, None]:
    """Fetch all pages using cursor-based pagination."""
    if params is None:
        params = {}
    url = f"{BASE_URL}/{path}"
    while True:
        try:
            response = session.get(url, params=params)
            response.raise_for_status()
        except req.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code in (403, 404):
                logger.warning(
                    "Request failed (%d) for %s. Skipping.",
                    e.response.status_code,
                    path,
                )
                return
            raise
        data = response.json()
        yield from data.get("data", [])
        cursor = data.get("meta", {}).get("pagination", {}).get("cursor")
        if not cursor:
            break
        params["cursor"] = cursor


def _flatten_series(
    session: req.Session, path: str, params: Optional[dict] = None
) -> Generator[dict, None, None]:
    """Fetch a series endpoint and flatten time-series data into rows.

    The API returns:
        data: [{"id": "x", "queries": [10, 20, ...]}]
        meta.series.times: ["2026-01-01T...", "2026-01-02T...", ...]

    This yields one row per (item, time) combination:
        {"id": "x", "timestamp": "2026-01-01T...", "queries": 10}
    """
    if params is None:
        params = {}
    params.setdefault("from", "-30d")

    url = f"{BASE_URL}/{path}"
    response = session.get(url, params=params)
    response.raise_for_status()
    data = response.json()

    times = data.get("meta", {}).get("series", {}).get("times", [])
    for item in data.get("data", []):
        queries = item.get("queries", [])
        for i, ts in enumerate(times):
            row = {k: v for k, v in item.items() if k != "queries"}
            row["timestamp"] = ts
            row["queries"] = queries[i] if i < len(queries) else 0
            yield row


def _iso_to_unix_ms(iso_timestamp: str) -> int:
    """Convert ISO 8601 timestamp to Unix milliseconds."""
    try:
        dt = datetime.fromisoformat(iso_timestamp.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)
    except (ValueError, AttributeError):
        logger.warning("Failed to parse ISO timestamp: %s", iso_timestamp)
        return 0


# --- Custom resources ---


@dlt.resource(name="logs", write_disposition="append")
def logs(
    api_key: str,
    profile_ids: Optional[list[str]] = None,
    last_timestamp=dlt.sources.incremental(
        "timestamp", initial_value="2020-01-01T00:00:00.000Z"
    ),
):
    """DNS query logs."""
    session = _make_session(api_key)
    from_ts = _iso_to_unix_ms(last_timestamp.last_value)

    for pid in profile_ids or []:
        for item in _get_paginated(
            session, f"profiles/{pid}/logs", params={"from": from_ts}
        ):
            item["_profiles_id"] = pid
            yield item


@dlt.resource(name="analytics_status_series", write_disposition="replace")
def analytics_status_series(api_key: str, profile_ids: Optional[list[str]] = None):
    """Query count by status over time."""
    session = _make_session(api_key)
    for pid in profile_ids or []:
        for row in _flatten_series(session, f"profiles/{pid}/analytics/status;series"):
            row["_profiles_id"] = pid
            yield row


@dlt.resource(name="analytics_domains_series", write_disposition="replace")
def analytics_domains_series(api_key: str, profile_ids: Optional[list[str]] = None):
    """Top queried domains over time."""
    session = _make_session(api_key)
    for pid in profile_ids or []:
        for row in _flatten_series(session, f"profiles/{pid}/analytics/domains;series"):
            row["_profiles_id"] = pid
            yield row


@dlt.resource(name="analytics_devices_series", write_disposition="replace")
def analytics_devices_series(api_key: str, profile_ids: Optional[list[str]] = None):
    """Query count by device over time."""
    session = _make_session(api_key)
    for pid in profile_ids or []:
        for row in _flatten_series(session, f"profiles/{pid}/analytics/devices;series"):
            row["_profiles_id"] = pid
            yield row


@dlt.resource(name="analytics_protocols_series", write_disposition="replace")
def analytics_protocols_series(api_key: str, profile_ids: Optional[list[str]] = None):
    """Query count by protocol over time."""
    session = _make_session(api_key)
    for pid in profile_ids or []:
        for row in _flatten_series(
            session, f"profiles/{pid}/analytics/protocols;series"
        ):
            row["_profiles_id"] = pid
            yield row


@dlt.resource(name="analytics_destinations_series", write_disposition="replace")
def analytics_destinations_series(
    api_key: str, profile_ids: Optional[list[str]] = None
):
    """Query count by destination country over time."""
    session = _make_session(api_key)
    for pid in profile_ids or []:
        for row in _flatten_series(
            session,
            f"profiles/{pid}/analytics/destinations;series",
            params={"type": "countries"},
        ):
            row["_profiles_id"] = pid
            yield row


@dlt.resource(name="analytics_encryption_series", write_disposition="replace")
def analytics_encryption_series(api_key: str, profile_ids: Optional[list[str]] = None):
    """Query count by encryption status over time."""
    session = _make_session(api_key)
    for pid in profile_ids or []:
        for row in _flatten_series(
            session, f"profiles/{pid}/analytics/encryption;series"
        ):
            row["_profiles_id"] = pid
            yield row

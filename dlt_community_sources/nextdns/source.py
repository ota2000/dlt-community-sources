"""dlt source for NextDNS API."""

import logging
from datetime import datetime
from typing import Optional, Sequence

import dlt
from dlt.sources import DltResource

from .client import NextDNSClient

logger = logging.getLogger(__name__)


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
    client = NextDNSClient(api_key)

    profile_ids = []
    if profile_id:
        profile_ids = [profile_id]
    else:
        for p in client.get_paginated("profiles"):
            profile_ids.append(p["id"])

    all_resources = [
        profiles(client),
        logs(client, profile_ids=profile_ids),
        analytics_status(client, profile_ids=profile_ids),
        analytics_domains(client, profile_ids=profile_ids),
        analytics_blocked_domains(client, profile_ids=profile_ids),
        analytics_reasons(client, profile_ids=profile_ids),
        analytics_devices(client, profile_ids=profile_ids),
        analytics_protocols(client, profile_ids=profile_ids),
        analytics_destinations(client, profile_ids=profile_ids),
        analytics_ips(client, profile_ids=profile_ids),
        analytics_query_types(client, profile_ids=profile_ids),
        analytics_ip_versions(client, profile_ids=profile_ids),
        analytics_dnssec(client, profile_ids=profile_ids),
        analytics_encryption(client, profile_ids=profile_ids),
        analytics_status_series(client, profile_ids=profile_ids),
        analytics_domains_series(client, profile_ids=profile_ids),
        analytics_devices_series(client, profile_ids=profile_ids),
        analytics_protocols_series(client, profile_ids=profile_ids),
        analytics_destinations_series(client, profile_ids=profile_ids),
        analytics_encryption_series(client, profile_ids=profile_ids),
    ]

    if resources:
        return [r for r in all_resources if r.name in resources]
    return all_resources


def _flatten_series(client: NextDNSClient, path: str, params: Optional[dict] = None):
    """Fetch a ;series endpoint and flatten time-series data into rows.

    The API returns:
        data: [{"id": "x", "queries": [10, 20, ...]}]
        meta.series.times: ["2026-01-01T...", "2026-01-02T...", ...]

    This yields one row per (item, time) combination:
        {"id": "x", "timestamp": "2026-01-01T...", "queries": 10}
    """
    if params is None:
        params = {}
    params.setdefault("from", "-30d")

    data = client.get(path, params=params)
    times = data.get("meta", {}).get("series", {}).get("times", [])
    for item in data.get("data", []):
        queries = item.get("queries", [])
        for i, ts in enumerate(times):
            row = {k: v for k, v in item.items() if k != "queries"}
            row["timestamp"] = ts
            row["queries"] = queries[i] if i < len(queries) else 0
            yield row


@dlt.resource(name="profiles", write_disposition="merge", primary_key="id")
def profiles(client: NextDNSClient):
    """NextDNS profiles."""
    yield from client.get_paginated("profiles")


def _iso_to_unix_ms(iso_timestamp: str) -> int:
    """Convert ISO 8601 timestamp to Unix milliseconds."""
    try:
        dt = datetime.fromisoformat(iso_timestamp.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)
    except (ValueError, AttributeError):
        return 0


@dlt.resource(name="logs", write_disposition="append")
def logs(
    client: NextDNSClient,
    profile_ids: Optional[list[str]] = None,
    last_timestamp=dlt.sources.incremental(
        "timestamp", initial_value="2020-01-01T00:00:00.000Z"
    ),
):
    """DNS query logs."""
    from_ts = _iso_to_unix_ms(last_timestamp.last_value)

    for pid in profile_ids or []:
        for item in client.get_paginated(
            f"profiles/{pid}/logs", params={"from": from_ts}
        ):
            item["_profile_id"] = pid
            yield item


@dlt.resource(name="analytics_status", write_disposition="replace")
def analytics_status(client: NextDNSClient, profile_ids: Optional[list[str]] = None):
    """Query count by status (default, blocked, allowed)."""
    for pid in profile_ids or []:
        for item in client.get_paginated(f"profiles/{pid}/analytics/status"):
            item["_profile_id"] = pid
            yield item


@dlt.resource(name="analytics_domains", write_disposition="replace")
def analytics_domains(client: NextDNSClient, profile_ids: Optional[list[str]] = None):
    """Top queried domains."""
    for pid in profile_ids or []:
        for item in client.get_paginated(f"profiles/{pid}/analytics/domains"):
            item["_profile_id"] = pid
            yield item


@dlt.resource(name="analytics_blocked_domains", write_disposition="replace")
def analytics_blocked_domains(
    client: NextDNSClient, profile_ids: Optional[list[str]] = None
):
    """Top blocked domains."""
    for pid in profile_ids or []:
        for item in client.get_paginated(
            f"profiles/{pid}/analytics/domains", params={"status": "blocked"}
        ):
            item["_profile_id"] = pid
            yield item


@dlt.resource(name="analytics_reasons", write_disposition="replace")
def analytics_reasons(client: NextDNSClient, profile_ids: Optional[list[str]] = None):
    """Block reasons breakdown."""
    for pid in profile_ids or []:
        for item in client.get_paginated(f"profiles/{pid}/analytics/reasons"):
            item["_profile_id"] = pid
            yield item


@dlt.resource(name="analytics_devices", write_disposition="replace")
def analytics_devices(client: NextDNSClient, profile_ids: Optional[list[str]] = None):
    """Query count by device."""
    for pid in profile_ids or []:
        for item in client.get_paginated(f"profiles/{pid}/analytics/devices"):
            item["_profile_id"] = pid
            yield item


@dlt.resource(name="analytics_protocols", write_disposition="replace")
def analytics_protocols(client: NextDNSClient, profile_ids: Optional[list[str]] = None):
    """Query count by protocol (DoH, DoT, etc.)."""
    for pid in profile_ids or []:
        for item in client.get_paginated(f"profiles/{pid}/analytics/protocols"):
            item["_profile_id"] = pid
            yield item


@dlt.resource(name="analytics_destinations", write_disposition="replace")
def analytics_destinations(
    client: NextDNSClient, profile_ids: Optional[list[str]] = None
):
    """Query count by destination country."""
    for pid in profile_ids or []:
        for item in client.get_paginated(
            f"profiles/{pid}/analytics/destinations", params={"type": "countries"}
        ):
            item["_profile_id"] = pid
            yield item


@dlt.resource(name="analytics_ips", write_disposition="replace")
def analytics_ips(client: NextDNSClient, profile_ids: Optional[list[str]] = None):
    """Query count by client IP."""
    for pid in profile_ids or []:
        for item in client.get_paginated(f"profiles/{pid}/analytics/ips"):
            item["_profile_id"] = pid
            yield item


@dlt.resource(name="analytics_query_types", write_disposition="replace")
def analytics_query_types(
    client: NextDNSClient, profile_ids: Optional[list[str]] = None
):
    """Query count by DNS query type (A, AAAA, MX, etc.)."""
    for pid in profile_ids or []:
        for item in client.get_paginated(f"profiles/{pid}/analytics/queryTypes"):
            item["_profile_id"] = pid
            yield item


@dlt.resource(name="analytics_ip_versions", write_disposition="replace")
def analytics_ip_versions(
    client: NextDNSClient, profile_ids: Optional[list[str]] = None
):
    """Query count by IP version (IPv4 vs IPv6)."""
    for pid in profile_ids or []:
        for item in client.get_paginated(f"profiles/{pid}/analytics/ipVersions"):
            item["_profile_id"] = pid
            yield item


@dlt.resource(name="analytics_dnssec", write_disposition="replace")
def analytics_dnssec(client: NextDNSClient, profile_ids: Optional[list[str]] = None):
    """Query count by DNSSEC validation status."""
    for pid in profile_ids or []:
        for item in client.get_paginated(f"profiles/{pid}/analytics/dnssec"):
            item["_profile_id"] = pid
            yield item


@dlt.resource(name="analytics_encryption", write_disposition="replace")
def analytics_encryption(
    client: NextDNSClient, profile_ids: Optional[list[str]] = None
):
    """Query count by encryption status."""
    for pid in profile_ids or []:
        for item in client.get_paginated(f"profiles/{pid}/analytics/encryption"):
            item["_profile_id"] = pid
            yield item


# --- Analytics Time Series ---


@dlt.resource(name="analytics_status_series", write_disposition="replace")
def analytics_status_series(
    client: NextDNSClient, profile_ids: Optional[list[str]] = None
):
    """Query count by status over time."""
    for pid in profile_ids or []:
        for row in _flatten_series(client, f"profiles/{pid}/analytics/status;series"):
            row["_profile_id"] = pid
            yield row


@dlt.resource(name="analytics_domains_series", write_disposition="replace")
def analytics_domains_series(
    client: NextDNSClient, profile_ids: Optional[list[str]] = None
):
    """Top queried domains over time."""
    for pid in profile_ids or []:
        for row in _flatten_series(client, f"profiles/{pid}/analytics/domains;series"):
            row["_profile_id"] = pid
            yield row


@dlt.resource(name="analytics_devices_series", write_disposition="replace")
def analytics_devices_series(
    client: NextDNSClient, profile_ids: Optional[list[str]] = None
):
    """Query count by device over time."""
    for pid in profile_ids or []:
        for row in _flatten_series(client, f"profiles/{pid}/analytics/devices;series"):
            row["_profile_id"] = pid
            yield row


@dlt.resource(name="analytics_protocols_series", write_disposition="replace")
def analytics_protocols_series(
    client: NextDNSClient, profile_ids: Optional[list[str]] = None
):
    """Query count by protocol over time."""
    for pid in profile_ids or []:
        for row in _flatten_series(
            client, f"profiles/{pid}/analytics/protocols;series"
        ):
            row["_profile_id"] = pid
            yield row


@dlt.resource(name="analytics_destinations_series", write_disposition="replace")
def analytics_destinations_series(
    client: NextDNSClient, profile_ids: Optional[list[str]] = None
):
    """Query count by destination country over time."""
    for pid in profile_ids or []:
        for row in _flatten_series(
            client,
            f"profiles/{pid}/analytics/destinations;series",
            params={"type": "countries"},
        ):
            row["_profile_id"] = pid
            yield row


@dlt.resource(name="analytics_encryption_series", write_disposition="replace")
def analytics_encryption_series(
    client: NextDNSClient, profile_ids: Optional[list[str]] = None
):
    """Query count by encryption status over time."""
    for pid in profile_ids or []:
        for row in _flatten_series(
            client, f"profiles/{pid}/analytics/encryption;series"
        ):
            row["_profile_id"] = pid
            yield row

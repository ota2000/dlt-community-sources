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
    ]

    if resources:
        return [r for r in all_resources if r.name in resources]
    return all_resources


@dlt.resource(name="profiles", write_disposition="merge", primary_key="id")
def profiles(client: NextDNSClient):
    """NextDNS profiles."""
    yield from client.get_paginated("profiles")


@dlt.resource(name="logs", write_disposition="append")
def logs(
    client: NextDNSClient,
    profile_ids: Optional[list[str]] = None,
    last_timestamp=dlt.sources.incremental(
        "timestamp", initial_value="2020-01-01T00:00:00.000Z"
    ),
):
    """DNS query logs."""
    # Convert ISO timestamp to Unix ms for NextDNS API 'from' parameter
    last_value = last_timestamp.last_value
    try:
        dt = datetime.fromisoformat(last_value.replace("Z", "+00:00"))
        from_ts = int(dt.timestamp() * 1000)
    except (ValueError, AttributeError):
        from_ts = 0

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

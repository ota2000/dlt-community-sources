"""dlt source for Twilio API."""

import logging
from email.utils import parsedate_to_datetime
from typing import Optional, Sequence

import dlt
from dlt.sources import DltResource

from .client import BASE_URL, TwilioClient

logger = logging.getLogger(__name__)


@dlt.source(name="twilio")
def twilio_source(
    account_sid: str = dlt.secrets.value,
    auth_token: str = dlt.secrets.value,
    resources: Optional[Sequence[str]] = None,
) -> list[DltResource]:
    """A dlt source for Twilio API.

    Args:
        account_sid: Twilio Account SID.
        auth_token: Twilio Auth Token.
        resources: List of resource names to load. None for all.

    Returns:
        List of dlt resources.
    """
    client = TwilioClient(account_sid, auth_token)

    all_resources = [
        messages(client),
        calls(client),
        accounts(client),
        usage_records(client),
        recordings(client),
        transcriptions(client),
        conferences(client),
        queues(client),
        incoming_phone_numbers(client),
        available_phone_numbers(client),
        addresses(client),
        keys(client),
    ]

    if resources:
        return [r for r in all_resources if r.name in resources]
    return all_resources


def _rfc2822_to_iso(value: str) -> str:
    """Convert RFC 2822 date to ISO 8601 for correct string comparison."""
    try:
        return parsedate_to_datetime(value).strftime("%Y-%m-%dT%H:%M:%S%z")
    except Exception:
        return value


@dlt.resource(name="messages", write_disposition="append", primary_key="sid")
def messages(
    client: TwilioClient,
    last_date=dlt.sources.incremental("_cursor", initial_value="2020-01-01"),
):
    """SMS/MMS messages."""
    params = {"DateSent>": last_date.last_value}
    for item in client.get_paginated("Messages", "messages", params=params):
        item["_cursor"] = _rfc2822_to_iso(item.get("date_sent", ""))
        yield item


@dlt.resource(name="calls", write_disposition="append", primary_key="sid")
def calls(
    client: TwilioClient,
    last_date=dlt.sources.incremental("_cursor", initial_value="2020-01-01"),
):
    """Voice calls."""
    params = {"StartTime>": last_date.last_value}
    for item in client.get_paginated("Calls", "calls", params=params):
        item["_cursor"] = _rfc2822_to_iso(item.get("start_time", ""))
        yield item


@dlt.resource(name="accounts", write_disposition="merge", primary_key="sid")
def accounts(client: TwilioClient):
    """Twilio accounts and subaccounts."""
    resp = client._request("GET", f"{BASE_URL}/Accounts/{client.account_sid}.json")
    yield resp.json()


@dlt.resource(name="usage_records", write_disposition="append")
def usage_records(
    client: TwilioClient,
    last_date=dlt.sources.incremental("start_date", initial_value="2020-01-01"),
):
    """Usage records (daily)."""
    params = {"StartDate": last_date.last_value}
    yield from client.get_paginated(
        "Usage/Records/Daily", "usage_records", params=params
    )


@dlt.resource(name="recordings", write_disposition="append", primary_key="sid")
def recordings(
    client: TwilioClient,
    last_date=dlt.sources.incremental("_cursor", initial_value="2020-01-01"),
):
    """Call recordings."""
    params = {"DateCreated>": last_date.last_value}
    for item in client.get_paginated("Recordings", "recordings", params=params):
        item["_cursor"] = _rfc2822_to_iso(item.get("date_created", ""))
        yield item


@dlt.resource(name="transcriptions", write_disposition="append", primary_key="sid")
def transcriptions(client: TwilioClient):
    """Transcriptions of recordings."""
    yield from client.get_paginated("Transcriptions", "transcriptions")


@dlt.resource(name="conferences", write_disposition="append", primary_key="sid")
def conferences(
    client: TwilioClient,
    last_date=dlt.sources.incremental("_cursor", initial_value="2020-01-01"),
):
    """Conference calls."""
    params = {"DateCreated>": last_date.last_value}
    for item in client.get_paginated("Conferences", "conferences", params=params):
        item["_cursor"] = _rfc2822_to_iso(item.get("date_created", ""))
        yield item


@dlt.resource(name="queues", write_disposition="merge", primary_key="sid")
def queues(client: TwilioClient):
    """Call queues."""
    yield from client.get_paginated("Queues", "queues")


@dlt.resource(
    name="incoming_phone_numbers", write_disposition="merge", primary_key="sid"
)
def incoming_phone_numbers(client: TwilioClient):
    """Phone numbers owned by the account."""
    yield from client.get_paginated("IncomingPhoneNumbers", "incoming_phone_numbers")


@dlt.resource(name="available_phone_numbers", write_disposition="replace")
def available_phone_numbers(client: TwilioClient, country_code: str = "US"):
    """Available phone numbers for purchase."""
    data = client.get(f"AvailablePhoneNumbers/{country_code}/Local")
    yield from data.get("available_phone_numbers", [])


@dlt.resource(name="addresses", write_disposition="merge", primary_key="sid")
def addresses(client: TwilioClient):
    """Addresses on the account."""
    yield from client.get_paginated("Addresses", "addresses")


@dlt.resource(name="keys", write_disposition="merge", primary_key="sid")
def keys(client: TwilioClient):
    """API keys."""
    yield from client.get_paginated("Keys", "keys")

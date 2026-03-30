"""dlt source for Twilio API."""

import logging
from collections.abc import Generator
from decimal import Decimal, InvalidOperation
from email.utils import parsedate_to_datetime
from typing import Optional, Sequence

import dlt
from dlt.sources import DltResource
from dlt.sources.helpers import requests as req
from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://api.twilio.com/2010-04-01"
DEFAULT_API_HOST = "https://api.twilio.com"


def _rest_api_config(
    account_sid: str, username: str, password: str, base_url: str
) -> RESTAPIConfig:
    """Build the REST API config for standard Twilio endpoints."""
    return {
        "client": {
            "base_url": f"{base_url}/Accounts/{account_sid}/",
            "auth": {
                "type": "http_basic",
                "username": username,
                "password": password,
            },
            "paginator": JSONLinkPaginator(next_url_path="next_page_uri"),
        },
        "resource_defaults": {
            "primary_key": "sid",
            "write_disposition": "merge",
            "endpoint": {
                "params": {"PageSize": 100},
                "response_actions": [
                    {"status_code": 403, "action": "ignore"},
                    {"status_code": 404, "action": "ignore"},
                ],
            },
        },
        "resources": [
            {
                "name": "transcriptions",
                "endpoint": {
                    "path": "Transcriptions.json",
                    "data_selector": "transcriptions",
                },
            },
            {
                "name": "queues",
                "endpoint": {
                    "path": "Queues.json",
                    "data_selector": "queues",
                },
            },
            {
                "name": "incoming_phone_numbers",
                "endpoint": {
                    "path": "IncomingPhoneNumbers.json",
                    "data_selector": "incoming_phone_numbers",
                },
            },
            {
                "name": "addresses",
                "endpoint": {
                    "path": "Addresses.json",
                    "data_selector": "addresses",
                },
            },
            {
                "name": "keys",
                "endpoint": {
                    "path": "Keys.json",
                    "data_selector": "keys",
                },
            },
            {
                "name": "outgoing_caller_ids",
                "endpoint": {
                    "path": "OutgoingCallerIds.json",
                    "data_selector": "outgoing_caller_ids",
                },
            },
            {
                "name": "applications",
                "endpoint": {
                    "path": "Applications.json",
                    "data_selector": "applications",
                },
            },
            {
                "name": "connect_apps",
                "endpoint": {
                    "path": "ConnectApps.json",
                    "data_selector": "connect_apps",
                },
            },
            {
                "name": "sip_domains",
                "endpoint": {
                    "path": "SIP/Domains.json",
                    "data_selector": "domains",
                },
            },
            {
                "name": "sip_ip_access_control_lists",
                "endpoint": {
                    "path": "SIP/IpAccessControlLists.json",
                    "data_selector": "ip_access_control_lists",
                },
            },
            {
                "name": "sip_credential_lists",
                "endpoint": {
                    "path": "SIP/CredentialLists.json",
                    "data_selector": "credential_lists",
                },
            },
        ],
    }


@dlt.source(name="twilio")
def twilio_source(
    account_sid: str = dlt.secrets.value,
    auth_token: Optional[str] = None,
    api_key_sid: Optional[str] = None,
    api_key_secret: Optional[str] = None,
    resources: Optional[Sequence[str]] = None,
    base_url: Optional[str] = None,
) -> list[DltResource]:
    """A dlt source for Twilio API.

    Authenticate with either auth_token or API key pair.

    Args:
        account_sid: Twilio Account SID.
        auth_token: Twilio Auth Token (account-level auth).
        api_key_sid: Twilio API Key SID (scoped auth, recommended for production).
        api_key_secret: Twilio API Key Secret.
        resources: List of resource names to load. None for all.
        base_url: Override the API base URL. Useful for testing.

    Returns:
        List of dlt resources.
    """
    url = base_url or DEFAULT_BASE_URL

    if api_key_sid and api_key_secret:
        username, password = api_key_sid, api_key_secret
    elif auth_token:
        username, password = account_sid, auth_token
    else:
        raise ValueError(
            "Provide either auth_token or both api_key_sid and api_key_secret"
        )

    # REST API resources (declarative)
    config = _rest_api_config(account_sid, username, password, url)
    rest_resources = rest_api_resources(config)

    # Custom resources (incremental with RFC 2822 date conversion)
    custom_resources = [
        messages(account_sid, username, password, base_url=url),
        calls(account_sid, username, password, base_url=url),
        accounts_resource(account_sid, username, password, base_url=url),
        usage_records(account_sid, username, password, base_url=url),
        recordings(account_sid, username, password, base_url=url),
        conferences(account_sid, username, password, base_url=url),
        notifications(account_sid, username, password, base_url=url),
        available_phone_numbers(account_sid, username, password, base_url=url),
    ]

    all_resources: list[DltResource] = rest_resources + custom_resources

    if resources:
        return [r for r in all_resources if r.name in resources]
    return all_resources


# --- Helpers ---


def _make_client(username: str, password: str) -> req.Client:
    """Create a dlt HTTP client with Basic auth and automatic retry."""
    client = req.Client()
    client.session.auth = (username, password)
    client.session.headers.update({"Accept": "application/json"})
    return client


def _get_paginated(
    client: req.Client,
    url: str,
    resource_key: str,
    params: Optional[dict] = None,
    api_host: str = DEFAULT_API_HOST,
) -> Generator[dict, None, None]:
    """Fetch all pages from a Twilio list endpoint."""
    if params is None:
        params = {}
    params.setdefault("PageSize", 100)

    while url:
        try:
            response = client.get(url, params=params)
            response.raise_for_status()
        except req.HTTPError as e:
            if e.response is not None and e.response.status_code in (403, 404):
                logger.warning(
                    "Request failed (%d) for %s. Skipping.",
                    e.response.status_code,
                    url,
                )
                return
            raise
        data = response.json()
        yield from data.get(resource_key, [])
        next_uri = data.get("next_page_uri")
        url = f"{api_host}{next_uri}" if next_uri else None
        params = None  # params are in the next_page_uri


def _rfc2822_to_iso(value: str) -> str:
    """Convert RFC 2822 date to ISO 8601 for correct string comparison."""
    try:
        return parsedate_to_datetime(value).strftime("%Y-%m-%dT%H:%M:%S%z")
    except Exception:
        logger.warning("Failed to parse RFC 2822 date: %s", value)
        return value


# --- Custom resources (incremental or non-standard) ---


@dlt.resource(name="messages", write_disposition="append", primary_key="sid")
def messages(
    account_sid: str,
    username: str,
    password: str,
    last_date=dlt.sources.incremental("_cursor", initial_value="2020-01-01"),
    base_url: str = DEFAULT_BASE_URL,
):
    """SMS/MMS messages."""
    client = _make_client(username, password)
    params = {"DateSent>": last_date.last_value}
    url = f"{base_url}/Accounts/{account_sid}/Messages.json"
    for item in _get_paginated(client, url, "messages", params=params):
        item["_cursor"] = _rfc2822_to_iso(item.get("date_sent", ""))
        yield item


@dlt.resource(name="calls", write_disposition="append", primary_key="sid")
def calls(
    account_sid: str,
    username: str,
    password: str,
    last_date=dlt.sources.incremental("_cursor", initial_value="2020-01-01"),
    base_url: str = DEFAULT_BASE_URL,
):
    """Voice calls."""
    client = _make_client(username, password)
    params = {"StartTime>": last_date.last_value}
    url = f"{base_url}/Accounts/{account_sid}/Calls.json"
    for item in _get_paginated(client, url, "calls", params=params):
        item["_cursor"] = _rfc2822_to_iso(item.get("start_time", ""))
        yield item


@dlt.resource(name="accounts", write_disposition="merge", primary_key="sid")
def accounts_resource(
    account_sid: str,
    username: str,
    password: str,
    base_url: str = DEFAULT_BASE_URL,
):
    """Twilio accounts and subaccounts."""
    client = _make_client(username, password)
    response = client.get(f"{base_url}/Accounts/{account_sid}.json")
    response.raise_for_status()
    yield response.json()


@dlt.resource(
    name="usage_records",
    write_disposition="merge",
    primary_key=["category", "start_date"],
)
def usage_records(
    account_sid: str,
    username: str,
    password: str,
    last_date=dlt.sources.incremental("start_date", initial_value="2020-01-01"),
    base_url: str = DEFAULT_BASE_URL,
):
    """Usage records (daily)."""
    client = _make_client(username, password)
    params = {"StartDate": last_date.last_value}
    url = f"{base_url}/Accounts/{account_sid}/Usage/Records/Daily.json"
    for item in _get_paginated(client, url, "usage_records", params=params):
        if "price" in item and item["price"]:
            try:
                item["price"] = Decimal(item["price"])
            except InvalidOperation:
                pass
        yield item


@dlt.resource(name="recordings", write_disposition="append", primary_key="sid")
def recordings(
    account_sid: str,
    username: str,
    password: str,
    last_date=dlt.sources.incremental("_cursor", initial_value="2020-01-01"),
    base_url: str = DEFAULT_BASE_URL,
):
    """Call recordings."""
    client = _make_client(username, password)
    params = {"DateCreated>": last_date.last_value}
    url = f"{base_url}/Accounts/{account_sid}/Recordings.json"
    for item in _get_paginated(client, url, "recordings", params=params):
        item["_cursor"] = _rfc2822_to_iso(item.get("date_created", ""))
        yield item


@dlt.resource(name="conferences", write_disposition="append", primary_key="sid")
def conferences(
    account_sid: str,
    username: str,
    password: str,
    last_date=dlt.sources.incremental("_cursor", initial_value="2020-01-01"),
    base_url: str = DEFAULT_BASE_URL,
):
    """Conference calls."""
    client = _make_client(username, password)
    params = {"DateCreated>": last_date.last_value}
    url = f"{base_url}/Accounts/{account_sid}/Conferences.json"
    for item in _get_paginated(client, url, "conferences", params=params):
        item["_cursor"] = _rfc2822_to_iso(item.get("date_created", ""))
        yield item


@dlt.resource(name="notifications", write_disposition="append", primary_key="sid")
def notifications(
    account_sid: str,
    username: str,
    password: str,
    last_date=dlt.sources.incremental("_cursor", initial_value="2020-01-01"),
    base_url: str = DEFAULT_BASE_URL,
):
    """Log notifications."""
    client = _make_client(username, password)
    params = {"MessageDate>": last_date.last_value}
    url = f"{base_url}/Accounts/{account_sid}/Notifications.json"
    for item in _get_paginated(client, url, "notifications", params=params):
        item["_cursor"] = _rfc2822_to_iso(item.get("message_date", ""))
        yield item


@dlt.resource(name="available_phone_numbers", write_disposition="replace")
def available_phone_numbers(
    account_sid: str,
    username: str,
    password: str,
    country_code: str = "US",
    base_url: str = DEFAULT_BASE_URL,
):
    """Available phone numbers for purchase."""
    client = _make_client(username, password)
    url = f"{base_url}/Accounts/{account_sid}/AvailablePhoneNumbers/{country_code}/Local.json"
    response = client.get(url)
    response.raise_for_status()
    yield from response.json().get("available_phone_numbers", [])

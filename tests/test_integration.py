"""Integration tests that hit real APIs.

These tests require environment variables to be set:
- ASC_KEY_ID, ASC_ISSUER_ID, ASC_PRIVATE_KEY_PATH
- TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN
- NEXTDNS_API_KEY

Run with: uv run pytest tests/test_integration.py -v
Skip if env vars are not set.
"""

import os

import pytest  # noqa: I001

# --- App Store Connect ---

ASC_KEY_ID = os.environ.get("ASC_KEY_ID")
ASC_ISSUER_ID = os.environ.get("ASC_ISSUER_ID")
ASC_PRIVATE_KEY_PATH = os.environ.get("ASC_PRIVATE_KEY_PATH")

asc_available = ASC_KEY_ID and ASC_ISSUER_ID and ASC_PRIVATE_KEY_PATH


@pytest.mark.skipif(not asc_available, reason="ASC credentials not set")
def test_asc_apps():
    from dlt_community_sources.app_store_connect.client import AppStoreConnectClient

    private_key = open(ASC_PRIVATE_KEY_PATH).read()
    client = AppStoreConnectClient(ASC_KEY_ID, ASC_ISSUER_ID, private_key)
    apps = list(client.get_paginated("apps"))
    assert len(apps) > 0
    assert "id" in apps[0]


@pytest.mark.skipif(not asc_available, reason="ASC credentials not set")
def test_asc_users():
    from dlt_community_sources.app_store_connect.client import AppStoreConnectClient

    private_key = open(ASC_PRIVATE_KEY_PATH).read()
    client = AppStoreConnectClient(ASC_KEY_ID, ASC_ISSUER_ID, private_key)
    users = list(client.get_paginated("users"))
    assert len(users) > 0


# --- Twilio ---

TWILIO_ACCOUNT_SID = os.environ.get("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN")

twilio_available = TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN


@pytest.mark.skipif(not twilio_available, reason="Twilio credentials not set")
def test_twilio_account():
    from dlt_community_sources.twilio.client import BASE_URL, TwilioClient

    client = TwilioClient(TWILIO_ACCOUNT_SID, auth_token=TWILIO_AUTH_TOKEN)
    resp = client._request("GET", f"{BASE_URL}/Accounts/{TWILIO_ACCOUNT_SID}.json")
    data = resp.json()
    assert data["sid"] == TWILIO_ACCOUNT_SID


@pytest.mark.skipif(not twilio_available, reason="Twilio credentials not set")
def test_twilio_usage_records():
    from dlt_community_sources.twilio.client import TwilioClient

    client = TwilioClient(TWILIO_ACCOUNT_SID, auth_token=TWILIO_AUTH_TOKEN)
    records = list(
        client.get_paginated("Usage/Records/Daily", "usage_records", page_size=10)
    )
    assert len(records) > 0


# --- NextDNS ---

NEXTDNS_API_KEY = os.environ.get("NEXTDNS_API_KEY")

nextdns_available = bool(NEXTDNS_API_KEY)


@pytest.mark.skipif(not nextdns_available, reason="NextDNS credentials not set")
def test_nextdns_profiles():
    from dlt_community_sources.nextdns.client import NextDNSClient

    client = NextDNSClient(NEXTDNS_API_KEY)
    profiles = list(client.get_paginated("profiles"))
    assert len(profiles) > 0
    assert "id" in profiles[0]


@pytest.mark.skipif(not nextdns_available, reason="NextDNS credentials not set")
def test_nextdns_analytics_status():
    from dlt_community_sources.nextdns.client import NextDNSClient

    client = NextDNSClient(NEXTDNS_API_KEY)
    profiles = list(client.get_paginated("profiles"))
    pid = profiles[0]["id"]
    # Should not raise
    list(client.get_paginated(f"profiles/{pid}/analytics/status"))

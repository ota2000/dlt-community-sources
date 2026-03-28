"""Integration tests that hit real APIs.

These tests require environment variables to be set:
- ASC_KEY_ID, ASC_ISSUER_ID, ASC_PRIVATE_KEY_PATH
- TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN
- NEXTDNS_API_KEY

Run with: uv run pytest tests/test_integration.py -v
Skip if env vars are not set.
"""

import os

import dlt
import pytest  # noqa: I001

# --- App Store Connect ---

ASC_KEY_ID = os.environ.get("ASC_KEY_ID")
ASC_ISSUER_ID = os.environ.get("ASC_ISSUER_ID")
ASC_PRIVATE_KEY_PATH = os.environ.get("ASC_PRIVATE_KEY_PATH")

asc_available = ASC_KEY_ID and ASC_ISSUER_ID and ASC_PRIVATE_KEY_PATH


@pytest.mark.skipif(not asc_available, reason="ASC credentials not set")
def test_asc_apps():
    from dlt_community_sources.app_store_connect import app_store_connect_source

    private_key = open(ASC_PRIVATE_KEY_PATH).read()
    source = app_store_connect_source(
        key_id=ASC_KEY_ID,
        issuer_id=ASC_ISSUER_ID,
        private_key=private_key,
        resources=["apps"],
    )
    pipeline = dlt.pipeline(
        pipeline_name="test_asc_apps",
        destination="duckdb",
        dataset_name="test_asc",
    )
    load_info = pipeline.run(source)
    assert load_info.loads_ids


@pytest.mark.skipif(not asc_available, reason="ASC credentials not set")
def test_asc_users():
    from dlt_community_sources.app_store_connect import app_store_connect_source

    private_key = open(ASC_PRIVATE_KEY_PATH).read()
    source = app_store_connect_source(
        key_id=ASC_KEY_ID,
        issuer_id=ASC_ISSUER_ID,
        private_key=private_key,
        resources=["users"],
    )
    pipeline = dlt.pipeline(
        pipeline_name="test_asc_users",
        destination="duckdb",
        dataset_name="test_asc",
    )
    load_info = pipeline.run(source)
    assert load_info.loads_ids


# --- Twilio ---

TWILIO_ACCOUNT_SID = os.environ.get("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN")
TWILIO_API_KEY_SID = os.environ.get("TWILIO_API_KEY_SID")
TWILIO_API_KEY_SECRET = os.environ.get("TWILIO_API_KEY_SECRET")

twilio_available = TWILIO_ACCOUNT_SID and (
    TWILIO_AUTH_TOKEN or (TWILIO_API_KEY_SID and TWILIO_API_KEY_SECRET)
)


def _twilio_auth_kwargs():
    if TWILIO_API_KEY_SID and TWILIO_API_KEY_SECRET:
        return {
            "api_key_sid": TWILIO_API_KEY_SID,
            "api_key_secret": TWILIO_API_KEY_SECRET,
        }
    return {"auth_token": TWILIO_AUTH_TOKEN}


@pytest.mark.skipif(not twilio_available, reason="Twilio credentials not set")
def test_twilio_queues():
    from dlt_community_sources.twilio import twilio_source

    source = twilio_source(
        account_sid=TWILIO_ACCOUNT_SID,
        resources=["queues"],
        **_twilio_auth_kwargs(),
    )
    pipeline = dlt.pipeline(
        pipeline_name="test_twilio_queues",
        destination="duckdb",
        dataset_name="test_twilio",
    )
    load_info = pipeline.run(source)
    assert load_info.loads_ids


@pytest.mark.skipif(not twilio_available, reason="Twilio credentials not set")
def test_twilio_usage_records():
    from dlt_community_sources.twilio import twilio_source

    source = twilio_source(
        account_sid=TWILIO_ACCOUNT_SID,
        resources=["usage_records"],
        **_twilio_auth_kwargs(),
    )
    pipeline = dlt.pipeline(
        pipeline_name="test_twilio_usage",
        destination="duckdb",
        dataset_name="test_twilio",
    )
    load_info = pipeline.run(source)
    assert load_info.loads_ids


# --- NextDNS ---

NEXTDNS_API_KEY = os.environ.get("NEXTDNS_API_KEY")

nextdns_available = bool(NEXTDNS_API_KEY)


@pytest.mark.skipif(not nextdns_available, reason="NextDNS credentials not set")
def test_nextdns_profiles():
    from dlt_community_sources.nextdns import nextdns_source

    source = nextdns_source(
        api_key=NEXTDNS_API_KEY,
        resources=["profiles"],
    )
    pipeline = dlt.pipeline(
        pipeline_name="test_nextdns_profiles",
        destination="duckdb",
        dataset_name="test_nextdns",
    )
    load_info = pipeline.run(source)
    assert load_info.loads_ids


@pytest.mark.skipif(not nextdns_available, reason="NextDNS credentials not set")
def test_nextdns_analytics_status():
    from dlt_community_sources.nextdns import nextdns_source

    source = nextdns_source(
        api_key=NEXTDNS_API_KEY,
        resources=["profiles", "analytics_status"],
    )
    pipeline = dlt.pipeline(
        pipeline_name="test_nextdns_analytics",
        destination="duckdb",
        dataset_name="test_nextdns",
    )
    load_info = pipeline.run(source)
    assert load_info.loads_ids

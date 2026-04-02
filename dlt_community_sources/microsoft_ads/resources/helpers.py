"""Shared helpers for Microsoft Ads resources."""

import logging
from collections.abc import Generator
from decimal import Decimal

from dlt.sources.helpers import requests as req

logger = logging.getLogger(__name__)

# Base URLs for Microsoft Advertising API v13
CAMPAIGN_MGMT_URL = "https://campaign.api.bingads.microsoft.com/CampaignManagement/v13"
REPORTING_URL = "https://reporting.api.bingads.microsoft.com/Reporting/v13"
CUSTOMER_MGMT_URL = (
    "https://clientcenter.api.bingads.microsoft.com/CustomerManagement/v13"
)
AD_INSIGHT_URL = "https://adinsight.api.bingads.microsoft.com/AdInsight/v13"
CUSTOMER_BILLING_URL = (
    "https://clientcenter.api.bingads.microsoft.com/CustomerBilling/v13"
)

# Polling config
POLL_INTERVAL_SECONDS = 10
POLL_MAX_WAIT_SECONDS = 600

# CSV type conversion
REPORT_INT_FIELDS = {"Impressions", "Clicks", "Conversions"}
REPORT_FLOAT_FIELDS = {
    "Ctr",
    "AverageCpc",
    "Spend",
    "ConversionRate",
    "Revenue",
    "ReturnOnAdSpend",
    "QualityScore",
    "CurrentMaxCpc",
}


def build_headers(
    access_token: str,
    developer_token: str,
    customer_id: str,
    account_id: str,
) -> dict:
    """Build headers for all Microsoft Advertising API requests."""
    return {
        "Authorization": f"Bearer {access_token}",
        "DeveloperToken": developer_token,
        "CustomerId": customer_id,
        "CustomerAccountId": account_id,
        "Content-Type": "application/json",
    }


def make_client(
    access_token: str,
    developer_token: str,
    customer_id: str,
    account_id: str,
) -> req.Client:
    """Create a dlt HTTP client with Microsoft Advertising auth headers."""
    client = req.Client()
    client.session.headers.update(
        build_headers(access_token, developer_token, customer_id, account_id)
    )
    return client


def post_rpc(client: req.Client, url: str, body: dict) -> dict:
    """Make a POST RPC call and return response JSON."""
    response = client.post(url, json=body)
    response.raise_for_status()
    return response.json()


def safe_rpc(client: req.Client, url: str, body: dict, key: str) -> list:
    """POST RPC with 403/404 graceful skip."""
    try:
        data = post_rpc(client, url, body)
        return data.get(key, [])
    except req.HTTPError as e:
        if e.response is not None and e.response.status_code in (403, 404):
            logger.warning("Skipping %s: %d", url, e.response.status_code)
            return []
        raise


def get_entities_paginated(
    client: req.Client,
    url: str,
    body: dict,
    entities_key: str,
    page_size: int = 1000,
) -> Generator[dict, None, None]:
    """Fetch entities with PageInfo-based pagination."""
    page_index = 0
    while True:
        body["PageInfo"] = {"Index": page_index, "Size": page_size}
        data = post_rpc(client, url, body)
        entities = data.get(entities_key, [])
        if not entities:
            break
        yield from entities
        if len(entities) < page_size:
            break
        page_index += 1


def convert_report_types(row: dict) -> dict:
    """Convert report CSV string values to numeric types in-place."""
    for field in REPORT_INT_FIELDS:
        if field in row and row[field] is not None:
            try:
                row[field] = int(row[field])
            except (ValueError, TypeError):
                pass
    for field in REPORT_FLOAT_FIELDS:
        if field in row and row[field] is not None:
            try:
                row[field] = Decimal(row[field])
            except (ValueError, TypeError):
                pass
    return row

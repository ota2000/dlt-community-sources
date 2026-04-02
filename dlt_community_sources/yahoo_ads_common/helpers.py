"""Yahoo Ads common helpers for POST RPC API calls.

Both Search Ads and Display Ads share the same request/response pattern:
- POST with JSON body
- Header: x-z-base-account-id
- Response: { "rval": { "totalNumEntries": N, "values": [...] } }
- Pagination: startIndex (1-based) + numberResults
"""

import csv
import io
import logging
import time
from collections.abc import Generator
from decimal import Decimal
from typing import Optional

from dlt.sources.helpers import requests as req

# Numeric fields for type conversion (shared across SS/YDA)
REPORT_INT_FIELDS = {"IMPS", "CLICKS", "CONVERSIONS"}
REPORT_FLOAT_FIELDS = {"CLICK_RATE", "AVG_CPC", "COST", "CONV_RATE", "CONV_VALUE"}
REPORT_METRIC_FIELDS = REPORT_INT_FIELDS | REPORT_FLOAT_FIELDS


def derive_primary_key(fields: list[str]) -> list[str]:
    """Derive primary key from report fields (all non-metric fields)."""
    return [f for f in fields if f not in REPORT_METRIC_FIELDS]


def convert_report_types(row: dict) -> dict:
    """Convert report string values to appropriate numeric types."""
    result = {}
    for k, v in row.items():
        if v == "--" or v == "":
            result[k] = None
        elif k in REPORT_INT_FIELDS:
            try:
                result[k] = int(v.replace(",", ""))
            except (ValueError, AttributeError):
                result[k] = v
        elif k in REPORT_FLOAT_FIELDS:
            try:
                result[k] = Decimal(v.replace(",", ""))
            except (ValueError, AttributeError):
                result[k] = v
        else:
            result[k] = v
    return result


logger = logging.getLogger(__name__)

# Pagination defaults
DEFAULT_PAGE_SIZE = 500
MAX_PAGE_SIZE = 10000

# Report polling
POLL_INTERVAL_SECONDS = 10
POLL_MAX_WAIT_SECONDS = 600


def make_client(
    access_token: str,
    account_id: str,
) -> req.Client:
    """Create an HTTP client with Yahoo Ads auth headers."""
    client = req.Client()
    client.session.headers.update(
        {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "x-z-base-account-id": str(account_id),
        }
    )
    return client


def post_rpc(
    client: req.Client,
    url: str,
    body: Optional[dict] = None,
) -> dict:
    """Execute a POST RPC call and return the response JSON."""
    response = client.post(url, json=body or {})
    response.raise_for_status()
    return response.json()


def get_entities(
    client: req.Client,
    url: str,
    account_id: str,
    selector_fields: Optional[dict] = None,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> Generator[dict, None, None]:
    """Fetch all entities with pagination.

    Handles the standard Yahoo Ads pagination pattern:
    - Request body contains accountId and paging (startIndex, numberResults)
    - Response contains rval.totalNumEntries and rval.values[]
    - Each value has an operationSucceeded flag
    """
    page_size = min(page_size, MAX_PAGE_SIZE)
    start_index = 1
    while True:
        body = {
            "accountId": int(account_id),
            "paging": {
                "startIndex": start_index,
                "numberResults": page_size,
            },
        }
        if selector_fields:
            body.update(selector_fields)

        data = post_rpc(client, url, body)
        rval = data.get("rval", {})
        total = rval.get("totalNumEntries", 0)
        values = rval.get("values", [])

        for entry in values:
            if entry.get("operationSucceeded", True):
                # Remove the wrapper and yield the inner object
                inner = {
                    k: v
                    for k, v in entry.items()
                    if k not in ("operationSucceeded", "errors")
                }
                # If there's a single inner key, yield its value directly
                keys = list(inner.keys())
                if len(keys) == 1:
                    yield inner[keys[0]]
                elif inner:
                    yield inner

        start_index += len(values)
        if start_index > total or not values:
            break


def safe_get_entities(
    client: req.Client,
    url: str,
    account_id: str,
    selector_fields: Optional[dict] = None,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> Generator[dict, None, None]:
    """Like get_entities but silently handles 403/404 errors."""
    try:
        yield from get_entities(client, url, account_id, selector_fields, page_size)
    except req.HTTPError as e:
        if e.response is not None and e.response.status_code in (403, 404):
            logger.warning("Skipping %s: HTTP %s", url, e.response.status_code)
        else:
            raise


def submit_report(
    client: req.Client,
    base_url: str,
    account_id: str,
    report_type: str,
    fields: list[str],
    start_date: str,
    end_date: str,
    report_download_format: str = "CSV",
) -> Optional[int]:
    """Submit a report definition. Returns reportJobId or None."""
    body = {
        "accountId": int(account_id),
        "operand": [
            {
                "accountId": int(account_id),
                "fields": fields,
                "reportDateRangeType": "CUSTOM_DATE",
                "dateRange": {
                    "startDate": start_date.replace("-", ""),
                    "endDate": end_date.replace("-", ""),
                },
                "reportType": report_type,
                "reportDownloadFormat": report_download_format,
                "reportDownloadEncode": "UTF8",
            }
        ],
    }
    data = post_rpc(client, f"{base_url}/ReportDefinitionService/add", body)
    values = data.get("rval", {}).get("values", [])
    if not values:
        return None
    report_def = values[0].get("reportDefinition", {})
    return report_def.get("reportJobId")


def poll_report(
    client: req.Client,
    base_url: str,
    account_id: str,
    report_job_id: int,
) -> Optional[str]:
    """Poll report until completion. Returns 'COMPLETED' status or None."""
    elapsed = 0
    while elapsed < POLL_MAX_WAIT_SECONDS:
        body = {
            "accountId": int(account_id),
            "reportJobIds": [report_job_id],
        }
        data = post_rpc(client, f"{base_url}/ReportDefinitionService/get", body)
        values = data.get("rval", {}).get("values", [])
        if not values:
            return None
        report_def = values[0].get("reportDefinition", {})
        status = report_def.get("reportJobStatus")
        logger.info("Report %s: status=%s", report_job_id, status)
        if status == "COMPLETED":
            return status
        if status in ("FAILED", "UNKNOWN"):
            logger.warning("Report %s: %s", report_job_id, status)
            return None
        time.sleep(POLL_INTERVAL_SECONDS)
        elapsed += POLL_INTERVAL_SECONDS
    logger.warning("Report %s timed out", report_job_id)
    return None


def download_report(
    client: req.Client,
    base_url: str,
    account_id: str,
    report_job_id: int,
) -> Generator[dict, None, None]:
    """Download a completed report and yield rows as dicts."""
    body = {
        "accountId": int(account_id),
        "reportJobId": report_job_id,
    }
    response = client.post(
        f"{base_url}/ReportDefinitionService/download",
        json=body,
    )
    response.raise_for_status()
    text = response.text
    reader = csv.DictReader(io.StringIO(text))
    for row in reader:
        yield dict(row)

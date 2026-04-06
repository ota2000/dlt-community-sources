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

logger = logging.getLogger(__name__)

# Primary key: only core identity fields (date + entity IDs)
_PK_FIELDS = {
    "DAY",
    "ACCOUNT_ID",
    "CAMPAIGN_ID",
    "ADGROUP_ID",
    "AD_ID",
    "KEYWORD_ID",
    "FEED_ITEM_ID",
    "AD_DISPLAY_OPTION",
    "MEDIA_ID",
}


def _extract_inner(entry: dict) -> Optional[dict]:
    """Extract inner object from API response entry, skipping wrapper fields."""
    if not entry.get("operationSucceeded", True):
        return None
    inner = {
        k: v for k, v in entry.items() if k not in ("operationSucceeded", "errors")
    }
    keys = list(inner.keys())
    if len(keys) == 1:
        return inner[keys[0]]
    return inner


class ReportFieldMeta:
    """Metadata from getReportFields API."""

    def __init__(
        self,
        field_names: list[str],
        field_type_map: dict[str, str],
        display_to_field: dict[str, str],
    ):
        self.field_names = field_names
        self.field_type_map = field_type_map
        self.display_to_field = display_to_field


def get_report_fields_with_types(
    client: req.Client,
    base_url: str,
    report_type: str,
    report_language: str = "EN",
    compatible_only: bool = True,
) -> ReportFieldMeta:
    """Fetch available report fields and their types from the API.

    When compatible_only=True (default), excludes fields that conflict with
    other fields using the impossibleCombinationFields metadata. This builds
    the largest conflict-free field set by iteratively removing fields with
    the most conflicts.

    Returns:
        ReportFieldMeta with field_names, field_type_map, and display_to_field mapping.
    """
    body = {"reportType": report_type}
    data = post_rpc(client, f"{base_url}/ReportDefinitionService/getReportFields", body)
    raw_fields = data.get("rval", {}).get("fields", [])
    type_map = {f["fieldName"]: f.get("fieldType", "STRING") for f in raw_fields}

    # Build display name → field name mapping
    display_key = (
        "displayFieldNameEn" if report_language == "EN" else "displayFieldNameJa"
    )
    display_to_field = {
        f.get(display_key, f["fieldName"]): f["fieldName"] for f in raw_fields
    }

    if not compatible_only:
        return ReportFieldMeta(
            [f["fieldName"] for f in raw_fields], type_map, display_to_field
        )

    # Build conflict graph and greedily remove highest-conflict fields
    conflict_map: dict[str, set[str]] = {
        f["fieldName"]: set(f.get("impossibleCombinationFields") or [])
        for f in raw_fields
    }
    candidates = set(conflict_map.keys())

    while True:
        # Find conflicts within current candidates
        conflicts: dict[str, int] = {}
        for name in candidates:
            cnt = len(conflict_map.get(name, set()) & candidates)
            if cnt > 0:
                conflicts[name] = cnt
        if not conflicts:
            break
        # Remove the field with the most conflicts
        worst = max(conflicts, key=conflicts.get)  # type: ignore[arg-type]
        candidates.discard(worst)

    # Preserve original field order
    names = [f["fieldName"] for f in raw_fields if f["fieldName"] in candidates]
    return ReportFieldMeta(names, type_map, display_to_field)


def get_report_fields(
    client: req.Client,
    base_url: str,
    report_type: str,
) -> list[str]:
    """Fetch compatible report field names from the API dynamically."""
    meta = get_report_fields_with_types(client, base_url, report_type)
    return meta.field_names


def derive_primary_key(fields: list[str]) -> list[str]:
    """Derive primary key from report fields using a whitelist of identity fields.

    Returns only fields present in _PK_FIELDS (date + entity IDs),
    excluding metric and dimension fields.
    """
    return [f for f in fields if f in _PK_FIELDS]


def convert_report_types(
    row: dict, field_type_map: Optional[dict[str, str]] = None
) -> dict:
    """Convert report string values to appropriate numeric types.

    Uses field_type_map from getReportFields API for dynamic type conversion.
    If field_type_map is not provided, values are returned as-is (strings).
    """
    result = {}
    for k, v in row.items():
        if v == "--" or v == "":
            result[k] = None
        elif field_type_map and field_type_map.get(k) == "LONG":
            try:
                result[k] = int(v.replace(",", ""))
            except (ValueError, AttributeError):
                result[k] = v
        elif field_type_map and field_type_map.get(k) in ("DOUBLE", "BID"):
            try:
                result[k] = Decimal(v.replace(",", ""))
            except (ValueError, AttributeError):
                result[k] = v
        else:
            result[k] = v
    return result


# Pagination defaults
DEFAULT_PAGE_SIZE = 500
MAX_PAGE_SIZE = 10000

# Report polling
POLL_INTERVAL_SECONDS = 5
POLL_MAX_WAIT_SECONDS = 600


def make_client(access_token: str, base_account_id: str) -> req.Client:
    """Create an HTTP client with Yahoo Ads auth headers."""
    client = req.Client()
    client.session.headers.update(
        {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "x-z-base-account-id": str(base_account_id),
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


def discover_accounts(
    client: req.Client,
    base_url: str,
) -> list[str]:
    """Discover all SERVING child accounts under the MCC.

    Uses AccountService/get to list accounts under the base account.
    Returns list of account IDs with SERVING status.
    """
    account_ids: list[str] = []
    start_index = 1
    page_size = 500

    while True:
        body = {
            "startIndex": start_index,
            "numberResults": page_size,
        }
        data = post_rpc(client, f"{base_url}/AccountService/get", body)
        rval = data.get("rval", {})
        total = rval.get("totalNumEntries", 0)
        values = rval.get("values", [])

        for entry in values:
            if not entry.get("operationSucceeded"):
                continue
            account = entry.get("account", {})
            if account.get("accountStatus") == "SERVING":
                account_ids.append(str(account["accountId"]))

        start_index += len(values)
        if start_index > total or not values:
            break

    logger.info("Discovered %d SERVING accounts under MCC", len(account_ids))
    return account_ids


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
            "startIndex": start_index,
            "numberResults": page_size,
        }
        if selector_fields:
            body.update(selector_fields)

        data = post_rpc(client, url, body)
        rval = data.get("rval") or {}
        total = rval.get("totalNumEntries", 0)
        values = rval.get("values") or []

        for entry in values:
            obj = _extract_inner(entry)
            if obj is not None:
                yield obj

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
    """Fetch entities with HTTP error handling (skip 400/403/404)."""
    try:
        yield from get_entities(client, url, account_id, selector_fields, page_size)
    except req.HTTPError as e:
        if e.response is not None and e.response.status_code in (400, 403, 404):
            logger.warning("Skipping %s: HTTP %s", url, e.response.status_code)
        else:
            raise


def get_entities_by_account_ids(
    client: req.Client,
    url: str,
    account_id: str,
) -> Generator[dict, None, None]:
    """Fetch entities using accountIds array (no pagination)."""
    body = {"accountIds": [int(account_id)]}
    data = post_rpc(client, url, body)
    rval = data.get("rval") or {}
    values = rval.get("values") or []
    for entry in values:
        obj = _extract_inner(entry)
        if obj is not None:
            yield obj


def get_entities_no_paging(
    client: req.Client,
    url: str,
    account_id: str,
) -> Generator[dict, None, None]:
    """Fetch entities with accountId only (no pagination params)."""
    body = {"accountId": int(account_id)}
    data = post_rpc(client, url, body)
    rval = data.get("rval") or {}
    values = rval.get("values") or []
    for entry in values:
        obj = _extract_inner(entry)
        if obj is not None:
            yield obj


def safe_fetch_entities(
    client: req.Client,
    url: str,
    account_id: str,
    body_style: str = "standard",
    page_size: int = DEFAULT_PAGE_SIZE,
) -> Generator[dict, None, None]:
    """Fetch entities with appropriate body style and error handling."""
    try:
        if body_style == "account_ids":
            yield from get_entities_by_account_ids(client, url, account_id)
        elif body_style == "no_paging":
            yield from get_entities_no_paging(client, url, account_id)
        elif body_style == "empty":
            # Some services (e.g. AccountLinkService) accept empty body
            data = post_rpc(client, url, {})
            rval = data.get("rval") or {}
            for entry in rval.get("values") or []:
                obj = _extract_inner(entry)
                if obj is not None:
                    yield obj
        else:
            yield from get_entities(client, url, account_id, page_size=page_size)
    except req.HTTPError as e:
        if e.response is not None and e.response.status_code in (400, 403, 404):
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
    report_language: str = "EN",
) -> Optional[int]:
    """Submit a report definition. Returns reportJobId or None."""
    body = {
        "accountId": int(account_id),
        "operand": [
            {
                "accountId": int(account_id),
                "reportName": f"dlt_{report_type.lower()}_{account_id}",
                "fields": fields,
                "reportDateRangeType": "CUSTOM_DATE",
                "dateRange": {
                    "startDate": start_date.replace("-", ""),
                    "endDate": end_date.replace("-", ""),
                },
                "reportType": report_type,
                "reportDownloadFormat": report_download_format,
                "reportDownloadEncode": "UTF8",
                "reportLanguage": report_language,
                "reportSkipReportSummary": "TRUE",
                "reportSkipColumnHeader": "FALSE",
            }
        ],
    }
    data = post_rpc(client, f"{base_url}/ReportDefinitionService/add", body)
    values = data.get("rval", {}).get("values", [])
    if not values:
        return None
    entry = values[0]
    if not entry.get("operationSucceeded"):
        errors = entry.get("errors", [])
        logger.warning("Report submit failed: %s", errors)
        return None
    report_def = entry.get("reportDefinition")
    if report_def is None:
        return None
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
    display_to_field: Optional[dict[str, str]] = None,
) -> Generator[dict, None, None]:
    """Download a completed report and yield rows as dicts.

    If display_to_field is provided, CSV column names (display names) are
    mapped back to API field names for consistent schema.
    """
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
        if display_to_field:
            yield {display_to_field.get(k, k): v for k, v in row.items()}
        else:
            yield dict(row)

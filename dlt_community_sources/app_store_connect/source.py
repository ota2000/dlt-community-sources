"""dlt source for Apple App Store Connect API."""

import csv
import gzip
import io
import logging
from collections.abc import Generator
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from typing import Optional, Sequence

import dlt
from dlt.sources import DltResource
from dlt.sources.helpers import requests as req
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig

from .auth import AppStoreConnectAuth

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://api.appstoreconnect.apple.com/v1"


def _rest_api_config(auth: AppStoreConnectAuth, base_url: str) -> RESTAPIConfig:
    """Build the REST API config for standard App Store Connect endpoints."""
    return {
        "client": {
            "base_url": f"{base_url}/",
            "auth": auth,
            "paginator": {
                "type": "json_link",
                "next_url_path": "links.next",
            },
        },
        "resource_defaults": {
            "primary_key": "id",
            "write_disposition": "merge",
            "endpoint": {
                "data_selector": "data",
                "response_actions": [
                    {"status_code": 403, "action": "ignore"},
                    {"status_code": 404, "action": "ignore"},
                ],
            },
        },
        "resources": [
            {"name": "apps", "endpoint": {"path": "apps"}},
            {
                "name": "app_store_versions",
                "endpoint": {
                    "path": "apps/{resources.apps.id}/appStoreVersions",
                },
            },
            {"name": "builds", "endpoint": {"path": "builds"}},
            {"name": "beta_testers", "endpoint": {"path": "betaTesters"}},
            {"name": "beta_groups", "endpoint": {"path": "betaGroups"}},
            {"name": "bundle_ids", "endpoint": {"path": "bundleIds"}},
            {"name": "certificates", "endpoint": {"path": "certificates"}},
            {"name": "devices", "endpoint": {"path": "devices"}},
            {
                "name": "in_app_purchases",
                "endpoint": {
                    "path": "apps/{resources.apps.id}/inAppPurchasesV2",
                },
            },
            {
                "name": "subscription_groups",
                "endpoint": {
                    "path": "apps/{resources.apps.id}/subscriptionGroups",
                },
            },
            {
                "name": "subscriptions",
                "endpoint": {
                    "path": "subscriptionGroups/{resources.subscription_groups.id}/subscriptions",
                },
            },
            {"name": "users", "endpoint": {"path": "users"}},
            {"name": "user_invitations", "endpoint": {"path": "userInvitations"}},
            {
                "name": "app_categories",
                "write_disposition": "replace",
                "endpoint": {"path": "appCategories"},
            },
            {
                "name": "territories",
                "write_disposition": "replace",
                "endpoint": {"path": "territories"},
            },
            {
                "name": "pre_release_versions",
                "endpoint": {"path": "preReleaseVersions"},
            },
            {
                "name": "beta_app_review_submissions",
                "endpoint": {"path": "betaAppReviewSubmissions"},
            },
            {
                "name": "beta_build_localizations",
                "endpoint": {"path": "betaBuildLocalizations"},
            },
            {
                "name": "beta_app_localizations",
                "endpoint": {"path": "betaAppLocalizations"},
            },
            {
                "name": "beta_license_agreements",
                "endpoint": {"path": "betaLicenseAgreements"},
            },
            {
                "name": "build_beta_details",
                "endpoint": {"path": "buildBetaDetails"},
            },
            {
                "name": "app_encryption_declarations",
                "endpoint": {"path": "appEncryptionDeclarations"},
            },
            {
                "name": "provisioning_profiles",
                "endpoint": {"path": "profiles"},
            },
            {
                "name": "review_submissions",
                "endpoint": {"path": "reviewSubmissions"},
            },
        ],
    }


@dlt.source(name="app_store_connect")
def app_store_connect_source(
    key_id: str = dlt.secrets.value,
    issuer_id: str = dlt.secrets.value,
    private_key: str = dlt.secrets.value,
    vendor_number: Optional[str] = None,
    resources: Optional[Sequence[str]] = None,
    base_url: Optional[str] = None,
) -> list[DltResource]:
    """A dlt source for Apple App Store Connect API.

    Args:
        key_id: API key ID from App Store Connect.
        issuer_id: Issuer ID from App Store Connect.
        private_key: Contents of the .p8 private key file.
        vendor_number: Vendor number for sales/finance reports.
        resources: List of resource names to load. None for all.
        base_url: Override the API base URL. Useful for testing.

    Returns:
        List of dlt resources.
    """
    url = base_url or DEFAULT_BASE_URL
    auth = AppStoreConnectAuth(
        key_id=key_id, issuer_id=issuer_id, private_key=private_key
    )

    # REST API resources (declarative)
    config = _rest_api_config(auth, url)
    rest_resources = rest_api_resources(config)

    # Report resources (custom, can't be done via rest_api)
    report_resources = [
        sales_reports(auth, vendor_number=vendor_number or "", base_url=url),
        finance_reports(auth, vendor_number=vendor_number or "", base_url=url),
        analytics_reports(auth, base_url=url),
    ]

    all_resources: list[DltResource] = rest_resources + report_resources

    if resources:
        return [r for r in all_resources if r.name in resources]
    return all_resources


# --- Report helpers ---


def _make_client(auth: AppStoreConnectAuth) -> req.Client:
    """Create a dlt HTTP client with per-request JWT auth and automatic retry."""
    client = req.Client()
    client.session.auth = auth
    return client


def _download_tsv(
    client: req.Client, url: str, params: Optional[dict] = None
) -> list[dict]:
    """Download a TSV report and parse it into a list of dicts."""
    try:
        response = client.get(url, params=params)
        response.raise_for_status()
    except req.HTTPError as e:
        if e.response is not None and e.response.status_code in (403, 404):
            logger.warning(
                "Report not available (%d) for %s. Skipping.",
                e.response.status_code,
                url,
            )
            return []
        raise
    content = response.content
    try:
        content = gzip.decompress(content)
    except gzip.BadGzipFile:
        pass
    text = content.decode("utf-8")
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    return list(reader)


def _download_gzip_tsv(client: req.Client, url: str) -> list[dict]:
    """Download a gzip-compressed TSV and parse it."""
    try:
        response = client.get(url)
        response.raise_for_status()
        text = gzip.decompress(response.content).decode("utf-8")
    except (req.HTTPError, gzip.BadGzipFile, UnicodeDecodeError) as e:
        logger.warning("Failed to download/decompress TSV from %s: %s", url, e)
        return []
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    return list(reader)


SALES_REPORT_DECIMAL_FIELDS = {"Units", "Customer Price", "Developer Proceeds"}
FINANCE_REPORT_DECIMAL_FIELDS = {
    "Quantity",
    "Partner Share",
    "Extended Partner Share",
    "Customer Price",
}


def _convert_decimal_fields(row: dict, fields: set[str]) -> dict:
    """Convert specified fields from string to Decimal in-place."""
    for field in fields:
        if field in row and row[field]:
            try:
                row[field] = Decimal(row[field])
            except InvalidOperation:
                pass
    return row


def _date_range(start: str, end: str) -> Generator[str, None, None]:
    """Generate dates from start to end (inclusive), YYYY-MM-DD format."""
    current = date.fromisoformat(start)
    end_date = date.fromisoformat(end)
    while current <= end_date:
        yield current.isoformat()
        current += timedelta(days=1)


def _month_range(start: str, end: str) -> Generator[str, None, None]:
    """Generate months from start to end (inclusive), YYYY-MM format."""
    current = date.fromisoformat(start + "-01")
    end_date = date.fromisoformat(end + "-01")
    while current <= end_date:
        yield current.strftime("%Y-%m")
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)


# --- Sales & Finance Reports (with incremental loading) ---


@dlt.resource(name="sales_reports", write_disposition="append")
def sales_reports(
    auth: AppStoreConnectAuth,
    vendor_number: str = "",
    report_type: str = "SALES",
    report_sub_type: str = "SUMMARY",
    frequency: str = "DAILY",
    version: str = "1_0",
    last_date=dlt.sources.incremental("_report_date", initial_value="2020-01-01"),
    base_url: str = DEFAULT_BASE_URL,
):
    """Download Sales and Trends reports with incremental loading."""
    if not vendor_number:
        return

    client = _make_client(auth)
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    start = last_date.last_value
    start_date = date.fromisoformat(start) + timedelta(days=1)
    start = start_date.isoformat()

    if start > yesterday:
        logger.info("sales_reports: already up to date (last=%s)", start)
        return

    for report_date in _date_range(start, yesterday):
        params = {
            "filter[vendorNumber]": vendor_number,
            "filter[reportType]": report_type,
            "filter[reportSubType]": report_sub_type,
            "filter[frequency]": frequency,
            "filter[reportDate]": report_date,
            "filter[version]": version,
        }
        rows = _download_tsv(client, f"{base_url}/salesReports", params=params)
        if not rows:
            continue
        for row in rows:
            _convert_decimal_fields(row, SALES_REPORT_DECIMAL_FIELDS)
            row["_report_date"] = report_date
            row["_report_type"] = report_type
            row["_frequency"] = frequency
        yield from rows


@dlt.resource(name="finance_reports", write_disposition="append")
def finance_reports(
    auth: AppStoreConnectAuth,
    vendor_number: str = "",
    region_code: str = "ZZ",
    report_type: str = "FINANCIAL",
    last_date=dlt.sources.incremental("_report_date", initial_value="2020-01"),
    base_url: str = DEFAULT_BASE_URL,
):
    """Download Finance reports with incremental loading."""
    if not vendor_number:
        return

    client = _make_client(auth)
    last_month = (date.today().replace(day=1) - timedelta(days=1)).strftime("%Y-%m")
    start = last_date.last_value
    start_d = date.fromisoformat(start + "-01")
    if start_d.month == 12:
        start_d = start_d.replace(year=start_d.year + 1, month=1)
    else:
        start_d = start_d.replace(month=start_d.month + 1)
    start = start_d.strftime("%Y-%m")

    if start > last_month:
        logger.info("finance_reports: already up to date (last=%s)", start)
        return

    for report_date in _month_range(start, last_month):
        params = {
            "filter[vendorNumber]": vendor_number,
            "filter[reportType]": report_type,
            "filter[regionCode]": region_code,
            "filter[reportDate]": report_date,
        }
        rows = _download_tsv(client, f"{base_url}/financeReports", params=params)
        if not rows:
            continue
        for row in rows:
            _convert_decimal_fields(row, FINANCE_REPORT_DECIMAL_FIELDS)
            row["_report_date"] = report_date
            row["_report_type"] = report_type
        yield from rows


# --- Analytics Reports ---


@dlt.resource(name="analytics_reports", write_disposition="append")
def analytics_reports(
    auth: AppStoreConnectAuth,
    last_processing_date=dlt.sources.incremental(
        "_processing_date", initial_value="2020-01-01"
    ),
    base_url: str = DEFAULT_BASE_URL,
):
    """Download Analytics reports with incremental loading."""
    client = _make_client(auth)

    def _get_paginated(path: str) -> Generator[dict, None, None]:
        url = f"{base_url}/{path}"
        while url:
            try:
                response = client.get(url)
                response.raise_for_status()
            except req.HTTPError as e:
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
            url = data.get("links", {}).get("next")

    for app in _get_paginated("apps"):
        app_id = app["id"]
        for request in _get_paginated(f"apps/{app_id}/analyticsReportRequests"):
            request_id = request["id"]
            for report in _get_paginated(
                f"analyticsReportRequests/{request_id}/reports"
            ):
                report_id = report["id"]
                report_name = report.get("attributes", {}).get("name", "unknown")
                category = report.get("attributes", {}).get("category", "unknown")

                for instance in _get_paginated(
                    f"analyticsReports/{report_id}/instances"
                ):
                    processing_date = instance.get("attributes", {}).get(
                        "processingDate", ""
                    )
                    granularity = instance.get("attributes", {}).get("granularity", "")
                    instance_id = instance["id"]

                    for segment in _get_paginated(
                        f"analyticsReportInstances/{instance_id}/segments"
                    ):
                        url = segment.get("attributes", {}).get("url")
                        if not url:
                            continue
                        rows = _download_gzip_tsv(client, url)
                        if not rows:
                            continue
                        for row in rows:
                            row["_report_name"] = report_name
                            row["_category"] = category
                            row["_processing_date"] = processing_date
                            row["_granularity"] = granularity
                        yield from rows

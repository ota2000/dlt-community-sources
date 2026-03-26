"""dlt source for Apple App Store Connect API."""

import logging
from datetime import date, timedelta
from typing import Optional, Sequence

import dlt
from dlt.sources import DltResource

from .client import BASE_URL, AppStoreConnectClient

logger = logging.getLogger(__name__)


@dlt.source(name="app_store_connect")
def app_store_connect_source(
    key_id: str = dlt.secrets.value,
    issuer_id: str = dlt.secrets.value,
    private_key: str = dlt.secrets.value,
    vendor_number: Optional[str] = None,
    resources: Optional[Sequence[str]] = None,
) -> list[DltResource]:
    """A dlt source for Apple App Store Connect API.

    Args:
        key_id: API key ID from App Store Connect.
        issuer_id: Issuer ID from App Store Connect.
        private_key: Contents of the .p8 private key file.
        vendor_number: Vendor number for sales/finance reports.
        resources: List of resource names to load. None for all.

    Returns:
        List of dlt resources.
    """
    client = AppStoreConnectClient(key_id, issuer_id, private_key)

    all_resources = [
        apps(client),
        app_store_versions(client),
        builds(client),
        beta_testers(client),
        beta_groups(client),
        bundle_ids(client),
        certificates(client),
        devices(client),
        in_app_purchases(client),
        subscriptions(client),
        subscription_groups(client),
        users(client),
        user_invitations(client),
        app_categories(client),
        territories(client),
        pre_release_versions(client),
        beta_app_review_submissions(client),
        beta_build_localizations(client),
        beta_app_localizations(client),
        beta_license_agreements(client),
        build_beta_details(client),
        app_encryption_declarations(client),
        provisioning_profiles(client),
        review_submissions(client),
        sales_reports(client, vendor_number=vendor_number or ""),
        finance_reports(client, vendor_number=vendor_number or ""),
        analytics_reports(client),
    ]

    if resources:
        return [r for r in all_resources if r.name in resources]
    return all_resources


# --- REST API Resources ---


@dlt.resource(name="apps", write_disposition="merge", primary_key="id")
def apps(client: AppStoreConnectClient):
    yield from client.get_paginated("apps")


@dlt.resource(name="app_store_versions", write_disposition="merge", primary_key="id")
def app_store_versions(client: AppStoreConnectClient):
    for app in client.get_paginated("apps"):
        app_id = app["id"]
        yield from client.get_paginated(f"apps/{app_id}/appStoreVersions")


@dlt.resource(name="builds", write_disposition="merge", primary_key="id")
def builds(client: AppStoreConnectClient):
    yield from client.get_paginated("builds")


@dlt.resource(name="beta_testers", write_disposition="merge", primary_key="id")
def beta_testers(client: AppStoreConnectClient):
    yield from client.get_paginated("betaTesters")


@dlt.resource(name="beta_groups", write_disposition="merge", primary_key="id")
def beta_groups(client: AppStoreConnectClient):
    yield from client.get_paginated("betaGroups")


@dlt.resource(name="bundle_ids", write_disposition="merge", primary_key="id")
def bundle_ids(client: AppStoreConnectClient):
    yield from client.get_paginated("bundleIds")


@dlt.resource(name="certificates", write_disposition="merge", primary_key="id")
def certificates(client: AppStoreConnectClient):
    yield from client.get_paginated("certificates")


@dlt.resource(name="devices", write_disposition="merge", primary_key="id")
def devices(client: AppStoreConnectClient):
    yield from client.get_paginated("devices")


@dlt.resource(name="in_app_purchases", write_disposition="merge", primary_key="id")
def in_app_purchases(client: AppStoreConnectClient):
    for app in client.get_paginated("apps"):
        app_id = app["id"]
        yield from client.get_paginated(f"apps/{app_id}/inAppPurchasesV2")


@dlt.resource(name="subscriptions", write_disposition="merge", primary_key="id")
def subscriptions(client: AppStoreConnectClient):
    for app in client.get_paginated("apps"):
        app_id = app["id"]
        for group in client.get_paginated(f"apps/{app_id}/subscriptionGroups"):
            group_id = group["id"]
            yield from client.get_paginated(
                f"subscriptionGroups/{group_id}/subscriptions"
            )


@dlt.resource(name="subscription_groups", write_disposition="merge", primary_key="id")
def subscription_groups(client: AppStoreConnectClient):
    for app in client.get_paginated("apps"):
        app_id = app["id"]
        yield from client.get_paginated(f"apps/{app_id}/subscriptionGroups")


@dlt.resource(name="users", write_disposition="merge", primary_key="id")
def users(client: AppStoreConnectClient):
    yield from client.get_paginated("users")


@dlt.resource(name="user_invitations", write_disposition="merge", primary_key="id")
def user_invitations(client: AppStoreConnectClient):
    yield from client.get_paginated("userInvitations")


@dlt.resource(name="app_categories", write_disposition="replace", primary_key="id")
def app_categories(client: AppStoreConnectClient):
    yield from client.get_paginated("appCategories")


@dlt.resource(name="territories", write_disposition="replace", primary_key="id")
def territories(client: AppStoreConnectClient):
    yield from client.get_paginated("territories")


@dlt.resource(name="pre_release_versions", write_disposition="merge", primary_key="id")
def pre_release_versions(client: AppStoreConnectClient):
    yield from client.get_paginated("preReleaseVersions")


@dlt.resource(
    name="beta_app_review_submissions", write_disposition="merge", primary_key="id"
)
def beta_app_review_submissions(client: AppStoreConnectClient):
    yield from client.get_paginated("betaAppReviewSubmissions")


@dlt.resource(
    name="beta_build_localizations", write_disposition="merge", primary_key="id"
)
def beta_build_localizations(client: AppStoreConnectClient):
    yield from client.get_paginated("betaBuildLocalizations")


@dlt.resource(
    name="beta_app_localizations", write_disposition="merge", primary_key="id"
)
def beta_app_localizations(client: AppStoreConnectClient):
    yield from client.get_paginated("betaAppLocalizations")


@dlt.resource(
    name="beta_license_agreements", write_disposition="merge", primary_key="id"
)
def beta_license_agreements(client: AppStoreConnectClient):
    yield from client.get_paginated("betaLicenseAgreements")


@dlt.resource(name="build_beta_details", write_disposition="merge", primary_key="id")
def build_beta_details(client: AppStoreConnectClient):
    yield from client.get_paginated("buildBetaDetails")


@dlt.resource(
    name="app_encryption_declarations", write_disposition="merge", primary_key="id"
)
def app_encryption_declarations(client: AppStoreConnectClient):
    yield from client.get_paginated("appEncryptionDeclarations")


@dlt.resource(name="provisioning_profiles", write_disposition="merge", primary_key="id")
def provisioning_profiles(client: AppStoreConnectClient):
    yield from client.get_paginated("profiles")


@dlt.resource(name="review_submissions", write_disposition="merge", primary_key="id")
def review_submissions(client: AppStoreConnectClient):
    yield from client.get_paginated("reviewSubmissions")


# --- Sales & Finance Reports (with incremental loading) ---


def _date_range(start: str, end: str):
    """Generate dates from start to end (inclusive), YYYY-MM-DD format."""
    current = date.fromisoformat(start)
    end_date = date.fromisoformat(end)
    while current <= end_date:
        yield current.isoformat()
        current += timedelta(days=1)


def _month_range(start: str, end: str):
    """Generate months from start to end (inclusive), YYYY-MM format."""
    current = date.fromisoformat(start + "-01")
    end_date = date.fromisoformat(end + "-01")
    while current <= end_date:
        yield current.strftime("%Y-%m")
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)


@dlt.resource(name="sales_reports", write_disposition="append")
def sales_reports(
    client: AppStoreConnectClient,
    vendor_number: str = "",
    report_type: str = "SALES",
    report_sub_type: str = "SUMMARY",
    frequency: str = "DAILY",
    version: str = "1_0",
    last_date=dlt.sources.incremental("_report_date", initial_value="2020-01-01"),
):
    """Download Sales and Trends reports with incremental loading.

    Args:
        client: API client.
        vendor_number: Your vendor number from App Store Connect.
        report_type: SALES, PRE_ORDER, NEWSSTAND, SUBSCRIPTION,
            SUBSCRIPTION_EVENT, SUBSCRIBER, SUBSCRIPTION_OFFER_CODE_REDEMPTION,
            INSTALLS, FIRST_ANNUAL, WIN_BACK_ELIGIBILITY.
        report_sub_type: SUMMARY, DETAILED, SUMMARY_INSTALL_TYPE,
            SUMMARY_TERRITORY, SUMMARY_CHANNEL.
        frequency: DAILY, WEEKLY, MONTHLY, YEARLY.
        version: Report version (default: 1_0).
        last_date: Managed by dlt incremental. Do not set manually.
    """
    if not vendor_number:
        return

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
        rows = client.download_tsv(f"{BASE_URL}/salesReports", params=params)
        if not rows:
            continue
        for row in rows:
            row["_report_date"] = report_date
            row["_report_type"] = report_type
            row["_frequency"] = frequency
        yield from rows


@dlt.resource(name="finance_reports", write_disposition="append")
def finance_reports(
    client: AppStoreConnectClient,
    vendor_number: str = "",
    region_code: str = "ZZ",
    report_type: str = "FINANCIAL",
    last_date=dlt.sources.incremental("_report_date", initial_value="2020-01"),
):
    """Download Finance reports with incremental loading.

    Args:
        client: API client.
        vendor_number: Your vendor number from App Store Connect.
        region_code: Two-letter region code, or ZZ for all regions.
        report_type: FINANCIAL, FINANCE_DETAIL.
        last_date: Managed by dlt incremental. Do not set manually.
    """
    if not vendor_number:
        return

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
        rows = client.download_tsv(f"{BASE_URL}/financeReports", params=params)
        if not rows:
            continue
        for row in rows:
            row["_report_date"] = report_date
            row["_report_type"] = report_type
        yield from rows


# --- Analytics Reports ---


@dlt.resource(name="analytics_reports", write_disposition="append")
def analytics_reports(
    client: AppStoreConnectClient,
    last_processing_date=dlt.sources.incremental(
        "_processing_date", initial_value="2020-01-01"
    ),
):
    """Download Analytics reports with incremental loading.

    Fetches all available analytics report instances and their segments.
    Requires an existing ONGOING or ONE_TIME_SNAPSHOT analytics report request.

    Report categories: APP_USAGE, APP_STORE_ENGAGEMENT, COMMERCE,
        FRAMEWORK_USAGE, PERFORMANCE.
    Instance granularity: DAILY, WEEKLY, MONTHLY.

    Args:
        client: API client.
        last_processing_date: Managed by dlt incremental. Do not set manually.
    """
    for app in client.get_paginated("apps"):
        app_id = app["id"]
        for request in client.get_paginated(f"apps/{app_id}/analyticsReportRequests"):
            request_id = request["id"]
            for report in client.get_paginated(
                f"analyticsReportRequests/{request_id}/reports"
            ):
                report_id = report["id"]
                report_name = report.get("attributes", {}).get("name", "unknown")
                category = report.get("attributes", {}).get("category", "unknown")

                for instance in client.get_paginated(
                    f"analyticsReports/{report_id}/instances"
                ):
                    instance_id = instance["id"]
                    processing_date = instance.get("attributes", {}).get(
                        "processingDate", ""
                    )
                    granularity = instance.get("attributes", {}).get("granularity", "")

                    for segment in client.get_paginated(
                        f"analyticsReportInstances/{instance_id}/segments"
                    ):
                        url = segment.get("attributes", {}).get("url")
                        if not url:
                            continue
                        rows = client.download_gzip_tsv(url)
                        if not rows:
                            continue
                        for row in rows:
                            row["_report_name"] = report_name
                            row["_category"] = category
                            row["_processing_date"] = processing_date
                            row["_granularity"] = granularity
                        yield from rows

"""dlt source for Microsoft Advertising API.

Microsoft Advertising uses POST RPC endpoints. All requests require
Authorization (Bearer), DeveloperToken, CustomerId, and AccountId headers.

REST URL pattern: {service_base}/{OperationName}
e.g., https://campaign.api.bingads.microsoft.com/CampaignManagement/v13/GetCampaignsByAccountId

SDK reference: https://github.com/BingAds/BingAds-Python-SDK
"""

import csv
import io
import logging
import time
import zipfile
from collections.abc import Generator
from datetime import date, timedelta
from typing import Optional, Sequence

import dlt
from dlt.sources import DltResource
from dlt.sources.helpers import requests as req

from .auth import refresh_access_token

logger = logging.getLogger(__name__)

# Base URLs
CAMPAIGN_MGMT_URL = "https://campaign.api.bingads.microsoft.com/CampaignManagement/v13"
REPORTING_URL = "https://reporting.api.bingads.microsoft.com/Reporting/v13"
CUSTOMER_MGMT_URL = (
    "https://clientcenter.api.bingads.microsoft.com/CustomerManagement/v13"
)

# Polling
POLL_INTERVAL_SECONDS = 10
POLL_MAX_WAIT_SECONDS = 600

# Report types (from SDK: bingads/v13/proxies/production/reporting_service.xml)
REPORT_TYPES = [
    "AccountPerformanceReportRequest",
    "AdDynamicTextPerformanceReportRequest",
    "AdExtensionByAdReportRequest",
    "AdExtensionByKeywordReportRequest",
    "AdExtensionDetailReportRequest",
    "AdGroupPerformanceReportRequest",
    "AdPerformanceReportRequest",
    "AgeGenderAudienceReportRequest",
    "AssetGroupPerformanceReportRequest",
    "AssetPerformanceReportRequest",
    "AudiencePerformanceReportRequest",
    "BidStrategyReportRequest",
    "BudgetSummaryReportRequest",
    "CallDetailReportRequest",
    "CampaignPerformanceReportRequest",
    "CombinationPerformanceReportRequest",
    "ConversionPerformanceReportRequest",
    "DestinationUrlPerformanceReportRequest",
    "DSAAutoTargetPerformanceReportRequest",
    "DSACategoryPerformanceReportRequest",
    "DSASearchQueryPerformanceReportRequest",
    "GeographicPerformanceReportRequest",
    "GoalsAndFunnelsReportRequest",
    "HotelDimensionPerformanceReportRequest",
    "HotelGroupPerformanceReportRequest",
    "KeywordPerformanceReportRequest",
    "NegativeKeywordConflictReportRequest",
    "ProductDimensionPerformanceReportRequest",
    "ProductPartitionPerformanceReportRequest",
    "ProductPartitionUnitPerformanceReportRequest",
    "ProductSearchQueryPerformanceReportRequest",
    "ProfessionalDemographicsAudienceReportRequest",
    "PublisherUsagePerformanceReportRequest",
    "SearchCampaignChangeHistoryReportRequest",
    "SearchQueryPerformanceReportRequest",
    "ShareOfVoiceReportRequest",
    "UserLocationPerformanceReportRequest",
]

# Default columns per report type
REPORT_COLUMNS = {
    "CampaignPerformanceReportRequest": [
        "TimePeriod",
        "AccountName",
        "AccountId",
        "CampaignName",
        "CampaignId",
        "CampaignStatus",
        "CurrencyCode",
        "Impressions",
        "Clicks",
        "Ctr",
        "AverageCpc",
        "Spend",
        "Conversions",
        "ConversionRate",
        "Revenue",
        "ReturnOnAdSpend",
        "QualityScore",
    ],
    "AdGroupPerformanceReportRequest": [
        "TimePeriod",
        "AccountName",
        "AccountId",
        "CampaignName",
        "CampaignId",
        "AdGroupName",
        "AdGroupId",
        "AdGroupStatus",
        "Impressions",
        "Clicks",
        "Ctr",
        "AverageCpc",
        "Spend",
        "Conversions",
        "ConversionRate",
        "Revenue",
        "ReturnOnAdSpend",
        "QualityScore",
    ],
    "AdPerformanceReportRequest": [
        "TimePeriod",
        "AccountName",
        "AccountId",
        "CampaignName",
        "CampaignId",
        "AdGroupName",
        "AdGroupId",
        "AdId",
        "AdTitle",
        "AdDescription",
        "AdStatus",
        "Impressions",
        "Clicks",
        "Ctr",
        "AverageCpc",
        "Spend",
        "Conversions",
        "ConversionRate",
        "Revenue",
    ],
    "KeywordPerformanceReportRequest": [
        "TimePeriod",
        "AccountName",
        "AccountId",
        "CampaignName",
        "CampaignId",
        "AdGroupName",
        "AdGroupId",
        "Keyword",
        "KeywordId",
        "KeywordStatus",
        "BidMatchType",
        "DeliveredMatchType",
        "CurrentMaxCpc",
        "Impressions",
        "Clicks",
        "Ctr",
        "AverageCpc",
        "Spend",
        "Conversions",
        "ConversionRate",
        "QualityScore",
    ],
}

# Report CSV fields requiring numeric conversion
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


def _build_headers(
    access_token: str,
    developer_token: str,
    customer_id: str,
    account_id: str,
) -> dict:
    """Build headers required for all Microsoft Advertising API requests."""
    return {
        "Authorization": f"Bearer {access_token}",
        "DeveloperToken": developer_token,
        "CustomerId": customer_id,
        "AccountId": account_id,
        "Content-Type": "application/json",
    }


def _make_client(
    access_token: str,
    developer_token: str,
    customer_id: str,
    account_id: str,
) -> req.Client:
    """Create a dlt HTTP client with Microsoft Advertising auth headers.

    Note: 429 retry handled by dlt's req.Client automatically.
    """
    client = req.Client()
    client.session.headers.update(
        _build_headers(access_token, developer_token, customer_id, account_id)
    )
    return client


def _post_rpc(client: req.Client, url: str, body: dict) -> dict:
    """Make a POST RPC call and return response JSON."""
    response = client.post(url, json=body)
    response.raise_for_status()
    return response.json()


def _get_entities_paginated(
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
        data = _post_rpc(client, url, body)
        entities = data.get(entities_key, [])
        if not entities:
            break
        yield from entities
        if len(entities) < page_size:
            break
        page_index += 1


def _convert_report_types(row: dict) -> dict:
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
                row[field] = float(row[field])
            except (ValueError, TypeError):
                pass
    return row


def _safe_rpc(client: req.Client, url: str, body: dict, key: str) -> list:
    """Make a POST RPC call, return list from key, skip on 403/404."""
    try:
        data = _post_rpc(client, url, body)
        return data.get(key, [])
    except req.HTTPError as e:
        if e.response is not None and e.response.status_code in (403, 404):
            logger.warning("Skipping %s: %d", url, e.response.status_code)
            return []
        raise


# --- Campaign Management resources ---
# Endpoint pattern: {CAMPAIGN_MGMT_URL}/{OperationName}
# SDK reference: bingads/v13/proxies/production/campaignmanagement_service.xml


@dlt.resource(name="campaigns", write_disposition="merge", primary_key="Id")
def campaigns(
    access_token: str,
    developer_token: str,
    customer_id: str,
    account_id: str,
    base_url: str = CAMPAIGN_MGMT_URL,
):
    """Fetch all campaigns. SDK: GetCampaignsByAccountId."""
    client = _make_client(access_token, developer_token, customer_id, account_id)
    data = _post_rpc(
        client,
        f"{base_url}/GetCampaignsByAccountId",
        {"AccountId": account_id, "CampaignType": "Search Shopping Audience"},
    )
    yield from data.get("Campaigns", [])


@dlt.resource(name="ad_groups", write_disposition="merge", primary_key="Id")
def ad_groups(
    access_token: str,
    developer_token: str,
    customer_id: str,
    account_id: str,
    base_url: str = CAMPAIGN_MGMT_URL,
):
    """Fetch all ad groups. SDK: GetAdGroupsByCampaignId."""
    client = _make_client(access_token, developer_token, customer_id, account_id)
    for camp in _safe_rpc(
        client,
        f"{base_url}/GetCampaignsByAccountId",
        {"AccountId": account_id, "CampaignType": "Search Shopping Audience"},
        "Campaigns",
    ):
        cid = camp.get("Id")
        if cid:
            yield from _safe_rpc(
                client,
                f"{base_url}/GetAdGroupsByCampaignId",
                {"CampaignId": cid},
                "AdGroups",
            )


@dlt.resource(name="ads", write_disposition="merge", primary_key="Id")
def ads(
    access_token: str,
    developer_token: str,
    customer_id: str,
    account_id: str,
    base_url: str = CAMPAIGN_MGMT_URL,
):
    """Fetch all ads. SDK: GetAdsByAdGroupId."""
    client = _make_client(access_token, developer_token, customer_id, account_id)
    for ag in ad_groups(
        access_token, developer_token, customer_id, account_id, base_url
    ):
        ag_id = ag.get("Id")
        if ag_id:
            yield from _safe_rpc(
                client,
                f"{base_url}/GetAdsByAdGroupId",
                {
                    "AdGroupId": ag_id,
                    "AdTypes": "AppInstall DynamicSearch ExpandedText Product ResponsiveAd ResponsiveSearch",
                },
                "Ads",
            )


@dlt.resource(name="keywords", write_disposition="merge", primary_key="Id")
def keywords(
    access_token: str,
    developer_token: str,
    customer_id: str,
    account_id: str,
    base_url: str = CAMPAIGN_MGMT_URL,
):
    """Fetch all keywords. SDK: GetKeywordsByAdGroupId."""
    client = _make_client(access_token, developer_token, customer_id, account_id)
    for ag in ad_groups(
        access_token, developer_token, customer_id, account_id, base_url
    ):
        ag_id = ag.get("Id")
        if ag_id:
            yield from _safe_rpc(
                client,
                f"{base_url}/GetKeywordsByAdGroupId",
                {"AdGroupId": ag_id},
                "Keywords",
            )


@dlt.resource(name="ad_extensions", write_disposition="merge", primary_key="Id")
def ad_extensions(
    access_token: str,
    developer_token: str,
    customer_id: str,
    account_id: str,
    base_url: str = CAMPAIGN_MGMT_URL,
):
    """Fetch all ad extensions. SDK: GetAdExtensionIdsByAccountId + GetAdExtensionsByIds."""
    client = _make_client(access_token, developer_token, customer_id, account_id)
    ext_types = "CallAdExtension CalloutAdExtension ImageAdExtension LocationAdExtension PriceAdExtension ReviewAdExtension SitelinkAdExtension StructuredSnippetAdExtension"
    id_data = _safe_rpc(
        client,
        f"{base_url}/GetAdExtensionIdsByAccountId",
        {"AccountId": account_id, "AdExtensionType": ext_types},
        "AdExtensionIds",
    )
    if not id_data:
        return
    # Flatten nested ID structure and fetch in batches
    ext_ids = []
    for group in id_data:
        if isinstance(group, dict):
            ext_ids.extend(group.get("long", []))
        elif isinstance(group, (int, str)):
            ext_ids.append(group)
    if ext_ids:
        yield from _safe_rpc(
            client,
            f"{base_url}/GetAdExtensionsByIds",
            {
                "AccountId": account_id,
                "AdExtensionIds": ext_ids,
                "AdExtensionType": ext_types,
            },
            "AdExtensions",
        )


@dlt.resource(name="audiences", write_disposition="merge", primary_key="Id")
def audiences(
    access_token: str,
    developer_token: str,
    customer_id: str,
    account_id: str,
    base_url: str = CAMPAIGN_MGMT_URL,
):
    """Fetch audiences. SDK: GetAudiencesByIds (all types)."""
    client = _make_client(access_token, developer_token, customer_id, account_id)
    yield from _safe_rpc(
        client,
        f"{base_url}/GetAudiencesByIds",
        {
            "Type": "Custom InMarket Product RemarketingList SimilarRemarketingList CombinedList CustomerList ImpressionBasedRemarketingList"
        },
        "Audiences",
    )


@dlt.resource(name="conversion_goals", write_disposition="merge", primary_key="Id")
def conversion_goals(
    access_token: str,
    developer_token: str,
    customer_id: str,
    account_id: str,
    base_url: str = CAMPAIGN_MGMT_URL,
):
    """Fetch conversion goals. SDK: GetConversionGoalsByIds."""
    client = _make_client(access_token, developer_token, customer_id, account_id)
    yield from _safe_rpc(
        client,
        f"{base_url}/GetConversionGoalsByIds",
        {
            "ConversionGoalTypes": "Url Duration Event AppInstall InStoreTransaction OfflineConversion"
        },
        "ConversionGoals",
    )


@dlt.resource(name="uet_tags", write_disposition="merge", primary_key="Id")
def uet_tags(
    access_token: str,
    developer_token: str,
    customer_id: str,
    account_id: str,
    base_url: str = CAMPAIGN_MGMT_URL,
):
    """Fetch UET tags. SDK: GetUetTagsByIds."""
    client = _make_client(access_token, developer_token, customer_id, account_id)
    yield from _safe_rpc(client, f"{base_url}/GetUetTagsByIds", {}, "UetTags")


@dlt.resource(name="labels", write_disposition="merge", primary_key="Id")
def labels(
    access_token: str,
    developer_token: str,
    customer_id: str,
    account_id: str,
    base_url: str = CAMPAIGN_MGMT_URL,
):
    """Fetch labels. SDK: GetLabelsByIds."""
    client = _make_client(access_token, developer_token, customer_id, account_id)
    yield from _get_entities_paginated(
        client, f"{base_url}/GetLabelsByIds", {}, "Labels"
    )


@dlt.resource(name="budgets", write_disposition="merge", primary_key="Id")
def budgets(
    access_token: str,
    developer_token: str,
    customer_id: str,
    account_id: str,
    base_url: str = CAMPAIGN_MGMT_URL,
):
    """Fetch shared budgets. SDK: GetBudgetsByIds."""
    client = _make_client(access_token, developer_token, customer_id, account_id)
    yield from _safe_rpc(client, f"{base_url}/GetBudgetsByIds", {}, "Budgets")


@dlt.resource(name="bid_strategies", write_disposition="merge", primary_key="Id")
def bid_strategies(
    access_token: str,
    developer_token: str,
    customer_id: str,
    account_id: str,
    base_url: str = CAMPAIGN_MGMT_URL,
):
    """Fetch bid strategies. SDK: GetBidStrategiesByIds."""
    client = _make_client(access_token, developer_token, customer_id, account_id)
    yield from _safe_rpc(
        client, f"{base_url}/GetBidStrategiesByIds", {}, "BidStrategies"
    )


@dlt.resource(name="shared_entities", write_disposition="merge", primary_key="Id")
def shared_entities(
    access_token: str,
    developer_token: str,
    customer_id: str,
    account_id: str,
    base_url: str = CAMPAIGN_MGMT_URL,
):
    """Fetch shared entities. SDK: GetSharedEntitiesByAccountId."""
    client = _make_client(access_token, developer_token, customer_id, account_id)
    yield from _safe_rpc(
        client,
        f"{base_url}/GetSharedEntitiesByAccountId",
        {"SharedEntityType": "NegativeKeywordList"},
        "SharedEntities",
    )


@dlt.resource(name="media", write_disposition="merge", primary_key="MediaId")
def media(
    access_token: str,
    developer_token: str,
    customer_id: str,
    account_id: str,
    base_url: str = CAMPAIGN_MGMT_URL,
):
    """Fetch media metadata. SDK: GetMediaMetaDataByAccountId."""
    client = _make_client(access_token, developer_token, customer_id, account_id)
    yield from _safe_rpc(
        client,
        f"{base_url}/GetMediaMetaDataByAccountId",
        {"MediaEnabledEntities": "ImageAdExtension ResponsiveAd"},
        "MediaMetaData",
    )


@dlt.resource(name="account_properties", write_disposition="replace")
def account_properties(
    access_token: str,
    developer_token: str,
    customer_id: str,
    account_id: str,
    base_url: str = CAMPAIGN_MGMT_URL,
):
    """Fetch account properties. SDK: GetAccountProperties."""
    client = _make_client(access_token, developer_token, customer_id, account_id)
    data = _post_rpc(
        client,
        f"{base_url}/GetAccountProperties",
        {"AccountPropertyNames": "TrackingUrlTemplate FinalUrlSuffix"},
    )
    props = data.get("AccountProperties", [])
    if props:
        yield {"account_id": account_id, "properties": props}


@dlt.resource(
    name="seasonality_adjustments", write_disposition="merge", primary_key="Id"
)
def seasonality_adjustments(
    access_token: str,
    developer_token: str,
    customer_id: str,
    account_id: str,
    base_url: str = CAMPAIGN_MGMT_URL,
):
    """Fetch seasonality adjustments. SDK: GetSeasonalityAdjustmentsByAccountId."""
    client = _make_client(access_token, developer_token, customer_id, account_id)
    yield from _safe_rpc(
        client,
        f"{base_url}/GetSeasonalityAdjustmentsByAccountId",
        {"AccountId": account_id},
        "SeasonalityAdjustments",
    )


@dlt.resource(name="data_exclusions", write_disposition="merge", primary_key="Id")
def data_exclusions(
    access_token: str,
    developer_token: str,
    customer_id: str,
    account_id: str,
    base_url: str = CAMPAIGN_MGMT_URL,
):
    """Fetch data exclusions. SDK: GetDataExclusionsByAccountId."""
    client = _make_client(access_token, developer_token, customer_id, account_id)
    yield from _safe_rpc(
        client,
        f"{base_url}/GetDataExclusionsByAccountId",
        {"AccountId": account_id},
        "DataExclusions",
    )


@dlt.resource(name="experiments", write_disposition="merge", primary_key="Id")
def experiments(
    access_token: str,
    developer_token: str,
    customer_id: str,
    account_id: str,
    base_url: str = CAMPAIGN_MGMT_URL,
):
    """Fetch experiments. SDK: GetExperimentsByIds."""
    client = _make_client(access_token, developer_token, customer_id, account_id)
    yield from _get_entities_paginated(
        client, f"{base_url}/GetExperimentsByIds", {}, "Experiments"
    )


@dlt.resource(name="import_jobs", write_disposition="merge", primary_key="Id")
def import_jobs(
    access_token: str,
    developer_token: str,
    customer_id: str,
    account_id: str,
    base_url: str = CAMPAIGN_MGMT_URL,
):
    """Fetch import jobs. SDK: GetImportJobsByIds."""
    client = _make_client(access_token, developer_token, customer_id, account_id)
    yield from _safe_rpc(
        client,
        f"{base_url}/GetImportJobsByIds",
        {"ImportType": "GoogleImportJob"},
        "ImportJobs",
    )


# --- Reporting ---


def _submit_report(
    client: req.Client,
    report_type: str,
    account_id: str,
    columns: list[str],
    start_date: str,
    end_date: str,
    aggregation: str = "Daily",
    base_url: str = REPORTING_URL,
) -> Optional[str]:
    """Submit a report request. Returns ReportRequestId or None."""
    start = date.fromisoformat(start_date)
    end_d = date.fromisoformat(end_date)
    body = {
        "ReportRequest": {
            "ExcludeColumnHeaders": False,
            "ExcludeReportFooter": True,
            "ExcludeReportHeader": True,
            "Format": "Csv",
            "FormatVersion": "2.0",
            "ReportName": report_type,
            "ReturnOnlyCompleteData": True,
            "Type": report_type,
            "Aggregation": aggregation,
            "Columns": columns,
            "Scope": {"AccountIds": [account_id]},
            "Time": {
                "CustomDateRangeStart": {
                    "Day": start.day,
                    "Month": start.month,
                    "Year": start.year,
                },
                "CustomDateRangeEnd": {
                    "Day": end_d.day,
                    "Month": end_d.month,
                    "Year": end_d.year,
                },
            },
        }
    }
    data = _post_rpc(client, f"{base_url}/SubmitGenerateReport", body)
    return data.get("ReportRequestId")


def _poll_report(
    client: req.Client,
    request_id: str,
    base_url: str = REPORTING_URL,
) -> Optional[str]:
    """Poll report until completion. Returns download URL or None.

    Note: 429 retry handled by dlt's req.Client automatically.
    """
    elapsed = 0
    while elapsed < POLL_MAX_WAIT_SECONDS:
        data = _post_rpc(
            client,
            f"{base_url}/PollGenerateReport",
            {"ReportRequestId": request_id},
        )
        status_obj = data.get("ReportRequestStatus", {})
        status = status_obj.get("Status")
        logger.info("Report %s: status=%s", request_id, status)

        if status == "Success":
            return status_obj.get("ReportDownloadUrl")
        if status == "Error":
            logger.warning("Report %s failed", request_id)
            return None

        time.sleep(POLL_INTERVAL_SECONDS)
        elapsed += POLL_INTERVAL_SECONDS

    logger.warning("Report %s timed out", request_id)
    return None


def _download_csv_report(client: req.Client, url: str) -> Generator[dict, None, None]:
    """Download ZIP-compressed CSV report and yield rows as dicts."""
    response = client.get(url)
    response.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
        for name in zf.namelist():
            if not name.endswith(".csv"):
                continue
            with zf.open(name) as f:
                text = io.TextIOWrapper(f, encoding="utf-8-sig")
                reader = csv.DictReader(text)
                for row in reader:
                    yield _convert_report_types(dict(row))


@dlt.resource(name="report", write_disposition="merge")
def report(
    access_token: str,
    developer_token: str,
    customer_id: str,
    account_id: str,
    report_type: str = "CampaignPerformanceReportRequest",
    columns: Optional[list[str]] = None,
    aggregation: str = "Daily",
    attribution_window_days: int = 7,
    last_date=dlt.sources.incremental("TimePeriod", initial_value="2020-01-01"),
    base_url: str = REPORTING_URL,
):
    """Fetch Microsoft Advertising report with incremental loading."""
    report_columns = columns or REPORT_COLUMNS.get(
        report_type,
        REPORT_COLUMNS["CampaignPerformanceReportRequest"],
    )
    client = _make_client(access_token, developer_token, customer_id, account_id)

    # Start date: go back attribution_window_days from last cursor
    last = last_date.last_value
    window_start = date.fromisoformat(last) - timedelta(days=attribution_window_days)
    start = window_start.isoformat()
    end = (date.today() - timedelta(days=1)).isoformat()

    if start > end:
        logger.info("report: already up to date")
        return

    logger.info("report: %s from %s to %s", report_type, start, end)

    request_id = _submit_report(
        client,
        report_type,
        account_id,
        report_columns,
        start,
        end,
        aggregation,
        base_url,
    )
    if not request_id:
        logger.warning("report: no request ID returned")
        return

    download_url = _poll_report(client, request_id, base_url)
    if not download_url:
        return

    yield from _download_csv_report(client, download_url)


# --- Customer Management ---


@dlt.resource(name="account_info", write_disposition="merge", primary_key="Id")
def account_info(
    access_token: str,
    developer_token: str,
    customer_id: str,
    account_id: str,
    base_url: str = CUSTOMER_MGMT_URL,
):
    """Fetch account details. SDK: GetAccount."""
    client = _make_client(access_token, developer_token, customer_id, account_id)
    data = _post_rpc(client, f"{base_url}/GetAccount", {"AccountId": account_id})
    account = data.get("Account")
    if account:
        yield account


@dlt.resource(name="customer_info", write_disposition="merge", primary_key="Id")
def customer_info(
    access_token: str,
    developer_token: str,
    customer_id: str,
    account_id: str,
    base_url: str = CUSTOMER_MGMT_URL,
):
    """Fetch customer details. SDK: GetCustomer."""
    client = _make_client(access_token, developer_token, customer_id, account_id)
    data = _post_rpc(client, f"{base_url}/GetCustomer", {"CustomerId": customer_id})
    customer = data.get("Customer")
    if customer:
        yield customer


@dlt.resource(name="accounts_info", write_disposition="merge", primary_key="AccountId")
def accounts_info(
    access_token: str,
    developer_token: str,
    customer_id: str,
    account_id: str,
    base_url: str = CUSTOMER_MGMT_URL,
):
    """Fetch all accounts info. SDK: GetAccountsInfo."""
    client = _make_client(access_token, developer_token, customer_id, account_id)
    yield from _safe_rpc(
        client,
        f"{base_url}/GetAccountsInfo",
        {"CustomerId": customer_id},
        "AccountsInfo",
    )


# --- Source ---


@dlt.source(name="microsoft_ads")
def microsoft_ads_source(
    client_id: str = dlt.secrets.value,
    client_secret: str = dlt.secrets.value,
    developer_token: str = dlt.secrets.value,
    refresh_token: str = dlt.secrets.value,
    account_id: str = dlt.secrets.value,
    customer_id: str = dlt.secrets.value,
    report_type: str = "CampaignPerformanceReportRequest",
    report_columns: Optional[list[str]] = None,
    aggregation: str = "Daily",
    attribution_window_days: int = 7,
    resources: Optional[Sequence[str]] = None,
    start_date: Optional[str] = None,
) -> list[DltResource]:
    """A dlt source for Microsoft Advertising API.

    Args:
        client_id: Azure AD app client ID.
        client_secret: Azure AD app client secret.
        developer_token: Microsoft Advertising developer token.
        refresh_token: OAuth refresh token (rotated on each use).
        account_id: Microsoft Advertising account ID.
        customer_id: Microsoft Advertising customer ID.
        report_type: Report type for the report resource.
        report_columns: Custom report columns.
        aggregation: Report aggregation (Daily, Weekly, Monthly).
        attribution_window_days: Days to re-fetch for attribution window.
        resources: Resource names to load. None for all.
        start_date: Override incremental start date (YYYY-MM-DD).

    Returns:
        List of dlt resources.
        The caller is responsible for persisting the new refresh_token.
    """
    # Refresh token → access_token (token rotation)
    tokens = refresh_access_token(client_id, client_secret, refresh_token)
    access_token = tokens["access_token"]
    # tokens["refresh_token"] should be persisted by the caller

    auth_args = (access_token, developer_token, customer_id, account_id)

    # Campaign Management resources
    master_resources = [
        campaigns(*auth_args),
        ad_groups(*auth_args),
        ads(*auth_args),
        keywords(*auth_args),
        ad_extensions(*auth_args),
        audiences(*auth_args),
        conversion_goals(*auth_args),
        uet_tags(*auth_args),
        labels(*auth_args),
        budgets(*auth_args),
        bid_strategies(*auth_args),
        shared_entities(*auth_args),
        media(*auth_args),
        account_properties(*auth_args),
        seasonality_adjustments(*auth_args),
        data_exclusions(*auth_args),
        experiments(*auth_args),
        import_jobs(*auth_args),
    ]

    # Customer Management resources
    customer_resources = [
        account_info(*auth_args),
        customer_info(*auth_args),
        accounts_info(*auth_args),
    ]

    # Report resource
    initial_value = start_date or "2020-01-01"
    report_resource = report(
        *auth_args,
        report_type=report_type,
        columns=report_columns,
        aggregation=aggregation,
        attribution_window_days=attribution_window_days,
        last_date=dlt.sources.incremental("TimePeriod", initial_value=initial_value),
    )
    # Primary key depends on report type
    pk = ["TimePeriod", "AccountId"]
    if "Campaign" in report_type:
        pk.append("CampaignId")
    if "AdGroup" in report_type:
        pk.append("AdGroupId")
    if "Ad" in report_type and "AdGroup" not in report_type:
        pk.append("AdId")
    if "Keyword" in report_type:
        pk.append("KeywordId")
    report_resource.apply_hints(primary_key=pk)

    all_resources = master_resources + customer_resources + [report_resource]

    if resources:
        return [r for r in all_resources if r.name in resources]
    return all_resources

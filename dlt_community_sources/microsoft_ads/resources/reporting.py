"""Reporting API resources.

SDK: bingads/v13/proxies/production/reporting_service.xml
"""

import csv
import io
import logging
import time
import zipfile
from collections.abc import Generator
from datetime import date, timedelta
from typing import Optional

import dlt
import requests
from dlt.sources.helpers import requests as req

from .helpers import (
    POLL_INTERVAL_SECONDS,
    POLL_MAX_WAIT_SECONDS,
    REPORTING_URL,
    convert_report_types,
    make_client,
    post_rpc,
)

logger = logging.getLogger(__name__)

# All report types from SDK
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
    data = post_rpc(client, f"{base_url}/GenerateReport/Submit", body)
    return data.get("ReportRequestId")


def _poll_report(
    client: req.Client,
    request_id: str,
    base_url: str = REPORTING_URL,
) -> Optional[str]:
    """Poll report until completion. Returns download URL or None."""
    elapsed = 0
    while elapsed < POLL_MAX_WAIT_SECONDS:
        data = post_rpc(
            client, f"{base_url}/GenerateReport/Poll", {"ReportRequestId": request_id}
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
    """Download ZIP-compressed CSV report and yield rows.

    Uses plain requests without auth headers because the download URL
    is a pre-signed Azure Blob Storage SAS URL.
    """
    response = requests.get(url, timeout=300)
    response.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
        for name in zf.namelist():
            if not name.endswith(".csv"):
                continue
            with zf.open(name) as f:
                text = io.TextIOWrapper(f, encoding="utf-8-sig")
                reader = csv.DictReader(text)
                for row in reader:
                    yield convert_report_types(dict(row))


@dlt.resource(
    name="report",
    write_disposition="merge",
    columns={"TimePeriod": {"data_type": "date"}},
)
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
    client = make_client(access_token, developer_token, customer_id, account_id)

    last = last_date.last_value
    # Handle both "2026-01-01" and "2026-01-01 00:00:00" formats
    last_date_str = last.split(" ")[0].split("T")[0]
    window_start = date.fromisoformat(last_date_str) - timedelta(
        days=attribution_window_days
    )
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

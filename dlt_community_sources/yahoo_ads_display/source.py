"""Yahoo Ads Display (YDA) dlt source.

API: https://ads-display.yahooapis.jp/api/v19
Docs: https://ads-developers.yahoo.co.jp/reference/ads-display-api/
SDK: https://github.com/yahoojp-marketing/ads-display-api-java-lib

All endpoints use POST RPC style. Pagination via startIndex/numberResults.
Includes LINE placement data (YDA serves ads on LINE surfaces).
"""

import logging
from datetime import date, timedelta
from typing import Optional

import dlt

from dlt_community_sources.yahoo_ads_common.auth import refresh_access_token
from dlt_community_sources.yahoo_ads_common.helpers import (
    convert_report_types,
    derive_primary_key,
    download_report,
    make_client,
    poll_report,
    safe_get_entities,
    submit_report,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://ads-display.yahooapis.jp/api/v19"

# ---------------------------------------------------------------------------
# Simple entity resources
# ---------------------------------------------------------------------------

_ENTITY_RESOURCES = [
    # (resource_name, service_path, write_disposition, primary_key)
    ("accounts", "AccountService/get", "merge", "accountId"),
    ("campaigns", "CampaignService/get", "merge", "campaignId"),
    ("ad_groups", "AdGroupService/get", "merge", "adGroupId"),
    ("ads", "AdGroupAdService/get", "merge", "adId"),
    ("ad_group_targets", "AdGroupTargetService/get", "replace", "targetId"),
    ("labels", "LabelService/get", "merge", "labelId"),
    ("bidding_strategies", "BiddingStrategyService/get", "merge", "biddingStrategyId"),
    ("campaign_budgets", "CampaignBudgetService/get", "merge", "budgetId"),
    ("audience_lists", "AudienceListService/get", "merge", "audienceListId"),
    (
        "conversion_trackers",
        "ConversionTrackerService/get",
        "merge",
        "conversionTrackerId",
    ),
    ("conversion_groups", "ConversionGroupService/get", "merge", "conversionGroupId"),
    ("media", "MediaService/get", "merge", "mediaId"),
    ("videos", "VideoService/get", "merge", "mediaId"),
    ("feeds", "FeedService/get", "merge", "feedId"),
    ("feed_sets", "FeedSetService/get", "merge", "feedSetId"),
    ("placement_url_lists", "PlacementUrlListService/get", "merge", "urlListId"),
    (
        "contents_keyword_lists",
        "ContentsKeywordListService/get",
        "merge",
        "contentsKeywordListId",
    ),
    ("retargeting_tags", "RetargetingTagService/get", "merge", "retargetingTagId"),
    ("account_authority", "AccountAuthorityService/get", "replace", "accountId"),
    ("account_tracking_urls", "AccountTrackingUrlService/get", "replace", "accountId"),
    ("ab_tests", "AbTestService/get", "merge", "abTestId"),
    ("brand_lift", "BrandLiftService/get", "merge", "brandLiftId"),
    ("guaranteed_campaigns", "GuaranteedCampaignService/get", "merge", "campaignId"),
    ("guaranteed_ad_groups", "GuaranteedAdGroupService/get", "merge", "adGroupId"),
    ("guaranteed_ads", "GuaranteedAdGroupAdService/get", "merge", "adId"),
    ("balance", "BalanceService/get", "replace", "accountId"),
    ("budget_orders", "BudgetOrderService/get", "merge", "budgetOrderId"),
    ("account_links", "AccountLinkService/get", "replace", "accountId"),
    ("app_links", "AppLinkService/get", "merge", "linkId"),
]

# Report types available in Display Ads (API v19 ReportDefinitionServiceReportType)
REPORT_TYPES = [
    "AD",
    "APP",
    "AUDIENCE_LIST_TARGET",
    "CONTENT_KEYWORD_LIST",
    "CONVERSION_PATH",
    "CROSS_CAMPAIGN_REACHES",
    "LABEL",
    "MODEL_COMPARISON",
    "PLACEMENT_TARGET",
    "PORTFOLIO_BIDDING",
    "REACH",
    "SEARCH_TARGET",
    "CAMPAIGN_BUDGET",
    "URL",
]

# Default report fields per report type
REPORT_FIELDS = {
    "AD": [
        "DAY",
        "ACCOUNT_ID",
        "CAMPAIGN_ID",
        "CAMPAIGN_NAME",
        "ADGROUP_ID",
        "ADGROUP_NAME",
        "AD_ID",
        "AD_NAME",
        "AD_TYPE",
        "IMPS",
        "CLICKS",
        "CLICK_RATE",
        "AVG_CPC",
        "COST",
        "CONVERSIONS",
        "CONV_RATE",
        "CONV_VALUE",
    ],
    "PLACEMENT_TARGET": [
        "DAY",
        "ACCOUNT_ID",
        "CAMPAIGN_ID",
        "CAMPAIGN_NAME",
        "ADGROUP_ID",
        "ADGROUP_NAME",
        "PLACEMENT_URL_LIST_NAME",
        "PLACEMENT_URL_LIST_TYPE",
        "IMPS",
        "CLICKS",
        "CLICK_RATE",
        "AVG_CPC",
        "COST",
        "CONVERSIONS",
        "CONV_RATE",
    ],
}


def _make_entity_resource(
    name: str,
    path: str,
    disposition: str,
    pk: str,
    access_token: str,
    account_id: str,
    base_url: str,
) -> dlt.sources.DltResource:
    """Create a single dlt resource for an entity endpoint."""
    url = f"{base_url}/{path}"

    @dlt.resource(name=name, write_disposition=disposition, primary_key=pk)
    def _fetch():
        client = make_client(access_token, account_id)
        yield from safe_get_entities(client, url, account_id)

    return _fetch


def _build_entity_resources(
    access_token: str,
    account_id: str,
    base_url: str,
) -> list[dlt.sources.DltResource]:
    """Create dlt resources for all entity endpoints."""
    return [
        _make_entity_resource(
            name, path, disposition, pk, access_token, account_id, base_url
        )
        for name, path, disposition, pk in _ENTITY_RESOURCES
    ]


@dlt.source(name="yahoo_ads_display")
def yahoo_ads_display_source(
    client_id: str = dlt.secrets.value,
    client_secret: str = dlt.secrets.value,
    refresh_token: str = dlt.secrets.value,
    account_id: str = dlt.config.value,
    report_type: str = "AD",
    report_fields: Optional[list[str]] = None,
    attribution_window_days: int = 7,
    resources: Optional[list[str]] = None,
    start_date: Optional[str] = None,
    base_url: str = BASE_URL,
):
    """Yahoo Ads Display (YDA) source.

    Includes LINE placement data (YDA serves ads on LINE surfaces).
    Use PLACEMENT_TARGET report type to identify LINE placements.

    Args:
        client_id: Yahoo Ads API client ID.
        client_secret: Yahoo Ads API client secret.
        refresh_token: OAuth refresh token.
        account_id: Yahoo Ads account ID.
        report_type: Report type (AD, PLACEMENT_TARGET, etc.).
        report_fields: Custom report fields. Defaults per report type.
        attribution_window_days: Days to re-fetch for attribution window.
        resources: Resource names to load. None for all.
        start_date: Override incremental start date (YYYY-MM-DD).
        base_url: API base URL override.
    """
    tokens = refresh_access_token(client_id, client_secret, refresh_token)
    access_token = tokens["access_token"]

    all_resources = _build_entity_resources(access_token, account_id, base_url)

    # Report resource
    fields = report_fields or REPORT_FIELDS.get(report_type, REPORT_FIELDS["AD"])
    pk = derive_primary_key(fields)
    has_day = "DAY" in fields
    initial = start_date or "2020-01-01"

    if has_day:

        @dlt.resource(
            name="report",
            write_disposition="merge",
            primary_key=pk,
            columns={"DAY": {"data_type": "date"}},
        )
        def _report(
            last_date=dlt.sources.incremental("DAY", initial_value=initial),
        ):
            client = make_client(access_token, account_id)
            last = last_date.last_value
            window_start = date.fromisoformat(last) - timedelta(
                days=attribution_window_days
            )
            start = window_start.isoformat()
            end = (date.today() - timedelta(days=1)).isoformat()

            if start > end:
                logger.info("report: already up to date")
                return

            logger.info("report: %s from %s to %s", report_type, start, end)

            job_id = submit_report(
                client, base_url, account_id, report_type, fields, start, end
            )
            if not job_id:
                logger.warning("report: no job ID returned")
                return

            status = poll_report(client, base_url, account_id, job_id)
            if not status:
                return

            for row in download_report(client, base_url, account_id, job_id):
                yield convert_report_types(row)

    else:

        @dlt.resource(
            name="report",
            write_disposition="replace",
            primary_key=pk,
        )
        def _report():
            client = make_client(access_token, account_id)
            start = "2020-01-01"
            end = (date.today() - timedelta(days=1)).isoformat()

            logger.info("report: %s (no DAY field, full replace)", report_type)

            job_id = submit_report(
                client, base_url, account_id, report_type, fields, start, end
            )
            if not job_id:
                logger.warning("report: no job ID returned")
                return

            status = poll_report(client, base_url, account_id, job_id)
            if not status:
                return

            for row in download_report(client, base_url, account_id, job_id):
                yield convert_report_types(row)

    all_resources.append(_report)

    if resources:
        all_resources = [r for r in all_resources if r.name in resources]

    return all_resources

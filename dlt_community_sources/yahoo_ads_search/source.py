"""Yahoo Ads Search (SS) dlt source.

API: https://ads-search.yahooapis.jp/api/v19
Docs: https://ads-developers.yahoo.co.jp/reference/ads-search-api/
SDK: https://github.com/yahoojp-marketing/ads-search-api-java-lib

All endpoints use POST RPC style. Pagination via startIndex/numberResults.
"""

import logging
from datetime import date, timedelta
from typing import Optional

import dlt

from dlt_community_sources.yahoo_ads_common.auth import refresh_access_token
from dlt_community_sources.yahoo_ads_common.helpers import (
    download_report,
    make_client,
    poll_report,
    safe_get_entities,
    submit_report,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://ads-search.yahooapis.jp/api/v19"

# ---------------------------------------------------------------------------
# Simple entity resources (all follow the same pattern)
# ---------------------------------------------------------------------------

_ENTITY_RESOURCES = [
    # (resource_name, service_path, write_disposition, primary_key)
    ("accounts", "AccountService/get", "merge", "accountId"),
    ("campaigns", "CampaignService/get", "merge", "campaignId"),
    ("ad_groups", "AdGroupService/get", "merge", "adGroupId"),
    ("ads", "AdGroupAdService/get", "merge", "adId"),
    ("ad_group_criterions", "AdGroupCriterionService/get", "merge", "criterionId"),
    ("campaign_criterions", "CampaignCriterionService/get", "merge", "criterionId"),
    ("bidding_strategies", "BiddingStrategyService/get", "merge", "biddingStrategyId"),
    ("campaign_budgets", "CampaignBudgetService/get", "merge", "budgetId"),
    ("labels", "LabelService/get", "merge", "labelId"),
    ("assets", "AssetService/get", "merge", "assetId"),
    ("audience_lists", "AudienceListService/get", "merge", "audienceListId"),
    ("conversion_trackers", "ConversionTrackerService/get", "merge", "conversionTrackerId"),
    ("account_shared", "AccountSharedService/get", "merge", "sharedListId"),
    ("ad_group_bid_multipliers", "AdGroupBidMultiplierService/get", "replace", "adGroupId"),
    ("campaign_targets", "CampaignTargetService/get", "replace", "targetId"),
    ("page_feed_asset_sets", "PageFeedAssetSetService/get", "merge", "pageFeedAssetSetId"),
    ("account_assets", "AccountAssetService/get", "merge", "assetId"),
    ("campaign_assets", "CampaignAssetService/get", "merge", "assetId"),
    ("ad_group_assets", "AdGroupAssetService/get", "merge", "assetId"),
    ("customizer_attributes", "CustomizerAttributeService/get", "merge", "customizerAttributeId"),
    ("account_tracking_urls", "AccountTrackingUrlService/get", "replace", "accountId"),
    ("ab_tests", "AbTestService/get", "merge", "abTestId"),
    ("seasonality_adjustments", "BiddingSeasonalityAdjustmentService/get", "merge", "biddingSeasonalityAdjustmentId"),
    ("learning_data_exclusions", "LearningDataExclusionService/get", "merge", "learningDataExclusionId"),
    ("conversion_groups", "ConversionGroupService/get", "merge", "conversionGroupId"),
    ("campaign_audience_lists", "CampaignAudienceListService/get", "replace", "campaignId"),
    ("ad_group_audience_lists", "AdGroupAudienceListService/get", "replace", "adGroupId"),
]

# Report types available in Search Ads
REPORT_TYPES = [
    "ACCOUNT",
    "CAMPAIGN",
    "ADGROUP",
    "AD",
    "KEYWORDS",
    "SEARCH_QUERY",
    "GEO",
    "FEED_ITEM",
    "GEO_TARGET",
    "SCHEDULE_TARGET",
    "DEVICE_TARGET",
    "AD_CUSTOMIZERS",
    "BID_STRATEGY",
    "TARGET_LIST",
    "LANDING_PAGE_URL",
    "KEYWORDLESS_QUERY",
]

# Default report fields per report type
REPORT_FIELDS = {
    "CAMPAIGN": [
        "DAY",
        "ACCOUNT_ID",
        "CAMPAIGN_ID",
        "CAMPAIGN_NAME",
        "CAMPAIGN_STATUS",
        "IMPS",
        "CLICKS",
        "CLICK_RATE",
        "AVG_CPC",
        "COST",
        "CONVERSIONS",
        "CONV_RATE",
        "CONV_VALUE",
    ],
    "ADGROUP": [
        "DAY",
        "ACCOUNT_ID",
        "CAMPAIGN_ID",
        "CAMPAIGN_NAME",
        "ADGROUP_ID",
        "ADGROUP_NAME",
        "ADGROUP_STATUS",
        "IMPS",
        "CLICKS",
        "CLICK_RATE",
        "AVG_CPC",
        "COST",
        "CONVERSIONS",
        "CONV_RATE",
        "CONV_VALUE",
    ],
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
    "KEYWORDS": [
        "DAY",
        "ACCOUNT_ID",
        "CAMPAIGN_ID",
        "CAMPAIGN_NAME",
        "ADGROUP_ID",
        "ADGROUP_NAME",
        "KEYWORD_ID",
        "KEYWORD",
        "KEYWORD_MATCH_TYPE",
        "IMPS",
        "CLICKS",
        "CLICK_RATE",
        "AVG_CPC",
        "COST",
        "CONVERSIONS",
        "CONV_RATE",
        "QUALITY_INDEX",
    ],
}

# Numeric fields for type conversion
REPORT_INT_FIELDS = {"IMPS", "CLICKS", "CONVERSIONS"}
REPORT_FLOAT_FIELDS = {"CLICK_RATE", "AVG_CPC", "COST", "CONV_RATE", "CONV_VALUE"}


def _convert_report_types(row: dict) -> dict:
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
                result[k] = float(v.replace(",", ""))
            except (ValueError, AttributeError):
                result[k] = v
        else:
            result[k] = v
    return result


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
        _make_entity_resource(name, path, disposition, pk, access_token, account_id, base_url)
        for name, path, disposition, pk in _ENTITY_RESOURCES
    ]


@dlt.source(name="yahoo_ads_search")
def yahoo_ads_search_source(
    client_id: str,
    client_secret: str,
    refresh_token: str,
    account_id: str,
    report_type: str = "CAMPAIGN",
    report_fields: Optional[list[str]] = None,
    attribution_window_days: int = 7,
    resources: Optional[list[str]] = None,
    start_date: Optional[str] = None,
    base_url: str = BASE_URL,
):
    """Yahoo Ads Search (SS) source.

    Args:
        client_id: Yahoo Ads API client ID.
        client_secret: Yahoo Ads API client secret.
        refresh_token: OAuth refresh token.
        account_id: Yahoo Ads account ID.
        report_type: Report type (CAMPAIGN, ADGROUP, AD, KEYWORDS, etc.).
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
    fields = report_fields or REPORT_FIELDS.get(
        report_type, REPORT_FIELDS["CAMPAIGN"]
    )

    @dlt.resource(name="report", write_disposition="merge", primary_key="DAY")
    def _report(
        last_date=dlt.sources.incremental("DAY", initial_value="2020-01-01"),
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
            yield _convert_report_types(row)

    all_resources.append(_report)

    if resources:
        all_resources = [r for r in all_resources if r.name in resources]

    return all_resources

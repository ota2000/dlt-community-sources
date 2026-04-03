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
    convert_report_types,
    derive_primary_key,
    discover_accounts,
    download_report,
    get_report_fields,
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
    (
        "conversion_trackers",
        "ConversionTrackerService/get",
        "merge",
        "conversionTrackerId",
    ),
    ("account_shared", "AccountSharedService/get", "merge", "sharedListId"),
    (
        "ad_group_bid_multipliers",
        "AdGroupBidMultiplierService/get",
        "replace",
        "adGroupId",
    ),
    ("campaign_targets", "CampaignTargetService/get", "replace", "targetId"),
    (
        "page_feed_asset_sets",
        "PageFeedAssetSetService/get",
        "merge",
        "pageFeedAssetSetId",
    ),
    ("account_assets", "AccountAssetService/get", "merge", "assetId"),
    ("campaign_assets", "CampaignAssetService/get", "merge", "assetId"),
    ("ad_group_assets", "AdGroupAssetService/get", "merge", "assetId"),
    (
        "customizer_attributes",
        "CustomizerAttributeService/get",
        "merge",
        "customizerAttributeId",
    ),
    ("account_tracking_urls", "AccountTrackingUrlService/get", "replace", "accountId"),
    ("ab_tests", "AbTestService/get", "merge", "abTestId"),
    (
        "seasonality_adjustments",
        "BiddingSeasonalityAdjustmentService/get",
        "merge",
        "biddingSeasonalityAdjustmentId",
    ),
    (
        "learning_data_exclusions",
        "LearningDataExclusionService/get",
        "merge",
        "learningDataExclusionId",
    ),
    ("conversion_groups", "ConversionGroupService/get", "merge", "conversionGroupId"),
    (
        "campaign_audience_lists",
        "CampaignAudienceListService/get",
        "replace",
        "campaignId",
    ),
    (
        "ad_group_audience_lists",
        "AdGroupAudienceListService/get",
        "replace",
        "adGroupId",
    ),
    ("balance", "BalanceService/get", "replace", "accountId"),
    ("budget_orders", "BudgetOrderService/get", "replace", "accountId"),
    (
        "shared_criterions",
        "SharedCriterionService/get",
        "merge",
        "criterionId",
    ),
    (
        "campaign_shared_sets",
        "CampaignSharedSetService/get",
        "replace",
        "campaignId",
    ),
    ("page_feed_assets", "PageFeedAssetService/get", "merge", "pageFeedAssetId"),
    ("ad_group_webpages", "AdGroupWebpageService/get", "replace", "adGroupId"),
    ("campaign_webpages", "CampaignWebpageService/get", "replace", "campaignId"),
    ("account_links", "AccountLinkService/get", "replace", "accountId"),
    ("app_links", "AppLinkService/get", "merge", "appLinkId"),
    (
        "account_customizers",
        "AccountCustomizerService/get",
        "replace",
        "customizerAttributeId",
    ),
    (
        "campaign_customizers",
        "CampaignCustomizerService/get",
        "replace",
        "customizerAttributeId",
    ),
    (
        "ad_group_customizers",
        "AdGroupCustomizerService/get",
        "replace",
        "customizerAttributeId",
    ),
    (
        "ad_group_criterion_customizers",
        "AdGroupCriterionCustomizerService/get",
        "replace",
        "criterionId",
    ),
]

# Report types available in Search Ads (API v19 ReportDefinitionServiceReportType)
REPORT_TYPES = [
    "ACCOUNT",
    "CAMPAIGN",
    "ADGROUP",
    "AD",
    "KEYWORDS",
    "SEARCH_QUERY",
    "GEO",
    "GEO_TARGET",
    "SCHEDULE_TARGET",
    "BID_STRATEGY",
    "CAMPAIGN_TARGET_LIST",
    "ADGROUP_TARGET_LIST",
    "LANDING_PAGE_URL",
    "KEYWORDLESS_QUERY",
    "WEBPAGE_CRITERION",
    "BID_MODIFIER",
    "CAMPAIGN_ASSET",
    "ADGROUP_ASSET",
    "ACCOUNT_ASSET",
    "RESPONSIVE_ADS_FOR_SEARCH_ASSET",
    "ASSET_COMBINATIONS",
    "CAMPAIGN_BUDGET",
]


def _make_entity_resource(
    name: str,
    path: str,
    disposition: str,
    pk: str,
    access_token: str,
    account_ids: list[str],
    base_account_id: str,
    base_url: str,
) -> dlt.sources.DltResource:
    """Create a single dlt resource for an entity endpoint."""
    url = f"{base_url}/{path}"

    @dlt.resource(name=name, write_disposition=disposition, primary_key=pk)
    def _fetch():
        client = make_client(access_token, base_account_id)
        for aid in account_ids:
            yield from safe_get_entities(client, url, aid)

    return _fetch


def _build_entity_resources(
    access_token: str,
    account_ids: list[str],
    base_account_id: str,
    base_url: str,
) -> list[dlt.sources.DltResource]:
    """Create dlt resources for all entity endpoints."""
    return [
        _make_entity_resource(
            name,
            path,
            disposition,
            pk,
            access_token,
            account_ids,
            base_account_id,
            base_url,
        )
        for name, path, disposition, pk in _ENTITY_RESOURCES
    ]


@dlt.source(name="yahoo_ads_search")
def yahoo_ads_search_source(
    client_id: str = dlt.secrets.value,
    client_secret: str = dlt.secrets.value,
    refresh_token: str = dlt.secrets.value,
    base_account_id: str = dlt.config.value,
    account_id: Optional[str] = None,
    report_type: str = "CAMPAIGN",
    report_fields: Optional[list[str]] = None,
    report_language: str = "EN",
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
        base_account_id: MCC account ID (used in x-z-base-account-id header).
        account_id: Child account ID. If None, auto-discovers all SERVING
            accounts under the MCC via AccountService/get.
        report_type: Report type (CAMPAIGN, ADGROUP, AD, KEYWORDS, etc.).
        report_fields: Custom report fields. If omitted, all available fields
            are fetched dynamically via getReportFields API.
        report_language: Report language (EN or JA). Defaults to EN.
        attribution_window_days: Days to re-fetch for attribution window.
        resources: Resource names to load. None for all.
        start_date: Override incremental start date (YYYY-MM-DD).
        base_url: API base URL override.
    """
    tokens = refresh_access_token(client_id, client_secret, refresh_token)
    access_token = tokens["access_token"]

    client = make_client(access_token, base_account_id)
    if account_id:
        account_ids = [account_id]
    else:
        account_ids = discover_accounts(client, base_url)

    all_resources = _build_entity_resources(
        access_token, account_ids, base_account_id, base_url
    )

    # Report resource
    if report_fields:
        fields = report_fields
    else:
        # Dynamically fetch all available fields from the API
        fields = get_report_fields(client, base_url, report_type)
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
            rpt_client = make_client(access_token, base_account_id)
            last = last_date.last_value
            window_start = date.fromisoformat(last) - timedelta(
                days=attribution_window_days
            )
            start = window_start.isoformat()
            end = (date.today() - timedelta(days=1)).isoformat()

            if start > end:
                logger.info("report: already up to date")
                return

            for aid in account_ids:
                logger.info(
                    "report: %s account=%s from %s to %s",
                    report_type,
                    aid,
                    start,
                    end,
                )

                job_id = submit_report(
                    rpt_client,
                    base_url,
                    aid,
                    report_type,
                    fields,
                    start,
                    end,
                    report_language=report_language,
                )
                if not job_id:
                    logger.warning("report: no job ID returned for account %s", aid)
                    continue

                status = poll_report(rpt_client, base_url, aid, job_id)
                if not status:
                    continue

                for row in download_report(rpt_client, base_url, aid, job_id):
                    yield convert_report_types(row)

    else:

        @dlt.resource(
            name="report",
            write_disposition="replace",
            primary_key=pk,
        )
        def _report():
            rpt_client = make_client(access_token, base_account_id)
            start = "2020-01-01"
            end = (date.today() - timedelta(days=1)).isoformat()

            for aid in account_ids:
                logger.info(
                    "report: %s account=%s (no DAY field, full replace)",
                    report_type,
                    aid,
                )

                job_id = submit_report(
                    rpt_client,
                    base_url,
                    aid,
                    report_type,
                    fields,
                    start,
                    end,
                    report_language=report_language,
                )
                if not job_id:
                    logger.warning("report: no job ID returned for account %s", aid)
                    continue

                status = poll_report(rpt_client, base_url, aid, job_id)
                if not status:
                    continue

                for row in download_report(rpt_client, base_url, aid, job_id):
                    yield convert_report_types(row)

    all_resources.append(_report)

    if resources:
        all_resources = [r for r in all_resources if r.name in resources]

    return all_resources

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

from dlt_community_sources._utils import wrap_resources_safe
from dlt_community_sources.yahoo_ads_common.auth import refresh_access_token
from dlt_community_sources.yahoo_ads_common.helpers import (
    convert_report_types,
    derive_primary_key,
    download_report,
    get_report_fields_with_types,
    make_client,
    poll_report,
    safe_fetch_entities,
    submit_report,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://ads-search.yahooapis.jp/api/v19"

# ---------------------------------------------------------------------------
# Simple entity resources (all follow the same pattern)
# ---------------------------------------------------------------------------

_ENTITY_RESOURCES = [
    # (resource_name, service_path, write_disposition, primary_key, body_style)
    # body_style "standard": accountId + startIndex + numberResults (most services)
    # body_style "account_ids": accountIds array, no paging
    # body_style "no_paging": accountId only, no paging params
    # body_style "empty": no body params (MCC-level services)
    ("accounts", "AccountService/get", "merge", "accountId", "empty"),
    ("campaigns", "CampaignService/get", "merge", "campaignId", "standard"),
    ("ad_groups", "AdGroupService/get", "merge", "adGroupId", "standard"),
    ("ads", "AdGroupAdService/get", "merge", "adId", "standard"),
    (
        "ad_group_criterions",
        "AdGroupCriterionService/get",
        "merge",
        "criterionId",
        "standard",
    ),
    (
        "campaign_criterions",
        "CampaignCriterionService/get",
        "merge",
        "criterionId",
        "standard",
    ),
    (
        "bidding_strategies",
        "BiddingStrategyService/get",
        "merge",
        "portfolioBiddingId",
        "standard",
    ),
    ("campaign_budgets", "CampaignBudgetService/get", "merge", "budgetId", "standard"),
    ("labels", "LabelService/get", "merge", "labelId", "standard"),
    ("assets", "AssetService/get", "merge", "assetId", "standard"),
    (
        "audience_lists",
        "AudienceListService/get",
        "merge",
        "audienceListId",
        "standard",
    ),
    (
        "conversion_trackers",
        "ConversionTrackerService/get",
        "merge",
        "conversionTrackerId",
        "standard",
    ),
    ("account_shared", "AccountSharedService/get", "merge", "sharedListId", "standard"),
    (
        "ad_group_bid_multipliers",
        "AdGroupBidMultiplierService/get",
        "merge",
        ["accountId", "adGroupId"],
        "standard",
    ),
    (
        "campaign_targets",
        "CampaignTargetService/get",
        "merge",
        ["accountId", "campaignId"],
        "standard",
    ),
    (
        "page_feed_asset_sets",
        "PageFeedAssetSetService/get",
        "merge",
        "pageFeedAssetSetId",
        "standard",
    ),
    ("account_assets", "AccountAssetService/get", "merge", "assetId", "standard"),
    ("campaign_assets", "CampaignAssetService/get", "merge", "assetId", "standard"),
    ("ad_group_assets", "AdGroupAssetService/get", "merge", "assetId", "standard"),
    (
        "customizer_attributes",
        "CustomizerAttributeService/get",
        "merge",
        "customizerAttributeId",
        "no_paging",
    ),
    (
        "account_tracking_urls",
        "AccountTrackingUrlService/get",
        "merge",
        "accountId",
        "account_ids",
    ),
    ("ab_tests", "AbTestService/get", "merge", "abTestId", "standard"),
    ("audit_logs", "AuditLogService/get", "append", "updateDateTime", "standard"),
    (
        "seasonality_adjustments",
        "BiddingSeasonalityAdjustmentService/get",
        "merge",
        "biddingSeasonalityAdjustmentId",
        "standard",
    ),
    (
        "learning_data_exclusions",
        "LearningDataExclusionService/get",
        "merge",
        "learningDataExclusionId",
        "standard",
    ),
    (
        "conversion_groups",
        "ConversionGroupService/get",
        "merge",
        "conversionGroupId",
        "standard",
    ),
    (
        "campaign_audience_lists",
        "CampaignAudienceListService/get",
        "merge",
        ["accountId", "campaignId"],
        "standard",
    ),
    (
        "ad_group_audience_lists",
        "AdGroupAudienceListService/get",
        "merge",
        ["accountId", "adGroupId"],
        "standard",
    ),
    ("balance", "BalanceService/get", "merge", "accountId", "account_ids"),
    ("budget_orders", "BudgetOrderService/get", "merge", "accountId", "account_ids"),
    (
        "shared_criterions",
        "SharedCriterionService/get",
        "merge",
        "criterionId",
        "standard",
    ),
    (
        "campaign_shared_sets",
        "CampaignSharedSetService/get",
        "merge",
        ["accountId", "campaignId"],
        "standard",
    ),
    (
        "page_feed_assets",
        "PageFeedAssetService/get",
        "merge",
        "pageFeedAssetId",
        "standard",
    ),
    (
        "ad_group_webpages",
        "AdGroupWebpageService/get",
        "merge",
        ["accountId", "adGroupId"],
        "standard",
    ),
    (
        "campaign_webpages",
        "CampaignWebpageService/get",
        "merge",
        ["accountId", "campaignId"],
        "standard",
    ),
    ("account_links", "AccountLinkService/get", "merge", "accountId", "empty"),
    ("app_links", "AppLinkService/get", "merge", "appLinkId", "standard"),
    (
        "account_customizers",
        "AccountCustomizerService/get",
        "merge",
        ["accountId", "customizerAttributeId"],
        "standard",
    ),
    (
        "campaign_customizers",
        "CampaignCustomizerService/get",
        "merge",
        ["accountId", "customizerAttributeId"],
        "standard",
    ),
    (
        "ad_group_customizers",
        "AdGroupCustomizerService/get",
        "merge",
        ["accountId", "customizerAttributeId"],
        "standard",
    ),
    (
        "ad_group_criterion_customizers",
        "AdGroupCriterionCustomizerService/get",
        "merge",
        ["accountId", "criterionId"],
        "standard",
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
    body_style: str,
    access_token: str,
    account_ids: list[str],
    base_account_id: str,
    base_url: str,
) -> dlt.sources.DltResource:
    """Create a single dlt resource for an entity endpoint."""
    url = f"{base_url}/{path}"

    # pk can be a string or list of strings
    pk_fields = [pk] if isinstance(pk, str) else pk

    @dlt.resource(name=name, write_disposition=disposition, primary_key=pk)
    def _fetch():
        client = make_client(access_token, base_account_id)
        for aid in account_ids:
            for row in safe_fetch_entities(client, url, aid, body_style):
                # Skip rows where primary key fields are null
                if all(row.get(k) is not None for k in pk_fields):
                    yield row

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
            body_style,
            access_token,
            account_ids,
            base_account_id,
            base_url,
        )
        for name, path, disposition, pk, body_style in _ENTITY_RESOURCES
    ]


@dlt.source(name="yahoo_ads_search")
def yahoo_ads_search_source(
    client_id: str = dlt.secrets.value,
    client_secret: str = dlt.secrets.value,
    refresh_token: str = dlt.secrets.value,
    base_account_id: str = dlt.config.value,
    account_id: str = dlt.config.value,
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
        base_account_id: MCC account ID (used in x-z-base-account-id header).
        account_id: Child account ID to load data from.
        report_type: Report type (CAMPAIGN, ADGROUP, AD, KEYWORDS, etc.).
        report_fields: Custom report fields. If omitted, all available fields
            are fetched dynamically via getReportFields API.
        attribution_window_days: Days to re-fetch for attribution window.
        resources: Resource names to load. None for all.
        start_date: Override incremental start date (YYYY-MM-DD).
        base_url: API base URL override.
    """
    tokens = refresh_access_token(client_id, client_secret, refresh_token)
    access_token = tokens["access_token"]

    account_ids = [account_id]
    client = make_client(access_token, base_account_id)

    all_resources = _build_entity_resources(
        access_token, account_ids, base_account_id, base_url
    )

    # Report resource
    if report_fields:
        fields = report_fields
        field_type_map = None
        display_to_field = None
    else:
        # Dynamically fetch all available fields and types from the API
        meta = get_report_fields_with_types(
            client, base_url, report_type, report_language="EN"
        )
        fields = meta.field_names
        field_type_map = meta.field_type_map
        display_to_field = meta.display_to_field
    pk = derive_primary_key(fields)
    has_day = "DAY" in fields
    initial = start_date or "2020-01-01"

    def _fetch_report(rpt_client, start, end):
        """Fetch report rows for all accounts in the given date range."""
        for aid in account_ids:
            job_id = submit_report(
                rpt_client,
                base_url,
                aid,
                report_type,
                fields,
                start,
                end,
                report_language="EN",
            )
            if not job_id:
                logger.warning("report: no job ID returned for account %s", aid)
                continue

            status = poll_report(rpt_client, base_url, aid, job_id)
            if not status:
                continue

            for row in download_report(
                rpt_client, base_url, aid, job_id, display_to_field
            ):
                yield convert_report_types(row, field_type_map)

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

            logger.info(
                "report: %s from %s to %s (%d accounts)",
                report_type,
                start,
                end,
                len(account_ids),
            )
            yield from _fetch_report(rpt_client, start, end)

    else:

        @dlt.resource(
            name="report",
            write_disposition="merge",
            primary_key=pk,
        )
        def _report():
            rpt_client = make_client(access_token, base_account_id)
            start = "2020-01-01"
            end = (date.today() - timedelta(days=1)).isoformat()

            logger.info(
                "report: %s (no DAY field, full replace, %d accounts)",
                report_type,
                len(account_ids),
            )
            yield from _fetch_report(rpt_client, start, end)

    all_resources.append(_report)

    all_resources = wrap_resources_safe(all_resources)

    if resources:
        all_resources = [r for r in all_resources if r.name in resources]

    return all_resources

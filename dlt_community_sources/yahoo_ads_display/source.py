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

from dlt_community_sources._utils import wrap_resources_safe
from dlt_community_sources.yahoo_ads_common.auth import refresh_access_token
from dlt_community_sources.yahoo_ads_common.helpers import (
    convert_report_types,
    derive_primary_key,
    discover_accounts,
    download_report,
    get_report_fields_with_types,
    make_client,
    poll_report,
    safe_fetch_entities,
    safe_get_entities,
    submit_report,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://ads-display.yahooapis.jp/api/v19"

# ---------------------------------------------------------------------------
# Simple entity resources
# ---------------------------------------------------------------------------

# (name, service_path, write_disposition, primary_key, body_style)
# body_style "standard": accountId + startIndex + numberResults (most services)
# body_style "account_ids": accountIds array, no paging
# body_style "no_paging": accountId only, no paging params
# body_style "empty": no body params (MCC-level services)
_ENTITY_RESOURCES = [
    ("accounts", "AccountService/get", "merge", "accountId", "empty"),
    ("campaigns", "CampaignService/get", "merge", "campaignId", "standard"),
    ("ad_groups", "AdGroupService/get", "merge", "adGroupId", "standard"),
    ("ads", "AdGroupAdService/get", "merge", "adId", "standard"),
    (
        "ad_group_targets",
        "AdGroupTargetService/get",
        "merge",
        ["accountId", "targetId"],
        "standard",
    ),
    ("labels", "LabelService/get", "merge", "labelId", "standard"),
    (
        "bidding_strategies",
        "BiddingStrategyService/get",
        "merge",
        "biddingStrategyId",
        "standard",
    ),
    ("campaign_budgets", "CampaignBudgetService/get", "merge", "budgetId", "standard"),
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
    (
        "conversion_groups",
        "ConversionGroupService/get",
        "merge",
        "conversionGroupId",
        "standard",
    ),
    ("media", "MediaService/get", "merge", "mediaId", "standard"),
    ("videos", "VideoService/get", "merge", "mediaId", "standard"),
    ("feeds", "FeedService/get", "merge", "feedId", "standard"),
    (
        "placement_url_lists",
        "PlacementUrlListService/get",
        "merge",
        "urlListId",
        "standard",
    ),
    (
        "contents_keyword_lists",
        "ContentsKeywordListService/get",
        "merge",
        "contentsKeywordListId",
        "standard",
    ),
    (
        "retargeting_tags",
        "RetargetingTagService/get",
        "merge",
        "retargetingTagId",
        "no_paging",
    ),
    (
        "account_authority",
        "AccountAuthorityService/get",
        "merge",
        "accountId",
        "account_ids",
    ),
    (
        "account_tracking_urls",
        "AccountTrackingUrlService/get",
        "merge",
        "accountId",
        "standard",
    ),
    ("ab_tests", "AbTestService/get", "merge", "abTestId", "no_paging"),
    ("brand_lift", "BrandLiftService/get", "merge", "brandLiftId", "standard"),
    (
        "guaranteed_campaigns",
        "GuaranteedCampaignService/get",
        "merge",
        "campaignId",
        "standard",
    ),
    (
        "guaranteed_ad_groups",
        "GuaranteedAdGroupService/get",
        "merge",
        "adGroupId",
        "standard",
    ),
    ("guaranteed_ads", "GuaranteedAdGroupAdService/get", "merge", "adId", "standard"),
    ("balance", "BalanceService/get", "merge", "accountId", "no_paging"),
    ("budget_orders", "BudgetOrderService/get", "merge", "accountId", "account_ids"),
    ("app_links", "AppLinkService/get", "merge", "linkId", "standard"),
    ("audit_logs", "AuditLogService/get", "append", "updateTimestamp", "standard"),
    ("tracking_tags", "TrackingTagService/get", "merge", "accountId", "no_paging"),
    ("base_accounts", "BaseAccountService/get", "merge", "accountId", "account_ids"),
    ("feed_data", "FeedDataService/get", "merge", "feedId", "standard"),
    ("feed_ftp", "FeedFtpService/get", "merge", "accountId", "no_paging"),
    (
        "offline_conversions",
        "OfflineConversionService/get",
        "append",
        "conversionTrackerId",
        "standard",
    ),
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


def _make_entity_resource(
    name: str,
    path: str,
    disposition: str,
    pk: str | list[str],
    body_style: str,
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
        if body_style == "empty":
            # MCC-level: fetch once, not per account
            yield from safe_fetch_entities(client, url, "", body_style=body_style)
        else:
            for aid in account_ids:
                yield from safe_fetch_entities(client, url, aid, body_style=body_style)

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


@dlt.source(name="yahoo_ads_display")
def yahoo_ads_display_source(
    client_id: str = dlt.secrets.value,
    client_secret: str = dlt.secrets.value,
    refresh_token: str = dlt.secrets.value,
    base_account_id: str = dlt.config.value,
    account_id: Optional[str] = None,
    report_type: str = "AD",
    report_fields: Optional[list[str]] = None,
    report_language: str = "EN",
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
        base_account_id: MCC account ID (used in x-z-base-account-id header).
        account_id: Child account ID. If None, auto-discovers all SERVING
            accounts under the MCC via AccountService/get.
        report_type: Report type (AD, PLACEMENT_TARGET, etc.).
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

    # AccountLinkService uses mccAccountId instead of accountId
    @dlt.resource(
        name="account_links", write_disposition="merge", primary_key="accountId"
    )
    def _account_links():
        try:
            link_client = make_client(access_token, base_account_id)
            url = f"{base_url}/AccountLinkService/get"
            resp = link_client.post(url, json={"mccAccountId": int(base_account_id)})
            resp.raise_for_status()
            data = resp.json()
            rval = data.get("rval") or {}
            for entry in rval.get("values") or []:
                if entry.get("operationSucceeded"):
                    yield entry.get("accountLink", {})
        except Exception as e:
            logger.warning("Skipping account_links: %s", e)

    # FeedSetService requires feedIds — fetch feeds first, then their sets
    @dlt.resource(name="feed_sets", write_disposition="merge", primary_key="feedSetId")
    def _feed_sets():
        fs_client = make_client(access_token, base_account_id)
        for aid in account_ids:
            feed_ids = []
            for feed in safe_fetch_entities(
                fs_client, f"{base_url}/FeedService/get", aid
            ):
                fid = feed.get("feedId")
                if fid:
                    feed_ids.append(int(fid))
            if not feed_ids:
                continue
            yield from safe_get_entities(
                fs_client,
                f"{base_url}/FeedSetService/get",
                aid,
                selector_fields={"feedIds": feed_ids},
            )

    # StatsService requires a `type` parameter — create one resource per stat type
    _STAT_TYPES = ["CAMPAIGN", "ADGROUP", "AD"]

    def _make_stats_resource(stat_type: str) -> dlt.sources.DltResource:
        rname = f"stats_{stat_type.lower()}"

        @dlt.resource(name=rname, write_disposition="replace")
        def _stats():
            stats_client = make_client(access_token, base_account_id)
            url = f"{base_url}/StatsService/get"
            for aid in account_ids:
                yield from safe_get_entities(
                    stats_client,
                    url,
                    aid,
                    selector_fields={"type": stat_type},
                )

        return _stats

    stats_resources = [_make_stats_resource(t) for t in _STAT_TYPES]

    all_resources = (
        [_account_links, _feed_sets]
        + stats_resources
        + _build_entity_resources(access_token, account_ids, base_account_id, base_url)
    )

    # Report resource
    if report_fields:
        fields = report_fields
        field_type_map = None
        display_to_field = None
    else:
        # Dynamically fetch all available fields and types from the API
        meta = get_report_fields_with_types(
            client, base_url, report_type, report_language=report_language
        )
        fields = meta.field_names
        field_type_map = meta.field_type_map
        display_to_field = meta.display_to_field
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

                for row in download_report(
                    rpt_client, base_url, aid, job_id, display_to_field
                ):
                    yield convert_report_types(row, field_type_map)

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

                for row in download_report(
                    rpt_client, base_url, aid, job_id, display_to_field
                ):
                    yield convert_report_types(row, field_type_map)

    all_resources.append(_report)

    all_resources = wrap_resources_safe(all_resources)

    if resources:
        all_resources = [r for r in all_resources if r.name in resources]

    return all_resources

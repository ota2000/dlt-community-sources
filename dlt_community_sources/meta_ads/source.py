"""dlt source for Meta (Facebook) Marketing API."""

import json
import logging
import time
from collections.abc import Generator
from decimal import Decimal
from typing import Optional, Sequence
from urllib.parse import urlencode

import dlt
from dlt.sources import DltResource
from dlt.sources.helpers import requests as req
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig

from dlt_community_sources._utils import wrap_resources_safe

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://graph.facebook.com/v25.0"
DEFAULT_FIELDS = {
    # SDK: facebook_business/adobjects/adaccount.py
    "ad_accounts": [
        "id",
        "name",
        "account_id",
        "account_status",
        "currency",
        "timezone_name",
        "business_name",
        "amount_spent",
        "balance",
        "spend_cap",
    ],
    # SDK: facebook_business/adobjects/adlabel.py
    "ad_labels": [
        "id",
        "name",
        "created_time",
        "updated_time",
    ],
    # SDK: facebook_business/adobjects/campaign.py
    "campaigns": [
        "id",
        "account_id",
        "adlabels",
        "bid_strategy",
        "boosted_object_id",
        "budget_rebalance_flag",
        "budget_remaining",
        "buying_type",
        "can_create_brand_lift_study",
        "can_use_spend_cap",
        "configured_status",
        "created_time",
        "daily_budget",
        "effective_status",
        "has_secondary_skadnetwork_reporting",
        "is_budget_schedule_enabled",
        "issues_info",
        "last_budget_toggling_time",
        "lifetime_budget",
        "name",
        "objective",
        "pacing_type",
        "promoted_object",
        "recommendations",
        "smart_promotion_type",
        "source_campaign",
        "source_campaign_id",
        "special_ad_categories",
        "special_ad_category",
        "special_ad_category_country",
        "spend_cap",
        "start_time",
        "status",
        "stop_time",
        "topline_id",
        "updated_time",
    ],
    # SDK: facebook_business/adobjects/adset.py
    "ad_sets": [
        "id",
        "account_id",
        "adlabels",
        "adset_schedule",
        "attribution_spec",
        "bid_adjustments",
        "bid_amount",
        "bid_constraints",
        "bid_info",
        "bid_strategy",
        "billing_event",
        "budget_remaining",
        "campaign",
        "campaign_active_time",
        "campaign_id",
        "configured_status",
        "created_time",
        "creative_sequence",
        "daily_budget",
        "daily_min_spend_target",
        "daily_spend_cap",
        "destination_type",
        "effective_status",
        "end_time",
        "existing_customer_budget_percentage",
        "frequency_control_specs",
        "instagram_user_id",
        "is_dynamic_creative",
        "issues_info",
        "learning_stage_info",
        "lifetime_budget",
        "lifetime_imps",
        "lifetime_min_spend_target",
        "lifetime_spend_cap",
        "multi_optimization_goal_weight",
        "name",
        "optimization_goal",
        "optimization_sub_event",
        "pacing_type",
        "promoted_object",
        "recommendations",
        "recurring_budget_semantics",
        "review_feedback",
        "rf_prediction_id",
        "source_adset",
        "source_adset_id",
        "start_time",
        "status",
        "targeting",
        "targeting_optimization_types",
        "updated_time",
        "use_new_app_click",
    ],
    # SDK: facebook_business/adobjects/ad.py
    "ads": [
        "id",
        "account_id",
        "ad_active_time",
        "ad_review_feedback",
        "ad_schedule_end_time",
        "ad_schedule_start_time",
        "adlabels",
        "adset",
        "adset_id",
        "bid_amount",
        "bid_info",
        "bid_type",
        "campaign",
        "campaign_id",
        "configured_status",
        "conversion_domain",
        "conversion_specs",
        "created_time",
        "creative",
        "demolink_hash",
        "display_sequence",
        "effective_status",
        "engagement_audience",
        "failed_delivery_checks",
        "issues_info",
        "last_updated_by_app_id",
        "name",
        "preview_shareable_link",
        "priority",
        "recommendations",
        "source_ad",
        "source_ad_id",
        "status",
        "targeting",
        "tracking_and_conversion_with_defaults",
        "tracking_specs",
        "updated_time",
    ],
    # SDK: facebook_business/adobjects/adcreative.py
    "ad_creatives": [
        "id",
        "account_id",
        "actor_id",
        "adlabels",
        "applink_treatment",
        "asset_feed_spec",
        "authorization_category",
        "body",
        "branded_content",
        "branded_content_sponsor_page_id",
        "call_to_action_type",
        "effective_authorization_category",
        "effective_instagram_media_id",
        "effective_object_story_id",
        "image_crops",
        "image_hash",
        "image_url",
        "instagram_permalink_url",
        "instagram_user_id",
        "link_deep_link_url",
        "link_destination_display_url",
        "link_og_id",
        "link_url",
        "name",
        "object_id",
        "object_store_url",
        "object_story_id",
        "object_story_spec",
        "object_type",
        "object_url",
        "platform_customizations",
        "playable_asset_id",
        "product_set_id",
        "source_instagram_media_id",
        "status",
        "template_url",
        "template_url_spec",
        "thumbnail_id",
        "thumbnail_url",
        "title",
        "url_tags",
        "video_id",
    ],
    # SDK: facebook_business/adobjects/lead.py
    "ad_leads": [
        "id",
        "ad_id",
        "ad_name",
        "adset_id",
        "adset_name",
        "campaign_id",
        "campaign_name",
        "created_time",
        "custom_disclaimer_responses",
        "field_data",
        "form_id",
        "home_listing",
        "is_organic",
        "partner_name",
        "platform",
        "post",
        "post_submission_check_result",
        "retailer_item_id",
    ],
    # SDK: facebook_business/adobjects/customaudience.py
    "custom_audiences": [
        "id",
        "account_id",
        "approximate_count_lower_bound",
        "approximate_count_upper_bound",
        "customer_file_source",
        "data_source",
        "data_source_types",
        "delete_time",
        "delivery_status",
        "description",
        "external_event_source",
        "household_audience",
        "is_value_based",
        "lookalike_audience_ids",
        "lookalike_spec",
        "name",
        "operation_status",
        "opt_out_link",
        "permission_for_actions",
        "pixel_id",
        "retention_days",
        "rule",
        "rule_aggregation",
        "sharing_status",
        "subtype",
        "time_content_updated",
        "time_created",
        "time_updated",
    ],
    # SDK: facebook_business/adobjects/customconversion.py (inferred)
    "custom_conversions": [
        "id",
        "account_id",
        "name",
        "description",
        "custom_event_type",
        "default_conversion_value",
        "is_archived",
        "pixel",
        "rule",
    ],
    # SDK: facebook_business/adobjects/adimage.py
    "ad_images": [
        "account_id",
        "created_time",
        "creatives",
        "hash",
        "height",
        "id",
        "is_associated_creatives_in_adgroups",
        "name",
        "original_height",
        "original_width",
        "permalink_url",
        "status",
        "updated_time",
        "url",
        "url_128",
        "width",
    ],
    # SDK: facebook_business/adobjects/advideo.py
    "ad_videos": [
        "id",
        "ad_breaks",
        "created_time",
        "custom_labels",
        "description",
        "embed_html",
        "embeddable",
        "format",
        "icon",
        "length",
        "permalink_url",
        "picture",
        "place",
        "post_id",
        "post_views",
        "privacy",
        "published",
        "source",
        "status",
        "title",
        "universal_video_id",
        "updated_time",
        "views",
    ],
    # Activities (no dedicated SDK model, fields from API docs)
    "activities": [
        "event_time",
        "event_type",
        "actor_id",
        "actor_name",
        "object_id",
        "object_name",
        "object_type",
        "extra_data",
        "date_time_in_timezone",
    ],
    # SDK: facebook_business/adobjects/savedaudience.py (inferred)
    "saved_audiences": [
        "id",
        "account",
        "approximate_count",
        "approximate_count_64bit",
        "description",
        "delete_time",
        "name",
        "operation_status",
        "permission_for_actions",
        "run_status",
        "sentence_lines",
        "targeting",
        "time_created",
        "time_updated",
    ],
}
# Insight metric fields requiring string-to-numeric conversion
INSIGHT_INT_FIELDS = {
    "impressions",
    "clicks",
    "reach",
    "unique_clicks",
    "inline_link_clicks",
}
INSIGHT_FLOAT_FIELDS = {
    "spend",
    "frequency",
    "cpc",
    "cpm",
    "ctr",
    "cpp",
    "cost_per_inline_link_click",
    "cost_per_inline_post_engagement",
    "cost_per_unique_click",
    "cost_per_unique_inline_link_click",
    "unique_ctr",
    "unique_inline_link_click_ctr",
    "cost_per_estimated_ad_recallers",
    "cost_per_thruplay",
    "cost_per_outbound_click",
    "cost_per_unique_outbound_click",
    "social_spend",
}
# SDK: facebook_business/adobjects/adsinsights.py
# Includes commonly available fields out of 170+ total.
# Pass custom list via insight_fields parameter for full coverage.
DEFAULT_INSIGHT_FIELDS = [
    # Date & identifiers
    "date_start",
    "date_stop",
    "account_id",
    "account_name",
    "campaign_id",
    "campaign_name",
    "adset_id",
    "adset_name",
    "ad_id",
    "ad_name",
    # Basic metrics
    "impressions",
    "clicks",
    "spend",
    "reach",
    "frequency",
    "cpc",
    "cpm",
    "ctr",
    "cpp",
    # Actions
    "actions",
    "action_values",
    "conversions",
    "conversion_values",
    "cost_per_action_type",
    "cost_per_conversion",
    "cost_per_result",
    "result_rate",
    # Unique metrics
    "unique_actions",
    "unique_clicks",
    "unique_ctr",
    "unique_conversions",
    "cost_per_unique_click",
    "cost_per_unique_action_type",
    "cost_per_unique_conversion",
    # Inline metrics
    "inline_link_clicks",
    "inline_link_click_ctr",
    "inline_post_engagement",
    "cost_per_inline_link_click",
    "cost_per_inline_post_engagement",
    "unique_inline_link_clicks",
    "unique_inline_link_click_ctr",
    "cost_per_unique_inline_link_click",
    # Outbound metrics
    "outbound_clicks",
    "outbound_clicks_ctr",
    "unique_outbound_clicks",
    "unique_outbound_clicks_ctr",
    "cost_per_outbound_click",
    "cost_per_unique_outbound_click",
    # Reach & estimation
    "estimated_ad_recallers",
    "estimated_ad_recall_rate",
    "cost_per_estimated_ad_recallers",
    "full_view_impressions",
    "full_view_reach",
    # Video
    "video_play_actions",
    "video_play_curve_actions",
    "video_thruplay_watched_actions",
    "video_avg_time_watched_actions",
    "video_p25_watched_actions",
    "video_p50_watched_actions",
    "video_p75_watched_actions",
    "video_p95_watched_actions",
    "video_p100_watched_actions",
    "video_15_sec_watched_actions",
    "video_30_sec_watched_actions",
    "video_continuous_2_sec_watched_actions",
    "unique_video_continuous_2_sec_watched_actions",
    "unique_video_view_15_sec",
    "cost_per_thruplay",
    "cost_per_15_sec_video_view",
    "cost_per_2_sec_continuous_video_view",
    # Canvas
    "canvas_avg_view_percent",
    "canvas_avg_view_time",
    # Catalog
    "catalog_segment_actions",
    "catalog_segment_value",
    # ROAS
    "purchase_roas",
    "mobile_app_purchase_roas",
    "website_purchase_roas",
    # Social
    "social_spend",
    # Ranking
    "quality_ranking",
    "engagement_rate_ranking",
    "conversion_rate_ranking",
    # Other
    "buying_type",
    "objective",
    "optimization_goal",
    "account_currency",
    "creative_media_type",
    "dda_countby_convs",
    "dda_results",
    "instant_experience_clicks_to_open",
    "instant_experience_clicks_to_start",
    "instant_experience_outbound_clicks",
]
# Polling interval and max wait time
POLL_INTERVAL_SECONDS = 10
POLL_MAX_WAIT_SECONDS = 600

INSIGHTS_PRIMARY_KEYS = {
    "account": ["date_start", "date_stop"],
    "campaign": ["date_start", "date_stop", "campaign_id"],
    "adset": ["date_start", "date_stop", "adset_id"],
    "ad": ["date_start", "date_stop", "ad_id"],
}


def _convert_insight_types(row: dict) -> dict:
    """Convert insight metric strings to numeric types in-place."""
    for field in INSIGHT_INT_FIELDS:
        if field in row and row[field] is not None:
            try:
                row[field] = int(row[field])
            except (ValueError, TypeError):
                pass
    for field in INSIGHT_FLOAT_FIELDS:
        if field in row and row[field] is not None:
            try:
                row[field] = Decimal(str(row[field]))
            except (ValueError, TypeError, ArithmeticError):
                pass
    return row


def _rest_api_config(
    access_token: str,
    account_id: str,
    base_url: str,
    custom_fields: Optional[dict[str, list[str]]] = None,
) -> RESTAPIConfig:
    """Build the REST API config for Meta Marketing API master data."""
    act_id = account_id if account_id.startswith("act_") else f"act_{account_id}"
    fields = {**DEFAULT_FIELDS, **(custom_fields or {})}

    return {
        "client": {
            "base_url": f"{base_url}/",
            "auth": {
                "type": "bearer",
                "token": access_token,
            },
            "paginator": {
                "type": "json_link",
                "next_url_path": "paging.next",
            },
        },
        "resource_defaults": {
            "primary_key": "id",
            "write_disposition": "merge",
            "endpoint": {
                "data_selector": "data",
                "response_actions": [
                    {"status_code": 400, "action": "ignore"},
                    {"status_code": 403, "action": "ignore"},
                    {"status_code": 404, "action": "ignore"},
                ],
            },
        },
        "resources": [
            {
                "name": "ad_accounts",
                "endpoint": {
                    "path": act_id,
                    "params": {
                        "fields": ",".join(fields["ad_accounts"]),
                    },
                    "data_selector": "$",
                },
            },
            {
                "name": "ad_labels",
                "endpoint": {
                    "path": f"{act_id}/adlabels",
                    "params": {
                        "fields": ",".join(fields["ad_labels"]),
                    },
                },
            },
            {
                "name": "campaigns",
                "endpoint": {
                    "path": f"{act_id}/campaigns",
                    "params": {
                        "fields": ",".join(fields["campaigns"]),
                    },
                },
            },
            {
                "name": "ad_sets",
                "endpoint": {
                    "path": f"{act_id}/adsets",
                    "params": {
                        "fields": ",".join(fields["ad_sets"]),
                    },
                },
            },
            {
                "name": "ads",
                "endpoint": {
                    "path": f"{act_id}/ads",
                    "params": {
                        "fields": ",".join(fields["ads"]),
                    },
                },
            },
            {
                "name": "ad_creatives",
                "endpoint": {
                    "path": f"{act_id}/adcreatives",
                    "params": {
                        "fields": ",".join(fields["ad_creatives"]),
                    },
                },
            },
            {
                "name": "custom_audiences",
                "endpoint": {
                    "path": f"{act_id}/customaudiences",
                    "params": {
                        "fields": ",".join(fields["custom_audiences"]),
                    },
                },
            },
            {
                "name": "custom_conversions",
                "endpoint": {
                    "path": f"{act_id}/customconversions",
                    "params": {
                        "fields": ",".join(fields["custom_conversions"]),
                    },
                },
            },
            {
                "name": "ad_images",
                "primary_key": "hash",
                "endpoint": {
                    "path": f"{act_id}/adimages",
                    "params": {
                        "fields": ",".join(fields["ad_images"]),
                    },
                },
            },
            {
                "name": "ad_videos",
                "columns": {"length": {"data_type": "double"}},
                "endpoint": {
                    "path": f"{act_id}/advideos",
                    "params": {
                        "fields": ",".join(fields["ad_videos"]),
                    },
                },
            },
            {
                "name": "activities",
                "primary_key": "",
                "write_disposition": "append",
                "endpoint": {
                    "path": f"{act_id}/activities",
                    "params": {
                        "fields": ",".join(fields["activities"]),
                    },
                },
            },
            {
                "name": "saved_audiences",
                "endpoint": {
                    "path": f"{act_id}/saved_audiences",
                    "params": {
                        "fields": ",".join(fields["saved_audiences"]),
                    },
                },
            },
        ],
    }


def discover_accounts(
    access_token: str,
    base_url: str = DEFAULT_BASE_URL,
) -> list[str]:
    """Discover all active ad accounts accessible by the token.

    Returns a list of account IDs (e.g., ["act_123", "act_456"]) that
    can be passed to ``meta_ads_source(account_id=...)``.

    Only accounts with account_status=1 (ACTIVE) are returned.
    To access disabled accounts, pass their account_id directly.
    """
    client = _make_client(access_token)
    accounts = []
    url = f"{base_url}/me/adaccounts?fields=id,account_status"
    for account in _get_paginated(client, url):
        if account.get("account_status") == 1:
            accounts.append(account["id"])
    return accounts


def _make_client(access_token: str) -> req.Client:
    """Create a dlt HTTP client with bearer auth."""
    client = req.Client()
    client.session.headers["Authorization"] = f"Bearer {access_token}"
    return client


def _poll_report(
    client: req.Client,
    report_run_id: str,
    base_url: str,
) -> bool:
    """Poll async report until completion. Returns True if completed.

    Handles 429 rate limit responses with exponential backoff and
    Retry-After header support.
    """
    elapsed = 0
    backoff_wait = POLL_INTERVAL_SECONDS
    while elapsed < POLL_MAX_WAIT_SECONDS:
        try:
            response = client.get(f"{base_url}/{report_run_id}")
            response.raise_for_status()
        except req.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                retry_after = e.response.headers.get("Retry-After")
                wait = int(retry_after) if retry_after else backoff_wait
                logger.warning("Rate limited during poll, waiting %ds", wait)
                time.sleep(wait)
                elapsed += wait
                backoff_wait = min(backoff_wait * 2, POLL_MAX_WAIT_SECONDS)
                continue
            raise
        backoff_wait = POLL_INTERVAL_SECONDS  # reset on success
        data = response.json()

        status = data.get("async_status")
        pct = data.get("async_percent_completion", 0)
        logger.info("Report %s: status=%s, completion=%s%%", report_run_id, status, pct)

        if status == "Job Completed":
            return True
        if status in ("Job Failed", "Job Skipped"):
            logger.warning("Report %s failed with status: %s", report_run_id, status)
            return False

        time.sleep(POLL_INTERVAL_SECONDS)
        elapsed += POLL_INTERVAL_SECONDS

    logger.warning(
        "Report %s timed out after %ds", report_run_id, POLL_MAX_WAIT_SECONDS
    )
    return False


def _fetch_insights_pages(
    client: req.Client,
    report_run_id: str,
    base_url: str,
) -> Generator[dict, None, None]:
    """Fetch all pages of insights results using cursor pagination."""
    url = f"{base_url}/{report_run_id}/insights"
    yield from _get_paginated(client, url)


def _get_paginated(
    client: req.Client,
    url: str,
    max_retries: int = 5,
) -> Generator[dict, None, None]:
    """Fetch all pages using cursor pagination.

    Handles 429 rate limit responses with exponential backoff (up to
    *max_retries* attempts per page) and Retry-After header support.
    """
    while url:
        backoff_wait = POLL_INTERVAL_SECONDS
        retries = 0
        while True:
            try:
                response = client.get(url)
                response.raise_for_status()
                break  # success
            except req.HTTPError as e:
                if e.response is not None and e.response.status_code in (
                    400,
                    403,
                    404,
                ):
                    logger.warning(
                        "Request failed (%d) for %s. Skipping.",
                        e.response.status_code,
                        url,
                    )
                    return
                if (
                    e.response is not None
                    and e.response.status_code == 429
                    and retries < max_retries
                ):
                    retry_after = e.response.headers.get("Retry-After")
                    wait = int(retry_after) if retry_after else backoff_wait
                    logger.warning(
                        "Rate limited on %s, waiting %ds (retry %d/%d)",
                        url,
                        wait,
                        retries + 1,
                        max_retries,
                    )
                    time.sleep(wait)
                    backoff_wait = min(backoff_wait * 2, POLL_MAX_WAIT_SECONDS)
                    retries += 1
                    continue
                raise
        data = response.json()
        yield from data.get("data", [])
        url = data.get("paging", {}).get("next")


@dlt.resource(name="ad_leads", write_disposition="append", primary_key="id")
def ad_leads(
    access_token: str,
    account_id: str,
    lead_fields: Optional[list[str]] = None,
    last_created_time=dlt.sources.incremental(
        "created_time", initial_value="2020-01-01T00:00:00+0000"
    ),
    base_url: str = DEFAULT_BASE_URL,
):
    """Fetch ad leads (lead form submissions) with incremental loading."""
    act_id = account_id if account_id.startswith("act_") else f"act_{account_id}"
    fields = lead_fields or DEFAULT_FIELDS["ad_leads"]
    client = _make_client(access_token)

    # Fetch leads via lead forms instead of iterating all ads.
    # Only accounts with leadgen_forms have lead data.
    since = last_created_time.last_value
    filtering = json.dumps(
        [{"field": "time_created", "operator": "GREATER_THAN", "value": since}]
    )
    forms_url = f"{base_url}/{act_id}/leadgen_forms?fields=id"
    for form in _get_paginated(client, forms_url):
        form_id = form["id"]
        leads_params = urlencode(
            [("fields", ",".join(fields)), ("filtering", filtering)]
        )
        leads_url = f"{base_url}/{form_id}/leads?{leads_params}"
        yield from _get_paginated(client, leads_url)


@dlt.resource(name="insights", write_disposition="merge")
def insights(
    access_token: str,
    account_id: str,
    level: str = "ad",
    insight_fields: Optional[list[str]] = None,
    time_increment: int = 1,
    breakdowns: Optional[list[str]] = None,
    action_breakdowns: Optional[list[str]] = None,
    attribution_window_days: int = 28,
    last_date=dlt.sources.incremental(
        "date_start", initial_value="2020-01-01", row_order="asc"
    ),
    base_url: str = DEFAULT_BASE_URL,
):
    """Fetch insights via async report with incremental loading and attribution window.

    Args:
        access_token: Access token.
        account_id: Ad account ID.
        level: Aggregation level (account, campaign, adset, ad).
        insight_fields: Fields/metrics to fetch.
        time_increment: Time granularity in days (1=daily, 7=weekly, etc.).
        breakdowns: Breakdown dimensions (e.g. ["age", "gender", "device_platform"]).
        action_breakdowns: Action breakdown dimensions (e.g. ["action_type"]).
        attribution_window_days: Days to re-fetch for attribution window.
        last_date: Incremental cursor.
        base_url: API base URL.
    """
    from datetime import date, timedelta

    act_id = account_id if account_id.startswith("act_") else f"act_{account_id}"
    fields = insight_fields or DEFAULT_INSIGHT_FIELDS
    client = _make_client(access_token)

    # Start date: go back attribution_window_days from last cursor
    last = last_date.last_value
    window_start = date.fromisoformat(last) - timedelta(days=attribution_window_days)
    start = window_start.isoformat()
    end = (date.today() - timedelta(days=1)).isoformat()

    if start > end:
        logger.info("insights: already up to date (start=%s > end=%s)", start, end)
        return

    logger.info("insights: fetching %s to %s (level=%s)", start, end, level)

    # Create async report job
    request_data = {
        "fields": ",".join(fields),
        "level": level,
        "time_increment": str(time_increment),
        "time_range": f'{{"since":"{start}","until":"{end}"}}',
    }
    if breakdowns:
        request_data["breakdowns"] = ",".join(breakdowns)
    if action_breakdowns:
        request_data["action_breakdowns"] = ",".join(action_breakdowns)

    response = client.post(
        f"{base_url}/{act_id}/insights",
        data=request_data,
    )
    if response.status_code != 200:
        error_body = (
            response.json()
            if response.headers.get("content-type", "").startswith("application/json")
            else response.text
        )
        logger.warning(
            "insights submit failed: %d %s for %s",
            response.status_code,
            error_body,
            act_id,
        )
        response.raise_for_status()
    report_run_id = response.json().get("report_run_id")

    if not report_run_id:
        logger.warning("insights: no report_run_id returned")
        return

    # Poll until completion
    if not _poll_report(client, report_run_id, base_url):
        return

    # Fetch results with type conversion
    for row in _fetch_insights_pages(client, report_run_id, base_url):
        yield _convert_insight_types(row)


@dlt.source(name="meta_ads")
def meta_ads_source(
    access_token: str = dlt.secrets.value,
    account_id: str = dlt.secrets.value,
    level: str = "ad",
    insight_fields: Optional[list[str]] = None,
    time_increment: int = 1,
    breakdowns: Optional[list[str]] = None,
    action_breakdowns: Optional[list[str]] = None,
    attribution_window_days: int = 28,
    custom_fields: Optional[dict[str, list[str]]] = None,
    resources: Optional[Sequence[str]] = None,
    base_url: Optional[str] = None,
    start_date: Optional[str] = None,
) -> list[DltResource]:
    """A dlt source for Meta (Facebook) Marketing API.

    Args:
        access_token: System User Token or long-lived access token.
        account_id: Ad account ID (with or without 'act_' prefix).
        level: Insights aggregation level (account, campaign, adset, ad).
        insight_fields: Custom list of insight fields/metrics.
        time_increment: Insights time granularity in days (1=daily).
        breakdowns: Insights breakdown dimensions (e.g. ["age", "gender"]).
        action_breakdowns: Insights action breakdown dimensions.
        attribution_window_days: Days to re-fetch for attribution window (default 28).
        custom_fields: Override default fields per resource (e.g. {"campaigns": ["id", "name"]}).
        resources: List of resource names to load. None for all.
        base_url: Override the API base URL.
        start_date: Override incremental start date (YYYY-MM-DD).

    Returns:
        List of dlt resources.
    """
    url = (base_url or DEFAULT_BASE_URL).rstrip("/")

    # REST API resources (master data)
    config = _rest_api_config(access_token, account_id, url, custom_fields)
    rest_resources = rest_api_resources(config)

    # Ad leads resource
    leads_resource = ad_leads(
        access_token=access_token,
        account_id=account_id,
        base_url=url,
    )
    leads_resource.apply_hints(
        columns={"created_time": {"data_type": "timestamp"}},
    )

    # Insights resource (async reports)
    initial_value = start_date or "2020-01-01"
    pk = INSIGHTS_PRIMARY_KEYS.get(level, ["date_start", "ad_id"])
    if breakdowns:
        pk = pk + breakdowns
    insights_resource = insights(
        access_token=access_token,
        account_id=account_id,
        level=level,
        insight_fields=insight_fields,
        time_increment=time_increment,
        breakdowns=breakdowns,
        action_breakdowns=action_breakdowns,
        attribution_window_days=attribution_window_days,
        last_date=dlt.sources.incremental(
            "date_start", initial_value=initial_value, row_order="asc"
        ),
        base_url=url,
    )
    insights_resource.apply_hints(
        primary_key=pk,
        columns={
            "date_start": {"data_type": "date"},
            "date_stop": {"data_type": "date"},
        },
    )

    # Apply timestamp column hints to master data resources
    timestamp_columns = {
        col: {"data_type": "timestamp"}
        for col in ("created_time", "updated_time", "start_time", "stop_time")
    }
    for r in rest_resources:
        r.apply_hints(columns=timestamp_columns)

    all_resources: list[DltResource] = rest_resources + [
        leads_resource,
        insights_resource,
    ]

    all_resources = wrap_resources_safe(all_resources)

    if resources:
        return [r for r in all_resources if r.name in resources]
    return all_resources

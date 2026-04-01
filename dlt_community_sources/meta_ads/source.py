"""dlt source for Meta (Facebook) Marketing API."""

import json
import logging
import time
from collections.abc import Generator
from typing import Optional, Sequence
from urllib.parse import urlencode

import dlt
from dlt.sources import DltResource
from dlt.sources.helpers import requests as req
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://graph.facebook.com/v25.0"
DEFAULT_FIELDS = {
    "campaigns": [
        "id",
        "name",
        "objective",
        "status",
        "effective_status",
        "daily_budget",
        "lifetime_budget",
        "budget_remaining",
        "spend_cap",
        "bid_strategy",
        "buying_type",
        "start_time",
        "stop_time",
        "created_time",
        "updated_time",
        "special_ad_categories",
    ],
    "ad_sets": [
        "id",
        "name",
        "campaign_id",
        "status",
        "effective_status",
        "daily_budget",
        "lifetime_budget",
        "budget_remaining",
        "bid_amount",
        "bid_strategy",
        "billing_event",
        "optimization_goal",
        "targeting",
        "start_time",
        "end_time",
        "created_time",
        "updated_time",
    ],
    "ads": [
        "id",
        "name",
        "adset_id",
        "campaign_id",
        "status",
        "effective_status",
        "creative",
        "created_time",
        "updated_time",
    ],
    "ad_creatives": [
        "id",
        "name",
        "title",
        "body",
        "image_url",
        "thumbnail_url",
        "url_tags",
        "object_story_spec",
        "asset_feed_spec",
    ],
    "ad_leads": [
        "id",
        "created_time",
        "ad_id",
        "ad_name",
        "adset_id",
        "adset_name",
        "campaign_id",
        "campaign_name",
        "form_id",
        "field_data",
        "platform",
    ],
    "custom_audiences": [
        "id",
        "name",
        "description",
        "subtype",
        "approximate_count",
        "data_source",
        "delivery_status",
        "operation_status",
        "time_created",
        "time_updated",
    ],
    "custom_conversions": [
        "id",
        "name",
        "description",
        "account_id",
        "custom_event_type",
        "default_conversion_value",
        "is_archived",
        "pixel",
        "rule",
    ],
    "ad_images": [
        "hash",
        "name",
        "url",
        "url_128",
        "width",
        "height",
        "created_time",
        "updated_time",
        "status",
    ],
    "ad_videos": [
        "id",
        "title",
        "description",
        "length",
        "source",
        "picture",
        "created_time",
        "updated_time",
    ],
    "activities": [
        "event_time",
        "event_type",
        "actor_id",
        "actor_name",
        "object_id",
        "object_name",
        "extra_data",
    ],
    "saved_audiences": [
        "id",
        "name",
        "description",
        "approximate_count",
        "targeting",
        "run_status",
        "time_created",
        "time_updated",
    ],
}
# Insights のメトリクスで文字列→数値変換が必要なフィールド
INSIGHT_INT_FIELDS = {"impressions", "clicks", "reach"}
INSIGHT_FLOAT_FIELDS = {"spend", "frequency", "cpc", "cpm", "ctr", "cpp"}
DEFAULT_INSIGHT_FIELDS = [
    "date_start",
    "date_stop",
    "campaign_id",
    "campaign_name",
    "adset_id",
    "adset_name",
    "ad_id",
    "ad_name",
    "impressions",
    "clicks",
    "spend",
    "reach",
    "frequency",
    "cpc",
    "cpm",
    "ctr",
    "cpp",
    "actions",
    "action_values",
    "conversions",
    "conversion_values",
    "cost_per_action_type",
    "cost_per_conversion",
    "video_avg_time_watched_actions",
    "video_p25_watched_actions",
    "video_p50_watched_actions",
    "video_p75_watched_actions",
    "video_p100_watched_actions",
]
# ポーリング間隔と最大待機時間
POLL_INTERVAL_SECONDS = 10
POLL_MAX_WAIT_SECONDS = 600

INSIGHTS_PRIMARY_KEYS = {
    "account": ["date_start"],
    "campaign": ["date_start", "campaign_id"],
    "adset": ["date_start", "adset_id"],
    "ad": ["date_start", "ad_id"],
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
                row[field] = float(row[field])
            except (ValueError, TypeError):
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
                    {"status_code": 403, "action": "ignore"},
                    {"status_code": 404, "action": "ignore"},
                ],
            },
        },
        "resources": [
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

    Note: 429 rate limit retry is handled by dlt's req.Client automatically
    (exponential backoff, Retry-After header support, max 5 attempts).
    """
    elapsed = 0
    while elapsed < POLL_MAX_WAIT_SECONDS:
        response = client.get(f"{base_url}/{report_run_id}")
        response.raise_for_status()
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
) -> Generator[dict, None, None]:
    """Fetch all pages using cursor pagination.

    Note: 429 rate limit retry is handled by dlt's req.Client automatically.
    """
    while url:
        try:
            response = client.get(url)
            response.raise_for_status()
        except req.HTTPError as e:
            if e.response is not None and e.response.status_code in (403, 404):
                logger.warning(
                    "Request failed (%d) for %s. Skipping.",
                    e.response.status_code,
                    url,
                )
                return
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

    # ads を取得して、各 ad の leads を取得
    # filtering パラメータで created_time > last_value のリードのみ取得
    since = last_created_time.last_value
    ads_params = urlencode(
        [
            ("fields", "id"),
            ("effective_status[]", "ACTIVE"),
            ("effective_status[]", "PAUSED"),
            ("effective_status[]", "ARCHIVED"),
        ]
    )
    ads_url = f"{base_url}/{act_id}/ads?{ads_params}"
    filtering = json.dumps(
        [{"field": "created_time", "operator": "GREATER_THAN", "value": since}]
    )
    for ad in _get_paginated(client, ads_url):
        ad_id = ad["id"]
        leads_params = urlencode(
            [("fields", ",".join(fields)), ("filtering", filtering)]
        )
        leads_url = f"{base_url}/{ad_id}/leads?{leads_params}"
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
    last_date=dlt.sources.incremental("date_start", initial_value="2020-01-01"),
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

    # 再取得開始日: attribution_window_days 分遡る
    last = last_date.last_value
    window_start = date.fromisoformat(last) - timedelta(days=attribution_window_days)
    start = window_start.isoformat()
    end = (date.today() - timedelta(days=1)).isoformat()

    if start > end:
        logger.info("insights: already up to date (start=%s > end=%s)", start, end)
        return

    logger.info("insights: fetching %s to %s (level=%s)", start, end, level)

    # 非同期レポートジョブを作成
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
    response.raise_for_status()
    report_run_id = response.json().get("report_run_id")

    if not report_run_id:
        logger.warning("insights: no report_run_id returned")
        return

    # ポーリング
    if not _poll_report(client, report_run_id, base_url):
        return

    # 結果取得（型変換を適用）
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
        last_date=dlt.sources.incremental("date_start", initial_value=initial_value),
        base_url=url,
    )
    insights_resource.apply_hints(primary_key=pk)

    all_resources: list[DltResource] = rest_resources + [
        leads_resource,
        insights_resource,
    ]

    if resources:
        return [r for r in all_resources if r.name in resources]
    return all_resources

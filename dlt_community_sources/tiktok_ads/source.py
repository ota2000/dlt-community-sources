"""dlt source for TikTok Marketing API (Business API)."""

import json
import logging
from collections.abc import Generator
from datetime import date, timedelta
from typing import Optional, Sequence

import dlt
from dlt.sources import DltResource
from dlt.sources.helpers import requests as req
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig

from .auth import refresh_access_token

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://business-api.tiktok.com/open_api/v1.3"
# レポートの1リクエストあたり最大日数
MAX_REPORT_DAYS = 30
# レポートのデフォルトメトリクス
DEFAULT_METRICS = [
    "spend",
    "impressions",
    "clicks",
    "cpc",
    "cpm",
    "ctr",
    "reach",
    "frequency",
    "conversions",
    "cost_per_conversion",
    "conversion_rate",
    "real_time_conversions",
    "real_time_cost_per_conversion",
    "real_time_conversion_rate",
    "result",
    "cost_per_result",
    "result_rate",
    "video_play_actions",
    "video_watched_2s",
    "video_watched_6s",
    "average_video_play_per_user",
    "video_views_p25",
    "video_views_p50",
    "video_views_p75",
    "video_views_p100",
    "likes",
    "comments",
    "shares",
    "follows",
    "profile_visits",
]
# レポートのデフォルトディメンション（data_level によって変わる）
REPORT_DIMENSIONS = {
    "AUCTION_ADVERTISER": ["advertiser_id", "stat_time_day"],
    "AUCTION_CAMPAIGN": ["campaign_id", "stat_time_day"],
    "AUCTION_ADGROUP": ["adgroup_id", "stat_time_day"],
    "AUCTION_AD": ["ad_id", "stat_time_day"],
}
# data_level と primary_key のマッピング
REPORT_PRIMARY_KEYS = {
    "AUCTION_ADVERTISER": ["stat_time_day", "advertiser_id"],
    "AUCTION_CAMPAIGN": ["stat_time_day", "campaign_id"],
    "AUCTION_ADGROUP": ["stat_time_day", "adgroup_id"],
    "AUCTION_AD": ["stat_time_day", "ad_id"],
}
# メトリクスの型変換
REPORT_INT_FIELDS = {
    "impressions",
    "clicks",
    "reach",
    "conversions",
    "real_time_conversions",
    "video_play_actions",
    "video_watched_2s",
    "video_watched_6s",
    "video_views_p25",
    "video_views_p50",
    "video_views_p75",
    "video_views_p100",
    "likes",
    "comments",
    "shares",
    "follows",
    "profile_visits",
    "result",
}
REPORT_FLOAT_FIELDS = {
    "spend",
    "cpc",
    "cpm",
    "ctr",
    "frequency",
    "cost_per_conversion",
    "conversion_rate",
    "real_time_cost_per_conversion",
    "real_time_conversion_rate",
    "cost_per_result",
    "result_rate",
    "average_video_play_per_user",
}


def _rest_api_config(
    access_token: str,
    advertiser_id: str,
    base_url: str,
) -> RESTAPIConfig:
    """Build the REST API config for TikTok Marketing API master data.

    Note: TikTok API uses 'Access-Token' header (not 'Authorization: Bearer').
    We use api_key auth type to set the custom header name.
    """
    return {
        "client": {
            "base_url": f"{base_url}/",
            "auth": {
                "type": "api_key",
                "name": "Access-Token",
                "api_key": access_token,
                "location": "header",
            },
            "paginator": {
                "type": "page_number",
                "base_page": 1,
                "page_param": "page",
                "total_path": "data.page_info.total_page",
            },
        },
        "resource_defaults": {
            "write_disposition": "merge",
            "endpoint": {
                "data_selector": "data.list",
                "params": {
                    "advertiser_id": advertiser_id,
                    "page_size": "100",
                },
                "response_actions": [
                    {"status_code": 403, "action": "ignore"},
                    {"status_code": 404, "action": "ignore"},
                ],
            },
        },
        "resources": [
            {
                "name": "campaigns",
                "primary_key": "campaign_id",
                "endpoint": {"path": "campaign/get/"},
            },
            {
                "name": "ad_groups",
                "primary_key": "adgroup_id",
                "endpoint": {"path": "adgroup/get/"},
            },
            {
                "name": "ads",
                "primary_key": "ad_id",
                "endpoint": {"path": "ad/get/"},
            },
            {
                "name": "custom_audiences",
                "primary_key": "custom_audience_id",
                "endpoint": {
                    "path": "dmp/custom_audience/list/",
                    "params": {"page_size": "100"},
                },
            },
            {
                "name": "saved_audiences",
                "primary_key": "saved_audience_id",
                "endpoint": {
                    "path": "dmp/saved_audience/list/",
                    "params": {"page_size": "100"},
                },
            },
            {
                "name": "creative_portfolios",
                "primary_key": "creative_portfolio_id",
                "endpoint": {
                    "path": "creative/portfolio/list/",
                    "params": {"page_size": "100"},
                },
            },
            {
                "name": "automated_rules",
                "primary_key": "rule_id",
                "endpoint": {
                    "path": "optimizer/rule/list/",
                    "params": {"page_size": "100"},
                },
            },
        ],
    }


def _make_client(access_token: str) -> req.Client:
    """Create a dlt HTTP client with TikTok Access-Token auth."""
    client = req.Client()
    client.session.headers["Access-Token"] = access_token
    return client


def _check_response(data: dict, context: str) -> bool:
    """Check TikTok API response for errors (HTTP 200 with code != 0)."""
    code = data.get("code", -1)
    if code != 0:
        logger.warning(
            "%s: API error code=%s, message=%s",
            context,
            code,
            data.get("message", "unknown"),
        )
        return False
    return True


def _convert_report_types(row: dict) -> dict:
    """Convert report metric strings to numeric types in-place."""
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


def _flatten_report_row(row: dict) -> dict:
    """Flatten TikTok report row by merging dimensions and metrics into a single dict."""
    flat = {}
    flat.update(row.get("dimensions", {}))
    flat.update(row.get("metrics", {}))
    return flat


def _date_chunks(
    start: str, end: str, max_days: int = MAX_REPORT_DAYS
) -> Generator[tuple[str, str], None, None]:
    """Split a date range into chunks of max_days."""
    current = date.fromisoformat(start)
    end_date = date.fromisoformat(end)
    while current <= end_date:
        chunk_end = min(current + timedelta(days=max_days - 1), end_date)
        yield current.isoformat(), chunk_end.isoformat()
        current = chunk_end + timedelta(days=1)


@dlt.resource(
    name="authorized_advertiser_ids",
    write_disposition="replace",
    primary_key="advertiser_id",
)
def authorized_advertiser_ids(
    access_token: str,
    app_id: str,
    secret: str,
    base_url: str = DEFAULT_BASE_URL,
):
    """Fetch advertiser IDs authorized for this access token."""
    client = _make_client(access_token)
    response = client.get(
        f"{base_url}/oauth2/advertiser/get/",
        params={"app_id": app_id, "secret": secret},
    )
    response.raise_for_status()
    data = response.json()
    if _check_response(data, "authorized_advertiser_ids"):
        for adv_id in data.get("data", {}).get("list", []):
            yield {"advertiser_id": adv_id}


@dlt.resource(
    name="advertiser_info", write_disposition="merge", primary_key="advertiser_id"
)
def advertiser_info(
    access_token: str,
    advertiser_id: str,
    base_url: str = DEFAULT_BASE_URL,
):
    """Fetch advertiser account info."""
    client = _make_client(access_token)
    response = client.get(
        f"{base_url}/advertiser/info/",
        params={"advertiser_ids": json.dumps([advertiser_id])},
    )
    response.raise_for_status()
    data = response.json()
    if _check_response(data, "advertiser_info"):
        yield from data.get("data", {}).get("list", [])


@dlt.resource(name="advertiser_balance", write_disposition="replace")
def advertiser_balance(
    access_token: str,
    advertiser_id: str,
    base_url: str = DEFAULT_BASE_URL,
):
    """Fetch advertiser account balance."""
    client = _make_client(access_token)
    response = client.get(
        f"{base_url}/advertiser/balance/get/",
        params={"advertiser_id": advertiser_id},
    )
    response.raise_for_status()
    data = response.json()
    if _check_response(data, "advertiser_balance"):
        balance_data = data.get("data", {})
        if balance_data:
            balance_data["advertiser_id"] = advertiser_id
            yield balance_data


@dlt.resource(name="advertiser_transactions", write_disposition="append")
def advertiser_transactions(
    access_token: str,
    advertiser_id: str,
    last_date=dlt.sources.incremental(
        "transaction_time", initial_value="2020-01-01 00:00:00"
    ),
    base_url: str = DEFAULT_BASE_URL,
):
    """Fetch advertiser account transactions with incremental loading."""
    client = _make_client(access_token)
    page = 1
    while True:
        response = client.get(
            f"{base_url}/advertiser/transaction/get/",
            params={
                "advertiser_id": advertiser_id,
                "page": str(page),
                "page_size": "100",
            },
        )
        response.raise_for_status()
        data = response.json()
        if not _check_response(data, f"advertiser_transactions page {page}"):
            break
        rows = data.get("data", {}).get("list", [])
        if not rows:
            break
        yield from rows
        page_info = data.get("data", {}).get("page_info", {})
        if page >= page_info.get("total_page", 1):
            break
        page += 1


@dlt.resource(name="apps", write_disposition="merge", primary_key="app_id")
def apps(
    access_token: str,
    advertiser_id: str,
    base_url: str = DEFAULT_BASE_URL,
):
    """Fetch apps associated with the advertiser."""
    client = _make_client(access_token)
    response = client.get(
        f"{base_url}/app/list/",
        params={"advertiser_id": advertiser_id},
    )
    response.raise_for_status()
    data = response.json()
    if _check_response(data, "apps"):
        yield from data.get("data", {}).get("list", [])


@dlt.resource(name="rule_results", write_disposition="append")
def rule_results(
    access_token: str,
    advertiser_id: str,
    base_url: str = DEFAULT_BASE_URL,
):
    """Fetch automated rule execution results."""
    client = _make_client(access_token)
    page = 1
    while True:
        response = client.get(
            f"{base_url}/optimizer/rule/result/list/",
            params={
                "advertiser_id": advertiser_id,
                "page": str(page),
                "page_size": "100",
            },
        )
        response.raise_for_status()
        data = response.json()
        if not _check_response(data, f"rule_results page {page}"):
            break
        rows = data.get("data", {}).get("list", [])
        if not rows:
            break
        yield from rows
        page_info = data.get("data", {}).get("page_info", {})
        if page >= page_info.get("total_page", 1):
            break
        page += 1


@dlt.resource(name="videos", write_disposition="merge", primary_key="video_id")
def videos(
    access_token: str,
    advertiser_id: str,
    base_url: str = DEFAULT_BASE_URL,
):
    """Fetch ad video assets via file/video/ad/search/."""
    client = _make_client(access_token)
    page = 1
    while True:
        response = client.get(
            f"{base_url}/file/video/ad/search/",
            params={
                "advertiser_id": advertiser_id,
                "page": str(page),
                "page_size": "100",
            },
        )
        response.raise_for_status()
        data = response.json()
        if not _check_response(data, f"videos page {page}"):
            break

        rows = data.get("data", {}).get("list", [])
        if not rows:
            break
        yield from rows

        page_info = data.get("data", {}).get("page_info", {})
        if page >= page_info.get("total_page", 1):
            break
        page += 1


@dlt.resource(name="report", write_disposition="merge")
def report(
    access_token: str,
    advertiser_id: str,
    data_level: str = "AUCTION_AD",
    metrics: Optional[list[str]] = None,
    dimensions: Optional[list[str]] = None,
    attribution_window_days: int = 7,
    last_date=dlt.sources.incremental("stat_time_day", initial_value="2020-01-01"),
    base_url: str = DEFAULT_BASE_URL,
):
    """Fetch TikTok Ads report with incremental loading and attribution window.

    Args:
        access_token: Access token.
        advertiser_id: Advertiser ID.
        data_level: AUCTION_ADVERTISER, AUCTION_CAMPAIGN, AUCTION_ADGROUP, or AUCTION_AD.
        metrics: Metrics to fetch.
        dimensions: Dimensions to group by.
        attribution_window_days: Days to re-fetch for attribution window.
        last_date: Incremental cursor.
        base_url: API base URL.
    """
    report_metrics = metrics or DEFAULT_METRICS
    report_dimensions = dimensions or REPORT_DIMENSIONS.get(
        data_level, ["ad_id", "stat_time_day"]
    )
    client = _make_client(access_token)

    last = last_date.last_value
    window_start = date.fromisoformat(last) - timedelta(days=attribution_window_days)
    start = window_start.isoformat()
    end = (date.today() - timedelta(days=1)).isoformat()

    if start > end:
        logger.info("report: already up to date (start=%s > end=%s)", start, end)
        return

    logger.info("report: fetching %s to %s (data_level=%s)", start, end, data_level)

    for chunk_start, chunk_end in _date_chunks(start, end):
        page = 1
        while True:
            params = {
                "advertiser_id": advertiser_id,
                "report_type": "BASIC",
                "data_level": data_level,
                "dimensions": json.dumps(report_dimensions),
                "metrics": json.dumps(report_metrics),
                "start_date": chunk_start,
                "end_date": chunk_end,
                "page": str(page),
                "page_size": "1000",
            }
            response = client.get(f"{base_url}/report/integrated/get/", params=params)
            response.raise_for_status()
            data = response.json()

            if not _check_response(
                data, f"report chunk {chunk_start}-{chunk_end} page {page}"
            ):
                break

            rows = data.get("data", {}).get("list", [])
            if not rows:
                break

            for row in rows:
                flat = _flatten_report_row(row)
                yield _convert_report_types(flat)

            page_info = data.get("data", {}).get("page_info", {})
            total_page = page_info.get("total_page", 1)
            if page >= total_page:
                break
            page += 1


@dlt.source(name="tiktok_ads")
def tiktok_ads_source(
    app_id: str = dlt.secrets.value,
    secret: str = dlt.secrets.value,
    refresh_token: str = dlt.secrets.value,
    advertiser_id: str = dlt.secrets.value,
    data_level: str = "AUCTION_AD",
    metrics: Optional[list[str]] = None,
    dimensions: Optional[list[str]] = None,
    attribution_window_days: int = 7,
    resources: Optional[Sequence[str]] = None,
    base_url: Optional[str] = None,
    start_date: Optional[str] = None,
) -> list[DltResource]:
    """A dlt source for TikTok Marketing API.

    Args:
        app_id: TikTok app ID.
        secret: TikTok app secret.
        refresh_token: Refresh token (rotated on each use).
        advertiser_id: TikTok advertiser ID.
        data_level: Report data level (AUCTION_ADVERTISER/CAMPAIGN/ADGROUP/AD).
        metrics: Custom list of report metrics.
        dimensions: Custom list of report dimensions.
        attribution_window_days: Days to re-fetch for attribution window (default 7).
        resources: List of resource names to load. None for all.
        base_url: Override the API base URL.
        start_date: Override incremental start date (YYYY-MM-DD).

    Returns:
        List of dlt resources.
        The caller (data-collector main.py) is responsible for persisting
        the new refresh_token to Secret Manager via refresh_access_token().
    """
    url = (base_url or DEFAULT_BASE_URL).rstrip("/")

    # refresh_token → access_token（トークンローテーション）
    tokens = refresh_access_token(app_id, secret, refresh_token)
    access_token = tokens["access_token"]
    # tokens["refresh_token"] は呼び出し元で Secret Manager に書き戻す

    # REST API resources (master data)
    config = _rest_api_config(access_token, advertiser_id, url)
    rest_resources = rest_api_resources(config)

    # Custom resources
    custom_resources = [
        authorized_advertiser_ids(
            access_token=access_token, app_id=app_id, secret=secret, base_url=url
        ),
        advertiser_info(
            access_token=access_token, advertiser_id=advertiser_id, base_url=url
        ),
        advertiser_balance(
            access_token=access_token, advertiser_id=advertiser_id, base_url=url
        ),
        advertiser_transactions(
            access_token=access_token, advertiser_id=advertiser_id, base_url=url
        ),
        apps(access_token=access_token, advertiser_id=advertiser_id, base_url=url),
        videos(access_token=access_token, advertiser_id=advertiser_id, base_url=url),
        rule_results(
            access_token=access_token, advertiser_id=advertiser_id, base_url=url
        ),
    ]

    # Report resource
    initial_value = start_date or "2020-01-01"
    pk = REPORT_PRIMARY_KEYS.get(data_level, ["stat_time_day", "ad_id"])
    report_resource = report(
        access_token=access_token,
        advertiser_id=advertiser_id,
        data_level=data_level,
        metrics=metrics,
        dimensions=dimensions,
        attribution_window_days=attribution_window_days,
        last_date=dlt.sources.incremental("stat_time_day", initial_value=initial_value),
        base_url=url,
    )
    report_resource.apply_hints(primary_key=pk)

    all_resources: list[DltResource] = (
        rest_resources
        + custom_resources
        + [
            report_resource,
        ]
    )

    if resources:
        return [r for r in all_resources if r.name in resources]
    return all_resources

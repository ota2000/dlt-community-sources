"""dlt source for X (Twitter) Ads API."""

import logging
import time
from collections.abc import Generator, Sequence
from datetime import date, timedelta
from decimal import Decimal
from typing import Optional

import dlt
from dlt.sources import DltResource

logger = logging.getLogger(__name__)

BASE_URL = "https://ads-api.x.com/12"

# Maximum entity_ids per stats API call
STATS_BATCH_SIZE = 20

# Default metric groups for stats API
DEFAULT_METRIC_GROUPS = [
    "ENGAGEMENT",
    "BILLING",
    "VIDEO",
    "MEDIA",
    "WEB_CONVERSION",
    "MOBILE_CONVERSION",
    "LIFE_TIME_VALUE_MOBILE_CONVERSION",
]

# Stats fields containing monetary amounts (micro-currency values)
STATS_MONEY_FIELDS = {
    "billed_charge_local_micro",
    "billed_engagements",
}


def _create_session(
    consumer_key: str,
    consumer_secret: str,
    access_token: str,
    access_token_secret: str,
):
    """Create a requests session with OAuth 1.0a authentication."""
    import requests
    from requests_oauthlib import OAuth1

    auth = OAuth1(
        consumer_key,
        client_secret=consumer_secret,
        resource_owner_key=access_token,
        resource_owner_secret=access_token_secret,
    )
    session = requests.Session()
    session.auth = auth
    return session


def _handle_rate_limit(response) -> None:
    """Handle 429 rate limit responses by waiting for Retry-After."""
    if response.status_code == 429:
        retry_after = int(response.headers.get("Retry-After", "60"))
        logger.warning(
            "Rate limited (429). Waiting %d seconds before retry.", retry_after
        )
        time.sleep(retry_after)


def _get_paginated(
    session,
    url: str,
    params: Optional[dict] = None,
    max_count: int = 1000,
) -> Generator[dict, None, None]:
    """Fetch all pages using cursor-based pagination.

    Args:
        session: Authenticated requests session.
        url: API endpoint URL.
        params: Additional query parameters.
        max_count: Number of items per page (max 1000).
    """
    request_params = dict(params or {})
    request_params["count"] = max_count

    while True:
        response = session.get(url, params=request_params)

        if response.status_code == 429:
            _handle_rate_limit(response)
            continue

        if response.status_code in (400, 403, 404):
            logger.warning(
                "Request failed (%d) for %s. Skipping.", response.status_code, url
            )
            return

        response.raise_for_status()
        data = response.json()

        for item in data.get("data", []):
            yield item

        next_cursor = data.get("next_cursor")
        if not next_cursor:
            break
        request_params["cursor"] = next_cursor


def _batch_ids(ids: list[str], batch_size: int = STATS_BATCH_SIZE) -> list[list[str]]:
    """Split a list of IDs into batches."""
    return [ids[i : i + batch_size] for i in range(0, len(ids), batch_size)]


def _convert_stats_types(row: dict) -> dict:
    """Convert stats metric values to appropriate types in-place.

    X Ads API returns monetary values in micro-currency (1/1,000,000).
    """
    for key, value in row.items():
        if key in STATS_MONEY_FIELDS and value is not None:
            try:
                row[key] = Decimal(str(value)) / Decimal("1000000")
            except (ValueError, TypeError, ArithmeticError):
                pass
    return row


@dlt.resource(name="accounts", write_disposition="merge", primary_key="id")
def accounts_resource(
    session,
    base_url: str = BASE_URL,
) -> Generator[dict, None, None]:
    """Fetch all ad accounts."""
    url = f"{base_url}/accounts"
    yield from _get_paginated(session, url)


@dlt.resource(name="campaigns", write_disposition="merge", primary_key="id")
def campaigns_resource(
    session,
    account_id: str,
    base_url: str = BASE_URL,
) -> Generator[dict, None, None]:
    """Fetch all campaigns for an account."""
    url = f"{base_url}/accounts/{account_id}/campaigns"
    yield from _get_paginated(session, url)


@dlt.resource(name="line_items", write_disposition="merge", primary_key="id")
def line_items_resource(
    session,
    account_id: str,
    base_url: str = BASE_URL,
) -> Generator[dict, None, None]:
    """Fetch all line items for an account."""
    url = f"{base_url}/accounts/{account_id}/line_items"
    yield from _get_paginated(session, url)


@dlt.resource(name="promoted_tweets", write_disposition="merge", primary_key="id")
def promoted_tweets_resource(
    session,
    account_id: str,
    base_url: str = BASE_URL,
) -> Generator[dict, None, None]:
    """Fetch all promoted tweets for an account."""
    url = f"{base_url}/accounts/{account_id}/promoted_tweets"
    yield from _get_paginated(session, url)


@dlt.resource(name="funding_instruments", write_disposition="merge", primary_key="id")
def funding_instruments_resource(
    session,
    account_id: str,
    base_url: str = BASE_URL,
) -> Generator[dict, None, None]:
    """Fetch all funding instruments for an account."""
    url = f"{base_url}/accounts/{account_id}/funding_instruments"
    yield from _get_paginated(session, url)


@dlt.resource(name="media_creatives", write_disposition="merge", primary_key="id")
def media_creatives_resource(
    session,
    account_id: str,
    base_url: str = BASE_URL,
) -> Generator[dict, None, None]:
    """Fetch all media creatives for an account."""
    url = f"{base_url}/accounts/{account_id}/media_creatives"
    yield from _get_paginated(session, url)


@dlt.resource(
    name="scheduled_promoted_tweets", write_disposition="merge", primary_key="id"
)
def scheduled_promoted_tweets_resource(
    session,
    account_id: str,
    base_url: str = BASE_URL,
) -> Generator[dict, None, None]:
    """Fetch all scheduled promoted tweets for an account."""
    url = f"{base_url}/accounts/{account_id}/scheduled_promoted_tweets"
    yield from _get_paginated(session, url)


@dlt.resource(name="tailored_audiences", write_disposition="merge", primary_key="id")
def tailored_audiences_resource(
    session,
    account_id: str,
    base_url: str = BASE_URL,
) -> Generator[dict, None, None]:
    """Fetch all tailored audiences for an account."""
    url = f"{base_url}/accounts/{account_id}/tailored_audiences"
    yield from _get_paginated(session, url)


@dlt.resource(name="targeting_criteria", write_disposition="merge", primary_key="id")
def targeting_criteria_resource(
    session,
    account_id: str,
    base_url: str = BASE_URL,
) -> Generator[dict, None, None]:
    """Fetch targeting criteria for all line items in an account."""
    # First fetch all line item IDs
    line_items_url = f"{base_url}/accounts/{account_id}/line_items"
    line_item_ids = [item["id"] for item in _get_paginated(session, line_items_url)]

    if not line_item_ids:
        return

    # Fetch targeting criteria in batches (API requires line_item_ids param)
    url = f"{base_url}/accounts/{account_id}/targeting_criteria"
    for batch in _batch_ids(line_item_ids):
        params = {"line_item_ids": ",".join(batch)}
        yield from _get_paginated(session, url, params=params)


@dlt.resource(name="campaign_stats", write_disposition="merge")
def campaign_stats_resource(
    session,
    account_id: str,
    metric_groups: Optional[list[str]] = None,
    attribution_window_days: int = 7,
    last_date=dlt.sources.incremental(
        "date", initial_value="2020-01-01", row_order="asc"
    ),
    base_url: str = BASE_URL,
) -> Generator[dict, None, None]:
    """Fetch daily campaign stats with incremental loading."""
    yield from _fetch_entity_stats(
        session=session,
        account_id=account_id,
        entity="CAMPAIGN",
        entity_resource_path="campaigns",
        metric_groups=metric_groups,
        attribution_window_days=attribution_window_days,
        last_date=last_date,
        base_url=base_url,
    )


@dlt.resource(name="line_item_stats", write_disposition="merge")
def line_item_stats_resource(
    session,
    account_id: str,
    metric_groups: Optional[list[str]] = None,
    attribution_window_days: int = 7,
    last_date=dlt.sources.incremental(
        "date", initial_value="2020-01-01", row_order="asc"
    ),
    base_url: str = BASE_URL,
) -> Generator[dict, None, None]:
    """Fetch daily line item stats with incremental loading."""
    yield from _fetch_entity_stats(
        session=session,
        account_id=account_id,
        entity="LINE_ITEM",
        entity_resource_path="line_items",
        metric_groups=metric_groups,
        attribution_window_days=attribution_window_days,
        last_date=last_date,
        base_url=base_url,
    )


@dlt.resource(name="promoted_tweet_stats", write_disposition="merge")
def promoted_tweet_stats_resource(
    session,
    account_id: str,
    metric_groups: Optional[list[str]] = None,
    attribution_window_days: int = 7,
    last_date=dlt.sources.incremental(
        "date", initial_value="2020-01-01", row_order="asc"
    ),
    base_url: str = BASE_URL,
) -> Generator[dict, None, None]:
    """Fetch daily promoted tweet stats with incremental loading."""
    yield from _fetch_entity_stats(
        session=session,
        account_id=account_id,
        entity="PROMOTED_TWEET",
        entity_resource_path="promoted_tweets",
        metric_groups=metric_groups,
        attribution_window_days=attribution_window_days,
        last_date=last_date,
        base_url=base_url,
    )


def _fetch_entity_stats(
    session,
    account_id: str,
    entity: str,
    entity_resource_path: str,
    metric_groups: Optional[list[str]],
    attribution_window_days: int,
    last_date,
    base_url: str,
) -> Generator[dict, None, None]:
    """Fetch daily stats for a given entity type.

    Args:
        session: Authenticated requests session.
        account_id: Ad account ID.
        entity: Entity type (CAMPAIGN, LINE_ITEM, PROMOTED_TWEET).
        entity_resource_path: API path segment for listing entities.
        metric_groups: Metric groups to fetch.
        attribution_window_days: Days to re-fetch for attribution window.
        last_date: Incremental cursor.
        base_url: API base URL.
    """
    groups = metric_groups or DEFAULT_METRIC_GROUPS

    # Determine date range
    last = last_date.last_value
    window_start = date.fromisoformat(last) - timedelta(days=attribution_window_days)
    start_date = window_start
    end_date = date.today() - timedelta(days=1)

    if start_date > end_date:
        logger.info(
            "%s stats: already up to date (start=%s > end=%s)",
            entity,
            start_date,
            end_date,
        )
        return

    # Collect all entity IDs
    entities_url = f"{base_url}/accounts/{account_id}/{entity_resource_path}"
    entity_ids = [item["id"] for item in _get_paginated(session, entities_url)]

    if not entity_ids:
        logger.info("%s stats: no entities found", entity)
        return

    logger.info(
        "%s stats: fetching %s to %s for %d entities",
        entity,
        start_date.isoformat(),
        end_date.isoformat(),
        len(entity_ids),
    )

    # Stats API allows max 90 days per request
    current_start = start_date
    while current_start <= end_date:
        current_end = min(current_start + timedelta(days=89), end_date)

        # ISO 8601 format required by X Ads API
        start_time = f"{current_start.isoformat()}T00:00:00Z"
        end_time = f"{(current_end + timedelta(days=1)).isoformat()}T00:00:00Z"

        for batch in _batch_ids(entity_ids):
            stats_url = f"{base_url}/stats/accounts/{account_id}"
            params = {
                "entity": entity,
                "entity_ids": ",".join(batch),
                "granularity": "DAY",
                "start_time": start_time,
                "end_time": end_time,
                "metric_groups": ",".join(groups),
            }

            while True:
                response = session.get(stats_url, params=params)

                if response.status_code == 429:
                    _handle_rate_limit(response)
                    continue

                if response.status_code in (400, 403, 404):
                    logger.warning(
                        "Stats request failed (%d) for %s. Skipping batch.",
                        response.status_code,
                        entity,
                    )
                    break

                response.raise_for_status()
                data = response.json()

                for entry in data.get("data", []):
                    entity_id = entry.get("id")
                    metrics = entry.get("id_data", [{}])

                    for i, day_metrics in enumerate(metrics):
                        row_date = (current_start + timedelta(days=i)).isoformat()
                        row = {
                            "entity_id": entity_id,
                            "entity_type": entity,
                            "date": row_date,
                            **day_metrics,
                        }
                        yield _convert_stats_types(row)
                break

        current_start = current_end + timedelta(days=1)


@dlt.source(name="x_ads")
def x_ads_source(
    consumer_key: str = dlt.secrets.value,
    consumer_secret: str = dlt.secrets.value,
    access_token: str = dlt.secrets.value,
    access_token_secret: str = dlt.secrets.value,
    account_id: str = dlt.config.value,
    base_url: str = BASE_URL,
    attribution_window_days: int = 7,
    resources: Optional[Sequence[str]] = None,
) -> list[DltResource]:
    """A dlt source for X (Twitter) Ads API.

    Args:
        consumer_key: OAuth 1.0a consumer key (API Key).
        consumer_secret: OAuth 1.0a consumer secret (API Secret).
        access_token: OAuth 1.0a access token.
        access_token_secret: OAuth 1.0a access token secret.
        account_id: X Ads account ID.
        base_url: API base URL (default: v12).
        attribution_window_days: Days to re-fetch for attribution window (default 7).
        resources: List of resource names to load. None for all.

    Returns:
        List of dlt resources.
    """
    url = base_url.rstrip("/")
    session = _create_session(
        consumer_key, consumer_secret, access_token, access_token_secret
    )

    # Master data resources
    accounts_res = accounts_resource(session=session, base_url=url)
    campaigns_res = campaigns_resource(
        session=session, account_id=account_id, base_url=url
    )
    line_items_res = line_items_resource(
        session=session, account_id=account_id, base_url=url
    )
    promoted_tweets_res = promoted_tweets_resource(
        session=session, account_id=account_id, base_url=url
    )
    funding_instruments_res = funding_instruments_resource(
        session=session, account_id=account_id, base_url=url
    )
    media_creatives_res = media_creatives_resource(
        session=session, account_id=account_id, base_url=url
    )
    scheduled_promoted_tweets_res = scheduled_promoted_tweets_resource(
        session=session, account_id=account_id, base_url=url
    )
    tailored_audiences_res = tailored_audiences_resource(
        session=session, account_id=account_id, base_url=url
    )
    targeting_criteria_res = targeting_criteria_resource(
        session=session, account_id=account_id, base_url=url
    )

    # Stats resources
    campaign_stats_res = campaign_stats_resource(
        session=session,
        account_id=account_id,
        attribution_window_days=attribution_window_days,
        base_url=url,
    )
    campaign_stats_res.apply_hints(
        primary_key=["entity_id", "date"],
        columns={"date": {"data_type": "date"}},
    )

    line_item_stats_res = line_item_stats_resource(
        session=session,
        account_id=account_id,
        attribution_window_days=attribution_window_days,
        base_url=url,
    )
    line_item_stats_res.apply_hints(
        primary_key=["entity_id", "date"],
        columns={"date": {"data_type": "date"}},
    )

    promoted_tweet_stats_res = promoted_tweet_stats_resource(
        session=session,
        account_id=account_id,
        attribution_window_days=attribution_window_days,
        base_url=url,
    )
    promoted_tweet_stats_res.apply_hints(
        primary_key=["entity_id", "date"],
        columns={"date": {"data_type": "date"}},
    )

    all_resources: list[DltResource] = [
        accounts_res,
        campaigns_res,
        line_items_res,
        promoted_tweets_res,
        funding_instruments_res,
        media_creatives_res,
        scheduled_promoted_tweets_res,
        tailored_audiences_res,
        targeting_criteria_res,
        campaign_stats_res,
        line_item_stats_res,
        promoted_tweet_stats_res,
    ]

    if resources:
        return [r for r in all_resources if r.name in resources]
    return all_resources

# Yahoo Ads Search

A [dlt](https://dlthub.com) source for [Yahoo Japan Ads Search API](https://ads-developers.yahoo.co.jp/reference/ads-search-api/) (LY Ads Search Ads, formerly Yahoo! JAPAN Ads SS).

Covers 27 entity resources and 1 configurable report resource with 16 report types.

## Installation

```bash
pip install dlt-community-sources[yahoo-ads-search]
```

## Usage

```python
import dlt
from dlt_community_sources.yahoo_ads_search import yahoo_ads_search_source

pipeline = dlt.pipeline(
    pipeline_name="yahoo_ads_search",
    destination="bigquery",
    dataset_name="source_yahoo_ads_search",
)

source = yahoo_ads_search_source(
    client_id="YOUR_CLIENT_ID",
    client_secret="YOUR_CLIENT_SECRET",
    refresh_token="YOUR_REFRESH_TOKEN",
    account_id="YOUR_ACCOUNT_ID",
)

load_info = pipeline.run(source)
```

### Load specific resources

```python
source = yahoo_ads_search_source(
    client_id="YOUR_CLIENT_ID",
    client_secret="YOUR_CLIENT_SECRET",
    refresh_token="YOUR_REFRESH_TOKEN",
    account_id="YOUR_ACCOUNT_ID",
    resources=["campaigns", "ad_groups", "report"],
)
```

### Custom report

```python
source = yahoo_ads_search_source(
    client_id="YOUR_CLIENT_ID",
    client_secret="YOUR_CLIENT_SECRET",
    refresh_token="YOUR_REFRESH_TOKEN",
    account_id="YOUR_ACCOUNT_ID",
    report_type="KEYWORDS",
    report_fields=["DAY", "KEYWORD", "IMPS", "CLICKS", "COST"],
    attribution_window_days=7,
)
```

## Resources

### Entity Resources (27)

| Resource | Write Disposition | Description |
|---|---|---|
| `accounts` | merge | Account details |
| `campaigns` | merge | Campaigns |
| `ad_groups` | merge | Ad groups |
| `ads` | merge | Ads |
| `ad_group_criterions` | merge | Ad group targeting criteria |
| `campaign_criterions` | merge | Campaign negative criteria |
| `bidding_strategies` | merge | Auto bidding strategies |
| `campaign_budgets` | merge | Shared budgets |
| `labels` | merge | Labels |
| `assets` | merge | Assets |
| `audience_lists` | merge | Audience lists |
| `conversion_trackers` | merge | Conversion trackers |
| `account_shared` | merge | Shared keyword lists |
| `ad_group_bid_multipliers` | replace | Bid adjustment rates |
| `campaign_targets` | replace | Campaign targeting settings |
| `page_feed_asset_sets` | merge | Page feed asset sets |
| `account_assets` | merge | Account-level assets |
| `campaign_assets` | merge | Campaign-level assets |
| `ad_group_assets` | merge | Ad group-level assets |
| `customizer_attributes` | merge | Customizer attributes |
| `account_tracking_urls` | replace | Account tracking URL settings |
| `ab_tests` | merge | A/B tests |
| `seasonality_adjustments` | merge | Seasonality adjustments |
| `learning_data_exclusions` | merge | Learning data exclusions |
| `conversion_groups` | merge | Conversion groups |
| `campaign_audience_lists` | replace | Campaign audience settings |
| `ad_group_audience_lists` | replace | Ad group audience settings |

### Report Resource

| Resource | Write Disposition | Incremental | Description |
|---|---|---|---|
| `report` | merge | by DAY | Configurable performance report |

Report types: `ACCOUNT`, `CAMPAIGN`, `ADGROUP`, `AD`, `KEYWORDS`, `SEARCH_QUERY`, `GEO`, `FEED_ITEM`, `GEO_TARGET`, `SCHEDULE_TARGET`, `DEVICE_TARGET`, `AD_CUSTOMIZERS`, `BID_STRATEGY`, `TARGET_LIST`, `LANDING_PAGE_URL`, `KEYWORDLESS_QUERY`

## Authentication

Yahoo Ads uses OAuth 2.0 Authorization Code Grant.

1. Register an app at [Yahoo! Ads Developer Center](https://ads-developers.yahoo.co.jp/)
2. Complete the OAuth consent flow to obtain a refresh token
3. The refresh token remains valid as long as it's used within 4 weeks (daily pipeline runs prevent expiry)

## Configuration

| Parameter | Default | Description |
|---|---|---|
| `client_id` | (required) | Yahoo Ads API client ID |
| `client_secret` | (required) | Yahoo Ads API client secret |
| `refresh_token` | (required) | OAuth refresh token |
| `account_id` | (required) | Yahoo Ads account ID |
| `report_type` | `CAMPAIGN` | Report type |
| `report_fields` | `None` | Custom report fields (defaults per report type) |
| `attribution_window_days` | `7` | Days to re-fetch for attribution window |
| `resources` | `None` | Resource names to load (None for all) |
| `start_date` | `None` | Override incremental start date (YYYY-MM-DD) |

## Notes

- **POST RPC style**: All endpoints use POST with JSON body (not REST GET).
- **Pagination**: Uses `startIndex` (1-based) and `numberResults` for offset pagination.
- **Report primary key**: Dynamically derived from report fields — all non-metric fields become the composite primary key.
- **Type conversion**: Report CSV values `--` are converted to `None`, numeric fields to int/float.
- **Permission errors**: Resources returning 403/404 are silently skipped.

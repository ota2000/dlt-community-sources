# Yahoo Ads Display

A dlt source for [Yahoo Japan Ads Display API](https://ads-developers.yahoo.co.jp/reference/ads-display-api/) (LY Ads Display Ads, formerly Yahoo! JAPAN Ads YDA).

Covers 29 entity resources and 1 configurable report resource with 14 report types. Includes LINE placement data (YDA serves ads on LINE surfaces).

## Installation

```bash
pip install dlt-community-sources[yahoo-ads-display]
```

## Usage

```python
import dlt
from dlt_community_sources.yahoo_ads_display import yahoo_ads_display_source

pipeline = dlt.pipeline(
    pipeline_name="yahoo_ads_display",
    destination="bigquery",
    dataset_name="source_yahoo_ads_display",
)

source = yahoo_ads_display_source(
    client_id="YOUR_CLIENT_ID",
    client_secret="YOUR_CLIENT_SECRET",
    refresh_token="YOUR_REFRESH_TOKEN",
    account_id="YOUR_ACCOUNT_ID",
)

load_info = pipeline.run(source)
```

### LINE placement report

Use `PLACEMENT_TARGET` report type to identify LINE placements:

```python
source = yahoo_ads_display_source(
    client_id="YOUR_CLIENT_ID",
    client_secret="YOUR_CLIENT_SECRET",
    refresh_token="YOUR_REFRESH_TOKEN",
    account_id="YOUR_ACCOUNT_ID",
    report_type="PLACEMENT_TARGET",
)
```

## Resources

### Entity Resources (29)

| Resource | Write Disposition | Description |
|---|---|---|
| `accounts` | merge | Account details |
| `campaigns` | merge | Campaigns |
| `ad_groups` | merge | Ad groups |
| `ads` | merge | Ads |
| `ad_group_targets` | replace | Targeting settings (geo, age, gender, device, placement, etc.) |
| `labels` | merge | Labels |
| `bidding_strategies` | merge | Portfolio bidding strategies |
| `campaign_budgets` | merge | Shared budgets |
| `audience_lists` | merge | Audience lists |
| `conversion_trackers` | merge | Conversion trackers |
| `conversion_groups` | merge | Conversion groups |
| `media` | merge | Image/media assets |
| `videos` | merge | Video assets |
| `feeds` | merge | Feeds (for dynamic ads) |
| `feed_sets` | merge | Feed sets |
| `placement_url_lists` | merge | Placement URL lists |
| `contents_keyword_lists` | merge | Content keyword lists |
| `retargeting_tags` | merge | Site retargeting tags |
| `account_authority` | replace | Account permissions/features |
| `account_tracking_urls` | replace | Account tracking URL settings |
| `ab_tests` | merge | A/B tests |
| `brand_lift` | merge | Brand lift studies |
| `guaranteed_campaigns` | merge | Guaranteed placement campaigns |
| `guaranteed_ad_groups` | merge | Guaranteed ad groups |
| `guaranteed_ads` | merge | Guaranteed ads |
| `balance` | replace | Account balance |
| `budget_orders` | merge | Budget orders |
| `account_links` | replace | MCC account links |
| `app_links` | merge | App conversion links |

### Report Resource

| Resource | Write Disposition | Incremental | Description |
|---|---|---|---|
| `report` | merge | by DAY | Configurable performance report |

Report types: `AD`, `APP`, `AUDIENCE_LIST_TARGET`, `CONTENT_KEYWORD_LIST`, `CONVERSION_PATH`, `CROSS_CAMPAIGN_REACHES`, `LABEL`, `MODEL_COMPARISON`, `PLACEMENT_TARGET`, `PORTFOLIO_BIDDING`, `REACH`, `SEARCH_TARGET`, `CAMPAIGN_BUDGET`, `URL`

## Authentication

Same as Yahoo Ads Search — see [yahoo_ads_search README](../yahoo_ads_search/README.md#authentication).

## Configuration

| Parameter | Default | Description |
|---|---|---|
| `client_id` | (required) | Yahoo Ads API client ID |
| `client_secret` | (required) | Yahoo Ads API client secret |
| `refresh_token` | (required) | OAuth refresh token |
| `account_id` | (required) | Yahoo Ads account ID |
| `report_type` | `AD` | Report type |
| `report_fields` | `None` | Custom report fields (defaults per report type) |
| `attribution_window_days` | `7` | Days to re-fetch for attribution window |
| `resources` | `None` | Resource names to load (None for all) |
| `start_date` | `None` | Override incremental start date (YYYY-MM-DD) |

## Notes

- **LINE placements**: YDA serves ads on LINE surfaces. Use `PLACEMENT_TARGET` report type with `PLACEMENT_URL_LIST_NAME` and `PLACEMENT_URL_LIST_TYPE` fields to identify LINE placement data.
- **POST RPC style**: All endpoints use POST with JSON body (not REST GET).
- **Report primary key**: Dynamically derived from report fields — all non-metric fields become the composite primary key.
- **Guaranteed campaigns**: Premium placement campaigns with reserved inventory (GuaranteedCampaignService).
- **Brand lift**: Brand lift measurement studies (BrandLiftService, display-only).

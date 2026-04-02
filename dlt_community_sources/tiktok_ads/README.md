# TikTok Ads

A dlt source for the [TikTok Marketing API](https://business-api.tiktok.com/portal/docs) (Business API).

## Installation

```bash
pip install "dlt-community-sources[tiktok-ads]"
```

## Usage

```python
import dlt
from dlt_community_sources.tiktok_ads import tiktok_ads_source
from dlt_community_sources.tiktok_ads.auth import refresh_access_token

# 1. Refresh token to get access_token (caller manages token lifecycle)
tokens = refresh_access_token(
    app_id="your_app_id",
    secret="your_app_secret",
    refresh_token="your_refresh_token",
)
access_token = tokens["access_token"]
# IMPORTANT: persist tokens["refresh_token"] to Secret Manager
# (TikTok rotates refresh_token on each use)

# 2. Run the source with access_token
pipeline = dlt.pipeline(
    pipeline_name="tiktok_ads",
    destination="bigquery",
    dataset_name="source_tiktok_ads",
)

source = tiktok_ads_source(
    access_token=access_token,
    advertiser_id="your_advertiser_id",
)

load_info = pipeline.run(source)
```

## Resources

| Resource | Write Disposition | Primary Key | Description |
|---|---|---|---|
| campaigns | merge | campaign_id | Campaign master data |
| ad_groups | merge | adgroup_id | Ad group master data |
| ads | merge | ad_id | Ad master data |
| custom_audiences | merge | custom_audience_id | DMP custom audiences |
| saved_audiences | merge | saved_audience_id | DMP saved audiences |
| creative_portfolios | merge | creative_portfolio_id | Creative portfolios |
| automated_rules | merge | rule_id | Automated optimization rules |
| authorized_advertiser_ids | replace | advertiser_id | Advertiser IDs authorized for this token (standalone, requires app_id/secret) |
| advertiser_info | merge | advertiser_id | Advertiser account info |
| advertiser_balance | replace | advertiser_id | Advertiser account balance |
| advertiser_transactions | append | — | Advertiser account transactions (incremental) |
| apps | merge | app_id | Apps associated with the advertiser |
| videos | merge | video_id | Ad video assets (via search) |
| rule_results | append | — | Automated rule execution results |
| report | merge | stat_time_day + entity_id (varies by data_level) | Daily performance report |

## Authentication

TikTok uses **OAuth 2.0 Authorization Code Grant** with token rotation:

1. Register an app at [TikTok for Business](https://business-api.tiktok.com/portal)
2. Obtain `auth_code` via browser authorization
3. Exchange for `access_token` + `refresh_token`
4. **Important**: `access_token` expires in 24 hours; `refresh_token` in 365 days
5. Each refresh returns a **new** `refresh_token` (must persist the new one)

The API uses `Access-Token` header (not `Authorization: Bearer`).

## Notes

- Reports are fetched in 30-day chunks (API limitation)
- Report dimensions/metrics response is nested (`{dimensions: {}, metrics: {}}`) and flattened automatically
- Metrics are converted from strings to numeric types (int/Decimal)
- Attribution window: re-fetches last 7 days by default (configurable)
- TikTok API returns HTTP 200 for errors (`code != 0`) — handled gracefully
- 429 rate limit retry handled by dlt's built-in HTTP client
- Images: TikTok API has no list-all endpoint (`file/image/ad/info/` requires specific `image_ids`). Image metadata is available via ad creatives.
- Default API version: v1.3

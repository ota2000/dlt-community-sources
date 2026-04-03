# X (Twitter) Ads

A dlt source for the [X (Twitter) Ads API](https://developer.x.com/en/docs/x-ads-api).

## Installation

```bash
pip install "dlt-community-sources[x-ads]"
```

## Usage

```python
import dlt
from dlt_community_sources.x_ads import x_ads_source

pipeline = dlt.pipeline(
    pipeline_name="x_ads",
    destination="bigquery",
    dataset_name="source_x_ads",
)

source = x_ads_source(
    consumer_key="your_consumer_key",
    consumer_secret="your_consumer_secret",
    access_token="your_access_token",
    access_token_secret="your_access_token_secret",
    account_id="your_account_id",
)

load_info = pipeline.run(source)
```

### Load specific resources

```python
source = x_ads_source(
    consumer_key="your_consumer_key",
    consumer_secret="your_consumer_secret",
    access_token="your_access_token",
    access_token_secret="your_access_token_secret",
    account_id="your_account_id",
    resources=["campaigns", "campaign_stats"],
)
```

### Customize attribution window

```python
source = x_ads_source(
    consumer_key="your_consumer_key",
    consumer_secret="your_consumer_secret",
    access_token="your_access_token",
    access_token_secret="your_access_token_secret",
    account_id="your_account_id",
    attribution_window_days=14,
)
```

## Resources

| Resource | Write Disposition | Primary Key | Description |
|---|---|---|---|
| accounts | merge | id | Ad account master data |
| campaigns | merge | id | Campaign master data |
| line_items | merge | id | Line item (ad group) master data |
| promoted_tweets | merge | id | Promoted tweet master data |
| funding_instruments | merge | id | Funding instrument master data |
| media_creatives | merge | id | Media creative master data |
| scheduled_promoted_tweets | merge | id | Scheduled promoted tweet master data |
| tailored_audiences | merge | id | Tailored audience (custom audience) master data |
| targeting_criteria | merge | id | Targeting criteria for line items |
| campaign_stats | merge | entity_id + date | Daily campaign performance metrics |
| line_item_stats | merge | entity_id + date | Daily line item performance metrics |
| promoted_tweet_stats | merge | entity_id + date | Daily promoted tweet performance metrics |

## Authentication

X Ads API uses **OAuth 1.0a** authentication. You need four credentials:

1. **Consumer Key** (API Key) - from the [X Developer Portal](https://developer.x.com/)
2. **Consumer Secret** (API Secret) - from the Developer Portal
3. **Access Token** - generated for your X Ads account
4. **Access Token Secret** - generated with the access token

### Steps

1. Create a project and app at the [X Developer Portal](https://developer.x.com/)
2. Apply for Ads API access
3. Generate consumer key/secret and access token/secret
4. Set the `account_id` for the ad account you want to access

## Notes

- Master data uses cursor-based pagination (up to 1000 items per page)
- Stats use the synchronous Stats API with DAY granularity
- Stats entity_ids are batched in groups of 20 (API limit)
- Stats requests are limited to 90-day windows per request
- Attribution window: by default, re-fetches the last 7 days on each run
- Monetary values from Stats API are returned in micro-currency (1/1,000,000) and automatically converted to standard units
- Rate limiting: 429 responses are handled with Retry-After header support
- 400/403/404 responses are skipped gracefully
- Targeting criteria requires fetching line item IDs first
- Default API version: v12

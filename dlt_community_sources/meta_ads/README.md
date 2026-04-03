# Meta (Facebook) Ads

A dlt source for the [Meta Marketing API](https://developers.facebook.com/docs/marketing-apis).

## Installation

```bash
pip install "dlt-community-sources[meta-ads]"
```

## Usage

```python
import dlt
from dlt_community_sources.meta_ads import meta_ads_source

pipeline = dlt.pipeline(
    pipeline_name="meta_ads",
    destination="bigquery",
    dataset_name="source_meta_ads",
)

source = meta_ads_source(
    access_token="your_system_user_token",
    account_id="act_123456789",
)

load_info = pipeline.run(source)
```

### Load specific resources

```python
source = meta_ads_source(
    access_token="your_token",
    account_id="act_123456789",
    resources=["campaigns", "insights"],
)
```

### Customize insights

```python
source = meta_ads_source(
    access_token="your_token",
    account_id="act_123456789",
    level="campaign",
    time_increment=1,
    breakdowns=["age", "gender"],
    action_breakdowns=["action_type"],
    attribution_window_days=7,
    start_date="2025-01-01",
)
```

### Override default fields

```python
source = meta_ads_source(
    access_token="your_token",
    account_id="act_123456789",
    custom_fields={"campaigns": ["id", "name", "status"]},
)
```

## Resources

| Resource | Write Disposition | Primary Key | Description |
|---|---|---|---|
| ad_accounts | merge | id | Ad account information |
| ad_labels | merge | id | Ad label management |
| campaigns | merge | id | Campaign master data |
| ad_sets | merge | id | Ad set master data |
| ads | merge | id | Ad master data |
| ad_creatives | merge | id | Ad creative master data |
| custom_audiences | merge | id | Custom audience segments |
| custom_conversions | merge | id | Custom conversion definitions |
| ad_images | merge | hash | Ad image assets |
| ad_videos | merge | id | Ad video assets |
| activities | append | (none) | Account activity/change logs |
| saved_audiences | merge | id | Saved audience configurations |
| ad_leads | append | id | Lead form submissions (incremental) |
| insights | merge | date_start + date_stop + campaign_id/adset_id/ad_id (+ breakdowns) | Daily performance metrics (async report) |

## Authentication

Use a **System User Token** (recommended for server-to-server):

1. Go to [Business Manager](https://business.facebook.com/) → Settings → System Users
2. Create a System User and grant access to the ad account
3. Generate a token with `ads_read` scope

System User Tokens do not expire.

Alternatively, use a long-lived User Access Token (expires in ~60 days).

## Notes

- Insights are fetched via async reports (POST → poll → GET results)
- Attribution window: by default, re-fetches the last 28 days on each run to capture delayed conversions
- Insights metrics (`impressions`, `clicks`, `spend`, etc.) are automatically converted from strings to numeric types (`int` / `Decimal`). If Meta adds new numeric metrics, add them to `INSIGHT_INT_FIELDS` or `INSIGHT_FLOAT_FIELDS` in source.py, or handle conversion in your pipeline
- Insights primary key varies by level: `date_start` + `date_stop` + `campaign_id`/`adset_id`/`ad_id`
- When `breakdowns` are specified, they are appended to the primary key
- Rate limiting: Meta uses a scoring system per ad account. The source respects 429 responses with retry
- 403/404 responses are skipped gracefully (e.g., deleted campaigns)
- `ad_leads` iterates through all ads to fetch lead form submissions
- Default API version: v25.0

# Yahoo Ads Search

A [dlt](https://dlthub.com) source for [Yahoo Japan Ads Search API](https://ads-developers.yahoo.co.jp/reference/ads-search-api/) (LINEヤフー広告 検索広告, formerly Yahoo! JAPAN Ads SS).

41 entity resources and 1 configurable report resource with 22 report types.

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
    base_account_id="YOUR_MCC_ACCOUNT_ID",
    account_id="YOUR_AD_ACCOUNT_ID",
)

load_info = pipeline.run(source)
```

### Load specific resources

```python
source = yahoo_ads_search_source(
    base_account_id="YOUR_MCC_ACCOUNT_ID",
    account_id="YOUR_AD_ACCOUNT_ID",
    resources=["campaigns", "ad_groups", "report"],
)
```

### Custom report

```python
source = yahoo_ads_search_source(
    base_account_id="YOUR_MCC_ACCOUNT_ID",
    account_id="YOUR_AD_ACCOUNT_ID",
    report_type="KEYWORDS",
    report_fields=["DAY", "KEYWORD", "IMPS", "CLICKS", "COST"],
    attribution_window_days=7,
)
```

### Multiple accounts

The source loads data for a single account per invocation. For multiple accounts, use `discover_accounts` to list child accounts under an MCC and run a separate pipeline per account. This ensures each account has its own incremental cursor.

```python
import dlt
from dlt_community_sources.yahoo_ads_common import (
    discover_accounts,
    make_client,
    refresh_access_token,
)
from dlt_community_sources.yahoo_ads_search import yahoo_ads_search_source

tokens = refresh_access_token(client_id, client_secret, refresh_token)
client = make_client(tokens["access_token"], base_account_id)
accounts = discover_accounts(client, "https://ads-search.yahooapis.jp/api/v19")

for account_id in accounts:
    pipeline = dlt.pipeline(
        pipeline_name=f"yahoo_ads_{account_id}",
        destination="bigquery",
        dataset_name="source_yahoo_ads",
    )
    source = yahoo_ads_search_source(
        base_account_id=base_account_id,
        account_id=account_id,
    )
    pipeline.run(source)
```

`discover_accounts` returns SERVING accounts only. To load data for an ENDED account, pass its `account_id` explicitly.

## Resources

### Entity Resources (41)

| Resource | Write Disposition | Description |
|---|---|---|
| `accounts` | merge | Account details (MCC-level, empty body) |
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
| `audit_logs` | append | Change history logs |
| `seasonality_adjustments` | merge | Seasonality adjustments |
| `learning_data_exclusions` | merge | Learning data exclusions |
| `conversion_groups` | merge | Conversion groups |
| `campaign_audience_lists` | replace | Campaign audience settings |
| `ad_group_audience_lists` | replace | Ad group audience settings |
| `balance` | replace | Account balance |
| `budget_orders` | replace | Budget orders |
| `shared_criterions` | merge | Shared negative keywords |
| `campaign_shared_sets` | replace | Campaign-to-shared-list associations |
| `page_feed_assets` | merge | Page feed assets |
| `ad_group_webpages` | replace | Ad group webpage targeting |
| `campaign_webpages` | replace | Campaign webpage exclusions |
| `account_links` | replace | MCC account links (empty body) |
| `app_links` | merge | App conversion links |
| `account_customizers` | replace | Account-level customizer values |
| `campaign_customizers` | replace | Campaign-level customizer values |
| `ad_group_customizers` | replace | Ad group-level customizer values |
| `ad_group_criterion_customizers` | replace | Criterion-level customizer values |

### Report Resource

| Resource | Write Disposition | Incremental | Description |
|---|---|---|---|
| `report` | merge | by DAY | Configurable performance report |

Report types: `ACCOUNT`, `CAMPAIGN`, `ADGROUP`, `AD`, `KEYWORDS`, `SEARCH_QUERY`, `GEO`, `GEO_TARGET`, `SCHEDULE_TARGET`, `BID_STRATEGY`, `CAMPAIGN_TARGET_LIST`, `ADGROUP_TARGET_LIST`, `LANDING_PAGE_URL`, `KEYWORDLESS_QUERY`, `WEBPAGE_CRITERION`, `BID_MODIFIER`, `CAMPAIGN_ASSET`, `ADGROUP_ASSET`, `ACCOUNT_ASSET`, `RESPONSIVE_ADS_FOR_SEARCH_ASSET`, `ASSET_COMBINATIONS`, `CAMPAIGN_BUDGET`

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
| `base_account_id` | (required) | MCC account ID (used in `x-z-base-account-id` header) |
| `account_id` | (required) | Child account ID to load data from |
| `report_type` | `CAMPAIGN` | Report type |
| `report_fields` | `None` | Custom report fields. If omitted, all available fields are fetched dynamically via `getReportFields` API |
| `attribution_window_days` | `7` | Days to re-fetch for attribution window |
| `resources` | `None` | Resource names to load (None for all) |
| `start_date` | `None` | Override incremental start date (YYYY-MM-DD) |

## How it works

### Dynamic report fields

Report fields are not hardcoded. The source calls `getReportFields` API at runtime to discover all available fields for the given report type. When the API version changes and fields are added or removed, the source adapts automatically.

Fields that conflict with each other (`impossibleCombinationFields`) are resolved by greedily removing the field with the most conflicts, producing the largest conflict-free field set.

### Dynamic type conversion

Report CSV values are all strings. Type conversion uses `fieldType` metadata from `getReportFields`:

- `LONG` → `int`
- `DOUBLE` / `BID` → `Decimal`
- `STRING` / `ENUM` → as-is
- `--` or empty → `None`

### CSV column name mapping

Report CSV headers use display names (e.g., `Account ID`) rather than API field names (e.g., `ACCOUNT_ID`). The source builds a mapping from `displayFieldNameEn` in `getReportFields` and renames columns after download for a consistent schema.

### Report primary key

Primary key includes only core identity fields: `DAY`, `ACCOUNT_ID`, `CAMPAIGN_ID`, `ADGROUP_ID`, `AD_ID`, `KEYWORD_ID`, etc. This avoids non-nullable PK errors from optional fields.

### Body styles

Yahoo Ads API services use different request body structures. Each entity resource has an appropriate `body_style`:

- `standard`: `accountId` + `startIndex` + `numberResults` (most services)
- `account_ids`: `accountIds` array, no pagination
- `no_paging`: `accountId` only
- `empty`: no body params (MCC-level services)

### Error handling

- HTTP 400/403/404 responses are silently skipped (some services are unavailable depending on account permissions)
- Other HTTP errors are raised

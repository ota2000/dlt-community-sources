# Microsoft Ads

A dlt source for [Microsoft Advertising API](https://learn.microsoft.com/en-us/advertising/guides/).

Covers 55 resources across 5 API services: Campaign Management, Customer Management, Ad Insight, Customer Billing, and Reporting (37 report types).

## Installation

```bash
pip install dlt-community-sources[microsoft-ads]
```

## Usage

```python
import dlt
from dlt_community_sources.microsoft_ads import microsoft_ads_source

pipeline = dlt.pipeline(
    pipeline_name="microsoft_ads",
    destination="bigquery",
    dataset_name="source_microsoft_ads",
)

source = microsoft_ads_source(
    client_id="YOUR_CLIENT_ID",
    client_secret="YOUR_CLIENT_SECRET",
    developer_token="YOUR_DEVELOPER_TOKEN",
    refresh_token="YOUR_REFRESH_TOKEN",
    account_id="YOUR_ACCOUNT_ID",
    customer_id="YOUR_CUSTOMER_ID",
)

load_info = pipeline.run(source)
```

### Load specific resources

```python
source = microsoft_ads_source(
    client_id="YOUR_CLIENT_ID",
    client_secret="YOUR_CLIENT_SECRET",
    developer_token="YOUR_DEVELOPER_TOKEN",
    refresh_token="YOUR_REFRESH_TOKEN",
    account_id="YOUR_ACCOUNT_ID",
    customer_id="YOUR_CUSTOMER_ID",
    resources=["campaigns", "ad_groups", "report"],
)
```

### Custom report

```python
source = microsoft_ads_source(
    client_id="YOUR_CLIENT_ID",
    client_secret="YOUR_CLIENT_SECRET",
    developer_token="YOUR_DEVELOPER_TOKEN",
    refresh_token="YOUR_REFRESH_TOKEN",
    account_id="YOUR_ACCOUNT_ID",
    customer_id="YOUR_CUSTOMER_ID",
    report_type="KeywordPerformanceReportRequest",
    report_columns=["TimePeriod", "Keyword", "Impressions", "Clicks", "Spend"],
    aggregation="Daily",
    attribution_window_days=7,
)
```

> **Note:** The report primary key includes fields like `AccountId`, `CampaignId`, etc. Make sure your custom `report_columns` includes these fields for correct merge behavior.

## Resources

### Campaign Management (33 resources)

| Resource | Write Disposition | Description |
|---|---|---|
| `campaigns` | merge | Campaigns (GetCampaignsByAccountId) |
| `ad_groups` | merge | Ad groups per campaign |
| `ads` | merge | Ads per ad group |
| `keywords` | merge | Keywords per ad group |
| `ad_extensions` | merge | Ad extensions (all types) |
| `ad_extension_associations` | replace | Ad extension to campaign associations |
| `campaign_criterions` | merge | Campaign targeting criterions |
| `ad_group_criterions` | merge | Ad group targeting criterions |
| `audiences` | merge | Remarketing/custom audiences |
| `audience_groups` | merge | Audience groups |
| `asset_groups` | merge | Asset groups per campaign |
| `conversion_goals` | merge | Conversion goals |
| `conversion_value_rules` | merge | Conversion value rules |
| `uet_tags` | merge | UET tags |
| `offline_conversion_reports` | append | Offline conversions |
| `labels` | merge | Labels |
| `label_associations_by_entity` | replace | Label to entity associations |
| `budgets` | merge | Shared budgets |
| `bid_strategies` | merge | Bid strategies |
| `negative_keywords` | replace | Shared negative keyword lists |
| `negative_sites_campaigns` | replace | Negative sites per campaign |
| `shared_entities` | merge | Shared entity lists |
| `shared_list_items` | replace | Items in shared lists |
| `media` | merge | Media library |
| `videos` | merge | Video library |
| `account_properties` | replace | Account properties |
| `account_migration_statuses` | replace | Account migration statuses |
| `seasonality_adjustments` | merge | Seasonality adjustments |
| `data_exclusions` | merge | Data exclusions |
| `experiments` | merge | Experiments |
| `import_jobs` | merge | Import jobs |
| `bmc_stores` | merge | BMC store associations |
| `brand_kits` | merge | Brand kits |

### Customer Management (10 resources)

| Resource | Write Disposition | Description |
|---|---|---|
| `account_info` | merge | Account details (GetAccount) |
| `accounts_info` | merge | Accounts summary (GetAccountsInfo) |
| `customer_info` | merge | Customer details (GetCustomer) |
| `customers_info` | merge | Customers summary (GetCustomersInfo) |
| `current_user` | replace | Current API user (GetCurrentUser) |
| `users_info` | merge | Users in customer (GetUsersInfo) |
| `customer_pilot_features` | replace | Customer pilot features |
| `account_pilot_features` | replace | Account pilot features |
| `linked_accounts_and_customers` | replace | Linked accounts and customers |
| `notifications` | append | Notifications |

### Ad Insight (8 resources)

| Resource | Write Disposition | Description |
|---|---|---|
| `auction_insight_data` | replace | Auction insight data |
| `bid_opportunities` | replace | Bid optimization opportunities |
| `budget_opportunities` | replace | Budget optimization opportunities |
| `keyword_opportunities` | replace | Keyword opportunities |
| `recommendations` | replace | Account recommendations |
| `performance_insights` | replace | Performance insights |
| `keyword_idea_categories` | replace | Keyword idea categories |
| `auto_apply_opt_in_status` | replace | Auto-apply recommendations opt-in status |

### Customer Billing (3 resources)

| Resource | Write Disposition | Description |
|---|---|---|
| `account_monthly_spend` | replace | Account monthly spend |
| `billing_documents_info` | append | Billing documents |
| `insertion_orders` | merge | Insertion orders |

### Reporting (1 resource, 37 report types)

| Resource | Write Disposition | Incremental | Description |
|---|---|---|---|
| `report` | merge | by TimePeriod | Configurable performance report |

Supported report types include: `CampaignPerformanceReportRequest`, `AdGroupPerformanceReportRequest`, `AdPerformanceReportRequest`, `KeywordPerformanceReportRequest`, and 33 more. See `REPORT_TYPES` in `resources/reporting.py` for the full list.

## Authentication

Microsoft Advertising uses OAuth 2.0 Authorization Code Grant via Microsoft Entra ID (Azure AD).

### Prerequisites

1. Register an app in [Azure Portal](https://portal.azure.com/#blade/Microsoft_AAD_RegisteredApps/ApplicationsListBlade)
2. Request a [Developer Token](https://learn.microsoft.com/en-us/advertising/guides/get-started#get-developer-token)
3. Complete the OAuth consent flow to obtain a refresh token

### Token rotation

The refresh token is rotated on each API call. The caller is responsible for persisting the new refresh token returned in the token response. For Cloud Run Jobs, store the token in Secret Manager.

## Configuration

| Parameter | Default | Description |
|---|---|---|
| `client_id` | (required) | Azure AD app client ID |
| `client_secret` | (required) | Azure AD app client secret |
| `developer_token` | (required) | Microsoft Advertising developer token |
| `refresh_token` | (required) | OAuth refresh token |
| `account_id` | (required) | Microsoft Advertising account ID |
| `customer_id` | (required) | Microsoft Advertising customer ID |
| `report_type` | `CampaignPerformanceReportRequest` | Report type |
| `report_columns` | `None` | Custom report columns (defaults per report type) |
| `aggregation` | `Daily` | Report aggregation (Daily, Weekly, Monthly) |
| `attribution_window_days` | `7` | Days to re-fetch for attribution window |
| `resources` | `None` | Resource names to load (None for all) |
| `start_date` | `None` | Override incremental start date (YYYY-MM-DD) |

## Notes

- **POST RPC style**: Microsoft Advertising API uses POST requests with JSON bodies (not REST). Each operation maps to a specific URL path.
- **Permission errors**: Resources that return 403 or 404 are silently skipped via `safe_rpc`.
- **Report polling**: Reports are submitted asynchronously and polled until completion (max 10 minutes). The response is a ZIP-compressed CSV.
- **Type conversion**: Report CSV values are automatically converted to int/float for numeric fields (Impressions, Clicks, Spend, etc.).
- **Primary key**: Report primary key is dynamically set based on report type (e.g., CampaignPerformanceReportRequest includes CampaignId).

## API Coverage

This source covers all Microsoft Advertising GET operations that can be called without entity-specific input parameters (i.e., operations suitable for periodic ETL). The following operations are intentionally excluded:

| Operation | Service | Reason |
|---|---|---|
| GetKeywordIdeas | Ad Insight | Requires seed keywords as input (tool-like usage) |
| GetKeywordTrafficEstimates | Ad Insight | Requires keyword list as input |
| GetHistoricalKeywordPerformance | Ad Insight | Requires keyword list as input |
| GetHistoricalSearchCount | Ad Insight | Requires keyword list as input |
| GetUser | Customer Management | Requires specific UserID; use `users_info` for listing |
| GetMediaAssociations | Campaign Management | Requires specific MediaIds from `media` resource |
| GetImportResults | Campaign Management | Requires specific ImportJobId from `import_jobs` resource |
| GetAssetGroupListingGroupsByIds | Campaign Management | Requires specific AssetGroupIds |
| GetBillingDocuments | Customer Billing | Requires DocumentIds from `billing_documents_info`; returns PDF/XML binary data |

These operations require entity-specific IDs or seed inputs, making them unsuitable for a general-purpose ETL source. If you need these, call the Microsoft Advertising API directly with the required parameters.

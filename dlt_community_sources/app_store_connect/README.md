# App Store Connect

A dlt source for [Apple App Store Connect API](https://developer.apple.com/documentation/appstoreconnectapi).

## Installation

```bash
pip install dlt-community-sources[app-store-connect]
```

## Usage

```python
from pathlib import Path

import dlt
from dlt_community_sources.app_store_connect import app_store_connect_source

pipeline = dlt.pipeline(
    pipeline_name="app_store_connect",
    destination="bigquery",
    dataset_name="source_app_store_connect",
)

source = app_store_connect_source(
    key_id="YOUR_KEY_ID",
    issuer_id="YOUR_ISSUER_ID",
    private_key=Path("AuthKey_XXXXX.p8").read_text(),
)

load_info = pipeline.run(source)
```

### With sales/finance reports

`vendor_number` is only needed for `sales_reports` and `finance_reports`. Without it, those two resources are silently skipped and all other resources load normally.

```python
source = app_store_connect_source(
    key_id="YOUR_KEY_ID",
    issuer_id="YOUR_ISSUER_ID",
    private_key=Path("AuthKey_XXXXX.p8").read_text(),
    vendor_number="YOUR_VENDOR_NUMBER",
)
```

### Load specific resources

```python
source = app_store_connect_source(
    key_id="YOUR_KEY_ID",
    issuer_id="YOUR_ISSUER_ID",
    private_key=Path("AuthKey_XXXXX.p8").read_text(),
    resources=["apps", "builds", "users"],
)
```

## Resources

| Resource | Write Disposition | Incremental | Description |
|---|---|---|---|
| `apps` | merge | - | App metadata |
| `app_store_versions` | merge | - | App Store version history |
| `builds` | merge | - | Build information |
| `beta_testers` | merge | - | TestFlight testers |
| `beta_groups` | merge | - | TestFlight groups |
| `bundle_ids` | merge | - | Bundle identifiers |
| `certificates` | merge | - | Signing certificates |
| `devices` | merge | - | Registered devices |
| `in_app_purchases` | merge | - | In-app purchase products |
| `subscriptions` | merge | - | Subscription products |
| `subscription_groups` | merge | - | Subscription groups |
| `users` | merge | - | Team members |
| `user_invitations` | merge | - | Pending user invitations |
| `app_categories` | replace | - | App Store categories |
| `territories` | replace | - | Available territories |
| `pre_release_versions` | merge | - | TestFlight pre-release versions |
| `beta_app_review_submissions` | merge | - | Beta app review submissions |
| `beta_build_localizations` | merge | - | Beta build localizations |
| `beta_app_localizations` | merge | - | Beta app localizations |
| `beta_license_agreements` | merge | - | Beta license agreements |
| `build_beta_details` | merge | - | Build beta details |
| `app_encryption_declarations` | merge | - | App encryption declarations |
| `provisioning_profiles` | merge | - | Provisioning profiles |
| `review_submissions` | merge | - | App review submissions |
| `sales_reports` | append | daily | Sales and trends reports |
| `finance_reports` | append | monthly | Financial reports |
| `analytics_reports` | append | by processing date | Analytics reports |

`sales_reports` and `finance_reports` require `vendor_number`.

## Authentication

You need an API key from [App Store Connect](https://appstoreconnect.apple.com/access/integrations/api).

| Parameter | Description |
|---|---|
| `key_id` | The identifier for your API key |
| `issuer_id` | Your team's issuer ID |
| `private_key` | Contents of the `.p8` file downloaded when creating the key |
| `vendor_number` | Your vendor number (required for sales/finance reports) |

## Configuration

| Parameter | Default | Description |
|---|---|---|
| `base_url` | `None` | Override the API base URL (useful for testing) |
| `report_type` | `"SALES"` | Sales report type (e.g. SALES, PRE_ORDER, NEWSSTAND) |
| `report_sub_type` | `"SUMMARY"` | Sales report sub type (e.g. SUMMARY, DETAILED) |
| `frequency` | `"DAILY"` | Sales report frequency (e.g. DAILY, WEEKLY, MONTHLY, YEARLY) |
| `report_version` | `"1_0"` | Sales report version |
| `finance_report_type` | `"FINANCIAL"` | Finance report type |
| `region_code` | `"ZZ"` | Finance report region code (ZZ for all, US, JP, etc.) |
| `start_date` | `None` | Override incremental start date (YYYY-MM-DD) |

## Notes

- **Permission errors**: Resources that return 403 (e.g., due to API key role restrictions) are silently skipped. The pipeline continues with accessible resources.
- **Analytics reports**: Requires an existing `ONGOING` or `ONE_TIME_SNAPSHOT` analytics report request to be created in App Store Connect. Without it, `analytics_reports` returns empty.
- **vendor_number**: Without it, `sales_reports` and `finance_reports` are silently skipped. All other resources load normally.

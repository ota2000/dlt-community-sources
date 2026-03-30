# NextDNS

A dlt source for [NextDNS API](https://nextdns.github.io/api/).

## Installation

```bash
pip install dlt-community-sources[nextdns]
```

## Usage

```python
import dlt
from dlt_community_sources.nextdns import nextdns_source

pipeline = dlt.pipeline(
    pipeline_name="nextdns",
    destination="bigquery",
    dataset_name="source_nextdns",
)

source = nextdns_source(
    api_key="YOUR_API_KEY",
)

load_info = pipeline.run(source)
```

### Load specific profile

```python
source = nextdns_source(
    api_key="YOUR_API_KEY",
    profile_id="abc123",
    resources=["logs", "analytics_domains"],
)
```

## Resources

| Resource | Write Disposition | Incremental | Description |
|---|---|---|---|
| `profiles` | merge | - | NextDNS profiles |
| `logs` | append | by timestamp | DNS query logs |
| `analytics_status` | replace | - | Query count by status |
| `analytics_domains` | replace | - | Top queried domains |
| `analytics_blocked_domains` | replace | - | Top blocked domains |
| `analytics_reasons` | replace | - | Block reasons breakdown |
| `analytics_devices` | replace | - | Query count by device |
| `analytics_protocols` | replace | - | Query count by protocol |
| `analytics_destinations` | replace | - | Query count by destination country |
| `analytics_ips` | replace | - | Query count by client IP |
| `analytics_query_types` | replace | - | Query count by DNS query type |
| `analytics_ip_versions` | replace | - | Query count by IP version |
| `analytics_dnssec` | replace | - | Query count by DNSSEC status |
| `analytics_encryption` | replace | - | Query count by encryption status |
| `analytics_status_series` | replace | - | Status over time (30d) |
| `analytics_domains_series` | replace | - | Top domains over time (30d) |
| `analytics_devices_series` | replace | - | Devices over time (30d) |
| `analytics_protocols_series` | replace | - | Protocols over time (30d) |
| `analytics_destinations_series` | replace | - | Destinations over time (30d) |
| `analytics_encryption_series` | replace | - | Encryption over time (30d) |

## Authentication

Get your API key from [NextDNS Account](https://my.nextdns.io/account).

| Parameter | Description |
|---|---|
| `api_key` | NextDNS API key |
| `profile_id` | Optional. Profile ID to load. Loads all profiles if omitted. |

## Configuration

| Parameter | Default | Description |
|---|---|---|
| `base_url` | `None` | Override the API base URL (useful for testing) |
| `series_period` | `"-30d"` | Time period for analytics series (e.g. "-7d", "-90d") |
| `destinations_type` | `"countries"` | Destinations analytics type (e.g. "countries", "gafam") |
| `start_date` | `None` | Override incremental start date for logs (ISO 8601) |

## Notes

- **profile_id**: If omitted, all profiles on the account are automatically discovered and loaded.
- **Empty data**: Resources with no data return empty tables without errors. This is normal for new accounts or profiles with no DNS queries yet.
- **Time-series resources** (`*_series`): Fetch the last 30 days of data by default. Override with `series_period`.
- **Permission errors**: Resources that return 403 or 404 are silently skipped.

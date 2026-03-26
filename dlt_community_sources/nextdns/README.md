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

## Authentication

Get your API key from [NextDNS Account](https://my.nextdns.io/account).

| Parameter | Description |
|---|---|
| `api_key` | NextDNS API key |
| `profile_id` | Optional. Profile ID to load. Loads all profiles if omitted. |

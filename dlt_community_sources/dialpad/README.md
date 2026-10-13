# Dialpad

A dlt source for [Dialpad API](https://developers.dialpad.com/reference).

## Installation

```bash
pip install dlt-community-sources[dialpad]
```

## Usage

```python
import dlt
from dlt_community_sources.dialpad import dialpad_source

pipeline = dlt.pipeline(
    pipeline_name="dialpad",
    destination="bigquery",
    dataset_name="source_dialpad",
)

source = dialpad_source(
    api_key="YOUR_API_KEY",
    start_date="2026-09-01",
)

load_info = pipeline.run(source)
```

### Load specific resources

```python
source = dialpad_source(api_key="YOUR_API_KEY", resources=["calls", "users"])
```

### Restrict calls to one target

```python
source = dialpad_source(
    api_key="YOUR_API_KEY",
    target_id=123456789,
    target_type="department",
)
```

## Resources

| Resource | Write Disposition | Incremental | Description |
|---|---|---|---|
| `calls` | merge | by date_started | Concluded calls |
| `users` | merge | - | Users |
| `offices` | merge | - | Offices |
| `departments` | merge | - | Departments |
| `call_centers` | merge | - | Call centers |

## Authentication

The API key is sent as a Bearer token.

```python
source = dialpad_source(api_key="YOUR_API_KEY")
```

| Parameter | Description |
|---|---|
| `api_key` | Dialpad API key |

Create API keys in the Dialpad admin settings. `calls` needs a **company admin**
key with the `calls:list` scope; a user-level key cannot list calls.

## Configuration

| Parameter | Default | Description |
|---|---|---|
| `resources` | `None` | List of resource names to load. `None` for all |
| `base_url` | `None` | Override the API base URL (useful for testing) |
| `sandbox` | `False` | Route requests to `https://sandbox.dialpad.com`. Ignored when `base_url` is given |
| `start_date` | `"2020-01-01T00:00:00+00:00"` | Incremental start date for `calls` (ISO 8601) |
| `lookback_seconds` | `86400` | How far back to re-request calls on each run |
| `target_id` | `None` | Restrict `calls` to one target (department, user, ...) |
| `target_type` | `None` | Type of `target_id` (e.g. `"department"`, `"user"`) |

## Notes

- **Only concluded calls**: the Call List API never returns calls that are still
  ringing or connected, so the most recent calls appear on a later run.
- **Lookback overlap**: calls are filtered by `date_started`, so a call that was
  still connected when a run finished already sits behind the cursor that run
  recorded. Each run therefore re-requests the last `lookback_seconds` (one day
  by default); without it those calls would never be loaded. Lower it only if
  your calls are short and the extra reads matter.
- **`calls` uses merge, not append**: the lookback deliberately re-fetches rows,
  which `merge` on `call_id` collapses instead of duplicating.
- **Cursor format**: the API reports times as millisecond epochs, which do not
  compare correctly as strings. Each row gets an ISO 8601 `_cursor` field
  derived from `date_started`, and that is what the incremental tracks.
- **Numbers arrive as strings**: `call_id`, `master_call_id` and the `date_*`
  fields come back as JSON strings so 64-bit values survive JavaScript number
  precision, even though the API reference types them as integers. Cast before
  comparing them as numbers downstream.
- **`calls` fails loudly**: it is the primary data of this source, so request
  failures raise `PrimaryResourceTerminalError` / `PrimaryResourceTransientError`
  rather than being skipped, and a row without a usable `date_started` is an
  error instead of a silently misplaced row. Use `contains_terminal_exception`
  in retry predicates — dlt wraps resource exceptions.
- **Auxiliary resources skip client errors**: `users`, `offices`, `departments`
  and `call_centers` return 400/403/404 on accounts that lack the feature or the
  scope, and are skipped so the pipeline continues.
- **Rate limit**: 1200 requests per minute for the endpoints used here. 429
  responses are retried with backoff by dlt's HTTP client.
- **Personal data**: call records include the other party's phone number and,
  depending on your plan, transcriptions and recording links. Drop the fields
  you do not want to land in your warehouse with dlt's
  [`add_map`](https://dlthub.com/docs/general-usage/resource#filter-transform-and-pivot-data)
  before running the pipeline.

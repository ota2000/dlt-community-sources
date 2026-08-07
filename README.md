# dlt-community-sources

[![CI](https://github.com/ota2000/dlt-community-sources/actions/workflows/ci.yaml/badge.svg)](https://github.com/ota2000/dlt-community-sources/actions/workflows/ci.yaml)
[![PyPI](https://img.shields.io/pypi/v/dlt-community-sources)](https://pypi.org/project/dlt-community-sources/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)

Community-maintained [dlt](https://dlthub.com/) sources for various APIs. Install only what you need via extras.

## Available Sources

| Source | Extra | Description |
|---|---|---|
| [App Store Connect](dlt_community_sources/app_store_connect/) | `app-store-connect` | Apple App Store Connect API |
| [Twilio](dlt_community_sources/twilio/) | `twilio` | Twilio SMS, Calls, Usage Records |
| [Meta Ads](dlt_community_sources/meta_ads/) | `meta-ads` | Meta (Facebook) Ads campaigns, ad sets, ads, and insights |
| [NextDNS](dlt_community_sources/nextdns/) | `nextdns` | NextDNS logs and analytics |
| [Microsoft Ads](dlt_community_sources/microsoft_ads/) | `microsoft-ads` | Microsoft Advertising (Bing Ads) with certificate auth support |
| [TikTok Ads](dlt_community_sources/tiktok_ads/) | `tiktok-ads` | TikTok Marketing API (Business API) |
| [Yahoo Ads Search](dlt_community_sources/yahoo_ads_search/) | `yahoo-ads-search` | Yahoo Japan Ads Search API (SS) with MCC support |

## Installation

```bash
pip install dlt-community-sources[app-store-connect]
```

Or with uv:

```bash
uv add dlt-community-sources[app-store-connect]
```

## Usage

```python
import dlt
from dlt_community_sources.twilio import twilio_source

pipeline = dlt.pipeline(
    pipeline_name="twilio",
    destination="duckdb",
    dataset_name="twilio_data",
)

source = twilio_source(
    account_sid="your_account_sid",
    api_key_sid="your_api_key_sid",
    api_key_secret="your_api_key_secret",
    resources=["messages", "calls"],
)

load_info = pipeline.run(source)
print(load_info)
```

See each source's README for detailed usage and authentication options.

## Features

All sources share these common features:

- Incremental loading where applicable
- Automatic token/auth refresh
- Rate limit handling with exponential backoff
- Works with any [dlt destination](https://dlthub.com/docs/dlt-ecosystem/destinations/)

## Error handling

Sources distinguish two kinds of resources:

- **Auxiliary resources** (metadata: campaigns, ads, audiences, ...) skip
  expected client errors (HTTP 400/403/404) per endpoint — some APIs return
  4xx for valid requests on accounts without certain features. Every skip is
  logged with the response body.
- **Primary resources** (the fact data a source exists for: `report`,
  `insights`) never skip. Failures raise `PrimaryResourceTerminalError`
  (request rejected — retrying will not help; 4xx other than 408/429) or
  `PrimaryResourceTransientError` (timeout, rate limit, provider-side job
  failure), so a load never "succeeds" with silently missing data. Both mix
  in dlt's `TerminalException` / `TransientException`. Note that dlt wraps
  resource exceptions in `PipelineStepFailed` / `ResourceExtractionError`,
  so retry helpers that inspect only one level (such as
  `dlt.pipeline.helpers.retry_load`) cannot see the classification — use
  `contains_terminal_exception`, which walks the exception chain:

```python
from tenacity import Retrying, retry_if_exception, stop_after_attempt
from dlt_community_sources._utils import contains_terminal_exception

for attempt in Retrying(
    stop=stop_after_attempt(3),
    retry=retry_if_exception(lambda e: not contains_terminal_exception(e)),
    reraise=True,
):
    with attempt:
        pipeline.run(source)
```

To load a subset of resources, either pass the source-specific `resources`
argument or use dlt's native selection: `source.with_resources("report")`.

## Operating in production

Lessons from running these sources on daily schedules:

- **Isolate failures per account.** When one runner processes many
  accounts/advertisers (one `dlt.pipeline` per account), wrap each account —
  and each discovery call — in its own try/except, collect failures, and exit
  non-zero at the end. A single account's terminal failure (e.g. an account
  put on hold by the provider) should not abort the remaining accounts, and
  one discovery failure (e.g. an MCC your API user is not invited to yet)
  should not kill the whole job.
- **Tune connection retries for your network.** dlt's requests client retries
  `ConnectionError`/`Timeout` with `request_max_attempts=5` and
  `request_backoff_factor=1` by default — it gives up in roughly fifteen
  seconds. If your egress path has transient blackouts, raise them via dlt
  config (environment variables `RUNTIME__REQUEST_MAX_ATTEMPTS`,
  `RUNTIME__REQUEST_BACKOFF_FACTOR`, `RUNTIME__REQUEST_MAX_RETRY_DELAY`).
- **Alert on job failures.** With this library's fail-loud behavior, data
  loss shows up as a non-zero exit instead of a green run with missing rows —
  but only if something watches the exit status. Wire your scheduler's
  failure signal (e.g. Cloud Run Job `completed_execution_count{result:failed}`)
  to an alert before relying on it.

## Development

```bash
# Fork the repository first, then:
git clone https://github.com/YOUR_USERNAME/dlt-community-sources.git
cd dlt-community-sources
uv sync --group dev
uv run pytest -v
uv run ruff check .
```

## AI-assisted development

This repository is set up for AI coding assistants with two layers of AI context:

- **Project rules** (`.ai/rules.md`) — coding conventions, testing patterns, and structure specific to this repo
- **[dltHub AI workbench](https://dlthub.com/context/)** — dlt ecosystem knowledge, MCP server, and guided skills for pipeline development

Both are synced to tool-specific locations (Claude Code, Cursor, Codex, etc.) via a single command:

```bash
bash scripts/sync-ai-rules.sh
```

The dltHub workbench provides an MCP server for source search (9,700+ APIs), pipeline inspection, and data exploration — plus skills that guide you through the full pipeline development workflow. See [CONTRIBUTING.md](CONTRIBUTING.md#ai-assisted-development-recommended) for details.

Edit `.ai/` only for project rules, then re-run the sync script. CI checks that files are in sync.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT

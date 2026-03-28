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
| [NextDNS](dlt_community_sources/nextdns/) | `nextdns` | NextDNS logs and analytics |

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
- Graceful permission handling (skips inaccessible resources)
- Works with any [dlt destination](https://dlthub.com/docs/dlt-ecosystem/destinations/)

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

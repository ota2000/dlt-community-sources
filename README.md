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

This repository is set up for AI coding assistants. Rules and skills are managed in [`.ai/`](.ai/) as a single source of truth and synced to tool-specific locations via [`scripts/sync-ai-rules.sh`](scripts/sync-ai-rules.sh).

| File | Tool |
|---|---|
| `.ai/rules.md` | Source of truth |
| `CLAUDE.md` | Claude Code |
| `AGENTS.md` | OpenAI Codex CLI |
| `GEMINI.md` | Gemini CLI |
| `.cursor/rules/` | Cursor |
| `.github/copilot-instructions.md` | GitHub Copilot |
| `.clinerules/` | Cline |
| `.continue/rules/` | Continue |
| `.ai/skills/*.md` → `.claude/skills/`, `.gemini/skills/` | Skills |

Edit `.ai/` only, then run `bash scripts/sync-ai-rules.sh`. CI checks that files are in sync.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT

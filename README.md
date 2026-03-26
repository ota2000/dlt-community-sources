# dlt-community-sources

[![CI](https://github.com/ota2000/dlt-community-sources/actions/workflows/ci.yaml/badge.svg)](https://github.com/ota2000/dlt-community-sources/actions/workflows/ci.yaml)
[![PyPI](https://img.shields.io/pypi/v/dlt-community-sources)](https://pypi.org/project/dlt-community-sources/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)

Community-maintained [dlt](https://dlthub.com/) sources for various APIs. Install only what you need via extras.

## Available Sources

| Source | Extra | Description |
|---|---|---|
| [App Store Connect](docs/app_store_connect.md) | `app-store-connect` | Apple App Store Connect API (15 resources) |

## Installation

```bash
pip install dlt-community-sources[app-store-connect]
```

Or with uv:

```bash
uv add dlt-community-sources[app-store-connect]
```

## Quick start

### App Store Connect

```python
import dlt
from dlt_community_sources.app_store_connect import app_store_connect_source

pipeline = dlt.pipeline(
    pipeline_name="app_store_connect",
    destination="bigquery",  # or any dlt destination
    dataset_name="source_app_store_connect",
)

source = app_store_connect_source(
    key_id="YOUR_KEY_ID",
    issuer_id="YOUR_ISSUER_ID",
    private_key=open("AuthKey_XXXXX.p8").read(),
    vendor_number="YOUR_VENDOR_NUMBER",
)

load_info = pipeline.run(source)
print(load_info)
```

## Features

All sources share these common features:

- Incremental loading where applicable
- Automatic token/auth refresh
- Rate limit handling with exponential backoff
- Graceful permission handling (skips inaccessible resources)
- Works with any [dlt destination](https://dlthub.com/docs/dlt-ecosystem/destinations/)

## Development

```bash
git clone https://github.com/ota2000/dlt-community-sources.git
cd dlt-community-sources
uv sync --group dev
uv run pytest -v
uv run ruff check .
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT

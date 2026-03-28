---
name: add-source
description: "Add a new dlt source to dlt-community-sources. MUST activate when: adding a new API source, creating a new connector, 'ソース追加', 'add source'."
---

# Add a New Source

When asked to add a new source, follow these steps exactly.

## 1. Research the API

- Check official API documentation
- Confirm: free access, OpenAPI spec availability, authentication method
- Verify no existing dlt source (verified-sources, PyPI, GitHub)
- Identify all GET endpoints for data extraction

## 2. Create the source directory

```
dlt_community_sources/{name}/
├── __init__.py
├── source.py        # Declarative rest_api config + custom @dlt.resource functions
├── auth.py          # Custom auth class (if needed, e.g., JWT)
├── py.typed
├── README.md
└── tests/
    ├── __init__.py
    ├── test_source.py  # Config + helper tests
    └── test_auth.py    # Auth tests (if auth.py exists)
```

## 3. Implement source.py

### Declarative REST API config (preferred for standard endpoints)

```python
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig

def _rest_api_config(auth) -> RESTAPIConfig:
    return {
        "client": {
            "base_url": "https://api.example.com/v1/",
            "auth": auth,  # dict or AuthConfigBase instance
            "paginator": {"type": "json_link", "next_url_path": "links.next"},
        },
        "resource_defaults": {
            "primary_key": "id",
            "write_disposition": "merge",
            "endpoint": {
                "data_selector": "data",
                "response_actions": [
                    {"status_code": 403, "action": "ignore"},
                    {"status_code": 404, "action": "ignore"},
                ],
            },
        },
        "resources": [
            {"name": "items", "endpoint": {"path": "items"}},
            # Parent-child: {"name": "sub_items", "endpoint": {"path": "items/{resources.items.id}/sub"}},
        ],
    }
```

### Custom resources (only when rest_api can't handle it)

Use custom `@dlt.resource` functions for: non-JSON responses (TSV/gzip), complex incremental logic, custom response transformation.

```python
from dlt.sources.helpers import requests as req

def _make_client(auth) -> req.Client:
    client = req.Client()  # Automatic retry on 429/5xx
    client.session.auth = auth
    return client
```

### Source function

```python
@dlt.source(name="my_source")
def my_source(...) -> list[DltResource]:
    config = _rest_api_config(auth)
    rest_resources = rest_api_resources(config)
    custom_resources = [my_custom_resource(...)]
    all_resources: list[DltResource] = rest_resources + custom_resources
    if resources:
        return [r for r in all_resources if r.name in resources]
    return all_resources
```

## 4. Implement tests

- `test_source.py`: test `_rest_api_config()` dict (resource names, defaults, parent-child)
- Test custom resource functions and helper functions directly
- Do NOT mock through dlt decorators

## 5. Write README.md

Must include:
1. Installation (`pip install dlt-community-sources[{name}]`)
2. Usage (with recommended auth method)
3. Resources table (must match implementation exactly)
4. Authentication (parameters table)
5. Notes (edge cases, skip behavior, defaults)

## 6. Update project files

- `pyproject.toml`: add extra under `[project.optional-dependencies]`
- Root `README.md`: add row to Available Sources table
- `tests/test_integration.py`: add real API test (skipped without env vars)

## 7. Verify

```bash
uv run ruff format .
uv run ruff check .
uv run pytest -v
```

## 8. Sync AI rules if changed

```bash
bash scripts/sync-ai-rules.sh
```

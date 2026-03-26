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
├── client.py
├── source.py
├── py.typed
├── README.md
└── tests/
    ├── __init__.py
    ├── test_client.py
    └── test_source.py
```

## 3. Implement client.py

- Class with `__init__`, `_request`, `get`, `get_paginated`
- 429: exponential backoff retry (MAX_RETRIES=5)
- 403/404: graceful skip in `get_paginated`
- 400: raise (do not skip)
- Use `requests.Session` for connection reuse
- Pagination: follow the API's native pagination (cursor, next_page_uri, etc.)
- On pagination, preserve original params (do not overwrite with cursor only)

## 4. Implement source.py

- `@dlt.source` function with `dlt.secrets.value` for credentials
- `resources` parameter for filtering
- `@dlt.resource` for each endpoint
- `write_disposition`: merge for master data, append for logs, replace for analytics
- `primary_key` required for merge resources
- Incremental loading where the API supports date filtering
- Cursor field must be ISO 8601 (convert if API returns different format)
- Resource functions should be thin — extract helpers for logic

## 5. Implement tests

- `test_source.py`: `test_source_has_all_resources`, `test_resource_filtering`
- `test_client.py`: pagination, 429 retry, 403 skip
- Test all helper functions directly (date conversion, data transformation)
- Do NOT mock through dlt decorators

## 6. Write README.md

Must include:
1. Installation (`pip install dlt-community-sources[{name}]`)
2. Usage (with recommended auth method)
3. Resources table (must match implementation exactly)
4. Authentication (parameters table)
5. Notes (edge cases, skip behavior, defaults)

## 7. Update project files

- `pyproject.toml`: add extra under `[project.optional-dependencies]`
- Root `README.md`: add row to Available Sources table
- `tests/test_integration.py`: add real API test (skipped without env vars)

## 8. Verify

```bash
uv run ruff format .
uv run ruff check .
uv run pytest -v
```

## 9. Sync AI rules if changed

```bash
bash scripts/sync-ai-rules.sh
```

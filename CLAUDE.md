# dlt-community-sources AI Rules

The source of truth is `.ai/rules.md`. This content is copied to `CLAUDE.md`, `.cursor/rules/`, `.github/copilot-instructions.md`, `.clinerules/`, `.continue/rules/` by `scripts/sync-ai-rules.sh`. **Always edit `.ai/rules.md`, never edit the copies directly.**

## Project

dlt-community-sources is a PyPI package providing community-maintained dlt sources for various APIs. Each source is a separate module under `dlt_community_sources/`.

## Commands

```bash
uv sync --group dev          # Install dependencies
uv run pytest -v             # Run tests
uv run ruff check .          # Lint
uv run ruff format --check . # Format check
```

## Structure

```
dlt_community_sources/
├── {source_name}/
│   ├── __init__.py      # Exports the source function
│   ├── client.py        # HTTP client with auth, pagination, retry
│   ├── source.py        # @dlt.source and @dlt.resource definitions
│   ├── py.typed         # PEP 561 marker
│   ├── README.md        # Source documentation
│   └── tests/
│       ├── test_client.py  # Client unit tests
│       └── test_source.py  # Source structure + helper tests
└── tests/
    └── test_integration.py # Real API tests (skipped without env vars)
```

## Conventions

### Error Handling

- 429: retry with exponential backoff (all clients)
- 403/404: skip gracefully with warning log (all clients)
- 400: raise — it indicates a bug, do not silently skip
- 401: refresh token and retry (App Store Connect only)

### Incremental Loading

- Use `dlt.sources.incremental` with a cursor field that sorts correctly as strings
- Cursor must be ISO 8601 format — never RFC 2822
- Twilio dates are RFC 2822 — convert via `_rfc2822_to_iso` before using as cursor
- NextDNS logs use Unix ms `from` parameter — convert via `_iso_to_unix_ms`

### Write Dispositions

- `merge` with `primary_key`: master data (apps, users, accounts, etc.)
- `append`: logs, events, time-series data
- `replace`: analytics snapshots that refresh each run

### Code Organization

- Resource functions are thin — business logic goes in helpers
- Helpers: `_flatten_series`, `_iso_to_unix_ms`, `_rfc2822_to_iso`, `_date_range`, `_month_range`
- Each source has its own client class with `_request`, `get`, `get_paginated`

### Testing

- Do NOT mock through dlt decorators — test helpers directly
- Test structure: `test_source_has_all_resources` + `test_resource_filtering` for every source
- Test helpers with unit tests (date conversion, series flattening, etc.)
- Integration tests in `tests/test_integration.py` — skipped without env vars

### Documentation

All source READMEs must have:
1. Installation
2. Usage (with recommended auth method)
3. Resources table (matching implementation exactly)
4. Authentication
5. Notes (edge cases, skip behavior, defaults)

### Versioning

- patch: bug fix, new resources to existing source
- minor: new source
- major: breaking change

## Adding a New Source

1. Create `dlt_community_sources/{name}/`
2. Implement `client.py`, `source.py`, `__init__.py`, `py.typed`
3. Add tests in `{name}/tests/`
4. Add `README.md` with all required sections
5. Add extra in `pyproject.toml` under `[project.optional-dependencies]`
6. Add row to root `README.md` Available Sources table
7. Add integration test in `tests/test_integration.py`
8. Run `uv run pytest -v && uv run ruff check . && uv run ruff format --check .`

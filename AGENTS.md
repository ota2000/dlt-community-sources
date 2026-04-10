# dlt-community-sources AI Rules

The source of truth is `.ai/rules.md`. This content is copied to `CLAUDE.md`, `.cursor/rules/`, `.github/copilot-instructions.md`, `.clinerules/`, `.continue/rules/` by `scripts/sync-ai-rules.sh`. **Always edit `.ai/rules.md`, never edit the copies directly.**

## Project

dlt-community-sources is a PyPI package providing community-maintained dlt sources for various APIs. Each source is a separate module under `dlt_community_sources/`.

## Commands

```bash
uv sync --group dev          # Install dependencies
uv run pytest -v             # Run tests (includes coverage check, threshold 70%)
uv run ruff check .          # Lint
uv run ruff format --check . # Format check
uv run pre-commit install    # Install pre-commit hooks (ruff, uv.lock, AI rules sync)
```

## Structure

```
dlt_community_sources/
├── {source_name}/
│   ├── __init__.py      # Exports the source function
│   ├── source.py        # Declarative rest_api config + custom @dlt.resource functions
│   ├── auth.py          # Custom auth class (if needed, e.g., JWT)
│   ├── py.typed         # PEP 561 marker
│   ├── README.md        # Source documentation
│   └── tests/
│       ├── test_source.py  # Config + helper tests
│       └── test_auth.py    # Auth tests (if auth.py exists)
└── tests/
    └── test_integration.py # Real API tests (skipped without env vars)
```

## Conventions

### Error Handling

- 429: retry with exponential backoff (handled by rest_api for declarative resources, manual for custom)
- 403/404: skip gracefully — use `response_actions` with `"action": "ignore"` in rest_api config, or catch in custom helpers
- 400: log warning and skip — some APIs return 400 for valid requests on accounts without certain features (e.g., Yahoo Ads FeedSetService). `wrap_resources_safe` handles this at the resource level so one failing endpoint does not stop the entire pipeline
- 401: refresh token and retry (App Store Connect uses per-request JWT via `session.auth`)

### Incremental Loading

- Use `dlt.sources.incremental` with a cursor field that sorts correctly as strings
- Cursor must be ISO 8601 format — if the API returns a different format, convert before using as cursor

### Write Dispositions

- `merge` with `primary_key`: master data (apps, users, accounts, etc.)
- `append`: logs, events, time-series data
- `replace`: analytics snapshots that refresh each run

### Code Organization

- Prefer dlt's `rest_api` declarative source (`RESTAPIConfig` dict + `rest_api_resources()`) for standard REST endpoints
- Use custom `@dlt.resource` functions only when `rest_api` can't handle it (non-JSON responses, complex incremental logic, custom response transformation)
- Extract reusable logic into module-level helper functions (`_make_session`, `_get_paginated`, `_download_tsv`, etc.)
- No custom HTTP client classes — use `rest_api` for declarative resources and `requests.Session` for custom ones

### Testing

- Do NOT mock through dlt decorators — test helpers directly
- Test the `_rest_api_config()` dict: verify resource names, defaults, parent-child relationships, write dispositions
- Test custom resource functions and helper functions with unit tests
- Integration tests in `tests/test_integration.py` — skipped without env vars
- Coverage threshold: 70% (enforced via `--cov-fail-under` in pyproject.toml)

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

### Releasing

- Publish via `gh workflow run publish.yaml -f version=X.Y.Z` (without `v` prefix)
- The workflow validates the version format and strips `v` if accidentally included
- Always update release notes after publishing (`gh release edit`)

## Adding a New Source

1. Create `dlt_community_sources/{name}/`
2. Implement `source.py` with `_rest_api_config()` + `rest_api_resources()`, `__init__.py`, `py.typed`
3. Add `auth.py` if the API requires custom authentication (e.g., JWT)
4. Add custom `@dlt.resource` functions in `source.py` for endpoints that `rest_api` can't handle
5. Add tests in `{name}/tests/`
6. Add `README.md` with all required sections
7. Add extra in `pyproject.toml` under `[project.optional-dependencies]`
8. Add row to root `README.md` Available Sources table
9. Add integration test in `tests/test_integration.py`
10. Run `uv run pytest -v && uv run ruff check . && uv run ruff format --check .`

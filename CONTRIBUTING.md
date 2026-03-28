# Contributing

Contributions are welcome!

## Setup

1. Fork the repository on GitHub
2. Clone your fork:

```bash
git clone https://github.com/YOUR_USERNAME/dlt-community-sources.git
cd dlt-community-sources
uv sync --group dev
uv run pre-commit install
```

## AI-assisted development (recommended)

This repository integrates the [dltHub AI workbench](https://dlthub.com/context/) which provides dlt-specific skills, MCP server, and workflow rules for AI coding assistants. The sync script installs everything automatically:

```bash
bash scripts/sync-ai-rules.sh
```

This sets up rules and skills for Claude Code, Cursor, and Codex. The dltHub workbench provides:

- **MCP server** (`dlt-workspace-mcp`) — source search, pipeline inspection, schema/data exploration, secrets management directly from your AI assistant
- **Skills** — guided workflows for common tasks (see table below)
- **Rules** — dlt ecosystem best practices automatically loaded into your AI context

### Available skills

| Skill | When to use |
|---|---|
| `find-source` | Looking for an existing dlt source for an API |
| `create-rest-api-pipeline` | Scaffolding a new REST API pipeline |
| `add-source` | Adding a new source to this repository (project-specific) |
| `add-resource` | Adding an endpoint to an existing source (project-specific) |
| `debug-pipeline` | After a pipeline run fails or behaves unexpectedly |
| `validate-data` | Checking loaded data, schemas, types |
| `adjust-endpoint` | Tuning pagination, incremental loading, date ranges |
| `view-data` | Querying and exploring loaded data |
| `setup-secrets` | Configuring API keys and credentials safely |
| `release` | Publishing a new version to PyPI (project-specific) |

### Workflow for adding a new source

```
find-source → create-rest-api-pipeline → debug-pipeline → validate-data → adjust-endpoint → add-source
```

1. Use `find-source` to check if dlt already has a source for the API (9,700+ covered)
2. Use `create-rest-api-pipeline` to scaffold and test a pipeline
3. Use `debug-pipeline` and `validate-data` to iterate until data looks right
4. Use `add-source` to integrate into this repository's structure

> **Note:** If `dlt` is not installed or unavailable, the sync script skips the dltHub workbench setup gracefully. Project rules and custom skills still work without it.

## Running tests

```bash
uv run pytest -v
```

## Linting

```bash
uv run ruff check .
uv run ruff format --check .
```

To auto-fix:

```bash
uv run ruff check --fix .
uv run ruff format .
```

## Adding a new source

1. Create a new directory under `dlt_community_sources/your_source/`
2. Add `__init__.py` and `source.py` with a declarative `_rest_api_config()` using `rest_api_resources()`
3. Add `auth.py` if the API requires custom authentication (e.g., JWT)
4. Add custom `@dlt.resource` functions for endpoints that `rest_api` can't handle (non-JSON responses, complex incremental loading)
5. Add tests under `dlt_community_sources/your_source/tests/`
6. Add an extra in `pyproject.toml` under `[project.optional-dependencies]`
7. Add a `README.md` in the source directory
8. Update the source table in `README.md`

## Adding a resource to an existing source

For standard REST endpoints:
1. Add an entry to the `_rest_api_config()` dict in `source.py`
2. Add a test
3. Update the resource table in the source's `README.md`

For non-standard endpoints (non-JSON, custom pagination, etc.):
1. Add a `@dlt.resource` function in `source.py`
2. Add it to the `custom_resources` list in the source function
3. Add a test
4. Update the resource table in the source's `README.md`

## AI rules

AI assistant rules are managed in `.ai/` as a single source of truth. Do not edit `CLAUDE.md`, `.cursor/rules/`, `.github/copilot-instructions.md`, `.clinerules/`, or `.continue/rules/` directly. Edit `.ai/rules.md` or `.ai/skills/*.md`, then run:

```bash
bash scripts/sync-ai-rules.sh
```

CI will fail if the generated files are out of sync.

## Pull requests

- Open a PR from your fork to `main`
- One feature/fix per PR
- Include tests for new functionality
- Make sure CI passes (`ruff check`, `ruff format --check`, `pytest`, AI rules sync)

## Releases

Releases are managed by maintainers via GitHub Actions. We follow [semver](https://semver.org/):

| Change | Version bump | Example |
|---|---|---|
| Bug fix | patch | 0.5.0 → 0.5.1 |
| New resources to existing source | patch | 0.5.1 → 0.5.2 |
| New source | minor | 0.5.2 → 0.6.0 |
| Breaking change | major | 0.6.0 → 1.0.0 |

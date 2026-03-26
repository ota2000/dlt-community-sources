# Contributing

Contributions are welcome!

## Setup

```bash
git clone https://github.com/ota2000/dlt-community-sources.git
cd dlt-community-sources
uv sync --group dev
```

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
2. Add `__init__.py`, `source.py`, `client.py`, and `auth.py` (if needed)
3. Add tests under `dlt_community_sources/your_source/tests/`
4. Add an extra in `pyproject.toml` under `[project.optional-dependencies]`
5. Add documentation in `docs/your_source.md`
6. Update the source table in `README.md`

## Adding a resource to an existing source

1. Add the resource function in `dlt_community_sources/your_source/source.py`
2. Add it to the `all_resources` list in the source function
3. Add a test
4. Update the resource table in `docs/your_source.md`

## Pull requests

- One feature/fix per PR
- Include tests for new functionality
- Make sure CI passes (`ruff check`, `ruff format --check`, `pytest`)

## Releases

Releases are automated via GitHub Actions. To publish a new version:

1. Create a tag: `git tag v0.1.0`
2. Push the tag: `git push origin v0.1.0`
3. The `publish.yaml` workflow will build and publish to PyPI

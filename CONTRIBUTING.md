# Contributing

Contributions are welcome!

## Setup

1. Fork the repository on GitHub
2. Clone your fork:

```bash
git clone https://github.com/YOUR_USERNAME/dlt-community-sources.git
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
5. Add a `README.md` in the source directory
6. Update the source table in `README.md`

## Adding a resource to an existing source

1. Add the resource function in `dlt_community_sources/your_source/source.py`
2. Add it to the `all_resources` list in the source function
3. Add a test
4. Update the resource table in the source's `README.md`

## Pull requests

- Open a PR from your fork to `main`
- One feature/fix per PR
- Include tests for new functionality
- Make sure CI passes (`ruff check`, `ruff format --check`, `pytest`)

## Releases

Releases are managed by maintainers via GitHub Actions. We follow [semver](https://semver.org/):

| Change | Version bump | Example |
|---|---|---|
| Bug fix | patch | 0.5.0 → 0.5.1 |
| New resources to existing source | patch | 0.5.1 → 0.5.2 |
| New source | minor | 0.5.2 → 0.6.0 |
| Breaking change | major | 0.6.0 → 1.0.0 |

---
name: add-resource
description: "Add a new resource to an existing dlt source. MUST activate when: adding an endpoint, 'リソース追加', 'add resource', 'add endpoint'."
---

# Add a Resource to an Existing Source

## 1. Check the API documentation

- Confirm the endpoint path, HTTP method, and response structure
- Check required parameters and pagination

## 2. Decide: declarative or custom

**Declarative (preferred)** — standard REST endpoint with JSON response:
Add an entry to `_rest_api_config()` in `source.py`.

**Custom** — non-JSON response, complex incremental logic, or custom transformation:
Add a `@dlt.resource` function in `source.py`.

## 3a. Add declarative resource (rest_api config)

Add to the `resources` list in `_rest_api_config()`:

```python
{
    "name": "resource_name",
    "endpoint": {"path": "endpoint/path"},
    # Optional overrides:
    # "write_disposition": "replace",
    # "endpoint": {"params": {"status": "active"}},
    # Parent-child: "endpoint": {"path": "parent/{resources.parent.id}/child"},
    # "include_from_parent": ["id"],
}
```

## 3b. Add custom resource

```python
@dlt.resource(name="resource_name", write_disposition="merge", primary_key="id")
def resource_name(auth, ...):
    """Description."""
    client = _make_client(auth)
    yield from _get_paginated(client, "endpoint/path", "response_key")
```

Add to the `custom_resources` list in the source function.

## 4. Add test

- Declarative: add name to `REST_API_RESOURCE_NAMES` list and verify in `test_rest_api_config_has_all_resources`
- Custom: add name to `CUSTOM_RESOURCE_NAMES` list and add helper tests if new logic was introduced

## 5. Update README.md

- Add row to Resources table (must match implementation)
- Add Notes if there are edge cases

## 6. Verify

```bash
uv run ruff format .
uv run ruff check .
uv run pytest -v
```

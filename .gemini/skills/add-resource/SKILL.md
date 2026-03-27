---
name: add-resource
description: "Add a new resource to an existing dlt source. MUST activate when: adding an endpoint, 'リソース追加', 'add resource', 'add endpoint'."
---

# Add a Resource to an Existing Source

## 1. Check the API documentation

- Confirm the endpoint path, HTTP method, and response structure
- Check required parameters
- Check pagination key name (e.g., `messages`, `data`, etc.)

## 2. Add the resource function in source.py

```python
@dlt.resource(name="resource_name", write_disposition="merge", primary_key="id")
def resource_name(client: XxxClient):
    """Description."""
    yield from client.get_paginated("EndpointPath", "response_key")
```

- Choose correct `write_disposition`: merge / append / replace
- Add `primary_key` for merge resources
- Add incremental loading if the API supports date filtering
- If dates are not ISO 8601, convert before using as cursor

## 3. Add to `all_resources` list in the source function

## 4. Add test

- Add resource name to `test_source_has_all_resources` expected list
- Add helper tests if new logic was introduced

## 5. Update README.md

- Add row to Resources table (must match implementation)
- Add Notes if there are edge cases

## 6. Verify

```bash
uv run ruff format .
uv run ruff check .
uv run pytest -v
```

---
name: release
description: "Release a new version to PyPI. MUST activate when: releasing, publishing, 'リリース', 'release', 'publish'."
---

# Release

## 1. Determine version bump

| Change | Bump | Example |
|---|---|---|
| Bug fix, new resources | patch | 0.5.2 → 0.5.3 |
| New source | minor | 0.5.3 → 0.6.0 |
| Breaking change | major | 0.6.0 → 1.0.0 |

## 2. Verify CI is green

```bash
gh run list --branch main --limit 1
```

All checks must pass before releasing.

## 3. Trigger the publish workflow

**Important: pass version WITHOUT the `v` prefix.** The workflow adds it automatically.

```bash
gh workflow run publish.yaml -f version=X.Y.Z
```

## 4. Wait for completion

```bash
gh run list --workflow=publish.yaml --limit 1
```

## 5. Update release notes

Auto-generated notes are often empty if PRs have no labels. Write meaningful notes:

```bash
gh release edit vX.Y.Z --notes "$(cat <<'EOF'
## What's Changed

### Category (e.g., Bug Fixes, New Sources, etc.)
- Description of change

**Full Changelog**: https://github.com/ota2000/dlt-community-sources/compare/vPREV...vX.Y.Z
EOF
)"
```

## 6. Verify the release

```bash
uv run --no-project --with "dlt-community-sources==X.Y.Z" python -c "
import dlt_community_sources
print(dlt_community_sources.__version__)
"
```

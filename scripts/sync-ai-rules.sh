#!/bin/bash
# Sync AI rules from single source of truth (.ai/rules.md) to all tool-specific files.
# Run after editing .ai/rules.md

set -euo pipefail

SOURCE=".ai/rules.md"

if [ ! -f "$SOURCE" ]; then
  echo "Error: $SOURCE not found"
  exit 1
fi

# Claude Code
cp "$SOURCE" CLAUDE.md
echo "✓ CLAUDE.md"

# Cursor
mkdir -p .cursor
cp "$SOURCE" .cursor/rules.md
echo "✓ .cursor/rules.md"

# GitHub Copilot
mkdir -p .github
cp "$SOURCE" .github/copilot-instructions.md
echo "✓ .github/copilot-instructions.md"

# Windsurf
cp "$SOURCE" .windsurfrules
echo "✓ .windsurfrules"

echo "Done. All AI rules synced from $SOURCE"

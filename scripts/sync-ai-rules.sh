#!/bin/bash
# Sync AI rules and skills from .ai/ to all tool-specific locations.
# Run after editing files under .ai/

set -euo pipefail

RULES=".ai/rules.md"

if [ ! -f "$RULES" ]; then
  echo "Error: $RULES not found"
  exit 1
fi

# --- Rules ---

# Claude Code
cp "$RULES" CLAUDE.md
echo "✓ CLAUDE.md"

# Cursor
mkdir -p .cursor
cp "$RULES" .cursor/rules.md
echo "✓ .cursor/rules.md"

# GitHub Copilot
cp "$RULES" .github/copilot-instructions.md
echo "✓ .github/copilot-instructions.md"

# Windsurf
cp "$RULES" .windsurfrules
echo "✓ .windsurfrules"

# --- Skills ---

if [ -d ".ai/skills" ]; then
  # Claude Code skills
  mkdir -p .claude/skills
  for skill in .ai/skills/*.md; do
    name=$(basename "$skill" .md)
    mkdir -p ".claude/skills/$name"
    cp "$skill" ".claude/skills/$name/SKILL.md"
    echo "✓ .claude/skills/$name/SKILL.md"
  done
fi

echo "Done. All AI rules and skills synced from .ai/"

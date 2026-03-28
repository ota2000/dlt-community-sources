#!/bin/bash
# Sync AI rules and skills from .ai/ and dltHub AI workbench to all tool-specific locations.
# Run after editing files under .ai/
#
# Order matters: dltHub workbench runs FIRST (it may overwrite AGENTS.md etc.),
# then .ai/rules.md overwrites on top so project-specific rules always win.

set -euo pipefail

RULES=".ai/rules.md"

if [ ! -f "$RULES" ]; then
  echo "Error: $RULES not found"
  exit 1
fi

# --- dltHub AI workbench (runs first, project rules overwrite after) ---

AGENTS=(claude cursor codex)

if command -v uv > /dev/null 2>&1 && uv run dlt --version > /dev/null 2>&1; then
  echo "Syncing dltHub AI workbench..."
  for agent in "${AGENTS[@]}"; do
    uv run dlt ai init --agent "$agent" --overwrite 2>&1 | sed 's/^/  /'
    uv run dlt ai toolkit rest-api-pipeline install --agent "$agent" --overwrite 2>&1 | sed 's/^/  /'
  done
  echo "✓ dltHub AI workbench synced"
  echo ""
else
  echo "⚠ Skipping dltHub AI workbench sync (dlt not available)"
  echo ""
fi

# --- Project rules (overwrites dltHub where paths overlap, e.g. AGENTS.md) ---

# Claude Code
cp "$RULES" CLAUDE.md
echo "✓ CLAUDE.md"

# Cursor (.cursor/rules/ directory)
mkdir -p .cursor/rules
cp "$RULES" .cursor/rules/rules.md
echo "✓ .cursor/rules/rules.md"

# GitHub Copilot
cp "$RULES" .github/copilot-instructions.md
echo "✓ .github/copilot-instructions.md"

# Cline
mkdir -p .clinerules
cp "$RULES" .clinerules/rules.md
echo "✓ .clinerules/rules.md"

# Continue
mkdir -p .continue/rules
cp "$RULES" .continue/rules/rules.md
echo "✓ .continue/rules/rules.md"

# OpenAI Codex CLI
cp "$RULES" AGENTS.md
echo "✓ AGENTS.md"

# Gemini CLI
cp "$RULES" GEMINI.md
echo "✓ GEMINI.md"

# --- Skills ---

if [ -d ".ai/skills" ] && compgen -G ".ai/skills/*.md" > /dev/null; then
  # Claude Code skills
  mkdir -p .claude/skills
  for skill in .ai/skills/*.md; do
    name=$(basename "$skill" .md)
    mkdir -p ".claude/skills/$name"
    cp "$skill" ".claude/skills/$name/SKILL.md"
    echo "✓ .claude/skills/$name/SKILL.md"
  done

  # Gemini CLI skills
  mkdir -p .gemini/skills
  for skill in .ai/skills/*.md; do
    name=$(basename "$skill" .md)
    mkdir -p ".gemini/skills/$name"
    cp "$skill" ".gemini/skills/$name/SKILL.md"
    echo "✓ .gemini/skills/$name/SKILL.md"
  done
fi

echo ""
echo "Done. All AI rules and skills synced."

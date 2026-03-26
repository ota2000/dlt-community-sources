# .ai/

Single source of truth for AI coding assistant rules and skills.

## How it works

`scripts/sync-ai-rules.sh` copies files from here to tool-specific locations. CI checks that copies are in sync.

## What goes where

### rules.md

Project-wide conventions that apply to all sources. Include:

- Build/test/lint commands
- Directory structure
- Error handling policy (which status codes to retry, skip, or raise)
- Write disposition and incremental loading conventions
- Testing approach
- Documentation requirements
- Versioning policy

Do NOT include:

- Source-specific implementation details (date formats, API quirks)
- Things that are obvious from reading the code
- Duplicates of what's already in CONTRIBUTING.md

### skills/

Step-by-step instructions for common tasks. Each skill is a separate `.md` file. Include:

- Concrete file paths and commands
- Checklists that can be followed mechanically
- The exact order of operations

Currently synced to Claude Code only (`.claude/skills/`).

## Editing

1. Edit files in `.ai/`
2. Run `bash scripts/sync-ai-rules.sh`
3. Commit all changes (source + generated copies)

.PHONY: help install test lint format sync check build clean pre-commit

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

install: ## Install dependencies
	uv sync --group dev

test: ## Run tests
	uv run pytest -v

lint: ## Run linter and format check
	uv run ruff check .
	uv run ruff format --check .

format: ## Auto-fix lint and format issues
	uv run ruff check --fix .
	uv run ruff format .

sync: ## Sync AI rules
	bash scripts/sync-ai-rules.sh

check: lint test ## Run lint + test

build: ## Build package
	uv build

clean: ## Remove build artifacts
	rm -rf dist/ build/ *.egg-info .pytest_cache .ruff_cache
	find . -type d -name __pycache__ -exec rm -rf {} +

pre-commit: ## Install pre-commit hooks
	uv run pre-commit install

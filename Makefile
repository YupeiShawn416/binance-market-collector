.PHONY: install check test
install:
	uv sync --frozen --extra dev --extra kraken --extra preview
check:
	uv run ruff check src tests scripts
	uv run ruff format --check src tests scripts
	uv run python scripts/check_vendor.py
test:
	uv run pytest

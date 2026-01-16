.PHONY: setup lint test run-scan run-report

setup:
	uv venv
	uv pip install -e ".[dev]"

lint:
	uv run ruff check .
	uv run ruff format --check .

test:
	uv run pytest

run-scan:
	uv run pmarb scan --duration-min 1

run-report:
	uv run pmarb report --db-path data/market.db --out-path data/report.md

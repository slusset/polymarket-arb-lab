# Polymarket Binary Arb Explorer

Read-only research tool for polling public Polymarket data, detecting buy-both-sides arbitrage conditions on binary YES/NO markets, and producing a daily ranked report. No order placement, no auth, no secrets.

## Setup

From the repo root:

```bash
uv venv
uv pip install -e ".[dev]"
```

If you do not have `uv`, use `poetry` (create a virtualenv and install the project in editable mode).

## Example commands

```bash
uv run pmarb scan --duration-min 1 --max-markets 50 --edge-threshold 0.01
uv run pmarb scan --offline-fixture data/fixtures/sample_fixture.json --duration-min 1
uv run pmarb report --db-path data/market.db --out-path data/report.md
```

## Configuration

Environment variables (defaults are public endpoints):

- `PM_MARKETS_URL` (default `https://gamma-api.polymarket.com/markets`)
- `PM_ORDERBOOK_URL` (default `https://gamma-api.polymarket.com/orderbook`)
- `PM_TIMEOUT_S` (default `10.0`)
- `PM_MIN_INTERVAL_S` (default `0.25`)
- `PM_MAX_RETRIES` (default `3`)
- `PM_DB_PATH` (default `data/market.db`)

## Top-of-book edge (and why it can be misleading)

This project computes a simple edge estimate:

```
edge = 1 - (ask_yes + ask_no) - fees - overhead
```

Using the best ask on each side is fast and reproducible, but it can be misleading because:

- The best asks may be too small to fill the target size.
- Deeper orderbook levels can widen the true cost for larger quantities.
- Latency, partial fills, and cancellations can erase the edge before execution.
- Fees can vary by venue rules; this tool uses a configurable flat model.

Treat the output as research signals only, not executable guarantees.

## Research-only disclaimer

This tool is for analysis and research only. It does not place orders, does not use secrets, and does not connect to trading endpoints.

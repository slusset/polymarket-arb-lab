# Polymarket Market Data Explorer

Read-only research tool for exporting Polymarket Gamma market data, normalizing it into CSV snapshots, and recording paired CLOB quotes with simple signals. No order placement, no auth, no secrets.

## Setup

From the repo root:

```bash
uv venv
uv pip install -e ".[dev]"
```

If you do not have `uv`, use `poetry` (create a virtualenv and install the project in editable mode).

## Example commands

```bash
uv run pmarb export --input data/polymarket_events_10.json --out data/snapshots/test-run
uv run pmarb paired-quotes --markets-csv data/snapshots/test-run/markets.csv --iters 1
```

## CLI details

- `pmarb export` downloads Gamma events (or reads a local JSON fixture) and writes normalized CSVs:
  - `events.csv`, `markets.csv`, `tokens.csv`, plus `watchlist.csv` and `watchlist_future.csv`
- By default it exports the open/future event set. Use `--event-set closed` to export closed events.
- `pmarb paired-quotes` reads `markets.csv`, loads OPEN_TRADABLE pairs, fetches CLOB `/book`,
  and writes `paired_quotes.csv` and `signals.csv` with enriched metadata.

## Research-only disclaimer

This tool is for analysis and research only. It does not place orders, does not use secrets, and does not connect to trading endpoints.

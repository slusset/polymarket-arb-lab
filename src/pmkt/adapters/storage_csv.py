from __future__ import annotations

import csv
import json
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from pmkt.domain.ports import UniverseSnapshot, UniverseWriter


def _normalize_value(value: Any) -> Any:
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    return value


def _row_from_dataclass(item: Any) -> dict[str, Any]:
    if is_dataclass(item):
        raw = asdict(item)
    elif isinstance(item, dict):
        raw = dict(item)
    else:
        raise TypeError(f"Unsupported row type: {type(item)!r}")
    return {key: _normalize_value(value) for key, value in raw.items()}


def _write_csv(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    rows = list(rows)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _parse_event_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        return datetime.fromisoformat(value)
    except ValueError:
        return None


class CsvUniverseWriter(UniverseWriter):
    def write(self, snapshot: UniverseSnapshot, out_dir: Path) -> None:
        out_dir.mkdir(parents=True, exist_ok=True)
        events_rows = [_row_from_dataclass(event) for event in snapshot.events]
        markets_rows = [_row_from_dataclass(market) for market in snapshot.markets]
        tokens_rows = [_row_from_dataclass(token) for token in snapshot.tokens]
        _write_csv(out_dir / "events.csv", events_rows)
        _write_csv(out_dir / "markets.csv", markets_rows)
        _write_csv(out_dir / "tokens.csv", tokens_rows)

        now = datetime.now(timezone.utc)
        watchlist_rows: list[dict[str, Any]] = []
        watchlist_future_rows: list[dict[str, Any]] = []
        for market, row in zip(snapshot.markets, markets_rows):
            if market.enable_order_book is True and market.accepting_orders is True:
                watchlist_rows.append(row)
            start_time = _parse_event_time(market.event_start_time)
            if (
                market.active is True
                and market.closed is False
                and start_time is not None
                and start_time > now
            ):
                watchlist_future_rows.append(row)
        _write_csv(out_dir / "watchlist.csv", watchlist_rows)
        _write_csv(out_dir / "watchlist_future.csv", watchlist_future_rows)

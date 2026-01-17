from __future__ import annotations

import logging
import sqlite3
import time
from typing import Iterable

from .models import MarketMetadata, OrderBookTop
from .utils import dumps_compact

logger = logging.getLogger(__name__)


class Storage:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._conn = sqlite3.connect(self.db_path)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._conn.execute("PRAGMA busy_timeout=5000;")
        self._init_schema()

    def close(self) -> None:
        self._conn.close()

    def _init_schema(self) -> None:
        cur = self._conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS markets (
                market_id TEXT PRIMARY KEY,
                title TEXT,
                status TEXT,
                outcomes_json TEXT,
                clob_token_ids_json TEXT,
                yes_clob_token_id TEXT,
                no_clob_token_id TEXT,
                volume REAL,
                extra_json TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS snapshots (
                id INTEGER PRIMARY KEY,
                ts TEXT,
                market_id TEXT,
                yes_best_ask REAL,
                yes_best_ask_size REAL,
                no_best_ask REAL,
                no_best_ask_size REAL,
                raw_json TEXT
            )
            """
        )
        self._ensure_columns(
            "markets",
            {
                "clob_token_ids_json": "TEXT",
                "yes_clob_token_id": "TEXT",
                "no_clob_token_id": "TEXT",
            },
        )
        self._conn.commit()

    def _execute_with_retry(self, action: str, func) -> bool:
        delays = [0.05, 0.1, 0.2, 0.4, 0.8]
        for attempt, delay in enumerate(delays, start=1):
            try:
                func()
                return True
            except sqlite3.OperationalError as exc:
                message = str(exc).lower()
                if "database is locked" not in message:
                    raise
                if attempt >= len(delays):
                    logger.error(
                        "database locked; giving up",
                        extra={"action": action, "attempt": attempt},
                    )
                    return False
                time.sleep(delay)
        return False

    def _ensure_columns(self, table: str, columns: dict[str, str]) -> None:
        cur = self._conn.cursor()
        cur.execute(f"PRAGMA table_info({table})")
        existing = {row[1] for row in cur.fetchall()}
        for name, col_type in columns.items():
            if name in existing:
                continue
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {name} {col_type}")

    def upsert_markets(self, markets: Iterable[MarketMetadata]) -> None:
        rows = [
            (
                market.market_id,
                market.title,
                market.status,
                dumps_compact(market.outcomes),
                dumps_compact(market.clob_token_ids),
                market.yes_clob_token_id,
                market.no_clob_token_id,
                market.volume,
                dumps_compact(market.raw),
            )
            for market in markets
        ]
        if not rows:
            return

        def _write() -> None:
            cur = self._conn.cursor()
            cur.execute("BEGIN")
            cur.executemany(
                """
                INSERT INTO markets (
                    market_id, title, status, outcomes_json, clob_token_ids_json,
                    yes_clob_token_id, no_clob_token_id, volume, extra_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(market_id) DO UPDATE SET
                    title = excluded.title,
                    status = excluded.status,
                    outcomes_json = excluded.outcomes_json,
                    clob_token_ids_json = excluded.clob_token_ids_json,
                    yes_clob_token_id = excluded.yes_clob_token_id,
                    no_clob_token_id = excluded.no_clob_token_id,
                    volume = excluded.volume,
                    extra_json = excluded.extra_json
                """,
                rows,
            )
            self._conn.commit()

        self._execute_with_retry("upsert_markets", _write)

    def insert_snapshots_batch(self, rows: Iterable[tuple[str, str, OrderBookTop]]) -> float | None:
        payload = [
            (
                ts,
                market_id,
                top.yes_best_ask,
                top.yes_best_ask_size,
                top.no_best_ask,
                top.no_best_ask_size,
                dumps_compact(top.raw),
            )
            for ts, market_id, top in rows
        ]
        if not payload:
            return 0.0

        def _write() -> None:
            cur = self._conn.cursor()
            cur.execute("BEGIN")
            cur.executemany(
                """
                INSERT INTO snapshots (
                    ts, market_id, yes_best_ask, yes_best_ask_size,
                    no_best_ask, no_best_ask_size, raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
            self._conn.commit()

        start = time.monotonic()
        ok = self._execute_with_retry("insert_snapshots_batch", _write)
        if not ok:
            return None
        return time.monotonic() - start

    def fetch_snapshots(self) -> list[sqlite3.Row]:
        cur = self._conn.cursor()
        cur.execute(
            """
            SELECT ts, market_id, yes_best_ask, yes_best_ask_size, no_best_ask, no_best_ask_size
            FROM snapshots
            ORDER BY market_id, ts
            """
        )
        return cur.fetchall()

    def fetch_markets(self) -> list[sqlite3.Row]:
        cur = self._conn.cursor()
        cur.execute(
            """
            SELECT market_id, title, status, outcomes_json, volume
            FROM markets
            """
        )
        return cur.fetchall()

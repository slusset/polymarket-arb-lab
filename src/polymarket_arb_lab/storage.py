from __future__ import annotations

import sqlite3
from typing import Iterable

from .models import MarketMetadata, OrderBookTop
from .utils import dumps_compact


class Storage:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._conn = sqlite3.connect(self.db_path)
        self._conn.row_factory = sqlite3.Row
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
        self._conn.commit()

    def upsert_markets(self, markets: Iterable[MarketMetadata]) -> None:
        cur = self._conn.cursor()
        for market in markets:
            cur.execute(
                """
                INSERT INTO markets (market_id, title, status, outcomes_json, volume, extra_json)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(market_id) DO UPDATE SET
                    title = excluded.title,
                    status = excluded.status,
                    outcomes_json = excluded.outcomes_json,
                    volume = excluded.volume,
                    extra_json = excluded.extra_json
                """,
                (
                    market.market_id,
                    market.title,
                    market.status,
                    dumps_compact(market.outcomes),
                    market.volume,
                    dumps_compact(market.raw),
                ),
            )
        self._conn.commit()

    def insert_snapshot(self, ts: str, market_id: str, top: OrderBookTop) -> None:
        cur = self._conn.cursor()
        cur.execute(
            """
            INSERT INTO snapshots (
                ts, market_id, yes_best_ask, yes_best_ask_size,
                no_best_ask, no_best_ask_size, raw_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ts,
                market_id,
                top.yes_best_ask,
                top.yes_best_ask_size,
                top.no_best_ask,
                top.no_best_ask_size,
                dumps_compact(top.raw),
            ),
        )
        self._conn.commit()

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

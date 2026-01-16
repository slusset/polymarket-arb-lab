from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .client import ApiClient
from .logic import FeeModel, edge_for_top, is_executable, parse_orderbook_top, select_markets
from .models import MarketMetadata
from .storage import Storage
from .tracker import OpportunityTracker
from .utils import utc_now_iso

logger = logging.getLogger(__name__)


@dataclass
class OfflineFixture:
    markets: list[dict[str, Any]]
    orderbooks: dict[str, dict[str, Any]]


def load_offline_fixture(path: str) -> OfflineFixture:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    markets = data.get("markets") or []
    orderbooks = data.get("orderbooks") or {}
    return OfflineFixture(markets=markets, orderbooks=orderbooks)


def scan_markets(
    storage: Storage,
    api_client: ApiClient | None,
    poll_interval_s: float,
    duration_min: float,
    max_markets: int | None,
    min_volume: float,
    quantities: list[float],
    fee_model: FeeModel,
    overhead: float,
    edge_threshold: float,
    offline_fixture: OfflineFixture | None = None,
) -> None:
    if offline_fixture:
        markets_data = offline_fixture.markets
    elif api_client:
        markets_data = api_client.fetch_markets()
    else:
        raise RuntimeError("no api client or offline fixture")

    markets = [MarketMetadata.from_api(m) for m in markets_data]
    storage.upsert_markets(markets)
    candidates = select_markets(markets, min_volume=min_volume, max_markets=max_markets)
    logger.info("selected markets", extra={"count": len(candidates)})

    tracker = OpportunityTracker(edge_threshold=edge_threshold, poll_interval_s=poll_interval_s)

    start = time.monotonic()
    deadline = start + duration_min * 60.0
    tick = 0
    while True:
        now = time.monotonic()
        if now > deadline:
            break
        tick += 1
        ts = utc_now_iso()
        for market in candidates:
            if offline_fixture:
                orderbook = offline_fixture.orderbooks.get(market.market_id, {})
            else:
                try:
                    orderbook = api_client.fetch_orderbook(market.market_id) if api_client else {}
                except Exception as exc:
                    logger.warning(
                        "orderbook fetch failed",
                        extra={"market_id": market.market_id, "err": str(exc)},
                    )
                    continue
            top = parse_orderbook_top(orderbook)
            storage.insert_snapshot(ts, market.market_id, top)
            edge = edge_for_top(top, fee_model, overhead)
            for qty in quantities:
                if edge is None:
                    tracker.update(ts, tick, market.market_id, qty, None)
                    continue
                if is_executable(top, qty):
                    tracker.update(ts, tick, market.market_id, qty, edge)
                    if edge > edge_threshold:
                        logger.info(
                            "opportunity",
                            extra={
                                "market_id": market.market_id,
                                "qty": qty,
                                "edge": round(edge, 6),
                            },
                        )
                else:
                    tracker.update(ts, tick, market.market_id, qty, None)
        time.sleep(poll_interval_s)

    tracker.close_all(utc_now_iso(), tick)

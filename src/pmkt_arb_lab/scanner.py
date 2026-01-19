from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .client import ApiClient
from .logic import (
    FeeModel,
    edge_for_top,
    is_executable,
    parse_clob_orderbook_top,
    parse_orderbook_top,
    select_markets,
)
from .models import MarketMetadata, OrderBookTop
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
    min_liquidity: float = 0.0,
    progress_every: int = 1,
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
    candidates, diagnostics = select_markets(
        markets,
        min_volume=min_volume,
        max_markets=max_markets,
        min_liquidity=min_liquidity,
    )
    logger.info(
        "selected markets count=%s total=%s", len(candidates), diagnostics.total
    )
    logger.info(
        "selection diagnostics %s",
        diagnostics,
    )
    if diagnostics.total == 0:
        logger.warning("no markets returned from api")
        return
    skip_reasons = diagnostics.top_reasons()
    if skip_reasons:
        reasons_str = ", ".join(f"{name}={count}" for name, count in skip_reasons)
        logger.info("selection skip reasons %s", reasons_str)
    if not candidates:
        logger.warning("no eligible markets after filtering")
        return

    tracker = OpportunityTracker(edge_threshold=edge_threshold, poll_interval_s=poll_interval_s)

    start = time.monotonic()
    deadline = start + duration_min * 60.0
    tick = 0
    total_missing_asks = 0
    total_missing_sizes = 0
    total_ask_sum = 0.0
    total_ask_sum_count = 0
    total_executable = 0
    total_executable_by_qty: dict[float, int] = {qty: 0 for qty in quantities}
    progress_every = max(1, progress_every)
    while True:
        now = time.monotonic()
        if now > deadline:
            break
        tick += 1
        ts = utc_now_iso()
        tick_missing_asks = 0
        tick_missing_sizes = 0
        tick_ask_sum = 0.0
        tick_ask_sum_count = 0
        tick_executable = 0
        tick_executable_by_qty: dict[float, int] = {qty: 0 for qty in quantities}
        tick_snapshots: list[tuple[str, str, OrderBookTop]] = []
        orderbooks_ok = 0
        orderbooks_failed = 0
        last_exception_summary: str | None = None
        logged_exception = False
        for market in candidates:
            try:
                if offline_fixture:
                    if market.yes_clob_token_id and market.no_clob_token_id:
                        yes_book = offline_fixture.orderbooks.get(market.yes_clob_token_id, {})
                        no_book = offline_fixture.orderbooks.get(market.no_clob_token_id, {})
                        top = parse_clob_orderbook_top(yes_book, no_book)
                        orderbooks_ok += 2
                    else:
                        orderbook = offline_fixture.orderbooks.get(market.market_id, {})
                        top = parse_orderbook_top(orderbook)
                        orderbooks_ok += 1
                else:
                    if not api_client:
                        continue
                    if market.yes_clob_token_id and market.no_clob_token_id:
                        try:
                            yes_book = api_client.fetch_orderbook(market.yes_clob_token_id)
                            orderbooks_ok += 1
                        except Exception:
                            orderbooks_failed += 1
                            raise
                        try:
                            no_book = api_client.fetch_orderbook(market.no_clob_token_id)
                            orderbooks_ok += 1
                        except Exception:
                            orderbooks_failed += 1
                            raise
                        top = parse_clob_orderbook_top(yes_book, no_book)
                    else:
                        logger.warning(
                            "missing clob token ids",
                            extra={"market_id": market.market_id},
                        )
                        continue
                if top.yes_best_ask is None or top.no_best_ask is None:
                    tick_missing_asks += 1
                else:
                    tick_ask_sum += top.yes_best_ask + top.no_best_ask
                    tick_ask_sum_count += 1
                if top.yes_best_ask_size is None or top.no_best_ask_size is None:
                    tick_missing_sizes += 1
                tick_snapshots.append((ts, market.market_id, top))
                edge = edge_for_top(top, fee_model, overhead)
                for qty in quantities:
                    if edge is None:
                        tracker.update(ts, tick, market.market_id, qty, None)
                        continue
                    if is_executable(top, qty):
                        tick_executable += 1
                        tick_executable_by_qty[qty] = tick_executable_by_qty.get(qty, 0) + 1
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
            except Exception as exc:
                last_exception_summary = f"{type(exc).__name__}: {exc}"
                if not logged_exception:
                    logger.exception(
                        "market processing failed",
                        extra={"market_id": market.market_id},
                    )
                    logged_exception = True
                continue
        commit_time_s = storage.insert_snapshots_batch(tick_snapshots)
        total_missing_asks += tick_missing_asks
        total_missing_sizes += tick_missing_sizes
        total_ask_sum += tick_ask_sum
        total_ask_sum_count += tick_ask_sum_count
        total_executable += tick_executable
        for qty, count in tick_executable_by_qty.items():
            total_executable_by_qty[qty] = total_executable_by_qty.get(qty, 0) + count
        if tick % progress_every == 0:
            avg_ask_sum = tick_ask_sum / tick_ask_sum_count if tick_ask_sum_count else 0.0
            commit_ms = int(commit_time_s * 1000) if commit_time_s is not None else None
            logger.info(
                "scan heartbeat",
                extra={
                    "tick": tick,
                    "elapsed_s": round(time.monotonic() - start, 3),
                    "markets": len(candidates),
                    "orderbooks_ok": orderbooks_ok,
                    "orderbooks_failed": orderbooks_failed,
                    "missing_asks": tick_missing_asks,
                    "missing_sizes": tick_missing_sizes,
                    "snapshots": len(tick_snapshots),
                    "avg_ask_sum": round(avg_ask_sum, 6),
                    "commit_ms": commit_ms,
                    "last_exception": last_exception_summary,
                },
            )
        time.sleep(poll_interval_s)

    tracker.close_all(utc_now_iso(), tick)
    avg_ask_sum = total_ask_sum / total_ask_sum_count if total_ask_sum_count else 0.0
    logger.info(
        "scan instrumentation",
        extra={
            "missing_asks": total_missing_asks,
            "missing_sizes": total_missing_sizes,
            "executable_total": total_executable,
            "executable_by_qty": total_executable_by_qty,
            "avg_ask_sum": round(avg_ask_sum, 6),
        },
    )

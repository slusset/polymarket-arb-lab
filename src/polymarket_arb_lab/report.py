from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

from .logic import FeeModel, edge_for_top, is_executable
from .models import OrderBookTop


@dataclass
class Window:
    market_id: str
    quantity: float
    start_ts: str
    end_ts: str
    duration_s: float
    edge: float


def _parse_ts(ts: str) -> datetime | None:
    try:
        return datetime.fromisoformat(ts)
    except ValueError:
        return None


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    values_sorted = sorted(values)
    mid = len(values_sorted) // 2
    if len(values_sorted) % 2 == 1:
        return values_sorted[mid]
    return (values_sorted[mid - 1] + values_sorted[mid]) / 2.0


def analyze_snapshots(
    rows: Iterable[dict],
    quantities: list[float],
    fee_model: FeeModel,
    overhead: float,
    edge_threshold: float,
) -> dict:
    events_by_market: dict[str, int] = defaultdict(int)
    seconds_by_market: dict[str, float] = defaultdict(float)
    edges_by_qty: dict[float, list[float]] = defaultdict(list)
    windows: list[Window] = []

    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        grouped[row["market_id"]].append(row)

    for market_id, items in grouped.items():
        open_windows: dict[float, tuple[str, datetime | None, float]] = {}
        last_ts: str | None = None
        last_dt: datetime | None = None
        for row in items:
            ts = row["ts"]
            dt = _parse_ts(ts)
            top = OrderBookTop(
                yes_best_ask=row["yes_best_ask"],
                yes_best_ask_size=row["yes_best_ask_size"],
                no_best_ask=row["no_best_ask"],
                no_best_ask_size=row["no_best_ask_size"],
                raw={},
            )
            edge = edge_for_top(top, fee_model, overhead)
            for qty in quantities:
                executable = is_executable(top, qty)
                if edge is not None and executable:
                    edges_by_qty[qty].append(edge)
                is_profitable = edge is not None and executable and edge > edge_threshold
                if is_profitable and qty not in open_windows:
                    open_windows[qty] = (ts, dt, edge or 0.0)
                if not is_profitable and qty in open_windows:
                    start_ts, start_dt, start_edge = open_windows.pop(qty)
                    duration_s = 0.0
                    if start_dt and dt:
                        duration_s = max(0.0, (dt - start_dt).total_seconds())
                    window = Window(
                        market_id=market_id,
                        quantity=qty,
                        start_ts=start_ts,
                        end_ts=ts,
                        duration_s=duration_s,
                        edge=start_edge,
                    )
                    windows.append(window)
                    events_by_market[market_id] += 1
                    seconds_by_market[market_id] += window.duration_s
            last_ts = ts
            last_dt = dt
        for qty, (start_ts, start_dt, start_edge) in open_windows.items():
            if last_ts is None:
                continue
            duration_s = 0.0
            if start_dt and last_dt:
                duration_s = max(0.0, (last_dt - start_dt).total_seconds())
            window = Window(
                market_id=market_id,
                quantity=qty,
                start_ts=start_ts,
                end_ts=last_ts,
                duration_s=duration_s,
                edge=start_edge,
            )
            windows.append(window)
            events_by_market[market_id] += 1
            seconds_by_market[market_id] += window.duration_s

    top_by_events = sorted(events_by_market.items(), key=lambda x: x[1], reverse=True)
    top_by_seconds = sorted(seconds_by_market.items(), key=lambda x: x[1], reverse=True)
    longest_windows = sorted(windows, key=lambda w: w.duration_s, reverse=True)

    edge_stats = {}
    for qty, values in edges_by_qty.items():
        if not values:
            continue
        edge_stats[qty] = {
            "count": len(values),
            "min": min(values),
            "max": max(values),
            "mean": sum(values) / len(values),
            "median": _median(values) or 0.0,
        }

    return {
        "top_by_events": top_by_events,
        "top_by_seconds": top_by_seconds,
        "longest_windows": longest_windows,
        "edge_stats": edge_stats,
        "total_markets": len(grouped),
        "total_windows": len(windows),
    }


def render_report(results: dict, max_rows: int = 10) -> str:
    lines: list[str] = []
    lines.append("# Polymarket Binary Arb Explorer Report")
    lines.append("")
    lines.append("## Summary")
    lines.append(f"- Markets scanned: {results['total_markets']}")
    lines.append(f"- Opportunity windows: {results['total_windows']}")
    lines.append("")

    lines.append("## Top Markets by Opportunity Events")
    if not results["top_by_events"]:
        lines.append("- No opportunities detected.")
    else:
        for market_id, count in results["top_by_events"][:max_rows]:
            lines.append(f"- {market_id}: {count} events")
    lines.append("")

    lines.append("## Top Markets by Total Opportunity Seconds")
    if not results["top_by_seconds"]:
        lines.append("- No opportunities detected.")
    else:
        for market_id, seconds in results["top_by_seconds"][:max_rows]:
            lines.append(f"- {market_id}: {seconds:.1f}s")
    lines.append("")

    lines.append("## Edge Distribution by Quantity")
    if not results["edge_stats"]:
        lines.append("- No executable edges computed.")
    else:
        for qty, stats in results["edge_stats"].items():
            lines.append(
                f"- q={qty}: n={stats['count']} min={stats['min']:.4f} "
                f"median={stats['median']:.4f} mean={stats['mean']:.4f} "
                f"max={stats['max']:.4f}"
            )
    lines.append("")

    lines.append("## Longest Opportunity Windows")
    if not results["longest_windows"]:
        lines.append("- No windows recorded.")
    else:
        for window in results["longest_windows"][:max_rows]:
            lines.append(
                f"- {window.market_id} q={window.quantity} duration={window.duration_s:.1f}s "
                f"start={window.start_ts} end={window.end_ts}"
            )
    lines.append("")

    return "\n".join(lines)

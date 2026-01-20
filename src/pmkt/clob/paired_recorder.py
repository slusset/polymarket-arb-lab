from __future__ import annotations

import csv
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable

from .client import ClobClient
from .paired import PairedBookSnapshot, make_paired_snapshot

logger = logging.getLogger(__name__)

MID_SUM_THRESHOLD = Decimal("0.02")
SPREAD_SUM_THRESHOLD = Decimal("0.06")
ONE_DOLLAR = Decimal("1.00")


@dataclass(slots=True)
class TradablePair:
    condition_id: str
    token_a_id: str
    token_b_id: str
    outcome_a: str
    outcome_b: str
    gamma_market_id: str | None = None
    question: str | None = None


def load_tradable_pairs(markets_csv: Path) -> list[TradablePair]:
    pairs: list[TradablePair] = []
    with markets_csv.open(encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if row.get("lifecycle_state") != "OPEN_TRADABLE":
                continue
            outcome_tokens = _parse_outcome_tokens(row)
            if len(outcome_tokens) != 2:
                continue
            outcome_a, outcome_b, token_a_id, token_b_id = _resolve_outcome_pair(
                outcome_tokens
            )
            condition_id = _extract_condition_id(row)
            pairs.append(
                TradablePair(
                    condition_id=condition_id,
                    token_a_id=token_a_id,
                    token_b_id=token_b_id,
                    outcome_a=outcome_a,
                    outcome_b=outcome_b,
                    gamma_market_id=row.get("market_id") or None,
                    question=row.get("question") or None,
                )
            )
    return pairs


def record_paired_quotes(
    pairs: Iterable[TradablePair],
    out_dir: Path,
    interval_seconds: float = 2.0,
    max_iters: int | None = None,
    client: ClobClient | None = None,
    mid_sum_threshold: Decimal = MID_SUM_THRESHOLD,
    spread_sum_threshold: Decimal = SPREAD_SUM_THRESHOLD,
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    quotes_path = out_dir / "paired_quotes.csv"
    signals_path = out_dir / "signals.csv"
    own_client = client is None
    client = client or ClobClient()
    try:
        iteration = 0
        while max_iters is None or iteration < max_iters:
            for pair in pairs:
                snapshot = _fetch_snapshot(pair, client)
                if snapshot is None:
                    continue
                _append_snapshot(quotes_path, snapshot)
                signals = _signals_for_snapshot(
                    snapshot,
                    mid_sum_threshold=mid_sum_threshold,
                    spread_sum_threshold=spread_sum_threshold,
                )
                for signal in signals:
                    _append_signal(signals_path, signal)
            iteration += 1
            if max_iters is None or iteration < max_iters:
                time.sleep(interval_seconds)
    finally:
        if own_client:
            client.close()


def _fetch_snapshot(pair: TradablePair, client: ClobClient) -> PairedBookSnapshot | None:
    try:
        book_a = client.get_order_book(pair.token_a_id)
        book_b = client.get_order_book(pair.token_b_id)
        book_a.market = pair.condition_id
        book_b.market = pair.condition_id
        return make_paired_snapshot(
            book_a,
            book_b,
            outcome_a=pair.outcome_a,
            outcome_b=pair.outcome_b,
        )
    except Exception as exc:  # noqa: BLE001 - keep polling
        logger.warning("Skipping pair %s due to error: %s", pair.condition_id, exc)
        return None


def _append_snapshot(path: Path, snapshot: PairedBookSnapshot) -> None:
    row = _snapshot_row(snapshot)
    _append_row(path, row)


def _append_signal(path: Path, signal: dict[str, Any]) -> None:
    _append_row(path, signal)


def _append_row(path: Path, row: dict[str, Any]) -> None:
    write_header = not path.exists()
    with path.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(row.keys()))
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def _snapshot_row(snapshot: PairedBookSnapshot) -> dict[str, Any]:
    return {
        "ts_ms": snapshot.ts_ms,
        "condition_id": snapshot.condition_id,
        "token_a_id": snapshot.token_a_id,
        "token_b_id": snapshot.token_b_id,
        "outcome_a": snapshot.outcome_a,
        "outcome_b": snapshot.outcome_b,
        "a_bid": str(snapshot.a_bid),
        "a_ask": str(snapshot.a_ask),
        "a_mid": str(snapshot.a_mid),
        "a_spread": str(snapshot.a_spread),
        "a_bid_sz": str(snapshot.a_bid_sz),
        "a_ask_sz": str(snapshot.a_ask_sz),
        "b_bid": str(snapshot.b_bid),
        "b_ask": str(snapshot.b_ask),
        "b_mid": str(snapshot.b_mid),
        "b_spread": str(snapshot.b_spread),
        "b_bid_sz": str(snapshot.b_bid_sz),
        "b_ask_sz": str(snapshot.b_ask_sz),
        "mid_sum": str(snapshot.mid_sum),
        "spread_sum": str(snapshot.spread_sum),
        "buy_both_cost": str(snapshot.buy_both_cost),
        "sell_both_proceeds": str(snapshot.sell_both_proceeds),
        "depth_bid_5_up": str(snapshot.depth_bid_5_up),
        "depth_ask_5_up": str(snapshot.depth_ask_5_up),
        "depth_bid_5_down": str(snapshot.depth_bid_5_down),
        "depth_ask_5_down": str(snapshot.depth_ask_5_down),
    }


def _signals_for_snapshot(
    snapshot: PairedBookSnapshot,
    mid_sum_threshold: Decimal,
    spread_sum_threshold: Decimal,
) -> list[dict[str, Any]]:
    signals: list[dict[str, Any]] = []
    ts_iso = datetime.fromtimestamp(snapshot.ts_ms / 1000, tz=timezone.utc).isoformat()
    if abs(snapshot.mid_sum - ONE_DOLLAR) >= mid_sum_threshold:
        signals.append(
            _signal_row(
                ts_iso,
                snapshot.condition_id,
                "MID_SUM_DRIFT",
                snapshot.mid_sum,
                {
                    "threshold": str(mid_sum_threshold),
                    "token_a_id": snapshot.token_a_id,
                    "token_b_id": snapshot.token_b_id,
                    "outcome_a": snapshot.outcome_a,
                    "outcome_b": snapshot.outcome_b,
                },
            )
        )
    if snapshot.spread_sum >= spread_sum_threshold:
        signals.append(
            _signal_row(
                ts_iso,
                snapshot.condition_id,
                "SPREAD_SUM_WIDE",
                snapshot.spread_sum,
                {
                    "threshold": str(spread_sum_threshold),
                    "token_a_id": snapshot.token_a_id,
                    "token_b_id": snapshot.token_b_id,
                    "outcome_a": snapshot.outcome_a,
                    "outcome_b": snapshot.outcome_b,
                },
            )
        )
    if snapshot.buy_both_cost <= ONE_DOLLAR:
        signals.append(
            _signal_row(
                ts_iso,
                snapshot.condition_id,
                "BUY_BOTH_UNDER_1",
                snapshot.buy_both_cost,
                {
                    "token_a_id": snapshot.token_a_id,
                    "token_b_id": snapshot.token_b_id,
                    "outcome_a": snapshot.outcome_a,
                    "outcome_b": snapshot.outcome_b,
                },
            )
        )
    if snapshot.sell_both_proceeds >= ONE_DOLLAR:
        signals.append(
            _signal_row(
                ts_iso,
                snapshot.condition_id,
                "SELL_BOTH_OVER_1",
                snapshot.sell_both_proceeds,
                {
                    "token_a_id": snapshot.token_a_id,
                    "token_b_id": snapshot.token_b_id,
                    "outcome_a": snapshot.outcome_a,
                    "outcome_b": snapshot.outcome_b,
                },
            )
        )
    return signals


def _signal_row(
    ts_iso: str, condition_id: str, signal_type: str, value: Decimal, details: dict[str, Any]
) -> dict[str, Any]:
    return {
        "ts_iso": ts_iso,
        "condition_id": condition_id,
        "signal_type": signal_type,
        "value": str(value),
        "details_json": json.dumps(details, ensure_ascii=True, sort_keys=True),
    }


def _parse_json_list(raw_value: str | None) -> list[Any]:
    if not raw_value:
        return []
    parsed = _parse_json_value(raw_value)
    return parsed if isinstance(parsed, list) else []


def _parse_json_value(raw_value: Any) -> Any:
    if raw_value is None:
        return None
    if isinstance(raw_value, (list, dict)):
        return raw_value
    if not isinstance(raw_value, str):
        return None
    try:
        parsed = json.loads(raw_value)
    except json.JSONDecodeError:
        return None
    if isinstance(parsed, str):
        try:
            parsed = json.loads(parsed)
        except json.JSONDecodeError:
            return parsed
    return parsed


def _parse_outcome_tokens(row: dict[str, Any]) -> dict[str, str]:
    tokens_raw = _parse_json_value(row.get("tokens"))
    outcome_tokens: dict[str, str] = {}
    if isinstance(tokens_raw, list):
        for token in tokens_raw:
            if not isinstance(token, dict):
                continue
            token_id = token.get("token_id") or token.get("tokenId") or token.get("id")
            outcome = token.get("outcome") or token.get("name")
            if token_id is None or outcome is None:
                continue
            outcome_tokens[str(outcome)] = str(token_id)
    if outcome_tokens:
        return outcome_tokens
    outcomes = _parse_json_list(row.get("outcomes"))
    clob_token_ids = _parse_json_list(row.get("clob_token_ids"))
    if len(outcomes) == 2 and len(clob_token_ids) == 2:
        for outcome, token_id in zip(outcomes, clob_token_ids):
            outcome_tokens[str(outcome)] = str(token_id)
    if not outcome_tokens:
        logger.warning("Skipping market %s due to missing tokens", row.get("market_id"))
    return outcome_tokens


def _extract_condition_id(row: dict[str, Any]) -> str:
    raw = row.get("raw")
    if raw:
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, dict):
            condition_id = parsed.get("conditionId") or parsed.get("condition_id")
            if condition_id:
                return str(condition_id)
    return str(row.get("market_id") or "")


def _resolve_outcome_pair(
    outcome_tokens: dict[str, str]
) -> tuple[str, str, str, str]:
    labels = list(outcome_tokens.keys())
    labels_lower = {label.lower(): label for label in labels}
    if {"yes", "no"}.issubset(labels_lower.keys()):
        yes_label = labels_lower["yes"]
        no_label = labels_lower["no"]
        return "Yes", "No", outcome_tokens[yes_label], outcome_tokens[no_label]
    if {"up", "down"}.issubset(labels_lower.keys()):
        up_label = labels_lower["up"]
        down_label = labels_lower["down"]
        return "Up", "Down", outcome_tokens[up_label], outcome_tokens[down_label]
    labels_sorted = sorted(labels, key=lambda label: label.lower())
    label_a, label_b = labels_sorted[0], labels_sorted[1]
    return (
        label_a,
        label_b,
        outcome_tokens[label_a],
        outcome_tokens[label_b],
    )

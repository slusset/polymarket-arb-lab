from __future__ import annotations

import json
from typing import Any, Iterable

from pmkt.domain.entities import Event, Market, Token


def _parse_list(raw_value: Any) -> list[str]:
    if raw_value is None:
        return []
    if isinstance(raw_value, list):
        if len(raw_value) == 1 and isinstance(raw_value[0], str):
            parsed = _parse_list(raw_value[0])
            return parsed or [str(raw_value[0])]
        return [str(item) for item in raw_value]
    if isinstance(raw_value, str):
        text = raw_value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, list):
            return [str(item).strip("\"") for item in parsed]
        return [item.strip() for item in text.split(",") if item.strip()]
    return []


def _extract_items(raw_json: Any, key: str) -> list[dict[str, Any]]:
    if raw_json is None:
        return []
    if isinstance(raw_json, list):
        return [item for item in raw_json if isinstance(item, dict)]
    if isinstance(raw_json, dict):
        if key in raw_json and isinstance(raw_json[key], list):
            return [item for item in raw_json[key] if isinstance(item, dict)]
        return [raw_json]
    return []


def _parse_tokens_from_market(
    market_raw: dict[str, Any], outcomes: list[str], clob_token_ids: list[str]
) -> list[Token]:
    tokens_raw = market_raw.get("tokens") or market_raw.get("outcomesTokens") or []
    tokens: list[Token] = []
    if isinstance(tokens_raw, list):
        for idx, token_raw in enumerate(tokens_raw):
            if not isinstance(token_raw, dict):
                continue
            token_id = (
                token_raw.get("token_id")
                or token_raw.get("tokenId")
                or token_raw.get("id")
                or token_raw.get("clobTokenId")
            )
            outcome = token_raw.get("outcome") or token_raw.get("name")
            if not outcome and idx < len(outcomes):
                outcome = outcomes[idx]
            if token_id and outcome:
                tokens.append(Token(token_id=str(token_id), outcome=str(outcome), raw=token_raw))
    if tokens:
        return tokens
    if outcomes and clob_token_ids and len(outcomes) == len(clob_token_ids):
        for outcome, token_id in zip(outcomes, clob_token_ids):
            tokens.append(Token(token_id=str(token_id), outcome=str(outcome), raw={}))
    return tokens


def parse_events(raw_json: Any) -> list[Event]:
    events: list[Event] = []
    for raw in _extract_items(raw_json, "events"):
        event_id = str(raw.get("id") or raw.get("event_id") or "")
        title = str(raw.get("title") or raw.get("question") or "")
        slug = str(raw.get("slug") or raw.get("ticker") or "")
        start_date = raw.get("startDate") or raw.get("start_date")
        end_date = raw.get("endDate") or raw.get("end_date")
        active_raw = raw.get("active")
        closed_raw = raw.get("closed")
        markets = parse_markets(raw.get("markets") or [])
        events.append(
            Event(
                event_id=event_id,
                title=title,
                slug=slug,
                start_date=str(start_date) if start_date else None,
                end_date=str(end_date) if end_date else None,
                active=bool(active_raw) if active_raw is not None else None,
                closed=bool(closed_raw) if closed_raw is not None else None,
                markets=markets,
                raw=raw,
            )
        )
    return events


def parse_markets(raw_json: Any) -> list[Market]:
    markets: list[Market] = []
    for raw in _extract_items(raw_json, "markets"):
        market_id = str(raw.get("id") or raw.get("market_id") or "")
        question = str(raw.get("question") or raw.get("title") or "")
        status = str(raw.get("status") or raw.get("state") or "")
        outcomes_raw = raw.get("outcomes") or raw.get("outcome") or []
        outcomes = _parse_list(outcomes_raw)
        clob_raw = raw.get("clobTokenIds") or raw.get("clob_token_ids")
        clob_token_ids = _parse_list(clob_raw)
        enable_order_book_raw = raw.get("enableOrderBook")
        enable_order_book = (
            bool(enable_order_book_raw) if enable_order_book_raw is not None else None
        )
        accepting_orders_raw = raw.get("acceptingOrders")
        accepting_orders = (
            bool(accepting_orders_raw) if accepting_orders_raw is not None else None
        )
        volume_raw = raw.get("volume") or raw.get("volumeNum") or raw.get("volume24hr")
        volume = float(volume_raw) if volume_raw is not None else None
        tokens = _parse_tokens_from_market(raw, outcomes, clob_token_ids)
        markets.append(
            Market(
                market_id=market_id,
                question=question,
                status=status,
                outcomes=outcomes,
                clob_token_ids=clob_token_ids,
                tokens=tokens,
                enable_order_book=enable_order_book,
                accepting_orders=accepting_orders,
                volume=volume,
                raw=raw,
            )
        )
    return markets


def parse_tokens(markets: Iterable[Market]) -> list[Token]:
    tokens: list[Token] = []
    for market in markets:
        if market.tokens:
            tokens.extend(market.tokens)
            continue
        if market.outcomes and market.clob_token_ids:
            if len(market.outcomes) == len(market.clob_token_ids):
                market.tokens = [
                    Token(token_id=token_id, outcome=outcome, raw={})
                    for outcome, token_id in zip(market.outcomes, market.clob_token_ids)
                ]
                tokens.extend(market.tokens)
    return tokens

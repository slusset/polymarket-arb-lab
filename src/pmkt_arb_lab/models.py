from __future__ import annotations

import json
from typing import Any, Literal

from pydantic import BaseModel, Field


class MarketMetadata(BaseModel):
    market_id: str
    title: str = ""
    status: str = ""
    outcomes: list[str] = Field(default_factory=list)
    clob_token_ids: list[str] = Field(default_factory=list)
    yes_clob_token_id: str | None = None
    no_clob_token_id: str | None = None
    closed: bool | None = None
    active: bool | None = None
    end_date: str | None = None
    start_date: str | None = None
    liquidity: float = 0.0
    volume: float = 0.0
    enable_order_book: bool | None = None
    raw: dict[str, Any] = Field(default_factory=dict)

    @staticmethod
    def _parse_clob_token_ids(raw_value: Any) -> list[str]:
        if raw_value is None:
            return []
        if isinstance(raw_value, list):
            if len(raw_value) == 1 and isinstance(raw_value[0], str):
                parsed = MarketMetadata._parse_clob_token_ids(raw_value[0])
                return parsed or [str(raw_value[0])]
            return [str(x) for x in raw_value]
        if isinstance(raw_value, str):
            text = raw_value.strip()
            if not text:
                return []
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                parsed = None
            if isinstance(parsed, list):
                return [str(x).strip("\"") for x in parsed]
            return [x.strip() for x in text.split(",") if x.strip()]
        return []

    @staticmethod
    def _parse_outcomes(raw_value: Any) -> list[str]:
        if raw_value is None:
            return []
        if isinstance(raw_value, list):
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
                return [str(item) for item in parsed]
            return [item.strip() for item in text.split(",") if item.strip()]
        return []

    @classmethod
    def from_api(cls, data: dict[str, Any]) -> "MarketMetadata":
        market_id = str(data.get("id") or data.get("market_id") or "")
        title = str(data.get("question") or data.get("title") or "")
        status = str(data.get("status") or data.get("state") or "")
        outcomes_raw = data.get("outcomes") or data.get("outcome") or []
        outcomes = cls._parse_outcomes(outcomes_raw)
        clob_raw = data.get("clobTokenIds") or data.get("clob_token_ids")
        clob_token_ids = cls._parse_clob_token_ids(clob_raw)
        yes_token_id = None
        no_token_id = None
        if outcomes and clob_token_ids and len(outcomes) == len(clob_token_ids):
            for outcome, token_id in zip(outcomes, clob_token_ids):
                outcome_norm = outcome.strip().lower()
                if outcome_norm == "yes":
                    yes_token_id = token_id
                elif outcome_norm == "no":
                    no_token_id = token_id
        closed_raw = data.get("closed")
        closed = bool(closed_raw) if closed_raw is not None else None
        active_raw = data.get("active")
        active = bool(active_raw) if active_raw is not None else None
        end_date = data.get("end_date") or data.get("endDate")
        start_date = data.get("start_date") or data.get("startDate")
        liquidity = float(data.get("liquidity") or data.get("liquidityNum") or 0.0)
        volume = float(data.get("volume") or data.get("volume24h") or 0.0)
        enable_order_book_raw = data.get("enableOrderBook")
        enable_order_book = (
            bool(enable_order_book_raw) if enable_order_book_raw is not None else None
        )
        return cls(
            market_id=market_id,
            title=title,
            status=status,
            outcomes=outcomes,
            clob_token_ids=clob_token_ids,
            yes_clob_token_id=yes_token_id,
            no_clob_token_id=no_token_id,
            closed=closed,
            active=active,
            end_date=str(end_date) if end_date else None,
            start_date=str(start_date) if start_date else None,
            liquidity=liquidity,
            volume=volume,
            enable_order_book=enable_order_book,
            raw=data,
        )


class OrderBookTop(BaseModel):
    yes_best_ask: float | None
    yes_best_ask_size: float | None
    no_best_ask: float | None
    no_best_ask_size: float | None
    raw: dict[str, Any] = Field(default_factory=dict)


class OpportunityEvent(BaseModel):
    market_id: str
    quantity: float
    start_ts: str
    end_ts: str
    edge: float
    duration_s: float


OutcomeSemantics = Literal["binary_yes_no", "unknown"]


class MarketClassification(BaseModel):
    market: MarketMetadata
    semantics: OutcomeSemantics
    is_open: bool

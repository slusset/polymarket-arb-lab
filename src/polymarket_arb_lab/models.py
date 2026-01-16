from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class MarketMetadata(BaseModel):
    market_id: str
    title: str = ""
    status: str = ""
    outcomes: list[str] = Field(default_factory=list)
    volume: float = 0.0
    raw: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_api(cls, data: dict[str, Any]) -> "MarketMetadata":
        market_id = str(data.get("id") or data.get("market_id") or "")
        title = str(data.get("question") or data.get("title") or "")
        status = str(data.get("status") or data.get("state") or "")
        outcomes_raw = data.get("outcomes") or data.get("outcome") or []
        outcomes: list[str]
        if isinstance(outcomes_raw, list):
            outcomes = [str(x) for x in outcomes_raw]
        elif isinstance(outcomes_raw, str):
            outcomes = [x.strip() for x in outcomes_raw.split(",") if x.strip()]
        else:
            outcomes = []
        volume = float(data.get("volume") or data.get("volume24h") or 0.0)
        return cls(
            market_id=market_id,
            title=title,
            status=status,
            outcomes=outcomes,
            volume=volume,
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

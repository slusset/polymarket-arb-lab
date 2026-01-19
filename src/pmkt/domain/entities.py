from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class Token:
    token_id: str
    outcome: str
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class Market:
    market_id: str
    question: str = ""
    status: str = ""
    outcomes: list[str] = field(default_factory=list)
    clob_token_ids: list[str] = field(default_factory=list)
    tokens: list[Token] = field(default_factory=list)
    enable_order_book: bool | None = None
    accepting_orders: bool | None = None
    volume: float | None = None
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class Event:
    event_id: str
    title: str = ""
    slug: str = ""
    start_date: str | None = None
    end_date: str | None = None
    active: bool | None = None
    closed: bool | None = None
    markets: list[Market] = field(default_factory=list)
    raw: dict[str, Any] = field(default_factory=dict)

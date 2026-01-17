from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Sequence


@dataclass(frozen=True)
class ApiConfig:
    markets_url: str
    orderbook_url: str
    timeout_s: float
    min_interval_s: float
    max_retries: int
    start_date_min: str | None = None
    end_date_min: str | None = None


@dataclass(frozen=True)
class ScanConfig:
    poll_interval_s: float
    duration_min: float
    max_markets: int | None
    min_volume: float
    quantities: Sequence[float]
    fee_bps: float
    fee_pct: float
    edge_threshold: float
    target_qty: float


@dataclass(frozen=True)
class StorageConfig:
    db_path: str


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_str(name: str) -> str | None:
    raw = os.getenv(name)
    if raw is None:
        return None
    raw = raw.strip()
    return raw or None


def api_config_from_env() -> ApiConfig:
    return ApiConfig(
        markets_url=os.getenv(
            "PM_MARKETS_URL",
            "https://gamma-api.polymarket.com/markets",
        ),
        orderbook_url=os.getenv(
            "PM_ORDERBOOK_URL",
            "https://clob.polymarket.com/book",
        ),
        timeout_s=_env_float("PM_TIMEOUT_S", 10.0),
        min_interval_s=_env_float("PM_MIN_INTERVAL_S", 0.25),
        max_retries=_env_int("PM_MAX_RETRIES", 3),
        start_date_min=_env_str("PM_START_DATE_MIN"),
        end_date_min=_env_str("PM_END_DATE_MIN"),
    )


def storage_config_from_env() -> StorageConfig:
    return StorageConfig(
        db_path=os.getenv("PM_DB_PATH", "data/market.db"),
    )

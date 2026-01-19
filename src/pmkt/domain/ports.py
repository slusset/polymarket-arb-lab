from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from .entities import Event, Market, Token


@dataclass(slots=True)
class UniverseSnapshot:
    events: list[Event]
    markets: list[Market]
    tokens: list[Token]


class UniverseWriter(Protocol):
    def write(self, snapshot: UniverseSnapshot, out_dir: Path) -> None: ...

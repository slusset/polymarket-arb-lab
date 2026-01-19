from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

from .models import OpportunityEvent


@dataclass
class OpportunityTracker:
    edge_threshold: float
    poll_interval_s: float
    open_windows: dict[tuple[str, float], dict[str, str]] = field(default_factory=dict)
    completed: list[OpportunityEvent] = field(default_factory=list)

    def update(
        self,
        ts: str,
        tick: int,
        market_id: str,
        quantity: float,
        edge: float | None,
    ) -> None:
        key = (market_id, quantity)
        is_profitable = edge is not None and edge > self.edge_threshold
        if is_profitable and key not in self.open_windows:
            self.open_windows[key] = {
                "start": ts,
                "edge": str(edge),
                "start_tick": str(tick),
            }
            return
        if not is_profitable and key in self.open_windows:
            start_ts = self.open_windows[key]["start"]
            start_edge = float(self.open_windows[key]["edge"])
            start_tick = int(self.open_windows[key]["start_tick"])
            duration_s = max(1, tick - start_tick) * self.poll_interval_s
            self.completed.append(
                OpportunityEvent(
                    market_id=market_id,
                    quantity=quantity,
                    start_ts=start_ts,
                    end_ts=ts,
                    edge=start_edge,
                    duration_s=duration_s,
                )
            )
            self.open_windows.pop(key, None)

    def close_all(self, ts: str, tick: int) -> None:
        for key, info in list(self.open_windows.items()):
            market_id, quantity = key
            start_ts = info["start"]
            start_edge = float(info["edge"])
            start_tick = int(info["start_tick"])
            duration_s = max(1, tick - start_tick) * self.poll_interval_s
            self.completed.append(
                OpportunityEvent(
                    market_id=market_id,
                    quantity=quantity,
                    start_ts=start_ts,
                    end_ts=ts,
                    edge=start_edge,
                    duration_s=duration_s,
                )
            )
            self.open_windows.pop(key, None)

    def events(self) -> Iterable[OpportunityEvent]:
        return list(self.completed)

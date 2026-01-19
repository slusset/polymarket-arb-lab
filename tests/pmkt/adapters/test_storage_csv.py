import csv
import json
from pathlib import Path

from pmkt.adapters.storage_csv import CsvUniverseWriter
from pmkt.domain.ports import UniverseSnapshot
from pmkt.gamma.normalize import parse_events, parse_tokens


def _load_fixture_events() -> list[dict[str, object]]:
    repo_root = Path(__file__).resolve().parents[3]
    path = repo_root / "data" / "polymarket_events_10.json"
    with path.open(encoding="utf-8") as handle:
        data = json.load(handle)
    assert isinstance(data, list)
    return data


def test_csv_universe_writer(tmp_path: Path) -> None:
    raw_events = _load_fixture_events()
    events = parse_events(raw_events)
    markets = [market for event in events for market in event.markets]
    tokens = parse_tokens(markets)
    snapshot = UniverseSnapshot(events=events, markets=markets, tokens=tokens)

    writer = CsvUniverseWriter()
    writer.write(snapshot, tmp_path)

    events_path = tmp_path / "events.csv"
    markets_path = tmp_path / "markets.csv"
    tokens_path = tmp_path / "tokens.csv"
    watchlist_path = tmp_path / "watchlist.csv"
    watchlist_future_path = tmp_path / "watchlist_future.csv"

    assert events_path.exists()
    assert markets_path.exists()
    assert tokens_path.exists()
    assert watchlist_path.exists()
    assert watchlist_future_path.exists()

    with events_path.open(encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == len(snapshot.events)
    assert "event_id" in rows[0]

    with markets_path.open(encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == len(snapshot.markets)
    assert "market_id" in rows[0]

    with tokens_path.open(encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == len(snapshot.tokens)
    assert "token_id" in rows[0]

    with watchlist_path.open(encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    if rows:
        assert "market_id" in rows[0]

    with watchlist_future_path.open(encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    if rows:
        assert "market_id" in rows[0]

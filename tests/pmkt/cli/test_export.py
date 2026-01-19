import csv
from pathlib import Path

from pmkt.cli import main


def test_export_command_with_fixture(tmp_path: Path) -> None:
    out_dir = tmp_path / "snapshot"
    main(
        [
            "export",
            "--input",
            "data/polymarket_events_10.json",
            "--out",
            str(out_dir),
        ]
    )

    events_path = out_dir / "events.csv"
    markets_path = out_dir / "markets.csv"
    tokens_path = out_dir / "tokens.csv"

    assert events_path.exists()
    assert markets_path.exists()
    assert tokens_path.exists()

    with events_path.open(encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert rows

    with markets_path.open(encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert rows

    with tokens_path.open(encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert rows

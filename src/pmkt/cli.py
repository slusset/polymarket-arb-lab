from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from pmkt.adapters.storage_csv import CsvUniverseWriter
from pmkt.clob.paired_recorder import build_market_index, load_tradable_pairs, record_paired_quotes
from pmkt.domain.ports import UniverseSnapshot
from pmkt.gamma.client import GammaClient
from pmkt.gamma.normalize import parse_events, parse_tokens

DEFAULT_EXPORT_LIMIT = 1000

def _setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pmarb")
    parser.add_argument(
        "--log-level",
        choices=("DEBUG", "INFO", "WARNING"),
        default="INFO",
        help="Logging verbosity",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    export_cmd = sub.add_parser("export", help="Export normalized universe snapshot to CSV")
    export_cmd.add_argument(
        "--out",
        type=str,
        default=None,
        help="Output directory (default: data/snapshots/<UTC_TIMESTAMP>)",
    )
    export_cmd.add_argument(
        "--event-set",
        choices=("open", "closed", "all"),
        default="open",
        help="Event subset to export (default: open)",
    )
    export_cmd.add_argument(
        "--input",
        type=str,
        default=None,
        help="Optional path to a local events JSON file",
    )
    export_cmd.add_argument("--limit", type=int, default=DEFAULT_EXPORT_LIMIT)
    export_cmd.add_argument(
        "--order",
        choices=("id", "volume", "liquidity", "createdAt"),
        default="id",
    )
    export_cmd.add_argument("--ascending", action="store_true", default=False)
    export_cmd.add_argument(
        "--log-level",
        choices=("DEBUG", "INFO", "WARNING"),
        default=None,
        help="Override global log level",
    )

    paired_cmd = sub.add_parser("paired-quotes", help="Record paired CLOB quotes to CSV")
    paired_cmd.add_argument(
        "--markets-csv",
        type=str,
        default="data/exports/markets.csv",
        help="Path to exported markets.csv",
    )
    paired_cmd.add_argument(
        "--out",
        type=str,
        default=None,
        help="Output directory (default: data/marketdata/<UTC_TIMESTAMP>)",
    )
    paired_cmd.add_argument("--interval", type=float, default=2.0)
    paired_cmd.add_argument("--iters", type=int, default=None)
    paired_cmd.add_argument(
        "--log-level",
        choices=("DEBUG", "INFO", "WARNING"),
        default=None,
        help="Override global log level",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)
    _setup_logging(args.log_level or "INFO")

    if args.command == "export":
        if args.log_level:
            _setup_logging(args.log_level)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        out_dir = Path(args.out) if args.out else Path("data") / "snapshots" / timestamp
        if args.input:
            input_path = Path(args.input)
            with input_path.open(encoding="utf-8") as handle:
                raw_events = json.load(handle)
        else:
            gamma_client = GammaClient()
            try:
                closed_param = None
                if args.event_set == "open":
                    closed_param = False
                elif args.event_set == "closed":
                    closed_param = True
                raw_events = gamma_client.fetch_events(
                    closed=closed_param,
                    limit=args.limit,
                    order=args.order,
                    ascending=args.ascending,
                )
            finally:
                gamma_client.close()
            if args.event_set == "open" and isinstance(raw_events, list):
                raw_events = _filter_future_events(raw_events)
        events = parse_events(raw_events)
        markets = [market for event in events for market in event.markets]
        tokens = parse_tokens(markets)
        snapshot = UniverseSnapshot(events=events, markets=markets, tokens=tokens)
        CsvUniverseWriter().write(snapshot, out_dir)
        print(
            f"Exported snapshot to {out_dir} "
            f"(events={len(events)}, markets={len(markets)}, tokens={len(tokens)})"
        )
        return

    if args.command == "paired-quotes":
        if args.log_level:
            _setup_logging(args.log_level)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        out_dir = Path(args.out) if args.out else Path("data") / "marketdata" / timestamp
        markets_csv = Path(args.markets_csv)
        pairs = load_tradable_pairs(markets_csv)
        market_index = build_market_index(markets_csv)
        record_paired_quotes(
            pairs,
            out_dir=out_dir,
            interval_seconds=args.interval,
            max_iters=args.iters,
            market_index=market_index,
        )
        print(f"Recorded paired quotes to {out_dir} (pairs={len(pairs)})")
        return


if __name__ == "__main__":
    main()


def _filter_future_events(raw_events: list[dict[str, object]]) -> list[dict[str, object]]:
    now = datetime.now(timezone.utc)
    filtered: list[dict[str, object]] = []
    for event in raw_events:
        if not isinstance(event, dict):
            continue
        if event.get("closed") is True:
            continue
        end_date = event.get("endDate") or event.get("end_date")
        parsed_end = _parse_iso_timestamp(end_date)
        if parsed_end is None:
            if event.get("active") is True:
                filtered.append(event)
            continue
        if parsed_end >= now:
            filtered.append(event)
    return filtered


def _parse_iso_timestamp(value: object) -> datetime | None:
    if not value:
        return None
    if not isinstance(value, str):
        value = str(value)
    text = value.strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text)
    except ValueError:
        return None

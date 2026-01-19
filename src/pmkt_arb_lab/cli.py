from __future__ import annotations

import argparse
import logging
from pathlib import Path

from .client import ApiClient
from .config import ApiConfig, api_config_from_env, storage_config_from_env
from .logic import FeeModel
from .models import MarketMetadata
from .report import analyze_snapshots, render_report
from .scanner import load_offline_fixture, scan_markets
from .storage import Storage
from .utils import parse_quantities

DEFAULT_QUANTITIES = [1, 5, 10, 25, 50]


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

    scan = sub.add_parser("scan", help="Poll markets and store snapshots")
    scan.add_argument("--poll-interval", type=float, default=5.0)
    scan.add_argument("--duration-min", type=float, default=5.0)
    scan.add_argument("--max-markets", type=int, default=None)
    scan.add_argument("--min-volume", type=float, default=0.0)
    scan.add_argument("--min-liquidity", type=float, default=0.0)
    scan.add_argument("--start-date-min", type=str, default=None)
    scan.add_argument("--end-date-min", type=str, default=None)
    scan.add_argument("--progress-every", type=int, default=1)
    scan.add_argument("--quantities", type=str, default=",")
    scan.add_argument("--fee-bps", type=float, default=0.0)
    scan.add_argument("--fee-pct", type=float, default=0.0)
    scan.add_argument("--overhead", type=float, default=0.0)
    scan.add_argument("--edge-threshold", type=float, default=0.0)
    scan.add_argument("--db-path", type=str, default=None)
    scan.add_argument("--offline-fixture", type=str, default=None)
    scan.add_argument(
        "--log-level",
        choices=("DEBUG", "INFO", "WARNING"),
        default=None,
        help="Override global log level",
    )

    report = sub.add_parser("report", help="Generate a markdown report")
    report.add_argument("--db-path", type=str, default=None)
    report.add_argument("--out-path", type=str, default="data/report.md")
    report.add_argument("--quantities", type=str, default=",")
    report.add_argument("--fee-bps", type=float, default=0.0)
    report.add_argument("--fee-pct", type=float, default=0.0)
    report.add_argument("--overhead", type=float, default=0.0)
    report.add_argument("--edge-threshold", type=float, default=0.0)
    report.add_argument(
        "--log-level",
        choices=("DEBUG", "INFO", "WARNING"),
        default=None,
        help="Override global log level",
    )

    inspect = sub.add_parser("inspect", help="Inspect current markets")
    inspect.add_argument("--limit", type=int, default=20)
    inspect.add_argument("--start-date-min", type=str, default=None)
    inspect.add_argument("--end-date-min", type=str, default=None)
    inspect.add_argument(
        "--log-level",
        choices=("DEBUG", "INFO", "WARNING"),
        default=None,
        help="Override global log level",
    )

    return parser


def _resolve_quantities(raw: str) -> list[float]:
    quantities = parse_quantities(raw)
    return quantities if quantities else list(DEFAULT_QUANTITIES)


def main(argv: list[str] | None = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)
    _setup_logging(args.log_level or "INFO")

    if args.command == "scan":
        if args.log_level:
            _setup_logging(args.log_level)
        storage_cfg = storage_config_from_env()
        db_path = args.db_path or storage_cfg.db_path
        storage = Storage(db_path)
        quantities = _resolve_quantities(args.quantities)
        fee_model = FeeModel(bps_per_leg=args.fee_bps, pct_per_leg=args.fee_pct)
        offline_fixture = None
        api_client = None
        if args.offline_fixture:
            offline_fixture = load_offline_fixture(args.offline_fixture)
        else:
            api_cfg = api_config_from_env()
            api_cfg = ApiConfig(
                markets_url=api_cfg.markets_url,
                orderbook_url=api_cfg.orderbook_url,
                timeout_s=api_cfg.timeout_s,
                min_interval_s=api_cfg.min_interval_s,
                max_retries=api_cfg.max_retries,
                start_date_min=args.start_date_min or api_cfg.start_date_min,
                end_date_min=args.end_date_min or api_cfg.end_date_min,
            )
            api_client = ApiClient(api_cfg)
        try:
            scan_markets(
                storage=storage,
                api_client=api_client,
                poll_interval_s=args.poll_interval,
                duration_min=args.duration_min,
                max_markets=args.max_markets,
                min_volume=args.min_volume,
                min_liquidity=args.min_liquidity,
                quantities=quantities,
                fee_model=fee_model,
                overhead=args.overhead,
                edge_threshold=args.edge_threshold,
                progress_every=args.progress_every,
                offline_fixture=offline_fixture,
            )
        finally:
            if api_client:
                api_client.close()
            storage.close()
        return

    if args.command == "report":
        if args.log_level:
            _setup_logging(args.log_level)
        storage_cfg = storage_config_from_env()
        db_path = args.db_path or storage_cfg.db_path
        storage = Storage(db_path)
        quantities = _resolve_quantities(args.quantities)
        fee_model = FeeModel(bps_per_leg=args.fee_bps, pct_per_leg=args.fee_pct)
        rows = [dict(row) for row in storage.fetch_snapshots()]
        results = analyze_snapshots(
            rows,
            quantities=quantities,
            fee_model=fee_model,
            overhead=args.overhead,
            edge_threshold=args.edge_threshold,
        )
        report_md = render_report(results)
        out_path = Path(args.out_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(report_md, encoding="utf-8")
        print(report_md)
        storage.close()
        return

    if args.command == "inspect":
        if args.log_level:
            _setup_logging(args.log_level)
        api_cfg = api_config_from_env()
        api_cfg = ApiConfig(
            markets_url=api_cfg.markets_url,
            orderbook_url=api_cfg.orderbook_url,
            timeout_s=api_cfg.timeout_s,
            min_interval_s=api_cfg.min_interval_s,
            max_retries=api_cfg.max_retries,
            start_date_min=args.start_date_min or api_cfg.start_date_min,
            end_date_min=args.end_date_min or api_cfg.end_date_min,
        )
        api_client = ApiClient(api_cfg)
        try:
            markets_data = api_client.fetch_markets()
        finally:
            api_client.close()
        markets = [MarketMetadata.from_api(m) for m in markets_data]
        rows = markets[: args.limit]
        header = (
            f"{'market_id':<20} {'closed':<6} {'end_date':<20} {'liquidity':>10} "
            f"{'volume':>10} {'yes_token':<12} {'no_token':<12} question"
        )
        print(header.rstrip())
        for market in rows:
            question = market.title.replace("\n", " ").strip()
            if len(question) > 60:
                question = question[:57] + "..."
            print(
                f"{market.market_id:<20} {str(market.closed):<6} {str(market.end_date):<20} "
                f"{market.liquidity:>10.2f} {market.volume:>10.2f} "
                f"{str(market.yes_clob_token_id):<12} {str(market.no_clob_token_id):<12} {question}"
            )
        return

if __name__ == "__main__":
    main()

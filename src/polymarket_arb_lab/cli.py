from __future__ import annotations

import argparse
import logging
from pathlib import Path

from .client import ApiClient
from .config import api_config_from_env, storage_config_from_env
from .logic import FeeModel
from .report import analyze_snapshots, render_report
from .scanner import load_offline_fixture, scan_markets
from .storage import Storage
from .utils import parse_quantities

DEFAULT_QUANTITIES = [1, 5, 10, 25, 50]


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pmarb")
    sub = parser.add_subparsers(dest="command", required=True)

    scan = sub.add_parser("scan", help="Poll markets and store snapshots")
    scan.add_argument("--poll-interval", type=float, default=5.0)
    scan.add_argument("--duration-min", type=float, default=5.0)
    scan.add_argument("--max-markets", type=int, default=None)
    scan.add_argument("--min-volume", type=float, default=0.0)
    scan.add_argument("--quantities", type=str, default=",")
    scan.add_argument("--fee-bps", type=float, default=0.0)
    scan.add_argument("--fee-pct", type=float, default=0.0)
    scan.add_argument("--overhead", type=float, default=0.0)
    scan.add_argument("--edge-threshold", type=float, default=0.0)
    scan.add_argument("--db-path", type=str, default=None)
    scan.add_argument("--offline-fixture", type=str, default=None)

    report = sub.add_parser("report", help="Generate a markdown report")
    report.add_argument("--db-path", type=str, default=None)
    report.add_argument("--out-path", type=str, default="data/report.md")
    report.add_argument("--quantities", type=str, default=",")
    report.add_argument("--fee-bps", type=float, default=0.0)
    report.add_argument("--fee-pct", type=float, default=0.0)
    report.add_argument("--overhead", type=float, default=0.0)
    report.add_argument("--edge-threshold", type=float, default=0.0)
    return parser


def _resolve_quantities(raw: str) -> list[float]:
    quantities = parse_quantities(raw)
    return quantities if quantities else list(DEFAULT_QUANTITIES)


def main() -> None:
    _setup_logging()
    parser = _build_parser()
    args = parser.parse_args()

    storage_cfg = storage_config_from_env()
    db_path = args.db_path or storage_cfg.db_path
    storage = Storage(db_path)

    if args.command == "scan":
        quantities = _resolve_quantities(args.quantities)
        fee_model = FeeModel(bps_per_leg=args.fee_bps, pct_per_leg=args.fee_pct)
        offline_fixture = None
        api_client = None
        if args.offline_fixture:
            offline_fixture = load_offline_fixture(args.offline_fixture)
        else:
            api_cfg = api_config_from_env()
            api_client = ApiClient(api_cfg)
        try:
            scan_markets(
                storage=storage,
                api_client=api_client,
                poll_interval_s=args.poll_interval,
                duration_min=args.duration_min,
                max_markets=args.max_markets,
                min_volume=args.min_volume,
                quantities=quantities,
                fee_model=fee_model,
                overhead=args.overhead,
                edge_threshold=args.edge_threshold,
                offline_fixture=offline_fixture,
            )
        finally:
            if api_client:
                api_client.close()
            storage.close()
        return

    if args.command == "report":
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


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

from catalyst_radar.connectors.csv_market import load_daily_bars_csv, load_securities_csv
from catalyst_radar.core.config import AppConfig
from catalyst_radar.pipeline.scan import run_scan
from catalyst_radar.storage.db import create_schema, engine_from_url
from catalyst_radar.storage.repositories import MarketRepository


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="catalyst-radar")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init-db")

    ingest = subparsers.add_parser("ingest-csv")
    ingest.add_argument("--securities", type=Path, required=True)
    ingest.add_argument("--daily-bars", type=Path, required=True)
    ingest.add_argument("--holdings", type=Path)

    scan = subparsers.add_parser("scan")
    scan.add_argument("--as-of", type=date.fromisoformat, required=True)

    return parser


def main(argv: list[str] | None = None) -> int:
    load_dotenv(".env.local")
    args = build_parser().parse_args(argv)
    config = AppConfig.from_env()
    engine = engine_from_url(config.database_url)

    if args.command == "init-db":
        create_schema(engine)
        print("initialized database")
        return 0

    if args.command == "ingest-csv":
        create_schema(engine)
        repo = MarketRepository(engine)
        securities = load_securities_csv(args.securities)
        daily_bars = load_daily_bars_csv(args.daily_bars)
        repo.upsert_securities(securities)
        repo.upsert_daily_bars(daily_bars)
        print(f"ingested securities={len(securities)} daily_bars={len(daily_bars)}")
        return 0

    if args.command == "scan":
        create_schema(engine)
        repo = MarketRepository(engine)
        results = run_scan(repo, as_of=args.as_of)
        for result in results:
            repo.save_scan_result(result.candidate, result.policy)
        print(f"scanned candidates={len(results)}")
        return 0

    raise ValueError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())

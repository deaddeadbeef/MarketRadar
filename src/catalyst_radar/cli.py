from __future__ import annotations

import argparse
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv

from catalyst_radar.connectors.base import (
    ConnectorHealth,
    ConnectorHealthStatus,
    ConnectorRecordKind,
    ConnectorRequest,
    NormalizedRecord,
)
from catalyst_radar.connectors.market_data import CsvMarketDataConnector
from catalyst_radar.core.config import AppConfig
from catalyst_radar.core.models import (
    DailyBar,
    DataQualitySeverity,
    HoldingSnapshot,
    JobStatus,
    Security,
)
from catalyst_radar.pipeline.scan import run_scan
from catalyst_radar.storage.db import create_schema, engine_from_url
from catalyst_radar.storage.provider_repositories import ProviderRepository
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

    provider_health = subparsers.add_parser("provider-health")
    provider_health.add_argument("--provider", required=True)

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
        market_repo = MarketRepository(engine)
        provider_repo = ProviderRepository(engine)
        connector = CsvMarketDataConnector(
            securities_path=args.securities,
            daily_bars_path=args.daily_bars,
            holdings_path=args.holdings,
        )
        return _ingest_csv_provider(
            connector=connector,
            market_repo=market_repo,
            provider_repo=provider_repo,
            securities_path=args.securities,
            daily_bars_path=args.daily_bars,
            holdings_path=args.holdings,
        )

    if args.command == "provider-health":
        create_schema(engine)
        provider_repo = ProviderRepository(engine)
        health = provider_repo.latest_health(args.provider)
        if health is None:
            print(f"provider={args.provider} status=unknown")
            return 1
        print(f"provider={health.provider} status={health.status.value}")
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


def _ingest_csv_provider(
    *,
    connector: CsvMarketDataConnector,
    market_repo: MarketRepository,
    provider_repo: ProviderRepository,
    securities_path: Path,
    daily_bars_path: Path,
    holdings_path: Path | None,
) -> int:
    health = connector.healthcheck()
    provider_repo.save_health(health)
    job_id = provider_repo.start_job(
        "csv_ingest",
        connector.provider,
        metadata={
            "securities": str(securities_path),
            "daily_bars": str(daily_bars_path),
            "holdings": str(holdings_path) if holdings_path is not None else None,
        },
    )
    if health.status == ConnectorHealthStatus.DOWN:
        provider_repo.finish_job(
            job_id,
            JobStatus.FAILED.value,
            requested_count=0,
            raw_count=0,
            normalized_count=0,
            error_summary=health.reason,
        )
        provider_repo.record_incident(
            provider=connector.provider,
            severity=DataQualitySeverity.CRITICAL,
            kind="csv_ingest",
            affected_tickers=(),
            reason=health.reason,
            fail_closed_action="abort-ingest",
            payload={
                "securities": str(securities_path),
                "daily_bars": str(daily_bars_path),
                "holdings": str(holdings_path) if holdings_path is not None else None,
            },
        )
        print(f"csv ingest failed: {health.reason}", file=sys.stderr)
        return 1

    raw_count = 0
    normalized_count = 0
    requested_count = 0
    try:
        request = ConnectorRequest(
            provider=connector.provider,
            endpoint="csv_ingest",
            params={
                "securities": str(securities_path),
                "daily_bars": str(daily_bars_path),
                "holdings": str(holdings_path) if holdings_path is not None else None,
            },
            requested_at=datetime.now(UTC),
        )
        raw_records = connector.fetch(request)
        requested_count = len(raw_records) + len(connector.rejected_payloads)
        raw_count = provider_repo.save_raw_records(raw_records)
        normalized_records = connector.normalize(raw_records)
        normalized_count = provider_repo.save_normalized_records(normalized_records)

        _record_rejected_payloads(provider_repo, connector)
        if connector.rejected_payloads:
            degraded_health = ConnectorHealth(
                provider=connector.provider,
                status=ConnectorHealthStatus.DEGRADED,
                checked_at=datetime.now(UTC),
                reason=f"rejected payloads={len(connector.rejected_payloads)}",
            )
            provider_repo.save_health(degraded_health)

        securities = _securities_from_normalized(normalized_records)
        daily_bars = _daily_bars_from_normalized(normalized_records)
        holdings = _holdings_from_normalized(normalized_records)
        market_repo.upsert_market_snapshot(
            securities_rows=securities,
            daily_bar_rows=daily_bars,
            holding_rows=holdings if holdings_path is not None else (),
        )

        provider_repo.finish_job(
            job_id,
            (
                JobStatus.PARTIAL_SUCCESS.value
                if connector.rejected_payloads
                else JobStatus.SUCCESS.value
            ),
            requested_count=requested_count,
            raw_count=raw_count,
            normalized_count=normalized_count,
            error_summary=(
                f"rejected payloads={len(connector.rejected_payloads)}"
                if connector.rejected_payloads
                else None
            ),
        )
        message = f"ingested securities={len(securities)} daily_bars={len(daily_bars)}"
        if holdings_path is not None:
            message = f"{message} holdings={len(holdings)}"
        print(message)
        return 0
    except Exception as exc:
        reason = str(exc)
        provider_repo.save_health(
            ConnectorHealth(
                provider=connector.provider,
                status=ConnectorHealthStatus.DOWN,
                checked_at=datetime.now(UTC),
                reason=reason,
            )
        )
        provider_repo.finish_job(
            job_id,
            JobStatus.FAILED.value,
            requested_count=requested_count,
            raw_count=raw_count,
            normalized_count=normalized_count,
            error_summary=reason,
        )
        provider_repo.record_incident(
            provider=connector.provider,
            severity=DataQualitySeverity.CRITICAL,
            kind="csv_ingest",
            affected_tickers=(),
            reason=reason,
            fail_closed_action="abort-ingest",
            payload={
                "securities": str(securities_path),
                "daily_bars": str(daily_bars_path),
                "holdings": str(holdings_path) if holdings_path is not None else None,
            },
        )
        print(f"csv ingest failed: {reason}", file=sys.stderr)
        return 1


def _record_rejected_payloads(
    provider_repo: ProviderRepository,
    connector: CsvMarketDataConnector,
) -> None:
    for rejected in connector.rejected_payloads:
        provider_repo.record_incident(
            provider=rejected.provider,
            severity=rejected.severity,
            kind=rejected.kind.value,
            affected_tickers=rejected.affected_tickers,
            reason=rejected.reason,
            fail_closed_action=rejected.fail_closed_action,
            payload=rejected.payload,
        )


def _securities_from_normalized(records: Sequence[NormalizedRecord]) -> list[Security]:
    return [
        _security_from_payload(record.payload)
        for record in records
        if record.kind == ConnectorRecordKind.SECURITY
    ]


def _daily_bars_from_normalized(records: Sequence[NormalizedRecord]) -> list[DailyBar]:
    return [
        _daily_bar_from_payload(record.payload)
        for record in records
        if record.kind == ConnectorRecordKind.DAILY_BAR
    ]


def _holdings_from_normalized(records: Sequence[NormalizedRecord]) -> list[HoldingSnapshot]:
    return [
        _holding_from_payload(record.payload)
        for record in records
        if record.kind == ConnectorRecordKind.HOLDING
    ]


def _security_from_payload(payload: Mapping[str, Any]) -> Security:
    return Security(
        ticker=str(payload["ticker"]).upper(),
        name=str(payload["name"]),
        exchange=str(payload["exchange"]),
        sector=str(payload["sector"]),
        industry=str(payload["industry"]),
        market_cap=float(payload["market_cap"]),
        avg_dollar_volume_20d=float(payload["avg_dollar_volume_20d"]),
        has_options=bool(payload["has_options"]),
        is_active=bool(payload["is_active"]),
        updated_at=_parse_datetime(payload["updated_at"]),
    )


def _daily_bar_from_payload(payload: Mapping[str, Any]) -> DailyBar:
    return DailyBar(
        ticker=str(payload["ticker"]).upper(),
        date=pd.Timestamp(payload["date"]).date(),
        open=float(payload["open"]),
        high=float(payload["high"]),
        low=float(payload["low"]),
        close=float(payload["close"]),
        volume=int(payload["volume"]),
        vwap=float(payload["vwap"]),
        adjusted=bool(payload["adjusted"]),
        provider=str(payload["provider"]),
        source_ts=_parse_datetime(payload["source_ts"]),
        available_at=_parse_datetime(payload["available_at"]),
    )


def _holding_from_payload(payload: Mapping[str, Any]) -> HoldingSnapshot:
    return HoldingSnapshot(
        ticker=str(payload["ticker"]).upper(),
        shares=float(payload["shares"]),
        market_value=float(payload["market_value"]),
        sector=str(payload["sector"]),
        theme=str(payload["theme"]),
        as_of=_parse_datetime(payload["as_of"]),
    )


def _parse_datetime(value: Any) -> datetime:
    parsed = pd.Timestamp(value).to_pydatetime()
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


if __name__ == "__main__":
    raise SystemExit(main())

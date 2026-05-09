from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from sqlalchemy import create_engine

from catalyst_radar.connectors.base import (
    ConnectorRecordKind,
    ConnectorRequest,
    NormalizedRecord,
)
from catalyst_radar.connectors.csv_market import load_daily_bars_csv, load_securities_csv
from catalyst_radar.connectors.market_data import CsvMarketDataConnector
from catalyst_radar.core.models import DailyBar, DataQualitySeverity
from catalyst_radar.pipeline.scan import run_scan
from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.provider_repositories import ProviderRepository
from catalyst_radar.storage.repositories import MarketRepository


def test_raw_daily_bar_missing_available_at_is_rejected_before_normalization(
    tmp_path: Path,
) -> None:
    daily_bars_csv = tmp_path / "daily_bars_missing_available_at.csv"
    daily_bars_csv.write_text(
        "\n".join(
            [
                "ticker,date,open,high,low,close,volume,vwap,adjusted,provider,"
                "source_ts,available_at",
                "BAD,2026-05-08,1,2,1,2,1000,2,true,sample,2026-05-08T20:00:00Z,",
            ]
        ),
        encoding="utf-8",
    )
    connector = CsvMarketDataConnector(
        securities_path="tests/fixtures/securities.csv",
        daily_bars_path=daily_bars_csv,
    )
    request = ConnectorRequest(
        provider="csv",
        endpoint="csv_ingest",
        params={},
        requested_at=datetime(2026, 5, 8, 21, tzinfo=UTC),
    )

    raw_records = connector.fetch(request)
    normalized_records = connector.normalize(raw_records)

    assert not [
        record
        for record in raw_records
        if record.kind == ConnectorRecordKind.DAILY_BAR
    ]
    assert not [
        record
        for record in normalized_records
        if record.kind == ConnectorRecordKind.DAILY_BAR
    ]
    assert len(connector.rejected_payloads) == 1
    rejected = connector.rejected_payloads[0]
    assert rejected.severity == DataQualitySeverity.CRITICAL
    assert rejected.fail_closed_action == "abort-ingest"
    assert "available_at" in rejected.reason


def test_naive_daily_bar_source_ts_is_rejected_before_normalization(
    tmp_path: Path,
) -> None:
    daily_bars_csv = tmp_path / "daily_bars_naive_source_ts.csv"
    daily_bars_csv.write_text(
        "\n".join(
            [
                "ticker,date,open,high,low,close,volume,vwap,adjusted,provider,"
                "source_ts,available_at",
                "BAD,2026-05-08,1,2,1,2,1000,2,true,sample,2026-05-08T20:00:00,"
                "2026-05-08T21:00:00Z",
            ]
        ),
        encoding="utf-8",
    )
    connector = CsvMarketDataConnector(
        securities_path="tests/fixtures/securities.csv",
        daily_bars_path=daily_bars_csv,
    )

    raw_records = connector.fetch(_request())

    assert not [
        record
        for record in raw_records
        if record.kind == ConnectorRecordKind.DAILY_BAR
    ]
    rejected = connector.rejected_payloads[0]
    assert rejected.severity == DataQualitySeverity.ERROR
    assert "source_ts must include timezone information" in rejected.reason


def test_naive_daily_bar_available_at_fails_closed(
    tmp_path: Path,
) -> None:
    daily_bars_csv = tmp_path / "daily_bars_naive_available_at.csv"
    daily_bars_csv.write_text(
        "\n".join(
            [
                "ticker,date,open,high,low,close,volume,vwap,adjusted,provider,"
                "source_ts,available_at",
                "BAD,2026-05-08,1,2,1,2,1000,2,true,sample,2026-05-08T20:00:00Z,"
                "2026-05-08T21:00:00",
            ]
        ),
        encoding="utf-8",
    )
    connector = CsvMarketDataConnector(
        securities_path="tests/fixtures/securities.csv",
        daily_bars_path=daily_bars_csv,
    )

    raw_records = connector.fetch(_request())

    assert not [
        record
        for record in raw_records
        if record.kind == ConnectorRecordKind.DAILY_BAR
    ]
    rejected = connector.rejected_payloads[0]
    assert rejected.severity == DataQualitySeverity.CRITICAL
    assert rejected.fail_closed_action == "abort-ingest"
    assert "available_at must include timezone information" in rejected.reason


def test_normalized_daily_bar_missing_available_at_is_rejected_before_persistence() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    create_schema(engine)
    repo = ProviderRepository(engine)
    missing_available_at = object.__new__(NormalizedRecord)
    object.__setattr__(missing_available_at, "provider", "csv")
    object.__setattr__(missing_available_at, "kind", ConnectorRecordKind.DAILY_BAR)
    object.__setattr__(missing_available_at, "identity", "AAA:2026-05-08")
    object.__setattr__(missing_available_at, "payload", {"ticker": "AAA"})
    object.__setattr__(
        missing_available_at,
        "source_ts",
        datetime(2026, 5, 8, 20, tzinfo=UTC),
    )
    object.__setattr__(missing_available_at, "available_at", None)
    object.__setattr__(missing_available_at, "raw_payload_hash", "hash")

    with pytest.raises(ValueError, match="datetime values must be timezone-aware"):
        repo.save_normalized_records([missing_available_at])


def test_future_available_bullish_bar_is_invisible_until_available() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    create_schema(engine)
    repo = MarketRepository(engine)
    fixture_dir = Path("tests/fixtures")
    repo.upsert_securities(load_securities_csv(fixture_dir / "securities.csv"))
    repo.upsert_daily_bars(load_daily_bars_csv(fixture_dir / "daily_bars.csv"))
    repo.upsert_daily_bars([_future_available_bullish_bar()])

    persisted_latest = repo.daily_bars("AAA", end=date(2026, 5, 8), lookback=1)
    earlier_visible = repo.daily_bars(
        "AAA",
        end=date(2026, 5, 8),
        lookback=1,
        available_at=datetime(2026, 5, 8, 21, tzinfo=UTC),
    )
    earlier_results = run_scan(repo, as_of=date(2026, 5, 8))
    earlier_aaa = next(result for result in earlier_results if result.ticker == "AAA")
    later_results = run_scan(repo, as_of=date(2026, 5, 9))
    later_aaa = next(result for result in later_results if result.ticker == "AAA")

    assert persisted_latest[0].close == 999
    assert earlier_visible[0].date == date(2026, 5, 7)
    assert earlier_visible[0].close == 105
    assert len(earlier_results) == 3
    assert earlier_aaa.candidate.entry_zone == (102.9, 107.1)
    assert later_aaa.candidate.entry_zone == (979.02, 1018.98)


def _future_available_bullish_bar() -> DailyBar:
    return DailyBar(
        ticker="AAA",
        date=date(2026, 5, 8),
        open=900,
        high=1000,
        low=880,
        close=999,
        volume=9_999_999,
        vwap=990,
        adjusted=True,
        provider="sample",
        source_ts=datetime(2026, 5, 8, 22, tzinfo=UTC),
        available_at=datetime(2026, 5, 8, 22, tzinfo=UTC),
    )


def _request() -> ConnectorRequest:
    return ConnectorRequest(
        provider="csv",
        endpoint="csv_ingest",
        params={},
        requested_at=datetime(2026, 5, 8, 21, tzinfo=UTC),
    )

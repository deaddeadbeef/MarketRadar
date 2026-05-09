from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
from sqlalchemy import create_engine, func, select

from catalyst_radar.cli import main
from catalyst_radar.connectors.base import ConnectorHealthStatus
from catalyst_radar.core.models import DataQualitySeverity, JobStatus
from catalyst_radar.storage.provider_repositories import ProviderRepository
from catalyst_radar.storage.repositories import MarketRepository
from catalyst_radar.storage.schema import (
    data_quality_incidents,
    job_runs,
    normalized_provider_records,
    raw_provider_records,
)


def test_ingest_csv_preserves_output_and_writes_provider_records(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = _database_url(tmp_path)
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    assert main(["init-db"]) == 0
    capsys.readouterr()

    assert (
        main(
            [
                "ingest-csv",
                "--securities",
                "tests/fixtures/securities.csv",
                "--daily-bars",
                "tests/fixtures/daily_bars.csv",
                "--holdings",
                "tests/fixtures/holdings.csv",
            ]
        )
        == 0
    )

    assert capsys.readouterr().out == "ingested securities=6 daily_bars=36 holdings=1\n"

    engine = create_engine(database_url, future=True)
    provider_repo = ProviderRepository(engine)
    market_repo = MarketRepository(engine)
    with engine.connect() as conn:
        raw_count = conn.execute(
            select(func.count()).select_from(raw_provider_records)
        ).scalar_one()
        normalized_count = conn.execute(
            select(func.count()).select_from(normalized_provider_records)
        ).scalar_one()
        job = conn.execute(select(job_runs).where(job_runs.c.job_type == "csv_ingest")).one()
        incident_count = conn.execute(
            select(func.count()).select_from(data_quality_incidents)
        ).scalar_one()

    health = provider_repo.latest_health("csv")
    assert health is not None
    assert health.status == ConnectorHealthStatus.HEALTHY
    assert raw_count == 43
    assert normalized_count == 43
    assert job.status == JobStatus.SUCCESS.value
    assert job.requested_count == 43
    assert job.raw_count == 43
    assert job.normalized_count == 43
    assert incident_count == 0
    assert len(market_repo.list_active_securities()) == 6
    assert len(market_repo.daily_bars("AAA", end=date(2026, 5, 8), lookback=100)) == 6
    assert len(market_repo.list_holdings()) == 1


def test_provider_health_command_prints_latest_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = _database_url(tmp_path)
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    assert (
        main(
            [
                "ingest-csv",
                "--securities",
                "tests/fixtures/securities.csv",
                "--daily-bars",
                "tests/fixtures/daily_bars.csv",
            ]
        )
        == 0
    )
    capsys.readouterr()

    assert main(["provider-health", "--provider", "csv"]) == 0

    assert capsys.readouterr().out == "provider=csv status=healthy\n"


def test_ingest_csv_missing_required_file_fails_closed_and_records_job(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = _database_url(tmp_path)
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    exit_code = main(
        [
            "ingest-csv",
            "--securities",
            "tests/fixtures/missing-securities.csv",
            "--daily-bars",
            "tests/fixtures/daily_bars.csv",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    assert "missing required csv path" in captured.err

    engine = create_engine(database_url, future=True)
    provider_repo = ProviderRepository(engine)
    with engine.connect() as conn:
        job = conn.execute(select(job_runs).where(job_runs.c.job_type == "csv_ingest")).one()
        incident = conn.execute(select(data_quality_incidents)).one()

    health = provider_repo.latest_health("csv")
    assert health is not None
    assert health.status == ConnectorHealthStatus.DOWN
    assert job.status == JobStatus.FAILED.value
    assert job.error_summary is not None
    assert "missing required csv path" in job.error_summary
    assert incident.severity == DataQualitySeverity.CRITICAL.value
    assert incident.fail_closed_action == "abort-ingest"


def test_ingest_csv_rejected_payload_records_incident_and_degraded_health(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_url = _database_url(tmp_path)
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    securities_csv = tmp_path / "securities.csv"
    securities_csv.write_text(
        "\n".join(
            [
                "ticker,name,exchange,sector,industry,market_cap,avg_dollar_volume_20d,"
                "has_options,is_active,updated_at",
                "BAD,Bad Timestamp,NASDAQ,Technology,Software,1,1,true,true,",
            ]
        ),
        encoding="utf-8",
    )

    assert (
        main(
            [
                "ingest-csv",
                "--securities",
                str(securities_csv),
                "--daily-bars",
                "tests/fixtures/daily_bars.csv",
            ]
        )
        == 0
    )

    engine = create_engine(database_url, future=True)
    provider_repo = ProviderRepository(engine)
    with engine.connect() as conn:
        job = conn.execute(select(job_runs).where(job_runs.c.job_type == "csv_ingest")).one()
        incident = conn.execute(select(data_quality_incidents)).one()

    health = provider_repo.latest_health("csv")
    assert health is not None
    assert health.status == ConnectorHealthStatus.DEGRADED
    assert job.status == JobStatus.PARTIAL_SUCCESS.value
    assert job.error_summary == "rejected payloads=1"
    assert incident.severity == DataQualitySeverity.ERROR.value
    assert incident.kind == "security"
    assert incident.affected_tickers == ["BAD"]
    assert "missing mandatory timestamp field" in incident.reason


def test_ingest_csv_missing_daily_bar_available_at_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = _database_url(tmp_path)
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
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

    exit_code = main(
        [
            "ingest-csv",
            "--securities",
            "tests/fixtures/securities.csv",
            "--daily-bars",
            str(daily_bars_csv),
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    assert "available_at" in captured.err

    engine = create_engine(database_url, future=True)
    provider_repo = ProviderRepository(engine)
    with engine.connect() as conn:
        job = conn.execute(select(job_runs).where(job_runs.c.job_type == "csv_ingest")).one()
        incident = conn.execute(select(data_quality_incidents)).one()
        normalized_count = conn.execute(
            select(func.count()).select_from(normalized_provider_records)
        ).scalar_one()

    health = provider_repo.latest_health("csv")
    assert health is not None
    assert health.status == ConnectorHealthStatus.DOWN
    assert job.status == JobStatus.FAILED.value
    assert job.normalized_count == 0
    assert normalized_count == 0
    assert incident.severity == DataQualitySeverity.CRITICAL.value
    assert incident.kind == "daily_bar"
    assert incident.affected_tickers == ["BAD"]
    assert incident.fail_closed_action == "abort-ingest"
    assert incident.available_at is None
    assert "available_at" in incident.reason


def _database_url(tmp_path: Path) -> str:
    return f"sqlite:///{(tmp_path / 'catalyst_radar.db').as_posix()}"

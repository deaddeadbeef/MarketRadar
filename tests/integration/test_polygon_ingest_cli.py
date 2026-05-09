from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pytest
from sqlalchemy import create_engine, func, select

from catalyst_radar.cli import main
from catalyst_radar.connectors.base import ConnectorHealthStatus
from catalyst_radar.core.models import JobStatus
from catalyst_radar.storage.provider_repositories import ProviderRepository
from catalyst_radar.storage.repositories import MarketRepository
from catalyst_radar.storage.schema import (
    data_quality_incidents,
    job_runs,
    normalized_provider_records,
    raw_provider_records,
)


@dataclass(frozen=True)
class CliResult:
    exit_code: int
    stdout: str
    stderr: str
    database_url: str


def test_polygon_ingest_requires_api_key(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    result = run_cli(
        ["ingest-polygon", "grouped-daily", "--date", "2026-05-08"],
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        capsys=capsys,
        env={"CATALYST_POLYGON_API_KEY": ""},
    )

    assert result.exit_code == 1
    assert result.stdout == ""
    assert "missing CATALYST_POLYGON_API_KEY" in result.stderr

    engine = create_engine(result.database_url, future=True)
    provider_repo = ProviderRepository(engine)
    with engine.connect() as conn:
        job = conn.execute(
            select(job_runs).where(job_runs.c.job_type == "polygon_grouped_daily")
        ).one()
        incident = conn.execute(select(data_quality_incidents)).one()

    health = provider_repo.latest_health("polygon")
    assert health is not None
    assert health.status == ConnectorHealthStatus.DOWN
    assert job.status == JobStatus.FAILED.value
    assert job.error_summary == "missing CATALYST_POLYGON_API_KEY"
    assert incident.fail_closed_action == "abort-ingest"


def test_polygon_fixture_ingest_persists_raw_normalized_and_daily_bars(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    result = run_cli(
        [
            "ingest-polygon",
            "grouped-daily",
            "--date",
            "2026-05-08",
            "--fixture",
            "tests/fixtures/polygon/grouped_daily_2026-05-08.json",
        ],
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        capsys=capsys,
        env={"CATALYST_POLYGON_API_KEY": "fixture-key"},
    )

    assert result.exit_code == 0
    assert (
        result.stdout
        == "ingested provider=polygon raw=6 normalized=6 securities=0 daily_bars=6 "
        "rejected=1\n"
    )
    assert result.stderr == ""

    engine = create_engine(result.database_url, future=True)
    provider_repo = ProviderRepository(engine)
    market_repo = MarketRepository(engine)
    with engine.connect() as conn:
        raw_count = conn.execute(
            select(func.count()).select_from(raw_provider_records)
        ).scalar_one()
        normalized_count = conn.execute(
            select(func.count()).select_from(normalized_provider_records)
        ).scalar_one()
        incident_count = conn.execute(
            select(func.count()).select_from(data_quality_incidents)
        ).scalar_one()
        job = conn.execute(
            select(job_runs).where(job_runs.c.job_type == "polygon_grouped_daily")
        ).one()

    health = provider_repo.latest_health("polygon")
    assert health is not None
    assert health.status == ConnectorHealthStatus.DEGRADED
    assert raw_count == 6
    assert normalized_count == 6
    assert incident_count == 1
    assert job.status == JobStatus.PARTIAL_SUCCESS.value
    assert job.requested_count == 7
    assert job.raw_count == 6
    assert job.normalized_count == 6
    assert len(market_repo.daily_bars("AAPL", end=date(2026, 5, 8), lookback=10)) == 1


def test_polygon_fixture_ingest_does_not_require_real_api_key(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    result = run_cli(
        [
            "ingest-polygon",
            "tickers",
            "--date",
            "2026-05-08",
            "--fixture",
            "tests/fixtures/polygon/tickers_page_1.json",
        ],
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        capsys=capsys,
        env={"CATALYST_POLYGON_API_KEY": ""},
    )

    assert result.exit_code == 0
    assert "securities=4" in result.stdout


def test_polygon_unadjusted_grouped_daily_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    fixture = tmp_path / "grouped_daily_unadjusted.json"
    fixture.write_text(
        '{"status":"OK","adjusted":false,"results":[{"T":"AAPL","t":1778198400000}]}',
        encoding="utf-8",
    )

    result = run_cli(
        [
            "ingest-polygon",
            "grouped-daily",
            "--date",
            "2026-05-08",
            "--fixture",
            str(fixture),
        ],
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        capsys=capsys,
        env={"CATALYST_POLYGON_API_KEY": ""},
    )

    assert result.exit_code == 1
    assert "not adjusted" in result.stderr

    engine = create_engine(result.database_url, future=True)
    with engine.connect() as conn:
        job = conn.execute(
            select(job_runs).where(job_runs.c.job_type == "polygon_grouped_daily")
        ).one()
        incident = conn.execute(select(data_quality_incidents)).one()

    assert job.status == JobStatus.FAILED.value
    assert incident.fail_closed_action == "abort-ingest"


def run_cli(
    argv: list[str],
    *,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    env: dict[str, str],
) -> CliResult:
    database_url = _database_url(tmp_path)
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("CATALYST_POLYGON_BASE_URL", "https://api.polygon.io")
    monkeypatch.setenv("CATALYST_HTTP_TIMEOUT_SECONDS", "3")
    monkeypatch.setenv("CATALYST_PROVIDER_AVAILABILITY_POLICY", "live_fetch")
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    exit_code = main(argv)
    captured = capsys.readouterr()
    return CliResult(
        exit_code=exit_code,
        stdout=captured.out,
        stderr=captured.err,
        database_url=database_url,
    )


def _database_url(tmp_path: Path) -> str:
    return f"sqlite:///{(tmp_path / 'catalyst_radar.db').as_posix()}"

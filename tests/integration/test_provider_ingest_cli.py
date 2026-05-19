from __future__ import annotations

import csv
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from sqlalchemy import create_engine, delete, func, select

from catalyst_radar.cli import main
from catalyst_radar.connectors.base import ConnectorHealthStatus
from catalyst_radar.core.models import DataQualitySeverity, JobStatus, Security
from catalyst_radar.market.manual_bars import MANUAL_BAR_COLUMNS
from catalyst_radar.storage.provider_repositories import ProviderRepository
from catalyst_radar.storage.repositories import MarketRepository
from catalyst_radar.storage.schema import (
    daily_bars,
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


def test_market_bars_template_uses_database_active_universe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = _database_url(tmp_path)
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _seed_csv_market(capsys)
    template_path = tmp_path / "manual-bars.csv"

    assert (
        main(
            [
                "market-bars",
                "template",
                "--expected-as-of",
                "2026-05-11",
                "--out",
                str(template_path),
            ]
        )
        == 0
    )

    captured = capsys.readouterr()
    assert "manual_market_bars_template status=ready" in captured.out
    assert "external_calls=0" in captured.out
    rows = _read_csv_rows(template_path)
    engine = create_engine(database_url, future=True)
    active_tickers = [
        security.ticker
        for security in MarketRepository(engine).list_active_securities()
    ]
    assert [row["ticker"] for row in rows] == active_tickers
    assert {row["date"] for row in rows} == {"2026-05-11"}
    assert {row["provider"] for row in rows} == {"manual_csv"}


def test_market_bars_import_requires_expected_full_active_coverage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = _database_url(tmp_path)
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _seed_csv_market(capsys)
    partial_bars = tmp_path / "partial-bars.csv"
    _write_manual_bars(partial_bars, ["AAA"], as_of="2026-05-11")

    exit_code = main(
        [
            "market-bars",
            "import",
            "--daily-bars",
            str(partial_bars),
            "--expected-as-of",
            "2026-05-11",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "manual_market_bars_import status=incomplete" in captured.out
    assert "coverage=bars_at_expected=1 existing=0 after_import=1 missing=5" in (
        captured.out
    )
    assert "missing_expected_tickers=" in captured.out
    assert "external_calls=0" in captured.out


def test_market_bars_missing_only_template_import_counts_existing_bars(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = _database_url(tmp_path)
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _seed_csv_market(capsys)
    engine = create_engine(database_url, future=True)
    missing_tickers = ["AAA", "XLK"]
    with engine.begin() as conn:
        conn.execute(
            delete(daily_bars).where(
                daily_bars.c.date == date(2026, 5, 8),
                daily_bars.c.ticker.in_(missing_tickers),
            )
        )
    template_path = tmp_path / "missing-bars.csv"

    assert (
        main(
            [
                "market-bars",
                "template",
                "--expected-as-of",
                "2026-05-08",
                "--out",
                str(template_path),
                "--missing-only",
            ]
        )
        == 0
    )

    template_output = capsys.readouterr()
    assert "manual_market_bars_template status=ready rows=2" in template_output.out
    assert "scope=missing_as_of_bars" in template_output.out
    assert "coverage=active=6 existing=4 missing=2 missing_only=true" in (
        template_output.out
    )
    assert "row_order=stock_like_then_unknown_then_non_stock" in template_output.out
    template_rows = _read_csv_rows(template_path)
    assert [row["ticker"] for row in template_rows] == missing_tickers
    assert {row["template_reason"] for row in template_rows} == {"missing_as_of_bar"}

    filled_path = tmp_path / "filled-missing-bars.csv"
    _write_manual_bars(filled_path, missing_tickers, as_of="2026-05-08")

    assert (
        main(
            [
                "market-bars",
                "import",
                "--daily-bars",
                str(filled_path),
                "--expected-as-of",
                "2026-05-08",
            ]
        )
        == 0
    )

    import_output = capsys.readouterr()
    assert "manual_market_bars_import status=ready" in import_output.out
    assert "coverage=bars_at_expected=2 existing=4 after_import=6 missing=0" in (
        import_output.out
    )
    assert "external_calls=0" in import_output.out


def test_market_bars_template_sorts_stock_like_rows_first(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = _database_url(tmp_path)
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    assert main(["init-db"]) == 0
    capsys.readouterr()
    engine = create_engine(database_url, future=True)
    MarketRepository(engine).upsert_securities(
        [
            _security("WUNT", "Unit", "UNIT"),
            _security("BSTK", "Common Stock", "CS"),
            _security("EETF", "ETF", "ETF"),
            _security("AADR", "ADR", "ADRC"),
            _security("ZUNK", "Unknown", ""),
        ]
    )
    template_path = tmp_path / "missing-bars.csv"

    assert (
        main(
            [
                "market-bars",
                "template",
                "--expected-as-of",
                "2026-05-15",
                "--out",
                str(template_path),
                "--missing-only",
            ]
        )
        == 0
    )

    captured = capsys.readouterr()
    assert "row_order=stock_like_then_unknown_then_non_stock" in captured.out
    rows = _read_csv_rows(template_path)
    assert [row["ticker"] for row in rows] == ["AADR", "BSTK", "ZUNK", "EETF", "WUNT"]
    assert [row["security_type"] for row in rows[:2]] == ["ADRC", "CS"]


def test_market_bars_import_rejects_blank_numeric_fields(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = _database_url(tmp_path)
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _seed_csv_market(capsys)
    invalid_bars = tmp_path / "invalid-bars.csv"
    _write_manual_bars(
        invalid_bars,
        ["AAA", "BBB", "CCC", "SPY", "XLI", "XLK"],
        as_of="2026-05-11",
        open_value="",
    )

    exit_code = main(
        [
            "market-bars",
            "import",
            "--daily-bars",
            str(invalid_bars),
            "--expected-as-of",
            "2026-05-11",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "manual market bars failed:" in captured.err
    assert "invalid open" in captured.err


def test_market_bars_import_executes_without_securities_csv(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = _database_url(tmp_path)
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _seed_csv_market(capsys)
    engine = create_engine(database_url, future=True)
    active_tickers = [
        security.ticker
        for security in MarketRepository(engine).list_active_securities()
    ]
    complete_bars = tmp_path / "complete-bars.csv"
    _write_manual_bars(complete_bars, active_tickers, as_of="2026-05-11")

    preview_code = main(
        [
            "market-bars",
            "import",
            "--daily-bars",
            str(complete_bars),
            "--expected-as-of",
            "2026-05-11",
        ]
    )
    preview = capsys.readouterr()
    assert preview_code == 0
    assert "manual_market_bars_import status=ready" in preview.out
    assert "Plan only: no database writes were made." in preview.out

    execute_code = main(
        [
            "market-bars",
            "import",
            "--daily-bars",
            str(complete_bars),
            "--expected-as-of",
            "2026-05-11",
            "--execute",
        ]
    )

    executed = capsys.readouterr()
    assert execute_code == 0
    assert "manual_market_bars_import status=imported" in executed.out
    assert "executed=true" in executed.out
    bars = MarketRepository(engine).daily_bars(
        "AAA",
        end=date(2026, 5, 11),
        lookback=1,
    )
    assert len(bars) == 1
    assert bars[0].date == date(2026, 5, 11)
    assert bars[0].provider == "manual_csv"


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


def _security(ticker: str, name: str, security_type: str) -> Security:
    return Security(
        ticker=ticker,
        name=name,
        exchange="NASDAQ",
        sector="Technology",
        industry="Software",
        market_cap=1_000_000_000.0,
        avg_dollar_volume_20d=10_000_000.0,
        has_options=True,
        is_active=True,
        updated_at=datetime(2026, 5, 15, 20, tzinfo=UTC),
        metadata={"type": security_type} if security_type else {},
    )


def _seed_csv_market(capsys: pytest.CaptureFixture[str]) -> None:
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
    capsys.readouterr()


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_manual_bars(
    path: Path,
    tickers: list[str],
    *,
    as_of: str,
    open_value: str = "100",
) -> None:
    stamp = datetime(2026, 5, 11, 21, tzinfo=UTC).isoformat()
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=MANUAL_BAR_COLUMNS)
        writer.writeheader()
        for index, ticker in enumerate(tickers):
            writer.writerow(
                {
                    "ticker": ticker,
                    "date": as_of,
                    "open": open_value,
                    "high": "101",
                    "low": "99",
                    "close": f"{100 + (index / 100):.2f}",
                    "volume": "1000000",
                    "vwap": "100",
                    "adjusted": "true",
                    "provider": "manual_csv",
                    "source_ts": stamp,
                    "available_at": stamp,
                }
            )

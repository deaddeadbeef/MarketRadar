from __future__ import annotations

import csv
import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from sqlalchemy import create_engine, delete, func, select

from catalyst_radar.cli import main
from catalyst_radar.connectors.base import ConnectorHealth, ConnectorHealthStatus
from catalyst_radar.core.models import DailyBar, DataQualitySeverity, JobStatus, Security
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


def test_market_bars_import_complete_rows_only_allows_incremental_import(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = _database_url(tmp_path)
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _seed_csv_market(capsys)
    incremental_bars = tmp_path / "incremental-bars.csv"
    _write_mixed_manual_bars(
        incremental_bars,
        complete_tickers=["AAA"],
        empty_tickers=["BBB"],
        as_of="2026-05-11",
    )

    assert (
        main(
            [
                "market-bars",
                "import",
                "--daily-bars",
                str(incremental_bars),
                "--expected-as-of",
                "2026-05-11",
                "--complete-rows-only",
            ]
        )
        == 0
    )

    preview = capsys.readouterr()
    assert "manual_market_bars_import status=ready_partial" in preview.out
    assert "complete_rows_only=true" in preview.out
    assert "coverage=bars_at_expected=1 existing=0 after_import=1 missing=5" in (
        preview.out
    )
    assert "fill_progress=complete=1 partial=0 empty=1 filled=1" in preview.out
    assert "post_import_verification status=preview_only missing=6" in preview.out
    assert "projected_missing=5 projection=would_still_block_market_bars" in preview.out
    assert "external_calls=0" in preview.out

    assert (
        main(
            [
                "market-bars",
                "import",
                "--daily-bars",
                str(incremental_bars),
                "--expected-as-of",
                "2026-05-11",
                "--complete-rows-only",
                "--execute",
            ]
        )
        == 0
    )

    executed = capsys.readouterr()
    assert "manual_market_bars_import status=partial_imported" in executed.out
    assert "executed=true" in executed.out
    assert "post_import_verification status=market_bars_still_blocked missing=5" in executed.out
    engine = create_engine(database_url, future=True)
    with engine.connect() as conn:
        imported = {
            str(row._mapping["ticker"])
            for row in conn.execute(
                select(daily_bars.c.ticker).where(daily_bars.c.date == date(2026, 5, 11))
            )
        }
    assert "AAA" in imported
    assert "BBB" not in imported


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


def test_market_bars_template_refuses_to_overwrite_filled_manual_rows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = _database_url(tmp_path)
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _seed_csv_market(capsys)
    template_path = tmp_path / "manual-bars.csv"
    _write_manual_bars(template_path, ["AAA"], as_of="2026-05-11")

    assert (
        main(
            [
                "market-bars",
                "template",
                "--expected-as-of",
                "2026-05-11",
                "--out",
                str(template_path),
                "--missing-only",
            ]
        )
        == 1
    )
    output = capsys.readouterr()
    assert output.out == ""
    assert "refusing to overwrite manual market-bar template" in output.err
    assert _read_csv_rows(template_path)[0]["open"] == "100"

    assert (
        main(
            [
                "market-bars",
                "template",
                "--expected-as-of",
                "2026-05-11",
                "--out",
                str(template_path),
                "--missing-only",
                "--overwrite",
            ]
        )
        == 0
    )

    overwrite_output = capsys.readouterr()
    assert "manual_market_bars_template status=ready" in overwrite_output.out
    assert all(not row["open"] for row in _read_csv_rows(template_path))


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
    assert "Rows include security names" in captured.out
    rows = _read_csv_rows(template_path)
    assert [row["ticker"] for row in rows] == ["AADR", "BSTK", "ZUNK", "EETF", "WUNT"]
    assert [row["security_type"] for row in rows[:2]] == ["ADRC", "CS"]
    assert [row["name"] for row in rows[:2]] == ["ADR", "Common Stock"]


def test_market_bars_stocks_only_template_and_import_scope(
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
            _security("BSTK", "Beta Stock", "CS"),
            _security("AADR", "Alpha ADR", "ADRC"),
            _security("EETF", "Example ETF", "ETF"),
            _security("WUNT", "Wrapper Unit", "UNIT"),
        ]
    )
    MarketRepository(engine).upsert_daily_bars(
        [
            _daily_bar("BSTK", date(2026, 5, 15)),
            _daily_bar("EETF", date(2026, 5, 15)),
        ]
    )
    template_path = tmp_path / "stock-bars.csv"

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
                "--stocks-only",
            ]
        )
        == 0
    )

    output = capsys.readouterr()
    assert "manual_market_bars_template status=ready rows=1" in output.out
    assert "scope=stock_like_missing_as_of_bars" in output.out
    assert "coverage=active=2 existing=1 missing=1" in output.out
    assert "stocks_only=true" in output.out
    template_rows = _read_csv_rows(template_path)
    assert [row["ticker"] for row in template_rows] == ["AADR"]
    assert template_rows[0]["name"] == "Alpha ADR"

    filled_path = tmp_path / "filled-stock-bars.csv"
    _write_manual_bars(filled_path, ["AADR"], as_of="2026-05-15")

    assert (
        main(
            [
                "market-bars",
                "import",
                "--daily-bars",
                str(filled_path),
                "--expected-as-of",
                "2026-05-15",
                "--stocks-only",
            ]
        )
        == 0
    )

    scoped_output = capsys.readouterr()
    assert "manual_market_bars_import status=ready" in scoped_output.out
    assert "coverage=bars_at_expected=1 existing=1 after_import=2 missing=0 scope=stock_like" in (
        scoped_output.out
    )
    assert "--stocks-only --execute" in scoped_output.out

    assert (
        main(
            [
                "market-bars",
                "import",
                "--daily-bars",
                str(filled_path),
                "--expected-as-of",
                "2026-05-15",
            ]
        )
        == 2
    )

    full_output = capsys.readouterr()
    assert "manual_market_bars_import status=incomplete" in full_output.out
    assert "scope=active_universe" in full_output.out
    assert "missing_expected_tickers=WUNT" in full_output.out


def test_market_bars_repair_plan_reports_manual_and_guarded_provider_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    database_url = _database_url(tmp_path)
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("CATALYST_POLYGON_API_KEY", "fixture-key")

    assert main(["init-db"]) == 0
    capsys.readouterr()
    engine = create_engine(database_url, future=True)
    MarketRepository(engine).upsert_securities(
        [
            _security("BSTK", "Beta Stock", "CS"),
            _security("AADR", "Alpha Acquisition ADR", "ADRC"),
            _security("EETF", "Example ETF", "ETF"),
        ]
    )
    MarketRepository(engine).upsert_daily_bars(
        [
            _daily_bar("BSTK", date(2026, 5, 15)),
            _daily_bar("EETF", date(2026, 5, 15)),
        ]
    )

    exit_code = main(
        [
            "market-bars",
            "repair-plan",
            "--expected-as-of",
            "2026-05-15",
            "--stocks-only",
            "--json",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    payload = json.loads(captured.out)
    assert payload["schema_version"] == "manual-market-bars-repair-plan-v1"
    assert payload["status"] == "attention"
    assert payload["coverage_scope"] == "stock_like"
    assert payload["active_security_count"] == 2
    assert payload["existing_as_of_bar_count"] == 1
    assert payload["missing_as_of_bar_count"] == 1
    assert payload["missing_as_of_bar_ticker_sample"] == ["AADR"]
    assert payload["missing_security_type_counts"] == {"ADRC": 1}
    assert payload["missing_with_local_history_count"] == 0
    assert payload["missing_with_local_history_sample"] == []
    assert payload["missing_without_local_history_count"] == 1
    assert payload["missing_without_local_history_sample"] == ["AADR"]
    assert payload["missing_universe_diagnostic"]["schema_version"] == (
        "manual-market-bars-missing-universe-diagnostic-v1"
    )
    assert payload["missing_universe_diagnostic"]["missing_count"] == 1
    assert payload["missing_universe_diagnostic"]["acquisition_or_spac_name_count"] == 1
    assert payload["missing_universe_diagnostic"]["no_composite_figi_count"] == 1
    assert payload["missing_universe_diagnostic"]["external_calls_made"] == 0
    assert payload["manual_template_command"].endswith(
        "--missing-only --stocks-only"
    )
    assert payload["manual_import_preview_command"].endswith("--stocks-only")
    assert payload["manual_import_execute_command"].endswith(
        "--stocks-only --execute"
    )
    assert payload["manual_incremental_import_preview_command"].endswith(
        "--stocks-only --complete-rows-only"
    )
    assert payload["manual_incremental_import_execute_command"].endswith(
        "--stocks-only --complete-rows-only --execute"
    )
    assert payload["operator_step"] == {
        "status": "needs_template",
        "kind": "generate_template",
        "action": "Generate the DB-backed missing-bar CSV for the full scope.",
        "command": payload["manual_template_command"],
        "after_manual_command": payload["manual_import_preview_command"],
        "manual_step": False,
        "external_calls_made": 0,
    }
    assert payload["required_fill_fields"] == [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "vwap",
    ]
    assert payload["blank_required_field_counts_if_new_template"] == {
        "open": 1,
        "high": 1,
        "low": 1,
        "close": 1,
        "volume": 1,
        "vwap": 1,
    }
    assert payload["template_row_count"] == 1
    assert payload["provider_fill_status"] == "ready_for_approval"
    assert payload["provider_fill_external_call_count"] == 1
    assert payload["provider_key_configured"] is True
    assert payload["provider_fill_command"] == (
        "catalyst-radar ingest-polygon grouped-daily "
        "--date 2026-05-15 --confirm-external-call"
    )
    assert payload["provider_saved_file_path"] == (
        "data\\local\\polygon-grouped-daily-2026-05-15.json"
    )
    assert payload["provider_saved_file_exists"] is False
    assert payload["provider_saved_file_status"] == "missing"
    assert "Capture or obtain" in payload["provider_saved_file_next_action"]
    assert payload["provider_saved_file_capture_command"] == (
        "catalyst-radar market-bars saved-capture "
        "--expected-as-of 2026-05-15 "
        "--out data\\local\\polygon-grouped-daily-2026-05-15.json "
        "--expect-active-count 2 "
        "--expect-existing-count 1 "
        "--expect-missing-count 1 "
        "--confirm-external-call --stocks-only"
    )
    assert payload["provider_saved_file_capture_api"] == (
        "POST /api/radar/market-bars/provider-fixture-capture"
    )
    assert payload["provider_saved_file_capture_request_body"] == {
        "expected_as_of": "2026-05-15",
        "output_path": "data\\local\\polygon-grouped-daily-2026-05-15.json",
        "confirm_external_call": False,
        "stocks_only": True,
        "expected_active_security_count": 2,
        "expected_existing_as_of_bar_count": 1,
        "expected_missing_as_of_bar_count": 1,
    }
    assert payload["provider_saved_file_capture_confirm_request_body"] == {
        **payload["provider_saved_file_capture_request_body"],
        "confirm_external_call": True,
    }
    assert payload["provider_saved_file_capture_external_call_count"] == 1
    approval_packet = payload["provider_saved_file_capture_approval_packet"]
    assert approval_packet["schema_version"] == (
        "market-bars-saved-capture-approval-packet-v1"
    )
    assert approval_packet["status"] == "approval_required"
    assert approval_packet["approval_required"] is True
    assert approval_packet["coverage_scope"] == "stock_like"
    assert approval_packet["expected_as_of"] == "2026-05-15"
    assert approval_packet["active_security_count"] == 2
    assert approval_packet["existing_as_of_bar_count"] == 1
    assert approval_packet["missing_as_of_bar_count"] == 1
    assert approval_packet["missing_as_of_bar_ticker_sample"] == ["AADR"]
    assert approval_packet["missing_as_of_bar_ticker_more"] == 0
    assert approval_packet["missing_security_type_counts"] == {"ADRC": 1}
    assert approval_packet["missing_universe_diagnostic"]["missing_count"] == 1
    assert approval_packet["missing_universe_diagnostic"]["external_calls_made"] == 0
    assert approval_packet["approval_guard"] == {
        "schema_version": "market-bars-saved-capture-approval-guard-v1",
        "expected_as_of": "2026-05-15",
        "stocks_only": True,
        "expected_active_security_count": 2,
        "expected_existing_as_of_bar_count": 1,
        "expected_missing_as_of_bar_count": 1,
        "external_calls_made": 0,
        "db_writes_made": 0,
    }
    assert approval_packet["external_calls_without_approval"] == 0
    assert approval_packet["external_calls_if_approved"] == 1
    assert approval_packet["db_writes_during_capture"] == 0
    assert approval_packet["tui_plan_command"] == "bars saved capture"
    assert approval_packet["tui_confirm_command"] == "bars saved capture confirm"
    assert "--stocks-only" in approval_packet["capture_cli_command"]
    assert approval_packet["capture_request_body"] == (
        payload["provider_saved_file_capture_request_body"]
    )
    assert approval_packet["capture_confirm_request_body"] == (
        payload["provider_saved_file_capture_confirm_request_body"]
    )
    assert [step["step"] for step in approval_packet["post_capture_zero_call_steps"]] == [
        "validate_saved_file",
        "preview_import",
        "execute_import_after_preview",
    ]
    post_steps = approval_packet["post_capture_zero_call_steps"]
    assert post_steps[0]["external_calls_made"] == 0
    assert post_steps[1]["cli_command"] == payload["provider_saved_file_import_command"]
    assert post_steps[2]["cli_command"] == (
        f"{payload['provider_saved_file_import_command']} --execute"
    )
    assert "0 provider calls" in " ".join(approval_packet["guardrails"])
    assert payload["provider_saved_file_import_command"] == (
        "catalyst-radar market-bars saved-import "
        "--expected-as-of 2026-05-15 "
        "--fixture data\\local\\polygon-grouped-daily-2026-05-15.json "
        "--stocks-only"
    )
    assert payload["provider_saved_file_validate_command"] == (
        "catalyst-radar market-bars saved-validate "
        "--expected-as-of 2026-05-15 "
        "--fixture data\\local\\polygon-grouped-daily-2026-05-15.json "
        "--stocks-only"
    )
    assert payload["provider_saved_file_validate_api"] == (
        "POST /api/radar/market-bars/provider-fixture-preview"
    )
    assert payload["provider_saved_file_validate_request_body"] == {
        "expected_as_of": "2026-05-15",
        "fixture_path": "data\\local\\polygon-grouped-daily-2026-05-15.json",
        "stocks_only": True,
    }
    assert payload["provider_saved_file_import_api"] == (
        "POST /api/radar/market-bars/provider-fixture-import"
    )
    assert payload["provider_saved_file_import_preview_request_body"] == {
        **payload["provider_saved_file_validate_request_body"],
        "execute": False,
    }
    assert payload["provider_saved_file_import_request_body"] == {
        **payload["provider_saved_file_validate_request_body"],
        "execute": True,
    }
    assert payload["provider_saved_file_external_call_count"] == 0
    assert "0 provider calls" in payload["provider_saved_file_boundary"]
    assert payload["external_calls_made"] == 0


def test_market_bars_status_cli_summarizes_zero_call_unblock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
):
    monkeypatch.chdir(tmp_path)
    database_url = _database_url(tmp_path)
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("CATALYST_POLYGON_API_KEY", "fixture-key")

    assert main(["init-db"]) == 0
    capsys.readouterr()
    engine = create_engine(database_url, future=True)
    MarketRepository(engine).upsert_securities(
        [
            _security("BSTK", "Beta Stock", "CS"),
            _security("AADR", "Alpha Acquisition ADR", "ADRC"),
        ]
    )
    MarketRepository(engine).upsert_daily_bars(
        [_daily_bar("BSTK", date(2026, 5, 15))]
    )

    exit_code = main(
        [
            "market-bars",
            "status",
            "--expected-as-of",
            "2026-05-15",
            "--stocks-only",
            "--json",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    payload = json.loads(captured.out)
    assert payload["schema_version"] == "market-bars-status-v1"
    assert payload["status"] == "blocked"
    assert payload["first_blocker"] == "market_bars"
    assert payload["expected_as_of"] == "2026-05-15"
    assert payload["expected_as_of_source"] == "argument"
    assert payload["coverage_scope"] == "stock_like"
    assert payload["active_security_count"] == 2
    assert payload["existing_as_of_bar_count"] == 1
    assert payload["missing_as_of_bar_count"] == 1
    assert payload["missing_as_of_bar_ticker_sample"] == ["AADR"]
    assert payload["missing_as_of_bar_ticker_more"] == 0
    assert payload["missing_security_type_counts"] == {"ADRC": 1}
    assert payload["missing_universe_diagnostic"]["missing_count"] == 1
    assert payload["missing_universe_diagnostic"]["external_calls_made"] == 0
    assert payload["manual"]["command"].startswith(
        "catalyst-radar market-bars template"
    )
    assert payload["saved_capture"]["status"] == "approval_required"
    assert payload["saved_capture"]["external_calls_if_approved"] == 1
    assert payload["saved_file"]["status"] == "missing"
    assert payload["recommended_action"]["kind"] == "saved_provider_capture"
    assert payload["recommended_action"]["approval_required"] is True
    assert payload["recommended_action"]["external_calls_required"] == 1
    assert payload["recommended_action"]["db_writes_required"] == 0
    assert (
        payload["recommended_action"]["tui_command"]
        == "bars saved capture confirm"
    )
    checklist = payload["unblock_checklist"]
    assert checklist["schema_version"] == "market-bars-unblock-checklist-v1"
    assert checklist["status"] == "approval_required"
    assert checklist["next_step_order"] == 2
    assert checklist["coverage_scope"] == "stock_like"
    assert checklist["missing_as_of_bar_count"] == 1
    assert checklist["external_calls_made"] == 0
    assert checklist["db_changes_made"] == 0
    assert checklist["steps"][0]["command"].endswith("--stocks-only")
    capture_step = checklist["steps"][1]
    assert capture_step["label"] == "Capture saved provider file"
    assert capture_step["external_calls_required"] == 1
    assert capture_step["db_changes_required"] == 0
    assert capture_step["tui_command"] == "bars saved capture confirm"
    after_clear = payload["after_market_bars_clear"]
    assert after_clear["schema_version"] == "market-bars-after-clear-v1"
    assert after_clear["current_blocker"] == "market_bars"
    assert after_clear["current_gap_count"] == 1
    assert after_clear["external_calls_made"] == 0
    assert payload["repair_plan"]["schema_version"] == (
        "manual-market-bars-repair-plan-v1"
    )
    assert payload["external_calls_made"] == 0
    assert payload["db_writes_made"] == 0

    exit_code = main(
        [
            "market-bars",
            "status",
            "--expected-as-of",
            "2026-05-15",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    assert exit_code == 0
    all_payload = json.loads(captured.out)
    assert all_payload["stocks_only"] is False
    stock_scope = all_payload["stock_scope"]
    assert stock_scope["schema_version"] == "market-bars-stock-scope-v1"
    assert stock_scope["status"] == "blocked"
    assert stock_scope["stock_like_active"] == 2
    assert stock_scope["stock_like_with_as_of_bar"] == 1
    assert stock_scope["stock_like_missing_as_of_bar"] == 1
    assert stock_scope["stock_like_coverage_pct"] == 50.0
    assert stock_scope["sample_missing_stock_like_tickers"] == ["AADR"]
    assert stock_scope["non_stock_missing_as_of_bar"] == 0
    assert stock_scope["manual_template_command"].endswith("--stocks-only")
    assert stock_scope["external_calls_made"] == 0
    assert stock_scope["db_writes_made"] == 0

    exit_code = main(["market-bars", "status", "--stocks-only", "--json"])
    captured = capsys.readouterr()
    assert exit_code == 0
    default_payload = json.loads(captured.out)
    assert default_payload["expected_as_of"] == "2026-05-15"
    assert default_payload["expected_as_of_source"] == "latest_daily_bar"
    assert default_payload["missing_as_of_bar_ticker_sample"] == ["AADR"]
    assert default_payload["external_calls_made"] == 0
    assert default_payload["db_writes_made"] == 0

    assert (
        main(
            [
                "market-bars",
                "status",
                "--stocks-only",
            ]
        )
        == 0
    )
    text = capsys.readouterr().out
    assert "market_bars_status status=blocked" in text
    assert "expected_as_of=2026-05-15" in text
    assert "expected_as_of_source=latest_daily_bar" in text
    assert "missing=1" in text
    assert "missing_as_of_tickers=AADR" in text
    assert "missing_security_types=ADRC:1" in text
    assert "missing_universe=active_metadata=1" in text
    assert "saved_capture status=approval_required" in text
    assert "calls_if_approved=1" in text
    assert "recommended_action kind=saved_provider_capture" in text
    assert "tui=bars saved capture confirm" in text
    assert "unblock_checklist status=approval_required" in text
    assert "next_step=2" in text
    assert "action=Capture saved provider file" in text
    assert "external_calls=1" in text
    assert "db_writes=0" in text
    assert "command=bars saved capture confirm" in text
    assert "after_market_bars_clear status=" in text
    assert "current=market_bars" in text
    assert "external_calls=0" in text
    assert "0 provider calls and 0 database writes" in text

    assert (
        main(
            [
                "market-bars",
                "status",
                "--expected-as-of",
                "2026-05-15",
            ]
        )
        == 0
    )
    text = capsys.readouterr().out
    assert "stock_scope status=blocked coverage=1/2 missing=1" in text
    assert "non_stock_missing=0" in text
    assert "stock_scope_missing_tickers=AADR" in text
    assert "manual-stock-bars-2026-05-15.csv" in text


def test_market_bars_status_cli_keeps_seeded_universe_on_market_bar_blocker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
):
    monkeypatch.chdir(tmp_path)
    database_url = _database_url(tmp_path)
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("CATALYST_POLYGON_API_KEY", "fixture-key")

    assert main(["init-db"]) == 0
    capsys.readouterr()
    engine = create_engine(database_url, future=True)
    MarketRepository(engine).upsert_securities(
        [
            _security("AAPL", "Apple Inc.", "CS"),
            _security("MSFT", "Microsoft Corporation", "CS"),
        ]
    )

    exit_code = main(["market-bars", "status", "--json"])

    captured = capsys.readouterr()
    assert exit_code == 0
    payload = json.loads(captured.out)
    assert payload["schema_version"] == "market-bars-status-v1"
    assert payload["status"] == "blocked"
    assert payload["first_blocker"] == "market_bars"
    assert payload["expected_as_of"] is None
    assert payload["expected_as_of_source"] == "not_available"
    assert payload["active_security_count"] == 2
    assert payload["existing_as_of_bar_count"] == 0
    assert payload["missing_as_of_bar_count"] == 2
    assert payload["missing_universe_diagnostic"] == {}
    assert payload["recommended_action"]["kind"] == "provide_expected_as_of"
    assert payload["recommended_action"]["external_calls_required"] == 0
    assert payload["recommended_action"]["db_writes_required"] == 0
    assert payload["unblock_checklist"]["steps"][0]["step"] == "set_expected_as_of"
    assert payload["external_calls_made"] == 0
    assert payload["db_writes_made"] == 0


def test_market_bars_status_cli_exposes_configured_universe_without_clearing_all_active(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
):
    monkeypatch.chdir(tmp_path)
    database_url = _database_url(tmp_path)
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("CATALYST_UNIVERSE_NAME", "liquid-us")

    assert main(["init-db"]) == 0
    capsys.readouterr()
    engine = create_engine(database_url, future=True)
    MarketRepository(engine).upsert_securities(
        [
            _security("AAPL", "Apple Inc.", "CS"),
            _security("MSFT", "Microsoft Corporation", "CS"),
            _security("AACBU", "Acme Acquisition Unit", "UNIT"),
        ]
    )
    MarketRepository(engine).upsert_daily_bars(
        [
            _daily_bar("AAPL", date(2026, 5, 15)),
            _daily_bar("MSFT", date(2026, 5, 15)),
        ]
    )
    ProviderRepository(engine).save_universe_snapshot(
        name="liquid-us",
        as_of=datetime(2026, 5, 15, 21, tzinfo=UTC),
        provider="polygon",
        source_ts=datetime(2026, 5, 15, 21, tzinfo=UTC),
        available_at=datetime(2026, 5, 15, 22, tzinfo=UTC),
        members=[
            {"ticker": "AAPL", "reason": "eligible", "rank": 1, "metadata": {}},
            {"ticker": "MSFT", "reason": "eligible", "rank": 2, "metadata": {}},
        ],
        metadata={"eligible_count": 2, "excluded_count": 1},
    )

    exit_code = main(
        [
            "market-bars",
            "status",
            "--expected-as-of",
            "2026-05-15",
            "--json",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    payload = json.loads(captured.out)
    assert payload["status"] == "blocked"
    assert payload["first_blocker"] == "market_bars"
    assert payload["coverage_scope"] == "active_universe"
    assert payload["active_security_count"] == 3
    assert payload["existing_as_of_bar_count"] == 2
    assert payload["missing_as_of_bar_count"] == 1
    configured = payload["configured_universe_scope"]
    assert configured["schema_version"] == "market-bars-configured-universe-scope-v1"
    assert configured["status"] == "ready"
    assert configured["universe"] == "liquid-us"
    assert configured["member_count"] == 2
    assert configured["with_as_of_bar_count"] == 2
    assert configured["missing_as_of_bar_count"] == 0
    assert configured["active_universe_missing_as_of_bar_count"] == 1
    assert configured["external_calls_made"] == 0
    assert configured["db_writes_made"] == 0
    assert "does not clear the all-active market-bar gate" in configured[
        "answer_boundary"
    ]
    assert payload["external_calls_made"] == 0
    assert payload["db_writes_made"] == 0

    assert (
        main(
            [
                "market-bars",
                "status",
                "--expected-as-of",
                "2026-05-15",
            ]
        )
        == 0
    )
    text = capsys.readouterr().out
    assert "market_bars_status status=blocked" in text
    assert "configured_universe status=ready name=liquid-us coverage=2/2 missing=0" in text
    assert "all_active_missing=1" in text
    assert "does not clear the all-active market-bar gate" in text


def test_market_bars_repair_plan_prefers_available_saved_provider_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
):
    monkeypatch.chdir(tmp_path)
    database_url = _database_url(tmp_path)
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("CATALYST_POLYGON_API_KEY", "fixture-key")

    assert main(["init-db"]) == 0
    capsys.readouterr()
    engine = create_engine(database_url, future=True)
    MarketRepository(engine).upsert_securities(
        [
            _security("BSTK", "Beta Stock", "CS"),
            _security("AADR", "Alpha ADR", "ADRC"),
        ]
    )
    MarketRepository(engine).upsert_daily_bars(
        [_daily_bar("BSTK", date(2026, 5, 15))]
    )
    saved_file = tmp_path / "data" / "local" / "polygon-grouped-daily-2026-05-15.json"
    saved_file.parent.mkdir(parents=True)
    saved_file.write_text('{"results": []}', encoding="utf-8")

    assert (
        main(
            [
                "market-bars",
                "repair-plan",
                "--expected-as-of",
                "2026-05-15",
                "--stocks-only",
                "--json",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)

    assert payload["provider_saved_file_exists"] is True
    assert payload["provider_saved_file_status"] == "available"
    approval_packet = payload["provider_saved_file_capture_approval_packet"]
    assert approval_packet["status"] == "saved_file_available"
    assert approval_packet["approval_required"] is False
    assert approval_packet["external_calls_if_approved"] == 0
    assert approval_packet["tui_confirm_command"] is None
    assert approval_packet["post_capture_zero_call_steps"][0]["tui_command"] == (
        "bars saved validate"
    )
    assert payload["operator_step"] == {
        "status": "saved_file_available",
        "kind": "validate_saved_provider_response",
        "action": (
            "Validate the saved Polygon/Massive grouped-daily JSON response; "
            "if it passes, import it to clear scan-date market-bar gaps."
        ),
        "command": payload["provider_saved_file_validate_command"],
        "after_manual_command": payload["provider_saved_file_import_command"],
        "manual_step": False,
        "external_calls_made": 0,
    }


def test_market_bars_saved_capture_cli_plans_without_provider_call(
    tmp_path,
    monkeypatch,
    capsys,
):
    monkeypatch.chdir(tmp_path)
    database_url = _database_url(tmp_path)
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("CATALYST_POLYGON_API_KEY", "fixture-key")

    assert main(["init-db"]) == 0
    capsys.readouterr()
    engine = create_engine(database_url, future=True)
    MarketRepository(engine).upsert_securities(
        [
            _security("BSTK", "Beta Stock", "CS"),
            _security("AADR", "Alpha Acquisition ADR", "ADRC"),
            _security("EETF", "Example ETF", "ETF"),
        ]
    )
    MarketRepository(engine).upsert_daily_bars(
        [
            _daily_bar("BSTK", date(2026, 5, 15)),
            _daily_bar("EETF", date(2026, 5, 15)),
        ]
    )

    exit_code = main(
        [
            "market-bars",
            "saved-capture",
            "--expected-as-of",
            "2026-05-15",
            "--stocks-only",
            "--json",
        ]
    )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == "market-bars-saved-capture-cli-plan-v1"
    assert payload["status"] == "approval_required"
    assert payload["approval_required"] is True
    assert payload["provider_key_configured"] is True
    assert payload["external_calls_without_approval"] == 0
    assert payload["external_calls_if_approved"] == 1
    assert payload["external_calls_made"] == 0
    assert payload["db_writes_made"] == 0
    assert payload["coverage_scope"] == "stock_like"
    assert payload["active_security_count"] == 2
    assert payload["existing_as_of_bar_count"] == 1
    assert payload["missing_as_of_bar_count"] == 1
    assert payload["missing_as_of_bar_ticker_sample"] == ["AADR"]
    assert payload["missing_as_of_bar_ticker_more"] == 0
    assert payload["missing_security_type_counts"] == {"ADRC": 1}
    assert payload["missing_universe_diagnostic"]["missing_count"] == 1
    assert payload["saved_file_path"] == (
        "data\\local\\polygon-grouped-daily-2026-05-15.json"
    )
    assert payload["saved_file_status"] == "missing"
    assert payload["capture_request_body"]["confirm_external_call"] is False
    assert payload["capture_request_body"]["stocks_only"] is True
    assert payload["capture_request_body"]["expected_active_security_count"] == 2
    assert payload["capture_request_body"]["expected_existing_as_of_bar_count"] == 1
    assert payload["capture_request_body"]["expected_missing_as_of_bar_count"] == 1
    assert payload["capture_confirm_request_body"]["confirm_external_call"] is True
    assert payload["capture_confirm_request_body"]["stocks_only"] is True
    assert payload["approval_guard"]["stocks_only"] is True
    assert payload["approval_guard"]["expected_missing_as_of_bar_count"] == 1
    assert "--expect-active-count 2" in payload["confirm_command"]
    assert "--expect-existing-count 1" in payload["confirm_command"]
    assert "--expect-missing-count 1" in payload["confirm_command"]
    assert "--stocks-only" in payload["confirm_command"]
    assert payload["validate_command"].endswith("--stocks-only")
    assert payload["import_preview_command"].endswith("--stocks-only")
    assert payload["import_execute_command"].endswith("--stocks-only --execute")
    assert payload["capture_api"] == (
        "POST /api/radar/market-bars/provider-fixture-capture"
    )
    assert not Path(payload["saved_file_path"]).exists()

    assert (
        main(
            [
                "market-bars",
                "saved-capture",
                "--expected-as-of",
                "2026-05-15",
                "--stocks-only",
            ]
        )
        == 0
    )
    text = capsys.readouterr().out
    assert "market_bars_saved_capture_plan status=approval_required" in text
    assert "missing_as_of_tickers=AADR" in text
    assert "missing_security_types=ADRC:1" in text
    assert "missing_universe=active_metadata=1" in text
    assert "external_calls_if_approved=1" in text
    assert "--confirm-external-call --stocks-only" in text
    assert "db_writes=0" in text

    blocked_code = main(
        [
            "market-bars",
            "saved-capture",
            "--expected-as-of",
            "2026-05-15",
            "--stocks-only",
            "--out",
            "data\\local\\polygon-grouped-daily-2026-05-15.json",
            "--expect-active-count",
            "2",
            "--expect-existing-count",
            "1",
            "--expect-missing-count",
            "99",
            "--confirm-external-call",
        ]
    )
    blocked_text = capsys.readouterr().out
    assert blocked_code == 2
    assert "market_bars_saved_capture_guard status=stale_approval" in blocked_text
    assert "missing_as_of_bar_count:expected=99 current=1" in blocked_text
    assert "external_calls=0" in blocked_text
    assert not Path("data\\local\\polygon-grouped-daily-2026-05-15.json").exists()


def test_market_bars_saved_file_cli_validates_and_imports_fixture(
    tmp_path,
    monkeypatch,
    capsys,
):
    monkeypatch.chdir(tmp_path)
    database_url = _database_url(tmp_path)
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("CATALYST_POLYGON_API_KEY", "fixture-key")
    fixture_path = (
        Path(__file__).resolve().parents[2]
        / "tests"
        / "fixtures"
        / "polygon"
        / "grouped_daily_2026-05-08.json"
    )
    saved_path = tmp_path / "polygon-grouped-daily-2026-05-08.json"

    assert main(["init-db"]) == 0
    capsys.readouterr()
    engine = create_engine(database_url, future=True)
    MarketRepository(engine).upsert_securities(
        [
            _security("AAPL", "Apple", "CS"),
            _security("MSFT", "Microsoft", "CS"),
            _security("GOOG", "Alphabet", "CS"),
        ]
    )

    capture_code = main(
        [
            "market-bars",
            "saved-capture",
            "--expected-as-of",
            "2026-05-08",
            "--fixture",
            str(fixture_path),
            "--out",
            str(saved_path),
            "--json",
        ]
    )
    assert capture_code == 0
    capture_payload = json.loads(capsys.readouterr().out)
    assert capture_payload["schema_version"] == (
        "polygon-grouped-daily-response-capture-v1"
    )
    assert capture_payload["source"] == "fixture"
    assert capture_payload["external_calls_made"] == 0
    assert capture_payload["db_writes_made"] == 0
    capture_verification = capture_payload["post_capture_verification"]
    assert capture_verification["status"] == "preview_only"
    assert capture_verification["source"] == "saved_provider_capture"
    assert capture_verification["missing_as_of_bar_count"] == 3
    assert capture_verification["projected_missing_after_import_count"] == 1
    assert capture_verification["preview_projection_status"] == "would_still_block_market_bars"
    assert capture_verification["external_calls_made"] == 0
    assert capture_verification["db_changes_made"] == 0
    assert capture_payload["validate_command"].startswith(
        "catalyst-radar market-bars saved-validate "
    )
    assert capture_payload["import_command"].startswith(
        "catalyst-radar market-bars saved-import "
    )
    assert saved_path.read_bytes() == fixture_path.read_bytes()

    human_capture_code = main(
        [
            "market-bars",
            "saved-capture",
            "--expected-as-of",
            "2026-05-08",
            "--fixture",
            str(fixture_path),
            "--out",
            str(saved_path),
        ]
    )
    assert human_capture_code == 0
    human_capture = capsys.readouterr().out
    assert "post_capture_verification status=preview_only" in human_capture
    assert "projected_missing=1" in human_capture
    assert "projection=would_still_block_market_bars" in human_capture
    assert "post_capture_next=" in human_capture

    validate_code = main(
        [
            "market-bars",
            "saved-validate",
            "--expected-as-of",
            "2026-05-08",
            "--fixture",
            str(saved_path),
            "--json",
        ]
    )
    assert validate_code == 0
    validate_payload = json.loads(capsys.readouterr().out)
    assert validate_payload["schema_version"] == (
        "polygon-grouped-daily-fixture-preview-v1"
    )
    assert validate_payload["status"] == "ready_with_rejections"
    assert validate_payload["external_calls_made"] == 0
    assert validate_payload["db_writes_made"] == 0

    preview_code = main(
        [
            "market-bars",
            "saved-import",
            "--expected-as-of",
            "2026-05-08",
            "--fixture",
            str(saved_path),
            "--json",
        ]
    )
    assert preview_code == 0
    preview_payload = json.loads(capsys.readouterr().out)
    assert preview_payload["schema_version"] == (
        "polygon-grouped-daily-fixture-import-v1"
    )
    assert preview_payload["executed"] is False
    verification = preview_payload["post_import_verification"]
    assert verification["status"] == "preview_only"
    assert verification["missing_as_of_bar_count"] == 3
    assert verification["projected_missing_after_import_count"] == 1
    assert verification["preview_projection_status"] == "would_still_block_market_bars"
    assert preview_payload["external_calls_made"] == 0
    assert preview_payload["db_writes_made"] == 0

    execute_code = main(
        [
            "market-bars",
            "saved-import",
            "--expected-as-of",
            "2026-05-08",
            "--fixture",
            str(saved_path),
            "--execute",
            "--json",
        ]
    )
    assert execute_code == 0
    execute_payload = json.loads(capsys.readouterr().out)
    assert execute_payload["schema_version"] == (
        "polygon-grouped-daily-fixture-import-v1"
    )
    assert execute_payload["status"] == "imported_with_rejections"
    assert execute_payload["executed"] is True
    assert execute_payload["external_calls_made"] == 0
    assert execute_payload["db_writes_made"] == 1
    assert execute_payload["daily_bar_count"] == 6
    assert execute_payload["rejected_count"] == 1
    assert execute_payload["post_import_verification"]["status"] == "market_bars_still_blocked"
    assert execute_payload["post_import_verification"]["missing_as_of_bar_count"] == 1
    assert execute_payload["post_import_verification"]["external_calls_made"] == 0
    assert execute_payload["post_import_verification"]["db_changes_made"] == 1

    with engine.connect() as conn:
        assert conn.execute(select(func.count()).select_from(job_runs)).scalar_one() == 1
        assert conn.execute(select(func.count()).select_from(daily_bars)).scalar_one() == 6


def test_market_bars_status_stops_looping_on_insufficient_saved_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    database_url = _database_url(tmp_path)
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("CATALYST_POLYGON_API_KEY", "fixture-key")
    fixture_path = (
        Path(__file__).resolve().parents[2]
        / "tests"
        / "fixtures"
        / "polygon"
        / "grouped_daily_2026-05-08.json"
    )
    saved_path = tmp_path / "data" / "local" / "polygon-grouped-daily-2026-05-08.json"
    saved_path.parent.mkdir(parents=True)
    saved_path.write_bytes(fixture_path.read_bytes())

    assert main(["init-db"]) == 0
    capsys.readouterr()
    engine = create_engine(database_url, future=True)
    MarketRepository(engine).upsert_securities(
        [
            _security("AAPL", "Apple Inc.", "CS"),
            _security("MSFT", "Microsoft Corp.", "CS"),
            _security("GLW", "Gap Leftover Works", "CS"),
        ]
    )

    execute_code = main(
        [
            "market-bars",
            "saved-import",
            "--expected-as-of",
            "2026-05-08",
            "--fixture",
            str(saved_path),
            "--execute",
            "--json",
        ]
    )
    assert execute_code == 0
    execute_payload = json.loads(capsys.readouterr().out)
    assert execute_payload["post_import_verification"]["missing_as_of_bar_count"] == 1
    assert execute_payload["external_calls_made"] == 0

    status_code = main(
        [
            "market-bars",
            "status",
            "--expected-as-of",
            "2026-05-08",
            "--json",
        ]
    )

    assert status_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "blocked"
    assert payload["missing_as_of_bar_count"] == 1
    assert payload["saved_file"]["status"] == "available"
    projection = payload["saved_file"]["projection"]
    assert projection["missing_covered_by_fixture_count"] == 0
    assert projection["missing_after_import_count"] == 1
    assert payload["recommended_action"]["kind"] == "manual_csv"
    assert payload["recommended_action"]["external_calls_required"] == 0
    assert "covers no remaining missing active tickers" in payload["next_action"]
    assert "saved grouped-daily file covers no remaining" in str(
        payload["recommended_action"]["reason"]
    )
    assert payload["external_calls_made"] == 0
    assert payload["db_writes_made"] == 0


def test_market_bars_residual_review_cli_flags_zero_liquidity_saved_gap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    database_url = _database_url(tmp_path)
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("CATALYST_POLYGON_API_KEY", "fixture-key")
    fixture_path = (
        Path(__file__).resolve().parents[2]
        / "tests"
        / "fixtures"
        / "polygon"
        / "grouped_daily_2026-05-08.json"
    )
    saved_path = tmp_path / "data" / "local" / "polygon-grouped-daily-2026-05-08.json"
    saved_path.parent.mkdir(parents=True)
    saved_path.write_bytes(fixture_path.read_bytes())

    assert main(["init-db"]) == 0
    capsys.readouterr()
    engine = create_engine(database_url, future=True)
    MarketRepository(engine).upsert_securities(
        [
            _security("AAPL", "Apple Inc.", "CS"),
            _security("MSFT", "Microsoft Corp.", "CS"),
            Security(
                ticker="AACO",
                name="Alpha Acquisition Corp.",
                exchange="NASDAQ",
                sector="Unknown",
                industry="Unknown",
                market_cap=0,
                avg_dollar_volume_20d=0,
                has_options=False,
                is_active=True,
                updated_at=datetime(2026, 5, 8, 20, tzinfo=UTC),
                metadata={"type": "CS"},
            ),
        ]
    )
    MarketRepository(engine).upsert_daily_bars(
        [
            _daily_bar("AAPL", date(2026, 5, 8)),
            _daily_bar("MSFT", date(2026, 5, 8)),
        ]
    )

    status_code = main(
        [
            "market-bars",
            "status",
            "--expected-as-of",
            "2026-05-08",
            "--json",
        ]
    )

    assert status_code == 0
    status_payload = json.loads(capsys.readouterr().out)
    assert status_payload["recommended_action"]["kind"] == (
        "residual_universe_review"
    )
    assert status_payload["recommended_action"]["external_calls_required"] == 0
    assert "residual-review" in status_payload["recommended_action"]["command"]
    assert status_payload["external_calls_made"] == 0
    assert status_payload["db_writes_made"] == 0

    review_code = main(
        [
            "market-bars",
            "residual-review",
            "--expected-as-of",
            "2026-05-08",
            "--json",
        ]
    )

    assert review_code == 0
    review_payload = json.loads(capsys.readouterr().out)
    assert review_payload["schema_version"] == "market-bars-residual-review-v1"
    assert review_payload["status"] == "universe_review_required"
    assert review_payload["clears_market_bar_gate"] is False
    assert review_payload["stock_like_missing_as_of_bar_count"] == 1
    assert review_payload["non_stock_missing_as_of_bar_count"] == 0
    assert review_payload["saved_file_projection"][
        "missing_covered_by_fixture_count"
    ] == 0
    assert review_payload["residual_evidence"]["zero_market_cap_count"] == 1
    assert review_payload["residual_evidence"][
        "zero_avg_dollar_volume_20d_count"
    ] == 1
    assert review_payload["residual_evidence"][
        "missing_without_local_history_count"
    ] == 1
    assert {option["kind"] for option in review_payload["decision_options"]} >= {
        "manual_bar_repair",
        "active_universe_repair",
        "keep_blocked",
    }
    assert review_payload["external_calls_made"] == 0
    assert review_payload["db_writes_made"] == 0


def test_market_bars_saved_file_cli_import_respects_stock_scope(
    tmp_path,
    monkeypatch,
    capsys,
):
    monkeypatch.chdir(tmp_path)
    database_url = _database_url(tmp_path)
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("CATALYST_POLYGON_API_KEY", "fixture-key")
    fixture_path = (
        Path(__file__).resolve().parents[2]
        / "tests"
        / "fixtures"
        / "polygon"
        / "grouped_daily_2026-05-08.json"
    )

    assert main(["init-db"]) == 0
    capsys.readouterr()
    engine = create_engine(database_url, future=True)
    MarketRepository(engine).upsert_securities(
        [
            _security("AAPL", "Apple", "CS"),
            _security("MSFT", "Microsoft", "CS"),
            _security("EETF", "Example ETF", "ETF"),
        ]
    )

    preview_code = main(
        [
            "market-bars",
            "saved-import",
            "--expected-as-of",
            "2026-05-08",
            "--fixture",
            str(fixture_path),
            "--stocks-only",
            "--json",
        ]
    )

    assert preview_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["stocks_only"] is True
    assert payload["coverage_scope"] == "stock_like"
    verification = payload["post_import_verification"]
    assert verification["stocks_only"] is True
    assert verification["coverage_scope"] == "stock_like"
    assert verification["active_security_count"] == 2
    assert verification["missing_as_of_bar_count"] == 2
    assert verification["projected_missing_after_import_count"] == 0
    assert verification["preview_projection_status"] == "would_clear_market_bars"
    assert payload["external_calls_made"] == 0
    assert payload["db_writes_made"] == 0

    human_code = main(
        [
            "market-bars",
            "saved-import",
            "--expected-as-of",
            "2026-05-08",
            "--fixture",
            str(fixture_path),
            "--stocks-only",
        ]
    )
    assert human_code == 0
    text = capsys.readouterr().out
    assert "stocks_only=true" in text
    assert "scope=stock_like" in text
    assert "projected_missing=0" in text
    assert "projection=would_clear_market_bars" in text


def test_market_bars_repair_plan_blocks_provider_fill_when_health_is_down(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    database_url = _database_url(tmp_path)
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("CATALYST_POLYGON_API_KEY", "fixture-key")

    assert main(["init-db"]) == 0
    capsys.readouterr()
    engine = create_engine(database_url, future=True)
    MarketRepository(engine).upsert_securities([_security("AADR", "Alpha ADR", "ADRC")])
    ProviderRepository(engine).save_health(
        ConnectorHealth(
            provider="polygon",
            status=ConnectorHealthStatus.DOWN,
            checked_at=datetime(2026, 5, 15, 21, tzinfo=UTC),
            reason="HTTP 403 from grouped daily",
            latency_ms=12.0,
        )
    )

    assert (
        main(
            [
                "market-bars",
                "repair-plan",
                "--expected-as-of",
                "2026-05-15",
                "--json",
            ]
        )
        == 0
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["provider_fill_status"] == "blocked_by_provider_health"
    assert payload["provider_health"] == {
        "provider": "polygon",
        "status": "down",
        "reason": "HTTP 403 from grouped daily",
        "checked_at": "2026-05-15T21:00:00+00:00",
    }
    assert "fix the Polygon/Massive provider health" in payload["next_action"]
    approval_packet = payload["provider_saved_file_capture_approval_packet"]
    assert approval_packet["status"] == "blocked_by_provider_health"
    assert approval_packet["approval_required"] is False
    assert approval_packet["external_calls_if_approved"] == 0
    assert payload["external_calls_made"] == 0

    assert (
        main(
            [
                "market-bars",
                "repair-plan",
                "--expected-as-of",
                "2026-05-15",
            ]
        )
        == 0
    )
    text = capsys.readouterr().out
    assert "provider_option=status=blocked_by_provider_health" in text
    assert "provider_health=status=down" in text
    assert "HTTP 403 from grouped daily" in text


def test_market_bars_repair_plan_warns_for_stale_eod_provider_health(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    database_url = _database_url(tmp_path)
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("CATALYST_POLYGON_API_KEY", "fixture-key")

    assert main(["init-db"]) == 0
    capsys.readouterr()
    engine = create_engine(database_url, future=True)
    MarketRepository(engine).upsert_securities([_security("AADR", "Alpha ADR", "ADRC")])
    ProviderRepository(engine).save_health(
        ConnectorHealth(
            provider="polygon",
            status=ConnectorHealthStatus.DOWN,
            checked_at=datetime(2026, 5, 19, 2, 12, tzinfo=UTC),
            reason=(
                "HTTP 403 from grouped daily; detail=NOT_AUTHORIZED: Attempted "
                "to request today's data before end of day."
            ),
            latency_ms=12.0,
        )
    )

    assert (
        main(
            [
                "market-bars",
                "repair-plan",
                "--expected-as-of",
                "2026-05-15",
                "--json",
            ]
        )
        == 0
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["provider_fill_status"] == (
        "ready_for_approval_with_health_warning"
    )
    assert payload["provider_health_blocks_fill"] is False
    assert "stale same-day EOD denial" in payload["provider_health_warning"]
    assert payload["provider_fill_external_call_count"] == 1
    assert payload["external_calls_made"] == 0

    assert (
        main(
            [
                "market-bars",
                "repair-plan",
                "--expected-as-of",
                "2026-05-15",
            ]
        )
        == 0
    )
    text = capsys.readouterr().out
    assert "provider_option=status=ready_for_approval_with_health_warning" in text
    assert "provider_health_warning=Stored Polygon/Massive health" in text


def test_market_bars_repair_plan_previews_existing_local_template(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    database_url = _database_url(tmp_path)
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    assert main(["init-db"]) == 0
    capsys.readouterr()
    engine = create_engine(database_url, future=True)
    MarketRepository(engine).upsert_securities(
        [
            _security("BSTK", "Beta Stock", "CS"),
            _security("AADR", "Alpha ADR", "ADRC"),
        ]
    )
    MarketRepository(engine).upsert_daily_bars(
        [_daily_bar("BSTK", date(2026, 5, 15))]
    )

    assert (
        main(
            [
                "market-bars",
                "template",
                "--expected-as-of",
                "2026-05-15",
                "--out",
                "data\\local\\manual-stock-bars-2026-05-15.csv",
                "--missing-only",
                "--stocks-only",
            ]
        )
        == 0
    )
    capsys.readouterr()

    assert (
        main(
            [
                "market-bars",
                "repair-plan",
                "--expected-as-of",
                "2026-05-15",
                "--stocks-only",
                "--json",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    preview = payload["local_template_preview"]
    assert payload["local_template_exists"] is True
    assert preview["status"] == "invalid"
    assert preview["row_count"] == 1
    assert preview["invalid_row_count"] == 1
    assert preview["blank_required_count"] == 6
    assert preview["blank_required_field_counts"] == {
        "open": 1,
        "high": 1,
        "low": 1,
        "close": 1,
        "volume": 1,
        "vwap": 1,
    }
    assert preview["fill_progress"] == {
        "complete_rows": 0,
        "partial_rows": 0,
        "empty_rows": 1,
        "filled_rows": 0,
    }
    assert preview["external_calls_made"] == 0
    assert payload["operator_step"]["status"] == "manual_fill_required"
    assert payload["operator_step"]["kind"] == "fill_first_complete_rows"
    assert "Fill all missing OHLCV/VWAP rows" in payload["operator_step"]["action"]
    assert "incremental checkpoint" in payload["operator_step"]["action"]
    assert payload["operator_step"]["manual_step"] is True
    assert payload["operator_step"]["command"] is None
    assert payload["operator_step"]["after_manual_command"].endswith(
        "--stocks-only --complete-rows-only"
    )
    assert payload["missing_security_type_counts"] == {"ADRC": 1}
    assert payload["missing_with_local_history_count"] == 0
    assert payload["missing_without_local_history_count"] == 1

    assert (
        main(
            [
                "market-bars",
                "repair-plan",
                "--expected-as-of",
                "2026-05-15",
                "--stocks-only",
            ]
        )
        == 0
    )
    captured = capsys.readouterr()
    assert (
        "local_template=path=data\\local\\manual-stock-bars-2026-05-15.csv exists=true"
        in captured.out
    )
    assert (
        "local_bar_history=missing_with_history=0 missing_without_history=1"
        in captured.out
    )
    assert "missing_security_types=ADRC:1" in captured.out
    assert "missing_without_local_history=AADR" in captured.out
    assert "local_template_preview=status=invalid" in captured.out
    assert (
        "local_template_fill_progress=complete=0 partial=0 empty=1 filled=0"
        in captured.out
    )
    assert "operator_step=status=manual_fill_required" in captured.out
    assert "after_manual=catalyst-radar market-bars import" in captured.out
    assert "local_template_blank_required_fields=open=1" in captured.out
    assert "local_template_invalid_examples=row 2 AADR 2026-05-15" in captured.out


def test_market_bars_repair_plan_guides_complete_rows_only_preview(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    database_url = _database_url(tmp_path)
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    assert main(["init-db"]) == 0
    capsys.readouterr()
    engine = create_engine(database_url, future=True)
    MarketRepository(engine).upsert_securities(
        [
            _security("AAA", "Alpha Stock", "CS"),
            _security("BBB", "Beta Stock", "CS"),
        ]
    )
    template_path = Path("data") / "local" / "manual-stock-bars-2026-05-15.csv"
    template_path.parent.mkdir(parents=True, exist_ok=True)
    _write_mixed_manual_bars(
        template_path,
        complete_tickers=["AAA"],
        empty_tickers=["BBB"],
        as_of="2026-05-15",
    )

    assert (
        main(
            [
                "market-bars",
                "repair-plan",
                "--expected-as-of",
                "2026-05-15",
                "--stocks-only",
                "--json",
            ]
        )
        == 0
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["local_template_preview"]["status"] == "invalid"
    assert payload["local_template_preview"]["bars_at_expected_as_of"] == 0
    assert payload["local_template_preview"]["coverage_after_import_count"] == 0
    assert payload["local_template_preview"]["missing_expected_count"] == 2
    assert payload["local_template_preview"]["missing_expected_tickers"] == [
        "AAA",
        "BBB",
    ]
    assert payload["local_template_preview"]["fill_progress"] == {
        "complete_rows": 1,
        "partial_rows": 0,
        "empty_rows": 1,
        "filled_rows": 1,
    }
    assert payload["operator_step"] == {
        "status": "needs_incremental_preview",
        "kind": "preview_complete_rows_only",
        "action": (
            "Preview the completed rows with --complete-rows-only; blank rows "
            "can remain blank until later."
        ),
        "command": payload["manual_incremental_import_preview_command"],
        "after_manual_command": payload["manual_incremental_import_execute_command"],
        "manual_step": False,
        "external_calls_made": 0,
    }

    assert (
        main(
            [
                "market-bars",
                "repair-plan",
                "--expected-as-of",
                "2026-05-15",
                "--stocks-only",
            ]
        )
        == 0
    )
    text = capsys.readouterr().out
    assert "operator_step=status=needs_incremental_preview" in text
    assert "command=catalyst-radar market-bars import" in text
    assert "--stocks-only --complete-rows-only" in text


def test_market_bars_repair_plan_keeps_invalid_numeric_in_fix_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    database_url = _database_url(tmp_path)
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    assert main(["init-db"]) == 0
    capsys.readouterr()
    engine = create_engine(database_url, future=True)
    MarketRepository(engine).upsert_securities(
        [
            _security("AAA", "Alpha Stock", "CS"),
            _security("BBB", "Beta Stock", "CS"),
        ]
    )
    template_path = Path("data") / "local" / "manual-stock-bars-2026-05-15.csv"
    template_path.parent.mkdir(parents=True, exist_ok=True)
    _write_mixed_manual_bars(
        template_path,
        complete_tickers=["AAA"],
        empty_tickers=["BBB"],
        as_of="2026-05-15",
    )
    rows = _read_csv_rows(template_path)
    rows[0]["open"] = "not-a-number"
    with template_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=MANUAL_BAR_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    assert (
        main(
            [
                "market-bars",
                "repair-plan",
                "--expected-as-of",
                "2026-05-15",
                "--stocks-only",
                "--json",
            ]
        )
        == 0
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["local_template_preview"]["invalid_numeric_count"] == 1
    assert payload["operator_step"]["status"] == "fix_invalid_rows"
    assert payload["operator_step"]["kind"] == "fix_csv_values"
    assert payload["operator_step"]["command"] == payload["manual_import_preview_command"]


def test_market_bars_repair_plan_detects_stale_blank_template_schema(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    database_url = _database_url(tmp_path)
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    assert main(["init-db"]) == 0
    capsys.readouterr()
    engine = create_engine(database_url, future=True)
    MarketRepository(engine).upsert_securities(
        [
            _security("BSTK", "Beta Stock", "CS"),
            _security("AADR", "Alpha ADR", "ADRC"),
        ]
    )
    MarketRepository(engine).upsert_daily_bars(
        [_daily_bar("BSTK", date(2026, 5, 15))]
    )

    template_path = Path("data") / "local" / "manual-stock-bars-2026-05-15.csv"
    template_path.parent.mkdir(parents=True, exist_ok=True)
    stale_columns = [column for column in MANUAL_BAR_COLUMNS if column != "name"]
    with template_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=stale_columns)
        writer.writeheader()
        writer.writerow(
            {
                "ticker": "AADR",
                "date": "2026-05-15",
                "security_type": "ADRC",
                "template_reason": "missing_as_of_bar",
                "open": "",
                "high": "",
                "low": "",
                "close": "",
                "volume": "",
                "vwap": "",
                "adjusted": "true",
                "provider": "manual_csv",
                "source_ts": "2026-05-15T21:00:00+00:00",
                "available_at": "2026-05-15T21:00:00+00:00",
            }
        )

    assert (
        main(
            [
                "market-bars",
                "repair-plan",
                "--expected-as-of",
                "2026-05-15",
                "--stocks-only",
                "--json",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["local_template_schema"]["status"] == "stale_context_columns"
    assert payload["local_template_schema"]["missing_context_columns"] == ["name"]
    assert payload["manual_template_regenerate_command"].endswith(
        "--missing-only --stocks-only --overwrite"
    )
    assert payload["operator_step"]["status"] == "stale_template_schema"
    assert payload["operator_step"]["kind"] == "regenerate_blank_template"
    assert payload["operator_step"]["command"] == payload[
        "manual_template_regenerate_command"
    ]
    assert payload["operator_step"]["manual_step"] is False
    assert payload["operator_step"]["external_calls_made"] == 0
    assert payload["local_template_preview"]["fill_progress"] == {
        "complete_rows": 0,
        "partial_rows": 0,
        "empty_rows": 1,
        "filled_rows": 0,
    }

    assert (
        main(
            [
                "market-bars",
                "repair-plan",
                "--expected-as-of",
                "2026-05-15",
                "--stocks-only",
            ]
        )
        == 0
    )
    text = capsys.readouterr().out
    assert "local_template_schema=status=stale_context_columns missing_context=name" in text
    assert "operator_step=status=stale_template_schema" in text
    assert "--stocks-only --overwrite" in text


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
    assert exit_code == 2
    assert captured.err == ""
    assert "manual_market_bars_import status=invalid" in captured.out
    assert "invalid=rows=6" in captured.out
    assert "fill_progress=complete=0 partial=6 empty=0 filled=6" in captured.out
    assert "blank_required=6" in captured.out
    assert "blank_required_fields=open=6" in captured.out
    assert "invalid_examples=row" in captured.out
    assert "Plan only: no database writes were made." not in captured.out
    assert "external_calls=0" in captured.out


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
    assert "projected_missing=0 projection=would_clear_market_bars" in preview.out
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
    assert "post_import_verification status=market_bars_cleared missing=0" in executed.out
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


def _write_mixed_manual_bars(
    path: Path,
    *,
    complete_tickers: list[str],
    empty_tickers: list[str],
    as_of: str,
) -> None:
    stamp = datetime(2026, 5, 11, 21, tzinfo=UTC).isoformat()
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=MANUAL_BAR_COLUMNS)
        writer.writeheader()
        for index, ticker in enumerate(complete_tickers):
            writer.writerow(
                {
                    "ticker": ticker,
                    "date": as_of,
                    "open": "100",
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
        for ticker in empty_tickers:
            writer.writerow(
                {
                    "ticker": ticker,
                    "date": as_of,
                    "open": "",
                    "high": "",
                    "low": "",
                    "close": "",
                    "volume": "",
                    "vwap": "",
                    "adjusted": "true",
                    "provider": "manual_csv",
                    "source_ts": stamp,
                    "available_at": stamp,
                }
            )


def _daily_bar(ticker: str, bar_date: date) -> DailyBar:
    return DailyBar(
        ticker=ticker,
        date=bar_date,
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.0,
        volume=1_000_000,
        vwap=100.0,
        adjusted=True,
        provider="manual_csv",
        source_ts=datetime(2026, 5, 15, 21, tzinfo=UTC),
        available_at=datetime(2026, 5, 15, 21, tzinfo=UTC),
    )

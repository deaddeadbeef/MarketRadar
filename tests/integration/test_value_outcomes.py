from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select

from apps.api.main import create_app
from catalyst_radar.cli import main
from catalyst_radar.core.models import DailyBar
from catalyst_radar.storage.db import create_schema, engine_from_url
from catalyst_radar.storage.repositories import MarketRepository
from catalyst_radar.storage.schema import value_ledger_entries, value_outcomes
from catalyst_radar.storage.validation_repositories import ValidationRepository
from catalyst_radar.validation.value_ledger import build_value_ledger_entry


def test_value_outcome_cli_preview_execute_and_list(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'value-outcome-cli.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = engine_from_url(database_url)
    create_schema(engine)
    entry_id = _seed_ledger_and_bars(engine)

    preview_exit = main(
        [
            "value-outcome",
            "update",
            "--ledger-id",
            entry_id,
            "--outcome-available-at",
            "2026-08-20T21:00:00+00:00",
            "--sector-etf",
            "XLK",
            "--invalidation-price",
            "95",
            "--preview",
            "--json",
        ]
    )

    assert preview_exit == 0
    preview = json.loads(capsys.readouterr().out)
    assert preview["mode"] == "preview"
    assert preview["external_calls_made"] == 0
    assert preview["db_writes_made"] == 0
    assert "value-outcome update" in preview["preview_command"]
    assert "--preview" in preview["preview_command"]
    assert "--execute" not in preview["preview_command"]
    assert "value-outcome update" in preview["execute_command"]
    assert "--execute" in preview["execute_command"]
    assert "--preview" not in preview["execute_command"]
    assert preview["api"] == "POST /api/value-outcomes/update"
    assert preview["api_preview_request_body"]["execute"] is False
    assert preview["api_execute_request_body"]["execute"] is True
    assert preview["api_execute_request_body"]["value_ledger_entry_id"] == entry_id
    outcome = preview["outcome"]
    assert outcome["status"] == "computed"
    assert outcome["trading_days_observed"] == 60
    assert outcome["payload"]["expected_review_horizon_days"] == 60
    assert outcome["payload"]["expected_review_horizon_expired"] is True
    assert outcome["setup_follow_through"] == "followed_through"
    assert outcome["payload"]["setup_follow_through"] == "followed_through"
    assert outcome["setup_follow_through_horizon_days"] == 20
    assert outcome["setup_follow_through_direction"] == "bullish"
    assert outcome["outcome_direction"] == "bullish"
    assert outcome["gap_outcome"] == "gap_up"
    assert round(outcome["gap_return"], 6) == 0.01
    assert round(outcome["return_5d"], 6) == 0.1
    assert round(outcome["directional_return_5d"], 6) == 0.1
    assert round(outcome["spy_relative_return_20d"], 6) == 0.25
    assert round(outcome["directional_spy_relative_return_20d"], 6) == 0.25
    assert outcome["sector_etf_ticker"] == "XLK"
    assert outcome["invalidation_touched"] is True
    with engine.connect() as conn:
        assert conn.execute(select(func.count()).select_from(value_outcomes)).scalar_one() == 0

    execute_exit = main(
        [
            "value-outcome",
            "update",
            "--ledger-id",
            entry_id,
            "--outcome-available-at",
            "2026-08-20T21:00:00+00:00",
            "--sector-etf",
            "XLK",
            "--invalidation-price",
            "95",
            "--execute",
            "--json",
        ]
    )

    assert execute_exit == 0
    executed = json.loads(capsys.readouterr().out)
    assert executed["db_writes_made"] == 1
    assert "--preview" in executed["preview_command"]
    assert executed["execute_command"] is None
    assert executed["api_execute_request_body"] is None
    list_exit = main(["value-outcome", "list", "--ledger-id", entry_id, "--json"])
    assert list_exit == 0
    listed = json.loads(capsys.readouterr().out)
    assert listed["count"] == 1
    assert listed["status_counts"] == {"computed": 1}
    outcome_id = executed["outcome"]["id"]
    show_exit = main(["value-outcome", "show", outcome_id, "--json"])
    assert show_exit == 0
    shown = json.loads(capsys.readouterr().out)
    assert shown["schema_version"] == "value-outcome-v1"
    assert shown["external_calls_made"] == 0
    assert shown["db_writes_made"] == 0
    assert shown["outcome"]["id"] == outcome_id
    assert shown["outcome"]["status"] == "computed"


def test_outcome_cli_alias_uses_value_outcome_contract(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'outcome-cli-alias.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = engine_from_url(database_url)
    create_schema(engine)
    entry_id = _seed_ledger_and_bars(engine)

    preview_exit = main(
        [
            "outcome",
            "update",
            "--ledger-id",
            entry_id,
            "--outcome-available-at",
            "2026-08-20T21:00:00+00:00",
            "--preview",
            "--json",
        ]
    )

    assert preview_exit == 0
    preview = json.loads(capsys.readouterr().out)
    assert preview["schema_version"] == "value-outcome-update-v1"
    assert preview["mode"] == "preview"
    assert preview["external_calls_made"] == 0
    assert preview["db_writes_made"] == 0
    assert preview["outcome"]["status"] == "computed"
    assert "value-outcome update" in preview["preview_command"]
    with engine.connect() as conn:
        assert conn.execute(select(func.count()).select_from(value_outcomes)).scalar_one() == 0

    list_exit = main(["outcome", "list", "--ledger-id", entry_id, "--json"])

    assert list_exit == 0
    listed = json.loads(capsys.readouterr().out)
    assert listed["schema_version"] == "value-outcomes-v1"
    assert listed["count"] == 0
    assert listed["external_calls_made"] == 0
    assert listed["db_writes_made"] == 0

    coverage_exit = main(
        [
            "outcome",
            "coverage",
            "--available-at",
            "2026-08-20T21:00:00+00:00",
            "--period-start",
            "2026-05-01",
            "--period-end",
            "2026-05-31",
            "--json",
        ]
    )

    assert coverage_exit == 0
    coverage = json.loads(capsys.readouterr().out)
    assert coverage["schema_version"] == "value-outcome-coverage-v1"
    assert coverage["ledger_entry_count"] == 1
    assert coverage["missing_outcome_count"] == 1
    assert coverage["external_calls_made"] == 0
    assert coverage["db_writes_made"] == 0


def test_value_outcome_coverage_reports_ledger_rows_missing_outcomes(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'value-outcome-coverage.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = engine_from_url(database_url)
    create_schema(engine)
    covered_entry_id = _seed_ledger_and_bars(engine)
    missing_entry_id = _seed_value_ledger_entry(
        engine,
        artifact_id="note-AAPL",
        ticker="AAPL",
    )
    execute_exit = main(
        [
            "value-outcome",
            "update",
            "--ledger-id",
            covered_entry_id,
            "--outcome-available-at",
            "2026-08-20T21:00:00+00:00",
            "--execute",
            "--json",
        ]
    )
    assert execute_exit == 0
    _ = capsys.readouterr()
    with engine.connect() as conn:
        ledger_before = [
            dict(row._mapping)
            for row in conn.execute(select(value_ledger_entries))
        ]
        assert conn.execute(select(func.count()).select_from(value_outcomes)).scalar_one() == 1

    coverage_exit = main(
        [
            "value-outcome",
            "coverage",
            "--available-at",
            "2026-08-20T21:00:00+00:00",
            "--period-start",
            "2026-05-01",
            "--period-end",
            "2026-05-31",
            "--json",
        ]
    )

    assert coverage_exit == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == "value-outcome-coverage-v1"
    assert payload["status"] == "gaps"
    assert payload["ledger_entry_count"] == 2
    assert payload["linked_outcome_count"] == 1
    assert payload["computed_outcome_count"] == 1
    assert payload["missing_outcome_count"] == 1
    assert payload["first_missing_value_ledger_entry_id"] == missing_entry_id
    assert payload["first_missing_ticker"] == "AAPL"
    assert payload["coverage_pct"] == 50.0
    assert payload["external_calls_made"] == 0
    assert payload["db_writes_made"] == 0
    covered = next(
        row
        for row in payload["rows"]
        if row["value_ledger_entry_id"] == covered_entry_id
    )
    assert covered["setup_follow_through"] == "followed_through"
    assert round(covered["directional_return_20d"], 6) == round(
        covered["return_20d"],
        6,
    )
    assert covered["gap_outcome"] == "gap_up"
    missing = next(
        row
        for row in payload["rows"]
        if row["value_ledger_entry_id"] == missing_entry_id
    )
    assert missing["outcome_status"] == "missing"
    assert "value-outcome update" in missing["preview_update_command"]
    assert "--preview" in missing["preview_update_command"]
    assert "--execute" not in missing["preview_update_command"]
    assert payload["canonical_next_command"] == missing["preview_update_command"]
    assert "--preview" in payload["canonical_next_command"]
    assert "--execute" not in payload["canonical_next_command"]
    with engine.connect() as conn:
        ledger_after = [
            dict(row._mapping)
            for row in conn.execute(select(value_ledger_entries))
        ]
        assert conn.execute(select(func.count()).select_from(value_outcomes)).scalar_one() == 1
    assert ledger_after == ledger_before


def test_value_outcome_coverage_reports_no_ledger_entries(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'value-outcome-empty-coverage.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = engine_from_url(database_url)
    create_schema(engine)

    coverage_exit = main(
        [
            "value-outcome",
            "coverage",
            "--available-at",
            "2026-05-31T21:00:00+00:00",
            "--period-start",
            "2026-05-01",
            "--period-end",
            "2026-05-31",
            "--json",
        ]
    )

    assert coverage_exit == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == "value-outcome-coverage-v1"
    assert payload["status"] == "no_ledger_entries"
    assert payload["ledger_entry_count"] == 0
    assert payload["linked_outcome_count"] == 0
    assert payload["missing_outcome_count"] == 0
    assert payload["computed_outcome_count"] == 0
    assert payload["first_missing_value_ledger_entry_id"] is None
    assert payload["first_missing_ticker"] is None
    assert payload["canonical_next_command"] is None
    assert payload["coverage_pct"] is None
    assert "value-ledger entries" in payload["next_action"]
    assert payload["external_calls_made"] == 0
    assert payload["db_writes_made"] == 0
    assert payload["rows"] == []


def test_value_outcome_coverage_api_and_monthly_report_surface_missing_rows(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'value-outcome-coverage-api.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = engine_from_url(database_url)
    create_schema(engine)
    entry_id = _seed_value_ledger_entry(engine, artifact_id="note-NVDA", ticker="NVDA")
    client = TestClient(create_app())

    coverage_response = client.get(
        "/api/value-outcomes/coverage",
        params={
            "available_at": "2026-05-31T21:00:00+00:00",
            "period_start": "2026-05-01",
            "period_end": "2026-05-31",
        },
    )

    assert coverage_response.status_code == 200
    coverage = coverage_response.json()
    assert coverage["ledger_entry_count"] == 1
    assert coverage["missing_outcome_count"] == 1
    assert coverage["first_missing_value_ledger_entry_id"] == entry_id
    assert coverage["first_missing_ticker"] == "NVDA"
    assert "value-outcome update" in coverage["canonical_next_command"]
    assert "--preview" in coverage["canonical_next_command"]
    assert "--execute" not in coverage["canonical_next_command"]
    assert coverage["external_calls_made"] == 0
    assert coverage["db_writes_made"] == 0
    assert coverage["rows"][0]["value_ledger_entry_id"] == entry_id

    report_response = client.get(
        "/api/value-report/monthly",
        params={
            "month": "2026-05",
            "available_at": "2026-05-31T21:00:00+00:00",
        },
    )

    assert report_response.status_code == 200
    report = report_response.json()
    assert report["first_blocker"] == "value_outcome_coverage"
    outcome_coverage = report["value_outcome_coverage"]
    assert outcome_coverage["status"] == "gaps"
    assert outcome_coverage["ledger_entry_count"] == 1
    assert outcome_coverage["missing_outcome_count"] == 1
    assert outcome_coverage["first_missing_value_ledger_entry_id"] == entry_id
    assert outcome_coverage["first_missing_ticker"] == "NVDA"
    assert outcome_coverage["canonical_next_command"] == coverage["canonical_next_command"]
    assert report["canonical_next_command"] == coverage["canonical_next_command"]
    assert "--preview" in report["canonical_next_command"]
    assert "--execute" not in report["canonical_next_command"]
    assert outcome_coverage["external_calls_made"] == 0
    assert outcome_coverage["db_writes_made"] == 0


def test_value_outcome_api_rejects_missing_future_bars_without_mutating_ledger(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'value-outcome-api.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = engine_from_url(database_url)
    create_schema(engine)
    entry_id = _seed_ledger_and_bars(engine, future_count=4)
    with engine.connect() as conn:
        before = conn.execute(select(value_ledger_entries).limit(1)).first()
    assert before is not None

    response = TestClient(create_app()).post(
        "/api/value-outcomes/update",
        json={
            "value_ledger_entry_id": entry_id,
            "outcome_available_at": "2026-05-25T21:00:00+00:00",
            "execute": True,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["external_calls_made"] == 0
    assert payload["db_writes_made"] == 1
    assert "--preview" in payload["preview_command"]
    assert payload["execute_command"] is None
    assert payload["api"] == "POST /api/value-outcomes/update"
    assert payload["api_preview_request_body"]["execute"] is False
    assert payload["api_execute_request_body"] is None
    outcome = payload["outcome"]
    assert outcome["status"] == "insufficient_data"
    assert outcome["trading_days_observed"] == 4
    assert outcome["payload"]["expected_review_horizon_days"] == 60
    assert outcome["payload"]["expected_review_horizon_expired"] is False
    assert outcome["setup_follow_through"] == "insufficient_data"
    assert outcome["gap_outcome"] == "gap_up"
    assert outcome["return_5d"] is None
    outcome_id = outcome["id"]
    show_response = TestClient(create_app()).get(f"/api/value-outcomes/{outcome_id}")
    assert show_response.status_code == 200
    shown = show_response.json()
    assert shown["external_calls_made"] == 0
    assert shown["db_writes_made"] == 0
    assert shown["outcome"]["id"] == outcome_id
    assert shown["outcome"]["status"] == "insufficient_data"
    missing_response = TestClient(create_app()).get("/api/value-outcomes/missing-outcome")
    assert missing_response.status_code == 404
    with engine.connect() as conn:
        after = conn.execute(select(value_ledger_entries).limit(1)).first()
    assert after is not None
    assert dict(after._mapping) == dict(before._mapping)


def test_value_outcome_ignores_future_bars_after_cutoff(tmp_path, monkeypatch) -> None:
    database_url = f"sqlite:///{(tmp_path / 'value-outcome-leakage.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = engine_from_url(database_url)
    create_schema(engine)
    entry_id = _seed_ledger_and_bars(
        engine,
        future_count=5,
        late_future_bar_index=5,
    )

    response = TestClient(create_app()).post(
        "/api/value-outcomes/update",
        json={
            "value_ledger_entry_id": entry_id,
            "outcome_available_at": "2026-05-25T21:00:00+00:00",
        },
    )

    assert response.status_code == 200
    outcome = response.json()["outcome"]
    assert outcome["status"] == "insufficient_data"
    assert outcome["trading_days_observed"] == 4
    assert outcome["return_5d"] is None
    assert outcome["payload"]["expected_review_horizon_days"] == 60
    assert outcome["payload"]["expected_review_horizon_expired"] is False
    assert outcome["payload"]["no_future_leakage"] is True


def test_value_outcome_marks_failed_bearish_follow_through_and_gap_down(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'value-outcome-follow-through.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = engine_from_url(database_url)
    create_schema(engine)
    entry_id = _seed_ledger_and_bars(
        engine,
        priced_in_direction="bearish",
        first_future_open=99,
    )

    response = TestClient(create_app()).post(
        "/api/value-outcomes/update",
        json={
            "value_ledger_entry_id": entry_id,
            "outcome_available_at": "2026-08-20T21:00:00+00:00",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["external_calls_made"] == 0
    assert payload["db_writes_made"] == 0
    outcome = payload["outcome"]
    assert outcome["status"] == "computed"
    assert outcome["setup_follow_through"] == "failed"
    assert outcome["payload"]["setup_follow_through"] == "failed"
    assert outcome["setup_follow_through_direction"] == "bearish"
    assert outcome["gap_outcome"] == "gap_down"
    assert round(outcome["gap_return"], 6) == -0.01


def test_value_outcome_exposes_directional_returns_for_bearish_follow_through(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'value-outcome-bearish-returns.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = engine_from_url(database_url)
    create_schema(engine)
    entry_id = _seed_ledger_and_bars(
        engine,
        priced_in_direction="bearish",
        first_future_open=99,
        invalidation_price=105,
        msft_close_fn=_bearish_msft_close,
    )

    response = TestClient(create_app()).post(
        "/api/value-outcomes/update",
        json={
            "value_ledger_entry_id": entry_id,
            "outcome_available_at": "2026-08-20T21:00:00+00:00",
            "sector_etf_ticker": "XLK",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["external_calls_made"] == 0
    assert payload["db_writes_made"] == 0
    outcome = payload["outcome"]
    assert outcome["status"] == "computed"
    assert outcome["setup_follow_through"] == "followed_through"
    assert outcome["outcome_direction"] == "bearish"
    assert round(outcome["return_20d"], 6) == -0.2
    assert round(outcome["directional_return_20d"], 6) == 0.2
    assert round(outcome["spy_relative_return_20d"], 6) == -0.25
    assert round(outcome["directional_spy_relative_return_20d"], 6) == 0.25
    assert round(outcome["sector_relative_return_20d"], 6) == pytest.approx(-0.222222)
    assert round(outcome["directional_sector_relative_return_20d"], 6) == pytest.approx(
        0.222222
    )
    assert outcome["payload"]["directional_return_20d"] == outcome[
        "directional_return_20d"
    ]
    assert outcome["payload"]["outcome_direction"] == "bearish"


def test_value_outcome_uses_bearish_high_for_invalidation_touch(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'value-outcome-bearish-stop.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = engine_from_url(database_url)
    create_schema(engine)
    entry_id = _seed_ledger_and_bars(
        engine,
        future_count=4,
        priced_in_direction="bearish",
        invalidation_price=105,
    )

    response = TestClient(create_app()).post(
        "/api/value-outcomes/update",
        json={
            "value_ledger_entry_id": entry_id,
            "outcome_available_at": "2026-05-25T21:00:00+00:00",
        },
    )

    assert response.status_code == 200
    outcome = response.json()["outcome"]
    assert outcome["status"] == "insufficient_data"
    assert outcome["trading_days_observed"] == 4
    assert outcome["setup_follow_through_direction"] == "bearish"
    assert outcome["invalidation_price"] == 105
    assert outcome["invalidation_touched"] is False
    assert outcome["payload"]["invalidation_touch_direction"] == "bearish"


def test_value_outcome_uses_bearish_direction_for_excursions(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'value-outcome-bearish-excursion.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = engine_from_url(database_url)
    create_schema(engine)
    entry_id = _seed_ledger_and_bars(
        engine,
        future_count=4,
        priced_in_direction="bearish",
        invalidation_price=105,
    )

    response = TestClient(create_app()).post(
        "/api/value-outcomes/update",
        json={
            "value_ledger_entry_id": entry_id,
            "outcome_available_at": "2026-05-25T21:00:00+00:00",
        },
    )

    assert response.status_code == 200
    outcome = response.json()["outcome"]
    assert outcome["setup_follow_through_direction"] == "bearish"
    assert round(outcome["max_adverse_excursion"], 6) == 0.04
    assert round(outcome["max_favorable_excursion"], 6) == -0.06
    assert outcome["payload"]["excursion_direction"] == "bearish"


def _seed_ledger_and_bars(
    engine,
    *,
    future_count: int = 60,
    late_future_bar_index: int | None = None,
    priced_in_direction: str = "bullish",
    first_future_open: float | None = None,
    invalidation_price: float = 95,
    msft_close_fn=None,
) -> str:
    msft_close_fn = msft_close_fn or _msft_close
    entry_id = _seed_value_ledger_entry(
        engine,
        artifact_id="note-MSFT",
        ticker="MSFT",
        priced_in_direction=priced_in_direction,
        invalidation_price=invalidation_price,
    )
    MarketRepository(engine).upsert_daily_bars(
        [
            _bar("MSFT", date(2026, 5, 15), 100),
            _bar("SPY", date(2026, 5, 15), 200),
            _bar("XLK", date(2026, 5, 15), 300),
            *[
                _bar(
                    "MSFT",
                    date(2026, 5, 15) + timedelta(days=offset),
                    msft_close_fn(offset),
                    open_price=first_future_open if offset == 1 else None,
                    low=94 if offset == 2 else None,
                    late=late_future_bar_index == offset,
                )
                for offset in range(1, future_count + 1)
            ],
            *[
                _bar("SPY", date(2026, 5, 15) + timedelta(days=offset), 200 + offset / 2)
                for offset in range(1, future_count + 1)
            ],
            *[
                _bar("XLK", date(2026, 5, 15) + timedelta(days=offset), 300 + offset / 3)
                for offset in range(1, future_count + 1)
            ],
        ]
    )
    return entry_id


def _seed_value_ledger_entry(
    engine,
    *,
    artifact_id: str,
    ticker: str,
    priced_in_direction: str = "bullish",
    invalidation_price: float = 95,
) -> str:
    entry = build_value_ledger_entry(
        artifact_type="manual_note",
        artifact_id=artifact_id,
        label="useful",
        ticker=ticker,
        priced_in_status=f"{priced_in_direction}_not_priced_in",
        priced_in_direction=priced_in_direction,
        as_of=date(2026, 5, 15),
        estimated_value_usd=10,
        confidence=1,
        source="test",
        available_at=datetime(2026, 5, 15, 21, tzinfo=UTC),
        payload={"invalidation_price": invalidation_price},
    )
    ValidationRepository(engine).upsert_value_ledger_entry(entry)
    return entry.id


def _msft_close(offset: int) -> float:
    if offset == 5:
        return 110
    if offset == 10:
        return 120
    if offset == 20:
        return 130
    if offset == 60:
        return 150
    return 100 + offset


def _bearish_msft_close(offset: int) -> float:
    return 100 - offset


def _bar(
    ticker: str,
    bar_date: date,
    close: float,
    *,
    open_price: float | None = None,
    low: float | None = None,
    late: bool = False,
) -> DailyBar:
    available_at = (
        datetime(2026, 5, 25, 22, tzinfo=UTC)
        if late
        else datetime.combine(bar_date, datetime.min.time(), tzinfo=UTC).replace(hour=20)
    )
    return DailyBar(
        ticker=ticker,
        date=bar_date,
        open=open_price if open_price is not None else close,
        high=close,
        low=low if low is not None else close,
        close=close,
        volume=1000,
        vwap=close,
        adjusted=True,
        provider="test",
        source_ts=available_at,
        available_at=available_at,
    )

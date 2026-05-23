from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta

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
            "--json",
        ]
    )

    assert preview_exit == 0
    preview = json.loads(capsys.readouterr().out)
    assert preview["mode"] == "preview"
    assert preview["external_calls_made"] == 0
    assert preview["db_writes_made"] == 0
    outcome = preview["outcome"]
    assert outcome["status"] == "computed"
    assert outcome["trading_days_observed"] == 60
    assert outcome["payload"]["expected_review_horizon_days"] == 60
    assert outcome["payload"]["expected_review_horizon_expired"] is True
    assert round(outcome["return_5d"], 6) == 0.1
    assert round(outcome["spy_relative_return_20d"], 6) == 0.25
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
    outcome = payload["outcome"]
    assert outcome["status"] == "insufficient_data"
    assert outcome["trading_days_observed"] == 4
    assert outcome["payload"]["expected_review_horizon_days"] == 60
    assert outcome["payload"]["expected_review_horizon_expired"] is False
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


def _seed_ledger_and_bars(
    engine,
    *,
    future_count: int = 60,
    late_future_bar_index: int | None = None,
) -> str:
    entry = build_value_ledger_entry(
        artifact_type="manual_note",
        artifact_id="note-MSFT",
        label="useful",
        ticker="MSFT",
        as_of=date(2026, 5, 15),
        estimated_value_usd=10,
        confidence=1,
        source="test",
        available_at=datetime(2026, 5, 15, 21, tzinfo=UTC),
        payload={"invalidation_price": 95},
    )
    ValidationRepository(engine).upsert_value_ledger_entry(entry)
    MarketRepository(engine).upsert_daily_bars(
        [
            _bar("MSFT", date(2026, 5, 15), 100),
            _bar("SPY", date(2026, 5, 15), 200),
            _bar("XLK", date(2026, 5, 15), 300),
            *[
                _bar(
                    "MSFT",
                    date(2026, 5, 15) + timedelta(days=offset),
                    _msft_close(offset),
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


def _bar(
    ticker: str,
    bar_date: date,
    close: float,
    *,
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
        open=close,
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

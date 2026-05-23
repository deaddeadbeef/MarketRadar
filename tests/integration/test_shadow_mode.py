from __future__ import annotations

import json
from datetime import UTC, date, datetime

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, insert, select

from apps.api.main import create_app
from catalyst_radar.cli import main
from catalyst_radar.core.config import AppConfig
from catalyst_radar.core.models import ActionState
from catalyst_radar.storage.db import create_schema, engine_from_url
from catalyst_radar.storage.schema import daily_bars, securities, shadow_mode_runs
from catalyst_radar.storage.validation_repositories import ValidationRepository
from catalyst_radar.validation.shadow_mode import (
    _shadow_mode_next_action,
    build_shadow_mode_run,
    classify_shadow_run_status,
    shadow_mode_status_payload,
)

AVAILABLE_AT = "2026-05-22T21:00:00+00:00"


def test_shadow_mode_cli_preview_execute_and_latest(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'shadow-mode-cli.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = engine_from_url(database_url)
    create_schema(engine)

    preview_exit = main(
        [
            "shadow-mode",
            "run",
            "--available-at",
            AVAILABLE_AT,
            "--preview",
            "--json",
        ]
    )

    assert preview_exit == 0
    preview = json.loads(capsys.readouterr().out)
    assert preview["mode"] == "preview"
    assert preview["status"] == "setup_required"
    assert preview["external_calls_made"] == 0
    assert preview["db_writes_made"] == 0
    assert preview["run"]["status"] == "setup_required"
    assert preview["run"]["provider_calls_made"] == 0
    with engine.connect() as conn:
        assert conn.execute(select(func.count()).select_from(shadow_mode_runs)).scalar_one() == 0

    execute_exit = main(
        [
            "shadow-mode",
            "run",
            "--available-at",
            AVAILABLE_AT,
            "--execute",
            "--json",
        ]
    )

    assert execute_exit == 0
    executed = json.loads(capsys.readouterr().out)
    assert executed["mode"] == "execute"
    assert executed["status"] == "setup_required"
    assert executed["db_writes_made"] == 1
    assert executed["run"]["db_writes_made"] == 1
    assert executed["run"]["status"] == "setup_required"
    with engine.connect() as conn:
        assert conn.execute(select(func.count()).select_from(shadow_mode_runs)).scalar_one() == 1

    latest_exit = main(["shadow-mode", "latest", "--json"])
    assert latest_exit == 0
    latest = json.loads(capsys.readouterr().out)
    assert latest["status"] == "setup_required"
    assert latest["run"]["id"] == executed["run"]["id"]
    assert latest["external_calls_made"] == 0
    assert latest["db_writes_made"] == 0

    status_exit = main(["shadow-mode", "status", "--json"])
    assert status_exit == 0
    status = json.loads(capsys.readouterr().out)
    assert status["status"] == "setup_required"
    assert status["latest"]["id"] == executed["run"]["id"]
    assert status["external_calls_made"] == 0
    assert status["db_writes_made"] == 0


def test_shadow_mode_api_preview_and_latest(tmp_path, monkeypatch) -> None:
    database_url = f"sqlite:///{(tmp_path / 'shadow-mode-api.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = engine_from_url(database_url)
    create_schema(engine)
    client = TestClient(create_app())

    preview_response = client.post(
        "/api/radar/shadow/runs",
        json={"available_at": AVAILABLE_AT},
    )

    assert preview_response.status_code == 200
    preview = preview_response.json()
    assert preview["mode"] == "preview"
    assert preview["status"] == "setup_required"
    assert preview["external_calls_made"] == 0
    assert preview["db_writes_made"] == 0
    with engine.connect() as conn:
        assert conn.execute(select(func.count()).select_from(shadow_mode_runs)).scalar_one() == 0

    execute_response = client.post(
        "/api/radar/shadow/runs",
        json={"available_at": AVAILABLE_AT, "execute": True},
    )

    assert execute_response.status_code == 200
    executed = execute_response.json()
    assert executed["status"] == "setup_required"
    assert executed["db_writes_made"] == 1
    latest_response = client.get("/api/radar/shadow/runs/latest")
    assert latest_response.status_code == 200
    latest = latest_response.json()
    assert latest["run"]["id"] == executed["run"]["id"]
    assert latest["external_calls_made"] == 0
    assert latest["db_writes_made"] == 0


def test_shadow_mode_preview_records_latest_market_bar_gap_without_radar_run(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'shadow-mode-gap.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = engine_from_url(database_url)
    create_schema(engine)
    cutoff = datetime.fromisoformat(AVAILABLE_AT)
    with engine.begin() as conn:
        conn.execute(
            insert(securities),
            [
                {
                    "ticker": "AAPL",
                    "name": "Apple Inc.",
                    "exchange": "NASDAQ",
                    "sector": "Technology",
                    "industry": "Consumer Electronics",
                    "market_cap": 3_000_000_000_000.0,
                    "avg_dollar_volume_20d": 1_000_000_000.0,
                    "has_options": True,
                    "is_active": True,
                    "updated_at": cutoff,
                    "metadata": {"type": "CS"},
                },
                {
                    "ticker": "MSFT",
                    "name": "Microsoft Corp.",
                    "exchange": "NASDAQ",
                    "sector": "Technology",
                    "industry": "Software",
                    "market_cap": 3_000_000_000_000.0,
                    "avg_dollar_volume_20d": 1_000_000_000.0,
                    "has_options": True,
                    "is_active": True,
                    "updated_at": cutoff,
                    "metadata": {"type": "CS"},
                },
            ],
        )
        conn.execute(
            insert(daily_bars),
            {
                "ticker": "AAPL",
                "date": date(2026, 5, 21),
                "provider": "polygon",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1_000_000,
                "vwap": 100.25,
                "adjusted": True,
                "source_ts": cutoff,
                "available_at": cutoff,
            },
        )

    preview_exit = main(
        [
            "shadow-mode",
            "run",
            "--available-at",
            AVAILABLE_AT,
            "--preview",
            "--json",
        ]
    )

    assert preview_exit == 0
    preview = json.loads(capsys.readouterr().out)
    assert preview["external_calls_made"] == 0
    assert preview["db_writes_made"] == 0
    assert preview["run"]["missing_market_bar_count"] == 1
    freshness = preview["run"]["payload"]["discovery_snapshot"]["freshness"]
    assert freshness["as_of_daily_bar_date"] == "2026-05-21"
    assert freshness["active_security_with_as_of_bar_count"] == 1
    assert freshness["missing_as_of_daily_bar_count"] == 1
    discovery_blocker = next(
        row
        for row in preview["run"]["payload"]["discovery_snapshot"]["blockers"]
        if row["code"] == "incomplete_daily_bar_coverage"
    )
    assert "market-bars residual-review" in discovery_blocker["next_action"]
    assert "manual-bars" not in discovery_blocker["next_action"]
    latest_bars = next(
        row
        for row in preview["run"]["payload"]["shadow_readiness"]["checks"]
        if row["code"] == "latest_market_bars"
    )
    assert latest_bars["metric"]["missing_as_of_daily_bar_count"] == 1


def test_shadow_mode_classification_distinguishes_ready_partial_and_blocked() -> None:
    assert (
        classify_shadow_run_status(
            shadow_readiness_status="ready",
            scan_scope="full_scan",
            candidate_count=5,
            scanned_securities=500,
            blocker_count=0,
        )
        == "valid_full_scan"
    )
    assert (
        classify_shadow_run_status(
            shadow_readiness_status="ready",
            scan_scope="selected_universe",
            candidate_count=5,
            scanned_securities=10,
            blocker_count=0,
        )
        == "valid_selected_universe_scan"
    )
    assert (
        classify_shadow_run_status(
            shadow_readiness_status="blocked",
            scan_scope="full_scan",
            candidate_count=3,
            scanned_securities=10,
            blocker_count=1,
        )
        == "partial_scan"
    )
    assert (
        classify_shadow_run_status(
            shadow_readiness_status="setup_required",
            scan_scope="unknown",
            candidate_count=0,
            scanned_securities=0,
            blocker_count=2,
        )
        == "setup_required"
    )
    assert (
        classify_shadow_run_status(
            shadow_readiness_status="blocked",
            scan_scope="unknown",
            candidate_count=0,
            scanned_securities=0,
            blocker_count=1,
        )
        == "blocked_scan"
    )


def test_shadow_mode_run_persists_planned_provider_call_boundary() -> None:
    available_at = datetime.fromisoformat(AVAILABLE_AT)

    run = build_shadow_mode_run(
        {
            "shadow_readiness": {
                "status": "ready",
                "canonical_next_action": "do not use for valid runs",
                "blockers": [],
                "call_boundary": {
                    "planned_run_external_call_count_max": 3,
                },
                "snapshots": {
                    "scan_scope": {"mode": "full_scan"},
                },
                "checks": [
                    {
                        "code": "latest_market_bars",
                        "metric": {"missing_as_of_daily_bar_count": 0},
                    },
                    {"code": "validation_ready", "status": "ready"},
                ],
            },
            "discovery_snapshot": {
                "yield": {
                    "candidate_states": 2,
                    "requested_securities": 10,
                    "scanned_securities": 10,
                },
                "freshness": {
                    "active_security_count": 10,
                    "missing_as_of_daily_bar_count": 0,
                    "latest_daily_bar_date": "2026-05-22",
                },
            },
            "latest_run": {"as_of": "2026-05-22"},
            "candidate_rows": [
                {"state": ActionState.WARNING.value},
                {"state": ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW.value},
            ],
        },
        run_date=available_at.date(),
        as_of=None,
        available_at=available_at,
        db_writes_made=0,
    )

    assert run.provider_calls_planned == 3
    assert run.provider_calls_made == 0
    assert run.payload["call_plan_external_call_count_max"] == 3
    assert run.status == "valid_full_scan"
    assert _shadow_mode_next_action(run) == (
        "Record value-ledger entries for surfaced Warning or manual-review candidates."
    )


def test_shadow_mode_next_action_uses_shadow_readiness_canonical_action() -> None:
    available_at = datetime.fromisoformat(AVAILABLE_AT)
    run = build_shadow_mode_run(
        {
            "shadow_readiness": {
                "status": "setup_required",
                "canonical_next_action": (
                    "catalyst-radar market-bars residual-review "
                    "--expected-as-of 2026-05-15"
                ),
                "blockers": [{"code": "latest_market_bars"}],
                "call_boundary": {"planned_run_external_call_count_max": 0},
                "snapshots": {"scan_scope": {"mode": "unknown"}},
                "checks": [
                    {
                        "code": "latest_market_bars",
                        "metric": {"missing_as_of_daily_bar_count": 1},
                    },
                    {"code": "validation_ready", "status": "blocked"},
                ],
            },
            "discovery_snapshot": {
                "yield": {
                    "candidate_states": 0,
                    "requested_securities": 2,
                    "scanned_securities": 0,
                },
                "freshness": {
                    "active_security_count": 2,
                    "missing_as_of_daily_bar_count": 1,
                    "latest_daily_bar_date": "2026-05-15",
                },
            },
            "latest_run": {"as_of": "2026-05-15"},
            "candidate_rows": [],
        },
        run_date=available_at.date(),
        as_of=None,
        available_at=available_at,
        db_writes_made=0,
    )

    assert _shadow_mode_next_action(run) == (
        "catalyst-radar market-bars residual-review --expected-as-of 2026-05-15"
    )


def test_shadow_mode_status_prefers_current_readiness_action_over_stale_latest(
    tmp_path,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'shadow-status-stale.db').as_posix()}"
    engine = engine_from_url(database_url)
    create_schema(engine)
    available_at = datetime.fromisoformat(AVAILABLE_AT)
    stale_run = build_shadow_mode_run(
        {
            "shadow_readiness": {
                "status": "setup_required",
                "canonical_next_action": "old stale setup action",
                "blockers": [{"code": "latest_market_bars"}],
                "call_boundary": {"planned_run_external_call_count_max": 0},
                "snapshots": {"scan_scope": {"mode": "unknown"}},
                "checks": [
                    {
                        "code": "latest_market_bars",
                        "metric": {"missing_as_of_daily_bar_count": 1},
                    },
                    {"code": "validation_ready", "status": "blocked"},
                ],
            },
            "discovery_snapshot": {
                "yield": {
                    "candidate_states": 0,
                    "requested_securities": 2,
                    "scanned_securities": 0,
                },
                "freshness": {
                    "active_security_count": 2,
                    "missing_as_of_daily_bar_count": 1,
                    "latest_daily_bar_date": "2026-05-15",
                },
            },
            "latest_run": {"as_of": "2026-05-15"},
            "candidate_rows": [],
        },
        run_date=available_at.date(),
        as_of=None,
        available_at=available_at,
        db_writes_made=1,
    )
    ValidationRepository(engine).upsert_shadow_mode_run(stale_run)

    payload = shadow_mode_status_payload(
        engine,
        AppConfig(database_url=database_url),
        available_at=available_at,
        shadow_readiness={
            "status": "setup_required",
            "ready": False,
            "canonical_next_action": (
                "catalyst-radar market-bars residual-review "
                "--expected-as-of 2026-05-15"
            ),
        },
    )

    assert payload["status"] == "setup_required"
    assert payload["latest"]["id"] == stale_run.id
    assert payload["next_action"] == (
        "catalyst-radar market-bars residual-review --expected-as-of 2026-05-15"
    )
    assert payload["external_calls_made"] == 0
    assert payload["db_writes_made"] == 0


def test_shadow_mode_status_keeps_latest_action_when_current_readiness_is_ready(
    tmp_path,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'shadow-status-ready.db').as_posix()}"
    engine = engine_from_url(database_url)
    create_schema(engine)
    available_at = datetime.fromisoformat(AVAILABLE_AT)
    latest_run = build_shadow_mode_run(
        {
            "shadow_readiness": {
                "status": "ready",
                "canonical_next_action": "do not override valid run",
                "blockers": [],
                "call_boundary": {"planned_run_external_call_count_max": 3},
                "snapshots": {"scan_scope": {"mode": "full_scan"}},
                "checks": [
                    {
                        "code": "latest_market_bars",
                        "metric": {"missing_as_of_daily_bar_count": 0},
                    },
                    {"code": "validation_ready", "status": "ready"},
                ],
            },
            "discovery_snapshot": {
                "yield": {
                    "candidate_states": 1,
                    "requested_securities": 10,
                    "scanned_securities": 10,
                },
                "freshness": {
                    "active_security_count": 10,
                    "missing_as_of_daily_bar_count": 0,
                    "latest_daily_bar_date": "2026-05-22",
                },
            },
            "latest_run": {"as_of": "2026-05-22"},
            "candidate_rows": [{"state": ActionState.WARNING.value}],
        },
        run_date=available_at.date(),
        as_of=None,
        available_at=available_at,
        db_writes_made=1,
    )
    ValidationRepository(engine).upsert_shadow_mode_run(latest_run)

    payload = shadow_mode_status_payload(
        engine,
        AppConfig(database_url=database_url),
        available_at=available_at,
        shadow_readiness={
            "status": "ready",
            "ready": True,
            "canonical_next_action": "do not override valid run",
        },
    )

    assert payload["status"] == "valid_full_scan"
    assert payload["latest"]["id"] == latest_run.id
    assert payload["next_action"] == (
        "Record value-ledger entries for surfaced Warning or manual-review candidates."
    )
    assert payload["external_calls_made"] == 0
    assert payload["db_writes_made"] == 0


def test_shadow_mode_requires_timezone_aware_cutoff(tmp_path, monkeypatch) -> None:
    database_url = f"sqlite:///{(tmp_path / 'shadow-mode-naive.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = engine_from_url(database_url)
    create_schema(engine)

    assert datetime.fromisoformat(AVAILABLE_AT).tzinfo == UTC
    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "shadow-mode",
                "run",
                "--available-at",
                "2026-05-22T21:00:00",
                "--json",
            ]
        )

    assert excinfo.value.code == 2

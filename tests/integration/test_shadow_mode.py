from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select

from apps.api.main import create_app
from catalyst_radar.cli import main
from catalyst_radar.storage.db import create_schema, engine_from_url
from catalyst_radar.storage.schema import shadow_mode_runs
from catalyst_radar.validation.shadow_mode import classify_shadow_run_status

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
    assert executed["db_writes_made"] == 1
    latest_response = client.get("/api/radar/shadow/runs/latest")
    assert latest_response.status_code == 200
    latest = latest_response.json()
    assert latest["run"]["id"] == executed["run"]["id"]
    assert latest["external_calls_made"] == 0
    assert latest["db_writes_made"] == 0


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

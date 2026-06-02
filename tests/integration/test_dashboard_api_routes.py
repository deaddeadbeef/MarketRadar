from __future__ import annotations

from fastapi.testclient import TestClient

from apps.api.main import create_app
from catalyst_radar.api.routes import dashboard as dashboard_routes
from catalyst_radar.storage.db import create_schema, engine_from_url


def test_get_dashboard_snapshot_returns_fast_redacted_payload(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "dashboard-snapshot-api.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)
    captured: dict[str, object] = {}

    def fake_dashboard_snapshot_payload(**kwargs):
        captured["snapshot_kwargs"] = kwargs
        return {
            "schema_version": "dashboard-cli-snapshot-v1",
            "snapshot_mode": "fast_view",
            "status": "blocked",
            "next_action": "Import bars",
            "next_command": "bars import",
            "external_calls_made": 0,
        }

    monkeypatch.setattr(
        dashboard_routes,
        "dashboard_snapshot_payload",
        fake_dashboard_snapshot_payload,
    )
    client = TestClient(create_app())

    response = client.get(
        "/api/dashboard/snapshot",
        params=[
            ("page", "review"),
            ("ticker", "msft"),
            ("available_at", "2026-05-18T16:00:00+00:00"),
            ("source_gap", "options,local_text"),
            ("source_gap", "bars"),
            ("decision_gap", "decision_card"),
            ("stocks_only", "true"),
            ("scan_limit", "12"),
            ("scan_offset", "24"),
            ("telemetry_limit", "5"),
        ],
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["schema_version"] == "dashboard-cli-snapshot-v1"
    assert payload["snapshot_mode"] == "fast_view"
    assert payload["selected_page"] == "review"
    assert payload["external_calls_made"] == 0
    snapshot_kwargs = captured["snapshot_kwargs"]
    assert snapshot_kwargs["dotenv_loaded"] is True
    assert snapshot_kwargs["fast_view"] is True
    filters = snapshot_kwargs["filters"].normalized()
    assert filters.ticker == "MSFT"
    assert filters.available_at.isoformat() == "2026-05-18T16:00:00+00:00"
    assert filters.priced_in_status == "actionable"
    assert filters.priced_in_usefulness == "decision_useful"
    assert filters.priced_in_source_gap == ("options", "local_text", "market_bars")
    assert filters.priced_in_decision_gap == ("decision_card",)
    assert filters.priced_in_stocks_only is True
    assert filters.priced_in_limit == 12
    assert filters.priced_in_offset == 0
    assert filters.telemetry_limit == 5


def test_get_dashboard_snapshot_can_request_full_diagnostic_payload(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "dashboard-snapshot-full-api.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)
    captured: dict[str, object] = {}

    def fake_dashboard_snapshot_payload(**kwargs):
        captured["snapshot_kwargs"] = kwargs
        return {
            "schema_version": "dashboard-cli-snapshot-v1",
            "snapshot_mode": "full",
            "external_calls_made": 0,
        }

    monkeypatch.setattr(
        dashboard_routes,
        "dashboard_snapshot_payload",
        fake_dashboard_snapshot_payload,
    )
    client = TestClient(create_app())

    response = client.get("/api/dashboard/snapshot?fast=false")

    assert response.status_code == 200
    assert response.json()["snapshot_mode"] == "full"
    assert captured["snapshot_kwargs"]["fast_view"] is False



def test_get_dashboard_manifest_returns_desktop_automation_contract(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "dashboard-manifest-api.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)
    client = TestClient(create_app())

    response = client.get("/api/dashboard/manifest")

    assert response.status_code == 200
    payload = response.json()
    assert payload["schema_version"] == "dashboard-ui-manifest-v1"
    assert payload["external_calls_made"] == 0
    assert payload["surfaces"]["default"] == "tauri_desktop"
    assert any(page["key"] == "overview" for page in payload["pages"])
    assert "workflow-nav" in payload["automation"]["landmarks"]
    assert "command-input" in payload["automation"]["landmarks"]
    assert "automation-state" in payload["automation"]["landmarks"]
    assert "snapshot-json-output" in payload["automation"]["landmarks"]
    assert any(
        "command box" in shortcut
        for shortcut in payload["automation"]["keyboard_shortcuts"]
    )
    assert payload["automation"]["native_window_title"] == "MarketRadar Command Center"
    assert payload["automation"]["native_executable"].endswith(
        "radar-desktop.exe"
    )
    assert any(
        step["step"] == "guarded-command"
        for step in payload["automation"]["computer_use_steps"]
    )
    assert any(
        step["step"] == "json-command"
        and step["target"] == "snapshot-json-output"
        for step in payload["automation"]["computer_use_steps"]
    )
    assert any(
        "provider_calls=0" in assertion
        for assertion in payload["automation"]["zero_call_assertions"]
    )
    assert payload["data_contract"]["snapshot_command"].endswith("--json --fast")

def _database_url(tmp_path, filename: str) -> str:
    return f"sqlite:///{tmp_path / filename}"


def _create_database(database_url: str):
    engine = engine_from_url(database_url)
    create_schema(engine)
    return engine

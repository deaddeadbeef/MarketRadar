from __future__ import annotations

from dataclasses import dataclass

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


def test_get_dashboard_snapshot_canonicalizes_page_aliases(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "dashboard-page-alias-api.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)
    captured: list[dict[str, object]] = []

    def fake_dashboard_snapshot_payload(**kwargs):
        captured.append(kwargs)
        return {
            "schema_version": "dashboard-cli-snapshot-v1",
            "snapshot_mode": "fast_view",
            "external_calls_made": 0,
        }

    monkeypatch.setattr(
        dashboard_routes,
        "dashboard_snapshot_payload",
        fake_dashboard_snapshot_payload,
    )
    client = TestClient(create_app())

    run_response = client.get(
        "/api/dashboard/snapshot",
        params={"page": "safe-run", "scan_offset": "9"},
    )
    review_response = client.get(
        "/api/dashboard/snapshot",
        params={"page": "decision-ready", "scan_offset": "9"},
    )

    assert run_response.status_code == 200
    run_payload = run_response.json()
    assert run_payload["selected_page"] == "run"
    assert run_payload["external_calls_made"] == 0
    run_filters = captured[0]["filters"].normalized()
    assert run_filters.priced_in_status == "all"
    assert run_filters.priced_in_usefulness is None
    assert run_filters.priced_in_offset == 9

    assert review_response.status_code == 200
    review_payload = review_response.json()
    assert review_payload["selected_page"] == "review"
    assert review_payload["external_calls_made"] == 0
    review_filters = captured[1]["filters"].normalized()
    assert review_filters.priced_in_status == "actionable"
    assert review_filters.priced_in_usefulness == "decision_useful"
    assert review_filters.priced_in_offset == 0


def test_get_dashboard_snapshot_preserves_candidate_detail_refresh(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "dashboard-candidate-detail-api.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)
    captured: dict[str, object] = {}

    def fake_dashboard_snapshot_payload(**kwargs):
        captured["snapshot_kwargs"] = kwargs
        return {
            "schema_version": "dashboard-cli-snapshot-v1",
            "snapshot_mode": "fast_view",
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
        params={
            "page": "candidate:msft",
            "ticker": "aapl",
            "scan_offset": "40",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["selected_page"] == "candidate:MSFT"
    assert payload["external_calls_made"] == 0
    filters = captured["snapshot_kwargs"]["filters"].normalized()
    assert filters.ticker == "MSFT"
    assert filters.priced_in_offset == 0


def test_get_dashboard_snapshot_preserves_alert_detail_refresh(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "dashboard-alert-detail-api.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)
    captured: dict[str, object] = {}

    def fake_dashboard_snapshot_payload(**kwargs):
        captured["snapshot_kwargs"] = kwargs
        return {
            "schema_version": "dashboard-cli-snapshot-v1",
            "snapshot_mode": "fast_view",
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
        params={
            "page": "alert:demo-alert-1",
            "scan_offset": "12",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["selected_page"] == "alert:demo-alert-1"
    assert payload["external_calls_made"] == 0
    filters = captured["snapshot_kwargs"]["filters"].normalized()
    assert filters.ticker is None
    assert filters.priced_in_offset == 12


def test_post_dashboard_command_reuses_cli_dispatcher_contract(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "dashboard-command-api.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)
    captured: dict[str, object] = {}

    def fake_dashboard_snapshot_payload(**kwargs):
        captured["snapshot_kwargs"] = kwargs
        return {
            "schema_version": "dashboard-cli-snapshot-v1",
            "snapshot_mode": "fast_view",
            "external_calls_made": 0,
        }

    @dataclass(frozen=True)
    class FakeUpdate:
        page: str
        filters: dashboard_routes.DashboardFilters
        exit_requested: bool = False
        message: str = "Local only: action saved; db_writes=1."

    def fake_apply_dashboard_command(raw, payload, page, filters, *, engine, config):
        captured["command"] = raw
        captured["payload"] = payload
        captured["page"] = page
        captured["filters"] = filters
        captured["engine"] = engine
        captured["config"] = config
        return FakeUpdate(page="broker", filters=filters.normalized())

    monkeypatch.setattr(
        dashboard_routes,
        "dashboard_snapshot_payload",
        fake_dashboard_snapshot_payload,
    )
    monkeypatch.setattr(
        dashboard_routes,
        "apply_dashboard_command",
        fake_apply_dashboard_command,
    )
    client = TestClient(create_app())

    response = client.post(
        "/api/dashboard/command",
        json={
            "command": "action msft watch codex smoke",
            "page": "broker",
            "ticker": "msft",
            "available_at": "2026-05-18T16:00:00+00:00",
            "source_gap": ["options"],
            "decision_gap": ["decision_card"],
            "stocks_only": True,
            "scan_limit": 12,
            "scan_offset": 24,
            "telemetry_limit": 5,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["schema_version"] == "dashboard-command-result-v1"
    assert payload["command"] == "action msft watch codex smoke"
    assert payload["page"] == "broker"
    assert payload["exit_requested"] is False
    assert payload["message"] == "Local only: action saved; db_writes=1."
    assert payload["snapshot"]["selected_page"] == "broker"
    assert payload["snapshot"]["external_calls_made"] == 0
    assert payload["filters"]["ticker"] == "MSFT"
    assert payload["filters"]["priced_in_source_gap"] == ["options"]
    assert payload["filters"]["priced_in_decision_gap"] == ["decision_card"]
    assert payload["filters"]["priced_in_stocks_only"] is True
    assert payload["filters"]["priced_in_limit"] == 12
    assert payload["filters"]["priced_in_offset"] == 24
    assert payload["filters"]["telemetry_limit"] == 5
    assert captured["command"] == "action msft watch codex smoke"
    assert captured["page"] == "broker"
    filters = captured["filters"].normalized()
    assert filters.ticker == "MSFT"
    assert filters.available_at.isoformat() == "2026-05-18T16:00:00+00:00"
    assert filters.priced_in_source_gap == ("options",)
    assert filters.priced_in_decision_gap == ("decision_card",)


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
    assert all(
        page["test_id"] == f"nav-page-{page['key']}"
        and page["description"]
        for page in payload["pages"]
    )
    assert any(
        page["key"] == "costs" and page["label"] == "Costs" and page["shortcut"] == "V"
        for page in payload["pages"]
    )
    assert any(
        page["key"] == "themes" and page["shortcut"] == "theme"
        for page in payload["pages"]
    )
    assert any(
        page["key"] == "validation" and page["shortcut"] == "valid"
        for page in payload["pages"]
    )
    assert any(page["key"] == "costs" and page["shortcut"] == "V" for page in payload["pages"])
    assert "workflow-nav" in payload["automation"]["landmarks"]
    assert "command-input" in payload["automation"]["landmarks"]
    assert "automation-state" in payload["automation"]["landmarks"]
    assert "keys-panel" in payload["automation"]["landmarks"]
    assert "keys-list" in payload["automation"]["landmarks"]
    assert "snapshot-panel" in payload["automation"]["landmarks"]
    assert "snapshot-source" in payload["automation"]["landmarks"]
    assert "snapshot-refresh" in payload["automation"]["landmarks"]
    assert "snapshot-page" in payload["automation"]["landmarks"]
    assert "snapshot-mode" in payload["automation"]["landmarks"]
    assert "snapshot-json-output" in payload["automation"]["landmarks"]
    assert any(
        "data-current-page" in note and "data-current-nav-page" in note
        for note in payload["automation"]["notes"]
    )
    assert any(
        "nav-page-candidates" in note and "nav-page-alerts" in note
        for note in payload["automation"]["notes"]
    )
    assert any(
        "command box" in shortcut
        for shortcut in payload["automation"]["keyboard_shortcuts"]
    )
    assert any(
        "Costs" in shortcut
        for shortcut in payload["automation"]["keyboard_shortcuts"]
    )
    assert any(
        "Ctrl+N moves forward; Ctrl+P moves backward" in shortcut
        for shortcut in payload["automation"]["keyboard_shortcuts"]
    )
    assert any(
        "ArrowRight/ArrowDown/Tab/J moves forward" in shortcut
        for shortcut in payload["automation"]["keyboard_shortcuts"]
    )
    assert any(
        "ArrowLeft/ArrowUp/Shift+Tab/K moves backward" in shortcut
        for shortcut in payload["automation"]["keyboard_shortcuts"]
    )
    assert any(
        "F5 or R refreshes the local snapshot" in shortcut
        for shortcut in payload["automation"]["keyboard_shortcuts"]
    )
    assert any(
        "Q closes the native desktop window" in shortcut
        for shortcut in payload["automation"]["keyboard_shortcuts"]
    )
    assert any(
        "themes or validation" in shortcut
        for shortcut in payload["automation"]["keyboard_shortcuts"]
    )
    assert any(
        "source-gap and decision-gap commands reject unsupported values" in shortcut
        for shortcut in payload["automation"]["keyboard_shortcuts"]
    )
    assert any(
        "next and prev page through scan rows" in shortcut
        for shortcut in payload["automation"]["keyboard_shortcuts"]
    )
    assert any(
        "clear-filters resets filters while preserving the row limit" in shortcut
        for shortcut in payload["automation"]["keyboard_shortcuts"]
    )
    assert any(
        "usefulness clears with all, any, none, or blank" in shortcut
        for shortcut in payload["automation"]["keyboard_shortcuts"]
    )
    assert any(
        "offset, limit, and available-at commands reject invalid values" in shortcut
        for shortcut in payload["automation"]["keyboard_shortcuts"]
    )
    assert any(
        "batch SOURCE opens an Ops source plan" in shortcut
        for shortcut in payload["automation"]["keyboard_shortcuts"]
    )
    assert any(
        "run execute starts the guarded" in shortcut
        for shortcut in payload["automation"]["keyboard_shortcuts"]
    )
    assert any(
        "action, trigger, ticket, feedback, ledger, and outcome" in shortcut
        and "guarded dashboard backend" in shortcut
        for shortcut in payload["automation"]["keyboard_shortcuts"]
    )
    assert any(
        "agent, bars, options, and cik/sec planning commands" in shortcut
        and "preview/status output" in shortcut
        and "execute and confirm variants" in shortcut
        for shortcut in payload["automation"]["keyboard_shortcuts"]
    )
    command_catalog = payload["automation"]["command_box_commands"]
    assert len(command_catalog) >= 20
    assert all(
        {"command", "meaning", "safety", "route"} <= set(command)
        for command in command_catalog
    )
    assert any(
        command["command"] == "bars saved capture/validate/import"
        and command["safety"] == "preview_only_confirm_execute_external"
        and command["route"] == "dashboard_backend"
        and "confirm/execute stays external" in command["meaning"]
        for command in command_catalog
    )
    assert any(
        command["command"] == "action / trigger / ticket / feedback"
        and command["safety"] == "local_db_only"
        and command["route"] == "dashboard_backend"
        for command in command_catalog
    )
    assert any(
        command["command"] == "catalyst-radar COMMAND"
        and command["safety"] == "external_boundary"
        and command["route"] == "powershell_boundary"
        for command in command_catalog
    )
    assert any(
        command["command"] == "json"
        and command["safety"] == "zero_provider_calls"
        and command["route"] == "local_snapshot_view"
        for command in command_catalog
    )
    assert any(
        "catalyst-radar commands" in shortcut
        for shortcut in payload["automation"]["keyboard_shortcuts"]
    )
    assert any(
        "Home opens Start, End opens Help" in shortcut
        for shortcut in payload["automation"]["keyboard_shortcuts"]
    )
    assert any(
        "keys-panel" in note
        and "snapshot-panel" in note
        and "snapshot-source" in note
        and "snapshot-page" in note
        for note in payload["automation"]["notes"]
    )
    assert payload["automation"]["native_window_title"] == "MarketRadar Command Center"
    assert payload["automation"]["native_executable"].endswith(
        "radar-desktop.exe"
    )
    assert any(
        step["step"] == "guarded-command"
        and "source-specific Ops plan" in step["expected"]
        for step in payload["automation"]["computer_use_steps"]
    )
    assert any(
        step["step"] == "source-batch-execute-boundary"
        and "--execute-batches 3" in step["expected"]
        for step in payload["automation"]["computer_use_steps"]
    )
    assert any(
        step["step"] == "safe-run-execute-command"
        and "radar_run telemetry contract" in step["expected"]
        for step in payload["automation"]["computer_use_steps"]
    )
    assert any(
        step["step"] == "local-dashboard-command"
        and "Local only" in step["expected"]
        and "db_writes=1" in step["expected"]
        and "no provider" in step["expected"]
        for step in payload["automation"]["computer_use_steps"]
    )
    assert any(
        step["step"] == "provider-preview-command"
        and "Market-bar status" in step["expected"]
        and "dashboard backend" in step["expected"]
        and "provider_calls=0" in step["expected"]
        for step in payload["automation"]["computer_use_steps"]
    )
    assert any(
        step["step"] == "filter-validation-command"
        and "Unsupported source-gap value" in step["expected"]
        and "filter is unchanged" in step["expected"]
        for step in payload["automation"]["computer_use_steps"]
    )
    assert any(
        step["step"] == "numeric-validation-command"
        and "Usage: limit 1-200" in step["expected"]
        and "scan limit is unchanged" in step["expected"]
        for step in payload["automation"]["computer_use_steps"]
    )
    assert any(
        step["step"] == "time-validation-command"
        and "Invalid timestamp" in step["expected"]
        and "available_at is unchanged" in step["expected"]
        for step in payload["automation"]["computer_use_steps"]
    )
    assert any(
        step["step"] == "pagination-boundary-command"
        and "Already at the end" in step["expected"]
        for step in payload["automation"]["computer_use_steps"]
    )
    assert any(
        step["step"] == "clear-filters-command"
        and "filter-limit remains 25" in step["expected"]
        and "scan_offset returns to 0" in step["expected"]
        for step in payload["automation"]["computer_use_steps"]
    )
    assert any(
        step["step"] == "optional-filter-clear-command"
        and "Usefulness filter cleared" in step["expected"]
        for step in payload["automation"]["computer_use_steps"]
    )
    assert any(
        step["step"] == "powershell-command"
        for step in payload["automation"]["computer_use_steps"]
    )
    assert any(
        step["step"] == "capture"
        and "keys-panel" in step["expected"]
        and "snapshot-panel" in step["expected"]
        and "snapshot-page=<PAGE>" in step["expected"]
        for step in payload["automation"]["computer_use_steps"]
    )
    assert any(
        step["step"] == "row-open"
        and step["target"] == "queue-row"
        and "candidate:<TICKER>" in step["expected"]
        and "nav=candidates" in step["expected"]
        and "nav=alerts" in step["expected"]
        for step in payload["automation"]["computer_use_steps"]
    )
    assert any(
        step["step"] == "json-command"
        and step["target"] == "snapshot-json-output"
        for step in payload["automation"]["computer_use_steps"]
    )
    assert any(
        step["step"] == "close-command"
        and step["target"] == "command-input"
        and "window closes" in step["expected"]
        for step in payload["automation"]["computer_use_steps"]
    )
    assert any(
        "provider_calls=0" in assertion
        for assertion in payload["automation"]["zero_call_assertions"]
    )
    assert any(
        "Full catalyst-radar commands" in assertion
        for assertion in payload["automation"]["zero_call_assertions"]
    )
    assert any(
        "Source batch plan commands" in assertion
        and "provider_calls=0" in assertion
        for assertion in payload["automation"]["zero_call_assertions"]
    )
    assert any(
        "Local broker, feedback, value-ledger, and outcome commands" in assertion
        and "guarded dashboard backend" in assertion
        and "provider, OpenAI, broker, order, or external calls" in assertion
        for assertion in payload["automation"]["zero_call_assertions"]
    )
    assert any(
        "Agent, market-bar, options, and SEC CIK preview/status commands" in assertion
        and "execute or confirm variants" in assertion
        and "external PowerShell boundaries" in assertion
        for assertion in payload["automation"]["zero_call_assertions"]
    )
    assert any(
        "Invalid source-gap or decision-gap" in assertion
        and "must not refresh" in assertion
        for assertion in payload["automation"]["zero_call_assertions"]
    )
    assert any(
        "Invalid offset, limit, or available-at" in assertion
        and "must not refresh" in assertion
        for assertion in payload["automation"]["zero_call_assertions"]
    )
    assert any(
        "Pagination commands" in assertion
        and "priced_in_queue.total_count" in assertion
        for assertion in payload["automation"]["zero_call_assertions"]
    )
    assert any(
        "clear-filters must preserve the chosen row limit" in assertion
        and "clearing ticker" in assertion
        for assertion in payload["automation"]["zero_call_assertions"]
    )
    assert any(
        "Optional usefulness filters" in assertion
        and "alert-status and alert-route clear" in assertion
        for assertion in payload["automation"]["zero_call_assertions"]
    )
    assert any(
        "queue rows" in assertion and "candidate/alert detail" in assertion
        for assertion in payload["automation"]["zero_call_assertions"]
    )
    assert any(
        "Dynamic detail pages" in assertion
        and "nav=<parent workflow page>" in assertion
        for assertion in payload["automation"]["zero_call_assertions"]
    )
    assert any(
        "q, quit, and exit close" in assertion
        for assertion in payload["automation"]["zero_call_assertions"]
    )
    assert any(
        "Local broker, feedback, value-ledger, and outcome commands" in note
        and "provider preview/status commands use the guarded dashboard backend" in note
        and "run execute uses the guarded radar-run" in note
        for note in payload["automation"]["notes"]
    )
    assert payload["data_contract"]["snapshot_command"].endswith("--json --fast")


def _database_url(tmp_path, filename: str) -> str:
    return f"sqlite:///{tmp_path / filename}"


def _create_database(database_url: str):
    engine = engine_from_url(database_url)
    create_schema(engine)
    return engine

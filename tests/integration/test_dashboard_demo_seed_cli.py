from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

from sqlalchemy import create_engine

from catalyst_radar.cli import main
from catalyst_radar.core.config import AppConfig
from catalyst_radar.dashboard.data import (
    load_alert_rows,
    load_candidate_rows,
    load_cost_summary,
    load_ipo_s1_rows,
    load_ops_health,
    load_ticker_detail,
    load_validation_summary,
    radar_readiness_payload,
)
from catalyst_radar.dashboard.demo_seed import DEMO_AVAILABLE_AT
from catalyst_radar.dashboard.tui import DashboardFilters, run_dashboard_tui


def test_seed_dashboard_demo_populates_command_center_layers(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'demo.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    assert main(["seed-dashboard-demo"]) == 0
    first_output = capsys.readouterr()
    assert "seeded dashboard demo ticker=ACME sec_events=1" in first_output.out
    assert first_output.err == ""

    assert main(["seed-dashboard-demo"]) == 0
    second_output = capsys.readouterr()
    assert "candidate_state=demo-state-acme" in second_output.out
    assert second_output.err == ""

    engine = create_engine(database_url, future=True)
    cutoff = DEMO_AVAILABLE_AT + timedelta(minutes=1)

    candidate_rows = load_candidate_rows(engine)
    assert [row["ticker"] for row in candidate_rows] == ["ACME"]
    assert candidate_rows[0]["state"] == "Warning"
    assert candidate_rows[0]["top_event_type"] == "financing"

    ipo_rows = load_ipo_s1_rows(engine, ticker="ACME", available_at=cutoff)
    assert [row["ticker"] for row in ipo_rows] == ["ACME"]
    assert ipo_rows[0]["proposed_ticker"] == "ACME"
    assert ipo_rows[0]["estimated_gross_proceeds"] == 225_000_000.0

    alert_rows = load_alert_rows(engine, ticker="ACME", available_at=cutoff)
    assert [row["id"] for row in alert_rows] == ["demo-alert-acme"]
    assert alert_rows[0]["feedback_label"] == "useful"

    ticker_detail = load_ticker_detail(engine, "ACME", available_at=cutoff)
    assert ticker_detail is not None
    assert ticker_detail["latest_candidate"]["candidate_packet_id"] == "demo-packet-acme"
    assert ticker_detail["events"][0]["payload"]["ipo_analysis"]["risk_flags"]

    validation_summary = load_validation_summary(engine)
    assert validation_summary["latest_run"]["id"] == "demo-validation-run-acme"
    assert validation_summary["report"]["candidate_count"] == 1

    cost_summary = load_cost_summary(engine, available_at=cutoff)
    assert cost_summary["attempt_count"] == 1
    assert cost_summary["total_actual_cost_usd"] == 0.03

    ops_health = load_ops_health(engine)
    assert "sec" in {row["provider"] for row in ops_health["providers"]}


def test_dashboard_snapshot_cli_outputs_dashboard_command_center_json(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'demo.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    cutoff = (DEMO_AVAILABLE_AT + timedelta(minutes=1)).isoformat()

    assert main(["seed-dashboard-demo"]) == 0
    capsys.readouterr()

    assert (
        main(
            [
                "dashboard-snapshot",
                "--ticker",
                "ACME",
                "--available-at",
                cutoff,
                "--json",
            ]
        )
        == 0
    )
    output = capsys.readouterr()
    assert output.err == ""

    payload = json.loads(output.out)
    assert payload["schema_version"] == "dashboard-cli-snapshot-v1"
    assert payload["feature_inventory"]
    assert payload["external_calls_made"] == 0
    assert payload["controls"]["ticker"] == "ACME"
    assert payload["readiness"]["schema_version"] == "radar-readiness-v1"
    assert payload["live_activation"]["schema_version"] == (
        "live-data-activation-contract-v1"
    )
    assert payload["call_plan"]["schema_version"] == "radar-run-call-plan-v1"
    assert payload["telemetry_coverage"]["schema_version"] == (
        "ops-telemetry-coverage-v1"
    )
    assert payload["candidates"]["count"] == 1
    assert payload["candidates"]["rows"][0]["ticker"] == "ACME"
    assert payload["alerts"]["count"] == 1
    assert payload["ipo_s1"]["count"] == 1
    assert payload["validation"]["latest_run"]["id"] == "demo-validation-run-acme"
    assert payload["costs"]["attempt_count"] == 1
    assert payload["ops_health"]["database"]["candidate_state_count"] == 1

    engine = create_engine(database_url, future=True)
    direct_readiness = radar_readiness_payload(engine, AppConfig.from_env())
    assert payload["readiness"]["status"] == direct_readiness["status"]
    assert payload["readiness"]["market_radar_usefulness"]["status"] == (
        direct_readiness["market_radar_usefulness"]["status"]
    )


def test_dashboard_snapshot_cli_outputs_human_readable_zero_call_summary(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'demo.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    cutoff = (DEMO_AVAILABLE_AT + timedelta(minutes=1)).isoformat()

    assert main(["seed-dashboard-demo"]) == 0
    capsys.readouterr()

    assert (
        main(
            [
                "dashboard-snapshot",
                "--ticker",
                "ACME",
                "--available-at",
                cutoff,
            ]
        )
        == 0
    )
    output = capsys.readouterr()
    assert output.err == ""
    for expected in (
        "Market Radar Terminal Dashboard",
        "Page: overview",
        "DB:",
        "Ticker: ACME",
        "Readiness",
        "Usefulness",
        "Dashboard Rows",
        "Operator next",
        "Market freshness",
        "Call plan",
        "External calls made: 0",
    ):
        assert expected in output.out


def test_dashboard_tui_supports_interactive_navigation_and_filters(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'demo.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    assert main(["seed-dashboard-demo"]) == 0
    capsys.readouterr()

    engine = create_engine(database_url, future=True)
    config = AppConfig.from_env()
    inputs = iter(
        [
            "candidates",
            "open 1",
            "ticker ACME",
            "features",
            "json",
            "run",
            "action ACME watch terminal-note",
            "trigger ACME price_above gte 100",
            "eval-triggers ACME",
            "ticket ACME buy 10 8",
            "feedback 1 acted terminal-feedback",
            "q",
        ]
    )
    outputs: list[str] = []

    assert (
        run_dashboard_tui(
            engine=engine,
            config=config,
            dotenv_loaded=False,
            filters=DashboardFilters(),
            input_fn=lambda _prompt: next(inputs),
            output_fn=outputs.append,
            clear_screen=False,
        )
        == 0
    )

    rendered = "\n".join(outputs)
    assert "Market Radar Terminal Dashboard" in rendered
    assert "Page: candidates" in rendered
    assert "Candidate ACME" in rendered
    assert "Current Market Radar Features" in rendered
    assert '"schema_version": "dashboard-cli-snapshot-v1"' in rendered
    assert "Run is guarded. Review the call plan" in rendered
    assert "Saved action: ACME watch active" in rendered
    assert "Saved trigger: ACME price_above" in rendered
    assert "Evaluated 1 trigger(s)" in rendered
    assert "Saved blocked order ticket: ACME BUY submission_allowed=False" in rendered
    assert "Saved alert feedback: demo-alert-acme ACME acted" in rendered

from __future__ import annotations

import asyncio
import html
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
from catalyst_radar.dashboard.tui import (
    DashboardFilters,
    MarketRadarDashboardApp,
    run_dashboard_tui,
)


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
        "Full-market priced-in queue - select a row to act",
        "UNIVERSE",
        "ACME",
        "Bullish not priced",
        "emotion",
        "reaction",
        "candidate rows are priced-in mismatch cards",
        "External calls made: 0",
    ):
        assert expected in output.out


def test_agent_brief_cli_outputs_zero_call_dry_run(
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
                "agent-brief",
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

    assert payload["schema_version"] == "market-radar-agent-brief-v1"
    assert payload["mode"] == "dry_run"
    assert payload["status"] == "dry_run"
    assert payload["external_calls_made"] == {
        "broker": 0,
        "market_data": 0,
        "openai": 0,
    }
    assert [agent["agent"] for agent in payload["agents"]] == [
        "Data Sentinel",
        "Catalyst Analyst",
        "Risk Officer",
        "Operator",
    ]
    assert payload["next_actions"]


def test_agent_brief_cli_real_mode_blocks_without_explicit_gates(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'demo.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("CATALYST_ENABLE_AGENT_SDK", "false")
    monkeypatch.setenv("CATALYST_ENABLE_PREMIUM_LLM", "false")
    monkeypatch.setenv("CATALYST_LLM_PROVIDER", "none")
    monkeypatch.setenv("CATALYST_AGENT_SDK_MODEL", "")
    monkeypatch.setenv("OPENAI_API_KEY", "")

    assert main(["seed-dashboard-demo"]) == 0
    capsys.readouterr()

    assert main(["agent-brief", "--real", "--json"]) == 2
    output = capsys.readouterr()
    assert output.err == ""
    payload = json.loads(output.out)
    assert payload["mode"] == "blocked"
    assert payload["status"] == "blocked"
    assert payload["external_calls_made"]["openai"] == 0
    assert "OpenAI real-mode gate" in {
        item["name"] for item in payload["security_checks"]
    }


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


def test_modern_dashboard_tui_supports_mouse_navigation(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'demo.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    assert main(["seed-dashboard-demo"]) == 0
    capsys.readouterr()

    engine = create_engine(database_url, future=True)
    app = MarketRadarDashboardApp(
        engine=engine,
        config=AppConfig.from_env(),
        dotenv_loaded=False,
        filters=DashboardFilters(),
        initial_page="tutorial",
    )

    async def run_app() -> None:
        async with app.run_test(size=(150, 44)) as pilot:
            await pilot.pause()
            frame = html.unescape(app.export_screenshot()).replace("\xa0", " ")
            assert "MRDR // MARKET RADAR" in frame
            assert "TUTORIAL" in frame
            assert "Tutorial - your first 90 seconds" in frame
            assert "Press 1 or click Insights" in frame
            assert "0  Tutorial" in frame
            assert "LEARN" in frame
            assert app.page == "tutorial"

            await pilot.press("1")
            await pilot.pause()
            assert app.page == "overview"
            frame = html.unescape(app.export_screenshot()).replace("\xa0", " ")
            assert "INSIGHTS" in frame
            assert "Full-market priced-in queue - select a row to act" in frame
            assert "UNIVERSE" in frame
            assert "ACME" in frame
            assert "Bullish not priced" in frame
            assert "candidate rows are priced-in mismatch cards" in frame
            assert "Candidates [1]" in frame
            assert "FRESH BARS" in frame
            assert "No - research only" in frame
            assert "KEYS" in frame
            assert "MOUSE" in frame
            assert "NEXT ACTION" in frame
            assert "LAST RESPONSE" in frame
            assert "CORE" in frame
            assert "REVIEW" in frame
            assert "OPERATE" in frame
            assert "Up/Down on sidebar" in frame

            app.query_one("#data-table").focus()
            await pilot.press("down")
            await pilot.press("enter")
            await pilot.pause()
            assert app.page == "candidate:ACME"
            frame = html.unescape(app.export_screenshot()).replace("\xa0", " ")
            assert "Opened insight for ACME" in frame
            assert ">> 4  Candidates [1]" in frame

            assert await pilot.click("#nav-alerts")
            await pilot.pause()
            assert app.page == "alerts"
            frame = html.unescape(app.export_screenshot()).replace("\xa0", " ")
            assert ">> 5  Alerts [1]" in frame

            await pilot.press("ctrl+p")
            await pilot.pause()
            assert app.page == "candidates"

            assert await pilot.click("#nav-candidates")
            await pilot.pause()
            assert app.page == "candidates"
            frame = html.unescape(app.export_screenshot()).replace("\xa0", " ")
            assert ">> 4  Candidates [1]" in frame

            app.query_one("#nav-help").focus()
            await pilot.press("up")
            await pilot.pause()
            assert app.focused is not None
            assert app.focused.id == "nav-features"
            await pilot.press("down")
            await pilot.pause()
            assert app.focused is not None
            assert app.focused.id == "nav-help"
            await pilot.press("enter")
            await pilot.pause()
            assert app.page == "help"
            frame = html.unescape(app.export_screenshot()).replace("\xa0", " ")
            assert ">> ?  Help" in frame
            assert "Click sidebar" in frame

            app.query_one("#action-refresh").focus()
            await pilot.press("enter")
            await pilot.pause()
            frame = html.unescape(app.export_screenshot()).replace("\xa0", " ")
            assert "LAST RESPONSE" in frame
            assert "Snapshot refreshed from the local database." in frame

    asyncio.run(run_app())

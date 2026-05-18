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
    _apply_command,
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
    assert payload["controls"]["priced_in_status"] == "all"
    assert payload["priced_in_queue"]["filters"]["status"] == "all"
    assert payload["controls"]["priced_in_usefulness"] is None
    assert payload["controls"]["priced_in_decision_gap"] == []
    assert payload["readiness"]["schema_version"] == "radar-readiness-v1"
    assert payload["live_activation"]["schema_version"] == (
        "live-data-activation-contract-v1"
    )
    assert payload["call_plan"]["schema_version"] == "radar-run-call-plan-v1"
    assert payload["priced_in_preflight"]["schema_version"] == "priced-in-preflight-v1"
    assert payload["priced_in_source_coverage"]["schema_version"] == (
        "priced-in-source-coverage-v1"
    )
    source_actions = {
        row["source"]: row for row in payload["priced_in_source_coverage"]["actions"]
    }
    assert source_actions["options"]["status"] == "missing"
    assert source_actions["options"]["command"].startswith(
        "catalyst-radar schwab-market-sync"
    )
    assert source_actions["options"]["command"].endswith("--ticker ACME")
    assert source_actions["options"]["sample_tickers"] == ["ACME"]
    assert source_actions["options"]["api_payload"] == {
        "tickers": ["ACME"],
        "include_history": True,
        "include_options": True,
    }
    assert source_actions["broker_context"]["api"] == (
        "POST /api/brokers/schwab/market-sync"
    )
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

    assert (
        main(
            [
                "dashboard-snapshot",
                "--usefulness",
                "research_useful",
                "--source-gap",
                "options",
                "--decision-gap",
                "decision_card",
                "--scan-limit",
                "1",
                "--scan-offset",
                "1",
                "--json",
            ]
        )
        == 0
    )
    output = capsys.readouterr()
    gap_payload = json.loads(output.out)

    assert output.err == ""
    assert gap_payload["controls"]["priced_in_usefulness"] == "research_useful"
    assert gap_payload["priced_in_queue"]["filters"]["usefulness"] == "research_useful"
    assert gap_payload["controls"]["priced_in_source_gap"] == ["options"]
    assert gap_payload["priced_in_queue"]["filters"]["source_gap"] == ["options"]
    assert gap_payload["controls"]["priced_in_decision_gap"] == ["decision_card"]
    assert gap_payload["priced_in_queue"]["filters"]["decision_gap"] == [
        "decision_card"
    ]
    assert gap_payload["controls"]["priced_in_limit"] == 1
    assert gap_payload["controls"]["priced_in_offset"] == 1
    assert gap_payload["priced_in_queue"]["filters"]["limit"] == 1
    assert gap_payload["priced_in_queue"]["filters"]["offset"] == 1

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
        "Full-market priced-in queue - showing",
        "UNIVERSE",
        "ACME",
        "Bullish not priced",
        "emotion",
        "reaction",
        "candidate rows are priced-in mismatch cards",
        "External calls made: 0",
    ):
        assert expected in output.out


def test_dashboard_snapshot_ops_page_shows_priced_in_source_actions(
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
                "--page",
                "ops",
            ]
        )
        == 0
    )
    output = capsys.readouterr()

    assert output.err == ""
    assert "Priced-in Source Gaps" in output.out
    assert "options" in output.out
    assert "schwab-market-sync" in output.out
    assert "ACME" in output.out


def test_dashboard_tui_once_can_show_full_scan_mode(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'demo.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    assert main(["seed-dashboard-demo"]) == 0
    capsys.readouterr()

    assert main(["dashboard-tui", "--once", "--scan-mode", "all", "--page", "overview"]) == 0
    output = capsys.readouterr()

    assert output.err == ""
    assert "Full-market priced-in queue - showing" in output.out
    assert "Evidence gaps" in output.out
    assert "First row is scan coverage" in output.out

    assert (
        main(
            [
                "dashboard-tui",
                "--once",
                "--scan-mode",
                "all",
                "--source-gap",
                "options",
                "--page",
                "overview",
            ]
        )
        == 0
    )
    output = capsys.readouterr()

    assert output.err == ""
    assert "source gaps options" in output.out
    assert "Active source gap filter: source gaps options." in output.out


def test_dashboard_scan_commands_page_full_scan_rows(tmp_path: Path, monkeypatch) -> None:
    database_url = f"sqlite:///{(tmp_path / 'demo.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = create_engine(database_url, future=True)
    config = AppConfig.from_env()
    payload = {
        "priced_in_queue": {
            "count": 50,
            "total_count": 120,
            "offset": 0,
            "filters": {"limit": 50},
        }
    }
    filters = DashboardFilters(priced_in_limit=50, priced_in_offset=0)

    next_update = _apply_command(
        "next",
        payload,
        "overview",
        filters,
        engine=engine,
        config=config,
    )
    assert next_update.page == "overview"
    assert next_update.filters.priced_in_offset == 50
    assert next_update.message == "Showing full-scan rows starting at 51."

    prev_update = _apply_command(
        "prev",
        payload,
        "overview",
        DashboardFilters(priced_in_limit=50, priced_in_offset=50),
        engine=engine,
        config=config,
    )
    assert prev_update.filters.priced_in_offset == 0

    offset_update = _apply_command(
        "offset 101",
        payload,
        "overview",
        filters,
        engine=engine,
        config=config,
    )
    assert offset_update.filters.priced_in_offset == 100

    limit_update = _apply_command(
        "limit 250",
        payload,
        "overview",
        DashboardFilters(priced_in_limit=50, priced_in_offset=50),
        engine=engine,
        config=config,
    )
    assert limit_update.filters.priced_in_limit == 200
    assert limit_update.filters.priced_in_offset == 0

    source_gap_update = _apply_command(
        "source-gap options,text",
        payload,
        "overview",
        DashboardFilters(priced_in_offset=50),
        engine=engine,
        config=config,
    )
    assert source_gap_update.filters.priced_in_source_gap == (
        "options",
        "local_text",
    )
    assert source_gap_update.filters.priced_in_offset == 0
    assert source_gap_update.message == "Source-gap filter: options, local_text."


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


def test_priced_in_queue_cli_outputs_same_zero_call_signal(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'demo.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    cutoff = (DEMO_AVAILABLE_AT + timedelta(minutes=1)).isoformat()

    assert main(["seed-dashboard-demo"]) == 0
    capsys.readouterr()

    assert main(["priced-in-queue", "--json"]) == 0
    output = capsys.readouterr()
    payload = json.loads(output.out)

    assert output.err == ""
    assert payload["schema_version"] == "priced-in-queue-v1"
    assert payload["external_calls_made"] == 0
    assert payload["usefulness_counts"] == {"research_useful": 1}
    assert payload["rows"][0]["ticker"] == "ACME"
    assert payload["rows"][0]["priced_in_status"] == "bullish_not_priced_in"
    assert payload["rows"][0]["emotion_reaction_gap"] == 49.0
    assert payload["source_coverage"]["schema_version"] == "priced-in-source-coverage-v1"
    assert payload["source_coverage"]["row_count"] == 1
    assert payload["source_coverage"]["options_gap_diagnostic"]["status"] == (
        "no_stored_options"
    )
    actions = {row["source"]: row for row in payload["source_coverage"]["actions"]}
    assert actions["options"]["status"] == "missing"
    assert actions["options"]["gap_count"] == 1
    assert actions["options"]["diagnostic"]["status"] == "no_stored_options"
    assert actions["options"]["batch_plan_command"] == (
        "catalyst-radar priced-in-source-batches --source options --batch-limit 5"
    )
    assert actions["options"]["sample_scope"] == (
        "These are all 1 missing/stale row(s) in the current filtered scan, "
        "not a separate scan universe."
    )
    assert actions["options"]["external_call_boundary"] == (
        "Live Schwab options are explicit, read-only, and rate-limited; "
        "current option chains must not be used as score input for older "
        "scan dates."
    )
    assert actions["broker_context"]["next_action"] == (
        "Sync read-only Schwab market context before sizing or trigger review."
    )
    assert "catalyst_events" in payload["rows"][0]["data_sources"]["available"]

    assert main(["priced-in-queue", "--usefulness", "research_useful", "--json"]) == 0
    output = capsys.readouterr()
    filtered_payload = json.loads(output.out)

    assert output.err == ""
    assert filtered_payload["filters"]["usefulness"] == "research_useful"
    assert filtered_payload["rows"][0]["usefulness"]["status"] == "research_useful"

    assert main(["priced-in-queue", "--full-scan", "--json"]) == 0
    output = capsys.readouterr()
    full_scan_payload = json.loads(output.out)

    assert output.err == ""
    assert full_scan_payload["filters"]["status"] == "all"

    assert main(["priced-in-queue", "--full-scan", "--all", "--json"]) == 0
    output = capsys.readouterr()
    full_scan_all_payload = json.loads(output.out)

    assert output.err == ""
    assert full_scan_all_payload["filters"]["status"] == "all"
    assert full_scan_all_payload["filters"]["offset"] == 0
    assert full_scan_all_payload["count"] == full_scan_all_payload["total_count"]
    assert full_scan_all_payload["has_more"] is False

    assert main(["priced-in-queue", "--mismatches", "--json"]) == 0
    output = capsys.readouterr()
    mismatch_payload = json.loads(output.out)

    assert output.err == ""
    assert mismatch_payload["filters"]["status"] == "actionable"

    assert main(["priced-in-queue", "--available-at", cutoff, "--json"]) == 0
    output = capsys.readouterr()
    cutoff_payload = json.loads(output.out)

    assert output.err == ""
    assert cutoff_payload["filters"]["available_at"] == cutoff

    assert main(["priced-in-queue", "--source-gap", "options", "--json"]) == 0
    output = capsys.readouterr()
    gap_payload = json.loads(output.out)

    assert output.err == ""
    assert gap_payload["filters"]["source_gap"] == ["options"]
    assert "options" in gap_payload["rows"][0]["data_sources"]["missing"]

    assert main(["priced-in-queue", "--decision-gap", "options", "--json"]) == 0
    output = capsys.readouterr()
    decision_gap_payload = json.loads(output.out)

    assert output.err == ""
    assert decision_gap_payload["filters"]["decision_gap"] == ["options"]
    assert "options" in (
        decision_gap_payload["rows"][0]["usefulness"]["missing_for_decision"]
    )

    assert main(["priced-in-queue"]) == 0
    output = capsys.readouterr()

    assert output.err == ""
    assert "usefulness_counts=research_useful:1" in output.out
    assert "scan_scope=scanned=" in output.out
    assert "visible_page=1" in output.out
    assert "source_actions:" in output.out
    assert "options status=missing" in output.out
    assert "gap_rows=1" in output.out
    assert "examples=ACME" in output.out
    assert "sample_scope=These are all 1 missing/stale row(s)" in output.out
    assert "full_scan_review=catalyst-radar priced-in-queue --full-scan" in output.out
    assert "full_scan_export=catalyst-radar priced-in-queue --full-scan" in output.out
    assert "--all --json" in output.out
    assert "batch_plan=catalyst-radar priced-in-source-batches --source options" in output.out
    assert "diagnostic=missing=1" in output.out
    assert "broker_context status=missing" in output.out

    assert (
        main(
            [
                "priced-in-source-batches",
                "--source",
                "options",
                "--batch-size",
                "1",
                "--batch-limit",
                "1",
                "--json",
            ]
        )
        == 0
    )
    output = capsys.readouterr()
    batch_payload = json.loads(output.out)

    assert output.err == ""
    assert batch_payload["schema_version"] == "priced-in-source-batches-v1"
    assert batch_payload["external_calls_made"] == 0
    assert batch_payload["source"] == "options"
    assert batch_payload["count"] == 1
    assert batch_payload["batches"][0]["tickers"] == ["ACME"]


def test_candidate_detail_cli_outputs_priced_in_evidence_brief(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'demo.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    assert main(["seed-dashboard-demo"]) == 0
    capsys.readouterr()

    assert main(["candidate-detail", "ACME", "--json"]) == 0
    output = capsys.readouterr()
    payload = json.loads(output.out)

    assert output.err == ""
    brief = payload["priced_in_evidence_brief"]
    assert brief["schema_version"] == "priced-in-evidence-brief-v1"
    assert brief["ticker"] == "ACME"
    assert brief["status"] == "bullish_not_priced_in"
    actions = {row["source"]: row for row in brief["source_actions"]}
    assert actions["options"]["status"] == "missing"
    assert actions["options"]["next_action"] == (
        "Use point-in-time options for the scan date; for a current scan, sync Schwab "
        "option-chain context, then rerun."
    )
    assert actions["options"]["command"].endswith("--ticker ACME")
    assert actions["options"]["sample_tickers"] == ["ACME"]
    assert brief["usefulness"]["status"] == "research_useful"
    assert brief["usefulness"]["decision_ready"] is False
    assert "options" in brief["usefulness"]["missing_for_decision"]
    assert brief["evidence"]
    assert brief["next_step"]

    assert main(["candidate-detail", "ACME"]) == 0
    output = capsys.readouterr()

    assert output.err == ""
    assert "candidate_detail ticker=ACME" in output.out
    assert "status=bullish_not_priced_in" in output.out
    assert "why_now=" in output.out
    assert "emotion_vs_reaction=" in output.out
    assert "usefulness=research_useful decision_ready=false" in output.out
    assert "source_actions:" in output.out
    assert "options status=missing" in output.out
    assert "sample=ACME" in output.out
    assert "evidence:" in output.out
    assert "next_step=" in output.out


def test_priced_in_preflight_cli_outputs_zero_call_plan(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'demo.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    assert main(["seed-dashboard-demo"]) == 0
    capsys.readouterr()

    assert main(["priced-in-preflight", "--json"]) == 0
    output = capsys.readouterr()
    payload = json.loads(output.out)

    assert output.err == ""
    assert payload["schema_version"] == "priced-in-preflight-v1"
    assert payload["external_calls_made"] == 0
    assert payload["rows"][0]["area"] == "universe"
    assert payload["commands"]["review_queue"] == "catalyst-radar priced-in-queue --json"


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
    assert "Usefulness" in rendered
    assert "Source gaps" in rendered
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
            assert "Full-market priced-in queue - showing" in frame
            assert "UNIVERSE" in frame
            assert "ACME" in frame
            assert "Bullish not priced" in frame
            assert "showing the first ranked page from the entire scan" in frame
            assert "M  Mismatches only" in frame
            assert "ALL Full scan rows" in frame
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

            await pilot.press("m")
            await pilot.pause()
            assert app.page == "overview"
            assert app.filters.priced_in_status == "actionable"
            frame = html.unescape(app.export_screenshot()).replace("\xa0", " ")
            assert "Mismatches from full scan - showing" in frame
            assert "Mismatches mode" in frame

            assert await pilot.click("#action-scan-all")
            await pilot.pause()
            assert app.filters.priced_in_status == "all"
            frame = html.unescape(app.export_screenshot()).replace("\xa0", " ")
            assert "Full Scan mode" in frame
            assert "Full-market priced-in queue - showing" in frame

            app.query_one("#data-table").focus()
            await pilot.press("down")
            await pilot.press("enter")
            await pilot.pause()
            assert app.page == "ops"

            await pilot.press("1")
            await pilot.pause()
            assert app.page == "overview"
            app.query_one("#data-table").focus()
            await pilot.press("down")
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

from __future__ import annotations

import asyncio
import csv
import html
import json
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path

from sqlalchemy import create_engine, func, select

from catalyst_radar.cli import _print_priced_in_answer, main
from catalyst_radar.core.config import AppConfig
from catalyst_radar.core.models import DailyBar, Security
from catalyst_radar.dashboard import data as dashboard_data_module
from catalyst_radar.dashboard import source_batches as source_batch_module
from catalyst_radar.dashboard import tui as dashboard_tui_module
from catalyst_radar.dashboard.data import (
    load_alert_rows,
    load_candidate_rows,
    load_cost_summary,
    load_ipo_s1_rows,
    load_ops_health,
    load_ticker_detail,
    load_validation_summary,
    priced_in_all_source_gap_batches_payload,
    priced_in_answer_payload,
    priced_in_queue_payload,
    radar_readiness_payload,
)
from catalyst_radar.dashboard.demo_seed import DEMO_AVAILABLE_AT
from catalyst_radar.dashboard.tui import (
    DashboardFilters,
    MarketRadarDashboardApp,
    _answer_evidence_completeness_summary,
    _answer_full_scan_scope_summary,
    _apply_command,
    _full_scan_coverage_row,
    _market_bar_manual_fill_progress_summary,
    _market_bar_missing_type_summary,
    _market_bar_operator_step_summary,
    _market_bar_provider_fill_summary,
    _market_bar_saved_capture_summary,
    _market_inbox_rows,
    _priced_in_overview_rows,
    _priced_in_review_rows,
    _priced_in_source_workflow_payload,
    _run_audit_source_blocker_hint,
    _run_mission_brief_items,
    _stock_market_bar_next_summary,
    dashboard_filters_for_page,
    dashboard_snapshot_payload,
    render_dashboard_tui,
    run_dashboard_tui,
)
from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.repositories import MarketRepository
from catalyst_radar.storage.schema import daily_bars, option_features, securities
from catalyst_radar.storage.validation_repositories import ValidationRepository


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
    monkeypatch.setenv("CATALYST_ENABLE_PREMIUM_LLM", "false")
    monkeypatch.setenv("CATALYST_LLM_PROVIDER", "none")
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
    assert payload["controls"]["priced_in_stocks_only"] is False
    assert payload["priced_in_queue"]["filters"]["status"] == "all"
    assert payload["controls"]["priced_in_usefulness"] is None
    assert payload["controls"]["priced_in_decision_gap"] == []
    assert payload["readiness"]["schema_version"] == "radar-readiness-v1"
    assert payload["trial_readiness"]["schema_version"] == "trial-readiness-v1"
    assert payload["trial_readiness"]["safe_to_try_read_only"] is True
    assert payload["trial_readiness"]["ready_for_investment_decision"] is False
    assert payload["live_activation"]["schema_version"] == (
        "live-data-activation-contract-v1"
    )
    assert payload["call_plan"]["schema_version"] == "radar-run-call-plan-v1"
    assert payload["priced_in_preflight"]["schema_version"] == "priced-in-preflight-v1"
    assert payload["priced_in_source_workflow"]["schema_version"] == (
        "priced-in-source-workflow-v1"
    )
    assert payload["priced_in_source_workflow"]["overview_command"] == (
        "catalyst-radar priced-in-source-batches --source all"
    )
    assert payload["priced_in_source_workflow"]["external_calls_made"] == 0
    assert payload["priced_in_source_workflow"]["steps"]
    assert payload["priced_in_source_workflow"]["priority_scope"] == (
        "full_scan_coverage"
    )
    assert payload["priced_in_source_workflow"]["decision_priority_scope"] == (
        "visible_priced_in_rows"
    )
    assert payload["priced_in_source_workflow"]["goal_alignment"][
        "schema_version"
    ] == "priced-in-goal-alignment-v1"
    assert "market emotion" in payload["priced_in_source_workflow"]["goal_alignment"][
        "goal"
    ]
    assert payload["priced_in_source_workflow"]["coverage_first_action"]
    assert payload["priced_in_source_workflow"]["decision_shortcut_action"].startswith(
        "Start with broker_context;"
    )
    options_step = next(
        step
        for step in payload["priced_in_source_workflow"]["steps"]
        if step["source"] == "options"
    )
    assert options_step[
        "decision_useful_gap_rows"
    ] == 1
    assert options_step["priority_sample_tickers"] == ["ACME"]
    assert payload["priced_in_answer"]["schema_version"] == "priced-in-answer-v1"
    assert payload["priced_in_answer"]["external_calls_made"] == 0
    assert payload["priced_in_audit"]["schema_version"] == (
        "priced-in-full-scan-audit-v1"
    )
    assert payload["priced_in_audit"]["external_calls_made"] == 0
    assert payload["priced_in_audit"]["scope"]["mode"] == "full_scan"
    assert payload["priced_in_answer"]["question"] == (
        "Has price fully matched market expectations?"
    )

    assert (
        main(
            [
                "dashboard-snapshot",
                "--ticker",
                "ACME",
                "--available-at",
                cutoff,
                "--stocks-only",
                "--json",
            ]
        )
        == 0
    )
    output = capsys.readouterr()
    stock_payload = json.loads(output.out)
    assert output.err == ""
    assert stock_payload["controls"]["priced_in_stocks_only"] is True
    assert stock_payload["priced_in_queue"]["filters"]["stocks_only"] is True
    assert stock_payload["priced_in_source_workflow"]["overview_command"] == (
        "catalyst-radar priced-in-source-batches --source all --stocks-only"
    )
    assert stock_payload["priced_in_source_workflow"]["goal_alignment"][
        "stocks_only"
    ] is True
    assert payload["priced_in_answer"]["answer"]
    assert payload["agent_brief"]["schema_version"] == "market-radar-agent-brief-v1"
    assert payload["agent_brief"]["external_calls_made"] == {
        "broker": 0,
        "market_data": 0,
        "openai": 0,
    }
    assert any(
        insight.startswith("Priced-in answer is")
        for insight in payload["agent_brief"]["insights"]
    )
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


def test_overview_renders_minimum_product_approval_stop_line() -> None:
    text = render_dashboard_tui(
        {
            "trial_readiness": {
                "status": "safe_read_only",
                "safe_to_try_read_only": True,
                "minimum_useful_product": {
                    "status": "blocked",
                    "ready": False,
                    "first_blocker": "market_bars",
                    "next_command": (
                        "catalyst-radar market-bars residual-review "
                        "--expected-as-of 2026-05-15"
                    ),
                    "approval_required_unblock": {
                        "status": "ready_to_execute",
                        "approval_required": True,
                        "external_calls_required": 0,
                        "db_writes_required_to_execute": 579,
                        "approval_command": (
                            "catalyst-radar market-bars residual-repair "
                            "--expected-as-of 2026-05-15 "
                            "--expect-missing-count 579 "
                            "--expect-eligible-count 579 --execute --json"
                        ),
                    },
                },
            },
            "priced_in_answer": {"full_market_trust_gate": {}},
            "priced_in_queue": {"rows": []},
        },
        page="overview",
        width=220,
    )

    assert "Shipped-product stop" in text
    assert "blocked; blocker market_bars" in text
    assert "Approval required: 579 DB write(s), 0 provider call(s)" in text
    assert "residual-repair --expected-as-of 2026-05-15" in text


def test_dashboard_snapshot_exposes_market_bar_approval_packet(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'dashboard-approval.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = create_engine(database_url, future=True)
    create_schema(engine)
    approval = {
        "schema_version": "shadow-readiness-approval-required-v1",
        "area": "market_bars",
        "status": "ready_to_execute",
        "approval_required": True,
        "expected_missing_count": 1,
        "expected_eligible_count": 1,
        "external_calls_required": 0,
        "external_calls_made": 0,
        "db_writes_required_to_execute": 1,
        "db_writes_made": 0,
        "preview_command": (
            "catalyst-radar market-bars residual-repair "
            "--expected-as-of 2026-05-08 --json"
        ),
        "approval_command": (
            "catalyst-radar market-bars residual-repair "
            "--expected-as-of 2026-05-08 --expect-missing-count 1 "
            "--expect-eligible-count 1 --execute --json"
        ),
    }

    def fake_shadow_readiness_payload(*_args, **_kwargs) -> dict[str, object]:
        return {
            "schema_version": "shadow-readiness-v1",
            "status": "setup_required",
            "ready": False,
            "first_blocker": "market_bars",
            "first_gap_count": 1,
            "canonical_next_action": "Review residual market-bar rows.",
            "canonical_next_command": (
                "catalyst-radar market-bars residual-review "
                "--expected-as-of 2026-05-08"
            ),
            "checks": [],
            "approval_required_unblock": approval,
            "external_calls_made": 0,
            "db_writes_made": 0,
        }

    monkeypatch.setattr(
        dashboard_data_module,
        "shadow_readiness_payload",
        fake_shadow_readiness_payload,
    )

    payload = dashboard_snapshot_payload(
        engine=engine,
        config=AppConfig.from_env({"CATALYST_SCAN_BATCH_SIZE": "2"}),
        dotenv_loaded=False,
        filters=DashboardFilters(available_at=datetime(2026, 5, 8, 21, tzinfo=UTC)),
    )

    assert payload["approval_required_unblock"] == approval
    assert payload["shadow_readiness"]["approval_required_unblock"] == approval
    assert payload["status"] == "setup_required"
    assert payload["first_blocker"] == "market_bars"
    assert payload["first_gap_count"] == 1
    assert payload["canonical_next_action"] == "Review residual market-bar rows."
    assert payload["canonical_next_command"] == (
        "catalyst-radar market-bars residual-review --expected-as-of 2026-05-08"
    )
    assert payload["next_action"] == payload["canonical_next_action"]
    assert payload["next_command"] == payload["canonical_next_command"]
    assert payload["approval_required_unblock"]["approval_required"] is True
    assert payload["approval_required_unblock"]["external_calls_required"] == 0
    assert payload["approval_required_unblock"]["db_writes_required_to_execute"] == 1
    assert payload["external_calls_made"] == 0


def test_source_workflow_skips_non_point_in_time_options_shortcut() -> None:
    preflight = {
        "evidence_plan": {
            "status": "attention",
            "steps": [
                {
                    "priority": 1,
                    "area": "catalyst_events",
                    "status": "attention",
                    "action": "Review event coverage.",
                    "command": (
                        "catalyst-radar priced-in-source-batches "
                        "--source catalyst_events --all --json"
                    ),
                },
                {
                    "priority": 2,
                    "area": "options",
                    "status": "attention",
                    "action": (
                        "Stored options exist after this scan date. Rerun only "
                        "with a current scan date and current bars, or ingest "
                        "point-in-time options for the original scan date."
                    ),
                    "command": (
                        "catalyst-radar priced-in-source-batches "
                        "--source options --all --json"
                    ),
                },
                {
                    "priority": 3,
                    "area": "broker_context",
                    "status": "attention",
                    "action": "Sync read-only Schwab market context.",
                    "command": (
                        "catalyst-radar priced-in-source-batches "
                        "--source broker_context --all --json"
                    ),
                },
            ],
        }
    }
    queue = {
        "rows": [
            {
                "ticker": "AAMI",
                "priced_in_status": "bullish_not_priced_in",
                "usefulness": {"status": "research_useful"},
                "data_sources": {
                    "available": ["market_bars", "catalyst_events", "local_text"],
                    "missing": ["options", "broker_context"],
                    "stale": [],
                },
            }
        ]
    }

    payload = _priced_in_source_workflow_payload(
        preflight,
        priced_in_queue=queue,
    )

    assert payload["decision_shortcut_action"].startswith(
        "Start with broker_context;"
    )


def test_dashboard_snapshot_reuses_priced_in_queue_preflight(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'demo.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    assert main(["seed-dashboard-demo"]) == 0
    engine = create_engine(database_url, future=True)

    from catalyst_radar.dashboard import tui as dashboard_tui

    calls = 0
    original_preflight = dashboard_tui.dashboard_data.priced_in_preflight_payload

    def counted_preflight(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original_preflight(*args, **kwargs)

    monkeypatch.setattr(
        dashboard_tui.dashboard_data,
        "priced_in_preflight_payload",
        counted_preflight,
    )

    payload = dashboard_snapshot_payload(
        engine=engine,
        config=AppConfig.from_env(),
        dotenv_loaded=True,
        filters=DashboardFilters(),
    )

    assert calls == 1
    assert payload["priced_in_preflight"]["schema_version"] == "priced-in-preflight-v1"
    assert payload["priced_in_preflight"] == payload["priced_in_queue"]["preflight"]


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
        "Latest scan results - rows",
        "Mailbox",
        "ACME",
        "Urgent",
        "Bullish not price",
        "emotion",
        "reaction",
        "review page, not the full scan universe",
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
    assert "Visible Review Page Source Gaps" in output.out
    assert "not the full scan universe" in output.out
    assert "Source Fill Workflow" in output.out
    assert "Full gaps" in output.out
    assert "Inspect" in output.out
    assert "Start with broker_context" in output.out
    assert "decision-ready row(s)" in output.out
    assert "options" in output.out
    assert "batch broker_context" in output.out
    assert "priced-in-source-batches" in output.out
    assert "priced-in-source-batches --source all" in output.out
    assert "Examples are sample tickers only" in output.out
    assert "`batch all` shows this source map without provider calls" in output.out
    assert "batch <source>" in output.out
    assert "batch <source> execute 3" in output.out
    assert "ACME" in output.out

    assert (
        main(
            [
                "dashboard-snapshot",
                "--ticker",
                "ACME",
                "--available-at",
                cutoff,
                "--stocks-only",
                "--page",
                "ops",
            ]
        )
        == 0
    )
    output = capsys.readouterr()

    assert output.err == ""
    assert "Goal" in output.out
    assert "Useful" in output.out
    assert "market emotion has not yet been matched" in output.out
    assert "stock rows" in output.out
    assert "priced-in-source-batches --source all --stocks-only" in output.out


def test_dashboard_batch_command_opens_full_scan_source_batch_plan(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'demo.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    assert main(["seed-dashboard-demo"]) == 0
    capsys.readouterr()

    update = _apply_command(
        "batch options",
        {},
        "overview",
        DashboardFilters(),
        engine=create_engine(database_url, future=True),
        config=AppConfig.from_env(),
    )

    assert update.page == "ops"
    assert update.filters == DashboardFilters()
    assert "options: ready;" in update.message
    assert "full-scan gap row" in update.message
    assert "plannable" in update.message
    assert "batch(es)" in update.message
    assert "This is a full-scan plan, not a watchlist." in update.message
    assert (
        "Showing batch 1-1 of 1 (1 ticker(s)); this includes every currently "
        "plannable ticker for this source."
    ) in update.message
    assert "Add `all` to summarize every chunk for this source." in update.message
    assert "first provider chunk only." in update.message
    assert "Command: catalyst-radar schwab-market-sync --ticker ACME" in (
        update.message
    )
    assert "`batch options execute`" in update.message
    assert (
        "Full chunk list: catalyst-radar priced-in-source-batches "
        "--source options --all --json"
    ) in update.message

    overview = _apply_command(
        "batch all",
        {},
        "overview",
        DashboardFilters(),
        engine=create_engine(database_url, future=True),
        config=AppConfig.from_env(),
    )

    assert overview.page == "ops"
    assert "plan-only and makes no provider calls" in overview.message
    assert "tickers below are only first safe provider chunks" in overview.message
    assert "Full scan universe:" in overview.message
    assert "Review rows: catalyst-radar priced-in-queue --full-scan --limit 50" in (
        overview.message
    )
    assert "Suggested first:" in overview.message
    assert "Coverage-first chunk (first provider chunk only):" in overview.message
    assert "Decision shortcut chunk (first provider chunk only):" in overview.message
    assert "tickers ACME" in overview.message
    assert "calls 1" in overview.message
    assert "options=ready" in overview.message
    assert "options=ready gaps=1 plan=1 batches=1" in overview.message
    assert (
        "First executable: catalyst-radar priced-in-source-batches "
        "--source local_text --execute-next"
    ) in overview.message or (
        "First executable: catalyst-radar priced-in-source-batches "
        "--source options --execute-next"
    ) in overview.message

    full_plan = _apply_command(
        "batch options all",
        {},
        "overview",
        DashboardFilters(),
        engine=create_engine(database_url, future=True),
        config=AppConfig.from_env(),
    )

    assert full_plan.page == "ops"
    assert "options: ready;" in full_plan.message
    assert "Full chunk plan requested" in full_plan.message
    assert (
        "this includes every currently plannable ticker for this source"
        in full_plan.message
    )
    assert "first provider chunk only." in full_plan.message
    assert "Command:" in full_plan.message


def test_dashboard_tui_value_ledger_and_outcome_commands_are_preview_first(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'demo.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    assert main(["seed-dashboard-demo"]) == 0
    capsys.readouterr()

    engine = create_engine(database_url, future=True)
    filters = DashboardFilters(available_at=DEMO_AVAILABLE_AT + timedelta(minutes=1))
    payload = dashboard_snapshot_payload(
        engine=engine,
        config=AppConfig.from_env(),
        dotenv_loaded=True,
        filters=filters,
    )

    coverage = _apply_command(
        "ledger coverage",
        payload,
        "costs",
        filters,
        engine=engine,
        config=AppConfig.from_env(),
    )
    assert coverage.page == "costs"
    assert "Value-ledger coverage: status=gaps" in coverage.message
    assert "external_calls=0 db_writes=0" in coverage.message
    assert "next_command=catalyst-radar value-ledger record" in coverage.message
    assert "--preview --json" in coverage.message

    preview = _apply_command(
        "ledger record 1 useful research accepted 12 0.5 preview-note",
        payload,
        "costs",
        filters,
        engine=engine,
        config=AppConfig.from_env(),
    )
    assert "Value ledger preview:" in preview.message
    assert "ACME useful research" in preview.message
    assert "db_writes=0" in preview.message
    assert ValidationRepository(engine).list_value_ledger_entries(limit=10) == []

    executed = _apply_command(
        "ledger record 1 useful research accepted 12 0.5 --execute acted-note",
        payload,
        "costs",
        filters,
        engine=engine,
        config=AppConfig.from_env(),
    )
    assert "Value ledger executed:" in executed.message
    assert "db_writes=1" in executed.message
    entries = ValidationRepository(engine).list_value_ledger_entries(limit=10)
    assert len(entries) == 1
    assert entries[0].ticker == "ACME"
    assert entries[0].source == "dashboard_tui"

    outcome_coverage = _apply_command(
        "outcome coverage",
        payload,
        "costs",
        filters,
        engine=engine,
        config=AppConfig.from_env(),
    )
    assert "Value-outcome coverage: status=gaps" in outcome_coverage.message
    assert "ledger=1 linked=0 missing=1" in outcome_coverage.message
    assert "external_calls=0 db_writes=0" in outcome_coverage.message
    assert "next_command=catalyst-radar value-outcome update" in (
        outcome_coverage.message
    )
    assert "--preview --json" in outcome_coverage.message

    outcome_preview = _apply_command(
        f"outcome update {entries[0].id} filter",
        payload,
        "costs",
        filters,
        engine=engine,
        config=AppConfig.from_env(),
    )
    assert "Value outcome preview:" in outcome_preview.message
    assert "external_calls=0 db_writes=0" in outcome_preview.message
    assert ValidationRepository(engine).list_value_outcomes(limit=10) == []


def test_dashboard_batch_all_response_separates_plan_route_and_blocked_rows(
    monkeypatch,
):
    def fake_payload(_engine, _config, **_kwargs):
        return {
            "headline": "Source map.",
            "scan_scope": {
                "ranked_rows": 12,
                "review_full_scan_command": "review rows",
                "export_full_scan_command": "export rows",
            },
            "sources": [
                {
                    "source": "catalyst_events",
                    "status": "ready",
                    "total_gap_rows": 12,
                    "plannable_gap_rows": 5,
                    "unplannable_gap_rows": 7,
                    "routed_gap_rows": 6,
                    "blocked_gap_rows": 1,
                    "diagnostic": {"blocked_rows": 99},
                    "batch_count": 1,
                    "execute_next_command": "run catalyst",
                },
                {
                    "source": "local_text",
                    "status": "blocked",
                    "total_gap_rows": 4,
                    "plannable_gap_rows": 0,
                    "unplannable_gap_rows": 4,
                    "routed_gap_rows": 0,
                    "blocked_gap_rows": 4,
                    "diagnostic": {},
                    "batch_count": 0,
                },
            ],
            "next_action": "Review catalyst_events.",
            "mission_brief": {
                "recommended_unblock_action": {
                    "kind": "saved_provider_capture",
                    "status": "approval_required",
                    "approval_required": True,
                    "external_calls_required": 1,
                    "db_writes_required": 0,
                    "command": "bars saved capture confirm",
                    "reason": "Approve one Polygon/Massive grouped-daily call.",
                }
            },
            "coverage_first_recommendation": {},
            "decision_shortcut_recommendation": {},
        }

    monkeypatch.setattr(
        "catalyst_radar.dashboard.tui.dashboard_data.priced_in_all_source_gap_batches_payload",
        fake_payload,
    )

    update = _apply_command(
        "batch all",
        {},
        "overview",
        DashboardFilters(),
        engine=create_engine("sqlite:///:memory:", future=True),
        config=AppConfig.from_env(),
    )

    assert update.page == "ops"
    assert "Recommended unblock:" in update.message
    assert "saved_provider_capture approval_required" in update.message
    assert "bars saved capture confirm" in update.message
    assert (
        "catalyst_events=ready gaps=12 plan=5 routed=6 blocked=1 batches=1"
        in update.message
    )
    assert "local_text=blocked gaps=4 plan=0 blocked=4 batches=0" in update.message


def test_dashboard_batch_command_explains_non_company_cik_gaps(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'demo.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    def fake_payload(_engine, _config, **kwargs) -> dict[str, object]:
        return {
            "status": "routed",
            "source": kwargs["source"],
            "total_gap_rows": 2,
            "plannable_gap_rows": 0,
            "routed_gap_rows": 2,
            "batch_count": 0,
            "next_action": "Use fund evidence.",
            "diagnostic": {
                "reason": "Non-company instruments are routed.",
                "next_action": "Use ETF evidence instead.",
                "sample_blocked_tickers": [],
                "missing_cik_type_counts": {},
                "missing_cik_company_like_rows": 0,
                "missing_cik_non_company_rows": 0,
                "missing_cik_unknown_type_rows": 0,
                "routed_non_company_rows": 2,
                "sample_routed_non_company_tickers": ["AAA", "BBB"],
                "non_company_evidence_route": "Use fund evidence.",
            },
            "batches": [],
        }

    monkeypatch.setattr(
        "catalyst_radar.dashboard.tui.dashboard_data.priced_in_source_gap_batches_payload",
        fake_payload,
    )

    update = _apply_command(
        "batch catalyst_events",
        {},
        "overview",
        DashboardFilters(),
        engine=create_engine(database_url, future=True),
        config=AppConfig.from_env(),
    )

    assert update.page == "ops"
    assert "Non-company routed: 2." in update.message
    assert "Examples: AAA, BBB." in update.message
    assert "Route: Use fund evidence." in update.message
    assert "Use ETF evidence instead." in update.message


def test_dashboard_batch_execute_runs_one_guarded_local_chunk(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'demo.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = create_engine(database_url, future=True)
    calls: dict[str, object] = {}
    plan_calls = 0

    def fake_batches_payload(_engine, _config, **kwargs) -> dict[str, object]:
        nonlocal plan_calls
        source = kwargs["source"]
        if source == "market_bars":
            return {
                "status": "ready",
                "source": "market_bars",
                "total_gap_rows": 0,
                "plannable_gap_rows": 0,
                "batch_count": 0,
                "batches": [],
            }
        plan_calls += 1
        assert source == "local_text"
        remaining_rows = max(0, 125 - (plan_calls * 2))
        return {
            "status": "ready",
            "source": "local_text",
            "total_gap_rows": remaining_rows,
            "plannable_gap_rows": remaining_rows,
            "batch_count": 25,
            "next_action": "Run local text.",
            "batches": [
                {
                    "number": 1,
                    "row_start": 1,
                    "row_end": 5,
                    "tickers": ["ACME", "MSFT"],
                    "call_plan_status": "local_only",
                    "api_payload": {
                        "as_of": "2026-05-15",
                        "available_at": "2026-05-18T16:00:00+00:00",
                        "tickers": ["ACME", "MSFT"],
                    },
                }
            ],
        }

    class FakeTextResult:
        feature_count = 2
        snippet_count = 4

    def fake_run_text_pipeline(_event_repo, _text_repo, **kwargs):
        calls["as_of"] = kwargs["as_of"].isoformat()
        calls["available_at"] = kwargs["available_at"].isoformat()
        calls["tickers"] = tuple(kwargs["tickers"])
        return FakeTextResult()

    monkeypatch.setattr(
        "catalyst_radar.dashboard.source_batches.priced_in_source_gap_batches_payload",
        fake_batches_payload,
    )
    monkeypatch.setattr(
        "catalyst_radar.dashboard.source_batches.run_text_pipeline",
        fake_run_text_pipeline,
    )

    update = _apply_command(
        "batch local_text execute",
        {},
        "overview",
        DashboardFilters(),
        engine=engine,
        config=AppConfig.from_env(),
    )

    assert update.page == "ops"
    assert update.message == (
        "Executed local_text chunk 1 (rows 1-5): tickers=2 features=2 "
        "snippets=4 external_calls=0. Post-check: Full-scan local_text coverage "
        "improved; 2 gap row(s) and 2 plannable row(s) cleared. Review the "
        "updated next batch before executing another chunk."
    )
    assert calls == {
        "as_of": "2026-05-15T21:00:00+00:00",
        "available_at": "2026-05-18T16:00:00+00:00",
        "tickers": ("ACME", "MSFT"),
    }
    calls.clear()

    alias_update = _apply_command(
        "batch execute local_text",
        {},
        "overview",
        DashboardFilters(),
        engine=engine,
        config=AppConfig.from_env(),
    )

    assert alias_update.message.startswith("Executed local_text chunk 1")
    assert calls["tickers"] == ("ACME", "MSFT")


def test_dashboard_batch_execute_can_run_capped_chunks(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'demo.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    captured: dict[str, object] = {}

    def fake_execute_batches(_engine, _config, **kwargs) -> dict[str, object]:
        captured.update(kwargs)
        return {
            "schema_version": "priced-in-source-batch-run-v1",
            "source": kwargs["source"],
            "status": "executed",
            "requested_batches": kwargs["max_batches"],
            "executed_batches": 3,
            "stopped_reason": "Reached max_batches=3.",
            "external_calls_made": 3,
            "before_plan": {"total_gap_rows": 10, "plannable_gap_rows": 10},
            "after_plan": {"total_gap_rows": 7, "plannable_gap_rows": 7},
            "gap_rows_resolved": 3,
            "plannable_rows_resolved": 3,
            "executions": [],
            "next_action": "Review the next batch plan before continuing.",
        }

    monkeypatch.setattr(
        "catalyst_radar.dashboard.tui.execute_source_batches",
        fake_execute_batches,
    )

    update = _apply_command(
        "batch catalyst_events execute 3",
        {},
        "overview",
        DashboardFilters(),
        engine=create_engine(database_url, future=True),
        config=AppConfig.from_env(),
    )

    assert update.page == "ops"
    assert "catalyst_events batch run executed" in update.message
    assert "executed 3/3 chunk(s)" in update.message
    assert captured["source"] == "catalyst_events"
    assert captured["max_batches"] == 3


def test_dashboard_run_page_shows_priced_in_evidence_plan(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'demo.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    assert main(["seed-dashboard-demo"]) == 0
    capsys.readouterr()

    assert main(["dashboard-tui", "--once", "--page", "run"]) == 0
    output = capsys.readouterr()

    assert output.err == ""
    assert "Mission Brief" in output.out
    assert "Current answer" in output.out
    assert "Trust gate" in output.out
    assert "Trust blocker" in output.out
    assert "Boundary" in output.out
    assert "Priced-in Evidence Plan" in output.out
    assert "Evidence status" in output.out
    assert "Full-scan evidence" in output.out
    assert "Visible-page source coverage" in output.out
    assert "Inspect source blocker" in output.out
    assert "Type `batch" in output.out
    assert "exact call budget" in output.out
    assert "priced-in-source-" in output.out


def test_dashboard_snapshot_priced_in_queue_loads_scan_rows_from_database(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'demo.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    assert main(["seed-dashboard-demo"]) == 0
    capsys.readouterr()

    engine = create_engine(database_url, future=True)
    config = AppConfig.from_env({"CATALYST_DATABASE_URL": database_url})
    captured_kwargs: list[dict[str, object]] = []
    original = dashboard_data_module.priced_in_queue_payload

    def capture_priced_in_queue_payload(*args, **kwargs):
        captured_kwargs.append(dict(kwargs))
        return original(*args, **kwargs)

    monkeypatch.setattr(
        dashboard_data_module,
        "priced_in_queue_payload",
        capture_priced_in_queue_payload,
    )

    payload = dashboard_snapshot_payload(
        engine=engine,
        config=config,
        dotenv_loaded=False,
        filters=DashboardFilters(priced_in_stocks_only=True),
    )

    assert captured_kwargs
    assert "candidate_rows" not in captured_kwargs[0]
    assert captured_kwargs[0]["include_planning_rows"] is True
    assert payload["priced_in_queue"]["scan_selection"]["mode"] != "supplied_rows"
    assert "planning_rows" not in payload["priced_in_queue"]


def test_dashboard_agent_page_shows_agent_brief(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'demo.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    assert main(["seed-dashboard-demo"]) == 0
    capsys.readouterr()

    assert main(["dashboard-tui", "--once", "--page", "agent"]) == 0
    output = capsys.readouterr()

    assert output.err == ""
    assert "Agent Brief" in output.out
    assert "openai=0" in output.out
    assert "OpenAI Agents SDK" in output.out
    assert "Copilot absent" in output.out
    assert "read only snapshot tools" in output.out
    assert "Priced-in answer is" in output.out


def test_tui_agent_run_preview_is_zero_call() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    create_schema(engine)
    payload = _minimal_missing_real_results_payload()

    update = _apply_command(
        "agent run",
        payload,
        "agent",
        DashboardFilters(),
        engine=engine,
        config=AppConfig.from_env({}),
    )

    assert update.page == "agent"
    assert "Agent previewed" in update.message
    assert "OpenAI calls=0" in update.message
    assert "OpenAI calls planned" in update.message


def test_tui_agent_run_execute_blocks_without_real_results() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    create_schema(engine)
    payload = _minimal_missing_real_results_payload()

    update = _apply_command(
        "agent run execute",
        payload,
        "agent",
        DashboardFilters(),
        engine=engine,
        config=AppConfig.from_env({}),
    )

    assert update.page == "agent"
    assert "Agent blocked" in update.message
    assert "OpenAI calls=0" in update.message
    assert "No real result yet" in update.message


def test_dashboard_once_empty_database_shows_no_real_result_not_demo(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'empty-dashboard.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    assert main(["dashboard-tui", "--once"]) == 0
    output = capsys.readouterr()

    assert output.err == ""
    assert "No real result yet" in output.out
    assert "Required next step:" in output.out
    assert "Provider calls made while viewing: 0" in output.out
    assert "ACME" not in output.out
    assert "Bullish not priced" not in output.out
    assert "External calls made: 0" in output.out


def _minimal_missing_real_results_payload() -> dict[str, object]:
    return {
        "schema_version": "dashboard-cli-snapshot-v1",
        "controls": {},
        "runtime_context": {},
        "real_results": {
            "schema_version": "dashboard-real-results-v1",
            "status": "missing",
            "headline": "No real result yet.",
            "next_action": (
                "Run/import real market data, then run "
                "`catalyst-radar priced-in-answer --limit 50`."
            ),
            "source": "none",
            "row_count": 0,
            "missing": ["priced-in scan rows"],
            "canned_data_allowed": False,
        },
        "readiness": {},
        "operator_next_step": {},
        "operator_work_queue": {"rows": []},
        "call_plan": {"max_external_call_count": 0},
        "priced_in_queue": {"rows": [], "total_count": 0},
        "priced_in_source_coverage": {},
        "priced_in_source_workflow": {},
        "priced_in_preflight": {},
        "priced_in_answer": {},
        "priced_in_audit": {},
        "candidates": {"rows": []},
        "alerts": {"rows": []},
        "broker": {"snapshot": {}, "exposure": {}},
        "ops_health": {},
        "telemetry": {},
        "external_calls_made": 0,
    }


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
    assert "View: Full scan" in output.out
    assert "Answer:" in output.out
    assert "Trade status:" in output.out
    assert "Trade safe:" in output.out
    assert "Latest scan results - rows" in output.out
    assert "Full scan audit:" in output.out
    assert "Instrument scope:" in output.out
    assert "Decision readiness:" in output.out
    assert "Mailbox" in output.out
    assert "Missing" in output.out
    assert "Next data step:" in output.out
    assert "Full-scan coverage:" in output.out
    assert "Shortlist context:" in output.out
    assert "review page, not the full scan universe" in output.out

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

    assert (
        main(
            [
                "dashboard-tui",
                "--once",
                "--scan-mode",
                "actionable",
                "--usefulness",
                "decision_useful",
                "--page",
                "overview",
            ]
        )
        == 0
    )
    output = capsys.readouterr()

    assert output.err == ""
    assert "Latest scan results - decision-ready not-priced-in rows" in output.out
    assert "These are the actionable answers" in output.out

    assert (
        main(
            [
                "dashboard-tui",
                "--once",
                "--stocks-only",
                "--scan-mode",
                "all",
                "--page",
                "ops",
            ]
        )
        == 0
    )
    output = capsys.readouterr()

    assert output.err == ""
    assert "Source Fill Workflow" in output.out
    assert "stock rows" in output.out
    assert "priced-in-source-batches --source all --stocks-only" in output.out


def test_dashboard_tui_once_defaults_to_latest_scan_results(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'demo.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    assert main(["seed-dashboard-demo"]) == 0
    capsys.readouterr()

    assert main(["dashboard-tui", "--once"]) == 0
    output = capsys.readouterr()

    assert output.err == ""
    assert "Page: overview" in output.out
    assert "Latest scan results - rows" in output.out
    assert "ACME" in output.out
    assert "Urgent" in output.out
    assert "Market Inbox" in output.out
    assert "has market emotion been fully priced in" in output.out
    assert "Tutorial - your first 90 seconds" not in output.out


def test_dashboard_tui_overview_is_novice_first_on_empty_database(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'empty.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    assert main(["dashboard-tui", "--once", "--page", "overview"]) == 0
    output = capsys.readouterr()

    assert output.err == ""
    assert "MarketRadar answers one question" in output.out
    assert "Can I act?" in output.out
    assert "Best next step" in output.out
    assert "No scan rows yet" in output.out
    assert "Start here:" in output.out
    assert "Browsing this dashboard made 0 calls" in output.out
    assert "0 Start" in output.out
    assert "1 Inbox" in output.out
    assert "2 Evidence Gaps" in output.out
    assert "3 Safe Run" in output.out
    assert "4 Candidate Review" in output.out
    assert "10 Agent Coach" in output.out


def test_dashboard_tui_overview_explains_scan_legend_for_novices(
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
    assert "MarketRadar answers one question" in output.out
    assert "Legend:" in output.out
    assert "Emotion" in output.out
    assert "Price reaction" in output.out
    assert "Gap" in output.out
    assert "Decision-ready" in output.out
    assert "NEXT SAFE ACTION" in output.out
    assert "LAST RESPONSE" in output.out
    assert "Cost before execute" in output.out


def test_run_mission_brief_useful_next_prefers_operator_step() -> None:
    payload = {
        "priced_in_answer": {
            "question": "Has price fully matched market expectations?",
            "answer": "Full-market priced-in answer is not ready.",
            "next_action": "Review residual rows before filling bars.",
            "operator_next_step": {
                "action": "Review residual rows before filling bars.",
                "command": (
                    "catalyst-radar market-bars residual-review "
                    "--expected-as-of 2026-05-15"
                ),
                "external_calls_required": 0,
                "db_writes_required": 0,
            },
            "full_market_trust_gate": {
                "status": "blocked",
                "answer": "market bars incomplete",
                "recommended_action": {
                    "kind": "residual_universe_review",
                    "status": "blocked",
                    "reason": "Review residual rows before filling bars.",
                    "cli_command": (
                        "catalyst-radar market-bars residual-review "
                        "--expected-as-of 2026-05-15"
                    ),
                    "external_calls_required": 0,
                    "db_writes_required": 0,
                },
            },
        },
        "priced_in_audit": {
            "sources": [
                {
                    "source": "market_bars",
                    "status": "attention",
                    "gap_count": 579,
                    "next_action": "Fill missing as-of bars.",
                }
            ],
            "source_coverage": {"summary": "market_bars 12090/12669"},
        },
    }

    items = dict(_run_mission_brief_items(payload))

    assert items["Useful next"] == "Review residual rows before filling bars."
    assert "Fill missing" not in items["Useful next"]


def test_dashboard_market_bar_missing_type_summary_is_human_readable() -> None:
    payload = {
        "priced_in_audit": {
            "market_bars": {
                "missing_as_of_bar": 3,
                "repair": {
                    "diagnostic": {
                        "missing_count": 3,
                        "type_counts": {"CS": 2, "UNIT": 1},
                        "company_like_missing_count": 2,
                        "fund_like_missing_count": 0,
                        "wrapper_missing_count": 1,
                        "unknown_missing_count": 0,
                    }
                },
            }
        }
    }

    assert _market_bar_missing_type_summary(payload) == (
        "3 missing scan-date bars; types CS:2, UNIT:1; "
        "company-like 2; fund-like 0; wrappers 1; unknown 0"
    )


def test_priced_in_answer_cli_prints_missing_universe_summary(capsys):
    payload = {
        "status": "blocked",
        "decision_ready": False,
        "can_make_investment_decision": False,
        "total_rows": 0,
        "mismatch_count": 0,
        "research_only_count": 0,
        "external_calls_made": 0,
        "question": "Has price fully matched market expectations?",
        "answer": "Not trusted yet.",
        "investment_decision_boundary": "Not trade approval.",
        "full_market_trust_gate": {
            "status": "blocked",
            "trusted_full_market_answer": False,
            "first_blocker": "market_bars",
            "first_gap_count": 523,
            "scanned_rows": 12087,
            "active_securities": 12613,
            "unscanned_rows": 526,
            "external_calls_made": 0,
            "blocker_detail": {
                "source": "market_bars",
                "missing_as_of_bar": 523,
                "complete_rows": 0,
                "partial_rows": 0,
                "empty_rows": 523,
                "provider_saved_file_status": "missing",
                "external_calls_made": 0,
                "missing_universe": {
                    "active_metadata_rows": 523,
                    "acquisition_or_spac_name_count": 308,
                    "no_composite_figi_count": 440,
                    "zero_avg_dollar_volume_20d_count": 523,
                    "summary": "523/523 missing ticker(s) still active locally.",
                    "external_calls_made": 0,
                },
            },
        },
        "top_rows": [],
    }

    _print_priced_in_answer(payload)
    output = capsys.readouterr()

    assert "trust_gate_universe=active=523" in output.out
    assert "spac_like=308" in output.out
    assert "no_figi=440" in output.out
    assert "zero_volume=523" in output.out
    assert "external_calls=0" in output.out


def test_priced_in_answer_cli_prints_next_source_plan(capsys):
    capture_cli_command = (
        "catalyst-radar market-bars saved-capture "
        "--expected-as-of 2026-05-15 "
        "--out data\\local\\polygon-grouped-daily-2026-05-15.json "
        "--confirm-external-call"
    )
    payload = {
        "status": "blocked",
        "decision_ready": False,
        "can_make_investment_decision": False,
        "external_calls_made": 0,
        "question": "Has price fully matched market expectations?",
        "answer": "Not trusted yet.",
        "investment_decision_boundary": "Not trade approval.",
        "full_market_trust_gate": {
            "status": "blocked",
            "trusted_full_market_answer": False,
            "first_blocker": "market_bars",
            "first_gap_count": 523,
            "scanned_rows": 12087,
            "active_securities": 12613,
            "unscanned_rows": 526,
            "unscanned_blocker_rows": 523,
            "external_calls_made": 0,
            "recommended_action": {
                "schema_version": "priced-in-market-bar-recommended-unblock-v1",
                "kind": "saved_provider_capture",
                "label": "Saved provider capture",
                "status": "approval_required",
                "reason": "Approve one saved grouped-daily provider call.",
                "command": "bars saved capture confirm",
                "cli_command": capture_cli_command,
                "tui_command": "bars saved capture confirm",
                "api": "POST /api/radar/market-bars/provider-fixture-capture",
                "request_body": {"confirm_external_call": True},
                "approval_required": True,
                "external_calls_required": 1,
                "db_writes_required": 0,
                "external_calls_made": 0,
            },
            "blocker_detail": {
                "unblock_options": [
                    {
                        "kind": "saved_provider_capture",
                        "status": "approval_required",
                        "approval_required": True,
                        "external_calls_required": 1,
                        "db_writes_during_step": 0,
                        "command": "bars saved capture confirm",
                        "cli_command": capture_cli_command,
                        "tui_command": "bars saved capture confirm",
                    }
                ],
                "saved_provider_capture": {
                    "status": "approval_required",
                    "approval_required": True,
                    "provider_key_configured": True,
                    "external_calls_if_approved": 1,
                    "db_writes_during_capture": 0,
                    "saved_file_status": "missing",
                    "saved_file_path": (
                        "data\\local\\polygon-grouped-daily-2026-05-15.json"
                    ),
                    "capture_api": (
                        "POST /api/radar/market-bars/provider-fixture-capture"
                    ),
                    "capture_command": "bars saved capture confirm",
                    "capture_cli_command": capture_cli_command,
                    "external_calls_made": 0,
                },
            },
            "after_current_blocker": {
                "current_blocker": "market_bars",
                "next_source": "catalyst_events",
                "next_status": "ready",
                "next_gap_count": 5512,
                "plan_command": (
                    "catalyst-radar priced-in-source-batches "
                    "--source catalyst_events --all --json"
                ),
                "execute_next_command": (
                    "catalyst-radar priced-in-source-batches "
                    "--source catalyst_events --execute-next"
                ),
                "external_calls_made": 0,
                "next_source_plan": {
                    "source": "catalyst_events",
                    "status": "ready",
                    "total_gap_rows": 12075,
                    "plannable_gap_rows": 5510,
                    "routed_gap_rows": 6563,
                    "blocked_gap_rows": 2,
                    "blocked_rows": 2,
                    "blocked_reason": "missing_cik",
                    "batch_count": 1102,
                    "next_chunk_external_calls": 5,
                    "sample_blocked_tickers": ["FRBA", "SSBI"],
                    "sample_routed_non_company_tickers": ["ABLVW"],
                    "fix_command": "catalyst-radar ingest-sec company-tickers",
                    "manual_template_command": (
                        "catalyst-radar ingest-sec cik-overrides-template "
                        "--out data\\local\\cik-overrides-template.csv"
                    ),
                    "manual_validate_command": (
                        "catalyst-radar ingest-sec cik-overrides "
                        "--csv <cik-overrides.csv> --validate-only"
                    ),
                    "manual_fix_command": (
                        "catalyst-radar ingest-sec cik-overrides "
                        "--csv <cik-overrides.csv>"
                    ),
                    "missing_cik": {
                        "missing_cik_company_like_rows": 2,
                        "sample_company_like_missing_cik_tickers": [
                            "FRBA",
                            "SSBI",
                        ],
                    },
                    "external_calls_made": 0,
                },
            },
        },
        "top_rows": [],
    }

    _print_priced_in_answer(payload)
    output = capsys.readouterr()

    assert "trust_gate_recommended_action=kind=saved_provider_capture" in output.out
    assert "approval_required=true" in output.out
    assert "calls=1" in output.out
    assert "db_writes=0" in output.out
    assert "command=catalyst-radar market-bars saved-capture" in output.out
    assert "--confirm-external-call" in output.out
    assert "tui=bars saved capture confirm" in output.out
    assert "trust_gate_unblock=saved_provider_capture" in output.out
    assert "trust_gate_saved_capture=status=approval_required" in output.out
    assert "trust_gate_next_source_plan=source=catalyst_events" in output.out
    assert "gaps=12075" in output.out
    assert "plan=5510" in output.out
    assert "routed=6563" in output.out
    assert "blocked=2" in output.out
    assert "reason=missing_cik" in output.out
    assert "blocked_sample=FRBA,SSBI" in output.out
    assert "trust_gate_next_source_unblock=source=catalyst_events" in output.out
    assert "company_like_missing_cik=2" in output.out
    assert "sample=FRBA,SSBI" in output.out
    assert "fix=catalyst-radar ingest-sec company-tickers" in output.out
    assert "template=catalyst-radar ingest-sec cik-overrides-template" in output.out
    assert "validate=catalyst-radar ingest-sec cik-overrides" in output.out
    assert "import=catalyst-radar ingest-sec cik-overrides" in output.out
    assert "external_calls=0" in output.out

    dashboard_payload = {"priced_in_answer": payload}
    overview = render_dashboard_tui(dashboard_payload, page="overview", width=320)
    run = render_dashboard_tui(dashboard_payload, page="run", width=320)

    assert "Recommended unblock: bars saved capture confirm" in overview
    assert "Recommended unblock" in run
    for rendered in (overview, run):
        assert "bars saved capture confirm" in rendered
        assert "1 provider call(s) if approved; 0 DB write(s)" in rendered
        assert "after market_bars: catalyst_events ready" in rendered
        assert "source plan gaps 12075" in rendered
        assert "next calls 5" in rendered
        assert "plan 5510" in rendered
        assert "routed 6563" in rendered
        assert "blocked 2 missing_cik" in rendered

    assert "missing CIK 2 FRBA, SSBI" in run
    assert "CIK fix `catalyst-radar ingest-sec company-tickers`" in run
    assert "repair `catalyst-radar ingest-sec cik-overrides-template" in run
    assert "validate `catalyst-radar" in run
    assert "ingest-sec cik-overrides --csv" in run
    assert "import `catalyst-radar ingest-sec cik-overrides" in run


def test_dashboard_source_blocker_prefers_dashboard_market_bar_actions() -> None:
    payload = {
        "priced_in_audit": {
            "sources": [
                {
                    "source": "market_bars",
                    "status": "blocked",
                    "command": (
                        "catalyst-radar market-bars template --expected-as-of "
                        "2026-05-15 --out data\\local\\manual-bars-2026-05-15.csv "
                        "--missing-only"
                    ),
                }
            ],
            "market_bars": {
                "repair": {
                    "dashboard_manual_template_command": "bars manual template",
                    "dashboard_manual_import_preview_command": "bars manual import",
                }
            },
        }
    }

    hint = _run_audit_source_blocker_hint(
        payload["priced_in_audit"]["sources"][0],
        payload,
    )

    assert hint == (
        "type `bars manual template` to create the CSV; "
        "type `bars manual import` to preview complete rows; 0 provider calls."
    )


def test_dashboard_manual_bar_fill_progress_summary_is_human_readable() -> None:
    payload = {
        "priced_in_audit": {
            "market_bars": {
                "repair": {
                    "template_row_count": 523,
                    "local_template_path": "data\\local\\manual-bars-2026-05-15.csv",
                    "stock_scope": {
                        "stock_like_active": 5652,
                        "stock_like_with_as_of_bar": 5521,
                        "stock_like_missing_as_of_bar": 131,
                        "next_action": (
                            "Fill stock-like missing as-of bars first; they are "
                            "required before the system can claim a complete "
                            "stocks-only priced-in answer."
                        ),
                        "manual_template_command": (
                            "catalyst-radar market-bars template --expected-as-of "
                            "2026-05-15 --out "
                            "data\\local\\manual-stock-bars-2026-05-15.csv "
                            "--missing-only --stocks-only"
                        ),
                        "operator_step": {
                            "status": "stale_template_schema",
                            "manual_step": False,
                            "external_calls_made": 0,
                            "action": (
                                "Regenerate the blank local CSV so it includes "
                                "name; then fill the named rows."
                            ),
                            "command": (
                                "catalyst-radar market-bars template "
                                "--expected-as-of 2026-05-15 --out "
                                "data\\local\\manual-stock-bars-2026-05-15.csv "
                                "--missing-only --stocks-only --overwrite"
                            ),
                        },
                    },
                    "operator_step": {
                        "status": "fix_partial_rows",
                        "manual_step": True,
                        "external_calls_made": 0,
                        "action": (
                            "Finish or clear partial OHLCV/VWAP rows in "
                            "data\\local\\manual-bars-2026-05-15.csv; partial rows "
                            "cannot be imported."
                        ),
                        "command": (
                            "catalyst-radar market-bars import --daily-bars "
                            "data\\local\\manual-bars-2026-05-15.csv --expected-as-of "
                            "2026-05-15"
                        ),
                        "after_manual_command": (
                            "catalyst-radar market-bars import --daily-bars "
                            "data\\local\\manual-bars-2026-05-15.csv --expected-as-of "
                            "2026-05-15 --complete-rows-only"
                        ),
                    },
                    "dashboard_manual_template_command": "bars manual template",
                    "dashboard_manual_import_preview_command": "bars manual import",
                    "dashboard_manual_import_execute_command": (
                        "bars manual import execute"
                    ),
                    "provider_fill_plan": {
                        "status": "ready_for_approval_with_health_warning",
                        "provider_label": "Polygon/Massive grouped daily",
                        "execute_external_call_count": 1,
                        "external_calls_made": 0,
                        "provider_health_warning": (
                            "Stored Polygon/Massive health is a stale same-day "
                            "EOD denial; the target date is historical."
                        ),
                        "provider_call_command": (
                            "catalyst-radar ingest-polygon grouped-daily "
                            "--date 2026-05-15 --confirm-external-call"
                        ),
                        "provider_saved_file_capture_command": (
                            "catalyst-radar market-bars saved-capture "
                            "--expected-as-of 2026-05-15 --out "
                            "data\\local\\polygon-grouped-daily-2026-05-15.json "
                            "--confirm-external-call"
                        ),
                        "provider_saved_file_capture_external_call_count": 1,
                        "provider_saved_file_capture_request_body": {
                            "expected_as_of": "2026-05-15",
                            "output_path": (
                                "data\\local\\polygon-grouped-daily-2026-05-15.json"
                            ),
                            "confirm_external_call": False,
                            "expected_active_security_count": 12613,
                            "expected_existing_as_of_bar_count": 12090,
                            "expected_missing_as_of_bar_count": 523,
                        },
                        "provider_saved_file_capture_confirm_request_body": {
                            "expected_as_of": "2026-05-15",
                            "output_path": (
                                "data\\local\\polygon-grouped-daily-2026-05-15.json"
                            ),
                            "confirm_external_call": True,
                            "expected_active_security_count": 12613,
                            "expected_existing_as_of_bar_count": 12090,
                            "expected_missing_as_of_bar_count": 523,
                        },
                        "provider_saved_file_capture_approval_packet": {
                            "schema_version": (
                                "market-bars-saved-capture-approval-packet-v1"
                            ),
                            "status": "approval_required",
                            "approval_required": True,
                            "question": (
                                "Approve one Polygon/Massive grouped-daily call "
                                "for 2026-05-15?"
                            ),
                            "expected_as_of": "2026-05-15",
                            "missing_as_of_bar_count": 523,
                            "missing_as_of_bar_ticker_sample": ["AACBR", "AACBU"],
                            "missing_as_of_bar_ticker_more": 521,
                            "approval_guard": {
                                "schema_version": (
                                    "market-bars-saved-capture-approval-guard-v1"
                                ),
                                "expected_as_of": "2026-05-15",
                                "stocks_only": False,
                                "expected_active_security_count": 12613,
                                "expected_existing_as_of_bar_count": 12090,
                                "expected_missing_as_of_bar_count": 523,
                                "external_calls_made": 0,
                                "db_writes_made": 0,
                            },
                            "external_calls_if_approved": 1,
                            "db_writes_during_capture": 0,
                            "tui_confirm_command": "bars saved capture confirm",
                        },
                        "provider_saved_file_validate_request_body": {
                            "expected_as_of": "2026-05-15",
                            "fixture_path": (
                                "data\\local\\polygon-grouped-daily-2026-05-15.json"
                            ),
                        },
                        "provider_saved_file_import_preview_request_body": {
                            "expected_as_of": "2026-05-15",
                            "fixture_path": (
                                "data\\local\\polygon-grouped-daily-2026-05-15.json"
                            ),
                            "execute": False,
                        },
                        "provider_saved_file_import_request_body": {
                            "expected_as_of": "2026-05-15",
                            "fixture_path": (
                                "data\\local\\polygon-grouped-daily-2026-05-15.json"
                            ),
                            "execute": True,
                        },
                        "provider_saved_file_validate_command": (
                            "catalyst-radar market-bars saved-validate "
                            "--expected-as-of 2026-05-15 --fixture "
                            "data\\local\\polygon-grouped-daily-2026-05-15.json"
                        ),
                        "provider_saved_file_import_command": (
                            "catalyst-radar market-bars saved-import "
                            "--expected-as-of 2026-05-15 --fixture "
                            "data\\local\\polygon-grouped-daily-2026-05-15.json"
                        ),
                        "provider_saved_file_exists": False,
                        "provider_saved_file_status": "missing",
                        "provider_saved_file_next_action": (
                            "Capture or obtain the saved grouped-daily JSON response "
                            "before running saved-file validate/import."
                        ),
                        "provider_saved_file_external_call_count": 0,
                    },
                    "local_template_preview": {
                        "status": "invalid",
                        "row_count": 523,
                        "fill_progress": {
                            "complete_rows": 12,
                            "partial_rows": 3,
                            "empty_rows": 508,
                            "filled_rows": 15,
                        },
                    },
                }
            }
        }
    }

    assert _market_bar_manual_fill_progress_summary(payload) == (
        "12/523 complete; 3 partial; 508 empty; 15 touched; preview invalid; "
        "file data\\local\\manual-bars-2026-05-15.csv"
    )
    assert "Command: bars manual import" in _market_bar_operator_step_summary(
        payload
    )
    assert _market_bar_operator_step_summary(payload).startswith(
        "Finish or clear partial OHLCV/VWAP rows"
    )
    assert _market_bar_provider_fill_summary(payload).startswith(
        "ready_for_approval_with_health_warning; 1 external call(s)"
    )
    saved_capture_summary = _market_bar_saved_capture_summary(
        {
            "status": "approval_required",
            "coverage_scope": "active_universe",
            "active_security_count": 12_613,
            "existing_as_of_bar_count": 12_090,
            "missing_as_of_bar_count": 523,
            "saved_file_status": "missing",
            "approval_required": True,
            "provider_key_configured": True,
            "external_calls_if_approved": 1,
            "db_writes_during_capture": 0,
        }
    )
    assert saved_capture_summary.startswith(
        "status approval_required; target scope active_universe, active 12613, "
        "existing 12090, missing 523; saved file missing"
    )
    assert _stock_market_bar_next_summary(payload).startswith(
        "5521/5652 stock-like rows have scan-date bars; 131 missing"
    )
    payload["priced_in_answer"] = {
        "full_market_trust_gate": {
            "status": "blocked",
            "answer": "1/6 evidence layers ready.",
            "blocker_detail": {
                "source": "market_bars",
                "missing_as_of_bar": 523,
                "complete_rows": 12,
                "empty_rows": 508,
                "provider_saved_file_status": "missing",
                "missing_universe": {
                    "schema_version": "priced-in-market-bar-missing-universe-v1",
                    "active_metadata_rows": 523,
                    "acquisition_or_spac_name_count": 308,
                    "no_composite_figi_count": 440,
                    "zero_avg_dollar_volume_20d_count": 523,
                    "summary": "523/523 missing ticker(s) still active locally.",
                    "operator_note": (
                        "This is universe-quality context only. It does not "
                        "exclude rows from the scan or reduce the missing-bar "
                        "requirement."
                    ),
                    "external_calls_made": 0,
                },
            },
        }
    }
    overview = render_dashboard_tui(payload, page="overview", width=160)
    ops = render_dashboard_tui(payload, page="ops", width=160)
    run = render_dashboard_tui(payload, page="run", width=160)
    assert "Stock bar next: 5521/5652 stock-like rows have scan-date bars" in overview
    assert "Stock bar next: 5521/5652 stock-like rows have scan-date bars" in ops
    assert "Regenerate the blank local CSV so it includes name" in overview
    assert "Regenerate the blank local CSV so it includes name" in ops
    assert "Direct provider fill: ready_for_approval_with_health_warning" in overview
    assert "Direct provider fill: ready_for_approval_with_health_warning" in ops
    assert overview.index("Saved file capture") < overview.index("Direct provider fill")
    assert ops.index("Saved file capture") < ops.index("Direct provider fill")
    assert "Saved file capture: approval_required" in overview
    assert "bars targeted; 1 external call(s) if approved" in overview
    assert "type `bars saved capture confirm`" in ops
    assert "manual CSV 12/523 complete" in run
    assert "saved file missing" in run
    assert "Missing universe" in run
    assert "523 active missing-bar rows" in run
    assert "Manual CSV action" in run
    assert "bars manual import" in run
    assert "Saved file capture" in run
    assert "approval_required" in run
    assert "bars saved capture confirm" in run
    assert "Saved file import: missing saved file" in overview
    assert "preview execute=false" in overview
    assert "import execute=true" in ops
    assert "Saved file check" in run
    assert "missing saved file" in run
    assert "validate fixture_path=data\\local\\polygon-grouped-daily" in run
    assert "Saved file import" in run
    assert "preview execute=false" in run
    assert "Manual CSV progress: 12/523 complete; 3 partial; 508 empty" in overview
    assert "Manual CSV progress: 12/523 complete; 3 partial; 508 empty" in ops
    assert "Market bar next: Finish or clear partial OHLCV/VWAP rows" in overview
    assert "Market bar next: Finish or clear partial OHLCV/VWAP rows" in ops


def _saved_file_command_payload(fixture_path, output_path):
    fixture = str(fixture_path)
    output = str(output_path)
    return {
        "priced_in_audit": {
            "market_bars": {
                "repair": {
                    "provider_fill_plan": {
                        "target_as_of": "2026-05-08",
                        "missing_as_of_bar": 3,
                        "provider_saved_file_capture_command": (
                            "catalyst-radar market-bars saved-capture "
                            "--expected-as-of 2026-05-08 --out "
                            f"{output} --confirm-external-call"
                        ),
                        "provider_saved_file_capture_request_body": {
                            "expected_as_of": "2026-05-08",
                            "output_path": output,
                            "confirm_external_call": False,
                            "expected_active_security_count": 3,
                            "expected_existing_as_of_bar_count": 0,
                            "expected_missing_as_of_bar_count": 3,
                        },
                        "provider_saved_file_capture_confirm_request_body": {
                            "expected_as_of": "2026-05-08",
                            "output_path": output,
                            "confirm_external_call": True,
                            "expected_active_security_count": 3,
                            "expected_existing_as_of_bar_count": 0,
                            "expected_missing_as_of_bar_count": 3,
                        },
                        "provider_saved_file_capture_approval_packet": {
                            "schema_version": (
                                "market-bars-saved-capture-approval-packet-v1"
                            ),
                            "status": "approval_required",
                            "approval_required": True,
                            "question": (
                                "Approve one Polygon/Massive grouped-daily call "
                                "for 2026-05-08?"
                            ),
                            "expected_as_of": "2026-05-08",
                            "missing_as_of_bar_count": 3,
                            "missing_as_of_bar_ticker_sample": ["AAPL", "MSFT"],
                            "missing_as_of_bar_ticker_more": 1,
                            "approval_guard": {
                                "schema_version": (
                                    "market-bars-saved-capture-approval-guard-v1"
                                ),
                                "expected_as_of": "2026-05-08",
                                "stocks_only": False,
                                "expected_active_security_count": 3,
                                "expected_existing_as_of_bar_count": 0,
                                "expected_missing_as_of_bar_count": 3,
                                "external_calls_made": 0,
                                "db_writes_made": 0,
                            },
                            "external_calls_if_approved": 1,
                            "db_writes_during_capture": 0,
                            "tui_confirm_command": "bars saved capture confirm",
                        },
                        "provider_saved_file_validate_request_body": {
                            "expected_as_of": "2026-05-08",
                            "fixture_path": fixture,
                        },
                        "provider_saved_file_import_preview_request_body": {
                            "expected_as_of": "2026-05-08",
                            "fixture_path": fixture,
                            "execute": False,
                        },
                        "provider_saved_file_import_request_body": {
                            "expected_as_of": "2026-05-08",
                            "fixture_path": fixture,
                            "execute": True,
                        },
                    }
                }
            }
        }
    }


def _seed_saved_file_command_universe(engine):
    updated_at = datetime(2026, 5, 8, tzinfo=UTC)
    MarketRepository(engine).upsert_securities(
        [
            Security(
                ticker="AAPL",
                name="Apple Inc.",
                exchange="NASDAQ",
                sector="Technology",
                industry="Consumer Electronics",
                market_cap=3_000_000_000_000,
                avg_dollar_volume_20d=10_000_000_000,
                has_options=True,
                is_active=True,
                updated_at=updated_at,
                metadata={"security_type": "CS", "type": "CS"},
            ),
            Security(
                ticker="MSFT",
                name="Microsoft Corp.",
                exchange="NASDAQ",
                sector="Technology",
                industry="Software",
                market_cap=2_800_000_000_000,
                avg_dollar_volume_20d=8_000_000_000,
                has_options=True,
                is_active=True,
                updated_at=updated_at,
                metadata={"security_type": "CS", "type": "CS"},
            ),
            Security(
                ticker="GOOG",
                name="Alphabet Inc.",
                exchange="NASDAQ",
                sector="Communication Services",
                industry="Internet Content",
                market_cap=1_900_000_000_000,
                avg_dollar_volume_20d=5_000_000_000,
                has_options=True,
                is_active=True,
                updated_at=updated_at,
                metadata={"security_type": "CS", "type": "CS"},
            ),
        ]
    )


def test_dashboard_bars_default_shows_zero_call_status(tmp_path: Path):
    database_url = f"sqlite:///{(tmp_path / 'status.db').as_posix()}"
    engine = create_engine(database_url, future=True)
    output_path = tmp_path / "polygon-grouped-daily-2026-05-08.json"
    payload = _saved_file_command_payload(
        Path("tests/fixtures/polygon/grouped_daily_2026-05-08.json"),
        output_path,
    )
    repair = payload["priced_in_audit"]["market_bars"]["repair"]
    repair.update(
        {
            "local_template_path": str(tmp_path / "manual-bars.csv"),
            "template_row_count": 3,
            "local_template_preview": {
                "status": "invalid",
                "row_count": 3,
                "fill_progress": {
                    "complete_rows": 1,
                    "partial_rows": 1,
                    "empty_rows": 1,
                    "filled_rows": 2,
                },
            },
            "operator_step": {
                "action": "Fill or clear incomplete OHLCV/VWAP rows.",
                "manual_step": True,
                "external_calls_made": 0,
            },
            "dashboard_manual_import_preview_command": "bars manual import",
            "dashboard_manual_import_execute_command": "bars manual import execute",
            "missing_as_of_bar_ticker_sample": ["AAPL", "MSFT"],
            "missing_as_of_bar_ticker_more": 1,
            "stock_scope": {
                "stock_like_active": 2,
                "stock_like_with_as_of_bar": 1,
                "stock_like_missing_as_of_bar": 1,
                "non_stock_missing_as_of_bar": 2,
                "sample_missing_stock_like_tickers": ["MSFT"],
                "sample_missing_stock_like_more": 1,
            },
        }
    )
    payload["priced_in_answer"] = {
        "full_market_trust_gate": {
            "status": "blocked",
            "first_blocker": "market_bars",
            "first_gap_count": 3,
            "after_current_blocker": {
                "current_blocker": "market_bars",
                "current_gap_count": 3,
                "next_source": "catalyst_events",
                "next_status": "ready",
                "next_gap_count": 7,
                "next_action": "Inspect SEC catalyst batches after bars clear.",
                "plan_command": (
                    "catalyst-radar priced-in-source-batches "
                    "--source catalyst_events --all --json"
                ),
                "execute_next_command": (
                    "catalyst-radar priced-in-source-batches "
                    "--source catalyst_events --execute-next"
                ),
                "external_calls_made": 0,
                "next_source_plan": {
                    "total_gap_rows": 7,
                    "plannable_gap_rows": 5,
                    "routed_gap_rows": 1,
                    "blocked_gap_rows": 1,
                    "batch_count": 1,
                    "next_chunk_external_calls": 5,
                    "sample_blocked_tickers": ["FRBA"],
                    "sample_routed_non_company_tickers": ["ABLVW"],
                    "external_calls_made": 0,
                },
            },
        }
    }

    update = _apply_command(
        "bars",
        payload,
        "run",
        DashboardFilters(),
        engine=engine,
        config=AppConfig.from_env({"CATALYST_DATABASE_URL": database_url}),
    )

    assert update.page == "run"
    assert (
        "Market-bar status: blocked; as_of=2026-05-08; missing=3"
        in update.message
    )
    assert "Manual CSV: 1/3 complete; 1 partial; 1 empty" in update.message
    assert (
        "Next manual action: Fill or clear incomplete OHLCV/VWAP rows."
        in update.message
    )
    assert "Command: bars manual import; execute after preview" in update.message
    assert "Saved capture: approval_required; 3 bars targeted" in update.message
    assert "1 external call(s) if approved" in update.message
    assert "Recommended: bars saved capture confirm" in update.message
    assert "1 provider call(s) if approved; 0 DB write(s)" in update.message
    assert (
        "Unblock checklist: review counts, approve/capture saved file, "
        "validate saved file, preview import, execute import, "
        "rerun priced-in answer"
    ) in update.message
    assert (
        "After bars clear: after market_bars: catalyst_events ready"
        in update.message
    )
    assert (
        "source plan gaps 7, next calls 5, plan 5, routed 1, blocked 1"
        in update.message
    )
    assert "external calls made 0" in update.message
    assert "Missing sample: AAPL, MSFT plus 1 more" in update.message
    assert (
        "Stock scope: 1/2 stock-like bars present; 1 missing; "
        "2 non-stock missing; sample MSFT plus 1 more; "
        "command bars manual stocks template; 0 provider calls"
    ) in update.message
    assert (
        "Status check made 0 provider calls and 0 database writes."
        in update.message
    )
    assert not output_path.exists()

    status = _apply_command(
        "bars status",
        payload,
        "run",
        DashboardFilters(),
        engine=engine,
        config=AppConfig.from_env({"CATALYST_DATABASE_URL": database_url}),
    )
    assert status.message == update.message


def test_dashboard_bars_saved_capture_requires_confirm_without_call(tmp_path: Path):
    database_url = f"sqlite:///{(tmp_path / 'capture.db').as_posix()}"
    engine = create_engine(database_url, future=True)
    create_schema(engine)
    output_path = tmp_path / "polygon-grouped-daily-2026-05-08.json"
    payload = _saved_file_command_payload(
        Path("tests/fixtures/polygon/grouped_daily_2026-05-08.json"),
        output_path,
    )

    update = _apply_command(
        "bars saved capture",
        payload,
        "run",
        DashboardFilters(),
        engine=engine,
        config=AppConfig.from_env({"CATALYST_DATABASE_URL": database_url}),
    )

    assert update.page == "run"
    assert "approval-gated" in update.message
    assert "status=approval_required" in update.message
    assert "external_calls_made=0" in update.message
    assert "db_writes_made=0" in update.message
    assert "target=2026-05-08" in update.message
    assert "current_missing=3" in update.message
    assert "missing_sample=AAPL, MSFT plus 1 more" in update.message
    assert "confirm_external_call=false" in update.message
    assert "bars saved import` to preview" in update.message
    assert "bars saved capture confirm" in update.message
    assert not output_path.exists()


def test_dashboard_bars_saved_capture_confirm_reports_post_capture_preview(
    tmp_path: Path,
    monkeypatch,
):
    database_path = tmp_path / "capture-confirm.db"
    database_url = f"sqlite:///{database_path.as_posix()}"
    engine = create_engine(database_url, future=True)
    create_schema(engine)
    _seed_saved_file_command_universe(engine)
    output_path = tmp_path / "polygon-grouped-daily-2026-05-08.json"
    payload = _saved_file_command_payload(
        Path("tests/fixtures/polygon/grouped_daily_2026-05-08.json"),
        output_path,
    )

    def fake_capture(**kwargs):
        saved_path = kwargs["output_path"]
        saved_path.write_text("{}", encoding="utf-8")
        return {
            "status": "ready",
            "source": "fixture",
            "bytes_written": 2,
            "external_calls_made": 0,
            "output_path": str(saved_path),
            "post_capture_preview": {
                "status": "ready",
                "daily_bar_count": 2,
                "rejected_count": 0,
                "external_calls_made": 0,
                "db_writes_made": 0,
                "coverage": {
                    "missing_covered_by_fixture_count": 2,
                    "missing_after_import_count": 1,
                    "stock_like_missing_after_import_count": 1,
                },
            },
        }

    monkeypatch.setattr(
        "catalyst_radar.dashboard.tui.capture_polygon_grouped_daily_response_with_preview",
        fake_capture,
    )
    update = _apply_command(
        "bars saved capture confirm",
        payload,
        "run",
        DashboardFilters(),
        engine=engine,
        config=AppConfig.from_env({"CATALYST_DATABASE_URL": database_url}),
    )

    assert update.page == "run"
    assert "Saved-file capture completed" in update.message
    assert "Post-capture preview: status=ready" in update.message
    assert "Post-capture verification: status=preview_only" in update.message
    assert "projected_missing=1" in update.message
    assert "projection=would_still_block_market_bars" in update.message
    assert "missing_covered=2" in update.message
    assert "missing_after_import=1" in update.message
    assert "external_calls=0" in update.message
    assert "bars saved import execute" in update.message


def test_dashboard_bars_saved_capture_confirm_blocks_stale_approval_guard(
    tmp_path: Path,
    monkeypatch,
):
    database_url = f"sqlite:///{(tmp_path / 'capture-stale.db').as_posix()}"
    engine = create_engine(database_url, future=True)
    create_schema(engine)
    _seed_saved_file_command_universe(engine)
    output_path = tmp_path / "polygon-grouped-daily-2026-05-08.json"
    payload = _saved_file_command_payload(
        Path("tests/fixtures/polygon/grouped_daily_2026-05-08.json"),
        output_path,
    )
    repair = payload["priced_in_audit"]["market_bars"]["repair"]
    plan = repair["provider_fill_plan"]
    plan["provider_saved_file_capture_confirm_request_body"][
        "expected_missing_as_of_bar_count"
    ] = 99
    plan["provider_saved_file_capture_approval_packet"]["approval_guard"][
        "expected_missing_as_of_bar_count"
    ] = 99

    def fail_capture(**kwargs):
        raise AssertionError("provider capture should not run")

    monkeypatch.setattr(
        "catalyst_radar.dashboard.tui.capture_polygon_grouped_daily_response_with_preview",
        fail_capture,
    )
    update = _apply_command(
        "bars saved capture confirm",
        payload,
        "run",
        DashboardFilters(),
        engine=engine,
        config=AppConfig.from_env({"CATALYST_DATABASE_URL": database_url}),
    )

    assert update.page == "run"
    assert "blocked by stale approval guard" in update.message
    assert "missing_as_of_bar_count expected=99 current=3" in update.message
    assert "external_calls=0" in update.message
    assert "db_writes=0" in update.message
    assert not output_path.exists()


def test_dashboard_bars_saved_validate_and_import_fixture_are_operator_actions(
    tmp_path: Path,
):
    database_url = f"sqlite:///{(tmp_path / 'import.db').as_posix()}"
    engine = create_engine(database_url, future=True)
    create_schema(engine)
    _seed_saved_file_command_universe(engine)
    fixture_path = Path("tests/fixtures/polygon/grouped_daily_2026-05-08.json")
    payload = _saved_file_command_payload(
        fixture_path,
        tmp_path / "polygon-grouped-daily-2026-05-08.json",
    )
    plan = payload["priced_in_audit"]["market_bars"]["repair"]["provider_fill_plan"]
    plan["coverage_scope"] = "stock_like"
    for key in (
        "provider_saved_file_validate_request_body",
        "provider_saved_file_import_preview_request_body",
        "provider_saved_file_import_request_body",
    ):
        plan[key]["stocks_only"] = True
    config = AppConfig.from_env({"CATALYST_DATABASE_URL": database_url})

    validate = _apply_command(
        "bars saved validate",
        payload,
        "run",
        DashboardFilters(),
        engine=engine,
        config=config,
    )
    assert "Saved-file validate: status=ready_with_rejections" in validate.message
    assert "scope=stock_like" in validate.message
    assert "external_calls=0" in validate.message
    assert "db_writes=0" in validate.message
    assert "missing_after_import=1" in validate.message

    preview = _apply_command(
        "bars saved import",
        payload,
        "run",
        DashboardFilters(),
        engine=engine,
        config=config,
    )
    assert "Saved-file import preview: status=ready_with_rejections" in preview.message
    assert "scope=stock_like" in preview.message
    assert "external_calls=0" in preview.message
    assert "db_writes=0" in preview.message
    assert "Post-import: status=preview_only; missing=3" in preview.message
    assert "projected_missing=1; projection=would_still_block_market_bars" in preview.message
    assert "bars saved import execute" in preview.message

    execute = _apply_command(
        "bars saved import execute",
        payload,
        "run",
        DashboardFilters(),
        engine=engine,
        config=config,
    )
    assert "Saved-file import executed" in execute.message
    assert "daily_bars=6" in execute.message
    assert "rejected=1" in execute.message
    assert "external_calls=0" in execute.message
    assert "db_writes=1" in execute.message
    assert "Post-import: status=market_bars_still_blocked; missing=1" in execute.message

    with engine.connect() as conn:
        assert conn.execute(select(func.count()).select_from(daily_bars)).scalar_one() == 6


def _manual_bar_command_payload(template_path):
    return {
        "priced_in_audit": {
            "market_bars": {
                "repair": {
                    "target_as_of": "2026-05-08",
                    "local_template_path": str(template_path.with_name("manual-full.csv")),
                    "stock_scope": {
                        "target_as_of": "2026-05-08",
                        "stock_like_active": 3,
                        "stock_like_with_as_of_bar": 0,
                        "stock_like_missing_as_of_bar": 3,
                        "local_template_path": str(template_path),
                    },
                }
            }
        }
    }


def _fill_first_manual_bar(template_path):
    with template_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
        fieldnames = list(rows[0].keys())
    rows[0].update(
        {
            "open": "100",
            "high": "101",
            "low": "99",
            "close": "100.50",
            "volume": "1000000",
            "vwap": "100.25",
        }
    )
    with template_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def test_dashboard_bars_manual_template_and_import_are_zero_call_actions(
    tmp_path: Path,
):
    database_url = f"sqlite:///{(tmp_path / 'manual-bars.db').as_posix()}"
    engine = create_engine(database_url, future=True)
    create_schema(engine)
    _seed_saved_file_command_universe(engine)
    stock_template = tmp_path / "manual-stock-bars-2026-05-08.csv"
    full_template = stock_template.with_name("manual-full.csv")
    payload = _manual_bar_command_payload(stock_template)
    config = AppConfig.from_env({"CATALYST_DATABASE_URL": database_url})

    template = _apply_command(
        "bars manual template",
        payload,
        "run",
        DashboardFilters(),
        engine=engine,
        config=config,
    )
    assert "Manual market-bar template ready" in template.message
    assert "rows=3" in template.message
    assert "stocks_only=false" in template.message
    assert "external_calls=0" in template.message
    assert full_template.exists()
    assert not stock_template.exists()

    _fill_first_manual_bar(full_template)
    preview = _apply_command(
        "bars manual import",
        payload,
        "run",
        DashboardFilters(),
        engine=engine,
        config=config,
    )
    assert "Manual market-bar import preview: status=ready_partial" in preview.message
    assert "complete_rows_only=true" in preview.message
    assert "complete=1" in preview.message
    assert "empty=2" in preview.message
    assert "missing_after_import=2" in preview.message
    assert "external_calls=0" in preview.message
    assert "db_writes=0" in preview.message
    assert "projected_missing=2; projection=would_still_block_market_bars" in preview.message

    execute = _apply_command(
        "bars manual import execute",
        payload,
        "run",
        DashboardFilters(),
        engine=engine,
        config=config,
    )
    assert "Manual market-bar import executed: status=partial_imported" in execute.message
    assert "external_calls=0" in execute.message
    assert "db_writes=1" in execute.message
    assert "Post-import: status=market_bars_still_blocked; missing=2" in execute.message

    with engine.connect() as conn:
        assert conn.execute(select(func.count()).select_from(daily_bars)).scalar_one() == 1


def test_dashboard_bars_manual_stocks_scope_uses_stock_template_path(tmp_path: Path):
    database_url = f"sqlite:///{(tmp_path / 'manual-full-bars.db').as_posix()}"
    engine = create_engine(database_url, future=True)
    create_schema(engine)
    _seed_saved_file_command_universe(engine)
    stock_template = tmp_path / "manual-stock-bars-2026-05-08.csv"
    full_template = stock_template.with_name("manual-full.csv")
    payload = _manual_bar_command_payload(stock_template)

    update = _apply_command(
        "bars manual stocks template",
        payload,
        "run",
        DashboardFilters(),
        engine=engine,
        config=AppConfig.from_env({"CATALYST_DATABASE_URL": database_url}),
    )

    assert "Manual market-bar template ready" in update.message
    assert "stocks_only=true" in update.message
    assert stock_template.exists()
    assert not full_template.exists()


def test_dashboard_options_fixture_commands_are_zero_call_operator_actions(
    tmp_path: Path,
    monkeypatch,
):
    database_url = f"sqlite:///{(tmp_path / 'options.db').as_posix()}"
    engine = create_engine(database_url, future=True)
    create_schema(engine)
    monkeypatch.chdir(tmp_path)

    def fake_template_payload(*args, **kwargs):
        return {
            "schema_version": "options-fixture-template-v1",
            "status": "ready",
            "provider": "manual",
            "live": False,
            "external_calls_made": 0,
            "source": "options",
            "stocks_only": bool(kwargs.get("stocks_only")),
            "source_gap_rows": 1,
            "row_count": 1,
            "target_as_of": "2026-05-10T21:00:00+00:00",
            "target_date": "2026-05-10",
            "fixture": {
                "as_of": "2026-05-10T21:00:00+00:00",
                "source_ts": "2026-05-10T21:00:00+00:00",
                "available_at": "2026-05-10T21:00:00+00:00",
                "provider": "options_fixture",
                "results": [
                    {
                        "ticker": "ACME",
                        "call_volume": "",
                        "put_volume": "",
                        "call_open_interest": "",
                        "put_open_interest": "",
                        "iv_percentile": "",
                        "skew": "",
                    }
                ],
            },
        }

    monkeypatch.setattr(
        "catalyst_radar.dashboard.tui.dashboard_data.options_fixture_template_payload",
        fake_template_payload,
    )
    config = AppConfig.from_env({"CATALYST_DATABASE_URL": database_url})

    template = _apply_command(
        "options template",
        {},
        "run",
        DashboardFilters(),
        engine=engine,
        config=config,
    )
    fixture_path = tmp_path / "data" / "local" / "point-in-time-options-2026-05-10.json"
    assert "Options fixture template ready" in template.message
    assert "rows=1" in template.message
    assert "external_calls=0" in template.message
    assert "db_writes=0" in template.message
    assert fixture_path.exists()

    invalid = _apply_command(
        "options validate",
        {},
        "run",
        DashboardFilters(),
        engine=engine,
        config=config,
    )
    assert "Options fixture validation: status=invalid" in invalid.message
    assert "blank_required=6" in invalid.message
    assert "external_calls=0" in invalid.message
    assert "db_writes=0" in invalid.message

    fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
    fixture["results"][0].update(
        {
            "call_volume": "1200",
            "put_volume": "800",
            "call_open_interest": "5000",
            "put_open_interest": "4100",
            "iv_percentile": "0.64",
            "skew": "-0.08",
        }
    )
    with fixture_path.open("w", encoding="utf-8") as handle:
        json.dump(fixture, handle, indent=2)

    preview = _apply_command(
        "options import",
        {},
        "run",
        DashboardFilters(),
        engine=engine,
        config=config,
    )
    assert "Options fixture import preview: status=ready" in preview.message
    assert "valid=1" in preview.message
    assert "external_calls=0" in preview.message
    assert "db_writes=0" in preview.message
    assert "options import execute" in preview.message

    with engine.connect() as conn:
        assert conn.execute(select(func.count()).select_from(option_features)).scalar_one() == 0

    executed = _apply_command(
        "options import execute",
        {},
        "run",
        DashboardFilters(),
        engine=engine,
        config=config,
    )
    assert "Options fixture import executed" in executed.message
    assert "option_features=1" in executed.message
    assert "external_calls=0" in executed.message
    assert "db_writes=1" in executed.message

    with engine.connect() as conn:
        assert conn.execute(select(func.count()).select_from(option_features)).scalar_one() == 1



def test_dashboard_sec_cik_commands_are_zero_call_operator_actions(
    tmp_path: Path,
    monkeypatch,
):
    database_url = f"sqlite:///{(tmp_path / 'sec-cik.db').as_posix()}"
    engine = create_engine(database_url, future=True)
    create_schema(engine)
    monkeypatch.chdir(tmp_path)
    MarketRepository(engine).upsert_securities(
        [
            Security(
                ticker="FRBA",
                name="First Bank",
                exchange="NASDAQ",
                sector="Financials",
                industry="Banks",
                market_cap=500_000_000,
                avg_dollar_volume_20d=5_000_000,
                has_options=True,
                is_active=True,
                updated_at=datetime(2026, 5, 10, tzinfo=UTC),
                metadata={"security_type": "CS", "type": "CS"},
            )
        ]
    )

    def fake_template_payload(*args, **kwargs):
        return {
            "schema_version": "sec-cik-override-template-v1",
            "status": "ready",
            "provider": "manual",
            "live": False,
            "external_calls_made": 0,
            "source": "catalyst_events",
            "stocks_only": bool(kwargs.get("stocks_only")),
            "source_gap_rows": 1,
            "row_count": 1,
            "columns": [
                "ticker",
                "cik",
                "sec_company_name",
                "security_type",
                "template_reason",
            ],
            "rows": [
                {
                    "ticker": "FRBA",
                    "cik": "0001504008",
                    "sec_company_name": "First Bank",
                    "security_type": "CS",
                    "template_reason": "missing_sec_cik_for_catalyst_events_source_gap",
                }
            ],
        }

    monkeypatch.setattr(
        "catalyst_radar.dashboard.tui.dashboard_data.sec_cik_override_template_payload",
        fake_template_payload,
    )
    config = AppConfig.from_env({"CATALYST_DATABASE_URL": database_url})

    template = _apply_command(
        "cik template",
        {},
        "run",
        DashboardFilters(),
        engine=engine,
        config=config,
    )
    csv_path = tmp_path / "data" / "local" / "cik-overrides-template.csv"
    assert template.page == "ops"
    assert "SEC CIK template ready" in template.message
    assert "rows=1" in template.message
    assert "external_calls=0" in template.message
    assert "db_writes=0" in template.message
    assert csv_path.exists()

    preview = _apply_command(
        "sec cik import",
        {},
        "run",
        DashboardFilters(),
        engine=engine,
        config=config,
    )
    assert "SEC CIK import preview: status=ready" in preview.message
    assert "updates=1" in preview.message
    assert "external_calls=0" in preview.message
    assert "db_writes=0" in preview.message
    assert "cik import execute" in preview.message

    with engine.connect() as conn:
        metadata = conn.execute(
            select(securities.c.metadata).where(securities.c.ticker == "FRBA")
        ).scalar_one()
    assert "cik" not in metadata

    executed = _apply_command(
        "cik import execute",
        {},
        "run",
        DashboardFilters(),
        engine=engine,
        config=config,
    )
    assert "SEC CIK import executed" in executed.message
    assert "updated=1" in executed.message
    assert "external_calls=0" in executed.message
    assert "db_writes=1" in executed.message

    with engine.connect() as conn:
        metadata = conn.execute(
            select(securities.c.metadata).where(securities.c.ticker == "FRBA")
        ).scalar_one()
    assert metadata["cik"] == "0001504008"
    assert metadata["cik_source"] == "manual_cik_override"

    help_screen = render_dashboard_tui({}, page="help", width=140)
    assert "cik template" in help_screen
    assert "cik validate" in help_screen
    assert "cik import" in help_screen

def test_dashboard_start_page_alias_opens_latest_scan_results() -> None:
    screen = render_dashboard_tui({}, page="start", width=120)

    assert "Page: overview" in screen
    assert "Market Inbox" in screen
    assert "Latest scan results" in screen


def test_dashboard_review_page_is_distinct_from_full_scan() -> None:
    review_filters = dashboard_filters_for_page(DashboardFilters(), "review")
    assert review_filters.priced_in_status == "actionable"
    assert review_filters.priced_in_usefulness == "decision_useful"
    assert review_filters.priced_in_offset == 0

    overview_filters = dashboard_filters_for_page(DashboardFilters(), "overview")
    assert overview_filters.priced_in_status == "all"
    assert overview_filters.priced_in_usefulness is None

    payload = {
        "runtime_context": {
            "database": {"name": "demo.db"},
            "build": {"commit": "test"},
        },
        "controls": {"ticker": None, "available_at": None},
        "external_calls_made": 0,
        "readiness": {
            "status": "research_only",
            "safe_to_make_investment_decision": False,
            "headline": "Current rows are research only.",
        },
        "priced_in_answer": {
            "status": "decision_ready",
            "decision_ready": True,
            "answer": "Not fully priced for 1 decision-ready row.",
            "investment_boundary": "Not trade approval.",
        },
        "priced_in_queue": {
            "status": "ready",
            "count": 2,
            "returned_count": 2,
            "total_count": 2,
            "offset": 0,
            "filters": {"status": "all", "usefulness": None, "limit": 50},
            "scan": {"scanned_candidate_states": 2},
            "rows": [
                {
                    "ticker": "ACME",
                    "priced_in_status": "bullish_not_priced_in",
                    "emotion_score": 80,
                    "reaction_score": 25,
                    "emotion_reaction_gap": 55,
                    "candidate_theme": "margin_inflection",
                    "data_sources": {
                        "available": ["market_bars", "catalyst_events", "local_text"],
                        "missing": ["options", "broker_context"],
                        "stale": [],
                    },
                    "usefulness": {
                        "status": "decision_useful",
                        "decision_ready": True,
                        "optional_context_gaps": ["options", "broker_context"],
                        "missing_for_decision": [],
                    },
                    "priced_in_evidence_brief": {
                        "evidence": [
                            {"title": "Margins inflecting", "source": "local_text"}
                        ]
                    },
                },
                {
                    "ticker": "BETA",
                    "priced_in_status": "bullish_not_priced_in",
                    "emotion_score": 70,
                    "reaction_score": 20,
                    "emotion_reaction_gap": 50,
                    "candidate_theme": "product_launch",
                    "data_sources": {
                        "available": ["market_bars"],
                        "missing": ["candidate_packet", "decision_card"],
                        "stale": [],
                    },
                    "usefulness": {
                        "status": "research_useful",
                        "decision_ready": False,
                    },
                },
            ],
        },
    }

    review_rows = _priced_in_review_rows(payload)
    assert [row["ticker"] for row in review_rows] == ["ACME"]
    assert review_rows[0]["optional_gaps"] == "options, broker_context"

    review = render_dashboard_tui(payload, page="review", width=140)
    assert "Decision Review" in review
    assert "not trade approval" in review
    assert "ACME" in review
    assert "options" in review
    assert "broker" in review
    assert "BETA" not in review

    overview = render_dashboard_tui(payload, page="overview", width=140)
    assert "Market Inbox" in overview
    assert "Latest scan results" in overview
    assert "ACME" in overview
    assert "BETA" in overview

    case = render_dashboard_tui(payload, page="candidate:ACME", width=180)
    assert "Candidate ACME" in case
    assert "ACME: no trade decision yet" in case
    assert "Fix source gaps" in case
    assert "Use the workflow navigation or open the highlighted row" not in case

    inbox_rows = _market_inbox_rows(payload)
    assert [row["mailbox"] for row in inbox_rows] == ["Urgent", "Waiting Evidence"]
    assert inbox_rows[0]["subject"].startswith("Bullish not priced")
    assert inbox_rows[0]["missing"] == "missing options, broker_context"
    assert "Open the case file" in inbox_rows[0]["next"]


def test_market_inbox_distinguishes_visible_page_from_full_queue() -> None:
    payload = {
        "controls": {"ticker": None, "available_at": None},
        "external_calls_made": 0,
        "readiness": {
            "status": "research_only",
            "safe_to_make_investment_decision": False,
            "headline": "Current rows are research only.",
        },
        "priced_in_answer": {
            "status": "blocked",
            "decision_ready": False,
            "answer": "Full answer is blocked until source gaps are filled.",
        },
        "priced_in_queue": {
            "status": "ready",
            "count": 2,
            "returned_count": 2,
            "total_count": 120,
            "offset": 50,
            "filters": {"status": "all", "usefulness": None, "limit": 2},
            "scan": {"scanned_candidate_states": 120},
            "usefulness_counts": {
                "research_useful": 9,
                "blocked": 58,
                "monitor_only": 53,
            },
            "rows": [
                {
                    "ticker": "ACME",
                    "priced_in_status": "bullish_not_priced_in",
                    "emotion_score": 80,
                    "reaction_score": 25,
                    "emotion_reaction_gap": 55,
                    "candidate_theme": "margin_inflection",
                    "data_sources": {
                        "available": ["market_bars"],
                        "missing": ["options"],
                        "stale": [],
                    },
                    "usefulness": {
                        "status": "research_useful",
                        "decision_ready": False,
                    },
                },
                {
                    "ticker": "BETA",
                    "priced_in_status": "neutral",
                    "emotion_score": 45,
                    "reaction_score": 45,
                    "emotion_reaction_gap": 0,
                    "candidate_theme": "monitoring",
                    "data_sources": {
                        "available": ["market_bars"],
                        "missing": ["catalyst_events"],
                        "stale": [],
                    },
                    "usefulness": {
                        "status": "monitor_only",
                        "decision_ready": False,
                    },
                },
            ],
        },
    }

    overview = render_dashboard_tui(payload, page="overview", width=180)

    assert (
        "Inbox summary: Visible page: 2 waiting evidence. "
        "Queue total: 120; research 9 / blocked 58 / monitor 53."
    ) in overview
    assert (
        "Visible page: 2 waiting evidence. "
        "Queue total: 120; research 9 / blocked 58 / monitor 53."
    ) in overview
    assert "No decision work on this page yet. Press 2 Evidence Gaps" in overview
    assert "Current queue: 2 waiting evidence" not in overview


def test_evidence_gaps_footer_names_first_must_fix_gap() -> None:
    payload = {
        "controls": {"ticker": None, "available_at": None},
        "external_calls_made": 0,
        "readiness": {
            "status": "research_only",
            "decision_mode": "research_only",
            "headline": "Evidence gaps remain.",
            "next_action": "Clear source gaps before acting.",
            "readiness_checklist": [],
        },
        "shadow_readiness": {},
        "operator_work_queue": {
            "status": "blocked",
            "headline": "2 setup blockers remain.",
            "rows": [
                {
                    "priority": "attention",
                    "area": "Research loop",
                    "item": "No telemetry yet.",
                    "next_action": "Run the radar once in dry-run mode.",
                },
                {
                    "priority": "must_fix",
                    "area": "Live market scan",
                    "item": "Market bars are incomplete.",
                    "next_action": "Fill market bars or open Ops for the source plan.",
                },
            ],
        },
    }

    readiness = render_dashboard_tui(payload, page="readiness", width=150)

    assert "Readiness And Work Queue" in readiness
    assert "First must fix: Live market scan" in readiness
    assert "Research-only" in readiness
    assert "Use the workflow navigation or open the highlighted row" not in readiness


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
        },
        "priced_in_answer": {
            "scan_scope": {
                "full_scan_export_command": (
                    "catalyst-radar priced-in-queue --full-scan --all --json"
                ),
                "current_filter_export_command": (
                    "catalyst-radar priced-in-queue --full-scan --all --json"
                ),
            },
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

    export_update = _apply_command(
        "export full",
        payload,
        "overview",
        filters,
        engine=engine,
        config=config,
    )
    assert export_update.page == "overview"
    assert export_update.message == (
        "Full-scan export command: "
        "catalyst-radar priced-in-queue --full-scan --all --json"
    )

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

    ready_update = _apply_command(
        "ready",
        payload,
        "overview",
        DashboardFilters(priced_in_status="all"),
        engine=engine,
        config=config,
    )
    assert ready_update.page == "review"
    assert ready_update.filters.priced_in_status == "actionable"
    assert ready_update.filters.priced_in_usefulness == "decision_useful"
    assert ready_update.filters.priced_in_offset == 0
    assert "Decision-ready view" in ready_update.message

    full_update = _apply_command(
        "full",
        payload,
        "overview",
        ready_update.filters,
        engine=engine,
        config=config,
    )
    assert full_update.filters.priced_in_status == "all"
    assert full_update.filters.priced_in_usefulness is None


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
    assert payload["runtime"]["schema_version"] == "market-radar-agent-runtime-v1"
    assert payload["runtime"]["orchestrator"] == "openai_agents_sdk"
    assert payload["runtime"]["copilot_dependency"] == "absent"
    assert payload["runtime"]["external_market_tools"] is False
    assert payload["runtime"]["broker_tools"] is False
    assert payload["runtime"]["shell_tools"] is False
    assert payload["runtime"]["web_tools"] is False
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
    assert payload["usefulness_counts"] == {"decision_useful": 1}
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
        "catalyst-radar priced-in-source-batches --source options --all --json"
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

    assert main(["priced-in-queue", "--usefulness", "decision_useful", "--json"]) == 0
    output = capsys.readouterr()
    filtered_payload = json.loads(output.out)

    assert output.err == ""
    assert filtered_payload["filters"]["usefulness"] == "decision_useful"
    assert filtered_payload["rows"][0]["usefulness"]["status"] == "decision_useful"

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

    assert main(["priced-in-queue", "--decision-ready", "--json"]) == 0
    output = capsys.readouterr()
    decision_ready_payload = json.loads(output.out)

    assert output.err == ""
    assert decision_ready_payload["filters"]["status"] == "actionable"
    assert decision_ready_payload["filters"]["usefulness"] == "decision_useful"
    assert decision_ready_payload["usefulness_counts"] == {"decision_useful": 1}
    assert decision_ready_payload["rows"][0]["usefulness"]["decision_ready"] is True

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
    assert decision_gap_payload["count"] == 0

    assert main(["priced-in-queue"]) == 0
    output = capsys.readouterr()

    assert output.err == ""
    assert "usefulness_counts=decision_useful:1" in output.out
    assert "scan_scope=scanned=" in output.out
    assert "visible_page=1" in output.out
    assert "source_actions:" in output.out
    assert "options status=missing" in output.out
    assert "gap_rows=1" in output.out
    assert "example_tickers=ACME" in output.out
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
    assert batch_payload["scan_scope"]["mode"] == "full_scan"
    assert batch_payload["scan_scope"]["full_scan_gap_rows"] == 1
    assert batch_payload["scan_scope"]["returned_tickers"] == 1
    assert batch_payload["scan_scope"]["tickers_are_batch_sample"] is False
    assert batch_payload["count"] == 1
    assert batch_payload["batches"][0]["tickers"] == ["ACME"]
    assert batch_payload["approval_checklist"]["approval_required"] is True
    assert batch_payload["approval_checklist"]["provider"] == "schwab"
    assert batch_payload["approval_checklist"]["trade_order_submission_allowed"] is False

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
            ]
        )
        == 0
    )
    output = capsys.readouterr()

    assert "scan_scope=mode=full_scan" in output.out
    assert "returned_tickers=1" in output.out
    assert "ticker_scope=returned_provider_batches" in output.out
    assert "ticker_scope_note=Returned tickers cover every currently returned" in output.out
    assert "approval_checklist=required=true provider=schwab" in output.out
    assert "approval_4=No trading permission" in output.out
    assert "scope_note=The full scan covers every matching ranked row" in output.out
    assert (
        "review_full_scan_source_gap=catalyst-radar priced-in-queue "
        "--full-scan --source-gap options --limit 50"
    ) in output.out
    assert (
        "export_full_scan_source_gap=catalyst-radar priced-in-queue "
        "--full-scan --source-gap options --all --json"
    ) in output.out

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
                "--all",
                "--json",
            ]
        )
        == 0
    )
    output = capsys.readouterr()
    all_batch_payload = json.loads(output.out)

    assert output.err == ""
    assert all_batch_payload["external_calls_made"] == 0
    assert all_batch_payload["all_batches"] is True
    assert all_batch_payload["count"] == all_batch_payload["batch_count"]
    assert all_batch_payload["next_batch_command"] is None

    assert (
        main(
            [
                "priced-in-source-batches",
                "--source",
                "all",
                "--limit",
                "1",
                "--json",
            ]
        )
        == 0
    )
    output = capsys.readouterr()
    overview = json.loads(output.out)

    assert output.err == ""
    assert overview["schema_version"] == "priced-in-source-batch-overview-v1"
    assert overview["external_calls_made"] == 0
    assert overview["scan_scope"]["schema_version"] == (
        "priced-in-source-overview-scan-scope-v1"
    )
    assert overview["scan_scope"]["mode"] == "full_scan"
    assert overview["scan_scope"]["examples_are_samples"] is True
    assert overview["goal_alignment"]["schema_version"] == (
        "priced-in-goal-alignment-v1"
    )
    assert "market emotion" in overview["goal_alignment"]["goal"]
    assert "fresh price reaction" in overview["goal_alignment"]["useful_definition"]
    assert overview["mission_brief"]["schema_version"] == "priced-in-mission-brief-v1"
    assert "market emotion" in overview["mission_brief"]["question"]
    assert overview["mission_brief"]["scan_progress"]["source_gap_rows"] == (
        overview["total_gap_rows"]
    )
    assert overview["mission_brief"]["roadmap"]
    source_rows = {row["source"]: row for row in overview["sources"]}
    assert source_rows["options"]["execute_next_command"] == (
        "catalyst-radar priced-in-source-batches --source options --execute-next"
    )
    assert source_rows["options"]["plan_command"] == (
        "catalyst-radar priced-in-source-batches --source options --all --json"
    )
    assert source_rows["options"]["command"] == source_rows["options"]["plan_command"]
    assert source_rows["options"]["first_batch"]["tickers"] == ["ACME"]
    assert source_rows["options"]["approval_checklist"]["approval_required"] is True

    assert (
        main(["priced-in-source-batches", "--source", "all", "--limit", "1"]) == 0
    )
    output = capsys.readouterr()

    assert output.err == ""
    assert "full_scan=mode=full_scan" in output.out
    assert "examples_are_samples=true" in output.out
    assert "scope_note=The full scan covers" in output.out
    assert "goal_alignment=status=aligned" in output.out
    assert "goal=Find stocks where market emotion" in output.out
    assert "mission_brief=question=Which stocks" in output.out
    assert "  answer=" in output.out
    assert "  roadmap=" in output.out
    assert "next_useful_step=" in output.out
    assert "approval_checklist=required=true provider=schwab" in output.out
    assert "full_scan_review=catalyst-radar priced-in-queue --full-scan --limit 50" in output.out
    assert (
        "full_scan_export=catalyst-radar priced-in-queue --full-scan --all --json"
        in output.out
    )
    assert "coverage_first_batch=" in output.out
    assert "scope=first_provider_chunk" in output.out
    assert "tickers=ACME" in output.out
    assert "calls=" in output.out

    assert (
        main(["priced-in-source-batches", "--source", "all", "--execute-next"]) == 2
    )
    output = capsys.readouterr()
    assert "source all is plan-only" in output.err


def test_priced_in_queue_cli_prints_non_company_evidence_route(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'demo.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    def fake_payload(*_args, **_kwargs):
        return {
            "schema_version": "priced-in-queue-v1",
            "status": "ready",
            "count": 1,
            "total_count": 1,
            "offset": 0,
            "has_more": False,
            "external_calls_made": 0,
            "filters": {"status": "all", "limit": 50, "offset": 0},
            "scan": {"scanned_securities": 1, "requested_securities": 0},
            "headline": "Latest full scan ranked 1 priced-in row(s); showing 1-1 of 1.",
            "next_action": "Review the largest emotion-versus-reaction gaps first.",
            "usefulness_counts": {"research_useful": 1},
            "source_coverage": {"summary": "market_bars 1/1", "actions": []},
            "instrument_scope": {},
            "rows": [
                {
                    "ticker": "ETFZ",
                    "priced_in_status": "bullish_not_priced_in",
                    "priced_in_direction": "bullish",
                    "emotion_reaction_gap": 53.0,
                    "emotion_score": 84.0,
                    "reaction_score": 31.0,
                    "priced_in_score": 77.0,
                    "score": 82.0,
                    "blocked": False,
                    "data_sources": {
                        "available": ["market_bars", "theme_peer_sector"],
                        "missing": ["options"],
                        "stale": [],
                    },
                    "usefulness": {
                        "status": "research_useful",
                        "next_command": "catalyst-radar build-packets --ticker ETFZ",
                    },
                    "non_company_evidence": {
                        "status": "available",
                        "route": "market_theme_fund_or_flow",
                        "summary": "ETFZ: ETFZ Thematic Fund: Instrument type ETF.",
                    },
                    "next_step": "Build a Candidate Packet before Decision Card review.",
                }
            ],
        }

    monkeypatch.setattr("catalyst_radar.cli.priced_in_queue_payload", fake_payload)

    assert main(["priced-in-queue"]) == 0
    output = capsys.readouterr()

    assert output.err == ""
    assert "ETFZ bullish_not_priced_in research_useful" in output.out
    assert (
        "non_company_evidence=status=available "
        "route=market_theme_fund_or_flow"
    ) in output.out
    assert "ETFZ Thematic Fund" in output.out


def test_dashboard_overview_rows_include_non_company_evidence_summary() -> None:
    rows = _priced_in_overview_rows(
        {
            "priced_in_queue": {
                "offset": 0,
                "rows": [
                    {
                        "ticker": "ETFZ",
                        "priced_in_status": "bullish_not_priced_in",
                        "emotion_score": 84.0,
                        "reaction_score": 31.0,
                        "emotion_reaction_gap": 53.0,
                        "candidate_theme": "ai_infrastructure",
                        "data_sources": {
                            "available": ["market_bars", "theme_peer_sector"],
                            "missing": ["options"],
                            "stale": [],
                        },
                        "usefulness": {
                            "label": "Research-useful mismatch",
                            "next_action": "Build a Candidate Packet.",
                        },
                        "non_company_evidence": {
                            "status": "available",
                            "summary": (
                                "ETFZ: ETFZ Thematic Fund: Instrument type ETF."
                            ),
                        },
                    }
                ],
            }
        }
    )

    assert rows[0]["ticker"] == "ETFZ"
    assert "non-company available" in rows[0]["why_now"]
    assert "ETFZ Thematic Fund" in rows[0]["why_now"]


def test_priced_in_source_batches_cli_prints_non_company_route(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'demo.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    def fake_payload(*_args, **_kwargs):
        return {
            "schema_version": "priced-in-source-batches-v1",
            "source": "catalyst_events",
            "status": "routed",
            "total_gap_rows": 2,
            "plannable_gap_rows": 0,
            "unplannable_gap_rows": 2,
            "routed_gap_rows": 2,
            "blocked_gap_rows": 0,
            "planned_at": "2026-05-18T16:00:00+00:00",
            "batch_size": 5,
            "count": 0,
            "batch_count": 0,
            "batch_offset": 0,
            "all_batches": False,
            "external_calls_made": 0,
            "headline": "2 full-scan rows have a catalyst_events gap.",
            "next_action": "Use fund evidence.",
            "execution_boundary": "Plan only.",
            "diagnostic": {
                "status": "routed",
                "eligible_rows": 0,
                "blocked_rows": 0,
                "blocked_reason": None,
                "reason": "Non-company instruments are routed.",
                "sample_blocked_tickers": [],
                "missing_cik_type_counts": {},
                "missing_cik_company_like_rows": 0,
                "missing_cik_non_company_rows": 0,
                "missing_cik_unknown_type_rows": 0,
                "routed_non_company_rows": 2,
                "sample_routed_non_company_tickers": ["AAA", "BBB"],
                "non_company_evidence_route": "Use fund evidence.",
                "next_action": "Use fund evidence.",
                "fix_command": None,
                "fix_api": None,
            },
            "batches": [],
        }

    monkeypatch.setattr(
        "catalyst_radar.cli.priced_in_source_gap_batches_payload",
        fake_payload,
    )

    assert main(["priced-in-source-batches", "--source", "catalyst_events"]) == 0
    output = capsys.readouterr()

    assert output.err == ""
    assert "status=routed" in output.out
    assert "non_company_route=routed=2 examples=AAA,BBB route=Use fund evidence." in (
        output.out
    )
    assert "diagnostic_next=Use fund evidence." in output.out


def test_priced_in_source_batches_cli_prints_options_point_in_time_import(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'demo.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    def fake_payload(*_args, **_kwargs):
        return {
            "schema_version": "priced-in-source-batches-v1",
            "source": "options",
            "status": "blocked",
            "total_gap_rows": 2,
            "plannable_gap_rows": 0,
            "unplannable_gap_rows": 2,
            "routed_gap_rows": 0,
            "planned_at": "2026-05-18T16:00:00+00:00",
            "batch_size": 5,
            "count": 0,
            "batch_count": 0,
            "batch_offset": 0,
            "all_batches": False,
            "external_calls_made": 0,
            "headline": "2 full-scan rows have an options gap.",
            "next_action": "Ingest point-in-time options.",
            "execution_boundary": "Plan only.",
            "diagnostic": {
                "status": "blocked",
                "eligible_rows": 0,
                "blocked_rows": 2,
                "blocked_reason": "newer_than_scan",
                "reason": "Stored options are newer than the scan.",
                "sample_blocked_tickers": ["AAPL"],
                "next_action": "Ingest point-in-time options.",
                "point_in_time_template_command": (
                    "catalyst-radar ingest-options --fixture-template "
                    "--out data\\local\\point-in-time-options-2026-05-15.json"
                ),
                "point_in_time_validate_command": (
                    "catalyst-radar ingest-options --fixture "
                    "data\\local\\point-in-time-options-2026-05-15.json "
                    "--validate-only --expected-as-of 2026-05-15"
                ),
                "point_in_time_import_command": (
                    "catalyst-radar ingest-options --fixture "
                    "<point-in-time-options-2026-05-15.json>"
                ),
                "point_in_time_fixture_progress": {
                    "status": "needs_fill",
                    "exists": True,
                    "row_count": 2,
                    "complete": 0,
                    "partial": 1,
                    "empty": 1,
                    "path": "data\\local\\point-in-time-options-2026-05-15.json",
                    "external_calls_made": 0,
                },
            },
            "batches": [],
        }

    monkeypatch.setattr(
        "catalyst_radar.cli.priced_in_source_gap_batches_payload",
        fake_payload,
    )

    assert main(["priced-in-source-batches", "--source", "options"]) == 0
    output = capsys.readouterr()

    assert output.err == ""
    assert "status=blocked" in output.out
    assert "blocked_examples=AAPL reason=newer_than_scan" in output.out
    assert (
        "diagnostic_point_in_time_template="
        "catalyst-radar ingest-options --fixture-template "
        "--out data\\local\\point-in-time-options-2026-05-15.json"
    ) in output.out
    assert (
        "diagnostic_point_in_time_validate="
        "catalyst-radar ingest-options --fixture "
        "data\\local\\point-in-time-options-2026-05-15.json "
        "--validate-only --expected-as-of 2026-05-15"
    ) in output.out
    assert (
        "diagnostic_point_in_time_import=catalyst-radar ingest-options --fixture "
        "<point-in-time-options-2026-05-15.json>"
    ) in output.out
    assert (
        "diagnostic_point_in_time_fixture=status=needs_fill exists=true rows=2 "
        "complete=0 partial=1 empty=1 "
        "path=data\\local\\point-in-time-options-2026-05-15.json"
    ) in output.out


def test_dashboard_batch_message_prints_options_point_in_time_import(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'demo.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    def fake_payload(*_args, **_kwargs):
        return {
            "schema_version": "priced-in-source-batches-v1",
            "source": "options",
            "status": "blocked",
            "total_gap_rows": 2,
            "plannable_gap_rows": 0,
            "unplannable_gap_rows": 2,
            "routed_gap_rows": 0,
            "batch_count": 0,
            "next_action": "Ingest point-in-time options.",
            "diagnostic": {
                "reason": "Stored options are newer than the scan.",
                "sample_blocked_tickers": ["AAPL"],
                "blocked_reason": "newer_than_scan",
                "next_action": "Ingest point-in-time options.",
                "point_in_time_template_command": (
                    "catalyst-radar ingest-options --fixture-template "
                    "--out data\\local\\point-in-time-options-2026-05-15.json"
                ),
                "point_in_time_validate_command": (
                    "catalyst-radar ingest-options --fixture "
                    "data\\local\\point-in-time-options-2026-05-15.json "
                    "--validate-only --expected-as-of 2026-05-15"
                ),
                "point_in_time_import_command": (
                    "catalyst-radar ingest-options --fixture "
                    "<point-in-time-options-2026-05-15.json>"
                ),
                "point_in_time_fixture_progress": {
                    "status": "needs_fill",
                    "exists": True,
                    "row_count": 2,
                    "complete": 0,
                    "partial": 1,
                    "empty": 1,
                    "path": "data\\local\\point-in-time-options-2026-05-15.json",
                    "external_calls_made": 0,
                },
            },
            "batches": [],
        }

    monkeypatch.setattr(
        "catalyst_radar.dashboard.tui.dashboard_data."
        "priced_in_source_gap_batches_payload",
        fake_payload,
    )

    update = _apply_command(
        "batch options",
        {},
        "overview",
        DashboardFilters(),
        engine=create_engine(database_url, future=True),
        config=AppConfig.from_env(),
    )

    assert update.page == "ops"
    assert "options: blocked;" in update.message
    assert "Blocked examples: AAPL." in update.message
    assert (
        "Template: catalyst-radar ingest-options --fixture-template "
        "--out data\\local\\point-in-time-options-2026-05-15.json."
    ) in update.message
    assert (
        "Validate: catalyst-radar ingest-options --fixture "
        "data\\local\\point-in-time-options-2026-05-15.json "
        "--validate-only --expected-as-of 2026-05-15."
    ) in update.message
    assert (
        "Point-in-time import: catalyst-radar ingest-options --fixture "
        "<point-in-time-options-2026-05-15.json>."
    ) in update.message
    assert (
        "Local template: needs_fill; 0 complete, 1 partial, 1 empty of 2 row(s) "
        "at data\\local\\point-in-time-options-2026-05-15.json."
    ) in update.message


def test_dashboard_batch_message_prints_market_bar_saved_file_repair(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'demo.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    def fake_payload(*_args, **_kwargs):
        return {
            "schema_version": "priced-in-source-batches-v1",
            "source": "market_bars",
            "status": "attention",
            "total_gap_rows": 131,
            "plannable_gap_rows": 0,
            "unplannable_gap_rows": 131,
            "routed_gap_rows": 0,
            "batch_count": 0,
            "next_action": "Fill missing market bars.",
            "diagnostic": {
                "reason": "5521/5652 stock-like rows have scan-date bars.",
                "blocked_reason": "missing_stock_like_as_of_bars",
                "blocked_rows": 131,
                "eligible_rows": 0,
                "sample_blocked_tickers": ["AACO"],
                "fix_command": (
                    "catalyst-radar market-bars template --expected-as-of "
                    "2026-05-15 --out "
                    "data\\local\\manual-stock-bars-2026-05-15.csv "
                    "--missing-only --stocks-only"
                ),
                "manual_validate_command": (
                    "catalyst-radar market-bars import --daily-bars "
                    "data\\local\\manual-stock-bars-2026-05-15.csv "
                    "--expected-as-of 2026-05-15 --stocks-only"
                ),
                "manual_fix_command": (
                    "catalyst-radar market-bars import --daily-bars "
                    "data\\local\\manual-stock-bars-2026-05-15.csv "
                    "--expected-as-of 2026-05-15 --stocks-only --execute"
                ),
                "provider_saved_file_path": (
                    "data\\local\\polygon-grouped-daily-2026-05-15.json"
                ),
                "provider_saved_file_exists": False,
                "provider_saved_file_status": "missing",
                "provider_saved_file_next_action": (
                    "Capture or obtain the saved grouped-daily JSON response."
                ),
                "provider_saved_file_capture_command": (
                    "catalyst-radar market-bars saved-capture "
                    "--expected-as-of 2026-05-15 --out "
                    "data\\local\\polygon-grouped-daily-2026-05-15.json "
                    "--confirm-external-call"
                ),
                "provider_saved_file_capture_external_call_count": 1,
                "provider_saved_file_validate_command": (
                    "catalyst-radar market-bars saved-validate "
                    "--expected-as-of 2026-05-15 --fixture "
                    "data\\local\\polygon-grouped-daily-2026-05-15.json"
                ),
                "provider_saved_file_import_command": (
                    "catalyst-radar market-bars saved-import "
                    "--expected-as-of 2026-05-15 --fixture "
                    "data\\local\\polygon-grouped-daily-2026-05-15.json"
                ),
                "provider_saved_file_external_call_count": 0,
                "provider_saved_file_boundary": (
                    "Capture makes one provider call only with explicit approval."
                ),
                "local_bar_history": {
                    "missing_with_history": 0,
                    "missing_without_history": 131,
                },
                "missing_universe": {
                    "active_metadata_rows": 131,
                    "acquisition_or_spac_name_count": 61,
                    "no_composite_figi_count": 103,
                    "zero_avg_dollar_volume_20d_count": 131,
                    "zero_market_cap_count": 131,
                    "operator_note": (
                        "This context does not exclude rows from the scan."
                    ),
                    "external_calls_made": 0,
                },
            },
            "batches": [],
        }

    monkeypatch.setattr(
        "catalyst_radar.dashboard.tui.dashboard_data."
        "priced_in_source_gap_batches_payload",
        fake_payload,
    )

    update = _apply_command(
        "batch market_bars",
        {},
        "overview",
        DashboardFilters(),
        engine=create_engine(database_url, future=True),
        config=AppConfig.from_env(),
    )

    assert update.page == "ops"
    assert "market_bars: attention;" in update.message
    assert "Saved file: missing; exists=false" in update.message
    assert "data\\local\\polygon-grouped-daily-2026-05-15.json" in update.message
    assert "Saved file capture: 1 external call(s); command" in update.message
    assert "--out data\\local\\polygon-grouped-daily-2026-05-15.json" in (
        update.message
    )
    assert "Saved file check: 0 external call(s); command" in update.message
    assert "Saved file import: 0 external call(s); command" in update.message
    assert "explicit approval" in update.message
    assert "Manual bar check: catalyst-radar market-bars import" in update.message
    assert "Manual bar import: catalyst-radar market-bars import" in update.message
    assert "CIK validate:" not in update.message
    assert "CIK import:" not in update.message
    assert "Local history: 0 with local bars; 131 without" in update.message
    assert "Universe context: active metadata 131" in update.message
    assert "61 acquisition/SPAC-style" in update.message
    assert "131 zero 20d dollar volume" in update.message


def test_priced_in_source_batches_execute_next_cli_runs_one_batch(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'demo.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    captured: dict[str, object] = {}

    def fake_execute(_engine, _config, **kwargs) -> dict[str, object]:
        captured.update(kwargs)
        return {
            "schema_version": "priced-in-source-batch-execution-v1",
            "source": kwargs["source"],
            "status": "executed",
            "reason": None,
            "external_calls_made": 0,
            "plan": {
                "status": "ready",
                "total_gap_rows": 123,
                "plannable_gap_rows": 123,
                "batch_count": 25,
                "batch_size": 5,
            },
            "batch": {
                "number": 1,
                "row_start": 1,
                "row_end": 5,
                "tickers": ["ACME"],
                "call_plan_status": "local_only",
                "api_payload": {"tickers": ["ACME"]},
            },
            "result": {
                "provider": "local_text",
                "endpoint": "features-batch",
                "ticker_count": 1,
                "feature_count": 1,
                "snippet_count": 2,
                "external_calls_made": 0,
            },
            "post_execution": {
                "schema_version": "priced-in-source-batch-post-execution-v1",
                "source": kwargs["source"],
                "status": "improved",
                "external_calls_made": 0,
                "before_gap_rows": 123,
                "after_gap_rows": 122,
                "gap_rows_resolved": 1,
                "before_plannable_rows": 123,
                "after_plannable_rows": 122,
                "plannable_rows_resolved": 1,
                "before_batch_count": 25,
                "after_batch_count": 25,
                "next_action": (
                    "Full-scan local_text coverage improved; review next batch."
                ),
                "all_batches_command": (
                    "catalyst-radar priced-in-source-batches "
                    "--source local_text --all --json"
                ),
            },
        }

    monkeypatch.setattr(
        "catalyst_radar.cli.execute_priced_in_source_batch",
        fake_execute,
    )

    assert (
        main(
            [
                "priced-in-source-batches",
                "--source",
                "local_text",
                "--execute-next",
                "--decision-gap",
                "candidate_packet",
            ]
        )
        == 0
    )
    output = capsys.readouterr()

    assert output.err == ""
    assert "priced_in_source_batch_execution source=local_text status=executed" in (
        output.out
    )
    assert "summary=Executed local_text chunk 1 (rows 1-5)" in output.out
    assert "features=1 snippets=2 external_calls=0" in output.out
    assert "post_execution=status=improved gap_rows=123->122" in output.out
    assert (
        "post_plan=catalyst-radar priced-in-source-batches "
        "--source local_text --all --json"
    ) in output.out
    assert captured["source"] == "local_text"
    assert captured["decision_gap"] == ["candidate_packet"]


def test_source_batch_run_executes_capped_chunks_and_reports_delta(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'demo.db').as_posix()}"
    engine = create_engine(database_url, future=True)
    config = AppConfig.from_env()
    plan_gap_rows = [10, 8]
    execute_calls = 0

    def fake_plan(_engine, _config, **kwargs) -> dict[str, object]:
        gap_rows = plan_gap_rows[min(len(plan_gap_rows) - 1, len(plan_calls))]
        plan_calls.append(kwargs)
        return {
            "status": "ready",
            "source": kwargs["source"],
            "total_gap_rows": gap_rows,
            "plannable_gap_rows": gap_rows,
            "unplannable_gap_rows": 0,
            "batch_count": max(1, gap_rows // 2),
            "batch_size": 5,
            "next_action": "Run next chunk.",
            "review_rows_command": (
                "catalyst-radar priced-in-queue --full-scan "
                f"--source-gap {kwargs['source']} --limit 50"
            ),
            "all_batches_command": (
                "catalyst-radar priced-in-source-batches "
                f"--source {kwargs['source']} --all --json"
            ),
            "batches": [{"number": 1, "tickers": ["ACME"]}],
        }

    def fake_execute(_engine, _config, **kwargs) -> dict[str, object]:
        nonlocal execute_calls
        execute_calls += 1
        return {
            "schema_version": "priced-in-source-batch-execution-v1",
            "source": kwargs["source"],
            "status": "executed",
            "external_calls_made": 1,
            "batch": {"number": execute_calls, "tickers": ["ACME"]},
            "result": {"provider": "sec", "event_count": 1},
        }

    plan_calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        source_batch_module,
        "priced_in_source_gap_batches_payload",
        fake_plan,
    )
    monkeypatch.setattr(
        source_batch_module,
        "execute_priced_in_source_batch",
        fake_execute,
    )

    payload = source_batch_module.execute_priced_in_source_batches(
        engine,
        config,
        source="catalyst_events",
        max_batches=2,
        status="all",
        usefulness="research_useful",
        decision_gap=["candidate_packet"],
    )

    assert payload["schema_version"] == "priced-in-source-batch-run-v1"
    assert payload["source"] == "catalyst_events"
    assert payload["status"] == "executed"
    assert payload["executed_batches"] == 2
    assert payload["external_calls_made"] == 2
    assert payload["gap_rows_resolved"] == 2
    assert payload["before_plan"]["total_gap_rows"] == 10
    assert payload["after_plan"]["total_gap_rows"] == 8
    assert "Review the next batch plan" in payload["next_action"]
    assert payload["next_command"] == (
        "catalyst-radar priced-in-source-batches --source catalyst_events "
        "--execute-batches 2"
    )
    assert len(payload["executions"]) == 2
    assert execute_calls == 2
    assert plan_calls[0]["status"] == "all"
    assert plan_calls[0]["usefulness"] == "research_useful"
    assert plan_calls[0]["decision_gap"] == ["candidate_packet"]


def test_priced_in_source_batches_execute_batches_cli_runs_capped_batch_run(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'demo.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    captured: dict[str, object] = {}

    def fake_execute_batches(_engine, _config, **kwargs) -> dict[str, object]:
        captured.update(kwargs)
        return {
            "schema_version": "priced-in-source-batch-run-v1",
            "source": kwargs["source"],
            "status": "executed",
            "requested_batches": kwargs["max_batches"],
            "executed_batches": 3,
            "stopped_reason": "Reached max_batches=3.",
            "external_calls_made": 3,
            "before_plan": {
                "total_gap_rows": 10,
                "plannable_gap_rows": 10,
                "batch_count": 4,
            },
            "after_plan": {
                "total_gap_rows": 7,
                "plannable_gap_rows": 7,
                "batch_count": 3,
            },
            "gap_rows_resolved": 3,
            "plannable_rows_resolved": 3,
            "next_action": "Review the next batch plan before continuing.",
            "next_command": (
                "catalyst-radar priced-in-source-batches --source catalyst_events "
                "--execute-batches 3"
            ),
            "executions": [
                {
                    "status": "executed",
                    "external_calls_made": 1,
                    "reason": None,
                    "batch": {"tickers": ["ACME"]},
                }
            ],
        }

    monkeypatch.setattr(
        "catalyst_radar.cli.execute_priced_in_source_batches",
        fake_execute_batches,
    )

    assert (
        main(
            [
                "priced-in-source-batches",
                "--source",
                "catalyst_events",
                "--execute-batches",
                "3",
                "--decision-gap",
                "candidate_packet",
            ]
        )
        == 0
    )
    output = capsys.readouterr()

    assert output.err == ""
    assert (
        "priced_in_source_batch_run source=catalyst_events status=executed "
        "executed=3/3 external_calls=3"
    ) in output.out
    assert "coverage=gap_rows=10->7 resolved=3" in output.out
    assert "next_command=catalyst-radar priced-in-source-batches" in output.out
    assert captured["source"] == "catalyst_events"
    assert captured["max_batches"] == 3
    assert captured["decision_gap"] == ["candidate_packet"]


def test_priced_in_source_batches_execute_batches_cli_shows_current_blocker(
    tmp_path: Path,
    monkeypatch,
    capsys,
):
    database_url = f"sqlite:///{(tmp_path / 'demo.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    def fake_execute_batches(_engine, _config, **kwargs):
        return {
            "schema_version": "priced-in-source-batch-run-v1",
            "source": kwargs["source"],
            "status": "blocked",
            "requested_batches": kwargs["max_batches"],
            "executed_batches": 0,
            "stopped_reason": "market_bars must be complete first.",
            "external_calls_made": 0,
            "before_plan": {
                "total_gap_rows": 10,
                "plannable_gap_rows": 10,
                "batch_count": 2,
            },
            "after_plan": {
                "total_gap_rows": 10,
                "plannable_gap_rows": 10,
                "batch_count": 2,
            },
            "gap_rows_resolved": 0,
            "plannable_rows_resolved": 0,
            "next_action": "Clear market_bars before source execution.",
            "next_command": "catalyst-radar market-bars status --expected-as-of 2026-05-15",
            "execution_blocker": {
                "blocked_by": "market_bars",
                "blocked_gap_rows": 523,
                "command": "catalyst-radar market-bars status --expected-as-of 2026-05-15",
                "external_calls_made": 0,
            },
            "executions": [],
        }

    monkeypatch.setattr(
        "catalyst_radar.cli.execute_priced_in_source_batches",
        fake_execute_batches,
    )

    assert (
        main(
            [
                "priced-in-source-batches",
                "--source",
                "catalyst_events",
                "--execute-batches",
                "3",
            ]
        )
        == 1
    )
    output = capsys.readouterr()

    assert output.err == ""
    assert "priced_in_source_batch_run source=catalyst_events status=blocked" in output.out
    assert "next_command=catalyst-radar market-bars status" in output.out
    assert "execution_blocker=blocked_by=market_bars" in output.out
    assert "gap_rows=523" in output.out
    assert "external_calls=0" in output.out


def test_priced_in_answer_cli_outputs_current_scan_answer(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'demo.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    assert main(["seed-dashboard-demo"]) == 0
    capsys.readouterr()

    assert main(["priced-in-answer"]) == 0
    output = capsys.readouterr()

    assert output.err == ""
    assert "priced_in_answer status=" in output.out
    assert "question=Has price fully matched market expectations?" in output.out
    assert "answer=" in output.out
    assert "investment_decision_ready=false" in output.out
    assert "investment_boundary=Priced-in answer readiness is not trade approval" in output.out
    assert "decision_readiness=status=" in output.out
    assert "evidence_completeness=all_sources_ready=" in output.out
    assert "operator_next_step=status=" in output.out
    assert "investment_ready=false" in output.out
    assert "operator_response=" in output.out
    assert "full_scan=mode=full_scan" in output.out
    assert "unscanned=" in output.out
    assert "unscanned_blockers=" in output.out
    assert "excluded=" in output.out
    assert "basis=" in output.out
    assert "sample=false" in output.out
    assert "review_full_scan=catalyst-radar priced-in-queue --full-scan" in output.out
    assert "full_market_trust_gate=" in output.out
    assert "trust_gate_ladder=" in output.out
    assert (
        "export_full_scan=catalyst-radar priced-in-queue --full-scan --all --json"
        in output.out
    )
    assert "actionable_rows_sample=ranked actionable mismatches" in output.out
    assert (
        "full_scan_export=catalyst-radar priced-in-queue --full-scan --all --json"
        in output.out
    )
    assert "external_calls=0" in output.out

    assert main(["priced-in-answer", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["schema_version"] == "priced-in-answer-v1"
    assert payload["external_calls_made"] == 0
    assert payload["question"] == "Has price fully matched market expectations?"
    assert payload["decision_ready"] is False
    assert payload["priced_in_answer_ready"] is False
    assert payload["can_make_investment_decision"] is False
    assert payload["manual_investment_decision_ready"] is False
    assert "not trade approval" in payload["investment_decision_boundary"]
    assert payload["decision_readiness"]["schema_version"] == (
        "priced-in-decision-readiness-v1"
    )
    assert payload["evidence_completeness"]["schema_version"] == (
        "priced-in-evidence-completeness-v1"
    )
    assert payload["evidence_completeness"]["all_sources_ready"] is False
    assert payload["evidence_completeness"]["total_source_count"] >= 1
    assert payload["full_scan"]["schema_version"] == (
        "priced-in-full-scan-summary-v1"
    )
    assert payload["full_market_trust_gate"]["schema_version"] == (
        "priced-in-full-market-trust-gate-v1"
    )
    assert payload["full_market_trust_gate"]["external_calls_made"] == 0
    assert payload["operator_next_step"]["schema_version"] == (
        "priced-in-operator-next-step-v1"
    )
    assert payload["operator_next_step"]["external_calls_made"] == 0
    assert payload["operator_next_step"]["db_writes_made"] == 0
    assert payload["operator_next_step"]["can_use_for_investment_decision"] is False
    ladder = payload["full_market_trust_gate"]["blocker_ladder"]
    assert ladder["schema_version"] == "priced-in-full-market-blocker-ladder-v1"
    assert ladder["external_calls_made"] == 0
    assert ladder["rows"]
    assert ladder["rows"][0]["external_calls_made"] == 0
    after_current = payload["full_market_trust_gate"].get("after_current_blocker")
    if after_current:
        assert "trust_gate_after_current=" in output.out
        assert after_current["schema_version"] == "priced-in-after-current-blocker-v1"
    else:
        assert payload["first_blocker"] == "universe"
        assert "trust_gate_after_current=" not in output.out
    assert payload["reviewable_subset"]["schema_version"] == (
        "priced-in-reviewable-subset-v1"
    )
    assert payload["reviewable_subset"]["external_calls_made"] == 0


def test_dashboard_summary_surfaces_unscanned_full_scan_rows() -> None:
    payload = {
        "priced_in_answer": {
            "full_scan": {
                "instrument_filter": "all",
                "active_securities": 12613,
                "scanned_rows": 12087,
                "unscanned_rows": 526,
                "unscanned_blocker_rows": 523,
                "scan_excluded_rows": 3,
                "scan_excluded_tickers": ["SPY", "XLI", "XLK"],
            }
        },
        "priced_in_audit": {
            "market_bars": {
                "missing_as_of_bar": 523,
                "repair": {"diagnostic": {"missing_count": 523}},
            }
        },
    }

    assert _answer_full_scan_scope_summary(payload) == (
        "Full-scan coverage: 12087/12613 active all-instrument row(s) scanned; "
        "526 unscanned; 523 missing scan-date market bar(s); "
        "3 benchmark reference row(s) intentionally excluded: SPY, XLI, XLK."
    )


def test_dashboard_summary_surfaces_stock_scope_market_bar_gap() -> None:
    payload = {
        "priced_in_answer": {
            "full_scan": {
                "instrument_filter": "stocks_only",
                "active_securities": 5652,
                "scanned_rows": 5521,
                "unscanned_rows": 131,
            }
        },
        "priced_in_audit": {
            "market_bars": {
                "missing_as_of_bar": 523,
                "repair": {
                    "diagnostic": {"missing_count": 523},
                    "stock_scope": {
                        "stock_like_active": 5652,
                        "stock_like_with_as_of_bar": 5521,
                        "stock_like_missing_as_of_bar": 131,
                    },
                },
            }
        },
    }

    assert _answer_full_scan_scope_summary(payload) == (
        "Full-scan coverage: 5521/5652 active stock-like row(s) scanned; "
        "131 unscanned; 131 missing stock-like scan-date market bar(s); "
        "523 all-instrument missing."
    )


def test_dashboard_summary_surfaces_answer_evidence_completeness() -> None:
    payload = {
        "priced_in_answer": {
            "evidence_completeness": {
                "ready_source_count": 1,
                "total_source_count": 6,
                "first_gap_source": "market_bars",
                "first_gap_count": 523,
                "summary": (
                    "1/6 priced-in evidence layer(s) complete; core 0/3; "
                    "first gaps market_bars:523, catalyst_events:5512."
                ),
            }
        }
    }

    assert _answer_evidence_completeness_summary(payload) == (
        "Evidence layers: 1/6 priced-in evidence layer(s) complete; core 0/3; "
        "first gaps market_bars:523, catalyst_events:5512."
    )


def test_priced_in_answer_uses_stock_scope_for_market_bar_coverage(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'demo.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    assert main(["seed-dashboard-demo"]) == 0
    capsys.readouterr()
    engine = create_engine(database_url, future=True)
    config = AppConfig.from_env({"CATALYST_DATABASE_URL": database_url})
    market_repo = MarketRepository(engine)
    market_repo.upsert_securities(
        [
            Security(
                ticker="ACME",
                name="Acme Corp",
                exchange="NASDAQ",
                sector="Technology",
                industry="Software",
                market_cap=1_000_000_000,
                avg_dollar_volume_20d=20_000_000,
                has_options=True,
                is_active=True,
                updated_at=DEMO_AVAILABLE_AT,
                metadata={"type": "CS"},
            )
        ]
    )
    market_repo.upsert_daily_bars(
        [
            DailyBar(
                ticker="ACME",
                date=DEMO_AVAILABLE_AT.date(),
                open=100,
                high=101,
                low=99,
                close=100,
                volume=1_000_000,
                vwap=100,
                adjusted=True,
                provider="manual_csv",
                source_ts=DEMO_AVAILABLE_AT,
                available_at=DEMO_AVAILABLE_AT,
            )
        ]
    )
    queue = priced_in_queue_payload(engine, config, stocks_only=True)
    queue["latest_run"] = {
        **dict(queue.get("latest_run") or {}),
        "as_of": DEMO_AVAILABLE_AT.date().isoformat(),
    }
    market_repo.upsert_securities(
        [
            Security(
                ticker="ZZZZ",
                name="Missing Bar Stock",
                exchange="NASDAQ",
                sector="Technology",
                industry="Software",
                market_cap=1_000_000_000,
                avg_dollar_volume_20d=20_000_000,
                has_options=True,
                is_active=True,
                updated_at=DEMO_AVAILABLE_AT,
                metadata={"type": "CS"},
            )
        ]
    )

    payload = priced_in_answer_payload(
        engine,
        config,
        stocks_only=True,
        queue=queue,
    )

    assert "market_bars 1/2 (1 missing)" in payload["source_coverage"]["summary"]
    assert payload["full_scan"]["active_securities"] == 2
    assert payload["full_scan"]["scanned_rows"] == 1
    assert payload["full_scan"]["unscanned_rows"] == 1
    assert payload["full_scan"]["scan_scope_basis"] == "stock_like_active_as_of_bars"
    assert payload["scan_scope"]["full_scan_export_command"] == (
        "catalyst-radar priced-in-queue --stocks-only --full-scan --all --json"
    )
    market_bar_blockers = [
        row for row in payload["trust_blockers"] if row["area"] == "market_bars"
    ]
    assert market_bar_blockers
    assert "market-bars template" in market_bar_blockers[0]["command"]
    assert "--stocks-only" in market_bar_blockers[0]["command"]
    assert "market_bars" in payload["source_coverage"]["weak_sources"]

    overview = priced_in_all_source_gap_batches_payload(
        engine,
        config,
        stocks_only=True,
    )
    source_rows = {row["source"]: row for row in overview["sources"]}
    assert overview["status"] == "attention"
    assert overview["scan_scope"]["active_securities"] == 2
    assert overview["scan_scope"]["scanned_rows"] == 1
    assert overview["scan_scope"]["unscanned_rows"] == 1
    assert overview["scan_scope"]["scan_scope_basis"] == (
        "stock_like_active_as_of_bars"
    )
    assert source_rows["market_bars"]["status"] == "attention"
    assert source_rows["market_bars"]["total_gap_rows"] == 1
    assert source_rows["market_bars"]["diagnostic"]["blocked_reason"] == (
        "missing_stock_like_as_of_bars"
    )
    assert source_rows["market_bars"]["diagnostic"][
        "blank_required_field_counts_if_new_template"
    ] == {
        "open": 1,
        "high": 1,
        "low": 1,
        "close": 1,
        "volume": 1,
        "vwap": 1,
    }
    assert source_rows["market_bars"]["diagnostic"]["template_row_count"] == 1
    assert "--stocks-only" in source_rows["market_bars"]["diagnostic"][
        "manual_template_command"
    ]
    assert "data\\local\\manual-stock-bars-" in source_rows["market_bars"][
        "diagnostic"
    ]["manual_validate_command"]
    assert "<fresh-bars.csv>" not in source_rows["market_bars"]["diagnostic"][
        "manual_validate_command"
    ]
    assert "data\\local\\manual-stock-bars-" in source_rows["market_bars"][
        "diagnostic"
    ]["manual_fix_command"]
    assert "<fresh-bars.csv>" not in source_rows["market_bars"]["diagnostic"][
        "manual_fix_command"
    ]
    assert overview["coverage_first_recommendation"]["source"] == "market_bars"
    assert "market-bars template" in overview["coverage_first_recommendation"][
        "command"
    ]
    assert "--stocks-only" in overview["coverage_first_recommendation"]["command"]
    assert overview["decision_shortcut_recommendation"] is None
    assert overview["decision_shortcut_blocker"]["blocked_by"] == "market_bars"
    assert overview["decision_shortcut_blocker"]["external_calls_required"] == 0

    snapshot = dashboard_snapshot_payload(
        engine=engine,
        config=config,
        dotenv_loaded=False,
        filters=DashboardFilters(priced_in_stocks_only=True),
    )
    workflow = snapshot["priced_in_source_workflow"]
    assert workflow["coverage_first_command"].startswith(
        "catalyst-radar market-bars template"
    )
    assert "--stocks-only" in workflow["coverage_first_command"]
    assert workflow["goal_alignment"]["next_command"].startswith(
        "catalyst-radar market-bars template"
    )
    assert workflow["decision_shortcut_action"] is None
    assert workflow["decision_shortcut_blocker"]["blocked_by"] == "market_bars"
    assert "Template generation and import preview are zero-call" in workflow[
        "goal_alignment"
    ]["provider_boundary"]


def test_dashboard_snapshot_reuses_priced_in_market_bar_audit(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'demo.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    assert main(["seed-dashboard-demo"]) == 0
    capsys.readouterr()
    engine = create_engine(database_url, future=True)
    config = AppConfig.from_env({"CATALYST_DATABASE_URL": database_url})
    market_bar_calls = 0
    broker_summary_calls = 0
    discovery_calls = 0
    original_market_bars = dashboard_data_module._priced_in_audit_market_bars
    original_broker_summary = dashboard_data_module.load_broker_summary
    original_discovery = dashboard_data_module.radar_discovery_snapshot_payload

    def counted_market_bars(*args, **kwargs):
        nonlocal market_bar_calls
        market_bar_calls += 1
        return original_market_bars(*args, **kwargs)

    def counted_broker_summary(*args, **kwargs):
        nonlocal broker_summary_calls
        broker_summary_calls += 1
        return original_broker_summary(*args, **kwargs)

    def counted_discovery(*args, **kwargs):
        nonlocal discovery_calls
        discovery_calls += 1
        return original_discovery(*args, **kwargs)

    monkeypatch.setattr(
        dashboard_data_module,
        "_priced_in_audit_market_bars",
        counted_market_bars,
    )
    monkeypatch.setattr(
        dashboard_data_module,
        "load_broker_summary",
        counted_broker_summary,
    )
    monkeypatch.setattr(
        dashboard_data_module,
        "radar_discovery_snapshot_payload",
        counted_discovery,
    )

    payload = dashboard_snapshot_payload(
        engine=engine,
        config=config,
        dotenv_loaded=False,
        filters=DashboardFilters(priced_in_stocks_only=True, priced_in_limit=1),
    )

    assert market_bar_calls == 1
    assert broker_summary_calls == 1
    assert discovery_calls == 1
    assert payload["priced_in_answer"]["schema_version"] == "priced-in-answer-v1"
    assert payload["priced_in_audit"]["market_bars"]["repair"]["schema_version"] == (
        "priced-in-market-bar-repair-v1"
    )


def test_priced_in_source_execution_blocks_until_stock_bars_complete(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'demo.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    assert main(["seed-dashboard-demo"]) == 0
    capsys.readouterr()
    engine = create_engine(database_url, future=True)
    MarketRepository(engine).upsert_securities(
        [
            Security(
                ticker="ACME",
                name="Acme Corp",
                exchange="NASDAQ",
                sector="Technology",
                industry="Software",
                market_cap=1_000_000_000,
                avg_dollar_volume_20d=20_000_000,
                has_options=True,
                is_active=True,
                updated_at=DEMO_AVAILABLE_AT,
                metadata={"type": "CS"},
            ),
            Security(
                ticker="ZZZZ",
                name="Missing Bar Stock",
                exchange="NASDAQ",
                sector="Technology",
                industry="Software",
                market_cap=1_000_000_000,
                avg_dollar_volume_20d=20_000_000,
                has_options=True,
                is_active=True,
                updated_at=DEMO_AVAILABLE_AT,
                metadata={"type": "CS"},
            )
        ]
    )
    MarketRepository(engine).upsert_daily_bars(
        [
            DailyBar(
                ticker="ACME",
                date=DEMO_AVAILABLE_AT.date(),
                open=100,
                high=101,
                low=99,
                close=100,
                volume=1_000_000,
                vwap=100,
                adjusted=True,
                provider="manual_csv",
                source_ts=DEMO_AVAILABLE_AT,
                available_at=DEMO_AVAILABLE_AT,
            )
        ]
    )

    payload = source_batch_module.execute_priced_in_source_batch(
        engine,
        AppConfig.from_env(),
        source="broker_context",
        stocks_only=True,
    )

    assert payload["status"] == "blocked"
    assert payload["external_calls_made"] == 0
    assert "market_bars must be complete" in payload["reason"]
    assert payload["execution_blocker"]["blocked_by"] == "market_bars"
    assert payload["execution_blocker"]["blocked_gap_rows"] == 1
    assert "market-bars template" in payload["execution_blocker"]["command"]

    run_payload = source_batch_module.execute_priced_in_source_batches(
        engine,
        AppConfig.from_env(),
        source="broker_context",
        max_batches=3,
        stocks_only=True,
    )

    assert run_payload["status"] == "blocked"
    assert run_payload["executed_batches"] == 0
    assert run_payload["external_calls_made"] == 0
    assert run_payload["execution_blocker"]["blocked_by"] == "market_bars"
    assert run_payload["execution_blocker"]["blocked_gap_rows"] == 1
    assert "market-bars template" in run_payload["next_command"]
    assert "--execute-batches" not in run_payload["next_command"]


def test_priced_in_source_batches_prioritize_full_market_bar_coverage(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'demo.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("CATALYST_POLYGON_API_KEY", "test-key")

    assert main(["seed-dashboard-demo"]) == 0
    capsys.readouterr()
    engine = create_engine(database_url, future=True)
    MarketRepository(engine).upsert_securities(
        [
            Security(
                ticker="ZZZZ",
                name="Missing Bar Stock",
                exchange="NASDAQ",
                sector="Technology",
                industry="Software",
                market_cap=1_000_000_000,
                avg_dollar_volume_20d=20_000_000,
                has_options=True,
                is_active=True,
                updated_at=DEMO_AVAILABLE_AT,
                metadata={"type": "CS"},
            )
        ]
    )

    config = AppConfig.from_env(
        {
            "CATALYST_DATABASE_URL": database_url,
            "CATALYST_POLYGON_API_KEY": "test-key",
        }
    )
    overview = priced_in_all_source_gap_batches_payload(engine, config)
    source_rows = {row["source"]: row for row in overview["sources"]}

    assert overview["status"] == "attention"
    assert overview["coverage_first_recommendation"]["source"] == "market_bars"
    assert overview["decision_shortcut_recommendation"] is None
    assert overview["decision_shortcut_blocker"]["blocked_by"] == "market_bars"
    source_gate = overview["source_execution_gate"]
    assert source_gate["status"] == "blocked"
    assert source_gate["execute_next_allowed"] is False
    assert source_gate["blocked_by"] == "market_bars"
    assert 1 <= source_gate["blocked_gap_rows"]
    assert source_gate["external_calls_made"] == 0
    assert source_gate["command"] == overview["coverage_first_recommendation"]["command"]
    catalyst_gate = source_rows["catalyst_events"]["current_blocker_gate"]
    assert catalyst_gate["status"] == "blocked"
    assert catalyst_gate["blocked_by"] == "market_bars"
    assert catalyst_gate["execute_next_allowed"] is False
    assert catalyst_gate["execute_batches_allowed"] is False
    assert catalyst_gate["decision_useful_now"] is False
    assert 1 <= catalyst_gate["blocked_gap_rows"]
    assert source_rows["catalyst_events"]["execute_next_command"] is None
    assert source_rows["catalyst_events"]["execute_batches_command"] is None
    assert source_rows["catalyst_events"]["execute_next_api"] is None
    unblock_options = {
        option["kind"]: option
        for option in overview["mission_brief"]["next_unblock_options"]
    }
    assert unblock_options["manual_csv"]["external_calls_required"] == 0
    assert "market-bars template" in unblock_options["manual_csv"]["command"]
    assert unblock_options["manual_csv"]["request_body"]["missing_only"] is True
    assert unblock_options["manual_csv"]["preview_request_body"]["execute"] is False
    assert unblock_options["manual_csv"]["execute_request_body"]["execute"] is True
    assert unblock_options["saved_provider_capture"]["status"] == "approval_required"
    assert unblock_options["saved_provider_capture"]["approval_required"] is True
    assert unblock_options["saved_provider_capture"]["external_calls_required"] == 1
    assert unblock_options["saved_provider_capture"]["db_writes_during_step"] == 0
    assert unblock_options["saved_provider_capture"]["command"] == (
        "bars saved capture confirm"
    )
    assert unblock_options["saved_provider_capture"]["cli_command"].startswith(
        "catalyst-radar market-bars saved-capture "
    )
    assert unblock_options["saved_provider_capture"]["tui_command"] == (
        "bars saved capture confirm"
    )
    assert unblock_options["saved_provider_capture"]["api"] == (
        "POST /api/radar/market-bars/provider-fixture-capture"
    )
    assert unblock_options["saved_provider_capture"]["request_body"][
        "confirm_external_call"
    ] is False
    assert unblock_options["saved_provider_capture"]["confirm_request_body"][
        "confirm_external_call"
    ] is True
    assert "Approve one Polygon/Massive" in unblock_options[
        "saved_provider_capture"
    ]["question"]
    assert unblock_options["validate_saved_file"]["external_calls_required"] == 0
    assert unblock_options["validate_saved_file"]["api"] == (
        "POST /api/radar/market-bars/provider-fixture-preview"
    )
    assert unblock_options["validate_saved_file"]["request_body"]["fixture_path"]
    assert unblock_options["preview_import"]["request_body"]["execute"] is False
    recommended = overview["mission_brief"]["recommended_unblock_action"]
    assert recommended["kind"] == "saved_provider_capture"
    assert recommended["status"] == "approval_required"
    assert recommended["approval_required"] is True
    assert recommended["external_calls_required"] == 1
    assert recommended["db_writes_required"] == 0
    assert recommended["command"] == "bars saved capture confirm"
    assert recommended["cli_command"].startswith(
        "catalyst-radar market-bars saved-capture "
    )
    assert recommended["tui_command"] == "bars saved capture confirm"
    assert recommended["request_body"]["confirm_external_call"] is True
    assert "Approve one Polygon/Massive" in recommended["reason"]
    assert overview["coverage_first_recommendation"]["coverage_basis"] == (
        "active_universe_as_of_bars"
    )
    assert "full-market coverage" in overview["coverage_first_recommendation"][
        "rationale"
    ]
    assert "active row" in overview["decision_shortcut_blocker"]["action"]
    assert source_rows["market_bars"]["status"] == "attention"
    assert source_rows["market_bars"]["coverage_basis"] == "active_universe_as_of_bars"
    assert source_rows["market_bars"]["blocked_gap_rows"] == source_rows[
        "market_bars"
    ]["total_gap_rows"]
    assert source_rows["market_bars"]["total_gap_rows"] >= 1
    assert source_rows["market_bars"]["diagnostic"]["blocked_reason"] == (
        "missing_active_as_of_bars"
    )
    assert "market-bars template" in source_rows["market_bars"]["diagnostic"][
        "manual_template_command"
    ]
    assert "--stocks-only" not in source_rows["market_bars"]["diagnostic"][
        "manual_template_command"
    ]
    assert "data\\local\\manual-bars-" in source_rows["market_bars"]["diagnostic"][
        "manual_validate_command"
    ]
    assert "<fresh-bars.csv>" not in source_rows["market_bars"]["diagnostic"][
        "manual_validate_command"
    ]
    assert "data\\local\\manual-bars-" in source_rows["market_bars"]["diagnostic"][
        "manual_fix_command"
    ]
    assert "<fresh-bars.csv>" not in source_rows["market_bars"]["diagnostic"][
        "manual_fix_command"
    ]
    assert source_rows["market_bars"]["plan_command"] == source_rows["market_bars"][
        "diagnostic"
    ]["manual_template_command"]
    assert source_rows["market_bars"]["command"] == source_rows["market_bars"][
        "plan_command"
    ]
    assert source_rows["market_bars"]["plan_api"] is None
    assert source_rows["market_bars"]["diagnostic"][
        "provider_saved_file_status"
    ] == "missing"
    provider_saved_file_path = source_rows["market_bars"]["diagnostic"][
        "provider_saved_file_path"
    ]
    assert "data\\local\\polygon-grouped-daily-" in provider_saved_file_path
    assert provider_saved_file_path in source_rows["market_bars"]["diagnostic"][
        "provider_saved_file_capture_command"
    ]
    assert "--out" in source_rows["market_bars"]["diagnostic"][
        "provider_saved_file_capture_command"
    ]
    assert "saved-validate" in source_rows["market_bars"]["diagnostic"][
        "provider_saved_file_validate_command"
    ]
    assert "--fixture" in source_rows["market_bars"]["diagnostic"][
        "provider_saved_file_import_command"
    ]
    assert (
        source_rows["market_bars"]["diagnostic"][
            "provider_saved_file_capture_external_call_count"
        ]
        == 1
    )
    approval_packet = source_rows["market_bars"]["diagnostic"][
        "provider_saved_file_capture_approval_packet"
    ]
    assert approval_packet["status"] == "approval_required"
    assert approval_packet["external_calls_if_approved"] == 1
    assert approval_packet["db_writes_during_capture"] == 0
    answer_options = (
        dashboard_data_module._priced_in_market_bar_blocker_unblock_options(
            {}, {"provider_saved_file_capture_approval_packet": approval_packet}
        )
    )
    answer_saved_option = next(
        option
        for option in answer_options
        if option["kind"] == "saved_provider_capture"
    )
    assert answer_saved_option["command"] == "bars saved capture confirm"
    assert answer_saved_option["cli_command"].startswith(
        "catalyst-radar market-bars saved-capture "
    )
    assert answer_saved_option["tui_command"] == "bars saved capture confirm"
    assert (
        source_rows["market_bars"]["diagnostic"][
            "provider_saved_file_external_call_count"
        ]
        == 0
    )
    assert source_rows["market_bars"]["diagnostic"]["local_bar_history"] == {
        "missing_with_history": 0,
        "missing_without_history": 1,
    }
    missing_universe = source_rows["market_bars"]["diagnostic"]["missing_universe"]
    assert missing_universe["active_metadata_rows"] == 1
    assert missing_universe["zero_avg_dollar_volume_20d_count"] == 0
    assert missing_universe["zero_market_cap_count"] == 0
    assert missing_universe["external_calls_made"] == 0

    assert main(["priced-in-source-batches", "--source", "all"]) == 0
    output = capsys.readouterr()
    assert output.err == ""
    assert (
        "recommended_unblock=saved_provider_capture status=approval_required "
        "approval_required=true calls=1 db_writes=0 "
        "command=catalyst-radar market-bars saved-capture"
    ) in output.out
    assert "tui=bars saved capture confirm" in output.out
    assert "reason=Approve one Polygon/Massive grouped-daily call" in output.out
    assert "unblock=manual_csv status=available calls=0" in output.out
    assert (
        "unblock=saved_provider_capture status=approval_required calls=1 "
        "db_writes=0 command=catalyst-radar market-bars saved-capture"
    ) in output.out
    assert "question=Approve one Polygon/Massive grouped-daily call" in output.out
    assert "provider_saved_file_status=status=missing" in output.out
    assert "provider_saved_file_capture=external_calls=1" in output.out
    assert "source_execution_gate=status=blocked" in output.out
    assert "execute_next_allowed=false" in output.out
    assert "blocked_by=market_bars" in output.out
    assert "reason=Source chunks may be planned" in output.out
    assert "plannable routed blocked batches" in output.out
    assert "--out data\\local\\polygon-grouped-daily-" in output.out
    assert "plan=catalyst-radar market-bars template" in output.out
    assert "provider_saved_file_validate=external_calls=0" in output.out
    assert "provider_saved_file_import=external_calls=0" in output.out
    assert (
        "local_bar_history=missing_with_history=0 missing_without_history=1"
        in output.out
    )
    assert "missing_universe=active_metadata=1" in output.out
    assert "zero_avg_dollar_volume_20d=0" in output.out

    assert main(["priced-in-source-batches", "--source", "options"]) == 0
    output = capsys.readouterr()
    assert output.err == ""
    assert "current_blocker_gate=status=blocked" in output.out
    assert "blocked_by=market_bars" in output.out
    assert "decision_useful_now=false" in output.out
    assert "execute_next_allowed=false" in output.out
    assert "current_blocker_reason=market_bars has" in output.out
    assert "current_blocker_prework=This source plan is review-only" in output.out
    assert "execute_batches=" not in output.out

    tui_update = _apply_command(
        "batch all",
        {},
        "overview",
        DashboardFilters(),
        engine=engine,
        config=AppConfig.from_env({"CATALYST_DATABASE_URL": database_url}),
    )
    assert "Source execution blocked by market_bars" in tui_update.message
    assert "planned source chunks are review-only" in tui_update.message
    assert "Recommended unblock" in tui_update.message
    assert "First executable:" not in tui_update.message
    assert "Capped run:" not in tui_update.message

    tui_source_update = _apply_command(
        "batch options",
        {},
        "overview",
        DashboardFilters(),
        engine=engine,
        config=AppConfig.from_env({"CATALYST_DATABASE_URL": database_url}),
    )
    assert "Current blocker: market_bars" in tui_source_update.message
    assert "review-only" in tui_source_update.message
    assert "Execution is blocked until the current blocker clears." in (
        tui_source_update.message
    )
    assert "batch options execute" not in tui_source_update.message

    assert main(["priced-in-source-batches", "--source", "market_bars"]) == 0
    output = capsys.readouterr()
    assert output.err == ""
    assert "diagnostic_provider_saved_file_status=status=missing" in output.out
    assert "diagnostic_provider_saved_file_capture=external_calls=1" in output.out
    assert "--out data\\local\\polygon-grouped-daily-" in output.out
    assert "plan=catalyst-radar market-bars template" in output.out
    assert "diagnostic_provider_saved_file_validate=external_calls=0" in output.out
    assert "diagnostic_provider_saved_file_import=external_calls=0" in output.out
    assert (
        "diagnostic_local_bar_history=missing_with_history=0 "
        "missing_without_history=1"
    ) in output.out
    assert "diagnostic_missing_universe=active_metadata=1" in output.out

    execution = source_batch_module.execute_priced_in_source_batch(
        engine,
        AppConfig.from_env({"CATALYST_DATABASE_URL": database_url}),
        source="broker_context",
    )

    assert execution["status"] == "blocked"
    assert execution["external_calls_made"] == 0
    assert execution["execution_blocker"]["blocked_by"] == "market_bars"
    assert execution["execution_blocker"]["blocked_gap_rows"] >= 1
    assert "full scan" in execution["reason"]


def test_priced_in_audit_cli_outputs_full_scan_audit(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'demo.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    assert main(["seed-dashboard-demo"]) == 0
    capsys.readouterr()

    assert main(["priced-in-audit"]) == 0
    output = capsys.readouterr()

    assert output.err == ""
    assert "priced_in_audit status=" in output.out
    assert (
        "question=Can MarketRadar answer whether price matches market expectations?"
        in output.out
    )
    assert "market_bars=status=" in output.out
    assert "market_bar_repair=status=" in output.out
    assert "template=catalyst-radar market-bars template" in output.out
    assert "preview_import=catalyst-radar market-bars import" in output.out
    assert "missing_bar_diagnostic=status=" in output.out
    assert "route_boundary=Market bars are required for price-reaction scoring" in (
        output.out
    )
    assert "provider_fill_plan=provider=Polygon/Massive grouped daily" in output.out
    assert "approval_boundary=This plan makes 0 provider calls" in output.out
    assert "manual_template=catalyst-radar market-bars template" in output.out
    assert "--missing-only" in output.out
    assert "source_coverage=ready=" in output.out
    assert "performance=cache=" in output.out
    assert "primary_full_scan=scope=full_active_universe" in output.out
    assert "boundary=The full scan is the ranked universe" in output.out
    assert "recommended_source_gap=source=" in output.out
    assert "boundary=Reviewing this recommendation makes 0 provider calls" in output.out
    assert "full_source_gap_export=catalyst-radar priced-in-audit" in output.out
    assert "sample_boundary=Example tickers are only a priority preview" in output.out
    assert "repair=status=attention diagnostic=no_stored_options" in output.out
    assert "point_in_time_import=catalyst-radar ingest-options --fixture" in output.out
    assert "provider_batch_allowed=true" in output.out
    assert "answer_shortlist=status=" in output.out
    assert "selection=priority_lens_not_scan_universe" in output.out
    assert "ticker rank status decision_ready gap emotion reaction missing next_step" in (
        output.out
    )
    assert "detail=catalyst-radar candidate-detail ACME" in output.out
    assert "source_gap_action=options" in output.out
    assert "instrument_scope=rows=" in output.out
    assert "sec_catalyst_applicability=applicable=" in output.out
    assert "full_scan_rows=" in output.out
    assert "full_scan_preview:" in output.out
    assert "ACME bullish_not_priced_in" in output.out
    assert "sources:" in output.out
    assert "- options status=" in output.out
    assert "- catalyst_events status=" in output.out
    assert "decision=" in output.out
    assert "research=" in output.out
    assert "actionable=" in output.out
    assert "commands:" in output.out

    assert main(["priced-in-audit", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["schema_version"] == "priced-in-full-scan-audit-v1"
    assert payload["external_calls_made"] == 0
    assert payload["scope"]["mode"] == "full_scan"
    assert payload["market_bars"]["repair"]["schema_version"] == (
        "priced-in-market-bar-repair-v1"
    )
    assert payload["market_bars"]["repair"]["external_calls_made"] == 0
    assert payload["market_bars"]["repair"]["diagnostic"]["schema_version"] == (
        "priced-in-market-bar-missing-diagnostic-v1"
    )
    assert payload["market_bars"]["repair"]["diagnostic"]["external_calls_made"] == 0
    assert payload["market_bars"]["repair"]["provider_fill_plan"][
        "schema_version"
    ] == "priced-in-market-bar-provider-fill-plan-v1"
    assert (
        payload["market_bars"]["repair"]["provider_fill_plan"]["external_calls_made"]
        == 0
    )
    assert payload["primary_scan"]["schema_version"] == (
        "priced-in-primary-full-scan-v1"
    )
    assert payload["primary_scan"]["scope"] == "full_active_universe"
    assert payload["preview"]["schema_version"] == "priced-in-full-scan-preview-v1"
    assert payload["preview"]["audit_page_command"] == (
        "catalyst-radar priced-in-audit --limit 25"
    )
    assert payload["answer_shortlist"]["schema_version"] == (
        "priced-in-answer-shortlist-v1"
    )
    assert payload["answer_shortlist"]["external_calls_made"] == 0
    assert payload["answer_shortlist"]["focus"] == "full_scan"
    assert payload["answer_shortlist"]["selection_scope"] == (
        "priority_lens_not_scan_universe"
    )
    assert payload["answer_shortlist"]["rows"][0]["ticker"] == "ACME"
    assert payload["answer_shortlist"]["rows"][0]["drilldown"][
        "detail_command"
    ] == "catalyst-radar candidate-detail ACME"
    assert payload["recommended_source_gap"]["full_scan_command"].startswith(
        "catalyst-radar priced-in-audit --source-gap"
    )
    assert "--all --json" in payload["recommended_source_gap"]["full_scan_command"]
    options_source = next(row for row in payload["sources"] if row["source"] == "options")
    assert options_source["repair"]["schema_version"] == (
        "priced-in-source-gap-repair-v1"
    )
    assert options_source["repair"]["external_calls_made"] == 0
    assert options_source["repair"]["source"] == "options"
    assert payload["preview_rows"][0]["ticker"] == "ACME"
    assert payload["source_coverage"]["source_count"] == 6
    catalyst_source = next(
        row for row in payload["sources"] if row["source"] == "catalyst_events"
    )
    if catalyst_source.get("repair"):
        assert catalyst_source["repair"]["schema_version"] == (
            "priced-in-source-gap-repair-v1"
        )
        assert catalyst_source["repair"]["source"] == "catalyst_events"
        assert catalyst_source["repair"]["external_calls_made"] == 0
    local_text_source = next(
        row for row in payload["sources"] if row["source"] == "local_text"
    )
    if local_text_source.get("repair"):
        assert local_text_source["repair"]["schema_version"] == (
            "priced-in-source-gap-repair-v1"
        )
        assert local_text_source["repair"]["source"] == "local_text"
        assert local_text_source["repair"]["external_calls_made"] == 0
    assert payload["instrument_scope"]["schema_version"] == (
        "priced-in-instrument-scope-v1"
    )

    assert main(["priced-in-audit", "--limit", "1", "--offset", "0", "--json"]) == 0
    paged_payload = json.loads(capsys.readouterr().out)

    assert paged_payload["preview"]["visible_rows"] == 1
    assert paged_payload["preview"]["audit_page_command"] == (
        "catalyst-radar priced-in-audit --limit 1"
    )

    assert main(["priced-in-audit", "--all", "--json"]) == 0
    all_rows_payload = json.loads(capsys.readouterr().out)

    assert all_rows_payload["preview"]["all_rows"] is True
    assert all_rows_payload["preview"]["visible_rows"] == (
        all_rows_payload["preview"]["total_rows"]
    )
    assert all_rows_payload["preview"]["audit_page_command"] == (
        "catalyst-radar priced-in-audit --all"
    )
    assert all_rows_payload["commands"]["audit_full_scan"] == (
        "catalyst-radar priced-in-audit --all --json"
    )

    assert main(["priced-in-audit", "--all"]) == 0
    all_rows_output = capsys.readouterr()

    assert "all_rows=true" in all_rows_output.out

    assert (
        main(["priced-in-audit", "--source-gap", "options", "--limit", "1", "--json"])
        == 0
    )
    source_gap_payload = json.loads(capsys.readouterr().out)

    assert source_gap_payload["scope"]["mode"] == "full_scan"
    assert source_gap_payload["preview"]["filter"]["source_gap"] == ["options"]
    assert source_gap_payload["preview"]["audit_page_command"] == (
        "catalyst-radar priced-in-audit --source-gap options --limit 1"
    )
    assert source_gap_payload["preview"]["source_gap_actions"][0]["source"] == "options"
    assert source_gap_payload["preview"]["source_gap_actions"][0]["plan_command"] == (
        "catalyst-radar priced-in-source-batches --source options --all --json"
    )
    assert source_gap_payload["preview"]["source_gap_actions"][0]["batch_status"] == (
        "ready"
    )
    assert source_gap_payload["preview"]["source_gap_actions"][0][
        "first_batch_tickers"
    ] == ["ACME"]
    assert source_gap_payload["preview"]["source_gap_actions"][0][
        "first_batch_external_calls"
    ] == 1
    assert source_gap_payload["preview"]["source_gap_actions"][0][
        "execute_next_command"
    ] == "catalyst-radar priced-in-source-batches --source options --execute-next"
    assert "options" in source_gap_payload["preview_rows"][0]["missing_sources"]

    assert main(["priced-in-audit", "--source-gap", "options", "--limit", "1"]) == 0
    source_gap_output = capsys.readouterr()

    assert "selected_source_gap_actions:" in source_gap_output.out
    assert "plan=catalyst-radar priced-in-source-batches --source options" in (
        source_gap_output.out
    )
    assert "boundary=Planning and browsing make 0 provider calls" in (
        source_gap_output.out
    )
    assert "source_gap_full_scan_export=catalyst-radar priced-in-queue" in (
        source_gap_output.out
    )
    assert "all_provider_batches=catalyst-radar priced-in-source-batches" in (
        source_gap_output.out
    )
    assert "provider_batch_plan=status=ready" in source_gap_output.out
    assert "first_provider_batch=tickers=ACME calls=1" in source_gap_output.out
    assert (
        "execute_next=catalyst-radar priced-in-source-batches "
        "--source options --execute-next"
    ) in source_gap_output.out
    assert "batch_scope=First provider batch only" in source_gap_output.out
    assert "ticker_scope_note=Returned tickers cover every currently returned" in (
        source_gap_output.out
    )


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
    assert brief["usefulness"]["status"] == "decision_useful"
    assert brief["usefulness"]["decision_ready"] is True
    assert "options" in brief["usefulness"]["optional_context_gaps"]
    assert "options" not in brief["usefulness"]["missing_for_decision"]
    assert brief["evidence"]
    assert brief["next_step"]

    assert main(["candidate-detail", "ACME"]) == 0
    output = capsys.readouterr()

    assert output.err == ""
    assert "candidate_detail ticker=ACME" in output.out
    assert "status=bullish_not_priced_in" in output.out
    assert "why_now=" in output.out
    assert "emotion_vs_reaction=" in output.out
    assert "usefulness=decision_useful decision_ready=true" in output.out
    assert "optional_context=broker_context,options" in output.out
    assert "source_actions:" in output.out
    assert "options status=missing" in output.out
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
    assert payload["first_gap"] == payload["first_blocker"]["area"]
    assert payload["first_blocker"]["external_calls_made"] == 0
    assert payload["operator_next_step"]["area"] == payload["first_blocker"]["area"]
    assert payload["operator_next_step"]["external_calls_made"] == 0
    assert payload["evidence_plan"]["schema_version"] == "priced-in-evidence-plan-v1"
    assert payload["evidence_plan"]["external_calls_made"] == 0
    assert payload["commands"]["review_queue"] == "catalyst-radar priced-in-queue --json"

    assert main(["priced-in-preflight", "--stocks-only", "--json"]) == 0
    stock_output = capsys.readouterr()
    stock_payload = json.loads(stock_output.out)
    assert stock_output.err == ""
    assert stock_payload["stocks_only"] is True
    assert stock_payload["instrument_filter"] == "stocks_only"
    assert stock_payload["commands"]["review_queue"] == (
        "catalyst-radar priced-in-queue --json --stocks-only"
    )
    assert "--stocks-only" in stock_payload["commands"]["market_bars_template"]

    assert main(["priced-in-preflight"]) == 0
    text_output = capsys.readouterr()
    assert text_output.err == ""
    assert "first_blocker area=" in text_output.out
    assert "operator_next_step area=" in text_output.out
    assert "evidence_plan status=" in text_output.out
    assert "priority area status depends_on action command" in text_output.out


def test_tui_full_scan_row_uses_preflight_first_blocker():
    row = _full_scan_coverage_row(
        freshness={"active_security_with_as_of_bar_count": 5521},
        database={
            "active_security_count": 5652,
            "active_security_with_latest_daily_bar_count": 5521,
        },
        scan_yield={"requested_securities": 5652, "scanned_securities": 5521},
        preflight={
            "next_action": "fallback action",
            "first_blocker": {
                "area": "market_bars",
                "status": "blocked",
                "source_gap_count": 131,
            },
            "operator_next_step": {
                "action": "Fill stock-like missing bars first.",
            },
        },
        candidate_count=5521,
        displayed_count=50,
        actionable_count=0,
    )

    assert row["next_action"] == "Fill stock-like missing bars first."
    assert "first blocker market_bars blocked; gaps 131" in row["why_now"]


def test_tui_now_command_explains_priced_in_action_and_response():
    payload = {
        "priced_in_answer": {
            "operator_next_step": {
                "schema_version": "priced-in-operator-next-step-v1",
                "status": "blocked",
                "trusted_priced_in_answer": False,
                "can_use_for_investment_decision": False,
                "investment_decision_boundary": "Decision support only.",
                "scope": "full_market",
                "first_blocker": "market_bars",
                "first_gap_count": 523,
                "action": "Capture one saved provider file.",
                "tui_command": "bars saved capture confirm",
                "external_calls_required": 1,
                "db_" + "writes_required": 0,
                "approval_required": True,
                "response_after_action": "Validate it before import.",
                "external_calls_made": 0,
                "db_" + "writes_made": 0,
            }
        }
    }

    update = _apply_command(
        "now",
        payload,
        "tutorial",
        DashboardFilters(),
        engine=create_engine("sqlite:///:memory:", future=True),
        config=AppConfig.from_env({}),
    )

    assert update.page == "overview"
    assert "Next priced-in action: Capture one saved provider file." in update.message
    assert "run bars saved capture confirm" in update.message
    assert "1 provider call(s) after approval" in update.message
    assert "0 database change(s)" in update.message
    assert "Expected response: Validate it before import." in update.message
    assert "Decision support only." in update.message
    assert "Viewing made 0 provider calls and 0 database changes." in update.message


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

    assert main(["agent-brief", "--real", "--json"]) == 0
    output = capsys.readouterr()
    assert output.err == ""
    payload = json.loads(output.out)
    assert payload["mode"] == "preview"
    assert payload["status"] == "preview"
    assert payload["external_calls_made"]["openai"] == 0

    assert main(["agent-brief", "--real", "--execute", "--json"]) == 2
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
            async def wait_for_payload() -> None:
                for _ in range(80):
                    if app.payload:
                        return
                    await asyncio.sleep(0.05)
                    await pilot.pause()
                raise AssertionError("dashboard snapshot did not load")

            async def wait_for_response(text: str) -> None:
                for _ in range(80):
                    if text in app.status_message:
                        return
                    await asyncio.sleep(0.05)
                    await pilot.pause()
                raise AssertionError(f"dashboard response not seen: {text}")

            await wait_for_payload()
            frame = html.unescape(app.export_screenshot()).replace("\xa0", " ")
            assert "MRDR // MARKET INBOX" in frame
            assert "START" in frame
            assert "Tutorial - your first 90 seconds" in frame
            assert "Press 1 or click Inbox" in frame
            assert "0  Start" in frame
            assert "LEARN" in frame
            assert app.page == "tutorial"

            await pilot.press("1")
            await pilot.pause()
            assert app.page == "overview"
            frame = html.unescape(app.export_screenshot()).replace("\xa0", " ")
            assert "MARKET INBOX" in frame
            assert "ATTENTION QUEUE" in frame
            assert "Latest scan results - rows" in frame
            assert "ACME" in frame
            assert "Bullish not priced" in frame
            assert "Missing / waiting" in frame
            assert "Mailbox" in frame
            assert "market emotion" in frame
            assert "price reaction" in frame
            assert "M  Mismatches only" in frame
            assert "ALL Full scan rows" in frame
            assert "Candidate Review" in frame
            assert "TRADE SAFETY" in frame
            assert "INBOX" in frame
            assert "COST BEFORE EXECUTE" in frame
            assert "ORDERS" in frame
            assert "research" in frame
            assert "KEYS" in frame
            assert "MOUSE" in frame
            assert "NEXT SAFE ACTION" in frame
            assert "Open 1 Urgent message(s) first" in frame
            assert "LAST RESPONSE" in frame
            assert "CORE" in frame
            assert "REVIEW" in frame
            assert "OPERATE" in frame
            assert "Up/Down on sidebar" in frame

            await pilot.press("2")
            await pilot.pause()
            assert app.page == "readiness"
            frame = html.unescape(app.export_screenshot()).replace("\xa0", " ")
            assert "Readiness checklist" in frame
            assert "First must fix:" in frame
            assert "Research-only" in frame

            app.query_one("#data-table").focus()
            await pilot.press("enter")
            await pilot.pause()
            frame = html.unescape(app.export_screenshot()).replace("\xa0", " ")
            assert "Research-only blocker selected:" in frame

            await pilot.press("1")
            await pilot.pause()
            assert app.page == "overview"

            await pilot.press("m")
            await wait_for_payload()
            assert app.page == "overview"
            assert app.filters.priced_in_status == "actionable"
            frame = html.unescape(app.export_screenshot()).replace("\xa0", " ")
            assert "Latest scan results - mismatches rows" in frame
            assert "Mismatches mode" in frame

            assert await pilot.click("#action-scan-all")
            await wait_for_payload()
            assert app.filters.priced_in_status == "all"
            frame = html.unescape(app.export_screenshot()).replace("\xa0", " ")
            assert "Full Scan mode" in frame
            assert "Latest scan results - rows" in frame

            app.query_one("#data-table").focus()
            await pilot.press("enter")
            await pilot.pause()
            assert app.page == "candidate:ACME"
            frame = html.unescape(app.export_screenshot()).replace("\xa0", " ")
            assert "Opened Market Inbox case" in frame
            assert "ACME: no trade decision yet" in frame
            assert "Fix source gaps" in frame

            await pilot.press("1")
            await pilot.pause()
            assert app.page == "overview"
            app.query_one("#data-table").focus()
            await pilot.press("enter")
            await pilot.pause()
            assert app.page == "candidate:ACME"
            frame = html.unescape(app.export_screenshot()).replace("\xa0", " ")
            assert "Opened Market Inbox case" in frame
            assert "ACME: no trade decision yet" in frame
            assert ">> 4  Candidate Review" in frame

            assert await pilot.click("#nav-alerts")
            await pilot.pause()
            assert app.page == "alerts"
            frame = html.unescape(app.export_screenshot()).replace("\xa0", " ")
            assert ">> 5  Alerts [1]" in frame

            await pilot.press("ctrl+p")
            await pilot.pause()
            assert app.page == "review"
            frame = html.unescape(app.export_screenshot()).replace("\xa0", " ")
            assert "Decision Review" in frame

            assert await pilot.click("#nav-candidates")
            await pilot.pause()
            assert app.page == "candidates"
            frame = html.unescape(app.export_screenshot()).replace("\xa0", " ")
            assert ">> 4  Candidate Review" in frame

            assert await pilot.click("#nav-ops")
            await pilot.pause()
            assert app.page == "ops"
            frame = html.unescape(app.export_screenshot()).replace("\xa0", " ")
            assert "Source coverage workbench" in frame
            assert "Enter shows plan" in frame
            assert "batch" in frame
            assert "Coverage-first" in frame

            app.query_one("#data-table").focus()
            await pilot.press("enter")
            await pilot.pause()
            frame = html.unescape(app.export_screenshot()).replace("\xa0", " ")
            assert "LAST RESPONSE" in frame
            assert "full-scan" in frame
            assert "first provider chunk" in frame
            assert "provider calls" in frame

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
            await wait_for_response("Snapshot refreshed from the local database.")
            frame = html.unescape(app.export_screenshot()).replace("\xa0", " ")
            assert "LAST RESPONSE" in frame
            assert "Snapshot refreshed from the local database." in frame

    asyncio.run(run_app())


def test_modern_dashboard_tui_paints_before_snapshot_load(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'slow.db').as_posix()}"
    engine = create_engine(database_url, future=True)
    started = threading.Event()
    release = threading.Event()

    def slow_snapshot(**kwargs: object) -> dict[str, object]:
        started.set()
        if not release.wait(timeout=5):
            raise TimeoutError("test did not release dashboard snapshot worker")
        return {
            "schema_version": "dashboard-cli-snapshot-v1",
            "external_calls_made": 0,
            "controls": {"ticker": None},
            "readiness": {"status": "research_only"},
            "priced_in_queue": {
                "filters": {"status": "all"},
                "count": 0,
                "total_count": 0,
                "offset": 0,
            },
            "priced_in_answer": {"status": "blocked", "answer": "loading test"},
            "priced_in_audit": {"status": "blocked"},
            "ops_health": {"database": {}},
            "broker": {"snapshot": {}},
            "runtime_context": {"build": {"commit": "test"}},
            "call_plan": {"max_external_call_count": 0},
            "candidates": {"count": 0, "rows": []},
            "alerts": {"count": 0, "rows": []},
            "ipo_s1": {"count": 0, "rows": []},
            "feature_inventory": [],
        }

    monkeypatch.setattr(dashboard_tui_module, "dashboard_snapshot_payload", slow_snapshot)

    app = MarketRadarDashboardApp(
        engine=engine,
        config=AppConfig.from_env(),
        dotenv_loaded=False,
        filters=DashboardFilters(),
        initial_page="tutorial",
    )

    async def run_app() -> None:
        try:
            async with app.run_test(size=(150, 44)) as pilot:
                for _ in range(40):
                    if started.is_set():
                        break
                    await asyncio.sleep(0.05)
                    await pilot.pause()
                assert started.is_set()

                frame = html.unescape(app.export_screenshot()).replace("\xa0", " ")
                assert "MRDR // MARKET INBOX" in frame
                assert "START" in frame
                assert "Tutorial - your first 90 seconds" in frame
                assert "Loading local dashboard snapshot" in frame
                assert app.payload == {}

                release.set()
                for _ in range(40):
                    if app.payload:
                        break
                    await asyncio.sleep(0.05)
                    await pilot.pause()
                assert app.payload["schema_version"] == "dashboard-cli-snapshot-v1"
                frame = html.unescape(app.export_screenshot()).replace("\xa0", " ")
                assert "Snapshot loaded from the local database" in frame
        finally:
            release.set()

    asyncio.run(run_app())

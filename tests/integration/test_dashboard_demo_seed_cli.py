from __future__ import annotations

import asyncio
import html
import json
from datetime import timedelta
from pathlib import Path

from sqlalchemy import create_engine

from catalyst_radar.cli import main
from catalyst_radar.core.config import AppConfig
from catalyst_radar.core.models import DailyBar, Security
from catalyst_radar.dashboard import source_batches as source_batch_module
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
    _apply_command,
    _market_bar_manual_fill_progress_summary,
    _market_bar_missing_type_summary,
    _market_bar_operator_step_summary,
    _market_bar_provider_fill_summary,
    _priced_in_overview_rows,
    _priced_in_review_rows,
    _priced_in_source_workflow_payload,
    _stock_market_bar_next_summary,
    dashboard_filters_for_page,
    dashboard_snapshot_payload,
    render_dashboard_tui,
    run_dashboard_tui,
)
from catalyst_radar.storage.repositories import MarketRepository


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
    assert payload["controls"]["priced_in_stocks_only"] is False
    assert payload["priced_in_queue"]["filters"]["status"] == "all"
    assert payload["controls"]["priced_in_usefulness"] is None
    assert payload["controls"]["priced_in_decision_gap"] == []
    assert payload["readiness"]["schema_version"] == "radar-readiness-v1"
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
        "Full-market priced-in queue - showing",
        "#",
        "ACME",
        "Bullish not priced",
        "emotion",
        "reaction",
        "ticker rows are the current priced-in scan page",
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
    assert "Source Fill Workflow" in output.out
    assert "Start with broker_context" in output.out
    assert "decision-ready row(s)" in output.out
    assert "options" in output.out
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
        plan_calls += 1
        assert kwargs["source"] == "local_text"
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
    assert "Priced-in Evidence Plan" in output.out
    assert "Evidence status" in output.out
    assert "Inspect source blocker" in output.out
    assert "Type `batch" in output.out
    assert "exact call budget" in output.out
    assert "priced-in-source-" in output.out


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
    assert "Priced-in answer is" in output.out


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
    assert "Full-market priced-in queue - showing" in output.out
    assert "Full scan audit:" in output.out
    assert "Instrument scope:" in output.out
    assert "Decision readiness:" in output.out
    assert "Data gaps" in output.out
    assert "Next data step:" in output.out
    assert "Full-scan coverage:" in output.out
    assert "Shortlist context:" in output.out
    assert "ticker rows are the current priced-in scan page" in output.out

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
    assert "Decision-ready not-priced-in rows - showing" in output.out
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
    assert _market_bar_operator_step_summary(payload).startswith(
        "Finish or clear partial OHLCV/VWAP rows"
    )
    assert _market_bar_provider_fill_summary(payload).startswith(
        "ready_for_approval_with_health_warning; 1 external call(s)"
    )
    assert _stock_market_bar_next_summary(payload).startswith(
        "5521/5652 stock-like rows have scan-date bars; 131 missing"
    )
    overview = render_dashboard_tui(payload, page="overview", width=160)
    ops = render_dashboard_tui(payload, page="ops", width=160)
    assert "Stock bar next: 5521/5652 stock-like rows have scan-date bars" in overview
    assert "Stock bar next: 5521/5652 stock-like rows have scan-date bars" in ops
    assert "Regenerate the blank local CSV so it includes name" in overview
    assert "Regenerate the blank local CSV so it includes name" in ops
    assert "Provider fill: ready_for_approval_with_health_warning" in overview
    assert "Provider fill: ready_for_approval_with_health_warning" in ops
    assert "Manual CSV progress: 12/523 complete; 3 partial; 508 empty" in overview
    assert "Manual CSV progress: 12/523 complete; 3 partial; 508 empty" in ops
    assert "Market bar next: Finish or clear partial OHLCV/VWAP rows" in overview
    assert "Market bar next: Finish or clear partial OHLCV/VWAP rows" in ops


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
    assert "Full-market priced-in queue" in overview
    assert "ACME" in overview
    assert "BETA" in overview


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
    source_rows = {row["source"]: row for row in overview["sources"]}
    assert source_rows["options"]["execute_next_command"] == (
        "catalyst-radar priced-in-source-batches --source options --execute-next"
    )
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
    assert "full_scan=mode=full_scan" in output.out
    assert "sample=false" in output.out
    assert "review_full_scan=catalyst-radar priced-in-queue --full-scan" in output.out
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
    assert payload["full_scan"]["schema_version"] == (
        "priced-in-full-scan-summary-v1"
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


def test_priced_in_source_batches_prioritize_full_market_bar_coverage(
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

    overview = priced_in_all_source_gap_batches_payload(
        engine,
        AppConfig.from_env({"CATALYST_DATABASE_URL": database_url}),
    )
    source_rows = {row["source"]: row for row in overview["sources"]}

    assert overview["status"] == "attention"
    assert overview["coverage_first_recommendation"]["source"] == "market_bars"
    assert overview["decision_shortcut_recommendation"] is None
    assert overview["decision_shortcut_blocker"]["blocked_by"] == "market_bars"
    assert overview["coverage_first_recommendation"]["coverage_basis"] == (
        "active_universe_as_of_bars"
    )
    assert "full-market coverage" in overview["coverage_first_recommendation"][
        "rationale"
    ]
    assert "active row" in overview["decision_shortcut_blocker"]["action"]
    assert source_rows["market_bars"]["status"] == "attention"
    assert source_rows["market_bars"]["coverage_basis"] == "active_universe_as_of_bars"
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
    assert payload["evidence_plan"]["schema_version"] == "priced-in-evidence-plan-v1"
    assert payload["evidence_plan"]["external_calls_made"] == 0
    assert payload["commands"]["review_queue"] == "catalyst-radar priced-in-queue --json"

    assert main(["priced-in-preflight"]) == 0
    text_output = capsys.readouterr()
    assert text_output.err == ""
    assert "evidence_plan status=" in text_output.out
    assert "priority area status depends_on action command" in text_output.out


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
            assert "ACME" in frame
            assert "Bullish not priced" in frame
            assert "Data gaps" in frame
            assert "showing the first ranked page from the entire scan" in frame
            assert "M  Mismatches only" in frame
            assert "ALL Full scan rows" in frame
            assert "Candidates [1]" in frame
            assert "PRICE ANSWER" in frame
            assert "Priced-in answer" in frame
            assert "Trade safe?" in frame
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
            await pilot.press("enter")
            await pilot.pause()
            assert app.page == "candidate:ACME"
            frame = html.unescape(app.export_screenshot()).replace("\xa0", " ")
            assert "Opened full-scan row 1 for ACME" in frame

            await pilot.press("1")
            await pilot.pause()
            assert app.page == "overview"
            app.query_one("#data-table").focus()
            await pilot.press("enter")
            await pilot.pause()
            assert app.page == "candidate:ACME"
            frame = html.unescape(app.export_screenshot()).replace("\xa0", " ")
            assert "Opened full-scan row 1 for ACME" in frame
            assert ">> 4  Candidates [1]" in frame

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
            assert ">> 4  Candidates [1]" in frame

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
            await pilot.pause()
            frame = html.unescape(app.export_screenshot()).replace("\xa0", " ")
            assert "LAST RESPONSE" in frame
            assert "Snapshot refreshed from the local database." in frame

    asyncio.run(run_app())

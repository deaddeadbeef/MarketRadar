from __future__ import annotations

import json
import shutil
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from uuid import uuid4

from sqlalchemy.engine import Engine
from textual import events
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Grid, Horizontal, Vertical
from textual.widgets import DataTable, Footer, Header, Input, Static

from catalyst_radar.agents.sdk_orchestrator import run_market_radar_agents
from catalyst_radar.brokers.interactive import (
    create_blocked_order_ticket,
    create_trigger,
    evaluate_triggers,
    opportunity_action_payload,
    order_ticket_payload,
    record_opportunity_action,
    trigger_payload,
)
from catalyst_radar.connectors.base import ConnectorRequest
from catalyst_radar.connectors.options import (
    OptionsAggregateConnector,
    validate_options_fixture_json,
    write_options_fixture_template_json,
)
from catalyst_radar.connectors.polygon_fixture import (
    capture_polygon_grouped_daily_response_with_preview,
    ingest_polygon_grouped_daily_fixture,
    preview_polygon_grouped_daily_fixture,
)
from catalyst_radar.connectors.provider_ingest import (
    ProviderIngestError,
    ingest_provider_records,
)
from catalyst_radar.core.config import AppConfig
from catalyst_radar.dashboard import data as dashboard_data
from catalyst_radar.dashboard.source_batches import (
    execute_priced_in_source_batch as execute_source_batch,
)
from catalyst_radar.dashboard.source_batches import (
    execute_priced_in_source_batches as execute_source_batches,
)
from catalyst_radar.dashboard.source_batches import (
    source_batch_execution_summary,
    source_batch_run_summary,
)
from catalyst_radar.events.sec_cik import (
    apply_sec_cik_overrides_csv,
    validate_sec_cik_overrides_csv,
    write_sec_cik_override_template_csv,
)
from catalyst_radar.feedback.service import (
    FeedbackError,
)
from catalyst_radar.feedback.service import (
    record_feedback as record_alert_feedback,
)
from catalyst_radar.jobs.scheduler import SchedulerConfig, run_once, scheduler_run_payload
from catalyst_radar.market.manual_bars import (
    import_manual_market_bars,
    saved_capture_approval_guard_payload,
    write_manual_market_bars_template,
)
from catalyst_radar.market.status import (
    market_bars_import_verification_payload,
    market_bars_post_capture_verification_payload,
)
from catalyst_radar.security.licenses import redact_restricted_external_payload
from catalyst_radar.storage.broker_repositories import BrokerRepository
from catalyst_radar.storage.feature_repositories import FeatureRepository
from catalyst_radar.storage.job_repositories import JobLockRepository
from catalyst_radar.storage.provider_repositories import ProviderRepository
from catalyst_radar.storage.repositories import MarketRepository

RADAR_RUN_COOLDOWN_LOCK_NAME = "manual_radar_run_cooldown"


@dataclass(frozen=True)
class DashboardFilters:
    ticker: str | None = None
    available_at: datetime | None = None
    alert_status: str | None = None
    alert_route: str | None = None
    priced_in_status: str = "all"
    priced_in_usefulness: str | None = None
    priced_in_source_gap: str | Sequence[str] | None = None
    priced_in_decision_gap: str | Sequence[str] | None = None
    priced_in_stocks_only: bool = False
    priced_in_limit: int = 50
    priced_in_offset: int = 0
    telemetry_limit: int = 8

    def normalized(self) -> DashboardFilters:
        ticker = (self.ticker or "").strip().upper() or None
        alert_status = (self.alert_status or "").strip() or None
        alert_route = (self.alert_route or "").strip() or None
        priced_in_status = _normalize_priced_in_status(self.priced_in_status)
        priced_in_source_gap = _normalize_source_gap_filter(
            self.priced_in_source_gap
        )
        priced_in_decision_gap = _normalize_decision_gap_filter(
            self.priced_in_decision_gap
        )
        return replace(
            self,
            ticker=ticker,
            alert_status=alert_status,
            alert_route=alert_route,
            priced_in_status=priced_in_status,
            priced_in_usefulness=_normalize_optional_filter(self.priced_in_usefulness),
            priced_in_source_gap=priced_in_source_gap,
            priced_in_decision_gap=priced_in_decision_gap,
            priced_in_stocks_only=bool(self.priced_in_stocks_only),
            priced_in_limit=min(200, max(1, int(self.priced_in_limit))),
            priced_in_offset=max(0, int(self.priced_in_offset)),
            telemetry_limit=max(1, int(self.telemetry_limit)),
        )


def _normalize_priced_in_status(value: object) -> str:
    status = str(value or "").strip().lower().replace(" ", "_")
    aliases = {
        "": "all",
        "m": "actionable",
        "mismatch": "actionable",
        "mismatches": "actionable",
        "not_priced_in": "actionable",
        "not-priced-in": "actionable",
        "full": "all",
        "full_scan": "all",
        "full-scan": "all",
    }
    status = aliases.get(status, status)
    allowed = {
        "all",
        "actionable",
        "bullish_not_priced_in",
        "bearish_not_priced_in",
        "blocked",
        "neutral",
        "stale",
        "fully_priced",
        "overextended_hype",
        "conflicted",
    }
    return status if status in allowed else "all"


def _normalize_decision_gap_filter(value: str | Sequence[str] | None) -> tuple[str, ...]:
    if value is None:
        raw_values: list[object] = []
    elif isinstance(value, str):
        raw_values = [value]
    else:
        raw_values = list(value)
    aliases = {
        "packet": "candidate_packet",
        "candidate-packet": "candidate_packet",
        "candidate_packets": "candidate_packet",
        "card": "decision_card",
        "decision_cards": "decision_card",
        "decision-card": "decision_card",
        "broker": "broker_context",
        "schwab": "broker_context",
        "portfolio": "broker_context",
        "options_flow": "options",
    }
    normalized: list[str] = []
    for raw in raw_values:
        for part in str(raw or "").replace(";", ",").split(","):
            gap = part.strip().lower().replace("-", "_").replace(" ", "_")
            if gap in {"", "all", "none"}:
                continue
            normalized.append(aliases.get(gap, gap))
    return tuple(dict.fromkeys(normalized))


def _normalize_optional_filter(value: object | None) -> str | None:
    normalized = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    return None if normalized in {"", "all", "any", "none"} else normalized


def _normalize_source_gap_filter(value: str | Sequence[str] | None) -> tuple[str, ...]:
    if value is None:
        raw_values: list[object] = []
    elif isinstance(value, str):
        raw_values = [value]
    else:
        raw_values = list(value)
    aliases = {
        "bars": "market_bars",
        "market": "market_bars",
        "market_data": "market_bars",
        "events": "catalyst_events",
        "event": "catalyst_events",
        "catalysts": "catalyst_events",
        "catalyst": "catalyst_events",
        "text": "local_text",
        "local": "local_text",
        "news": "local_text",
        "narrative": "local_text",
        "option": "options",
        "options_flow": "options",
        "theme": "theme_peer_sector",
        "themes": "theme_peer_sector",
        "peer": "theme_peer_sector",
        "sector": "theme_peer_sector",
        "broker": "broker_context",
        "schwab": "broker_context",
        "portfolio": "broker_context",
    }
    normalized: list[str] = []
    for raw in raw_values:
        for part in str(raw or "").replace(";", ",").split(","):
            source = part.strip().lower().replace("-", "_").replace(" ", "_")
            if source in {"", "all", "any", "none"}:
                continue
            normalized.append(aliases.get(source, source))
    return tuple(dict.fromkeys(normalized))


DASHBOARD_FEATURES: tuple[dict[str, str], ...] = (
    {
        "area": "Readiness",
        "feature": "Investment readiness, usefulness score, and operator next step",
        "page": "overview, readiness",
        "use": "Know whether output is research-only or decision-useful.",
    },
    {
        "area": "Market data",
        "feature": "Run as-of coverage, latest bar coverage, stale-bar blockers",
        "page": "overview, ops",
        "use": "Verify fresh bars before relying on real market data.",
    },
    {
        "area": "Radar run",
        "feature": "Latest run path, required steps, optional gates, call plan",
        "page": "overview, run",
        "use": "Check what will call external providers before executing a cycle.",
    },
    {
        "area": "Candidates",
        "feature": "Candidate queue, decision labels, research gaps, card readiness",
        "page": "candidates",
        "use": "Work the research shortlist and manual-review queue.",
    },
    {
        "area": "Alerts",
        "feature": "Alert rows, route/status filters, suppression context",
        "page": "alerts",
        "use": "Review planned and dry-run alert output before delivery.",
    },
    {
        "area": "IPO/S-1",
        "feature": "SEC S-1 analysis rows, terms, risk flags, source links",
        "page": "ipo",
        "use": "Inspect live SEC catalyst evidence.",
    },
    {
        "area": "Themes",
        "feature": "Theme aggregation over candidate rows",
        "page": "themes",
        "use": "Spot clustered catalysts and repeated setup types.",
    },
    {
        "area": "Validation",
        "feature": "Validation run, useful-alert rate, false positives",
        "page": "validation",
        "use": "Track whether the radar is producing useful output.",
    },
    {
        "area": "Costs",
        "feature": "LLM budget ledger summary and cost per useful alert",
        "page": "costs",
        "use": "Keep optional agentic review bounded.",
    },
    {
        "area": "Agent",
        "feature": "Dry-run multi-agent brief over the priced-in answer and evidence plan",
        "page": "agent",
        "use": "Summarize what matters without hidden provider or broker calls.",
    },
    {
        "area": "Broker",
        "feature": "Read-only Schwab connection, balances, positions, order kill switch",
        "page": "broker",
        "use": "Use portfolio context without enabling real order submission.",
    },
    {
        "area": "Ops",
        "feature": "Provider health, database counts, jobs, degraded mode",
        "page": "ops",
        "use": "Diagnose stale data and provider failures.",
    },
    {
        "area": "Telemetry",
        "feature": "Audit tape and coverage over required operational events",
        "page": "telemetry",
        "use": "Verify operational evidence before trusting status.",
    },
)

PAGE_ALIASES: Mapping[str, str] = {
    "0": "tutorial",
    "learn": "tutorial",
    "start": "tutorial",
    "tut": "tutorial",
    "tutorial": "tutorial",
    "1": "overview",
    "home": "overview",
    "insight": "overview",
    "insights": "overview",
    "o": "overview",
    "overview": "overview",
    "2": "readiness",
    "ready": "readiness",
    "readiness": "readiness",
    "3": "run",
    "run": "run",
    "plan": "run",
    "4": "candidates",
    "c": "candidates",
    "candidates": "candidates",
    "11": "review",
    "d": "review",
    "decision": "review",
    "decisions": "review",
    "decision-ready": "review",
    "decision_ready": "review",
    "review": "review",
    "5": "alerts",
    "a": "alerts",
    "alerts": "alerts",
    "6": "ipo",
    "ipo": "ipo",
    "s1": "ipo",
    "7": "broker",
    "b": "broker",
    "broker": "broker",
    "8": "ops",
    "ops": "ops",
    "9": "telemetry",
    "t": "telemetry",
    "telemetry": "telemetry",
    "10": "agent",
    "agent": "agent",
    "agents": "agent",
    "brief": "agent",
    "themes": "themes",
    "validation": "validation",
    "costs": "costs",
    "features": "features",
    "help": "help",
}

NAVIGATION_TEXT = (
    "0 Tutorial | 1 Insights | 2 Readiness | 3 Run | 4 Candidates | 5 Alerts | "
    "6 IPO/S-1 | 7 Broker | 8 Ops | 9 Telemetry | 10 Agent | 11 Review | "
    "features | help | q"
)

MODERN_PAGES: tuple[tuple[str, str, str], ...] = (
    ("tutorial", "0", "Tutorial"),
    ("overview", "1", "Insights"),
    ("readiness", "2", "Readiness"),
    ("run", "3", "Run"),
    ("candidates", "4", "Candidates"),
    ("review", "11", "Decision Review"),
    ("alerts", "5", "Alerts"),
    ("ipo", "6", "IPO/S-1"),
    ("broker", "7", "Broker"),
    ("ops", "8", "Ops"),
    ("telemetry", "9", "Telemetry"),
    ("agent", "10", "Agent"),
    ("features", "F", "Features"),
    ("help", "?", "Help"),
)


def dashboard_filters_for_page(
    filters: DashboardFilters,
    page: str,
) -> DashboardFilters:
    resolved_page = _normalize_page(page)
    normalized = filters.normalized()
    if resolved_page != "review":
        return normalized
    return replace(
        normalized,
        priced_in_status="actionable",
        priced_in_usefulness="decision_useful",
        priced_in_offset=0,
    ).normalized()


class FocusRow(Static):
    """One-line clickable/focusable row for terminal navigation."""

    can_focus = True


def dashboard_snapshot_payload(
    *,
    engine: Engine,
    config: AppConfig,
    dotenv_loaded: bool,
    filters: DashboardFilters,
) -> dict[str, object]:
    filters = filters.normalized()
    latest_run = dashboard_data.load_radar_run_summary(engine)
    latest_run_cutoff = _datetime_or_none(
        latest_run.get("finished_at") or latest_run.get("decision_available_at")
    )
    data_available_at = filters.available_at or latest_run_cutoff
    candidate_rows = (
        dashboard_data.load_radar_run_candidate_rows(
            engine,
            latest_run,
            include_post_run_artifacts=True,
        )
        if filters.available_at is None and latest_run
        else dashboard_data.load_candidate_rows(engine, available_at=data_available_at)
    )
    theme_rows = dashboard_data.load_theme_rows(engine, available_at=data_available_at)
    alert_rows = dashboard_data.load_alert_rows(
        engine,
        ticker=filters.ticker,
        status=filters.alert_status,
        route=filters.alert_route,
        available_at=data_available_at,
    )
    ipo_rows = dashboard_data.load_ipo_s1_rows(
        engine,
        ticker=filters.ticker,
        available_at=data_available_at,
    )
    validation_summary = dashboard_data.load_validation_summary(engine)
    cost_summary = dashboard_data.load_cost_summary(
        engine,
        available_at=data_available_at,
    )
    broker_summary = dashboard_data.load_broker_summary(engine)
    ops_health = dashboard_data.load_ops_health(engine)
    discovery_snapshot = dashboard_data.radar_discovery_snapshot_payload(
        engine,
        config,
        radar_run_summary=latest_run,
        ops_health=ops_health,
    )
    runtime_context = dashboard_data.runtime_context_payload(
        config,
        radar_run_summary=latest_run,
        dotenv_loaded=dotenv_loaded,
    )
    actionability = dashboard_data.actionability_breakdown_payload(candidate_rows)
    investment_readiness = dashboard_data.investment_readiness_payload(
        discovery_snapshot,
        actionability,
        candidate_rows,
    )
    operator_work_queue = dashboard_data.operator_work_queue_payload(
        config,
        radar_run_summary=latest_run,
        broker_summary=broker_summary,
        discovery_snapshot=discovery_snapshot,
        candidate_rows=candidate_rows,
    )
    priced_in_queue = dashboard_data.priced_in_queue_payload(
        engine,
        config,
        limit=filters.priced_in_limit,
        offset=filters.priced_in_offset,
        available_at=filters.available_at,
        status=filters.priced_in_status,
        usefulness=filters.priced_in_usefulness,
        source_gap=filters.priced_in_source_gap,
        decision_gap=filters.priced_in_decision_gap,
        stocks_only=filters.priced_in_stocks_only,
        include_planning_rows=True,
        latest_run_summary=latest_run,
        broker_summary=broker_summary,
        discovery_snapshot=discovery_snapshot,
    )
    priced_in_source_coverage = (
        priced_in_queue.get("source_coverage")
        if isinstance(priced_in_queue.get("source_coverage"), Mapping)
        else dashboard_data.priced_in_source_coverage_summary(candidate_rows)
    )
    priced_in_preflight = (
        dict(priced_in_queue["preflight"])
        if isinstance(priced_in_queue.get("preflight"), Mapping)
        else dashboard_data.priced_in_preflight_payload(
            engine,
            config,
            latest_run=latest_run,
            discovery_snapshot=discovery_snapshot,
            source_coverage=priced_in_source_coverage,
        )
    )
    priced_in_market_bars = dashboard_data._priced_in_audit_market_bars(
        engine,
        config,
        priced_in_queue,
        priced_in_preflight,
    )
    priced_in_answer = dashboard_data.priced_in_answer_payload(
        engine,
        config,
        queue=priced_in_queue,
        preflight=priced_in_preflight,
        stocks_only=filters.priced_in_stocks_only,
        market_bars=priced_in_market_bars,
    )
    priced_in_source_workflow = _priced_in_source_workflow_payload(
        priced_in_preflight,
        priced_in_queue=priced_in_queue,
        priced_in_answer=priced_in_answer,
    )
    priced_in_audit = dashboard_data.priced_in_full_scan_audit_payload(
        engine,
        config,
        queue=priced_in_queue,
        preflight=priced_in_preflight,
        stocks_only=filters.priced_in_stocks_only,
        market_bars=priced_in_market_bars,
    )
    operator_next_step = dashboard_data.operator_next_step_payload(operator_work_queue)
    readiness_payload = dashboard_data.radar_readiness_payload(
        engine,
        config,
        radar_run_summary=latest_run,
        candidate_rows=candidate_rows,
        broker_summary=broker_summary,
        ops_health=ops_health,
        discovery_snapshot=discovery_snapshot,
    )
    telemetry = dashboard_data.telemetry_tape_payload(
        ops_health,
        limit=filters.telemetry_limit,
    )
    display_priced_in_queue = dict(priced_in_queue)
    display_priced_in_queue.pop("planning_rows", None)
    payload = {
        "schema_version": "dashboard-cli-snapshot-v1",
        "feature_inventory": list(DASHBOARD_FEATURES),
        "controls": {
            "ticker": filters.ticker,
            "available_at": (
                data_available_at.isoformat() if data_available_at is not None else None
            ),
            "alert_status": filters.alert_status,
            "alert_route": filters.alert_route,
            "priced_in_status": filters.priced_in_status,
            "priced_in_usefulness": filters.priced_in_usefulness,
            "priced_in_source_gap": list(filters.priced_in_source_gap or ()),
            "priced_in_decision_gap": list(filters.priced_in_decision_gap or ()),
            "priced_in_stocks_only": filters.priced_in_stocks_only,
            "priced_in_limit": filters.priced_in_limit,
            "priced_in_offset": filters.priced_in_offset,
            "telemetry_limit": filters.telemetry_limit,
        },
        "runtime_context": runtime_context,
        "readiness": readiness_payload,
        "radar_run_cooldown": dashboard_data.radar_run_cooldown_payload(engine, config),
        "latest_run": latest_run,
        "discovery_snapshot": discovery_snapshot,
        "actionability_breakdown": actionability,
        "investment_readiness": investment_readiness,
        "operator_work_queue": operator_work_queue,
        "operator_next_step": operator_next_step,
        "priced_in_preflight": priced_in_preflight,
        "priced_in_queue": display_priced_in_queue,
        "priced_in_answer": priced_in_answer,
        "priced_in_audit": priced_in_audit,
        "priced_in_source_coverage": priced_in_source_coverage,
        "priced_in_source_workflow": priced_in_source_workflow,
        "candidates": {
            "count": len(candidate_rows),
            "rows": candidate_rows,
        },
        "themes": {
            "count": len(theme_rows),
            "rows": theme_rows,
        },
        "alerts": {
            "count": len(alert_rows),
            "rows": alert_rows,
        },
        "ipo_s1": {
            "count": len(ipo_rows),
            "rows": ipo_rows,
        },
        "validation": validation_summary,
        "costs": cost_summary,
        "live_activation": dashboard_data.live_data_activation_contract_payload(
            config,
            radar_run_summary=latest_run,
            broker_summary=broker_summary,
        ),
        "call_plan": dashboard_data.radar_run_call_plan_payload(engine, config),
        "broker": broker_summary,
        "ops_health": ops_health,
        "telemetry": telemetry,
        "telemetry_coverage": dashboard_data.telemetry_coverage_payload(engine),
        "external_calls_made": 0,
    }
    payload["agent_brief"] = run_market_radar_agents(payload, config, real=False)
    redacted = redact_restricted_external_payload(payload)
    return redacted if isinstance(redacted, dict) else payload


def run_dashboard_tui(
    *,
    engine: Engine,
    config: AppConfig,
    dotenv_loaded: bool,
    filters: DashboardFilters,
    initial_page: str = "overview",
    input_fn: Callable[[str], str] = input,
    output_fn: Callable[[str], object] = print,
    clear_screen: bool = True,
) -> int:
    if input_fn is input and output_fn is print:
        app = MarketRadarDashboardApp(
            engine=engine,
            config=config,
            dotenv_loaded=dotenv_loaded,
            filters=filters,
            initial_page=initial_page,
        )
        result = app.run(
            mouse=True,
            inline=not clear_screen,
            inline_no_clear=not clear_screen,
        )
        return int(result or 0)
    return _run_dashboard_tui_legacy(
        engine=engine,
        config=config,
        dotenv_loaded=dotenv_loaded,
        filters=filters,
        initial_page=initial_page,
        input_fn=input_fn,
        output_fn=output_fn,
        clear_screen=clear_screen,
    )


def _run_dashboard_tui_legacy(
    *,
    engine: Engine,
    config: AppConfig,
    dotenv_loaded: bool,
    filters: DashboardFilters,
    initial_page: str = "overview",
    input_fn: Callable[[str], str] = input,
    output_fn: Callable[[str], object] = print,
    clear_screen: bool = True,
) -> int:
    current_filters = filters.normalized()
    page = _normalize_page(initial_page)
    message = ""
    while True:
        payload = dashboard_snapshot_payload(
            engine=engine,
            config=config,
            dotenv_loaded=dotenv_loaded,
            filters=current_filters,
        )
        screen = render_dashboard_tui(payload, page=page)
        if message:
            screen = f"{screen}\n\n{message}"
            message = ""
        if clear_screen:
            screen = f"\033[2J\033[H{screen}"
        output_fn(screen)
        raw = input_fn("market-radar> ").strip()
        update = _apply_command(
            raw,
            payload,
            page,
            current_filters,
            engine=engine,
            config=config,
        )
        if update.exit_requested:
            return 0
        page = update.page
        current_filters = update.filters
        message = update.message


class MarketRadarDashboardApp(App[int]):
    """Modern mouse-friendly terminal dashboard for Windows Terminal."""

    CSS = """
    Screen {
        background: #070b10;
        color: #d7dde8;
    }

    Header {
        background: #05080c;
        color: #f2f5f8;
    }

    Footer {
        background: #05080c;
        color: #aeb8c6;
    }

    #workspace {
        height: 1fr;
    }

    #sidebar {
        width: 30;
        min-width: 28;
        background: #08111a;
        border-right: solid #1b3a52;
        padding: 1 1;
    }

    .brand {
        height: 3;
        content-align: center middle;
        text-style: bold;
        color: #7ee787;
        background: #0c1a24;
        border: round #25516f;
        margin-bottom: 1;
    }

    .side-section {
        height: 1;
        color: #58a6ff;
        text-style: bold;
        margin: 1 0 0 0;
    }

    .nav-item {
        width: 100%;
        height: 1;
        content-align: left middle;
        padding: 0 1;
        color: #b7c2d0;
        background: #0b141d;
    }

    .nav-item:hover {
        background: #12263a;
        color: #ffffff;
    }

    .nav-item.active {
        background: #17466b;
        color: #f2fdff;
        text-style: bold;
    }

    .side-action {
        width: 100%;
        height: 1;
        content-align: left middle;
        padding: 0 1;
        color: #7ee787;
        background: #0d1f19;
        margin-top: 0;
    }

    .side-action:hover {
        background: #133c2d;
        color: #ffffff;
    }

    .side-action.active {
        background: #17466b;
        color: #f2fdff;
        text-style: bold;
    }

    .nav-item:focus, .side-action:focus {
        background: #235a83;
        color: #ffffff;
        text-style: bold;
    }

    #main {
        width: 1fr;
        padding: 0 1;
    }

    #hero {
        height: 4;
        border: round #25516f;
        background: #0c141d;
        padding: 0 1;
        margin-bottom: 0;
    }

    #nav-helpbar {
        height: 2;
        background: #09131c;
        color: #b7c2d0;
        padding: 0 1;
        margin-bottom: 0;
    }

    #metric-row {
        layout: grid;
        grid-size: 4 1;
        grid-gutter: 0 1;
        height: 4;
        margin-bottom: 0;
    }

    .metric {
        height: 4;
        border: round #20394f;
        background: #0b141d;
        padding: 0 1;
    }

    #guide {
        height: 4;
        border: round #315473;
        background: #0a151f;
        color: #d7dde8;
        padding: 0 1;
        margin-bottom: 0;
    }

    #section-title {
        height: 1;
        content-align: left middle;
        text-style: bold;
        color: #ffffff;
    }

    #data-table {
        height: 1fr;
        border: round #25516f;
        background: #080d13;
    }

    #detail {
        height: 3;
        border: round #26384d;
        background: #0b141d;
        padding: 0 1;
        margin-top: 0;
    }

    #operator-row {
        layout: grid;
        grid-size: 2 1;
        grid-gutter: 0 1;
        height: 4;
        margin-top: 0;
    }

    #operator-action, #operator-response {
        height: 4;
        border: round #26384d;
        padding: 0 1;
    }

    #operator-action {
        background: #0d1f19;
    }

    #operator-response {
        background: #0d1721;
    }

    #command {
        height: 3;
        margin-top: 0;
    }
    """

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("r", "refresh", "Refresh"),
        Binding("0", "go('tutorial')", "Tutorial", priority=True),
        Binding("1", "go('overview')", "Insights", priority=True),
        Binding("2", "go('readiness')", "Readiness", priority=True),
        Binding("3", "go('run')", "Run", priority=True),
        Binding("4", "go('candidates')", "Candidates", priority=True),
        Binding("5", "go('alerts')", "Alerts", priority=True),
        Binding("6", "go('ipo')", "IPO/S-1", priority=True),
        Binding("7", "go('broker')", "Broker", priority=True),
        Binding("8", "go('ops')", "Ops", priority=True),
        Binding("9", "go('telemetry')", "Telemetry", priority=True),
        Binding("ctrl+a", "go('agent')", "Agent", priority=True),
        ("f", "go('features')", "Features"),
        ("?", "go('help')", "Help"),
        ("d", "decision_ready_scan", "Decision-ready"),
        ("m", "toggle_scan_mode", "Scan mode"),
        Binding("ctrl+n", "next_page", "Next page", priority=True),
        Binding("ctrl+p", "previous_page", "Prev page", priority=True),
        ("escape", "focus_command", "Command"),
    ]

    def __init__(
        self,
        *,
        engine: Engine,
        config: AppConfig,
        dotenv_loaded: bool,
        filters: DashboardFilters,
        initial_page: str,
    ) -> None:
        super().__init__()
        self.engine = engine
        self.config = config
        self.dotenv_loaded = dotenv_loaded
        self.page = _normalize_page(initial_page)
        self.filters = dashboard_filters_for_page(filters, self.page)
        self.payload: Mapping[str, object] = {}
        self.status_message = ""

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal(id="workspace"):
            with Vertical(id="sidebar"):
                yield Static("MRDR // MARKET RADAR", classes="brand")
                for page_key, shortcut, label in MODERN_PAGES:
                    if page_key == "tutorial":
                        yield Static("LEARN", classes="side-section")
                    elif page_key == "overview":
                        yield Static("CORE", classes="side-section")
                    elif page_key == "candidates":
                        yield Static("REVIEW", classes="side-section")
                    elif page_key == "broker":
                        yield Static("OPERATE", classes="side-section")
                    elif page_key == "features":
                        yield Static("SYSTEM", classes="side-section")
                    yield FocusRow(
                        self._nav_label(page_key, shortcut, label),
                        id=f"nav-{page_key}",
                        classes="nav-item",
                    )
                yield Static("OPS", classes="side-section")
                yield FocusRow("R  Refresh snapshot", id="action-refresh", classes="side-action")
                yield FocusRow("RUN Review call plan", id="action-run-page", classes="side-action")
                yield Static("SCAN", classes="side-section")
                yield FocusRow(
                    "D  Decision-ready",
                    id="action-scan-ready",
                    classes="side-action",
                )
                yield FocusRow(
                    "M  Mismatches only",
                    id="action-scan-mismatches",
                    classes="side-action",
                )
                yield FocusRow("ALL Full scan rows", id="action-scan-all", classes="side-action")
            with Vertical(id="main"):
                yield Static(id="hero")
                yield Static(id="nav-helpbar")
                with Grid(id="metric-row"):
                    yield Static(id="metric-readiness", classes="metric")
                    yield Static(id="metric-market", classes="metric")
                    yield Static(id="metric-calls", classes="metric")
                    yield Static(id="metric-broker", classes="metric")
                yield Static(id="guide")
                yield Static(id="section-title")
                yield DataTable(id="data-table", cursor_type="row")
                yield Static(id="detail")
                with Grid(id="operator-row"):
                    yield Static(id="operator-action")
                    yield Static(id="operator-response")
                yield Input(
                    placeholder=(
                        "Type a command or click a row. Try: stocks, ready, full, mismatches, "
                        "2, 4, run, refresh, help, q"
                    ),
                    id="command",
                )
        yield Footer()

    def on_mount(self) -> None:
        self.reload_snapshot()
        self.refresh_view()
        if self.page == "tutorial":
            self.query_one("#nav-tutorial", FocusRow).focus()
        elif self.page == "overview":
            self.query_one("#data-table", DataTable).focus()
        else:
            self.query_one("#command", Input).focus()

    def reload_snapshot(self) -> None:
        self.payload = dashboard_snapshot_payload(
            engine=self.engine,
            config=self.config,
            dotenv_loaded=self.dotenv_loaded,
            filters=self.filters,
        )

    def refresh_view(self) -> None:
        self._refresh_nav()
        self._refresh_scan_actions()
        self._refresh_header()
        self._refresh_table()
        self.query_one("#nav-helpbar", Static).update(self._navigation_text())
        self.query_one("#guide", Static).update(self._guide_text())
        self.query_one("#operator-action", Static).update(self._action_text())
        self.query_one("#operator-response", Static).update(self._response_text())

    def action_refresh(self) -> None:
        self.reload_snapshot()
        self.status_message = "Snapshot refreshed from the local database."
        self.refresh_view()

    def action_go(self, page: str) -> None:
        self.page = _normalize_page(page)
        self.filters = dashboard_filters_for_page(self.filters, self.page)
        self.status_message = ""
        self.reload_snapshot()
        self.refresh_view()

    def on_click(self, event: events.Click) -> None:
        widget_id = event.widget.id if event.widget else ""
        if widget_id.startswith("nav-"):
            event.stop()
            self.action_go(widget_id.removeprefix("nav-"))
            return
        if widget_id == "action-refresh":
            event.stop()
            self.action_refresh()
            return
        if widget_id == "action-run-page":
            event.stop()
            self.action_go("run")
            self.status_message = "Review the call plan, then type run execute if intended."
            self.refresh_view()
            return
        if widget_id == "action-scan-ready":
            event.stop()
            self.action_decision_ready_scan()
            return
        if widget_id == "action-scan-mismatches":
            event.stop()
            self._set_scan_mode("actionable")
            return
        if widget_id == "action-scan-all":
            event.stop()
            self._set_scan_mode("all")
            return

    def on_key(self, event: events.Key) -> None:
        if event.key == "ctrl+n":
            event.stop()
            self.action_next_page()
            return
        if event.key == "ctrl+p":
            event.stop()
            self.action_previous_page()
            return
        if isinstance(self.focused, FocusRow) and event.key in {"down", "j"}:
            event.stop()
            self._move_sidebar_focus(1)
            return
        if isinstance(self.focused, FocusRow) and event.key in {"up", "k"}:
            event.stop()
            self._move_sidebar_focus(-1)
            return
        if event.key != "enter":
            return
        focused_id = self.focused.id if self.focused else ""
        if focused_id.startswith("nav-"):
            event.stop()
            self.action_go(focused_id.removeprefix("nav-"))
            return
        if focused_id == "action-refresh":
            event.stop()
            self.action_refresh()
            return
        if focused_id == "action-run-page":
            event.stop()
            self.action_go("run")
            self.status_message = "Review the call plan, then type run execute if intended."
            self.refresh_view()
            return
        if focused_id == "action-scan-ready":
            event.stop()
            self.action_decision_ready_scan()
            return
        if focused_id == "action-scan-mismatches":
            event.stop()
            self._set_scan_mode("actionable")
            return
        if focused_id == "action-scan-all":
            event.stop()
            self._set_scan_mode("all")

    def action_toggle_scan_mode(self) -> None:
        current = _normalize_priced_in_status(self.filters.priced_in_status)
        self._set_scan_mode("all" if current == "actionable" else "actionable")

    def action_decision_ready_scan(self) -> None:
        self.page = "review"
        self.filters = dashboard_filters_for_page(self.filters, self.page)
        self.status_message = (
            "Decision-ready view: showing not-priced-in rows that passed the "
            "usefulness gate. Press Enter to open a row; type full for the whole "
            "ranked universe."
        )
        self.reload_snapshot()
        self.refresh_view()

    def _set_scan_mode(self, status: str) -> None:
        resolved = _normalize_priced_in_status(status)
        self.filters = replace(
            self.filters,
            priced_in_status=resolved,
            priced_in_usefulness=None,
            priced_in_offset=0,
        ).normalized()
        self.page = "overview"
        self.status_message = (
            "Full Scan mode: showing review page 1; coverage line shows the scan universe."
            if resolved == "all"
            else "Mismatches mode: showing only bullish/bearish not-priced-in rows."
        )
        self.reload_snapshot()
        self.refresh_view()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        raw = event.value.strip()
        event.input.value = ""
        update = _apply_command(
            raw,
            self.payload,
            self.page,
            self.filters,
            engine=self.engine,
            config=self.config,
        )
        if update.exit_requested:
            self.exit(0)
            return
        self.page = update.page
        self.filters = update.filters
        self.status_message = update.message
        self.reload_snapshot()
        self.refresh_view()

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        if self.page in {"overview", "review"}:
            row = self._row_by_key(event.row_key.value)
            target_page = str(row.get("target_page") or "").strip()
            if target_page:
                self.page = target_page
                self.status_message = str(row.get("status_message") or "")
                self.refresh_view()
        elif self.page == "candidates":
            row = self._row_by_key(event.row_key.value)
            ticker = str(row.get("ticker") or "").upper()
            if ticker:
                self.page = f"candidate:{ticker}"
                self.status_message = f"Opened candidate {ticker}."
                self.refresh_view()
        elif self.page == "alerts":
            row = self._row_by_key(event.row_key.value)
            alert_id = str(row.get("id") or "")
            if alert_id:
                self.page = f"alert:{alert_id}"
                self.status_message = f"Opened alert {alert_id}."
                self.refresh_view()
        elif self.page == "ops":
            row = self._row_by_key(event.row_key.value)
            source = str(row.get("source") or "").strip()
            if source:
                self.status_message = _priced_in_source_batch_message(
                    self.engine,
                    self.config,
                    source=source,
                    filters=self.filters,
                )
                self.reload_snapshot()
                self.refresh_view()

    def _row_by_key(self, key: object) -> Mapping[str, object]:
        key_text = str(key)
        for row in self._current_rows():
            if str(row.get("_row_key") or "") == key_text:
                return row
        return {}

    def _refresh_nav(self) -> None:
        active = self._active_nav_page()
        for page_key, shortcut, label in MODERN_PAGES:
            item = self.query_one(f"#nav-{page_key}", FocusRow)
            item.set_class(page_key == active, "active")
            item.update(self._nav_label(page_key, shortcut, label))

    def _refresh_scan_actions(self) -> None:
        status = _normalize_priced_in_status(self.filters.priced_in_status)
        usefulness = _normalize_optional_filter(self.filters.priced_in_usefulness)
        ready = self.query_one("#action-scan-ready", FocusRow)
        mismatch = self.query_one("#action-scan-mismatches", FocusRow)
        full = self.query_one("#action-scan-all", FocusRow)
        ready_active = status == "actionable" and usefulness == "decision_useful"
        ready.set_class(ready_active, "active")
        mismatch.set_class(status == "actionable" and not ready_active, "active")
        full.set_class(status == "all", "active")
        ready.update((">> " if ready_active else "   ") + "D  Decision-ready")
        mismatch.update(
            (">> " if status == "actionable" and not ready_active else "   ")
            + "M  Mismatches only"
        )
        full.update((">> " if status == "all" else "   ") + "ALL Full scan rows")

    def _nav_label(self, page_key: str, shortcut: str, label: str) -> str:
        active = self._active_nav_page() == page_key
        marker = ">>" if active else "  "
        counts = self._nav_count_suffix(page_key)
        return f"{marker} {shortcut:<2} {label}{counts}"

    def _active_nav_page(self) -> str:
        active = self.page.split(":", 1)[0]
        if active == "candidate":
            return "candidates"
        if active == "alert":
            return "alerts"
        return active

    def _nav_count_suffix(self, page_key: str) -> str:
        if not self.payload:
            return ""
        if page_key == "candidates":
            return f" [{_mapping(self.payload.get('candidates')).get('count') or 0}]"
        if page_key == "alerts":
            return f" [{_mapping(self.payload.get('alerts')).get('count') or 0}]"
        if page_key == "ipo":
            return f" [{_mapping(self.payload.get('ipo_s1')).get('count') or 0}]"
        return ""

    def action_next_page(self) -> None:
        self._move_page(1)

    def action_previous_page(self) -> None:
        self._move_page(-1)

    def action_focus_command(self) -> None:
        self.query_one("#command", Input).focus()

    def _move_page(self, delta: int) -> None:
        active = self._active_nav_page()
        page_keys = [page_key for page_key, _, _ in MODERN_PAGES]
        try:
            current = page_keys.index(active)
        except ValueError:
            current = 0
        self.action_go(page_keys[(current + delta) % len(page_keys)])

    def _move_sidebar_focus(self, delta: int) -> None:
        focus_ids = [f"nav-{page_key}" for page_key, _, _ in MODERN_PAGES]
        focus_ids.extend(
            [
                "action-refresh",
                "action-run-page",
                "action-scan-ready",
                "action-scan-mismatches",
                "action-scan-all",
            ]
        )
        focused_id = self.focused.id if self.focused else ""
        if focused_id not in focus_ids:
            focused_id = f"nav-{self._active_nav_page()}"
        index = focus_ids.index(focused_id)
        self.query_one(f"#{focus_ids[(index + delta) % len(focus_ids)]}", FocusRow).focus()

    def _navigation_text(self) -> str:
        return (
            "[bold #58a6ff]KEYS[/] 1 insights | 4 candidates | D ready | "
            "11 review | M full/mismatch | next/prev rows | Ctrl+N/P page\n"
            "[bold #58a6ff]MOUSE[/] click sidebar/table | Tab focus | "
            "Up/Down on sidebar | Enter open | Esc command | q quit\n"
        )

    def _response_text(self) -> str:
        response = self.status_message or "Ready. No command has run in this view."
        return f"[bold #58a6ff]LAST RESPONSE[/]\n{response}"

    def _action_text(self) -> str:
        page = self.page.split(":", 1)[0]
        queue = _mapping(self.payload.get("priced_in_queue"))
        offset = int(_number_or_zero(queue.get("offset")))
        count = int(_number_or_zero(queue.get("count")))
        total = int(_number_or_zero(queue.get("total_count")))
        page_text = (
            f" Visible rows {offset + 1}-{offset + count} of {total}; "
            "type next, prev, offset <row>, or limit <rows>."
            if total and count
            else ""
        )
        page_action = {
            "tutorial": "Follow the numbered rows. Press 1 when you are ready for insights.",
            "overview": (
                "Select an insight row. Press M or click SCAN to switch between "
                f"Mismatches and Full Scan.{page_text}"
            ),
            "review": (
                "Review decision-ready priced-in rows. Press Enter to open the "
                f"candidate and Decision Card context.{page_text}"
            ),
            "run": "Review call budget, then type run execute only if intended.",
            "candidates": "Click or focus a row and press Enter to open a candidate.",
            "alerts": "Click or focus a row and press Enter to open an alert.",
            "agent": "Review the dry-run multi-agent brief; it makes zero provider calls.",
            "broker": "Use action, trigger, eval-triggers, or ticket for local broker artifacts.",
            "help": "Use the help table as the command reference.",
        }.get(
            page,
            "Use the sidebar, page keys, or Ctrl+N/Ctrl+P to move; type a command below.",
        )
        return (
            "[bold #7ee787]NEXT ACTION[/]\n"
            f"{page_action}"
        )

    def _refresh_header(self) -> None:
        readiness = _mapping(self.payload.get("readiness"))
        freshness = _mapping(_mapping(readiness.get("discovery_snapshot")).get("freshness"))
        database = _mapping(_mapping(self.payload.get("ops_health")).get("database"))
        call_plan = _mapping(self.payload.get("call_plan"))
        broker = _mapping(_mapping(self.payload.get("broker")).get("snapshot"))
        runtime = _mapping(self.payload.get("runtime_context"))
        controls = _mapping(self.payload.get("controls"))
        answer = _mapping(self.payload.get("priced_in_answer"))
        audit = _mapping(self.payload.get("priced_in_audit"))
        next_step = _priced_in_operator_step(self.payload) or _mapping(
            self.payload.get("operator_next_step")
        )
        next_action = next_step.get("action") or readiness.get("next_action")
        can_act = _decision_label(readiness)
        audit_status = str(audit.get("status") or "").strip().lower()
        answer_status = _human_label(
            audit_status or str(answer.get("status") or "unknown")
        )
        answer_ready = (
            "ready"
            if bool(answer.get("decision_ready"))
            and audit_status not in {"blocked", "attention"}
            else "not ready"
        )
        view_label = _priced_in_view_label(self.payload)
        active_page = self.page.split(":", 1)[0]
        page_title = (
            "TUTORIAL"
            if active_page == "tutorial"
            else "INSIGHTS"
            if active_page == "overview"
            else self.page.upper()
        )

        if active_page == "tutorial":
            self.query_one("#hero", Static).update(
                "\n".join(
                    [
                        "[bold #7ee787]MARKET RADAR[/] // [b]TUTORIAL[/b]",
                        (
                            "This walkthrough teaches the controls. "
                            "It does not run providers, trade, or change data."
                        ),
                        "[bold #58a6ff]Do next[/] Read the rows below, then press 1 for Insights.",
                    ]
                )
            )
            self.query_one("#metric-readiness", Static).update(
                _metric_text("Step 1", "Learn controls", "mouse, keys, commands")
            )
            self.query_one("#metric-market", Static).update(
                _metric_text("Step 2", "Open Insights", "press 1")
            )
            self.query_one("#metric-calls", Static).update(
                _metric_text("Safety", "0 calls", "tutorial is local")
            )
            self.query_one("#metric-broker", Static).update(
                _metric_text("Orders", "Disabled", "no real trades")
            )
            return

        self.query_one("#hero", Static).update(
            "\n".join(
                [
                    (
                        f"[bold #7ee787]MARKET RADAR[/] // [b]{page_title}[/b]  "
                        f"[dim]view {view_label} | Priced-in answer {answer_status} "
                        f"({answer_ready}) | "
                        f"Trade safe? {can_act} | "
                        f"status {readiness.get('status') or 'unknown'} | "
                        f"{self.payload.get('external_calls_made', 0)} calls while viewing[/dim]"
                    ),
                    (
                        f"[bold]View[/] {view_label}; "
                        f"[bold]Priced-in answer[/] {answer_status}; "
                        f"[bold]Trade safe?[/] {can_act}. "
                        f"{readiness.get('headline') or 'No readiness headline.'} "
                        f"[dim]Build {(_nested(runtime, 'build', 'commit') or 'n/a')} | "
                        f"Ticker {controls.get('ticker') or 'all'}[/dim]"
                    ),
                    f"[bold #58a6ff]Do next[/] {_clip(next_action or 'No operator action.', 118)}",
                ]
            )
        )
        self.query_one("#metric-readiness", Static).update(
            _metric_text(
                "Price answer",
                answer_status,
                f"{answer_ready}; trade {can_act}",
            )
        )
        self.query_one("#metric-market", Static).update(
            _metric_text(
                "Fresh bars",
                database.get("latest_daily_bar_date") or freshness.get("latest_daily_bar_date"),
                (
                    f"latest "
                    f"{database.get('active_security_with_latest_daily_bar_count')}/"
                    f"{database.get('active_security_count')}; run "
                    f"{freshness.get('active_security_with_as_of_bar_count')}/"
                    f"{freshness.get('active_security_count')}"
                ),
            )
        )
        self.query_one("#metric-calls", Static).update(
            _metric_text(
                "Run calls",
                f"{call_plan.get('max_external_call_count')} max",
                "0 while browsing",
            )
        )
        self.query_one("#metric-broker", Static).update(
            _metric_text(
                "Orders",
                "Disabled",
                f"broker {broker.get('connection_status') or 'n/a'}",
            )
        )

    def _guide_text(self) -> str:
        page = self.page.split(":", 1)[0]
        readiness = _mapping(self.payload.get("readiness"))
        candidates = _mapping(self.payload.get("candidates"))
        alerts = _mapping(self.payload.get("alerts"))
        call_plan = _mapping(self.payload.get("call_plan"))
        next_step = _priced_in_operator_step(self.payload) or _mapping(
            self.payload.get("operator_next_step")
        )
        can_act = _decision_label(readiness)
        next_action = next_step.get("action") or readiness.get("next_action") or "Open Readiness."
        usefulness = _mapping(readiness.get("market_radar_usefulness"))
        blocked_layers = usefulness.get("blocked_layers")
        if page == "tutorial":
            return "\n".join(
                [
                    "[bold #7ee787]TUTORIAL[/]  Do these in order. Nothing external runs here.",
                    "[bold]1.[/] Press 1 or click Insights to see what needs attention.",
                    "[bold]2.[/] Press D for decision-ready rows, M for broader mismatches.",
                    (
                        "[bold]3.[/] Press 2 for blockers, 4 for candidates, "
                        "or 3 to review the run plan; "
                        "type run execute only by intent."
                    ),
                ]
            )
        if page == "overview":
            discovery = _mapping(self.payload.get("discovery_snapshot"))
            scan_yield = _mapping(discovery.get("yield"))
            queue = _mapping(self.payload.get("priced_in_queue"))
            answer = _mapping(self.payload.get("priced_in_answer"))
            full_scan = _mapping(answer.get("full_scan"))
            scan_scope = _mapping(answer.get("scan_scope"))
            status_filter = _priced_in_status_filter(queue)
            mode = "Full Scan" if status_filter == "all" else "Mismatches"
            offset = int(_number_or_zero(queue.get("offset")))
            count = int(_number_or_zero(queue.get("count")))
            total = int(_number_or_zero(queue.get("total_count")))
            if status_filter == "all":
                mode_help = (
                    "showing review page 1 from the ranked scan"
                    if offset == 0
                    else "showing a later review page from the ranked scan"
                )
            else:
                mode_help = (
                    "showing only bullish/bearish not-priced-in rows"
                    if offset == 0
                    else "showing a later page of bullish/bearish not-priced-in rows"
                )
            scope_text = str(
                scan_scope.get("explanation")
                or f"showing rows {offset + 1}-{offset + count} of {total}."
            )
            scanned_rows = (
                full_scan.get("scanned_rows")
                or scan_yield.get("scanned_securities")
                or "n/a"
            )
            active_rows = (
                full_scan.get("active_securities")
                or scan_yield.get("requested_securities")
                or "n/a"
            )
            return "\n".join(
                [
                    (
                        f"[bold #7ee787]{mode.upper()}[/]  Market emotion versus price "
                        f"reaction; {mode_help}."
                    ),
                    (
                        f"[bold]Coverage:[/] scanned "
                        f"{scanned_rows} row(s) from {active_rows} "
                        f"active securities; ranked {total}; visible page {count}."
                    ),
                    (
                        f"[bold]Scope:[/] {scope_text}"
                    ),
                    (
                        f"[bold]Decision blocker:[/] "
                        f"{_decision_readiness_summary(self.payload)}"
                    ),
                    (
                        f"[bold]Priced-in answer:[/] "
                        f"{answer.get('answer') or 'Open Insights for current answer.'}"
                    ),
                    (
                        f"[bold]Full-scan source workflow:[/] "
                        f"{_overview_source_workflow_hint(self.payload)}"
                    ),
                    (
                        f"[bold]Controls:[/] M toggles view; next/prev pages rows; "
                        f"export full prints all ranked rows; these tickers are only "
                        f"the current page; {can_act}; "
                        f"{blocked_layers or 0} useful layer(s) blocked."
                    ),
                ]
            )
        if page == "review":
            answer = _mapping(self.payload.get("priced_in_answer"))
            queue = _mapping(self.payload.get("priced_in_queue"))
            readiness = _mapping(self.payload.get("readiness"))
            count = int(_number_or_zero(queue.get("count")))
            total = int(_number_or_zero(queue.get("total_count")))
            return "\n".join(
                [
                    (
                        "[bold #7ee787]DECISION REVIEW[/]  "
                        "These rows passed the priced-in usefulness gate."
                    ),
                    (
                        f"[bold]Answer:[/] {answer.get('answer') or 'No priced-in answer.'} "
                        f"[bold]Visible:[/] {count}/{total} decision-ready row(s)."
                    ),
                    (
                        f"[bold]Boundary:[/] "
                        f"{answer.get('investment_boundary') or 'Not trade approval.'} "
                        f"Trade safe? {_decision_label(readiness)}."
                    ),
                    (
                        f"[bold]Remaining context:[/] "
                        f"{_decision_review_optional_summary(_priced_in_review_rows(self.payload))}"
                    ),
                    (
                        "[bold]Do next:[/] press Enter/click a ticker to inspect evidence; "
                        "use Broker only for local watch/trigger/ticket artifacts."
                    ),
                ]
            )
        if page == "readiness":
            return "\n".join(
                [
                    "[bold #7ee787]USE THIS PAGE[/] Clear blockers before trusting output.",
                    f"[bold]Current answer:[/] {can_act}.",
                    f"[bold]Look for:[/] blocked or stale rows. [bold]Do next:[/] {next_action}",
                ]
            )
        if page == "run":
            return "\n".join(
                [
                    "[bold #7ee787]USE THIS PAGE[/] A run may call external providers.",
                    (
                        f"[bold]Budget:[/] max {call_plan.get('max_external_call_count')} calls. "
                        f"[bold]Status:[/] {call_plan.get('status') or 'unknown'}."
                    ),
                    "[bold]Do next:[/] inspect rows first; type run execute only when you mean it.",
                ]
            )
        if page == "candidates":
            return "\n".join(
                [
                    "[bold #7ee787]USE THIS PAGE[/] Review companies, not trade signals.",
                    (
                        f"[bold]Rows:[/] {candidates.get('count') or 0}. "
                        "Click a ticker row or press Enter."
                    ),
                    (
                        "[bold]Do next:[/] open one candidate, then save "
                        "watch/ready/dismiss from Broker if useful."
                    ),
                ]
            )
        if page == "alerts":
            return "\n".join(
                [
                    "[bold #7ee787]USE THIS PAGE[/] Judge whether alert output is useful or noisy.",
                    (
                        f"[bold]Rows:[/] {alerts.get('count') or 0}. "
                        "Click an alert row or press Enter."
                    ),
                    "[bold]Do next:[/] use feedback <#|id> useful|noisy|acted [notes].",
                ]
            )
        if page == "agent":
            brief = _mapping(self.payload.get("agent_brief"))
            calls = _mapping(brief.get("external_calls_made"))
            runtime = _mapping(brief.get("runtime"))
            return "\n".join(
                [
                    "[bold #7ee787]USE THIS PAGE[/] Read the multi-agent operator brief.",
                    (
                        f"[bold]Mode:[/] {brief.get('mode') or 'dry_run'}; "
                        f"[bold]Status:[/] {brief.get('status') or 'unknown'}; "
                        f"[bold]Calls:[/] OpenAI {calls.get('openai', 0)}, "
                        f"market {calls.get('market_data', 0)}, broker {calls.get('broker', 0)}."
                    ),
                    f"[bold]Runtime:[/] {_agent_runtime_label(runtime)}.",
                    "[bold]Do next:[/] follow the first Next Action row, then return to Insights.",
                ]
            )
        if page == "broker":
            return "\n".join(
                [
                    "[bold #7ee787]USE THIS PAGE[/] Save local review artifacts only.",
                    "[bold]Safety:[/] real order submission is disabled.",
                    "[bold]Do next:[/] action <ticker> watch|ready|simulate_entry|dismiss.",
                ]
            )
        if page == "ops":
            return "\n".join(
                [
                    "[bold #7ee787]USE THIS PAGE[/] Fill source gaps for the full scan.",
                    (
                        "[bold]Click/Enter:[/] a source row to inspect its plan. "
                        "This is plan-only and makes 0 provider calls."
                    ),
                    (
                        "[bold]Execute:[/] type batch <source> execute only when "
                        "the provider and call budget are intentional."
                    ),
                ]
            )
        if page == "help":
            return "\n".join(
                [
                    "[bold #7ee787]USE THIS PAGE[/] Command reference.",
                    (
                        "[bold]Click:[/] sidebar rows to move. "
                        "[bold]Type:[/] commands in the bottom box."
                    ),
                    "[bold]Do next:[/] Esc focuses the command box; q quits.",
                ]
            )
        return "\n".join(
            [
                "[bold #7ee787]USE THIS PAGE[/] Inspect this evidence before acting elsewhere.",
                "[bold]Do next:[/] click rows when available, or return to Insights with 1.",
                "[bold]Reminder:[/] navigation and filtering make 0 provider calls.",
            ]
        )

    def _refresh_table(self) -> None:
        title, columns, rows, detail = self._table_model()
        self.query_one("#section-title", Static).update(title)
        table = self.query_one("#data-table", DataTable)
        table.clear(columns=True)
        table.zebra_stripes = True
        table.cursor_type = "row"
        for _, label, width in columns:
            table.add_column(label, width=width)
        for index, row in enumerate(rows, start=1):
            row_key = str(row.get("_row_key") or row.get("ticker") or row.get("id") or index)
            table.add_row(
                *[_clip(row.get(key), width) for key, _, width in columns],
                key=row_key,
            )
        self.query_one("#detail", Static).update(detail)

    def _table_model(
        self,
    ) -> tuple[
        str,
        Sequence[tuple[str, str, int]],
        list[Mapping[str, object]],
        str,
    ]:
        page = self.page
        if page.startswith("candidate:"):
            return self._candidate_detail_model(page.split(":", 1)[1])
        if page.startswith("alert:"):
            return self._alert_detail_model(page.split(":", 1)[1])
        if page == "tutorial":
            return self._tutorial_model()
        if page == "overview":
            return self._overview_model()
        if page == "review":
            return self._review_model()
        if page == "readiness":
            return (
                "Readiness checklist",
                [
                    ("area", "Area", 18),
                    ("status", "Status", 14),
                    ("finding", "Finding", 44),
                    ("next_action", "Next action", 58),
                ],
                _rows(_mapping(self.payload.get("readiness")).get("readiness_checklist")),
                "Use this page to decide what blocks a human investment decision.",
            )
        if page == "run":
            call_plan = _mapping(self.payload.get("call_plan"))
            return (
                "Run call plan",
                [
                    ("layer", "Layer", 18),
                    ("provider", "Provider", 14),
                    ("status", "Status", 16),
                    ("external_call_count_max", "Calls", 8),
                    ("next_action", "Next action", 66),
                ],
                _rows(call_plan.get("rows")),
                f"{call_plan.get('headline') or ''} {call_plan.get('next_action') or ''}",
            )
        if page == "candidates":
            rows = [
                _candidate_table_row(row, row_key=str(row.get("ticker") or index))
                for index, row in enumerate(_candidate_rows(self.payload), start=1)
            ]
            return (
                "Candidates - click a row or press Enter to open",
                [
                    ("ticker", "Ticker", 8),
                    ("priced_in_status", "Priced-in", 20),
                    ("emotion_reaction_gap", "Gap", 8),
                    ("score", "Score", 8),
                    ("data_coverage", "Data", 32),
                    ("why_now", "Why now", 50),
                    ("next_step", "Next step", 36),
                ],
                rows,
                (
                    "Gap is emotion minus price reaction. Positive means the market "
                    "may not have fully priced it."
                ),
            )
        if page == "alerts":
            rows = [
                {**dict(row), "_row_key": str(row.get("id") or index)}
                for index, row in enumerate(
                    _rows(_mapping(self.payload.get("alerts")).get("rows")),
                    start=1,
                )
            ]
            return (
                "Alerts - click a row or press Enter to open",
                [
                    ("id", "ID", 18),
                    ("ticker", "Ticker", 8),
                    ("status", "Status", 12),
                    ("route", "Route", 22),
                    ("priority", "Priority", 10),
                    ("title", "Title", 58),
                ],
                rows,
                "Commands: alert-status <status|all>, feedback <#|id> <label> [notes]",
            )
        if page == "ipo":
            return (
                "IPO / S-1 catalyst evidence",
                [
                    ("ticker", "Ticker", 8),
                    ("proposed_ticker", "Proposed", 10),
                    ("form_type", "Form", 8),
                    ("filing_date", "Filed", 12),
                    ("estimated_gross_proceeds", "Proceeds", 14),
                    ("summary", "Summary", 70),
                ],
                _rows(_mapping(self.payload.get("ipo_s1")).get("rows")),
                "SEC catalyst rows remain source-labeled and safe for research review.",
            )
        if page == "broker":
            broker = _mapping(self.payload.get("broker"))
            return (
                "Broker actions and local order-preview tickets",
                [
                    ("ticker", "Ticker", 8),
                    ("action", "Action", 18),
                    ("status", "Status", 12),
                    ("notes", "Notes", 46),
                    ("created_at", "Created", 24),
                ],
                _rows(broker.get("opportunity_actions")),
                "Read-only Schwab context is allowed; real order submission remains disabled.",
            )
        if page == "ops":
            rows = _source_coverage_workbench_rows(self.payload)
            return (
                "Source coverage workbench - Enter shows plan, not execution",
                [
                    ("priority", "#", 4),
                    ("source", "Source", 18),
                    ("status", "Status", 14),
                    ("gap_rows", "Gaps", 8),
                    ("useful_rows", "Useful rows", 18),
                    ("examples", "Examples", 24),
                    ("plan", "Plan", 22),
                    ("next_action", "Next action", 46),
                ],
                rows,
                _source_coverage_workbench_detail(self.payload, rows),
            )
        if page == "telemetry":
            telemetry = _mapping(self.payload.get("telemetry"))
            return (
                "Telemetry audit tape",
                [
                    ("occurred_at", "Occurred", 24),
                    ("event", "Event", 28),
                    ("status", "Status", 12),
                    ("summary", "Summary", 72),
                ],
                _rows(telemetry.get("events")),
                f"{telemetry.get('headline') or ''} Next: {telemetry.get('next_action') or ''}",
            )
        if page == "agent":
            brief = _mapping(self.payload.get("agent_brief"))
            runtime = _mapping(brief.get("runtime"))
            return (
                "Agent brief - dry run, zero hidden provider calls",
                [
                    ("kind", "Kind", 12),
                    ("item", "Item", 28),
                    ("detail", "Detail", 98),
                ],
                _agent_brief_rows(brief),
                (
                    f"{brief.get('decision_boundary') or 'Manual research boundary.'} "
                    f"{_agent_runtime_label(runtime)}."
                ),
            )
        if page == "features":
            return (
                "Feature inventory",
                [
                    ("area", "Area", 18),
                    ("feature", "Feature", 46),
                    ("page", "Page", 20),
                    ("use", "Operational use", 58),
                ],
                _rows(self.payload.get("feature_inventory")),
                "This is the current terminal replacement surface inventory.",
            )
        return self._help_model()

    def _current_rows(self) -> list[Mapping[str, object]]:
        _, _, rows, _ = self._table_model()
        return rows

    def _tutorial_model(
        self,
    ) -> tuple[str, Sequence[tuple[str, str, int]], list[Mapping[str, object]], str]:
        rows = _tutorial_mission_rows(self.payload) + _tutorial_control_rows()
        return (
            "Tutorial - your first 90 seconds",
            [("step", "Step", 6), ("do", "Do this", 34), ("result", "What happens", 96)],
            rows,
            _tutorial_caption(self.payload),
        )

    def _overview_model(
        self,
    ) -> tuple[str, Sequence[tuple[str, str, int]], list[Mapping[str, object]], str]:
        return (
            _overview_title(self.payload),
            [
                ("rank", "#", 3),
                ("ticker", "Ticker", 6),
                ("signal", "Signal", 19),
                ("emotion_reaction_gap", "Gap", 5),
                ("data_coverage", "Data gaps", 14),
                ("why_now", "Why now", 27),
                ("next_action", "Next action", 25),
            ],
            _priced_in_overview_rows(self.payload),
            _overview_caption(self.payload),
        )

    def _review_model(
        self,
    ) -> tuple[str, Sequence[tuple[str, str, int]], list[Mapping[str, object]], str]:
        rows = _priced_in_review_rows(self.payload)
        return (
            "Decision Review - priced-in answer, not trade approval",
            [
                ("rank", "#", 3),
                ("ticker", "Ticker", 6),
                ("signal", "Signal", 19),
                ("emotion_reaction_gap", "Gap", 6),
                ("optional_gaps", "Optional gaps", 22),
                ("top_evidence", "Top evidence", 30),
                ("next_action", "Next action", 34),
            ],
            rows,
            _decision_review_caption(self.payload, rows),
        )

    def _candidate_detail_model(
        self,
        ticker: str,
    ) -> tuple[str, Sequence[tuple[str, str, int]], list[Mapping[str, object]], str]:
        ticker = ticker.upper()
        row = _candidate_detail_row(self.payload, ticker)
        rows = _candidate_detail_table_rows(row)
        return (
            f"Candidate {ticker}",
            [("key", "Field", 24), ("value", "Value", 110)],
            rows,
            "Verify the evidence, then decide watch, trigger, ticket, or dismiss.",
        )

    def _alert_detail_model(
        self,
        alert_id: str,
    ) -> tuple[str, Sequence[tuple[str, str, int]], list[Mapping[str, object]], str]:
        rows = _rows(_mapping(self.payload.get("alerts")).get("rows"))
        row = next((item for item in rows if str(item.get("id") or "") == alert_id), {})
        return (
            f"Alert {alert_id}",
            [("key", "Field", 24), ("value", "Value", 110)],
            _mapping_items(_compact_detail(row)),
            "Use feedback <alert-id|#> <label> [notes] to record alert usefulness.",
        )

    def _help_model(
        self,
    ) -> tuple[str, Sequence[tuple[str, str, int]], list[Mapping[str, object]], str]:
        rows = [
            {"command": "Click sidebar row", "meaning": "Switch pages with mouse support."},
            {"command": "Click candidate/alert row", "meaning": "Open the selected detail view."},
            {"command": "0, 1..10, Ctrl+A, f, ?", "meaning": "Keyboard page shortcuts."},
            {
                "command": "tutorial / insights / start",
                "meaning": "Open the walkthrough or the market insight queue.",
            },
            {
                "command": "stocks / ready / full / mismatches",
                "meaning": (
                    "Switch Insights between stock-like rows, decision-ready rows, "
                    "full universe rows, and the broader mismatch queue."
                ),
            },
            {
                "command": "next / prev / offset <row>",
                "meaning": "Page through the full ranked scan without provider calls.",
            },
            {
                "command": "export full",
                "meaning": "Print the full-scan JSON export command.",
            },
            {"command": "limit <1-200>", "meaning": "Change visible scan rows per page."},
            {
                "command": "source-gap <source|all>",
                "meaning": "Show scan rows missing options, text, events, bars, or broker context.",
            },
            {
                "command": "batch <source>",
                "meaning": "Plan full-scan source fill and show the next safe chunk.",
            },
            {
                "command": "batch <source> execute",
                "meaning": "Run only the next guarded chunk; refresh and repeat deliberately.",
            },
            {
                "command": "batch <source> execute 3",
                "meaning": "Run a capped source-fill batch set and stop on blockers.",
            },
            {
                "command": "bars saved capture",
                "meaning": (
                    "Plan saved Polygon/Massive capture; add confirm for one provider call."
                ),
            },
            {
                "command": "bars manual template",
                "meaning": "Generate the full-universe missing-bar CSV.",
            },
            {
                "command": "bars manual import",
                "meaning": "Preview or execute complete-row manual market-bar import.",
            },
            {
                "command": "bars saved validate/import",
                "meaning": "Validate or preview/import the saved grouped-daily file.",
            },
            {
                "command": "options template / validate / import",
                "meaning": "Create, check, or explicitly import point-in-time options evidence.",
            },
            {
                "command": "cik template / validate / import",
                "meaning": "Create, check, or explicitly import local SEC CIK overrides.",
            },
            {"command": "ticker <SYMBOL|all>", "meaning": "Filter ticker-aware pages."},
            {"command": "run execute", "meaning": "Start one guarded capped radar cycle."},
            {
                "command": "action / trigger / ticket",
                "meaning": "Save local broker-context artifacts only.",
            },
            {
                "command": "feedback <alert-id|#> <label>",
                "meaning": "Record useful/noisy/acted alert feedback.",
            },
            {"command": "r or Refresh", "meaning": "Reload local database state."},
            {"command": "q", "meaning": "Quit."},
        ]
        return (
            "Help",
            [("command", "Command", 34), ("meaning", "Meaning", 92)],
            rows,
            (
                "The TUI renders local snapshots only. Navigation and refresh "
                "make zero provider calls."
            ),
        )


def render_dashboard_tui(
    payload: Mapping[str, object],
    *,
    page: str = "overview",
    width: int | None = None,
) -> str:
    resolved_width = _resolve_width(width)
    page = _normalize_page(page)
    lines = _header_lines(payload, page, resolved_width)
    if page == "tutorial":
        lines.extend(_tutorial_lines(payload, resolved_width))
    elif page == "overview":
        lines.extend(_overview_lines(payload, resolved_width))
    elif page == "review":
        lines.extend(_review_lines(payload, resolved_width))
    elif page == "readiness":
        lines.extend(_readiness_lines(payload, resolved_width))
    elif page == "run":
        lines.extend(_run_lines(payload, resolved_width))
    elif page == "candidates":
        lines.extend(_candidates_lines(payload, resolved_width))
    elif page.startswith("candidate:"):
        lines.extend(_candidate_detail_lines(payload, page.split(":", 1)[1], resolved_width))
    elif page == "alerts":
        lines.extend(_alerts_lines(payload, resolved_width))
    elif page.startswith("alert:"):
        lines.extend(_alert_detail_lines(payload, page.split(":", 1)[1], resolved_width))
    elif page == "ipo":
        lines.extend(_ipo_lines(payload, resolved_width))
    elif page == "themes":
        lines.extend(_themes_lines(payload, resolved_width))
    elif page == "validation":
        lines.extend(_validation_lines(payload, resolved_width))
    elif page == "costs":
        lines.extend(_costs_lines(payload, resolved_width))
    elif page == "broker":
        lines.extend(_broker_lines(payload, resolved_width))
    elif page == "ops":
        lines.extend(_ops_lines(payload, resolved_width))
    elif page == "telemetry":
        lines.extend(_telemetry_lines(payload, resolved_width))
    elif page == "agent":
        lines.extend(_agent_lines(payload, resolved_width))
    elif page == "features":
        lines.extend(_feature_lines(payload, resolved_width))
    else:
        lines.extend(_help_lines(resolved_width))
    lines.extend(_footer_lines(resolved_width))
    return "\n".join(lines)


def dashboard_json_default(value: object) -> object:
    if isinstance(value, datetime | date):
        return value.isoformat()
    return str(value)


@dataclass(frozen=True)
class _CommandUpdate:
    page: str
    filters: DashboardFilters
    exit_requested: bool = False
    message: str = ""


def _priced_in_operator_step(payload: Mapping[str, object]):
    answer = _mapping(payload.get("priced_in_answer"))
    step = _mapping(answer.get("operator_next_step"))
    if step:
        return step
    return _mapping(payload.get("priced_in_operator_next_step"))


def _operator_next_step_summary(step: Mapping[str, object]):
    if not step:
        return ""
    parts = [
        str(step.get("action") or step.get("action_label") or "No action recorded.")
    ]
    command = step.get("tui_command") or step.get("command")
    if command:
        parts.append(f"run {command}")
    calls = int(_number_or_zero(step.get("external_calls_required")))
    changes = int(_number_or_zero(step.get("db_" + "writes_required")))
    approval = " after approval" if bool(step.get("approval_required")) else ""
    parts.append(f"{calls} provider call(s){approval}")
    parts.append(f"{changes} database change(s)")
    blocker = step.get("first_blocker")
    gap = int(_number_or_zero(step.get("first_gap_count")))
    if blocker:
        parts.append(f"blocker {blocker}; gap {gap}")
    return "; ".join(parts)


def _operator_next_step_message(payload: Mapping[str, object]):
    step = _priced_in_operator_step(payload)
    if not step:
        return "No priced-in operator step is available. Refresh the dashboard snapshot."
    summary = _operator_next_step_summary(step)
    response = str(step.get("response_after_action") or "").strip()
    boundary = str(step.get("investment_decision_boundary") or "").strip()
    lines = [f"Next priced-in action: {summary}"]
    if response:
        lines.append(f"Expected response: {response}")
    if boundary:
        lines.append(f"Boundary: {boundary}")
    lines.append(
        f"Viewing made {int(_number_or_zero(step.get('external_calls_made')))} "
        f"provider calls and {int(_number_or_zero(step.get('db_' + 'writes_made')))} "
        "database changes."
    )
    return " ".join(lines)


def _apply_command(
    raw: str,
    payload: Mapping[str, object],
    page: str,
    filters: DashboardFilters,
    *,
    engine: Engine,
    config: AppConfig,
) -> _CommandUpdate:
    if not raw:
        return _CommandUpdate(page=page, filters=filters, message="Refreshed.")
    command, _, rest = raw.partition(" ")
    command = command.strip().lower()
    value = rest.strip()
    if command in {"q", "quit", "exit"}:
        return _CommandUpdate(page=page, filters=filters, exit_requested=True)
    if command in {"r", "refresh"}:
        return _CommandUpdate(page=page, filters=filters, message="Refreshed.")
    if command in {"now", "what-now", "whatnow", "todo", "do"}:
        return _CommandUpdate(
            page="overview",
            filters=filters,
            message=_operator_next_step_message(payload),
        )
    if command in {"all", "full", "full-scan"}:
        return _CommandUpdate(
            page="overview",
            filters=replace(
                filters,
                priced_in_status="all",
                priced_in_usefulness=None,
                priced_in_stocks_only=False,
                priced_in_offset=0,
            ).normalized(),
            message=(
                "Full Scan mode: showing review page 1; coverage line shows the scan universe."
            ),
        )
    if command in {"stock", "stocks", "stocks-only", "stocks_only"}:
        return _CommandUpdate(
            page="overview",
            filters=replace(
                filters,
                priced_in_status="all",
                priced_in_usefulness=None,
                priced_in_stocks_only=True,
                priced_in_offset=0,
            ).normalized(),
            message=(
                "Stocks-only mode: showing common-stock and ADR rows from the "
                "local ranked scan. Type full to return to all instruments."
            ),
        )
    if command in {"d", "ready", "decision", "decision-ready", "decision_ready"}:
        return _CommandUpdate(
            page="review",
            filters=replace(
                filters,
                priced_in_status="actionable",
                priced_in_usefulness="decision_useful",
                priced_in_offset=0,
            ).normalized(),
            message=(
                "Decision-ready view: showing not-priced-in rows that passed the "
                "usefulness gate. Type full for the whole ranked universe."
            ),
        )
    if command in {"m", "mismatch", "mismatches", "actionable"}:
        return _CommandUpdate(
            page="overview",
            filters=replace(
                filters,
                priced_in_status="actionable",
                priced_in_usefulness=None,
                priced_in_offset=0,
            ).normalized(),
            message="Mismatches mode: showing only bullish/bearish not-priced-in rows.",
        )
    if command == "scan":
        scan_status = _normalize_priced_in_status(value)
        return _CommandUpdate(
            page="overview",
            filters=replace(
                filters,
                priced_in_status=scan_status,
                priced_in_usefulness=None,
                priced_in_offset=0,
            ).normalized(),
            message=(
                "Full Scan mode: showing review page 1; coverage line shows the scan universe."
                if scan_status == "all"
                else f"Scan filter updated: {scan_status}."
            ),
        )
    if command in {"next", "more"}:
        queue = _mapping(payload.get("priced_in_queue"))
        count = int(_number_or_zero(queue.get("count")))
        limit = int(_number_or_zero(_mapping(queue.get("filters")).get("limit"))) or count
        total = int(_number_or_zero(queue.get("total_count")))
        offset = int(_number_or_zero(queue.get("offset")))
        next_offset = offset + max(1, limit)
        if total and next_offset >= total:
            return _CommandUpdate(
                page="overview",
                filters=filters,
                message="Already at the end of the current scan filter.",
            )
        return _CommandUpdate(
            page="overview",
            filters=replace(filters, priced_in_offset=next_offset).normalized(),
            message=f"Showing full-scan rows starting at {next_offset + 1}.",
        )
    if command in {"prev", "previous", "back"}:
        limit = max(1, filters.priced_in_limit)
        offset = max(0, filters.priced_in_offset - limit)
        return _CommandUpdate(
            page="overview",
            filters=replace(filters, priced_in_offset=offset).normalized(),
            message=f"Showing full-scan rows starting at {offset + 1}.",
        )
    if command == "export":
        answer = _mapping(payload.get("priced_in_answer"))
        scan_scope = _mapping(answer.get("scan_scope"))
        if value.lower() in {"", "full", "full-scan", "scan", "all"}:
            export_command = str(
                scan_scope.get("full_scan_export_command")
                or "catalyst-radar priced-in-queue --full-scan --all --json"
            )
            return _CommandUpdate(
                page="overview",
                filters=filters,
                message=f"Full-scan export command: {export_command}",
            )
        if value.lower() in {"current", "filter", "filtered"}:
            export_command = str(
                scan_scope.get("current_filter_export_command")
                or "catalyst-radar priced-in-queue --all --json"
            )
            return _CommandUpdate(
                page="overview",
                filters=filters,
                message=f"Current-filter export command: {export_command}",
            )
        return _CommandUpdate(
            page=page,
            filters=filters,
            message="Usage: export full or export current.",
        )
    if command == "offset":
        if not value.isdigit():
            return _CommandUpdate(page=page, filters=filters, message="Usage: offset <row>")
        offset = max(0, int(value) - 1)
        return _CommandUpdate(
            page="overview",
            filters=replace(filters, priced_in_offset=offset).normalized(),
            message=f"Showing full-scan rows starting at {offset + 1}.",
        )
    if command == "limit":
        if not value.isdigit():
            return _CommandUpdate(page=page, filters=filters, message="Usage: limit <1-200>")
        limit = min(200, max(1, int(value)))
        return _CommandUpdate(
            page="overview",
            filters=replace(
                filters,
                priced_in_limit=limit,
                priced_in_offset=0,
            ).normalized(),
            message=f"Showing {limit} full-scan row(s) per page.",
        )
    if command in {"decision-gap", "decision_gaps", "gap"}:
        decision_gaps = _normalize_decision_gap_filter(value)
        return _CommandUpdate(
            page="overview",
            filters=replace(
                filters,
                priced_in_decision_gap=decision_gaps,
                priced_in_offset=0,
            ).normalized(),
            message=(
                "Decision-gap filter cleared."
                if not decision_gaps
                else f"Decision-gap filter: {', '.join(decision_gaps)}."
            ),
        )
    if command in {"source-gap", "source_gaps", "data-gap", "data_gaps"}:
        source_gaps = _normalize_source_gap_filter(value)
        return _CommandUpdate(
            page="overview",
            filters=replace(
                filters,
                priced_in_source_gap=source_gaps,
                priced_in_offset=0,
            ).normalized(),
            message=(
                "Source-gap filter cleared."
                if not source_gaps
                else f"Source-gap filter: {', '.join(source_gaps)}."
            ),
        )
    if command in {"bars", "bar", "market-bars", "market_bars"}:
        return _CommandUpdate(
            page="run",
            filters=filters,
            message=_execute_market_bar_command(
                engine,
                config,
                payload,
                value,
                filters=filters,
            ),
        )
    if command in {"options", "option", "options-flow", "options_flow"}:
        return _CommandUpdate(
            page="run",
            filters=filters,
            message=_execute_options_fixture_command(
                engine,
                config,
                value,
                filters=filters,
            ),
        )
    if command in {"cik", "ciks", "sec-cik", "sec_cik"} or (
        command == "sec"
        and (value.split(maxsplit=1) or [""])[0].lower() in {"cik", "ciks"}
    ):
        sec_value = value
        if command == "sec":
            _head, _sep, sec_value = value.partition(" ")
        return _CommandUpdate(
            page="ops",
            filters=filters,
            message=_execute_sec_cik_command(
                engine,
                config,
                sec_value,
                filters=filters,
            ),
        )
    if command in {"batch", "batches", "source-batch", "source-batches"}:
        source, execute_batch, all_batches, max_batches = _parse_source_batch_command(
            value
        )
        return _CommandUpdate(
            page="ops",
            filters=filters,
            message=(
                _execute_priced_in_source_batch(
                    engine,
                    config,
                    source=source,
                    filters=filters,
                    max_batches=max_batches,
                )
                if execute_batch
                else _priced_in_source_batch_message(
                    engine,
                    config,
                    source=source,
                    filters=filters,
                    all_batches=all_batches,
                )
            ),
        )
    if command in {"usefulness", "useful"}:
        usefulness = _normalize_optional_filter(value)
        return _CommandUpdate(
            page="overview",
            filters=replace(
                filters,
                priced_in_usefulness=usefulness,
                priced_in_offset=0,
            ).normalized(),
            message=(
                "Usefulness filter cleared."
                if usefulness is None
                else f"Usefulness filter: {usefulness}."
            ),
        )
    if command in {"j", "json"}:
        return _CommandUpdate(
            page=page,
            filters=filters,
            message=json.dumps(payload, default=dashboard_json_default, sort_keys=True),
        )
    if command == "run":
        if value.lower() != "execute":
            return _CommandUpdate(
                page="run",
                filters=filters,
                message=(
                    "Run is guarded. Review the call plan, then type "
                    "`run execute` to start one capped radar cycle."
                ),
            )
        return _CommandUpdate(
            page="run",
            filters=filters,
            message=_execute_guarded_radar_run(engine, config, payload),
        )
    if command == "action":
        return _CommandUpdate(
            page="broker",
            filters=filters,
            message=_save_opportunity_action(engine, value),
        )
    if command == "trigger":
        return _CommandUpdate(
            page="broker",
            filters=filters,
            message=_save_market_trigger(engine, value),
        )
    if command in {"eval-triggers", "evaluate-triggers"}:
        return _CommandUpdate(
            page="broker",
            filters=filters,
            message=_evaluate_market_triggers(engine, value),
        )
    if command == "ticket":
        return _CommandUpdate(
            page="broker",
            filters=filters,
            message=_save_blocked_order_ticket(engine, config, value),
        )
    if command == "feedback":
        return _CommandUpdate(
            page="alerts",
            filters=filters,
            message=_record_alert_feedback(engine, payload, value),
        )
    if command in {"clear", "clear-filters", "reset"}:
        return _CommandUpdate(
            page=page,
            filters=DashboardFilters(
                telemetry_limit=filters.telemetry_limit,
                priced_in_limit=filters.priced_in_limit,
            ),
            message="Filters cleared.",
        )
    if command in {"ticker", "tkr"}:
        ticker = value.upper()
        next_filters = replace(
            filters,
            ticker=None if ticker in {"", "ALL", "NONE"} else ticker,
            priced_in_offset=0,
        )
        return _CommandUpdate(page=page, filters=next_filters, message="Ticker filter updated.")
    if command in {"available-at", "cutoff"}:
        if value.lower() in {"", "latest", "all", "none"}:
            return _CommandUpdate(
                page=page,
                filters=replace(filters, available_at=None, priced_in_offset=0),
                message="Available-at filter cleared.",
            )
        parsed = _datetime_or_none(value)
        if parsed is None:
            return _CommandUpdate(page=page, filters=filters, message="Invalid timestamp.")
        return _CommandUpdate(
            page=page,
            filters=replace(filters, available_at=parsed, priced_in_offset=0),
            message="Available-at filter updated.",
        )
    if command == "alert-status":
        next_value = None if value.lower() in {"", "all", "none"} else value
        return _CommandUpdate(
            page=page,
            filters=replace(filters, alert_status=next_value),
            message="Alert status filter updated.",
        )
    if command == "alert-route":
        next_value = None if value.lower() in {"", "all", "none"} else value
        return _CommandUpdate(
            page=page,
            filters=replace(filters, alert_route=next_value),
            message="Alert route filter updated.",
        )
    if command == "open":
        next_page = _open_target_page(payload, page, value)
        if next_page is None:
            return _CommandUpdate(page=page, filters=filters, message="Nothing to open.")
        return _CommandUpdate(page=next_page, filters=filters)
    next_page = _normalize_page(raw)
    if next_page != "help" or raw.lower() in PAGE_ALIASES:
        return _CommandUpdate(
            page=next_page,
            filters=dashboard_filters_for_page(filters, next_page),
        )
    return _CommandUpdate(
        page=page,
        filters=filters,
        message=f"Unknown command: {raw}. Type help for commands.",
    )


def _priced_in_source_batch_message(
    engine: Engine,
    config: AppConfig,
    *,
    source: str,
    filters: DashboardFilters,
    all_batches: bool = False,
) -> str:
    if not source.strip():
        return (
            "Usage: batch <source>. Try: batch catalyst_events, batch local_text, "
            "batch options. Add all to summarize the full chunk plan, execute "
            "to run one guarded chunk, or execute 3 for a capped run."
        )
    if source.strip().lower() in {"all", "*"}:
        return _priced_in_all_source_batch_message(
            engine,
            config,
            filters=filters,
        )
    payload_or_error = _first_priced_in_source_batch_payload(
        engine,
        config,
        source=source,
        filters=filters,
        all_batches=all_batches,
    )
    if isinstance(payload_or_error, str):
        return payload_or_error
    payload = payload_or_error
    source_name = str(payload.get("source") or source).strip()
    status = str(payload.get("status") or "unknown")
    total_gap_rows = int(_number_or_zero(payload.get("total_gap_rows")))
    plannable_gap_rows = int(_number_or_zero(payload.get("plannable_gap_rows")))
    routed_gap_rows = int(_number_or_zero(payload.get("routed_gap_rows")))
    batch_count = int(_number_or_zero(payload.get("batch_count")))
    next_action = str(payload.get("next_action") or "").strip()
    diagnostic = _mapping(payload.get("diagnostic"))
    reason = str(diagnostic.get("reason") or "").strip()
    diagnostic_next = str(diagnostic.get("next_action") or "").strip()
    diagnostic_command = str(diagnostic.get("fix_command") or "").strip()
    point_in_time_template = str(
        diagnostic.get("point_in_time_template_command") or ""
    ).strip()
    point_in_time_validate = str(
        diagnostic.get("point_in_time_validate_command") or ""
    ).strip()
    point_in_time_import = str(
        diagnostic.get("point_in_time_import_command") or ""
    ).strip()
    point_in_time_progress = _mapping(
        diagnostic.get("point_in_time_fixture_progress")
    )
    point_in_time_progress_suffix = _point_in_time_options_progress_suffix(
        point_in_time_progress
    )
    saved_file_suffix = _source_batch_provider_saved_file_suffix(diagnostic)
    repair_context_suffix = _source_batch_repair_context_suffix(diagnostic)
    manual_validate_command = str(
        diagnostic.get("manual_validate_command") or ""
    ).strip()
    manual_fix_command = str(diagnostic.get("manual_fix_command") or "").strip()
    manual_validate_label, manual_fix_label = _source_batch_manual_command_labels(
        source_name
    )
    blocked_samples = _texts(diagnostic.get("sample_blocked_tickers"))
    missing_cik_suffix = _missing_cik_diagnostic_suffix(diagnostic)
    blocker_suffix = _source_batch_diagnostic_summary(diagnostic)
    non_company_route_suffix = _non_company_route_suffix(diagnostic)
    next_batch_command = str(payload.get("next_batch_command") or "").strip()
    scan_scope = _mapping(payload.get("scan_scope"))
    current_gate = _mapping(payload.get("current_blocker_gate"))
    current_gate_suffix = _source_batch_current_gate_suffix(current_gate)
    execute_next_command = str(payload.get("execute_next_command") or "").strip()
    command = ""
    all_batches_command = str(payload.get("all_batches_command") or "").strip()
    review_rows_command = str(payload.get("review_rows_command") or "").strip()
    export_rows_command = str(payload.get("export_rows_command") or "").strip()
    calls = ""
    batches = _rows(payload.get("batches"))
    if batches:
        command = str(batches[0].get("command") or "").strip()
        api = str(batches[0].get("api") or "").strip()
        api_suffix = f" API: {api}." if api else ""
        call_count = int(_number_or_zero(batches[0].get("external_calls_required")))
        breakdown = _mapping(batches[0].get("external_call_breakdown"))
        if breakdown:
            pieces = [
                f"{key}={int(_number_or_zero(value))}"
                for key, value in sorted(breakdown.items())
                if int(_number_or_zero(value)) > 0
            ]
            calls = f" Calls: {call_count} ({', '.join(pieces)})."
        else:
            calls = f" Calls: {call_count}."
    else:
        api_suffix = ""
    prefix = (
        f"{source_name}: {status}; {total_gap_rows} full-scan gap row(s), "
        f"{plannable_gap_rows} plannable, {routed_gap_rows} routed, "
        f"{batch_count} batch(es)."
    )
    if command:
        returned_tickers = int(_number_or_zero(scan_scope.get("returned_tickers")))
        batch_start = scan_scope.get("returned_batch_start")
        batch_end = scan_scope.get("returned_batch_end")
        ticker_scope_note = (
            "these are not the whole ticker list"
            if bool(scan_scope.get("tickers_are_batch_sample"))
            else "this includes every currently plannable ticker for this source"
        )
        mode_note = (
            " Full chunk plan requested; the TUI summarizes it instead of printing "
            "every ticker."
            if all_batches
            else " Add `all` to summarize every chunk for this source."
        )
        chunk_scope = (
            f" Showing batch {batch_start}-{batch_end} of {batch_count} "
            f"({returned_tickers} ticker(s)); {ticker_scope_note}.{mode_note}"
            if batch_start and batch_end and batch_count
            else (
                " Showing the next provider chunk; this is not the whole ticker list. "
                "Add `all` to summarize every chunk for this source."
            )
        )
        full_suffix = (
            f" Full chunk list: {all_batches_command}."
            if all_batches_command
            else ""
        )
        row_review_suffix = (
            f" Review every matching full-scan row: {review_rows_command}."
            if review_rows_command
            else ""
        )
        row_export_suffix = (
            f" Export every matching full-scan row: {export_rows_command}."
            if export_rows_command
            else ""
        )
        next_suffix = f" Next chunk page: {next_batch_command}." if next_batch_command else ""
        blocked_suffix = (
            f" Blocked examples: {', '.join(blocked_samples)}."
            if blocked_samples
            else ""
        )
        diagnostic_suffix = (
            f" Diagnostic next: {diagnostic_next}."
            if diagnostic_next
            else ""
        )
        command_suffix = (
            f" Diagnostic command: {diagnostic_command}."
            if diagnostic_command
            else ""
        )
        point_in_time_template_suffix = (
            f" Template: {point_in_time_template}."
            if point_in_time_template
            else ""
        )
        point_in_time_validate_suffix = (
            f" Validate: {point_in_time_validate}."
            if point_in_time_validate
            else ""
        )
        point_in_time_suffix = (
            f" Point-in-time import: {point_in_time_import}."
            if point_in_time_import
            else ""
        )
        manual_fix_suffix = (
            f" {manual_fix_label}: {manual_fix_command}."
            if manual_fix_command
            else ""
        )
        manual_validate_suffix = (
            f" {manual_validate_label}: {manual_validate_command}."
            if manual_validate_command
            else ""
        )
        return (
            f"first provider chunk only. {prefix} This is a full-scan plan, "
            f"not a watchlist.{current_gate_suffix}{chunk_scope}"
            f"{calls}{api_suffix} "
            f"Command: {command}. "
            f"{_source_batch_execution_hint(source_name, execute_next_command)}"
            f"{blocked_suffix}"
            f"{blocker_suffix}{missing_cik_suffix}{non_company_route_suffix}"
            f"{diagnostic_suffix}{command_suffix}"
            f"{point_in_time_template_suffix}"
            f"{point_in_time_validate_suffix}"
            f"{point_in_time_suffix}"
            f"{point_in_time_progress_suffix}"
            f"{saved_file_suffix}"
            f"{repair_context_suffix}"
            f"{manual_validate_suffix}"
            f"{manual_fix_suffix}"
            f"{full_suffix}{row_review_suffix}{row_export_suffix}{next_suffix}"
        )
    blocked_suffix = (
        f" Blocked examples: {', '.join(blocked_samples)}."
        if blocked_samples
        else ""
    )
    diagnostic_suffix = (
        f" {diagnostic_next}" if diagnostic_next and diagnostic_next != next_action else ""
    )
    detail = (
        next_action
        or diagnostic_next
        or reason
        or "No runnable batch is available for this source."
    )
    if diagnostic_command:
        detail = f"{detail} Command: {diagnostic_command}."
    if point_in_time_template:
        detail = f"{detail} Template: {point_in_time_template}."
    if point_in_time_validate:
        detail = f"{detail} Validate: {point_in_time_validate}."
    if point_in_time_import:
        detail = f"{detail} Point-in-time import: {point_in_time_import}."
    if point_in_time_progress_suffix:
        detail = f"{detail}{point_in_time_progress_suffix}"
    if saved_file_suffix:
        detail = f"{detail}{saved_file_suffix}"
    if repair_context_suffix:
        detail = f"{detail}{repair_context_suffix}"
    if manual_validate_command:
        detail = f"{detail} {manual_validate_label}: {manual_validate_command}."
    if manual_fix_command:
        detail = f"{detail} {manual_fix_label}: {manual_fix_command}."
    detail = (
        f"{detail}{blocked_suffix}{blocker_suffix}{missing_cik_suffix}"
        f"{non_company_route_suffix}{diagnostic_suffix}"
    )
    return f"{prefix} {current_gate_suffix} {detail}".strip()


def _source_batch_execution_hint(source_name, execute_next_command):
    if execute_next_command:
        return f"Run from TUI with `batch {source_name} execute` if intended."
    return "Execution is blocked until the current blocker clears."


def _source_batch_current_gate_suffix(gate):
    if not gate:
        return ""
    if str(gate.get("status") or "").strip() != "blocked":
        return ""
    blocked_by = str(gate.get("blocked_by") or "source gate").strip()
    gaps = int(_number_or_zero(gate.get("blocked_gap_rows")))
    reason = str(gate.get("reason") or "").strip()
    command = str(gate.get("command") or "").strip()
    boundary = str(gate.get("prework_boundary") or "").strip()
    text = (
        f" Current blocker: {blocked_by} still has {gaps} gap row(s); "
        "this source is review-only and not decision-useful yet."
    )
    if reason:
        text += f" Gate: {reason}"
    if command:
        text += f" Clear first: {command}."
    if boundary:
        text += f" Boundary: {boundary}"
    return text

def _source_batch_manual_command_labels(source: str) -> tuple[str, str]:
    normalized = source.strip().lower()
    if normalized == "market_bars":
        return ("Manual bar check", "Manual bar import")
    if normalized == "catalyst_events":
        return ("CIK validate", "CIK import")
    return ("Manual validate", "Manual import")


def _source_batch_repair_context_suffix(diagnostic: Mapping[str, object]) -> str:
    pieces: list[str] = []
    local_history = _mapping(diagnostic.get("local_bar_history"))
    if local_history:
        with_history = int(_number_or_zero(local_history.get("missing_with_history")))
        without_history = int(
            _number_or_zero(local_history.get("missing_without_history"))
        )
        pieces.append(
            f" Local history: {with_history} with local bars; "
            f"{without_history} without."
        )
    missing_universe = _mapping(diagnostic.get("missing_universe"))
    if missing_universe:
        active = int(_number_or_zero(missing_universe.get("active_metadata_rows")))
        acquisition = int(
            _number_or_zero(missing_universe.get("acquisition_or_spac_name_count"))
        )
        no_figi = int(_number_or_zero(missing_universe.get("no_composite_figi_count")))
        zero_volume = int(
            _number_or_zero(missing_universe.get("zero_avg_dollar_volume_20d_count"))
        )
        zero_market_cap = int(
            _number_or_zero(missing_universe.get("zero_market_cap_count"))
        )
        note = str(missing_universe.get("operator_note") or "").strip()
        note_text = f" {note}" if note else ""
        pieces.append(
            " Universe context: "
            f"active metadata {active}; {acquisition} acquisition/SPAC-style; "
            f"{no_figi} without composite FIGI; "
            f"{zero_volume} zero 20d dollar volume; "
            f"{zero_market_cap} zero market cap.{note_text}"
        )
    return "".join(pieces)


def _source_batch_provider_saved_file_suffix(
    diagnostic: Mapping[str, object],
) -> str:
    status = str(diagnostic.get("provider_saved_file_status") or "").strip()
    path = str(diagnostic.get("provider_saved_file_path") or "").strip()
    if not status and not path:
        return ""
    exists = str(bool(diagnostic.get("provider_saved_file_exists"))).lower()
    next_action = str(
        diagnostic.get("provider_saved_file_next_action") or ""
    ).strip()
    capture = str(
        diagnostic.get("provider_saved_file_capture_command") or ""
    ).strip()
    validate = str(
        diagnostic.get("provider_saved_file_validate_command") or ""
    ).strip()
    import_command = str(
        diagnostic.get("provider_saved_file_import_command") or ""
    ).strip()
    capture_calls = int(
        _number_or_zero(diagnostic.get("provider_saved_file_capture_external_call_count"))
    )
    saved_file_calls = int(
        _number_or_zero(diagnostic.get("provider_saved_file_external_call_count"))
    )
    boundary = str(diagnostic.get("provider_saved_file_boundary") or "").strip()
    pieces = [
        f" Saved file: {status or 'unknown'}; exists={exists}; path {path}."
    ]
    if next_action:
        pieces.append(f" Saved file next: {next_action}")
    if capture:
        pieces.append(
            f" Saved file capture: {capture_calls} external call(s); "
            f"command {capture}."
        )
    if validate:
        pieces.append(
            f" Saved file check: {saved_file_calls} external call(s); "
            f"command {validate}."
        )
    if import_command:
        pieces.append(
            f" Saved file import: {saved_file_calls} external call(s); "
            f"command {import_command}."
        )
    if boundary:
        pieces.append(f" Saved file boundary: {boundary}")
    return "".join(pieces)


def _point_in_time_options_progress_suffix(progress: Mapping[str, object]) -> str:
    if not progress:
        return ""
    status = str(progress.get("status") or "unknown").strip()
    path = str(progress.get("path") or "").strip()
    exists = bool(progress.get("exists"))
    rows = int(_number_or_zero(progress.get("row_count")))
    complete = int(_number_or_zero(progress.get("complete")))
    partial = int(_number_or_zero(progress.get("partial")))
    empty = int(_number_or_zero(progress.get("empty")))
    if not exists:
        return f" Local template: {status}; create {path}." if path else ""
    return (
        f" Local template: {status}; {complete} complete, {partial} partial, "
        f"{empty} empty of {rows} row(s) at {path}."
    )


def _source_batch_diagnostic_summary(diagnostic: Mapping[str, object]) -> str:
    blocked_rows = int(_number_or_zero(diagnostic.get("blocked_rows")))
    eligible_rows = int(_number_or_zero(diagnostic.get("eligible_rows")))
    reason = str(diagnostic.get("blocked_reason") or "").strip()
    samples = _texts(diagnostic.get("sample_blocked_tickers"))
    if blocked_rows <= 0 and not reason and not samples:
        return ""
    sample_text = f"; examples {', '.join(samples)}" if samples else ""
    reason_text = f"; reason {reason}" if reason else ""
    return (
        f" Source blocker: {eligible_rows} eligible, "
        f"{blocked_rows} blocked{reason_text}{sample_text}."
    )


def _missing_cik_diagnostic_suffix(diagnostic: Mapping[str, object]) -> str:
    type_counts = _mapping(diagnostic.get("missing_cik_type_counts"))
    if not type_counts:
        return ""
    pieces = [
        f"{key}:{int(_number_or_zero(value))}"
        for key, value in sorted(type_counts.items(), key=lambda item: str(item[0]))
        if int(_number_or_zero(value)) > 0
    ]
    counts = ", ".join(pieces)
    company_like = int(_number_or_zero(diagnostic.get("missing_cik_company_like_rows")))
    non_company = int(_number_or_zero(diagnostic.get("missing_cik_non_company_rows")))
    unknown = int(_number_or_zero(diagnostic.get("missing_cik_unknown_type_rows")))
    template = str(diagnostic.get("manual_template_command") or "").strip()
    template_text = f" Template: {template}." if template else ""
    return (
        " Missing CIK types: "
        f"{counts}; company-like {company_like}, non-company {non_company}, "
        f"unknown {unknown}.{template_text}"
    )


def _non_company_route_suffix(diagnostic: Mapping[str, object]) -> str:
    routed = int(_number_or_zero(diagnostic.get("routed_non_company_rows")))
    if routed <= 0:
        return ""
    samples = _texts(diagnostic.get("sample_routed_non_company_tickers"))
    route = str(diagnostic.get("non_company_evidence_route") or "").strip()
    sample_text = f" Examples: {', '.join(samples)}." if samples else ""
    route_text = f" Route: {route}" if route else ""
    return f" Non-company routed: {routed}.{sample_text}{route_text}"


def _priced_in_all_source_batch_message(
    engine: Engine,
    config: AppConfig,
    *,
    filters: DashboardFilters,
) -> str:
    payload = dashboard_data.priced_in_all_source_gap_batches_payload(
        engine,
        config,
        available_at=filters.available_at,
        status=filters.priced_in_status,
        usefulness=filters.priced_in_usefulness,
        decision_gap=filters.priced_in_decision_gap,
        stocks_only=filters.priced_in_stocks_only,
    )
    rows = _rows(payload.get("sources"))
    pieces = [
        (
            f"{row.get('source')}={row.get('status')} "
            f"{_source_batch_gap_summary(row)} "
            f"batches={int(_number_or_zero(row.get('batch_count')))}"
        )
        for row in rows
    ]
    ready_rows = [
        row
        for row in rows
        if str(row.get("status") or "") == "ready"
        and str(row.get("execute_next_command") or "").strip()
    ]
    first_ready = (
        sorted(ready_rows, key=_source_batch_priority_key)[0] if ready_rows else None
    )
    execution_gate = _mapping(payload.get("source_execution_gate"))
    execution_blocked = str(execution_gate.get("status") or "") == "blocked"
    command = (
        f" First executable: {first_ready.get('execute_next_command')}."
        if first_ready and not execution_blocked
        else ""
    )
    capped_command = (
        f" Capped run: {first_ready.get('execute_batches_command')}."
        if first_ready and not execution_blocked and first_ready.get("execute_batches_command")
        else ""
    )
    execution_gate_text = _all_source_execution_gate_text(execution_gate)
    next_action = str(payload.get("next_action") or "").strip()
    next_action_text = f" Suggested first: {next_action}" if next_action else ""
    recommended_unblock_text = _all_source_mission_recommended_unblock(payload)
    recommendation_text = _all_source_recommendation_detail(payload)
    scan_scope = _mapping(payload.get("scan_scope"))
    ranked_rows = int(_number_or_zero(scan_scope.get("ranked_rows")))
    review_command = str(scan_scope.get("review_full_scan_command") or "").strip()
    export_command = str(scan_scope.get("export_full_scan_command") or "").strip()
    scan_text = (
        f" Full scan universe: {ranked_rows} ranked row(s)."
        if ranked_rows
        else " Full scan universe: all ranked rows."
    )
    review_text = (
        f" Review rows: {review_command}."
        if review_command
        else ""
    )
    export_text = (
        f" Export rows: {export_command}."
        if export_command
        else ""
    )
    return (
        f"{payload.get('headline')} This is plan-only and makes no provider calls. "
        "Full scan is already the ranked universe; the tickers below are only "
        "first safe provider chunks. Source execution is split into safe chunks. "
        f"{scan_text}{review_text}{export_text} "
        f"{'; '.join(pieces)}.{next_action_text}{recommended_unblock_text}"
        f"{recommendation_text}{execution_gate_text}{command}{capped_command}"
    )


def _all_source_execution_gate_text(gate: Mapping[str, object]) -> str:
    if str(gate.get("status") or "") != "blocked":
        return ""
    blocked_by = str(gate.get("blocked_by") or "source gate").strip()
    gaps = int(_number_or_zero(gate.get("blocked_gap_rows")))
    command = str(gate.get("command") or "").strip()
    command_text = f" Unblock command: {command}." if command else ""
    return (
        f" Source execution blocked by {blocked_by} ({gaps} gap row(s)); "
        "planned source chunks are review-only until this clears."
        f"{command_text}"
    )



def _all_source_mission_recommended_unblock(payload):
    mission = _mapping(payload.get("mission_brief"))
    action = _mapping(mission.get("recommended_unblock_action"))
    command = str(action.get("tui_command") or action.get("command") or "").strip()
    if not command:
        return ""
    kind = str(action.get("kind") or "action").strip()
    status = str(action.get("status") or "unknown").strip()
    calls = int(_number_or_zero(action.get("external_calls_required")))
    writes = int(_number_or_zero(action.get("db_writes_required")))
    approval = "approval required" if bool(action.get("approval_required")) else "no approval"
    reason = str(action.get("reason") or "").strip()
    reason_text = f" Reason: {reason}." if reason else ""
    return (
        f" Recommended unblock: {kind} {status}; {approval}; "
        f"calls {calls}; DB writes {writes}; command {command}.{reason_text}"
    )


def _all_source_recommendation_detail(payload: Mapping[str, object]) -> str:
    details: list[str] = []
    for key, label in (
        ("coverage_first_recommendation", "Coverage-first chunk"),
        ("decision_shortcut_recommendation", "Decision shortcut chunk"),
    ):
        recommendation = _mapping(payload.get(key))
        source = str(recommendation.get("source") or "").strip()
        if not source:
            continue
        row = _source_plan_row(payload, source)
        first_batch = _mapping(row.get("first_batch")) if row else {}
        if not first_batch:
            continue
        tickers = ", ".join(_texts(first_batch.get("tickers"))) or "n/a"
        calls = int(_number_or_zero(first_batch.get("external_calls_required")))
        command = str(first_batch.get("command") or recommendation.get("command") or "")
        details.append(
            f" {label} (first provider chunk only): {source} rows "
            f"{first_batch.get('row_start')}-{first_batch.get('row_end')}; "
            f"tickers {tickers}; calls {calls}; command {command}."
            f"{_source_batch_diagnostic_summary(_mapping(row.get('diagnostic')))}"
        )
    return "".join(details)


def _source_plan_row(
    payload: Mapping[str, object],
    source: str,
) -> Mapping[str, object]:
    for row in _rows(payload.get("sources")):
        if str(row.get("source") or "").strip() == source:
            return row
    return {}


def _source_batch_priority_key(row: Mapping[str, object]) -> tuple[int, int, int, str]:
    decision_rows = int(_number_or_zero(row.get("decision_useful_gap_rows")))
    research_rows = int(_number_or_zero(row.get("research_useful_gap_rows")))
    actionable_rows = int(_number_or_zero(row.get("actionable_gap_rows")))
    source = str(row.get("source") or "")
    try:
        source_order = dashboard_data.PRICED_IN_SOURCE_CLASSES.index(source)
    except ValueError:
        source_order = len(dashboard_data.PRICED_IN_SOURCE_CLASSES)
    if decision_rows:
        return (0, -decision_rows, source_order, source)
    if research_rows:
        return (1, -research_rows, source_order, source)
    if actionable_rows:
        return (2, -actionable_rows, source_order, source)
    return (3, 0, source_order, source)


def _parse_source_batch_command(value: str) -> tuple[str, bool, bool, int]:
    parts = [part.strip() for part in value.split() if part.strip()]
    execute_words = {"execute", "exec", "run"}
    full_plan_words = {"all", "full", "full-scan", "fullscan", "plan"}
    lowered = [part.lower() for part in parts]
    if lowered == ["all"]:
        return "all", False, False, 1
    execute = any(part in execute_words for part in lowered)
    all_batches = any(part in full_plan_words for part in lowered)
    numeric_parts = [int(part) for part in lowered if part.isdigit()]
    max_batches = max(1, numeric_parts[-1]) if numeric_parts else 1
    source_parts = [
        part
        for part in parts
        if part.lower() not in execute_words | full_plan_words and not part.isdigit()
    ]
    return " ".join(source_parts), execute, all_batches, max_batches


def _first_priced_in_source_batch_payload(
    engine: Engine,
    config: AppConfig,
    *,
    source: str,
    filters: DashboardFilters,
    all_batches: bool = False,
) -> Mapping[str, object] | str:
    try:
        return dashboard_data.priced_in_source_gap_batches_payload(
            engine,
            config,
            source=source,
            batch_limit=1,
            all_batches=all_batches,
            available_at=filters.available_at,
            status=filters.priced_in_status,
            usefulness=filters.priced_in_usefulness,
            decision_gap=filters.priced_in_decision_gap,
            stocks_only=filters.priced_in_stocks_only,
        )
    except ValueError as exc:
        return str(exc)



_SEC_CIK_COMMAND_USAGE = (
    "Usage: cik template, cik validate, cik import, or cik import execute."
)
_SEC_CIK_SCOPE_TOKENS = {
    "stock",
    "stocks",
    "stock-like",
    "stocks-only",
    "stocks_only",
    "full",
    "all",
    "active",
    "universe",
}


def _execute_sec_cik_command(
    engine: Engine,
    config: AppConfig,
    value: str,
    *,
    filters: DashboardFilters,
):
    parts = [part.strip().lower() for part in value.split() if part.strip()]
    if parts and parts[0] in {"manual", "override", "overrides"}:
        parts = parts[1:]
    if not parts:
        return _SEC_CIK_COMMAND_USAGE
    stocks_only = _sec_cik_stocks_only(parts, filters)
    command_parts = [part for part in parts if part not in _SEC_CIK_SCOPE_TOKENS]
    if not command_parts:
        return _SEC_CIK_COMMAND_USAGE
    action = command_parts[0]
    if action in {"create", "generate"}:
        action = "template"
    if action in {"check", "preview"}:
        action = "validate"
    try:
        template = _sec_cik_template_payload(
            engine,
            config,
            filters=filters,
            stocks_only=stocks_only,
        )
        csv_path = _sec_cik_default_path()
        if action == "template":
            result = write_sec_cik_override_template_csv(
                csv_path,
                _rows(template.get("rows")),
            )
            return _sec_cik_template_message(template, result.as_payload())
        if action == "validate":
            validation = validate_sec_cik_overrides_csv(engine, csv_path).as_payload()
            return _sec_cik_validation_message(
                "SEC CIK validation",
                validation,
                include_execute_hint=True,
            )
        if action == "import":
            validation = validate_sec_cik_overrides_csv(engine, csv_path).as_payload()
            execute = "execute" in command_parts
            if not execute:
                return _sec_cik_validation_message(
                    "SEC CIK import preview",
                    validation,
                    include_execute_hint=True,
                )
            if str(validation.get("status") or "") == "blocked":
                return _sec_cik_validation_message(
                    "SEC CIK import blocked",
                    validation,
                    include_execute_hint=False,
                )
            result = apply_sec_cik_overrides_csv(engine, csv_path)
            return _sec_cik_import_execute_message(result.as_payload())
    except (FileNotFoundError, KeyError, PermissionError, ValueError) as exc:
        return f"SEC CIK action failed: {exc}"
    return _SEC_CIK_COMMAND_USAGE


def _sec_cik_stocks_only(
    parts: Sequence[str],
    filters: DashboardFilters,
):
    if any(part in {"full", "all", "active", "universe"} for part in parts):
        return False
    if any(part.startswith("stock") for part in parts):
        return True
    return bool(filters.priced_in_stocks_only)


def _sec_cik_template_payload(
    engine: Engine,
    config: AppConfig,
    *,
    filters: DashboardFilters,
    stocks_only: bool,
):
    return dashboard_data.sec_cik_override_template_payload(
        engine,
        config,
        available_at=filters.available_at,
        status=filters.priced_in_status,
        usefulness=filters.priced_in_usefulness,
        decision_gap=filters.priced_in_decision_gap,
        stocks_only=stocks_only,
    )


def _sec_cik_default_path():
    return Path("data") / "local" / "cik-overrides-template.csv"


def _sec_cik_template_message(
    template: Mapping[str, object],
    result: Mapping[str, object],
):
    return (
        "SEC CIK template ready; "
        f"rows={template.get('row_count')}; "
        f"stocks_only={str(bool(template.get('stocks_only'))).lower()}; "
        f"path={result.get('output_path')}; "
        f"external_calls={template.get('external_calls_made')}; "
        "db_writes=0. Fill exact SEC CIKs, then run cik validate; use "
        "cik import execute only after reviewing validation."
    )


def _sec_cik_validation_message(
    label: str,
    validation: Mapping[str, object],
    *,
    include_execute_hint: bool,
):
    status = str(validation.get("status") or "unknown")
    parts = [
        f"{label}: status={status}",
        f"requested={validation.get('requested_count')}",
        f"valid={validation.get('valid_count')}",
        f"updates={validation.get('update_candidate_count')}",
        f"skipped={validation.get('skipped_count')}",
        f"unmatched={validation.get('unmatched_count')}",
        f"invalid={validation.get('invalid_count')}",
        f"duplicates={validation.get('duplicate_count')}",
        f"external_calls={validation.get('external_calls_made')}",
        "db_writes=0",
    ]
    next_action = str(validation.get("next_action") or "").strip()
    if include_execute_hint and status in {"ready", "attention", "noop"}:
        parts.append("execute with cik import execute after reviewing exact CIKs")
    elif next_action:
        parts.append(f"next={next_action}")
    return "; ".join(parts)


def _sec_cik_import_execute_message(result: Mapping[str, object]):
    parts = [
        "SEC CIK import executed",
        f"requested={result.get('requested_count')}",
        f"updated={result.get('updated_count')}",
        f"skipped={result.get('skipped_count')}",
        f"unmatched={result.get('unmatched_count')}",
        f"invalid={result.get('invalid_count')}",
        f"external_calls={result.get('external_calls_made')}",
        "db_writes=1",
    ]
    next_action = str(result.get("next_action") or "").strip()
    if next_action:
        parts.append(f"next={next_action}")
    return "; ".join(parts)


_OPTIONS_COMMAND_USAGE = (
    "Usage: options template, options validate, options import, "
    "or options import execute."
)
_OPTIONS_SCOPE_TOKENS = {
    "stock",
    "stocks",
    "stock-like",
    "stocks-only",
    "stocks_only",
    "full",
    "all",
    "active",
    "universe",
}


def _execute_options_fixture_command(
    engine: Engine,
    config: AppConfig,
    value: str,
    *,
    filters: DashboardFilters,
):
    parts = [part.strip().lower() for part in value.split() if part.strip()]
    if parts and parts[0] in {"fixture", "manual", "point-in-time", "point_in_time"}:
        parts = parts[1:]
    if not parts:
        return _OPTIONS_COMMAND_USAGE
    stocks_only = _options_fixture_stocks_only(parts, filters)
    command_parts = [part for part in parts if part not in _OPTIONS_SCOPE_TOKENS]
    if not command_parts:
        return _OPTIONS_COMMAND_USAGE
    action = command_parts[0]
    if action in {"check", "preview"}:
        action = "validate"
    try:
        template = _options_fixture_template_payload(
            engine,
            config,
            filters=filters,
            stocks_only=stocks_only,
        )
        fixture_path = _options_fixture_default_path(template)
        expected_as_of = _options_fixture_expected_as_of(template)
        if action in {"create", "generate", "template"}:
            wr = write_options_fixture_template_json(
                fixture_path,
                _mapping(template.get("fixture")),
            )
            return _options_fixture_template_message(
                template,
                wr.as_payload(),
            )
        if action == "validate":
            validation = validate_options_fixture_json(
                fixture_path,
                expected_as_of=expected_as_of,
            ).as_payload()
            return _options_fixture_validation_message(
                "Options fixture validation",
                validation,
                include_execute_hint=True,
            )
        if action == "import":
            validation = validate_options_fixture_json(
                fixture_path,
                expected_as_of=expected_as_of,
            ).as_payload()
            execute = "execute" in command_parts
            if not execute:
                return _options_fixture_validation_message(
                    "Options fixture import preview",
                    validation,
                    include_execute_hint=True,
                )
            if str(validation.get("status") or "") != "ready":
                return _options_fixture_validation_message(
                    "Options fixture import blocked",
                    validation,
                    include_execute_hint=False,
                )
            result = _ingest_options_fixture(
                engine,
                fixture_path=fixture_path,
            )
            return _options_fixture_import_execute_message(result)
    except (
        FileNotFoundError,
        KeyError,
        PermissionError,
        ProviderIngestError,
        RuntimeError,
        ValueError,
    ) as exc:
        return f"Options fixture action failed: {exc}"
    return _OPTIONS_COMMAND_USAGE


def _options_fixture_stocks_only(
    parts: Sequence[str],
    filters: DashboardFilters,
):
    if any(part in {"full", "all", "active", "universe"} for part in parts):
        return False
    if any(part.startswith("stock") for part in parts):
        return True
    return bool(filters.priced_in_stocks_only)


def _options_fixture_template_payload(
    engine: Engine,
    config: AppConfig,
    *,
    filters: DashboardFilters,
    stocks_only: bool,
):
    return dashboard_data.options_fixture_template_payload(
        engine,
        config,
        available_at=filters.available_at,
        status=filters.priced_in_status,
        usefulness=filters.priced_in_usefulness,
        decision_gap=filters.priced_in_decision_gap,
        stocks_only=stocks_only,
    )


def _options_fixture_default_path(template: Mapping[str, object]):
    target = str(template.get("target_date") or "").strip()
    if not target or "<" in target or ">" in target:
        raise ValueError(
            "options fixture target date is ambiguous; set one scan date before "
            "creating or importing a point-in-time fixture"
        )
    return Path("data") / "local" / f"point-in-time-options-{target}.json"


def _options_fixture_expected_as_of(template: Mapping[str, object]):
    target = str(template.get("target_date") or "").strip()
    if not target or "<" in target or ">" in target:
        return None
    return date.fromisoformat(target)


def _options_fixture_template_message(
    template: Mapping[str, object],
    wr: Mapping[str, object],
):
    return (
        "Options fixture template ready; "
        f"rows={template.get('row_count')}; "
        f"stocks_only={str(bool(template.get('stocks_only'))).lower()}; "
        f"target={template.get('target_date')}; "
        f"path={wr.get('output_path')}; "
        f"external_calls={template.get('external_calls_made')}; "
        "db_writes=0. Fill point-in-time option fields, then run "
        "options validate; use options import execute only after validation is ready."
    )


def _options_fixture_validation_message(
    label: str,
    validation: Mapping[str, object],
    *,
    include_execute_hint: bool,
):
    status = str(validation.get("status") or "unknown")
    parts = [
        f"{label}: status={status}",
        f"rows={validation.get('row_count')}",
        f"valid={validation.get('valid_row_count')}",
        f"invalid={validation.get('invalid_row_count')}",
        f"blank_required={validation.get('blank_required_count')}",
        f"invalid_numeric={validation.get('invalid_numeric_count')}",
        f"missing_fields={validation.get('missing_field_count')}",
        f"duplicates={validation.get('duplicate_ticker_count')}",
        f"as_of={validation.get('as_of')}",
        f"path={validation.get('path')}",
        f"external_calls={validation.get('external_calls_made')}",
        "db_writes=0",
    ]
    if include_execute_hint and status == "ready":
        parts.append("execute with options import execute after reviewing")
    elif status != "ready":
        next_action = str(validation.get("next_action") or "").strip()
        if next_action:
            parts.append(f"next={next_action}")
    return "; ".join(parts)


def _ingest_options_fixture(
    engine: Engine,
    *,
    fixture_path: Path,
):
    connector = OptionsAggregateConnector(fixture_path=fixture_path)
    request = ConnectorRequest(
        provider="options_fixture",
        endpoint="fixture",
        params={"fixture": str(fixture_path)},
        requested_at=datetime.now(UTC),
    )
    return ingest_provider_records(
        connector=connector,
        request=request,
        market_repo=MarketRepository(engine),
        provider_repo=ProviderRepository(engine),
        job_type="options_fixture",
        metadata={"provider": "options_fixture", "fixture": str(fixture_path)},
        feature_repo=FeatureRepository(engine),
    )


def _options_fixture_import_execute_message(result):
    return (
        "Options fixture import executed; "
        f"raw={result.raw_count}; "
        f"normalized={result.normalized_count}; "
        f"option_features={result.option_feature_count}; "
        f"rejected={result.rejected_count}; "
        "external_calls=0; db_writes=1. Refresh the dashboard and rerun "
        "the priced-in source roadmap before trusting options evidence."
    )


_MARKET_BAR_COMMAND_USAGE = (
    "Usage: bars, bars status, bars manual template/import, "
    "or bars saved capture/import."
)


def _execute_market_bar_command(
    engine: Engine,
    config: AppConfig,
    payload: Mapping[str, object],
    value: str,
    *,
    filters: DashboardFilters,
) -> str:
    parts = [part.strip().lower() for part in value.split() if part.strip()]
    if not parts:
        return _market_bar_status_message(payload)
    head = parts[0]
    if head in {"status", "next", "plan"}:
        return _market_bar_status_message(payload)
    if head in {"help", "?"}:
        return _MARKET_BAR_COMMAND_USAGE
    if head in {"manual", "csv", "template", "preview", "validate", "import"}:
        return _execute_market_bar_manual_command(
            engine,
            payload,
            parts,
            filters=filters,
        )
    if head in {"saved", "saved-file", "file"}:
        return _execute_market_bar_saved_file_command(
            engine,
            config,
            payload,
            value,
        )
    return _MARKET_BAR_COMMAND_USAGE


def _market_bar_status_message(payload: Mapping[str, object]) -> str:
    answer = _mapping(payload.get("priced_in_answer"))
    trust_gate = _mapping(answer.get("full_market_trust_gate"))
    audit = _mapping(payload.get("priced_in_audit"))
    market = _mapping(audit.get("market_bars"))
    repair = _mapping(market.get("repair"))
    provider_plan = _mapping(repair.get("provider_fill_plan"))
    status = str(
        _first_value(
            trust_gate.get("status"),
            market.get("status"),
            repair.get("status"),
            "unknown",
        )
    ).strip()
    first_blocker = str(trust_gate.get("first_blocker") or "").strip()
    gate_gap = (
        trust_gate.get("first_gap_count")
        if first_blocker == "market_bars"
        else None
    )
    missing = int(
        _number_or_zero(
            _first_value(
                gate_gap,
                market.get("missing_as_of_bar"),
                repair.get("missing_as_of_bar_count"),
                repair.get("missing_as_of_bar"),
                repair.get("missing_expected_count"),
                provider_plan.get("missing_as_of_bar_count"),
                provider_plan.get("missing_as_of_bar"),
            )
        )
    )
    target = str(
        _first_value(
            repair.get("target_as_of"),
            repair.get("expected_as_of"),
            market.get("expected_as_of"),
            provider_plan.get("target_as_of"),
            provider_plan.get("expected_as_of"),
        )
        or ""
    ).strip()
    headline = [f"Market-bar status: {status}"]
    if target:
        headline.append(f"as_of={target}")
    if missing:
        headline.append(f"missing={missing}")
    parts = ["; ".join(headline)]
    recommended = _market_bar_recommended_action_summary(payload)
    if recommended:
        parts.append(f"Recommended: {recommended}")
        parts.append(
            "Unblock checklist: review counts, approve/capture saved file, "
            "validate saved file, preview import, execute import, "
            "rerun priced-in answer"
        )
    after_clear = _market_bar_after_clear_summary(payload)
    if after_clear:
        parts.append(f"After bars clear: {after_clear}")
    missing_sample = _market_bar_missing_sample_summary(payload)
    if missing_sample:
        parts.append(f"Missing sample: {missing_sample}")
    stock_scope = _market_bar_stock_scope_summary(payload)
    if stock_scope:
        parts.append(f"Stock scope: {stock_scope}")
    manual_progress = _market_bar_manual_fill_progress_summary(payload)
    if manual_progress:
        parts.append(f"Manual CSV: {manual_progress}")
    operator_step = _market_bar_operator_step_summary(payload)
    if operator_step:
        parts.append(f"Next manual action: {operator_step}")
    saved_capture = _market_bar_provider_saved_file_capture_summary(payload)
    if saved_capture:
        parts.append(f"Saved capture: {saved_capture}")
    saved_validate = _market_bar_provider_saved_file_validate_summary(payload)
    if saved_validate:
        parts.append(f"Saved validate: {saved_validate}")
    saved_import = _market_bar_provider_saved_file_summary(payload)
    if saved_import:
        parts.append(f"Saved import: {saved_import}")
    parts.append("Status check made 0 provider calls and 0 database writes.")
    return " | ".join(part for part in parts if part)


def _market_bar_after_clear_summary(payload: Mapping[str, object]):
    preview = _mapping(payload.get("after_market_bars_clear"))
    if not preview:
        answer = _mapping(payload.get("priced_in_answer"))
        trust_gate = _mapping(answer.get("full_market_trust_gate"))
        preview = _mapping(trust_gate.get("after_current_blocker"))
    if str(preview.get("current_blocker") or "").strip() != "market_bars":
        return ""
    return _after_current_blocker_summary(preview)


def _market_bar_missing_sample_summary(payload: Mapping[str, object]) -> str:
    repair = _market_bar_repair_payload(payload)
    plan = _market_bar_provider_fill_plan(payload)
    packet = _mapping(plan.get("provider_saved_file_capture_approval_packet"))
    sample = _texts(
        repair.get("missing_as_of_bar_ticker_sample")
        or payload.get("missing_as_of_bar_ticker_sample")
        or packet.get("missing_as_of_bar_ticker_sample")
    )
    if not sample:
        return ""
    more = int(
        _number_or_zero(
            repair.get("missing_as_of_bar_ticker_more")
            or payload.get("missing_as_of_bar_ticker_more")
            or packet.get("missing_as_of_bar_ticker_more")
        )
    )
    suffix = f" plus {more} more" if more else ""
    return ", ".join(sample[:8]) + suffix


def _market_bar_stock_scope_summary(payload: Mapping[str, object]):
    repair = _market_bar_repair_payload(payload)
    stock_scope = _mapping(payload.get("stock_scope") or repair.get("stock_scope"))
    if not stock_scope:
        return ""
    active = int(_number_or_zero(stock_scope.get("stock_like_active")))
    with_bar = int(_number_or_zero(stock_scope.get("stock_like_with_as_of_bar")))
    missing = int(_number_or_zero(stock_scope.get("stock_like_missing_as_of_bar")))
    if active <= 0:
        return ""
    parts = [f"{with_bar}/{active} stock-like bars present"]
    parts.append(f"{missing} missing" if missing else "ready")
    non_stock_missing = int(
        _number_or_zero(stock_scope.get("non_stock_missing_as_of_bar"))
    )
    if non_stock_missing:
        parts.append(f"{non_stock_missing} non-stock missing")
    sample = _texts(
        stock_scope.get("sample_missing_stock_like_tickers")
        or stock_scope.get("sample_missing_tickers")
    )
    if sample:
        more = int(
            _number_or_zero(
                stock_scope.get("sample_missing_stock_like_more")
                or stock_scope.get("sample_missing_more")
            )
        )
        suffix = f" plus {more} more" if more else ""
        parts.append(f"sample {', '.join(sample[:6])}{suffix}")
    if missing:
        parts.append("command bars manual stocks template")
    parts.append("0 provider calls")
    return "; ".join(parts)


def _market_bar_recommended_action_summary(payload):
    recommended = _mapping(payload.get("recommended_action"))
    if recommended:
        command = str(
            recommended.get("tui_command") or recommended.get("command") or ""
        ).strip()
        calls = int(_number_or_zero(recommended.get("external_calls_required")))
        writes = int(_number_or_zero(recommended.get("db_writes_required")))
        if command:
            return (
                f"{command}; {calls} provider call(s) if approved; "
                f"{writes} DB write(s)"
            )
    audit = _mapping(payload.get("priced_in_audit"))
    market = _mapping(audit.get("market_bars"))
    repair = _mapping(market.get("repair"))
    provider_plan = _mapping(repair.get("provider_fill_plan"))
    approval = _mapping(
        _first_value(
            repair.get("provider_saved_file_capture_approval_packet"),
            provider_plan.get("provider_saved_file_capture_approval_packet"),
        )
    )
    if approval.get("status") == "approval_required":
        command = str(
            approval.get("tui_confirm_command") or "bars saved capture confirm"
        )
        calls = int(_number_or_zero(approval.get("external_calls_if_approved")))
        writes = int(_number_or_zero(approval.get("db_writes_during_capture")))
        return (
            f"{command}; {calls} provider call(s) if approved; "
            f"{writes} DB write(s)"
        )
    saved_status = str(
        _first_value(
            approval.get("saved_file_status"),
            repair.get("provider_saved_file_status"),
        )
        or ""
    ).strip()
    if saved_status == "available":
        return "bars saved validate; 0 provider calls; 0 DB writes"
    operator = _mapping(repair.get("operator_step"))
    command = str(
        operator.get("command") or operator.get("after_manual_command") or ""
    ).strip()
    if command:
        return f"{command}; 0 provider calls before execute"
    return ""

_MARKET_BAR_MANUAL_SCOPE_TOKENS = {
    "stock",
    "stocks",
    "stock-like",
    "stocks-only",
    "stocks_only",
    "full",
    "all",
    "active",
    "universe",
}


def _execute_market_bar_manual_command(
    engine: Engine,
    payload: Mapping[str, object],
    parts: Sequence[str],
    *,
    filters: DashboardFilters,
) -> str:
    normalized = [part for part in parts if part != "manual"]
    stocks_only = _market_bar_manual_stocks_only(normalized, filters)
    command_parts = [
        part
        for part in normalized
        if part not in _MARKET_BAR_MANUAL_SCOPE_TOKENS
    ]
    if not command_parts:
        return _MARKET_BAR_COMMAND_USAGE
    action = command_parts[0]
    if action in {"check", "preview", "validate"}:
        action = "import"
    try:
        repair = _market_bar_manual_repair(payload, stocks_only=stocks_only)
        if action == "template":
            result = write_manual_market_bars_template(
                engine,
                output_path=_market_bar_manual_path(repair),
                expected_as_of=_market_bar_manual_date(repair),
                missing_only=True,
                stocks_only=stocks_only,
                overwrite="overwrite" in command_parts,
            )
            return _manual_market_bar_template_message(result.as_payload())
        if action == "import":
            execute = "execute" in command_parts
            result = import_manual_market_bars(
                engine,
                daily_bars_path=_market_bar_manual_path(repair),
                expected_as_of=_market_bar_manual_date(repair),
                stocks_only=stocks_only,
                complete_rows_only=True,
                execute=execute,
            )
            import_payload = result.as_payload()
            import_payload["post_import_verification"] = (
                market_bars_import_verification_payload(
                    engine,
                    AppConfig.from_env(),
                    expected_as_of=result.expected_as_of,
                    stocks_only=result.stocks_only,
                    executed=result.executed,
                    source="manual_csv",
                    db_changes_made=1 if result.executed else 0,
                    projected_missing_after_import_count=(
                        None if result.executed else len(result.missing_expected_tickers)
                    ),
                    projected_db_changes_made=None if result.executed else 1,
                )
            )
            return _manual_market_bar_import_message(import_payload)
    except (FileNotFoundError, KeyError, PermissionError, ValueError) as exc:
        return f"Manual market-bar action failed: {exc}"
    return _MARKET_BAR_COMMAND_USAGE


def _market_bar_repair_payload(payload: Mapping[str, object]) -> Mapping[str, object]:
    audit = _mapping(payload.get("priced_in_audit"))
    market = _mapping(audit.get("market_bars"))
    repair = _mapping(market.get("repair"))
    if repair:
        return repair
    return _mapping(payload.get("repair_plan"))


def _market_bar_manual_repair(
    payload: Mapping[str, object],
    *,
    stocks_only: bool,
) -> Mapping[str, object]:
    repair = _market_bar_repair_payload(payload)
    if stocks_only:
        stock_scope = _mapping(repair.get("stock_scope"))
        if stock_scope:
            return stock_scope
    if repair:
        return repair
    preflight = _mapping(payload.get("priced_in_preflight"))
    first_blocker = _mapping(preflight.get("first_blocker"))
    if str(first_blocker.get("area") or "") == "market_bars":
        if stocks_only:
            stock_scope = _mapping(first_blocker.get("stock_scope"))
            if stock_scope:
                return stock_scope
        return first_blocker
    return {}


def _market_bar_manual_stocks_only(
    parts: Sequence[str],
    filters: DashboardFilters,
) -> bool:
    if any(part in {"full", "all", "active", "universe"} for part in parts):
        return False
    if any(part.startswith("stock") for part in parts):
        return True
    return bool(filters.priced_in_stocks_only)



def _market_bar_manual_date(repair: Mapping[str, object]) -> date:
    value = str(
        repair.get("target_as_of") or repair.get("expected_as_of") or ""
    ).strip()
    if not value:
        raise ValueError("manual market-bar repair data is missing target_as_of")
    return date.fromisoformat(value)


def _market_bar_manual_path(repair: Mapping[str, object]) -> Path:
    value = str(
        repair.get("local_template_path")
        or repair.get("daily_bars_path")
        or ""
    ).strip()
    if not value:
        raise ValueError("manual market-bar repair data is missing local_template_path")
    return Path(value)


def _market_bar_post_import_summary(
    payload: Mapping[str, object],
    *,
    payload_key: str = "post_import_verification",
    label: str = "Post-import",
):
    verification = _mapping(payload.get(payload_key))
    if not verification:
        return ""
    next_value = (
        verification.get("next_blocker")
        or verification.get("next_blocker_action")
        or verification.get("next_action")
    )
    projected = verification.get("projected_missing_after_import_count")
    projection = verification.get("preview_projection_status")
    projection_text = (
        f"; projected_missing={projected}; projection={projection}"
        if projected is not None
        else ""
    )
    return (
        f"{label}: "
        f"status={verification.get('status')}; "
        f"missing={verification.get('missing_as_of_bar_count')}"
        f"{projection_text}; "
        f"next={_clip(next_value or 'rerun priced-in answer', 96)}"
    )


def _manual_market_bar_template_message(payload: Mapping[str, object]) -> str:
    return (
        "Manual market-bar template ready; "
        f"rows={payload.get('row_count')}; "
        f"stocks_only={str(bool(payload.get('stocks_only'))).lower()}; "
        f"path={payload.get('output_path')}; "
        f"external_calls={payload.get('external_calls_made')}; "
        "db_writes=0. Fill complete OHLCV/VWAP rows, then run "
        "`bars manual import` to preview."
    )


def _manual_market_bar_import_message(payload: Mapping[str, object]) -> str:
    fill = _mapping(payload.get("fill_progress"))
    executed = bool(payload.get("executed"))
    db_writes = 1 if executed else 0
    label = "executed" if executed else "preview"
    parts = [
        f"Manual market-bar import {label}: status={payload.get('status')}",
        f"complete_rows_only={str(bool(payload.get('complete_rows_only'))).lower()}",
        f"complete={fill.get('complete_rows')}",
        f"partial={fill.get('partial_rows')}",
        f"empty={fill.get('empty_rows')}",
        f"missing_after_import={payload.get('missing_expected_count')}",
        f"external_calls={payload.get('external_calls_made')}",
        f"db_writes={db_writes}",
    ]
    post_import = _market_bar_post_import_summary(payload)
    if post_import:
        parts.append(post_import)
    next_action = str(payload.get("next_action") or "").strip()
    if next_action:
        parts.append(f"next={next_action}")
    if not executed:
        parts.append("execute with `bars manual import execute` after preview")
    return "; ".join(parts)


_MARKET_BAR_SAVED_FILE_USAGE = (
    "Usage: bars saved capture, bars saved capture confirm, "
    "bars saved validate, bars saved import, or bars saved import execute."
)


def _execute_market_bar_saved_file_command(
    engine: Engine,
    config: AppConfig,
    payload: Mapping[str, object],
    value: str,
) -> str:
    parts = [part.strip().lower() for part in value.split() if part.strip()]
    if parts and parts[0] in {"saved", "saved-file", "file"}:
        parts = parts[1:]
    if not parts:
        return _MARKET_BAR_SAVED_FILE_USAGE
    action = parts[0]
    if action in {"check", "preview"}:
        action = "validate"
    if action not in {"capture", "validate", "import"}:
        return _MARKET_BAR_SAVED_FILE_USAGE
    try:
        if action == "capture":
            return _market_bar_saved_file_capture_command(
                engine,
                config,
                payload,
                confirmed="confirm" in parts or "execute" in parts,
            )
        if action == "validate":
            body = _market_bar_saved_file_request_body(
                payload,
                "provider_saved_file_validate_request_body",
            )
            preview = _preview_saved_market_bar_file(engine, config, body)
            return _saved_market_bar_preview_message("Saved-file validate", preview)
        execute = "execute" in parts
        body = _market_bar_saved_file_request_body(
            payload,
            "provider_saved_file_import_request_body"
            if execute
            else "provider_saved_file_import_preview_request_body",
        )
        if execute:
            preview = _preview_saved_market_bar_file(engine, config, body)
            if str(preview.get("status") or "") == "invalid":
                return _saved_market_bar_preview_message(
                    "Saved-file import blocked",
                    preview,
                )
            result = ingest_polygon_grouped_daily_fixture(
                config=config,
                market_repo=MarketRepository(engine),
                provider_repo=ProviderRepository(engine),
                date_value=_market_bar_saved_file_date(body),
                fixture_path=_market_bar_saved_file_path(body, "fixture_path"),
            )
            verification = market_bars_import_verification_payload(
                engine,
                config,
                expected_as_of=_market_bar_saved_file_date(body),
                executed=True,
                source="saved_provider_file",
                db_changes_made=1,
            )
            post_import = _market_bar_post_import_summary(
                {"post_import_verification": verification}
            )
            return (
                "Saved-file import executed; "
                f"daily_bars={result.daily_bar_count}; "
                f"rejected={result.rejected_count}; "
                "external_calls=0; db_writes=1. "
                f"{post_import}"
            )
        preview = _preview_saved_market_bar_file(engine, config, body)
        coverage = _mapping(preview.get("coverage"))
        verification = market_bars_import_verification_payload(
            engine,
            config,
            expected_as_of=_market_bar_saved_file_date(body),
            executed=False,
            source="saved_provider_file",
            db_changes_made=0,
            projected_missing_after_import_count=int(
                coverage.get("missing_after_import_count") or 0
            ),
            projected_db_changes_made=1,
        )
        post_import = _market_bar_post_import_summary(
            {"post_import_verification": verification}
        )
        return (
            f"{_saved_market_bar_preview_message('Saved-file import preview', preview)} "
            f"{post_import}. "
            "No database writes were made; type `bars saved import execute` "
            "only after the preview covers the intended missing bars."
        )
    except (
        FileNotFoundError,
        KeyError,
        PermissionError,
        RuntimeError,
        ValueError,
        ProviderIngestError,
    ) as exc:
        return f"Saved-file market-bar action failed: {exc}"


def _market_bar_saved_file_capture_command(
    engine: Engine,
    config: AppConfig,
    payload: Mapping[str, object],
    *,
    confirmed: bool,
) -> str:
    body = _market_bar_saved_file_request_body(
        payload,
        "provider_saved_file_capture_confirm_request_body"
        if confirmed
        else "provider_saved_file_capture_request_body",
    )
    output_path = _market_bar_saved_file_path(body, "output_path")
    plan = _market_bar_provider_fill_plan(payload)
    packet = _mapping(plan.get("provider_saved_file_capture_approval_packet"))
    target = str(
        packet.get("expected_as_of")
        or plan.get("target_as_of")
        or body.get("expected_as_of")
        or ""
    ).strip()
    missing = int(
        _number_or_zero(
            packet.get("missing_as_of_bar_count") or plan.get("missing_as_of_bar"),
        ),
    )
    if not confirmed:
        approval_status = str(packet.get("status") or "approval_required")
        confirm_command = str(packet.get("tui_confirm_command") or "bars saved capture confirm")
        target_text = f"target={target}; " if target else ""
        missing_text = f"current_missing={missing}; " if missing else ""
        missing_sample = _market_bar_missing_sample_summary(payload)
        sample_text = f"missing_sample={missing_sample}; " if missing_sample else ""
        return (
            "Saved-file capture is approval-gated; "
            f"status={approval_status}; {target_text}{missing_text}{sample_text}"
            "external_calls_made=0; db_writes_made=0; "
            f"safe request body confirm_external_call=false output_path={output_path}. "
            f"Type `{confirm_command}` only if you approve one "
            "Polygon/Massive grouped-daily provider call. After capture, "
            "type `bars saved import` to preview the saved file, then "
            "`bars saved import execute` only if coverage matches intent."
        )
    approval_guard = _mapping(packet.get("approval_guard"))
    expected_active = _optional_int(
        body.get("expected_active_security_count")
        or approval_guard.get("expected_active_security_count")
    )
    expected_existing = _optional_int(
        body.get("expected_existing_as_of_bar_count")
        or approval_guard.get("expected_existing_as_of_bar_count")
    )
    expected_missing = _optional_int(
        body.get("expected_missing_as_of_bar_count")
        or approval_guard.get("expected_missing_as_of_bar_count")
    )
    guard = saved_capture_approval_guard_payload(
        engine,
        expected_as_of=_market_bar_saved_file_date(body),
        stocks_only=str(plan.get("coverage_scope") or "") == "stock_like",
        expected_active_security_count=expected_active,
        expected_existing_as_of_bar_count=expected_existing,
        expected_missing_as_of_bar_count=expected_missing,
    )
    if guard.get("status") != "ready":
        return _saved_capture_approval_guard_message(guard)
    target_date = _market_bar_saved_file_date(body)
    captured = capture_polygon_grouped_daily_response_with_preview(
        config=config,
        market_repo=MarketRepository(engine),
        date_value=target_date,
        output_path=output_path,
        confirm_external_call=True,
    )
    captured["post_capture_verification"] = market_bars_post_capture_verification_payload(
        engine,
        config,
        expected_as_of=target_date,
        capture_payload=captured,
        stocks_only=str(plan.get("coverage_scope") or "") == "stock_like",
    )
    preview = captured.get("post_capture_preview")
    preview_message = ""
    if isinstance(preview, Mapping):
        preview_message = " " + _saved_market_bar_preview_message(
            "Post-capture preview",
            preview,
        )
    verification_message = _market_bar_post_import_summary(
        captured,
        payload_key="post_capture_verification",
        label="Post-capture verification",
    )
    source = captured.get("source")
    bytes_written = captured.get("bytes_written")
    external_calls = captured.get("external_calls_made")
    saved_output = captured.get("output_path")
    return (
        "Saved-file capture completed; "
        f"source={source}; "
        f"bytes={bytes_written}; "
        f"external_calls={external_calls}; "
        f"output={saved_output}."
        f"{preview_message} "
        f"{verification_message} "
        "Next: bars saved import execute only if the preview matches intent."
    )


def _saved_capture_approval_guard_message(payload: Mapping[str, object]) -> str:
    mismatches = _mapping(payload.get("mismatches"))
    mismatch_parts = []
    for key, value in sorted(mismatches.items()):
        detail = _mapping(value)
        mismatch_parts.append(
            f"{key} expected={detail.get('expected')} current={detail.get('current')}"
        )
    missing_fields = _texts(payload.get("missing_expectation_fields"))
    issue = "; ".join(mismatch_parts)
    if missing_fields:
        issue = f"missing guard fields={', '.join(missing_fields)}"
    return (
        "Saved-file capture blocked by stale approval guard; "
        f"status={payload.get('status')}; {issue or 'review required'}; "
        f"external_calls={payload.get('external_calls_made')}; "
        f"db_writes={payload.get('db_writes_made')}. "
        "Re-run `bars saved capture`, review the current counts, then confirm again."
    )


def _preview_saved_market_bar_file(
    engine: Engine,
    config: AppConfig,
    body: Mapping[str, object],
) -> Mapping[str, object]:
    return preview_polygon_grouped_daily_fixture(
        config=config,
        market_repo=MarketRepository(engine),
        date_value=_market_bar_saved_file_date(body),
        fixture_path=_market_bar_saved_file_path(body, "fixture_path"),
    )


def _saved_market_bar_preview_message(
    label: str,
    preview: Mapping[str, object],
) -> str:
    coverage = _mapping(preview.get("coverage"))
    parts = [
        f"{label}: status={preview.get('status')}",
        f"daily_bars={preview.get('daily_bar_count')}",
        f"rejected={preview.get('rejected_count')}",
        f"missing_covered={coverage.get('missing_covered_by_fixture_count')}",
        f"missing_after_import={coverage.get('missing_after_import_count')}",
        "stock_missing_after_import="
        f"{coverage.get('stock_like_missing_after_import_count')}",
        f"external_calls={preview.get('external_calls_made')}",
        "db_writes=0",
    ]
    next_action = str(preview.get("next_action") or "").strip()
    if next_action:
        parts.append(f"next={next_action}")
    return "; ".join(parts)


def _market_bar_saved_file_request_body(
    payload: Mapping[str, object],
    key: str,
) -> Mapping[str, object]:
    plan = _market_bar_provider_fill_plan(payload)
    body = _mapping(plan.get(key))
    if not body:
        raise ValueError(
            "saved-file request body is missing; refresh the dashboard or run "
            "market-bars repair-plan first",
        )
    return body


def _market_bar_provider_fill_plan(payload: Mapping[str, object]) -> Mapping[str, object]:
    audit = _mapping(payload.get("priced_in_audit"))
    market = _mapping(audit.get("market_bars"))
    repair = _mapping(market.get("repair"))
    plan = _mapping(repair.get("provider_fill_plan"))
    if plan:
        return plan
    preflight = _mapping(payload.get("priced_in_preflight"))
    first_blocker = _mapping(preflight.get("first_blocker"))
    if str(first_blocker.get("area") or "") == "market_bars":
        return first_blocker
    return {}


def _market_bar_saved_file_date(body: Mapping[str, object]) -> date:
    value = str(body.get("expected_as_of") or "").strip()
    if not value:
        raise ValueError("saved-file request body is missing expected_as_of")
    return date.fromisoformat(value)


def _market_bar_saved_file_path(body: Mapping[str, object], key: str) -> Path:
    value = str(body.get(key) or "").strip()
    if not value:
        raise ValueError(f"saved-file request body is missing {key}")
    return Path(value)


def _execute_priced_in_source_batch(
    engine: Engine,
    config: AppConfig,
    *,
    source: str,
    filters: DashboardFilters,
    max_batches: int = 1,
) -> str:
    if not source.strip():
        return (
            "Usage: batch <source> execute. Try: batch catalyst_events execute, "
            "batch local_text execute, batch options execute, or "
            "batch catalyst_events execute 3."
        )
    if source.strip().lower() in {"all", "*"}:
        return (
            "batch all is plan-only. Choose one source before running execute, "
            "for example: batch catalyst_events execute."
        )
    try:
        if int(max_batches) > 1:
            payload = execute_source_batches(
                engine,
                config,
                source=source,
                max_batches=int(max_batches),
                available_at=filters.available_at,
                status=filters.priced_in_status,
                usefulness=filters.priced_in_usefulness,
                decision_gap=filters.priced_in_decision_gap,
                stocks_only=filters.priced_in_stocks_only,
            )
            return source_batch_run_summary(payload)
        payload = execute_source_batch(
            engine,
            config,
            source=source,
            available_at=filters.available_at,
            status=filters.priced_in_status,
            usefulness=filters.priced_in_usefulness,
            decision_gap=filters.priced_in_decision_gap,
            stocks_only=filters.priced_in_stocks_only,
        )
    except ValueError as exc:
        return str(exc)
    return source_batch_execution_summary(payload)


def _execute_guarded_radar_run(
    engine: Engine,
    config: AppConfig,
    payload: Mapping[str, object],
) -> str:
    call_plan = _mapping(payload.get("call_plan"))
    if str(call_plan.get("status") or "") == "blocked":
        return f"Radar run blocked: {call_plan.get('next_action') or 'Review call plan.'}"
    cooldown = _mapping(payload.get("radar_run_cooldown"))
    if cooldown.get("allowed") is False:
        return (
            "Radar run rate limited: "
            f"{cooldown.get('detail') or cooldown.get('next_action') or 'Wait for cooldown.'}"
        )
    now = datetime.now(UTC)
    lock = JobLockRepository(engine).acquire(
        RADAR_RUN_COOLDOWN_LOCK_NAME,
        owner=f"dashboard-tui-cooldown:{uuid4().hex}",
        ttl=timedelta(seconds=max(1, int(config.radar_run_min_interval_seconds))),
        now=now,
        metadata={
            "operation": "manual_radar_run",
            "source": "dashboard_tui",
            "max_external_call_count": call_plan.get("max_external_call_count"),
            "will_call_external_providers": call_plan.get("will_call_external_providers"),
        },
    )
    if not lock.acquired:
        retry = _retry_after_seconds(lock.expires_at, now)
        return f"Radar run rate limited for {retry} second(s)."
    result = run_once(
        engine=engine,
        config=SchedulerConfig(
            owner="dashboard-tui",
            run_llm=True,
            llm_dry_run=True,
            dry_run_alerts=True,
        ),
    )
    run_payload = scheduler_run_payload(result)
    daily_result = _mapping(run_payload.get("daily_result"))
    if not result.acquired_lock:
        return f"Radar run skipped: {result.reason or 'lock held'}."
    return (
        "Radar run finished: "
        f"status={daily_result.get('status') or result.reason or 'unknown'}; "
        f"required={daily_result.get('required_completed_count')}/"
        f"{daily_result.get('required_step_count')}; "
        f"call_plan_max_external={call_plan.get('max_external_call_count')}. "
        "Refresh to inspect updated readiness."
    )


def _save_opportunity_action(engine: Engine, value: str) -> str:
    parts = value.split(maxsplit=2)
    if len(parts) < 2:
        return "Usage: action <ticker> <watch|ready|simulate_entry|dismiss> [notes]"
    ticker, action = parts[0].upper(), parts[1]
    notes = parts[2] if len(parts) > 2 else None
    try:
        row = record_opportunity_action(
            repo=BrokerRepository(engine),
            ticker=ticker,
            action=action,
            notes=notes,
            payload={"source": "dashboard_tui"},
            actor_source="dashboard_tui",
            actor_id="local-tui",
            actor_role="analyst",
        )
    except ValueError as exc:
        return f"Action rejected: {exc}"
    payload = opportunity_action_payload(row)
    return f"Saved action: {payload.get('ticker')} {payload.get('action')} {payload.get('status')}"


def _save_market_trigger(engine: Engine, value: str) -> str:
    parts = value.split(maxsplit=4)
    if len(parts) < 4:
        return (
            "Usage: trigger <ticker> <price_above|price_below|volume_above|"
            "relative_volume_above|call_put_ratio_above> <gte|lte|gt|lt> <threshold> [notes]"
        )
    ticker, trigger_type, operator, threshold_text = (
        parts[0].upper(),
        parts[1],
        parts[2],
        parts[3],
    )
    notes = parts[4] if len(parts) > 4 else None
    try:
        row = create_trigger(
            repo=BrokerRepository(engine),
            ticker=ticker,
            trigger_type=trigger_type,
            operator=operator,
            threshold=float(threshold_text),
            notes=notes,
            payload={"source": "dashboard_tui"},
            actor_source="dashboard_tui",
            actor_id="local-tui",
            actor_role="analyst",
        )
    except ValueError as exc:
        return f"Trigger rejected: {exc}"
    payload = trigger_payload(row)
    return f"Saved trigger: {payload.get('ticker')} {payload.get('trigger_type')}"


def _evaluate_market_triggers(engine: Engine, value: str) -> str:
    tickers = [value.strip().upper()] if value.strip() else []
    try:
        rows = evaluate_triggers(
            repo=BrokerRepository(engine),
            tickers=tickers,
            actor_source="dashboard_tui",
            actor_id="local-tui",
            actor_role="analyst",
        )
    except ValueError as exc:
        return f"Trigger evaluation rejected: {exc}"
    fired = [row for row in rows if row.status.value == "fired"]
    return f"Evaluated {len(rows)} trigger(s); fired {len(fired)}."


def _save_blocked_order_ticket(
    engine: Engine,
    config: AppConfig,
    value: str,
) -> str:
    parts = value.split(maxsplit=5)
    if len(parts) < 4:
        return (
            "Usage: ticket <ticker> <buy|sell> <entry_price> <invalidation_price> "
            "[risk_pct] [notes]"
        )
    ticker, side, entry_text, invalidation_text = (
        parts[0].upper(),
        parts[1],
        parts[2],
        parts[3],
    )
    risk_pct = None
    notes = None
    if len(parts) >= 5:
        try:
            risk_pct = float(parts[4])
            notes = parts[5] if len(parts) >= 6 else None
        except ValueError:
            notes = " ".join(parts[4:])
    try:
        row = create_blocked_order_ticket(
            repo=BrokerRepository(engine),
            ticker=ticker,
            side=side,
            entry_price=float(entry_text),
            invalidation_price=float(invalidation_text),
            risk_per_trade_pct=risk_pct,
            notes=notes,
            config=config,
            actor_source="dashboard_tui",
            actor_id="local-tui",
            actor_role="analyst",
        )
    except ValueError as exc:
        return f"Order ticket rejected: {exc}"
    payload = order_ticket_payload(row)
    return (
        "Saved blocked order ticket: "
        f"{payload.get('ticker')} {payload.get('side')} "
        f"submission_allowed={payload.get('submission_allowed')}"
    )


def _record_alert_feedback(
    engine: Engine,
    payload: Mapping[str, object],
    value: str,
) -> str:
    parts = value.split(maxsplit=2)
    if len(parts) < 2:
        return (
            "Usage: feedback <alert-id|#> "
            "<useful|noisy|too_late|too_early|ignored|acted> [notes]"
        )
    alert_rows = _rows(_mapping(payload.get("alerts")).get("rows"))
    alert = _row_by_index_or_key(alert_rows, parts[0], key="id")
    if not alert:
        return "Alert feedback rejected: alert not found in current alert rows."
    label = parts[1]
    notes = parts[2] if len(parts) > 2 else None
    try:
        result = record_alert_feedback(
            engine,
            artifact_type="alert",
            artifact_id=str(alert["id"]),
            ticker=str(alert["ticker"]),
            label=label,
            notes=notes,
            source="dashboard_tui",
            actor_id="local-tui",
            actor_role="analyst",
        )
    except FeedbackError as exc:
        return f"Alert feedback rejected: {exc}"
    useful_label = result.useful_label
    return (
        "Saved alert feedback: "
        f"{useful_label.artifact_id} {useful_label.ticker} {useful_label.label}"
    )


def _open_target_page(
    payload: Mapping[str, object],
    page: str,
    value: str,
) -> str | None:
    if page in {"overview", "review"}:
        rows = (
            _priced_in_review_rows(payload)
            if page == "review"
            else _priced_in_overview_rows(payload)
        )
        row = _row_by_index_or_key(rows, value, key="ticker")
        ticker = str(row.get("ticker") or "").strip().upper() if row else ""
        return f"candidate:{ticker}" if ticker else None
    if page == "candidates":
        rows = _candidate_rows(payload)
        row = _row_by_index_or_key(rows, value, key="ticker")
        ticker = str(row.get("ticker") or "").strip().upper() if row else ""
        return f"candidate:{ticker}" if ticker else None
    if page == "alerts":
        rows = _rows(_mapping(payload.get("alerts")).get("rows"))
        row = _row_by_index_or_key(rows, value, key="id")
        alert_id = str(row.get("id") or "").strip() if row else ""
        return f"alert:{alert_id}" if alert_id else None
    return None


def _row_by_index_or_key(
    rows: Sequence[Mapping[str, object]],
    value: str,
    *,
    key: str,
) -> Mapping[str, object] | None:
    token = value.strip()
    if token.isdigit():
        index = int(token) - 1
        if 0 <= index < len(rows):
            return rows[index]
    token_upper = token.upper()
    for row in rows:
        if str(row.get(key) or "").strip().upper() == token_upper:
            return row
    return None


def _header_lines(
    payload: Mapping[str, object],
    page: str,
    width: int,
) -> list[str]:
    runtime = _mapping(payload.get("runtime_context"))
    controls = _mapping(payload.get("controls"))
    readiness = _mapping(payload.get("readiness"))
    answer = _mapping(payload.get("priced_in_answer"))
    audit = _mapping(payload.get("priced_in_audit"))
    audit_status = str(audit.get("status") or "").strip().lower()
    answer_status = audit_status or str(answer.get("status") or "unknown")
    answer_ready = (
        "true"
        if bool(answer.get("decision_ready"))
        and audit_status not in {"blocked", "attention"}
        else "false"
    )
    view_label = _priced_in_view_label(payload)
    return [
        _rule("Market Radar Terminal Dashboard", width, char="="),
        (
            f"Page: {page} | "
            f"View: {view_label} | "
            f"Answer: {_human_label(answer_status)} "
            f"ready={answer_ready} | "
            f"Trade status: {_human_label(readiness.get('status') or 'unknown')} | "
            f"Trade safe: {_text(readiness.get('safe_to_make_investment_decision'))} | "
            f"External calls made: {_text(payload.get('external_calls_made', 0))}"
        ),
        (
            f"DB: {_nested(runtime, 'database', 'name') or 'n/a'} | "
            f"Build: {_nested(runtime, 'build', 'commit') or 'n/a'} | "
            f"Ticker: {controls.get('ticker') or 'all'} | "
            f"Cutoff: {controls.get('available_at') or 'latest'}"
        ),
        NAVIGATION_TEXT,
    ]


def _priced_in_view_label(payload: Mapping[str, object]) -> str:
    queue = _mapping(payload.get("priced_in_queue"))
    filters = _mapping(queue.get("filters"))
    status = str(filters.get("status") or "all").strip().lower()
    usefulness = str(filters.get("usefulness") or "").strip().lower()
    if status in {"", "all"}:
        return "Full scan"
    if status == "actionable" and usefulness == "decision_useful":
        return "Decision-ready filter"
    if status == "actionable":
        return "Mismatches filter"
    return f"{_human_label(status)} filter"


def _tutorial_mission_rows(payload: Mapping[str, object]) -> list[Mapping[str, object]]:
    step_labels = {
        "Question": ("WHY", "Read the mission"),
        "Current answer": ("NOW", "Check the answer"),
        "Scan progress": ("SCAN", "Check coverage"),
        "Trust blocker": ("BLOCK", "Find the blocker"),
        "Useful next": ("NEXT", "Do the next useful thing"),
        "Trust gate": ("GATE", "Check trust gate"),
        "Boundary": ("SAFE", "Respect the boundary"),
    }
    rows: list[Mapping[str, object]] = []
    for label, value in _run_mission_brief_items(payload):
        if label not in step_labels:
            continue
        step, action = step_labels[label]
        text = str(value or "").strip()
        if not text:
            continue
        rows.append(
            {
                "step": step,
                "do": action,
                "result": text,
            }
        )
    return rows


def _tutorial_control_rows() -> list[Mapping[str, object]]:
    return [
        {
            "step": "1",
            "do": "Press 1 or click Insights",
            "result": "See the current insight queue: ticker, signal, why, and action.",
        },
        {
            "step": "2",
            "do": "Press 2 or click Readiness",
            "result": "See exactly what blocks a decision-useful workflow.",
        },
        {
            "step": "3",
            "do": "Press D or click Decision-ready",
            "result": "Show only not-priced-in rows that passed the usefulness gate.",
        },
        {
            "step": "4",
            "do": "Press 4 or click Candidates",
            "result": "Review companies. These are research rows, not trade signals.",
        },
        {
            "step": "5",
            "do": "Press 3 or click Run",
            "result": "Review external-call budget before running anything.",
        },
        {
            "step": "6",
            "do": "Use the bottom command box",
            "result": "Try ticker AAPL, refresh, help, or q. Esc focuses the box.",
        },
    ]


def _tutorial_caption(payload: Mapping[str, object]) -> str:
    prefix = (
        "Read WHY/NOW/NEXT first; then follow the numbered rows. "
        if _tutorial_mission_rows(payload)
        else ""
    )
    return (
        f"{prefix}Safe rule: clicking, filtering, tutorial, and refresh make "
        "0 provider calls."
    )


def _tutorial_lines(payload: Mapping[str, object], width: int) -> list[str]:
    lines: list[str] = []
    mission_items = _run_mission_brief_items(payload)
    if mission_items:
        lines.append(_rule("Mission - why this exists", width))
        lines.extend(_kv_lines(mission_items, width=width))
        lines.append("")
    lines.append(_rule("Tutorial - your first 90 seconds", width))
    lines.extend(
        _table_lines(
            _tutorial_control_rows(),
            [
                ("step", "Step", 6),
                ("do", "Do this", 28),
                ("result", "What happens", 80),
            ],
            width=width,
            limit=8,
        )
    )
    lines.append("")
    lines.extend(
        _kv_lines(
            (
                ("Safe rule", _tutorial_caption(payload)),
                ("Orders", "Real order submission is disabled."),
                ("Exit", "Press q."),
            ),
            width=width,
        )
    )
    return lines


def _priced_in_overview_rows(payload: Mapping[str, object]) -> list[Mapping[str, object]]:
    priced_in_queue = _mapping(payload.get("priced_in_queue"))
    queue_rows = _rows(priced_in_queue.get("rows"))
    offset = int(_number_or_zero(priced_in_queue.get("offset")))
    rows: list[Mapping[str, object]] = []
    for index, candidate in enumerate(queue_rows, start=1):
        ticker = str(candidate.get("ticker") or "").strip().upper()
        if not ticker:
            continue
        usefulness = _mapping(candidate.get("usefulness"))
        priced_status = str(candidate.get("priced_in_status") or "").strip()
        emotion = candidate.get("emotion_score")
        reaction = candidate.get("reaction_score")
        gap = candidate.get("emotion_reaction_gap")
        setup = (
            candidate.get("setup_type")
            or candidate.get("candidate_theme")
            or candidate.get("top_event_type")
            or "candidate"
        )
        why_now = _join_nonempty(
            (
                _priced_in_mismatch_text(emotion, reaction, gap),
                _non_company_evidence_table_summary(candidate),
                _priced_in_reason(candidate),
                _human_label(setup),
                candidate.get("top_catalyst"),
                candidate.get("why_now"),
            ),
            separator="; ",
        )
        row_number = offset + index
        rows.append(
            {
                **dict(candidate),
                "_row_key": f"scan-{row_number}-{ticker}",
                "rank": row_number,
                "ticker": ticker,
                "signal": "Blocked mismatch"
                if bool(candidate.get("blocked"))
                else _priced_in_signal(priced_status, fallback="Candidate"),
                "usefulness": usefulness,
                "usefulness_label": usefulness.get("label")
                or _human_label(usefulness.get("status") or "unknown"),
                "data_coverage": _priced_in_gap_summary(candidate),
                "why_now": why_now or "No priced-in explanation recorded.",
                "next_action": usefulness.get("next_action")
                or candidate.get("priced_in_next_step")
                or candidate.get("next_step")
                or "Open candidate detail and review the evidence.",
                "target_page": f"candidate:{ticker}",
                "status_message": (
                    f"Opened full-scan row {row_number} for {ticker}. "
                    "Review evidence before any action."
                ),
            }
        )
    return rows


def _non_company_evidence_table_summary(row: Mapping[str, object]) -> str:
    evidence = _mapping(row.get("non_company_evidence"))
    if not evidence:
        return ""
    status = str(evidence.get("status") or "").strip()
    summary = str(evidence.get("summary") or "").strip()
    if not summary:
        return ""
    prefix = f"non-company {status}" if status else "non-company"
    return f"{prefix}: {summary}"


def _priced_in_gap_summary(row: Mapping[str, object]) -> str:
    data_sources = row.get("data_sources") or row.get("priced_in_data_sources")
    if not isinstance(data_sources, Mapping):
        return "unknown"
    usefulness = row.get("usefulness")
    if not isinstance(usefulness, Mapping):
        usefulness = {}
    routed = {
        str(item)
        for item in _rows_or_values(usefulness.get("routed_optional_sources"))
        if str(item).strip()
    }
    missing = [
        str(item)
        for item in _rows_or_values(data_sources.get("missing"))
        if str(item).strip() and str(item) not in routed
    ]
    stale = [
        str(item)
        for item in _rows_or_values(data_sources.get("stale"))
        if str(item).strip() and str(item) not in routed
    ]
    parts: list[str] = []
    if missing:
        parts.append(f"missing {', '.join(missing[:3])}")
    if stale:
        parts.append(f"stale {', '.join(stale[:3])}")
    if routed:
        parts.append(f"routed {', '.join(sorted(routed)[:3])}")
    if not parts:
        return "none"
    return "; ".join(parts)


def _market_insight_rows(payload: Mapping[str, object]) -> list[Mapping[str, object]]:
    readiness = _mapping(payload.get("readiness"))
    usefulness = _mapping(readiness.get("market_radar_usefulness"))
    freshness = _mapping(_mapping(readiness.get("discovery_snapshot")).get("freshness"))
    discovery = _mapping(payload.get("discovery_snapshot"))
    scan_yield = _mapping(discovery.get("yield"))
    database = _mapping(_mapping(payload.get("ops_health")).get("database"))
    call_plan = _mapping(payload.get("call_plan"))
    preflight = _mapping(payload.get("priced_in_preflight"))
    priced_in_queue = _mapping(payload.get("priced_in_queue"))
    priced_in_answer = _mapping(payload.get("priced_in_answer"))
    priced_in_audit = _mapping(payload.get("priced_in_audit"))
    source_coverage = _mapping(payload.get("priced_in_source_coverage"))
    can_act = _decision_label(readiness)
    queue_rows = (
        _rows(priced_in_queue.get("rows"))
        if isinstance(priced_in_queue.get("rows"), list | tuple)
        else _candidate_rows(payload)
    )
    scan_total = int(
        _number_or_zero(
            _first_value(
                scan_yield.get("scanned_candidate_states"),
                scan_yield.get("candidate_states"),
                len(queue_rows),
            )
        )
    )
    rows: list[Mapping[str, object]] = []

    if priced_in_answer:
        rows.append(
            {
                "_row_key": "priced-in-answer",
                "scope": "ANSWER",
                "signal": _human_label(priced_in_answer.get("status") or "priced-in answer"),
                "why_now": priced_in_answer.get("answer")
                or priced_in_answer.get("headline")
                or "Current priced-in answer is available.",
                "next_action": priced_in_answer.get("next_action")
                or "Open the priced-in queue.",
                "target_page": "run"
                if priced_in_answer.get("next_command")
                else "overview",
                "status_message": "Opened current priced-in answer context.",
            }
        )
        decision_readiness = _mapping(priced_in_answer.get("decision_readiness"))
        recommended_gap = _mapping(decision_readiness.get("recommended_gap"))
        if decision_readiness:
            rows.append(
                {
                    "_row_key": "decision-readiness",
                    "scope": "READY",
                    "signal": _human_label(
                        decision_readiness.get("status") or "decision readiness"
                    ),
                    "why_now": decision_readiness.get("summary")
                    or "Decision readiness is available.",
                    "next_action": recommended_gap.get("next_action")
                    or "Open Readiness or Ops to clear decision blockers.",
                    "target_page": "ops",
                    "status_message": "Opened Ops. Clear the recommended decision gap first.",
                }
            )

    rows.append(
        _full_scan_coverage_row(
            freshness=freshness,
            database=database,
            scan_yield=scan_yield,
            preflight=preflight,
            candidate_count=scan_total,
            displayed_count=len(queue_rows),
            actionable_count=_priced_in_actionable_count(priced_in_queue),
        )
    )
    instrument_scope = _mapping(priced_in_audit.get("instrument_scope"))
    if not instrument_scope:
        instrument_scope = _mapping(priced_in_queue.get("instrument_scope"))
    if instrument_scope:
        rows.append(_instrument_scope_row(instrument_scope))
    rows.append(
        _source_coverage_row(
            freshness=freshness,
            database=database,
            source_coverage=source_coverage,
            full_scan_evidence=_answer_evidence_completeness_summary(payload),
        )
    )

    for candidate in queue_rows:
        ticker = str(candidate.get("ticker") or "").strip().upper()
        if not ticker:
            continue
        state = _text(candidate.get("state") or candidate.get("decision_status") or "candidate")
        score = candidate.get("score") or candidate.get("final_score")
        priced_status = str(candidate.get("priced_in_status") or "").strip()
        emotion = candidate.get("emotion_score")
        reaction = candidate.get("reaction_score")
        gap = candidate.get("emotion_reaction_gap")
        setup = (
            candidate.get("setup_type")
            or candidate.get("candidate_theme")
            or candidate.get("top_event_type")
            or "candidate"
        )
        priced_reason = _priced_in_reason(candidate)
        risk = candidate.get("risk_or_gap") or candidate.get("hard_blocks") or "Review evidence"
        score_text = f"score {_text(score)}" if score not in (None, "") else ""
        mismatch_text = _priced_in_mismatch_text(emotion, reaction, gap)
        blockers = candidate.get("blockers")
        blocker_text = ""
        if isinstance(blockers, list | tuple) and blockers:
            blocker_text = "blocked: " + ", ".join(str(item) for item in blockers[:3])
        why_now = _join_nonempty(
            (mismatch_text, priced_reason, score_text, _human_label(setup), blocker_text, risk),
            separator="; ",
        )
        rows.append(
            {
                "_row_key": f"candidate-{ticker}",
                "scope": ticker,
                "signal": "Blocked mismatch"
                if bool(candidate.get("blocked"))
                else _priced_in_signal(priced_status, fallback=f"{state} candidate"),
                "why_now": why_now,
                "next_action": candidate.get("priced_in_next_step")
                or candidate.get("next_step")
                or "Open candidate detail and decide watch, ready, or dismiss.",
                "target_page": f"candidate:{ticker}",
                "status_message": (
                    f"Opened insight for {ticker}. Review evidence before any action."
                ),
            }
        )

    for index, alert in enumerate(_rows(_mapping(payload.get("alerts")).get("rows")), start=1):
        alert_id = str(alert.get("id") or "").strip()
        ticker = str(alert.get("ticker") or "ALERT").strip().upper()
        if not alert_id:
            continue
        status = _join_nonempty(
            (alert.get("status"), alert.get("route"), alert.get("priority")),
            separator=" / ",
        )
        rows.append(
            {
                "_row_key": f"alert-{index}",
                "scope": ticker,
                "signal": "Planned alert",
                "why_now": alert.get("summary") or alert.get("title") or status,
                "next_action": "Open alert, then record useful/noisy/acted feedback.",
                "target_page": f"alert:{alert_id}",
                "status_message": f"Opened alert insight for {ticker}.",
            }
        )
        if index >= 3:
            break

    readiness_reason = readiness.get("headline") or usefulness.get("headline")
    if readiness_reason:
        rows.append(
            {
                "_row_key": "readiness",
                "scope": "ALL",
                "signal": can_act,
                "why_now": readiness_reason,
                "next_action": readiness.get("next_action") or "Open Readiness.",
                "target_page": "readiness",
                "status_message": "Opened Readiness. Clear blockers before relying on output.",
            }
        )

    rows.extend(
        [
            {
                "_row_key": "run",
                "scope": "ALL",
                "signal": "Refresh intelligence",
                "why_now": call_plan.get("headline")
                or "Run plan is available for a capped radar cycle.",
                "next_action": "Open Run plan; execute only if the call budget is intentional.",
                "target_page": "run",
                "status_message": "Opened call plan. Type run execute only if intended.",
            },
        ]
    )
    return rows


def _source_coverage_row(
    *,
    freshness: Mapping[str, object],
    database: Mapping[str, object],
    source_coverage: Mapping[str, object],
    full_scan_evidence: str,
) -> Mapping[str, object]:
    freshness_gap = (
        f"bar coverage {freshness.get('active_security_with_as_of_bar_count')}/"
        f"{freshness.get('active_security_count')}; "
        f"latest bar {database.get('latest_daily_bar_date') or 'n/a'}"
    )
    source_gap = _source_coverage_gap_text(source_coverage)
    visible_gap = f"visible page source gaps: {source_gap}" if source_gap else ""
    full_scan = (
        full_scan_evidence.replace("Evidence layers: ", "")
        if full_scan_evidence
        else ""
    )
    why_now = _join_nonempty(
        (freshness_gap, full_scan, visible_gap),
        separator="; ",
    )
    return {
        "_row_key": "ops",
        "scope": "DATA",
        "signal": "Evidence gaps",
        "why_now": why_now,
        "next_action": _source_coverage_next_action(source_coverage),
        "target_page": "ops",
        "status_message": "Opened Ops. Use the full-scan source workflow before trusting output.",
    }


def _full_scan_coverage_row(
    *,
    freshness: Mapping[str, object],
    database: Mapping[str, object],
    scan_yield: Mapping[str, object],
    preflight: Mapping[str, object],
    candidate_count: int,
    displayed_count: int,
    actionable_count: int,
) -> Mapping[str, object]:
    active_count = int(
        _number_or_zero(
            _first_value(
                freshness.get("active_security_count"),
                database.get("active_security_count"),
            )
        )
    )
    requested = int(_number_or_zero(scan_yield.get("requested_securities")))
    scanned = int(_number_or_zero(scan_yield.get("scanned_securities")))
    latest_with_bars = int(
        _number_or_zero(
            _first_value(
                database.get("active_security_with_latest_daily_bar_count"),
                database.get("active_security_with_daily_bar_count"),
            )
        )
    )
    run_with_bars = int(_number_or_zero(freshness.get("active_security_with_as_of_bar_count")))
    denominator = active_count or requested or scanned
    operator_next = _mapping(preflight.get("operator_next_step"))
    first_blocker = _mapping(preflight.get("first_blocker"))
    preflight_next_action = str(
        operator_next.get("action") or preflight.get("next_action") or ""
    )
    if denominator < 500:
        signal = "Universe too small"
        next_action = preflight_next_action
    elif scanned and denominator and scanned < max(1, int(denominator * 0.9)):
        signal = "Partial scan"
        next_action = preflight_next_action or (
            "Open Ops/Run and fix missing bars before trusting the ranked queue."
        )
    else:
        signal = "Full scan coverage"
        next_action = preflight_next_action or (
            "Use next/prev/offset to page the full scan; type export full "
            "for all ranked rows, then work the largest mismatches first."
        )
    blocker_area = str(first_blocker.get("area") or "").strip()
    blocker_status = str(first_blocker.get("status") or "").strip()
    blocker_gaps = int(_number_or_zero(first_blocker.get("source_gap_count")))
    blocker_text = ""
    if blocker_area:
        blocker_text = f"first blocker {blocker_area}"
        if blocker_status:
            blocker_text = f"{blocker_text} {blocker_status}"
        if blocker_gaps:
            blocker_text = f"{blocker_text}; gaps {blocker_gaps}"
    why_now = _join_nonempty(
        (
            f"active {active_count or 'n/a'}; requested {requested or 'n/a'}; "
            f"scanned {scanned or 'n/a'}; ranked {candidate_count}; "
            f"actionable {actionable_count}; showing {displayed_count}; "
            f"latest bars {latest_with_bars or 'n/a'}/{active_count or 'n/a'}; "
            f"run bars {run_with_bars or 'n/a'}/{active_count or 'n/a'}",
            blocker_text,
        ),
        separator="; ",
    )
    return {
        "_row_key": "full-scan-coverage",
        "scope": "UNIVERSE",
        "signal": signal,
        "why_now": why_now,
        "next_action": next_action,
        "target_page": "ops",
        "status_message": (
            "Opened Ops coverage. The full ranked scan stays on Insights; "
            "page it with next/prev/offset."
        ),
    }


def _instrument_scope_row(instrument_scope: Mapping[str, object]) -> Mapping[str, object]:
    sec_scope = _mapping(instrument_scope.get("sec_catalyst_applicability"))
    row_count = int(_number_or_zero(instrument_scope.get("row_count")))
    company_like = int(_number_or_zero(instrument_scope.get("company_like_rows")))
    non_company = int(_number_or_zero(instrument_scope.get("non_company_rows")))
    unknown = int(_number_or_zero(instrument_scope.get("unknown_type_rows")))
    why_now = (
        f"full scan rows {row_count}; operating-company rows {company_like}; "
        f"ETF/fund/wrapper rows {non_company}; unknown type {unknown}. "
        "Non-company rows stay in the scan, but SEC company filings are not "
        "their evidence route."
    )
    return {
        "_row_key": "instrument-scope",
        "scope": "SCOPE",
        "signal": "Instrument routing",
        "why_now": why_now,
        "next_action": sec_scope.get("next_action")
        or "Open Ops to choose the right evidence route for each instrument type.",
        "target_page": "ops",
        "status_message": (
            "Opened Ops. Full scan stays broad; evidence routes differ by instrument."
        ),
    }


def _source_coverage_next_action(source_coverage: Mapping[str, object]) -> str:
    by_source = {
        str(action.get("source") or ""): action
        for action in _rows(source_coverage.get("actions"))
    }
    raw_sources = source_coverage.get("weak_sources")
    weak_sources = [
        str(item)
        for item in (raw_sources if isinstance(raw_sources, list | tuple) else ())
        if str(item).strip()
    ]
    for source in weak_sources:
        action = by_source.get(source)
        if not action or str(action.get("status") or "") in {"ready", "not_applicable"}:
            continue
        next_action = str(action.get("next_action") or "").strip()
        batch_command = str(action.get("batch_plan_command") or "").strip()
        command = str(action.get("command") or "").strip()
        if next_action and batch_command:
            return f"{next_action} Batch plan: {batch_command}"
        if next_action and command:
            return f"{next_action} Command: {command}"
        if next_action:
            return next_action
    for action in _rows(source_coverage.get("actions")):
        if str(action.get("status") or "") not in {"ready", "not_applicable"}:
            next_action = str(action.get("next_action") or "").strip()
            if next_action:
                return next_action
    if not weak_sources:
        return "Open Ops to verify providers and jobs before trusting output."
    first = weak_sources[0]
    if first == "market_bars":
        return "Refresh market bars, then rerun the priced-in queue."
    if first == "catalyst_events":
        return "Enable or refresh catalyst event ingestion before trusting emotion."
    if first == "local_text":
        return "Run local text intelligence for candidate narratives."
    if first == "options":
        return (
            "Use point-in-time options for the scan date; current Schwab options "
            "belong in a current rerun."
        )
    if first == "theme_peer_sector":
        return "Review theme, peer, and sector context before acting."
    if first == "broker_context":
        return "Sync read-only Schwab market context before sizing or trigger review."
    return "Open Ops to fix missing source coverage before trusting output."


def _source_coverage_gap_text(source_coverage: Mapping[str, object]) -> str:
    sources = _mapping(source_coverage.get("sources"))
    raw_sources = source_coverage.get("weak_sources")
    weak_sources = [
        str(item)
        for item in (raw_sources if isinstance(raw_sources, list | tuple) else ())
        if str(item).strip()
    ]
    parts: list[str] = []
    for source in weak_sources[:3]:
        values = _mapping(sources.get(source))
        missing = int(_number_or_zero(values.get("missing")))
        stale = int(_number_or_zero(values.get("stale")))
        row_count = int(_number_or_zero(values.get("row_count")))
        if missing:
            detail = f"{source} missing {missing}/{row_count or missing}"
        elif stale:
            detail = f"{source} stale {stale}/{row_count or stale}"
        else:
            detail = f"{source} coverage {_text(values.get('coverage_pct'))}%"
        raw_samples = values.get("sample_tickers")
        samples = [
            str(ticker)
            for ticker in (raw_samples if isinstance(raw_samples, list | tuple) else ())
            if str(ticker).strip()
        ][:3]
        if samples:
            detail += f" examples {','.join(samples)}"
        parts.append(detail)
    if parts:
        return "; ".join(parts)
    return str(source_coverage.get("summary") or "").strip()


def _priced_in_signal(status: str, *, fallback: str) -> str:
    labels = {
        "bullish_not_priced_in": "Bullish not priced",
        "bearish_not_priced_in": "Bearish not priced",
        "fully_priced": "Fully priced",
        "overextended_hype": "Overextended",
        "conflicted": "Conflict",
        "stale": "Stale bars",
        "blocked": "Blocked",
        "neutral": "No mismatch",
    }
    return labels.get(status, fallback)


def _priced_in_reason(row: Mapping[str, object]) -> str:
    status = str(row.get("priced_in_status") or "").strip().lower()
    if status in {"", "neutral"}:
        return ""
    return str(row.get("priced_in_reason") or "").strip()


def _priced_in_mismatch_text(emotion: object, reaction: object, gap: object) -> str:
    if emotion in (None, "") or reaction in (None, ""):
        return ""
    parts = [f"emotion {_text(emotion)}", f"reaction {_text(reaction)}"]
    if gap not in (None, ""):
        parts.append(f"gap {_text(gap)}")
    return " / ".join(parts)


def _overview_lines(payload: Mapping[str, object], width: int) -> list[str]:
    lines = [_rule(_overview_title(payload), width)]
    audit_summary = _full_scan_audit_summary(payload)
    if audit_summary:
        lines.append(f"Full scan audit: {audit_summary}")
    stock_bar_summary = _stock_market_bar_next_summary(payload)
    full_scan_summary = _answer_full_scan_scope_summary(payload)
    if full_scan_summary:
        lines.append(
            "Full scan coverage: "
            f"{_clip(full_scan_summary.replace('Full-scan coverage: ', ''), max(20, width - 22))}"
        )
    evidence_summary = _answer_evidence_completeness_summary(payload)
    if evidence_summary:
        lines.append(
            "Evidence layers: "
            f"{_clip(evidence_summary.replace('Evidence layers: ', ''), max(20, width - 20))}"
        )
    recommended_unblock = _market_bar_recommended_action_summary(
        {
            "recommended_action": _mapping(
                _mapping(
                    _mapping(payload.get("priced_in_answer")).get(
                        "full_market_trust_gate"
                    )
                ).get("recommended_action")
            )
        }
    )
    if recommended_unblock:
        lines.append(
            "Recommended unblock: "
            f"{_clip(recommended_unblock, max(20, width - 23))}"
        )
    if stock_bar_summary:
        lines.append(
            "Stock bar next: "
            f"{_clip(stock_bar_summary, max(20, width - 17))}"
        )
    answer = _mapping(payload.get("priced_in_answer"))
    trust_gate = _mapping(answer.get("full_market_trust_gate"))
    next_source_plan_summary = _after_current_blocker_summary(
        _mapping(trust_gate.get("after_current_blocker"))
    )
    if next_source_plan_summary:
        lines.append(
            "Next source plan: "
            f"{_clip(next_source_plan_summary, max(20, width - 22))}"
        )
    missing_type_summary = _market_bar_missing_type_summary(payload)
    if missing_type_summary:
        lines.append(
            f"Missing bar types: {_clip(missing_type_summary, max(20, width - 19))}"
        )
    manual_progress_summary = _market_bar_manual_fill_progress_summary(payload)
    if manual_progress_summary:
        lines.append(
            "Manual CSV progress: "
            f"{_clip(manual_progress_summary, max(20, width - 22))}"
        )
    operator_step_summary = _market_bar_operator_step_summary(payload)
    if operator_step_summary:
        lines.append(
            "Market bar next: "
            f"{_clip(operator_step_summary, max(20, width - 18))}"
        )
    saved_file_capture_summary = _market_bar_provider_saved_file_capture_summary(
        payload,
    )
    if saved_file_capture_summary:
        lines.append(
            "Saved file capture: "
            f"{_clip(saved_file_capture_summary, max(20, width - 22))}"
        )
    saved_file_validate_summary = _market_bar_provider_saved_file_validate_summary(
        payload,
    )
    if saved_file_validate_summary:
        lines.append(
            "Saved file check: "
            f"{_clip(saved_file_validate_summary, max(20, width - 20))}"
        )
    saved_file_summary = _market_bar_provider_saved_file_summary(payload)
    if saved_file_summary:
        lines.append(
            "Saved file import: "
            f"{_clip(saved_file_summary, max(20, width - 21))}"
        )
    provider_fill_summary = _market_bar_provider_fill_summary(payload)
    if provider_fill_summary:
        lines.append(
            "Direct provider fill: "
            f"{_clip(provider_fill_summary, max(20, width - 24))}"
        )
    instrument_summary = _full_scan_instrument_scope_summary(payload)
    if instrument_summary:
        lines.append(f"Instrument scope: {instrument_summary}")
    decision_summary = _decision_readiness_summary(payload)
    if decision_summary:
        lines.append(f"Decision readiness: {decision_summary}")
    lines.extend(
        _table_lines(
            _priced_in_overview_rows(payload),
            [
                ("rank", "#", 3),
                ("ticker", "Ticker", 6),
                ("signal", "Signal", 19),
                ("emotion_reaction_gap", "Gap", 5),
                ("data_coverage", "Data gaps", 14),
                ("why_now", "Why now", 27),
                ("next_action", "Next action", 25),
            ],
            width=width,
            limit=50,
        )
    )
    lines.append("")
    lines.append(_overview_caption(payload))
    return lines


def _review_lines(payload: Mapping[str, object], width: int) -> list[str]:
    rows = _priced_in_review_rows(payload)
    answer = _mapping(payload.get("priced_in_answer"))
    readiness = _mapping(payload.get("readiness"))
    lines = [_rule("Decision Review - priced-in answer, not trade approval", width)]
    lines.append(
        "Answer: "
        f"{answer.get('answer') or 'No priced-in answer.'} "
        f"Trade safe? {_decision_label(readiness)}."
    )
    boundary = str(answer.get("investment_boundary") or "").strip()
    if boundary:
        lines.append(f"Boundary: {boundary}")
    lines.append(f"Remaining optional context: {_decision_review_optional_summary(rows)}")
    lines.extend(
        _table_lines(
            rows,
            [
                ("rank", "#", 3),
                ("ticker", "Ticker", 6),
                ("signal", "Signal", 19),
                ("emotion_reaction_gap", "Gap", 6),
                ("optional_gaps", "Optional gaps", 22),
                ("top_evidence", "Top evidence", 30),
                ("next_action", "Next action", 34),
            ],
            width=width,
            limit=50,
        )
    )
    lines.append("")
    lines.append(_decision_review_caption(payload, rows))
    return lines


def _priced_in_review_rows(payload: Mapping[str, object]) -> list[Mapping[str, object]]:
    rows: list[Mapping[str, object]] = []
    for row in _priced_in_overview_rows(payload):
        if not _decision_review_row_is_ready(row):
            continue
        ticker = str(row.get("ticker") or "").strip().upper()
        if not ticker:
            continue
        optional_gaps = _decision_review_optional_gaps(row)
        rows.append(
            {
                **dict(row),
                "_row_key": f"review-{row.get('rank')}-{ticker}",
                "optional_gaps": optional_gaps,
                "top_evidence": _decision_review_top_evidence(row),
                "next_action": (
                    "Open Decision Card; verify optional gaps before trading."
                    if optional_gaps != "none"
                    else "Open Decision Card and verify the thesis."
                ),
                "target_page": f"candidate:{ticker}",
                "status_message": (
                    f"Opened decision-ready priced-in row for {ticker}. "
                    "This is still not trade approval."
                ),
            }
        )
    return rows


def _decision_review_row_is_ready(row: Mapping[str, object]) -> bool:
    usefulness = _mapping(row.get("usefulness"))
    return bool(usefulness.get("decision_ready")) or (
        str(usefulness.get("status") or "").strip() == "decision_useful"
    )


def _decision_review_optional_gaps(row: Mapping[str, object]) -> str:
    usefulness = _mapping(row.get("usefulness"))
    gaps = [
        str(item)
        for item in _rows_or_values(usefulness.get("optional_context_gaps"))
        if str(item).strip()
    ]
    if not gaps:
        data_sources = row.get("data_sources") or row.get("priced_in_data_sources")
        if isinstance(data_sources, Mapping):
            missing_for_decision = {
                str(item)
                for item in _rows_or_values(usefulness.get("missing_for_decision"))
                if str(item).strip()
            }
            gaps = [
                str(item)
                for item in _rows_or_values(data_sources.get("missing"))
                if str(item).strip() and str(item) not in missing_for_decision
            ]
    if not gaps:
        return "none"
    return ", ".join(dict.fromkeys(gaps))


def _decision_review_top_evidence(row: Mapping[str, object]) -> str:
    brief = _mapping(row.get("priced_in_evidence_brief"))
    evidence_rows = _rows(brief.get("evidence"))
    if evidence_rows:
        first = evidence_rows[0]
        title = str(first.get("title") or "").strip()
        source = str(first.get("source") or "").strip()
        if title and source:
            return f"{title} / {source}"
        if title:
            return title
    top_support = _mapping(row.get("top_supporting_evidence"))
    title = str(top_support.get("title") or "").strip()
    if title:
        return title
    return str(row.get("top_catalyst") or row.get("source") or "local evidence")


def _decision_review_optional_summary(rows: Sequence[Mapping[str, object]]) -> str:
    counts: dict[str, int] = {}
    for row in rows:
        for gap in _decision_review_optional_gaps(row).split(","):
            gap = gap.strip()
            if not gap or gap == "none":
                continue
            counts[gap] = counts.get(gap, 0) + 1
    if not counts:
        return "none across visible decision-ready rows"
    return "; ".join(f"{gap} missing on {count}" for gap, count in sorted(counts.items()))


def _decision_review_caption(
    payload: Mapping[str, object],
    rows: Sequence[Mapping[str, object]],
) -> str:
    answer = _mapping(payload.get("priced_in_answer"))
    queue = _mapping(payload.get("priced_in_queue"))
    scan_total = _priced_in_scan_total(queue)
    return (
        f"This page shows {len(rows)} decision-ready priced-in row(s) from "
        f"{scan_total or 'the'} latest scan. Decision-ready means the price/emotion "
        "answer can be reviewed by a human; trade safety remains a separate "
        "readiness gate. Press Enter/click a row to inspect the Decision Card "
        f"context. {answer.get('investment_boundary') or ''} Browsing makes 0 provider calls."
    )


def _full_scan_audit_summary(payload: Mapping[str, object]) -> str:
    audit = _mapping(payload.get("priced_in_audit"))
    if not audit:
        return ""
    scope = _mapping(audit.get("scope"))
    market = _mapping(audit.get("market_bars"))
    repair = _mapping(market.get("repair"))
    stock_scope = _mapping(repair.get("stock_scope"))
    coverage = _mapping(audit.get("source_coverage"))
    active = int(_number_or_zero(scope.get("active_securities")))
    ranked = int(_number_or_zero(scope.get("ranked_rows")))
    with_bars = int(_number_or_zero(market.get("with_as_of_bar")))
    ready_sources = int(_number_or_zero(coverage.get("ready_source_count")))
    source_count = int(_number_or_zero(coverage.get("source_count")))
    next_action = _clip(str(audit.get("next_action") or "").strip(), 120)
    parts = [
        f"{audit.get('status')}",
        f"ranked {ranked}/{active}",
        f"bars {with_bars}/{active}",
        f"sources {ready_sources}/{source_count}",
    ]
    if stock_scope:
        stock_with = int(_number_or_zero(stock_scope.get("stock_like_with_as_of_bar")))
        stock_active = int(_number_or_zero(stock_scope.get("stock_like_active")))
        stock_missing = int(_number_or_zero(stock_scope.get("stock_like_missing_as_of_bar")))
        parts.append(f"stock bars {stock_with}/{stock_active} missing {stock_missing}")
    if next_action:
        parts.append(f"next {next_action}")
    return "; ".join(parts)


def _stock_market_bar_next_summary(payload: Mapping[str, object]) -> str:
    audit = _mapping(payload.get("priced_in_audit"))
    market = _mapping(audit.get("market_bars"))
    repair = _mapping(market.get("repair"))
    stock_scope = _mapping(repair.get("stock_scope"))
    if not stock_scope:
        return ""
    missing = int(_number_or_zero(stock_scope.get("stock_like_missing_as_of_bar")))
    if missing <= 0:
        return ""
    active = int(_number_or_zero(stock_scope.get("stock_like_active")))
    with_bar = int(_number_or_zero(stock_scope.get("stock_like_with_as_of_bar")))
    operator_step = _mapping(stock_scope.get("operator_step"))
    next_action = str(
        operator_step.get("action") or stock_scope.get("next_action") or ""
    ).strip()
    command = str(
        operator_step.get("command")
        or stock_scope.get("manual_template_command")
        or ""
    ).strip()
    parts = [
        f"{with_bar}/{active} stock-like rows have scan-date bars",
        f"{missing} missing",
    ]
    if next_action:
        parts.append(f"next {next_action}")
    if command:
        parts.append(f"command {command}")
    return "; ".join(parts)


def _market_bar_provider_fill_summary(payload: Mapping[str, object]) -> str:
    provider_plan = _market_bar_provider_fill_plan(payload)
    if not provider_plan:
        return ""
    command = str(provider_plan.get("provider_call_command") or "").strip()
    calls = int(_number_or_zero(provider_plan.get("execute_external_call_count")))
    if not command or calls <= 0:
        return ""
    status = str(provider_plan.get("status") or "unknown").strip()
    warning = str(provider_plan.get("provider_health_warning") or "").strip()
    saved_command = str(
        provider_plan.get("provider_saved_file_capture_command") or ""
    ).strip()
    parts = [status, f"{calls} external call(s)", "explicit approval required"]
    if saved_command:
        parts.append("diagnostic direct ingest; prefer saved file capture")
    if warning:
        parts.append(f"warning {warning}")
    parts.append(f"command {command}")
    return "; ".join(parts)


def _saved_file_request_field(
    source: Mapping[str, object],
    body_key: str,
    field: str,
    label: str,
):
    body = _mapping(source.get(body_key))
    if not body or field not in body:
        return ""
    value = body.get(field)
    if isinstance(value, bool):
        value_text = str(value).lower()
    else:
        value_text = str(value or "").strip()
    if not value_text:
        return ""
    prefix = f"{label} " if label else ""
    return f"{prefix}{field}={value_text}"


def _saved_file_request_boundary(source: Mapping[str, object], fields, label: str):
    pieces = [
        piece
        for body_key, field, field_label in fields
        if (
            piece := _saved_file_request_field(source, body_key, field, field_label)
        )
    ]
    if not pieces:
        return ""
    return f"; {label} " + "; ".join(pieces)


def _market_bar_provider_saved_file_summary(payload: Mapping[str, object]) -> str:
    provider_plan = _market_bar_provider_fill_plan(payload)
    if not provider_plan:
        return ""
    command = str(
        provider_plan.get("provider_saved_file_import_command") or ""
    ).strip()
    if not command:
        return ""
    calls = int(_number_or_zero(provider_plan.get("provider_saved_file_external_call_count")))
    status = str(provider_plan.get("provider_saved_file_status") or "unknown").strip()
    exists_value = provider_plan.get("provider_saved_file_exists")
    boundary = _saved_file_request_boundary(
        provider_plan,
        (
            ("provider_saved_file_import_preview_request_body", "execute", "preview"),
            ("provider_saved_file_import_request_body", "execute", "import"),
        ),
        "request bodies",
    )
    if status == "missing" or (exists_value is False and status != "available"):
        next_action = str(
            provider_plan.get("provider_saved_file_next_action") or ""
        ).strip()
        prefix = "missing saved file"
        next_suffix = f"; next {next_action}" if next_action else ""
        return (
            f"{prefix}; {calls} external call(s){boundary}"
            f"{next_suffix}; command {command}"
        )
    return f"{status}; {calls} external call(s){boundary}; command {command}"


def _market_bar_provider_saved_file_capture_summary(
    payload: Mapping[str, object],
) -> str:
    provider_plan = _market_bar_provider_fill_plan(payload)
    if not provider_plan:
        return ""
    command = str(
        provider_plan.get("provider_saved_file_capture_command") or ""
    ).strip()
    if not command:
        return ""
    packet = _mapping(provider_plan.get("provider_saved_file_capture_approval_packet"))
    if packet:
        status = str(packet.get("status") or "unknown")
        missing = int(_number_or_zero(packet.get("missing_as_of_bar_count")))
        calls = int(_number_or_zero(packet.get("external_calls_if_approved")))
        db_writes = int(_number_or_zero(packet.get("db_writes_during_capture")))
        confirm = str(packet.get("tui_confirm_command") or "n/a")
        question = str(packet.get("question") or "").strip()
        return (
            f"{status}; {missing} bars targeted; {calls} external call(s) if "
            f"approved; {db_writes} db writes during capture; type `{confirm}`. "
            f"{question}"
        )
    calls = int(
        _number_or_zero(
            provider_plan.get("provider_saved_file_capture_external_call_count"),
        ),
    )
    boundary = _saved_file_request_boundary(
        provider_plan,
        (
            (
                "provider_saved_file_capture_request_body",
                "confirm_external_call",
                "safe",
            ),
            (
                "provider_saved_file_capture_confirm_request_body",
                "confirm_external_call",
                "confirm",
            ),
        ),
        "request bodies",
    )
    return (
        f"{calls} external call(s); explicit approval required"
        f"{boundary}; command {command}"
    )


def _market_bar_provider_saved_file_validate_summary(
    payload: Mapping[str, object],
) -> str:
    provider_plan = _market_bar_provider_fill_plan(payload)
    if not provider_plan:
        return ""
    command = str(
        provider_plan.get("provider_saved_file_validate_command") or ""
    ).strip()
    if not command:
        return ""
    calls = int(_number_or_zero(provider_plan.get("provider_saved_file_external_call_count")))
    status = str(provider_plan.get("provider_saved_file_status") or "unknown").strip()
    exists_value = provider_plan.get("provider_saved_file_exists")
    boundary = _saved_file_request_boundary(
        provider_plan,
        (("provider_saved_file_validate_request_body", "fixture_path", "validate"),),
        "request body",
    )
    if status == "missing" or (exists_value is False and status != "available"):
        next_action = str(
            provider_plan.get("provider_saved_file_next_action") or ""
        ).strip()
        prefix = "missing saved file"
        next_suffix = f"; next {next_action}" if next_action else ""
        return (
            f"{prefix}; {calls} external call(s){boundary}"
            f"{next_suffix}; command {command}"
        )
    return f"{status}; {calls} external call(s){boundary}; command {command}"


def _full_scan_instrument_scope_summary(payload: Mapping[str, object]) -> str:
    audit = _mapping(payload.get("priced_in_audit"))
    queue = _mapping(payload.get("priced_in_queue"))
    instrument = _mapping(audit.get("instrument_scope"))
    if not instrument:
        instrument = _mapping(queue.get("instrument_scope"))
    if not instrument:
        return ""
    row_count = int(_number_or_zero(instrument.get("row_count")))
    company_like = int(_number_or_zero(instrument.get("company_like_rows")))
    non_company = int(_number_or_zero(instrument.get("non_company_rows")))
    unknown = int(_number_or_zero(instrument.get("unknown_type_rows")))
    sec_scope = _mapping(instrument.get("sec_catalyst_applicability"))
    next_action = _clip(str(sec_scope.get("next_action") or "").strip(), 90)
    parts = [
        f"rows {row_count}",
        f"companies {company_like}",
        f"fund/wrapper {non_company}",
        f"unknown {unknown}",
    ]
    if next_action:
        parts.append(f"next {next_action}")
    return "; ".join(parts)


def _market_bar_missing_type_summary(payload: Mapping[str, object]) -> str:
    audit = _mapping(payload.get("priced_in_audit"))
    market = _mapping(audit.get("market_bars"))
    repair = _mapping(market.get("repair"))
    diagnostic = _mapping(repair.get("diagnostic"))
    type_counts = _mapping(diagnostic.get("type_counts"))
    if not type_counts:
        return ""
    pieces = [
        f"{key}:{int(_number_or_zero(value))}"
        for key, value in sorted(type_counts.items(), key=lambda item: str(item[0]))
        if int(_number_or_zero(value)) > 0
    ]
    if not pieces:
        return ""
    missing = int(
        _number_or_zero(
            _first_value(
                diagnostic.get("missing_count"),
                market.get("missing_as_of_bar"),
            )
        )
    )
    company_like = int(_number_or_zero(diagnostic.get("company_like_missing_count")))
    fund_like = int(_number_or_zero(diagnostic.get("fund_like_missing_count")))
    wrappers = int(_number_or_zero(diagnostic.get("wrapper_missing_count")))
    unknown = int(_number_or_zero(diagnostic.get("unknown_missing_count")))
    route = (
        f"company-like {company_like}; fund-like {fund_like}; "
        f"wrappers {wrappers}; unknown {unknown}"
    )
    return f"{missing} missing scan-date bars; types {', '.join(pieces)}; {route}"


def _market_bar_missing_count(payload: Mapping[str, object]) -> int:
    audit = _mapping(payload.get("priced_in_audit"))
    market = _mapping(audit.get("market_bars"))
    repair = _mapping(market.get("repair"))
    diagnostic = _mapping(repair.get("diagnostic"))
    return int(
        _number_or_zero(
            _first_value(
                diagnostic.get("missing_count"),
                market.get("missing_as_of_bar"),
                repair.get("missing_as_of_bar"),
            )
        )
    )


def _market_bar_manual_fill_progress_summary(payload: Mapping[str, object]) -> str:
    audit = _mapping(payload.get("priced_in_audit"))
    market = _mapping(audit.get("market_bars"))
    repair = _mapping(market.get("repair"))
    preview = _mapping(repair.get("local_template_preview"))
    progress = _mapping(repair.get("local_template_fill_progress"))
    if not progress:
        progress = _mapping(preview.get("fill_progress"))
    if not progress:
        return ""

    complete = int(_number_or_zero(progress.get("complete_rows")))
    partial = int(_number_or_zero(progress.get("partial_rows")))
    empty = int(_number_or_zero(progress.get("empty_rows")))
    filled = int(_number_or_zero(progress.get("filled_rows")))
    row_count = int(
        _number_or_zero(
            _first_value(preview.get("row_count"), repair.get("template_row_count"))
        )
    )
    total = row_count if row_count > 0 else complete + partial + empty
    if total <= 0:
        return ""

    status = str(preview.get("status") or "not_previewed").strip()
    path = str(
        repair.get("local_template_path") or preview.get("daily_bars_path") or ""
    ).strip()
    path_text = f"; file {path}" if path else ""
    return (
        f"{complete}/{total} complete; {partial} partial; {empty} empty; "
        f"{filled} touched; preview {status}{path_text}"
    )


def _market_bar_operator_step_summary(payload: Mapping[str, object]) -> str:
    audit = _mapping(payload.get("priced_in_audit"))
    market = _mapping(audit.get("market_bars"))
    repair = _mapping(market.get("repair"))
    step = _mapping(repair.get("operator_step"))
    if not step:
        return ""
    action = str(step.get("action") or "").strip()
    if not action:
        return ""
    command = str(step.get("command") or "").strip()
    dashboard_preview = str(
        repair.get("dashboard_manual_import_preview_command") or ""
    ).strip()
    dashboard_execute = str(
        repair.get("dashboard_manual_import_execute_command") or ""
    ).strip()
    after_manual = str(step.get("after_manual_command") or "").strip()
    external_calls = int(_number_or_zero(step.get("external_calls_made")))
    manual = bool(step.get("manual_step"))
    command_text = command or "manual edit"
    if manual and dashboard_preview:
        command_text = dashboard_preview
        if dashboard_execute:
            command_text = (
                f"{command_text}; execute after preview with {dashboard_execute}"
            )
        return f"{action} Command: {command_text}. Calls: {external_calls}."
    if manual and after_manual:
        command_text = f"{command_text}; then {after_manual}"
    return f"{action} Command: {command_text}. Calls: {external_calls}."


def _decision_readiness_summary(payload: Mapping[str, object]) -> str:
    answer = _mapping(payload.get("priced_in_answer"))
    readiness = _mapping(answer.get("decision_readiness"))
    if not readiness:
        return ""
    summary = str(readiness.get("summary") or "").strip()
    recommended = _mapping(readiness.get("recommended_gap"))
    command = str(recommended.get("command") or "").strip()
    if command:
        return f"{summary} Command: {command}"
    return summary


def _overview_source_workflow_hint(payload: Mapping[str, object]) -> str:
    full_scan_summary = _answer_full_scan_scope_summary(payload)
    if full_scan_summary:
        return full_scan_summary

    preflight = _mapping(payload.get("priced_in_preflight"))
    evidence_plan = _mapping(preflight.get("evidence_plan"))
    evidence_steps = _rows(evidence_plan.get("steps"))
    first_evidence_step = evidence_steps[0] if evidence_steps else {}
    if (
        str(first_evidence_step.get("area") or "") == "market_bars"
        and str(first_evidence_step.get("status") or "") == "blocked"
    ):
        why = str(first_evidence_step.get("why") or "").strip()
        command = str(first_evidence_step.get("command") or "").strip()
        command_text = f" Command: {command}" if command else ""
        why_text = f" {why}" if why else ""
        return f"Fresh full scan blocked by market bars.{why_text}{command_text}"

    workflow = _mapping(payload.get("priced_in_source_workflow"))
    steps = _rows(workflow.get("steps"))
    source_coverage = _mapping(payload.get("priced_in_source_coverage"))
    action_by_source = {
        str(action.get("source") or "").strip(): action
        for action in _rows(source_coverage.get("actions"))
        if str(action.get("source") or "").strip()
    }
    coverage_step = steps[0] if steps else {}
    coverage_source = str(coverage_step.get("source") or "").strip()
    coverage_gap_count = int(
        _number_or_zero(
            _source_action_gap_count(_mapping(action_by_source.get(coverage_source)))
        )
    )
    coverage_text = (
        f"{coverage_source} ({coverage_gap_count} full-scan gap row(s))"
        if coverage_source and coverage_gap_count
        else coverage_source
    )
    coverage = str(
        workflow.get("coverage_first_action") or workflow.get("next_action") or ""
    ).strip()
    decision_step = next(
        (
            step
            for step in steps
            if int(_number_or_zero(step.get("decision_useful_gap_rows"))) > 0
        ),
        {},
    )
    decision_source = str(decision_step.get("source") or "").strip()
    decision_rows = int(_number_or_zero(decision_step.get("decision_useful_gap_rows")))
    if coverage_source and decision_source:
        decision_text = (
            f"{decision_source} ({decision_rows} decision-ready row(s))"
            if decision_rows
            else decision_source
        )
        return (
            f"Full-scan coverage: {coverage_text}. "
            f"Shortlist context: {decision_text}."
        )
    if coverage_source:
        return f"Full-scan coverage: {coverage_text}."
    if decision_source:
        return f"Shortlist context: {decision_source}."
    if coverage:
        return f"Full-scan coverage: {_clip(coverage, 140)}"
    return "Open Ops or run batch all to inspect source gaps."


def _answer_full_scan_scope_summary(payload: Mapping[str, object]) -> str:
    answer = _mapping(payload.get("priced_in_answer"))
    full_scan = _mapping(answer.get("full_scan"))
    active = int(_number_or_zero(full_scan.get("active_securities")))
    scanned = int(_number_or_zero(full_scan.get("scanned_rows")))
    unscanned = int(_number_or_zero(full_scan.get("unscanned_rows")))
    if active <= 0 or unscanned <= 0:
        return ""
    missing = _market_bar_missing_count(payload)
    excluded = int(_number_or_zero(full_scan.get("scan_excluded_rows")))
    unscanned_blockers = int(
        _number_or_zero(full_scan.get("unscanned_blocker_rows"))
    )
    if unscanned_blockers <= 0 and unscanned:
        unscanned_blockers = max(0, unscanned - excluded)
    scope = str(full_scan.get("instrument_filter") or "full").strip()
    scope_label = {
        "all": "all-instrument",
        "stocks_only": "stock-like",
    }.get(scope, scope)
    suffixes: list[str] = []
    if scope == "stocks_only":
        audit = _mapping(payload.get("priced_in_audit"))
        market = _mapping(audit.get("market_bars"))
        repair = _mapping(market.get("repair"))
        stock_scope = _mapping(repair.get("stock_scope"))
        stock_missing = int(
            _number_or_zero(stock_scope.get("stock_like_missing_as_of_bar"))
        )
        if stock_missing <= 0:
            stock_missing = unscanned_blockers or unscanned
        if stock_missing:
            suffixes.append(
                f"{stock_missing} missing stock-like scan-date market bar(s)"
            )
            if missing and missing != stock_missing:
                suffixes.append(f"{missing} all-instrument missing")
    elif missing:
        suffixes.append(f"{missing} missing scan-date market bar(s)")
    if excluded:
        tickers = ", ".join(
            str(ticker).strip().upper()
            for ticker in _rows_or_values(full_scan.get("scan_excluded_tickers"))
            if str(ticker).strip()
        )
        label = f"{excluded} benchmark reference row(s) intentionally excluded"
        suffixes.append(f"{label}: {tickers}" if tickers else label)
    blocker = "; " + "; ".join(suffixes) if suffixes else ""
    return (
        f"Full-scan coverage: {scanned}/{active} active {scope_label} row(s) scanned; "
        f"{unscanned} unscanned{blocker}."
    )


def _answer_evidence_completeness_summary(payload: Mapping[str, object]) -> str:
    answer = _mapping(payload.get("priced_in_answer"))
    evidence = _mapping(answer.get("evidence_completeness"))
    if not evidence:
        return ""
    summary = str(evidence.get("summary") or "").strip()
    if summary:
        return f"Evidence layers: {summary}"
    ready = int(_number_or_zero(evidence.get("ready_source_count")))
    total = int(_number_or_zero(evidence.get("total_source_count")))
    if total <= 0:
        return ""
    first_gap = str(evidence.get("first_gap_source") or "").strip()
    first_gap_count = int(_number_or_zero(evidence.get("first_gap_count")))
    suffix = (
        f"; first gap {first_gap}:{first_gap_count}"
        if first_gap and first_gap_count
        else ""
    )
    return f"Evidence layers: {ready}/{total} complete{suffix}."


def _overview_title(payload: Mapping[str, object]) -> str:
    queue = _mapping(payload.get("priced_in_queue"))
    total = int(_number_or_zero(queue.get("total_count")))
    returned = int(_number_or_zero(queue.get("returned_count") or queue.get("count")))
    offset = int(_number_or_zero(queue.get("offset")))
    start = offset + 1 if returned else 0
    end = offset + returned
    scan_total = _priced_in_scan_total(queue)
    scan_status = str(queue.get("status") or "").strip()
    status_filter = _priced_in_status_filter(queue)
    source_gap = _source_gap_filter_summary(queue)
    decision_gap = _decision_gap_filter_summary(queue)
    if status_filter == "actionable":
        usefulness = _usefulness_counts_summary(queue)
        suffix_parts = [part for part in (usefulness, source_gap, decision_gap) if part]
        suffix = f"; {'; '.join(suffix_parts)}" if suffix_parts else ""
        if _is_decision_ready_filter(queue):
            return (
                "Decision-ready not-priced-in rows - showing rows "
                f"{start}-{end} of {total}; scan {scan_total}{suffix}"
            )
        return (
            f"Mismatches from full scan - showing rows {start}-{end} of {total}; "
            f"scan {scan_total}{suffix}"
        )
    if total:
        usefulness = _usefulness_counts_summary(queue)
        suffix_parts = [part for part in (usefulness, source_gap, decision_gap) if part]
        suffix = f"; {'; '.join(suffix_parts)}" if suffix_parts else ""
        scope = _priced_in_scope_title_suffix(payload, queue)
        scope_text = f"; {scope}" if scope else ""
        if scan_status == "selected_universe":
            return (
                f"Selected-universe priced-in queue - showing rows "
                f"{start}-{end} of {total}{suffix}"
            )
        if scan_status == "previous_scan":
            return (
                f"Previous full-market priced-in scan - showing rows "
                f"{start}-{end} of {total}{suffix}"
            )
        return (
            f"Visible priced-in review page - rows {start}-{end} of {total}"
            f"{scope_text}{suffix}"
        )
    return "Visible priced-in review page - select a row to inspect evidence"


def _priced_in_scope_title_suffix(
    payload: Mapping[str, object],
    queue: Mapping[str, object],
) -> str:
    answer = _mapping(payload.get("priced_in_answer"))
    full_scan = _mapping(answer.get("full_scan"))
    active = int(_number_or_zero(full_scan.get("active_securities")))
    scanned = int(_number_or_zero(full_scan.get("scanned_rows")))
    unscanned = int(_number_or_zero(full_scan.get("unscanned_rows")))
    if active > 0 and scanned > 0:
        instrument_filter = str(full_scan.get("instrument_filter") or "").strip()
        label = "stock-like scan" if instrument_filter == "stocks_only" else "active scan"
        suffix = f"{label} {scanned}/{active}"
        if unscanned > 0:
            suffix = f"{suffix}, {unscanned} unscanned"
        return suffix
    scan_total = _priced_in_scan_total(queue)
    if scan_total:
        return f"scan rows {scan_total}"
    return ""


def _overview_caption(payload: Mapping[str, object]) -> str:
    queue = _mapping(payload.get("priced_in_queue"))
    total = int(_number_or_zero(queue.get("total_count")))
    returned = int(_number_or_zero(queue.get("returned_count") or queue.get("count")))
    offset = int(_number_or_zero(queue.get("offset")))
    start = offset + 1 if returned else 0
    end = offset + returned
    scan_total = _priced_in_scan_total(queue)
    scan_status = str(queue.get("status") or "").strip()
    scan_label = _priced_in_scan_row_label(queue)
    status_filter = _priced_in_status_filter(queue)
    source_gap = _source_gap_filter_summary(queue)
    source_gap_text = f" Active source gap filter: {source_gap}." if source_gap else ""
    source_hint = _overview_source_workflow_hint(payload)
    source_hint_text = f" Next data step: {source_hint} " if source_hint else ""
    if status_filter == "actionable":
        usefulness = _usefulness_counts_summary(queue)
        usefulness_text = f" Usefulness mix: {usefulness}." if usefulness else ""
        decision_gap = _decision_gap_filter_summary(queue)
        decision_gap_text = f" Active decision gap filter: {decision_gap}." if decision_gap else ""
        if total:
            if _is_decision_ready_filter(queue):
                return (
                    f"This page shows rows {start}-{end}: {returned} decision-ready "
                    "not-priced-in row(s) from "
                    f"{scan_total or 'the'} {scan_label}. "
                    "These are the actionable answers; type full to inspect the "
                    "whole ranked universe or mismatches for blocked/research rows."
                    f"{usefulness_text}{source_gap_text}{decision_gap_text}"
                    f"{source_hint_text} "
                    "Browsing makes 0 provider calls."
                )
            return (
                f"This page shows rows {start}-{end}: {returned} bullish/bearish "
                "not-priced-in mismatch "
                f"card(s) from {scan_total or 'the'} {scan_label}. "
                "Press M or click SCAN -> Full Scan to inspect neutral, blocked, "
                "stale, and fully-priced rows."
                f"{usefulness_text}{source_gap_text}{decision_gap_text}"
                f"{source_hint_text} "
                "Browsing makes 0 provider calls."
            )
        return (
            f"No actionable not-priced-in mismatch is currently ranked from "
            f"{scan_total or 'the'} {scan_label}. Press M or click "
            "SCAN -> Full Scan to inspect neutral, blocked, stale, and fully-priced rows. "
            f"{source_hint_text}"
            "Browsing makes 0 provider calls."
        )
    if total and returned < total:
        usefulness = _usefulness_counts_summary(queue)
        usefulness_text = f" Usefulness mix: {usefulness}." if usefulness else ""
        decision_gap = _decision_gap_filter_summary(queue)
        decision_gap_text = f" Active decision gap filter: {decision_gap}." if decision_gap else ""
        if scan_status == "selected_universe":
            latest_run = _mapping(queue.get("latest_run"))
            universe = str(latest_run.get("universe") or "selected").strip()
            return (
                f"This page shows rows {start}-{end}: {returned} visible rows from "
                f"{total} latest-scan rows in universe={universe}. "
                "That is a selected universe, not the whole active market. "
                "Run the radar without --universe to scan all active securities. "
                f"{usefulness_text}{source_gap_text}{decision_gap_text} "
                "Browsing makes 0 provider calls."
            )
        return (
            "This table is one review page, not the full scan universe. "
            f"It shows rows {start}-{end}: {returned} visible rows from "
            f"{total} {scan_label}. "
            "The coverage lines above tell you the real active-market scan scope. "
            "Press M or click SCAN -> Mismatches to return to the smaller action queue. "
            "Use priced-in-queue --status all --limit/--offset or the API offset "
            "parameter to page deeper; use priced-in-queue --full-scan --all --json "
            "for the full export. In the TUI type next, prev, offset <row>, "
            f"or limit <rows>.{usefulness_text}{source_gap_text}{decision_gap_text} "
            f"{source_hint_text}"
            "Browsing makes 0 provider calls."
        )
    return (
        "The table is the current priced-in review page, not the full scan "
        "universe or a separate watchlist. Enter opens the relevant evidence page."
        f"{source_gap_text}{source_hint_text} Browsing makes 0 provider calls."
    )


def _priced_in_scan_total(queue: Mapping[str, object]) -> int:
    scan = _mapping(queue.get("scan"))
    return int(
        _number_or_zero(
            _first_value(
                scan.get("scanned_candidate_states"),
                scan.get("candidate_states"),
                scan.get("scanned_securities"),
            )
        )
    )


def _priced_in_scan_row_label(queue: Mapping[str, object]) -> str:
    scan_selection = _mapping(queue.get("scan_selection"))
    if str(scan_selection.get("mode") or "") == "previous_useful_scan":
        selected_as_of = str(scan_selection.get("selected_candidate_as_of") or "").strip()
        if selected_as_of:
            return f"row(s) in the previous scan dated {selected_as_of}"
        return "previous-scan row(s)"
    return "latest-scan row(s)"


def _priced_in_status_filter(queue: Mapping[str, object]) -> str:
    return _normalize_priced_in_status(_mapping(queue.get("filters")).get("status"))


def _is_decision_ready_filter(queue: Mapping[str, object]) -> bool:
    usefulness = str(_mapping(queue.get("filters")).get("usefulness") or "")
    return (
        _priced_in_status_filter(queue) == "actionable"
        and usefulness == "decision_useful"
    )


def _source_gap_filter_summary(queue: Mapping[str, object]) -> str:
    raw_sources = _mapping(queue.get("filters")).get("source_gap")
    sources = raw_sources if isinstance(raw_sources, list | tuple) else ()
    normalized = [str(source) for source in sources if str(source).strip()]
    if not normalized:
        return ""
    return f"source gaps {', '.join(normalized)}"


def _priced_in_actionable_count(queue: Mapping[str, object]) -> int:
    counts = _mapping(queue.get("status_counts"))
    total = int(_number_or_zero(counts.get("bullish_not_priced_in"))) + int(
        _number_or_zero(counts.get("bearish_not_priced_in"))
    )
    if total:
        return total
    if _priced_in_status_filter(queue) == "actionable":
        return int(_number_or_zero(queue.get("total_count")))
    return 0


def _usefulness_counts_summary(queue: Mapping[str, object]) -> str:
    counts = _mapping(queue.get("usefulness_counts"))
    labels = (
        ("decision_useful", "decision"),
        ("research_useful", "research"),
        ("blocked", "blocked"),
        ("not_useful", "not useful"),
        ("monitor_only", "monitor"),
    )
    parts = [
        f"{label} {int(_number_or_zero(counts.get(key)))}"
        for key, label in labels
        if int(_number_or_zero(counts.get(key))) > 0
    ]
    return " / ".join(parts)


def _decision_gap_filter_summary(queue: Mapping[str, object]) -> str:
    raw_gaps = _mapping(queue.get("filters")).get("decision_gap")
    gaps = raw_gaps if isinstance(raw_gaps, list | tuple) else ()
    normalized = [str(gap) for gap in gaps if str(gap).strip()]
    if not normalized:
        return ""
    return f"decision gaps {', '.join(normalized)}"


def _readiness_lines(payload: Mapping[str, object], width: int) -> list[str]:
    readiness = _mapping(payload.get("readiness"))
    queue = _mapping(payload.get("operator_work_queue"))
    lines = [_rule("Readiness And Work Queue", width)]
    lines.extend(
        _kv_lines(
            (
                ("Status", readiness.get("status")),
                ("Decision mode", readiness.get("decision_mode")),
                ("Headline", readiness.get("headline")),
                ("Next action", readiness.get("next_action")),
                ("Evidence", readiness.get("evidence")),
                ("Queue", f"{queue.get('status')}; {queue.get('headline')}"),
            ),
            width=width,
        )
    )
    lines.append("")
    lines.extend(
        _table_lines(
            _rows(readiness.get("readiness_checklist")),
            [
                ("area", "Area", 18),
                ("status", "Status", 12),
                ("finding", "Finding", 38),
                ("next_action", "Next Action", 44),
            ],
            width=width,
            limit=12,
        )
    )
    lines.append("")
    lines.extend(
        _table_lines(
            _rows(queue.get("rows")),
            [
                ("priority", "Priority", 14),
                ("area", "Area", 18),
                ("item", "Item", 42),
                ("next_action", "Action", 42),
            ],
            width=width,
            limit=10,
        )
    )
    return lines


def _run_lines(payload: Mapping[str, object], width: int) -> list[str]:
    latest = _mapping(payload.get("latest_run"))
    call_plan = _mapping(payload.get("call_plan"))
    activation = _mapping(payload.get("live_activation"))
    cooldown = _mapping(payload.get("radar_run_cooldown"))
    audit = _mapping(payload.get("priced_in_audit"))
    audit_sources = _rows(audit.get("sources"))
    evidence_plan = _mapping(_mapping(payload.get("priced_in_preflight")).get("evidence_plan"))
    full_scan_evidence = _answer_evidence_completeness_summary(payload)
    mission_items = _run_mission_brief_items(payload)
    lines: list[str] = []
    if mission_items:
        lines.append(_rule("Mission Brief", width))
        lines.extend(_kv_lines(mission_items, width=width))
        lines.append("")
    lines.append(_rule("Radar Run And Call Plan", width))
    lines.extend(
        _kv_lines(
            (
                ("Latest run", latest.get("status") or "unknown"),
                ("Required path", f"{latest.get('required_completed_count')}/"
                f"{latest.get('required_step_count')}"),
                ("Run as-of", latest.get("as_of") or "n/a"),
                ("Activation", f"{activation.get('status')}; {activation.get('headline')}"),
                ("Cooldown", f"{cooldown.get('status')}; {cooldown.get('detail')}"),
                ("Call plan", f"{call_plan.get('status')}; {call_plan.get('headline')}"),
                ("Next", call_plan.get("next_action")),
                ("Max external calls", call_plan.get("max_external_call_count")),
            ),
            width=width,
        )
    )
    lines.append("")
    lines.extend(
        _table_lines(
            _rows(call_plan.get("rows")),
            [
                ("layer", "Layer", 18),
                ("provider", "Provider", 12),
                ("status", "Status", 14),
                ("external_call_count_max", "Max", 6),
                ("next_action", "Next Action", 58),
            ],
            width=width,
            limit=12,
        )
    )
    if audit_sources:
        lines.append("")
        lines.append(_rule("Priced-in Evidence Plan", width))
        blocker = _run_first_audit_source_blocker(audit_sources)
        coverage = _mapping(audit.get("source_coverage"))
        evidence_items: list[tuple[str, object]] = [
            (
                "Evidence status",
                f"{audit.get('status')}; {audit.get('answer')}",
            ),
            (
                "Next evidence step",
                blocker.get("next_action") if blocker else audit.get("next_action"),
            ),
            ("Full-scan evidence", full_scan_evidence),
            ("Visible-page source coverage", coverage.get("summary")),
        ]
        blocker_hint = _run_audit_source_blocker_hint(blocker, payload)
        if blocker_hint:
            evidence_items.append(("Inspect source blocker", blocker_hint))
        manual_hint = _market_bar_manual_action_summary(payload)
        if manual_hint:
            evidence_items.append(("Manual CSV action", manual_hint))
        saved_capture_hint = _market_bar_provider_saved_file_capture_summary(payload)
        if saved_capture_hint:
            evidence_items.append(("Saved file capture", saved_capture_hint))
        saved_validate_hint = _market_bar_provider_saved_file_validate_summary(payload)
        if saved_validate_hint:
            evidence_items.append(("Saved file check", saved_validate_hint))
        saved_import_hint = _market_bar_provider_saved_file_summary(payload)
        if saved_import_hint:
            evidence_items.append(("Saved file import", saved_import_hint))
        provider_hint = _run_audit_provider_fill_hint(blocker)
        if provider_hint:
            evidence_items.append(("Direct provider fill", provider_hint))
        lines.extend(_kv_lines(evidence_items, width=width))
        lines.append("")
        lines.extend(
            _table_lines(
                _run_audit_source_rows(audit_sources),
                [
                    ("source", "Source", 18),
                    ("status", "Status", 12),
                    ("coverage", "Coverage", 16),
                    ("gap_count", "Gaps", 8),
                    ("next_action", "Next Action", 58),
                    ("command", "Command", 56),
                ],
                width=width,
                limit=8,
            )
        )
    else:
        saved_action_items = _run_saved_file_action_items(payload)
        if saved_action_items:
            lines.append("")
            lines.append(_rule("Market Bar Saved-File Actions", width))
            lines.extend(_kv_lines(saved_action_items, width=width))
    if not audit_sources and evidence_plan:
        lines.append("")
        lines.append(_rule("Priced-in Evidence Plan", width))
        evidence_items: list[tuple[str, object]] = [
            (
                "Evidence status",
                f"{evidence_plan.get('status')}; {evidence_plan.get('headline')}",
            ),
            ("Next evidence step", evidence_plan.get("next_action")),
        ]
        blocker_hint = _run_source_blocker_hint(evidence_plan)
        if blocker_hint:
            evidence_items.append(("Inspect source blocker", blocker_hint))
        lines.extend(
            _kv_lines(
                evidence_items,
                width=width,
            )
        )
        lines.append("")
        lines.extend(
            _table_lines(
                _evidence_plan_step_rows(evidence_plan),
                [
                    ("priority", "#", 4),
                    ("area", "Area", 18),
                    ("status", "Status", 12),
                    ("depends_on", "Depends", 26),
                    ("action", "Action", 52),
                    ("command", "Command", 56),
                ],
                width=width,
                limit=8,
            )
        )
    lines.append("")
    lines.append(
        "Operational note: execute live runs only after this call plan matches intent. "
        "Type `run execute` to start one capped cycle."
    )
    return lines


def _run_mission_brief_items(
    payload: Mapping[str, object],
) -> list[tuple[str, object]]:
    answer = _mapping(payload.get("priced_in_answer"))
    audit = _mapping(payload.get("priced_in_audit"))
    audit_sources = _rows(audit.get("sources"))
    blocker = _run_first_audit_source_blocker(audit_sources)
    question = answer.get("question") or audit.get("question")
    current = answer.get("answer") or audit.get("answer")
    full_scan = _mapping(answer.get("full_scan"))
    scope = _mapping(audit.get("scope"))
    active = int(
        _number_or_zero(
            _first_value(
                full_scan.get("active_securities"),
                scope.get("active_securities"),
            )
        )
    )
    scanned = int(
        _number_or_zero(
            _first_value(full_scan.get("scanned_rows"), scope.get("scanned_rows"))
        )
    )
    ranked = int(
        _number_or_zero(
            _first_value(full_scan.get("ranked_rows"), scope.get("ranked_rows"))
        )
    )
    source_coverage = _mapping(audit.get("source_coverage"))
    coverage_text = str(source_coverage.get("summary") or "").strip()
    progress_parts = []
    if active or scanned or ranked:
        progress_parts.append(
            f"active {active}; scanned {scanned}; ranked {ranked}"
        )
    if coverage_text:
        progress_parts.append(coverage_text)
    blocker_text = ""
    if blocker:
        source = blocker.get("source") or "source"
        gaps = int(_number_or_zero(blocker.get("gap_count")))
        status = blocker.get("status") or "attention"
        blocker_text = f"{source} {status}; gaps {gaps}"
    next_action = (
        blocker.get("next_action")
        if blocker
        else audit.get("next_action") or answer.get("next_action")
    )
    items: list[tuple[str, object]] = []
    if question:
        items.append(("Question", question))
    if current:
        items.append(("Current answer", current))
    trust_gate = _mapping(answer.get("full_market_trust_gate"))
    operator_step = _priced_in_operator_step(payload)
    operator_step_text = _operator_next_step_summary(operator_step)
    if operator_step_text:
        items.append(("Do now", operator_step_text))
    if trust_gate:
        gate_text = f"{trust_gate.get('status')}; {trust_gate.get('answer')}"
        blocker_detail = _mapping(trust_gate.get("blocker_detail"))
        manual_csv_text = ""
        saved_capture_text = ""
        universe_text = ""
        if blocker_detail.get("source") == "market_bars":
            complete = int(_number_or_zero(blocker_detail.get("complete_rows")))
            missing = int(
                _number_or_zero(blocker_detail.get("missing_as_of_bar"))
            )
            empty = int(_number_or_zero(blocker_detail.get("empty_rows")))
            saved = blocker_detail.get("provider_saved_file_status") or "n/a"
            gate_text = (
                f"{gate_text}; manual CSV {complete}/{missing} complete"
                f", empty {empty}; saved file {saved}"
            )
            manual_csv_text = _market_bar_manual_csv_summary(
                _mapping(blocker_detail.get("manual_csv"))
            )
            saved_capture_text = _market_bar_saved_capture_summary(
                _mapping(blocker_detail.get("saved_provider_capture"))
            )
            universe_text = _market_bar_missing_universe_summary(
                _mapping(blocker_detail.get("missing_universe"))
            )
        items.append(("Trust gate", gate_text))
        recommended_unblock = _market_bar_recommended_action_summary(
            {
                "recommended_action": _mapping(
                    _mapping(
                        _mapping(payload.get("priced_in_answer")).get(
                            "full_market_trust_gate"
                        )
                    ).get("recommended_action")
                )
            }
        )
        if recommended_unblock:
            items.append(("Recommended unblock", recommended_unblock))
        ladder_text = _trust_gate_blocker_ladder_summary(
            _mapping(trust_gate.get("blocker_ladder"))
        )
        if ladder_text:
            items.append(("Blocker ladder", ladder_text))
        after_current_text = _after_current_blocker_summary(
            _mapping(trust_gate.get("after_current_blocker"))
        )
        if after_current_text:
            items.append(("After current", after_current_text))
        if manual_csv_text:
            items.append(("Manual CSV", manual_csv_text))
        if saved_capture_text:
            items.append(("Saved capture", saved_capture_text))
        if universe_text:
            items.append(("Missing universe", universe_text))
    if progress_parts:
        items.append(("Scan progress", "; ".join(progress_parts)))
    if blocker_text:
        items.append(("Trust blocker", blocker_text))
    if next_action:
        items.append(("Useful next", next_action))
    unblock_summary = _run_market_bar_unblock_summary(payload, blocker)
    if unblock_summary:
        items.append(("Unblock options", unblock_summary))
    if items:
        items.append(
            (
                "Boundary",
                (
                    "Viewing and navigation make 0 provider calls; execute only "
                    "one reviewed action at a time."
                ),
            )
        )
    return items


def _trust_gate_blocker_ladder_summary(ladder: Mapping[str, object]):
    rows = [
        row for row in _rows(ladder.get("rows")) if isinstance(row, Mapping)
    ]
    if not rows:
        return ""
    parts = []
    for row in rows[:5]:
        step = int(_number_or_zero(row.get("step")))
        source = str(row.get("source") or "source")
        gap_count = int(_number_or_zero(row.get("gap_count")))
        status = str(row.get("status") or "attention")
        parts.append(f"{step} {source} {status} gaps {gap_count}")
    return "; ".join(parts)


def _after_current_blocker_summary(preview: Mapping[str, object]):
    if not preview:
        return ""
    current = str(preview.get("current_blocker") or "current blocker")
    source = str(preview.get("next_source") or "").strip()
    if not source:
        return ""
    status = str(preview.get("next_status") or "attention")
    gaps = int(_number_or_zero(preview.get("next_gap_count")))
    action = str(preview.get("next_action") or "").strip()
    plan = str(preview.get("plan_command") or "").strip()
    execute = str(preview.get("execute_next_command") or "").strip()
    next_plan = _mapping(preview.get("next_source_plan"))
    parts = [f"after {current}: {source} {status}"]
    if not next_plan:
        parts[0] = f"{parts[0]}; gaps {gaps}"
    if next_plan:
        total = int(_number_or_zero(next_plan.get("total_gap_rows")))
        plannable = int(_number_or_zero(next_plan.get("plannable_gap_rows")))
        routed = int(_number_or_zero(next_plan.get("routed_gap_rows")))
        blocked = int(
            _number_or_zero(next_plan.get("blocked_gap_rows"))
            if "blocked_gap_rows" in next_plan
            else _number_or_zero(next_plan.get("blocked_rows"))
        )
        reason = str(next_plan.get("blocked_reason") or "").strip()
        plan_parts = []
        if total:
            plan_parts.append(f"gaps {total}")
        if "next_chunk_external_calls" in next_plan:
            calls = int(_number_or_zero(next_plan.get("next_chunk_external_calls")))
            plan_parts.append(f"next calls {calls}")
        if plannable:
            plan_parts.append(f"plan {plannable}")
        if routed:
            plan_parts.append(f"routed {routed}")
        if blocked:
            blocked_text = f"blocked {blocked}"
            if reason:
                blocked_text = f"{blocked_text} {reason}"
            plan_parts.append(blocked_text)
        if plan_parts:
            parts.append("source plan " + ", ".join(plan_parts))
        missing_cik = _mapping(next_plan.get("missing_cik"))
        if missing_cik:
            count = int(
                _number_or_zero(
                    missing_cik.get("missing_cik_company_like_rows")
                )
            )
            sample = ", ".join(
                _texts(
                    missing_cik.get("sample_company_like_missing_cik_tickers")
                )[:3]
            )
            if count or sample:
                detail = f"missing CIK {count}"
                if sample:
                    detail = f"{detail} {sample}"
                parts.append(detail)
        fix = str(next_plan.get("fix_command") or "").strip()
        if fix:
            parts.append(f"CIK fix `{fix}`")
        batches = int(_number_or_zero(next_plan.get("batch_count")))
        if batches:
            parts.append(f"{batches} batch(es)")
        blocked_sample = ", ".join(
            _texts(next_plan.get("sample_blocked_tickers"))[:3]
        )
        if blocked_sample:
            parts.append(f"blocked {blocked_sample}")
        routed_sample = ", ".join(
            _texts(next_plan.get("sample_routed_non_company_tickers"))[:3]
        )
        if routed_sample:
            parts.append(f"routed {routed_sample}")
        repair = str(next_plan.get("manual_template_command") or "").strip()
        if repair:
            parts.append(f"repair `{repair}`")
        validate = str(next_plan.get("manual_validate_command") or "").strip()
        if validate:
            parts.append(f"validate `{validate}`")
        manual_import = str(next_plan.get("manual_fix_command") or "").strip()
        if manual_import:
            parts.append(f"import `{manual_import}`")
        if "external_calls_made" in next_plan:
            made = int(_number_or_zero(next_plan.get("external_calls_made")))
            parts.append(f"external calls made {made}")
    if action:
        parts.append(action)
    if plan:
        parts.append(f"plan `{plan}`")
    if execute:
        parts.append(f"execute later `{execute}`")
    return "; ".join(parts)


def _market_bar_manual_csv_summary(manual_csv: Mapping[str, object]):
    if not manual_csv:
        return ""
    complete = int(_number_or_zero(manual_csv.get("complete_rows")))
    missing = int(_number_or_zero(manual_csv.get("missing_row_count")))
    partial = int(_number_or_zero(manual_csv.get("partial_rows")))
    empty = int(_number_or_zero(manual_csv.get("empty_rows")))
    fields = [
        str(field).strip()
        for field in _rows_or_values(manual_csv.get("required_fill_fields"))
        if str(field).strip()
    ]
    sample = [
        str(ticker).strip().upper()
        for ticker in _rows_or_values(manual_csv.get("sample_missing_tickers"))
        if str(ticker).strip()
    ]
    path = str(manual_csv.get("path") or "").strip()
    parts = [f"{complete}/{missing} complete", f"partial {partial}", f"empty {empty}"]
    if fields:
        parts.append("fields " + ", ".join(fields))
    if sample:
        parts.append("sample " + ", ".join(sample[:5]))
    if path:
        parts.append(path)
    return "; ".join(parts)


def _market_bar_saved_capture_summary(saved_capture: Mapping[str, object]):
    if not saved_capture:
        return ""
    status = str(saved_capture.get("status") or "unknown")
    saved_file = str(saved_capture.get("saved_file_status") or "n/a")
    approval = "yes" if saved_capture.get("approval_required") else "no"
    provider_key = "yes" if saved_capture.get("provider_key_configured") else "no"
    calls = int(_number_or_zero(saved_capture.get("external_calls_if_approved")))
    writes = int(_number_or_zero(saved_capture.get("db_writes_during_capture")))
    path = str(saved_capture.get("saved_file_path") or "").strip()
    api = str(saved_capture.get("capture_api") or "").strip()
    next_action = str(saved_capture.get("next_action") or "").strip()
    scope = str(saved_capture.get("coverage_scope") or "").strip()
    active_value = saved_capture.get("active_security_count")
    existing_value = saved_capture.get("existing_as_of_bar_count")
    missing_value = saved_capture.get("missing_as_of_bar_count")
    target_parts: list[str] = []
    if scope:
        target_parts.append(f"scope {scope}")
    if active_value is not None:
        target_parts.append(f"active {int(_number_or_zero(active_value))}")
    if existing_value is not None:
        target_parts.append(f"existing {int(_number_or_zero(existing_value))}")
    if missing_value is not None:
        target_parts.append(f"missing {int(_number_or_zero(missing_value))}")
    parts = [f"status {status}"]
    if target_parts:
        parts.append("target " + ", ".join(target_parts))
    parts.extend(
        [
            f"saved file {saved_file}",
            f"approval {approval}",
            f"key {provider_key}",
            f"calls if approved {calls}",
            f"db writes {writes}",
        ],
    )
    if path:
        parts.append(path)
    if api:
        parts.append(api)
    if next_action:
        parts.append(next_action)
    return "; ".join(parts)


def _market_bar_missing_universe_summary(
    missing_universe: Mapping[str, object],
):
    if not missing_universe:
        return ""
    active = int(_number_or_zero(missing_universe.get("active_metadata_rows")))
    spac_like = int(
        _number_or_zero(missing_universe.get("acquisition_or_spac_name_count"))
    )
    no_figi = int(_number_or_zero(missing_universe.get("no_composite_figi_count")))
    zero_volume = int(
        _number_or_zero(missing_universe.get("zero_avg_dollar_volume_20d_count"))
    )
    summary = str(missing_universe.get("summary") or "").strip()
    note = str(missing_universe.get("operator_note") or "").strip()
    parts = [
        f"{active} active missing-bar rows",
        f"{spac_like} SPAC/acq-style",
        f"{no_figi} no composite FIGI",
        f"{zero_volume} zero local avg volume",
    ]
    if summary:
        parts.append(summary)
    if note:
        parts.append(note)
    return "; ".join(parts)


def _run_market_bar_unblock_summary(
    payload: Mapping[str, object],
    blocker: Mapping[str, object] | None,
) -> str:
    if not blocker or str(blocker.get("source") or "") != "market_bars":
        return ""
    answer = _mapping(payload.get("priced_in_answer"))
    trust_gate = _mapping(answer.get("full_market_trust_gate"))
    blocker_detail = _mapping(trust_gate.get("blocker_detail"))
    option_text = _market_bar_unblock_option_summary(
        _rows(blocker_detail.get("unblock_options"))
    )
    if option_text:
        return option_text
    manual = _market_bar_manual_action_summary(payload)
    capture = _market_bar_provider_saved_file_capture_summary(payload)
    parts: list[str] = []
    if manual:
        parts.append("manual CSV: 0 provider calls")
    if capture:
        parts.append(f"saved capture: {capture}")
    return "; ".join(parts)


def _market_bar_unblock_option_summary(options):
    parts = []
    for option in options[:4]:
        if not isinstance(option, Mapping):
            continue
        kind = str(option.get("kind") or "option")
        status = str(option.get("status") or "unknown")
        calls = int(_number_or_zero(option.get("external_calls_required")))
        command = str(option.get("command") or "").strip()
        if command:
            parts.append(f"{kind}: {status}, {calls} call(s), `{command}`")
        else:
            parts.append(f"{kind}: {status}, {calls} call(s)")
    return "; ".join(parts)


def _market_bar_manual_action_summary(payload: Mapping[str, object]) -> str:
    audit = _mapping(payload.get("priced_in_audit"))
    market = _mapping(audit.get("market_bars"))
    repair = _mapping(market.get("repair"))
    if not repair:
        return ""
    template = str(repair.get("dashboard_manual_template_command") or "").strip()
    preview = str(repair.get("dashboard_manual_import_preview_command") or "").strip()
    execute = str(repair.get("dashboard_manual_import_execute_command") or "").strip()
    if not template and not preview:
        return ""
    parts = ["0 provider calls"]
    if template:
        parts.append(f"type `{template}` to create or refresh the CSV")
    if preview:
        parts.append(f"type `{preview}` to preview complete rows")
    if execute:
        parts.append(f"type `{execute}` only after preview to write local DB rows")
    return "; ".join(parts) + "."


def _run_saved_file_action_items(
    payload: Mapping[str, object],
):
    items: list[tuple[str, object]] = []
    manual_hint = _market_bar_manual_action_summary(payload)
    if manual_hint:
        items.append(("Manual CSV action", manual_hint))
    saved_capture_hint = _market_bar_provider_saved_file_capture_summary(payload)
    if saved_capture_hint:
        items.append(("Saved file capture", saved_capture_hint))
    saved_validate_hint = _market_bar_provider_saved_file_validate_summary(payload)
    if saved_validate_hint:
        items.append(("Saved file check", saved_validate_hint))
    saved_import_hint = _market_bar_provider_saved_file_summary(payload)
    if saved_import_hint:
        items.append(("Saved file import", saved_import_hint))
    return items


def _run_first_audit_source_blocker(
    sources: Sequence[Mapping[str, object]],
) -> Mapping[str, object] | None:
    priority = {
        source: index
        for index, source in enumerate(dashboard_data.PRICED_IN_SOURCE_CLASSES)
    }
    blockers = [
        row
        for row in sources
        if str(row.get("status") or "").strip() not in {"ready", "not_applicable"}
    ]
    if not blockers:
        return None
    return sorted(
        blockers,
        key=lambda row: priority.get(
            str(row.get("source") or ""),
            len(priority),
        ),
    )[0]


def _run_audit_source_blocker_hint(
    blocker: Mapping[str, object] | None,
    payload: Mapping[str, object] | None = None,
) -> str | None:
    if not blocker:
        return None
    source = str(blocker.get("source") or "").strip()
    if not source:
        return None
    command = str(blocker.get("command") or "").strip()
    dashboard_template = str(
        blocker.get("dashboard_manual_template_command") or ""
    ).strip()
    dashboard_preview = str(
        blocker.get("dashboard_manual_import_preview_command") or ""
    ).strip()
    if source == "market_bars" and payload and not (
        dashboard_template or dashboard_preview
    ):
        audit = _mapping(payload.get("priced_in_audit"))
        market = _mapping(audit.get("market_bars"))
        repair = _mapping(market.get("repair"))
        dashboard_template = str(
            repair.get("dashboard_manual_template_command") or ""
        ).strip()
        dashboard_preview = str(
            repair.get("dashboard_manual_import_preview_command") or ""
        ).strip()
    if source == "market_bars" and (dashboard_template or dashboard_preview):
        action_parts = []
        if dashboard_template:
            action_parts.append(f"type `{dashboard_template}` to create the CSV")
        if dashboard_preview:
            action_parts.append(f"type `{dashboard_preview}` to preview complete rows")
        return "; ".join(action_parts) + "; 0 provider calls."
    if source == "market_bars" and command:
        return f"Run `{command}` to create the missing-bar template; preview before import."
    if source in dashboard_data.PRICED_IN_SOURCE_CLASSES:
        return (
            f"Type `batch {source}` for blockers, first provider chunk, "
            f"and exact call budget; type `batch all` for the source map."
        )
    return None


def _run_audit_provider_fill_hint(blocker: Mapping[str, object] | None) -> str | None:
    if not blocker or str(blocker.get("source") or "") != "market_bars":
        return None
    command = str(blocker.get("provider_fill_command") or "").strip()
    if not command:
        return None
    status = str(blocker.get("provider_fill_status") or "unknown").strip()
    calls = int(_number_or_zero(blocker.get("provider_fill_external_call_count")))
    return (
        f"{status}; {calls} external call(s) only after explicit approval; "
        f"diagnostic direct ingest, prefer saved file capture: `{command}`"
    )


def _run_audit_source_rows(
    sources: Sequence[Mapping[str, object]],
) -> list[Mapping[str, object]]:
    rows: list[Mapping[str, object]] = []
    for source in sources:
        available = int(_number_or_zero(source.get("available")))
        row_count = int(_number_or_zero(source.get("row_count")))
        rows.append(
            {
                "source": source.get("source"),
                "status": source.get("status"),
                "coverage": f"{available}/{row_count}" if row_count else "n/a",
                "gap_count": int(_number_or_zero(source.get("gap_count"))),
                "next_action": source.get("next_action"),
                "command": source.get("command"),
            }
        )
    return rows


def _run_source_blocker_hint(evidence_plan: Mapping[str, object]) -> str | None:
    for step in _rows(evidence_plan.get("steps")):
        source = str(step.get("area") or "").strip()
        if source in dashboard_data.PRICED_IN_SOURCE_CLASSES:
            return (
                f"Type `batch {source}` for blockers, first provider chunk, "
                f"and exact call budget; type `batch all` for the source map."
            )
    return None


def _evidence_plan_step_rows(
    evidence_plan: Mapping[str, object],
) -> list[Mapping[str, object]]:
    rows = []
    for step in _rows(evidence_plan.get("steps")):
        depends_on = step.get("depends_on")
        depends = (
            ", ".join(str(item) for item in depends_on if str(item).strip())
            if isinstance(depends_on, list | tuple)
            else ""
        )
        rows.append({**step, "depends_on": depends or "none"})
    return rows


def _agent_brief_rows(brief: Mapping[str, object]) -> list[Mapping[str, object]]:
    rows: list[Mapping[str, object]] = []
    runtime = _mapping(brief.get("runtime"))
    if runtime:
        rows.append(
            {
                "kind": "Runtime",
                "item": _agent_runtime_name(runtime.get("orchestrator")),
                "detail": _agent_runtime_label(runtime),
            }
        )
    for agent in _rows(brief.get("agents")):
        rows.append(
            {
                "kind": "Agent",
                "item": agent.get("agent") or "agent",
                "detail": agent.get("summary") or agent.get("role") or "",
            }
        )
    for index, insight in enumerate(_texts(brief.get("insights")), start=1):
        rows.append({"kind": "Insight", "item": str(index), "detail": insight})
    for index, action in enumerate(_texts(brief.get("next_actions")), start=1):
        rows.append({"kind": "Next", "item": str(index), "detail": action})
    for check in _rows(brief.get("security_checks")):
        name = str(check.get("name") or "check")
        status = str(check.get("status") or "unknown")
        rows.append(
            {
                "kind": "Safety",
                "item": f"{name}: {status}",
                "detail": check.get("detail") or "",
            }
        )
    if not rows:
        rows.append(
            {
                "kind": "Agent",
                "item": "No brief",
                "detail": "Refresh the dashboard snapshot to build the dry-run agent brief.",
            }
        )
    return rows


def _agent_runtime_name(value: object) -> str:
    normalized = str(value or "").strip().lower()
    if normalized == "openai_agents_sdk":
        return "OpenAI Agents SDK"
    return str(value or "unknown")


def _agent_runtime_label(runtime: Mapping[str, object]) -> str:
    orchestrator = _agent_runtime_name(runtime.get("orchestrator") or "openai_agents_sdk")
    assistant_dependency = str(runtime.get("co" + "pilot_dependency") or "absent").replace(
        "_",
        " ",
    )
    tools = str(runtime.get("tool_surface") or "specialist_agents_only").replace("_", " ")
    gate = str(runtime.get("real_mode_gate_status") or "unknown")
    blocked_tools: list[str] = []
    if runtime.get("external_market_tools") is False:
        blocked_tools.append("market")
    if runtime.get("broker_tools") is False:
        blocked_tools.append("broker")
    if runtime.get("shell_tools") is False:
        blocked_tools.append("shell")
    if runtime.get("web_tools") is False:
        blocked_tools.append("web")
    blocked_summary = ", ".join(blocked_tools) or "none"
    return (
        f"{orchestrator}; {'Co' 'pilot'} {assistant_dependency}; tools {tools}; "
        f"real gate {gate}; blocked tools {blocked_summary}"
    )


def _candidates_lines(payload: Mapping[str, object], width: int) -> list[str]:
    rows = [
        _candidate_table_row(row, row_key=str(index))
        for index, row in enumerate(_candidate_rows(payload), start=1)
    ]
    lines = [_rule("Candidates", width)]
    lines.extend(
        _table_lines(
            _indexed(rows),
            [
                ("index", "#", 4),
                ("ticker", "Ticker", 8),
                ("priced_in_status", "Priced-in", 20),
                ("emotion_reaction_gap", "Gap", 8),
                ("score", "Score", 8),
                ("data_coverage", "Data", 32),
                ("why_now", "Why Now", 42),
                ("next_step", "Next Step", 34),
            ],
            width=width,
            limit=30,
        )
    )
    lines.append(
        "Gap is emotion minus price reaction. Positive gap means the market may not "
        "have fully priced the catalyst."
    )
    lines.append("Use `open <#|ticker>` to inspect a candidate.")
    return lines


def _candidate_table_row(row: Mapping[str, object], *, row_key: str) -> Mapping[str, object]:
    brief = _mapping(row.get("research_brief"))
    next_step = (
        ((_priced_in_reason(row) and row.get("priced_in_next_step")) or None)
        or row.get("next_step")
        or row.get("decision_next_step")
        or brief.get("next_step")
    )
    return {
        **dict(row),
        "_row_key": row_key,
        "score": row.get("score") or row.get("final_score"),
        "data_coverage": _data_coverage_summary(row),
        "why_now": (
            _priced_in_reason(row)
            or brief.get("why_now")
            or row.get("top_event_title")
            or row.get("risk_or_gap")
        ),
        "next_step": next_step or "Open candidate detail and review the evidence.",
        "priced_in_status": row.get("priced_in_status") or "n/a",
    }


def _data_coverage_summary(row: Mapping[str, object]) -> str:
    data_sources = row.get("priced_in_data_sources") or row.get("data_sources")
    if isinstance(data_sources, Mapping):
        summary = str(data_sources.get("summary") or "").strip()
        if summary:
            return summary
    return "n/a"


def _candidate_detail_row(payload: Mapping[str, object], ticker: str) -> Mapping[str, object]:
    ticker = ticker.strip().upper()
    for candidate in _candidate_rows(payload):
        if str(candidate.get("ticker") or "").strip().upper() == ticker:
            return candidate
    queue_rows = _rows(_mapping(payload.get("priced_in_queue")).get("rows"))
    for candidate in queue_rows:
        if str(candidate.get("ticker") or "").strip().upper() == ticker:
            return candidate
    return {}


def _candidate_detail_table_rows(row: Mapping[str, object]) -> list[Mapping[str, object]]:
    if not row:
        return _mapping_items(_compact_detail(row))
    return [{"key": key, "value": value} for key, value in _candidate_detail_kv_pairs(row)]


def _candidate_detail_kv_pairs(row: Mapping[str, object]) -> tuple[tuple[str, object], ...]:
    brief = _mapping(row.get("priced_in_evidence_brief"))
    if brief:
        evidence_rows = _rows(brief.get("evidence"))
        evidence = "; ".join(
            _join_nonempty(
                (item.get("title"), item.get("source")),
                separator=" / ",
            )
            for item in evidence_rows[:3]
            if item.get("title")
        )
        blockers = ", ".join(str(item) for item in _rows_or_values(brief.get("blockers")))
        return (
            ("Signal", _priced_in_signal(str(brief.get("status") or ""), fallback="Candidate")),
            ("Usefulness", _candidate_usefulness_summary(brief)),
            ("Why now", brief.get("why_now")),
            ("Non-company evidence", _non_company_evidence_table_summary(brief)),
            ("Emotion vs reaction", _priced_in_mismatch_text(
                brief.get("emotion_score"),
                brief.get("reaction_score"),
                brief.get("emotion_reaction_gap"),
            )),
            ("Priced-in score", brief.get("priced_in_score")),
            ("Top evidence", evidence or brief.get("top_catalyst")),
            (
                "Source",
                _join_nonempty(
                    (brief.get("source"), brief.get("source_url")),
                    separator=" / ",
                ),
            ),
            ("Data coverage", _data_coverage_summary(row)),
            ("Source gaps", _candidate_source_action_summary(brief)),
            ("Blocked", "yes" if brief.get("blocked") else "no"),
            ("Blockers", blockers),
            ("Next step", brief.get("next_step")),
            ("State", row.get("state")),
            ("Decision card", row.get("decision_card_id") or row.get("card")),
        )
    return (
        ("State", row.get("state")),
        ("Decision", row.get("decision_status")),
        ("Score", row.get("score") or row.get("final_score")),
        ("Priced-in status", row.get("priced_in_status")),
        ("Emotion score", row.get("emotion_score")),
        ("Reaction score", row.get("reaction_score")),
        ("Emotion minus reaction", row.get("emotion_reaction_gap")),
        ("Priced-in reason", row.get("priced_in_reason")),
        ("Non-company evidence", _non_company_evidence_table_summary(row)),
        ("Data coverage", _data_coverage_summary(row)),
        ("Setup", row.get("setup") or row.get("setup_type")),
        ("Top catalyst", row.get("top_catalyst") or row.get("top_event_title")),
        ("Risk / gap", row.get("risk_or_gap")),
        (
            "Next step",
            row.get("priced_in_next_step") or row.get("next_step") or row.get("decision_next_step"),
        ),
        ("Readiness gate", row.get("readiness_gate") or row.get("decision_readiness_gate")),
        ("Schwab context", row.get("schwab_context_status")),
        ("Decision card", row.get("decision_card_id") or row.get("card")),
    )


def _rows_or_values(value: object) -> list[object]:
    if isinstance(value, list | tuple):
        return list(value)
    return []


def _candidate_source_action_summary(brief: Mapping[str, object]) -> str:
    gaps = [
        action
        for action in _rows(brief.get("source_actions"))
        if str(action.get("status") or "") not in {"ready", "not_applicable"}
    ]
    if not gaps:
        return "none"
    return "; ".join(
        _join_nonempty(
            (action.get("source"), action.get("next_action")),
            separator=": ",
        )
        for action in gaps[:3]
    )


def _candidate_usefulness_summary(brief: Mapping[str, object]) -> str:
    usefulness = _mapping(brief.get("usefulness"))
    if not usefulness:
        return "n/a"
    return _join_nonempty(
        (
            usefulness.get("label") or usefulness.get("status"),
            usefulness.get("next_action"),
        ),
        separator=": ",
    )


def _candidate_detail_lines(
    payload: Mapping[str, object],
    ticker: str,
    width: int,
) -> list[str]:
    ticker = ticker.strip().upper()
    row = _candidate_detail_row(payload, ticker)
    lines = [_rule(f"Candidate {ticker or 'n/a'}", width)]
    if not row:
        lines.append("Candidate not found for the current filters.")
        return lines
    lines.extend(_kv_lines(_candidate_detail_kv_pairs(row), width=width))
    return lines


def _alerts_lines(payload: Mapping[str, object], width: int) -> list[str]:
    rows = _rows(_mapping(payload.get("alerts")).get("rows"))
    lines = [_rule("Alerts", width)]
    lines.extend(
        _table_lines(
            _indexed(rows),
            [
                ("index", "#", 4),
                ("id", "ID", 18),
                ("ticker", "Ticker", 8),
                ("status", "Status", 12),
                ("route", "Route", 22),
                ("priority", "Priority", 10),
                ("title", "Title", 48),
            ],
            width=width,
            limit=16,
        )
    )
    lines.append(
        "Use `alert-status planned|dry_run|sent|failed|all`, `open <#|id>`, "
        "or `feedback <#|id> <label> [notes]`."
    )
    return lines


def _alert_detail_lines(payload: Mapping[str, object], alert_id: str, width: int) -> list[str]:
    rows = _rows(_mapping(payload.get("alerts")).get("rows"))
    row = next((item for item in rows if str(item.get("id") or "") == alert_id), {})
    lines = [_rule(f"Alert {alert_id or 'n/a'}", width)]
    if not row:
        lines.append("Alert not found for the current filters.")
        return lines
    lines.extend(
        _kv_lines(
            (
                ("Ticker", row.get("ticker")),
                ("Status", row.get("status")),
                ("Route", row.get("route")),
                ("Priority", row.get("priority")),
                ("Title", row.get("title")),
                ("Reason", row.get("reason")),
                ("Created", row.get("created_at")),
            ),
            width=width,
        )
    )
    return lines


def _ipo_lines(payload: Mapping[str, object], width: int) -> list[str]:
    rows = _rows(_mapping(payload.get("ipo_s1")).get("rows"))
    lines = [_rule("IPO / S-1", width)]
    lines.extend(
        _table_lines(
            _indexed(rows),
            [
                ("index", "#", 4),
                ("ticker", "Ticker", 8),
                ("proposed_ticker", "Proposed", 10),
                ("form_type", "Form", 8),
                ("filing_date", "Filed", 12),
                ("estimated_gross_proceeds", "Proceeds", 14),
                ("summary", "Summary", 62),
            ],
            width=width,
            limit=12,
        )
    )
    return lines


def _themes_lines(payload: Mapping[str, object], width: int) -> list[str]:
    rows = _rows(_mapping(payload.get("themes")).get("rows"))
    lines = [_rule("Themes", width)]
    lines.extend(
        _table_lines(
            rows,
            [
                ("theme", "Theme", 24),
                ("candidate_count", "Candidates", 12),
                ("avg_score", "Avg Score", 12),
                ("top_tickers", "Top Tickers", 24),
                ("states", "States", 34),
            ],
            width=width,
            limit=14,
        )
    )
    return lines


def _validation_lines(payload: Mapping[str, object], width: int) -> list[str]:
    validation = _mapping(payload.get("validation"))
    report = _mapping(validation.get("report"))
    lines = [_rule("Validation", width)]
    lines.extend(
        _kv_lines(
            (
                ("Latest run", _nested(validation, "latest_run", "id") or "n/a"),
                ("Status", _nested(validation, "latest_run", "status") or "n/a"),
                ("Candidate count", report.get("candidate_count")),
                ("Useful alert rate", report.get("useful_alert_rate")),
                ("False positive count", report.get("false_positive_count")),
                ("Unsupported claim rate", report.get("unsupported_claim_rate")),
            ),
            width=width,
        )
    )
    return lines


def _costs_lines(payload: Mapping[str, object], width: int) -> list[str]:
    costs = _mapping(payload.get("costs"))
    lines = [_rule("Costs", width)]
    lines.extend(
        _kv_lines(
            (
                ("Attempt count", costs.get("attempt_count")),
                ("Actual cost", costs.get("total_actual_cost_usd")),
                ("Estimated cost", costs.get("total_estimated_cost_usd")),
                ("Useful alerts", costs.get("useful_alert_count")),
                ("Cost per useful alert", costs.get("cost_per_useful_alert")),
            ),
            width=width,
        )
    )
    lines.extend(
        _table_lines(
            _mapping_items(_mapping(costs.get("status_counts"))),
            [("key", "Status", 24), ("value", "Count", 12)],
            width=width,
            limit=10,
        )
    )
    return lines


def _broker_lines(payload: Mapping[str, object], width: int) -> list[str]:
    broker = _mapping(payload.get("broker"))
    snapshot = _mapping(broker.get("snapshot"))
    exposure = _mapping(broker.get("exposure"))
    lines = [_rule("Broker / Portfolio", width)]
    lines.extend(
        _kv_lines(
            (
                ("Connection", snapshot.get("connection_status")),
                ("Broker", snapshot.get("broker")),
                ("Last sync", snapshot.get("last_successful_sync_at")),
                ("Account count", snapshot.get("account_count")),
                ("Position count", snapshot.get("position_count")),
                ("Open orders", snapshot.get("open_order_count")),
                ("Portfolio equity", exposure.get("portfolio_equity")),
            ),
            width=width,
        )
    )
    lines.append("Trading safety: order submission remains disabled unless explicitly configured.")
    lines.append("")
    lines.extend(
        _table_lines(
            _rows(broker.get("opportunity_actions")),
            [
                ("ticker", "Ticker", 8),
                ("action", "Action", 16),
                ("status", "Status", 12),
                ("notes", "Notes", 48),
                ("created_at", "Created", 24),
            ],
            width=width,
            limit=8,
        )
    )
    lines.append("")
    lines.extend(
        _table_lines(
            _rows(broker.get("triggers")),
            [
                ("ticker", "Ticker", 8),
                ("trigger_type", "Trigger", 24),
                ("operator", "Op", 6),
                ("threshold", "Threshold", 12),
                ("status", "Status", 12),
                ("latest_value", "Latest", 12),
            ],
            width=width,
            limit=8,
        )
    )
    lines.append("")
    lines.extend(
        _table_lines(
            _rows(broker.get("order_tickets")),
            [
                ("ticker", "Ticker", 8),
                ("side", "Side", 8),
                ("entry_price", "Entry", 12),
                ("invalidation_price", "Stop", 12),
                ("status", "Status", 14),
                ("submission_allowed", "Submit", 8),
            ],
            width=width,
            limit=8,
        )
    )
    lines.append(
        "Commands: action <ticker> <watch|ready|simulate_entry|dismiss>, "
        "trigger <ticker> <type> <op> <threshold>, eval-triggers [ticker], "
        "ticket <ticker> <buy|sell> <entry> <stop>."
    )
    return lines


def _source_action_sample_tickers(action: Mapping[str, object]) -> str:
    raw_samples = action.get("sample_tickers")
    samples = (
        [str(ticker) for ticker in raw_samples if str(ticker).strip()]
        if isinstance(raw_samples, list | tuple)
        else []
    )
    return ",".join(samples) if samples else "n/a"


def _source_action_gap_count(action: Mapping[str, object]) -> str:
    gap_count = int(
        _number_or_zero(action.get("gap_count"))
        or _number_or_zero(action.get("missing")) + _number_or_zero(action.get("stale"))
    )
    return str(gap_count)


def _source_batch_gap_summary(row: Mapping[str, object]) -> str:
    total = int(_number_or_zero(row.get("total_gap_rows")))
    plannable = int(_number_or_zero(row.get("plannable_gap_rows")))
    routed = int(_number_or_zero(row.get("routed_gap_rows")))
    unplannable = int(_number_or_zero(row.get("unplannable_gap_rows")))
    diagnostic = _mapping(row.get("diagnostic"))
    if "blocked_gap_rows" in row:
        blocked = int(_number_or_zero(row.get("blocked_gap_rows")))
    else:
        blocked = int(_number_or_zero(diagnostic.get("blocked_rows")))
        if blocked <= 0:
            blocked = max(0, unplannable - routed)
    parts = [f"gaps={total}"]
    if plannable or routed or blocked:
        parts.append(f"plan={plannable}")
    if routed:
        parts.append(f"routed={routed}")
    if blocked:
        parts.append(f"blocked={blocked}")
    return " ".join(parts)


def _priced_in_source_workflow_payload(
    preflight: Mapping[str, object],
    *,
    priced_in_queue: Mapping[str, object] | None = None,
    priced_in_answer: Mapping[str, object] | None = None,
) -> dict[str, object]:
    plan = _mapping(preflight.get("evidence_plan"))
    queue_filters = _mapping(_mapping(priced_in_queue or {}).get("filters"))
    stocks_only = bool(queue_filters.get("stocks_only"))
    source_names = set(dashboard_data.PRICED_IN_SOURCE_CLASSES)
    priority_counts = _source_workflow_priority_counts(priced_in_queue or {})
    answer_blockers = _source_workflow_answer_blockers(priced_in_answer or {})
    steps: list[dict[str, object]] = []
    for step in _rows(plan.get("steps")):
        source = str(step.get("area") or "").strip()
        command = str(step.get("command") or "").strip()
        if source not in source_names and "priced-in-source-batches" not in command:
            continue
        priority = priority_counts.get(source, {})
        blocker = answer_blockers.get(source, {})
        blocker_gap_count = int(_number_or_zero(blocker.get("gap_count")))
        blocker_action = str(blocker.get("next_action") or "").strip()
        blocker_command = str(blocker.get("command") or "").strip()
        promoted_market_bar_blocker = bool(
            source == "market_bars" and blocker_gap_count > 0
        )
        steps.append(
            {
                "preflight_priority": (
                    -1
                    if promoted_market_bar_blocker
                    else int(_number_or_zero(step.get("priority")))
                ),
                "source": source,
                "status": blocker.get("status") or step.get("status"),
                "depends_on": _texts(step.get("depends_on")),
                "action": blocker_action or step.get("action") or step.get("next_action"),
                "command": blocker_command or command or None,
                "api": step.get("api"),
                "gap_rows": blocker_gap_count or None,
                "decision_useful_gap_rows": int(
                    _number_or_zero(priority.get("decision_useful_gap_rows"))
                ),
                "research_useful_gap_rows": int(
                    _number_or_zero(priority.get("research_useful_gap_rows"))
                ),
                "actionable_gap_rows": int(
                    _number_or_zero(priority.get("actionable_gap_rows"))
                ),
                "priority_sample_tickers": _texts(
                    priority.get("priority_sample_tickers")
                ),
            }
        )
    steps = sorted(steps, key=_source_workflow_coverage_key)
    for index, step in enumerate(steps, start=1):
        step["priority"] = index
    coverage_step = steps[0] if steps else {}
    use_coverage_step = bool(
        coverage_step.get("source") == "market_bars"
        and int(_number_or_zero(coverage_step.get("gap_rows"))) > 0
    )
    next_action = (
        coverage_step.get("action")
        if use_coverage_step
        else plan.get("next_action")
        or coverage_step.get("action")
        or "Review full-scan source coverage."
    )
    next_command = (
        coverage_step.get("command")
        if use_coverage_step
        else plan.get("next_command") or coverage_step.get("command")
    )
    decision_suggested = None
    if not use_coverage_step:
        decision_suggested = next(
            (
                step
                for step in sorted(steps, key=_source_workflow_priority_key)
                if _source_workflow_has_priority(step)
                and not _source_workflow_priority_blocked(step)
            ),
            None,
        )
    decision_shortcut_action = None
    decision_shortcut_command = None
    decision_shortcut_blocker = None
    if use_coverage_step:
        row_label = "stock-like row" if stocks_only else "active row"
        decision_shortcut_blocker = {
            "blocked_by": "market_bars",
            "blocked_gap_rows": int(_number_or_zero(coverage_step.get("gap_rows"))),
            "action": (
                "Clear market_bars first; decision shortcuts are hidden until "
                f"every {row_label} has scan-date price reaction."
            ),
            "command": next_command,
            "external_calls_required": 0,
        }
    if decision_suggested is not None:
        decision_shortcut_action = _source_workflow_suggested_action(decision_suggested)
        decision_shortcut_command = decision_suggested.get("command") or (
            "catalyst-radar priced-in-source-batches "
            f"--source {decision_suggested.get('source')}"
        )
    goal_alignment = _source_workflow_goal_alignment(
        priced_in_queue or {},
        steps=steps,
        stocks_only=stocks_only,
        next_action=next_action,
        next_command=next_command,
    )
    return {
        "schema_version": "priced-in-source-workflow-v1",
        "status": plan.get("status") or "unknown",
        "headline": plan.get("headline"),
        "next_action": next_action,
        "next_command": next_command,
        "coverage_first_action": next_action,
        "coverage_first_command": next_command,
        "decision_shortcut_action": decision_shortcut_action,
        "decision_shortcut_command": decision_shortcut_command,
        "decision_shortcut_blocker": decision_shortcut_blocker,
        "priority_scope": "full_scan_coverage",
        "decision_priority_scope": "visible_priced_in_rows",
        "goal_alignment": goal_alignment,
        "overview_command": (
            "catalyst-radar priced-in-source-batches --source all"
            + (" --stocks-only" if stocks_only else "")
        ),
        "overview_api": (
            "GET /api/radar/priced-in/source-batches?source=all"
            + ("&stocks_only=true" if stocks_only else "")
        ),
        "external_calls_made": 0,
        "steps": steps,
        "step_count": len(steps),
    }


def _source_workflow_answer_blockers(
    priced_in_answer: Mapping[str, object],
) -> dict[str, dict[str, object]]:
    return {
        str(blocker.get("area") or "").strip(): dict(blocker)
        for blocker in _rows(priced_in_answer.get("trust_blockers"))
        if str(blocker.get("area") or "").strip()
    }


def _source_workflow_goal_alignment(
    priced_in_queue: Mapping[str, object],
    *,
    steps: Sequence[Mapping[str, object]],
    stocks_only: bool,
    next_action: object,
    next_command: object,
) -> dict[str, object]:
    total = int(_number_or_zero(priced_in_queue.get("total_count")))
    returned = int(
        _number_or_zero(priced_in_queue.get("returned_count"))
        or _number_or_zero(priced_in_queue.get("count"))
    )
    coverage_step = steps[0] if steps else {}
    source = str(coverage_step.get("source") or "").strip()
    useful_rows = _source_workflow_useful_rows(coverage_step)
    current_scope = "stock rows" if stocks_only else "ranked rows"
    current_blocker = (
        f"{source} is the first source coverage step"
        + (f"; useful rows: {useful_rows}." if useful_rows != "none" else ".")
        if source
        else "No source coverage step is currently visible."
    )
    market_bar_blocker = bool(
        source == "market_bars"
        and int(_number_or_zero(coverage_step.get("gap_rows"))) > 0
    )
    source_next_step = None
    if market_bar_blocker:
        source_next_step = str(
            next_action
            or "Create the missing stock-bar template and preview the import."
        )
    elif source:
        source_next_step = (
            f"Type batch {source} to inspect the full-scan source plan; run "
            f"batch {source} execute only if the provider budget is intentional."
        )
    provider_boundary = (
        "Template generation and import preview are zero-call. Provider fills "
        "or source-batch execution require explicit approval."
        if market_bar_blocker
        else (
            "Browsing, clicking, filtering, and refresh are zero-call. Only "
            "batch <source> execute runs a reviewed provider chunk."
        )
    )
    return {
        "schema_version": "priced-in-goal-alignment-v1",
        "status": "aligned",
        "goal": (
            "Find stocks where market emotion has not yet been matched by "
            "price reaction."
        ),
        "useful_definition": (
            "Useful means a ranked stock row has fresh price reaction plus "
            "enough catalyst/context evidence to judge the emotion-price gap."
        ),
        "stocks_only": bool(stocks_only),
        "instrument_filter": "stocks_only" if stocks_only else "all_instruments",
        "ranked_rows": total,
        "visible_rows": returned,
        "current_state": (
            f"This view is showing {returned} of {total} {current_scope}."
        ),
        "current_blocker": current_blocker,
        "next_useful_step": str(
            source_next_step
            or next_action
            or "Review source coverage before adding more data."
        ),
        "next_command": (
            next_command
            if market_bar_blocker
            else f"batch {source}"
            if source
            else next_command
        ),
        "provider_boundary": provider_boundary,
    }


def _source_workflow_priority_counts(
    priced_in_queue: Mapping[str, object],
) -> dict[str, dict[str, object]]:
    counts: dict[str, dict[str, object]] = {
        source: {
            "decision_useful_gap_rows": 0,
            "research_useful_gap_rows": 0,
            "actionable_gap_rows": 0,
            "priority_sample_tickers": [],
        }
        for source in dashboard_data.PRICED_IN_SOURCE_CLASSES
    }
    actionable_statuses = {"bullish_not_priced_in", "bearish_not_priced_in"}
    for row in _rows(priced_in_queue.get("rows")):
        ticker = str(row.get("ticker") or "").strip().upper()
        priced_status = str(row.get("priced_in_status") or "").strip().lower()
        usefulness_status = str(
            _mapping(row.get("usefulness")).get("status") or ""
        ).strip().lower()
        data_sources = _mapping(row.get("data_sources"))
        gaps = {
            source
            for source in (
                *_texts(data_sources.get("missing")),
                *_texts(data_sources.get("stale")),
            )
            if source in counts
        }
        for source in gaps:
            source_counts = counts[source]
            if priced_status in actionable_statuses:
                source_counts["actionable_gap_rows"] = int(
                    _number_or_zero(source_counts.get("actionable_gap_rows"))
                ) + 1
            if usefulness_status == "decision_useful":
                source_counts["decision_useful_gap_rows"] = int(
                    _number_or_zero(source_counts.get("decision_useful_gap_rows"))
                ) + 1
                _append_source_workflow_sample(source_counts, ticker)
            elif usefulness_status == "research_useful":
                source_counts["research_useful_gap_rows"] = int(
                    _number_or_zero(source_counts.get("research_useful_gap_rows"))
                ) + 1
                _append_source_workflow_sample(source_counts, ticker)
    return counts


def _append_source_workflow_sample(counts: dict[str, object], ticker: str) -> None:
    if not ticker:
        return
    samples = counts.get("priority_sample_tickers")
    if not isinstance(samples, list):
        samples = []
        counts["priority_sample_tickers"] = samples
    if ticker not in samples and len(samples) < 5:
        samples.append(ticker)


def _source_workflow_priority_key(step: Mapping[str, object]) -> tuple[int, int, int, int]:
    decision_rows = int(_number_or_zero(step.get("decision_useful_gap_rows")))
    research_rows = int(_number_or_zero(step.get("research_useful_gap_rows")))
    actionable_rows = int(_number_or_zero(step.get("actionable_gap_rows")))
    source = str(step.get("source") or "")
    try:
        source_order = dashboard_data.PRICED_IN_SOURCE_CLASSES.index(source)
    except ValueError:
        source_order = len(dashboard_data.PRICED_IN_SOURCE_CLASSES)
    preflight_priority = int(_number_or_zero(step.get("preflight_priority")))
    if decision_rows:
        return (0, -decision_rows, source_order, preflight_priority)
    if research_rows:
        return (1, -research_rows, source_order, preflight_priority)
    if actionable_rows:
        return (2, -actionable_rows, source_order, preflight_priority)
    return (3, 0, source_order, preflight_priority)


def _source_workflow_coverage_key(step: Mapping[str, object]) -> tuple[int, int]:
    preflight_priority = int(_number_or_zero(step.get("preflight_priority")))
    source = str(step.get("source") or "")
    try:
        source_order = dashboard_data.PRICED_IN_SOURCE_CLASSES.index(source)
    except ValueError:
        source_order = len(dashboard_data.PRICED_IN_SOURCE_CLASSES)
    return (preflight_priority or 99, source_order)


def _source_workflow_has_priority(step: Mapping[str, object]) -> bool:
    return any(
        int(_number_or_zero(step.get(key))) > 0
        for key in (
            "decision_useful_gap_rows",
            "research_useful_gap_rows",
            "actionable_gap_rows",
        )
    )


def _source_workflow_priority_blocked(step: Mapping[str, object]) -> bool:
    source = str(step.get("source") or "").strip()
    if source != "options":
        return False
    action = str(step.get("action") or "").strip().lower()
    return any(
        marker in action
        for marker in (
            "stored options exist after this scan date",
            "point-in-time options",
            "after the scan cutoff",
            "decision cutoff",
        )
    )


def _source_workflow_suggested_action(step: Mapping[str, object]) -> str:
    source = str(step.get("source") or "source")
    samples = _texts(step.get("priority_sample_tickers"))
    sample_text = f" Example: {', '.join(samples)}." if samples else ""
    decision_rows = int(_number_or_zero(step.get("decision_useful_gap_rows")))
    if decision_rows:
        return (
            f"Start with {source}; it fills context for {decision_rows} "
            "decision-ready row(s) in the visible ranked page. "
            f"Type batch {source} to inspect the full-scan plan.{sample_text}"
        )
    research_rows = int(_number_or_zero(step.get("research_useful_gap_rows")))
    if research_rows:
        return (
            f"Start with {source}; it clears evidence for {research_rows} "
            "research-useful row(s) in the visible ranked page. "
            f"Type batch {source} to inspect the full-scan plan.{sample_text}"
        )
    actionable_rows = int(_number_or_zero(step.get("actionable_gap_rows")))
    return (
        f"Start with {source}; it covers {actionable_rows} actionable mismatch "
        "row(s) in the visible ranked page. "
        f"Type batch {source} to inspect the full-scan plan.{sample_text}"
    )


def _source_workflow_lines(payload: Mapping[str, object], width: int) -> list[str]:
    workflow = _mapping(payload.get("priced_in_source_workflow"))
    steps = _rows(workflow.get("steps"))
    if not steps:
        return []
    lines = [_rule("Source Fill Workflow", width)]
    goal = _mapping(workflow.get("goal_alignment"))
    if goal:
        full_scan_summary = _answer_full_scan_scope_summary(payload)
        goal_items: list[tuple[str, object]] = [
            ("Goal", goal.get("goal")),
            ("Useful", goal.get("useful_definition")),
        ]
        if full_scan_summary:
            goal_items.append(("Full scan", full_scan_summary))
        evidence_summary = _answer_evidence_completeness_summary(payload)
        if evidence_summary:
            goal_items.append(("Evidence", evidence_summary))
        goal_items.extend(
            [
                ("Now", goal.get("current_state")),
                ("Blocker", goal.get("current_blocker")),
                ("Next", goal.get("next_useful_step")),
                ("Safety", goal.get("provider_boundary")),
            ]
        )
        lines.extend(
            _kv_lines(
                goal_items,
                width=width,
            )
        )
        lines.append("")
    lines.extend(
        _kv_lines(
            (
                ("Status", workflow.get("status")),
                (
                    "Coverage-first",
                    workflow.get("coverage_first_action")
                    or workflow.get("next_action"),
                ),
                ("Decision shortcut", workflow.get("decision_shortcut_action")),
                ("All-source plan", workflow.get("overview_command")),
            ),
            width=width,
        )
    )
    table_rows = [
        {
            **step,
            "depends_on": ",".join(_texts(step.get("depends_on"))) or "none",
            "gap_summary": _source_workflow_gap_summary(step),
            "inspect_command": _source_workflow_inspect_command(step),
            "useful_rows": _source_workflow_useful_rows(step),
        }
        for step in steps
    ]
    lines.extend(
        _table_lines(
            table_rows,
            [
                ("priority", "#", 4),
                ("source", "Source", 14),
                ("status", "Status", 12),
                ("gap_summary", "Full gaps", 16),
                ("useful_rows", "Useful rows", 18),
                ("depends_on", "After", 18),
                ("action", "Do this", 48),
                ("inspect_command", "Inspect", 24),
            ],
            width=width,
            limit=8,
        )
    )
    lines.append(
        "`batch all` shows this source map without provider calls; "
        "`batch <source> all` summarizes the full chunk plan; "
        "`batch <source> execute` runs one guarded chunk; "
        "`batch <source> execute 3` runs a capped set."
    )
    lines.append(
        "Full scan = the whole ranked universe. Source-fill tickers = the next "
        "rate-limited provider chunk, not the ticker universe."
    )
    return lines


def _source_workflow_gap_summary(step: Mapping[str, object]):
    gap_rows = int(_number_or_zero(step.get("gap_rows")))
    if gap_rows <= 0:
        return "none"
    return f"{gap_rows} full-scan"


def _source_workflow_inspect_command(step: Mapping[str, object]):
    source = str(step.get("source") or "").strip()
    if source == "market_bars":
        return "bars status"
    if source:
        return f"batch {source}"
    command = str(step.get("command") or "").strip()
    return command or "n/a"


def _source_workflow_useful_rows(step: Mapping[str, object]) -> str:
    decision_rows = int(_number_or_zero(step.get("decision_useful_gap_rows")))
    research_rows = int(_number_or_zero(step.get("research_useful_gap_rows")))
    actionable_rows = int(_number_or_zero(step.get("actionable_gap_rows")))
    parts = []
    if decision_rows:
        parts.append(f"decision {decision_rows}")
    if research_rows:
        parts.append(f"research {research_rows}")
    if actionable_rows:
        parts.append(f"action {actionable_rows}")
    return ", ".join(parts) if parts else "none"


def _source_coverage_workbench_rows(
    payload: Mapping[str, object],
) -> list[Mapping[str, object]]:
    workflow = _mapping(payload.get("priced_in_source_workflow"))
    coverage = _mapping(payload.get("priced_in_source_coverage"))
    action_by_source = {
        str(action.get("source") or "").strip(): action
        for action in _rows(coverage.get("actions"))
        if str(action.get("source") or "").strip()
    }
    rows: list[Mapping[str, object]] = []
    for index, step in enumerate(_rows(workflow.get("steps")), start=1):
        source = str(step.get("source") or "").strip()
        if not source:
            continue
        action = _mapping(action_by_source.get(source))
        examples = (
            _source_action_sample_tickers(action)
            if action
            else ",".join(_texts(step.get("priority_sample_tickers"))) or "n/a"
        )
        rows.append(
            {
                "_row_key": f"source-{source}",
                "priority": step.get("priority") or index,
                "source": source,
                "status": action.get("status") or step.get("status") or "unknown",
                "gap_rows": _source_action_gap_count(action) if action else "n/a",
                "useful_rows": _source_workflow_useful_rows(step),
                "examples": examples,
                "plan": f"batch {source}",
                "next_action": step.get("action") or "Inspect the source plan.",
            }
        )
    if rows:
        return sorted(rows, key=_source_coverage_workbench_sort_key)
    for index, action in enumerate(_rows(coverage.get("actions")), start=1):
        source = str(action.get("source") or "").strip()
        if not source:
            continue
        rows.append(
            {
                "_row_key": f"source-{source}",
                "priority": index,
                "source": source,
                "status": action.get("status") or "unknown",
                "gap_rows": _source_action_gap_count(action),
                "useful_rows": "n/a",
                "examples": _source_action_sample_tickers(action),
                "plan": f"batch {source}",
                "next_action": action.get("next_action")
                or action.get("action")
                or "Inspect the source plan.",
            }
        )
    return sorted(rows, key=_source_coverage_workbench_sort_key)


def _source_coverage_workbench_sort_key(row: Mapping[str, object]) -> tuple[int, int, str]:
    source = str(row.get("source") or "")
    try:
        source_order = dashboard_data.PRICED_IN_SOURCE_CLASSES.index(source)
    except ValueError:
        source_order = len(dashboard_data.PRICED_IN_SOURCE_CLASSES)
    status = str(row.get("status") or "").strip().lower()
    gap_rows = int(_number_or_zero(row.get("gap_rows")))
    useful_rows = str(row.get("useful_rows") or "").strip().lower()
    if gap_rows <= 0 and status in {"ready", "no_gaps"}:
        return (4, source_order, source)
    if "decision" in useful_rows:
        return (0, source_order, source)
    if "research" in useful_rows or "action" in useful_rows:
        return (1, source_order, source)
    if gap_rows > 0:
        return (2, source_order, source)
    return (3, source_order, source)


def _source_coverage_workbench_detail(
    payload: Mapping[str, object],
    rows: Sequence[Mapping[str, object]],
) -> str:
    workflow = _mapping(payload.get("priced_in_source_workflow"))
    coverage_first = str(
        workflow.get("coverage_first_action")
        or workflow.get("next_action")
        or "Review source gaps."
    ).strip()
    decision_shortcut = str(workflow.get("decision_shortcut_action") or "").strip()
    row_count = len(rows)
    shortcut_text = (
        f" Decision shortcut: {decision_shortcut}"
        if decision_shortcut
        else " No decision shortcut is currently runnable."
    )
    return (
        f"{row_count} source row(s). Coverage-first: {coverage_first}"
        f"{shortcut_text} Enter/click is plan-only; execute requires "
        "batch <source> execute."
    )


def _ops_lines(payload: Mapping[str, object], width: int) -> list[str]:
    ops = _mapping(payload.get("ops_health"))
    database = _mapping(ops.get("database"))
    degraded = _mapping(ops.get("degraded_mode"))
    lines = [_rule("Operations", width)]
    lines.extend(
        _kv_lines(
            (
                ("Database status", database.get("status")),
                ("Candidates", database.get("candidate_state_count")),
                ("Packets", database.get("candidate_packet_count")),
                ("Decision cards", database.get("decision_card_count")),
                ("Latest daily bar", database.get("latest_daily_bar_date")),
                ("Degraded mode", degraded.get("enabled")),
                ("Max action state", degraded.get("max_action_state")),
            ),
            width=width,
        )
    )
    missing_type_summary = _market_bar_missing_type_summary(payload)
    if missing_type_summary:
        lines.append(
            f"Missing bar types: {_clip(missing_type_summary, max(20, width - 19))}"
        )
    stock_bar_summary = _stock_market_bar_next_summary(payload)
    if stock_bar_summary:
        lines.append(
            "Stock bar next: "
            f"{_clip(stock_bar_summary, max(20, width - 17))}"
        )
    manual_progress_summary = _market_bar_manual_fill_progress_summary(payload)
    if manual_progress_summary:
        lines.append(
            "Manual CSV progress: "
            f"{_clip(manual_progress_summary, max(20, width - 22))}"
        )
    operator_step_summary = _market_bar_operator_step_summary(payload)
    if operator_step_summary:
        lines.append(
            "Market bar next: "
            f"{_clip(operator_step_summary, max(20, width - 18))}"
        )
    saved_file_capture_summary = _market_bar_provider_saved_file_capture_summary(
        payload,
    )
    if saved_file_capture_summary:
        lines.append(
            "Saved file capture: "
            f"{_clip(saved_file_capture_summary, max(20, width - 22))}"
        )
    saved_file_validate_summary = _market_bar_provider_saved_file_validate_summary(
        payload,
    )
    if saved_file_validate_summary:
        lines.append(
            "Saved file check: "
            f"{_clip(saved_file_validate_summary, max(20, width - 20))}"
        )
    saved_file_summary = _market_bar_provider_saved_file_summary(payload)
    if saved_file_summary:
        lines.append(
            "Saved file import: "
            f"{_clip(saved_file_summary, max(20, width - 21))}"
        )
    provider_fill_summary = _market_bar_provider_fill_summary(payload)
    if provider_fill_summary:
        lines.append(
            "Direct provider fill: "
            f"{_clip(provider_fill_summary, max(20, width - 24))}"
        )
    source_actions = [
        {
            **action,
            "gap_rows": _source_action_gap_count(action),
            "examples": _source_action_sample_tickers(action),
            "batch_plan": action.get("batch_plan_command") or action.get("command"),
        }
        for action in _rows(_mapping(payload.get("priced_in_source_coverage")).get("actions"))
        if str(action.get("status") or "") not in {"ready", "not_applicable"}
    ]
    if source_actions:
        lines.append("")
        lines.append(_rule("Visible Review Page Source Gaps", width))
        lines.extend(
            _table_lines(
                source_actions,
                [
                    ("source", "Source", 18),
                    ("status", "Status", 12),
                    ("coverage_pct", "Coverage", 10),
                    ("gap_rows", "Gap rows", 10),
                    ("examples", "Examples", 22),
                    ("batch_plan", "Batch plan", 62),
                ],
                width=width,
                limit=8,
            )
        )
        lines.append(
            "This table is source coverage for the visible review page, not the "
            "full scan universe. The Source Fill Workflow below shows full-scan "
            "gaps and guarded batch plans. Examples are sample tickers only. "
            "Type `batch <source>` to show the full-scan plan; type "
            "`batch <source> execute` to run only the next guarded chunk, or "
            "`batch <source> execute 3` for a capped run."
        )
    workflow_lines = _source_workflow_lines(payload, width)
    if workflow_lines:
        lines.append("")
        lines.extend(workflow_lines)
    lines.append("")
    lines.extend(
        _table_lines(
            _rows(ops.get("providers")),
            [
                ("provider", "Provider", 16),
                ("status", "Status", 12),
                ("checked_at", "Checked", 24),
                ("reason", "Reason", 62),
            ],
            width=width,
            limit=10,
        )
    )
    lines.append("")
    lines.extend(
        _table_lines(
            _rows(ops.get("jobs")),
            [
                ("job_type", "Job", 24),
                ("provider", "Provider", 12),
                ("status", "Status", 12),
                ("requested_count", "Req", 6),
                ("normalized_count", "Norm", 6),
                ("finished_at", "Finished", 24),
            ],
            width=width,
            limit=8,
        )
    )
    return lines


def _telemetry_lines(payload: Mapping[str, object], width: int) -> list[str]:
    telemetry = _mapping(payload.get("telemetry"))
    coverage = _mapping(payload.get("telemetry_coverage"))
    lines = [_rule("Telemetry", width)]
    lines.extend(
        _kv_lines(
            (
                ("Telemetry", f"{telemetry.get('status')}; {telemetry.get('headline')}"),
                ("Events", telemetry.get("event_count")),
                ("Attention", telemetry.get("attention_count")),
                ("Guarded", telemetry.get("guarded_count")),
                ("Coverage", f"{coverage.get('status')}; {coverage.get('headline')}"),
                (
                    "Required ready",
                    f"{coverage.get('ready_required_domain_count')}/"
                    f"{coverage.get('required_domain_count')}",
                ),
                ("Missing required", coverage.get("missing_required_count")),
            ),
            width=width,
        )
    )
    lines.append("")
    lines.extend(
        _table_lines(
            _rows(telemetry.get("events")),
            [
                ("occurred_at", "Occurred", 24),
                ("event", "Event", 24),
                ("status", "Status", 12),
                ("summary", "Summary", 66),
            ],
            width=width,
            limit=12,
        )
    )
    lines.append("")
    lines.extend(
        _table_lines(
            _rows(coverage.get("domains")),
            [
                ("domain", "Domain", 30),
                ("status", "Status", 12),
                ("event_count", "Events", 8),
                ("operator_action", "Operator Action", 62),
            ],
            width=width,
            limit=8,
        )
    )
    return lines


def _feature_lines(payload: Mapping[str, object], width: int) -> list[str]:
    lines = [_rule("Current Market Radar Features", width)]
    lines.extend(
        _table_lines(
            _rows(payload.get("feature_inventory")),
            [
                ("area", "Area", 16),
                ("feature", "Feature", 44),
                ("page", "TUI Page", 20),
                ("use", "Operational Use", 46),
            ],
            width=width,
            limit=20,
        )
    )
    return lines


def _agent_lines(payload: Mapping[str, object], width: int) -> list[str]:
    brief = _mapping(payload.get("agent_brief"))
    calls = _mapping(brief.get("external_calls_made"))
    runtime = _mapping(brief.get("runtime"))
    lines = [_rule("Agent Brief", width)]
    lines.append(
        f"Mode: {brief.get('mode') or 'dry_run'} | "
        f"Status: {brief.get('status') or 'unknown'} | "
        f"Calls: openai={calls.get('openai', 0)}, "
        f"market={calls.get('market_data', 0)}, broker={calls.get('broker', 0)}"
    )
    if runtime:
        lines.append(f"Runtime: {_agent_runtime_label(runtime)}")
    boundary = brief.get("decision_boundary")
    if boundary:
        lines.append(f"Boundary: {boundary}")
    lines.extend(
        _table_lines(
            _agent_brief_rows(brief),
            [
                ("kind", "Kind", 10),
                ("item", "Item", 24),
                ("detail", "Detail", 82),
            ],
            width=width,
            limit=18,
        )
    )
    return lines


def _help_lines(width: int) -> list[str]:
    lines = [_rule("Help", width)]
    commands = [
        ("0..10 or page name", "Switch page."),
        ("features", "List current Market Radar features and where they live in the TUI."),
        ("open <#|ticker>", "Open a candidate from the candidates page."),
        ("open <#|alert-id>", "Open an alert from the alerts page."),
        ("ticker <SYMBOL|all>", "Filter candidate-adjacent pages by ticker where supported."),
        ("available-at <ISO|latest>", "Set or clear the point-in-time data cutoff."),
        ("ready", "Show only decision-useful not-priced-in rows from the full scan."),
        ("now", "Show the single next priced-in action, response, and cost."),
        ("usefulness <status|all>", "Filter Insights by usefulness verdict."),
        ("source-gap <source|all>", "Filter Insights by missing/stale data source."),
        ("batch <source>", "Plan full-scan source fill and show the next safe chunk."),
        ("batch <source> execute", "Run only the next guarded source-fill chunk."),
        ("batch <source> execute 3", "Run a capped source-fill batch set."),
        ("bars", "Show market-bar blocker status and safe next actions."),
        ("bars manual template", "Generate the full-universe missing-bar CSV."),
        ("bars manual import", "Preview or execute complete-row manual import."),
        ("bars saved capture", "Plan saved capture; add confirm for one provider call."),
        ("bars saved validate", "Validate the saved grouped-daily file from disk."),
        ("bars saved import", "Preview or execute the saved-file import."),
        ("cik template", "Create the local SEC CIK override CSV."),
        ("cik validate", "Validate the local CIK override CSV with zero calls."),
        ("cik import", "Preview or explicitly execute CIK metadata import."),
        ("options template", "Create the point-in-time options JSON template."),
        ("options validate", "Validate the local options fixture with zero calls."),
        ("options import", "Preview or explicitly execute options fixture import."),
        ("decision-gap <gap|all>", "Filter Insights by missing decision evidence."),
        ("next / prev", "Page through the current Insights scan rows."),
        ("offset <row>", "Jump to a 1-based full-scan row number."),
        ("limit <1-200>", "Change Insights rows per page."),
        ("alert-status <status|all>", "Filter alerts by status."),
        ("alert-route <route|all>", "Filter alerts by route."),
        ("refresh", "Reload the local database snapshot."),
        ("run", "Show the guarded run instruction on the run page."),
        ("run execute", "Start one capped radar cycle after reviewing the call plan."),
        ("json", "Print the redacted JSON snapshot used by the TUI."),
        ("action <ticker> <action> [notes]", "Save watch/ready/simulate_entry/dismiss."),
        ("trigger <ticker> <type> <op> <threshold>", "Save a market trigger."),
        ("eval-triggers [ticker]", "Evaluate saved triggers against stored market context."),
        ("ticket <ticker> <side> <entry> <stop>", "Save a blocked order-preview ticket."),
        ("feedback <alert-id|#> <label>", "Record alert feedback from current alert rows."),
        ("clear-filters", "Reset filters."),
        ("q", "Quit."),
    ]
    lines.extend(_table_lines([{"command": a, "meaning": b} for a, b in commands],
                              [("command", "Command", 28), ("meaning", "Meaning", 84)],
                              width=width,
                              limit=30))
    return lines


def _retry_after_seconds(reset_at: datetime | None, now: datetime) -> int:
    if reset_at is None:
        return 1
    return max(1, int((reset_at - now).total_seconds()))


def _dashboard_count_lines(payload: Mapping[str, object], width: int) -> list[str]:
    return [
        _rule("Dashboard Rows", width),
        (
            f"Candidates: {_mapping(payload.get('candidates')).get('count') or 0} | "
            f"Alerts: {_mapping(payload.get('alerts')).get('count') or 0} | "
            f"IPO/S-1: {_mapping(payload.get('ipo_s1')).get('count') or 0} | "
            f"Themes: {_mapping(payload.get('themes')).get('count') or 0}"
        ),
    ]


def _metric_text(title: str, value: object, detail: object) -> str:
    return (
        f"[dim]{title.upper()}[/dim] [bold #7ee787]{_text(value)}[/]\n"
        f"[dim]{_text(detail)}[/dim]"
    )


def _decision_label(readiness: Mapping[str, object]) -> str:
    if readiness.get("safe_to_make_investment_decision") is True:
        return "Yes, after manual review"
    status = str(readiness.get("status") or "").strip().replace("_", " ")
    if status:
        return f"No - {status}"
    return "No - not decision ready"


def _ops_detail(payload: Mapping[str, object]) -> str:
    database = _mapping(_mapping(payload.get("ops_health")).get("database"))
    degraded = _mapping(_mapping(payload.get("ops_health")).get("degraded_mode"))
    return (
        f"database={database.get('status')}; "
        f"latest_bar={database.get('latest_daily_bar_date')}; "
        f"degraded={degraded.get('enabled')}; "
        f"max_action_state={degraded.get('max_action_state')}"
    )


def _compact_detail(row: Mapping[str, object]) -> Mapping[str, object]:
    if not row:
        return {"status": "No row found for the current filters."}
    excluded = {"payload", "raw_payload", "metadata", "_row_key"}
    compact: dict[str, object] = {}
    for key, value in row.items():
        if key in excluded or value in (None, "", [], {}):
            continue
        compact[str(key)] = value
        if len(compact) >= 14:
            break
    return compact


def _footer_lines(width: int) -> list[str]:
    return [
        _rule("Commands", width),
        "Type a page name, number, filter command, refresh, json, help, or q.",
    ]


def _candidate_rows(payload: Mapping[str, object]) -> list[Mapping[str, object]]:
    direct_rows = _rows(_mapping(payload.get("candidates")).get("rows"))
    if direct_rows:
        return direct_rows
    readiness = _mapping(payload.get("readiness"))
    labeled = _rows(readiness.get("candidate_decision_labels"))
    if labeled:
        return labeled
    return []


def _join_nonempty(values: Sequence[object], *, separator: str = " ") -> str:
    parts = [_text(value) for value in values if value not in (None, "", [], {})]
    return separator.join(part for part in parts if part != "n/a")


def _human_label(value: object) -> str:
    return _text(value).replace("_", " ").strip()


def _indexed(rows: Sequence[Mapping[str, object]]) -> list[dict[str, object]]:
    return [{"index": index, **dict(row)} for index, row in enumerate(rows, start=1)]


def _mapping_items(value: Mapping[str, object]) -> list[dict[str, object]]:
    return [{"key": key, "value": item} for key, item in sorted(value.items())]


def _kv_lines(items: Sequence[tuple[str, object]], *, width: int) -> list[str]:
    label_width = min(24, max(14, width // 5))
    value_width = max(20, width - label_width - 3)
    lines: list[str] = []
    for label, value in items:
        text = _text(value)
        wrapped = _wrap(text, value_width)
        first, *rest = wrapped or [""]
        lines.append(f"{label:<{label_width}} : {first}")
        for line in rest:
            lines.append(f"{'':<{label_width}} : {line}")
    return lines


def _table_lines(
    rows: Sequence[Mapping[str, object]],
    columns: Sequence[tuple[str, str, int]],
    *,
    width: int,
    limit: int,
) -> list[str]:
    if not rows:
        return ["No rows."]
    available = max(40, width - (3 * (len(columns) - 1)))
    requested = sum(column[2] for column in columns)
    scale = min(1.0, available / requested) if requested else 1.0
    widths = [max(4, int(column[2] * scale)) for column in columns]
    header = " | ".join(
        _clip(label, column_width).ljust(column_width)
        for (_, label, _), column_width in zip(columns, widths, strict=True)
    )
    separator = "-+-".join("-" * column_width for column_width in widths)
    lines = [header, separator]
    for row in rows[:limit]:
        lines.append(
            " | ".join(
                _clip(row.get(key), column_width).ljust(column_width)
                for (key, _, _), column_width in zip(columns, widths, strict=True)
            )
        )
    if len(rows) > limit:
        lines.append(f"... {len(rows) - limit} more row(s)")
    return lines


def _rule(title: str, width: int, *, char: str = "-") -> str:
    text = f" {title} "
    if len(text) >= width:
        return text[:width]
    right = width - len(text)
    return f"{text}{char * right}"


def _normalize_page(value: str) -> str:
    text = (value or "overview").strip().lower()
    if text.startswith("candidate:") or text.startswith("alert:"):
        return text
    return PAGE_ALIASES.get(text, "help")


def _resolve_width(width: int | None) -> int:
    if width is not None:
        return max(80, min(width, 160))
    return max(80, min(shutil.get_terminal_size((120, 30)).columns, 160))


def _rows(value: object) -> list[Mapping[str, object]]:
    if isinstance(value, Mapping):
        return [value]
    if isinstance(value, list | tuple):
        return [row for row in value if isinstance(row, Mapping)]
    return []


def _mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


def _first_value(*values: object) -> object:
    for value in values:
        if value not in (None, ""):
            return value
    return None


def _optional_int(value: object) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _number_or_zero(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _texts(value: object) -> list[str]:
    if not isinstance(value, list | tuple):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _nested(source: Mapping[str, object], *keys: str) -> object | None:
    value: object = source
    for key in keys:
        if not isinstance(value, Mapping):
            return None
        value = value.get(key)
    return value


def _clip(value: object, width: int) -> str:
    text = _text(value).replace("\n", " ")
    if len(text) <= width:
        return text
    if width <= 3:
        return text[:width]
    return f"{text[: width - 3]}..."


def _wrap(value: str, width: int) -> list[str]:
    words = value.split()
    if not words:
        return [""]
    lines: list[str] = []
    current = ""
    for word in words:
        if len(word) > width:
            if current:
                lines.append(current)
                current = ""
            lines.extend(word[index : index + width] for index in range(0, len(word), width))
            continue
        candidate = f"{current} {word}".strip()
        if len(candidate) > width and current:
            lines.append(current)
            current = word
        else:
            current = candidate
    if current:
        lines.append(current)
    return lines


def _text(value: object) -> str:
    if value is None or value == "":
        return "n/a"
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, list | tuple):
        return ", ".join(_text(item) for item in value) or "n/a"
    if isinstance(value, Mapping):
        return json.dumps(value, default=dashboard_json_default, sort_keys=True)
    return str(value)


def _datetime_or_none(value: object) -> datetime | None:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    if isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    return None

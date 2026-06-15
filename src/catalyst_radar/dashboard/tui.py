from __future__ import annotations

import json
import shlex
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
from textual.widgets import DataTable, Header, Input, Static
from textual.worker import Worker, WorkerState

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
from catalyst_radar.security.audit import AuditLogRepository
from catalyst_radar.security.licenses import redact_restricted_external_payload
from catalyst_radar.storage.broker_repositories import BrokerRepository
from catalyst_radar.storage.budget_repositories import BudgetLedgerRepository
from catalyst_radar.storage.feature_repositories import FeatureRepository
from catalyst_radar.storage.job_repositories import JobLockRepository
from catalyst_radar.storage.provider_repositories import ProviderRepository
from catalyst_radar.storage.repositories import MarketRepository
from catalyst_radar.storage.validation_repositories import ValidationRepository
from catalyst_radar.trading.order_ticket import (
    OrderTicketExecutionError,
    run_workbench_order_ticket,
)
from catalyst_radar.trading.paper_decision import (
    PaperDecisionExecutionError,
    run_paper_decision,
)
from catalyst_radar.trading.platform import build_trading_platform_plan
from catalyst_radar.validation.models import PaperDecision
from catalyst_radar.validation.shadow_mode import shadow_mode_status_payload
from catalyst_radar.validation.value_ledger import (
    build_value_ledger_entry,
    load_value_ledger_candidate_coverage_payload,
    load_value_ledger_entries_payload,
    load_value_ledger_entry_payload,
    load_value_ledger_summary_payload,
    value_ledger_artifact_context,
    value_ledger_write_payload,
)
from catalyst_radar.validation.value_outcomes import (
    load_value_outcome_coverage_payload,
    load_value_outcome_payload,
    load_value_outcomes_payload,
    value_outcome_update_payload,
)

RADAR_RUN_COOLDOWN_LOCK_NAME = "manual_radar_run_cooldown"
DASHBOARD_CANDIDATE_ROW_LIMIT = 200
PAPER_DECISION_RECORDED_EVENT = "paper_decision_recorded"
ORDER_TICKET_PREVIEW_SAVED_EVENT = "telemetry.operator.order_ticket.preview_saved"


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


PRICED_IN_SOURCE_GAP_VALUES = (
    "market_bars",
    "catalyst_events",
    "local_text",
    "options",
    "theme_peer_sector",
    "broker_context",
)

PRICED_IN_DECISION_GAP_VALUES = (
    "candidate_packet",
    "decision_card",
    "options",
    "broker_context",
)


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
        "feature": "Readiness, value, next step",
        "page": "1 Inbox, 2 Gaps",
        "use": "Show research-only vs decision use.",
    },
    {
        "area": "Market data",
        "feature": "Bars, freshness, blockers",
        "page": "1 Inbox, 8 Ops",
        "use": "Check prices before relying.",
    },
    {
        "area": "Radar run",
        "feature": "Run plan, gates, call budget",
        "page": "3 Safe Run",
        "use": "Review calls before executing.",
    },
    {
        "area": "Candidates",
        "feature": "Queue, labels, evidence gaps",
        "page": "4 Candidates",
        "use": "Review stock cases.",
    },
    {
        "area": "Alerts",
        "feature": "Alert rows and filters",
        "page": "5 Alerts",
        "use": "Review research notifications.",
    },
    {
        "area": "IPO/S-1",
        "feature": "SEC rows, terms, risks",
        "page": "6 IPO/S-1",
        "use": "Inspect filing catalysts.",
    },
    {
        "area": "Themes",
        "feature": "Theme aggregation over candidate rows",
        "page": "themes",
        "use": "Spot catalyst clusters.",
    },
    {
        "area": "Validation",
        "feature": "Replay, usefulness, misses",
        "page": "validation",
        "use": "Measure if output helped.",
    },
    {
        "area": "Costs",
        "feature": "Budget and value proof",
        "page": "costs",
        "use": "Keep optional agentic review bounded.",
    },
    {
        "area": "Agent",
        "feature": "Agents SDK review brief",
        "page": "agent",
        "use": "Summarize with spend gates.",
    },
    {
        "area": "Broker",
        "feature": "Read-only Schwab context",
        "page": "broker",
        "use": "Compare candidates to portfolio.",
    },
    {
        "area": "Ops",
        "feature": "Provider health, jobs, modes",
        "page": "ops",
        "use": "Diagnose stale data.",
    },
    {
        "area": "Telemetry",
        "feature": "Audit tape and run coverage",
        "page": "telemetry",
        "use": "Verify run evidence.",
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
    "inbox": "overview",
    "insight": "overview",
    "insights": "overview",
    "mail": "overview",
    "messages": "overview",
    "command-center": "overview",
    "workbench": "overview",
    "o": "overview",
    "overview": "overview",
    "portfolio": "portfolio",
    "portfolio-monitor": "portfolio",
    "portfolio-monitoring": "portfolio",
    "market": "market-radar",
    "market-radar": "market-radar",
    "radar": "market-radar",
    "scout": "market-radar",
    "scanner": "market-radar",
    "trade": "trade-planner",
    "trade-plan": "trade-planner",
    "trade-planner": "trade-planner",
    "planner": "trade-planner",
    "risk": "risk-desk",
    "risk-desk": "risk-desk",
    "risk-controls": "risk-desk",
    "paper": "paper-trading",
    "paper-trade": "paper-trading",
    "paper-trading": "paper-trading",
    "paper-trader": "paper-trading",
    "backtest": "backtest",
    "backtests": "backtest",
    "replay": "backtest",
    "replays": "backtest",
    "2": "readiness",
    "blockers": "readiness",
    "evidence": "readiness",
    "evidence-gaps": "readiness",
    "evidence_gaps": "readiness",
    "gaps": "readiness",
    "ready": "readiness",
    "readiness": "readiness",
    "3": "run",
    "call-plan": "run",
    "call_plan": "run",
    "safe": "run",
    "safe-run": "run",
    "safe_run": "run",
    "run": "run",
    "plan": "run",
    "4": "candidates",
    "c": "candidates",
    "candidate": "candidates",
    "candidate-review": "candidates",
    "candidate_review": "candidates",
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
    "broker-desk": "broker",
    "8": "ops",
    "ops": "ops",
    "9": "telemetry",
    "t": "telemetry",
    "telemetry": "telemetry",
    "10": "agent",
    "agent": "agent",
    "agent-cockpit": "agent",
    "agents": "agent",
    "brief": "agent",
    "themes": "themes",
    "validation": "validation",
    "costs": "costs",
    "features": "features",
    "journal": "journal",
    "journals": "journal",
    "trade-journal": "journal",
    "decision-journal": "journal",
    "help": "help",
}

NAVIGATION_TEXT = (
    "0 Start | 1 Inbox | 2 Evidence Gaps | 3 Safe Run | "
    "4 Candidate Review | 5 Alerts | "
    "6 IPO/S-1 | 7 Broker | 8 Ops | 9 Telemetry | Ctrl+A Agent Coach | review | "
    "features | help | q"
)

MODERN_PAGES: tuple[tuple[str, str, str], ...] = (
    ("tutorial", "0", "Start"),
    ("overview", "1", "Inbox"),
    ("readiness", "2", "Evidence Gaps"),
    ("run", "3", "Safe Run"),
    ("candidates", "4", "Candidate Review"),
    ("review", "", "Decision Review"),
    ("alerts", "5", "Alerts"),
    ("ipo", "6", "IPO/S-1"),
    ("broker", "7", "Broker"),
    ("ops", "8", "Ops"),
    ("telemetry", "9", "Telemetry"),
    ("agent", "^A", "Agent Coach"),
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
    fast_view: bool = False,
) -> dict[str, object]:
    filters = filters.normalized()
    latest_run = dashboard_data.load_radar_run_summary(engine)
    latest_run_cutoff = _datetime_or_none(
        latest_run.get("finished_at") or latest_run.get("decision_available_at")
    )
    data_available_at = filters.available_at or latest_run_cutoff
    priced_in_candidate_rows: list[dict[str, object]] | None = None
    if fast_view:
        priced_in_candidate_rows = (
            dashboard_data.load_radar_run_candidate_rows(
                engine,
                latest_run,
                limit=None,
                include_post_run_artifacts=True,
                include_briefs=False,
            )
            if filters.available_at is None and latest_run
            else dashboard_data.load_candidate_rows(
                engine,
                available_at=data_available_at,
                limit=None,
                include_briefs=False,
            )
        )
        candidate_rows = priced_in_candidate_rows[:DASHBOARD_CANDIDATE_ROW_LIMIT]
    else:
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
    value_ledger = dashboard_data.load_value_ledger_summary(
        engine,
        available_at=data_available_at,
    )
    value_outcomes = dashboard_data.load_value_outcome_summary(engine)
    value_report = dashboard_data.load_monthly_value_report(
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
        candidate_rows=priced_in_candidate_rows if fast_view else None,
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
    priced_in_queue_kwargs: dict[str, object] = {
        "limit": filters.priced_in_limit,
        "offset": filters.priced_in_offset,
        "available_at": filters.available_at,
        "status": filters.priced_in_status,
        "usefulness": filters.priced_in_usefulness,
        "source_gap": filters.priced_in_source_gap,
        "decision_gap": filters.priced_in_decision_gap,
        "stocks_only": filters.priced_in_stocks_only,
        "include_planning_rows": True,
        "latest_run_summary": latest_run,
        "broker_summary": broker_summary,
        "discovery_snapshot": discovery_snapshot,
    }
    if priced_in_candidate_rows is not None:
        priced_in_queue_kwargs["candidate_rows"] = priced_in_candidate_rows
        priced_in_queue_kwargs["total_count"] = len(priced_in_candidate_rows)
    priced_in_queue = dashboard_data.priced_in_queue_payload(
        engine,
        config,
        **priced_in_queue_kwargs,
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
    telemetry_coverage = dashboard_data.telemetry_coverage_payload(engine)
    call_plan = dashboard_data.radar_run_call_plan_payload(engine, config)
    shadow_readiness = dashboard_data.shadow_readiness_payload(
        engine,
        config,
        radar_readiness=readiness_payload,
        priced_in_answer=priced_in_answer,
        call_plan=call_plan,
        ops_health=ops_health,
        validation_summary=validation_summary,
        include_approval_required_unblock=not fast_view,
    )
    shadow_status = shadow_mode_status_payload(
        engine,
        config,
        available_at=data_available_at,
        shadow_readiness=shadow_readiness,
    )
    trial_readiness = dashboard_data.trial_readiness_payload(
        engine,
        config,
        available_at=data_available_at,
        priced_in_answer=priced_in_answer,
        shadow_readiness=shadow_readiness,
        value_report=value_report,
        include_approval_required_unblock=not fast_view,
    )
    approval_required_unblock = _dashboard_approval_required_unblock(
        shadow_readiness=shadow_readiness,
        trial_readiness=trial_readiness,
    )
    top_level_blocker = _dashboard_top_level_blocker_contract(
        shadow_readiness=shadow_readiness,
        priced_in_answer=priced_in_answer,
        operator_next_step=operator_next_step,
    )
    display_priced_in_queue = dict(priced_in_queue)
    display_priced_in_queue.pop("planning_rows", None)
    real_results = _dashboard_real_results_payload(
        latest_run=latest_run,
        priced_in_queue=display_priced_in_queue,
        candidate_rows=candidate_rows,
        discovery_snapshot=discovery_snapshot,
    )
    trading_workbench = _trading_workbench_snapshot_payload(
        engine=engine,
        config=config,
        available_at=data_available_at,
        priced_in_queue=display_priced_in_queue,
        candidate_rows=candidate_rows,
        alert_rows=alert_rows,
        ipo_rows=ipo_rows,
        theme_rows=theme_rows,
        priced_in_answer=priced_in_answer,
        broker_summary=broker_summary,
        validation_summary=validation_summary,
        cost_summary=cost_summary,
        value_ledger=value_ledger,
        value_outcomes=value_outcomes,
        value_report=value_report,
        ops_health=ops_health,
        telemetry=telemetry,
        telemetry_coverage=telemetry_coverage,
        call_plan=call_plan,
        readiness_payload=readiness_payload,
        trial_readiness=trial_readiness,
        shadow_readiness=shadow_readiness,
        runtime_context=runtime_context,
    )
    payload = {
        "schema_version": "dashboard-cli-snapshot-v1",
        "snapshot_mode": "fast_view" if fast_view else "full",
        "status": top_level_blocker["status"],
        "first_blocker": top_level_blocker["first_blocker"],
        "first_gap_count": top_level_blocker["first_gap_count"],
        "canonical_next_action": top_level_blocker["canonical_next_action"],
        "canonical_next_command": top_level_blocker["canonical_next_command"],
        "next_action": top_level_blocker["canonical_next_action"],
        "next_command": top_level_blocker["canonical_next_command"],
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
        "real_results": real_results,
        "readiness": readiness_payload,
        "trial_readiness": trial_readiness,
        "shadow_readiness": shadow_readiness,
        "shadow_mode": shadow_status,
        "radar_run_cooldown": dashboard_data.radar_run_cooldown_payload(engine, config),
        "latest_run": latest_run,
        "discovery_snapshot": discovery_snapshot,
        "actionability_breakdown": actionability,
        "investment_readiness": investment_readiness,
        "operator_work_queue": operator_work_queue,
        "operator_next_step": operator_next_step,
        "approval_required_unblock": approval_required_unblock,
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
        "value_ledger": value_ledger,
        "value_outcomes": value_outcomes,
        "value_report": value_report,
        "live_activation": dashboard_data.live_data_activation_contract_payload(
            config,
            radar_run_summary=latest_run,
            broker_summary=broker_summary,
        ),
        "call_plan": call_plan,
        "broker": broker_summary,
        "trading_workbench": trading_workbench,
        "ops_health": ops_health,
        "telemetry": telemetry,
        "telemetry_coverage": telemetry_coverage,
        "external_calls_made": 0,
    }
    payload["agent_brief"] = run_market_radar_agents(payload, config, real=False)
    _attach_agent_brief_to_workbench(payload)
    redacted = redact_restricted_external_payload(payload)
    return redacted if isinstance(redacted, dict) else payload


def _trading_workbench_snapshot_payload(
    *,
    engine: Engine,
    config: AppConfig,
    available_at: datetime | None,
    priced_in_queue: Mapping[str, object],
    candidate_rows: Sequence[Mapping[str, object]],
    alert_rows: Sequence[Mapping[str, object]],
    ipo_rows: Sequence[Mapping[str, object]],
    theme_rows: Sequence[Mapping[str, object]],
    priced_in_answer: Mapping[str, object],
    broker_summary: Mapping[str, object],
    validation_summary: Mapping[str, object],
    cost_summary: Mapping[str, object],
    value_ledger: Mapping[str, object],
    value_outcomes: Mapping[str, object],
    value_report: Mapping[str, object],
    ops_health: Mapping[str, object],
    telemetry: Mapping[str, object],
    telemetry_coverage: Mapping[str, object],
    call_plan: Mapping[str, object],
    readiness_payload: Mapping[str, object],
    trial_readiness: Mapping[str, object],
    shadow_readiness: Mapping[str, object],
    runtime_context: Mapping[str, object],
) -> dict[str, object]:
    queue_rows = _rows(priced_in_queue.get("rows") or priced_in_queue.get("items"))
    candidate_by_ticker = {
        str(row.get("ticker") or "").strip().upper(): row
        for row in candidate_rows
        if str(row.get("ticker") or "").strip()
    }
    broker_snapshot = _mapping(_mapping(broker_summary.get("snapshot")))
    broker_exposure = _mapping(_mapping(broker_summary.get("exposure")))
    broker_tickets = _rows(broker_summary.get("order_tickets"))
    broker_positions = _rows(broker_summary.get("positions"))
    broker_balances = _rows(broker_summary.get("balances"))
    broker_open_orders = _rows(broker_summary.get("open_orders"))
    broker_triggers = _rows(broker_summary.get("triggers"))
    broker_opportunity_actions = _rows(broker_summary.get("opportunity_actions"))
    validation_report = _mapping(validation_summary.get("report"))
    latest_validation = _mapping(validation_summary.get("latest_run"))
    priced_in_answer_counts = _mapping(priced_in_answer.get("counts"))
    priced_in_decision_readiness = _mapping(
        priced_in_answer.get("decision_readiness")
    )
    paper_rows = _rows(validation_summary.get("paper_trades"))
    useful_labels = _rows(validation_summary.get("useful_labels"))
    ledger_entries = _first_nonnegative_int(
        _mapping(value_ledger.get("summary")).get("entry_count"),
        value_ledger.get("entry_count"),
        len(_rows(value_ledger.get("entries"))),
    )
    outcome_count = _first_nonnegative_int(
        value_outcomes.get("outcome_count"),
        _mapping(value_outcomes.get("summary")).get("outcome_count"),
        len(_rows(value_outcomes.get("rows"))),
    )
    decision_ready_rows = [
        row for row in queue_rows if _workbench_decision_ready(row)
    ]
    decision_card_count = sum(
        1 for row in candidate_rows if row.get("decision_card_id") or row.get("card")
    )
    hard_block_rows = [
        row
        for row in queue_rows
        if bool(row.get("blocked"))
        or _rows(row.get("blockers"))
        or _rows(_mapping(row.get("portfolio_impact")).get("hard_blocks"))
        or _rows(row.get("portfolio_hard_blocks"))
    ]
    first_queue_row = queue_rows[0] if queue_rows else {}
    trial_checks = _rows(trial_readiness.get("checks"))
    shadow_checks = _rows(shadow_readiness.get("checks"))
    broker_connected = bool(
        broker_exposure.get("broker_connected")
        or str(broker_snapshot.get("connection_status") or "").lower()
        in {"connected", "ok", "ready"}
    )
    ops_database = _mapping(ops_health.get("database"))
    ops_stale = _mapping(ops_health.get("stale_data"))
    ops_degraded = _mapping(ops_health.get("degraded_mode"))
    ops_telemetry = _mapping(ops_health.get("telemetry"))
    ops_provider_rows = [
        _workbench_ops_provider_row(row) for row in _rows(ops_health.get("providers"))[:6]
    ]
    ops_job_rows = [
        _workbench_ops_job_row(row) for row in _rows(ops_health.get("jobs"))[:6]
    ]
    ops_call_plan_rows = [
        _workbench_call_plan_row(row) for row in _rows(call_plan.get("rows"))[:8]
    ]
    radar_readiness_check_rows = [
        _workbench_readiness_check_row(row, source="radar_readiness")
        for row in _rows(readiness_payload.get("readiness_checklist"))[:12]
    ]
    telemetry_event_rows = [
        _workbench_telemetry_event_row(row) for row in _rows(telemetry.get("events"))[:8]
    ]
    telemetry_coverage_rows = [
        _workbench_telemetry_coverage_row(row)
        for row in _rows(telemetry_coverage.get("domains"))[:8]
    ]
    ops_call_plan_status = str(call_plan.get("status") or "unknown")
    portfolio_equity = _first_value(
        broker_exposure.get("portfolio_equity"),
        broker_snapshot.get("portfolio_equity"),
    )
    focus_row = _workbench_queue_row(
        first_queue_row,
        candidate_by_ticker=candidate_by_ticker,
    )
    active_plan = _workbench_active_plan_payload(
        engine=engine,
        config=config,
        available_at=available_at,
        focus_row=focus_row,
        broker_exposure=broker_exposure,
    )
    active_risk = _mapping(active_plan.get("risk_approval"))
    active_order = _mapping(active_plan.get("order_intent"))
    trade_setup_rows = _workbench_trade_setup_rows(active_plan)
    trade_sizing_rows = _workbench_trade_sizing_rows(active_plan)
    trade_paper_intent_rows = _workbench_paper_intent_rows(active_plan)
    trade_order_intent_rows = _workbench_order_intent_rows(active_plan)
    risk_approval_rows = _workbench_risk_approval_rows(active_plan)
    agent_capability_rows = [
        _workbench_agent_capability_row(row)
        for row in _rows(active_plan.get("capability_map"))
    ]
    risk_block_rows = [
        *_workbench_active_risk_block_rows(active_risk),
        *[_workbench_queue_risk_block_row(row) for row in hard_block_rows[:5]],
    ]
    readiness_check_rows = sorted(
        [
            *(
                _workbench_readiness_check_row(row, source="shadow_readiness")
                for row in shadow_checks
            ),
            *(
                _workbench_readiness_check_row(row, source="trial_readiness")
                for row in trial_checks
            ),
        ],
        key=_workbench_readiness_check_sort_key,
    )
    readiness_block_count = sum(
        1
        for row in readiness_check_rows
        if str(row.get("status") or "").lower() == "blocked"
    )
    journal_ledger_payload = load_value_ledger_entries_payload(
        engine,
        available_at=available_at,
        limit=5,
    )
    journal_outcomes_payload = load_value_outcomes_payload(
        engine,
        available_at=available_at,
        limit=5,
    )
    journal_ledger_rows = _rows(journal_ledger_payload.get("entries"))
    journal_outcome_source_rows = _rows(journal_outcomes_payload.get("outcomes"))
    journal_entry_rows = [
        _workbench_value_ledger_entry_row(row)
        for row in journal_ledger_rows
    ]
    cost_value_rows = [
        _workbench_value_economics_row(row)
        for row in journal_ledger_rows[:5]
    ]
    journal_outcome_rows = [
        _workbench_value_outcome_row(row)
        for row in journal_outcome_source_rows
    ]
    validation_useful_label_rows = [
        _workbench_useful_label_row(row) for row in useful_labels[:5]
    ]
    validation_result_rows = _workbench_validation_result_rows(
        engine=engine,
        latest_validation=latest_validation,
        available_at=available_at,
    )
    audit_repo = AuditLogRepository(engine)
    paper_execution_audit_rows = [
        _workbench_execution_audit_row(event)
        for event in audit_repo.list_events(
            event_type=PAPER_DECISION_RECORDED_EVENT,
            limit=5,
            newest_first=True,
        )
    ]
    ticket_execution_audit_rows = [
        _workbench_execution_audit_row(event)
        for event in audit_repo.list_events(
            event_type=ORDER_TICKET_PREVIEW_SAVED_EVENT,
            limit=5,
            newest_first=True,
        )
    ]
    paper_trade_rows = [
        _workbench_paper_trade_row(row) for row in paper_rows[:5]
    ]
    portfolio_position_rows = [
        _workbench_portfolio_position_row(row, portfolio_equity=portfolio_equity)
        for row in broker_positions[:5]
    ]
    portfolio_balance_rows = [
        _workbench_portfolio_balance_row(row) for row in broker_balances[:5]
    ]
    portfolio_exposure_rows = _workbench_portfolio_exposure_rows(broker_exposure)
    portfolio_open_order_rows = _workbench_portfolio_open_order_rows(
        broker_open_orders[:5]
    )
    broker_ticket_rows = [
        _workbench_order_ticket_row(row) for row in broker_tickets[:5]
    ]
    trade_lifecycle_rows = _workbench_trade_lifecycle_rows(
        active_plan=active_plan,
        paper_trade_rows=paper_trade_rows,
        paper_execution_audit_rows=paper_execution_audit_rows,
        validation_result_rows=validation_result_rows,
        value_ledger_rows=journal_ledger_rows,
        value_outcome_rows=journal_outcome_source_rows,
    )
    alert_module_rows = [_workbench_alert_row(row) for row in alert_rows[:5]]
    alert_trigger_rows = [
        _workbench_market_trigger_row(row) for row in broker_triggers[:5]
    ]
    candidate_module_rows = [
        _workbench_queue_row(row, candidate_by_ticker=candidate_by_ticker)
        for row in queue_rows[:5]
    ]
    opportunity_action_rows = [
        _workbench_opportunity_action_row(row)
        for row in broker_opportunity_actions[:5]
    ]
    ipo_module_rows = [_workbench_ipo_s1_row(row) for row in ipo_rows[:5]]
    theme_module_rows = [_workbench_theme_row(row) for row in theme_rows[:5]]
    feature_inventory_rows = [
        _workbench_feature_inventory_row(row) for row in DASHBOARD_FEATURES
    ]
    cost_budget_rows = [
        _workbench_budget_ledger_row(row) for row in _rows(cost_summary.get("rows"))[:5]
    ]
    action_bus = _workbench_action_bus_payload(
        active_plan=active_plan,
        trade_lifecycle_rows=trade_lifecycle_rows,
        portfolio_position_rows=portfolio_position_rows,
        portfolio_balance_rows=portfolio_balance_rows,
        portfolio_exposure_rows=portfolio_exposure_rows,
        portfolio_open_order_rows=portfolio_open_order_rows,
        risk_approval_rows=risk_approval_rows,
        agent_action_rows=[
            _workbench_agent_action_row(action, index=index)
            for index, action in enumerate(_rows(_mapping(active_plan).get("next_actions")))
        ],
    )
    workflow_map = _workbench_workflow_map_payload(
        action_bus=action_bus,
        queue_rows=queue_rows,
        candidate_module_rows=candidate_module_rows,
        priced_in_answer=priced_in_answer,
        active_plan=active_plan,
        risk_approval_rows=risk_approval_rows,
        paper_trade_rows=paper_trade_rows,
        broker_ticket_rows=broker_ticket_rows,
        trade_lifecycle_rows=trade_lifecycle_rows,
        validation_result_rows=validation_result_rows,
    )
    priority_queue = _workbench_priority_queue_payload(
        action_bus=action_bus,
        workflow_map=workflow_map,
    )
    supervision_gates = _workbench_supervision_gates_payload(
        action_bus=action_bus,
        priority_queue=priority_queue,
    )
    decision_brief = _workbench_decision_brief_payload(
        focus_row=focus_row,
        active_plan=active_plan,
        workflow_map=workflow_map,
        priority_queue=priority_queue,
        supervision_gates=supervision_gates,
    )
    scenario_matrix = _workbench_scenario_matrix_payload(
        active_plan=active_plan,
        decision_brief=decision_brief,
    )
    risk_envelope = _workbench_risk_envelope_payload(
        active_plan=active_plan,
        decision_brief=decision_brief,
        scenario_matrix=scenario_matrix,
        broker_snapshot=broker_snapshot,
        broker_exposure=broker_exposure,
        broker_connected=broker_connected,
        open_order_count=_first_nonnegative_int(
            broker_snapshot.get("open_order_count"),
            len(broker_open_orders),
        ),
    )
    portfolio_impact_preview = _workbench_portfolio_impact_preview_payload(
        active_plan=active_plan,
        risk_envelope=risk_envelope,
        broker_connected=broker_connected,
    )
    position_sizing = _workbench_position_sizing_payload(
        active_plan=active_plan,
        scenario_matrix=scenario_matrix,
        risk_envelope=risk_envelope,
    )
    capital_allocation = _workbench_capital_allocation_payload(
        active_plan=active_plan,
        risk_envelope=risk_envelope,
        portfolio_impact_preview=portfolio_impact_preview,
        position_sizing=position_sizing,
        portfolio_position_rows=portfolio_position_rows,
        portfolio_open_order_rows=portfolio_open_order_rows,
        paper_trade_rows=paper_trade_rows,
    )
    order_ticket_draft = _workbench_order_ticket_draft_payload(
        active_plan=active_plan,
        position_sizing=position_sizing,
        risk_envelope=risk_envelope,
        action_bus=action_bus,
    )
    paper_trade_preview = _workbench_paper_trade_preview_payload(
        active_plan=active_plan,
        position_sizing=position_sizing,
        order_ticket_draft=order_ticket_draft,
        risk_envelope=risk_envelope,
        action_bus=action_bus,
    )
    pretrade_compliance = _workbench_pretrade_compliance_payload(
        active_plan=active_plan,
        decision_brief=decision_brief,
        risk_envelope=risk_envelope,
        capital_allocation=capital_allocation,
        order_ticket_draft=order_ticket_draft,
        paper_trade_preview=paper_trade_preview,
        supervision_gates=supervision_gates,
    )
    learning_loop = _workbench_learning_loop_payload(
        active_plan=active_plan,
        paper_trade_preview=paper_trade_preview,
        trade_lifecycle_rows=trade_lifecycle_rows,
        validation_result_rows=validation_result_rows,
        journal_entry_rows=journal_entry_rows,
        journal_outcome_rows=journal_outcome_rows,
        useful_label_rows=validation_useful_label_rows,
    )
    strategy_review = _workbench_strategy_review_payload(
        active_plan=active_plan,
        scenario_matrix=scenario_matrix,
        risk_envelope=risk_envelope,
        learning_loop=learning_loop,
    )
    trade_monitor = _workbench_trade_monitor_payload(
        active_plan=active_plan,
        risk_envelope=risk_envelope,
        paper_trade_preview=paper_trade_preview,
        learning_loop=learning_loop,
        strategy_review=strategy_review,
        trade_lifecycle_rows=trade_lifecycle_rows,
        paper_trade_rows=paper_trade_rows,
        portfolio_open_order_rows=portfolio_open_order_rows,
        alert_rows=alert_module_rows,
        trigger_rows=alert_trigger_rows,
    )
    trade_runbook = _workbench_trade_runbook_payload(
        decision_brief=decision_brief,
        scenario_matrix=scenario_matrix,
        risk_envelope=risk_envelope,
        action_bus=action_bus,
    )
    operator_state = _workbench_operator_state_payload(
        decision_brief=decision_brief,
        scenario_matrix=scenario_matrix,
        risk_envelope=risk_envelope,
        trade_runbook=trade_runbook,
        action_bus=action_bus,
        workflow_map=workflow_map,
        priority_queue=priority_queue,
        supervision_gates=supervision_gates,
    )
    execution_sandbox = _workbench_execution_sandbox_payload(
        operator_state=operator_state,
        trade_runbook=trade_runbook,
        action_bus=action_bus,
        risk_envelope=risk_envelope,
    )
    trade_readiness_brief = _workbench_trade_readiness_brief_payload(
        decision_brief=decision_brief,
        risk_envelope=risk_envelope,
        capital_allocation=capital_allocation,
        paper_trade_preview=paper_trade_preview,
        pretrade_compliance=pretrade_compliance,
        learning_loop=learning_loop,
        strategy_review=strategy_review,
        trade_monitor=trade_monitor,
        trade_runbook=trade_runbook,
        operator_state=operator_state,
        execution_sandbox=execution_sandbox,
    )
    agent_playbook = _workbench_agent_playbook_payload(
        trade_readiness_brief=trade_readiness_brief,
        priority_queue=priority_queue,
        action_bus=action_bus,
        execution_sandbox=execution_sandbox,
        operator_state=operator_state,
    )
    market_intelligence_dossier = _workbench_market_intelligence_dossier_payload(
        focus_row=focus_row,
        candidate_rows=candidate_module_rows,
        alert_rows=alert_module_rows,
        trigger_rows=alert_trigger_rows,
        opportunity_action_rows=opportunity_action_rows,
        ipo_rows=ipo_module_rows,
        theme_rows=theme_module_rows,
        decision_brief=decision_brief,
        risk_envelope=risk_envelope,
        trade_readiness_brief=trade_readiness_brief,
        agent_playbook=agent_playbook,
    )
    case_file = _workbench_case_file_payload(
        active_plan=active_plan,
        decision_brief=decision_brief,
        scenario_matrix=scenario_matrix,
        risk_envelope=risk_envelope,
        capital_allocation=capital_allocation,
        paper_trade_preview=paper_trade_preview,
        pretrade_compliance=pretrade_compliance,
        learning_loop=learning_loop,
        strategy_review=strategy_review,
        trade_monitor=trade_monitor,
        workflow_map=workflow_map,
        priority_queue=priority_queue,
        supervision_gates=supervision_gates,
        operator_state=operator_state,
        execution_sandbox=execution_sandbox,
        trade_readiness_brief=trade_readiness_brief,
        agent_playbook=agent_playbook,
        market_intelligence_dossier=market_intelligence_dossier,
    )
    return {
        "schema_version": "trading-workbench-snapshot-v1",
        "external_calls_made": 0,
        "primary_tool": "market-radar",
        "case_file": case_file,
        "active_plan": active_plan,
        "execution_boundary": {
            "live_trading_enabled": False,
            "broker_order_submission": "disabled",
            "autonomous_execution": "disabled",
            "paper_trading": "preview_only",
            "provider_calls_for_browsing": 0,
        },
        "action_bus": action_bus,
        "workflow_map": workflow_map,
        "priority_queue": priority_queue,
        "supervision_gates": supervision_gates,
        "decision_brief": decision_brief,
        "scenario_matrix": scenario_matrix,
        "risk_envelope": risk_envelope,
        "portfolio_impact_preview": portfolio_impact_preview,
        "position_sizing": position_sizing,
        "capital_allocation": capital_allocation,
        "order_ticket_draft": order_ticket_draft,
        "paper_trade_preview": paper_trade_preview,
        "pretrade_compliance": pretrade_compliance,
        "learning_loop": learning_loop,
        "strategy_review": strategy_review,
        "trade_monitor": trade_monitor,
        "trade_runbook": trade_runbook,
        "operator_state": operator_state,
        "execution_sandbox": execution_sandbox,
        "trade_readiness_brief": trade_readiness_brief,
        "agent_playbook": agent_playbook,
        "market_intelligence_dossier": market_intelligence_dossier,
        "modules": {
            "portfolio": {
                "status": "ready" if broker_summary else "blocked",
                "summary": "Read-only portfolio and broker context.",
                "metrics": {
                    "broker_connected": broker_connected,
                    "position_count": _first_nonnegative_int(
                        broker_exposure.get("position_count"),
                        broker_snapshot.get("position_count"),
                    ),
                    "account_count": _first_nonnegative_int(
                        broker_exposure.get("account_count"),
                        broker_snapshot.get("account_count"),
                    ),
                    "portfolio_equity": _first_value(
                        broker_exposure.get("portfolio_equity"),
                        broker_snapshot.get("portfolio_equity"),
                    ),
                    "cash": _first_value(broker_exposure.get("cash")),
                    "buying_power": _first_value(broker_exposure.get("buying_power")),
                    "broker_data_stale": bool(broker_exposure.get("broker_data_stale")),
                    "position_preview_count": len(portfolio_position_rows),
                    "balance_count": len(portfolio_balance_rows),
                    "open_order_count": _first_nonnegative_int(
                        broker_snapshot.get("open_order_count"),
                        len(broker_open_orders),
                    ),
                    "gross_exposure_pct": _nested(
                        broker_exposure,
                        "exposure_before",
                        "gross_exposure_pct",
                    ),
                    "single_name_exposure_count": len(
                        _mapping(
                            _nested(
                                broker_exposure,
                                "exposure_before",
                                "single_name",
                            )
                        )
                    ),
                },
                "positions": portfolio_position_rows,
                "balances": portfolio_balance_rows,
                "exposure_rows": portfolio_exposure_rows,
                "open_order_checks": portfolio_open_order_rows,
                "next_action": (
                    "Review read-only positions before planning risk."
                    if portfolio_position_rows
                    else "Use broker sync only for read-only portfolio context."
                ),
                "source_keys": [
                    "broker.snapshot",
                    "broker.exposure",
                    "broker.positions",
                    "broker.balances",
                    "broker.open_orders",
                ],
            },
            "market-radar": {
                "status": "ready" if queue_rows else "blocked",
                "summary": "Scouted catalyst queue and decision-usefulness triage.",
                "metrics": {
                    "queue_count": _first_nonnegative_int(
                        priced_in_queue.get("returned_count"),
                        priced_in_queue.get("count"),
                        len(queue_rows),
                    ),
                    "total_count": _first_nonnegative_int(
                        priced_in_queue.get("total_count"),
                        len(queue_rows),
                    ),
                    "candidate_count": len(candidate_rows),
                    "alert_count": len(alert_rows),
                },
                "rows": [
                    _workbench_queue_row(row, candidate_by_ticker=candidate_by_ticker)
                    for row in queue_rows[:5]
                ],
                "next_action": "Open the top queue row before planning a trade.",
                "source_keys": ["priced_in_queue", "candidates", "alerts"],
            },
            "candidates": {
                "status": "ready" if candidate_module_rows else "blocked",
                "summary": "Candidate evidence queue for single-name review.",
                "metrics": {
                    "candidate_count": len(candidate_rows),
                    "queue_count": len(queue_rows),
                    "decision_card_count": decision_card_count,
                    "candidate_packet_count": sum(
                        1 for row in candidate_rows if row.get("candidate_packet_id")
                    ),
                    "decision_ready_count": len(decision_ready_rows),
                    "monitor_only_count": sum(
                        1
                        for row in candidate_module_rows
                        if str(row.get("usefulness_status") or "").lower()
                        == "monitor_only"
                    ),
                    "external_calls_made": 0,
                },
                "rows": candidate_module_rows,
                "next_action": (
                    "Open a candidate row and review evidence before planning."
                    if candidate_module_rows
                    else "Run Market Radar before reviewing candidates."
                ),
                "source_keys": [
                    "priced_in_queue.rows",
                    "candidates.rows",
                    "decision_cards",
                    "candidate_packets",
                ],
            },
            "review": {
                "status": priced_in_answer.get("status") or "blocked",
                "summary": priced_in_answer.get("headline")
                or priced_in_answer.get("answer")
                or "Priced-in answer and decision-readiness review.",
                "metrics": {
                    "priced_in_answer_ready": bool(
                        priced_in_answer.get("priced_in_answer_ready")
                    ),
                    "can_make_investment_decision": bool(
                        priced_in_answer.get("can_make_investment_decision")
                    ),
                    "manual_investment_decision_ready": bool(
                        priced_in_answer.get("manual_investment_decision_ready")
                    ),
                    "visible_rows": _first_nonnegative_int(
                        priced_in_answer_counts.get("visible_rows"),
                        len(queue_rows),
                    ),
                    "total_rows": _first_nonnegative_int(
                        priced_in_answer_counts.get("total_rows"),
                        len(queue_rows),
                    ),
                    "decision_ready_rows": _first_nonnegative_int(
                        priced_in_answer_counts.get("decision_ready_rows")
                    ),
                    "actionable_mismatch_rows": _first_nonnegative_int(
                        priced_in_answer_counts.get("actionable_mismatch_rows")
                    ),
                    "trust_blocker_count": len(
                        _rows(priced_in_answer.get("trust_blockers"))
                    ),
                    "decision_readiness_status": priced_in_decision_readiness.get(
                        "status"
                    ),
                    "external_calls_made": 0,
                },
                "rows": candidate_module_rows,
                "next_action": priced_in_answer.get("next_action")
                or "Resolve decision-readiness blockers before acting.",
                "source_keys": [
                    "priced_in_answer",
                    "priced_in_answer.decision_readiness",
                    "priced_in_queue.rows",
                    "priced_in_answer.trust_blockers",
                ],
            },
            "readiness": {
                "status": readiness_payload.get("status") or "blocked",
                "summary": readiness_payload.get("headline")
                or "Readiness blockers and evidence gaps.",
                "metrics": {
                    "readiness_status": readiness_payload.get("status"),
                    "decision_mode": readiness_payload.get("decision_mode"),
                    "safe_to_make_investment_decision": bool(
                        readiness_payload.get("safe_to_make_investment_decision")
                    ),
                    "readiness_check_count": len(radar_readiness_check_rows),
                    "blocked_readiness_check_count": sum(
                        1
                        for row in radar_readiness_check_rows
                        if str(row.get("status") or "").lower() == "blocked"
                    ),
                    "attention_readiness_check_count": sum(
                        1
                        for row in radar_readiness_check_rows
                        if str(row.get("status") or "").lower() == "attention"
                    ),
                    "optional_readiness_check_count": sum(
                        1
                        for row in radar_readiness_check_rows
                        if str(row.get("status") or "").lower() == "optional"
                    ),
                    "external_calls_made": 0,
                },
                "readiness_checks": radar_readiness_check_rows,
                "next_action": readiness_payload.get("next_action")
                or "Clear readiness gaps before relying on output.",
                "source_keys": [
                    "readiness.readiness_checklist",
                    "readiness.investment_readiness",
                    "readiness.operator_next_step",
                ],
            },
            "run": {
                "status": "blocked" if ops_call_plan_status == "blocked" else "ready",
                "summary": call_plan.get("headline")
                or "Safe provider-call plan and execution gates.",
                "metrics": {
                    "call_plan_status": ops_call_plan_status,
                    "call_plan_layer_count": len(ops_call_plan_rows),
                    "blocked_call_plan_layer_count": sum(
                        1
                        for row in ops_call_plan_rows
                        if str(row.get("status") or "").lower() == "blocked"
                    ),
                    "max_external_call_count": _first_nonnegative_int(
                        call_plan.get("max_external_call_count")
                    ),
                    "will_call_external_providers": bool(
                        call_plan.get("will_call_external_providers")
                    ),
                    "external_calls_made": 0,
                },
                "call_plan_rows": ops_call_plan_rows,
                "next_action": call_plan.get("next_action")
                or "Review call plan before executing provider work.",
                "source_keys": [
                    "call_plan",
                    "radar_run.guardrails",
                    "operator_next_step",
                ],
            },
            "alerts": {
                "status": (
                    "ready"
                    if alert_module_rows or alert_trigger_rows or opportunity_action_rows
                    else "blocked"
                ),
                "summary": "Research alerts, saved triggers, and operator routing.",
                "metrics": {
                    "alert_count": len(alert_rows),
                    "dry_run_alert_count": sum(
                        1
                        for row in alert_rows
                        if str(row.get("status") or "").lower() == "dry_run"
                    ),
                    "planned_alert_count": sum(
                        1
                        for row in alert_rows
                        if str(row.get("status") or "").lower() == "planned"
                    ),
                    "trigger_count": len(broker_triggers),
                    "active_trigger_count": sum(
                        1
                        for row in broker_triggers
                        if str(row.get("status") or "").lower() == "active"
                    ),
                    "opportunity_action_count": len(broker_opportunity_actions),
                    "external_calls_made": 0,
                },
                "alerts": alert_module_rows,
                "triggers": alert_trigger_rows,
                "opportunity_actions": opportunity_action_rows,
                "next_action": (
                    "Open alert evidence or review saved local trigger rules."
                    if alert_module_rows or alert_trigger_rows
                    else "Use alerts only after reviewed research evidence exists."
                ),
                "source_keys": [
                    "alerts.rows",
                    "broker.triggers",
                    "broker.opportunity_actions",
                ],
            },
            "ipo": {
                "status": "ready" if ipo_module_rows else "blocked",
                "summary": "IPO/S-1 primary-source catalyst evidence.",
                "metrics": {
                    "ipo_s1_count": len(ipo_rows),
                    "s1_filing_count": sum(
                        1
                        for row in ipo_rows
                        if str(row.get("form_type") or "").upper() == "S-1"
                    ),
                    "primary_source_count": sum(
                        1
                        for row in ipo_rows
                        if str(row.get("source") or "").lower() in {"sec", "sec edgar"}
                    ),
                    "external_calls_made": 0,
                },
                "ipo_s1_rows": ipo_module_rows,
                "next_action": (
                    "Open S-1 terms and risk flags as research evidence."
                    if ipo_module_rows
                    else "Continue with Market Radar until IPO/S-1 rows are available."
                ),
                "source_keys": ["ipo_s1.rows", "events.payload.ipo_analysis"],
            },
            "themes": {
                "status": "ready" if theme_module_rows else "blocked",
                "summary": "Clustered catalyst themes across candidate rows.",
                "metrics": {
                    "theme_count": len(theme_rows),
                    "themed_candidate_count": sum(
                        _first_nonnegative_int(row.get("candidate_count"))
                        for row in theme_rows
                    ),
                    "top_theme": (
                        theme_module_rows[0].get("theme") if theme_module_rows else None
                    ),
                    "external_calls_made": 0,
                },
                "theme_rows": theme_module_rows,
                "next_action": (
                    "Compare theme concentration before selecting a ticker."
                    if theme_module_rows
                    else "Continue with Market Radar until theme clusters are available."
                ),
                "source_keys": ["themes.rows", "signal_features.payload.candidate.metadata"],
            },
            "features": {
                "status": "ready",
                "summary": "Feature inventory and local platform routing.",
                "metrics": {
                    "feature_count": len(feature_inventory_rows),
                    "feature_area_count": len(
                        {
                            str(row.get("area") or "")
                            for row in feature_inventory_rows
                            if row.get("area")
                        }
                    ),
                    "external_calls_made": 0,
                },
                "feature_rows": feature_inventory_rows,
                "next_action": "Use the inventory to route work to the right local module.",
                "source_keys": [
                    "feature_inventory",
                    "dashboard.page_aliases",
                    "desktop.platform_manifest",
                ],
            },
            "costs": {
                "status": "ready",
                "summary": "Operating cost, budget ledger, and decision-support value.",
                "metrics": {
                    "total_actual_cost_usd": _first_value(
                        cost_summary.get("total_actual_cost_usd"),
                        0.0,
                    ),
                    "total_estimated_cost_usd": _first_value(
                        cost_summary.get("total_estimated_cost_usd"),
                        0.0,
                    ),
                    "validation_total_cost_usd": _first_value(
                        cost_summary.get("validation_total_cost_usd"),
                        0.0,
                    ),
                    "budget_attempt_count": _first_nonnegative_int(
                        cost_summary.get("attempt_count")
                    ),
                    "useful_alert_count": _first_nonnegative_int(
                        cost_summary.get("useful_alert_count")
                    ),
                    "cost_per_useful_alert": cost_summary.get(
                        "cost_per_useful_alert"
                    ),
                    "value_ledger_entry_count": ledger_entries,
                    "confidence_weighted_value_usd": value_ledger.get(
                        "confidence_weighted_value_usd"
                    ),
                    "net_confidence_weighted_value_usd": value_ledger.get(
                        "net_confidence_weighted_value_usd"
                    ),
                    "monthly_value_status": (
                        value_report.get("status") or value_report.get("verdict")
                    ),
                    "external_calls_made": 0,
                },
                "budget_rows": cost_budget_rows,
                "value_economics_rows": cost_value_rows,
                "caps": _mapping(cost_summary.get("caps")),
                "next_action": (
                    value_report.get("canonical_next_action")
                    or "Compare budget spend with decision-support value."
                ),
                "source_keys": ["costs", "budget_ledger", "value_ledger", "value_report"],
            },
            "trade-planner": {
                "status": (
                    "ready" if decision_ready_rows or decision_card_count else "blocked"
                ),
                "summary": "Decision-card trade-plan readiness and next candidate.",
                "metrics": {
                    "decision_ready_count": len(decision_ready_rows),
                    "decision_card_count": decision_card_count,
                    "queue_count": len(queue_rows),
                    "active_plan_status": active_plan.get("status"),
                    "active_plan_autonomy": active_plan.get("autonomy_level"),
                    "trade_setup_count": len(trade_setup_rows),
                    "sizing_row_count": len(trade_sizing_rows),
                    "paper_intent_count": len(trade_paper_intent_rows),
                    "order_intent_count": len(trade_order_intent_rows),
                    "missing_trade_plan_count": sum(
                        1
                        for row in queue_rows
                        if row.get("decision_gap") == "trade_plan"
                        or "trade_plan" in _rows(row.get("missing_trade_plan"))
                    ),
                },
                "focus": focus_row,
                "active_plan": active_plan,
                "trade_setup_rows": trade_setup_rows,
                "sizing_rows": trade_sizing_rows,
                "paper_intent_rows": trade_paper_intent_rows,
                "order_intent_rows": trade_order_intent_rows,
                "next_action": "Build or review a decision card before paper intent.",
                "source_keys": [
                    "priced_in_queue.rows",
                    "decision_cards",
                    "trading_workbench.active_plan.strategy_proposal",
                    "trading_workbench.active_plan.order_intent",
                    "trading_workbench.active_plan.paper_decision",
                    "trading_workbench.active_plan.order_ticket",
                ],
            },
            "risk-desk": {
                "status": (
                    "ready"
                    if not hard_block_rows
                    and not risk_block_rows
                    and readiness_block_count == 0
                    else "blocked"
                ),
                "summary": "Portfolio impact, hard blocks, and readiness gates.",
                "metrics": {
                    "hard_block_count": len(hard_block_rows),
                    "risk_block_count": len(risk_block_rows),
                    "readiness_check_count": len(readiness_check_rows),
                    "readiness_block_count": readiness_block_count,
                    "trial_block_count": sum(
                        1 for check in trial_checks if check.get("status") == "blocked"
                    ),
                    "shadow_block_count": sum(
                        1 for check in shadow_checks if check.get("status") == "blocked"
                    ),
                    "paper_trade_block_count": len(
                        _texts(active_risk.get("paper_trade_blocks"))
                    ),
                    "live_submission_block_count": len(
                        _texts(active_risk.get("live_submission_blocks"))
                    ),
                    "risk_approval_row_count": len(risk_approval_rows),
                },
                "active_plan": active_plan,
                "risk_approval_rows": risk_approval_rows,
                "risk_blocks": risk_block_rows,
                "readiness_checks": readiness_check_rows,
                "next_action": "Clear hard blocks before paper or live consideration.",
                "source_keys": [
                    "trial_readiness",
                    "shadow_readiness",
                    "portfolio_impact",
                    "trading_workbench.active_plan.risk_approval",
                    "trading_workbench.active_plan.execution_controls",
                ],
            },
            "paper-trading": {
                "status": "ready" if paper_rows else "blocked",
                "summary": "Paper-only execution history and supervised intent boundary.",
                "metrics": {
                    "paper_trade_count": len(paper_rows),
                    "open_paper_trade_count": sum(
                        1
                        for row in paper_rows
                        if str(row.get("state") or "").lower() == "open"
                    ),
                    "latest_trade_id": (
                        paper_trade_rows[0].get("id")
                        if paper_trade_rows
                        else None
                    ),
                    "approved_for_paper_trade": bool(
                        active_risk.get("approved_for_paper_trade")
                    ),
                    "approved_for_live_submission": False,
                    "execution_audit_event_count": len(paper_execution_audit_rows),
                    "latest_execution_audit_id": (
                        paper_execution_audit_rows[0].get("id")
                        if paper_execution_audit_rows
                        else None
                    ),
                    "trade_lifecycle_count": len(trade_lifecycle_rows),
                },
                "active_plan": active_plan,
                "trade_lifecycle_rows": trade_lifecycle_rows,
                "paper_trades": paper_trade_rows,
                "execution_audit_rows": paper_execution_audit_rows,
                "next_action": (
                    "Review local paper outcomes; broker submission remains disabled."
                    if paper_trade_rows
                    else "Use paper execution only after risk approval."
                ),
                "source_keys": [
                    "validation.paper_trades",
                    f"audit_events.{PAPER_DECISION_RECORDED_EVENT}",
                    "trading_workbench.trade_lifecycle_rows",
                ],
            },
            "broker": {
                "status": "read_only",
                "summary": (
                    "Broker desk shows read-only context and blocked local tickets; "
                    "order submission is disabled."
                ),
                "metrics": {
                    "broker_connected": broker_connected,
                    "order_ticket_count": len(broker_tickets),
                    "blocked_order_ticket_count": sum(
                        1
                        for row in broker_ticket_rows
                        if str(row.get("status") or "").lower() == "blocked"
                    ),
                    "latest_ticket_id": (
                        broker_ticket_rows[0].get("id")
                        if broker_ticket_rows
                        else None
                    ),
                    "order_submission_allowed": bool(
                        active_order.get("submission_allowed")
                    ),
                    "broker_order_submitted": bool(
                        active_order.get("broker_order_submitted")
                    ),
                    "ticket_audit_event_count": len(ticket_execution_audit_rows),
                    "latest_ticket_audit_id": (
                        ticket_execution_audit_rows[0].get("id")
                        if ticket_execution_audit_rows
                        else None
                    ),
                },
                "order_tickets": broker_ticket_rows,
                "ticket_audit_rows": ticket_execution_audit_rows,
                "next_action": (
                    "Review blocked local tickets; broker submission remains disabled."
                    if broker_ticket_rows
                    else "Authenticate only when portfolio context is needed."
                ),
                "source_keys": [
                    "broker",
                    "broker.order_tickets",
                    f"audit_events.{ORDER_TICKET_PREVIEW_SAVED_EVENT}",
                ],
            },
            "backtest": {
                "status": "ready" if validation_report or latest_validation else "blocked",
                "summary": "Validation, replay, and historical evidence.",
                "metrics": {
                    "latest_validation_run": latest_validation.get("id"),
                    "candidate_count": _first_nonnegative_int(
                        validation_report.get("candidate_count")
                    ),
                    "paper_trade_count": len(paper_rows),
                    "validation_result_preview_count": len(validation_result_rows),
                    "trade_lifecycle_count": len(trade_lifecycle_rows),
                },
                "trade_lifecycle_rows": trade_lifecycle_rows,
                "validation_results": validation_result_rows,
                "next_action": (
                    "Compare candidate logic against local validation evidence."
                    if validation_result_rows
                    else "Compare candidate logic against validation evidence."
                ),
                "source_keys": [
                    "validation.latest_run",
                    "validation.report",
                    "trading_workbench.trade_lifecycle_rows",
                ],
            },
            "validation": {
                "status": "ready" if validation_report or latest_validation else "blocked",
                "summary": "Validation quality gates and useful-alert evidence.",
                "metrics": {
                    "latest_validation_run": latest_validation.get("id"),
                    "latest_validation_status": latest_validation.get("status"),
                    "candidate_count": _first_nonnegative_int(
                        validation_report.get("candidate_count")
                    ),
                    "false_positive_count": _first_nonnegative_int(
                        validation_report.get("false_positive_count")
                    ),
                    "useful_alert_rate": validation_report.get("useful_alert_rate"),
                    "useful_label_count": len(useful_labels),
                    "leakage_failure_count": _first_nonnegative_int(
                        validation_report.get("leakage_failure_count")
                    ),
                    "paper_trade_count": len(paper_rows),
                    "validation_result_preview_count": len(validation_result_rows),
                    "external_calls_made": 0,
                },
                "validation_results": validation_result_rows,
                "useful_label_rows": validation_useful_label_rows,
                "next_action": (
                    "Review false positives, useful labels, and leakage flags."
                    if validation_report or validation_result_rows
                    else "Run validation before trusting strategy changes."
                ),
                "source_keys": [
                    "validation.latest_run",
                    "validation.report",
                    "validation.useful_labels",
                ],
            },
            "journal": {
                "status": (
                    "ready" if ledger_entries or outcome_count or useful_labels else "blocked"
                ),
                "summary": "Decision feedback, value ledger, and outcomes.",
                "metrics": {
                    "value_ledger_entry_count": ledger_entries,
                    "outcome_count": outcome_count,
                    "journal_entry_preview_count": len(journal_entry_rows),
                    "journal_outcome_preview_count": len(journal_outcome_rows),
                    "feedback_label_count": len(useful_labels),
                    "monthly_value_status": value_report.get("status"),
                    "trade_lifecycle_count": len(trade_lifecycle_rows),
                    "linked_outcome_count": sum(
                        1 for row in trade_lifecycle_rows if row.get("outcome_id")
                    ),
                },
                "trade_lifecycle_rows": trade_lifecycle_rows,
                "value_ledger_entries": journal_entry_rows,
                "value_outcomes": journal_outcome_rows,
                "next_action": (
                    "Review local value ledger and outcome evidence."
                    if journal_entry_rows or journal_outcome_rows
                    else "Record feedback and outcome evidence locally."
                ),
                "source_keys": [
                    "value_ledger",
                    "value_outcomes",
                    "value_report",
                    "trading_workbench.trade_lifecycle_rows",
                ],
            },
            "ops": {
                "status": (
                    "blocked"
                    if bool(ops_degraded.get("enabled"))
                    or ops_call_plan_status == "blocked"
                    else "ready"
                ),
                "summary": "Provider health, runtime context, and call boundaries.",
                "metrics": {
                    "database_status": ops_database.get("status"),
                    "candidate_state_count": _first_nonnegative_int(
                        ops_database.get("candidate_state_count")
                    ),
                    "provider_count": len(ops_provider_rows),
                    "stale_provider_count": len(_texts(ops_stale.get("providers"))),
                    "degraded_mode_enabled": bool(ops_degraded.get("enabled")),
                    "job_count": len(_rows(ops_health.get("jobs"))),
                    "telemetry_event_count": _first_nonnegative_int(
                        ops_telemetry.get("event_count")
                    ),
                    "call_plan_status": ops_call_plan_status,
                    "max_external_call_count": _first_nonnegative_int(
                        call_plan.get("max_external_call_count")
                    ),
                    "will_call_external_providers": bool(
                        call_plan.get("will_call_external_providers")
                    ),
                    "openai_key_configured": bool(
                        runtime_context.get("openai_key_configured")
                    ),
                    "schwab_credentials_configured": bool(
                        runtime_context.get("schwab_credentials_configured")
                    ),
                    "external_calls_made": 0,
                },
                "provider_rows": ops_provider_rows,
                "job_rows": ops_job_rows,
                "call_plan_rows": ops_call_plan_rows,
                "runtime": {
                    "environment": runtime_context.get("environment"),
                    "database": runtime_context.get("database"),
                    "daily_market_provider": runtime_context.get(
                        "daily_market_provider"
                    ),
                    "daily_event_provider": runtime_context.get(
                        "daily_event_provider"
                    ),
                    "evidence": runtime_context.get("evidence"),
                },
                "next_action": (
                    "Resolve stale provider or data-health blockers before agent expansion."
                    if bool(ops_degraded.get("enabled"))
                    else call_plan.get("next_action")
                    or "Review runtime health before executing provider work."
                ),
                "source_keys": ["ops_health", "runtime_context", "call_plan"],
            },
            "telemetry": {
                "status": telemetry_coverage.get("status")
                or telemetry.get("status")
                or "missing",
                "summary": (
                    telemetry_coverage.get("headline")
                    or telemetry.get("headline")
                    or "Telemetry audit status."
                ),
                "metrics": {
                    "event_count": _first_nonnegative_int(
                        telemetry.get("event_count")
                    ),
                    "rendered_event_count": len(telemetry_event_rows),
                    "attention_count": _first_nonnegative_int(
                        telemetry.get("attention_count")
                    ),
                    "guarded_count": _first_nonnegative_int(
                        telemetry.get("guarded_count")
                    ),
                    "total_coverage_event_count": _first_nonnegative_int(
                        telemetry_coverage.get("total_event_count")
                    ),
                    "missing_required_count": _first_nonnegative_int(
                        telemetry_coverage.get("missing_required_count")
                    ),
                    "ready_required_domain_count": _first_nonnegative_int(
                        telemetry_coverage.get("ready_required_domain_count")
                    ),
                    "required_domain_count": _first_nonnegative_int(
                        telemetry_coverage.get("required_domain_count")
                    ),
                    "external_calls_made": 0,
                },
                "telemetry_events": telemetry_event_rows,
                "coverage_domains": telemetry_coverage_rows,
                "next_action": (
                    telemetry_coverage.get("next_action")
                    or telemetry.get("next_action")
                    or "Review telemetry before relying on automation."
                ),
                "source_keys": [
                    "telemetry.events",
                    "telemetry_coverage.domains",
                    "audit_events",
                ],
            },
            "agent": {
                "status": "preview_only",
                "summary": "Agent cockpit remains preview and budget-gated.",
                "metrics": {
                    "agent_sdk_enabled": bool(
                        _mapping(runtime_context.get("agent")).get("agent_sdk_enabled")
                    ),
                    "external_calls_made": 0,
                    "capability_count": len(agent_capability_rows),
                    "ready_capability_count": sum(
                        1
                        for row in agent_capability_rows
                        if str(row.get("status") or "").lower()
                        in {"available", "ready"}
                    ),
                    "blocked_capability_count": sum(
                        1
                        for row in agent_capability_rows
                        if str(row.get("status") or "").lower()
                        not in {"available", "ready"}
                    ),
                },
                "capability_map": agent_capability_rows,
                "next_action": (
                    "Review agent capabilities; execution remains gated."
                    if agent_capability_rows
                    else "Preview agent reasoning; execute remains gated."
                ),
                "source_keys": [
                    "runtime_context",
                    "agent_brief",
                    "trading_workbench.case_file",
                    "trading_workbench.agent_playbook",
                    "trading_workbench.market_intelligence_dossier",
                    "trading_workbench.active_plan.capability_map",
                ],
            },
        },
    }


def _attach_agent_brief_to_workbench(payload: dict[str, object]) -> None:
    workbench = payload.get("trading_workbench")
    if not isinstance(workbench, dict):
        return
    modules = workbench.get("modules")
    if not isinstance(modules, dict):
        return
    modules["agent"] = _workbench_agent_brief_module(
        existing=_mapping(modules.get("agent")),
        agent_brief=_mapping(payload.get("agent_brief")),
        runtime_context=_mapping(payload.get("runtime_context")),
    )


def _workbench_agent_brief_module(
    *,
    existing: Mapping[str, object],
    agent_brief: Mapping[str, object],
    runtime_context: Mapping[str, object],
) -> dict[str, object]:
    capability_rows = _rows(existing.get("capability_map"))
    runtime = _mapping(agent_brief.get("runtime"))
    runtime_agent = _mapping(runtime_context.get("agent"))
    credit_gate = _mapping(agent_brief.get("credit_gate"))
    real_results = _mapping(agent_brief.get("real_results"))
    planned_calls = _mapping(agent_brief.get("external_calls_planned"))
    made_calls = _mapping(agent_brief.get("external_calls_made"))
    security_check_rows = [
        _workbench_agent_security_check_row(row)
        for row in _rows(agent_brief.get("security_checks"))[:8]
    ]
    contribution_rows = [
        _workbench_agent_contribution_row(row)
        for row in _rows(agent_brief.get("agents"))[:6]
    ]
    action_rows = [
        _workbench_agent_action_row(action, index=index)
        for index, action in enumerate(_texts(agent_brief.get("next_actions"))[:8], start=1)
    ]
    insight_rows = [
        _workbench_agent_insight_row(insight, index=index)
        for index, insight in enumerate(_texts(agent_brief.get("insights"))[:8], start=1)
    ]
    blocked_security_checks = sum(
        1
        for row in security_check_rows
        if str(row.get("status") or "").lower() == "blocked"
    )
    total_external_calls_made = sum(
        _first_nonnegative_int(value) for value in made_calls.values()
    )
    return {
        "status": agent_brief.get("status") or existing.get("status") or "preview_only",
        "summary": (
            f"Agent cockpit {agent_brief.get('mode') or 'preview'} brief; "
            f"{agent_brief.get('decision_boundary') or 'execution gated'}."
        ),
        "metrics": {
            "agent_brief_status": agent_brief.get("status"),
            "agent_mode": agent_brief.get("mode"),
            "agent_sdk_enabled": bool(runtime_agent.get("agent_sdk_enabled")),
            "capability_count": len(capability_rows),
            "ready_capability_count": sum(
                1
                for row in capability_rows
                if str(row.get("status") or "").lower() in {"available", "ready"}
            ),
            "blocked_capability_count": sum(
                1
                for row in capability_rows
                if str(row.get("status") or "").lower() not in {"available", "ready"}
            ),
            "agent_contribution_count": len(contribution_rows),
            "insight_count": len(insight_rows),
            "next_action_count": len(action_rows),
            "security_check_count": len(security_check_rows),
            "blocked_security_check_count": blocked_security_checks,
            "credit_gate_status": credit_gate.get("status"),
            "real_results_status": real_results.get("status"),
            "real_results_gate_status": runtime.get("real_results_gate_status"),
            "external_openai_calls_planned": _first_nonnegative_int(
                planned_calls.get("openai")
            ),
            "external_openai_calls_made": _first_nonnegative_int(
                made_calls.get("openai")
            ),
            "external_market_calls_planned": _first_nonnegative_int(
                planned_calls.get("market_data")
            ),
            "external_market_calls_made": _first_nonnegative_int(
                made_calls.get("market_data")
            ),
            "external_calls_made": total_external_calls_made,
        },
        "runtime": {
            "orchestrator": runtime.get("orchestrator"),
            "provider": runtime.get("provider"),
            "tool_surface": runtime.get("tool_surface"),
            "real_mode_gate_status": runtime.get("real_mode_gate_status"),
            "real_results_gate_status": runtime.get("real_results_gate_status"),
            "credit_gate_status": runtime.get("credit_gate_status"),
            "max_turns": runtime.get("max_turns"),
        },
        "credit_gate": {
            "status": credit_gate.get("status"),
            "estimated_cost_usd": credit_gate.get("estimated_cost_usd"),
            "max_openai_calls": credit_gate.get("max_openai_calls"),
            "missing": _texts(credit_gate.get("missing")),
            "next_action": credit_gate.get("next_action"),
        },
        "decision_boundary": agent_brief.get("decision_boundary"),
        "agent_preview_command": "agent",
        "agent_execute_boundary_command": "agent execute",
        "primary_command": "agent",
        "capability_map": capability_rows,
        "agent_contributions": contribution_rows,
        "agent_actions": action_rows,
        "agent_insights": insight_rows,
        "security_checks": security_check_rows,
        "next_action": (
            action_rows[0].get("action")
            if action_rows
            else credit_gate.get("next_action")
            or existing.get("next_action")
            or "Preview agent reasoning; execute remains gated."
        ),
        "source_keys": [
            "agent_brief",
            "agent_brief.agents",
            "agent_brief.next_actions",
            "agent_brief.security_checks",
            "trading_workbench.case_file",
            "trading_workbench.agent_playbook",
            "trading_workbench.market_intelligence_dossier",
            "trading_workbench.active_plan.capability_map",
        ],
    }


def _workbench_active_plan_payload(
    *,
    engine: Engine,
    config: AppConfig,
    available_at: datetime | None,
    focus_row: Mapping[str, object],
    broker_exposure: Mapping[str, object],
) -> dict[str, object]:
    decision_card_id = str(focus_row.get("decision_card_id") or "").strip()
    if not decision_card_id:
        return _missing_workbench_active_plan("no_decision_card")
    cutoff = available_at or datetime.now(UTC)
    card = ValidationRepository(engine).decision_card_payload(
        decision_card_id,
        available_at=cutoff,
    )
    if card is None:
        return _missing_workbench_active_plan(
            "decision_card_not_found",
            decision_card_id=decision_card_id,
        )
    plan = build_trading_platform_plan(
        card,
        available_at=cutoff,
        config=config,
        broker_data_stale=bool(broker_exposure.get("broker_data_stale")),
    ).to_payload()
    compact_plan = _compact_workbench_active_plan(plan)
    compact_plan["portfolio_impact"] = _workbench_active_plan_portfolio_impact(card)
    return compact_plan


def _missing_workbench_active_plan(
    reason: str,
    *,
    decision_card_id: str | None = None,
) -> dict[str, object]:
    return {
        "schema_version": "trading-workbench-active-plan-v1",
        "status": "missing",
        "reason": reason,
        "decision_card_id": decision_card_id,
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "no_execution": True,
        "next_action": "Review a MarketRadar decision card before planning a trade.",
    }


def _compact_workbench_active_plan(plan: Mapping[str, object]) -> dict[str, object]:
    strategy = _mapping(plan.get("strategy_proposal"))
    risk = _mapping(plan.get("risk_approval"))
    risk_limits = _mapping(risk.get("limits"))
    order = _mapping(plan.get("order_intent"))
    controls = _mapping(plan.get("execution_controls"))
    supervision = _mapping(plan.get("supervision"))
    paper_intent = _mapping(plan.get("agentic_paper_intent"))
    paper_decision = _mapping(paper_intent.get("paper_decision"))
    ticket_entry_price = _first_value(
        order.get("limit_price"),
        strategy.get("entry_price"),
    )
    ticket_invalidation_price = _first_value(
        order.get("stop_price"),
        order.get("invalidation_price"),
        strategy.get("invalidation_price"),
    )
    paper_hard_blocks = list(
        dict.fromkeys(
            [
                *_texts(paper_intent.get("hard_blocks")),
                *_texts(risk.get("paper_trade_blocks")),
            ]
        )
    )
    return {
        "schema_version": "trading-workbench-active-plan-v1",
        "source_schema_version": plan.get("schema_version"),
        "status": plan.get("status"),
        "autonomy_level": plan.get("autonomy_level"),
        "decision_card_id": plan.get("decision_card_id"),
        "ticker": plan.get("ticker"),
        "recommended_paper_decision": paper_intent.get("recommended_paper_decision"),
        "external_calls_made": plan.get("external_calls_made", 0),
        "db_writes_made": plan.get("db_writes_made", 0),
        "broker_order_submitted": plan.get("broker_order_submitted", False),
        "order_submission_allowed": plan.get("order_submission_allowed", False),
        "no_execution": plan.get("no_execution", True),
        "strategy_proposal": strategy,
        "risk_approval": {
            "approved_for_paper_trade": bool(risk.get("approved_for_paper_trade")),
            "approved_for_live_submission": False,
            "paper_trade_blocks": _texts(risk.get("paper_trade_blocks")),
            "live_submission_blocks": _texts(risk.get("live_submission_blocks")),
            "portfolio_hard_blocks": _texts(risk.get("portfolio_hard_blocks")),
            "estimated_max_loss": risk.get("estimated_max_loss"),
            "requires_manual_approval": bool(risk.get("requires_manual_approval")),
            "live_submission_reason": risk.get("live_submission_reason"),
        },
        "order_intent": {
            "route": order.get("route"),
            "side": order.get("side"),
            "quantity": order.get("quantity"),
            "limit_price": order.get("limit_price"),
            "stop_price": order.get("stop_price"),
            "estimated_notional": order.get("estimated_notional"),
            "estimated_max_loss": order.get("estimated_max_loss"),
            "submission_allowed": bool(order.get("submission_allowed")),
            "broker_order_submitted": bool(order.get("broker_order_submitted")),
        },
        "execution_controls": {
            "external_calls_made": controls.get("external_calls_made", 0),
            "db_writes_made": controls.get("db_writes_made", 0),
            "broker_order_submitted": bool(controls.get("broker_order_submitted")),
            "order_submission_allowed": bool(controls.get("order_submission_allowed")),
            "no_execution": bool(controls.get("no_execution", True)),
            "live_trading_kill_switch": controls.get("live_trading_kill_switch"),
            "broker_adapter_mode": controls.get("broker_adapter_mode"),
        },
        "paper_decision": {
            "decision_card_id": paper_decision.get(
                "decision_card_id",
                plan.get("decision_card_id"),
            ),
            "decision": paper_decision.get("decision"),
            "available_at": paper_decision.get("available_at"),
            "entry_price": paper_decision.get("entry_price"),
            "entry_at": paper_decision.get("entry_at"),
            "hard_blocks": paper_hard_blocks,
            "external_calls_required": paper_intent.get("external_calls_required", 0),
            "external_calls_made": paper_intent.get("external_calls_made", 0),
            "db_writes_required": paper_intent.get("db_writes_required", 0),
            "db_writes_made": paper_intent.get("db_writes_made", 0),
            "broker_order_submitted": bool(
                paper_intent.get("broker_order_submitted", False)
            ),
            "order_submission_allowed": bool(
                paper_intent.get("order_submission_allowed", False)
            ),
            "no_execution": bool(paper_intent.get("no_execution", True)),
            "preview_command": paper_decision.get(
                "preview_command",
                supervision.get("paper_decision_preview_command"),
            ),
            "execute_command": paper_decision.get(
                "execute_command",
                supervision.get("paper_decision_execute_command"),
            ),
        },
        "order_ticket": {
            "ticker": plan.get("ticker") or strategy.get("ticker"),
            "side": order.get("side"),
            "entry_price": ticket_entry_price,
            "invalidation_price": ticket_invalidation_price,
            "risk_per_trade_pct": risk_limits.get("risk_per_trade_pct"),
            "available_at": plan.get("available_at")
            or paper_decision.get("available_at"),
            "external_calls_required": 0,
            "external_calls_made": 0,
            "db_writes_required": 1,
            "db_writes_made": 0,
            "broker_order_submitted": False,
            "submission_allowed": False,
            "no_execution": True,
            "preview_command": "order-ticket preview",
            "record_command": "order-ticket record",
        },
        "supervision": {
            "requires_manual_approval": bool(supervision.get("requires_manual_approval")),
            "no_autonomous_execution": bool(supervision.get("no_autonomous_execution")),
            "paper_decision_preview_command": supervision.get(
                "paper_decision_preview_command"
            ),
            "paper_decision_execute_command": supervision.get(
                "paper_decision_execute_command"
            ),
        },
        "capability_map": _rows(plan.get("capability_map")),
        "next_action": plan.get("next_action"),
    }


def _workbench_active_plan_portfolio_impact(card: Mapping[str, object]) -> dict[str, object]:
    card_payload = _mapping(card.get("payload"))
    identity = _mapping(card_payload.get("identity"))
    impact = _mapping(card_payload.get("portfolio_impact"))
    if not impact:
        return {
            "external_calls_made": 0,
            "db_writes_made": 0,
            "broker_order_submitted": False,
            "order_submission_allowed": False,
        }
    return {
        "ticker": _first_value(identity.get("ticker"), card.get("ticker")),
        "proposed_notional": _workbench_round_float(
            _optional_float(impact.get("proposed_notional"))
        ),
        "max_loss": _workbench_round_float(_optional_float(impact.get("max_loss"))),
        "portfolio_penalty": _workbench_round_float(
            _optional_float(impact.get("portfolio_penalty"))
        ),
        "hard_blocks": _texts(impact.get("hard_blocks")),
        "exposures": {
            "single_name": _workbench_portfolio_impact_exposure(
                impact,
                flat_prefix="single_name",
                nested_key="single_name",
            ),
            "sector": _workbench_portfolio_impact_exposure(
                impact,
                flat_prefix="sector",
                nested_key="sector",
            ),
            "theme": _workbench_portfolio_impact_exposure(
                impact,
                flat_prefix="theme",
                nested_key="theme",
            ),
            "correlated_basket": _workbench_portfolio_impact_exposure(
                impact,
                flat_prefix="correlated",
                nested_key="correlated_basket",
            ),
        },
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
    }


def _workbench_portfolio_impact_exposure(
    source: Mapping[str, object],
    *,
    flat_prefix: str,
    nested_key: str,
) -> dict[str, object]:
    nested = _mapping(source.get(nested_key))
    before = _optional_float(
        _first_value(
            nested.get("before_pct"),
            nested.get("before"),
            source.get(f"{flat_prefix}_before_pct"),
            source.get(f"{nested_key}_before_pct"),
        )
    )
    after = _optional_float(
        _first_value(
            nested.get("after_pct"),
            nested.get("after"),
            source.get(f"{flat_prefix}_after_pct"),
            source.get(f"{nested_key}_after_pct"),
        )
    )
    return {
        "before_pct": _workbench_round_ratio(before),
        "after_pct": _workbench_round_ratio(after),
        "delta_pct": _workbench_round_ratio(
            after - before if before is not None and after is not None else None
        ),
    }


def _workbench_decision_ready(row: Mapping[str, object]) -> bool:
    usefulness = _mapping(row.get("usefulness"))
    if usefulness.get("decision_ready") is True:
        return True
    status = str(usefulness.get("status") or row.get("usefulness") or "").lower()
    return status in {"decision_useful", "actionable", "eligible"}


def _workbench_trade_setup_rows(
    active_plan: Mapping[str, object],
) -> list[dict[str, object]]:
    if not active_plan or active_plan.get("status") == "missing":
        return []
    strategy = _mapping(active_plan.get("strategy_proposal"))
    return [
        {
            "decision_card_id": active_plan.get("decision_card_id"),
            "ticker": active_plan.get("ticker") or strategy.get("ticker"),
            "setup_type": strategy.get("setup_type"),
            "action_state": strategy.get("action_state"),
            "direction": strategy.get("direction"),
            "entry_zone": strategy.get("entry_zone"),
            "entry_price": strategy.get("entry_price"),
            "entry_price_source": strategy.get("entry_price_source"),
            "invalidation_price": strategy.get("invalidation_price"),
            "target_price": strategy.get("target_price"),
            "reward_risk": strategy.get("reward_risk"),
            "final_score": strategy.get("final_score"),
            "time_stop_days": strategy.get("time_stop_days"),
            "external_calls_made": 0,
            "db_writes_made": 0,
            "broker_order_submitted": False,
            "order_submission_allowed": False,
            "next_action": (
                active_plan.get("next_action")
                or "Review the setup before any paper intent."
            ),
        }
    ]


def _workbench_trade_sizing_rows(
    active_plan: Mapping[str, object],
) -> list[dict[str, object]]:
    if not active_plan or active_plan.get("status") == "missing":
        return []
    order = _mapping(active_plan.get("order_intent"))
    risk = _mapping(active_plan.get("risk_approval"))
    ticket = _mapping(active_plan.get("order_ticket"))
    return [
        {
            "ticker": active_plan.get("ticker") or ticket.get("ticker"),
            "side": order.get("side") or ticket.get("side"),
            "quantity": order.get("quantity"),
            "estimated_notional": order.get("estimated_notional"),
            "estimated_max_loss": _first_value(
                order.get("estimated_max_loss"),
                risk.get("estimated_max_loss"),
            ),
            "risk_per_trade_pct": ticket.get("risk_per_trade_pct"),
            "entry_price": _first_value(order.get("limit_price"), ticket.get("entry_price")),
            "invalidation_price": _first_value(
                order.get("stop_price"),
                ticket.get("invalidation_price"),
            ),
            "paper_approved": bool(risk.get("approved_for_paper_trade")),
            "live_approved": False,
            "external_calls_made": 0,
            "db_writes_made": 0,
            "broker_order_submitted": False,
            "order_submission_allowed": False,
            "next_action": "Use sizing as paper-review input; broker submission is disabled.",
        }
    ]


def _workbench_paper_intent_rows(
    active_plan: Mapping[str, object],
) -> list[dict[str, object]]:
    if not active_plan or active_plan.get("status") == "missing":
        return []
    paper = _mapping(active_plan.get("paper_decision"))
    return [
        {
            "decision_card_id": paper.get("decision_card_id")
            or active_plan.get("decision_card_id"),
            "ticker": active_plan.get("ticker"),
            "decision": paper.get("decision"),
            "available_at": paper.get("available_at"),
            "entry_price": paper.get("entry_price"),
            "entry_at": paper.get("entry_at"),
            "hard_block_count": len(_texts(paper.get("hard_blocks"))),
            "external_calls_required": paper.get("external_calls_required", 0),
            "external_calls_made": paper.get("external_calls_made", 0),
            "db_writes_required": paper.get("db_writes_required", 0),
            "db_writes_made": paper.get("db_writes_made", 0),
            "broker_order_submitted": bool(paper.get("broker_order_submitted")),
            "order_submission_allowed": bool(paper.get("order_submission_allowed")),
            "no_execution": bool(paper.get("no_execution", True)),
            "preview_command": paper.get("preview_command"),
            "execute_command": paper.get("execute_command"),
            "next_action": "Preview or record locally; no broker order is submitted.",
        }
    ]


def _workbench_order_intent_rows(
    active_plan: Mapping[str, object],
) -> list[dict[str, object]]:
    if not active_plan or active_plan.get("status") == "missing":
        return []
    order = _mapping(active_plan.get("order_intent"))
    ticket = _mapping(active_plan.get("order_ticket"))
    return [
        {
            "ticker": active_plan.get("ticker") or ticket.get("ticker"),
            "route": order.get("route"),
            "side": order.get("side") or ticket.get("side"),
            "quantity": order.get("quantity"),
            "limit_price": order.get("limit_price"),
            "stop_price": order.get("stop_price"),
            "estimated_notional": order.get("estimated_notional"),
            "estimated_max_loss": order.get("estimated_max_loss"),
            "submission_allowed": bool(order.get("submission_allowed")),
            "ticket_entry_price": ticket.get("entry_price"),
            "ticket_invalidation_price": ticket.get("invalidation_price"),
            "preview_command": ticket.get("preview_command"),
            "record_command": ticket.get("record_command"),
            "external_calls_required": ticket.get("external_calls_required", 0),
            "external_calls_made": ticket.get("external_calls_made", 0),
            "db_writes_required": ticket.get("db_writes_required", 0),
            "db_writes_made": ticket.get("db_writes_made", 0),
            "broker_order_submitted": False,
            "order_submission_allowed": False,
            "no_execution": bool(ticket.get("no_execution", True)),
            "next_action": "Save only as a blocked local ticket after manual review.",
        }
    ]


def _workbench_risk_approval_rows(
    active_plan: Mapping[str, object],
) -> list[dict[str, object]]:
    if not active_plan or active_plan.get("status") == "missing":
        return []
    risk = _mapping(active_plan.get("risk_approval"))
    controls = _mapping(active_plan.get("execution_controls"))
    paper_blocks = _texts(risk.get("paper_trade_blocks"))
    live_blocks = _texts(risk.get("live_submission_blocks"))
    return [
        {
            "gate": "paper_trade",
            "status": "approved" if risk.get("approved_for_paper_trade") else "blocked",
            "approved": bool(risk.get("approved_for_paper_trade")),
            "block_count": len(paper_blocks),
            "blocks": paper_blocks,
            "estimated_max_loss": risk.get("estimated_max_loss"),
            "requires_manual_approval": bool(risk.get("requires_manual_approval")),
            "external_calls_made": 0,
            "db_writes_made": 0,
            "broker_order_submitted": False,
            "order_submission_allowed": False,
            "risk_review_command": "risk-desk",
            "paper_preview_command": "paper-decision preview",
            "primary_command": "paper-decision preview",
            "next_action": "Resolve paper blocks before supervised paper review.",
        },
        {
            "gate": "live_submission",
            "status": "disabled",
            "approved": False,
            "block_count": len(live_blocks),
            "blocks": live_blocks,
            "estimated_max_loss": risk.get("estimated_max_loss"),
            "requires_manual_approval": True,
            "external_calls_made": 0,
            "db_writes_made": 0,
            "broker_order_submitted": False,
            "order_submission_allowed": False,
            "risk_review_command": "risk-desk",
            "live_boundary_command": "order-ticket preview",
            "primary_command": "risk-desk",
            "next_action": risk.get("live_submission_reason")
            or "Live submission remains disabled by the platform boundary.",
        },
        {
            "gate": "execution_controls",
            "status": "disabled"
            if controls.get("no_execution", True)
            else "manual_review",
            "approved": False,
            "block_count": 0,
            "blocks": [],
            "estimated_max_loss": risk.get("estimated_max_loss"),
            "requires_manual_approval": bool(
                risk.get("requires_manual_approval", True)
            ),
            "external_calls_made": controls.get("external_calls_made", 0),
            "db_writes_made": controls.get("db_writes_made", 0),
            "broker_order_submitted": bool(controls.get("broker_order_submitted")),
            "order_submission_allowed": bool(controls.get("order_submission_allowed")),
            "risk_review_command": "risk-desk",
            "live_boundary_command": "order-ticket preview",
            "primary_command": "risk-desk",
            "next_action": (
                "Execution controls enforce no-execution and read-only broker mode."
            ),
        },
    ]


def _workbench_action_bus_payload(
    *,
    active_plan: Mapping[str, object],
    trade_lifecycle_rows: Sequence[Mapping[str, object]],
    portfolio_position_rows: Sequence[Mapping[str, object]],
    portfolio_balance_rows: Sequence[Mapping[str, object]],
    portfolio_exposure_rows: Sequence[Mapping[str, object]],
    portfolio_open_order_rows: Sequence[Mapping[str, object]],
    risk_approval_rows: Sequence[Mapping[str, object]],
    agent_action_rows: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    actions: list[dict[str, object]] = []

    def add_action(
        action_id: str,
        *,
        module: str,
        label: str,
        action_kind: str,
        command: object = None,
        target_page: object = None,
        status: str = "enabled",
        local_write_allowed: bool = False,
        db_writes_required: object = 0,
        next_action: object = None,
        source: str = "trading_workbench",
    ) -> None:
        command_text = str(command or "").strip()
        target_text = str(target_page or module).strip()
        if not command_text and not target_text:
            return
        safety = (
            "external_boundary"
            if action_kind == "boundary"
            else "local_db_write"
            if local_write_allowed
            else "zero_call_navigation"
            if action_kind == "page"
            else "local_backend_preview"
        )
        actions.append(
            {
                "id": action_id,
                "module": module,
                "label": label,
                "action_kind": action_kind,
                "command": command_text or None,
                "target_page": target_text,
                "status": status,
                "safety": safety,
                "local_write_allowed": local_write_allowed,
                "external_calls_allowed": False,
                "external_calls_made": 0,
                "db_writes_required": db_writes_required or 0,
                "db_writes_made": 0,
                "broker_order_submitted": False,
                "order_submission_allowed": False,
                "live_trading_enabled": False,
                "source": source,
                "next_action": next_action
                or "Review this supervised workbench action before continuing.",
            }
        )

    paper = _mapping(active_plan.get("paper_decision"))
    ticket = _mapping(active_plan.get("order_ticket"))
    can_paper = bool(
        paper.get("decision_card_id") and paper.get("decision") and paper.get("available_at")
    )
    can_ticket = bool(
        ticket.get("ticker")
        and ticket.get("side")
        and ticket.get("entry_price")
        and ticket.get("invalidation_price")
    )
    add_action(
        "paper-decision-preview",
        module="paper-trading",
        label="Preview paper decision",
        action_kind="backend_command",
        command="paper-decision preview",
        status="enabled" if can_paper else "blocked",
        next_action="Preview the supervised paper decision without writing rows.",
        source="trading_workbench.active_plan.paper_decision",
    )
    add_action(
        "paper-decision-record",
        module="paper-trading",
        label="Record paper decision",
        action_kind="backend_command",
        command="paper-decision execute",
        status="enabled" if can_paper else "blocked",
        local_write_allowed=True,
        db_writes_required=paper.get("db_writes_required", 0),
        next_action="Record the paper decision locally; no broker order is submitted.",
        source="trading_workbench.active_plan.paper_decision",
    )
    add_action(
        "order-ticket-preview",
        module="broker",
        label="Preview order ticket",
        action_kind="backend_command",
        command="order-ticket preview",
        status="enabled" if can_ticket else "blocked",
        next_action="Preview a blocked local order ticket before any record step.",
        source="trading_workbench.active_plan.order_ticket",
    )
    add_action(
        "order-ticket-record",
        module="broker",
        label="Save blocked ticket",
        action_kind="backend_command",
        command="order-ticket record",
        status="enabled" if can_ticket else "blocked",
        local_write_allowed=True,
        db_writes_required=ticket.get("db_writes_required", 0),
        next_action="Save a local blocked ticket; live submission stays disabled.",
        source="trading_workbench.active_plan.order_ticket",
    )
    if portfolio_position_rows or portfolio_balance_rows or portfolio_exposure_rows:
        add_action(
            "portfolio-review",
            module="portfolio",
            label="Review portfolio",
            action_kind="page",
            command="portfolio",
            target_page="portfolio",
            next_action="Open read-only portfolio context before sizing risk.",
            source="trading_workbench.modules.portfolio",
        )
    if risk_approval_rows:
        add_action(
            "risk-desk-review",
            module="risk-desk",
            label="Review risk desk",
            action_kind="page",
            command="risk-desk",
            target_page="risk-desk",
            next_action="Open paper/live approval gates and current risk blocks.",
            source="trading_workbench.modules.risk-desk",
        )
    if portfolio_open_order_rows:
        add_action(
            "broker-boundary-review",
            module="broker",
            label="Review broker boundary",
            action_kind="page",
            command="broker",
            target_page="broker",
            next_action="Open the read-only broker desk; order submission is disabled.",
            source="trading_workbench.modules.broker",
        )

    lifecycle_row = next(
        (
            row
            for row in trade_lifecycle_rows
            if row.get("ledger_show_command")
            or row.get("outcome_preview_command")
            or row.get("outcome_update_command")
        ),
        {},
    )
    if lifecycle_row:
        if lifecycle_row.get("ledger_show_command"):
            add_action(
                "lifecycle-ledger-review",
                module="journal",
                label="Open lifecycle ledger",
                action_kind="backend_command",
                command=lifecycle_row.get("ledger_show_command"),
                next_action="Open linked local ledger evidence for this lifecycle row.",
                source="trading_workbench.trade_lifecycle_rows",
            )
        if lifecycle_row.get("outcome_preview_command"):
            add_action(
                "lifecycle-outcome-preview",
                module="journal",
                label="Preview lifecycle outcome",
                action_kind="backend_command",
                command=lifecycle_row.get("outcome_preview_command"),
                next_action="Preview local outcome evidence before updating rows.",
                source="trading_workbench.trade_lifecycle_rows",
            )
        if lifecycle_row.get("outcome_update_command"):
            add_action(
                "lifecycle-outcome-update",
                module="journal",
                label="Update lifecycle outcome",
                action_kind="backend_command",
                command=lifecycle_row.get("outcome_update_command"),
                local_write_allowed=True,
                db_writes_required=1,
                next_action="Update local outcome evidence after manual review.",
                source="trading_workbench.trade_lifecycle_rows",
            )

    agent_preview_command = "agent"
    if agent_action_rows:
        agent_preview_command = str(
            agent_action_rows[0].get("agent_preview_command") or "agent"
        )
    add_action(
        "agent-preview",
        module="agent",
        label="Preview agent review",
        action_kind="backend_command",
        command=agent_preview_command,
        next_action="Run a guarded agent preview; execute remains disabled.",
        source="trading_workbench.modules.agent",
    )
    add_action(
        "agent-execute-boundary",
        module="agent",
        label="Agent execute boundary",
        action_kind="boundary",
        command="agent execute",
        status="disabled",
        next_action="Agent execution remains an external approval boundary.",
        source="trading_workbench.modules.agent",
    )

    return {
        "schema_version": "trading-workbench-action-bus-v1",
        "status": "ready" if actions else "empty",
        "primary_action_id": next(
            (
                row["id"]
                for row in actions
                if row.get("status") == "enabled"
                and row.get("action_kind") != "boundary"
            ),
            None,
        ),
        "metrics": {
            "action_count": len(actions),
            "backend_command_count": sum(
                1 for row in actions if row.get("action_kind") == "backend_command"
            ),
            "page_route_count": sum(
                1 for row in actions if row.get("action_kind") == "page"
            ),
            "boundary_count": sum(
                1 for row in actions if row.get("action_kind") == "boundary"
            ),
            "local_write_count": sum(
                1 for row in actions if row.get("local_write_allowed") is True
            ),
        },
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "live_trading_enabled": False,
        "actions": actions,
    }


def _workbench_workflow_map_payload(
    *,
    action_bus: Mapping[str, object],
    queue_rows: Sequence[Mapping[str, object]],
    candidate_module_rows: Sequence[Mapping[str, object]],
    priced_in_answer: Mapping[str, object],
    active_plan: Mapping[str, object],
    risk_approval_rows: Sequence[Mapping[str, object]],
    paper_trade_rows: Sequence[Mapping[str, object]],
    broker_ticket_rows: Sequence[Mapping[str, object]],
    trade_lifecycle_rows: Sequence[Mapping[str, object]],
    validation_result_rows: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    actions_by_id = {
        str(row.get("id")): row
        for row in _rows(action_bus.get("actions"))
        if row.get("id")
    }
    paper_gate = next(
        (
            row
            for row in risk_approval_rows
            if str(row.get("gate") or "") == "paper_trade"
        ),
        {},
    )
    live_gate = next(
        (
            row
            for row in risk_approval_rows
            if str(row.get("gate") or "") == "live_submission"
        ),
        {},
    )
    decision_ready = bool(
        priced_in_answer.get("manual_investment_decision_ready")
        or priced_in_answer.get("can_make_investment_decision")
        or priced_in_answer.get("priced_in_answer_ready")
    )
    stages = [
        _workbench_workflow_stage(
            "market-scout",
            label="Market Scout",
            module="market-radar",
            status="ready" if queue_rows else "blocked",
            action=_workbench_workflow_page_action(
                module="market-radar",
                label="Open Market Radar",
                page="market-radar",
            ),
            evidence_count=len(queue_rows),
            source_key="trading_workbench.modules.market-radar.rows",
            next_action=(
                "Open the scouted queue before promoting a trade idea."
                if queue_rows
                else "Run MarketRadar before building the supervised workflow."
            ),
        ),
        _workbench_workflow_stage(
            "candidate-review",
            label="Candidate Review",
            module="candidates",
            status="ready" if candidate_module_rows else "blocked",
            action=_workbench_workflow_page_action(
                module="candidates",
                label="Review Candidates",
                page="candidates",
            ),
            evidence_count=len(candidate_module_rows),
            source_key="trading_workbench.modules.candidates.rows",
            next_action=(
                "Review the candidate evidence packet before planning."
                if candidate_module_rows
                else "Create candidate evidence before planning a trade."
            ),
        ),
        _workbench_workflow_stage(
            "decision-review",
            label="Decision Review",
            module="review",
            status="ready" if decision_ready else "blocked",
            action=_workbench_workflow_page_action(
                module="review",
                label="Open Decision Review",
                page="review",
            ),
            evidence_count=_first_nonnegative_int(
                _mapping(priced_in_answer.get("counts")).get("decision_ready_rows"),
                len(
                    [
                        row
                        for row in candidate_module_rows
                        if bool(row.get("decision_ready"))
                    ]
                ),
            ),
            source_key="trading_workbench.modules.review",
            next_action=priced_in_answer.get("next_action")
            or "Resolve decision-readiness blockers before planning.",
        ),
        _workbench_workflow_stage(
            "trade-planning",
            label="Trade Planning",
            module="trade-planner",
            status=(
                "blocked"
                if active_plan.get("status") in {None, "missing", "blocked"}
                else "ready"
            ),
            action=_workbench_workflow_page_action(
                module="trade-planner",
                label="Open Trade Planner",
                page="trade-planner",
            ),
            evidence_count=0
            if active_plan.get("status") in {None, "missing"}
            else 1,
            source_key="trading_workbench.active_plan",
            next_action=active_plan.get("next_action")
            or "Build a supervised trade plan from a reviewed decision card.",
        ),
        _workbench_workflow_stage(
            "risk-approval",
            label="Risk Approval",
            module="risk-desk",
            status="ready" if paper_gate.get("approved") else "blocked",
            action=actions_by_id.get("risk-desk-review")
            or _workbench_workflow_page_action(
                module="risk-desk",
                label="Review Risk Desk",
                page="risk-desk",
            ),
            evidence_count=len(risk_approval_rows),
            source_key="trading_workbench.modules.risk-desk.risk_approval_rows",
            next_action=paper_gate.get("next_action")
            or "Resolve paper-trade risk blocks before paper review.",
        ),
        _workbench_workflow_stage(
            "paper-trading",
            label="Paper Trading",
            module="paper-trading",
            status="ready" if paper_trade_rows else "blocked",
            action=actions_by_id.get("paper-decision-preview")
            or _workbench_workflow_command_action(
                module="paper-trading",
                label="Preview Paper Decision",
                command="paper-decision preview",
            ),
            evidence_count=len(paper_trade_rows),
            source_key="trading_workbench.modules.paper-trading.paper_trades",
            next_action=(
                "Review paper trade evidence and execution audit."
                if paper_trade_rows
                else "Preview the supervised paper decision before recording."
            ),
        ),
        _workbench_workflow_stage(
            "broker-boundary",
            label="Broker Boundary",
            module="broker",
            status="disabled",
            action=actions_by_id.get("broker-boundary-review")
            or _workbench_workflow_page_action(
                module="broker",
                label="Review Broker Boundary",
                page="broker",
            ),
            evidence_count=len(broker_ticket_rows),
            source_key="trading_workbench.modules.broker.order_tickets",
            next_action=live_gate.get("next_action")
            or "Live broker submission remains disabled.",
        ),
        _workbench_workflow_stage(
            "journal-validation",
            label="Journal And Validation",
            module="journal",
            status="ready"
            if trade_lifecycle_rows or validation_result_rows
            else "blocked",
            action=_workbench_workflow_page_action(
                module="journal",
                label="Open Journal",
                page="journal",
            ),
            evidence_count=len(trade_lifecycle_rows) + len(validation_result_rows),
            source_key="trading_workbench.modules.journal",
            next_action=(
                "Review lifecycle, journal, and validation evidence."
                if trade_lifecycle_rows or validation_result_rows
                else "Record local review evidence before trusting the workflow."
            ),
        ),
        _workbench_workflow_stage(
            "agent-review",
            label="Agent Review",
            module="agent",
            status="ready",
            action=actions_by_id.get("agent-preview")
            or _workbench_workflow_command_action(
                module="agent",
                label="Preview Agent Review",
                command="agent",
            ),
            evidence_count=1,
            source_key="trading_workbench.modules.agent",
            next_action="Preview agent reasoning; autonomous execution remains disabled.",
        ),
    ]
    active_stage_id = next(
        (
            str(stage["id"])
            for stage in stages
            if stage.get("status") == "blocked"
        ),
        str(stages[-1]["id"]) if stages else None,
    )
    return {
        "schema_version": "trading-workbench-workflow-map-v1",
        "status": "blocked"
        if any(stage.get("status") == "blocked" for stage in stages)
        else "ready",
        "active_stage_id": active_stage_id,
        "stage_count": len(stages),
        "blocked_stage_count": sum(
            1 for stage in stages if stage.get("status") == "blocked"
        ),
        "disabled_stage_count": sum(
            1 for stage in stages if stage.get("status") == "disabled"
        ),
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "live_trading_enabled": False,
        "stages": stages,
        "transitions": _workbench_workflow_transitions(stages),
    }


def _workbench_workflow_stage(
    stage_id: str,
    *,
    label: str,
    module: str,
    status: str,
    action: Mapping[str, object],
    evidence_count: int,
    source_key: str,
    next_action: object,
) -> dict[str, object]:
    return {
        "id": stage_id,
        "label": label,
        "module": module,
        "status": status,
        "action": dict(action),
        "evidence_count": evidence_count,
        "source_key": source_key,
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "live_trading_enabled": False,
        "next_action": next_action,
    }


def _workbench_workflow_page_action(
    *,
    module: str,
    label: str,
    page: str,
) -> dict[str, object]:
    return {
        "id": f"{module}-workflow-page",
        "module": module,
        "label": label,
        "action_kind": "page",
        "command": page,
        "target_page": page,
        "status": "enabled",
        "safety": "zero_call_navigation",
        "local_write_allowed": False,
        "external_calls_allowed": False,
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "live_trading_enabled": False,
    }


def _workbench_workflow_command_action(
    *,
    module: str,
    label: str,
    command: str,
) -> dict[str, object]:
    return {
        "id": f"{module}-workflow-command",
        "module": module,
        "label": label,
        "action_kind": "backend_command",
        "command": command,
        "target_page": module,
        "status": "enabled",
        "safety": "local_backend_preview",
        "local_write_allowed": False,
        "external_calls_allowed": False,
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "live_trading_enabled": False,
    }


def _workbench_workflow_transitions(
    stages: Sequence[Mapping[str, object]],
) -> list[dict[str, object]]:
    transitions = []
    for previous, current in zip(stages, stages[1:], strict=False):
        blocked = str(previous.get("status") or "") == "blocked"
        disabled = str(current.get("status") or "") == "disabled"
        transitions.append(
            {
                "from_stage": previous.get("id"),
                "to_stage": current.get("id"),
                "status": "disabled" if disabled else "blocked" if blocked else "open",
                "external_calls_made": 0,
                "broker_order_submitted": False,
                "order_submission_allowed": False,
            }
        )
    return transitions


def _workbench_priority_queue_payload(
    *,
    action_bus: Mapping[str, object],
    workflow_map: Mapping[str, object],
) -> dict[str, object]:
    stages = _rows(workflow_map.get("stages"))
    actions = _rows(action_bus.get("actions"))
    active_stage_id = str(workflow_map.get("active_stage_id") or "").strip()
    items: list[dict[str, object]] = []
    used_action_ids: set[str] = set()

    def add_stage_item(
        stage: Mapping[str, object],
        *,
        reason: str,
        priority: int,
    ) -> None:
        action = _mapping(stage.get("action"))
        source_action_id = str(action.get("id") or "").strip()
        if source_action_id:
            used_action_ids.add(source_action_id)
        items.append(
            _workbench_priority_item(
                item_id=f"priority-stage-{stage.get('id')}",
                item_kind="workflow_stage",
                module=stage.get("module"),
                label=stage.get("label"),
                status=stage.get("status"),
                reason=reason,
                priority=priority,
                source_stage_id=stage.get("id"),
                source_action_id=source_action_id or None,
                action=action,
                next_action=stage.get("next_action"),
            )
        )

    active_stage = next(
        (stage for stage in stages if str(stage.get("id") or "") == active_stage_id),
        {},
    )
    if active_stage:
        add_stage_item(
            active_stage,
            reason="Active workflow blocker"
            if active_stage.get("status") == "blocked"
            else "Active workflow stage",
            priority=100,
        )

    for stage in stages:
        stage_id = str(stage.get("id") or "")
        if stage_id == active_stage_id or stage.get("status") != "blocked":
            continue
        add_stage_item(stage, reason="Workflow blocker", priority=90)

    for _index, action in sorted(
        enumerate(actions),
        key=lambda pair: _workbench_priority_action_sort_key(pair[1], pair[0]),
    ):
        action_id = str(action.get("id") or "").strip()
        if not action_id or action_id in used_action_ids:
            continue
        items.append(
            _workbench_priority_item(
                item_id=f"priority-action-{action_id}",
                item_kind="action_bus",
                module=action.get("module"),
                label=action.get("label"),
                status=action.get("status"),
                reason=_workbench_priority_action_reason(action),
                priority=_workbench_priority_action_priority(action),
                source_stage_id=None,
                source_action_id=action_id,
                action=action,
                next_action=action.get("next_action"),
            )
        )

    for rank, item in enumerate(items, start=1):
        item["rank"] = rank

    return {
        "schema_version": "trading-workbench-priority-queue-v1",
        "status": "blocked"
        if any(item.get("status") == "blocked" for item in items)
        else "ready"
        if items
        else "empty",
        "active_stage_id": active_stage_id or None,
        "primary_item_id": items[0]["id"] if items else None,
        "metrics": {
            "item_count": len(items),
            "blocked_item_count": sum(
                1 for item in items if item.get("status") == "blocked"
            ),
            "enabled_item_count": sum(
                1 for item in items if item.get("status") == "enabled"
            ),
            "backend_command_count": sum(
                1 for item in items if item.get("action_kind") == "backend_command"
            ),
            "page_route_count": sum(
                1 for item in items if item.get("action_kind") == "page"
            ),
            "local_write_count": sum(
                1 for item in items if item.get("local_write_allowed") is True
            ),
            "boundary_count": sum(
                1 for item in items if item.get("action_kind") == "boundary"
            ),
        },
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "live_trading_enabled": False,
        "items": items,
    }


def _workbench_priority_item(
    *,
    item_id: str,
    item_kind: str,
    module: object,
    label: object,
    status: object,
    reason: str,
    priority: int,
    source_stage_id: object,
    source_action_id: object,
    action: Mapping[str, object],
    next_action: object,
) -> dict[str, object]:
    action_kind = str(action.get("action_kind") or "backend_command").strip()
    command = str(action.get("command") or "").strip()
    target_page = str(action.get("target_page") or module or "").strip()
    local_write_allowed = bool(action.get("local_write_allowed"))
    return {
        "id": item_id,
        "rank": 0,
        "priority": priority,
        "item_kind": item_kind,
        "module": module,
        "label": label,
        "status": status,
        "reason": reason,
        "source_stage_id": source_stage_id,
        "source_action_id": source_action_id,
        "action_kind": action_kind,
        "command": command or None,
        "target_page": target_page or None,
        "safety": action.get("safety")
        or (
            "external_boundary"
            if action_kind == "boundary"
            else "local_db_write"
            if local_write_allowed
            else "zero_call_navigation"
            if action_kind == "page"
            else "local_backend_preview"
        ),
        "local_write_allowed": local_write_allowed,
        "external_calls_allowed": False,
        "external_calls_made": 0,
        "db_writes_required": action.get("db_writes_required") or 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "live_trading_enabled": False,
        "next_action": next_action
        or "Review this supervised priority item before continuing.",
    }


def _workbench_supervision_gates_payload(
    *,
    action_bus: Mapping[str, object],
    priority_queue: Mapping[str, object],
) -> dict[str, object]:
    actions = _rows(action_bus.get("actions"))
    local_write_actions = [
        row for row in actions if row.get("local_write_allowed") is True
    ]
    preview_actions = [
        row
        for row in actions
        if row.get("action_kind") == "backend_command"
        and row.get("local_write_allowed") is not True
        and row.get("status") == "enabled"
    ]
    queue_local_write_required = sum(
        int(_number_or_zero(row.get("db_writes_required")))
        for row in _rows(priority_queue.get("items"))
        if row.get("local_write_allowed") is True
    )
    action_local_write_required = sum(
        int(_number_or_zero(row.get("db_writes_required")))
        for row in local_write_actions
    )
    db_writes_required = max(queue_local_write_required, action_local_write_required)
    if local_write_actions and db_writes_required <= 0:
        db_writes_required = len(local_write_actions)
    preview_action = preview_actions[0] if preview_actions else {}

    gates = [
        _workbench_supervision_gate(
            gate_id="zero-call-browsing",
            rank=1,
            gate_kind="read_only",
            module="platform",
            label="Zero-call browsing",
            status="ready",
            approval_required=False,
            requires_arm_before_run=False,
            action_kind="page",
            command="overview",
            target_page="overview",
            safety="zero_call_navigation",
            local_write_allowed=False,
            db_writes_allowed=False,
            db_writes_required=0,
            next_action="Browse the local workbench snapshot without provider calls.",
        ),
        _workbench_supervision_gate(
            gate_id="local-backend-preview",
            rank=2,
            gate_kind="local_preview",
            module="platform",
            label="Local backend previews",
            status="ready" if preview_actions else "blocked",
            approval_required=False,
            requires_arm_before_run=False,
            action_kind="backend_command",
            command=preview_action.get("command") or "agent",
            target_page=preview_action.get("target_page") or "agent",
            safety="local_backend_preview",
            local_write_allowed=False,
            db_writes_allowed=False,
            db_writes_required=0,
            next_action="Use preview commands before arming any local write.",
        ),
        _workbench_supervision_gate(
            gate_id="guarded-local-write",
            rank=3,
            gate_kind="local_write",
            module="platform",
            label="Guarded local writes",
            status="approval_required" if local_write_actions else "blocked",
            approval_required=bool(local_write_actions),
            requires_arm_before_run=True,
            action_kind="backend_command",
            command=(local_write_actions[0].get("command") if local_write_actions else None),
            target_page=(
                local_write_actions[0].get("target_page")
                if local_write_actions
                else "paper-trading"
            ),
            safety="local_db_write",
            local_write_allowed=bool(local_write_actions),
            db_writes_allowed=bool(local_write_actions),
            db_writes_required=db_writes_required,
            next_action=(
                "Arm a local write only after reviewing its preview; "
                "click again to confirm."
            ),
        ),
        _workbench_supervision_gate(
            gate_id="broker-submission",
            rank=4,
            gate_kind="broker_submission",
            module="broker",
            label="Broker submission",
            status="disabled",
            approval_required=True,
            requires_arm_before_run=False,
            action_kind="boundary",
            command="order-ticket submit",
            target_page="broker",
            safety="external_boundary",
            local_write_allowed=False,
            db_writes_allowed=False,
            db_writes_required=0,
            next_action="Broker order submission remains disabled.",
        ),
        _workbench_supervision_gate(
            gate_id="agent-execute",
            rank=5,
            gate_kind="agent_execute",
            module="agent",
            label="Agent execute",
            status="disabled",
            approval_required=True,
            requires_arm_before_run=False,
            action_kind="boundary",
            command="agent execute",
            target_page="agent",
            safety="external_boundary",
            local_write_allowed=False,
            db_writes_allowed=False,
            db_writes_required=0,
            next_action="Agent execution remains outside clickable browsing controls.",
        ),
        _workbench_supervision_gate(
            gate_id="autonomous-execution",
            rank=6,
            gate_kind="autonomous_execution",
            module="platform",
            label="Autonomous execution",
            status="out_of_scope",
            approval_required=True,
            requires_arm_before_run=False,
            action_kind="boundary",
            command="autonomous execute",
            target_page="overview",
            safety="external_boundary",
            local_write_allowed=False,
            db_writes_allowed=False,
            db_writes_required=0,
            next_action="Autonomous live trading is out of scope.",
        ),
    ]
    primary_gate_id = next(
        (
            str(row.get("id"))
            for row in gates
            if row.get("status") == "approval_required"
        ),
        str(gates[0]["id"]) if gates else None,
    )
    return {
        "schema_version": "trading-workbench-supervision-gates-v1",
        "status": "approval_required"
        if any(row.get("status") == "approval_required" for row in gates)
        else "ready"
        if gates
        else "empty",
        "primary_gate_id": primary_gate_id,
        "metrics": {
            "gate_count": len(gates),
            "approval_required_count": sum(
                1 for row in gates if row.get("status") == "approval_required"
            ),
            "disabled_gate_count": sum(
                1 for row in gates if row.get("status") in {"disabled", "out_of_scope"}
            ),
            "local_write_gate_count": sum(
                1 for row in gates if row.get("local_write_allowed") is True
            ),
            "external_call_gate_count": 0,
            "broker_submission_gate_count": sum(
                1 for row in gates if row.get("order_submission_allowed") is True
            ),
        },
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "live_trading_enabled": False,
        "gates": gates,
    }


def _workbench_supervision_gate(
    *,
    gate_id: str,
    rank: int,
    gate_kind: str,
    module: str,
    label: str,
    status: str,
    approval_required: bool,
    requires_arm_before_run: bool,
    action_kind: str,
    command: object,
    target_page: object,
    safety: str,
    local_write_allowed: bool,
    db_writes_allowed: bool,
    db_writes_required: int,
    next_action: str,
) -> dict[str, object]:
    command_text = str(command or "").strip()
    target_text = str(target_page or module or "").strip()
    return {
        "id": gate_id,
        "rank": rank,
        "gate_kind": gate_kind,
        "module": module,
        "label": label,
        "status": status,
        "approval_required": approval_required,
        "requires_arm_before_run": requires_arm_before_run,
        "action_kind": action_kind,
        "command": command_text or None,
        "target_page": target_text or None,
        "safety": safety,
        "local_write_allowed": local_write_allowed,
        "external_calls_allowed": False,
        "external_calls_made": 0,
        "db_writes_allowed": db_writes_allowed,
        "db_writes_required": db_writes_required,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "live_trading_enabled": False,
        "next_action": next_action,
    }


def _workbench_decision_brief_payload(
    *,
    focus_row: Mapping[str, object],
    active_plan: Mapping[str, object],
    workflow_map: Mapping[str, object],
    priority_queue: Mapping[str, object],
    supervision_gates: Mapping[str, object],
) -> dict[str, object]:
    strategy = _mapping(active_plan.get("strategy_proposal"))
    risk = _mapping(active_plan.get("risk_approval"))
    primary_item = _workbench_primary_priority_item(priority_queue)
    ticker = str(
        _first_value(focus_row.get("ticker"), active_plan.get("ticker")) or ""
    ).strip()
    subject = _first_value(
        focus_row.get("subject"),
        strategy.get("setup_type"),
        active_plan.get("next_action"),
    )
    decision_card_id = _first_value(
        focus_row.get("decision_card_id"),
        active_plan.get("decision_card_id"),
    )
    paper_blocks = _texts(risk.get("paper_trade_blocks"))
    live_blocks = _texts(risk.get("live_submission_blocks"))
    approval_required_count = _first_nonnegative_int(
        _mapping(supervision_gates.get("metrics")).get("approval_required_count")
    )
    status = str(
        _first_value(active_plan.get("status"), workflow_map.get("status"), "missing")
    )
    evidence_chain = [
        {
            "step": "market-scout",
            "label": "MarketRadar scout",
            "status": "ready" if ticker else "missing",
            "artifact": subject or ticker or "No active scout row",
        },
        {
            "step": "decision-card",
            "label": "Decision card",
            "status": "available" if decision_card_id else "missing",
            "artifact": decision_card_id or "No decision card",
        },
        {
            "step": "risk-approval",
            "label": "Risk approval",
            "status": "ready"
            if risk.get("approved_for_paper_trade")
            else "blocked"
            if paper_blocks or live_blocks
            else "review",
            "artifact": f"paper_blocks={len(paper_blocks)} live_blocks={len(live_blocks)}",
        },
        {
            "step": "supervision",
            "label": "Supervision gate",
            "status": supervision_gates.get("status") or "unknown",
            "artifact": supervision_gates.get("primary_gate_id") or "none",
        },
    ]
    return {
        "schema_version": "trading-workbench-decision-brief-v1",
        "status": status,
        "source_tool": "market-radar",
        "ticker": ticker or None,
        "decision_card_id": decision_card_id,
        "headline": f"{ticker}: {subject}" if ticker and subject else subject or ticker,
        "autonomy_level": active_plan.get("autonomy_level"),
        "recommended_paper_decision": active_plan.get("recommended_paper_decision"),
        "scout": {
            "ticker": ticker or None,
            "subject": subject,
            "score": focus_row.get("score"),
            "setup": focus_row.get("setup"),
            "state": focus_row.get("state"),
            "usefulness_status": focus_row.get("usefulness_status"),
            "decision_ready": bool(focus_row.get("decision_ready")),
            "decision_card_id": decision_card_id,
            "next_action": focus_row.get("next_action"),
        },
        "setup": {
            "setup_type": strategy.get("setup_type"),
            "direction": strategy.get("direction"),
            "entry_price": strategy.get("entry_price"),
            "invalidation_price": strategy.get("invalidation_price"),
            "reward_risk": strategy.get("reward_risk"),
            "action_state": strategy.get("action_state"),
        },
        "risk": {
            "approved_for_paper_trade": bool(risk.get("approved_for_paper_trade")),
            "approved_for_live_submission": bool(
                risk.get("approved_for_live_submission")
            ),
            "paper_block_count": len(paper_blocks),
            "live_block_count": len(live_blocks),
            "estimated_max_loss": risk.get("estimated_max_loss"),
            "requires_manual_approval": bool(risk.get("requires_manual_approval")),
        },
        "workflow": {
            "status": workflow_map.get("status"),
            "active_stage_id": workflow_map.get("active_stage_id"),
            "primary_priority_item_id": priority_queue.get("primary_item_id"),
            "primary_supervision_gate_id": supervision_gates.get("primary_gate_id"),
            "approval_required_count": approval_required_count,
        },
        "next_action": {
            "label": primary_item.get("label"),
            "action_kind": primary_item.get("action_kind"),
            "command": primary_item.get("command"),
            "target_page": primary_item.get("target_page"),
            "safety": primary_item.get("safety"),
            "source": "priority_queue" if primary_item else "active_plan",
        },
        "evidence_chain": evidence_chain,
        "metrics": {
            "evidence_count": len(evidence_chain),
            "paper_block_count": len(paper_blocks),
            "live_block_count": len(live_blocks),
            "approval_required_count": approval_required_count,
            "external_calls_made": 0,
        },
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "live_trading_enabled": False,
    }


def _workbench_primary_priority_item(
    priority_queue: Mapping[str, object],
) -> Mapping[str, object]:
    items = _rows(priority_queue.get("items"))
    primary_item_id = str(priority_queue.get("primary_item_id") or "").strip()
    if primary_item_id:
        for item in items:
            if str(item.get("id") or "") == primary_item_id:
                return item
    return items[0] if items else {}


def _workbench_scenario_matrix_payload(
    *,
    active_plan: Mapping[str, object],
    decision_brief: Mapping[str, object],
) -> dict[str, object]:
    strategy = _mapping(active_plan.get("strategy_proposal"))
    risk = _mapping(active_plan.get("risk_approval"))
    order = _mapping(active_plan.get("order_intent"))
    ticket = _mapping(active_plan.get("order_ticket"))
    setup = _mapping(decision_brief.get("setup"))
    entry_price = _optional_float(
        _first_value(
            strategy.get("entry_price"),
            order.get("limit_price"),
            ticket.get("entry_price"),
            setup.get("entry_price"),
        )
    )
    invalidation_price = _optional_float(
        _first_value(
            strategy.get("invalidation_price"),
            ticket.get("invalidation_price"),
            order.get("stop_price"),
            setup.get("invalidation_price"),
        )
    )
    reward_risk = _optional_float(
        _first_value(strategy.get("reward_risk"), setup.get("reward_risk"))
    )
    target_price = _optional_float(
        _first_value(
            strategy.get("target_price"),
            ticket.get("target_price"),
            order.get("target_price"),
        )
    )
    risk_per_share = None
    if entry_price is not None and invalidation_price is not None:
        risk_per_share = abs(entry_price - invalidation_price)
    if (
        target_price is None
        and entry_price is not None
        and risk_per_share is not None
        and reward_risk is not None
    ):
        direction = str(
            _first_value(strategy.get("direction"), order.get("side"), "")
        ).lower()
        if direction in {"bearish", "short", "sell"}:
            target_price = entry_price - (risk_per_share * reward_risk)
        else:
            target_price = entry_price + (risk_per_share * reward_risk)

    raw_quantity = _optional_int(
        _first_value(order.get("quantity"), ticket.get("quantity"), ticket.get("shares"))
    )
    quantity = raw_quantity if raw_quantity is not None and raw_quantity > 0 else None
    paper_blocks = _texts(risk.get("paper_trade_blocks"))
    live_blocks = _texts(risk.get("live_submission_blocks"))
    blockers = list(dict.fromkeys([*paper_blocks, *live_blocks]))
    sizing_status = "blocked" if quantity is None or blockers else "ready"
    status = str(
        _first_value(active_plan.get("status"), decision_brief.get("status"), "missing")
    )
    scenarios = [
        _workbench_scenario_row(
            scenario_id="invalidation",
            label="Invalidation",
            scenario_kind="downside",
            price=invalidation_price,
            entry_price=entry_price,
            status="blocked" if blockers else "review",
            boundary="max_loss",
            next_action="Resolve paper-trade blockers before recording any local decision.",
        ),
        _workbench_scenario_row(
            scenario_id="entry",
            label="Entry",
            scenario_kind="reference",
            price=entry_price,
            entry_price=entry_price,
            status="reference",
            boundary="planned_entry",
            next_action="Use as the reference price for review.",
        ),
        _workbench_scenario_row(
            scenario_id="target",
            label="Reward target",
            scenario_kind="upside",
            price=target_price,
            entry_price=entry_price,
            status="review",
            boundary="target_reward",
            next_action="Compare upside to risk before any paper record.",
        ),
    ]
    return {
        "schema_version": "trading-workbench-scenario-matrix-v1",
        "status": status,
        "source_tool": decision_brief.get("source_tool") or "market-radar",
        "ticker": _first_value(decision_brief.get("ticker"), active_plan.get("ticker")),
        "decision_card_id": _first_value(
            decision_brief.get("decision_card_id"),
            active_plan.get("decision_card_id"),
        ),
        "assumptions": {
            "entry_price": _workbench_round_float(entry_price),
            "invalidation_price": _workbench_round_float(invalidation_price),
            "target_price": _workbench_round_float(target_price),
            "risk_per_share": _workbench_round_float(risk_per_share),
            "reward_risk": _workbench_round_float(reward_risk),
            "estimated_max_loss": _workbench_round_float(
                _optional_float(risk.get("estimated_max_loss"))
            ),
            "quantity": quantity,
            "sizing_status": sizing_status,
        },
        "scenarios": scenarios,
        "blockers": blockers,
        "metrics": {
            "scenario_count": len(scenarios),
            "downside_count": sum(
                1 for row in scenarios if row.get("scenario_kind") == "downside"
            ),
            "upside_count": sum(
                1 for row in scenarios if row.get("scenario_kind") == "upside"
            ),
            "risk_reward": _workbench_round_float(reward_risk),
            "estimated_max_loss": _workbench_round_float(
                _optional_float(risk.get("estimated_max_loss"))
            ),
            "external_calls_made": 0,
        },
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "live_trading_enabled": False,
    }


def _workbench_scenario_row(
    *,
    scenario_id: str,
    label: str,
    scenario_kind: str,
    price: float | None,
    entry_price: float | None,
    status: str,
    boundary: str,
    next_action: str,
) -> dict[str, object]:
    move_pct = None
    pnl_per_share = None
    if price is not None and entry_price not in (None, 0):
        move_pct = ((price - entry_price) / entry_price) * 100
        pnl_per_share = price - entry_price
    return {
        "id": scenario_id,
        "label": label,
        "scenario_kind": scenario_kind,
        "price": _workbench_round_float(price),
        "move_pct": _workbench_round_float(move_pct),
        "pnl_per_share": _workbench_round_float(pnl_per_share),
        "status": status,
        "boundary": boundary,
        "next_action": next_action,
    }


def _workbench_round_float(value: float | None) -> float | None:
    return round(value, 1) if value is not None else None


def _workbench_risk_envelope_payload(
    *,
    active_plan: Mapping[str, object],
    decision_brief: Mapping[str, object],
    scenario_matrix: Mapping[str, object],
    broker_snapshot: Mapping[str, object],
    broker_exposure: Mapping[str, object],
    broker_connected: bool,
    open_order_count: int,
) -> dict[str, object]:
    risk = _mapping(active_plan.get("risk_approval"))
    order = _mapping(active_plan.get("order_intent"))
    ticket = _mapping(active_plan.get("order_ticket"))
    boundary = {
        "broker_order_submission": "disabled",
        "live_trading_enabled": False,
        "autonomous_execution": "disabled",
    }
    assumptions = _mapping(scenario_matrix.get("assumptions"))
    portfolio_equity = _optional_float(
        _first_value(
            broker_exposure.get("portfolio_equity"),
            broker_snapshot.get("portfolio_equity"),
        )
    )
    estimated_notional = _optional_float(order.get("estimated_notional"))
    estimated_max_loss = _optional_float(
        _first_value(
            assumptions.get("estimated_max_loss"),
            order.get("estimated_max_loss"),
            risk.get("estimated_max_loss"),
        )
    )
    raw_quantity = _optional_int(
        _first_value(order.get("quantity"), ticket.get("quantity"), ticket.get("shares"))
    )
    quantity = raw_quantity if raw_quantity is not None and raw_quantity > 0 else None
    blockers = list(dict.fromkeys(_texts(scenario_matrix.get("blockers"))))
    paper_blocks = _texts(risk.get("paper_trade_blocks"))
    live_blocks = _texts(risk.get("live_submission_blocks"))
    if not blockers:
        blockers = list(dict.fromkeys([*paper_blocks, *live_blocks]))
    broker_data_stale = bool(broker_exposure.get("broker_data_stale"))
    sizing_status = str(assumptions.get("sizing_status") or "")
    if not sizing_status:
        sizing_status = "blocked" if quantity is None or blockers else "ready"
    checks = [
        _workbench_risk_envelope_check(
            check_id="broker-data-freshness",
            label="Broker data freshness",
            status="blocked" if broker_data_stale else "ready",
            scope="portfolio",
            finding="stale_broker_data" if broker_data_stale else "broker_data_current",
            next_action=(
                "Refresh read-only broker context before relying on sizing."
                if broker_data_stale
                else "Broker context is current enough for sizing review."
            ),
        ),
        _workbench_risk_envelope_check(
            check_id="position-sizing",
            label="Position sizing",
            status="blocked" if quantity is None else "ready",
            scope="trade-planner",
            finding=(
                "missing_position_sizing:shares"
                if quantity is None
                else "shares_sized"
            ),
            next_action=(
                "Resolve share sizing before paper review."
                if quantity is None
                else "Use sized shares as paper-review input."
            ),
        ),
        _workbench_risk_envelope_check(
            check_id="paper-trade-gate",
            label="Paper trade gate",
            status="blocked" if paper_blocks else "ready",
            scope="paper-trading",
            finding=paper_blocks[0] if paper_blocks else "paper_trade_gate_clear",
            next_action=(
                "Resolve paper blocks before recording a local decision."
                if paper_blocks
                else "Paper gate is ready for supervised local review."
            ),
        ),
        _workbench_risk_envelope_check(
            check_id="live-submission-gate",
            label="Live submission gate",
            status="disabled",
            scope="broker",
            finding=(
                "broker_submission_disabled"
                if "broker_submission_disabled" in live_blocks
                else "live_submission_disabled"
            ),
            next_action="Live broker submission remains disabled.",
        ),
        _workbench_risk_envelope_check(
            check_id="execution-boundary",
            label="Execution boundary",
            status="disabled",
            scope="supervision",
            finding="live_trading_disabled",
            next_action="Use local preview or paper-only commands after approval.",
        ),
    ]
    blocked_check_count = sum(1 for row in checks if row.get("status") == "blocked")
    disabled_check_count = sum(1 for row in checks if row.get("status") == "disabled")
    status = "blocked" if blocked_check_count else "ready"
    return {
        "schema_version": "trading-workbench-risk-envelope-v1",
        "status": status,
        "source_tool": decision_brief.get("source_tool") or "market-radar",
        "ticker": _first_value(decision_brief.get("ticker"), active_plan.get("ticker")),
        "decision_card_id": _first_value(
            decision_brief.get("decision_card_id"),
            active_plan.get("decision_card_id"),
        ),
        "portfolio_context": {
            "broker_connected": broker_connected,
            "broker_data_stale": broker_data_stale,
            "position_count": _first_nonnegative_int(
                broker_exposure.get("position_count"),
                broker_snapshot.get("position_count"),
            ),
            "open_order_count": open_order_count,
            "portfolio_equity": portfolio_equity,
            "cash": _first_value(broker_exposure.get("cash")),
            "buying_power": _first_value(broker_exposure.get("buying_power")),
            "gross_exposure_pct": _first_value(
                _nested(broker_exposure, "exposure_before", "gross_exposure_pct")
            ),
            "single_name_exposure_count": len(
                _mapping(_nested(broker_exposure, "exposure_before", "single_name"))
            ),
        },
        "sizing_context": {
            "side": _first_value(order.get("side"), ticket.get("side")),
            "quantity": quantity,
            "sizing_status": sizing_status,
            "entry_price": assumptions.get("entry_price"),
            "invalidation_price": assumptions.get("invalidation_price"),
            "target_price": assumptions.get("target_price"),
            "risk_per_share": assumptions.get("risk_per_share"),
            "reward_risk": assumptions.get("reward_risk"),
            "estimated_notional": _workbench_round_float(estimated_notional),
            "estimated_notional_pct_of_equity": _workbench_round_ratio(
                _workbench_ratio(estimated_notional, portfolio_equity)
            ),
            "estimated_max_loss": _workbench_round_float(estimated_max_loss),
            "max_loss_pct_of_equity": _workbench_round_ratio(
                _workbench_ratio(estimated_max_loss, portfolio_equity)
            ),
            "risk_per_trade_pct": ticket.get("risk_per_trade_pct"),
        },
        "gate_context": {
            "paper_approved": bool(risk.get("approved_for_paper_trade")),
            "live_approved": bool(risk.get("approved_for_live_submission")),
            "requires_manual_approval": bool(risk.get("requires_manual_approval")),
            "paper_block_count": len(paper_blocks),
            "live_block_count": len(live_blocks),
            "open_order_count": open_order_count,
            **boundary,
        },
        "checks": checks,
        "blockers": blockers,
        "metrics": {
            "check_count": len(checks),
            "blocked_check_count": blocked_check_count,
            "disabled_check_count": disabled_check_count,
            "paper_block_count": len(paper_blocks),
            "live_block_count": len(live_blocks),
            "estimated_max_loss": _workbench_round_float(estimated_max_loss),
            "max_loss_pct_of_equity": _workbench_round_ratio(
                _workbench_ratio(estimated_max_loss, portfolio_equity)
            ),
            "external_calls_made": 0,
        },
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "live_trading_enabled": False,
    }


def _workbench_risk_envelope_check(
    *,
    check_id: str,
    label: str,
    status: str,
    scope: str,
    finding: str,
    next_action: str,
) -> dict[str, object]:
    return {
        "id": check_id,
        "label": label,
        "status": status,
        "scope": scope,
        "finding": finding,
        "next_action": next_action,
    }


def _workbench_portfolio_impact_preview_payload(
    *,
    active_plan: Mapping[str, object],
    risk_envelope: Mapping[str, object],
    broker_connected: bool,
) -> dict[str, object]:
    impact = _mapping(active_plan.get("portfolio_impact"))
    risk = _mapping(active_plan.get("risk_approval"))
    portfolio = _mapping(risk_envelope.get("portfolio_context"))
    proposed_notional = _optional_float(impact.get("proposed_notional"))
    max_loss = _optional_float(
        _first_value(impact.get("max_loss"), risk.get("estimated_max_loss"))
    )
    portfolio_equity = _optional_float(portfolio.get("portfolio_equity"))
    portfolio_hard_blocks = _texts(impact.get("hard_blocks")) or _texts(
        risk.get("portfolio_hard_blocks")
    )
    paper_blocks = _texts(risk.get("paper_trade_blocks"))
    live_blocks = _texts(risk.get("live_submission_blocks"))
    broker_data_stale = bool(portfolio.get("broker_data_stale"))
    exposure_rows = _workbench_portfolio_impact_exposure_rows(impact)
    exposure_delta_ready = any(row.get("status") == "ready" for row in exposure_rows)
    has_impact_source = proposed_notional is not None or max_loss is not None
    blockers = list(
        dict.fromkeys(
            [
                *portfolio_hard_blocks,
                *(["missing_portfolio_impact"] if not has_impact_source else []),
                *(["stale_broker_data"] if broker_data_stale else []),
                *(
                    ["missing_portfolio_impact:exposure_deltas"]
                    if not exposure_delta_ready
                    else []
                ),
                *paper_blocks,
                *live_blocks,
            ]
        )
    )
    primary_blocker = blockers[0] if blockers else None
    checks = [
        _workbench_portfolio_impact_check(
            check_id="impact-source",
            label="Impact source",
            status="ready" if has_impact_source else "blocked",
            scope="market-radar",
            finding="portfolio_impact_available"
            if has_impact_source
            else "missing_portfolio_impact",
            next_action="Use stored decision-card impact as a risk-desk input."
            if has_impact_source
            else "Review or rebuild the decision card before sizing impact.",
        ),
        _workbench_portfolio_impact_check(
            check_id="broker-data-freshness",
            label="Broker data freshness",
            status="blocked" if broker_data_stale else "ready",
            scope="portfolio",
            finding="stale_broker_data" if broker_data_stale else "broker_data_current",
            next_action="Refresh read-only broker context before relying on impact."
            if broker_data_stale
            else "Broker context is current enough for impact review.",
        ),
        _workbench_portfolio_impact_check(
            check_id="exposure-deltas",
            label="Exposure deltas",
            status="ready" if exposure_delta_ready else "blocked",
            scope="risk-desk",
            finding=(
                "exposure_deltas_available"
                if exposure_delta_ready
                else "missing_portfolio_impact:exposure_deltas"
            ),
            next_action=(
                "Review before/after concentration deltas before paper review."
                if exposure_delta_ready
                else "Rebuild portfolio impact before relying on concentration deltas."
            ),
        ),
        _workbench_portfolio_impact_check(
            check_id="portfolio-hard-blocks",
            label="Portfolio hard blocks",
            status="blocked" if portfolio_hard_blocks else "ready",
            scope="risk-desk",
            finding=portfolio_hard_blocks[0]
            if portfolio_hard_blocks
            else "no_portfolio_hard_blocks",
            next_action=(
                "Resolve portfolio hard blocks before paper review."
                if portfolio_hard_blocks
                else "No portfolio hard blocks are attached to the decision card."
            ),
        ),
        _workbench_portfolio_impact_check(
            check_id="paper-trade-gate",
            label="Paper trade gate",
            status="blocked" if paper_blocks else "ready",
            scope="paper-trading",
            finding=paper_blocks[0] if paper_blocks else "paper_trade_gate_clear",
            next_action="Resolve paper blocks before recording a local decision."
            if paper_blocks
            else "Paper gate is ready for supervised local review.",
        ),
        _workbench_portfolio_impact_check(
            check_id="live-submission-gate",
            label="Live submission gate",
            status="disabled",
            scope="broker",
            finding=(
                "broker_submission_disabled"
                if "broker_submission_disabled" in live_blocks
                else "live_submission_disabled"
            ),
            next_action="Live broker submission remains disabled.",
        ),
    ]
    ready_check_count = sum(1 for row in checks if row.get("status") == "ready")
    blocked_check_count = sum(1 for row in checks if row.get("status") == "blocked")
    disabled_check_count = sum(1 for row in checks if row.get("status") == "disabled")
    ticker = _first_value(impact.get("ticker"), active_plan.get("ticker"))
    decision_card_id = active_plan.get("decision_card_id")
    return {
        "schema_version": "trading-workbench-portfolio-impact-preview-v1",
        "status": "blocked" if blocked_check_count else "ready",
        "source_tool": "market-radar",
        "ticker": ticker,
        "decision_card_id": decision_card_id,
        "preview_id": f"impact-{str(ticker or 'unknown').lower()}-{decision_card_id or 'no-card'}",
        "impact_mode": "read_only_preview",
        "primary_blocker": primary_blocker,
        "primary_next_action": (
            "Refresh read-only broker context and rebuild impact before paper review."
            if primary_blocker
            else "Review portfolio impact before paper review."
        ),
        "impact": {
            "ticker": ticker,
            "proposed_notional": _workbench_round_float(proposed_notional),
            "proposed_notional_pct_of_equity": _workbench_round_ratio(
                _workbench_ratio(proposed_notional, portfolio_equity)
            ),
            "max_loss": _workbench_round_float(max_loss),
            "max_loss_pct_of_equity": _workbench_round_ratio(
                _workbench_ratio(max_loss, portfolio_equity)
            ),
            "portfolio_penalty": impact.get("portfolio_penalty"),
            "hard_blocks": portfolio_hard_blocks,
            "hard_block_count": len(portfolio_hard_blocks),
            "broker_connected": broker_connected,
            "broker_data_stale": broker_data_stale,
            "portfolio_equity": _workbench_round_float(portfolio_equity),
            "cash": _workbench_round_float(_optional_float(portfolio.get("cash"))),
            "buying_power": _workbench_round_float(
                _optional_float(portfolio.get("buying_power"))
            ),
            "current_gross_exposure_pct": portfolio.get("gross_exposure_pct"),
            "single_name_exposure_count": portfolio.get("single_name_exposure_count"),
            "submission_allowed": False,
            "broker_order_submitted": False,
            "live_trading_enabled": False,
        },
        "exposures": exposure_rows,
        "checks": checks,
        "blockers": blockers,
        "metrics": {
            "check_count": len(checks),
            "ready_check_count": ready_check_count,
            "blocked_check_count": blocked_check_count,
            "disabled_check_count": disabled_check_count,
            "exposure_scope_count": len(exposure_rows),
            "ready_exposure_scope_count": sum(
                1 for row in exposure_rows if row.get("status") == "ready"
            ),
            "hard_block_count": len(portfolio_hard_blocks),
            "proposed_notional": _workbench_round_float(proposed_notional),
            "max_loss": _workbench_round_float(max_loss),
            "current_gross_exposure_pct": portfolio.get("gross_exposure_pct"),
            "external_calls_made": 0,
            "db_writes_made": 0,
        },
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "live_trading_enabled": False,
    }


def _workbench_portfolio_impact_exposure_rows(
    impact: Mapping[str, object],
) -> list[dict[str, object]]:
    exposures = _mapping(impact.get("exposures"))
    return [
        _workbench_portfolio_impact_exposure_row(
            exposures,
            scope="single_name",
            label="Single name",
        ),
        _workbench_portfolio_impact_exposure_row(
            exposures,
            scope="sector",
            label="Sector",
        ),
        _workbench_portfolio_impact_exposure_row(
            exposures,
            scope="theme",
            label="Theme",
        ),
        _workbench_portfolio_impact_exposure_row(
            exposures,
            scope="correlated_basket",
            label="Correlated basket",
        ),
    ]


def _workbench_portfolio_impact_exposure_row(
    exposures: Mapping[str, object],
    *,
    scope: str,
    label: str,
) -> dict[str, object]:
    exposure = _mapping(exposures.get(scope))
    before = _optional_float(exposure.get("before_pct"))
    after = _optional_float(exposure.get("after_pct"))
    delta = _optional_float(exposure.get("delta_pct"))
    ready = before is not None and after is not None
    return {
        "scope": scope,
        "label": label,
        "status": "ready" if ready else "blocked",
        "before_pct": _workbench_round_ratio(before),
        "after_pct": _workbench_round_ratio(after),
        "delta_pct": _workbench_round_ratio(delta),
        "finding": "exposure_delta_available"
        if ready
        else f"missing_{scope}_exposure_delta",
        "next_action": "Review concentration change before paper review."
        if ready
        else "Refresh or rebuild portfolio impact for this exposure scope.",
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
    }


def _workbench_portfolio_impact_check(
    *,
    check_id: str,
    label: str,
    status: str,
    scope: str,
    finding: str,
    next_action: str,
) -> dict[str, object]:
    return {
        "id": check_id,
        "label": label,
        "status": status,
        "scope": scope,
        "finding": finding,
        "next_action": next_action,
    }


def _workbench_position_sizing_payload(
    *,
    active_plan: Mapping[str, object],
    scenario_matrix: Mapping[str, object],
    risk_envelope: Mapping[str, object],
) -> dict[str, object]:
    portfolio = _mapping(risk_envelope.get("portfolio_context"))
    sizing = _mapping(risk_envelope.get("sizing_context"))
    gate = _mapping(risk_envelope.get("gate_context"))
    risk = _mapping(active_plan.get("risk_approval"))
    order = _mapping(active_plan.get("order_intent"))
    assumptions = _mapping(scenario_matrix.get("assumptions"))
    entry_price = _optional_float(
        _first_value(sizing.get("entry_price"), assumptions.get("entry_price"))
    )
    invalidation_price = _optional_float(
        _first_value(
            sizing.get("invalidation_price"),
            assumptions.get("invalidation_price"),
        )
    )
    target_price = _optional_float(
        _first_value(sizing.get("target_price"), assumptions.get("target_price"))
    )
    risk_per_share = _optional_float(
        _first_value(sizing.get("risk_per_share"), assumptions.get("risk_per_share"))
    )
    if risk_per_share is None and entry_price is not None and invalidation_price is not None:
        risk_per_share = abs(entry_price - invalidation_price)
    portfolio_equity = _optional_float(portfolio.get("portfolio_equity"))
    buying_power = _optional_float(portfolio.get("buying_power"))
    risk_per_trade_pct = _optional_float(sizing.get("risk_per_trade_pct"))
    risk_budget = (
        portfolio_equity * risk_per_trade_pct
        if portfolio_equity is not None and risk_per_trade_pct is not None
        else None
    )
    suggested_quantity = None
    if risk_budget is not None and risk_per_share not in (None, 0):
        suggested_quantity = max(0, int(risk_budget // risk_per_share))
    estimated_notional = (
        suggested_quantity * entry_price
        if suggested_quantity is not None and entry_price is not None
        else None
    )
    estimated_max_loss = (
        suggested_quantity * risk_per_share
        if suggested_quantity is not None and risk_per_share is not None
        else None
    )
    current_quantity = _optional_int(order.get("quantity"))
    if current_quantity is not None and current_quantity <= 0:
        current_quantity = None
    blockers = list(dict.fromkeys(_texts(risk_envelope.get("blockers"))))
    paper_blocks = _texts(risk.get("paper_trade_blocks"))
    live_blocks = _texts(risk.get("live_submission_blocks"))
    primary_blocker = next(
        (
            blocker
            for blocker in blockers
            if blocker == "missing_position_sizing:shares"
        ),
        blockers[0] if blockers else None,
    )
    status = "blocked" if primary_blocker else "ready"
    checks = [
        _workbench_position_sizing_check(
            check_id="entry-stop-inputs",
            label="Entry and stop",
            status="ready"
            if entry_price is not None and invalidation_price is not None
            else "blocked",
            scope="trade-planner",
            finding="entry_stop_available"
            if entry_price is not None and invalidation_price is not None
            else "missing_entry_stop",
            next_action="Use entry and invalidation as risk-per-share inputs.",
        ),
        _workbench_position_sizing_check(
            check_id="risk-budget",
            label="Risk budget",
            status="ready" if risk_budget is not None else "blocked",
            scope="portfolio",
            finding=f"risk_budget={_workbench_round_float(risk_budget)}"
            if risk_budget is not None
            else "risk_budget_missing",
            next_action="Use the risk budget as a sizing ceiling.",
        ),
        _workbench_position_sizing_check(
            check_id="share-sizing",
            label="Share sizing",
            status="blocked" if current_quantity is None else "ready",
            scope="trade-planner",
            finding=(
                "missing_position_sizing:shares"
                if current_quantity is None
                else "shares_sized"
            ),
            next_action=(
                "Select or confirm shares before paper review."
                if current_quantity is None
                else "Use confirmed shares as paper-review input."
            ),
        ),
        _workbench_position_sizing_check(
            check_id="broker-data-freshness",
            label="Broker data freshness",
            status="blocked" if portfolio.get("broker_data_stale") else "ready",
            scope="portfolio",
            finding=(
                "stale_broker_data"
                if portfolio.get("broker_data_stale")
                else "broker_data_current"
            ),
            next_action=(
                "Refresh read-only broker context before relying on sizing."
                if portfolio.get("broker_data_stale")
                else "Broker context is current enough for sizing review."
            ),
        ),
        _workbench_position_sizing_check(
            check_id="paper-trade-gate",
            label="Paper trade gate",
            status="blocked" if paper_blocks else "ready",
            scope="paper-trading",
            finding=paper_blocks[0] if paper_blocks else "paper_trade_gate_clear",
            next_action=(
                "Resolve paper blocks before recording a local decision."
                if paper_blocks
                else "Paper gate is ready for supervised local review."
            ),
        ),
        _workbench_position_sizing_check(
            check_id="live-submission-gate",
            label="Live submission gate",
            status="disabled",
            scope="broker",
            finding=(
                "broker_submission_disabled"
                if "broker_submission_disabled" in live_blocks
                else "live_submission_disabled"
            ),
            next_action="Live broker submission remains disabled.",
        ),
    ]
    ready_check_count = sum(1 for row in checks if row.get("status") == "ready")
    blocked_check_count = sum(1 for row in checks if row.get("status") == "blocked")
    disabled_check_count = sum(1 for row in checks if row.get("status") == "disabled")
    return {
        "schema_version": "trading-workbench-position-sizing-v1",
        "status": status,
        "source_tool": risk_envelope.get("source_tool") or "market-radar",
        "ticker": _first_value(risk_envelope.get("ticker"), active_plan.get("ticker")),
        "decision_card_id": _first_value(
            risk_envelope.get("decision_card_id"),
            active_plan.get("decision_card_id"),
        ),
        "sizing_mode": "risk_budget_preview",
        "primary_blocker": primary_blocker,
        "primary_next_action": "Select or confirm shares before paper review."
        if primary_blocker
        else "Review sized shares before paper review.",
        "inputs": {
            "side": sizing.get("side"),
            "entry_price": _workbench_round_float(entry_price),
            "invalidation_price": _workbench_round_float(invalidation_price),
            "target_price": _workbench_round_float(target_price),
            "risk_per_share": _workbench_round_float(risk_per_share),
            "reward_risk": sizing.get("reward_risk"),
            "portfolio_equity": _workbench_round_float(portfolio_equity),
            "cash": _workbench_round_float(_optional_float(portfolio.get("cash"))),
            "buying_power": _workbench_round_float(buying_power),
            "risk_per_trade_pct": risk_per_trade_pct,
            "current_quantity": current_quantity,
            "current_estimated_notional": sizing.get("estimated_notional"),
            "current_estimated_max_loss": sizing.get("estimated_max_loss"),
        },
        "recommendation": {
            "status": status,
            "label": "Risk-budget preview",
            "suggested_quantity": suggested_quantity,
            "risk_budget": _workbench_round_float(risk_budget),
            "estimated_notional": _workbench_round_float(estimated_notional),
            "estimated_max_loss": _workbench_round_float(estimated_max_loss),
            "estimated_notional_pct_of_equity": _workbench_round_ratio(
                _workbench_ratio(estimated_notional, portfolio_equity)
            ),
            "estimated_max_loss_pct_of_equity": _workbench_round_ratio(
                _workbench_ratio(estimated_max_loss, portfolio_equity)
            ),
            "buying_power_usage_pct": _workbench_round_ratio(
                _workbench_ratio(estimated_notional, buying_power)
            ),
            "no_execution": True,
            "next_action": "Resolve sizing blockers before recording a local paper decision.",
        },
        "checks": checks,
        "blockers": blockers,
        "metrics": {
            "check_count": len(checks),
            "ready_check_count": ready_check_count,
            "blocked_check_count": blocked_check_count,
            "disabled_check_count": disabled_check_count,
            "suggested_quantity": suggested_quantity,
            "risk_budget": _workbench_round_float(risk_budget),
            "estimated_notional": _workbench_round_float(estimated_notional),
            "estimated_max_loss": _workbench_round_float(estimated_max_loss),
            "risk_per_share": _workbench_round_float(risk_per_share),
            "external_calls_made": 0,
        },
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": bool(gate.get("order_submission_allowed", False)),
        "live_trading_enabled": False,
    }


def _workbench_position_sizing_check(
    *,
    check_id: str,
    label: str,
    status: str,
    scope: str,
    finding: str,
    next_action: str,
) -> dict[str, object]:
    return {
        "id": check_id,
        "label": label,
        "status": status,
        "scope": scope,
        "finding": finding,
        "next_action": next_action,
    }


def _workbench_capital_allocation_payload(
    *,
    active_plan: Mapping[str, object],
    risk_envelope: Mapping[str, object],
    portfolio_impact_preview: Mapping[str, object],
    position_sizing: Mapping[str, object],
    portfolio_position_rows: Sequence[Mapping[str, object]],
    portfolio_open_order_rows: Sequence[Mapping[str, object]],
    paper_trade_rows: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    portfolio = _mapping(risk_envelope.get("portfolio_context"))
    impact = _mapping(portfolio_impact_preview.get("impact"))
    recommendation = _mapping(position_sizing.get("recommendation"))
    active_ticker = _learning_loop_ticker(
        _first_value(
            position_sizing.get("ticker"),
            portfolio_impact_preview.get("ticker"),
            risk_envelope.get("ticker"),
            active_plan.get("ticker"),
        )
    )
    decision_card_id = _first_value(
        position_sizing.get("decision_card_id"),
        portfolio_impact_preview.get("decision_card_id"),
        risk_envelope.get("decision_card_id"),
        active_plan.get("decision_card_id"),
    )
    portfolio_equity = _optional_float(portfolio.get("portfolio_equity"))
    buying_power = _optional_float(portfolio.get("buying_power"))
    cash = _optional_float(portfolio.get("cash"))
    proposed_notional = _optional_float(impact.get("proposed_notional"))
    suggested_notional = _optional_float(recommendation.get("estimated_notional"))
    suggested_max_loss = _optional_float(recommendation.get("estimated_max_loss"))
    suggested_quantity = _optional_int(recommendation.get("suggested_quantity"))
    risk_budget = _optional_float(recommendation.get("risk_budget"))
    proposed_max_loss = _optional_float(impact.get("max_loss"))
    effective_notional = _first_value(suggested_notional, proposed_notional)
    effective_max_loss = _first_value(suggested_max_loss, proposed_max_loss)
    buying_power_usage_pct = _workbench_round_ratio(
        _workbench_ratio(_optional_float(effective_notional), buying_power)
    )
    notional_pct_of_equity = _workbench_round_ratio(
        _workbench_ratio(_optional_float(effective_notional), portfolio_equity)
    )
    max_loss_pct_of_equity = _workbench_round_ratio(
        _workbench_ratio(_optional_float(effective_max_loss), portfolio_equity)
    )
    broker_data_stale = bool(portfolio.get("broker_data_stale"))
    exposure_ready_count = _first_nonnegative_int(
        _mapping(portfolio_impact_preview.get("metrics")).get(
            "ready_exposure_scope_count"
        )
    )
    exposure_scope_count = _first_nonnegative_int(
        _mapping(portfolio_impact_preview.get("metrics")).get(
            "exposure_scope_count"
        )
    )
    active_paper_rows = [
        row
        for row in _workbench_trade_monitor_matching_rows(
            paper_trade_rows,
            ticker=active_ticker,
        )
        if str(row.get("state") or "").strip().lower()
        in {"open", "active", "entered", "monitoring"}
    ]
    active_paper_notional = sum(
        _number_or_zero(row.get("notional")) for row in active_paper_rows
    )
    open_order_rows = [
        row
        for row in _workbench_trade_monitor_matching_rows(
            portfolio_open_order_rows,
            ticker=active_ticker,
        )
        if row.get("id")
        and str(row.get("status") or "").strip().lower()
        not in {"", "none", "cancelled", "canceled", "filled", "rejected"}
    ]
    current_position = _workbench_trade_monitor_match(
        portfolio_position_rows,
        ticker=active_ticker,
    )
    current_market_value = _optional_float(current_position.get("market_value"))
    current_exposure_pct = _optional_float(current_position.get("exposure_pct"))
    impact_blockers = _texts(portfolio_impact_preview.get("blockers"))
    sizing_blockers = _texts(position_sizing.get("blockers"))
    risk_blockers = _texts(risk_envelope.get("blockers"))
    blockers = list(dict.fromkeys([*impact_blockers, *sizing_blockers, *risk_blockers]))
    checks = [
        _workbench_capital_allocation_check(
            check_id="portfolio-capital-context",
            label="Portfolio capital context",
            status="blocked" if broker_data_stale else "ready",
            scope="portfolio",
            finding="stale_broker_data" if broker_data_stale else "capital_context_ready",
            evidence=(
                f"cash={_workbench_round_float(cash)}; "
                f"buying_power={_workbench_round_float(buying_power)}"
            ),
            next_action=(
                "Refresh read-only broker capital before relying on allocation."
                if broker_data_stale
                else "Use read-only capital context for allocation review."
            ),
        ),
        _workbench_capital_allocation_check(
            check_id="risk-budget-capacity",
            label="Risk budget capacity",
            status=(
                "ready"
                if risk_budget is not None and suggested_max_loss is not None
                else "blocked"
            ),
            scope="risk-desk",
            finding=f"risk_budget={_workbench_round_float(risk_budget)}"
            if risk_budget is not None
            else "missing_risk_budget",
            evidence=(
                f"max_loss={_workbench_round_float(suggested_max_loss)}; "
                f"loss_pct={max_loss_pct_of_equity}"
            ),
            next_action="Compare risk budget with suggested max loss before allocation.",
        ),
        _workbench_capital_allocation_check(
            check_id="buying-power-usage",
            label="Buying power usage",
            status="ready"
            if buying_power not in (None, 0)
            and _optional_float(effective_notional) is not None
            and _optional_float(effective_notional) <= buying_power
            else "blocked",
            scope="portfolio",
            finding=f"buying_power_usage={buying_power_usage_pct}"
            if buying_power_usage_pct is not None
            else "missing_buying_power_usage",
            evidence=f"suggested_notional={_workbench_round_float(suggested_notional)}",
            next_action="Review buying-power usage before creating any local ticket.",
        ),
        _workbench_capital_allocation_check(
            check_id="exposure-deltas",
            label="Exposure deltas",
            status="ready" if exposure_ready_count else "blocked",
            scope="risk-desk",
            finding="exposure_deltas_available"
            if exposure_ready_count
            else "missing_portfolio_impact:exposure_deltas",
            evidence=f"{exposure_ready_count}/{exposure_scope_count} exposure scopes ready",
            next_action=(
                "Review concentration deltas before allocating capital."
                if exposure_ready_count
                else "Refresh or rebuild portfolio impact before allocating capital."
            ),
        ),
        _workbench_capital_allocation_check(
            check_id="active-paper-capital",
            label="Active paper capital",
            status="review" if active_paper_rows else "ready",
            scope="paper-trading",
            finding="active_paper_trade_open"
            if active_paper_rows
            else "no_active_paper_trade",
            evidence=(
                f"{len(active_paper_rows)} active paper trades; "
                f"notional={_workbench_round_float(active_paper_notional)}"
            ),
            next_action=(
                "Review active paper exposure before adding capital to this idea."
                if active_paper_rows
                else "No active paper exposure is linked to this allocation."
            ),
        ),
        _workbench_capital_allocation_check(
            check_id="open-order-overlap",
            label="Open order overlap",
            status="review" if open_order_rows else "ready",
            scope="broker",
            finding="open_orders_present" if open_order_rows else "no_open_orders",
            evidence=f"{len(open_order_rows)} open orders",
            next_action=(
                "Review open orders before changing allocation state."
                if open_order_rows
                else "No read-only open orders overlap this allocation."
            ),
        ),
        _workbench_capital_allocation_check(
            check_id="allocation-boundary",
            label="Allocation boundary",
            status="disabled",
            scope="broker",
            finding="broker_submission_disabled",
            evidence="allocation changes require manual approval",
            next_action="No allocation, order, or broker state is changed from browsing.",
        ),
    ]
    ready_count = sum(1 for row in checks if row.get("status") == "ready")
    blocked_count = sum(1 for row in checks if row.get("status") == "blocked")
    review_count = sum(1 for row in checks if row.get("status") == "review")
    disabled_count = sum(1 for row in checks if row.get("status") == "disabled")
    primary_blocker = blockers[0] if blockers else None
    if primary_blocker is None:
        primary_blocker = next(
            (
                str(row.get("finding"))
                for row in checks
                if row.get("status") == "blocked" and row.get("finding")
            ),
            None,
        )
    return {
        "schema_version": "trading-workbench-capital-allocation-v1",
        "status": "blocked"
        if blocked_count
        else "review"
        if review_count
        else "ready",
        "source_tool": "market-radar",
        "ticker": active_ticker,
        "decision_card_id": decision_card_id,
        "allocation_id": (
            f"capital-allocation-{str(active_ticker or 'unknown').lower()}-"
            f"{decision_card_id or 'no-card'}"
        ),
        "allocation_mode": "read_only_capital_review",
        "primary_blocker": primary_blocker,
        "primary_next_action": (
            "Resolve allocation blockers before changing capital exposure."
            if primary_blocker
            else "Review capital allocation manually before any local ticket."
        ),
        "capital_context": {
            "broker_connected": bool(portfolio.get("broker_connected")),
            "broker_data_stale": broker_data_stale,
            "portfolio_equity": _workbench_round_float(portfolio_equity),
            "cash": _workbench_round_float(cash),
            "buying_power": _workbench_round_float(buying_power),
            "gross_exposure_pct": portfolio.get("gross_exposure_pct"),
            "position_count": _first_nonnegative_int(portfolio.get("position_count")),
            "open_order_count": len(open_order_rows),
            "current_position_market_value": _workbench_round_float(
                current_market_value
            ),
            "current_position_exposure_pct": _workbench_round_ratio(
                current_exposure_pct
            ),
        },
        "allocation_plan": {
            "proposed_notional": _workbench_round_float(proposed_notional),
            "proposed_notional_pct_of_equity": impact.get(
                "proposed_notional_pct_of_equity"
            ),
            "suggested_quantity": suggested_quantity,
            "suggested_notional": _workbench_round_float(suggested_notional),
            "suggested_notional_pct_of_equity": notional_pct_of_equity,
            "risk_budget": _workbench_round_float(risk_budget),
            "proposed_max_loss": _workbench_round_float(proposed_max_loss),
            "suggested_max_loss": _workbench_round_float(suggested_max_loss),
            "max_loss_pct_of_equity": max_loss_pct_of_equity,
            "buying_power_usage_pct": buying_power_usage_pct,
            "allocation_allowed": False,
            "requires_manual_approval": True,
            "no_execution": True,
        },
        "exposure_context": {
            "exposure_scope_count": exposure_scope_count,
            "ready_exposure_scope_count": exposure_ready_count,
            "missing_exposure_scope_count": max(
                0,
                exposure_scope_count - exposure_ready_count,
            ),
            "current_gross_exposure_pct": portfolio.get("gross_exposure_pct"),
            "projected_notional_pct_of_equity": notional_pct_of_equity,
            "has_exposure_deltas": exposure_ready_count > 0,
        },
        "checks": checks,
        "blockers": blockers,
        "metrics": {
            "check_count": len(checks),
            "ready_check_count": ready_count,
            "review_check_count": review_count,
            "blocked_check_count": blocked_count,
            "disabled_check_count": disabled_count,
            "suggested_quantity": suggested_quantity,
            "suggested_notional": _workbench_round_float(suggested_notional),
            "buying_power_usage_pct": buying_power_usage_pct,
            "active_paper_trade_count": len(active_paper_rows),
            "open_order_count": len(open_order_rows),
            "external_calls_made": 0,
            "db_writes_made": 0,
        },
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "allocation_update_allowed": False,
        "live_trading_enabled": False,
    }


def _workbench_capital_allocation_check(
    *,
    check_id: str,
    label: str,
    status: str,
    scope: str,
    finding: str,
    evidence: str,
    next_action: str,
) -> dict[str, object]:
    return {
        "id": check_id,
        "label": label,
        "status": status,
        "scope": scope,
        "finding": finding,
        "evidence": evidence,
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "allocation_update_allowed": False,
        "live_trading_enabled": False,
        "next_action": next_action,
    }


def _workbench_order_ticket_draft_payload(
    *,
    active_plan: Mapping[str, object],
    position_sizing: Mapping[str, object],
    risk_envelope: Mapping[str, object],
    action_bus: Mapping[str, object],
) -> dict[str, object]:
    order = _mapping(active_plan.get("order_intent"))
    ticket_source = _mapping(active_plan.get("order_ticket"))
    inputs = _mapping(position_sizing.get("inputs"))
    recommendation = _mapping(position_sizing.get("recommendation"))
    risk = _mapping(active_plan.get("risk_approval"))
    actions = {
        str(row.get("id")): row
        for row in _rows(action_bus.get("actions"))
        if row.get("id")
    }
    preview_action = _mapping(actions.get("order-ticket-preview"))
    record_action = _mapping(actions.get("order-ticket-record"))
    side = _first_value(ticket_source.get("side"), order.get("side"), inputs.get("side"))
    entry_price = _optional_float(
        _first_value(
            ticket_source.get("entry_price"),
            order.get("limit_price"),
            inputs.get("entry_price"),
        )
    )
    limit_price = _optional_float(_first_value(order.get("limit_price"), entry_price))
    stop_price = _optional_float(
        _first_value(
            order.get("stop_price"),
            ticket_source.get("invalidation_price"),
            inputs.get("invalidation_price"),
        )
    )
    invalidation_price = _optional_float(
        _first_value(ticket_source.get("invalidation_price"), stop_price)
    )
    quantity = _optional_int(
        _first_value(
            order.get("quantity"),
            ticket_source.get("quantity"),
            ticket_source.get("shares"),
        )
    )
    if quantity is not None and quantity <= 0:
        quantity = None
    suggested_quantity = _optional_int(recommendation.get("suggested_quantity"))
    if suggested_quantity is not None and suggested_quantity <= 0:
        suggested_quantity = None
    effective_quantity = quantity if quantity is not None else suggested_quantity
    risk_per_share = _optional_float(inputs.get("risk_per_share"))
    risk_budget = _optional_float(recommendation.get("risk_budget"))
    estimated_notional = _optional_float(recommendation.get("estimated_notional"))
    estimated_max_loss = _optional_float(recommendation.get("estimated_max_loss"))
    if (
        estimated_notional is None
        and effective_quantity is not None
        and limit_price is not None
    ):
        estimated_notional = effective_quantity * limit_price
    if (
        estimated_max_loss is None
        and effective_quantity is not None
        and risk_per_share is not None
    ):
        estimated_max_loss = effective_quantity * risk_per_share
    blockers = list(
        dict.fromkeys(
            _texts(position_sizing.get("blockers"))
            or _texts(risk_envelope.get("blockers"))
        )
    )
    paper_blocks = _texts(risk.get("paper_trade_blocks"))
    live_blocks = _texts(risk.get("live_submission_blocks"))
    if not blockers:
        blockers = list(dict.fromkeys([*paper_blocks, *live_blocks]))
    primary_blocker = (
        "missing_position_sizing:shares"
        if quantity is None
        else paper_blocks[0]
        if paper_blocks
        else blockers[0]
        if blockers
        else None
    )
    checks = [
        _workbench_order_ticket_draft_check(
            check_id="entry-stop-inputs",
            label="Entry and stop",
            status="ready"
            if entry_price is not None and invalidation_price is not None
            else "blocked",
            scope="trade-planner",
            finding="entry_stop_available"
            if entry_price is not None and invalidation_price is not None
            else "missing_entry_stop",
            next_action="Review entry, limit, and stop before saving a local blocked ticket.",
        ),
        _workbench_order_ticket_draft_check(
            check_id="quantity-confirmation",
            label="Quantity confirmation",
            status="blocked" if quantity is None else "ready",
            scope="trade-planner",
            finding=(
                "missing_position_sizing:shares"
                if quantity is None
                else "shares_confirmed"
            ),
            next_action=(
                "Confirm share quantity before saving a local blocked ticket."
                if quantity is None
                else "Use confirmed shares for the local blocked ticket."
            ),
        ),
        _workbench_order_ticket_draft_check(
            check_id="risk-budget",
            label="Risk budget",
            status="ready" if risk_budget is not None else "blocked",
            scope="risk-desk",
            finding=f"risk_budget={_workbench_round_float(risk_budget)}"
            if risk_budget is not None
            else "risk_budget_missing",
            next_action="Use the risk budget as a ticket sizing ceiling.",
        ),
        _workbench_order_ticket_draft_check(
            check_id="paper-trade-gate",
            label="Paper trade gate",
            status="blocked" if paper_blocks else "ready",
            scope="paper-trading",
            finding=paper_blocks[0] if paper_blocks else "paper_trade_gate_clear",
            next_action=(
                "Resolve paper blocks before saving a local blocked ticket."
                if paper_blocks
                else "Paper gate is ready for a supervised local ticket."
            ),
        ),
        _workbench_order_ticket_draft_check(
            check_id="live-submission-gate",
            label="Live submission gate",
            status="disabled",
            scope="broker",
            finding=(
                "broker_submission_disabled"
                if "broker_submission_disabled" in live_blocks
                else "live_submission_disabled"
            ),
            next_action="Live broker submission remains disabled.",
        ),
    ]
    ready_check_count = sum(1 for row in checks if row.get("status") == "ready")
    blocked_check_count = sum(1 for row in checks if row.get("status") == "blocked")
    disabled_check_count = sum(1 for row in checks if row.get("status") == "disabled")
    ticker = _first_value(
        ticket_source.get("ticker"),
        position_sizing.get("ticker"),
        risk_envelope.get("ticker"),
        active_plan.get("ticker"),
    )
    decision_card_id = _first_value(
        position_sizing.get("decision_card_id"),
        risk_envelope.get("decision_card_id"),
        active_plan.get("decision_card_id"),
    )
    return {
        "schema_version": "trading-workbench-order-ticket-draft-v1",
        "status": "blocked" if blocked_check_count else "ready",
        "source_tool": position_sizing.get("source_tool") or "market-radar",
        "ticker": ticker,
        "decision_card_id": decision_card_id,
        "draft_id": f"draft-{str(ticker or 'unknown').lower()}-{decision_card_id or 'no-card'}",
        "ticket_mode": "local_blocked_preview",
        "primary_blocker": primary_blocker,
        "primary_next_action": (
            "Confirm share quantity before saving a local blocked ticket."
            if primary_blocker
            else "Preview the local blocked ticket before any record step."
        ),
        "ticket": {
            "ticker": ticker,
            "side": side,
            "order_type": "stop_limit",
            "time_in_force": "day",
            "entry_price": _workbench_round_float(entry_price),
            "limit_price": _workbench_round_float(limit_price),
            "stop_price": _workbench_round_float(stop_price),
            "invalidation_price": _workbench_round_float(invalidation_price),
            "quantity": quantity,
            "suggested_quantity": suggested_quantity,
            "risk_per_share": _workbench_round_float(risk_per_share),
            "risk_budget": _workbench_round_float(risk_budget),
            "estimated_notional": _workbench_round_float(estimated_notional),
            "estimated_max_loss": _workbench_round_float(estimated_max_loss),
            "estimated_notional_pct_of_equity": recommendation.get(
                "estimated_notional_pct_of_equity"
            ),
            "estimated_max_loss_pct_of_equity": recommendation.get(
                "estimated_max_loss_pct_of_equity"
            ),
            "submission_allowed": False,
            "broker_order_submitted": False,
            "live_trading_enabled": False,
        },
        "commands": {
            "preview": preview_action.get("command")
            or ticket_source.get("preview_command")
            or "order-ticket preview",
            "record": record_action.get("command")
            or ticket_source.get("record_command")
            or "order-ticket record",
            "live_submit": "broker live submission",
        },
        "checks": checks,
        "blockers": blockers,
        "metrics": {
            "check_count": len(checks),
            "ready_check_count": ready_check_count,
            "blocked_check_count": blocked_check_count,
            "disabled_check_count": disabled_check_count,
            "suggested_quantity": suggested_quantity,
            "estimated_notional": _workbench_round_float(estimated_notional),
            "estimated_max_loss": _workbench_round_float(estimated_max_loss),
            "external_calls_made": 0,
            "db_writes_made": 0,
        },
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "live_trading_enabled": False,
    }


def _workbench_order_ticket_draft_check(
    *,
    check_id: str,
    label: str,
    status: str,
    scope: str,
    finding: str,
    next_action: str,
) -> dict[str, object]:
    return {
        "id": check_id,
        "label": label,
        "status": status,
        "scope": scope,
        "finding": finding,
        "next_action": next_action,
    }


def _workbench_paper_trade_preview_payload(
    *,
    active_plan: Mapping[str, object],
    position_sizing: Mapping[str, object],
    order_ticket_draft: Mapping[str, object],
    risk_envelope: Mapping[str, object],
    action_bus: Mapping[str, object],
) -> dict[str, object]:
    paper = _mapping(active_plan.get("paper_decision"))
    risk = _mapping(active_plan.get("risk_approval"))
    ticket = _mapping(order_ticket_draft.get("ticket"))
    sizing_recommendation = _mapping(position_sizing.get("recommendation"))
    sizing_inputs = _mapping(position_sizing.get("inputs"))
    actions = {
        str(row.get("id")): row
        for row in _rows(action_bus.get("actions"))
        if row.get("id")
    }
    preview_action = _mapping(actions.get("paper-decision-preview"))
    record_action = _mapping(actions.get("paper-decision-record"))
    paper_blocks = _texts(risk.get("paper_trade_blocks"))
    live_blocks = _texts(risk.get("live_submission_blocks"))
    blockers = list(
        dict.fromkeys(
            [
                *paper_blocks,
                *_texts(position_sizing.get("blockers")),
                *_texts(risk_envelope.get("blockers")),
                *live_blocks,
            ]
        )
    )
    decision_card_id = _first_value(
        paper.get("decision_card_id"),
        active_plan.get("decision_card_id"),
        order_ticket_draft.get("decision_card_id"),
    )
    ticker = _first_value(
        order_ticket_draft.get("ticker"),
        position_sizing.get("ticker"),
        risk_envelope.get("ticker"),
        active_plan.get("ticker"),
    )
    decision = paper.get("decision")
    entry_price = _optional_float(
        _first_value(
            paper.get("entry_price"),
            ticket.get("entry_price"),
            sizing_inputs.get("entry_price"),
        )
    )
    confirmed_quantity = _optional_int(ticket.get("quantity"))
    if confirmed_quantity is not None and confirmed_quantity <= 0:
        confirmed_quantity = None
    suggested_quantity = _optional_int(
        _first_value(
            ticket.get("suggested_quantity"),
            sizing_recommendation.get("suggested_quantity"),
        )
    )
    if suggested_quantity is not None and suggested_quantity <= 0:
        suggested_quantity = None
    confirmed_notional = (
        _optional_float(ticket.get("estimated_notional"))
        if confirmed_quantity is not None
        else None
    )
    if (
        confirmed_notional is None
        and confirmed_quantity is not None
        and entry_price is not None
    ):
        confirmed_notional = confirmed_quantity * entry_price
    suggested_notional = _optional_float(
        _first_value(
            sizing_recommendation.get("estimated_notional"),
            ticket.get("estimated_notional"),
        )
    )
    max_loss = _optional_float(
        _first_value(
            ticket.get("estimated_max_loss"),
            sizing_recommendation.get("estimated_max_loss"),
            risk.get("estimated_max_loss"),
        )
    )
    has_paper_intent = bool(decision_card_id and decision and paper.get("available_at"))
    preview_command = (
        preview_action.get("command")
        or paper.get("preview_command")
        or "paper-decision preview"
    )
    record_command = (
        record_action.get("command")
        or paper.get("execute_command")
        or "paper-decision execute"
    )
    record_local_write = bool(record_action.get("local_write_allowed"))
    record_db_writes_required = _first_nonnegative_int(
        record_action.get("db_writes_required")
    )
    if record_local_write and record_db_writes_required <= 0:
        record_db_writes_required = 2
    primary_blocker = (
        paper_blocks[0]
        if paper_blocks
        else "missing_position_sizing:shares"
        if confirmed_quantity is None
        else blockers[0]
        if blockers
        else None
    )
    checks = [
        _workbench_paper_trade_preview_check(
            check_id="paper-intent-source",
            label="Paper intent source",
            status="ready" if has_paper_intent else "blocked",
            scope="market-radar",
            finding="paper_intent_available"
            if has_paper_intent
            else "missing_paper_intent",
            next_action="Use the stored agentic paper intent for preview."
            if has_paper_intent
            else "Review or rebuild the decision card before paper preview.",
        ),
        _workbench_paper_trade_preview_check(
            check_id="paper-risk-gate",
            label="Paper risk gate",
            status="blocked" if paper_blocks else "ready",
            scope="risk-desk",
            finding=paper_blocks[0] if paper_blocks else "paper_trade_gate_clear",
            next_action="Resolve paper-trade blockers before recording locally."
            if paper_blocks
            else "Paper risk gate is clear for supervised preview.",
        ),
        _workbench_paper_trade_preview_check(
            check_id="confirmed-size",
            label="Confirmed size",
            status="blocked" if confirmed_quantity is None else "ready",
            scope="trade-planner",
            finding="missing_position_sizing:shares"
            if confirmed_quantity is None
            else "shares_confirmed",
            next_action="Confirm share quantity before recording a paper decision."
            if confirmed_quantity is None
            else "Use confirmed size for the paper decision.",
        ),
        _workbench_paper_trade_preview_check(
            check_id="preview-command",
            label="Preview command",
            status="ready" if preview_command else "blocked",
            scope="paper-trading",
            finding="local_preview_available"
            if preview_command
            else "missing_preview_command",
            next_action="Preview through the backend without writing rows."
            if preview_command
            else "Rebuild paper intent command metadata.",
        ),
        _workbench_paper_trade_preview_check(
            check_id="record-command",
            label="Record command",
            status="approval_required" if record_local_write else "disabled",
            scope="paper-trading",
            finding="manual_arm_required"
            if record_local_write
            else "local_record_disabled",
            next_action="Arm and record only after manual approval."
            if record_local_write
            else "Local record is disabled for this preview.",
        ),
        _workbench_paper_trade_preview_check(
            check_id="live-submission-gate",
            label="Live submission gate",
            status="disabled",
            scope="broker",
            finding=(
                "broker_submission_disabled"
                if "broker_submission_disabled" in live_blocks
                else "live_submission_disabled"
            ),
            next_action="Live broker submission remains disabled.",
        ),
    ]
    ready_check_count = sum(1 for row in checks if row.get("status") == "ready")
    blocked_check_count = sum(1 for row in checks if row.get("status") == "blocked")
    approval_required_count = sum(
        1 for row in checks if row.get("status") == "approval_required"
    )
    disabled_check_count = sum(1 for row in checks if row.get("status") == "disabled")
    return {
        "schema_version": "trading-workbench-paper-trade-preview-v1",
        "status": "blocked" if blocked_check_count else "ready",
        "source_tool": "market-radar",
        "ticker": ticker,
        "decision_card_id": decision_card_id,
        "preview_id": (
            f"paper-preview-{str(ticker or 'unknown').lower()}-"
            f"{decision_card_id or 'no-card'}"
        ),
        "preview_mode": "supervised_paper_preview",
        "primary_blocker": primary_blocker,
        "primary_next_action": (
            "Resolve paper-trade blockers before recording locally."
            if primary_blocker
            else "Preview the paper decision before any guarded record step."
        ),
        "paper_decision": {
            "decision": decision,
            "available_at": paper.get("available_at"),
            "entry_price": _workbench_round_float(entry_price),
            "entry_at": paper.get("entry_at"),
            "confirmed_quantity": confirmed_quantity,
            "suggested_quantity": suggested_quantity,
            "confirmed_notional": _workbench_round_float(confirmed_notional),
            "suggested_notional": _workbench_round_float(suggested_notional),
            "estimated_max_loss": _workbench_round_float(max_loss),
            "paper_approved": bool(risk.get("approved_for_paper_trade")),
            "record_allowed": False,
            "requires_arm_before_record": record_local_write,
            "record_db_writes_required": record_db_writes_required,
            "no_execution": True,
        },
        "commands": {
            "preview": preview_command,
            "record": record_command,
            "live_submit": "broker live submission",
        },
        "checks": checks,
        "blockers": blockers,
        "metrics": {
            "check_count": len(checks),
            "ready_check_count": ready_check_count,
            "blocked_check_count": blocked_check_count,
            "approval_required_count": approval_required_count,
            "disabled_check_count": disabled_check_count,
            "paper_block_count": len(paper_blocks),
            "confirmed_quantity": confirmed_quantity,
            "suggested_quantity": suggested_quantity,
            "confirmed_notional": _workbench_round_float(confirmed_notional),
            "suggested_notional": _workbench_round_float(suggested_notional),
            "estimated_max_loss": _workbench_round_float(max_loss),
            "external_calls_made": 0,
            "db_writes_made": 0,
        },
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "live_trading_enabled": False,
    }


def _workbench_paper_trade_preview_check(
    *,
    check_id: str,
    label: str,
    status: str,
    scope: str,
    finding: str,
    next_action: str,
) -> dict[str, object]:
    return {
        "id": check_id,
        "label": label,
        "status": status,
        "scope": scope,
        "finding": finding,
        "next_action": next_action,
    }


def _workbench_pretrade_compliance_payload(
    *,
    active_plan: Mapping[str, object],
    decision_brief: Mapping[str, object],
    risk_envelope: Mapping[str, object],
    capital_allocation: Mapping[str, object],
    order_ticket_draft: Mapping[str, object],
    paper_trade_preview: Mapping[str, object],
    supervision_gates: Mapping[str, object],
) -> dict[str, object]:
    risk = _mapping(active_plan.get("risk_approval"))
    setup = _mapping(decision_brief.get("setup"))
    risk_gate = _mapping(risk_envelope.get("gate_context"))
    risk_sizing = _mapping(risk_envelope.get("sizing_context"))
    allocation_plan = _mapping(capital_allocation.get("allocation_plan"))
    ticket = _mapping(order_ticket_draft.get("ticket"))
    paper_decision = _mapping(paper_trade_preview.get("paper_decision"))
    supervision_metrics = _mapping(supervision_gates.get("metrics"))
    allocation_metrics = _mapping(capital_allocation.get("metrics"))
    paper_metrics = _mapping(paper_trade_preview.get("metrics"))
    risk_metrics = _mapping(risk_envelope.get("metrics"))
    ticker = _learning_loop_ticker(
        _first_value(
            paper_trade_preview.get("ticker"),
            order_ticket_draft.get("ticker"),
            capital_allocation.get("ticker"),
            risk_envelope.get("ticker"),
            decision_brief.get("ticker"),
            active_plan.get("ticker"),
        )
    )
    decision_card_id = _first_value(
        paper_trade_preview.get("decision_card_id"),
        order_ticket_draft.get("decision_card_id"),
        capital_allocation.get("decision_card_id"),
        risk_envelope.get("decision_card_id"),
        decision_brief.get("decision_card_id"),
        active_plan.get("decision_card_id"),
    )
    paper_blocks = _texts(risk.get("paper_trade_blocks"))
    live_blocks = _texts(risk.get("live_submission_blocks"))
    blockers = list(
        dict.fromkeys(
            [
                *_texts(risk_envelope.get("blockers")),
                *_texts(capital_allocation.get("blockers")),
                *_texts(order_ticket_draft.get("blockers")),
                *_texts(paper_trade_preview.get("blockers")),
            ]
        )
    )
    active_status = str(active_plan.get("status") or "missing")
    approval_required_count = _first_nonnegative_int(
        supervision_metrics.get("approval_required_count")
    )
    checks = [
        _workbench_pretrade_compliance_check(
            check_id="decision-state",
            label="Decision state",
            status="blocked" if active_status == "blocked" or paper_blocks else "ready",
            scope="decision-review",
            finding=paper_blocks[0] if paper_blocks else "decision_state_ready",
            evidence=(
                f"action_state={setup.get('action_state')}; "
                f"decision_card={decision_card_id or 'none'}"
            ),
            next_action=(
                "Resolve decision-state blockers before any local trade record."
                if paper_blocks or active_status == "blocked"
                else "Decision state is ready for supervised pre-trade review."
            ),
        ),
        _workbench_pretrade_compliance_check(
            check_id="risk-envelope",
            label="Risk envelope",
            status="blocked"
            if str(risk_envelope.get("status") or "") == "blocked"
            else "ready",
            scope="risk-desk",
            finding=_texts(risk_envelope.get("blockers"))[0]
            if _texts(risk_envelope.get("blockers"))
            else "risk_envelope_ready",
            evidence=(
                f"risk_blocks={_first_nonnegative_int(risk_metrics.get('blocked_check_count'))}; "
                f"max_loss={risk_sizing.get('estimated_max_loss')}"
            ),
            next_action="Resolve risk envelope checks before proceeding.",
        ),
        _workbench_pretrade_compliance_check(
            check_id="capital-allocation",
            label="Capital allocation",
            status=str(capital_allocation.get("status") or "unknown"),
            scope="portfolio",
            finding=capital_allocation.get("primary_blocker")
            or capital_allocation.get("status")
            or "allocation_review",
            evidence=(
                f"suggested_notional={allocation_plan.get('suggested_notional')}; "
                f"buying_power_usage={allocation_plan.get('buying_power_usage_pct')}; "
                f"allocation_allowed={allocation_plan.get('allocation_allowed')}"
            ),
            next_action=capital_allocation.get("primary_next_action")
            or "Review capital allocation before ticketing.",
        ),
        _workbench_pretrade_compliance_check(
            check_id="order-ticket-draft",
            label="Order ticket draft",
            status=str(order_ticket_draft.get("status") or "unknown"),
            scope="broker",
            finding=order_ticket_draft.get("primary_blocker")
            or order_ticket_draft.get("status")
            or "ticket_review",
            evidence=(
                f"suggested_quantity={ticket.get('suggested_quantity')}; "
                f"submission_allowed={ticket.get('submission_allowed')}"
            ),
            next_action=order_ticket_draft.get("primary_next_action")
            or "Review blocked local ticket before any broker handoff.",
        ),
        _workbench_pretrade_compliance_check(
            check_id="paper-trade-preview",
            label="Paper trade preview",
            status=str(paper_trade_preview.get("status") or "unknown"),
            scope="paper-trading",
            finding=paper_trade_preview.get("primary_blocker")
            or paper_trade_preview.get("status")
            or "paper_preview",
            evidence=(
                f"decision={paper_decision.get('decision')}; "
                f"paper_blocks={_first_nonnegative_int(paper_metrics.get('paper_block_count'))}; "
                f"record_allowed={paper_decision.get('record_allowed')}"
            ),
            next_action=paper_trade_preview.get("primary_next_action")
            or "Preview paper trade before any local record.",
        ),
        _workbench_pretrade_compliance_check(
            check_id="supervision-approval",
            label="Supervision approval",
            status="approval_required" if approval_required_count else "ready",
            scope="supervision",
            finding=supervision_gates.get("primary_gate_id") or "supervision_ready",
            evidence=f"approval_required_count={approval_required_count}",
            next_action=(
                "Require manual approval before guarded local writes."
                if approval_required_count
                else "No supervision approval gate is pending."
            ),
        ),
        _workbench_pretrade_compliance_check(
            check_id="live-execution-boundary",
            label="Live execution boundary",
            status="disabled",
            scope="broker",
            finding="live_trading_disabled",
            evidence="broker_submission_disabled; autonomous_execution_disabled",
            next_action="Do not submit broker orders or run autonomous execution.",
        ),
    ]
    ready_count = sum(1 for row in checks if row.get("status") == "ready")
    blocked_count = sum(1 for row in checks if row.get("status") == "blocked")
    approval_count = sum(
        1 for row in checks if row.get("status") == "approval_required"
    )
    disabled_count = sum(1 for row in checks if row.get("status") == "disabled")
    primary_blocker = blockers[0] if blockers else None
    if primary_blocker is None:
        primary_blocker = next(
            (
                str(row.get("finding"))
                for row in checks
                if row.get("status") == "blocked" and row.get("finding")
            ),
            None,
        )
    return {
        "schema_version": "trading-workbench-pretrade-compliance-v1",
        "status": "blocked"
        if blocked_count
        else "approval_required"
        if approval_count
        else "ready",
        "source_tool": "market-radar",
        "ticker": ticker,
        "decision_card_id": decision_card_id,
        "compliance_id": (
            f"pretrade-compliance-{str(ticker or 'unknown').lower()}-"
            f"{decision_card_id or 'no-card'}"
        ),
        "review_mode": "read_only_pretrade_compliance",
        "primary_blocker": primary_blocker,
        "primary_next_action": (
            "Resolve pre-trade compliance blockers before any local trade record or broker handoff."
            if primary_blocker
            else "Review manual approval state before any local trade record."
        ),
        "trade_context": {
            "active_plan_status": active_plan.get("status"),
            "action_state": setup.get("action_state"),
            "recommended_paper_decision": active_plan.get(
                "recommended_paper_decision"
            ),
            "side": ticket.get("side") or _mapping(active_plan.get("order_intent")).get(
                "side"
            ),
            "suggested_quantity": ticket.get("suggested_quantity"),
            "suggested_notional": allocation_plan.get("suggested_notional")
            or ticket.get("estimated_notional"),
            "estimated_max_loss": ticket.get("estimated_max_loss")
            or risk_sizing.get("estimated_max_loss"),
            "paper_approved": bool(risk_gate.get("paper_approved")),
            "live_approved": bool(risk_gate.get("live_approved")),
            "allocation_allowed": bool(allocation_plan.get("allocation_allowed")),
            "order_submission_allowed": False,
        },
        "boundary_context": {
            "requires_manual_approval": bool(
                risk_gate.get("requires_manual_approval")
                or approval_required_count
                or risk.get("requires_manual_approval")
            ),
            "approval_required_count": approval_required_count,
            "broker_order_submission": "disabled",
            "broker_order_submitted": False,
            "order_submission_allowed": False,
            "live_trading_enabled": False,
            "autonomous_execution": "disabled",
            "external_calls_made": 0,
            "db_writes_made": 0,
            "no_execution": True,
        },
        "checks": checks,
        "blockers": blockers,
        "metrics": {
            "check_count": len(checks),
            "ready_check_count": ready_count,
            "blocked_check_count": blocked_count,
            "approval_required_count": approval_count,
            "disabled_check_count": disabled_count,
            "compliance_blocker_count": len(blockers),
            "risk_block_count": _first_nonnegative_int(
                risk_metrics.get("blocked_check_count")
            ),
            "allocation_blocked_check_count": _first_nonnegative_int(
                allocation_metrics.get("blocked_check_count")
            ),
            "paper_block_count": len(paper_blocks),
            "live_block_count": len(live_blocks),
            "external_calls_made": 0,
            "db_writes_made": 0,
        },
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "live_trading_enabled": False,
    }


def _workbench_pretrade_compliance_check(
    *,
    check_id: str,
    label: str,
    status: str,
    scope: str,
    finding: object,
    evidence: str,
    next_action: object,
) -> dict[str, object]:
    return {
        "id": check_id,
        "label": label,
        "status": status,
        "scope": scope,
        "finding": finding,
        "evidence": evidence,
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "live_trading_enabled": False,
        "next_action": next_action,
    }


def _workbench_trade_readiness_brief_payload(
    *,
    decision_brief: Mapping[str, object],
    risk_envelope: Mapping[str, object],
    capital_allocation: Mapping[str, object],
    paper_trade_preview: Mapping[str, object],
    pretrade_compliance: Mapping[str, object],
    learning_loop: Mapping[str, object],
    strategy_review: Mapping[str, object],
    trade_monitor: Mapping[str, object],
    trade_runbook: Mapping[str, object],
    operator_state: Mapping[str, object],
    execution_sandbox: Mapping[str, object],
) -> dict[str, object]:
    ticker = _learning_loop_ticker(
        _first_value(
            pretrade_compliance.get("ticker"),
            trade_monitor.get("ticker"),
            strategy_review.get("ticker"),
            learning_loop.get("ticker"),
            decision_brief.get("ticker"),
        )
    )
    decision_card_id = _first_value(
        pretrade_compliance.get("decision_card_id"),
        trade_monitor.get("decision_card_id"),
        strategy_review.get("decision_card_id"),
        learning_loop.get("decision_card_id"),
        decision_brief.get("decision_card_id"),
    )
    paper_decision = _mapping(paper_trade_preview.get("paper_decision"))
    pretrade_metrics = _mapping(pretrade_compliance.get("metrics"))
    risk_metrics = _mapping(risk_envelope.get("metrics"))
    risk_sizing = _mapping(risk_envelope.get("sizing_context"))
    allocation_plan = _mapping(capital_allocation.get("allocation_plan"))
    learning_metrics = _mapping(learning_loop.get("metrics"))
    strategy_metrics = _mapping(strategy_review.get("metrics"))
    monitor_metrics = _mapping(trade_monitor.get("metrics"))
    runbook_metrics = _mapping(trade_runbook.get("metrics"))
    operator_handoff = _mapping(operator_state.get("agent_handoff"))
    sandbox_metrics = _mapping(execution_sandbox.get("metrics"))
    strategy_update_allowed = bool(strategy_review.get("strategy_update_allowed"))
    monitor_ready = str(trade_monitor.get("status") or "") == "ready"
    pretrade_status = str(pretrade_compliance.get("status") or "unknown")
    paper_status = str(paper_trade_preview.get("status") or "unknown")
    paper_record_allowed = bool(paper_decision.get("record_allowed")) and (
        pretrade_status == "ready"
    )
    paper_record_status = (
        "blocked"
        if pretrade_status == "blocked" or paper_status == "blocked"
        else "approval_required"
        if (
            pretrade_status == "approval_required"
            or paper_decision.get("requires_arm_before_record")
        )
        else "ready"
    )
    checks = [
        _workbench_trade_readiness_check(
            check_id="market-radar-scout",
            label="MarketRadar scout",
            status="ready" if decision_card_id else "blocked",
            module="market-radar",
            gate_kind="source",
            finding="scout_linked" if decision_card_id else "missing_decision_card",
            evidence=(
                f"source_tool={decision_brief.get('source_tool') or 'market-radar'}; "
                f"ticker={ticker or 'none'}; decision_card={decision_card_id or 'none'}"
            ),
            next_action=decision_brief.get("primary_next_action")
            or "Review the scouted MarketRadar case before acting.",
        ),
        _workbench_trade_readiness_check(
            check_id="decision-readiness",
            label="Decision readiness",
            status=str(decision_brief.get("status") or "unknown"),
            module="review",
            gate_kind="decision",
            finding=decision_brief.get("primary_blocker") or decision_brief.get("status"),
            evidence=(
                f"decision_card={decision_card_id or 'none'}; "
                f"action_state={_mapping(decision_brief.get('setup')).get('action_state')}"
            ),
            next_action=trade_runbook.get("primary_next_action")
            or "Review decision readiness before continuing.",
        ),
        _workbench_trade_readiness_check(
            check_id="risk-readiness",
            label="Risk readiness",
            status=str(risk_envelope.get("status") or "unknown"),
            module="risk-desk",
            gate_kind="risk",
            finding=risk_envelope.get("primary_blocker") or risk_envelope.get("status"),
            evidence=(
                f"risk_blocks={_first_nonnegative_int(risk_metrics.get('blocked_check_count'))}; "
                f"max_loss={risk_sizing.get('estimated_max_loss')}"
            ),
            next_action=risk_envelope.get("primary_next_action")
            or "Resolve risk-envelope blockers before local writes.",
        ),
        _workbench_trade_readiness_check(
            check_id="capital-readiness",
            label="Capital readiness",
            status=str(capital_allocation.get("status") or "unknown"),
            module="portfolio",
            gate_kind="allocation",
            finding=capital_allocation.get("primary_blocker")
            or capital_allocation.get("status"),
            evidence=(
                f"suggested_notional={allocation_plan.get('suggested_notional')}; "
                f"allocation_allowed={allocation_plan.get('allocation_allowed')}"
            ),
            next_action=capital_allocation.get("primary_next_action")
            or "Review capital allocation before ticketing.",
        ),
        _workbench_trade_readiness_check(
            check_id="pretrade-compliance",
            label="Pre-trade compliance",
            status=pretrade_status,
            module="broker",
            gate_kind="compliance",
            finding=pretrade_compliance.get("primary_blocker")
            or pretrade_compliance.get("status"),
            evidence=(
                "blocked_checks="
                f"{_first_nonnegative_int(pretrade_metrics.get('blocked_check_count'))}; "
                "approval_required="
                f"{_first_nonnegative_int(pretrade_metrics.get('approval_required_count'))}"
            ),
            next_action=pretrade_compliance.get("primary_next_action")
            or "Review pre-trade compliance before broker handoff.",
        ),
        _workbench_trade_readiness_check(
            check_id="paper-record-readiness",
            label="Paper record readiness",
            status=paper_record_status,
            module="paper-trading",
            gate_kind="paper-record",
            finding=paper_trade_preview.get("primary_blocker")
            or (
                "manual_arm_required"
                if paper_record_status == "approval_required"
                else paper_status
            ),
            evidence=(
                f"decision={paper_decision.get('decision')}; "
                f"record_allowed={paper_record_allowed}; "
                f"requires_arm={paper_decision.get('requires_arm_before_record')}"
            ),
            next_action=paper_trade_preview.get("primary_next_action")
            or "Preview and record paper state only after approval.",
        ),
        _workbench_trade_readiness_check(
            check_id="learning-readiness",
            label="Learning readiness",
            status=str(learning_loop.get("status") or "unknown"),
            module="journal",
            gate_kind="learning-loop",
            finding=learning_loop.get("primary_blocker") or learning_loop.get("status"),
            evidence=(
                f"stage={learning_loop.get('learning_stage')}; "
                f"blocked_cards={_first_nonnegative_int(learning_metrics.get('blocked_card_count'))}"
            ),
            next_action=learning_loop.get("primary_next_action")
            or "Review learning loop evidence before strategy changes.",
        ),
        _workbench_trade_readiness_check(
            check_id="strategy-update-boundary",
            label="Strategy update boundary",
            status="ready" if strategy_update_allowed else "disabled",
            module="agent",
            gate_kind="strategy-update",
            finding="strategy_update_allowed"
            if strategy_update_allowed
            else "autonomous_strategy_update_disabled",
            evidence=(
                f"strategy_update_allowed={strategy_update_allowed}; "
                f"blocked_hypotheses={_first_nonnegative_int(strategy_metrics.get('blocked_hypothesis_count'))}"
            ),
            next_action=strategy_review.get("primary_next_action")
            or "Do not auto-change strategy rules.",
        ),
        _workbench_trade_readiness_check(
            check_id="monitoring-readiness",
            label="Monitoring readiness",
            status=str(trade_monitor.get("status") or "unknown"),
            module="portfolio",
            gate_kind="monitor",
            finding=trade_monitor.get("primary_blocker") or trade_monitor.get("status"),
            evidence=(
                "active_paper_trades="
                f"{_first_nonnegative_int(monitor_metrics.get('active_paper_trade_count'))}; "
                "watch_blockers="
                f"{_first_nonnegative_int(monitor_metrics.get('blocked_watch_item_count'))}"
            ),
            next_action=trade_monitor.get("primary_next_action")
            or "Review monitoring state before changing position state.",
        ),
        _workbench_trade_readiness_check(
            check_id="broker-handoff-boundary",
            label="Broker handoff boundary",
            status="disabled",
            module="broker",
            gate_kind="broker-boundary",
            finding="live_trading_disabled",
            evidence="broker_submission_disabled; autonomous_execution_disabled",
            next_action="Do not submit broker orders or run autonomous execution.",
        ),
    ]
    ready_count = sum(1 for row in checks if row.get("status") == "ready")
    blocked_count = sum(1 for row in checks if row.get("status") == "blocked")
    approval_required_count = sum(
        1 for row in checks if row.get("status") == "approval_required"
    )
    disabled_count = sum(1 for row in checks if row.get("status") == "disabled")
    primary_blocker = (
        pretrade_compliance.get("primary_blocker")
        or operator_state.get("primary_blocker")
        or next(
            (
                row.get("finding")
                for row in checks
                if row.get("status") == "blocked" and row.get("finding")
            ),
            None,
        )
    )
    status = (
        "blocked"
        if blocked_count
        else "approval_required"
        if approval_required_count
        else "ready"
    )
    return {
        "schema_version": "trading-workbench-readiness-brief-v1",
        "status": status,
        "source_tool": decision_brief.get("source_tool") or "market-radar",
        "ticker": ticker,
        "decision_card_id": decision_card_id,
        "readiness_id": (
            f"readiness-brief-{str(ticker or 'unknown').lower()}-"
            f"{decision_card_id or 'no-card'}"
        ),
        "operating_mode": "agentic_decision_support",
        "primary_blocker": primary_blocker,
        "primary_next_action": (
            "Resolve trade-readiness blockers before paper record, broker handoff, "
            "or strategy update."
            if status == "blocked"
            else "Require manual approval before guarded local writes."
            if status == "approval_required"
            else "Ready for supervised paper preview; live trading remains disabled."
        ),
        "agent_handoff": {
            "next_page": operator_handoff.get("next_page") or "review",
            "next_command": operator_handoff.get("next_command") or "review",
            "safety": operator_handoff.get("safety") or "zero_call_navigation",
            "can_execute_without_approval": bool(
                operator_handoff.get("can_execute_without_approval")
            ),
            "local_write_requires_arm": bool(
                operator_handoff.get("local_write_requires_arm")
            ),
        },
        "readiness_modes": {
            "paper_record": {
                "status": paper_record_status,
                "allowed": paper_record_allowed,
                "approval_required": paper_record_status == "approval_required",
                "record_db_writes_required": _first_nonnegative_int(
                    paper_decision.get("record_db_writes_required")
                ),
            },
            "broker_handoff": {
                "status": "disabled",
                "allowed": False,
                "broker_order_submitted": False,
                "order_submission_allowed": False,
            },
            "strategy_update": {
                "status": "ready" if strategy_update_allowed else "disabled",
                "allowed": strategy_update_allowed,
                "autonomous_update_allowed": bool(
                    strategy_review.get("autonomous_update_allowed")
                ),
            },
            "monitoring": {
                "status": trade_monitor.get("status") or "unknown",
                "ready": monitor_ready,
                "active_paper_trade_count": _first_nonnegative_int(
                    monitor_metrics.get("active_paper_trade_count")
                ),
            },
        },
        "checks": checks,
        "metrics": {
            "check_count": len(checks),
            "ready_check_count": ready_count,
            "blocked_check_count": blocked_count,
            "approval_required_count": approval_required_count,
            "disabled_check_count": disabled_count,
            "runbook_blocked_step_count": _first_nonnegative_int(
                runbook_metrics.get("blocked_step_count")
            ),
            "pretrade_blocked_check_count": _first_nonnegative_int(
                pretrade_metrics.get("blocked_check_count")
            ),
            "sandbox_disabled_lane_count": _first_nonnegative_int(
                sandbox_metrics.get("disabled_lane_count")
            ),
            "external_calls_made": 0,
            "db_writes_made": 0,
        },
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "live_trading_enabled": False,
        "paper_record_allowed": paper_record_allowed,
        "broker_handoff_allowed": False,
        "strategy_update_allowed": strategy_update_allowed,
        "monitoring_ready": monitor_ready,
    }


def _workbench_trade_readiness_check(
    *,
    check_id: str,
    label: str,
    status: str,
    module: str,
    gate_kind: str,
    finding: object,
    evidence: str,
    next_action: object,
) -> dict[str, object]:
    return {
        "id": check_id,
        "label": label,
        "status": status,
        "module": module,
        "gate_kind": gate_kind,
        "finding": finding,
        "evidence": evidence,
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "live_trading_enabled": False,
        "next_action": next_action,
    }


def _workbench_agent_playbook_payload(
    *,
    trade_readiness_brief: Mapping[str, object],
    priority_queue: Mapping[str, object],
    action_bus: Mapping[str, object],
    execution_sandbox: Mapping[str, object],
    operator_state: Mapping[str, object],
) -> dict[str, object]:
    priority_items = {
        str(row.get("id")): row
        for row in _rows(priority_queue.get("items"))
        if row.get("id")
    }
    lanes = {
        str(row.get("id")): row
        for row in _rows(execution_sandbox.get("lanes"))
        if row.get("id")
    }
    checks = {
        str(row.get("id")): row
        for row in _rows(trade_readiness_brief.get("checks"))
        if row.get("id")
    }
    tasks: list[dict[str, object]] = []

    def add_priority_task(item_id: str, *, task_kind: str, source: str) -> None:
        item = priority_items.get(item_id)
        if not item:
            return
        tasks.append(
            _workbench_agent_playbook_task(
                task_id=item_id,
                rank=len(tasks) + 1,
                module=str(item.get("module") or "agent"),
                label=str(item.get("label") or item_id),
                status=_workbench_agent_playbook_status(item.get("status")),
                task_kind=task_kind,
                action_kind=str(item.get("action_kind") or "review"),
                command=item.get("command"),
                target_page=item.get("target_page") or item.get("module"),
                safety=item.get("safety") or "zero_call_navigation",
                source=source,
                source_id=item_id,
                evidence=item.get("reason") or item.get("label") or item_id,
                next_action=item.get("next_action")
                or "Review this supervised playbook task.",
                local_write_allowed=bool(item.get("local_write_allowed")),
                db_writes_required=item.get("db_writes_required"),
            )
        )

    def add_lane_task(
        lane_id: str,
        *,
        task_id: str,
        label: str,
        task_kind: str,
        source: str,
    ) -> None:
        lane = lanes.get(lane_id)
        if not lane:
            return
        tasks.append(
            _workbench_agent_playbook_task(
                task_id=task_id,
                rank=len(tasks) + 1,
                module=str(lane.get("module") or "agent"),
                label=label,
                status=_workbench_agent_playbook_status(lane.get("status")),
                task_kind=task_kind,
                action_kind=str(lane.get("action_kind") or "backend_command"),
                command=lane.get("command"),
                target_page=lane.get("target_page") or lane.get("module"),
                safety=lane.get("safety") or "local_backend_preview",
                source=source,
                source_id=lane_id,
                evidence=lane.get("evidence") or label,
                next_action=lane.get("next_action")
                or "Review this supervised execution lane.",
                local_write_allowed=bool(lane.get("local_write_allowed")),
                requires_arm_before_run=bool(lane.get("requires_arm_before_run")),
                db_writes_required=lane.get("db_writes_required"),
            )
        )

    add_priority_task(
        "priority-stage-decision-review",
        task_kind="readiness_gate",
        source="trading_workbench.priority_queue.decision-review",
    )
    add_priority_task(
        "priority-stage-trade-planning",
        task_kind="planning_gate",
        source="trading_workbench.priority_queue.trade-planning",
    )
    add_priority_task(
        "priority-stage-risk-approval",
        task_kind="risk_gate",
        source="trading_workbench.priority_queue.risk-approval",
    )
    add_priority_task(
        "priority-action-agent-preview",
        task_kind="safe_preview",
        source="trading_workbench.action_bus.agent-preview",
    )
    add_priority_task(
        "priority-action-paper-decision-preview",
        task_kind="safe_preview",
        source="trading_workbench.action_bus.paper-decision-preview",
    )
    add_priority_task(
        "priority-action-order-ticket-preview",
        task_kind="safe_preview",
        source="trading_workbench.action_bus.order-ticket-preview",
    )
    add_lane_task(
        "paper-record",
        task_id="execution-lane-paper-record",
        label="Guarded paper record",
        task_kind="guarded_local_write",
        source="trading_workbench.execution_sandbox.paper-record",
    )
    add_lane_task(
        "ticket-record",
        task_id="execution-lane-ticket-record",
        label="Guarded ticket record",
        task_kind="guarded_local_write",
        source="trading_workbench.execution_sandbox.ticket-record",
    )

    monitoring_check = checks.get("monitoring-readiness")
    if monitoring_check:
        tasks.append(
            _workbench_agent_playbook_task(
                task_id="readiness-monitoring",
                rank=len(tasks) + 1,
                module=str(monitoring_check.get("module") or "portfolio"),
                label=str(monitoring_check.get("label") or "Monitoring readiness"),
                status=_workbench_agent_playbook_status(monitoring_check.get("status")),
                task_kind=str(monitoring_check.get("gate_kind") or "monitor"),
                action_kind="page",
                command="portfolio",
                target_page="portfolio",
                safety="zero_call_navigation",
                source="trading_workbench.trade_readiness_brief.monitoring-readiness",
                source_id="monitoring-readiness",
                evidence=monitoring_check.get("evidence")
                or monitoring_check.get("finding")
                or "monitoring readiness",
                next_action=monitoring_check.get("next_action")
                or "Review monitoring before changing position state.",
            )
        )
    add_lane_task(
        "agent-execute",
        task_id="execution-lane-agent-execute",
        label="Agent execute boundary",
        task_kind="agent_boundary",
        source="trading_workbench.execution_sandbox.agent-execute",
    )

    # Fall back to the action bus if the current fixture has no priority queue.
    if not tasks:
        for action in _rows(action_bus.get("actions"))[:8]:
            action_id = str(action.get("id") or "").strip()
            if not action_id:
                continue
            action_kind = str(action.get("action_kind") or "backend_command")
            task_kind = (
                "agent_boundary"
                if action_kind == "boundary"
                else "guarded_local_write"
                if action.get("local_write_allowed")
                else "safe_preview"
                if action_kind == "backend_command"
                else "review"
            )
            tasks.append(
                _workbench_agent_playbook_task(
                    task_id=f"action-{action_id}",
                    rank=len(tasks) + 1,
                    module=str(action.get("module") or "agent"),
                    label=str(action.get("label") or action_id),
                    status=_workbench_agent_playbook_status(action.get("status")),
                    task_kind=task_kind,
                    action_kind=action_kind,
                    command=action.get("command"),
                    target_page=action.get("target_page") or action.get("module"),
                    safety=action.get("safety") or "local_backend_preview",
                    source=f"trading_workbench.action_bus.{action_id}",
                    source_id=action_id,
                    evidence=action.get("source") or action_id,
                    next_action=action.get("next_action")
                    or "Review this supervised action.",
                    local_write_allowed=bool(action.get("local_write_allowed")),
                    db_writes_required=action.get("db_writes_required"),
                )
            )

    blocked_count = sum(1 for row in tasks if row.get("status") == "blocked")
    approval_required_count = sum(
        1 for row in tasks if row.get("status") == "approval_required"
    )
    disabled_count = sum(1 for row in tasks if row.get("status") == "disabled")
    ready_count = sum(1 for row in tasks if row.get("status") == "ready")
    primary_task = next(
        (
            row
            for row in tasks
            if row.get("status") in {"blocked", "approval_required"}
        ),
        tasks[0] if tasks else {},
    )
    handoff = _mapping(trade_readiness_brief.get("agent_handoff"))
    ticker = _first_value(
        trade_readiness_brief.get("ticker"),
        operator_state.get("ticker"),
        execution_sandbox.get("ticker"),
    )
    decision_card_id = _first_value(
        trade_readiness_brief.get("decision_card_id"),
        operator_state.get("decision_card_id"),
        execution_sandbox.get("decision_card_id"),
    )
    status = (
        "blocked"
        if blocked_count
        else "approval_required"
        if approval_required_count
        else "ready"
        if ready_count
        else "empty"
    )
    return {
        "schema_version": "trading-workbench-agent-playbook-v1",
        "status": status,
        "source_tool": trade_readiness_brief.get("source_tool") or "market-radar",
        "ticker": ticker,
        "decision_card_id": decision_card_id,
        "playbook_id": (
            f"agent-playbook-{str(ticker or 'unknown').lower()}-"
            f"{decision_card_id or 'no-card'}"
        ),
        "operating_mode": "supervised_agent_playbook",
        "primary_task_id": primary_task.get("id"),
        "primary_blocker": (
            trade_readiness_brief.get("primary_blocker")
            or primary_task.get("evidence")
            or primary_task.get("label")
        ),
        "primary_next_action": primary_task.get("next_action")
        or trade_readiness_brief.get("primary_next_action")
        or "Review supervised agent playbook tasks.",
        "primary_action": {
            "task_id": primary_task.get("id"),
            "module": primary_task.get("module"),
            "command": primary_task.get("command"),
            "target_page": primary_task.get("target_page"),
            "safety": primary_task.get("safety"),
            "requires_arm_before_run": bool(
                primary_task.get("requires_arm_before_run")
            ),
            "can_execute_without_approval": bool(
                primary_task.get("can_execute_without_approval")
            ),
        },
        "agent_handoff": {
            "next_page": primary_task.get("target_page")
            or handoff.get("next_page")
            or "agent",
            "next_command": primary_task.get("command")
            or handoff.get("next_command")
            or "agent",
            "safety": primary_task.get("safety")
            or handoff.get("safety")
            or "zero_call_navigation",
            "can_execute_without_approval": bool(
                primary_task.get("can_execute_without_approval")
            ),
            "local_write_requires_arm": bool(
                primary_task.get("requires_arm_before_run")
            )
            or bool(handoff.get("local_write_requires_arm")),
        },
        "permissions": {
            "provider_calls_for_browsing": 0,
            "external_calls_allowed": False,
            "external_calls_made": 0,
            "local_write_requires_arm": True,
            "autonomous_execution": "disabled",
            "broker_order_submission": "disabled",
            "order_submission_allowed": False,
            "live_trading_enabled": False,
            "strategy_update_allowed": bool(
                trade_readiness_brief.get("strategy_update_allowed")
            ),
        },
        "tasks": tasks,
        "metrics": {
            "task_count": len(tasks),
            "ready_task_count": ready_count,
            "blocked_task_count": blocked_count,
            "approval_required_count": approval_required_count,
            "disabled_task_count": disabled_count,
            "safe_preview_task_count": sum(
                1 for row in tasks if row.get("task_kind") == "safe_preview"
            ),
            "guarded_write_task_count": sum(
                1 for row in tasks if row.get("task_kind") == "guarded_local_write"
            ),
            "zero_call_navigation_count": sum(
                1 for row in tasks if row.get("safety") == "zero_call_navigation"
            ),
            "external_boundary_count": sum(
                1
                for row in tasks
                if row.get("safety")
                in {"external_boundary", "agent_execution_boundary"}
            ),
            "external_calls_made": 0,
            "db_writes_made": 0,
        },
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "live_trading_enabled": False,
    }


def _workbench_agent_playbook_status(value: object) -> str:
    status = str(value or "unknown").strip().lower()
    if status == "enabled":
        return "ready"
    return status or "unknown"


def _workbench_agent_playbook_task(
    *,
    task_id: str,
    rank: int,
    module: str,
    label: str,
    status: str,
    task_kind: str,
    action_kind: str,
    command: object,
    target_page: object,
    safety: object,
    source: str,
    source_id: str,
    evidence: object,
    next_action: object,
    local_write_allowed: bool = False,
    requires_arm_before_run: bool = False,
    db_writes_required: object = 0,
) -> dict[str, object]:
    safe_preview = str(safety or "") == "local_backend_preview"
    zero_call_navigation = str(safety or "") == "zero_call_navigation"
    can_execute = (
        status == "ready"
        and not local_write_allowed
        and action_kind != "boundary"
        and (safe_preview or zero_call_navigation)
    )
    return {
        "id": task_id,
        "rank": rank,
        "module": module,
        "label": label,
        "status": status,
        "task_kind": task_kind,
        "action_kind": action_kind,
        "command": str(command or "").strip() or None,
        "target_page": str(target_page or module).strip() or module,
        "safety": str(safety or "zero_call_navigation"),
        "source": source,
        "source_id": source_id,
        "evidence": evidence,
        "next_action": next_action,
        "local_write_allowed": local_write_allowed,
        "requires_arm_before_run": requires_arm_before_run or local_write_allowed,
        "can_execute_without_approval": can_execute,
        "external_calls_allowed": False,
        "external_calls_made": 0,
        "db_writes_required": _first_nonnegative_int(db_writes_required),
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "live_trading_enabled": False,
    }


def _workbench_case_file_payload(
    *,
    active_plan: Mapping[str, object],
    decision_brief: Mapping[str, object],
    scenario_matrix: Mapping[str, object],
    risk_envelope: Mapping[str, object],
    capital_allocation: Mapping[str, object],
    paper_trade_preview: Mapping[str, object],
    pretrade_compliance: Mapping[str, object],
    learning_loop: Mapping[str, object],
    strategy_review: Mapping[str, object],
    trade_monitor: Mapping[str, object],
    workflow_map: Mapping[str, object],
    priority_queue: Mapping[str, object],
    supervision_gates: Mapping[str, object],
    operator_state: Mapping[str, object],
    execution_sandbox: Mapping[str, object],
    trade_readiness_brief: Mapping[str, object],
    agent_playbook: Mapping[str, object],
    market_intelligence_dossier: Mapping[str, object],
) -> dict[str, object]:
    ticker = str(
        _first_value(
            active_plan.get("ticker"),
            market_intelligence_dossier.get("ticker"),
            trade_readiness_brief.get("ticker"),
            agent_playbook.get("ticker"),
        )
        or ""
    ).strip().upper()
    decision_card_id = _first_value(
        active_plan.get("decision_card_id"),
        decision_brief.get("decision_card_id"),
        market_intelligence_dossier.get("decision_card_id"),
    )
    headline = _first_value(
        market_intelligence_dossier.get("headline"),
        decision_brief.get("headline"),
        _mapping(market_intelligence_dossier.get("primary_signal")).get("subject"),
        ticker,
    )
    setup = _mapping(decision_brief.get("setup"))
    signal = _mapping(market_intelligence_dossier.get("primary_signal"))
    readiness_metrics = _mapping(trade_readiness_brief.get("metrics"))
    scenario_metrics = _mapping(scenario_matrix.get("metrics"))
    risk_metrics = _mapping(risk_envelope.get("metrics"))
    allocation_metrics = _mapping(capital_allocation.get("metrics"))
    paper_metrics = _mapping(paper_trade_preview.get("metrics"))
    pretrade_metrics = _mapping(pretrade_compliance.get("metrics"))
    learning_metrics = _mapping(learning_loop.get("metrics"))
    monitor_metrics = _mapping(trade_monitor.get("metrics"))
    playbook_metrics = _mapping(agent_playbook.get("metrics"))
    supervision_metrics = _mapping(supervision_gates.get("metrics"))
    handoff = _mapping(agent_playbook.get("agent_handoff"))
    workflow_stage = _first_value(
        workflow_map.get("active_stage_id"),
        operator_state.get("active_step_id"),
        handoff.get("next_page"),
    )
    primary_blocker = _first_value(
        trade_readiness_brief.get("primary_blocker"),
        operator_state.get("primary_blocker"),
        pretrade_compliance.get("primary_blocker"),
        active_plan.get("next_action"),
    )
    paper_record_allowed = bool(trade_readiness_brief.get("paper_record_allowed"))
    broker_handoff_allowed = bool(trade_readiness_brief.get("broker_handoff_allowed"))
    strategy_update_allowed = bool(
        trade_readiness_brief.get("strategy_update_allowed")
        or strategy_review.get("strategy_update_allowed")
    )
    monitoring_ready = bool(trade_readiness_brief.get("monitoring_ready"))
    tools = [
        _workbench_case_file_tool(
            tool_id="market-radar-scout",
            rank=1,
            module="market-radar",
            label="MarketRadar scout",
            status="ready" if ticker else "blocked",
            tool_kind="scout",
            target_page="market-radar",
            finding=signal.get("subject") or headline,
            evidence=(
                f"signal={signal.get('state')}; "
                f"score={signal.get('score')}; "
                f"dossier={market_intelligence_dossier.get('primary_card_id')}"
            ),
            next_action="Open the MarketRadar scout before changing the case.",
        ),
        _workbench_case_file_tool(
            tool_id="decision-review",
            rank=2,
            module="review",
            label="Decision review",
            status=str(decision_brief.get("status") or "unknown"),
            tool_kind="decision",
            target_page="review",
            finding=decision_brief.get("recommended_paper_decision")
            or decision_brief.get("status"),
            evidence=f"card={decision_card_id}; stage={workflow_stage}",
            command="review",
            next_action=market_intelligence_dossier.get("primary_next_action")
            or "Resolve decision readiness before planning execution.",
        ),
        _workbench_case_file_tool(
            tool_id="trade-planner",
            rank=3,
            module="trade-planner",
            label="Trade planner",
            status=str(scenario_matrix.get("status") or "unknown"),
            tool_kind="planning",
            target_page="trade-planner",
            finding="scenario_matrix",
            evidence=(
                f"scenarios={scenario_metrics.get('scenario_count')}; "
                f"reward_risk={scenario_metrics.get('risk_reward')}; "
                f"max_loss={scenario_metrics.get('estimated_max_loss')}"
            ),
            next_action="Review scenario matrix before sizing or allocation.",
        ),
        _workbench_case_file_tool(
            tool_id="risk-envelope",
            rank=4,
            module="risk-desk",
            label="Risk envelope",
            status=str(risk_envelope.get("status") or "unknown"),
            tool_kind="risk",
            target_page="risk-desk",
            finding=primary_blocker or risk_envelope.get("status"),
            evidence=(
                "blocked_checks="
                f"{_first_nonnegative_int(risk_metrics.get('blocked_check_count'))}; "
                f"max_loss={risk_metrics.get('estimated_max_loss')}"
            ),
            next_action=risk_envelope.get("primary_next_action")
            or "Review risk envelope before any paper or broker handoff.",
        ),
        _workbench_case_file_tool(
            tool_id="capital-allocation",
            rank=5,
            module="portfolio",
            label="Capital allocation",
            status=str(capital_allocation.get("status") or "unknown"),
            tool_kind="capital",
            target_page="portfolio",
            finding="allocation_blocked"
            if not capital_allocation.get("allocation_allowed")
            else "allocation_ready",
            evidence=(
                f"suggested_notional={allocation_metrics.get('suggested_notional')}; "
                "buying_power_usage_pct="
                f"{allocation_metrics.get('buying_power_usage_pct')}"
            ),
            next_action="Review portfolio impact and allocation before sizing.",
        ),
        _workbench_case_file_tool(
            tool_id="paper-broker-boundary",
            rank=6,
            module="paper-trading",
            label="Paper and broker boundary",
            status=str(pretrade_compliance.get("status") or "unknown"),
            tool_kind="execution-boundary",
            target_page="paper-trading",
            finding=pretrade_compliance.get("primary_blocker")
            or "paper_record_blocked",
            evidence=(
                f"paper_allowed={str(paper_record_allowed).lower()}; "
                f"broker_handoff={str(broker_handoff_allowed).lower()}; "
                f"paper_blocks={paper_metrics.get('paper_block_count')}"
            ),
            command=_mapping(paper_trade_preview.get("commands")).get("preview"),
            next_action="Keep paper and broker actions behind explicit review.",
        ),
        _workbench_case_file_tool(
            tool_id="learning-validation",
            rank=7,
            module="validation",
            label="Learning and validation",
            status=str(learning_loop.get("status") or "unknown"),
            tool_kind="learning",
            target_page="validation",
            finding=learning_loop.get("learning_stage"),
            evidence=(
                f"validation_results={learning_metrics.get('validation_result_count')}; "
                f"outcomes={learning_metrics.get('outcome_count')}; "
                f"strategy_update_allowed={str(strategy_update_allowed).lower()}"
            ),
            next_action="Use validation as evidence; strategy updates remain supervised.",
        ),
        _workbench_case_file_tool(
            tool_id="trade-monitor",
            rank=8,
            module="alerts",
            label="Trade monitor",
            status=str(trade_monitor.get("status") or "unknown"),
            tool_kind="monitoring",
            target_page="alerts",
            finding=trade_monitor.get("monitor_stage"),
            evidence=(
                f"active_paper_trades={monitor_metrics.get('active_paper_trade_count')}; "
                f"alerts={monitor_metrics.get('alert_count')}; "
                f"triggers={monitor_metrics.get('trigger_count')}"
            ),
            next_action="Review monitoring context before changing watch state.",
        ),
        _workbench_case_file_tool(
            tool_id="agent-handoff",
            rank=9,
            module="agent",
            label="Agent handoff",
            status=str(agent_playbook.get("status") or "unknown"),
            tool_kind="agent",
            target_page=handoff.get("next_page") or "agent",
            finding=agent_playbook.get("primary_task_id"),
            evidence=(
                f"tasks={playbook_metrics.get('task_count')}; "
                f"blocked={playbook_metrics.get('blocked_task_count')}; "
                f"safe_previews={playbook_metrics.get('safe_preview_task_count')}"
            ),
            command=handoff.get("next_command") or "agent",
            safety=handoff.get("safety") or "zero_call_navigation",
            next_action=agent_playbook.get("primary_next_action")
            or "Use the supervised playbook as the next action list.",
        ),
        _workbench_case_file_tool(
            tool_id="live-execution-boundary",
            rank=10,
            module="broker",
            label="Live execution boundary",
            status="disabled",
            tool_kind="boundary",
            target_page="broker",
            finding="live_trading_disabled",
            evidence="broker_submission=disabled; autonomous_execution=disabled",
            next_action="Do not submit live orders from this workbench.",
        ),
    ]
    ready_count = sum(1 for row in tools if row.get("status") == "ready")
    blocked_count = sum(1 for row in tools if row.get("status") == "blocked")
    disabled_count = sum(1 for row in tools if row.get("status") == "disabled")
    review_count = sum(1 for row in tools if row.get("status") == "review")
    primary_tool = next(
        (row for row in tools if row.get("status") == "blocked"),
        tools[0] if tools else {},
    )
    approval_required_count = max(
        _first_nonnegative_int(supervision_metrics.get("approval_required_count")),
        _first_nonnegative_int(playbook_metrics.get("approval_required_count")),
        _first_nonnegative_int(pretrade_metrics.get("approval_required_count")),
    )
    status = "blocked" if blocked_count else "review" if review_count else "ready"
    return {
        "schema_version": "trading-workbench-case-file-v1",
        "status": status,
        "case_id": (
            f"case-{str(ticker or 'unknown').lower()}-"
            f"{decision_card_id or 'no-card'}"
        ),
        "source_tool": "market-radar",
        "operating_mode": "supervised_trade_case",
        "ticker": ticker or None,
        "decision_card_id": decision_card_id,
        "headline": headline,
        "active_stage_id": workflow_stage,
        "active_module": operator_state.get("active_module"),
        "primary_tool_id": primary_tool.get("id"),
        "primary_blocker": primary_blocker,
        "primary_next_action": primary_tool.get("next_action"),
        "identity": {
            "ticker": ticker or None,
            "decision_card_id": decision_card_id,
            "headline": headline,
            "setup": setup.get("setup_type"),
            "direction": setup.get("direction"),
            "score": signal.get("score"),
            "signal_state": signal.get("state"),
            "recommended_paper_decision": active_plan.get(
                "recommended_paper_decision"
            ),
        },
        "handoff": {
            "next_page": handoff.get("next_page"),
            "next_command": handoff.get("next_command"),
            "safety": handoff.get("safety"),
            "primary_task_id": agent_playbook.get("primary_task_id"),
            "can_execute_without_approval": bool(
                handoff.get("can_execute_without_approval")
            ),
        },
        "permissions": {
            "paper_record_allowed": paper_record_allowed,
            "broker_handoff_allowed": broker_handoff_allowed,
            "strategy_update_allowed": strategy_update_allowed,
            "monitoring_ready": monitoring_ready,
            "exit_update_allowed": bool(trade_monitor.get("exit_update_allowed")),
            "order_submission_allowed": False,
            "live_trading_enabled": False,
            "autonomous_execution_enabled": False,
        },
        "workflow": {
            "status": workflow_map.get("status"),
            "active_stage_id": workflow_stage,
            "priority_item_id": priority_queue.get("primary_item_id"),
            "supervision_gate_id": supervision_gates.get("primary_gate_id"),
            "execution_lane_id": execution_sandbox.get("active_lane_id"),
        },
        "tools": tools,
        "source_keys": [
            "trading_workbench.market_intelligence_dossier",
            "trading_workbench.decision_brief",
            "trading_workbench.scenario_matrix",
            "trading_workbench.risk_envelope",
            "trading_workbench.capital_allocation",
            "trading_workbench.paper_trade_preview",
            "trading_workbench.pretrade_compliance",
            "trading_workbench.learning_loop",
            "trading_workbench.strategy_review",
            "trading_workbench.trade_monitor",
            "trading_workbench.agent_playbook",
        ],
        "metrics": {
            "linked_tool_count": len(tools),
            "ready_tool_count": ready_count,
            "blocked_tool_count": blocked_count,
            "review_tool_count": review_count,
            "disabled_tool_count": disabled_count,
            "approval_required_count": approval_required_count,
            "safe_preview_task_count": _first_nonnegative_int(
                playbook_metrics.get("safe_preview_task_count")
            ),
            "guarded_write_task_count": _first_nonnegative_int(
                playbook_metrics.get("guarded_write_task_count")
            ),
            "blocked_check_count": _first_nonnegative_int(
                readiness_metrics.get("blocked_check_count")
            ),
            "external_calls_made": 0,
            "db_writes_made": 0,
        },
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "live_trading_enabled": False,
    }


def _workbench_case_file_tool(
    *,
    tool_id: str,
    rank: int,
    module: str,
    label: str,
    status: str,
    tool_kind: str,
    target_page: object,
    finding: object,
    evidence: object,
    next_action: object,
    command: object = None,
    safety: object = "zero_call_navigation",
) -> dict[str, object]:
    return {
        "id": tool_id,
        "rank": rank,
        "module": module,
        "label": label,
        "status": status,
        "tool_kind": tool_kind,
        "target_page": target_page,
        "finding": finding,
        "evidence": evidence,
        "command": command,
        "safety": safety,
        "next_action": next_action,
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "live_trading_enabled": False,
    }


def _workbench_market_intelligence_dossier_payload(
    *,
    focus_row: Mapping[str, object],
    candidate_rows: Sequence[Mapping[str, object]],
    alert_rows: Sequence[Mapping[str, object]],
    trigger_rows: Sequence[Mapping[str, object]],
    opportunity_action_rows: Sequence[Mapping[str, object]],
    ipo_rows: Sequence[Mapping[str, object]],
    theme_rows: Sequence[Mapping[str, object]],
    decision_brief: Mapping[str, object],
    risk_envelope: Mapping[str, object],
    trade_readiness_brief: Mapping[str, object],
    agent_playbook: Mapping[str, object],
) -> dict[str, object]:
    ticker = str(
        _first_value(
            focus_row.get("ticker"),
            decision_brief.get("ticker"),
            risk_envelope.get("ticker"),
            agent_playbook.get("ticker"),
        )
        or ""
    ).strip().upper()
    decision_card_id = _first_value(
        focus_row.get("decision_card_id"),
        decision_brief.get("decision_card_id"),
        agent_playbook.get("decision_card_id"),
    )
    scout_subject = _first_value(
        focus_row.get("subject"),
        decision_brief.get("headline"),
        ticker,
    )
    matching_alerts = _workbench_dossier_rows_for_ticker(alert_rows, ticker)
    matching_triggers = _workbench_dossier_rows_for_ticker(trigger_rows, ticker)
    matching_actions = _workbench_dossier_rows_for_ticker(
        opportunity_action_rows,
        ticker,
    )
    matching_themes = [
        row
        for row in theme_rows
        if ticker and ticker in {text.upper() for text in _texts(row.get("top_tickers"))}
    ]
    primary_alert = _mapping(matching_alerts[0]) if matching_alerts else {}
    primary_trigger = _mapping(matching_triggers[0]) if matching_triggers else {}
    primary_theme = _mapping(matching_themes[0]) if matching_themes else {}
    primary_ipo = _mapping(ipo_rows[0]) if ipo_rows else {}
    decision_metrics = _mapping(decision_brief.get("metrics"))
    risk_metrics = _mapping(risk_envelope.get("metrics"))
    risk_blockers = _texts(risk_envelope.get("blockers"))
    playbook_metrics = _mapping(agent_playbook.get("metrics"))
    playbook_handoff = _mapping(agent_playbook.get("agent_handoff"))
    readiness_metrics = _mapping(trade_readiness_brief.get("metrics"))
    cards = [
        _workbench_market_intelligence_card(
            card_id="market-radar-scout",
            rank=1,
            module="market-radar",
            label="MarketRadar scout",
            status="ready" if ticker else "blocked",
            context_kind="scout",
            finding=scout_subject or "missing_scout",
            evidence=(
                f"score={focus_row.get('score')}; "
                f"state={focus_row.get('state')}; "
                f"usefulness={focus_row.get('usefulness_status')}"
            ),
            source="trading_workbench.modules.market-radar.rows",
            target_page="market-radar",
            next_action=focus_row.get("next_action")
            or "Open the MarketRadar scout row before planning.",
        ),
        _workbench_market_intelligence_card(
            card_id="candidate-evidence",
            rank=2,
            module="candidates",
            label="Candidate evidence",
            status="ready" if candidate_rows else "blocked",
            context_kind="candidate",
            finding=decision_card_id or "missing_decision_card",
            evidence=(
                f"candidate_rows={len(candidate_rows)}; "
                f"decision_card={decision_card_id or 'none'}"
            ),
            source="trading_workbench.modules.candidates.rows",
            target_page="candidates",
            next_action=(
                "Review the candidate evidence packet before planning."
                if candidate_rows
                else "Create candidate evidence before planning."
            ),
        ),
        _workbench_market_intelligence_card(
            card_id="decision-brief",
            rank=3,
            module="review",
            label="Decision brief",
            status=str(decision_brief.get("status") or "unknown"),
            context_kind="decision",
            finding=decision_brief.get("recommended_paper_decision")
            or decision_brief.get("status"),
            evidence=(
                "paper_blocks="
                f"{_first_nonnegative_int(decision_metrics.get('paper_block_count'))}; "
                "live_blocks="
                f"{_first_nonnegative_int(decision_metrics.get('live_block_count'))}"
            ),
            source="trading_workbench.decision_brief",
            target_page="review",
            command=_mapping(decision_brief.get("next_action")).get("command"),
            next_action="Resolve decision-readiness context before using the thesis.",
        ),
        _workbench_market_intelligence_card(
            card_id="risk-envelope",
            rank=4,
            module="risk-desk",
            label="Risk envelope",
            status=str(risk_envelope.get("status") or "unknown"),
            context_kind="risk",
            finding=risk_blockers[0] if risk_blockers else "risk_context_clear",
            evidence=(
                "blocked_checks="
                f"{_first_nonnegative_int(risk_metrics.get('blocked_check_count'))}; "
                f"max_loss={risk_metrics.get('estimated_max_loss')}"
            ),
            source="trading_workbench.risk_envelope",
            target_page="risk-desk",
            next_action=risk_envelope.get("primary_next_action")
            or "Review risk envelope before any paper or broker handoff.",
        ),
        _workbench_market_intelligence_card(
            card_id="alert-context",
            rank=5,
            module="alerts",
            label="Alert and trigger context",
            status="ready"
            if matching_alerts or matching_triggers
            else "review"
            if alert_rows or trigger_rows
            else "blocked",
            context_kind="alert",
            finding=(
                primary_trigger.get("id")
                or primary_alert.get("id")
                or "no_ticker_alert_context"
            ),
            evidence=(
                f"alerts={len(matching_alerts)}; "
                f"triggers={len(matching_triggers)}; "
                f"operator_actions={len(matching_actions)}"
            ),
            source="trading_workbench.modules.alerts",
            target_page="alerts",
            next_action=(
                "Review alert and trigger evidence before changing watch state."
                if matching_alerts or matching_triggers
                else "Create or inspect local alert context if the thesis needs monitoring."
            ),
        ),
        _workbench_market_intelligence_card(
            card_id="theme-context",
            rank=6,
            module="themes",
            label="Theme context",
            status="ready" if matching_themes else "review" if theme_rows else "blocked",
            context_kind="theme",
            finding=primary_theme.get("theme") or "no_related_theme",
            evidence=(
                f"themes={len(theme_rows)}; "
                f"related={len(matching_themes)}; "
                "top_tickers="
                f"{','.join(_texts(primary_theme.get('top_tickers'))[:4]) or 'none'}"
            ),
            source="trading_workbench.modules.themes",
            target_page="themes",
            next_action=(
                "Use related theme context as thesis background, not approval."
                if matching_themes
                else "Review theme rows before using macro context in the thesis."
            ),
        ),
        _workbench_market_intelligence_card(
            card_id="ipo-watchlist",
            rank=7,
            module="ipo",
            label="IPO/S-1 watchlist",
            status="review" if ipo_rows else "ready",
            context_kind="market-calendar",
            finding=(
                f"{primary_ipo.get('ticker')} {primary_ipo.get('form_type')}"
                if primary_ipo
                else "no_current_ipo_watchlist"
            ),
            evidence=(
                f"ipo_s1_rows={len(ipo_rows)}; "
                f"risk_flags={len(_texts(primary_ipo.get('risk_flags')))}"
            ),
            source="trading_workbench.modules.ipo",
            target_page="ipo",
            next_action=(
                "Treat IPO/S-1 rows as market context; no trade is approved."
                if ipo_rows
                else "No IPO/S-1 watchlist context is present."
            ),
        ),
        _workbench_market_intelligence_card(
            card_id="agent-playbook",
            rank=8,
            module="agent",
            label="Agent playbook",
            status=str(agent_playbook.get("status") or "unknown"),
            context_kind="agent",
            finding=agent_playbook.get("primary_task_id") or "no_agent_task",
            evidence=(
                f"tasks={_first_nonnegative_int(playbook_metrics.get('task_count'))}; "
                "blocked="
                f"{_first_nonnegative_int(playbook_metrics.get('blocked_task_count'))}; "
                "safe_previews="
                f"{_first_nonnegative_int(playbook_metrics.get('safe_preview_task_count'))}"
            ),
            source="trading_workbench.agent_playbook",
            target_page=playbook_handoff.get("next_page") or "agent",
            command=playbook_handoff.get("next_command") or "agent",
            safety=playbook_handoff.get("safety") or "zero_call_navigation",
            next_action=agent_playbook.get("primary_next_action")
            or "Use the supervised playbook as the next action list.",
        ),
    ]
    ready_count = sum(1 for row in cards if row.get("status") == "ready")
    blocked_count = sum(1 for row in cards if row.get("status") == "blocked")
    review_count = sum(1 for row in cards if row.get("status") == "review")
    disabled_count = sum(1 for row in cards if row.get("status") == "disabled")
    primary_card = next(
        (row for row in cards if row.get("status") == "blocked"),
        cards[0] if cards else {},
    )
    status = "blocked" if blocked_count else "review" if review_count else "ready"
    return {
        "schema_version": "trading-workbench-market-intelligence-dossier-v1",
        "status": status,
        "source_tool": "market-radar",
        "ticker": ticker or None,
        "decision_card_id": decision_card_id,
        "dossier_id": (
            f"market-intelligence-{str(ticker or 'unknown').lower()}-"
            f"{decision_card_id or 'no-card'}"
        ),
        "operating_mode": "supervised_market_context",
        "headline": decision_brief.get("headline") or scout_subject,
        "primary_card_id": primary_card.get("id"),
        "primary_next_action": primary_card.get("next_action")
        or agent_playbook.get("primary_next_action")
        or "Review market intelligence before following the agent playbook.",
        "primary_signal": {
            "ticker": ticker or None,
            "subject": scout_subject,
            "score": focus_row.get("score"),
            "setup": focus_row.get("setup"),
            "state": focus_row.get("state"),
            "usefulness_status": focus_row.get("usefulness_status"),
            "decision_ready": bool(focus_row.get("decision_ready")),
            "decision_card_id": decision_card_id,
            "recommended_paper_decision": decision_brief.get(
                "recommended_paper_decision"
            ),
        },
        "market_context": {
            "alert_count": len(matching_alerts),
            "trigger_count": len(matching_triggers),
            "opportunity_action_count": len(matching_actions),
            "theme_count": len(theme_rows),
            "related_theme_count": len(matching_themes),
            "ipo_s1_count": len(ipo_rows),
            "primary_theme": primary_theme.get("theme"),
            "primary_alert_id": primary_alert.get("id"),
            "primary_trigger_id": primary_trigger.get("id"),
        },
        "agent_context": {
            "primary_task_id": agent_playbook.get("primary_task_id"),
            "next_page": playbook_handoff.get("next_page"),
            "next_command": playbook_handoff.get("next_command"),
            "safety": playbook_handoff.get("safety"),
            "blocked_task_count": _first_nonnegative_int(
                playbook_metrics.get("blocked_task_count")
            ),
            "safe_preview_task_count": _first_nonnegative_int(
                playbook_metrics.get("safe_preview_task_count")
            ),
        },
        "readiness_context": {
            "status": trade_readiness_brief.get("status"),
            "blocked_check_count": _first_nonnegative_int(
                readiness_metrics.get("blocked_check_count")
            ),
            "disabled_check_count": _first_nonnegative_int(
                readiness_metrics.get("disabled_check_count")
            ),
            "paper_record_allowed": bool(
                trade_readiness_brief.get("paper_record_allowed")
            ),
            "broker_handoff_allowed": bool(
                trade_readiness_brief.get("broker_handoff_allowed")
            ),
        },
        "cards": cards,
        "source_keys": [
            "trading_workbench.modules.market-radar.rows",
            "trading_workbench.modules.candidates.rows",
            "trading_workbench.modules.alerts",
            "trading_workbench.modules.themes",
            "trading_workbench.modules.ipo",
            "trading_workbench.decision_brief",
            "trading_workbench.risk_envelope",
            "trading_workbench.agent_playbook",
        ],
        "metrics": {
            "card_count": len(cards),
            "ready_card_count": ready_count,
            "blocked_card_count": blocked_count,
            "review_card_count": review_count,
            "disabled_card_count": disabled_count,
            "alert_context_count": len(matching_alerts) + len(matching_triggers),
            "theme_context_count": len(matching_themes),
            "ipo_watchlist_count": len(ipo_rows),
            "external_calls_made": 0,
            "db_writes_made": 0,
        },
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "live_trading_enabled": False,
    }


def _workbench_dossier_rows_for_ticker(
    rows: Sequence[Mapping[str, object]],
    ticker: str,
) -> list[Mapping[str, object]]:
    if not ticker:
        return []
    return [
        row
        for row in rows
        if str(row.get("ticker") or "").strip().upper() == ticker
    ]


def _workbench_market_intelligence_card(
    *,
    card_id: str,
    rank: int,
    module: str,
    label: str,
    status: str,
    context_kind: str,
    finding: object,
    evidence: object,
    source: str,
    target_page: str,
    next_action: object,
    command: object = None,
    safety: object = "zero_call_navigation",
) -> dict[str, object]:
    return {
        "id": card_id,
        "rank": rank,
        "module": module,
        "label": label,
        "status": status,
        "context_kind": context_kind,
        "finding": finding,
        "evidence": evidence,
        "source": source,
        "target_page": target_page,
        "command": command,
        "safety": safety,
        "next_action": next_action,
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "live_trading_enabled": False,
    }


def _workbench_learning_loop_payload(
    *,
    active_plan: Mapping[str, object],
    paper_trade_preview: Mapping[str, object],
    trade_lifecycle_rows: Sequence[Mapping[str, object]],
    validation_result_rows: Sequence[Mapping[str, object]],
    journal_entry_rows: Sequence[Mapping[str, object]],
    journal_outcome_rows: Sequence[Mapping[str, object]],
    useful_label_rows: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    paper_decision = _mapping(paper_trade_preview.get("paper_decision"))
    decision_card_id = _first_value(
        active_plan.get("decision_card_id"),
        paper_trade_preview.get("decision_card_id"),
    )
    ticker = _learning_loop_ticker(
        _first_value(active_plan.get("ticker"), paper_trade_preview.get("ticker"))
    )
    lifecycle = _workbench_learning_loop_lifecycle(
        trade_lifecycle_rows,
        decision_card_id=decision_card_id,
        ticker=ticker,
    )
    ticker = _learning_loop_ticker(
        _first_value(ticker, lifecycle.get("ticker"), active_plan.get("ticker"))
    )
    decision_card_id = _first_value(
        decision_card_id,
        lifecycle.get("decision_card_id"),
    )
    validation_result = _workbench_learning_loop_match(
        validation_result_rows,
        expected_id=lifecycle.get("validation_result_id"),
        decision_card_id=decision_card_id,
        ticker=ticker,
    )
    journal_entry = _workbench_learning_loop_match(
        journal_entry_rows,
        expected_id=lifecycle.get("ledger_entry_id"),
        ticker=ticker,
    )
    journal_outcome = _workbench_learning_loop_match(
        journal_outcome_rows,
        expected_id=lifecycle.get("outcome_id"),
        related_id=lifecycle.get("ledger_entry_id"),
        related_key="value_ledger_entry_id",
        ticker=ticker,
    )
    lifecycle_stage = str(lifecycle.get("current_stage") or "").strip()
    validation_result_id = _first_value(
        lifecycle.get("validation_result_id"),
        validation_result.get("id"),
    )
    ledger_entry_id = _first_value(
        lifecycle.get("ledger_entry_id"),
        journal_entry.get("id"),
    )
    outcome_id = _first_value(lifecycle.get("outcome_id"), journal_outcome.get("id"))
    primary_blocker = (
        paper_trade_preview.get("primary_blocker")
        if str(paper_trade_preview.get("status") or "").lower() == "blocked"
        else None
    )
    cards = [
        _workbench_learning_loop_card(
            card_id="radar-signal",
            label="Radar signal",
            module="market-radar",
            status="ready" if decision_card_id else "blocked",
            finding=decision_card_id or "missing_decision_card",
            evidence=str(decision_card_id or "no decision card"),
            command="market-radar",
            target_page="market-radar",
            next_action=(
                "Use the active MarketRadar decision card as the loop source."
                if decision_card_id
                else "Review radar output before learning-loop analysis."
            ),
        ),
        _workbench_learning_loop_card(
            card_id="paper-preview",
            label="Paper preview",
            module="paper-trading",
            status=str(paper_trade_preview.get("status") or "blocked"),
            finding=paper_trade_preview.get("primary_blocker")
            or "paper_preview_available",
            evidence=str(paper_trade_preview.get("preview_id") or "paper preview"),
            command=_mapping(paper_trade_preview.get("commands")).get("preview")
            or "paper-decision preview",
            target_page="paper-trading",
            next_action=str(
                paper_trade_preview.get("primary_next_action")
                or "Review supervised paper preview before recording."
            ),
        ),
        _workbench_learning_loop_card(
            card_id="paper-evidence",
            label="Paper evidence",
            module="paper-trading",
            status="ready" if lifecycle.get("paper_trade_id") else "blocked",
            finding=lifecycle.get("paper_state") or "missing_paper_trade",
            evidence=str(lifecycle.get("paper_trade_id") or "no paper trade"),
            command="paper",
            target_page="paper-trading",
            next_action=(
                "Use the local paper trade as outcome context."
                if lifecycle.get("paper_trade_id")
                else "Record or link a local paper decision after approval."
            ),
        ),
        _workbench_learning_loop_card(
            card_id="validation-replay",
            label="Validation replay",
            module="validation",
            status="ready" if validation_result_id else "blocked",
            finding=validation_result.get("state") or "missing_validation_result",
            evidence=str(validation_result_id or "no validation result"),
            command="validation",
            target_page="validation",
            next_action=(
                "Review replay score and leakage flags before strategy changes."
                if validation_result_id
                else "Run or link validation evidence for this decision."
            ),
        ),
        _workbench_learning_loop_card(
            card_id="journal-entry",
            label="Journal entry",
            module="journal",
            status="ready" if ledger_entry_id else "blocked",
            finding=journal_entry.get("outcome_status") or "missing_value_ledger",
            evidence=str(ledger_entry_id or "no ledger entry"),
            command=lifecycle.get("ledger_show_command") or "ledger",
            target_page="journal",
            next_action=(
                "Review local value ledger evidence."
                if ledger_entry_id
                else "Record local value evidence before outcome review."
            ),
        ),
        _workbench_learning_loop_card(
            card_id="outcome-review",
            label="Outcome review",
            module="journal",
            status="ready" if outcome_id else "blocked",
            finding=journal_outcome.get("status") or "missing_outcome",
            evidence=str(outcome_id or "no outcome"),
            command=lifecycle.get("outcome_show_command")
            or lifecycle.get("outcome_preview_command")
            or "outcome",
            target_page="journal",
            next_action=(
                "Compare realized outcome with the original decision."
                if outcome_id
                else "Compute or attach outcome evidence before updating logic."
            ),
        ),
        _workbench_learning_loop_card(
            card_id="strategy-update-boundary",
            label="Strategy update boundary",
            module="agent",
            status="disabled",
            finding="autonomous_strategy_update_disabled",
            evidence="manual review required",
            command="agent",
            target_page="agent",
            next_action="Use learning evidence for review; do not auto-change strategy.",
        ),
    ]
    blocked_card_count = sum(1 for row in cards if row.get("status") == "blocked")
    disabled_card_count = sum(1 for row in cards if row.get("status") == "disabled")
    ready_card_count = sum(1 for row in cards if row.get("status") == "ready")
    if primary_blocker is None:
        primary_blocker = next(
            (
                str(row.get("finding"))
                for row in cards
                if row.get("status") == "blocked" and row.get("finding")
            ),
            None,
        )
    primary_next_action = next(
        (
            str(row.get("next_action"))
            for row in cards
            if row.get("status") == "blocked" and row.get("next_action")
        ),
        str(lifecycle.get("next_action") or "Review linked learning evidence."),
    )
    return {
        "schema_version": "trading-workbench-learning-loop-v1",
        "status": "blocked" if blocked_card_count else "ready",
        "source_tool": "market-radar",
        "ticker": ticker,
        "decision_card_id": decision_card_id,
        "loop_id": (
            f"learning-loop-{str(ticker or 'unknown').lower()}-"
            f"{decision_card_id or 'no-card'}"
        ),
        "learning_stage": lifecycle_stage or "unlinked",
        "primary_blocker": primary_blocker,
        "primary_next_action": primary_next_action,
        "primary_signal": {
            "plan_status": active_plan.get("status"),
            "recommended_paper_decision": active_plan.get(
                "recommended_paper_decision"
            ),
            "source_tool": "market-radar",
            "paper_preview_status": paper_trade_preview.get("status"),
            "paper_decision": paper_decision.get("decision"),
            "suggested_quantity": paper_decision.get("suggested_quantity"),
        },
        "paper_state": {
            "preview_status": paper_trade_preview.get("status"),
            "preview_id": paper_trade_preview.get("preview_id"),
            "paper_trade_id": lifecycle.get("paper_trade_id"),
            "paper_state": lifecycle.get("paper_state"),
            "record_allowed": False,
            "broker_order_submitted": False,
        },
        "validation_state": {
            "validation_result_id": validation_result_id,
            "run_id": validation_result.get("run_id"),
            "state": validation_result.get("state"),
            "final_score": validation_result.get("final_score"),
            "positive_labels": _texts(validation_result.get("positive_labels")),
            "leakage_flags": _texts(validation_result.get("leakage_flags")),
            "next_action": validation_result.get("next_action"),
        },
        "journal_state": {
            "ledger_entry_id": ledger_entry_id,
            "outcome_id": outcome_id,
            "outcome_status": _first_value(
                lifecycle.get("outcome_status"),
                journal_outcome.get("status"),
            ),
            "return_20d": _workbench_round_ratio(
                _optional_float(
                    _first_value(
                        lifecycle.get("return_20d"),
                        journal_outcome.get("return_20d"),
                    )
                )
            ),
            "spy_relative_return_20d": _workbench_round_ratio(
                _optional_float(
                    _first_value(
                        lifecycle.get("spy_relative_return_20d"),
                        journal_outcome.get("spy_relative_return_20d"),
                    )
                )
            ),
            "invalidation_touched": bool(journal_outcome.get("invalidation_touched")),
            "primary_command": lifecycle.get("primary_command"),
        },
        "cards": cards,
        "metrics": {
            "card_count": len(cards),
            "ready_card_count": ready_card_count,
            "blocked_card_count": blocked_card_count,
            "disabled_card_count": disabled_card_count,
            "trade_lifecycle_count": len(_rows(trade_lifecycle_rows)),
            "paper_trade_count": sum(
                1 for row in _rows(trade_lifecycle_rows) if row.get("paper_trade_id")
            ),
            "validation_result_count": len(_rows(validation_result_rows)),
            "journal_entry_count": len(_rows(journal_entry_rows)),
            "outcome_count": len(_rows(journal_outcome_rows)),
            "useful_label_count": len(_rows(useful_label_rows)),
            "linked_outcome_count": sum(
                1 for row in _rows(trade_lifecycle_rows) if row.get("outcome_id")
            ),
            "external_calls_made": 0,
            "db_writes_made": 0,
        },
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "live_trading_enabled": False,
    }


def _learning_loop_ticker(value: object) -> str | None:
    ticker = str(value or "").strip().upper()
    return ticker or None


def _workbench_learning_loop_lifecycle(
    rows: Sequence[Mapping[str, object]],
    *,
    decision_card_id: object,
    ticker: object,
) -> Mapping[str, object]:
    card_text = str(decision_card_id or "").strip()
    ticker_text = _learning_loop_ticker(ticker)
    for row in _rows(rows):
        if card_text and str(row.get("decision_card_id") or "").strip() == card_text:
            return row
    for row in _rows(rows):
        if ticker_text and _learning_loop_ticker(row.get("ticker")) == ticker_text:
            return row
    return _mapping(_rows(rows)[0]) if _rows(rows) else {}


def _workbench_learning_loop_match(
    rows: Sequence[Mapping[str, object]],
    *,
    expected_id: object = None,
    related_id: object = None,
    related_key: str | None = None,
    decision_card_id: object = None,
    ticker: object = None,
) -> Mapping[str, object]:
    expected_text = str(expected_id or "").strip()
    related_text = str(related_id or "").strip()
    card_text = str(decision_card_id or "").strip()
    ticker_text = _learning_loop_ticker(ticker)
    mapped_rows = _rows(rows)
    if expected_text:
        for row in mapped_rows:
            if str(row.get("id") or "").strip() == expected_text:
                return row
    if related_text and related_key:
        for row in mapped_rows:
            if str(row.get(related_key) or "").strip() == related_text:
                return row
    if card_text:
        for row in mapped_rows:
            if str(row.get("decision_card_id") or "").strip() == card_text:
                return row
    if ticker_text:
        for row in mapped_rows:
            if _learning_loop_ticker(row.get("ticker")) == ticker_text:
                return row
    return _mapping(mapped_rows[0]) if mapped_rows else {}


def _workbench_learning_loop_card(
    *,
    card_id: str,
    label: str,
    module: str,
    status: object,
    finding: object,
    evidence: str,
    command: object,
    target_page: str,
    next_action: str,
) -> dict[str, object]:
    return {
        "id": card_id,
        "label": label,
        "module": module,
        "status": str(status or "unknown"),
        "finding": finding,
        "evidence": evidence,
        "command": str(command or "").strip() or None,
        "target_page": target_page,
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "live_trading_enabled": False,
        "next_action": next_action,
    }


def _workbench_strategy_review_payload(
    *,
    active_plan: Mapping[str, object],
    scenario_matrix: Mapping[str, object],
    risk_envelope: Mapping[str, object],
    learning_loop: Mapping[str, object],
) -> dict[str, object]:
    signal = _mapping(learning_loop.get("primary_signal"))
    validation = _mapping(learning_loop.get("validation_state"))
    journal = _mapping(learning_loop.get("journal_state"))
    risk_sizing = _mapping(risk_envelope.get("sizing_context"))
    gate_context = _mapping(risk_envelope.get("gate_context"))
    assumptions = _mapping(scenario_matrix.get("assumptions"))
    risk_blockers = _texts(risk_envelope.get("blockers"))
    validation_result_id = validation.get("validation_result_id")
    outcome_id = journal.get("outcome_id")
    leakage_flags = _texts(validation.get("leakage_flags"))
    positive_labels = _texts(validation.get("positive_labels"))
    return_20d = _optional_float(journal.get("return_20d"))
    spy_relative = _optional_float(journal.get("spy_relative_return_20d"))
    paper_block_count = _first_nonnegative_int(gate_context.get("paper_block_count"))
    primary_blocker = risk_blockers[0] if risk_blockers else None
    hypotheses = [
        _workbench_strategy_review_hypothesis(
            hypothesis_id="thesis-validation",
            label="Thesis validation",
            driver="validation_replay",
            status="blocked"
            if leakage_flags
            else "ready"
            if validation_result_id
            else "review",
            evidence=(
                f"score={validation.get('final_score')}; "
                f"labels={len(positive_labels)}; leakage={len(leakage_flags)}"
            ),
            next_action=(
                "Investigate leakage flags before changing thesis rules."
                if leakage_flags
                else "Use replay evidence as one input to manual strategy review."
                if validation_result_id
                else "Attach validation replay evidence before strategy review."
            ),
        ),
        _workbench_strategy_review_hypothesis(
            hypothesis_id="outcome-attribution",
            label="Outcome attribution",
            driver="value_outcome",
            status="ready" if outcome_id else "blocked",
            evidence=(
                f"return_20d={_workbench_round_ratio(return_20d)}; "
                f"spy_relative={_workbench_round_ratio(spy_relative)}"
            )
            if outcome_id
            else "missing_outcome",
            next_action=(
                "Compare realized outcome with the original paper decision."
                if outcome_id
                else "Compute or link outcome evidence before adjusting strategy."
            ),
        ),
        _workbench_strategy_review_hypothesis(
            hypothesis_id="risk-calibration",
            label="Risk calibration",
            driver="risk_envelope",
            status="blocked" if paper_block_count else "review",
            evidence=(
                f"paper_blocks={paper_block_count}; "
                f"max_loss={risk_sizing.get('estimated_max_loss')}; "
                f"reward_risk={assumptions.get('reward_risk')}"
            ),
            next_action=(
                "Resolve paper risk blockers before promoting any rule change."
                if paper_block_count
                else "Review risk/reward assumptions before changing sizing rules."
            ),
        ),
        _workbench_strategy_review_hypothesis(
            hypothesis_id="strategy-update-boundary",
            label="Strategy update boundary",
            driver="agent_boundary",
            status="disabled",
            evidence="autonomous_strategy_update_disabled",
            next_action="Use this dossier for review; do not auto-change strategy.",
        ),
    ]
    blocked_count = sum(1 for row in hypotheses if row.get("status") == "blocked")
    ready_count = sum(1 for row in hypotheses if row.get("status") == "ready")
    review_count = sum(1 for row in hypotheses if row.get("status") == "review")
    disabled_count = sum(1 for row in hypotheses if row.get("status") == "disabled")
    return {
        "schema_version": "trading-workbench-strategy-review-v1",
        "status": "blocked"
        if blocked_count
        else "review"
        if review_count
        else "ready",
        "source_tool": learning_loop.get("source_tool") or "market-radar",
        "ticker": _first_value(learning_loop.get("ticker"), active_plan.get("ticker")),
        "decision_card_id": _first_value(
            learning_loop.get("decision_card_id"),
            active_plan.get("decision_card_id"),
        ),
        "review_id": (
            f"strategy-review-"
            f"{str(_first_value(learning_loop.get('ticker'), 'unknown')).lower()}-"
            f"{_first_value(learning_loop.get('decision_card_id'), 'no-card')}"
        ),
        "strategy_stage": learning_loop.get("learning_stage") or "unlinked",
        "primary_blocker": primary_blocker,
        "primary_next_action": (
            "Resolve risk and paper blockers before using this as a rule-change input."
            if primary_blocker
            else "Review strategy hypotheses manually before changing rules."
        ),
        "strategy_context": {
            "plan_status": active_plan.get("status"),
            "recommended_paper_decision": signal.get("recommended_paper_decision"),
            "paper_decision": signal.get("paper_decision"),
            "entry_price": assumptions.get("entry_price"),
            "invalidation_price": assumptions.get("invalidation_price"),
            "target_price": assumptions.get("target_price"),
            "reward_risk": assumptions.get("reward_risk"),
            "estimated_max_loss": risk_sizing.get("estimated_max_loss"),
        },
        "evidence": {
            "validation_result_id": validation_result_id,
            "validation_state": validation.get("state"),
            "final_score": validation.get("final_score"),
            "positive_labels": positive_labels,
            "leakage_flags": leakage_flags,
            "ledger_entry_id": journal.get("ledger_entry_id"),
            "outcome_id": outcome_id,
            "outcome_status": journal.get("outcome_status"),
            "return_20d": _workbench_round_ratio(return_20d),
            "spy_relative_return_20d": _workbench_round_ratio(spy_relative),
            "invalidation_touched": bool(journal.get("invalidation_touched")),
        },
        "recommendation": {
            "decision": "manual_review_required",
            "strategy_update_allowed": False,
            "autonomous_update_allowed": False,
            "requires_human_approval": True,
            "agent_execute_boundary": "agent execute",
            "next_action": "Review strategy hypotheses; do not auto-change rules.",
        },
        "commands": {
            "review": "agent",
            "validation": "validation",
            "journal": journal.get("primary_command") or "journal",
            "strategy_update": "agent execute",
        },
        "hypotheses": hypotheses,
        "metrics": {
            "hypothesis_count": len(hypotheses),
            "ready_hypothesis_count": ready_count,
            "review_hypothesis_count": review_count,
            "blocked_hypothesis_count": blocked_count,
            "disabled_hypothesis_count": disabled_count,
            "positive_label_count": len(positive_labels),
            "leakage_flag_count": len(leakage_flags),
            "paper_block_count": paper_block_count,
            "external_calls_made": 0,
            "db_writes_made": 0,
        },
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "strategy_update_allowed": False,
        "autonomous_update_allowed": False,
        "live_trading_enabled": False,
    }


def _workbench_strategy_review_hypothesis(
    *,
    hypothesis_id: str,
    label: str,
    driver: str,
    status: str,
    evidence: str,
    next_action: str,
) -> dict[str, object]:
    return {
        "id": hypothesis_id,
        "label": label,
        "driver": driver,
        "status": status,
        "evidence": evidence,
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "strategy_update_allowed": False,
        "autonomous_update_allowed": False,
        "live_trading_enabled": False,
        "next_action": next_action,
    }


def _workbench_trade_monitor_payload(
    *,
    active_plan: Mapping[str, object],
    risk_envelope: Mapping[str, object],
    paper_trade_preview: Mapping[str, object],
    learning_loop: Mapping[str, object],
    strategy_review: Mapping[str, object],
    trade_lifecycle_rows: Sequence[Mapping[str, object]],
    paper_trade_rows: Sequence[Mapping[str, object]],
    portfolio_open_order_rows: Sequence[Mapping[str, object]],
    alert_rows: Sequence[Mapping[str, object]],
    trigger_rows: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    ticker = _learning_loop_ticker(
        _first_value(
            strategy_review.get("ticker"),
            learning_loop.get("ticker"),
            paper_trade_preview.get("ticker"),
            risk_envelope.get("ticker"),
            active_plan.get("ticker"),
        )
    )
    decision_card_id = _first_value(
        strategy_review.get("decision_card_id"),
        learning_loop.get("decision_card_id"),
        paper_trade_preview.get("decision_card_id"),
        risk_envelope.get("decision_card_id"),
        active_plan.get("decision_card_id"),
    )
    lifecycle = _workbench_learning_loop_lifecycle(
        trade_lifecycle_rows,
        decision_card_id=decision_card_id,
        ticker=ticker,
    )
    paper_trade = _workbench_trade_monitor_match(
        paper_trade_rows,
        expected_id=lifecycle.get("paper_trade_id"),
        decision_card_id=decision_card_id,
        ticker=ticker,
    )
    paper_watch_rows = _workbench_trade_monitor_matching_rows(
        paper_trade_rows,
        ticker=ticker,
    )
    active_paper_rows = [
        row
        for row in paper_watch_rows
        if str(row.get("state") or "").strip().lower()
        in {"open", "active", "entered", "monitoring"}
    ]
    if not paper_trade and active_paper_rows:
        paper_trade = active_paper_rows[0]

    open_order_rows = [
        row
        for row in _workbench_trade_monitor_matching_rows(
            portfolio_open_order_rows,
            ticker=ticker,
        )
        if row.get("id")
        and str(row.get("status") or "").strip().lower()
        not in {"", "none", "cancelled", "canceled", "filled", "rejected"}
    ]
    alert_watch_rows = _workbench_trade_monitor_matching_rows(alert_rows, ticker=ticker)
    trigger_watch_rows = _workbench_trade_monitor_matching_rows(
        trigger_rows,
        ticker=ticker,
    )
    active_trigger_count = sum(
        1
        for row in trigger_watch_rows
        if str(row.get("status") or "").strip().lower() == "active"
    )
    primary_alert = _mapping(alert_watch_rows[0]) if alert_watch_rows else {}
    primary_trigger = _mapping(trigger_watch_rows[0]) if trigger_watch_rows else {}
    risk_metrics = _mapping(risk_envelope.get("metrics"))
    risk_portfolio = _mapping(risk_envelope.get("portfolio_context"))
    risk_sizing = _mapping(risk_envelope.get("sizing_context"))
    risk_gate = _mapping(risk_envelope.get("gate_context"))
    risk_blockers = _texts(risk_envelope.get("blockers"))
    strategy_context = _mapping(strategy_review.get("strategy_context"))
    journal_state = _mapping(learning_loop.get("journal_state"))
    blocked_check_count = _first_nonnegative_int(
        risk_metrics.get("blocked_check_count")
    )
    disabled_check_count = _first_nonnegative_int(
        risk_metrics.get("disabled_check_count")
    )
    active_trade = {
        "paper_trade_id": paper_trade.get("id"),
        "paper_state": paper_trade.get("state"),
        "decision": paper_trade.get("decision"),
        "entry_price": _workbench_round_float(
            _optional_float(
                _first_value(paper_trade.get("entry_price"), risk_sizing.get("entry_price"))
            )
        ),
        "invalidation_price": _workbench_round_float(
            _optional_float(
                _first_value(
                    paper_trade.get("invalidation_price"),
                    risk_sizing.get("invalidation_price"),
                )
            )
        ),
        "shares": paper_trade.get("shares"),
        "notional": _workbench_round_float(_optional_float(paper_trade.get("notional"))),
        "max_loss": _workbench_round_float(
            _optional_float(
                _first_value(paper_trade.get("max_loss"), risk_sizing.get("estimated_max_loss"))
            )
        ),
        "outcome_id": journal_state.get("outcome_id"),
        "outcome_status": journal_state.get("outcome_status"),
        "no_execution": bool(paper_trade.get("no_execution", True)),
        "broker_order_submitted": False,
        "order_submission_allowed": False,
    }
    exit_plan = {
        "entry_price": active_trade["entry_price"],
        "invalidation_price": active_trade["invalidation_price"],
        "target_price": _workbench_round_float(
            _optional_float(
                _first_value(
                    strategy_context.get("target_price"),
                    risk_sizing.get("target_price"),
                )
            )
        ),
        "stop_status": "watch"
        if active_trade.get("invalidation_price") is not None
        else "blocked",
        "target_status": "watch"
        if _first_value(strategy_context.get("target_price"), risk_sizing.get("target_price"))
        is not None
        else "unlinked",
        "exit_update_allowed": False,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "live_trading_enabled": False,
        "primary_command": "broker",
        "next_action": "Review exit context manually; broker submission remains disabled.",
    }
    watch_items = [
        _workbench_trade_monitor_watch_item(
            item_id="active-paper-trade",
            label="Active paper trade",
            status="ready" if active_paper_rows else "blocked",
            scope="paper-trading",
            finding=active_trade.get("paper_state") or "missing_active_paper_trade",
            evidence=str(active_trade.get("paper_trade_id") or "no paper trade"),
            next_action=(
                "Use the open paper trade as the monitored position state."
                if active_paper_rows
                else "Record or link a paper trade before monitoring position state."
            ),
        ),
        _workbench_trade_monitor_watch_item(
            item_id="invalidation-watch",
            label="Invalidation watch",
            status="ready"
            if exit_plan.get("invalidation_price") is not None
            else "blocked",
            scope="risk-desk",
            finding=f"invalidation={exit_plan.get('invalidation_price')}"
            if exit_plan.get("invalidation_price") is not None
            else "missing_invalidation_price",
            evidence=(
                f"entry={exit_plan.get('entry_price')}; "
                f"target={exit_plan.get('target_price')}"
            ),
            next_action="Compare stored invalidation and target before changing state.",
        ),
        _workbench_trade_monitor_watch_item(
            item_id="alert-trigger-watch",
            label="Alert and trigger watch",
            status="ready" if alert_watch_rows or trigger_watch_rows else "review",
            scope="alerts",
            finding=primary_trigger.get("id")
            or primary_alert.get("id")
            or "no_alert_or_trigger",
            evidence=f"{len(alert_watch_rows)} alerts; {len(trigger_watch_rows)} triggers",
            next_action=(
                "Open alert evidence or evaluate saved trigger rules."
                if alert_watch_rows or trigger_watch_rows
                else "Create local alert or trigger context if this trade needs monitoring."
            ),
        ),
        _workbench_trade_monitor_watch_item(
            item_id="portfolio-open-orders",
            label="Open order check",
            status="review" if open_order_rows else "ready",
            scope="broker",
            finding="open_orders_present" if open_order_rows else "no_open_orders",
            evidence=f"{len(open_order_rows)} open orders",
            next_action=(
                "Review read-only open orders before changing paper state."
                if open_order_rows
                else "No open broker orders are present in the read-only snapshot."
            ),
        ),
        _workbench_trade_monitor_watch_item(
            item_id="risk-blockers",
            label="Risk blockers",
            status="blocked" if blocked_check_count else "ready",
            scope="risk-desk",
            finding=risk_blockers[0] if risk_blockers else "risk_checks_clear",
            evidence=(
                f"{blocked_check_count} blocked checks; "
                f"{disabled_check_count} disabled boundaries"
            ),
            next_action=(
                "Resolve risk blockers before changing position state."
                if blocked_check_count
                else "Risk blockers are clear for supervised monitoring."
            ),
        ),
        _workbench_trade_monitor_watch_item(
            item_id="exit-boundary",
            label="Exit boundary",
            status="disabled",
            scope="broker",
            finding="broker_submission_disabled",
            evidence="exit orders require manual broker workflow",
            next_action="No exit order is submitted from this monitor.",
        ),
    ]
    ready_count = sum(1 for row in watch_items if row.get("status") == "ready")
    blocked_count = sum(1 for row in watch_items if row.get("status") == "blocked")
    review_count = sum(1 for row in watch_items if row.get("status") == "review")
    disabled_count = sum(1 for row in watch_items if row.get("status") == "disabled")
    primary_blocker = risk_blockers[0] if risk_blockers else None
    if primary_blocker is None:
        primary_blocker = next(
            (
                str(row.get("finding"))
                for row in watch_items
                if row.get("status") == "blocked" and row.get("finding")
            ),
            None,
        )
    monitor_stage = (
        lifecycle.get("current_stage")
        or learning_loop.get("learning_stage")
        or strategy_review.get("strategy_stage")
        or "unlinked"
    )
    return {
        "schema_version": "trading-workbench-trade-monitor-v1",
        "status": "blocked"
        if blocked_count
        else "review"
        if review_count
        else "monitoring",
        "source_tool": "market-radar",
        "ticker": ticker,
        "decision_card_id": decision_card_id,
        "monitor_id": (
            f"trade-monitor-{str(ticker or 'unknown').lower()}-"
            f"{decision_card_id or 'no-card'}"
        ),
        "monitor_stage": monitor_stage,
        "primary_blocker": primary_blocker,
        "primary_next_action": (
            "Resolve risk and monitor blockers before changing position state."
            if primary_blocker
            else "Monitor active trade evidence; exits and broker submission require manual review."
        ),
        "active_trade": active_trade,
        "risk_watch": {
            "risk_envelope_status": risk_envelope.get("status"),
            "blocked_check_count": blocked_check_count,
            "disabled_check_count": disabled_check_count,
            "paper_block_count": _first_nonnegative_int(risk_gate.get("paper_block_count")),
            "live_block_count": _first_nonnegative_int(risk_gate.get("live_block_count")),
            "estimated_max_loss": risk_sizing.get("estimated_max_loss"),
            "broker_data_stale": bool(risk_portfolio.get("broker_data_stale")),
            "broker_order_submitted": False,
            "order_submission_allowed": False,
            "live_trading_enabled": False,
            "blockers": risk_blockers,
        },
        "alert_watch": {
            "alert_count": len(alert_watch_rows),
            "trigger_count": len(trigger_watch_rows),
            "active_trigger_count": active_trigger_count,
            "primary_alert_id": primary_alert.get("id"),
            "primary_trigger_id": primary_trigger.get("id"),
            "primary_trigger_type": primary_trigger.get("trigger_type"),
            "latest_trigger_value": primary_trigger.get("latest_value"),
            "next_action": "Open alert evidence or evaluate saved trigger rules.",
        },
        "exit_plan": exit_plan,
        "commands": {
            "paper_trade": "paper",
            "alerts": "alerts",
            "journal": journal_state.get("primary_command") or "journal",
            "risk": "risk-desk",
            "broker_boundary": "broker",
        },
        "watch_items": watch_items,
        "metrics": {
            "watch_item_count": len(watch_items),
            "ready_watch_item_count": ready_count,
            "review_watch_item_count": review_count,
            "blocked_watch_item_count": blocked_count,
            "disabled_watch_item_count": disabled_count,
            "active_paper_trade_count": len(active_paper_rows),
            "alert_count": len(alert_watch_rows),
            "trigger_count": len(trigger_watch_rows),
            "active_trigger_count": active_trigger_count,
            "open_order_count": len(open_order_rows),
            "blocked_check_count": blocked_check_count,
            "external_calls_made": 0,
            "db_writes_made": 0,
        },
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "position_state_update_allowed": False,
        "exit_update_allowed": False,
        "live_trading_enabled": False,
    }


def _workbench_trade_monitor_matching_rows(
    rows: Sequence[Mapping[str, object]],
    *,
    ticker: object,
) -> list[Mapping[str, object]]:
    ticker_text = _learning_loop_ticker(ticker)
    mapped_rows = _rows(rows)
    if not ticker_text:
        return mapped_rows
    return [
        row
        for row in mapped_rows
        if _learning_loop_ticker(row.get("ticker")) == ticker_text
    ]


def _workbench_trade_monitor_match(
    rows: Sequence[Mapping[str, object]],
    *,
    expected_id: object = None,
    decision_card_id: object = None,
    ticker: object = None,
) -> Mapping[str, object]:
    expected_text = str(expected_id or "").strip()
    card_text = str(decision_card_id or "").strip()
    ticker_text = _learning_loop_ticker(ticker)
    mapped_rows = _rows(rows)
    if expected_text:
        for row in mapped_rows:
            if str(row.get("id") or "").strip() == expected_text:
                return row
    if card_text:
        for row in mapped_rows:
            if str(row.get("decision_card_id") or "").strip() == card_text:
                return row
    if ticker_text:
        for row in mapped_rows:
            if _learning_loop_ticker(row.get("ticker")) == ticker_text:
                return row
    return _mapping(mapped_rows[0]) if mapped_rows else {}


def _workbench_trade_monitor_watch_item(
    *,
    item_id: str,
    label: str,
    status: str,
    scope: str,
    finding: object,
    evidence: str,
    next_action: str,
) -> dict[str, object]:
    return {
        "id": item_id,
        "label": label,
        "status": status,
        "scope": scope,
        "finding": finding,
        "evidence": evidence,
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "position_state_update_allowed": False,
        "exit_update_allowed": False,
        "live_trading_enabled": False,
        "next_action": next_action,
    }


def _workbench_ratio(
    numerator: float | None,
    denominator: float | None,
) -> float | None:
    if numerator is None or denominator in (None, 0):
        return None
    return numerator / denominator


def _workbench_round_ratio(value: float | None) -> float | None:
    return round(value, 4) if value is not None else None


def _workbench_trade_runbook_payload(
    *,
    decision_brief: Mapping[str, object],
    scenario_matrix: Mapping[str, object],
    risk_envelope: Mapping[str, object],
    action_bus: Mapping[str, object],
) -> dict[str, object]:
    actions = {
        str(row.get("id")): row
        for row in _rows(action_bus.get("actions"))
        if row.get("id")
    }
    scenario_count = _first_nonnegative_int(
        _mapping(scenario_matrix.get("metrics")).get("scenario_count")
    )
    blocked_check_count = _first_nonnegative_int(
        _mapping(risk_envelope.get("metrics")).get("blocked_check_count")
    )
    paper_preview = actions.get("paper-decision-preview", {})
    paper_record = actions.get("paper-decision-record", {})
    ticket_preview = actions.get("order-ticket-preview", {})
    paper_record_writes = _first_nonnegative_int(
        paper_record.get("db_writes_required")
    )
    if paper_record_writes <= 0:
        paper_record_writes = 2
    steps = [
        _workbench_runbook_step(
            step_id="decision-review",
            rank=1,
            module="review",
            label="Decision readiness",
            status="blocked",
            step_kind="review",
            action_kind="page",
            command="review",
            target_page="review",
            safety="zero_call_navigation",
            source="trading_workbench.decision_brief",
            evidence=str(decision_brief.get("decision_card_id") or "no decision card"),
            next_action="Review decision readiness before continuing.",
        ),
        _workbench_runbook_step(
            step_id="scenario-review",
            rank=2,
            module="trade-planner",
            label="Scenario review",
            status="review",
            step_kind="analysis",
            action_kind="page",
            command="trade-planner",
            target_page="trade-planner",
            safety="zero_call_navigation",
            source="trading_workbench.scenario_matrix",
            evidence=f"{scenario_count} scenarios",
            next_action="Compare downside, entry, and reward target before sizing.",
        ),
        _workbench_runbook_step(
            step_id="risk-envelope",
            rank=3,
            module="risk-desk",
            label="Risk envelope",
            status="blocked",
            step_kind="risk",
            action_kind="page",
            command="risk-desk",
            target_page="risk-desk",
            safety="zero_call_navigation",
            source="trading_workbench.risk_envelope",
            evidence=f"{blocked_check_count} blocked checks",
            next_action="Resolve risk-envelope blockers before local writes.",
        ),
        _workbench_runbook_step(
            step_id="paper-decision-preview",
            rank=4,
            module="paper-trading",
            label="Preview paper decision",
            status="ready",
            step_kind="preview",
            action_kind="backend_command",
            command=paper_preview.get("command") or "paper-decision preview",
            target_page=paper_preview.get("target_page") or "paper-trading",
            safety=paper_preview.get("safety") or "local_backend_preview",
            source="trading_workbench.action_bus.paper-decision-preview",
            evidence="local preview",
            next_action="Preview only after decision and risk blockers are clear.",
        ),
        _workbench_runbook_step(
            step_id="paper-decision-record",
            rank=5,
            module="paper-trading",
            label="Record paper decision",
            status="approval_required",
            step_kind="guarded_write",
            action_kind="backend_command",
            command=paper_record.get("command") or "paper-decision execute",
            target_page=paper_record.get("target_page") or "paper-trading",
            safety=paper_record.get("safety") or "local_db_write",
            local_write_allowed=True,
            requires_arm_before_run=True,
            db_writes_required=paper_record_writes,
            source="trading_workbench.action_bus.paper-decision-record",
            evidence="guarded local write",
            next_action="Arm and record only after manual approval.",
        ),
        _workbench_runbook_step(
            step_id="order-ticket-preview",
            rank=6,
            module="broker",
            label="Preview blocked ticket",
            status="ready",
            step_kind="preview",
            action_kind="backend_command",
            command=ticket_preview.get("command") or "order-ticket preview",
            target_page=ticket_preview.get("target_page") or "broker",
            safety=ticket_preview.get("safety") or "local_backend_preview",
            source="trading_workbench.action_bus.order-ticket-preview",
            evidence="blocked ticket preview",
            next_action="Preview the local ticket; live submission stays disabled.",
        ),
        _workbench_runbook_step(
            step_id="live-submission-boundary",
            rank=7,
            module="broker",
            label="Live submission boundary",
            status="disabled",
            step_kind="boundary",
            action_kind="boundary",
            command="broker live submission",
            target_page="broker",
            safety="external_boundary",
            source="trading_workbench.execution_boundary",
            evidence="live trading disabled",
            next_action="Live broker submission remains disabled.",
        ),
        _workbench_runbook_step(
            step_id="journal-validation",
            rank=8,
            module="journal",
            label="Journal and validation",
            status="ready",
            step_kind="review",
            action_kind="page",
            command="journal",
            target_page="journal",
            safety="zero_call_navigation",
            source="trading_workbench.modules.journal",
            evidence="journal and validation review",
            next_action="Review lifecycle, journal, and validation evidence.",
        ),
    ]
    active_step_id = next(
        (
            str(row.get("id"))
            for row in steps
            if row.get("status") in {"blocked", "approval_required"}
        ),
        steps[0]["id"] if steps else None,
    )
    blocked_step_count = sum(1 for row in steps if row.get("status") == "blocked")
    approval_required_count = sum(
        1 for row in steps if row.get("status") == "approval_required"
    )
    disabled_step_count = sum(1 for row in steps if row.get("status") == "disabled")
    return {
        "schema_version": "trading-workbench-runbook-v1",
        "status": "blocked"
        if blocked_step_count
        else "approval_required"
        if approval_required_count
        else "ready",
        "source_tool": decision_brief.get("source_tool") or "market-radar",
        "ticker": _first_value(
            decision_brief.get("ticker"),
            scenario_matrix.get("ticker"),
            risk_envelope.get("ticker"),
        ),
        "decision_card_id": _first_value(
            decision_brief.get("decision_card_id"),
            scenario_matrix.get("decision_card_id"),
            risk_envelope.get("decision_card_id"),
        ),
        "active_step_id": active_step_id,
        "primary_next_action": "Review decision readiness before continuing.",
        "steps": steps,
        "metrics": {
            "step_count": len(steps),
            "blocked_step_count": blocked_step_count,
            "approval_required_count": approval_required_count,
            "disabled_step_count": disabled_step_count,
            "preview_step_count": sum(
                1 for row in steps if row.get("step_kind") == "preview"
            ),
            "local_write_step_count": sum(
                1 for row in steps if row.get("local_write_allowed") is True
            ),
            "external_calls_made": 0,
        },
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "live_trading_enabled": False,
    }


def _workbench_runbook_step(
    *,
    step_id: str,
    rank: int,
    module: str,
    label: str,
    status: str,
    step_kind: str,
    action_kind: str,
    command: object,
    target_page: object,
    safety: object,
    source: str,
    evidence: str,
    next_action: str,
    local_write_allowed: bool = False,
    requires_arm_before_run: bool = False,
    db_writes_required: object = 0,
) -> dict[str, object]:
    return {
        "id": step_id,
        "rank": rank,
        "module": module,
        "label": label,
        "status": status,
        "step_kind": step_kind,
        "action_kind": action_kind,
        "command": str(command or "").strip() or None,
        "target_page": str(target_page or module).strip(),
        "safety": str(safety or "").strip() or "zero_call_navigation",
        "local_write_allowed": local_write_allowed,
        "requires_arm_before_run": requires_arm_before_run,
        "external_calls_allowed": False,
        "external_calls_made": 0,
        "db_writes_required": _first_nonnegative_int(db_writes_required),
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "live_trading_enabled": False,
        "source": source,
        "evidence": evidence,
        "next_action": next_action,
    }


def _workbench_execution_sandbox_payload(
    *,
    operator_state: Mapping[str, object],
    trade_runbook: Mapping[str, object],
    action_bus: Mapping[str, object],
    risk_envelope: Mapping[str, object],
) -> dict[str, object]:
    actions = {
        str(row.get("id")): row
        for row in _rows(action_bus.get("actions"))
        if row.get("id")
    }
    paper_preview = actions.get("paper-decision-preview", {})
    ticket_preview = actions.get("order-ticket-preview", {})
    paper_record = actions.get("paper-decision-record", {})
    ticket_record = actions.get("order-ticket-record", {})
    paper_record_writes = _first_nonnegative_int(
        paper_record.get("db_writes_required")
    )
    if paper_record_writes <= 0:
        paper_record_writes = 2
    ticket_record_writes = _first_nonnegative_int(
        ticket_record.get("db_writes_required")
    )
    if ticket_record_writes <= 0:
        ticket_record_writes = 1
    primary_next_action = (
        "Review decision readiness before previewing or recording execution artifacts."
    )
    lanes = [
        _workbench_execution_sandbox_lane(
            lane_id="review-before-execution",
            rank=1,
            module="review",
            label="Review before execution",
            status="blocked",
            lane_kind="precondition",
            action_kind="page",
            command="review",
            target_page="review",
            safety="zero_call_navigation",
            source="trading_workbench.operator_state",
            evidence=str(operator_state.get("primary_blocker") or "Decision readiness"),
            next_action=primary_next_action,
        ),
        _workbench_execution_sandbox_lane(
            lane_id="paper-preview",
            rank=2,
            module="paper-trading",
            label="Paper decision preview",
            status="ready"
            if paper_preview.get("status") == "enabled"
            else "blocked",
            lane_kind="preview",
            action_kind="backend_command",
            command=paper_preview.get("command") or "paper-decision preview",
            target_page=paper_preview.get("target_page") or "paper-trading",
            safety=paper_preview.get("safety") or "local_backend_preview",
            source="trading_workbench.action_bus.paper-decision-preview",
            evidence="local preview",
            next_action="Preview the paper decision only after blockers are reviewed.",
        ),
        _workbench_execution_sandbox_lane(
            lane_id="ticket-preview",
            rank=3,
            module="broker",
            label="Broker ticket preview",
            status="ready"
            if ticket_preview.get("status") == "enabled"
            else "blocked",
            lane_kind="preview",
            action_kind="backend_command",
            command=ticket_preview.get("command") or "order-ticket preview",
            target_page=ticket_preview.get("target_page") or "broker",
            safety=ticket_preview.get("safety") or "local_backend_preview",
            source="trading_workbench.action_bus.order-ticket-preview",
            evidence="blocked ticket preview",
            next_action="Preview the local ticket; live submission stays disabled.",
        ),
        _workbench_execution_sandbox_lane(
            lane_id="paper-record",
            rank=4,
            module="paper-trading",
            label="Guarded paper record",
            status="approval_required",
            lane_kind="guarded_write",
            action_kind="backend_command",
            command=paper_record.get("command") or "paper-decision execute",
            target_page=paper_record.get("target_page") or "paper-trading",
            safety=paper_record.get("safety") or "local_db_write",
            local_write_allowed=True,
            requires_arm_before_run=True,
            db_writes_required=paper_record_writes,
            source="trading_workbench.action_bus.paper-decision-record",
            evidence="guarded local write",
            next_action="Arm and record only after manual approval.",
        ),
        _workbench_execution_sandbox_lane(
            lane_id="ticket-record",
            rank=5,
            module="broker",
            label="Guarded ticket record",
            status="approval_required",
            lane_kind="guarded_write",
            action_kind="backend_command",
            command=ticket_record.get("command") or "order-ticket record",
            target_page=ticket_record.get("target_page") or "broker",
            safety=ticket_record.get("safety") or "local_db_write",
            local_write_allowed=True,
            requires_arm_before_run=True,
            db_writes_required=ticket_record_writes,
            source="trading_workbench.action_bus.order-ticket-record",
            evidence="blocked ticket local write",
            next_action="Arm and save only after manual approval.",
        ),
        _workbench_execution_sandbox_lane(
            lane_id="live-submit",
            rank=6,
            module="broker",
            label="Live broker submission",
            status="disabled",
            lane_kind="external_boundary",
            action_kind="boundary",
            command="broker live submission",
            target_page="broker",
            safety="external_boundary",
            source="trading_workbench.execution_boundary",
            evidence="live trading disabled",
            next_action="Live broker submission remains disabled.",
        ),
        _workbench_execution_sandbox_lane(
            lane_id="agent-execute",
            rank=7,
            module="agent",
            label="Agent execution",
            status="disabled",
            lane_kind="agent_boundary",
            action_kind="boundary",
            command="agent execute",
            target_page="agent",
            safety="agent_execution_boundary",
            source="trading_workbench.supervision_gates",
            evidence="agent execution disabled",
            next_action="Agent execution remains disabled.",
        ),
    ]
    blocked_lane_count = sum(1 for row in lanes if row.get("status") == "blocked")
    approval_required_count = sum(
        1 for row in lanes if row.get("status") == "approval_required"
    )
    disabled_lane_count = sum(1 for row in lanes if row.get("status") == "disabled")
    return {
        "schema_version": "trading-workbench-execution-sandbox-v1",
        "status": "blocked"
        if blocked_lane_count
        else "approval_required"
        if approval_required_count
        else "ready",
        "source_tool": operator_state.get("source_tool") or "market-radar",
        "ticker": _first_value(
            operator_state.get("ticker"),
            trade_runbook.get("ticker"),
            risk_envelope.get("ticker"),
        ),
        "decision_card_id": _first_value(
            operator_state.get("decision_card_id"),
            trade_runbook.get("decision_card_id"),
            risk_envelope.get("decision_card_id"),
        ),
        "active_lane_id": next(
            (
                str(row.get("id"))
                for row in lanes
                if row.get("status") in {"blocked", "approval_required"}
            ),
            lanes[0]["id"] if lanes else None,
        ),
        "primary_next_action": primary_next_action,
        "lanes": lanes,
        "metrics": {
            "lane_count": len(lanes),
            "preview_lane_count": sum(
                1 for row in lanes if row.get("lane_kind") == "preview"
            ),
            "guarded_write_lane_count": sum(
                1 for row in lanes if row.get("lane_kind") == "guarded_write"
            ),
            "approval_required_count": approval_required_count,
            "blocked_lane_count": blocked_lane_count,
            "disabled_lane_count": disabled_lane_count,
            "external_calls_made": 0,
            "db_writes_made": 0,
        },
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "live_trading_enabled": False,
    }


def _workbench_execution_sandbox_lane(
    *,
    lane_id: str,
    rank: int,
    module: str,
    label: str,
    status: str,
    lane_kind: str,
    action_kind: str,
    command: object,
    target_page: object,
    safety: object,
    source: str,
    evidence: str,
    next_action: str,
    local_write_allowed: bool = False,
    requires_arm_before_run: bool = False,
    db_writes_required: object = 0,
) -> dict[str, object]:
    return {
        "id": lane_id,
        "rank": rank,
        "module": module,
        "label": label,
        "status": status,
        "lane_kind": lane_kind,
        "action_kind": action_kind,
        "command": str(command or "").strip() or None,
        "target_page": str(target_page or module).strip(),
        "safety": str(safety or "").strip() or "zero_call_navigation",
        "local_write_allowed": local_write_allowed,
        "requires_arm_before_run": requires_arm_before_run,
        "external_calls_allowed": False,
        "external_calls_made": 0,
        "db_writes_required": _first_nonnegative_int(db_writes_required),
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "live_trading_enabled": False,
        "source": source,
        "evidence": evidence,
        "next_action": next_action,
    }


def _workbench_operator_state_payload(
    *,
    decision_brief: Mapping[str, object],
    scenario_matrix: Mapping[str, object],
    risk_envelope: Mapping[str, object],
    trade_runbook: Mapping[str, object],
    action_bus: Mapping[str, object],
    workflow_map: Mapping[str, object],
    priority_queue: Mapping[str, object],
    supervision_gates: Mapping[str, object],
) -> dict[str, object]:
    steps = _rows(trade_runbook.get("steps"))
    active_step_id = str(trade_runbook.get("active_step_id") or "")
    active_step = next(
        (
            row
            for row in steps
            if str(row.get("id") or "") == active_step_id
        ),
        steps[0] if steps else {},
    )
    supervision_metrics = _mapping(supervision_gates.get("metrics"))
    runbook_metrics = _mapping(trade_runbook.get("metrics"))
    action_metrics = _mapping(action_bus.get("metrics"))
    priority_metrics = _mapping(priority_queue.get("metrics"))
    scenario_metrics = _mapping(scenario_matrix.get("metrics"))
    risk_metrics = _mapping(risk_envelope.get("metrics"))
    sizing_context = _mapping(risk_envelope.get("sizing_context"))
    primary_next_action = _workbench_operator_next_action(active_step)
    state_cards = [
        _workbench_operator_state_card(
            card_id="decision",
            module="review",
            label="Decision",
            status=decision_brief.get("status") or "unknown",
            evidence=decision_brief.get("decision_card_id") or "no decision card",
            next_action=trade_runbook.get("primary_next_action")
            or "Review decision readiness before continuing.",
        ),
        _workbench_operator_state_card(
            card_id="scenario",
            module="trade-planner",
            label="Scenario",
            status=scenario_matrix.get("status") or "unknown",
            evidence=(
                f"{_first_nonnegative_int(scenario_metrics.get('scenario_count'))} scenarios"
            ),
            next_action="Compare downside, entry, and reward target before sizing.",
        ),
        _workbench_operator_state_card(
            card_id="risk",
            module="risk-desk",
            label="Risk",
            status=risk_envelope.get("status") or "unknown",
            evidence=(
                f"{_first_nonnegative_int(risk_metrics.get('blocked_check_count'))} "
                "blocked checks"
            ),
            next_action="Resolve risk-envelope blockers before local writes.",
        ),
        _workbench_operator_state_card(
            card_id="supervision",
            module="platform",
            label="Supervision",
            status=supervision_gates.get("status") or "unknown",
            evidence=supervision_gates.get("primary_gate_id") or "no active gate",
            next_action="Arm local writes only after manual approval.",
        ),
        _workbench_operator_state_card(
            card_id="boundary",
            module="broker",
            label="Live boundary",
            status="disabled",
            evidence="live trading disabled",
            next_action="Live broker submission remains disabled.",
        ),
    ]
    boundaries = {
        "provider_calls_for_browsing": 0,
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "live_trading_enabled": False,
        "autonomous_execution": "disabled",
    }
    return {
        "schema_version": "trading-workbench-operator-state-v1",
        "status": trade_runbook.get("status") or decision_brief.get("status") or "unknown",
        "operating_mode": "supervised_decision_support",
        "source_tool": decision_brief.get("source_tool") or "market-radar",
        "ticker": _first_value(
            decision_brief.get("ticker"),
            scenario_matrix.get("ticker"),
            risk_envelope.get("ticker"),
            trade_runbook.get("ticker"),
        ),
        "decision_card_id": _first_value(
            decision_brief.get("decision_card_id"),
            scenario_matrix.get("decision_card_id"),
            risk_envelope.get("decision_card_id"),
            trade_runbook.get("decision_card_id"),
        ),
        "headline": decision_brief.get("headline"),
        "active_module": active_step.get("module") or "platform",
        "active_stage_id": workflow_map.get("active_stage_id"),
        "active_step_id": active_step.get("id"),
        "primary_blocker": active_step.get("label") or "Review operator state",
        "primary_next_action": primary_next_action,
        "readiness": {
            "decision_brief_status": decision_brief.get("status") or "unknown",
            "scenario_matrix_status": scenario_matrix.get("status") or "unknown",
            "risk_envelope_status": risk_envelope.get("status") or "unknown",
            "runbook_status": trade_runbook.get("status") or "unknown",
            "supervision_status": supervision_gates.get("status") or "unknown",
            "approval_required": _first_nonnegative_int(
                supervision_metrics.get("approval_required_count")
            )
            > 0,
            "local_write_armed": False,
        },
        "risk": {
            "sizing_status": sizing_context.get("sizing_status") or "unknown",
            "blocked_check_count": _first_nonnegative_int(
                risk_metrics.get("blocked_check_count")
            ),
            "paper_block_count": _first_nonnegative_int(
                risk_metrics.get("paper_block_count")
            ),
            "live_block_count": _first_nonnegative_int(
                risk_metrics.get("live_block_count")
            ),
            "estimated_max_loss": sizing_context.get("estimated_max_loss"),
            "max_loss_pct_of_equity": sizing_context.get("max_loss_pct_of_equity"),
        },
        "agent_handoff": {
            "next_page": primary_next_action.get("target_page"),
            "next_command": primary_next_action.get("command"),
            "control_kind": primary_next_action.get("action_kind"),
            "safety": primary_next_action.get("safety"),
            "can_execute_without_approval": (
                primary_next_action.get("action_kind") == "page"
                and not primary_next_action.get("local_write_allowed")
                and not primary_next_action.get("requires_arm_before_run")
            ),
            "local_write_requires_arm": bool(
                primary_next_action.get("requires_arm_before_run")
            ),
            "disabled_reason": None
            if primary_next_action.get("action_kind") != "boundary"
            else "disabled_boundary",
        },
        "state_cards": state_cards,
        "boundaries": boundaries,
        "metrics": {
            "state_card_count": len(state_cards),
            "runbook_step_count": _first_nonnegative_int(
                runbook_metrics.get("step_count")
            ),
            "blocked_step_count": _first_nonnegative_int(
                runbook_metrics.get("blocked_step_count")
            ),
            "approval_required_count": _first_nonnegative_int(
                supervision_metrics.get("approval_required_count"),
                runbook_metrics.get("approval_required_count"),
            ),
            "disabled_boundary_count": _first_nonnegative_int(
                supervision_metrics.get("disabled_gate_count"),
                runbook_metrics.get("disabled_step_count"),
            ),
            "action_count": _first_nonnegative_int(action_metrics.get("action_count")),
            "priority_item_count": _first_nonnegative_int(
                priority_metrics.get("item_count")
            ),
            "external_calls_made": 0,
        },
        **boundaries,
    }


def _workbench_operator_next_action(
    step: Mapping[str, object],
) -> dict[str, object]:
    return {
        "label": step.get("label") or "Review operator state",
        "action_kind": step.get("action_kind") or "page",
        "command": step.get("command") or step.get("target_page") or "overview",
        "target_page": step.get("target_page") or step.get("module") or "overview",
        "safety": step.get("safety") or "zero_call_navigation",
        "source": "trade_runbook",
        "local_write_allowed": bool(step.get("local_write_allowed")),
        "requires_arm_before_run": bool(step.get("requires_arm_before_run")),
    }


def _workbench_operator_state_card(
    *,
    card_id: str,
    module: str,
    label: str,
    status: object,
    evidence: object,
    next_action: object,
) -> dict[str, object]:
    return {
        "id": card_id,
        "module": module,
        "label": label,
        "status": str(status or "unknown"),
        "evidence": str(evidence or "-"),
        "next_action": str(next_action or "-"),
    }


def _workbench_priority_action_sort_key(
    action: Mapping[str, object],
    index: int,
) -> tuple[int, int, int]:
    action_id = str(action.get("id") or "")
    status = str(action.get("status") or "")
    action_kind = str(action.get("action_kind") or "")
    local_write = bool(action.get("local_write_allowed"))
    preferred = {
        "agent-preview": 0,
        "paper-decision-preview": 1,
        "order-ticket-preview": 2,
        "portfolio-review": 3,
        "lifecycle-ledger-review": 4,
        "lifecycle-outcome-preview": 5,
    }
    if action_id == "agent-preview":
        tier = 0
    elif status == "enabled" and not local_write and action_kind != "boundary":
        tier = 1
    elif status == "enabled" and local_write:
        tier = 2
    elif status == "blocked":
        tier = 3
    elif action_kind == "boundary":
        tier = 4
    else:
        tier = 5
    return (tier, preferred.get(action_id, 100), index)


def _workbench_priority_action_priority(action: Mapping[str, object]) -> int:
    action_id = str(action.get("id") or "")
    action_kind = str(action.get("action_kind") or "")
    if action_id == "agent-preview":
        return 85
    if action.get("status") == "enabled" and not action.get("local_write_allowed"):
        return 80
    if action.get("status") == "enabled":
        return 70
    if action.get("status") == "blocked":
        return 50
    if action_kind == "boundary":
        return 20
    return 10


def _workbench_priority_action_reason(action: Mapping[str, object]) -> str:
    action_kind = str(action.get("action_kind") or "")
    if action_kind == "boundary":
        return "Disabled execution boundary"
    if action.get("local_write_allowed"):
        return "Guarded local write"
    if action_kind == "page":
        return "Safe module handoff"
    if action.get("status") == "blocked":
        return "Blocked supervised action"
    return "Safe local preview"


def _workbench_queue_row(
    row: Mapping[str, object],
    *,
    candidate_by_ticker: Mapping[str, Mapping[str, object]] | None = None,
) -> dict[str, object]:
    if not row:
        return {}
    ticker = str(
        row.get("ticker") or row.get("symbol") or row.get("security") or ""
    ).upper()
    candidate = _mapping((candidate_by_ticker or {}).get(ticker)) if ticker else {}
    usefulness = _mapping(row.get("usefulness"))
    return {
        "ticker": ticker or None,
        "state": row.get("state") or row.get("status") or row.get("usefulness"),
        "subject": row.get("subject") or row.get("title") or row.get("why_now"),
        "usefulness_status": usefulness.get("status"),
        "decision_ready": bool(usefulness.get("decision_ready")),
        "decision_card_id": (
            row.get("decision_card_id")
            or candidate.get("decision_card_id")
            or candidate.get("card")
        ),
        "score": _first_value(row.get("score"), row.get("final_score")),
        "setup": row.get("setup") or row.get("setup_type"),
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "next_action": (
            row.get("next_action")
            or usefulness.get("next_action")
            or row.get("next_step")
            or row.get("command")
        ),
    }


def _workbench_alert_row(row: Mapping[str, object]) -> dict[str, object]:
    return {
        "id": row.get("id"),
        "ticker": row.get("ticker"),
        "route": row.get("route"),
        "status": row.get("status"),
        "priority": row.get("priority"),
        "trigger_kind": row.get("trigger_kind"),
        "score_trigger": row.get("score_trigger"),
        "feedback_label": row.get("feedback_label"),
        "available_at": row.get("available_at"),
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "next_action": "Open alert evidence; feedback stays local.",
    }


def _workbench_market_trigger_row(row: Mapping[str, object]) -> dict[str, object]:
    return {
        "id": row.get("id"),
        "ticker": row.get("ticker"),
        "trigger_type": row.get("trigger_type"),
        "operator": row.get("operator"),
        "threshold": row.get("threshold"),
        "latest_value": row.get("latest_value"),
        "status": row.get("status"),
        "fired_at": row.get("fired_at"),
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "next_action": "Evaluate against stored market context; no broker order is submitted.",
    }


def _workbench_opportunity_action_row(row: Mapping[str, object]) -> dict[str, object]:
    return {
        "id": row.get("id"),
        "ticker": row.get("ticker"),
        "action": row.get("action"),
        "status": row.get("status"),
        "notes": row.get("notes"),
        "created_at": row.get("created_at"),
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "next_action": "Review local operator action before changing watch state.",
    }


def _workbench_ipo_s1_row(row: Mapping[str, object]) -> dict[str, object]:
    return {
        "id": row.get("id"),
        "ticker": row.get("ticker") or row.get("proposed_ticker"),
        "form_type": row.get("form_type"),
        "filing_date": row.get("filing_date"),
        "exchange": row.get("exchange"),
        "price_range_low": row.get("price_range_low"),
        "price_range_high": row.get("price_range_high"),
        "estimated_gross_proceeds": row.get("estimated_gross_proceeds"),
        "risk_flags": _texts(row.get("risk_flags")),
        "summary": row.get("summary"),
        "document_url": row.get("document_url") or row.get("source_url"),
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "next_action": "Review the filing as research evidence; no trade is approved.",
    }


def _workbench_theme_row(row: Mapping[str, object]) -> dict[str, object]:
    return {
        "theme": row.get("theme"),
        "candidate_count": row.get("candidate_count"),
        "avg_score": row.get("avg_score"),
        "top_tickers": _texts(row.get("top_tickers")),
        "states": _mapping(row.get("states")),
        "latest_as_of": row.get("latest_as_of"),
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "next_action": "Open candidate rows before using the theme in a thesis.",
    }


def _workbench_feature_inventory_row(row: Mapping[str, object]) -> dict[str, object]:
    return {
        "area": row.get("area"),
        "feature": row.get("feature"),
        "page": row.get("page"),
        "use": row.get("use"),
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "next_action": "Open the listed local page; no trading action is implied.",
    }


def _workbench_budget_ledger_row(row: Mapping[str, object]) -> dict[str, object]:
    return {
        "id": row.get("id"),
        "available_at": row.get("available_at"),
        "ticker": row.get("ticker"),
        "task": row.get("task"),
        "model": row.get("model"),
        "provider": row.get("provider"),
        "status": row.get("status"),
        "skip_reason": row.get("skip_reason"),
        "input_tokens": row.get("input_tokens"),
        "cached_input_tokens": row.get("cached_input_tokens"),
        "output_tokens": row.get("output_tokens"),
        "estimated_cost_usd": row.get("estimated_cost_usd"),
        "actual_cost_usd": row.get("actual_cost_usd"),
        "currency": row.get("currency"),
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "next_action": "Review budget evidence before expanding real agent calls.",
    }


def _workbench_value_economics_row(row: Mapping[str, object]) -> dict[str, object]:
    weighted_value = _number_or_zero(row.get("confidence_weighted_value_usd"))
    production_cost = _number_or_zero(row.get("cost_to_produce_usd"))
    return {
        "id": row.get("id"),
        "entry_date": row.get("entry_date"),
        "ticker": row.get("ticker"),
        "label": row.get("label"),
        "artifact_type": row.get("artifact_type"),
        "estimated_value_usd": row.get("estimated_value_usd"),
        "confidence": row.get("confidence"),
        "confidence_weighted_value_usd": row.get("confidence_weighted_value_usd"),
        "cost_to_produce_usd": row.get("cost_to_produce_usd"),
        "net_confidence_weighted_value_usd": round(
            weighted_value - production_cost,
            4,
        ),
        "provider_call_count": row.get("provider_call_count"),
        "llm_call_count": row.get("llm_call_count"),
        "outcome_status": row.get("outcome_status"),
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "next_action": "Compare value evidence against production cost.",
    }


def _workbench_ops_provider_row(row: Mapping[str, object]) -> dict[str, object]:
    return {
        "provider": row.get("provider"),
        "status": row.get("status"),
        "checked_at": row.get("checked_at"),
        "source": row.get("source"),
        "reason": row.get("reason"),
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "next_action": "Resolve provider status before relying on fresh signals.",
    }


def _workbench_ops_job_row(row: Mapping[str, object]) -> dict[str, object]:
    return {
        "id": row.get("id"),
        "job_type": row.get("job_type"),
        "status": row.get("status"),
        "started_at": row.get("started_at"),
        "finished_at": row.get("finished_at"),
        "requested_count": row.get("requested_count"),
        "raw_count": row.get("raw_count"),
        "normalized_count": row.get("normalized_count"),
        "error_summary": row.get("error_summary"),
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "next_action": "Review failed or stale jobs before expanding automation.",
    }


def _workbench_call_plan_row(row: Mapping[str, object]) -> dict[str, object]:
    return {
        "layer": row.get("layer"),
        "status": row.get("status"),
        "external_call_count_max": row.get("external_call_count_max"),
        "approval_required": bool(row.get("approval_required")),
        "guardrail": row.get("guardrail"),
        "next_action": row.get("next_action")
        or "Review call plan before executing provider work.",
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
    }


def _workbench_telemetry_event_row(row: Mapping[str, object]) -> dict[str, object]:
    return {
        "occurred_at": row.get("occurred_at"),
        "event": row.get("event"),
        "status": row.get("status"),
        "reason": row.get("reason"),
        "artifact": row.get("artifact"),
        "summary": row.get("summary"),
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "next_action": "Use as audit evidence; no trading action is implied.",
    }


def _workbench_telemetry_coverage_row(row: Mapping[str, object]) -> dict[str, object]:
    return {
        "domain": row.get("domain"),
        "status": row.get("status"),
        "required": bool(row.get("required")),
        "event_count": row.get("event_count"),
        "last_seen_at": row.get("last_seen_at"),
        "missing_events": _texts(row.get("missing_events")),
        "operator_action": row.get("operator_action"),
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "next_action": row.get("operator_action")
        or "Review telemetry coverage before relying on automation.",
    }


def _workbench_agent_capability_row(row: Mapping[str, object]) -> dict[str, object]:
    status = str(row.get("status") or "unknown").strip() or "unknown"
    level = str(row.get("level") or "").strip()
    name = str(row.get("name") or "capability").strip() or "capability"
    boundary_by_status = {
        "available": "read_only",
        "ready": "manual_approval_required",
        "blocked": "blocked",
        "disabled": "disabled",
        "out_of_scope": "out_of_scope",
    }
    boundary = boundary_by_status.get(status.lower(), "preview_only")
    return {
        "level": level or None,
        "name": name,
        "status": status,
        "description": row.get("description"),
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "boundary": boundary,
        "next_action": "Review manually; no autonomous execution is enabled.",
    }


def _workbench_agent_contribution_row(row: Mapping[str, object]) -> dict[str, object]:
    return {
        "agent": row.get("agent"),
        "role": row.get("role"),
        "summary": row.get("summary"),
        "confidence": row.get("confidence"),
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "next_action": "Review this agent contribution as decision support only.",
    }


def _workbench_agent_action_row(action: str, *, index: int) -> dict[str, object]:
    return {
        "rank": index,
        "action": action,
        "status": "manual_review",
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "agent_preview_command": "agent",
        "agent_execute_boundary_command": "agent execute",
        "primary_command": "agent",
        "next_action": action,
    }


def _workbench_agent_insight_row(insight: str, *, index: int) -> dict[str, object]:
    return {
        "rank": index,
        "insight": insight,
        "status": "decision_support",
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "next_action": "Use as context; it is not trade approval.",
    }


def _workbench_agent_security_check_row(row: Mapping[str, object]) -> dict[str, object]:
    return {
        "name": row.get("name"),
        "status": row.get("status") or "unknown",
        "detail": row.get("detail"),
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "next_action": "Resolve blocked checks before real agent execution.",
    }


def _workbench_active_risk_block_rows(
    risk: Mapping[str, object],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for scope, blocks, next_action in (
        (
            "paper_trade",
            _texts(risk.get("paper_trade_blocks")),
            "Resolve before supervised paper trade review.",
        ),
        (
            "live_submission",
            _texts(risk.get("live_submission_blocks")),
            "Live submission remains disabled by the platform boundary.",
        ),
        (
            "portfolio",
            _texts(risk.get("portfolio_hard_blocks")),
            "Resolve portfolio hard blocks before sizing a trade.",
        ),
    ):
        for block in blocks:
            rows.append(
                {
                    "source": f"active_plan.{scope}",
                    "scope": scope,
                    "code": block,
                    "status": "blocked",
                    "finding": block,
                    "boundary": "manual_review_required",
                    "external_calls_made": 0,
                    "db_writes_made": 0,
                    "broker_order_submitted": False,
                    "order_submission_allowed": False,
                    "next_action": next_action,
                }
            )
    return rows


def _workbench_queue_risk_block_row(row: Mapping[str, object]) -> dict[str, object]:
    portfolio = _mapping(row.get("portfolio_impact"))
    blocks = list(
        dict.fromkeys(
            [
                *_texts(row.get("blockers")),
                *_texts(portfolio.get("hard_blocks")),
                *_texts(row.get("portfolio_hard_blocks")),
            ]
        )
    )
    finding = ", ".join(blocks) if blocks else "blocked"
    return {
        "source": "priced_in_queue",
        "scope": "queue",
        "ticker": row.get("ticker") or row.get("symbol"),
        "code": blocks[0] if blocks else "blocked",
        "status": "blocked",
        "finding": finding,
        "boundary": "manual_review_required",
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "next_action": (
            row.get("next_action")
            or row.get("command")
            or "Resolve the queue hard block before planning risk."
        ),
    }


def _workbench_readiness_check_row(
    row: Mapping[str, object],
    *,
    source: str,
) -> dict[str, object]:
    return {
        "source": source,
        "code": row.get("code"),
        "area": row.get("area"),
        "status": row.get("status") or "unknown",
        "finding": row.get("finding"),
        "evidence": row.get("evidence"),
        "next_action": row.get("next_action") or row.get("next_command"),
        "external_calls_made": row.get("external_calls_made", 0),
        "db_writes_made": row.get("db_writes_made", 0),
        "broker_order_submitted": False,
        "order_submission_allowed": False,
    }


def _workbench_readiness_check_sort_key(
    row: Mapping[str, object],
) -> tuple[int, int, str]:
    status_rank = {
        "blocked": 0,
        "setup_required": 1,
        "warning": 2,
        "ready": 3,
    }
    source_rank = {
        "shadow_readiness": 0,
        "trial_readiness": 1,
    }
    return (
        status_rank.get(str(row.get("status") or "").lower(), 4),
        source_rank.get(str(row.get("source") or ""), 2),
        str(row.get("area") or row.get("code") or ""),
    )


def _workbench_order_ticket_row(row: Mapping[str, object]) -> dict[str, object]:
    preview = _mapping(row.get("preview"))
    return {
        "id": row.get("id"),
        "ticker": row.get("ticker"),
        "side": row.get("side"),
        "quantity": row.get("quantity"),
        "limit_price": row.get("limit_price"),
        "invalidation_price": row.get("invalidation_price"),
        "risk_budget": row.get("risk_budget"),
        "status": row.get("status"),
        "submission_allowed": bool(row.get("submission_allowed")),
        "created_at": row.get("created_at"),
        "hard_blocks": _texts(preview.get("hard_blocks")),
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": bool(row.get("submission_allowed")),
        "next_action": "Review manually; broker submission is disabled.",
    }


def _workbench_execution_audit_row(event: object) -> dict[str, object]:
    metadata = _mapping(getattr(event, "metadata", {}))
    after_payload = _mapping(getattr(event, "after_payload", {}))
    event_type = str(getattr(event, "event_type", "") or "")
    artifact_type = getattr(event, "artifact_type", None)
    artifact_id = getattr(event, "artifact_id", None)
    hard_blocks = _texts(getattr(event, "hard_blocks", [])) or _texts(
        metadata.get("hard_blocks")
    )
    decision = getattr(event, "decision", None)
    db_writes_made = _workbench_execution_audit_db_writes(
        event_type=event_type,
        decision=decision,
        hard_blocks=hard_blocks,
    )
    submission_allowed = bool(metadata.get("submission_allowed", False))
    broker_order_submitted = False
    return {
        "id": getattr(event, "id", None),
        "event_type": event_type,
        "actor_source": getattr(event, "actor_source", None),
        "actor_role": getattr(event, "actor_role", None),
        "artifact_type": artifact_type,
        "artifact_id": artifact_id,
        "ticker": getattr(event, "ticker", None),
        "decision": decision,
        "record_state": metadata.get("state") or metadata.get("status"),
        "status": getattr(event, "status", None),
        "reason": getattr(event, "reason", None),
        "hard_blocks": hard_blocks,
        "paper_trade_id": getattr(event, "paper_trade_id", None)
        or after_payload.get("paper_trade_id"),
        "order_ticket_id": (
            artifact_id if artifact_type == "order_ticket" else after_payload.get("id")
        ),
        "occurred_at": getattr(event, "occurred_at", None),
        "available_at": getattr(event, "available_at", None),
        "external_calls_made": 0,
        "db_writes_made": db_writes_made,
        "broker_order_submitted": broker_order_submitted,
        "order_submission_allowed": submission_allowed,
        "no_execution": not broker_order_submitted and not submission_allowed,
        "next_action": (
            "Review the local paper audit before any new decision."
            if event_type == PAPER_DECISION_RECORDED_EVENT
            else "Review the blocked local ticket; broker submission remains disabled."
        ),
    }


def _workbench_execution_audit_db_writes(
    *,
    event_type: str,
    decision: object,
    hard_blocks: Sequence[str],
) -> int:
    if event_type == PAPER_DECISION_RECORDED_EVENT:
        return (
            3
            if str(decision or "").lower() == "approved" and hard_blocks
            else 2
        )
    if event_type == ORDER_TICKET_PREVIEW_SAVED_EVENT:
        return 1
    return 0


def _workbench_portfolio_position_row(
    row: Mapping[str, object],
    *,
    portfolio_equity: object,
) -> dict[str, object]:
    market_value = _optional_float(row.get("market_value"))
    equity = _optional_float(portfolio_equity)
    return {
        "account_id": row.get("account_id"),
        "as_of": row.get("as_of"),
        "ticker": row.get("ticker"),
        "quantity": row.get("quantity"),
        "average_price": row.get("average_price"),
        "market_value": market_value,
        "unrealized_pnl": row.get("unrealized_pnl"),
        "sector": row.get("sector"),
        "theme": row.get("theme"),
        "exposure_pct": (
            round(market_value / equity, 4)
            if market_value is not None and equity not in (None, 0)
            else None
        ),
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "portfolio_review_command": "portfolio",
        "risk_review_command": "risk-desk",
        "primary_command": "risk-desk",
        "next_action": "Use as read-only portfolio context; order submission is disabled.",
    }


def _workbench_portfolio_balance_row(row: Mapping[str, object]) -> dict[str, object]:
    return {
        "account_id": row.get("account_id"),
        "display_name": row.get("display_name"),
        "as_of": row.get("as_of"),
        "cash": row.get("cash"),
        "buying_power": row.get("buying_power"),
        "liquidation_value": row.get("liquidation_value"),
        "equity": row.get("equity"),
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "portfolio_review_command": "portfolio",
        "risk_review_command": "risk-desk",
        "primary_command": "portfolio",
        "next_action": "Use balance context for sizing only; broker submission is disabled.",
    }


def _workbench_portfolio_exposure_rows(
    exposure: Mapping[str, object],
) -> list[dict[str, object]]:
    exposure_before = _mapping(exposure.get("exposure_before"))
    single_name = _mapping(exposure_before.get("single_name"))
    gross_exposure = exposure_before.get("gross_exposure_pct")
    snapshot_as_of = exposure.get("snapshot_as_of")
    stale = bool(exposure.get("broker_data_stale"))
    status = (
        "missing"
        if gross_exposure is None and not snapshot_as_of
        else "stale"
        if stale
        else "ready"
    )
    rows = [
        {
            "scope": "portfolio",
            "metric": "gross_exposure_pct",
            "value": gross_exposure,
            "status": status,
            "snapshot_as_of": snapshot_as_of,
            "broker_data_stale": stale,
            "external_calls_made": 0,
            "db_writes_made": 0,
            "broker_order_submitted": False,
            "order_submission_allowed": False,
            "portfolio_review_command": "portfolio",
            "risk_review_command": "risk-desk",
            "primary_command": "risk-desk",
            "next_action": "Review concentration before sizing any new plan.",
        }
    ]
    rows.extend(
        {
            "scope": "single_name",
            "metric": ticker,
            "value": value,
            "status": status,
            "snapshot_as_of": snapshot_as_of,
            "broker_data_stale": stale,
            "external_calls_made": 0,
            "db_writes_made": 0,
            "broker_order_submitted": False,
            "order_submission_allowed": False,
            "portfolio_review_command": "portfolio",
            "risk_review_command": "risk-desk",
            "primary_command": "risk-desk",
            "next_action": "Compare against single-name limits before paper review.",
        }
        for ticker, value in sorted(single_name.items())
    )
    return rows


def _workbench_portfolio_open_order_rows(
    rows: Sequence[Mapping[str, object]],
) -> list[dict[str, object]]:
    if not rows:
        return [
            {
                "id": None,
                "account_id": None,
                "ticker": None,
                "side": None,
                "order_type": None,
                "quantity": 0,
                "limit_price": None,
                "status": "none",
                "submitted_at": None,
                "external_calls_made": 0,
                "db_writes_made": 0,
                "broker_order_submitted": False,
                "order_submission_allowed": False,
                "portfolio_review_command": "portfolio",
                "broker_review_command": "broker",
                "primary_command": "broker",
                "next_action": "No open broker orders in the read-only snapshot.",
            }
        ]
    return [_workbench_portfolio_open_order_row(row) for row in rows]


def _workbench_portfolio_open_order_row(
    row: Mapping[str, object],
) -> dict[str, object]:
    return {
        "id": row.get("id"),
        "account_id": row.get("account_id"),
        "ticker": row.get("ticker"),
        "side": row.get("side"),
        "order_type": row.get("order_type"),
        "quantity": row.get("quantity"),
        "limit_price": row.get("limit_price"),
        "status": row.get("status"),
        "submitted_at": row.get("submitted_at"),
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "portfolio_review_command": "portfolio",
        "broker_review_command": "broker",
        "primary_command": "broker",
        "next_action": "Read-only open-order context; no broker order is submitted.",
    }


def _workbench_paper_trade_row(row: Mapping[str, object]) -> dict[str, object]:
    payload = _mapping(row.get("payload"))
    return {
        "id": row.get("id"),
        "decision_card_id": row.get("decision_card_id"),
        "ticker": row.get("ticker"),
        "decision": row.get("decision"),
        "state": row.get("state"),
        "entry_price": row.get("entry_price"),
        "entry_at": row.get("entry_at"),
        "invalidation_price": row.get("invalidation_price"),
        "shares": row.get("shares"),
        "notional": row.get("notional"),
        "max_loss": row.get("max_loss"),
        "available_at": row.get("available_at"),
        "no_execution": bool(payload.get("no_execution", True)),
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "next_action": "Track outcome locally; no broker order was submitted.",
    }


def _workbench_value_ledger_entry_row(row: Mapping[str, object]) -> dict[str, object]:
    return {
        "id": row.get("id"),
        "entry_date": row.get("entry_date"),
        "ticker": row.get("ticker"),
        "label": row.get("label"),
        "artifact_type": row.get("artifact_type"),
        "artifact_id": row.get("artifact_id"),
        "supported_action": row.get("supported_action"),
        "user_decision": row.get("user_decision"),
        "estimated_value_usd": row.get("estimated_value_usd"),
        "confidence": row.get("confidence"),
        "confidence_weighted_value_usd": row.get(
            "confidence_weighted_value_usd"
        ),
        "outcome_status": row.get("outcome_status"),
        "source": row.get("source"),
        "next_action": "Review outcome evidence or update the local journal.",
    }


def _workbench_value_outcome_row(row: Mapping[str, object]) -> dict[str, object]:
    return {
        "id": row.get("id"),
        "value_ledger_entry_id": row.get("value_ledger_entry_id"),
        "ticker": row.get("ticker"),
        "status": row.get("status"),
        "trading_days_observed": row.get("trading_days_observed"),
        "entry_price": row.get("entry_price"),
        "return_20d": row.get("return_20d"),
        "spy_relative_return_20d": row.get("spy_relative_return_20d"),
        "max_adverse_excursion": row.get("max_adverse_excursion"),
        "max_favorable_excursion": row.get("max_favorable_excursion"),
        "invalidation_touched": bool(row.get("invalidation_touched")),
        "outcome_available_at": row.get("outcome_available_at"),
        "next_action": "Compare realized outcome with the original decision.",
    }


def _workbench_trade_lifecycle_rows(
    *,
    active_plan: Mapping[str, object],
    paper_trade_rows: Sequence[Mapping[str, object]],
    paper_execution_audit_rows: Sequence[Mapping[str, object]],
    validation_result_rows: Sequence[Mapping[str, object]],
    value_ledger_rows: Sequence[Mapping[str, object]],
    value_outcome_rows: Sequence[Mapping[str, object]],
) -> list[dict[str, object]]:
    rows: dict[str, dict[str, object]] = {}
    paper_trade_to_card: dict[str, str] = {}

    def ensure(
        *,
        decision_card_id: object = None,
        ticker: object = None,
        paper_trade_id: object = None,
        ledger_entry_id: object = None,
    ) -> dict[str, object]:
        card_text = str(decision_card_id or "").strip()
        trade_text = str(paper_trade_id or "").strip()
        ledger_text = str(ledger_entry_id or "").strip()
        ticker_text = str(ticker or "").strip().upper()
        key = card_text or trade_text or ledger_text or ticker_text or "unlinked"
        if key not in rows:
            rows[key] = {
                "id": f"trade-lifecycle:{key}",
                "ticker": ticker_text or None,
                "decision_card_id": card_text or None,
                "plan_status": None,
                "recommended_paper_decision": None,
                "paper_trade_id": trade_text or None,
                "paper_decision": None,
                "paper_state": None,
                "audit_event_id": None,
                "validation_result_id": None,
                "ledger_entry_id": ledger_text or None,
                "outcome_id": None,
                "outcome_status": None,
                "return_20d": None,
                "spy_relative_return_20d": None,
                "current_stage": "unlinked",
                "external_calls_made": 0,
                "db_writes_made": 0,
                "broker_order_submitted": False,
                "order_submission_allowed": False,
                "no_execution": True,
                "ledger_show_command": None,
                "outcome_show_command": None,
                "outcome_preview_command": None,
                "outcome_update_command": None,
                "primary_command": "ledger coverage",
                "next_action": "Link this lifecycle row to local validation evidence.",
            }
        row = rows[key]
        if ticker_text and not row.get("ticker"):
            row["ticker"] = ticker_text
        if card_text and not row.get("decision_card_id"):
            row["decision_card_id"] = card_text
        if trade_text and not row.get("paper_trade_id"):
            row["paper_trade_id"] = trade_text
        if ledger_text and not row.get("ledger_entry_id"):
            row["ledger_entry_id"] = ledger_text
        return row

    if active_plan and active_plan.get("status") != "missing":
        row = ensure(
            decision_card_id=active_plan.get("decision_card_id"),
            ticker=active_plan.get("ticker"),
        )
        row["plan_status"] = active_plan.get("status")
        row["recommended_paper_decision"] = active_plan.get(
            "recommended_paper_decision"
        )

    for trade in paper_trade_rows:
        row = ensure(
            decision_card_id=trade.get("decision_card_id"),
            ticker=trade.get("ticker"),
            paper_trade_id=trade.get("id"),
        )
        row["paper_trade_id"] = row.get("paper_trade_id") or trade.get("id")
        row["paper_decision"] = row.get("paper_decision") or trade.get("decision")
        row["paper_state"] = row.get("paper_state") or trade.get("state")
        card_id = str(trade.get("decision_card_id") or "").strip()
        trade_id = str(trade.get("id") or "").strip()
        if card_id and trade_id:
            paper_trade_to_card[trade_id] = card_id

    for audit in paper_execution_audit_rows:
        card_id = (
            audit.get("artifact_id")
            if audit.get("artifact_type") == "decision_card"
            else None
        )
        paper_trade_id = str(audit.get("paper_trade_id") or "").strip()
        row = ensure(
            decision_card_id=card_id or paper_trade_to_card.get(paper_trade_id),
            ticker=audit.get("ticker"),
            paper_trade_id=paper_trade_id,
        )
        row["audit_event_id"] = audit.get("id")
        row["db_writes_made"] = max(
            int(_number_or_zero(row.get("db_writes_made"))),
            int(_number_or_zero(audit.get("db_writes_made"))),
        )

    for result in validation_result_rows:
        row = ensure(
            decision_card_id=result.get("decision_card_id"),
            ticker=result.get("ticker"),
        )
        row["validation_result_id"] = result.get("id")

    outcomes_by_ledger_id = {
        str(outcome.get("value_ledger_entry_id") or "").strip(): outcome
        for outcome in value_outcome_rows
        if str(outcome.get("value_ledger_entry_id") or "").strip()
    }
    consumed_outcome_ids: set[str] = set()
    for entry in value_ledger_rows:
        paper_trade_id = (
            entry.get("artifact_id")
            if entry.get("artifact_type") == "paper_trade"
            else None
        )
        row = ensure(
            decision_card_id=entry.get("decision_card_id")
            or paper_trade_to_card.get(str(paper_trade_id or "").strip()),
            ticker=entry.get("ticker"),
            paper_trade_id=paper_trade_id,
            ledger_entry_id=entry.get("id"),
        )
        row["ledger_entry_id"] = entry.get("id")
        row["paper_decision"] = row.get("paper_decision") or entry.get(
            "user_decision"
        )
        ledger_id = str(entry.get("id") or "").strip()
        outcome = outcomes_by_ledger_id.get(ledger_id)
        if outcome:
            _workbench_apply_lifecycle_outcome(row, outcome)
            consumed_outcome_ids.add(str(outcome.get("id") or ""))

    for outcome in value_outcome_rows:
        outcome_id = str(outcome.get("id") or "")
        if outcome_id in consumed_outcome_ids:
            continue
        row = ensure(
            ticker=outcome.get("ticker"),
            ledger_entry_id=outcome.get("value_ledger_entry_id"),
        )
        _workbench_apply_lifecycle_outcome(row, outcome)

    for row in rows.values():
        row["current_stage"] = _workbench_lifecycle_stage(row)
        row["next_action"] = _workbench_lifecycle_next_action(row)
        _workbench_apply_lifecycle_commands(row)

    return sorted(
        rows.values(),
        key=lambda row: (
            str(row.get("ticker") or ""),
            str(row.get("decision_card_id") or ""),
            str(row.get("paper_trade_id") or ""),
            str(row.get("ledger_entry_id") or ""),
        ),
    )[:5]


def _workbench_apply_lifecycle_outcome(
    row: dict[str, object],
    outcome: Mapping[str, object],
) -> None:
    row["outcome_id"] = outcome.get("id")
    row["outcome_status"] = outcome.get("status")
    row["return_20d"] = outcome.get("return_20d")
    row["spy_relative_return_20d"] = outcome.get("spy_relative_return_20d")


def _workbench_lifecycle_stage(row: Mapping[str, object]) -> str:
    if row.get("outcome_id"):
        return (
            "outcome_computed"
            if str(row.get("outcome_status") or "").lower() == "computed"
            else "outcome_pending"
        )
    if row.get("ledger_entry_id"):
        return "journaled"
    if row.get("validation_result_id") and row.get("paper_trade_id"):
        return "validated_paper_trade"
    if row.get("validation_result_id"):
        return "validated"
    if row.get("audit_event_id"):
        return "paper_decision_recorded"
    if row.get("paper_trade_id"):
        state = str(row.get("paper_state") or "recorded").strip().lower()
        return f"paper_trade_{state or 'recorded'}"
    if row.get("plan_status"):
        return "planned"
    return "unlinked"


def _workbench_lifecycle_next_action(row: Mapping[str, object]) -> str:
    stage = str(row.get("current_stage") or "")
    if stage == "outcome_computed":
        return "Compare realized outcome with the original paper decision."
    if stage == "outcome_pending":
        return "Review pending outcome before changing strategy logic."
    if stage == "journaled":
        return "Compute or review the linked value outcome."
    if stage == "validated_paper_trade":
        return "Record journal evidence or compute the trade outcome."
    if stage == "validated":
        return "Link validation evidence to a local paper decision."
    if stage == "paper_decision_recorded":
        return "Link the paper decision to value journal evidence."
    if stage.startswith("paper_trade_"):
        return "Track the local paper trade and record outcome evidence."
    if stage == "planned":
        return "Record a local paper decision only after approval gates."
    return "Link this lifecycle row to local validation evidence."


def _workbench_apply_lifecycle_commands(row: dict[str, object]) -> None:
    ledger_id = str(row.get("ledger_entry_id") or "").strip()
    outcome_id = str(row.get("outcome_id") or "").strip()
    ledger_arg = shlex.quote(ledger_id) if ledger_id else ""
    outcome_arg = shlex.quote(outcome_id) if outcome_id else ""
    row["ledger_show_command"] = f"ledger show {ledger_arg}" if ledger_arg else None
    row["outcome_show_command"] = (
        f"outcome show {outcome_arg}" if outcome_arg else None
    )
    row["outcome_preview_command"] = (
        f"outcome update {ledger_arg} filter" if ledger_arg else None
    )
    row["outcome_update_command"] = (
        f"outcome update {ledger_arg} filter --execute" if ledger_arg else None
    )
    row["primary_command"] = (
        row.get("outcome_show_command")
        or row.get("outcome_preview_command")
        or row.get("ledger_show_command")
        or "ledger coverage"
    )


def _workbench_useful_label_row(row: Mapping[str, object]) -> dict[str, object]:
    return {
        "id": row.get("id"),
        "artifact_type": row.get("artifact_type"),
        "artifact_id": row.get("artifact_id"),
        "ticker": row.get("ticker"),
        "label": row.get("label"),
        "notes": row.get("notes"),
        "created_at": row.get("created_at"),
        "external_calls_made": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "next_action": "Use as validation evidence, not as trade approval.",
    }


def _workbench_validation_result_rows(
    *,
    engine: Engine,
    latest_validation: Mapping[str, object],
    available_at: datetime | None,
) -> list[dict[str, object]]:
    run_id = str(latest_validation.get("id") or "").strip()
    if not run_id:
        return []
    results = ValidationRepository(engine).list_validation_results(
        run_id,
        available_at=available_at,
    )
    rows: list[dict[str, object]] = []
    for result in results[:5]:
        labels = _mapping(result.labels)
        positive_labels = [
            str(key)
            for key, value in labels.items()
            if value is True and str(key).strip()
        ]
        rows.append(
            {
                "id": result.id,
                "run_id": result.run_id,
                "ticker": result.ticker,
                "as_of": result.as_of.isoformat(),
                "available_at": result.available_at.isoformat(),
                "state": result.state.value,
                "final_score": result.final_score,
                "decision_card_id": result.decision_card_id,
                "baseline": result.baseline,
                "positive_labels": positive_labels,
                "leakage_flags": list(result.leakage_flags),
                "next_action": "Review replay result before changing strategy logic.",
            }
        )
    return rows


def _dashboard_real_results_payload(
    *,
    latest_run: Mapping[str, object] | None,
    priced_in_queue: Mapping[str, object],
    candidate_rows: Sequence[Mapping[str, object]],
    discovery_snapshot: Mapping[str, object],
) -> dict[str, object]:
    latest = _mapping(latest_run)
    source_modes = _dashboard_real_results_source_modes(discovery_snapshot)
    latest_status = str(latest.get("status") or "").strip().lower()
    row_count = int(
        _number_or_zero(
            priced_in_queue.get("total_count")
            or priced_in_queue.get("returned_count")
            or priced_in_queue.get("count")
            or len(candidate_rows)
        )
    )
    missing: list[str] = []
    if not latest:
        missing.append("latest radar run")
    elif latest_status not in {"success", "completed"}:
        missing.append("successful latest radar run")
    if row_count <= 0:
        missing.append("priced-in scan rows")
    source_missing = _dashboard_real_results_source_blockers(source_modes)
    missing.extend(source_missing)

    ready = not missing
    as_of = latest.get("as_of")
    cutoff = latest.get("decision_available_at") or latest.get("finished_at")
    return {
        "schema_version": "dashboard-real-results-v1",
        "status": "ready" if ready else "missing",
        "headline": (
            f"Real scan context ready: {row_count} priced-in row(s)."
            if ready
            else "No real result yet."
        ),
        "next_action": (
            "Review candidates, then run agent-brief --real --execute if desired."
            if ready
            else (
                "Run/import real market data, then run "
                "`catalyst-radar priced-in-answer --limit 50`."
            )
        ),
        "source": "local_database_provider_backed_scan" if ready else "none",
        "row_count": row_count,
        "latest_run_id": latest.get("id"),
        "latest_run_status": latest.get("status"),
        "as_of": as_of.isoformat() if hasattr(as_of, "isoformat") else as_of,
        "cutoff": cutoff.isoformat() if hasattr(cutoff, "isoformat") else cutoff,
        "missing": missing,
        "canned_data_allowed": False,
        "canned_data_detected": _dashboard_real_results_canned_source_detected(
            source_modes
        ),
        "source_modes": source_modes,
        "runtime_contract": (
            "Normal product paths require live market and live catalyst-event "
            "source modes. Fixture, demo, sample, and csv rows are allowed only "
            "in tests or explicit development commands."
        ),
    }


def _dashboard_real_results_source_modes(
    discovery_snapshot: Mapping[str, object],
) -> dict[str, str]:
    modes = _mapping(_mapping(discovery_snapshot).get("source_modes"))
    return {
        "market": str(modes.get("market") or "unknown"),
        "market_provider": str(modes.get("market_provider") or "unknown"),
        "events": str(modes.get("events") or "unknown"),
        "event_provider": str(modes.get("event_provider") or "unknown"),
    }


def _dashboard_real_results_source_blockers(
    source_modes: Mapping[str, object],
) -> list[str]:
    missing: list[str] = []
    market_mode = str(source_modes.get("market") or "").strip().lower()
    event_mode = str(source_modes.get("events") or "").strip().lower()
    if market_mode != "live":
        missing.append("live market data source")
    if event_mode != "live":
        missing.append("live catalyst event source")
    return missing


def _dashboard_real_results_canned_source_detected(
    source_modes: Mapping[str, object],
) -> bool:
    markers = {"fixture", "sample", "demo", "csv", "news_fixture"}
    return any(
        str(source_modes.get(key) or "").strip().lower() in markers
        for key in ("market", "market_provider", "events", "event_provider")
    )


def _dashboard_top_level_blocker_contract(
    *,
    shadow_readiness: Mapping[str, object],
    priced_in_answer: Mapping[str, object],
    operator_next_step: Mapping[str, object],
) -> dict[str, object]:
    first_blocker = _first_nonblank(
        shadow_readiness.get("first_blocker"),
        priced_in_answer.get("first_blocker"),
        operator_next_step.get("first_blocker"),
    )
    action = _first_nonblank(
        shadow_readiness.get("canonical_next_action"),
        shadow_readiness.get("next_action"),
        priced_in_answer.get("canonical_next_action"),
        priced_in_answer.get("next_action"),
        operator_next_step.get("action"),
    )
    command = _first_nonblank(
        shadow_readiness.get("canonical_next_command"),
        shadow_readiness.get("next_command"),
        priced_in_answer.get("canonical_next_command"),
        priced_in_answer.get("next_command"),
        operator_next_step.get("command"),
    )
    return {
        "status": _first_nonblank(
            shadow_readiness.get("status"),
            priced_in_answer.get("status"),
            operator_next_step.get("status"),
        )
        or "unknown",
        "first_blocker": first_blocker,
        "first_gap_count": _first_nonnegative_int(
            shadow_readiness.get("first_gap_count"),
            priced_in_answer.get("first_gap_count"),
            operator_next_step.get("first_gap_count"),
        ),
        "canonical_next_action": action,
        "canonical_next_command": command,
    }


def _first_nonblank(*values: object) -> object | None:
    for value in values:
        if isinstance(value, str):
            text = value.strip()
            if text:
                return text
        elif value is not None:
            return value
    return None


def _first_nonnegative_int(*values: object) -> int:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        number = int(_number_or_zero(value))
        if number >= 0:
            return number
    return 0


def _dashboard_approval_required_unblock(
    *,
    shadow_readiness: Mapping[str, object],
    trial_readiness: Mapping[str, object],
) -> dict[str, object] | None:
    shadow_approval = shadow_readiness.get("approval_required_unblock")
    if isinstance(shadow_approval, Mapping) and shadow_approval:
        return dict(shadow_approval)
    minimum_product = trial_readiness.get("minimum_useful_product")
    if isinstance(minimum_product, Mapping):
        trial_approval = minimum_product.get("approval_required_unblock")
        if isinstance(trial_approval, Mapping) and trial_approval:
            return dict(trial_approval)
    return None


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
        height: 8;
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
        height: 4;
        border: round #26384d;
        background: #0b141d;
        padding: 0 1;
        margin-top: 0;
    }

    #operator-row {
        layout: grid;
        grid-size: 2 1;
        grid-gutter: 0 1;
        height: 5;
        margin-top: 0;
    }

    #operator-action, #operator-response {
        height: 5;
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

    #shortcut-footer {
        height: 1;
        background: #05080c;
        color: #aeb8c6;
        padding: 0 1;
    }
    """

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("r", "refresh", "Refresh"),
        Binding("0", "go('tutorial')", "Tutorial", priority=True),
        Binding("1", "go('overview')", "Inbox", priority=True),
        Binding("2", "go('readiness')", "Evidence Gaps", priority=True),
        Binding("3", "go('run')", "Safe Run", priority=True),
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
        self._snapshot_generation = 0
        self._snapshot_worker: Worker[
            tuple[int, str, dict[str, object]]
        ] | None = None

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
                yield FocusRow(
                    "SETUP First setup step",
                    id="action-setup",
                    classes="side-action",
                )
                yield FocusRow("R  Refresh snapshot", id="action-refresh", classes="side-action")
                yield FocusRow("PLAN Safe run review", id="action-run-page", classes="side-action")
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
                yield FocusRow("ALL Scan rows", id="action-scan-all", classes="side-action")
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
                        "Type a command or click a message. Try: setup, inbox, ready, full, "
                        "mismatches, 2, 4, run, refresh, help, q"
                    ),
                    id="command",
                )
        yield Static(id="shortcut-footer")

    def on_mount(self) -> None:
        self.status_message = "Loading local dashboard snapshot..."
        self.refresh_view()
        self._focus_initial_widget()
        self._start_snapshot_reload(
            loading_message="Loading local dashboard snapshot...",
            success_message="Snapshot loaded from the local database.",
            clear_payload=True,
        )

    def _focus_initial_widget(self) -> None:
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
            fast_view=True,
        )

    def _load_snapshot_payload(
        self,
        generation: int,
        filters: DashboardFilters,
        success_message: str,
    ) -> tuple[int, str, dict[str, object]]:
        return (
            generation,
            success_message,
            dashboard_snapshot_payload(
                engine=self.engine,
                config=self.config,
                dotenv_loaded=self.dotenv_loaded,
                filters=filters,
                fast_view=True,
            ),
        )

    def _start_snapshot_reload(
        self,
        *,
        loading_message: str,
        success_message: str,
        clear_payload: bool = False,
    ) -> None:
        self._snapshot_generation += 1
        generation = self._snapshot_generation
        filters = self.filters
        if clear_payload:
            self.payload = {}
        self.status_message = loading_message
        self.refresh_view()
        self._snapshot_worker = self.run_worker(
            lambda: self._load_snapshot_payload(
                generation,
                filters,
                success_message,
            ),
            name="dashboard-snapshot",
            group="dashboard-snapshot",
            description="Load the local dashboard snapshot",
            exit_on_error=False,
            exclusive=True,
            thread=True,
        )

    def on_worker_state_changed(self, event: Worker.StateChanged) -> None:
        if event.worker is not self._snapshot_worker:
            return
        if event.state == WorkerState.SUCCESS:
            generation, success_message, payload = event.worker.result or (0, "", {})
            if generation != self._snapshot_generation:
                return
            self.payload = payload
            self.status_message = success_message
            self.refresh_view()
            return
        if event.state == WorkerState.ERROR:
            error = event.worker.error
            detail = str(error) if error else "unknown error"
            self.status_message = f"Snapshot load failed: {detail}"
            self.refresh_view()

    def refresh_view(self) -> None:
        self._refresh_nav()
        self._refresh_setup_action()
        self._refresh_scan_actions()
        self._refresh_header()
        self._refresh_table()
        self.query_one("#nav-helpbar", Static).update(self._navigation_text())
        self.query_one("#guide", Static).update(self._guide_text())
        self.query_one("#operator-action", Static).update(self._action_text())
        self.query_one("#operator-response", Static).update(self._response_text())
        self.query_one("#command", Input).placeholder = self._command_placeholder()
        self.query_one("#shortcut-footer", Static).update(self._shortcut_footer_text())

    def action_refresh(self) -> None:
        self._start_snapshot_reload(
            loading_message="Refreshing local dashboard snapshot...",
            success_message="Snapshot refreshed from the local database.",
        )

    def action_go(self, page: str) -> None:
        old_filters = self.filters
        self.page = _normalize_page(page)
        self.filters = dashboard_filters_for_page(self.filters, self.page)
        self.status_message = _page_navigation_status_message(self.page, self.payload)
        if self.filters != old_filters:
            self._start_snapshot_reload(
                loading_message=(
                    f"{self.status_message} Refreshing local snapshot only."
                ),
                success_message=self.status_message,
                clear_payload=True,
            )
        else:
            self.refresh_view()

    def on_click(self, event: events.Click) -> None:
        widget_id = event.widget.id if event.widget else ""
        if widget_id.startswith("nav-"):
            event.stop()
            self.action_go(widget_id.removeprefix("nav-"))
            return
        if widget_id == "action-setup":
            event.stop()
            self.action_show_setup()
            return
        if widget_id == "action-refresh":
            event.stop()
            self.action_refresh()
            return
        if widget_id == "action-run-page":
            event.stop()
            self.action_go("run")
            self.status_message = (
                "Follow NEXT SAFE ACTION. No calls. 0 provider call(s), "
                "0 DB write(s). Safe Run opened."
            )
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
        if focused_id == "action-setup":
            event.stop()
            self.action_show_setup()
            return
        if focused_id == "action-refresh":
            event.stop()
            self.action_refresh()
            return
        if focused_id == "action-run-page":
            event.stop()
            self.action_go("run")
            self.status_message = (
                "Follow NEXT SAFE ACTION. No calls. 0 provider call(s), "
                "0 DB write(s). Safe Run opened."
            )
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
        self._start_snapshot_reload(
            loading_message=self.status_message,
            success_message=self.status_message,
            clear_payload=True,
        )

    def action_show_setup(self) -> None:
        old_filters = self.filters
        self.page = "readiness"
        self.filters = dashboard_filters_for_page(self.filters, self.page)
        self.status_message = _setup_command_status_message(self.payload)
        if self.filters != old_filters:
            self._start_snapshot_reload(
                loading_message=self.status_message,
                success_message=self.status_message,
                clear_payload=True,
            )
            return
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
            _all_scan_rows_mode_message(self.payload)
            if resolved == "all"
            else "Mismatches mode: showing only bullish/bearish not-priced-in rows."
        )
        self._start_snapshot_reload(
            loading_message=self.status_message,
            success_message=self.status_message,
            clear_payload=True,
        )

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
        old_filters = self.filters
        self.page = update.page
        self.filters = update.filters
        self.status_message = update.message
        command = raw.partition(" ")[0].strip().lower()
        should_reload = (
            not raw
            or command in _SNAPSHOT_RELOAD_COMMANDS
            or self.filters != old_filters
        )
        if should_reload:
            self._start_snapshot_reload(
                loading_message=update.message or "Refreshing local dashboard snapshot...",
                success_message=update.message or "Snapshot refreshed from the local database.",
                clear_payload=self.filters != old_filters,
            )
        else:
            self.refresh_view()

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        if self.page == "tutorial":
            row = self._row_by_key(event.row_key.value)
            if row:
                target_page = str(row.get("_target_page") or "").strip()
                self.status_message = _tutorial_row_status_message(
                    row,
                    target_page=target_page,
                )
                if target_page:
                    old_filters = self.filters
                    self.page = target_page
                    self.filters = dashboard_filters_for_page(self.filters, self.page)
                    if self.filters != old_filters:
                        self._start_snapshot_reload(
                            loading_message=self.status_message,
                            success_message=self.status_message,
                            clear_payload=True,
                        )
                        return
                self.refresh_view()
        elif self.page in {"overview", "review"}:
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
                self.status_message = (
                    f"Opened candidate {ticker}. No calls. Review evidence before action."
                )
                self.refresh_view()
        elif self.page == "alerts":
            row = self._row_by_key(event.row_key.value)
            alert_id = str(row.get("id") or "")
            if alert_id:
                self.page = f"alert:{alert_id}"
                self.status_message = _alert_open_status_message(row, alert_id)
                self.refresh_view()
        elif self.page.startswith("candidate:"):
            row = self._row_by_key(event.row_key.value)
            if row:
                self.status_message = _detail_row_status_message(
                    "Candidate detail",
                    row,
                )
                self.refresh_view()
        elif self.page.startswith("alert:"):
            row = self._row_by_key(event.row_key.value)
            if row:
                self.status_message = _detail_row_status_message(
                    "Alert detail",
                    row,
                )
                self.refresh_view()
        elif self.page == "readiness":
            row = self._row_by_key(event.row_key.value)
            if row:
                self.status_message = _readiness_row_status_message(row)
                self.refresh_view()
        elif self.page == "run":
            row = self._row_by_key(event.row_key.value)
            if row:
                self.status_message = _run_row_status_message(row)
                self.refresh_view()
        elif self.page == "agent":
            row = self._row_by_key(event.row_key.value)
            if row:
                self.status_message = _agent_row_status_message(row)
                self.refresh_view()
        elif self.page == "ipo":
            row = self._row_by_key(event.row_key.value)
            if row:
                self.status_message = _ipo_row_status_message(row)
                self.refresh_view()
        elif self.page == "broker":
            row = self._row_by_key(event.row_key.value)
            if row:
                self.status_message = _broker_row_status_message(row)
                self.refresh_view()
        elif self.page == "telemetry":
            row = self._row_by_key(event.row_key.value)
            if row:
                self.status_message = _telemetry_row_status_message(row)
                self.refresh_view()
        elif self.page == "themes":
            row = self._row_by_key(event.row_key.value)
            if row:
                self.status_message = _theme_row_status_message(row)
                self.refresh_view()
        elif self.page in {"validation", "costs"}:
            row = self._row_by_key(event.row_key.value)
            if row:
                detail_kind = "Validation" if self.page == "validation" else "Costs"
                self.status_message = _detail_row_status_message(
                    detail_kind,
                    row,
                )
                self.refresh_view()
        elif self.page == "features":
            row = self._row_by_key(event.row_key.value)
            if row:
                target_page = _feature_row_target_page(row)
                self.status_message = _feature_row_status_message(row)
                if target_page:
                    self.page = target_page
                self.refresh_view()
        elif self.page == "help":
            row = self._row_by_key(event.row_key.value)
            if row:
                self.status_message = _help_row_status_message(row)
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
                self._start_snapshot_reload(
                    loading_message=self.status_message,
                    success_message=self.status_message,
                )

    def _row_by_key(self, key: object) -> Mapping[str, object]:
        key_text = str(key)
        for index, row in enumerate(self._current_rows(), start=1):
            if str(row.get("_row_key") or "") == key_text:
                return row
            generated = str(row.get("ticker") or row.get("id") or index)
            if generated == key_text:
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
        all_rows_label = (
            "ALL Scanned rows"
            if _priced_in_scan_scope_is_partial(self.payload)
            else "ALL Full scan rows"
        )
        full.update((">> " if status == "all" else "   ") + all_rows_label)

    def _nav_label(self, page_key: str, shortcut: str, label: str) -> str:
        active = self._active_nav_page() == page_key
        marker = ">>" if active else "  "
        counts = self._nav_count_suffix(page_key)
        return f"{marker} {shortcut:<2} {label}{counts}"

    def _shortcut_footer_text(self) -> str:
        return (
            "[bold #f0a500]q[/] Quit  "
            "[bold #f0a500]r[/] Refresh  "
            "[bold #f0a500]0[/] Start  "
            "[bold #f0a500]1[/] Inbox  "
            "[bold #f0a500]2[/] Gaps  "
            "[bold #f0a500]3[/] Run  "
            "[bold #f0a500]4[/] Review  "
            "[bold #f0a500]5[/] Alerts  "
            "[bold #f0a500]6[/] IPO  "
            "[bold #f0a500]7[/] Broker  "
            "[bold #f0a500]8[/] Ops  "
            "[bold #f0a500]9[/] Log  "
            "[bold #f0a500]^A[/] Agent  "
            "[bold #f0a500]^N/^P[/] Page  "
            "[bold #f0a500]?[/] Help"
        )

    def _refresh_setup_action(self) -> None:
        setup = self.query_one("#action-setup", FocusRow)
        active = _real_results_empty(self.payload)
        setup.set_class(active, "active")
        label = "SETUP First setup step" if active else "SETUP Setup status"
        setup.update((">> " if active else "   ") + label)

    def _command_placeholder(self) -> str:
        if _real_results_empty(self.payload):
            return (
                "No scan yet. Try: setup, 2 Evidence Gaps, 3 Safe Run, "
                "refresh, help, q"
            )
        page_parts = self.page.split(":", 1)
        page = page_parts[0]
        page_ref = page_parts[1] if len(page_parts) > 1 else ""
        ticker = page_ref.upper() if page_ref else "TICKER"
        if page == "tutorial":
            return "Tutorial. Press 1 for Inbox, 2 for Gaps, 3 for Run, or q to quit."
        if page == "overview":
            return "Inbox. Try: open 1, ready, full, mismatches, next, prev, 2, 3, help, q"
        if page == "readiness":
            first_gap = _readiness_first_work_item(self.payload)
            action = str(
                first_gap.get("next_action") or first_gap.get("action") or ""
            ).strip()
            if _first_catalyst_radar_command(action):
                return (
                    "Evidence Gaps. Do not paste PowerShell here. Run the "
                    "first-blocker command outside dashboard; refresh when done. "
                    "3, inbox, help, q"
                )
            return "Evidence Gaps. Try: batch SOURCE, bars manual import, 3, refresh, help, q"
        if page == "run":
            step = _priced_in_operator_step(self.payload)
            command = (
                str(step.get("tui_command") or step.get("command") or "").strip()
                if step
                else ""
            )
            status = str(step.get("status") or "").strip().lower() if step else ""
            if status == "blocked" or (command and "run execute" not in command):
                return (
                    "Safe Run. Do not paste PowerShell. Outside: NEXT SAFE "
                    "ACTION; run execute waits. 2, q"
                )
            return (
                "Safe Run. Try: run execute only after reviewing calls; "
                "2, inbox, refresh, help, q"
            )
        if page == "candidate":
            return (
                f"Candidate {ticker}. Try: 2 Evidence Gaps, inbox, "
                f"action {ticker} watch, help, q"
            )
        if page == "candidates":
            readiness = _mapping(self.payload.get("readiness"))
            if readiness.get("safe_to_make_investment_decision") is not True:
                return (
                    "Candidate Review. Evidence first. Try: 2 Evidence Gaps, "
                    "inbox, full, help, q"
                )
            return "Candidate Review. Try: open 1, ticker AAPL, inbox, ready, full, help, q"
        if page == "review":
            if not _priced_in_review_rows(self.payload):
                return (
                    "Decision Review. No rows yet. Try: inbox, 2 Evidence Gaps, "
                    "full, mismatches, help, q"
                )
            return "Decision Review. Try: open 1, inbox, full, mismatches, broker, help, q"
        if page == "alerts":
            return "Alerts. Try: open 1, feedback 1 useful/noisy/acted, inbox, help, q"
        if page == "alert":
            alert_ref = page_ref or "ALERT_ID"
            alert_label, feedback_ref = _alert_feedback_prompt_parts(
                self.payload, alert_ref
            )
            return (
                f"Alert {alert_label}. Try: feedback {feedback_ref} "
                "useful/noisy/acted, alerts, inbox, help, q"
            )
        if page == "broker":
            return (
                "Broker. Try: action TICKER watch, trigger TICKER ..., "
                "ticket TICKER ..., help, q"
            )
        if page == "ops":
            return (
                "Source workbench. Try: batch SOURCE, "
                "batch SOURCE execute, source-gap SOURCE, help, q"
            )
        if page == "agent":
            return "Agent Coach. Try: agent, agent execute only with budget, inbox, 2, help, q"
        return (
            "Type a command or click a message. Try: inbox, ready, full, "
            "mismatches, 2, 4, run, refresh, help, q"
        )

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
        if page_key == "overview":
            queue = _mapping(self.payload.get("priced_in_queue"))
            count = int(
                _number_or_zero(
                    queue.get("total_count")
                    or queue.get("returned_count")
                    or queue.get("count")
                )
            )
            if count:
                return f" [{count:,}]"
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
                "action-setup",
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
            "[bold #58a6ff]KEYS[/] 0 Start  1 Inbox  2 Gaps  3 Run  4 Review  5 Alerts\n"
            "[bold #58a6ff]MORE[/] 6 IPO 7 Broker 8 Ops 9 Log ^A "
            "D Ready M All F ? q\n"
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
            "type next, prev, offset ROW, or limit ROWS."
            if total and count
            else ""
        )
        inbox_action = _market_inbox_next_safe_action(self.payload)
        candidate_action = (
            _candidate_case_next_safe_action(
                self.payload,
                self.page.split(":", 1)[1] if ":" in self.page else "",
            )
            if page == "candidate"
            else ""
        )
        page_action = {
            "tutorial": "Tutorial: press 1 for Inbox.",
            "overview": inbox_action,
            "readiness": _readiness_next_safe_action(
                self.payload,
                command_first=False,
            ),
            "review": (
                (
                    "No decision-ready rows yet. Press 2 for Evidence Gaps or "
                    "1 for Inbox; nothing opens here."
                )
                if not _priced_in_review_rows(self.payload)
                else (
                    "Review decision-ready priced-in rows. Press Enter to open the "
                    f"candidate and Decision Card context.{page_text}"
                )
            ),
            "run": _run_page_next_safe_action(self.payload),
            "candidates": _candidates_next_safe_action(self.payload),
            "alerts": "Click or focus a row and press Enter to open an alert.",
            "ipo": _footer_next_action(self.payload, "ipo"),
            "agent": _modern_agent_next_safe_action(self.payload),
            "broker": _modern_broker_next_safe_action(self.payload),
            "ops": _ops_next_safe_action(self.payload),
            "telemetry": _telemetry_next_safe_action(self.payload),
            "features": _footer_next_action(self.payload, "features"),
            "themes": _footer_next_action(self.payload, "themes"),
            "validation": _footer_next_action(self.payload, "validation"),
            "costs": _footer_next_action(self.payload, "costs"),
            "help": "Use the help table as the command reference.",
            "candidate": candidate_action,
        }.get(
            page,
            "Use the sidebar, page keys, or Ctrl+N/Ctrl+P to move; type a command below.",
        )
        cost_summary = (
            "Cost: 0 provider/OpenAI calls. Feedback is local."
            if page == "alerts"
            else _modern_cost_boundary_summary()
        )
        return (
            "[bold #7ee787]NEXT SAFE ACTION[/]\n"
            f"{page_action}\n"
            f"{cost_summary}"
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
        readiness_status = _human_status_label(readiness.get("status") or "unknown")
        broker_status = _human_status_label(broker.get("connection_status") or "n/a")
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
            else _page_display_label(self.page, self.payload).upper()
        )

        if active_page == "tutorial":
            self.query_one("#hero", Static).update(
                "\n".join(
                    [
                        "[bold #7ee787]MARKET RADAR[/] // [b]START[/b]",
                        (
                            "This walkthrough teaches the controls. "
                            "It does not run providers, trade, or change data."
                        ),
                        (
                            "[bold #58a6ff]Do next[/] Read the rows below, "
                            "then press 1 for Inbox."
                        ),
                    ]
                )
            )
            self.query_one("#metric-readiness", Static).update(
                _metric_text("Step 1", "Learn controls", "mouse, keys, commands")
            )
            self.query_one("#metric-market", Static).update(
                _metric_text("Step 2", "Open Inbox", "press 1")
            )
            self.query_one("#metric-calls", Static).update(
                _metric_text("Safety", "0 calls", "tutorial is local")
            )
            self.query_one("#metric-broker", Static).update(
                _metric_text("Orders", "Disabled", "no real trades")
            )
            return

        if active_page == "overview":
            cards = _novice_cockpit_cards(self.payload)
            card_by_label = {str(card.get("label")): card for card in cards}
            can_act_card = _mapping(card_by_label.get("Can I act?"))
            next_card = _mapping(card_by_label.get("Best next step"))
            rows_card = _mapping(card_by_label.get("Rows"))
            inbox_rows = _market_inbox_rows(self.payload)
            inbox_counts = _market_inbox_counts(inbox_rows)
            inbox_intro = (
                "Setup checklist shows how to create the first real scan; "
                "there are no stock result rows yet."
                if _real_results_empty(self.payload)
                else (
                    "Every message is a scan result asking whether market emotion "
                    "has outrun price reaction."
                )
            )
            next_value = _clip(
                next_card.get("value") or next_action or "No operator action.",
                118,
            )
            inbox_summary = _market_inbox_count_summary(inbox_counts)
            inbox_value, inbox_detail = _market_inbox_metric_summary(self.payload)
            inbox_value = inbox_value or inbox_summary or rows_card.get("value") or "0 messages"
            self.query_one("#hero", Static).update(
                "\n".join(
                    [
                        "[bold #7ee787]MARKET INBOX[/] // [b]ATTENTION QUEUE[/b]",
                        inbox_intro,
                        (
                            f"[bold]Can I act?[/] {can_act_card.get('value') or can_act}; "
                            f"[bold]Inbox[/] {inbox_value}; "
                            f"[bold]View[/] {view_label}; "
                            f"[dim]{self.payload.get('external_calls_made', 0)} "
                            "calls while viewing[/dim]"
                        ),
                        (
                            f"[bold #58a6ff]Best next step[/] {next_value}"
                        ),
                    ]
                )
            )
            self.query_one("#metric-readiness", Static).update(
                _metric_text(
                    "Trade safety",
                    can_act_card.get("value") or can_act,
                    _clip(can_act_card.get("detail") or answer_status, 52),
                )
            )
            self.query_one("#metric-market", Static).update(
                _metric_text(
                    "Inbox",
                    inbox_value,
                    inbox_detail
                    or (
                        f"fresh bars "
                        f"{database.get('active_security_with_latest_daily_bar_count')}/"
                        f"{database.get('active_security_count')}"
                    ),
                )
            )
            self.query_one("#metric-calls", Static).update(
                _metric_text(
                    "Cost before execute",
                    "0 browsing calls",
                    _execution_cost_summary(self.payload),
                )
            )
            self.query_one("#metric-broker", Static).update(
                _metric_text(
                    "Orders",
                    "Disabled",
                    f"broker {broker_status}",
                )
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
                        f"mode {readiness_status} | "
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
                f"broker {broker_status}",
            )
        )

    def _guide_text(self) -> str:
        raw_page = self.page
        page = raw_page.split(":", 1)[0]
        readiness = _mapping(self.payload.get("readiness"))
        candidates = _mapping(self.payload.get("candidates"))
        alerts = _mapping(self.payload.get("alerts"))
        call_plan = _mapping(self.payload.get("call_plan"))
        next_step = _priced_in_operator_step(self.payload) or _mapping(
            self.payload.get("operator_next_step")
        )
        can_act = _decision_label(readiness)
        next_action = (
            next_step.get("action") or readiness.get("next_action") or "Open Evidence Gaps."
        )
        if page == "tutorial":
            return "\n".join(
                [
                    "[bold #7ee787]START[/]  Do these in order. Nothing external runs here.",
                    "[bold]1.[/] Press 1 or click Inbox to see what needs attention.",
                    "[bold]2.[/] Press D for decision-ready rows, M for broader mismatches.",
                    (
                        "[bold]3.[/] Press 2 for Evidence Gaps, 4 for Candidate Review, "
                        "or 3 for Safe Run; "
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
            status_filter = _priced_in_status_filter(queue)
            partial_scan = _priced_in_scan_scope_is_partial(self.payload)
            mode = (
                "All Scanned Rows"
                if status_filter == "all" and partial_scan
                else "Full Scan"
                if status_filter == "all"
                else "Mismatches"
            )
            offset = int(_number_or_zero(queue.get("offset")))
            count = int(_number_or_zero(queue.get("count")))
            total = int(_number_or_zero(queue.get("total_count")))
            if status_filter == "all":
                if partial_scan:
                    mode_help = "showing rows from the current partial/selected scan"
                else:
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
            scanned_count = int(_number_or_zero(scanned_rows))
            active_count = int(_number_or_zero(active_rows))
            scanned_text = f"{scanned_count:,}" if scanned_count else str(scanned_rows)
            active_text = f"{active_count:,}" if active_count else str(active_rows)
            return "\n".join(
                [
                    (
                        f"[bold #7ee787]MARKET INBOX[/]  Latest scan results are grouped "
                        f"like messages. {mode} is {mode_help}."
                    ),
                    (
                        "[bold]Core question:[/] has market emotion been fully priced in, "
                        "or is price still behind mood?"
                    ),
                    (
                        f"[bold]Can I act?[/] {_decision_label(readiness)}. "
                        f"[bold]Coverage:[/] scanned {scanned_text} of "
                        f"{active_text} active securities; {total:,} ranked; "
                        f"loaded {count:,} rows."
                    ),
                    (
                        "[bold]Mailboxes:[/] Urgent = decision-ready; Worth Reading = "
                        "research; Waiting Evidence = data gaps."
                    ),
                    (
                        "[bold]Legend:[/] Gap = emotion - price reaction; browsing "
                        "and opening rows make 0 provider calls."
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
            first_gap = _readiness_first_work_item(self.payload)
            if first_gap:
                priority = str(first_gap.get("priority") or "gap").replace("_", " ")
                area = str(
                    first_gap.get("area")
                    or first_gap.get("item")
                    or "Evidence gap"
                ).strip()
                action = str(
                    first_gap.get("next_action") or first_gap.get("action") or ""
                ).strip()
                command = _first_catalyst_radar_command(action)
                command_lines = []
                if command:
                    command_lines = [
                        f"[bold]PowerShell command:[/] {command}",
                        f"[bold]Where to run:[/] {_POWERSHELL_RUN_LOCATION}",
                        (
                            "[bold]Command boundary:[/] "
                            f"{_powershell_command_boundary(command)}"
                        ),
                    ]
                return "\n".join(
                    [
                        (
                            f"[bold #7ee787]STOPLIGHT[/] {can_act}; "
                            "0 calls, 0 orders; red rows block trust."
                        ),
                        f"[bold]First blocker:[/] {priority} - {area}.",
                        (
                            f"[bold]Safe:[/] 0 calls, 0 orders. "
                            f"[bold]Do next:[/] {_clip(action or next_action, 110)}"
                        ),
                        *command_lines,
                    ]
                )
            return "\n".join(
                [
                    "[bold #7ee787]USE THIS PAGE[/] Clear blockers before trusting output.",
                    f"[bold]Current answer:[/] {can_act}.",
                    f"[bold]Look for:[/] blocked or stale rows. [bold]Do next:[/] {next_action}",
                ]
            )
        if page == "run":
            command = str(
                next_step.get("tui_command") or next_step.get("command") or ""
            ).strip()
            command_lines = []
            if command.startswith("catalyst-radar"):
                command_lines = [
                    f"[bold]PowerShell command:[/] {command}",
                    f"[bold]Where to run:[/] {_POWERSHELL_RUN_LOCATION}",
                    (
                        "[bold]Command boundary:[/] "
                        f"{_powershell_command_boundary(command)}"
                    ),
                ]
            run_next = (
                "[bold]Do next:[/] run the PowerShell command below first; "
                "run execute waits."
                if command and "run execute" not in command
                else "[bold]Do next:[/] inspect rows first; type run execute only when you mean it."
            )
            return "\n".join(
                [
                    "[bold #7ee787]USE THIS PAGE[/] A run may call external providers.",
                    (
                        f"[bold]Budget:[/] max {call_plan.get('max_external_call_count')} calls. "
                        f"[bold]Status:[/] {call_plan.get('status') or 'unknown'}."
                    ),
                    run_next,
                    *command_lines,
                ]
            )
        if page == "candidates":
            if readiness.get("safe_to_make_investment_decision") is not True:
                return "\n".join(
                    [
                        (
                            "[bold #7ee787]USE THIS PAGE[/] Review companies, "
                            "not trade signals."
                        ),
                        (
                            f"[bold]Rows:[/] {candidates.get('count') or 0} "
                            "research rows. Opening a row is inspection only."
                        ),
                        (
                            "[bold]Do next:[/] press 2 Evidence Gaps first; "
                            "candidate rows are not trade ideas yet."
                        ),
                    ]
                )
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
                    (
                        "[bold #7ee787]USE THIS PAGE[/] Review alert "
                        "notifications, not trade signals."
                    ),
                    (
                        f"[bold]Rows:[/] {alerts.get('count') or 0}. "
                        "Click an alert row or press Enter; no broker/order call."
                    ),
                    (
                        "[bold]Do next:[/] open one alert, then record local "
                        "feedback useful/noisy/acted."
                    ),
                ]
            )
        if page == "ipo":
            return "\n".join(
                [
                    "[bold #7ee787]USE THIS PAGE[/] Inspect SEC catalyst evidence only.",
                    (
                        "[bold]Do next:[/] open Inbox or Candidate Review before "
                        "treating a filing as actionable."
                    ),
                    "[bold]Reminder:[/] browsing IPO/S-1 rows makes 0 SEC calls.",
                ]
            )
        if page == "telemetry":
            return "\n".join(
                [
                    "[bold #7ee787]USE THIS PAGE[/] Audit what the dashboard already did.",
                    (
                        "[bold]Do next:[/] check recent failures, then use Run or Ops "
                        "only after the call boundary is clear."
                    ),
                    "[bold]Reminder:[/] telemetry is local history; browsing makes 0 calls.",
                ]
            )
        if page == "themes":
            return "\n".join(
                [
                    "[bold #7ee787]USE THIS PAGE[/] Spot research clusters, not trade ideas.",
                    (
                        "[bold]Do next:[/] return to Inbox or Candidate Review for "
                        "ticker-level evidence before acting."
                    ),
                    "[bold]Reminder:[/] theme browsing makes 0 provider calls.",
                ]
            )
        if page == "validation":
            return "\n".join(
                [
                    "[bold #7ee787]USE THIS PAGE[/] Check whether alerts proved useful.",
                    (
                        "[bold]Do next:[/] run validation replay/report only after "
                        "stored evidence and outcomes exist."
                    ),
                    "[bold]Boundary:[/] keep decisions research-only until validation exists.",
                ]
            )
        if page == "costs":
            return "\n".join(
                [
                    "[bold #7ee787]USE THIS PAGE[/] Prove value before expanding spend.",
                    (
                        "[bold]Do next:[/] record feedback, value-ledger rows, and "
                        "outcomes before counting wins."
                    ),
                    "[bold]Boundary:[/] value rows are local; writes require explicit commands.",
                ]
            )
        if page == "features":
            return "\n".join(
                [
                    "[bold #7ee787]USE THIS PAGE[/] Use this as the dashboard map.",
                    (
                        "[bold]Do next:[/] open the page listed for the feature you "
                        "want, then follow that page's Next Safe Action."
                    ),
                    "[bold]Reminder:[/] the inventory is local and makes 0 provider calls.",
                ]
            )
        if page == "agent":
            brief = _mapping(self.payload.get("agent_brief"))
            calls = _mapping(brief.get("external_calls_made"))
            runtime = _mapping(brief.get("runtime"))
            if _real_results_empty(self.payload):
                return "\n".join(
                    [
                        "[bold #7ee787]USE THIS PAGE[/] Agent Coach is locked until setup.",
                        (
                            "[bold]Why:[/] no real scan rows exist, so there is "
                            "nothing useful for the agents to analyze."
                        ),
                        (
                            f"[bold]Calls:[/] OpenAI {calls.get('openai', 0)}, "
                            f"market {calls.get('market_data', 0)}, "
                            f"broker {calls.get('broker', 0)}."
                        ),
                        "[bold]Do next:[/] open 2 Evidence Gaps and clear the first blocker.",
                    ]
                )
            return "\n".join(
                [
                    "[bold #7ee787]USE THIS PAGE[/] Read the multi-agent operator brief.",
                    (
                        f"[bold]Mode:[/] "
                        f"{_human_status_label(brief.get('mode') or 'dry_run')}; "
                        "[bold]Status:[/] "
                        f"{_human_status_label(brief.get('status') or 'unknown')}; "
                        f"[bold]Calls:[/] OpenAI {calls.get('openai', 0)}, "
                        f"market {calls.get('market_data', 0)}, broker {calls.get('broker', 0)}."
                    ),
                    f"[bold]Runtime:[/] {_agent_runtime_label(runtime)}.",
                    "[bold]Do next:[/] follow the first Next Action row, then return to Inbox.",
                ]
            )
        if page == "broker":
            broker = _mapping(self.payload.get("broker"))
            snapshot = _mapping(broker.get("snapshot"))
            exposure = _mapping(broker.get("exposure"))
            connection = str(
                snapshot.get("connection_status")
                or exposure.get("connection_status")
                or "missing"
            )
            orders_enabled = bool(exposure.get("order_submission_enabled"))
            order_status = "enabled" if orders_enabled else "disabled"
            return "\n".join(
                [
                    (
                        "[bold #7ee787]BROKER SAFETY[/] Portfolio context only; "
                        "not trade approval."
                    ),
                    (
                        f"[bold]Connection:[/] {connection}; "
                        f"[bold]orders:[/] {order_status}; browsing makes 0 Schwab calls."
                    ),
                    (
                        "[bold]Do next:[/] use local watch/trigger/ticket artifacts only, "
                        "or authenticate Schwab intentionally."
                    ),
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
                        "[bold]Execute:[/] type batch SOURCE execute only when "
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
        if raw_page.startswith("candidate:"):
            ticker = raw_page.split(":", 1)[1].strip().upper()
            row = _candidate_detail_row(self.payload, ticker)
            brief = _mapping(row.get("priced_in_evidence_brief"))
            if row and _candidate_case_has_source_gaps(row, brief):
                return "\n".join(
                    [
                        "[bold #7ee787]USE THIS PAGE[/] Research case file; not trade approval.",
                        (
                            "[bold]Do next:[/] press 2 Evidence Gaps for global blockers "
                            "before building packets or tickets."
                        ),
                        "[bold]Reminder:[/] browsing this case makes 0 provider calls.",
                    ]
                )
            return "\n".join(
                [
                    "[bold #7ee787]USE THIS PAGE[/] Inspect this evidence before acting elsewhere.",
                    "[bold]Do next:[/] review the rows, then return to Inbox with 1.",
                    "[bold]Reminder:[/] navigation and filtering make 0 provider calls.",
                ]
            )
        return "\n".join(
            [
                "[bold #7ee787]USE THIS PAGE[/] Inspect this evidence before acting elsewhere.",
                "[bold]Do next:[/] click rows when available, or return to Inbox with 1.",
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
                "Evidence Gaps - blockers before any decision",
                [
                    ("area", "Area", 18),
                    ("status", "Status", 14),
                    ("finding", "Finding", 44),
                    ("next_action", "Next action", 58),
                ],
                _readiness_modern_table_rows(self.payload),
                (
                    "Rows explain evidence areas. Start with blocked rows; "
                    "Enter only inspects and makes no calls."
                ),
            )
        if page == "run":
            call_plan = _mapping(self.payload.get("call_plan"))
            return (
                "Run call plan",
                [
                    ("layer", "Layer", 16),
                    ("provider", "Provider", 12),
                    ("status", "Status", 14),
                    ("external_call_count_max", "Calls", 5),
                    ("next_action", "Next action", 50),
                ],
                _run_modern_table_rows(self.payload),
                f"{call_plan.get('headline') or ''} {call_plan.get('next_action') or ''}",
            )
        if page == "candidates":
            decision_safe = (
                _mapping(self.payload.get("readiness")).get(
                    "safe_to_make_investment_decision"
                )
                is True
            )
            rows = [
                _candidate_table_row(row, row_key=str(row.get("ticker") or index))
                for index, row in enumerate(_candidate_rows(self.payload), start=1)
            ]
            return (
                (
                    "Candidates - click a row or press Enter to open"
                    if decision_safe
                    else "Candidates - open rows only to inspect evidence"
                ),
                [
                    ("ticker", "Ticker", 8),
                    ("priced_in_status", "Priced-in", 20),
                    ("emotion_reaction_gap", "Gap", 7),
                    ("score", "Score", 7),
                    ("data_coverage", "Evidence", 36),
                    ("why_now", "Why now", 30),
                    ("next_step", "Next step", 31),
                ],
                rows,
                (
                    "Gap is emotion minus price reaction. Positive means the market "
                    "may not have fully priced it."
                ),
            )
        if page == "alerts":
            rows = [
                _alert_table_row(row, row_key=str(row.get("id") or index))
                for index, row in enumerate(
                    _rows(_mapping(self.payload.get("alerts")).get("rows")),
                    start=1,
                )
            ]
            return (
                "Alerts - research notifications, not trade signals",
                [
                    ("ticker", "Ticker", 8),
                    ("status_label", "Status", 12),
                    ("route_label", "Delivery", 22),
                    ("priority_label", "Priority", 10),
                    ("title", "Message", 76),
                ],
                rows,
                (
                    "Open a row number to review; the detail view shows the exact "
                    "feedback command."
                ),
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
                "Broker safety and Schwab sync status",
                [
                    ("area", "Area", 20),
                    ("status", "Status", 12),
                    ("meaning", "Meaning", 38),
                    ("next_action", "Next safe action", 34),
                ],
                _broker_status_rows(broker),
                "Rows are local broker status only. Selecting a row makes no Schwab call.",
            )
        if page == "ops":
            rows = _source_coverage_workbench_rows(self.payload)
            return (
                "Source coverage workbench - Enter shows plan, not execution",
                [
                    ("priority", "#", 4),
                    ("source_label", "Source", 18),
                    ("status_label", "Status", 14),
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
                    ("event_label", "Event", 28),
                    ("status_label", "Status", 14),
                    ("summary_label", "Summary", 70),
                ],
                _telemetry_event_rows(telemetry),
                _humanize_dashboard_text(
                    f"{telemetry.get('headline') or ''} "
                    f"Next: {telemetry.get('next_action') or ''}"
                ),
            )
        if page == "themes":
            return (
                "Themes - clustered catalyst patterns",
                [
                    ("theme", "Theme", 26),
                    ("candidate_count", "Candidates", 12),
                    ("avg_score", "Avg score", 12),
                    ("top_tickers", "Top tickers", 24),
                    ("states", "States", 44),
                ],
                _rows(_mapping(self.payload.get("themes")).get("rows")),
                "Theme rows are local candidate clusters. Selecting a row makes no calls.",
            )
        if page == "validation":
            return (
                "Validation - useful-alert evidence",
                [("key", "Question", 28), ("value", "Answer", 104)],
                _validation_status_rows(_mapping(self.payload.get("validation"))),
                "Validation rows summarize stored replay/report evidence only.",
            )
        if page == "costs":
            return (
                "Costs and value proof",
                [("key", "Question", 30), ("value", "Answer", 102)],
                _cost_status_rows(self.payload),
                "Costs rows are local budget/value evidence; writes require explicit commands.",
            )
        if page == "agent":
            brief = _mapping(self.payload.get("agent_brief"))
            runtime = _mapping(brief.get("runtime"))
            if _real_results_empty(self.payload):
                return (
                    "Agent Coach - locked until setup",
                    [
                        ("kind", "Kind", 12),
                        ("item", "Item", 28),
                        ("detail", "Detail", 98),
                    ],
                    _agent_setup_locked_rows(self.payload, brief),
                    (
                        "Agent Coach is a zero-call gate preview for now. Clear "
                        "Evidence Gaps before using agent execute."
                    ),
                )
            boundary = (
                _human_agent_text(brief.get("decision_boundary") or "")
                or "Manual research boundary."
            )
            return (
                "Agent brief - preview by default, execute spends OpenAI budget",
                [
                    ("kind", "Kind", 12),
                    ("item", "Item", 28),
                    ("detail", "Detail", 98),
                ],
                _agent_brief_rows(brief, self.payload),
                f"{boundary} {_agent_runtime_label(runtime)}.",
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
        rows = _tutorial_mission_rows(self.payload) + _tutorial_control_rows(
            self.payload
        )
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
            _market_inbox_title(self.payload),
            [
                ("mailbox", "Mailbox", 16),
                ("ticker", "Ticker", 6),
                ("subject", "Subject", 24),
                ("why", "Why this reached you", 36),
                ("missing", "Missing / waiting", 22),
                ("next", "Next safe action", 30),
            ],
            _market_inbox_rows(self.payload),
            _market_inbox_caption(self.payload),
        )

    def _review_model(
        self,
    ) -> tuple[str, Sequence[tuple[str, str, int]], list[Mapping[str, object]], str]:
        review_rows = _priced_in_review_rows(self.payload)
        table_rows = review_rows or [_decision_review_empty_modern_row()]
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
            table_rows,
            _decision_review_caption(self.payload, review_rows),
        )

    def _candidate_detail_model(
        self,
        ticker: str,
    ) -> tuple[str, Sequence[tuple[str, str, int]], list[Mapping[str, object]], str]:
        ticker = ticker.upper()
        row = _candidate_detail_row(self.payload, ticker)
        rows = _candidate_case_detail_table_rows(self.payload, ticker, row)
        return (
            f"Candidate {ticker}",
            [("key", "Case question", 24), ("value", "Answer", 110)],
            rows,
            "Verify evidence first. Watch/trigger/dismiss are local; "
            "tickets wait for Decision Review.",
        )

    def _alert_detail_model(
        self,
        alert_id: str,
    ) -> tuple[str, Sequence[tuple[str, str, int]], list[Mapping[str, object]], str]:
        row = _alert_detail_row(self.payload, alert_id)
        return (
            _alert_display_title(row, alert_id),
            [("key", "Alert question", 24), ("value", "Answer", 110)],
            _alert_case_detail_table_rows(
                row,
                feedback_ref=_alert_feedback_prompt_parts(self.payload, alert_id)[1],
            ),
            "Use feedback ROW|ALERT_ID LABEL [notes] to record alert usefulness.",
        )

    def _help_model(
        self,
    ) -> tuple[str, Sequence[tuple[str, str, int]], list[Mapping[str, object]], str]:
        rows = [
            {"command": "Click sidebar row", "meaning": "Switch pages with mouse support."},
            {"command": "Click candidate/alert row", "meaning": "Open the selected detail view."},
            {"command": "0, 1..9, Ctrl+A, f, ?", "meaning": "Keyboard page shortcuts."},
            {
                "command": "setup / first",
                "meaning": "Show the first setup command and where to run it.",
            },
            {
                "command": "start / tutorial / inbox",
                "meaning": "Use start/tutorial for the walkthrough; inbox for scan messages.",
            },
            {
                "command": "stocks / ready / full / mismatches",
                "meaning": (
                    "Switch Inbox between stock-like rows, decision-ready rows, "
                    "full universe rows, and the broader mismatch queue."
                ),
            },
            {
                "command": "next / prev / offset ROW",
                "meaning": "Page through the full ranked scan without provider calls.",
            },
            {
                "command": "export full",
                "meaning": "Print the full-scan JSON export command.",
            },
            {"command": "limit 1-200", "meaning": "Change loaded Inbox rows per page."},
            {
                "command": "source-gap SOURCE|all",
                "meaning": "Show scan rows missing options, text, events, bars, or broker context.",
            },
            {
                "command": "batch SOURCE",
                "meaning": "Plan full-scan source fill and show the next safe chunk.",
            },
            {
                "command": "batch SOURCE execute",
                "meaning": "Run only the next guarded chunk; refresh and repeat deliberately.",
            },
            {
                "command": "batch SOURCE execute 3",
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
                "command": "agent / agent execute",
                "meaning": "Preview real Agents SDK gates, or explicitly spend OpenAI budget once.",
            },
            {
                "command": "cik template / validate / import",
                "meaning": "Create, check, or explicitly import local SEC CIK overrides.",
            },
            {"command": "ticker SYMBOL|all", "meaning": "Filter ticker-aware pages."},
            {"command": "run execute", "meaning": "Start one guarded capped radar cycle."},
            {
                "command": "action / trigger / ticket",
                "meaning": "Save local broker-context artifacts only.",
            },
            {
                "command": "feedback ROW|ALERT_ID LABEL",
                "meaning": "Record useful/noisy/acted alert feedback.",
            },
            {
                "command": "ledger coverage / record",
                "meaning": (
                    "Review or save local value-ledger rows; --execute is "
                    "required to write."
                ),
            },
            {
                "command": "outcome coverage / update",
                "meaning": (
                    "Review or compute local forward outcomes; --execute is "
                    "required to write."
                ),
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
    lines.extend(_footer_lines(resolved_width, payload=payload, page=page))
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


_SNAPSHOT_RELOAD_COMMANDS = {
    "action",
    "batch",
    "batches",
    "bar",
    "bars",
    "cik",
    "ciks",
    "eval-triggers",
    "evaluate-triggers",
    "feedback",
    "ledger",
    "market-bars",
    "market_bars",
    "option",
    "options",
    "options-flow",
    "options_flow",
    "outcome",
    "outcomes",
    "refresh",
    "r",
    "run",
    "sec",
    "sec-cik",
    "sec_cik",
    "source-batch",
    "source-batches",
    "ticket",
    "trigger",
    "value-ledger",
    "value-outcome",
    "value_ledger",
    "value_outcome",
}

_COMMAND_NO_SIDE_EFFECTS = "No API calls/orders/writes."
_POWERSHELL_RUN_LOCATION = (
    "Run it in a normal PowerShell prompt, not in the dashboard command box."
)
_POWERSHELL_COMMANDS_SHOWN_IN_TUI = {
    "build-packets",
    "build-decision-cards",
    "ingest-csv",
    "ingest-polygon",
    "market-bars",
    "priced-in-queue",
}


def _command_no_side_effects(message: str) -> str:
    return f"{_COMMAND_NO_SIDE_EFFECTS}\n{message}"


def _first_catalyst_radar_command(value: object) -> str:
    text = str(value or "").strip()
    marker = "catalyst-radar "
    start = text.find(marker)
    if start < 0:
        return ""
    command = text[start:]
    if start > 0 and text[start - 1] == "`":
        command = command.split("`", 1)[0]
    else:
        command = command.splitlines()[0].split(";", 1)[0]
    return command.strip().strip("`").rstrip(".,;")


def _catalyst_child_command(shell_command: str) -> str:
    command = shell_command.strip()
    if not command.lower().startswith("catalyst-radar "):
        return ""
    rest = command.partition(" ")[2].strip()
    return rest.split(maxsplit=1)[0].lower() if rest else ""


def _powershell_command_boundary(shell_command: str) -> str:
    shell_command_lower = shell_command.lower()
    child_command = _catalyst_child_command(shell_command)
    if " market-bars residual-review " in f" {shell_command_lower} ":
        return (
            "Read-only market-bar review; no provider, OpenAI, broker, order, "
            "or DB write calls."
        )
    if child_command in {"build-packets", "build-decision-cards"}:
        return _candidate_case_command_boundary(f"catalyst-radar {child_command} ")
    return "Run it only after accepting the command's call/write boundary."


def _powershell_command_context_items(
    shell_command: str,
    *,
    include_command: bool = False,
) -> tuple[tuple[str, object], ...]:
    if not shell_command.strip().lower().startswith("catalyst-radar "):
        return ()
    items: list[tuple[str, object]] = []
    if include_command:
        items.append(("PowerShell command", shell_command.strip()))
    items.extend(
        [
            ("Where to run", _POWERSHELL_RUN_LOCATION),
            ("Command boundary", _powershell_command_boundary(shell_command)),
        ]
    )
    return tuple(items)


def _powershell_command_guidance(raw: str) -> str:
    text = raw.strip()
    if not text:
        return ""
    command, _, _rest = text.partition(" ")
    command_name = command.strip().lower()
    if command_name == "catalyst-radar":
        shell_command = text
    elif command_name in _POWERSHELL_COMMANDS_SHOWN_IN_TUI:
        if command_name == "market-bars" and not _rest.strip():
            return ""
        shell_command = f"catalyst-radar {text}"
    else:
        return ""

    boundary = _powershell_command_boundary(shell_command)
    return _command_no_side_effects(
        "PowerShell command, not a dashboard command. "
        f"Run this in a normal PowerShell prompt: {shell_command}. {boundary}"
    )


def _priced_in_operator_step(payload: Mapping[str, object]):
    answer = _mapping(payload.get("priced_in_answer"))
    step = _mapping(answer.get("operator_next_step"))
    if step:
        return step
    return _mapping(payload.get("priced_in_operator_next_step"))


def _operator_next_step_summary(
    step: Mapping[str, object],
    *,
    include_command: bool = True,
):
    if not step:
        return ""
    action = _human_source_status_text(
        step.get("action") or step.get("action_label") or "No action recorded."
    ).rstrip(".;")
    parts = [action]
    command = str(step.get("tui_command") or step.get("command") or "").strip()
    if command and include_command:
        command_label = (
            "PowerShell command"
            if command.startswith("catalyst-radar")
            else "dashboard command"
            if step.get("tui_command")
            else "command"
        )
        parts.append(f"{command_label}: {command}")
    calls = int(_number_or_zero(step.get("external_calls_required")))
    changes = int(_number_or_zero(step.get("db_" + "writes_required")))
    approval = " after approval" if bool(step.get("approval_required")) else ""
    parts.append(f"{calls} provider call(s){approval}")
    parts.append(f"{changes} database change(s)")
    blocker = step.get("first_blocker")
    gap = int(_number_or_zero(step.get("first_gap_count")))
    if blocker:
        parts.append(f"blocker {_human_source_name(blocker)}; gap {gap}")
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


def _current_priced_in_blocker_next_action(payload: Mapping[str, object]) -> str:
    step = _priced_in_operator_step(payload) or _mapping(
        payload.get("operator_next_step")
    )
    if step:
        action = _human_source_status_text(
            step.get("action") or step.get("action_label") or ""
        ).strip()
        command = str(step.get("tui_command") or step.get("command") or "").strip()
        if action and command:
            return f"{action.rstrip('.;')}. Use `{command}`."
        if command:
            return f"Use `{command}`."
        if action:
            return action
    answer = _mapping(payload.get("priced_in_answer"))
    trust_gate = _mapping(answer.get("full_market_trust_gate"))
    recommended = _mapping(trust_gate.get("recommended_action"))
    if recommended:
        action = _human_source_status_text(
            recommended.get("reason")
            or recommended.get("label")
            or recommended.get("next_action")
            or ""
        ).strip()
        command = str(
            recommended.get("tui_command")
            or recommended.get("command")
            or recommended.get("cli_command")
            or ""
        ).strip()
        if action and command:
            return f"{action.rstrip('.;')}. Use `{command}`."
        if command:
            return f"Use `{command}`."
        if action:
            return action
    return ""


def _operator_step_cost_detail(step: Mapping[str, object]) -> str:
    if not step:
        return ""
    command = str(step.get("tui_command") or step.get("command") or "").strip()
    calls = int(_number_or_zero(step.get("external_calls_required")))
    writes = int(_number_or_zero(step.get("db_" + "writes_required")))
    approval = "approval required" if bool(step.get("approval_required")) else "no approval"
    parts = []
    if command:
        parts.append(f"Command: {command}.")
    parts.append(f"Budget: {calls} provider call(s), {writes} DB write(s); {approval}.")
    return "\n".join(parts)


def _run_page_next_safe_action(payload: Mapping[str, object]) -> str:
    if _real_results_empty(payload):
        return _no_real_result_next_action(payload, _mapping(payload.get("real_results")))
    step = _priced_in_operator_step(payload)
    if not step:
        return "Review call budget, then type run execute only if intended."
    action = _human_source_status_text(
        step.get("action") or step.get("action_label") or "Review the next blocker."
    ).strip()
    command = str(step.get("tui_command") or step.get("command") or "").strip()
    calls = int(_number_or_zero(step.get("external_calls_required")))
    writes = int(_number_or_zero(step.get("db_" + "writes_required")))
    approval = "approval required" if bool(step.get("approval_required")) else "no approval"
    if command:
        return "\n".join(
            [
                "Run the PowerShell command shown above.",
                f"Budget: {_count_text(calls, 'provider call')}, "
                f"{_count_text(writes, 'DB write')}; {approval}.",
            ]
        )
    return " ".join(
        [
            f"Budget: {calls} provider call(s), {writes} DB write(s); {approval}.",
            action,
        ]
    )


def _candidates_next_safe_action(payload: Mapping[str, object]) -> str:
    if _real_results_empty(payload):
        return _no_real_result_next_action(payload, _mapping(payload.get("real_results")))
    readiness = _mapping(payload.get("readiness"))
    if readiness.get("safe_to_make_investment_decision") is True:
        return "Open a candidate, then verify evidence before any manual decision."
    return "Research-only: press 2 Evidence Gaps first."


def _modern_broker_next_safe_action(payload: Mapping[str, object]) -> str:
    if _real_results_empty(payload):
        return "No real results yet. Set up data sources first."
    broker = _mapping(payload.get("broker"))
    snapshot = _mapping(broker.get("snapshot"))
    exposure = _mapping(broker.get("exposure"))
    connected = bool(exposure.get("broker_connected"))
    connection_status = str(
        snapshot.get("connection_status")
        or exposure.get("connection_status")
        or ("connected" if connected else "missing")
    ).strip()
    orders_enabled = bool(exposure.get("order_submission_enabled"))
    if orders_enabled:
        return "Orders enabled: verify broker policy first."
    if not connected or connection_status.lower() not in {"connected", "ready"}:
        connection_label = _human_status_label(connection_status or "missing")
        return f"Broker {connection_label}. Browsing makes 0 Schwab calls."
    return "Broker read-only. Use local tickets/watch; orders disabled."


def _minimum_product_stop_line_summary(payload: Mapping[str, object]) -> str:
    trial = _mapping(payload.get("trial_readiness"))
    gate = _mapping(trial.get("minimum_useful_product"))
    if not gate:
        return ""
    if bool(gate.get("ready")):
        return "ready for read-only decision support; still not trade approval."
    blocker = _human_status_label(gate.get("first_blocker") or "unknown")
    status = _human_status_label(gate.get("status") or "blocked")
    command = str(gate.get("next_command") or "").strip()
    parts = [
        f"{status}; blocker {blocker}",
    ]
    if command:
        parts.append(f"inspect `{command}`")
    return "; ".join(parts)


def _minimum_product_approval_summary(payload: Mapping[str, object]) -> str:
    approval = _minimum_product_approval_unblock(payload)
    if not approval:
        return ""
    writes = int(_number_or_zero(approval.get("db_writes_required_to_execute")))
    calls = int(_number_or_zero(approval.get("external_calls_required")))
    return f"approval required: {writes} DB write(s), {calls} provider call(s)"


def _minimum_product_approval_command(payload: Mapping[str, object]) -> str:
    approval = _minimum_product_approval_unblock(payload)
    if not approval:
        return ""
    return str(approval.get("approval_command") or "").strip()


def _minimum_product_approval_unblock(
    payload: Mapping[str, object],
) -> Mapping[str, object]:
    trial = _mapping(payload.get("trial_readiness"))
    gate = _mapping(trial.get("minimum_useful_product"))
    approval = _mapping(gate.get("approval_required_unblock"))
    return approval


def _execute_agent_command(
    engine: Engine,
    config: AppConfig,
    payload: Mapping[str, object],
    value: str,
) -> str:
    try:
        tokens = shlex.split(value)
    except ValueError:
        return _command_no_side_effects(
            "Usage: agent [TICKER] OR agent [TICKER] execute [max-openai-calls]."
        )
    lowered = [token.lower() for token in tokens]
    execute = "execute" in lowered
    max_calls = 3
    ticker = ""
    for token in lowered:
        if token.isdigit():
            max_calls = max(1, min(8, int(token)))
        elif token.startswith("max="):
            _, _, raw = token.partition("=")
            if raw.isdigit():
                max_calls = max(1, min(8, int(raw)))
    command_words = {"run", "preview", "execute", "real", "agent"}
    for token in tokens:
        normalized = token.strip()
        lowered_token = normalized.lower()
        if (
            not normalized
            or lowered_token in command_words
            or lowered_token.startswith("max=")
            or lowered_token.isdigit()
        ):
            continue
        ticker = normalized.upper()
        break
    run_payload = payload
    if ticker:
        run_payload = dashboard_snapshot_payload(
            engine=engine,
            config=config,
            dotenv_loaded=True,
            filters=DashboardFilters(ticker=ticker),
        )
    brief = run_market_radar_agents(
        run_payload,
        config,
        real=True,
        execute=execute,
        max_openai_calls=max_calls,
        ledger_repo=BudgetLedgerRepository(engine),
    )
    calls = _mapping(brief.get("external_calls_made"))
    credit = _mapping(brief.get("credit_gate"))
    real_results = _mapping(brief.get("real_results"))
    action = "executed" if execute and brief.get("status") == "completed" else (
        "blocked" if execute else "previewed"
    )
    next_execute = (
        f"agent {ticker} execute"
        if ticker
        else "agent execute"
    )
    no_real_result = (
        " No real result yet."
        if str(real_results.get("status") or "") == "missing"
        else ""
    )
    return (
        f"Agent {action}: status={brief.get('status')}; "
        f"OpenAI calls={int(_number_or_zero(calls.get('openai')))}; "
        f"OpenAI calls planned<={max_calls}; "
        f"real_results={real_results.get('status', 'unknown')} "
        f"rows={real_results.get('row_count', 0)}; "
        f"credit_gate={credit.get('status', 'unknown')} "
        f"estimated_cost={_format_usd_amount(credit.get('estimated_cost_usd', 0))}. "
        f"{no_real_result} Use `{next_execute}` only after the preview matches your intent."
    )


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
    powershell_guidance = _powershell_command_guidance(raw)
    if powershell_guidance:
        return _CommandUpdate(page=page, filters=filters, message=powershell_guidance)
    if command in {"q", "quit", "exit"}:
        return _CommandUpdate(page=page, filters=filters, exit_requested=True)
    if command in {"r", "refresh"}:
        return _CommandUpdate(page=page, filters=filters, message="Refreshed.")
    if command in {"setup", "first", "first-step", "first_step"}:
        return _CommandUpdate(
            page="readiness",
            filters=dashboard_filters_for_page(filters, "readiness"),
            message=_setup_command_status_message(payload),
        )
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
            message=_all_scan_rows_mode_message(payload),
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
                _all_scan_rows_mode_message(payload)
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
            message=_command_no_side_effects(
                "Usage: export full or export current."
            ),
        )
    if command == "offset":
        if not value.isdigit():
            return _CommandUpdate(
                page=page,
                filters=filters,
                message=_command_no_side_effects("Usage: offset ROW."),
            )
        offset = max(0, int(value) - 1)
        return _CommandUpdate(
            page="overview",
            filters=replace(filters, priced_in_offset=offset).normalized(),
            message=f"Showing full-scan rows starting at {offset + 1}.",
        )
    if command == "limit":
        if not value.isdigit():
            return _CommandUpdate(
                page=page,
                filters=filters,
                message=_command_no_side_effects("Usage: limit 1-200."),
            )
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
        invalid_gaps = _unsupported_filter_values(
            decision_gaps,
            allowed=PRICED_IN_DECISION_GAP_VALUES,
        )
        if invalid_gaps:
            return _CommandUpdate(
                page=page,
                filters=filters,
                message=_unsupported_gap_filter_message(
                    "decision-gap",
                    invalid_gaps,
                    allowed=PRICED_IN_DECISION_GAP_VALUES,
                ),
            )
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
        invalid_gaps = _unsupported_filter_values(
            source_gaps,
            allowed=PRICED_IN_SOURCE_GAP_VALUES,
        )
        if invalid_gaps:
            return _CommandUpdate(
                page=page,
                filters=filters,
                message=_unsupported_gap_filter_message(
                    "source-gap",
                    invalid_gaps,
                    allowed=PRICED_IN_SOURCE_GAP_VALUES,
                ),
            )
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
    if command in {"agent", "agent-brief", "agents"}:
        return _CommandUpdate(
            page="agent",
            filters=filters,
            message=_execute_agent_command(engine, config, payload, value),
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
    if command in {"order-ticket", "order_ticket", "workbench-ticket", "workbench_ticket"}:
        return _CommandUpdate(
            page="broker",
            filters=filters,
            message=_execute_workbench_order_ticket_command(
                engine,
                payload,
                config,
                value,
            ),
        )
    if command == "feedback":
        return _CommandUpdate(
            page="alerts",
            filters=filters,
            message=_record_alert_feedback(engine, payload, value),
        )
    if command in {"ledger", "value-ledger", "value_ledger"}:
        return _CommandUpdate(
            page="costs",
            filters=filters,
            message=_execute_value_ledger_command(
                engine,
                payload,
                value,
                filters=filters,
            ),
        )
    if command in {"outcome", "outcomes", "value-outcome", "value_outcome"}:
        return _CommandUpdate(
            page="costs",
            filters=filters,
            message=_execute_value_outcome_command(
                engine,
                value,
                filters=filters,
            ),
        )
    if command in {"paper-decision", "paper_decision"}:
        return _CommandUpdate(
            page="paper-trading",
            filters=filters,
            message=_execute_workbench_paper_decision_command(
                engine,
                payload,
                value,
            ),
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
            return _CommandUpdate(
                page=page,
                filters=filters,
                message=_command_no_side_effects("Invalid timestamp."),
            )
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
            return _CommandUpdate(
                page=page,
                filters=filters,
                message=_open_command_no_match_message(page, value),
            )
        return _CommandUpdate(
            page=next_page,
            filters=filters,
            message=_page_navigation_status_message(next_page, payload),
        )
    next_page = _normalize_page(raw)
    if next_page != "help" or raw.lower() in PAGE_ALIASES:
        return _CommandUpdate(
            page=next_page,
            filters=dashboard_filters_for_page(filters, next_page),
            message=_page_navigation_status_message(next_page, payload),
        )
    return _CommandUpdate(
        page=page,
        filters=filters,
        message=_command_no_side_effects(
            f"Unknown command: {raw}. Type help for commands."
        ),
    )


def apply_dashboard_command(
    raw: str,
    payload: Mapping[str, object],
    page: str,
    filters: DashboardFilters,
    *,
    engine: Engine,
    config: AppConfig,
) -> _CommandUpdate:
    return _apply_command(
        raw,
        payload,
        page,
        filters,
        engine=engine,
        config=config,
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
        return _command_no_side_effects(
            "Usage: batch SOURCE. Try: batch catalyst_events, batch local_text, "
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
        return _unsupported_source_batch_message(source, payload_or_error)
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


def _unsupported_source_batch_message(source: str, detail: str) -> str:
    source_text = source.strip() or "blank"
    allowed = ", ".join(dashboard_data.PRICED_IN_SOURCE_CLASSES)
    detail_text = f" Detail: {_clip(detail, 120)}" if detail else ""
    return (
        f"Unsupported batch source: {source_text}. No calls made. "
        f"Use one of: {allowed}; or type batch all for the source map."
        f"{detail_text}"
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
        stocks_only = bool(body.get("stocks_only"))
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
                stocks_only=stocks_only,
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
        projected_missing_key = (
            "stock_like_missing_after_import_count"
            if stocks_only
            else "missing_after_import_count"
        )
        verification = market_bars_import_verification_payload(
            engine,
            config,
            expected_as_of=_market_bar_saved_file_date(body),
            stocks_only=stocks_only,
            executed=False,
            source="saved_provider_file",
            db_changes_made=0,
            projected_missing_after_import_count=int(
                coverage.get(projected_missing_key) or 0
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
        return _saved_file_market_bar_failure_message(exc)


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
    preview = preview_polygon_grouped_daily_fixture(
        config=config,
        market_repo=MarketRepository(engine),
        date_value=_market_bar_saved_file_date(body),
        fixture_path=_market_bar_saved_file_path(body, "fixture_path"),
    )
    stocks_only = bool(body.get("stocks_only"))
    preview["stocks_only"] = stocks_only
    preview["coverage_scope"] = "stock_like" if stocks_only else "active_universe"
    return preview


def _saved_market_bar_preview_message(
    label: str,
    preview: Mapping[str, object],
) -> str:
    coverage = _mapping(preview.get("coverage"))
    parts = [
        f"{label}: status={preview.get('status')}",
        f"scope={preview.get('coverage_scope') or 'active_universe'}",
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


def _saved_file_market_bar_failure_message(exc: Exception) -> str:
    detail = str(exc)
    preflight_context = (
        "No provider calls or database writes were made. "
        if "saved-file request body is missing" in detail
        else "Review provider and database state before retrying. "
    )
    return (
        f"Saved-file market-bar action failed: {preflight_context}{detail}. "
        "Refresh Run/Ops and retry only after the saved-file "
        "plan is visible."
    )


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
            "Usage: batch SOURCE execute. Try: batch catalyst_events execute, "
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
        return "Usage: action TICKER watch|ready|simulate_entry|dismiss [notes]"
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
    return (
        "Local only; db_writes=1; no broker order submitted. "
        f"Saved action: {payload.get('ticker')} {payload.get('action')} "
        f"{payload.get('status')}."
    )


def _save_market_trigger(engine: Engine, value: str) -> str:
    parts = value.split(maxsplit=4)
    if len(parts) < 4:
        return (
            "Usage: trigger TICKER price_above|price_below|volume_above|"
            "relative_volume_above|call_put_ratio_above gte|lte|gt|lt THRESHOLD [notes]"
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
    return (
        "Local only; db_writes=1; no broker order submitted. "
        f"Saved trigger: {payload.get('ticker')} {payload.get('trigger_type')}."
    )


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
            "Usage: ticket TICKER buy|sell ENTRY_PRICE INVALIDATION_PRICE "
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
        "Local preview only; db_writes=1; no broker order submitted. "
        "Saved blocked order ticket: "
        f"{payload.get('ticker')} {payload.get('side')} "
        f"submission_allowed={payload.get('submission_allowed')}. "
    )


def _record_alert_feedback(
    engine: Engine,
    payload: Mapping[str, object],
    value: str,
) -> str:
    parts = value.split(maxsplit=2)
    if len(parts) < 2:
        return (
            "Usage: feedback ROW|ALERT_ID "
            "useful|noisy|too_late|too_early|ignored|acted [notes]"
        )
    alert_rows = _rows(_mapping(payload.get("alerts")).get("rows"))
    alert = _row_by_index_or_key(alert_rows, parts[0], key="id")
    if not alert:
        return (
            "No feedback row was saved; external_calls=0 db_writes=0. "
            "Alert feedback rejected: alert not found in current alert rows. "
            "Use open ALERT_ID or refresh if you expected the alert here."
        )
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
        return (
            "No feedback row was saved; external_calls=0 db_writes=0. "
            f"Alert feedback rejected: {exc}."
        )
    useful_label = result.useful_label
    return (
        "Saved alert feedback: "
        f"{useful_label.artifact_id} {useful_label.ticker} {useful_label.label}"
    )


def _execute_value_ledger_command(
    engine: Engine,
    payload: Mapping[str, object],
    value: str,
    *,
    filters: DashboardFilters,
) -> str:
    tokens_or_error = _command_tokens(value)
    if isinstance(tokens_or_error, str):
        return tokens_or_error
    tokens = tokens_or_error
    if not tokens:
        return _value_ledger_usage()
    subcommand = tokens[0].lower()
    args = tokens[1:]
    available_at = filters.available_at or datetime.now(UTC)
    if subcommand in {"coverage", "cov"}:
        coverage = load_value_ledger_candidate_coverage_payload(
            engine,
            available_at=available_at,
        )
        return _value_ledger_coverage_message(coverage)
    if subcommand in {"summary", "sum"}:
        summary = load_value_ledger_summary_payload(engine, available_at=available_at)
        return _value_ledger_summary_message(summary)
    if subcommand == "list":
        limit = _first_positive_int(args, default=10)
        entries = load_value_ledger_entries_payload(
            engine,
            available_at=available_at,
            ticker=filters.ticker,
            limit=limit,
        )
        return _value_ledger_list_message(entries)
    if subcommand == "show":
        if not args:
            return "Usage: ledger show VALUE_LEDGER_ID"
        try:
            entry = load_value_ledger_entry_payload(engine, entry_id=args[0])
        except ValueError as exc:
            return f"Value ledger show rejected: {exc}"
        return _value_ledger_entry_message(entry)
    if subcommand in {"record", "label", "add"}:
        return _record_value_ledger_from_tui(
            engine,
            payload,
            args,
            available_at=available_at,
        )
    return _value_ledger_usage()


def _execute_value_outcome_command(
    engine: Engine,
    value: str,
    *,
    filters: DashboardFilters,
) -> str:
    tokens_or_error = _command_tokens(value)
    if isinstance(tokens_or_error, str):
        return tokens_or_error
    tokens = tokens_or_error
    if not tokens:
        return _value_outcome_usage()
    subcommand = tokens[0].lower()
    args = tokens[1:]
    available_at = filters.available_at or datetime.now(UTC)
    if subcommand in {"coverage", "cov"}:
        coverage = load_value_outcome_coverage_payload(
            engine,
            available_at=available_at,
        )
        return _value_outcome_coverage_message(coverage)
    if subcommand == "list":
        ledger_id = args[0] if args and args[0].lower() not in {"all", "*"} else None
        limit = _first_positive_int(args[1:] if ledger_id else args, default=10)
        outcomes = load_value_outcomes_payload(
            engine,
            value_ledger_entry_id=ledger_id,
            available_at=available_at,
            ticker=filters.ticker,
            limit=limit,
        )
        return _value_outcome_list_message(outcomes)
    if subcommand == "show":
        if not args:
            return "Usage: outcome show VALUE_OUTCOME_ID"
        try:
            outcome = load_value_outcome_payload(engine, outcome_id=args[0])
        except ValueError as exc:
            return f"Value outcome show rejected: {exc}"
        return _value_outcome_message(outcome)
    if subcommand == "update":
        return _update_value_outcome_from_tui(
            engine,
            args,
            fallback_available_at=available_at,
        )
    return _value_outcome_usage()


def _execute_workbench_paper_decision_command(
    engine: Engine,
    payload: Mapping[str, object],
    value: str,
) -> str:
    tokens_or_error = _command_tokens(value)
    if isinstance(tokens_or_error, str):
        return _command_no_side_effects(tokens_or_error)
    tokens = tokens_or_error
    mode = tokens[0].lower() if tokens else "preview"
    if mode in {"preview", "pre"}:
        execute = False
    elif mode in {"execute", "record", "save"}:
        execute = True
    else:
        return _command_no_side_effects(
            "Usage: paper-decision preview|execute. The dashboard uses the "
            "current active trading plan only."
        )
    if len(tokens) > 1:
        return _command_no_side_effects(
            "Usage: paper-decision preview|execute. Extra paper-decision "
            "parameters belong in a reviewed PowerShell command."
        )

    active_plan = _mapping(
        _mapping(payload.get("trading_workbench")).get("active_plan")
    )
    paper_decision = _mapping(active_plan.get("paper_decision"))
    decision_card_id = str(
        paper_decision.get("decision_card_id")
        or active_plan.get("decision_card_id")
        or ""
    ).strip()
    if not decision_card_id:
        return _command_no_side_effects(
            "No active decision card is available for a paper decision."
        )
    decision_text = str(
        paper_decision.get("decision")
        or active_plan.get("recommended_paper_decision")
        or ""
    ).strip()
    try:
        decision = PaperDecision(decision_text)
    except ValueError:
        return _command_no_side_effects(
            "No supported paper decision is available for the active plan."
        )
    available_at = _datetime_or_none(paper_decision.get("available_at"))
    if available_at is None:
        return _command_no_side_effects(
            "No point-in-time cutoff is available for the active paper decision."
        )
    entry_price = _optional_float(paper_decision.get("entry_price"))
    entry_at = _datetime_or_none(paper_decision.get("entry_at"))
    try:
        result = run_paper_decision(
            engine,
            decision_card_id=decision_card_id,
            decision=decision,
            available_at=available_at,
            entry_price=entry_price,
            entry_at=entry_at,
            execute=execute,
            actor_source="dashboard_tui",
            actor_id="local-tui",
            actor_role="analyst",
        )
    except PaperDecisionExecutionError as exc:
        return _command_no_side_effects(f"Paper decision rejected: {exc}")
    trade = _mapping(result.get("trade"))
    return (
        f"paper_decision mode={result.get('mode')} "
        f"id={trade.get('id') or 'n/a'} "
        f"decision_card_id={trade.get('decision_card_id') or decision_card_id} "
        f"ticker={trade.get('ticker') or active_plan.get('ticker') or 'n/a'} "
        f"decision={trade.get('decision') or decision.value} "
        f"state={trade.get('state') or 'n/a'} "
        "no_execution=true "
        f"external_calls={result.get('external_calls_made', 0)} "
        f"db_writes_required={result.get('db_writes_required', 0)} "
        f"db_writes_made={result.get('db_writes_made', 0)} "
        "broker_order_submitted=false order_submission_allowed=false. "
        f"{result.get('next_action') or 'Paper decision stayed local.'}"
    )


def _execute_workbench_order_ticket_command(
    engine: Engine,
    payload: Mapping[str, object],
    config: AppConfig,
    value: str,
) -> str:
    tokens_or_error = _command_tokens(value)
    if isinstance(tokens_or_error, str):
        return _command_no_side_effects(tokens_or_error)
    tokens = tokens_or_error
    mode = tokens[0].lower() if tokens else "preview"
    if mode in {"preview", "pre"}:
        execute = False
    elif mode in {"record", "execute", "save"}:
        execute = True
    else:
        return _command_no_side_effects(
            "Usage: order-ticket preview|record. The dashboard uses the current "
            "active trading plan only."
        )
    if len(tokens) > 1:
        return _command_no_side_effects(
            "Usage: order-ticket preview|record. Extra order-ticket parameters "
            "belong in a reviewed PowerShell command."
        )

    active_plan = _mapping(
        _mapping(payload.get("trading_workbench")).get("active_plan")
    )
    order_ticket = _mapping(active_plan.get("order_ticket"))
    ticker = str(
        order_ticket.get("ticker")
        or active_plan.get("ticker")
        or ""
    ).strip()
    side = str(order_ticket.get("side") or "").strip()
    entry_price = _optional_float(order_ticket.get("entry_price"))
    invalidation_price = _optional_float(order_ticket.get("invalidation_price"))
    if not ticker or not side or entry_price is None or invalidation_price is None:
        return _command_no_side_effects(
            "No complete active order ticket is available for the current plan."
        )
    available_at = (
        _datetime_or_none(order_ticket.get("available_at"))
        or _datetime_or_none(_mapping(active_plan.get("paper_decision")).get("available_at"))
        or datetime.now(UTC)
    )
    try:
        result = run_workbench_order_ticket(
            engine,
            ticker=ticker,
            side=side,
            entry_price=entry_price,
            invalidation_price=invalidation_price,
            risk_per_trade_pct=_optional_float(
                order_ticket.get("risk_per_trade_pct")
            ),
            config=config,
            available_at=available_at,
            execute=execute,
            actor_source="dashboard_tui",
            actor_id="local-tui",
            actor_role="analyst",
        )
    except OrderTicketExecutionError as exc:
        return _command_no_side_effects(f"Order ticket rejected: {exc}")
    preview = _mapping(result.get("preview"))
    ticket = _mapping(result.get("ticket"))
    hard_blocks = ",".join(_texts(preview.get("hard_blocks"))) or "none"
    return (
        f"order_ticket mode={result.get('mode')} "
        f"id={ticket.get('id') or 'n/a'} "
        f"ticker={preview.get('ticker') or ticker.upper()} "
        f"side={preview.get('side') or side.lower()} "
        f"status={ticket.get('status') or 'blocked'} "
        "submission_allowed=false "
        "broker_order_submitted=false "
        "no_execution=true "
        f"external_calls={result.get('external_calls_made', 0)} "
        f"db_writes_required={result.get('db_writes_required', 0)} "
        f"db_writes_made={result.get('db_writes_made', 0)} "
        f"proposed_shares={preview.get('proposed_shares', 0)} "
        f"hard_blocks={hard_blocks}. "
        f"{result.get('next_action') or 'Order ticket stayed local.'}"
    )


def _record_value_ledger_from_tui(
    engine: Engine,
    payload: Mapping[str, object],
    args: Sequence[str],
    *,
    available_at: datetime,
) -> str:
    args, execute = _strip_flag(args, "--execute")
    if len(args) < 6:
        return (
            "Usage: ledger record CANDIDATE_ID|TICKER|# LABEL "
            "watch|research|avoid|paper_trade|reject|live_review|no_action "
            "accepted|rejected|wait|ignored|paper-only|avoided|unknown "
            "VALUE_USD CONFIDENCE [--execute] [notes]"
        )
    selector, label, supported_action, user_decision, value_text, confidence_text = (
        args[0],
        args[1],
        args[2],
        args[3],
        args[4],
        args[5],
    )
    notes = " ".join(args[6:]).strip() or None
    try:
        estimated_value = float(value_text)
        confidence = float(confidence_text)
    except ValueError:
        return "Value ledger record rejected: value-usd and confidence must be numbers."
    artifact_id = _candidate_state_id_from_tui_selector(payload, selector)
    if artifact_id is None:
        return "Value ledger record rejected: candidate row not found."
    try:
        artifact_context = value_ledger_artifact_context(
            engine,
            artifact_type="candidate_state",
            artifact_id=artifact_id,
            available_at=available_at,
        )
        entry = build_value_ledger_entry(
            artifact_type="candidate_state",
            artifact_id=artifact_id,
            label=label,
            supported_action=supported_action,
            user_decision=user_decision,
            estimated_value_usd=estimated_value,
            confidence=confidence,
            source="dashboard_tui",
            available_at=available_at,
            notes=notes,
            artifact_context=artifact_context,
        )
        if execute:
            ValidationRepository(engine).upsert_value_ledger_entry(entry)
        plan = value_ledger_write_payload(entry, execute=execute)
    except ValueError as exc:
        return f"Value ledger record rejected: {exc}"
    entry_payload = _mapping(plan.get("entry"))
    return (
        f"Value ledger {plan.get('mode')}: "
        f"{entry_payload.get('ticker') or 'n/a'} {entry_payload.get('label')} "
        f"{entry_payload.get('supported_action') or 'n/a'} "
        f"value={entry_payload.get('estimated_value_usd')} "
        f"confidence={entry_payload.get('confidence')}; "
        f"id={entry_payload.get('id')}; "
        f"external_calls={plan.get('external_calls_made')} "
        f"db_writes={plan.get('db_writes_made')}. "
        f"{plan.get('next_action')}"
    )


def _update_value_outcome_from_tui(
    engine: Engine,
    args: Sequence[str],
    *,
    fallback_available_at: datetime,
) -> str:
    args, execute = _strip_flag(args, "--execute")
    if not args:
        return (
            "Usage: outcome update VALUE_LEDGER_ID "
            "OUTCOME_AVAILABLE_AT|filter [--execute] [sector ETF] "
            "[invalidation PRICE]"
        )
    ledger_id = args[0]
    remainder = list(args[1:])
    outcome_available_at = fallback_available_at
    if remainder and remainder[0].lower() not in {"filter", "current"}:
        parsed = _datetime_or_none(remainder[0])
        if parsed is None:
            return "Value outcome update rejected: invalid outcome-available-at."
        outcome_available_at = parsed
        remainder = remainder[1:]
    elif remainder:
        remainder = remainder[1:]
    sector_etf: str | None = None
    invalidation_price: float | None = None
    index = 0
    while index < len(remainder):
        token = remainder[index].lower()
        if token in {"sector", "--sector-etf", "--sector"} and index + 1 < len(remainder):
            sector_etf = remainder[index + 1].upper()
            index += 2
            continue
        if (
            token in {"invalidation", "--invalidation-price", "--invalidation"}
            and index + 1 < len(remainder)
        ):
            try:
                invalidation_price = float(remainder[index + 1])
            except ValueError:
                return "Value outcome update rejected: invalidation price must be numeric."
            index += 2
            continue
        return f"Value outcome update rejected: unknown option {remainder[index]}."
    try:
        plan = value_outcome_update_payload(
            engine,
            value_ledger_entry_id=ledger_id,
            outcome_available_at=outcome_available_at,
            execute=execute,
            sector_etf_ticker=sector_etf,
            invalidation_price=invalidation_price,
        )
    except ValueError as exc:
        return f"Value outcome update rejected: {exc}"
    outcome = _mapping(plan.get("outcome"))
    return (
        f"Value outcome {plan.get('mode')}: "
        f"{outcome.get('ticker') or 'n/a'} status={outcome.get('status')} "
        f"observed={outcome.get('trading_days_observed')} "
        f"20d={outcome.get('return_20d')} "
        f"follow_through={outcome.get('setup_follow_through')} "
        f"gap={outcome.get('gap_outcome')}; "
        f"id={outcome.get('id')}; "
        f"external_calls={plan.get('external_calls_made')} "
        f"db_writes={plan.get('db_writes_made')}. "
        f"{plan.get('next_action')}"
    )


def _value_ledger_coverage_message(payload: Mapping[str, object]) -> str:
    next_command = str(payload.get("canonical_next_command") or "").strip()
    command_text = f" next_command={next_command}" if next_command else ""
    return (
        "Value-ledger coverage: "
        f"status={payload.get('status')} "
        f"surfaced={payload.get('surfaced_candidate_count')} "
        f"logged={payload.get('logged_candidate_count')} "
        f"missing={payload.get('missing_ledger_count')} "
        f"coverage={payload.get('coverage_pct')}%; "
        f"external_calls={payload.get('external_calls_made')} "
        f"db_writes={payload.get('db_writes_made')}. "
        f"{payload.get('next_action')}"
        f"{command_text}"
    )


def _value_ledger_summary_message(payload: Mapping[str, object]) -> str:
    return (
        "Value-ledger summary: "
        f"entries={payload.get('entry_count')} "
        f"useful={payload.get('useful_entry_count')} "
        f"weighted_value={payload.get('confidence_weighted_value_usd')} "
        f"net={payload.get('net_confidence_weighted_value_usd')} "
        f"target_coverage={payload.get('target_coverage_pct')}%; "
        f"external_calls={payload.get('external_calls_made')} "
        f"db_writes={payload.get('db_writes_made')}."
    )


def _value_ledger_list_message(payload: Mapping[str, object]) -> str:
    entries = _rows(payload.get("entries"))
    ids = ", ".join(str(row.get("id") or "") for row in entries[:3] if row.get("id"))
    return (
        "Value-ledger list: "
        f"count={payload.get('count')} "
        f"shown={len(entries)} "
        f"first={ids or 'none'}; "
        f"external_calls={payload.get('external_calls_made')} "
        f"db_writes={payload.get('db_writes_made')}."
    )


def _value_ledger_entry_message(payload: Mapping[str, object]) -> str:
    entry = _mapping(payload.get("entry"))
    return (
        "Value-ledger entry: "
        f"{entry.get('ticker') or 'n/a'} {entry.get('label')} "
        f"action={entry.get('supported_action') or 'n/a'} "
        f"decision={entry.get('user_decision') or 'n/a'} "
        f"value={entry.get('estimated_value_usd')} "
        f"outcome={entry.get('outcome_status')}; "
        f"external_calls={payload.get('external_calls_made')} "
        f"db_writes={payload.get('db_writes_made')}."
    )


def _value_outcome_coverage_message(payload: Mapping[str, object]) -> str:
    next_command = str(payload.get("canonical_next_command") or "").strip()
    command_text = f" next_command={next_command}" if next_command else ""
    return (
        "Value-outcome coverage: "
        f"status={payload.get('status')} "
        f"ledger={payload.get('ledger_entry_count')} "
        f"linked={payload.get('linked_outcome_count')} "
        f"missing={payload.get('missing_outcome_count')} "
        f"coverage={payload.get('coverage_pct')}%; "
        f"external_calls={payload.get('external_calls_made')} "
        f"db_writes={payload.get('db_writes_made')}. "
        f"{payload.get('next_action')}"
        f"{command_text}"
    )


def _value_outcome_list_message(payload: Mapping[str, object]) -> str:
    outcomes = _rows(payload.get("outcomes"))
    ids = ", ".join(str(row.get("id") or "") for row in outcomes[:3] if row.get("id"))
    return (
        "Value-outcome list: "
        f"count={payload.get('count')} "
        f"status_counts={payload.get('status_counts')} "
        f"first={ids or 'none'}; "
        f"external_calls={payload.get('external_calls_made')} "
        f"db_writes={payload.get('db_writes_made')}."
    )


def _value_outcome_message(payload: Mapping[str, object]) -> str:
    outcome = _mapping(payload.get("outcome"))
    return (
        "Value outcome: "
        f"{outcome.get('ticker') or 'n/a'} status={outcome.get('status')} "
        f"5d={outcome.get('return_5d')} 20d={outcome.get('return_20d')} "
        f"follow_through={outcome.get('setup_follow_through')} "
        f"gap={outcome.get('gap_outcome')}; "
        f"external_calls={payload.get('external_calls_made')} "
        f"db_writes={payload.get('db_writes_made')}."
    )


def _candidate_state_id_from_tui_selector(
    payload: Mapping[str, object],
    selector: str,
) -> str | None:
    rows = _candidate_rows(payload)
    if selector.isdigit():
        index = int(selector) - 1
        if 0 <= index < len(rows):
            return str(rows[index].get("id") or "").strip() or None
    selector_upper = selector.strip().upper()
    for row in rows:
        row_id = str(row.get("id") or "").strip()
        ticker = str(row.get("ticker") or "").strip().upper()
        if selector == row_id or selector_upper == ticker:
            return row_id or None
    return selector.strip() or None


def _command_tokens(value: str) -> list[str] | str:
    try:
        return shlex.split(value)
    except ValueError as exc:
        return f"Command parse rejected: {exc}"


def _strip_flag(args: Sequence[str], flag: str) -> tuple[list[str], bool]:
    values: list[str] = []
    found = False
    for arg in args:
        if arg == flag:
            found = True
            continue
        values.append(arg)
    return values, found


def _first_positive_int(args: Sequence[str], *, default: int) -> int:
    for arg in args:
        try:
            value = int(arg)
        except ValueError:
            continue
        if value > 0:
            return value
    return default


def _value_ledger_usage() -> str:
    return (
        "Usage: ledger coverage | ledger summary | ledger list [limit] | "
        "ledger show ID | ledger record CANDIDATE_ID|TICKER|# LABEL "
        "SUPPORTED_ACTION USER_DECISION VALUE_USD CONFIDENCE "
        "[--execute] [notes]"
    )


def _value_outcome_usage() -> str:
    return (
        "Usage: outcome coverage | outcome list [ledger-id|all] [limit] | "
        "outcome show ID | outcome update LEDGER_ID "
        "OUTCOME_AVAILABLE_AT|filter [--execute]"
    )


def _open_target_page(
    payload: Mapping[str, object],
    page: str,
    value: str,
) -> str | None:
    next_page: str | None = None
    if page in {"overview", "review"}:
        rows = (
            _priced_in_review_rows(payload)
            if page == "review"
            else _priced_in_overview_rows(payload)
        )
        row = _row_by_index_or_key(rows, value, key="ticker")
        ticker = str(row.get("ticker") or "").strip().upper() if row else ""
        next_page = f"candidate:{ticker}" if ticker else None
    elif page == "candidates":
        rows = _candidate_rows(payload)
        row = _row_by_index_or_key(rows, value, key="ticker")
        ticker = str(row.get("ticker") or "").strip().upper() if row else ""
        next_page = f"candidate:{ticker}" if ticker else None
    elif page == "alerts":
        rows = _rows(_mapping(payload.get("alerts")).get("rows"))
        row = _row_by_index_or_key(rows, value, key="id")
        alert_id = str(row.get("id") or "").strip() if row else ""
        next_page = f"alert:{alert_id}" if alert_id else None
    if next_page or value.strip().isdigit():
        return next_page
    return _global_open_target_page(payload, value)


def _global_open_target_page(
    payload: Mapping[str, object],
    value: str,
) -> str | None:
    for rows in (
        _priced_in_overview_rows(payload),
        _priced_in_review_rows(payload),
        _candidate_rows(payload),
    ):
        row = _row_by_index_or_key(rows, value, key="ticker")
        ticker = str(row.get("ticker") or "").strip().upper() if row else ""
        if ticker:
            return f"candidate:{ticker}"
    alert_rows = _rows(_mapping(payload.get("alerts")).get("rows"))
    alert = _row_by_index_or_key(alert_rows, value, key="id")
    alert_id = str(alert.get("id") or "").strip() if alert else ""
    return f"alert:{alert_id}" if alert_id else None


def _open_command_no_match_message(page: str, value: str) -> str:
    token = value.strip()
    if not token:
        return (
            "Open command needs a target. No calls made. Type open TICKER, "
            "open ALERT_ID, or use row numbers on Inbox, Candidate Review, or Alerts."
        )
    if token.isdigit():
        page_label = _page_display_label(page) or "this page"
        return (
            f"No row {token} is openable on {page_label}. No calls made. "
            "Use row numbers on Inbox, Candidate Review, or Alerts; from any page "
            "type open TICKER or open ALERT_ID."
        )
    return (
        f"No local candidate or alert matched {token}. No calls made. Try open TICKER, "
        "open ALERT_ID, or refresh if you expected it in the latest scan."
    )


def _unsupported_filter_values(
    values: Sequence[str],
    *,
    allowed: Sequence[str],
) -> tuple[str, ...]:
    allowed_values = set(allowed)
    return tuple(value for value in values if value not in allowed_values)


def _unsupported_gap_filter_message(
    command: str,
    invalid_values: Sequence[str],
    *,
    allowed: Sequence[str],
) -> str:
    invalid = ", ".join(invalid_values)
    allowed_text = ", ".join(allowed)
    return (
        f"Unsupported {command} value: {invalid}. No calls made; filter unchanged. "
        f"Use all or one of: {allowed_text}."
    )


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
        "ready"
        if bool(answer.get("decision_ready"))
        and audit_status not in {"blocked", "attention"}
        else "not ready"
    )
    view_label = _priced_in_view_label(payload)
    lines = [_rule("Market Radar Terminal Dashboard", width, char="=")]
    lines.extend(
        _wrap(
            (
                f"Page: {_page_display_label(page, payload)} | "
                f"View: {view_label} | "
                f"Answer: {_human_label(answer_status)} ({answer_ready}) | "
                f"Trade status: {_human_label(readiness.get('status') or 'unknown')} | "
                f"Trade safe: {_decision_label(readiness)} | "
                f"External calls made: {_text(payload.get('external_calls_made', 0))}"
            ),
            width,
        )
    )
    lines.extend(
        _wrap(
            (
                f"DB: {_nested(runtime, 'database', 'name') or 'n/a'} | "
                f"Build: {_nested(runtime, 'build', 'commit') or 'n/a'} | "
                f"Ticker: {controls.get('ticker') or 'all'} | "
                f"Cutoff: {controls.get('available_at') or 'latest'}"
            ),
            width,
        )
    )
    lines.extend(_wrap(NAVIGATION_TEXT, width))
    return lines


def _page_display_label(
    page: str,
    payload: Mapping[str, object] | None = None,
) -> str:
    normalized = _normalize_page(page)
    if normalized.startswith("candidate:"):
        ticker = normalized.split(":", 1)[1].strip().upper()
        return f"Candidate {ticker}" if ticker else "Candidate"
    if normalized.startswith("alert:"):
        alert_id = normalized.split(":", 1)[1].strip()
        row = _alert_detail_row(payload or {}, alert_id)
        if row:
            return _alert_display_title(row, alert_id)
        return f"Alert {alert_id}" if alert_id else "Alert"
    labels = {page_key: label for page_key, _, label in MODERN_PAGES}
    labels.update(
        {
            "themes": "Themes",
            "validation": "Validation",
            "costs": "Costs",
        }
    )
    return labels.get(normalized, _human_label(normalized) or "Help")


def _priced_in_view_label(payload: Mapping[str, object]) -> str:
    queue = _mapping(payload.get("priced_in_queue"))
    filters = _mapping(queue.get("filters"))
    status = str(filters.get("status") or "all").strip().lower()
    usefulness = str(filters.get("usefulness") or "").strip().lower()
    if status in {"", "all"}:
        return "All scanned rows" if _priced_in_scan_scope_is_partial(payload) else "Full scan"
    if status == "actionable" and usefulness == "decision_useful":
        return "Decision-ready filter"
    if status == "actionable":
        return "Mismatches filter"
    return f"{_human_label(status)} filter"


def _all_scan_rows_mode_message(payload: Mapping[str, object]) -> str:
    if _priced_in_scan_scope_is_partial(payload):
        return (
            "All Scanned Rows mode: showing the current scan page; coverage line "
            "shows this is not full-market coverage yet."
        )
    return "Full Scan mode: showing review page 1; coverage line shows the scan universe."


def _priced_in_scan_scope_is_partial(payload: Mapping[str, object]) -> bool:
    answer = _mapping(payload.get("priced_in_answer"))
    full_scan = _mapping(answer.get("full_scan"))
    active = int(_number_or_zero(full_scan.get("active_securities")))
    scanned = int(_number_or_zero(full_scan.get("scanned_rows")))
    if active > 0 and scanned > 0 and scanned < active:
        return True
    queue = _mapping(payload.get("priced_in_queue"))
    return str(queue.get("status") or "").strip() in {
        "selected_universe",
        "partial_scan",
        "universe_too_small",
    }


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
    for label, value in _run_source_status_display_items(
        _run_mission_brief_items(payload)
    ):
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


def _tutorial_control_rows(
    payload: Mapping[str, object] | None = None,
) -> list[Mapping[str, object]]:
    if payload is not None and _real_results_empty(payload):
        return [
            {
                "step": "1",
                "do": "Read Mission setup command",
                "result": (
                    "If you accept the call/write, run it in PowerShell."
                ),
            },
            {
                "_target_page": "readiness",
                "step": "2",
                "do": "Press 2: Evidence Gaps",
                "result": "Verify the first blocker and setup cost.",
            },
            {
                "_target_page": "run",
                "step": "3",
                "do": "Press 3: Safe Run",
                "result": "After setup, review one capped scan before executing.",
            },
            {
                "_target_page": "overview",
                "step": "4",
                "do": "Press 1: Inbox",
                "result": "After a real scan, read the insight queue.",
            },
            {
                "_target_page": "candidates",
                "step": "5",
                "do": "Press 4: Candidate Review",
                "result": "Review companies. These are research rows, not trade signals.",
            },
            {
                "step": "6",
                "do": "Use the bottom command box",
                "result": (
                    "Dashboard commands only; run setup commands in PowerShell."
                ),
            },
        ]
    return [
        {
            "_target_page": "overview",
            "step": "1",
            "do": "Press 1: Inbox",
            "result": "See the current insight queue: ticker, signal, why, and action.",
        },
        {
            "_target_page": "readiness",
            "step": "2",
            "do": "Press 2: Evidence Gaps",
            "result": "See exactly what blocks a decision-useful workflow.",
        },
        {
            "_target_page": "review",
            "step": "3",
            "do": "Press D: Decision Review",
            "result": "Show only not-priced-in rows that passed the usefulness gate.",
        },
        {
            "_target_page": "candidates",
            "step": "4",
            "do": "Press 4: Candidate Review",
            "result": "Review companies. These are research rows, not trade signals.",
        },
        {
            "_target_page": "run",
            "step": "5",
            "do": "Press 3: Safe Run",
            "result": "Review external-call budget before running anything.",
        },
        {
            "step": "6",
            "do": "Use the bottom command box",
            "result": (
                "Dashboard commands only; run setup commands in PowerShell."
            ),
        },
    ]


def _tutorial_caption(payload: Mapping[str, object]) -> str:
    prefix = (
        "Read WHY/NOW/NEXT first. "
        if _tutorial_mission_rows(payload)
        else ""
    )
    return f"{prefix}Safe rule: clicks and filters make 0 provider calls."


def _tutorial_lines(payload: Mapping[str, object], width: int) -> list[str]:
    lines: list[str] = []
    mission_items = _run_source_status_display_items(_run_mission_brief_items(payload))
    if mission_items:
        lines.append(_rule("Mission - why this exists", width))
        lines.extend(_kv_lines(mission_items, width=width))
        lines.append("")
    lines.append(_rule("Tutorial - your first 90 seconds", width))
    lines.extend(
        _table_lines(
            _tutorial_control_rows(payload),
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
                    f"Opened full-scan row {row_number} for {ticker}. No calls. "
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
    routed_raw = {
        str(item)
        for item in _rows_or_values(usefulness.get("routed_optional_sources"))
        if str(item).strip()
    }
    missing = [
        _human_source_name(item)
        for item in _rows_or_values(data_sources.get("missing"))
        if str(item).strip() and str(item) not in routed_raw
    ]
    stale = [
        _human_source_name(item)
        for item in _rows_or_values(data_sources.get("stale"))
        if str(item).strip() and str(item) not in routed_raw
    ]
    routed = [_human_source_name(item) for item in sorted(routed_raw)]
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


def _market_inbox_rows(payload: Mapping[str, object]) -> list[Mapping[str, object]]:
    messages: list[Mapping[str, object]] = []
    overview_rows = _priced_in_overview_rows(payload)
    if not overview_rows and _real_results_empty(payload):
        return _first_scan_setup_rows(payload)
    for row in overview_rows:
        ticker = str(row.get("ticker") or "").strip().upper()
        signal = str(row.get("signal") or "Market signal").strip()
        subject = _market_inbox_subject(signal)
        missing = str(row.get("data_coverage") or "unknown").strip() or "unknown"
        next_action = str(row.get("next_action") or "").strip()
        if next_action == "Open candidate detail and review the evidence.":
            next_action = "Open the case file and review evidence."
        mailbox = _market_inbox_mailbox(row)
        if mailbox == "Waiting Evidence":
            next_action = "Evidence Gaps first."
            status_message = (
                f"Evidence Gaps first. No calls. {ticker} is waiting on evidence."
            )
        elif mailbox == "Blocked":
            next_action = "Open Evidence Gaps first; clear blockers."
            status_message = (
                f"Blocked case: {ticker}. No calls. Press 2 Evidence Gaps first."
            )
        elif mailbox == "Worth Reading":
            next_action = "Open case; verify gaps."
            status_message = (
                f"Worth reading: {ticker}. No calls. Open the case file, then "
                "verify missing evidence before action."
            )
        else:
            status_message = (
                f"Opened Market Inbox case for {ticker}. No calls. "
                "Review evidence before any action."
            )
        message = dict(row)
        message.update(
            {
                "mailbox": mailbox,
                "subject": subject,
                "why": _market_inbox_why(row),
                "missing": "No current data gap" if missing == "none" else missing,
                "next": next_action or "Open the case file and review evidence.",
                "status_message": status_message,
            }
        )
        messages.append(message)
    return messages


def _market_inbox_subject(signal: str) -> str:
    text = " ".join(str(signal or "Market signal").split())
    replacements = {
        "bullish_not_priced_in": "Bullish not priced in",
        "bearish_not_priced_in": "Bearish not priced in",
        "fully_priced": "Fully priced",
        "overextended_hype": "Overextended hype",
        "no_mismatch": "No mismatch",
    }
    normalized = text.strip().lower().replace("-", "_").replace(" ", "_")
    text = replacements.get(normalized, text)
    if " - gap " in text.lower():
        text = text[: text.lower().find(" - gap ")].strip()
    return _clip(text, 40)


def _market_inbox_why(row: Mapping[str, object]) -> str:
    emotion = row.get("emotion_score")
    reaction = row.get("reaction_score")
    if emotion not in (None, "") and reaction not in (None, ""):
        return (
            f"mood {_format_market_inbox_score(emotion)} vs "
            f"price {_format_market_inbox_score(reaction)}"
        )
    why_now = str(row.get("why_now") or "").strip()
    return why_now or "No explanation recorded."


def _format_market_inbox_score(value: object) -> str:
    number = _number_or_zero(value)
    text = f"{number:.1f}"
    return text.rstrip("0").rstrip(".")


def _first_scan_setup_rows(payload: Mapping[str, object]) -> list[Mapping[str, object]]:
    answer = _mapping(payload.get("priced_in_answer"))
    trust_gate = _mapping(answer.get("full_market_trust_gate"))
    ladder_rows = _rows(_mapping(trust_gate.get("blocker_ladder")).get("rows"))
    if not ladder_rows:
        ladder_rows = [
            {
                "source": "universe",
                "next_action": "Create the scan universe before expecting results.",
                "plan_command": answer.get("canonical_next_command"),
                "status": "blocked",
            },
            {
                "source": "market_bars",
                "next_action": "Fill fresh prices for the scan universe.",
                "status": "blocked",
            },
            {
                "source": "scan",
                "next_action": "Run one capped scan after the data blockers clear.",
                "status": "blocked",
            },
        ]
    else:
        ladder_rows = list(ladder_rows)
    if not any(str(row.get("source") or "") == "scan" for row in ladder_rows):
        scan_row = {
            "source": "scan",
            "next_action": "Run one capped scan after the data blockers clear.",
            "status": "blocked",
        }
        insert_at = len(ladder_rows)
        for index, row in enumerate(ladder_rows):
            if str(row.get("source") or "") == "agent_review":
                insert_at = index
                break
        ladder_rows.insert(insert_at, scan_row)
    rows: list[Mapping[str, object]] = []
    for index, row in enumerate(ladder_rows[:4], start=1):
        source = str(row.get("source") or f"setup_step_{index}").strip()
        command = str(
            _first_nonblank(
                row.get("plan_command"),
                row.get("command"),
                row.get("execute_next_command"),
                answer.get("canonical_next_command") if index == 1 else None,
            )
            or ""
        ).strip()
        next_action = str(row.get("next_action") or "").strip()
        source_label = _human_label(source)
        setup_next = _first_scan_setup_next(source, command, next_action)
        status_next = (
            f"PowerShell setup command: {command}"
            if source == "universe" and command
            else setup_next
        )
        rows.append(
            {
                "_row_key": f"first-scan-{index}-{source}",
                "mailbox": "Setup",
                "ticker": "-",
                "subject": _first_scan_setup_subject(source, index),
                "why": _first_scan_setup_why(source, next_action),
                "missing": source_label,
                "next": setup_next,
                "target_page": _first_scan_setup_target(source),
                "status_message": (
                    f"Setup step {index}: {source_label}. No calls were made. "
                    f"{status_next}"
                ),
                "status": row.get("status") or "blocked",
                "source": source,
                "command": command,
            }
        )
    return rows


def _first_scan_setup_subject(source: str, index: int) -> str:
    subjects = {
        "universe": "1. Build the stock universe",
        "market_bars": "2. Fill latest prices",
        "catalyst_events": "3. Add catalyst evidence",
        "local_text": "3. Add narrative evidence",
        "agent_review": "4. Add the AI review",
        "scan": "3. Run one capped scan",
    }
    return subjects.get(source, f"{index}. Set up {(_human_label(source) or 'setup')}")


def _first_scan_setup_why(source: str, next_action: str) -> str:
    why_by_source = {
        "universe": "MarketRadar has no active stock list to scan yet.",
        "market_bars": "It cannot compare mood with price reaction until prices are fresh.",
        "catalyst_events": "It needs real events before judging market emotion.",
        "local_text": "Narrative evidence is not ready for the scan date.",
        "agent_review": "AI review is optional and must stay explicitly budget-gated.",
        "scan": "No priced-in scan rows exist yet.",
    }
    return why_by_source.get(source, next_action or "This setup blocker is still open.")


def _first_scan_setup_next(source: str, command: str, next_action: str) -> str:
    if source == "universe" and command:
        return "Run PowerShell command above."
    if source == "market_bars":
        return "Use Evidence Gaps for bars."
    if source == "agent_review":
        return "Approve budget in Agent Coach."
    if source == "scan":
        return "Review budget in Safe Run."
    return next_action or "Open Evidence Gaps and set up this blocker first."


def _first_scan_setup_target(source: str) -> str:
    if source == "agent_review":
        return "agent"
    if source in {"scan", "catalyst_events", "local_text"}:
        return "run"
    return "readiness"


def _market_inbox_mailbox(row: Mapping[str, object]) -> str:
    usefulness = _mapping(row.get("usefulness"))
    usefulness_status = str(usefulness.get("status") or "").strip().lower()
    signal = str(row.get("signal") or "").strip().lower()
    coverage = str(row.get("data_coverage") or "").strip().lower()
    decision_ready = (
        bool(usefulness.get("decision_ready"))
        or usefulness_status == "decision_useful"
    )
    blocked = bool(row.get("blocked")) or "blocked" in signal
    waiting_for_evidence = (
        coverage not in {"", "none"}
        and any(token in coverage for token in ("missing", "stale", "unknown"))
    )
    mismatch = "not priced" in signal or (
        "mismatch" in signal and not signal.startswith("no mismatch")
    )
    if decision_ready and not blocked:
        return "Urgent"
    if blocked:
        return "Blocked"
    if usefulness_status in {"research_useful", "watch_useful"} or mismatch:
        return "Worth Reading"
    if waiting_for_evidence:
        return "Waiting Evidence"
    return "Monitor"


def _market_inbox_counts(
    rows: Sequence[Mapping[str, object]],
) -> Mapping[str, int]:
    counts = {
        "Urgent": 0,
        "Worth Reading": 0,
        "Waiting Evidence": 0,
        "Blocked": 0,
        "Monitor": 0,
    }
    for row in rows:
        mailbox = str(row.get("mailbox") or "Monitor")
        counts[mailbox] = counts.get(mailbox, 0) + 1
    return counts


def _market_inbox_count_summary(counts: Mapping[str, int]) -> str:
    total = sum(counts.values())
    if total <= 0:
        return ""
    visible_labels = ("Urgent", "Worth Reading", "Blocked", "Waiting Evidence")
    parts = [
        f"{counts[label]} {label.lower()}"
        for label in visible_labels
        if counts.get(label)
    ]
    has_priority_parts = bool(parts)
    if not parts:
        parts.append(f"{total} message(s)")
    priority_total = sum(
        counts.get(label, 0)
        for label in visible_labels
    )
    if has_priority_parts and total > priority_total:
        parts.append(f"{total} total")
    return ", ".join(parts)


def _market_inbox_metric_summary(payload: Mapping[str, object]) -> tuple[str, str]:
    queue = _mapping(payload.get("priced_in_queue"))
    rows = _market_inbox_rows(payload)
    if _real_results_empty(payload) and rows:
        return (
            "setup checklist",
            f"{len(rows)} setup row(s); 0 stock results",
        )
    loaded = len(rows)
    returned = int(
        _number_or_zero(queue.get("returned_count") or queue.get("count") or loaded)
    )
    if not loaded and returned:
        loaded = returned
    total = int(_number_or_zero(queue.get("total_count")))
    offset = int(_number_or_zero(queue.get("offset")))
    value = ""
    if total and loaded and (offset > 0 or loaded < total):
        value = f"{loaded:,} loaded / {total:,} total"
    elif total:
        value = f"{total:,} messages"
    elif loaded:
        value = f"{loaded:,} loaded"

    loaded_summary = _market_inbox_count_summary(_market_inbox_counts(rows))
    usefulness = _usefulness_counts_summary(queue)
    detail_parts: list[str] = []
    if loaded_summary:
        detail_parts.append(f"loaded page: {loaded_summary}")
    if usefulness:
        detail_parts.append(f"queue: {usefulness}")
    return value, "; ".join(detail_parts)


def _market_inbox_scope_summary(payload: Mapping[str, object]) -> str:
    queue = _mapping(payload.get("priced_in_queue"))
    rows = _market_inbox_rows(payload)
    if _real_results_empty(payload) and rows:
        return f"Setup checklist: {len(rows)} instruction row(s), 0 stock result rows"
    parts: list[str] = []
    visible_summary = _market_inbox_count_summary(_market_inbox_counts(rows))
    if visible_summary:
        parts.append(f"Visible page: {visible_summary}")
    total = int(_number_or_zero(queue.get("total_count")))
    usefulness = _usefulness_counts_summary(queue)
    if total:
        queue_summary = f"Queue total: {total:,}"
        if usefulness:
            queue_summary = f"{queue_summary}; {usefulness}"
        parts.append(queue_summary)
    elif usefulness:
        parts.append(f"Queue mix: {usefulness}")
    return ". ".join(parts)


def _market_inbox_next_safe_action(payload: Mapping[str, object]) -> str:
    rows = _market_inbox_rows(payload)
    if not rows:
        return (
            "No scan messages yet. Import/fetch market data, then run a capped scan "
            "before treating this as insight."
        )
    if _real_results_empty(payload):
        blocker = _readiness_first_setup_blocker(payload)
        area = _human_source_name(blocker.get("area") if blocker else "Active universe")
        return (
            f"{_setup_blocker_first_label(area)}. Open Evidence Gaps for "
            "blockers; only run provider commands intentionally."
        )
    counts = _market_inbox_counts(rows)
    urgent = counts.get("Urgent", 0)
    worth_reading = counts.get("Worth Reading", 0)
    waiting = counts.get("Waiting Evidence", 0)
    blocked = counts.get("Blocked", 0)
    visible_total = sum(counts.values())
    if urgent:
        return "Open first Urgent message. Verify evidence."
    if worth_reading:
        return "Open first Worth Reading row. Research only."
    if waiting and waiting == visible_total:
        return (
            "Press 2 Evidence Gaps first. These rows are not trade ideas until "
            "blockers clear."
        )
    if waiting:
        return (
            "Skip Waiting Evidence rows unless you are repairing data. Open any "
            "Worth Reading or Urgent rows first."
        )
    if blocked:
        return (
            "Rows are blocked. Open Evidence Gaps before relying on this scan."
        )
    return (
        "Monitor only. Do nothing until new evidence creates an Urgent or Worth "
        "Reading message."
    )


def _candidate_case_next_safe_action(payload: Mapping[str, object], ticker: str) -> str:
    ticker = ticker.strip().upper()
    row = _candidate_detail_row(payload, ticker)
    if not row:
        return (
            f"{ticker or 'Candidate'} is not visible in the current scan filters. "
            "Return to Inbox or clear filters before acting."
        )
    brief = _mapping(row.get("priced_in_evidence_brief"))
    top_evidence = ""
    if brief:
        evidence_rows = _rows(brief.get("evidence"))
        top_evidence = str(
            _first_nonblank(
                *[item.get("title") for item in evidence_rows[:1] if item.get("title")],
                brief.get("top_catalyst"),
            )
            or ""
        ).strip()
    next_step = str(
        _first_nonblank(
            brief.get("next_step") if brief else None,
            row.get("priced_in_next_step"),
            row.get("next_step"),
            row.get("decision_next_step"),
        )
        or ""
    ).strip()
    source_gaps = _candidate_case_source_gap_summary(row, brief)
    has_source_gaps = source_gaps not in {"", "none", "n/a"}
    evidence_target = f"Top evidence: {_clip(top_evidence, 52)}. " if top_evidence else ""
    if has_source_gaps:
        gap_labels = _candidate_case_source_gap_labels(row, brief)
        gap_label = ", ".join(gap_labels[:4]) if gap_labels else "listed evidence"
        if len(gap_labels) > 4:
            gap_label = f"{gap_label}, +{len(gap_labels) - 4} more"
        action_gap_label = _candidate_case_source_gap_action_label(gap_labels)
        return (
            f"{ticker}: Open 2 Evidence Gaps for global blockers; add "
            f"{action_gap_label or gap_label} evidence. No packet yet."
        )
    if next_step:
        return (
            f"{ticker}: no trade decision yet. Verify evidence first. "
            f"{evidence_target}Then: {_clip(next_step, 80)}."
        )
    return (
        f"{ticker}: no trade decision yet. Verify evidence first, then return to "
        "Inbox or Decision Review."
    )


def _candidate_case_source_gap_labels(
    row: Mapping[str, object],
    brief: Mapping[str, object],
) -> list[str]:
    labels: list[str] = []

    def add_label(value: object) -> None:
        label = _human_source_name(value).strip().lower()
        if label and label not in labels:
            labels.append(label)

    if brief:
        for action in _rows(brief.get("source_actions")):
            if str(action.get("status") or "") in {"ready", "not_applicable"}:
                continue
            add_label(action.get("source"))
    data_sources = row.get("priced_in_data_sources") or row.get("data_sources")
    if isinstance(data_sources, Mapping):
        for value in [*_texts(data_sources.get("missing")), *_texts(data_sources.get("stale"))]:
            add_label(value)
    return labels


def _candidate_case_source_gap_action_label(gap_labels: Sequence[str]) -> str:
    compact: list[str] = []
    for label in gap_labels[:3]:
        action_label = label.replace("broker context", "broker")
        action_label = action_label.replace("catalyst events", "catalysts")
        if action_label and action_label not in compact:
            compact.append(action_label)
    if not compact:
        return ""
    summary = "/".join(compact)
    if len(gap_labels) > 3:
        summary = f"{summary}/+{len(gap_labels) - 3} more"
    return summary


def _candidate_case_source_gap_summary(
    row: Mapping[str, object],
    brief: Mapping[str, object],
) -> str:
    if brief:
        source_actions = _candidate_source_action_summary(brief)
        if source_actions not in {"", "none", "n/a"}:
            return source_actions
    data_sources = row.get("priced_in_data_sources") or row.get("data_sources")
    if not isinstance(data_sources, Mapping):
        return "none"
    missing = _texts(data_sources.get("missing"))
    stale = _texts(data_sources.get("stale"))
    parts: list[str] = []
    if missing:
        missing_labels = [_human_source_name(value).lower() for value in missing[:3]]
        parts.append(f"missing {', '.join(missing_labels)}")
    if stale:
        stale_labels = [_human_source_name(value).lower() for value in stale[:3]]
        parts.append(f"stale {', '.join(stale_labels)}")
    return "; ".join(parts) if parts else "none"


def _readiness_first_work_item(payload: Mapping[str, object]) -> Mapping[str, object]:
    if _real_results_empty(payload):
        setup_blocker = _readiness_first_setup_blocker(payload)
        if setup_blocker:
            return setup_blocker
    queue = _mapping(payload.get("operator_work_queue"))
    rows = _rows(queue.get("rows"))
    priority_order = {"must_fix": 0, "blocked": 1, "attention": 2, "research": 3}
    ordered_rows = sorted(
        rows,
        key=lambda row: priority_order.get(
            str(row.get("priority") or "").strip().lower(),
            9,
        ),
    )
    return ordered_rows[0] if ordered_rows else {}


def _readiness_first_setup_blocker(payload: Mapping[str, object]) -> Mapping[str, object]:
    shadow = _mapping(payload.get("shadow_readiness"))
    for row in _rows(shadow.get("checks")):
        status = str(row.get("status") or "").strip().lower()
        if status not in {"blocked", "attention", "setup_required"}:
            continue
        code = str(row.get("code") or "").strip()
        area = str(row.get("area") or code or "Setup blocker").strip()
        return {
            **dict(row),
            "priority": "setup",
            "area": area,
            "item": row.get("finding") or area,
            "next_action": _readiness_setup_next_action(row),
        }
    return {}


def _readiness_setup_next_action(row: Mapping[str, object]) -> str:
    code = str(row.get("code") or "").strip().lower()
    row_action = _humanize_dashboard_text(row.get("next_action")).strip()
    if code == "active_universe":
        if row_action:
            return (
                f"{row_action} Run setup commands in PowerShell, not in the "
                "dashboard command box. Continue only if you accept the data "
                "change or provider call."
            )
        return (
            "Seed or refresh the stock universe intentionally only after approving "
            "the data change or provider call."
        )
    if code == "latest_market_bars":
        return row_action or "Fill latest price bars after the universe exists."
    if code == "scan_scope":
        return row_action or "Run one capped scan after the universe and prices are ready."
    if code == "trust_gate":
        return row_action or "Clear the first trust-gate blocker before treating rows as insight."
    return row_action


def _readiness_next_safe_action(
    payload: Mapping[str, object],
    *,
    command_first: bool = True,
) -> str:
    setup_footer = _setup_command_footer_action(payload)
    if setup_footer:
        return setup_footer
    row = _readiness_first_work_item(payload)
    if row:
        priority = str(row.get("priority") or "gap").replace("_", " ")
        area = str(row.get("area") or row.get("item") or "Evidence gap").strip()
        action = str(row.get("next_action") or row.get("action") or "").strip()
        command = _first_backticked_command(action)
        action_text = _text_without_backticked_command(action, command)
        lines = []
        if command and command_first:
            lines.append(f"Use `{command}`.")
        suffix = f" {action_text}" if action_text else ""
        lines.append(
            f"Research-only. First {priority}: "
            f"{area}.{suffix}"
        )
        if command and not command_first:
            lines.append(f"Use `{command}`.")
        return "\n".join(lines)
    readiness = _mapping(payload.get("readiness"))
    next_action = str(readiness.get("next_action") or "").strip()
    if next_action:
        command = _first_backticked_command(next_action)
        action_text = _text_without_backticked_command(next_action, command)
        lines = []
        if command and command_first:
            lines.append(f"Use `{command}`.")
        suffix = f": {action_text}" if action_text else "."
        lines.append(
            "Research-only until clear. Clear readiness before acting"
            f"{suffix}"
        )
        if command and not command_first:
            lines.append(f"Use `{command}`.")
        return "\n".join(lines)
    return "No evidence gaps are listed. Return to Inbox or Decision Review."


def _first_backticked_command(value: str) -> str:
    _, separator, rest = value.partition("`")
    if not separator:
        return ""
    command, closing, _ = rest.partition("`")
    if not closing:
        return ""
    return command.strip()


def _text_without_backticked_command(value: str, command: str) -> str:
    text = value.strip()
    if command:
        text = text.replace(f"with `{command}`", "with the command above")
        text = text.replace(f"`{command}`", "the command above").strip()
    return " ".join(text.split())


def _readiness_row_status_message(row: Mapping[str, object]) -> str:
    area = str(row.get("area") or row.get("code") or row.get("item") or "Evidence gap")
    status = str(row.get("status") or row.get("priority") or "needs review")
    finding = str(row.get("finding") or row.get("item") or "").strip()
    next_action = _humanize_dashboard_text(
        row.get("next_action") or row.get("action") or ""
    ).strip()
    finding_text = f" Finding: {_clip(finding, 90)}" if finding else ""
    next_text = f" Next: {_clip(next_action, 110)}" if next_action else ""
    return (
        f"No calls. Research-only blocker selected: {area} ({status})."
        f"{finding_text}{next_text}"
    )


def _tutorial_row_status_message(
    row: Mapping[str, object],
    *,
    target_page: str = "",
) -> str:
    step = str(row.get("step") or "Tutorial").strip()
    action = str(row.get("do") or "Read this row").strip()
    result = str(row.get("result") or "").strip()
    route = f" Opened {target_page}." if target_page else ""
    result_text = f" Result: {_clip(result, 76)}" if result else ""
    return f"Tutorial row selected: No calls. {step} - {_clip(action, 42)}.{route}{result_text}"


def _detail_row_status_message(kind: str, row: Mapping[str, object]) -> str:
    question = str(row.get("key") or "Detail").strip()
    answer = str(row.get("value") or "No answer captured.").strip()
    return f"{kind} selected: No calls. {question}: {_clip(answer, 86)}"


def _run_row_status_message(row: Mapping[str, object]) -> str:
    layer = str(row.get("layer") or row.get("name") or "Run layer").strip()
    provider = str(row.get("provider") or "n/a").strip()
    status = str(row.get("status") or "review").strip()
    calls = int(_number_or_zero(row.get("external_call_count_max")))
    next_action = str(row.get("next_action") or "").strip()
    next_text = f" Next: {_clip(next_action, 84)}" if next_action else ""
    return (
        f"Run layer selected: {layer}. No call made. To spend calls, type "
        f"run execute. Provider {provider}; status {status}; max calls {calls}."
        f"{next_text}"
    )


def _agent_row_status_message(row: Mapping[str, object]) -> str:
    kind = str(row.get("kind") or "Agent").strip()
    item = str(row.get("item") or "step").strip()
    detail = str(row.get("detail") or "").strip()
    detail_text = f" Detail: {_clip(detail, 96)}" if detail else ""
    if bool(row.get("_setup_locked")):
        return (
            f"Agent setup row selected: {kind} / {item}. No calls made; "
            "clear Evidence Gaps before using agent execute."
            f"{detail_text}"
        )
    return (
        f"Agent step selected: {kind} / {item}. No calls made; "
        f"agent execute is required to spend OpenAI budget.{detail_text}"
    )


def _ipo_row_status_message(row: Mapping[str, object]) -> str:
    ticker = str(row.get("ticker") or row.get("proposed_ticker") or "n/a").strip().upper()
    form = str(row.get("form_type") or "SEC filing").strip()
    filed = str(row.get("filing_date") or row.get("source_ts") or "unknown date").strip()
    summary = str(row.get("summary") or row.get("risk_flags") or "").strip()
    summary_text = f" Summary: {_clip(summary, 92)}" if summary else ""
    return (
        "SEC row selected. No call made. Next safe action: open the candidate "
        f"case or keep as research evidence. {ticker} {form} filed {filed}."
        f"{summary_text}"
    )


def _broker_row_status_message(row: Mapping[str, object]) -> str:
    area = str(row.get("area") or "Broker status").strip()
    status = str(row.get("status") or "review").strip()
    next_action = str(row.get("next_action") or "").strip()
    next_text = f" Next: {_clip(next_action, 106)}" if next_action else ""
    return (
        f"Broker row selected: No Schwab call made. {area} ({status})."
        f"{next_text}"
    )


def _telemetry_row_status_message(row: Mapping[str, object]) -> str:
    event = str(
        row.get("event_label")
        or _human_telemetry_event(row.get("event"))
        or "Telemetry event"
    ).strip()
    status = str(row.get("status_label") or _human_status_label(row.get("status"))).strip()
    summary = str(
        row.get("summary_label")
        or _humanize_telemetry_summary(row.get("summary"))
        or ""
    ).strip()
    summary_text = f" Summary: {_clip(summary, 104)}" if summary else ""
    return (
        f"Telemetry row selected: No calls. Refresh after run. "
        f"{event} ({status})."
        f"{summary_text}"
    )


def _theme_row_status_message(row: Mapping[str, object]) -> str:
    theme = str(row.get("theme") or "Theme").strip()
    count = int(_number_or_zero(row.get("candidate_count")))
    tickers = ", ".join(_texts(row.get("top_tickers"))[:4])
    ticker_text = f" Top: {tickers}." if tickers else ""
    return f"Theme selected: No calls. {theme}; candidates={count}.{ticker_text}"


def _feature_row_target_page(row: Mapping[str, object]) -> str:
    page_hint = str(row.get("page") or "").strip()
    if not page_hint:
        return ""
    for raw_part in page_hint.split(","):
        part = raw_part.strip()
        if not part:
            continue
        first_token = part.split(maxsplit=1)[0].strip().lower()
        if first_token in PAGE_ALIASES:
            return PAGE_ALIASES[first_token]
        target = _normalize_page(part)
        if target != "help" or part.lower() in {"?", "help"}:
            return target
    return ""


def _feature_row_status_message(row: Mapping[str, object]) -> str:
    area = str(row.get("area") or "Feature").strip()
    use = str(row.get("use") or "").strip()
    if "research-only" in use and "decision-useful" in use:
        use = "research-only vs decision-useful"
    summary = _clip(use or area, 60)
    return f"Feature selected: No calls. {summary}. Area: {area}."


def _help_row_status_message(row: Mapping[str, object]) -> str:
    command = str(row.get("command") or "help").strip()
    meaning = str(row.get("meaning") or "").strip()
    meaning_text = f" Meaning: {_clip(meaning, 112)}" if meaning else ""
    return (
        f"Help row selected: {command}. No calls made; type the command in "
        f"the bottom box or use the matching shortcut.{meaning_text}"
    )


def _page_navigation_status_message(
    page: str,
    payload: Mapping[str, object] | None = None,
) -> str:
    normalized = _normalize_page(page)
    if normalized.startswith("candidate:"):
        ticker = normalized.split(":", 1)[1].upper()
        return f"Opened candidate {ticker}. No calls. Review evidence before action."
    if normalized.startswith("alert:"):
        alert_id = normalized.split(":", 1)[1]
        row = _alert_detail_row(payload or {}, alert_id)
        return _alert_open_status_message(row, alert_id)
    labels = {page_key: label for page_key, _, label in MODERN_PAGES}
    labels.update(
        {
            "themes": "Themes",
            "validation": "Validation",
            "costs": "Costs",
        }
    )
    label = labels.get(normalized, _human_label(normalized) or "page")
    return f"Opened {label}. No calls."


def _validation_status_rows(validation: Mapping[str, object]) -> list[Mapping[str, object]]:
    latest_run = _mapping(validation.get("latest_run"))
    report = _mapping(validation.get("report"))
    return [
        {"key": "Latest run", "value": latest_run.get("id") or "n/a"},
        {"key": "Run status", "value": latest_run.get("status") or "n/a"},
        {"key": "Candidate count", "value": report.get("candidate_count") or 0},
        {"key": "Useful alert rate", "value": report.get("useful_alert_rate")},
        {"key": "False positives", "value": report.get("false_positive_count") or 0},
        {
            "key": "Unsupported claim rate",
            "value": report.get("unsupported_claim_rate"),
        },
        {
            "key": "Next safe action",
            "value": (
                "Run validation replay/report before trusting measured usefulness."
                if not latest_run
                else "Review validation report before changing score policy."
            ),
        },
    ]


def _cost_status_rows(payload: Mapping[str, object]) -> list[Mapping[str, object]]:
    costs = _mapping(payload.get("costs"))
    value_ledger = _mapping(payload.get("value_ledger"))
    value_report = _mapping(payload.get("value_report"))
    candidate_coverage = _mapping(value_report.get("candidate_ledger_coverage"))
    outcome_coverage = _mapping(value_report.get("value_outcome_coverage"))
    validation_evidence = _mapping(value_report.get("validation_evidence"))
    return [
        {
            "key": "Actual cost",
            "value": _format_usd_amount(costs.get("total_actual_cost_usd") or 0),
        },
        {
            "key": "Estimated cost",
            "value": _format_usd_amount(costs.get("total_estimated_cost_usd") or 0),
        },
        {"key": "Useful alerts", "value": costs.get("useful_alert_count") or 0},
        {
            "key": "Cost per useful alert",
            "value": _cost_per_useful_alert_text(costs),
        },
        {
            "key": "Weighted value",
            "value": _format_usd_amount(
                value_ledger.get("confidence_weighted_value_usd") or 0,
            ),
        },
        {
            "key": "Monthly value verdict",
            "value": _human_status_label(
                value_report.get("verdict") or value_report.get("status") or "n/a",
            ),
        },
        {
            "key": "Candidate ledger coverage",
            "value": _candidate_ledger_coverage_text(candidate_coverage),
        },
        {
            "key": "Value outcome coverage",
            "value": _value_outcome_coverage_text(outcome_coverage),
        },
        {
            "key": "Validation evidence",
            "value": _human_status_label(validation_evidence.get("status") or "n/a"),
        },
        {
            "key": "Next safe action",
            "value": value_report.get("canonical_next_action") or "Review value coverage.",
        },
    ]


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
                    or "Open Evidence Gaps or Ops to clear decision blockers.",
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
                "next_action": "Review alert; record local feedback only.",
                "target_page": f"alert:{alert_id}",
                "status_message": (
                    f"Opened alert insight for {ticker}; not a trade signal."
                ),
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
                "next_action": readiness.get("next_action") or "Open Evidence Gaps.",
                "target_page": "readiness",
                "status_message": (
                    "Opened Evidence Gaps. Clear blockers before relying on output."
                ),
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
            "Opened Ops coverage. The full ranked scan stays on Inbox; "
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
        source_label = _human_source_name(source)
        if missing:
            detail = f"{source_label} missing {missing}/{row_count or missing}"
        elif stale:
            detail = f"{source_label} stale {stale}/{row_count or stale}"
        else:
            detail = f"{source_label} coverage {_text(values.get('coverage_pct'))}%"
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


def _real_results_missing(payload: Mapping[str, object]) -> bool:
    return str(_mapping(payload.get("real_results")).get("status") or "") == "missing"


def _real_results_empty(payload: Mapping[str, object]) -> bool:
    return _real_results_missing(payload) and not (
        _rows(_mapping(payload.get("priced_in_queue")).get("rows"))
        or _rows(_mapping(payload.get("candidates")).get("rows"))
    )


def _no_real_result_lines(payload: Mapping[str, object], width: int) -> list[str]:
    real_results = _mapping(payload.get("real_results"))
    missing = ", ".join(_texts(real_results.get("missing"))) or "real scan rows"
    next_action = _no_real_result_next_action(payload, real_results)
    lines = [
        "No real result yet: no market scan has run.",
    ]
    lines.extend(_wrap(f"Required next step: {next_action}", width))
    command = _first_scan_setup_command(payload)
    if command:
        lines.extend(_wrap(f"PowerShell setup command: {command}", width))
        lines.extend(
            _wrap(
                "Where to run it: use a normal PowerShell prompt, not the "
                "dashboard command box. Execute it only after you accept the "
                "data change or provider call.",
                width,
            )
        )
    lines.extend(
        _wrap(
            "Why this page is blank: MarketRadar has no real scan rows to review yet.",
            width,
        )
    )
    lines.append("Provider calls made while viewing: 0.")
    lines.extend(_wrap(f"Missing: {missing}", width))
    lines.append(
        "Demo rows are never loaded automatically; use seed-dashboard-demo only "
        "when you intentionally want a demo."
    )
    return lines


def _visible_scan_row_count(
    payload: Mapping[str, object],
    real_results: Mapping[str, object],
) -> int:
    queue_rows = len(_rows(_mapping(payload.get("priced_in_queue")).get("rows")))
    candidate_rows = len(_rows(_mapping(payload.get("candidates")).get("rows")))
    reported_rows = int(_number_or_zero(real_results.get("row_count")))
    return max(reported_rows, queue_rows, candidate_rows)


def _agent_waiting_on_trusted_evidence_lines(
    payload: Mapping[str, object],
    width: int,
) -> list[str]:
    real_results = _mapping(payload.get("real_results"))
    missing = ", ".join(_texts(real_results.get("missing"))) or "trusted scan evidence"
    next_action = _no_real_result_next_action(payload, real_results)
    row_count = _visible_scan_row_count(payload, real_results)
    row_label = "row" if row_count == 1 else "rows"
    lines = ["Agent Coach is waiting on trusted scan evidence."]
    lines.extend(
        _wrap(
            f"Visible scan rows: {row_count} {row_label}; manual review is "
            "available, but agent execute stays blocked until the evidence gates clear.",
            width,
        )
    )
    if real_results.get("canned_data_detected"):
        lines.extend(
            _wrap(
                "Rows may include demo, fixture, or CSV data. Treat them as UI "
                "practice, not investment evidence.",
                width,
            )
        )
    lines.extend(_wrap(f"Missing evidence gates: {missing}.", width))
    lines.extend(_wrap(f"Required next step: {next_action}", width))
    lines.append("Provider calls made while viewing: 0.")
    return lines


def _first_scan_setup_command(payload: Mapping[str, object]) -> str:
    for row in _first_scan_setup_rows(payload):
        command = str(row.get("command") or "").strip()
        if command:
            return command
    answer = _mapping(payload.get("priced_in_answer"))
    minimum_useful = _mapping(payload.get("minimum_useful_product"))
    return str(
        _first_nonblank(
            payload.get("canonical_next_command"),
            answer.get("canonical_next_command"),
            minimum_useful.get("canonical_next_command"),
            minimum_useful.get("next_command"),
        )
        or ""
    ).strip()


def _setup_command_footer_action(payload: Mapping[str, object]) -> str:
    operator_step = _priced_in_operator_step(payload)
    command = _first_scan_setup_command(payload) or str(
        operator_step.get("tui_command") or operator_step.get("command") or ""
    ).strip()
    if not _real_results_empty(payload) or not command:
        return ""
    blocker = _readiness_first_setup_blocker(payload)
    area = _human_source_name(
        blocker.get("area") if blocker else "setup blocker"
    )
    return (
        f"{_setup_blocker_first_label(area)}: run PowerShell command above after accepting "
        "call/write."
    )


def _setup_command_status_message(payload: Mapping[str, object]) -> str:
    command = _first_scan_setup_command(payload)
    if command:
        return _command_no_side_effects(
            "Run the page's PowerShell command after approval; then press r."
        )
    if _real_results_empty(payload):
        return _command_no_side_effects(
            "Setup is still blocked, but no setup command is available in the "
            "snapshot. Open Evidence Gaps and review the first blocked row."
        )
    return _command_no_side_effects(
        "Setup is not the first blocker anymore. Open Inbox for scan messages or "
        "Evidence Gaps for the current blocker."
    )


def _setup_blocker_first_label(area: object) -> str:
    label = _human_source_name(area or "setup blocker")
    return f"Set up {label} first"


def _no_real_result_next_action(
    payload: Mapping[str, object],
    real_results: Mapping[str, object],
) -> str:
    if _real_results_empty(payload):
        blocker = _readiness_first_setup_blocker(payload)
        if blocker:
            area = _human_source_name(blocker.get("area") or "setup blocker")
            action = _humanize_dashboard_text(blocker.get("next_action"))
            return f"{_setup_blocker_first_label(area)}: {action}"
    current_blocker_action = _current_priced_in_blocker_next_action(payload)
    if current_blocker_action:
        return current_blocker_action
    return (
        str(real_results.get("next_action") or "").strip()
        or "Run/import real market data, then rerun the priced-in answer."
    )


def _modern_agent_next_safe_action(payload: Mapping[str, object]) -> str:
    if _real_results_missing(payload):
        return "Agent locked: 0 OpenAI calls. Press 2 Evidence Gaps."
    return "Review agent preview; run agent execute only after budget approval."


def _overview_lines(payload: Mapping[str, object], width: int) -> list[str]:
    lines = [_rule(_market_inbox_title(payload), width)]
    if _real_results_empty(payload):
        lines.extend(_novice_cockpit_lines(payload, width))
        lines.append(_priced_in_beginner_legend(width))
        lines.append("")
        command = _first_scan_setup_command(payload)
        blocker = _readiness_first_setup_blocker(payload)
        if blocker:
            area = _human_source_name(blocker.get("area") or "setup blocker")
            next_action = f"{_setup_blocker_first_label(area)}."
        else:
            next_action = "Start with setup row 1."
        lines.append(
            "No real result yet: no market scan has run, so there are no stock-analysis messages."
        )
        lines.append("First setup task: build the active stock universe.")
        lines.extend(
            _wrap(
                "This page becomes your Market Inbox after the first capped "
                "scan. For now it only shows setup mail.",
                width,
            )
        )
        if next_action:
            lines.extend(_wrap(f"Required next step: {next_action}", width))
        if command:
            lines.extend(_wrap(f"PowerShell setup command: {command}", width))
            lines.append(
                "Where to run: normal PowerShell prompt, not in the dashboard command box."
            )
        lines.append("After setup: press 2 Evidence Gaps for bars, then 3 Safe Run.")
        lines.append("")
        setup_rows = _market_inbox_rows(payload)
        if setup_rows:
            lines.append("Setup mail - these are instructions, not stock results.")
            lines.extend(
                _table_lines(
                    setup_rows,
                    [
                        ("subject", "Step", 28),
                        ("why", "Why this matters", 42),
                        ("next", "Next safe action", 42),
                    ],
                    width=width,
                    limit=10,
                )
            )
            lines.append("")
        lines.extend(
            _wrap(
                "When scan rows exist, this page groups them as messages: "
                "Urgent first, Worth Reading second, Waiting Evidence only "
                "after data repair.",
                width,
            )
        )
        return lines
    lines.extend(_market_inbox_focus_lines(payload, width))
    lines.append("")
    inbox_scope = _market_inbox_scope_summary(payload)
    if inbox_scope:
        lines.append(f"Inbox summary: {inbox_scope}.")
    overview_rows = _market_inbox_rows(payload)
    if overview_rows:
        lines.extend(_market_inbox_triage_context_lines(payload, width))
    else:
        lines.extend(_market_inbox_diagnostic_lines(payload, width))
    if overview_rows:
        lines.extend(
            _table_lines(
                overview_rows,
                [
                    ("mailbox", "Mailbox", 16),
                    ("ticker", "Ticker", 6),
                    ("subject", "Subject", 24),
                    ("why", "Why this reached you", 31),
                    ("missing", "Missing / waiting", 20),
                    ("next", "Next safe action", 27),
                ],
                width=width,
                limit=50,
            )
        )
    else:
        lines.extend(_novice_empty_scan_lines(width))
    lines.append("")
    lines.extend(_wrap(_market_inbox_caption(payload), width))
    return lines


def _market_inbox_focus_lines(payload: Mapping[str, object], width: int) -> list[str]:
    calls = int(_number_or_zero(payload.get("external_calls_made")))
    lines = _wrap(
        "MarketRadar answers one question: has market emotion been fully "
        "priced in, or is price still behind?",
        width,
    )
    lines.extend(
        _wrap(
            "Read this like email: open rows that look worth reading, clear "
            "Evidence Gaps first, and do not treat any row as actionable until "
            "Decision Review says it is ready.",
            width,
        )
    )
    lines.append(_priced_in_beginner_legend(width))
    lines.append(f"Browsing this dashboard made {calls} calls and 0 order submissions.")
    return lines


def _market_inbox_triage_context_lines(
    payload: Mapping[str, object],
    width: int,
) -> list[str]:
    lines: list[str] = []
    decision_summary = _decision_readiness_summary(payload)
    if decision_summary:
        lines.extend(_wrap(f"Current blocker: {decision_summary}", width))
    next_action = _current_priced_in_blocker_next_action(payload)
    if next_action and next_action not in (decision_summary or ""):
        lines.extend(_wrap(f"Do first: {next_action}", width))
    research_preview = _market_inbox_research_preview(payload)
    if research_preview:
        lines.extend(_wrap(research_preview, width))
    lines.extend(
        _wrap(
            "Details: press 2 Evidence Gaps for blockers, 3 Safe Run for call "
            "budget, 8 Ops for provider health, or Ctrl+A Agent Coach for a "
            "zero-call agent preview.",
            width,
        )
    )
    lines.append("Messages below are research mail, not trade ideas.")
    return lines


def _market_inbox_research_preview(payload: Mapping[str, object]) -> str:
    priced_queue = _mapping(payload.get("priced_in_queue"))
    usefulness_counts = _mapping(priced_queue.get("usefulness_counts"))
    research_count = int(_number_or_zero(usefulness_counts.get("research_useful")))
    if research_count <= 0:
        return ""
    tickers: list[str] = []
    for row in _priced_in_overview_rows(payload):
        usefulness = _mapping(row.get("usefulness"))
        if str(usefulness.get("status") or "").strip() != "research_useful":
            continue
        ticker = str(row.get("ticker") or "").strip().upper()
        if ticker and ticker not in tickers:
            tickers.append(ticker)
        if len(tickers) >= 5:
            break
    lead_label = "lead" if research_count == 1 else "leads"
    examples = f" Visible examples: {', '.join(tickers)}." if tickers else ""
    return (
        f"Worth reading now: {research_count} research {lead_label}. "
        "Press Enter on the highlighted row or click a Worth Reading row to "
        "open the first case; press 4 Candidate Review for the full review "
        f"table; press 2 Evidence Gaps before trusting any row.{examples}"
    )


def _market_inbox_diagnostic_lines(
    payload: Mapping[str, object],
    width: int,
) -> list[str]:
    lines: list[str] = []
    minimum_stop = _minimum_product_stop_line_summary(payload)
    if minimum_stop:
        lines.append(
            "Shipped-product stop: "
            f"{_clip(minimum_stop, max(20, width - 24))}"
        )
        approval_summary = _minimum_product_approval_summary(payload)
        if approval_summary:
            approval_body = approval_summary.removeprefix("approval required: ")
            lines.append(
                "Approval required: "
                f"{_clip(approval_body, max(20, width - 19))}"
            )
        approval_command = _minimum_product_approval_command(payload)
        if approval_command:
            lines.append(
                "Approval command: "
                f"{_clip(approval_command, max(20, width - 18))}"
            )
    audit_summary = _full_scan_audit_summary(payload)
    if audit_summary:
        lines.extend(_wrap(f"Full scan audit: {audit_summary}", width))
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
        lines.extend(_wrap(f"Instrument scope: {instrument_summary}", width))
    decision_summary = _decision_readiness_summary(payload)
    if decision_summary:
        lines.extend(_wrap(f"Decision readiness: {decision_summary}", width))
    return lines


def _novice_cockpit_lines(payload: Mapping[str, object], width: int) -> list[str]:
    lines = _wrap(
        "MarketRadar answers one question: has market emotion toward a stock already "
        "been priced in?",
        width,
    )
    lines.append("Core question: has market emotion been fully priced in?")
    for card in _novice_cockpit_cards(payload):
        label = str(card["label"])
        prefix = f"{label}: "
        value_lines = _wrap(str(card["value"]), max(16, width - len(prefix)))
        lines.append(f"{prefix}{value_lines[0]}")
        for wrapped_value in value_lines[1:]:
            lines.append(f"{' ' * len(prefix)}{wrapped_value}")
        detail = str(card.get("detail") or "").strip()
        if detail:
            for detail_segment in detail.splitlines():
                for wrapped_detail in _wrap(detail_segment, max(20, width - 4)):
                    lines.append(f"  {wrapped_detail}")
    lines.append(
        f"Browsing this dashboard made {int(_number_or_zero(payload.get('external_calls_made')))} "
        "calls and 0 order submissions."
    )
    return lines


def _novice_cockpit_cards(payload: Mapping[str, object]) -> list[Mapping[str, object]]:
    answer = _mapping(payload.get("priced_in_answer"))
    readiness = _mapping(payload.get("readiness"))
    real_results = _mapping(payload.get("real_results"))
    next_step = _priced_in_operator_step(payload) or _mapping(
        payload.get("operator_next_step")
    )
    queue = _mapping(payload.get("priced_in_queue"))
    row_count = int(_number_or_zero(queue.get("total_count") or queue.get("count")))
    safe = bool(readiness.get("safe_to_make_investment_decision"))
    answer_text = (
        answer.get("answer")
        or answer.get("headline")
        or readiness.get("headline")
        or "Evidence is not ready."
    )
    next_action = (
        next_step.get("action")
        or answer.get("next_action")
        or readiness.get("next_action")
        or "Open Inbox."
    )
    if _real_results_empty(payload):
        blocker = _readiness_first_setup_blocker(payload)
        area = _human_source_name(blocker.get("area") if blocker else "Active universe")
        next_action = (
            f"{_setup_blocker_first_label(area)}, then open Evidence Gaps."
        )
        next_detail = (
            "Only run provider commands intentionally; browsing this dashboard "
            "makes 0 calls."
        )
    else:
        next_detail = (
            _operator_step_cost_detail(next_step)
            or next_step.get("expected_response")
            or _current_priced_in_blocker_next_action(payload)
            or real_results.get("next_action")
            or "Browsing does not spend provider, OpenAI, broker, or order calls."
        )
    return [
        {
            "label": "What this is",
            "value": "MarketRadar answers one question",
            "detail": "Has market emotion toward a stock already been priced in?",
        },
        {
            "label": "Can I act?",
            "value": "No - research only" if not safe else "Manual review only",
            "detail": answer_text,
        },
        {
            "label": "Best next step",
            "value": next_action,
            "detail": next_detail,
        },
        {
            "label": "Rows",
            "value": f"{row_count} scan row(s)",
            "detail": (
                "No scan rows yet"
                if row_count == 0
                else "Open a row to inspect evidence before deciding anything."
            ),
        },
    ]


def _priced_in_beginner_legend(width: int) -> str:
    return _clip(
        "Legend: Emotion = market mood; Price reaction = price move; "
        "Gap = emotion - reaction; Decision-ready = enough evidence.",
        width,
    )


def _novice_empty_scan_lines(width: int) -> list[str]:
    return [
        "",
        _rule("No scan rows yet", width),
        "No scan rows yet. Start here:",
        "First setup task: build the active stock universe.",
        "Setup rows are instructions, not stock results.",
        "1. Import or fetch a ticker universe.",
        "2. Fill fresh market bars.",
        "3. Run a capped scan.",
        "Nothing on this page is a trade signal until scan rows and evidence exist.",
    ]


def _review_lines(payload: Mapping[str, object], width: int) -> list[str]:
    rows = _priced_in_review_rows(payload)
    answer = _mapping(payload.get("priced_in_answer"))
    readiness = _mapping(payload.get("readiness"))
    lines = [_rule("Decision Review - priced-in answer, not trade approval", width)]
    if _real_results_empty(payload):
        lines.extend(
            _locked_review_setup_lines(
                payload,
                width,
                title="No decision review yet.",
                unlocks=(
                    "This page summarizes whether the priced-in answer is ready "
                    "for human review after real scan rows exist."
                ),
                after_setup=(
                    "open Evidence Gaps, run one capped scan, then review "
                    "candidate packets first."
                ),
            )
        )
        return lines
    lines.append(
        "Answer: "
        f"{answer.get('answer') or 'No priced-in answer.'} "
        f"Trade safe? {_decision_label(readiness)}."
    )
    boundary = str(answer.get("investment_boundary") or "").strip()
    if boundary:
        lines.append(f"Boundary: {boundary}")
    lines.append(f"Remaining optional context: {_decision_review_optional_summary(rows)}")
    if rows:
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
    else:
        lines.extend(
            _wrap(
                "No decision-ready rows yet. Evidence Gaps must clear before "
                "Decision Review can show reviewable candidates.",
                width,
            )
        )
        lines.extend(
            _wrap(
                "Next: press 2 for Evidence Gaps, or press 1 to return to Inbox "
                "research messages.",
                width,
            )
        )
    lines.append("")
    lines.extend(_wrap(_decision_review_caption(payload, rows), width))
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
                    f"Opened decision-ready priced-in row for {ticker}. No calls. "
                    "This is still not trade approval."
                ),
            }
        )
    return rows


def _decision_review_empty_modern_row() -> Mapping[str, object]:
    return {
        "_row_key": "review-empty",
        "rank": "-",
        "ticker": "-",
        "signal": "No ready rows",
        "emotion_reaction_gap": "",
        "optional_gaps": "Evidence Gaps first",
        "top_evidence": "Evidence not ready",
        "next_action": "Return to Inbox or Evidence Gaps.",
        "target_page": "overview",
        "status_message": (
            "No decision-ready rows yet. Returned to Inbox; use Evidence Gaps "
            "or Full Scan for research-only rows."
        ),
    }


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
    if not rows:
        scan_scope = f"{scan_total} ranked rows" if scan_total else "the latest scan"
        return (
            f"0 decision-ready rows from {scan_scope}. "
            "Required evidence must clear first. Press 2 Evidence Gaps or 1 Inbox."
        )
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
        _human_status_label(audit.get("status")),
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
    status = _human_status_label(provider_plan.get("status") or "unknown")
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
    raw_status = str(provider_plan.get("provider_saved_file_status") or "unknown").strip()
    status = _human_status_label(raw_status)
    exists_value = provider_plan.get("provider_saved_file_exists")
    boundary = _saved_file_request_boundary(
        provider_plan,
        (
            ("provider_saved_file_import_preview_request_body", "execute", "preview"),
            ("provider_saved_file_import_request_body", "execute", "import"),
        ),
        "request bodies",
    )
    if raw_status == "missing" or (exists_value is False and raw_status != "available"):
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
        status = _human_status_label(packet.get("status") or "unknown")
        missing = int(_number_or_zero(packet.get("missing_as_of_bar_count")))
        calls = int(_number_or_zero(packet.get("external_calls_if_approved")))
        db_writes = int(_number_or_zero(packet.get("db_writes_during_capture")))
        confirm = str(packet.get("tui_confirm_command") or "").strip()
        question = str(packet.get("question") or "").strip()
        next_action = str(
            provider_plan.get("provider_saved_file_next_action") or question
        ).strip()
        instruction = (
            f"type `{confirm}`"
            if confirm and calls > 0
            else "no capture command needed"
        )
        action_suffix = f" {next_action}" if next_action else ""
        return (
            f"{status}; {missing} bars targeted; {calls} external call(s) if "
            f"approved; {db_writes} db writes during capture; {instruction}."
            f"{action_suffix}"
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


def _market_bar_provider_saved_capture_confirm_command(
    payload: Mapping[str, object],
) -> str:
    provider_plan = _market_bar_provider_fill_plan(payload)
    if not provider_plan:
        return ""
    packet = _mapping(provider_plan.get("provider_saved_file_capture_approval_packet"))
    command = _market_bar_saved_capture_confirm_command(packet)
    if command:
        return command
    if packet:
        return ""
    provider_command = str(
        provider_plan.get("provider_saved_file_capture_command") or ""
    ).strip()
    provider_calls = int(
        _number_or_zero(
            provider_plan.get("provider_saved_file_capture_external_call_count")
        )
    )
    if provider_command and provider_calls > 0:
        return "bars saved capture confirm"
    return ""


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
    raw_status = str(provider_plan.get("provider_saved_file_status") or "unknown").strip()
    status = _human_status_label(raw_status)
    exists_value = provider_plan.get("provider_saved_file_exists")
    boundary = _saved_file_request_boundary(
        provider_plan,
        (("provider_saved_file_validate_request_body", "fixture_path", "validate"),),
        "request body",
    )
    if raw_status == "missing" or (exists_value is False and raw_status != "available"):
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


def _full_scan_scope_label(full_scan: Mapping[str, object]) -> str:
    scope = str(full_scan.get("instrument_filter") or "full").strip()
    return {
        "all": "all-instrument",
        "stocks_only": "stock-like",
    }.get(scope, scope)


def _current_scan_coverage_hint(payload: Mapping[str, object]) -> str:
    answer = _mapping(payload.get("priced_in_answer"))
    full_scan = _mapping(answer.get("full_scan"))
    active = int(_number_or_zero(full_scan.get("active_securities")))
    scanned = int(_number_or_zero(full_scan.get("scanned_rows")))
    unscanned = int(_number_or_zero(full_scan.get("unscanned_rows")))
    if active <= 0 or unscanned <= 0:
        return ""
    missing = _market_bar_missing_count(payload)
    parts = [
        "not full-market yet",
        f"{scanned:,}/{active:,} active scanned",
        f"{unscanned:,} unscanned",
    ]
    if missing:
        parts.append(f"{missing:,} missing bars")
    return f"Current scan coverage: {'; '.join(parts)}."


def _overview_source_workflow_hint(payload: Mapping[str, object]) -> str:
    full_scan_summary = _answer_full_scan_scope_summary(payload)
    if full_scan_summary:
        if _priced_in_scan_scope_is_partial(payload):
            return _current_scan_coverage_hint(payload) or full_scan_summary.replace(
                "Full-scan coverage:",
                "Current scan coverage:",
                1,
            )
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
    coverage_source_label = _human_source_name(coverage_source)
    coverage_gap_count = int(
        _number_or_zero(
            _source_action_gap_count(_mapping(action_by_source.get(coverage_source)))
        )
    )
    coverage_text = (
        f"{coverage_source_label} ({coverage_gap_count} full-scan gap row(s))"
        if coverage_source and coverage_gap_count
        else coverage_source_label
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
    decision_source_label = _human_source_name(decision_source)
    decision_rows = int(_number_or_zero(decision_step.get("decision_useful_gap_rows")))
    coverage_label = (
        "Current scan coverage"
        if _priced_in_scan_scope_is_partial(payload)
        else "Full-scan coverage"
    )
    if coverage_source and decision_source:
        decision_text = (
            f"{decision_source_label} ({decision_rows} decision-ready row(s))"
            if decision_rows
            else decision_source_label
        )
        return (
            f"{coverage_label}: {coverage_text}. "
            f"Shortlist context: {decision_text}."
        )
    if coverage_source:
        return f"{coverage_label}: {coverage_text}."
    if decision_source:
        return f"Shortlist context: {decision_source_label}."
    if coverage:
        return f"{coverage_label}: {_clip(coverage, 140)}"
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
    scope_label = _full_scan_scope_label(full_scan)
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
        return f"Evidence layers: {_human_source_status_text(summary)}"
    ready = int(_number_or_zero(evidence.get("ready_source_count")))
    total = int(_number_or_zero(evidence.get("total_source_count")))
    if total <= 0:
        return ""
    first_gap = str(evidence.get("first_gap_source") or "").strip()
    first_gap_count = int(_number_or_zero(evidence.get("first_gap_count")))
    first_gap_label = _human_source_name(first_gap)
    suffix = (
        f"; first gap {first_gap_label}:{first_gap_count}"
        if first_gap and first_gap_count
        else ""
    )
    return f"Evidence layers: {ready}/{total} complete{suffix}."


def _overview_title(payload: Mapping[str, object]) -> str:
    return _latest_scan_results_title(payload)


def _market_inbox_title(payload: Mapping[str, object]) -> str:
    return f"Market Inbox - {_latest_scan_results_title(payload)}"


def _latest_scan_results_title(payload: Mapping[str, object]) -> str:
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
                "Latest scan results - decision-ready not-priced-in rows "
                f"{start}-{end} of {total}; scan {scan_total}{suffix}"
            )
        return (
            f"Latest scan results - mismatches rows {start}-{end} of {total}; "
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
                f"Latest selected-universe scan results - rows "
                f"{start}-{end} of {total}{suffix}"
            )
        if scan_status == "previous_scan":
            return (
                f"Previous full-market scan results - rows "
                f"{start}-{end} of {total}{suffix}"
            )
        return (
            f"Latest scan results - rows {start}-{end} of {total}"
            f"{scope_text}{suffix}"
        )
    return "Latest scan results - no rows yet; run or import scan evidence first"


def _market_inbox_caption(payload: Mapping[str, object]) -> str:
    queue = _mapping(payload.get("priced_in_queue"))
    total = int(_number_or_zero(queue.get("total_count")))
    returned = int(_number_or_zero(queue.get("returned_count") or queue.get("count")))
    source_hint = _overview_source_workflow_hint(payload)
    source_hint_text = f" Next data step: {source_hint}" if source_hint else ""
    source_gap = _source_gap_filter_summary(queue)
    source_gap_text = f" Active source gap filter: {source_gap}." if source_gap else ""
    answer_text = (
        " These are the actionable answers; type full to inspect the whole "
        "ranked universe."
        if _is_decision_ready_filter(queue)
        else ""
    )
    if source_hint.startswith("Current scan coverage:"):
        compact_hint = source_hint.removeprefix("Current scan coverage:").strip()
        source_gap_prefix = f"{source_gap_text.strip()} " if source_gap_text else ""
        return (
            f"{source_gap_prefix}"
            f"Next data step: {compact_hint} "
            "Inbox triage: open the top row; this is one review page, "
            "not the full scan universe. Waiting Evidence means data repair."
            f"{answer_text} Browsing makes 0 provider calls."
        )
    detailed_scope = _overview_caption(payload)
    if total and returned < total and detailed_scope:
        return (
            "Inbox triage: open the top row; Waiting Evidence means data repair. "
            f"{detailed_scope}"
        )
    return (
        "Inbox triage: open the top row; this is one review page, not the full "
        "scan universe. Waiting Evidence means data repair. "
        f"{answer_text}{source_gap_text}{source_hint_text} "
        "Browsing makes 0 provider calls."
    )


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
            plan_command = (
                "powershell -ExecutionPolicy Bypass -File "
                "scripts/run-full-market-scan.ps1"
            )
            return (
                f"This page shows rows {start}-{end}: {returned} visible rows from "
                f"{total} latest-scan rows in universe={universe}. "
                "That is a selected universe, not the whole active market. "
                f"Plan the all-active scan with `{plan_command}`; execute only "
                f"after review with `{plan_command} -Execute`. "
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
            "for the full export. In the TUI type next, prev, offset ROW, "
            f"or limit ROWS.{usefulness_text}{source_gap_text}{decision_gap_text} "
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
    normalized = [
        _human_source_name(source)
        for source in sources
        if str(source).strip()
    ]
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


def _readiness_modern_table_rows(payload: Mapping[str, object]) -> list[Mapping[str, object]]:
    rows: list[Mapping[str, object]] = []
    first_gap = _readiness_first_work_item(payload)
    action = str(
        first_gap.get("next_action") or first_gap.get("action") or ""
    ).strip()
    command = _first_catalyst_radar_command(action)
    if command:
        rows.extend(
            [
                {
                    "area": "Run in PowerShell",
                    "status": "manual",
                    "finding": command,
                    "next_action": "Copy into PowerShell; do not enter below.",
                },
                {
                    "area": "Safety boundary",
                    "status": "zero call",
                    "finding": _powershell_command_boundary(command),
                    "next_action": "Dashboard shows this only; running it is separate.",
                },
            ]
        )
    rows.extend(_rows(_mapping(payload.get("readiness")).get("readiness_checklist")))
    return rows


def _run_modern_table_rows(payload: Mapping[str, object]) -> list[Mapping[str, object]]:
    rows: list[Mapping[str, object]] = []
    command = str(
        _priced_in_operator_step(payload).get("tui_command")
        or _priced_in_operator_step(payload).get("command")
        or ""
    ).strip()
    if command.startswith("catalyst-radar"):
        rows.extend(
            [
                {
                    "layer": "Review command",
                    "provider": "local",
                    "status": "manual",
                    "external_call_count_max": 0,
                    "next_action": "Use command above; run outside dashboard.",
                },
                {
                    "layer": "Run location",
                    "provider": "PowerShell",
                    "status": "local",
                    "external_call_count_max": 0,
                    "next_action": "Copy to PowerShell; do not enter below.",
                },
                {
                    "layer": "Safety boundary",
                    "provider": "local",
                    "status": "zero call",
                    "external_call_count_max": 0,
                    "next_action": "Read-only review; no provider/broker/DB calls.",
                },
            ]
        )
    for row in _rows(_mapping(payload.get("call_plan")).get("rows")):
        rows.append(
            {
                **dict(row),
                "status": _run_plan_status_label(row),
                "next_action": _run_plan_next_action_label(row),
            }
        )
    return rows


def _run_plan_status_label(row: Mapping[str, object]) -> str:
    status = str(row.get("status") or "unknown").strip().lower()
    if status == "live_call_planned":
        return "live call"
    if status == "live_calls_planned":
        return "live calls"
    if status == "local_or_dry_run_only":
        return "local/dry"
    return _human_status_label(status)


def _run_plan_next_action_label(row: Mapping[str, object]) -> str:
    layer = str(row.get("layer") or "").strip().lower()
    if layer == "scan provider":
        return "No action unless changing scan scope."
    if layer == "market data":
        return "Keep cooldown; inspect rejected count."
    if layer in {"news/events", "news events"}:
        return "Run only if target count matches budget."
    if layer == "llm review":
        return "Use after live data quality is acceptable."
    if layer == "alert delivery":
        return "Use alert workflows after review."
    if layer == "schwab":
        return "Use broker sync controls separately."
    return _humanize_dashboard_text(row.get("next_action"))


def _readiness_lines(payload: Mapping[str, object], width: int) -> list[str]:
    readiness = _mapping(payload.get("readiness"))
    shadow = _mapping(payload.get("shadow_readiness"))
    shadow_mode = _mapping(payload.get("shadow_mode"))
    latest_shadow = _mapping(shadow_mode.get("latest"))
    boundary = _mapping(shadow.get("call_boundary"))
    queue = _mapping(payload.get("operator_work_queue"))
    lines = [_rule("Evidence Gaps And Work Queue", width)]
    first_gap = _readiness_first_work_item(payload)
    setup_first = bool(first_gap and _real_results_empty(payload))
    readiness_next_action = readiness.get("next_action")
    readiness_evidence = _human_readiness_evidence(readiness.get("evidence"))
    if first_gap:
        priority = _human_status_label(first_gap.get("priority") or "gap")
        area = _human_source_name(
            first_gap.get("area") or first_gap.get("item") or "Evidence gap"
        )
        action = _humanize_dashboard_text(
            first_gap.get("next_action") or first_gap.get("action") or ""
        )
        display_action = action
        setup_command = _first_scan_setup_command(payload) if setup_first else ""
        if setup_first and setup_command:
            display_action = (
                "Run the PowerShell setup command below after reviewing the "
                "call/write budget."
            )
        first_blocker = f"{priority}: {area}"
        if setup_first:
            first_blocker = _setup_blocker_first_label(area)
        top_items: list[tuple[str, object]] = [
            ("Stoplight", "Red rows block trust; green rows are already clear."),
            ("First blocker", first_blocker),
            (
                "Safe interaction",
                "Open rows to inspect; 0 calls, 0 orders.",
            ),
            ("Do next", display_action),
        ]
        if setup_first and setup_command:
            top_items.extend(
                [
                    ("PowerShell command", setup_command),
                    (
                        "Where to run",
                        "Run it in a normal PowerShell prompt, not in the "
                        "dashboard command box.",
                    ),
                ]
            )
        elif action_command := _first_catalyst_radar_command(display_action):
            top_items.extend(
                _powershell_command_context_items(
                    action_command,
                    include_command=True,
                )
            )
        lines.extend(
            _kv_lines(
                top_items,
                width=width,
            )
        )
        lines.append("")
        if setup_first:
            readiness_next_action = (
                f"{_setup_blocker_first_label(area)}: {display_action}"
            )
            readiness_evidence = f"{area} setup blocked; no market scan yet"
    lines.extend(
        _kv_lines(
            (
                ("Status", _human_label(readiness.get("status"))),
                ("Decision mode", _human_label(readiness.get("decision_mode"))),
                ("Headline", readiness.get("headline")),
                ("Next action", readiness_next_action),
                ("Evidence", readiness_evidence),
                (
                    "Queue",
                    (
                        f"{_human_status_label(queue.get('status'))}; "
                        f"{_humanize_dashboard_text(queue.get('headline'))}"
                    ),
                ),
            ),
            width=width,
        )
    )
    lines.append("")
    if shadow:
        shadow_items = (
            _readiness_setup_shadow_items(shadow, boundary)
            if setup_first
            else (
                (
                    "Setup check",
                    f"{_human_status_label(shadow.get('status'))}; "
                    f"{_readiness_ready_label(shadow.get('ready'))}",
                ),
                (
                    "Setup next",
                    _humanize_dashboard_text(shadow.get("canonical_next_action")),
                ),
                (
                    "Setup call budget",
                    "readiness check: 0 calls, 0 writes; safe-run max="
                    f"{boundary.get('planned_run_external_call_count_max') or 0}",
                ),
                (
                    "Latest setup run",
                    (
                        f"{_human_status_label(latest_shadow.get('status'))}; "
                        f"run_date={latest_shadow.get('run_date') or 'n/a'}; "
                        f"writes={latest_shadow.get('db_writes_made') or 0}"
                    )
                    if latest_shadow
                    else "none recorded",
                ),
                (
                    "Useful means",
                    _humanize_dashboard_text(shadow.get("useful_definition")),
                ),
            )
        )
        lines.extend(
            _kv_lines(
                shadow_items,
                width=width,
            )
        )
        lines.append("")
        check_rows = (
            _readiness_setup_ladder_rows(shadow.get("checks"))
            if setup_first
            else shadow.get("checks")
        )
        lines.extend(
            _table_lines(
                _readiness_table_rows(check_rows),
                [
                    ("code", "Check", 24),
                    ("status", "Status", 10),
                    ("finding", "Finding", 48),
                    ("next_action", "Next Action", 38),
                ],
                width=width,
                limit=12,
            )
        )
        lines.append("")
    if setup_first:
        lines.append(_rule("Current Work Queue", width))
        lines.extend(
            _wrap(
                "Setup is not complete yet. Later evidence, Decision Card, LLM, "
                "telemetry, alert, and broker tasks stay hidden until the active "
                "universe exists.",
                width,
            )
        )
        lines.extend(
            _table_lines(
                _readiness_table_rows(_readiness_setup_first_rows(first_gap)),
                [
                    ("priority", "Priority", 14),
                    ("area", "Area", 18),
                    ("item", "Item", 42),
                    ("next_action", "Action", 42),
                ],
                width=width,
                limit=4,
            )
        )
    else:
        lines.extend(
            _table_lines(
                _readiness_table_rows(readiness.get("readiness_checklist")),
                [
                    ("area", "Area", 24),
                    ("status", "Status", 10),
                    ("finding", "Finding", 44),
                    ("next_action", "Next Action", 36),
                ],
                width=width,
                limit=12,
            )
        )
        lines.append("")
        lines.extend(
            _table_lines(
                _readiness_table_rows(queue.get("rows")),
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


def _readiness_setup_first_rows(
    first_gap: Mapping[str, object],
) -> list[Mapping[str, object]]:
    area = first_gap.get("area") or first_gap.get("item") or "Setup"
    return [
        {
            "priority": first_gap.get("priority") or "setup",
            "area": area,
            "item": first_gap.get("item") or area,
            "next_action": "Run PowerShell command above.",
        },
        {
            "priority": "later",
            "area": "Later tasks",
            "item": "Hidden until setup is complete",
            "next_action": "Hidden until setup is complete.",
        },
    ]


def _readiness_setup_ladder_rows(value: object) -> list[Mapping[str, object]]:
    setup_codes = {"active_universe", "latest_market_bars", "scan_scope", "trust_gate"}
    return [
        {
            **dict(row),
            "code": _readiness_setup_ladder_label(row),
            "finding": _readiness_setup_ladder_finding(row),
            "next_action": _readiness_setup_ladder_action(row),
        }
        for row in _rows(value)
        if str(row.get("code") or "").strip().lower() in setup_codes
    ]


def _readiness_setup_ladder_label(row: Mapping[str, object]) -> str:
    code = str(row.get("code") or "").strip().lower()
    if code == "active_universe":
        return "Active universe"
    if code == "latest_market_bars":
        return "Latest market bars"
    if code == "scan_scope":
        return "Scan rows"
    if code == "trust_gate":
        return "Evidence layers"
    return _human_source_name(row.get("code"))


def _readiness_setup_ladder_finding(row: Mapping[str, object]) -> str:
    code = str(row.get("code") or "").strip().lower()
    if code == "active_universe":
        return "No stock universe is loaded yet."
    if code == "latest_market_bars":
        return "Latest prices wait for the universe."
    if code == "scan_scope":
        return "No priced-in scan rows exist yet."
    if code == "trust_gate":
        return "Enough evidence is not ready yet."
    return _humanize_dashboard_text(row.get("finding"))


def _readiness_setup_ladder_action(row: Mapping[str, object]) -> str:
    code = str(row.get("code") or "").strip().lower()
    if code == "active_universe":
        return "Run PowerShell command above."
    if code == "latest_market_bars":
        return "After universe, fill latest bars."
    if code == "scan_scope":
        return "After setup, run one capped scan."
    if code == "trust_gate":
        return "Review results after setup."
    return "Set up this blocker first."


def _readiness_setup_shadow_items(
    shadow: Mapping[str, object],
    boundary: Mapping[str, object],
) -> tuple[tuple[str, object], ...]:
    max_calls = int(_number_or_zero(
        boundary.get("planned_run_external_call_count_max")
    ))
    return (
        (
            "Setup check",
            f"{_human_status_label(shadow.get('status'))}; "
            f"{_readiness_ready_label(shadow.get('ready'))}",
        ),
        (
            "Setup step",
            "Seed or refresh the universe with the PowerShell command above; "
            "do not type it in the dashboard.",
        ),
        (
            "Setup budget",
            f"readiness check: 0 calls, 0 writes; setup run max={max_calls}",
        ),
        (
            "After setup",
            "Fill latest bars next, then use Safe Run for one capped scan.",
        ),
    )


def _readiness_table_rows(value: object) -> list[Mapping[str, object]]:
    rows: list[Mapping[str, object]] = []
    for row in _rows(value):
        rows.append(
            {
                **dict(row),
                "area": _human_source_name(row.get("area")),
                "code": _human_source_name(row.get("code")),
                "status": _human_status_label(row.get("status")),
                "finding": _humanize_dashboard_text(row.get("finding")),
                "next_action": _humanize_dashboard_text(row.get("next_action")),
                "priority": _human_status_label(row.get("priority")),
                "item": _humanize_dashboard_text(row.get("item")),
            }
        )
    return rows


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
        lines.extend(
            _kv_lines(_run_source_status_display_items(mission_items), width=width)
        )
        lines.append("")
    if _real_results_empty(payload):
        lines.extend(_run_setup_locked_lines(payload, width))
        return lines
    lines.append(_rule("Radar Run And Call Plan", width))
    lines.extend(
        _kv_lines(
            (
                ("Latest run", _human_status_label(latest.get("status") or "unknown")),
                ("Required path", _run_required_path_text(payload, latest)),
                ("Run as-of", latest.get("as_of") or "n/a"),
                (
                    "Activation",
                    f"{_human_status_label(activation.get('status'))}; "
                    f"{activation.get('headline')}",
                ),
                (
                    "Cooldown",
                    f"{_human_status_label(cooldown.get('status'))}; "
                    f"{cooldown.get('detail')}",
                ),
                (
                    "Call plan",
                    f"{_human_status_label(call_plan.get('status'))}; "
                    f"{call_plan.get('headline')}",
                ),
                ("Next", call_plan.get("next_action")),
                ("Max external calls", call_plan.get("max_external_call_count")),
            ),
            width=width,
        )
    )
    lines.append("")
    lines.extend(
        _table_lines(
            _call_plan_table_rows(call_plan),
            [
                ("layer", "Layer", 18),
                ("provider", "Provider", 12),
                ("status", "Status", 18),
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
                f"{_human_status_label(audit.get('status'))}; {audit.get('answer')}",
            ),
            (
                "Next evidence step",
                blocker.get("next_action") if blocker else audit.get("next_action"),
            ),
            ("Full-scan evidence", full_scan_evidence),
            (
                "Visible-page source coverage",
                _human_source_status_text(coverage.get("summary")),
            ),
        ]
        blocker_hint = _run_audit_source_blocker_hint(blocker, payload)
        if blocker_hint:
            evidence_items.append(("Inspect source blocker", blocker_hint))
        manual_hint = _market_bar_manual_action_summary(payload)
        if manual_hint:
            evidence_items.append(("Manual CSV action", manual_hint))
        saved_capture_command = _market_bar_provider_saved_capture_confirm_command(
            payload
        )
        if saved_capture_command:
            evidence_items.append(
                ("Saved capture command", f"`{saved_capture_command}`")
            )
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
        lines.extend(
            _kv_lines(_run_source_status_display_items(evidence_items), width=width)
        )
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
                f"{_human_status_label(evidence_plan.get('status'))}; "
                f"{evidence_plan.get('headline')}",
            ),
            ("Next evidence step", evidence_plan.get("next_action")),
        ]
        blocker_hint = _run_source_blocker_hint(evidence_plan)
        if blocker_hint:
            evidence_items.append(("Inspect source blocker", blocker_hint))
        lines.extend(
            _kv_lines(
                _run_source_status_display_items(evidence_items),
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
    lines.extend(
        _wrap(_run_operational_note(payload), width)
    )
    return lines


def _run_setup_locked_lines(payload: Mapping[str, object], width: int) -> list[str]:
    call_plan = _mapping(payload.get("call_plan"))
    real_results = _mapping(payload.get("real_results"))
    next_action = _no_real_result_next_action(payload, real_results)
    setup_command = (
        _first_scan_setup_command(payload) if _real_results_empty(payload) else ""
    )
    max_calls = call_plan.get("max_external_call_count")
    if max_calls in (None, ""):
        max_calls = 0
    max_call_count = int(_number_or_zero(max_calls))
    display_next_action = next_action
    if setup_command:
        blocker = _readiness_first_setup_blocker(payload)
        area = _human_source_name(
            blocker.get("area") if blocker else "setup blocker"
        )
        display_next_action = (
            f"{_setup_blocker_first_label(area)}. Use the PowerShell command "
            "below after accepting the data change or provider call."
        )
    first_step_items: list[tuple[str, object]] = [
        ("Can I run now?", "No. No real scan rows exist yet."),
        (
            "Why locked?",
            (
                "MarketRadar needs an active universe and fresh price "
                "reaction before it can compare emotion against price."
            ),
        ),
        ("Do this first", display_next_action),
    ]
    if setup_command:
        first_step_items.extend(
            [
                ("PowerShell command", setup_command),
                (
                    "Where to run",
                    "Run it in a normal PowerShell prompt, not in the "
                    "dashboard command box.",
                ),
            ]
        )
    first_step_items.extend(
        [
            (
                "Run execute later",
                (
                    "After setup, this page reviews one capped radar cycle "
                    "before you intentionally run it."
                ),
            ),
            ("Browsing cost", "0 provider calls, 0 OpenAI calls, 0 orders."),
            (
                "Current execute cap",
                f"{_count_text(max_call_count, 'provider call')} after approval.",
            ),
        ]
    )
    lines = [_rule("Safe Run Locked Until Setup Is Complete", width)]
    lines.extend(
        _kv_lines(
            first_step_items,
            width=width,
        )
    )
    lines.append("")
    lines.extend(
        _wrap(
            (
                "Do not type run execute while this page says locked. Run the "
                "PowerShell setup command in a normal PowerShell prompt first; "
                "use 2 Evidence Gaps only to inspect blockers."
            ),
            width,
        )
    )
    return lines


def _run_required_path_text(
    payload: Mapping[str, object],
    latest: Mapping[str, object],
) -> str:
    completed = latest.get("required_completed_count")
    total = latest.get("required_step_count")
    if completed is not None and total is not None:
        return f"{completed}/{total}"
    if _real_results_empty(payload):
        blocker = _readiness_first_setup_blocker(payload)
        if blocker:
            area = _human_source_name(blocker.get("area") or "setup blocker")
            return f"setup blocked; {_setup_blocker_first_label(area)}"
    return "not started"


def _run_operational_note(payload: Mapping[str, object]) -> str:
    if _real_results_empty(payload):
        blocker = _readiness_first_setup_blocker(payload)
        if blocker:
            area = _human_source_name(blocker.get("area") or "setup blocker")
            action = _humanize_dashboard_text(blocker.get("next_action"))
            return (
                f"Operational note: Run execute is not the next step yet. "
                f"{_setup_blocker_first_label(area)}: {action}"
            )
    return (
        "Operational note: execute live runs only after this call plan matches intent. "
        "Type `run execute` to start one capped cycle."
    )


def _call_plan_table_rows(call_plan: Mapping[str, object]) -> list[Mapping[str, object]]:
    rows: list[Mapping[str, object]] = []
    for row in _rows(call_plan.get("rows")):
        rows.append(
            {
                **dict(row),
                "status": _run_plan_status_label(row),
                "next_action": _run_plan_next_action_label(row),
            }
        )
    return rows


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
    coverage_text = _human_source_status_text(source_coverage.get("summary"))
    progress_parts = []
    if active or scanned or ranked:
        progress_parts.append(
            f"active {active}; scanned {scanned}; ranked {ranked}"
        )
    if coverage_text:
        progress_parts.append(coverage_text)
    blocker_text = ""
    if blocker:
        source = _human_source_name(blocker.get("source") or "source")
        gaps = int(_number_or_zero(blocker.get("gap_count")))
        status = _human_status_label(blocker.get("status") or "attention")
        blocker_text = f"{source} {status}; gaps {gaps}"
    operator_step = _priced_in_operator_step(payload)
    next_action = (
        operator_step.get("action")
        or answer.get("next_action")
        or audit.get("next_action")
        or (blocker.get("next_action") if blocker else None)
    )
    items: list[tuple[str, object]] = []
    if question:
        items.append(("Question", question))
    if current:
        items.append(("Current answer", current))
    trust_gate = _mapping(answer.get("full_market_trust_gate"))
    separate_setup_command = _real_results_empty(payload) and bool(
        _first_scan_setup_command(payload)
        or str(
            operator_step.get("tui_command") or operator_step.get("command") or ""
        ).strip()
    )
    setup_do_now = ""
    if separate_setup_command:
        action = _human_source_status_text(
            operator_step.get("action")
            or operator_step.get("action_label")
            or next_action
            or "Run the setup command."
        ).rstrip(".;")
        if action:
            setup_do_now = action
            items.append(("Do now", action))
        setup_cost = _operator_next_step_setup_cost(operator_step)
        if setup_cost:
            items.append(("Setup cost", setup_cost))
        setup_blocker = _operator_next_step_setup_blocker(operator_step)
        if setup_blocker:
            items.append(("Why blocked", setup_blocker))
    else:
        operator_step_text = _operator_next_step_summary(
            operator_step,
            include_command=True,
        )
        if operator_step_text:
            items.append(("Do now", operator_step_text))
    command = str(
        operator_step.get("tui_command") or operator_step.get("command") or ""
    ).strip()
    if separate_setup_command:
        command = _first_scan_setup_command(payload) or command
    if command:
        items.extend(
            _powershell_command_context_items(
                command,
                include_command=separate_setup_command,
            )
        )
    if trust_gate:
        gate_text = (
            f"{_human_status_label(trust_gate.get('status'))}; "
            f"{trust_gate.get('answer')}"
        )
        blocker_detail = _mapping(trust_gate.get("blocker_detail"))
        manual_csv_text = ""
        saved_capture: Mapping[str, object] = {}
        saved_capture_text = ""
        universe_text = ""
        if blocker_detail.get("source") == "market_bars":
            complete = int(_number_or_zero(blocker_detail.get("complete_rows")))
            missing = int(
                _number_or_zero(blocker_detail.get("missing_as_of_bar"))
            )
            empty = int(_number_or_zero(blocker_detail.get("empty_rows")))
            saved = _human_status_label(
                blocker_detail.get("provider_saved_file_status") or "n/a"
            )
            gate_text = (
                f"{gate_text}; manual CSV {complete}/{missing} complete"
                f", empty {empty}; saved file {saved}"
            )
            manual_csv_text = _market_bar_manual_csv_summary(
                _mapping(blocker_detail.get("manual_csv"))
            )
            saved_capture = _mapping(blocker_detail.get("saved_provider_capture"))
            capture_approval = _mapping(
                blocker_detail.get("provider_saved_file_capture_approval_packet")
            )
            if capture_approval:
                saved_capture = {**capture_approval, **saved_capture}
            saved_capture_text = _market_bar_saved_capture_summary(saved_capture)
            universe_text = _market_bar_missing_universe_summary(
                _mapping(blocker_detail.get("missing_universe"))
            )
        trust_label = "Trust gate"
        if _real_results_empty(payload):
            trust_label = "Evidence check"
            gate_text = _setup_evidence_check_text(trust_gate)
        items.append((trust_label, gate_text))
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
        ladder = _mapping(trust_gate.get("blocker_ladder"))
        if _real_results_empty(payload):
            ladder_text = _setup_blocker_ladder_summary(ladder)
            ladder_label = "Setup order"
        else:
            ladder_text = _trust_gate_blocker_ladder_summary(ladder)
            ladder_label = "Blocker ladder"
        if ladder_text:
            items.append((ladder_label, ladder_text))
        after_current_text = _after_current_blocker_summary(
            _mapping(trust_gate.get("after_current_blocker"))
        )
        if after_current_text:
            items.append(("After current", after_current_text))
        items.extend(
            _after_current_manual_command_items(
                _mapping(trust_gate.get("after_current_blocker"))
            )
        )
        if manual_csv_text:
            items.append(("Manual CSV", manual_csv_text))
        saved_capture_command = _market_bar_saved_capture_confirm_command(
            saved_capture
        )
        if saved_capture_command:
            items.append(
                ("Saved capture command", f"`{saved_capture_command}`")
            )
        if saved_capture_text:
            items.append(("Saved capture", saved_capture_text))
        if universe_text:
            items.append(("Missing universe", universe_text))
    if progress_parts:
        items.append(("Scan progress", "; ".join(progress_parts)))
    if blocker_text:
        items.append(("Trust blocker", blocker_text))
    if next_action:
        useful_next = _humanize_dashboard_text(next_action)
        if not (
            separate_setup_command
            and setup_do_now
            and _same_dashboard_sentence(useful_next, setup_do_now)
        ):
            items.append(("Useful next", useful_next))
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


def _operator_next_step_setup_cost(step: Mapping[str, object]) -> str:
    calls = int(_number_or_zero(step.get("external_calls_required")))
    changes = int(_number_or_zero(step.get("db_" + "writes_required")))
    call_text = _count_text(calls, "provider call")
    if bool(step.get("approval_required")) and calls:
        call_text = f"{call_text} after approval"
    return f"{call_text}; {_count_text(changes, 'database change')}."


def _operator_next_step_setup_blocker(step: Mapping[str, object]) -> str:
    blocker = str(step.get("first_blocker") or "").strip().lower()
    if not blocker:
        return ""
    labels = {
        "universe": "Active universe is not set up yet.",
        "active_universe": "Active universe is not set up yet.",
        "market_bars": "Latest market bars are missing or stale.",
        "scan": "No scan rows exist yet.",
        "agent_review": "AI review has not been approved or run.",
    }
    return labels.get(
        blocker,
        f"{_human_source_name(blocker)} is not set up yet.",
    )


def _count_text(count: int, noun: str) -> str:
    suffix = "" if count == 1 else "s"
    return f"{count} {noun}{suffix}"


def _same_dashboard_sentence(left: object, right: object) -> bool:
    return _sentence_key(left) == _sentence_key(right)


def _sentence_key(value: object) -> str:
    return str(value or "").strip().rstrip(".;").casefold()


def _setup_evidence_check_text(trust_gate: Mapping[str, object]) -> str:
    status = _human_status_label(trust_gate.get("status") or "blocked")
    answer = _human_source_status_text(trust_gate.get("answer"))
    answer = answer.replace(
        "priced-in evidence layer(s) complete",
        "evidence layers ready",
    )
    if answer:
        return f"{status}; {answer}"
    return f"{status}; setup evidence is not ready yet."


def _setup_blocker_ladder_summary(ladder: Mapping[str, object]) -> str:
    rows = [row for row in _rows(ladder.get("rows")) if isinstance(row, Mapping)]
    if not rows:
        return ""
    parts = []
    for row in rows[:5]:
        step = int(_number_or_zero(row.get("step")))
        source = _setup_blocker_name(row.get("source"))
        status = _human_status_label(row.get("status") or "waiting")
        prefix = f"{step}. " if step else ""
        parts.append(f"{prefix}{source} ({status})")
    return "; ".join(parts)


def _setup_blocker_name(source: object) -> str:
    key = str(source or "").strip().lower()
    names = {
        "universe": "Active universe",
        "active_universe": "Active universe",
        "market_bars": "Latest market bars",
        "agent_review": "AI review",
        "scan": "Capped scan",
    }
    return names.get(key, _human_source_name(source or "setup"))


def _run_source_status_display_items(
    items: Sequence[tuple[str, object]],
) -> list[tuple[str, object]]:
    return [(label, _human_source_status_text(value)) for label, value in items]


def _trust_gate_blocker_ladder_summary(ladder: Mapping[str, object]):
    rows = [
        row for row in _rows(ladder.get("rows")) if isinstance(row, Mapping)
    ]
    if not rows:
        return ""
    parts = []
    for row in rows[:5]:
        step = int(_number_or_zero(row.get("step")))
        source = _human_source_name(row.get("source") or "source")
        gap_count = int(_number_or_zero(row.get("gap_count")))
        status = _human_status_label(row.get("status") or "attention")
        parts.append(f"{step} {source} {status} gaps {gap_count}")
    return "; ".join(parts)


def _after_current_blocker_summary(preview: Mapping[str, object]):
    if not preview:
        return ""
    current = _human_source_name(preview.get("current_blocker") or "current blocker")
    source = _human_source_name(preview.get("next_source") or "")
    if not source:
        return ""
    status = _human_status_label(preview.get("next_status") or "attention")
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
                blocked_text = f"{blocked_text} {_human_status_label(reason)}"
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


def _after_current_manual_command_items(
    preview: Mapping[str, object],
) -> list[tuple[str, object]]:
    next_plan = _mapping(preview.get("next_source_plan"))
    if not next_plan:
        return []
    command_fields = (
        ("CIK repair", "repair", "manual_template_command"),
        ("CIK validate", "validate", "manual_validate_command"),
        ("CIK import", "import", "manual_fix_command"),
    )
    items: list[tuple[str, object]] = []
    for label, verb, field in command_fields:
        command = str(next_plan.get(field) or "").strip()
        if command:
            items.append((label, f"{verb} `{command}`"))
    return items


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
    status = _human_status_label(saved_capture.get("status") or "unknown")
    saved_file = _human_status_label(saved_capture.get("saved_file_status") or "n/a")
    approval = "yes" if saved_capture.get("approval_required") else "no"
    provider_key = "yes" if saved_capture.get("provider_key_configured") else "no"
    calls = int(_number_or_zero(saved_capture.get("external_calls_if_approved")))
    writes = int(_number_or_zero(saved_capture.get("db_writes_during_capture")))
    path = str(saved_capture.get("saved_file_path") or "").strip()
    api = str(saved_capture.get("capture_api") or "").strip()
    next_action = str(saved_capture.get("next_action") or "").strip()
    confirm_command = _market_bar_saved_capture_confirm_command(saved_capture)
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
    if confirm_command:
        parts.append(f"type `{confirm_command}`")
    if next_action:
        parts.append(next_action)
    return "; ".join(parts)


def _market_bar_saved_capture_confirm_command(
    saved_capture: Mapping[str, object],
) -> str:
    if not saved_capture:
        return ""
    confirm_command = str(
        saved_capture.get("tui_confirm_command")
        or saved_capture.get("dashboard_saved_capture_confirm_command")
        or saved_capture.get("capture_command")
        or saved_capture.get("tui_command")
        or ""
    ).strip()
    if not confirm_command and saved_capture.get("approval_required"):
        return "bars saved capture confirm"
    return confirm_command


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
    kind_labels = {
        "manual_csv": "manual CSV",
        "saved_provider_capture": "saved file capture",
        "validate_saved_file": "saved file check",
        "preview_import": "saved file import preview",
    }
    for option in options[:4]:
        if not isinstance(option, Mapping):
            continue
        kind = str(option.get("kind") or "option")
        kind_label = kind_labels.get(kind, _human_label(kind))
        status = _human_status_label(option.get("status") or "unknown")
        calls = int(_number_or_zero(option.get("external_calls_required")))
        command = str(option.get("command") or "").strip()
        if command:
            parts.append(f"{kind_label}: {status}, {calls} call(s), `{command}`")
        else:
            parts.append(f"{kind_label}: {status}, {calls} call(s)")
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
    saved_capture_command = _market_bar_provider_saved_capture_confirm_command(
        payload
    )
    if saved_capture_command:
        items.append(("Saved capture command", f"`{saved_capture_command}`"))
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
    status = _human_status_label(blocker.get("provider_fill_status") or "unknown")
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
                "source": _human_source_name(source.get("source")),
                "status": _human_status_label(source.get("status")),
                "coverage": f"{available}/{row_count}" if row_count else "n/a",
                "gap_count": int(_number_or_zero(source.get("gap_count"))),
                "next_action": _human_source_status_text(source.get("next_action")),
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
        rows.append(
            {
                **step,
                "area": _human_source_name(step.get("area")),
                "status": _human_status_label(step.get("status")),
                "depends_on": _human_source_status_text(depends or "none"),
                "action": _humanize_dashboard_text(step.get("action")),
            }
        )
    return rows


def _agent_brief_rows(
    brief: Mapping[str, object],
    payload: Mapping[str, object] | None = None,
) -> list[Mapping[str, object]]:
    rows: list[Mapping[str, object]] = _agent_coach_summary_rows(brief, payload)
    runtime = _mapping(brief.get("runtime"))
    if runtime:
        rows.append(
            {
                "kind": "Runtime",
                "item": _agent_runtime_name(runtime.get("orchestrator")),
                "detail": _agent_runtime_label(runtime),
            }
        )
    real_results = _mapping(brief.get("real_results"))
    if real_results:
        status = _human_status_label(real_results.get("status") or "unknown")
        next_action = (
            _no_real_result_next_action(payload, real_results)
            if payload is not None
            else _human_agent_text(real_results.get("next_action"))
        )
        if (
            payload is not None
            and _real_results_missing(payload)
            and not _real_results_empty(payload)
        ):
            row_count = _visible_scan_row_count(payload, real_results)
            missing = ", ".join(_texts(real_results.get("missing"))) or status
            rows.append(
                {
                    "kind": "Gate",
                    "item": "Trusted scan evidence",
                    "detail": (
                        f"scan rows {row_count} visible; missing {missing}; "
                        f"next {_human_agent_text(next_action)}"
                    ),
                }
            )
        else:
            rows.append(
                {
                    "kind": "Gate",
                    "item": f"Real results: {status}",
                    "detail": (
                        f"rows {real_results.get('row_count', 0)}; "
                        f"latest run {real_results.get('latest_run_id') or 'n/a'}; "
                        f"next {_human_agent_text(next_action)}"
                    ),
                }
            )
    credit_gate = _mapping(brief.get("credit_gate"))
    if credit_gate:
        status = _human_status_label(credit_gate.get("status") or "unknown")
        rows.append(
            {
                "kind": "Gate",
                "item": f"OpenAI budget: {status}",
                "detail": (
                    f"estimate={_format_usd_amount(credit_gate.get('estimated_cost_usd', 0))}; "
                    f"daily={_format_usd_amount(credit_gate.get('daily_spend_usd', 0))}/"
                    f"{_format_usd_amount(credit_gate.get('daily_budget_usd', 0))}; "
                    f"monthly={_format_usd_amount(credit_gate.get('monthly_spend_usd', 0))}/"
                    f"{_format_usd_amount(credit_gate.get('monthly_budget_usd', 0))}"
                ),
            }
        )
    for agent in _rows(brief.get("agents")):
        rows.append(
            {
                "kind": "Agent",
                "item": agent.get("agent") or "agent",
                "detail": _human_agent_text(
                    agent.get("summary") or agent.get("role") or ""
                ),
            }
        )
    for index, insight in enumerate(_texts(brief.get("insights")), start=1):
        rows.append(
            {
                "kind": "Insight",
                "item": str(index),
                "detail": _human_agent_text(insight),
            }
        )
    for index, action in enumerate(_texts(brief.get("next_actions")), start=1):
        rows.append(
            {
                "kind": "Next",
                "item": str(index),
                "detail": _human_agent_text(action),
            }
        )
    for check in _rows(brief.get("security_checks")):
        name = str(check.get("name") or "check")
        status = _human_status_label(check.get("status") or "unknown")
        rows.append(
            {
                "kind": "Safety",
                "item": f"{name}: {status}",
                "detail": _human_agent_text(check.get("detail") or ""),
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


def _agent_coach_summary_rows(
    brief: Mapping[str, object],
    payload: Mapping[str, object] | None = None,
) -> list[Mapping[str, object]]:
    runtime = _mapping(brief.get("runtime"))
    real_results = _mapping(brief.get("real_results"))
    credit_gate = _mapping(brief.get("credit_gate"))
    next_actions = _texts(brief.get("next_actions"))
    blocked_tools = _agent_blocked_tool_labels(runtime)
    blocked = _agent_tool_list_label(blocked_tools)
    setup_action = (
        _no_real_result_next_action(payload, real_results)
        if payload is not None and _real_results_empty(payload)
        else ""
    )
    evidence_waiting = (
        payload is not None
        and _real_results_missing(payload)
        and not _real_results_empty(payload)
    )
    evidence_action = (
        _no_real_result_next_action(payload, real_results) if evidence_waiting else ""
    )
    next_action = _human_agent_text(
        setup_action
        or evidence_action
        or _first_nonblank(
            credit_gate.get("next_action"),
            real_results.get("next_action"),
            next_actions[0] if next_actions else None,
        )
        or "Stay in preview until gates are ready."
    )
    credit_status = credit_gate.get("status") or runtime.get("credit_gate_status") or "unknown"
    rows = [
        {
            "kind": "Start",
            "item": "What can the agent do?",
            "detail": (
                "Read the local dashboard snapshot and propose research next actions; "
                "it cannot trade from this page."
            ),
        },
    ]
    if evidence_waiting:
        row_count = _visible_scan_row_count(payload, real_results)
        missing = ", ".join(_texts(real_results.get("missing"))) or "trusted evidence"
        rows.append(
            {
                "kind": "Gate",
                "item": "Trusted scan evidence",
                "detail": (
                    f"scan rows {row_count} visible; missing {missing}; "
                    "agent execute stays blocked."
                ),
            }
    )
    rows.extend(
        [
            {
                "kind": "Cost",
                "item": "OpenAI calls",
                "detail": (
                    "No calls made in preview; agent execute is required and still "
                    "passes through the credit gate."
                ),
            },
            {
                "kind": "Safety",
                "item": "What is blocked?",
                "detail": (
                    "real mode gate: "
                    f"{_human_status_label(runtime.get('real_mode_gate_status') or 'unknown')}; "
                    f"credit: {_human_status_label(credit_status)}; "
                    f"blocked tools: {blocked}."
                ),
            },
            {
                "kind": "Next",
                "item": "Safe next action",
                "detail": next_action,
            },
        ]
    )
    return rows


def _agent_runtime_name(value: object) -> str:
    normalized = str(value or "").strip().lower()
    if normalized == "openai_agents_sdk":
        return "OpenAI Agents SDK"
    return str(value or "unknown")


def _agent_tool_list_label(values: Sequence[str]) -> str:
    labels = [str(value).strip() for value in values if str(value).strip()]
    if not labels:
        return "none"
    if len(labels) == 1:
        return labels[0]
    if len(labels) == 2:
        return f"{labels[0]} and {labels[1]}"
    return f"{', '.join(labels[:-1])}, and {labels[-1]}"


def _agent_assistant_dependency_label(runtime: Mapping[str, object]) -> str:
    assistant_dependency = str(runtime.get("co" + "pilot_dependency") or "absent").strip()
    return (
        "GitHub Copilot not used"
        if assistant_dependency in {"", "absent", "none", "false"}
        else f"assistant dependency {_human_status_label(assistant_dependency)}"
    )


def _agent_blocked_tool_labels(runtime: Mapping[str, object]) -> list[str]:
    blocked_tools: list[str] = []
    for key, label in (
        ("external_market_tools", "market"),
        ("broker_tools", "broker"),
        ("shell_tools", "shell"),
        ("web_tools", "web"),
    ):
        if runtime.get(key) is False:
            blocked_tools.append(label)
    return blocked_tools


def _agent_disabled_tools_text(blocked_tools: Sequence[str]) -> str:
    if not blocked_tools:
        return "no tools disabled"
    return f"{_agent_tool_list_label(blocked_tools)} tools disabled"


def _agent_runtime_setup_items(runtime: Mapping[str, object]) -> list[tuple[str, str]]:
    gate = _human_status_label(runtime.get("real_mode_gate_status") or "unknown")
    evidence = _human_status_label(runtime.get("real_results_gate_status") or "unknown")
    credit = _human_status_label(runtime.get("credit_gate_status") or "unknown")
    blocked_tools = _agent_blocked_tool_labels(runtime)
    return [
        (
            "Runtime",
            _agent_runtime_name(runtime.get("orchestrator") or "openai_agents_sdk"),
        ),
        ("Assistant", _agent_assistant_dependency_label(runtime)),
        ("Agent gates", f"real-agent gate {gate}; scan evidence {evidence}"),
        ("OpenAI spend", f"OpenAI spend {credit}"),
        ("Disabled tools", _agent_disabled_tools_text(blocked_tools)),
    ]


def _agent_runtime_label(runtime: Mapping[str, object]) -> str:
    orchestrator = _agent_runtime_name(runtime.get("orchestrator") or "openai_agents_sdk")
    assistant_text = _agent_assistant_dependency_label(runtime)
    tools = str(runtime.get("tool_surface") or "read_only_snapshot_tools").replace("_", "-")
    tools = tools.replace("read-only-snapshot-tools", "read-only snapshot tools")
    gate = _human_status_label(runtime.get("real_mode_gate_status") or "unknown")
    real_results_gate = _human_status_label(
        runtime.get("real_results_gate_status") or "unknown"
    )
    credit_gate = _human_status_label(runtime.get("credit_gate_status") or "unknown")
    blocked_tools = _agent_blocked_tool_labels(runtime)
    blocked_text = _agent_disabled_tools_text(blocked_tools)
    return (
        f"{orchestrator}; {assistant_text}; tools use {tools}; "
        f"real-agent gate {gate}; scan evidence {real_results_gate}; "
        f"OpenAI spend {credit_gate}; {blocked_text}"
    )


def _candidates_lines(payload: Mapping[str, object], width: int) -> list[str]:
    readiness = _mapping(payload.get("readiness"))
    decision_safe = readiness.get("safe_to_make_investment_decision") is True
    rows = [
        _candidate_table_row(row, row_key=str(index), decision_safe=decision_safe)
        for index, row in enumerate(_candidate_rows(payload), start=1)
    ]
    lines = [_rule("Candidates", width)]
    if _real_results_empty(payload):
        lines.extend(
            _locked_review_setup_lines(
                payload,
                width,
                title="No candidate packets yet.",
                unlocks=(
                    "This page opens individual stock cases after MarketRadar has "
                    "real scan rows."
                ),
                after_setup=(
                    "fill latest bars, run one capped scan, then return here "
                    "from Inbox."
                ),
            )
        )
        return lines
    if not decision_safe:
        lines.extend(
            _wrap(
                "Research-only: candidates are inspection targets, not trade ideas. "
                "Press 2 Evidence Gaps before acting.",
                width,
            )
        )
    lines.extend(
        _table_lines(
            _indexed(rows),
            _candidate_text_columns(width),
            width=width,
            limit=30,
        )
    )
    lines.extend(
        _wrap(
            "Gap is emotion minus price reaction. Positive gap means the market may not "
            "have fully priced the catalyst.",
            width,
        )
    )
    lines.extend(
        _wrap(
            "Use `open #|TICKER` to inspect a candidate; this is not trade approval.",
            width,
        )
    )
    return lines


def _candidate_text_columns(width: int) -> list[tuple[str, str, int]]:
    if width >= 160:
        return [
            ("index", "#", 4),
            ("ticker", "Ticker", 7),
            ("priced_in_status", "Priced-in", 20),
            ("emotion_reaction_gap", "Gap", 7),
            ("score", "Score", 7),
            ("data_coverage", "Evidence", 36),
            ("why_now", "Why Now", 30),
            ("next_step", "Next Step", 28),
        ]
    return [
        ("index", "#", 4),
        ("ticker", "Ticker", 6),
        ("priced_in_status", "Priced-in", 14),
        ("emotion_reaction_gap", "Gap", 6),
        ("score", "Score", 6),
        ("data_coverage", "Evidence", 24),
        ("why_now", "Why Now", 20),
        ("next_step", "Next Step", 19),
    ]


def _candidate_table_row(
    row: Mapping[str, object],
    *,
    row_key: str,
    decision_safe: bool = False,
) -> Mapping[str, object]:
    brief = _mapping(row.get("research_brief"))
    source_next_step = (
        ((_priced_in_reason(row) and row.get("priced_in_next_step")) or None)
        or row.get("next_step")
        or row.get("decision_next_step")
        or brief.get("next_step")
    )
    next_step = (
        source_next_step
        if decision_safe
        else "Fix Evidence Gaps first."
    )
    priced_in_status = str(row.get("priced_in_status") or "").strip()
    why_now = (
        _priced_in_reason(row)
        or brief.get("why_now")
        or row.get("top_event_title")
        or row.get("risk_or_gap")
    )
    return {
        **dict(row),
        "_row_key": row_key,
        "score": row.get("score") or row.get("final_score"),
        "data_coverage": _data_coverage_summary(row),
        "why_now": _humanize_dashboard_text(why_now),
        "next_step": _humanize_dashboard_text(
            next_step or "Open candidate detail and review the evidence."
        ),
        "priced_in_status": _priced_in_signal(
            priced_in_status,
            fallback=_human_status_label(priced_in_status or "n/a"),
        ),
    }


def _data_coverage_summary(row: Mapping[str, object]) -> str:
    data_sources = row.get("priced_in_data_sources") or row.get("data_sources")
    if isinstance(data_sources, Mapping):
        available = _human_source_names(data_sources.get("available"))
        missing = _human_source_names(data_sources.get("missing"))
        stale = _human_source_names(data_sources.get("stale"))
        parts: list[str] = []
        if available:
            parts.append(f"have {_compact_source_names(available)}")
        if missing:
            parts.append(f"need {_compact_source_names(missing)}")
        if stale:
            parts.append(f"stale {_compact_source_names(stale)}")
        if parts:
            return "; ".join(parts)
        summary = str(data_sources.get("summary") or "").strip()
        if summary:
            return _human_source_status_text(summary)
    return "n/a"


def _human_source_names(value: object) -> list[str]:
    return [
        _human_source_name(item)
        for item in _rows_or_values(value)
        if str(item).strip()
    ]


def _compact_source_names(names: Sequence[str]) -> str:
    clean = [name for name in names if name]
    if not clean:
        return ""
    first = _short_source_name(clean[0])
    if len(clean) == 1:
        return first
    return f"{first} +{len(clean) - 1}"


def _short_source_name(name: str) -> str:
    labels = {
        "broker context": "broker",
        "candidate packet": "packet",
        "catalyst events": "events",
        "decision card": "card",
        "local text": "text",
        "theme/peer/sector": "theme/peers",
    }
    return labels.get(name, name)


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


def _candidate_case_detail_table_rows(
    payload: Mapping[str, object],
    ticker: str,
    row: Mapping[str, object],
) -> list[Mapping[str, object]]:
    if not row:
        return _mapping_items(_compact_detail(row))
    pairs = (
        *_candidate_case_summary_kv_pairs(payload, ticker, row),
        *_candidate_detail_kv_pairs(row),
    )
    return [{"key": key, "value": value} for key, value in pairs]


def _candidate_case_summary_kv_pairs(
    payload: Mapping[str, object],
    ticker: str,
    row: Mapping[str, object],
) -> tuple[tuple[str, object], ...]:
    readiness = _mapping(payload.get("readiness"))
    safe = bool(readiness.get("safe_to_make_investment_decision"))
    brief = _mapping(row.get("priced_in_evidence_brief"))
    source_gaps = _candidate_case_source_gap_summary(row, brief)
    why = (
        _priced_in_reason(row)
        or brief.get("why_now")
        or row.get("top_event_title")
        or row.get("top_catalyst")
        or row.get("risk_or_gap")
        or "No plain-language reason captured."
    )
    can_act = (
        "No - research only until readiness says this is decision-ready."
        if not safe
        else "Not trade approval; verify the evidence before any action."
    )
    pairs: list[tuple[str, object]] = [
        ("Can I act now?", can_act),
        ("What happened?", why),
        ("What is missing?", source_gaps or "none"),
        ("Next safe action", _candidate_case_next_safe_action(payload, ticker)),
    ]
    next_command = _candidate_case_next_command(row, ticker)
    if next_command:
        pairs.append(("Next command", next_command))
        pairs.append(
            (
                "Where to run",
                "normal PowerShell prompt, not the dashboard command box.",
            )
        )
        pairs.append(("Command boundary", _candidate_case_command_boundary(next_command)))
    return tuple(pairs)


def _candidate_case_next_command(row: Mapping[str, object], ticker: str) -> str:
    brief = _mapping(row.get("priced_in_evidence_brief"))
    if _candidate_case_has_source_gaps(row, brief):
        return ""
    usefulness = _mapping(brief.get("usefulness")) or _mapping(row.get("usefulness"))
    explicit = str(
        _first_nonblank(
            row.get("priced_in_next_command"),
            brief.get("next_command"),
            usefulness.get("next_command"),
        )
        or ""
    ).strip()
    if explicit:
        return explicit
    next_step = str(
        _first_nonblank(
            brief.get("next_step") if brief else None,
            row.get("priced_in_next_step"),
            row.get("next_step"),
            row.get("decision_next_step"),
        )
        or ""
    ).lower()
    command_ticker = (
        ticker.strip().upper()
        or str(row.get("ticker") or "TICKER").strip().upper()
    )
    command_ticker = command_ticker or "TICKER"
    command_as_of = _candidate_case_command_as_of(row)
    if "candidate packet" in next_step and not str(row.get("candidate_packet_id") or "").strip():
        return (
            "catalyst-radar build-packets "
            f"--as-of {command_as_of} --ticker {command_ticker} "
            "--min-state ResearchOnly"
        )
    if (
        "decision card" in next_step
        and str(row.get("candidate_packet_id") or "").strip()
        and not str(row.get("decision_card_id") or "").strip()
    ):
        return (
            "catalyst-radar build-decision-cards "
            f"--as-of {command_as_of} --ticker {command_ticker} "
            "--min-state ResearchOnly"
        )
    return ""


def _candidate_case_has_source_gaps(
    row: Mapping[str, object],
    brief: Mapping[str, object],
) -> bool:
    source_gaps = _candidate_case_source_gap_summary(row, brief)
    return source_gaps not in {"", "none", "n/a"}


def _candidate_case_command_as_of(row: Mapping[str, object]) -> str:
    parsed = _datetime_or_none(row.get("as_of"))
    if parsed is None:
        return "LATEST_TRADING_DATE"
    return parsed.date().isoformat()


def _candidate_case_command_boundary(command: str) -> str:
    if " build-packets " in f" {command} ":
        return "Local DB write; no provider, OpenAI, broker, or order calls."
    if " build-decision-cards " in f" {command} ":
        return "Local review artifact write; no broker or order calls."
    return "Review before running; browsing this page made 0 calls."


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
        source_gaps = _candidate_source_action_summary(brief)
        hard_blocker = (
            "yes - hard blocker recorded"
            if brief.get("blocked")
            else "no hard blocker recorded"
        )
        blocked_by_source_gaps = _candidate_case_has_source_gaps(row, brief)
        usefulness_summary = (
            "Research-useful mismatch; blocked until Evidence Gaps clear."
            if blocked_by_source_gaps
            else _candidate_usefulness_summary(brief)
        )
        next_step = (
            "Open 2 Evidence Gaps for global blockers before packets."
            if blocked_by_source_gaps
            else brief.get("next_step")
        )
        return (
            ("Signal", _priced_in_signal(str(brief.get("status") or ""), fallback="Candidate")),
            ("Usefulness", usefulness_summary),
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
            ("Source gaps", source_gaps),
            ("Hard blocker", hard_blocker),
            ("Blocker details", blockers or "none recorded"),
            ("Next step", next_step),
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
            (_human_source_name(action.get("source")).lower(), action.get("next_action")),
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
    lines.extend(
        _kv_lines(
            (
                *_candidate_case_summary_kv_pairs(payload, ticker, row),
                *_candidate_detail_kv_pairs(row),
            ),
            width=width,
        )
    )
    return lines


def _alert_detail_row(payload: Mapping[str, object], alert_id: str) -> Mapping[str, object]:
    rows = _rows(_mapping(payload.get("alerts")).get("rows"))
    return next((item for item in rows if str(item.get("id") or "") == alert_id), {})


def _alert_feedback_prompt_parts(
    payload: Mapping[str, object],
    alert_id: str,
) -> tuple[str, str]:
    alert_ref = str(alert_id or "ALERT_ID").strip()
    rows = _rows(_mapping(payload.get("alerts")).get("rows"))
    for index, row in enumerate(rows, start=1):
        if str(row.get("id") or "").strip() == alert_ref:
            label = str(row.get("ticker") or alert_ref).strip().upper()
            return label, str(index)
    return alert_ref, alert_ref


def _alert_case_detail_table_rows(
    row: Mapping[str, object],
    *,
    feedback_ref: str | None = None,
) -> list[Mapping[str, object]]:
    if not row:
        return _mapping_items(_compact_detail(row))
    pairs = (
        *_alert_case_summary_kv_pairs(row, feedback_ref=feedback_ref),
        *_alert_detail_kv_pairs(row),
    )
    return [{"key": key, "value": value} for key, value in pairs]


def _alert_case_summary_kv_pairs(
    row: Mapping[str, object],
    *,
    feedback_ref: str | None = None,
) -> tuple[tuple[str, object], ...]:
    alert_id = str(row.get("id") or "ALERT_ID").strip()
    ticker = str(row.get("ticker") or "n/a").strip().upper()
    reason = (
        row.get("summary")
        or row.get("reason")
        or row.get("title")
        or _alert_evidence_summary(row)
        or "No plain-language reason captured."
    )
    trigger = _join_nonempty(
        (
            _human_status_label(row.get("trigger_kind")),
            _human_status_label(row.get("route")),
            _human_status_label(row.get("priority")),
        ),
        separator=" / ",
    )
    feedback_command = f"feedback {feedback_ref or alert_id} useful|noisy|acted [notes]"
    return (
        ("Why did I get this?", reason),
        ("Is this a trade signal?", "No - alert rows are review prompts, not trade approval."),
        (
            "Next safe action",
            f"Open the {ticker} candidate case, then record feedback after review.",
        ),
        ("Feedback command", feedback_command),
        ("Trigger", trigger),
    )


def _alert_detail_kv_pairs(row: Mapping[str, object]) -> tuple[tuple[str, object], ...]:
    return (
        ("Ticker", row.get("ticker")),
        ("Status", _human_status_label(row.get("status"))),
        ("Delivery", _human_status_label(row.get("route"))),
        ("Priority", _human_status_label(row.get("priority"))),
        ("Title", row.get("title")),
        ("Reason", row.get("reason") or row.get("summary")),
        ("Created", row.get("created_at")),
        ("Feedback", row.get("feedback_label") or row.get("feedback")),
    )


def _alert_evidence_summary(row: Mapping[str, object]) -> str:
    payload = _mapping(row.get("payload"))
    evidence = _rows(payload.get("evidence"))
    titles = [str(item.get("title") or "").strip() for item in evidence]
    return "; ".join(title for title in titles if title)


def _locked_review_setup_lines(
    payload: Mapping[str, object],
    width: int,
    *,
    title: str,
    unlocks: str,
    after_setup: str,
) -> list[str]:
    command = _first_scan_setup_command(payload)
    blocker = _readiness_first_setup_blocker(payload)
    if blocker:
        area = _human_source_name(blocker.get("area") or "setup blocker")
        next_action = f"{_setup_blocker_first_label(area)}."
    else:
        next_action = "Start with setup row 1."
    lines = [title, "No market scan has run yet, so this page is locked."]
    lines.extend(_wrap(unlocks, width))
    lines.extend(
        _kv_lines(
            [
                ("Do first", next_action),
                ("PowerShell command", command or "No setup command recorded."),
                (
                    "Approval",
                    "Continue only if you accept the data change or provider call.",
                ),
                (
                    "Where to run",
                    "normal PowerShell prompt, not the dashboard command box.",
                ),
                ("After setup", after_setup),
                ("Browsing", "0 provider calls, 0 OpenAI calls, 0 orders."),
            ],
            width=width,
        )
    )
    return lines


def _alerts_lines(payload: Mapping[str, object], width: int) -> list[str]:
    rows = _rows(_mapping(payload.get("alerts")).get("rows"))
    lines = [_rule("Alerts", width)]
    lines.extend(_wrap("Alerts are research notifications, not trade signals or orders.", width))
    if not rows:
        if _real_results_empty(payload):
            lines.extend(
                _locked_review_setup_lines(
                    payload,
                    width,
                    title="No alert messages yet.",
                    unlocks=(
                        "Alerts appear only after real scan rows survive evidence "
                        "gates and become research notifications."
                    ),
                    after_setup=(
                        "review candidates first; alerts come from reviewed "
                        "research rows."
                    ),
                )
            )
        else:
            lines.extend(
                _wrap(
                    "No alert rows yet. Alerts appear after reviewed candidates create "
                    "research notifications; nothing here is a trade signal.",
                    width,
                )
            )
        return lines
    lines.extend(
        _table_lines(
            _indexed(
                [
                    _alert_table_row(row, row_key=str(row.get("id") or index))
                    for index, row in enumerate(rows, start=1)
                ]
            ),
            [
                ("index", "#", 4),
                ("ticker", "Ticker", 8),
                ("status_label", "Status", 12),
                ("route_label", "Delivery", 22),
                ("priority_label", "Priority", 10),
                ("title", "Message", 68),
            ],
            width=width,
            limit=16,
        )
    )
    lines.extend(
        _wrap(
            "Use `open #` to review; the detail view shows the exact feedback "
            "command and records local review only.",
            width,
        )
    )
    return lines


def _alert_table_row(row: Mapping[str, object], *, row_key: str) -> Mapping[str, object]:
    return {
        **dict(row),
        "_row_key": row_key,
        "status_label": _human_status_label(row.get("status")),
        "route_label": _human_status_label(row.get("route")),
        "priority_label": _human_status_label(row.get("priority")),
        "title": _humanize_dashboard_text(row.get("title")),
    }


def _alert_display_title(row: Mapping[str, object], alert_id: str) -> str:
    ticker = str(row.get("ticker") or "").strip().upper()
    if ticker:
        return f"Alert {ticker}"
    title = _humanize_dashboard_text(row.get("title")).strip()
    if title:
        return f"Alert - {title}"
    return f"Alert {alert_id or 'n/a'}"


def _alert_open_status_message(row: Mapping[str, object], alert_id: str) -> str:
    ticker = str(row.get("ticker") or "").strip().upper()
    label = f"{ticker} alert" if ticker else f"alert {alert_id}"
    return (
        f"No calls. Not a trade signal. Opened {label}. "
        "Record local feedback after review."
    )


def _alert_detail_lines(payload: Mapping[str, object], alert_id: str, width: int) -> list[str]:
    row = _alert_detail_row(payload, alert_id)
    lines = [_rule(_alert_display_title(row, alert_id), width)]
    if not row:
        lines.append("Alert not found for the current filters.")
        return lines
    lines.extend(
        _kv_lines(
            (
                *_alert_case_summary_kv_pairs(
                    row,
                    feedback_ref=_alert_feedback_prompt_parts(payload, alert_id)[1],
                ),
                *_alert_detail_kv_pairs(row),
            ),
            width=width,
        )
    )
    return lines


def _ipo_lines(payload: Mapping[str, object], width: int) -> list[str]:
    rows = _rows(_mapping(payload.get("ipo_s1")).get("rows"))
    lines = [_rule("IPO / S-1", width)]
    if not rows:
        if _real_results_empty(payload):
            lines.extend(
                _locked_review_setup_lines(
                    payload,
                    width,
                    title="No IPO/S-1 catalyst rows yet.",
                    unlocks=(
                        "IPO/S-1 filings are optional catalyst evidence after the "
                        "main market scan is set up."
                    ),
                    after_setup=(
                        "review Inbox and Candidate Review first; refresh SEC "
                        "ingestion only when you intentionally need new filing evidence."
                    ),
                )
            )
        else:
            lines.extend(
                _wrap(
                    "No IPO/S-1 rows in this snapshot. Continue with Inbox or "
                    "Candidates, or refresh SEC ingestion if you expected new filings.",
                    width,
                )
            )
        return lines
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
    if not rows:
        if _real_results_empty(payload):
            lines.extend(
                _locked_review_setup_lines(
                    payload,
                    width,
                    title="No theme clusters yet.",
                    unlocks=(
                        "Theme clusters appear only after real scan rows reveal "
                        "repeated catalyst patterns across stocks."
                    ),
                    after_setup=(
                        "review Inbox and Candidate Review first; themes are "
                        "secondary context, not trade signals."
                    ),
                )
            )
        else:
            lines.extend(
                _wrap(
                    "No theme clusters in this snapshot. Continue with Inbox or "
                    "Candidates until repeated catalyst patterns appear.",
                    width,
                )
            )
        return lines
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
    latest_run = _mapping(validation.get("latest_run"))
    report = _mapping(validation.get("report"))
    lines = [_rule("Validation", width)]
    if not latest_run and not report:
        if _real_results_empty(payload):
            lines.extend(
                _locked_review_setup_lines(
                    payload,
                    width,
                    title="No validation report yet.",
                    unlocks=(
                        "Validation measures whether MarketRadar's research rows "
                        "were useful after real scan evidence exists."
                    ),
                    after_setup=(
                        "run one capped scan, review candidates, then return here "
                        "after validation replay or outcome tracking exists."
                    ),
                )
            )
        else:
            lines.extend(
                _wrap(
                    "No validation report yet. Keep this research-only until shadow or "
                    "paper validation has outcomes to measure.",
                    width,
                )
            )
            lines.extend(
                _wrap(
                    "Next: use Inbox and Candidates for research review; return here "
                    "after validation replay or outcome tracking exists.",
                    width,
                )
            )
        return lines
    lines.extend(
        _kv_lines(
            (
                ("Latest run", latest_run.get("id") or "n/a"),
                ("Status", latest_run.get("status") or "n/a"),
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
    value_ledger = _mapping(payload.get("value_ledger"))
    value_outcomes = _mapping(payload.get("value_outcomes"))
    value_report = _mapping(payload.get("value_report"))
    candidate_coverage = _mapping(value_report.get("candidate_ledger_coverage"))
    outcome_coverage = _mapping(value_report.get("value_outcome_coverage"))
    validation_evidence = _mapping(value_report.get("validation_evidence"))
    validation_items: list[tuple[str, object]] = [
        (
            "Validation evidence",
            _human_status_label(validation_evidence.get("status") or "not_started"),
        ),
    ]
    if validation_evidence:
        validation_items.append(
            (
                "Mission baselines measured",
                _baseline_coverage_text(validation_evidence),
            )
        )
    metric_items = _validation_metric_items(validation_evidence)
    if metric_items:
        validation_items.extend(metric_items)
    else:
        validation_items.append(
            (
                "Validation next step",
                "No validation baseline yet. Keep value claims tentative until "
                "shadow or paper validation records outcomes.",
            )
        )
    lines = [_rule("Costs", width)]
    if _costs_waiting_for_first_scan(payload, costs, value_ledger, value_outcomes):
        lines.extend(
            _locked_review_setup_lines(
                payload,
                width,
                title="No cost or value proof yet.",
                unlocks=(
                    "Costs become useful after a real scan creates candidates, "
                    "feedback, and outcomes to measure."
                ),
                after_setup=(
                    "run one capped scan, review candidates, then record feedback "
                    "before judging whether MarketRadar is worth its cost."
                ),
            )
        )
        # Keep the pre-scan target readable; detailed decimals belong in the ledger.
        lines.extend(_kv_lines(_costs_empty_value_rows(value_ledger), width=width))
        return lines
    lines.extend(
        _kv_lines(
            (
                ("Attempt count", costs.get("attempt_count")),
                ("Actual cost", _format_usd_amount(costs.get("total_actual_cost_usd"))),
                (
                    "Estimated cost",
                    _format_usd_amount(costs.get("total_estimated_cost_usd")),
                ),
                ("Useful alerts", costs.get("useful_alert_count")),
                ("Cost per useful alert", _cost_per_useful_alert_text(costs)),
            ),
            width=width,
        )
    )
    lines.append("")
    lines.extend(
        _kv_lines(
            (
                ("Value ledger entries", value_ledger.get("entry_count")),
                (
                    "Weighted value",
                    _format_usd_amount(
                        value_ledger.get("confidence_weighted_value_usd"),
                    ),
                ),
                ("Ledger cost", _format_usd_amount(value_ledger.get("cost_to_produce_usd"))),
                (
                    "Net weighted value",
                    _format_usd_amount(
                        value_ledger.get("net_confidence_weighted_value_usd"),
                    ),
                ),
                (
                    "Monthly target",
                    _format_usd_amount(value_ledger.get("target_monthly_value_usd")),
                ),
                (
                    "Target coverage",
                    _format_percentage_amount(value_ledger.get("target_coverage_pct")),
                ),
                (
                    "ChatGPT Pro offset",
                    _format_percentage_amount(value_ledger.get("chatgpt_pro_offset_pct")),
                ),
            ),
            width=width,
        )
    )
    lines.append("")
    lines.extend(
        _kv_lines(
            (
                (
                    "Monthly value verdict",
                    _human_status_label(value_report.get("verdict") or "n/a"),
                ),
                ("Report month", value_report.get("month") or "n/a"),
                (
                    "Net decision-support value",
                    _format_usd_amount(
                        value_report.get("net_decision_support_value_usd"),
                    ),
                ),
                (
                    "$40 threshold met",
                    _yes_no_label(value_report.get("plausibly_earned_at_least_40_usd")),
                ),
                ("Useful insights", value_report.get("useful_insights_count")),
                ("Noisy insights", value_report.get("noisy_insights_count")),
                ("False positives", value_report.get("false_positive_count")),
                (
                    "Candidate ledger coverage",
                    _candidate_ledger_coverage_text(candidate_coverage),
                ),
                (
                    "Missing candidate ledgers",
                    candidate_coverage.get("missing_ledger_count"),
                ),
                (
                    "Value outcome coverage",
                    _value_outcome_coverage_text(outcome_coverage),
                ),
                (
                    "Missing value outcomes",
                    outcome_coverage.get("missing_outcome_count"),
                ),
                *validation_items,
                (
                    "Monthly value blocker",
                    _human_status_label(value_report.get("first_blocker") or "none"),
                ),
                ("Value next action", value_report.get("canonical_next_action") or "n/a"),
                (
                    "Value next command",
                    value_report.get("canonical_next_command") or "n/a",
                ),
            ),
            width=width,
        )
    )
    if value_report:
        lines.extend(_wrap(str(value_report.get("decision_support_note") or ""), width))
    lines.append("")
    evidence_examples = _rows(value_report.get("value_evidence_examples"))
    if evidence_examples:
        lines.append("Monthly evidence examples")
        lines.extend(
            _table_lines(
                evidence_examples,
                [
                    ("category", "Type", 14),
                    ("ticker", "Ticker", 8),
                    ("feedback_label", "Label", 14),
                    ("supported_action", "Action", 12),
                    ("user_decision", "Decision", 10),
                    ("outcome_status", "Outcome", 10),
                    ("primary_return_text", "Return", 10),
                    ("attributed_value_usd", "Value", 8),
                    ("artifact_id", "Artifact", 20),
                ],
                width=width,
                limit=8,
            )
        )
        lines.append("")
    lines.extend(_wrap(str(value_ledger.get("useful_definition") or ""), width))
    lines.append("")
    lines.extend(
        _kv_lines(
            (
                ("Outcome rows", value_outcomes.get("outcome_count")),
                ("Outcome status counts", value_outcomes.get("status_counts")),
            ),
            width=width,
        )
    )
    lines.append("")
    top_entries = _rows(value_ledger.get("top_entries"))
    if top_entries:
        lines.extend(
            _table_lines(
                top_entries,
                [
                    ("entry_date", "Date", 12),
                    ("ticker", "Ticker", 8),
                    ("label", "Label", 20),
                    ("supported_action", "Action", 14),
                    ("user_decision", "Decision", 14),
                    ("confidence_weighted_value_usd", "Weighted", 12),
                    ("outcome_status", "Outcome", 12),
                    ("artifact_id", "Artifact", 36),
                ],
                width=width,
                limit=8,
            )
        )
    else:
        lines.extend(
            _wrap(
                "No value-ledger entries yet. Record alert feedback and outcomes before "
                "judging whether Market Radar is worth its cost.",
                width,
            )
        )
    lines.append("")
    status_counts = _mapping_items(_mapping(costs.get("status_counts")))
    if status_counts:
        lines.extend(
            _table_lines(
                status_counts,
                [("key", "Status", 24), ("value", "Count", 12)],
                width=width,
                limit=10,
            )
        )
    else:
        lines.extend(
            _wrap(
                "No cost attempts have been recorded. Browse freely; provider and OpenAI "
                "costs appear here only after an executed run.",
                width,
            )
        )
    return lines


def _costs_waiting_for_first_scan(
    payload: Mapping[str, object],
    costs: Mapping[str, object],
    value_ledger: Mapping[str, object],
    value_outcomes: Mapping[str, object],
) -> bool:
    value_report = _mapping(payload.get("value_report"))
    return (
        _real_results_empty(payload)
        and int(_number_or_zero(costs.get("attempt_count"))) <= 0
        and int(_number_or_zero(value_ledger.get("entry_count"))) <= 0
        and int(_number_or_zero(value_outcomes.get("outcome_count"))) <= 0
        and str(value_report.get("first_blocker") or "") == "candidate_evidence"
    )


def _costs_empty_value_rows(
    value_ledger: Mapping[str, object],
) -> tuple[tuple[str, object], ...]:
    target = value_ledger.get("target_monthly_value_usd") or 40
    if isinstance(target, int | float) and not isinstance(target, bool):
        target_text = f"${target:g}"
    else:
        target_text = f"${target}"
    return (
        ("Monthly target", f"{target_text} of decision-support value."),
        (
            "Useful means",
            "saved research time, avoided a bad action, or produced "
            "a forward-testable hypothesis.",
        ),
        (
            "Cost attempts",
            "none recorded; browsing this page spends 0 provider and "
            "0 OpenAI calls.",
        ),
    )


def _cost_per_useful_alert_text(costs: Mapping[str, object]) -> object:
    useful_count = int(_number_or_zero(costs.get("useful_alert_count")))
    if useful_count <= 0:
        return "not measurable (0 useful alerts)"
    cost_per_useful = costs.get("cost_per_useful_alert")
    if cost_per_useful in (None, ""):
        return "not measured yet"
    return _format_usd_amount(cost_per_useful)


def _candidate_ledger_coverage_text(coverage: Mapping[str, object]) -> str:
    if not coverage:
        return "n/a"
    logged = int(_number_or_zero(coverage.get("logged_candidate_count")))
    surfaced = int(_number_or_zero(coverage.get("surfaced_candidate_count")))
    if surfaced <= 0:
        return f"{logged}/0 (no surfaced candidates)"
    pct = coverage.get("coverage_pct")
    pct_text = f"{pct}%" if pct is not None else "coverage pending"
    return f"{logged}/{surfaced} ({pct_text})"


def _value_outcome_coverage_text(coverage: Mapping[str, object]) -> str:
    if not coverage:
        return "n/a"
    linked = int(_number_or_zero(coverage.get("linked_outcome_count")))
    entries = int(_number_or_zero(coverage.get("ledger_entry_count")))
    if entries <= 0:
        return f"{linked}/0 (no ledger entries)"
    pct = coverage.get("coverage_pct")
    pct_text = f"{pct}%" if pct is not None else "coverage pending"
    return f"{linked}/{entries} ({pct_text})"


def _validation_metric_items(validation: Mapping[str, object]) -> list[tuple[str, object]]:
    candidates = [
        (
            "Baseline comparison",
            _baseline_result_counts_text(validation),
        ),
        (
            "Precision at 5 / 10",
            _precision_pair_text(validation),
        ),
        (
            "Backtest hit rate",
            _backtest_hit_rate_text(validation),
        ),
        (
            "Backtest drawdown proxy",
            _backtest_drawdown_text(validation),
        ),
        (
            "Backtest slippage",
            _backtest_slippage_text(validation),
        ),
        (
            "Backtest benchmark",
            _backtest_benchmark_text(validation),
        ),
    ]
    return [
        (label, value)
        for label, value in candidates
        if not _missing_validation_metric(value)
    ]


def _missing_validation_metric(value: object) -> bool:
    text = str(value or "").strip().lower()
    return text in {"", "n/a", "n/a / n/a"}


def _baseline_coverage_text(validation: Mapping[str, object]) -> str:
    if not validation:
        return "n/a"
    measured = _sequence_count(validation.get("measured_baselines"))
    required = _sequence_count(validation.get("required_baselines"))
    return f"{measured}/{required}"


def _baseline_result_counts_text(validation: Mapping[str, object]) -> str:
    counts = _mapping(validation.get("baseline_result_counts"))
    if not counts:
        return "n/a"
    ordered = [
        ("marketradar_wins", "MR wins"),
        ("tie", "ties"),
        ("baseline_wins", "baseline wins"),
        ("insufficient_evidence", "insufficient"),
        ("missing", "missing"),
    ]
    parts = [
        f"{label}={int(counts.get(key) or 0)}"
        for key, label in ordered
        if int(counts.get(key) or 0) > 0
    ]
    return ", ".join(parts) if parts else "n/a"


def _precision_pair_text(validation: Mapping[str, object]) -> str:
    if not validation:
        return "n/a"
    at_5 = validation.get("precision_at_5")
    at_10 = validation.get("precision_at_10")
    return f"{at_5 if at_5 is not None else 'n/a'} / {at_10 if at_10 is not None else 'n/a'}"


def _backtest_hit_rate_text(validation: Mapping[str, object]) -> str:
    summary = _mapping(validation.get("backtest_summary"))
    if not summary:
        return "n/a"
    hit_rate = _pct_text(summary.get("hit_rate"))
    positive = summary.get("positive_count")
    labeled = summary.get("labeled_count")
    if positive is not None and labeled is not None:
        return f"{hit_rate} ({positive}/{labeled} labeled)"
    return hit_rate


def _backtest_drawdown_text(validation: Mapping[str, object]) -> str:
    summary = _mapping(validation.get("backtest_summary"))
    drawdown = _mapping(summary.get("drawdown_proxy"))
    value = drawdown.get("value")
    if value is None:
        return "n/a"
    return f"{_pct_text(value)} max adverse"


def _backtest_slippage_text(validation: Mapping[str, object]) -> str:
    summary = _mapping(validation.get("backtest_summary"))
    slippage = _mapping(summary.get("slippage_assumption"))
    if not slippage:
        return "n/a"
    bps = slippage.get("round_trip_bps")
    applied = bool(slippage.get("applied_to_returns"))
    return f"{bps if bps is not None else 'n/a'} bps, {'applied' if applied else 'not applied'}"


def _backtest_benchmark_text(validation: Mapping[str, object]) -> str:
    summary = _mapping(validation.get("backtest_summary"))
    benchmark = _mapping(summary.get("benchmark_comparison"))
    if not benchmark:
        return "n/a"
    parts = [
        f"MR wins={int(benchmark.get('marketradar_wins') or 0)}",
        f"baseline wins={int(benchmark.get('baseline_wins') or 0)}",
        f"ties={int(benchmark.get('ties') or 0)}",
        f"insufficient={int(benchmark.get('insufficient_evidence') or 0)}",
    ]
    measured = benchmark.get("measured_baseline_count")
    required = benchmark.get("required_baseline_count")
    if measured is not None and required is not None:
        parts.append(f"measured={measured}/{required}")
    return ", ".join(parts)


def _pct_text(value: object) -> str:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return "n/a"
    return f"{value * 100:.2f}%"


def _sequence_count(value: object) -> int:
    if isinstance(value, Sequence) and not isinstance(value, str):
        return len(value)
    return 0


def _broker_status_rows(broker: Mapping[str, object]) -> list[Mapping[str, object]]:
    snapshot = _mapping(broker.get("snapshot"))
    exposure = _mapping(broker.get("exposure"))
    broker_name = str(snapshot.get("broker") or exposure.get("broker") or "schwab").upper()
    connected = bool(exposure.get("broker_connected"))
    connection_status = str(
        snapshot.get("connection_status")
        or exposure.get("connection_status")
        or ("connected" if connected else "missing")
    )
    last_sync = str(snapshot.get("last_successful_sync_at") or "never")
    account_count = int(_number_or_zero(snapshot.get("account_count")))
    position_count = int(_number_or_zero(snapshot.get("position_count")))
    open_orders = int(_number_or_zero(snapshot.get("open_order_count")))
    read_only = bool(exposure.get("read_only", True))
    orders_enabled = bool(exposure.get("order_submission_enabled"))
    stale = bool(exposure.get("broker_data_stale"))

    rows: list[Mapping[str, object]] = [
        {
            "_row_key": "broker-connection",
            "area": "Schwab connection",
            "status": connection_status,
            "meaning": (
                f"{broker_name}; acct {account_count}; pos {position_count}; "
                f"sync {last_sync}"
            ),
            "next_action": (
                "Auth Schwab first."
                if not connected
                else "Use read-only sync only."
            ),
        },
        {
            "_row_key": "broker-orders",
            "area": "Order safety",
            "status": "disabled" if not orders_enabled else "enabled",
            "meaning": (
                "Orders disabled; local tickets only."
                if not orders_enabled
                else "Orders enabled; review policy first."
            ),
            "next_action": "Use local ticket previews.",
        },
        {
            "_row_key": "broker-readonly",
            "area": "Broker tools",
            "status": "read-only" if read_only else "write-capable",
            "meaning": (
                "Research context only; not approval."
                if read_only
                else "Write-capable; confirm every action."
            ),
            "next_action": "Keep separate from decisions.",
        },
        {
            "_row_key": "broker-freshness",
            "area": "Portfolio freshness",
            "status": "stale" if stale else "fresh",
            "meaning": f"orders {open_orders}; sync {last_sync}",
            "next_action": (
                "Sync only by intent."
                if stale
                else "No sync needed to browse."
            ),
        },
    ]
    for limit in _rows(broker.get("rate_limits")):
        operation = str(limit.get("operation") or "broker_sync")
        allowed = bool(limit.get("allowed"))
        retry = int(_number_or_zero(limit.get("retry_after_seconds")))
        interval = int(_number_or_zero(limit.get("min_interval_seconds")))
        rows.append(
            {
                "_row_key": f"broker-rate-{operation}",
                "area": f"Limit: {operation}",
                "status": "allowed" if allowed else "cooldown",
                "meaning": f"min {interval}s; retry {retry}s",
                "next_action": (
                    "Allowed; browsing calls 0."
                    if allowed
                    else "Wait for cooldown."
                ),
            }
        )
    return rows


def _broker_next_safe_action(payload: Mapping[str, object]) -> str:
    if _real_results_empty(payload):
        return _no_real_result_next_action(payload, _mapping(payload.get("real_results")))
    broker = _mapping(payload.get("broker"))
    snapshot = _mapping(broker.get("snapshot"))
    exposure = _mapping(broker.get("exposure"))
    connected = bool(exposure.get("broker_connected"))
    connection_status = str(
        snapshot.get("connection_status")
        or exposure.get("connection_status")
        or ("connected" if connected else "missing")
    ).strip()
    orders_enabled = bool(exposure.get("order_submission_enabled"))
    if orders_enabled:
        return (
            "Order submission appears enabled. Verify broker policy before any "
            "broker action."
        )
    if not connected or connection_status.lower() not in {"connected", "ready"}:
        connection_label = _human_status_label(connection_status or "missing")
        return (
            f"Broker {connection_label}; browsing makes 0 Schwab "
            "calls. Authenticate only when you want portfolio context."
        )
    return (
        "Broker is read-only context. Use local watch/trigger/ticket artifacts "
        "only; orders stay disabled."
    )


def _telemetry_event_rows(
    telemetry: Mapping[str, object],
    *,
    setup_blocker: Mapping[str, object] | None = None,
) -> list[Mapping[str, object]]:
    events = _rows(telemetry.get("events"))
    if events:
        return [
            _telemetry_event_table_row(row, row_key=str(index))
            for index, row in enumerate(events, start=1)
        ]
    empty_summary = (
        "Nothing has recorded telemetry locally. Refresh after an intentional "
        "guarded run."
    )
    if setup_blocker:
        empty_summary = "Setup first; telemetry appears after a guarded run."
    return [
        {
            "_row_key": "telemetry-empty",
            "occurred_at": "No telemetry yet",
            "event": "No local audit events",
            "event_label": "No local audit events",
            "status": "waiting",
            "status_label": "waiting",
            "summary": empty_summary,
            "summary_label": empty_summary,
        }
    ]


def _telemetry_event_table_row(
    row: Mapping[str, object],
    *,
    row_key: str,
) -> Mapping[str, object]:
    return {
        **dict(row),
        "_row_key": str(row.get("id") or row.get("event_id") or row_key),
        "event_label": _human_telemetry_event(row.get("event")),
        "status_label": _human_status_label(row.get("status")),
        "summary_label": _humanize_telemetry_summary(row.get("summary")),
    }


def _telemetry_rollup_rows(telemetry: Mapping[str, object]) -> list[Mapping[str, object]]:
    rows: list[Mapping[str, object]] = []
    for index, row in enumerate(_rows(telemetry.get("rollup")), start=1):
        rows.append(
            {
                "_row_key": f"telemetry-rollup-{index}",
                "category": _human_status_label(row.get("category")),
                "count": row.get("count"),
                "latest_status": _human_status_label(row.get("latest_status")),
                "latest_reason": _humanize_telemetry_summary(row.get("latest_reason")),
                "operator_action": _humanize_dashboard_text(row.get("operator_action")),
            }
        )
    return rows


def _human_telemetry_event(value: object) -> str:
    text = _text(value)
    if text == "n/a":
        return text
    return _human_label(text.replace(".", " "))


def _humanize_telemetry_summary(value: object) -> str:
    text = _humanize_dashboard_text(value)
    replacements = (
        ("step=", "step "),
        ("outcome=", "outcome "),
        ("category=", "category "),
        ("provider=", "provider "),
        ("status=", "status "),
        ("command=", "command "),
        ("llm_review", "llm review"),
        ("validation_update", "validation update"),
        ("run_daily", "run daily"),
    )
    for raw, replacement in replacements:
        text = text.replace(raw, replacement)
    return text


def _telemetry_next_safe_action(payload: Mapping[str, object]) -> str:
    telemetry = _mapping(payload.get("telemetry"))
    coverage = _mapping(payload.get("telemetry_coverage"))
    missing_required = int(_number_or_zero(coverage.get("missing_required_count")))
    attention = int(_number_or_zero(telemetry.get("attention_count")))
    event_count = int(_number_or_zero(telemetry.get("event_count")))
    waiting_domains = [
        row
        for row in _rows(coverage.get("domains"))
        if str(row.get("status") or "").strip().lower() == "waiting"
    ]
    setup_footer = _setup_command_footer_action(payload)
    if setup_footer:
        return setup_footer
    if missing_required:
        return (
            f"Telemetry missing {missing_required} required domain(s). Inspect "
            "coverage rows before trusting run diagnosis."
        )
    if attention:
        return f"Telemetry: inspect {attention} attention item(s)."
    if waiting_domains:
        return (
            "Telemetry core ready; optional waiting domains fill after universe "
            "seed or dashboard actions."
        )
    if event_count <= 0:
        return (
            "No telemetry yet. Refresh after an intentional run before diagnosing "
            "workflow health."
        )
    return "Telemetry core ready; use this page as the audit trail after intentional runs."


def _broker_lines(payload: Mapping[str, object], width: int) -> list[str]:
    broker = _mapping(payload.get("broker"))
    snapshot = _mapping(broker.get("snapshot"))
    exposure = _mapping(broker.get("exposure"))
    lines = [_rule("Broker / Portfolio", width)]
    if _real_results_empty(payload):
        lines.append(
            "Broker is optional; you do not need Schwab connected to start the market scan."
        )
        lines.extend(
            _locked_review_setup_lines(
                payload,
                width,
                title="No broker action needed yet.",
                unlocks=(
                    "Portfolio context can help later, after a real candidate exists "
                    "and you choose to compare it with your account."
                ),
                after_setup=(
                    "review candidates first; authenticate Schwab only if you want "
                    "portfolio context."
                ),
            )
        )
        lines.extend(
            _kv_lines(
                (
                    ("Schwab status", snapshot.get("connection_status")),
                    ("Orders", "disabled unless explicitly configured"),
                    ("Broker calls", "0 Schwab calls while browsing this page."),
                ),
                width=width,
            )
        )
        return lines
    lines.extend(
        _kv_lines(
            (
                ("Connection", _human_status_label(snapshot.get("connection_status"))),
                ("Broker", snapshot.get("broker")),
                ("Last sync", snapshot.get("last_successful_sync_at")),
                ("Account count", snapshot.get("account_count")),
                ("Position count", snapshot.get("position_count")),
                ("Open orders", snapshot.get("open_order_count")),
                ("Portfolio equity", _format_usd_amount(exposure.get("portfolio_equity"))),
            ),
            width=width,
        )
    )
    lines.extend(
        _wrap(
            "Trading safety: order submission remains disabled unless explicitly configured.",
            width,
        )
    )
    lines.extend(_wrap(_broker_next_safe_action(payload), width))
    auth_lines = _broker_auth_setup_lines(snapshot)
    if auth_lines:
        lines.append("")
        lines.append(_rule("Schwab Auth Setup", width))
        for line in auth_lines:
            lines.extend(_wrap(line, width))
    lines.append("")
    action_rows = _rows(broker.get("opportunity_actions"))
    lines.append(_rule("Local Watch Actions", width))
    if action_rows:
        lines.extend(
            _table_lines(
                action_rows,
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
    else:
        lines.extend(
            _wrap(
                "No saved watch/ready/dismiss actions yet. Use `action TICKER watch` "
                "after reviewing a candidate.",
                width,
            )
        )
    lines.append("")
    trigger_rows = _rows(broker.get("triggers"))
    lines.append(_rule("Local Trigger Rules", width))
    if trigger_rows:
        lines.extend(
            _table_lines(
                trigger_rows,
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
    else:
        lines.extend(
            _wrap(
                "No saved local trigger rules yet. Use `trigger TICKER TYPE OP "
                "THRESHOLD` only after research review.",
                width,
            )
        )
    lines.append("")
    ticket_rows = _rows(broker.get("order_tickets"))
    lines.append(_rule("Blocked Order Tickets", width))
    if ticket_rows:
        lines.extend(
            _table_lines(
                ticket_rows,
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
    else:
        lines.extend(
            _wrap(
                "No blocked order tickets yet. Tickets are local previews; they do not "
                "submit broker orders.",
                width,
            )
        )
    lines.extend(
        _wrap(
            "Commands: action TICKER watch|ready|simulate_entry|dismiss, "
            "trigger TICKER TYPE OP THRESHOLD, eval-triggers [ticker], "
            "ticket TICKER buy|sell ENTRY STOP.",
            width,
        )
    )
    return lines


def _broker_auth_setup_lines(snapshot: Mapping[str, object]) -> list[str]:
    status = str(snapshot.get("connection_status") or "").strip().lower()
    if status not in {"needs_auth", "needs auth", "disconnected", "missing"}:
        return []
    return [
        (
            "Use Schwab only for read-only portfolio or market context; it is "
            "not required for market scanning."
        ),
        (
            "Set SCHWAB_CLIENT_ID, SCHWAB_CLIENT_SECRET, SCHWAB_REDIRECT_URI, "
            "and BROKER_TOKEN_ENCRYPTION_KEY in .env.local."
        ),
        (
            "Start the local API, then open "
            "https://127.0.0.1:8443/api/brokers/schwab/connect."
        ),
        (
            "Keep SCHWAB_ORDER_SUBMISSION_ENABLED=false; tickets stay local "
            "previews and do not submit orders."
        ),
        "Runbook: docs/runbooks/schwab.md.",
    ]


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
            "batch SOURCE execute runs a reviewed provider chunk."
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
        setup_blocker = (
            _readiness_first_setup_blocker(payload)
            if _real_results_empty(payload)
            else {}
        )
        setup_blocker_area = _human_source_name(setup_blocker.get("area"))
        setup_blocker_action = _humanize_dashboard_text(
            setup_blocker.get("next_action")
        )
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
                (
                    "Blocker",
                    (
                        f"First setup: {setup_blocker_area}"
                        if setup_blocker
                        else _human_source_status_text(goal.get("current_blocker"))
                    ),
                ),
                (
                    "Next",
                    setup_blocker_action if setup_blocker else goal.get("next_useful_step"),
                ),
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
                ("Status", _human_status_label(workflow.get("status"))),
                (
                    "Coverage-first",
                    _human_source_status_text(
                        workflow.get("coverage_first_action")
                        or workflow.get("next_action")
                    ),
                ),
                (
                    "Decision shortcut",
                    _human_source_status_text(
                        workflow.get("decision_shortcut_action")
                        or "None yet - fill required evidence first."
                    ),
                ),
                ("All-source plan", workflow.get("overview_command")),
            ),
            width=width,
        )
    )
    table_rows = [
        {
            **step,
            "source_label": _human_source_name(step.get("source")),
            "status_label": _human_status_label(step.get("status")),
            "depends_on_label": _source_workflow_depends_on_label(
                step.get("depends_on")
            ),
            "gap_summary": _source_workflow_gap_summary(step),
            "inspect_command": _source_workflow_inspect_command(step),
            "useful_rows": _source_workflow_useful_rows(step),
            "action": _human_source_status_text(step.get("action")),
        }
        for step in steps
    ]
    lines.extend(
        _table_lines(
            table_rows,
            [
                ("priority", "#", 4),
                ("source_label", "Source", 18),
                ("status_label", "Status", 12),
                ("gap_summary", "Full gaps", 16),
                ("useful_rows", "Useful rows", 18),
                ("depends_on_label", "After", 18),
                ("action", "Do this", 48),
                ("inspect_command", "Inspect", 24),
            ],
            width=width,
            limit=8,
        )
    )
    lines.extend(
        _wrap(
            "`batch all` shows this source map without provider calls; "
            "`batch SOURCE all` summarizes the full chunk plan; "
            "`batch SOURCE execute` runs one guarded chunk; "
            "`batch SOURCE execute 3` runs a capped set.",
            width,
        )
    )
    lines.extend(
        _wrap(
            "Full scan = the whole ranked universe. Source-fill tickers = the next "
            "rate-limited provider chunk, not the ticker universe.",
            width,
        )
    )
    return lines


def _source_workflow_gap_summary(step: Mapping[str, object]):
    gap_rows = int(_number_or_zero(step.get("gap_rows")))
    if gap_rows <= 0:
        return "none"
    return f"{gap_rows} full-scan"


def _source_workflow_depends_on_label(value: object) -> str:
    sources = [_human_source_name(item) for item in _texts(value)]
    return ", ".join(source for source in sources if source) or "none"


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
                "source_label": _human_source_name(source),
                "status": action.get("status") or step.get("status") or "unknown",
                "status_label": _human_status_label(
                    action.get("status") or step.get("status") or "unknown"
                ),
                "gap_rows": _source_action_gap_count(action) if action else "n/a",
                "useful_rows": _source_workflow_useful_rows(step),
                "examples": examples,
                "plan": f"batch {source}",
                "next_action": _human_source_status_text(
                    step.get("action") or "Inspect the source plan."
                ),
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
                "source_label": _human_source_name(source),
                "status": action.get("status") or "unknown",
                "status_label": _human_status_label(action.get("status") or "unknown"),
                "gap_rows": _source_action_gap_count(action),
                "useful_rows": "n/a",
                "examples": _source_action_sample_tickers(action),
                "plan": f"batch {source}",
                "next_action": _human_source_status_text(
                    action.get("next_action")
                    or action.get("action")
                    or "Inspect the source plan."
                ),
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
    coverage_first = _human_source_status_text(
        workflow.get("coverage_first_action")
        or workflow.get("next_action")
        or "Review source gaps."
    )
    raw_decision_shortcut = workflow.get("decision_shortcut_action")
    decision_shortcut = (
        _human_source_status_text(raw_decision_shortcut)
        if raw_decision_shortcut not in (None, "")
        else ""
    )
    row_count = len(rows)
    shortcut_text = (
        f" Decision shortcut: {decision_shortcut}"
        if decision_shortcut
        else " No decision shortcut is currently runnable."
    )
    return (
        f"{row_count} source row(s). Coverage-first: {coverage_first}"
        f"{shortcut_text} Enter/click is plan-only; execute requires "
        "batch SOURCE execute."
    )


def _ops_next_safe_action(payload: Mapping[str, object]) -> str:
    if _real_results_empty(payload):
        setup_action = _readiness_next_safe_action(payload)
        if setup_action:
            return setup_action
    workflow = _mapping(payload.get("priced_in_source_workflow"))
    workflow_source = _coverage_first_workflow_source(workflow)
    if workflow_source:
        command = str(
            workflow.get("coverage_first_command") or workflow.get("next_command") or ""
        ).strip()
        source_label = _human_source_name(workflow_source)
        if command:
            return f"{source_label.capitalize()} first: 2=command; Enter=plan."
        action = _human_source_status_text(
            workflow.get("coverage_first_action")
            or workflow.get("next_action")
            or "Review source gaps."
        )
        command_text = f" execute: batch {workflow_source} execute."
        action_text = ""
        if not command and action:
            action_text = f" {_clip(action, 72)}"
        return (
            f"Coverage-first: {source_label}. "
            f"Plan-only;{command_text}{action_text}"
        )
    rows = _source_coverage_workbench_rows(payload)
    if rows:
        row = rows[0]
        source = str(row.get("source") or "source").strip()
        source_label = _human_source_name(source)
        action = str(row.get("next_action") or row.get("plan") or "").strip()
        action_text = f" {_clip(action, 48)}" if action else ""
        return (
            f"Coverage-first: {source_label}. Plan-only; "
            f"execute: batch {source} execute.{action_text}"
        )
    action = _human_source_status_text(
        workflow.get("coverage_first_action")
        or workflow.get("next_action")
        or "Review source gaps."
    )
    return (
        f"Coverage-first: {_clip(action, 72)} Plan-only; "
        "execute requires an explicit batch command."
    )


def _coverage_first_workflow_source(workflow: Mapping[str, object]) -> str:
    source = str(workflow.get("coverage_first_source") or "").strip()
    if source:
        return source
    command = str(
        workflow.get("coverage_first_command") or workflow.get("next_command") or ""
    ).strip()
    if command:
        try:
            command_parts = shlex.split(command)
        except ValueError:
            command_parts = command.split()
        for index, part in enumerate(command_parts):
            if part == "--source" and index + 1 < len(command_parts):
                return command_parts[index + 1]
            if part.startswith("--source="):
                return part.split("=", 1)[1]
        lowered = command.lower()
        if "market-bars" in lowered or "market_bars" in lowered:
            return "market_bars"
        for source_name in dashboard_data.PRICED_IN_SOURCE_CLASSES:
            source_token = source_name.replace("_", "-")
            if source_name in lowered or source_token in lowered:
                return source_name
    target_action = str(
        workflow.get("coverage_first_action") or workflow.get("next_action") or ""
    ).strip()
    if target_action:
        for step in _rows(workflow.get("steps")):
            step_source = str(step.get("source") or "").strip()
            step_action = str(step.get("action") or "").strip()
            if step_source and step_action == target_action:
                return step_source
    for step in _rows(workflow.get("steps")):
        step_source = str(step.get("source") or "").strip()
        if step_source:
            return step_source
    return ""


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
                ("Degraded mode", _enabled_label(degraded.get("enabled"))),
                ("Max action state", _human_status_label(degraded.get("max_action_state"))),
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
    setup_locked = _real_results_empty(payload)
    if setup_locked:
        lines.append("")
        lines.extend(_ops_setup_locked_lines(payload, width))
    source_actions = []
    if not setup_locked:
        source_actions = [
            {
                **action,
                "source_label": _human_source_name(action.get("source")),
                "status_label": _human_status_label(action.get("status")),
                "gap_rows": _source_action_gap_count(action),
                "examples": _source_action_sample_tickers(action),
                "batch_plan": action.get("batch_plan_command") or action.get("command"),
            }
            for action in _rows(
                _mapping(payload.get("priced_in_source_coverage")).get("actions")
            )
            if str(action.get("status") or "") not in {"ready", "not_applicable"}
        ]
    if source_actions:
        lines.append("")
        lines.append(_rule("Visible Review Page Source Gaps", width))
        lines.extend(
            _table_lines(
                source_actions,
                [
                    ("source_label", "Source", 18),
                    ("status_label", "Status", 12),
                    ("coverage_pct", "Coverage", 10),
                    ("gap_rows", "Gap rows", 10),
                    ("examples", "Examples", 22),
                    ("batch_plan", "Batch plan", 62),
                ],
                width=width,
                limit=8,
            )
        )
        lines.extend(
            _wrap(
                "This table is source coverage for the visible review page, not the "
                "full scan universe. The Source Fill Workflow below shows full-scan "
                "gaps and guarded batch plans. Examples are sample tickers only. "
                "Type `batch SOURCE` to show the full-scan plan; type "
                "`batch SOURCE execute` to run only the next guarded chunk, or "
                "`batch SOURCE execute 3` for a capped run.",
                width,
            )
        )
    workflow_lines = [] if setup_locked else _source_workflow_lines(payload, width)
    if workflow_lines:
        lines.append("")
        lines.extend(workflow_lines)
    provider_rows = _rows(ops.get("providers"))
    lines.append("")
    lines.append(_rule("Provider Health", width))
    if provider_rows:
        lines.extend(
            _table_lines(
                provider_rows,
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
    else:
        lines.append("Provider health: no local provider checks recorded.")
    job_rows = _rows(ops.get("jobs"))
    lines.append("")
    lines.append(_rule("Recent Jobs", width))
    if job_rows:
        lines.extend(
            _table_lines(
                job_rows,
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
    else:
        lines.append("Recent jobs: no local job rows recorded.")
    return lines


def _ops_setup_locked_lines(payload: Mapping[str, object], width: int) -> list[str]:
    blocker = _readiness_first_setup_blocker(payload)
    command = _first_scan_setup_command(payload)
    next_action = _no_real_result_next_action(
        payload,
        _mapping(payload.get("real_results")),
    )
    blocker_label = next_action
    do_first = ""
    if blocker:
        area = _human_source_name(blocker.get("area") or "setup blocker")
        blocker_label = f"{_setup_blocker_first_label(area)}."
        do_first = _humanize_dashboard_text(blocker.get("next_action"))
        if command:
            do_first = (
                "Use the PowerShell command below after accepting the data "
                "change or provider call."
            )
    setup_rows: list[tuple[str, object]] = [
        ("Can Ops diagnose runs?", "Not yet. No real scan rows exist."),
        ("First blocker", blocker_label),
    ]
    if do_first:
        setup_rows.append(("Do first", do_first))
    if command:
        setup_rows.extend(
            [
                ("PowerShell command", command),
                (
                    "Where to run",
                    (
                        "Run it in a normal PowerShell prompt, not in the "
                        "dashboard command box."
                    ),
                ),
            ]
        )
    setup_rows.extend(
        [
            (
                "Still useful",
                (
                    "Database, provider health, and recent jobs below are local "
                    "diagnostics; viewing them makes 0 calls."
                ),
            ),
            (
                "Hidden for now",
                (
                    "Source-fill tables and batch commands appear after setup, "
                    "when they can repair real scan evidence."
                ),
            ),
        ]
    )
    lines = [_rule("Ops Setup Gate", width)]
    lines.extend(
        _kv_lines(
            setup_rows,
            width=width,
        )
    )
    return lines


def _telemetry_lines(payload: Mapping[str, object], width: int) -> list[str]:
    telemetry = _mapping(payload.get("telemetry"))
    coverage = _mapping(payload.get("telemetry_coverage"))
    setup_blocker = (
        _readiness_first_setup_blocker(payload) if _real_results_empty(payload) else {}
    )
    lines = [_rule("Telemetry", width)]
    if setup_blocker:
        lines.extend(
            _locked_review_setup_lines(
                payload,
                width,
                title="No telemetry audit events yet.",
                unlocks=(
                    "Telemetry becomes useful after setup and one guarded run "
                    "records local events."
                ),
                after_setup=(
                    "run one capped scan, then return here to inspect run health."
                ),
            )
        )
        lines.append("")
        lines.extend(
            _table_lines(
                _telemetry_event_rows(telemetry, setup_blocker=setup_blocker),
                [
                    ("occurred_at", "Occurred", 24),
                    ("event_label", "Event", 24),
                    ("status_label", "Status", 14),
                    ("summary_label", "Summary", 64),
                ],
                width=width,
                limit=1,
            )
        )
        return lines
    lines.extend(
        _kv_lines(
            (
                (
                    "Telemetry",
                    (
                        f"{_human_status_label(telemetry.get('status'))}; "
                        f"{_humanize_dashboard_text(telemetry.get('headline'))}"
                    ),
                ),
                ("Events", telemetry.get("event_count")),
                ("Attention", telemetry.get("attention_count")),
                ("Guarded", telemetry.get("guarded_count")),
                (
                    "Coverage",
                    (
                        f"{_human_status_label(coverage.get('status'))}; "
                        f"{_humanize_dashboard_text(coverage.get('headline'))}"
                    ),
                ),
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
    rollup_rows = _telemetry_rollup_rows(telemetry)
    if rollup_rows and int(_number_or_zero(telemetry.get("attention_count"))) > 0:
        lines.append(_rule("Telemetry Attention Rollup", width))
        lines.extend(
            _table_lines(
                rollup_rows,
                [
                    ("category", "Category", 20),
                    ("count", "Count", 8),
                    ("latest_status", "Latest", 16),
                    ("operator_action", "Operator Action", 70),
                ],
                width=width,
                limit=4,
            )
        )
        lines.append("")
    lines.extend(
        _table_lines(
            _telemetry_event_rows(telemetry, setup_blocker=setup_blocker),
            [
                ("occurred_at", "Occurred", 24),
                ("event_label", "Event", 24),
                ("status_label", "Status", 14),
                ("summary_label", "Summary", 64),
            ],
            width=width,
            limit=12,
        )
    )
    lines.append("")
    domain_rows = _telemetry_domain_rows(coverage, setup_blocker=setup_blocker)
    lines.extend(
        _table_lines(
            domain_rows,
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


def _telemetry_domain_rows(
    coverage: Mapping[str, object],
    *,
    setup_blocker: Mapping[str, object] | None = None,
) -> list[Mapping[str, object]]:
    rows = [dict(row) for row in _rows(coverage.get("domains"))]
    if not setup_blocker:
        return rows
    area = str(setup_blocker.get("area") or "setup").strip()
    action = str(setup_blocker.get("next_action") or "").strip()
    setup_first = _setup_blocker_first_label(area)
    if action:
        setup_first = f"{setup_first}: {action}"
    for row in rows:
        status = str(row.get("status") or "").strip().lower()
        if status == "missing":
            row["operator_action"] = (
                f"{setup_first} Telemetry fills after setup and an intentional guarded run."
            )
    return rows


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
    if _real_results_empty(payload):
        lines.extend(_agent_setup_locked_lines(payload, brief, width))
        return lines
    if _real_results_missing(payload):
        lines.extend(_agent_waiting_on_trusted_evidence_lines(payload, width))
    lines.extend(
        _wrap(
            f"Mode: {_human_status_label(brief.get('mode') or 'dry_run')} | "
            f"Status: {_human_status_label(brief.get('status') or 'unknown')} | "
            f"Calls: openai={calls.get('openai', 0)}, "
            f"market={calls.get('market_data', 0)}, broker={calls.get('broker', 0)}",
            width,
        )
    )
    if runtime:
        lines.extend(_wrap(f"Runtime: {_agent_runtime_label(runtime)}", width))
    boundary = brief.get("decision_boundary")
    if boundary:
        lines.extend(_wrap(f"Boundary: {_human_agent_text(boundary)}", width))
    lines.extend(
        _table_lines(
            _agent_brief_rows(brief, payload),
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


def _agent_setup_locked_lines(
    payload: Mapping[str, object],
    brief: Mapping[str, object],
    width: int,
) -> list[str]:
    runtime = _mapping(brief.get("runtime"))
    lines = [_rule("Agent Coach Locked Until Setup Is Complete", width)]
    lines.append(
        "no market scan has run yet, so the agent has no real stock evidence to analyze."
    )
    items = [
        (row["item"], row["detail"])
        for row in _agent_setup_locked_rows(payload, brief)
        if row.get("kind") != "Runtime"
    ]
    if runtime:
        items.extend(_agent_runtime_setup_items(runtime))
    lines.extend(_kv_lines(items, width=width))
    lines.append("")
    lines.extend(
        _wrap(
            (
                "Do not run agent execute while this page says locked. Run the "
                "PowerShell setup command first, then return here for a zero-call "
                "preview after real scan evidence exists."
            ),
            width,
        )
    )
    return lines


def _agent_setup_locked_rows(
    payload: Mapping[str, object],
    brief: Mapping[str, object],
) -> list[Mapping[str, object]]:
    calls = _mapping(brief.get("external_calls_made"))
    runtime = _mapping(brief.get("runtime"))
    command = _first_scan_setup_command(payload)
    blocker = _readiness_first_setup_blocker(payload)
    if blocker:
        area = _human_source_name(blocker.get("area") or "setup blocker")
        next_action = f"{_setup_blocker_first_label(area)}."
    else:
        next_action = "Start with setup row 1."
    rows: list[Mapping[str, object]] = [
        {
            "_setup_locked": True,
            "kind": "Setup",
            "item": "Can the agent help now?",
            "detail": "Not with stock analysis yet. No real scan rows exist.",
        },
        {
            "_setup_locked": True,
            "kind": "Setup",
            "item": "Do first",
            "detail": next_action,
        },
        {
            "_setup_locked": True,
            "kind": "Setup",
            "item": "PowerShell command",
            "detail": command or "No setup command recorded.",
        },
        {
            "_setup_locked": True,
            "kind": "Setup",
            "item": "Approval",
            "detail": "Continue only if you accept the data change or provider call.",
        },
        {
            "_setup_locked": True,
            "kind": "Setup",
            "item": "Where to run",
            "detail": "normal PowerShell prompt, not the dashboard command box.",
        },
        {
            "_setup_locked": True,
            "kind": "Safety",
            "item": "Safe preview",
            "detail": (
                "This page only reports gates while setup is incomplete; browsing "
                "makes 0 OpenAI, market, broker, or order calls."
            ),
        },
        {
            "_setup_locked": True,
            "kind": "Cost",
            "item": "OpenAI calls",
            "detail": (
                f"preview={calls.get('openai', 0)}; "
                f"market={calls.get('market_data', 0)}; "
                f"broker={calls.get('broker', 0)}"
            ),
        },
        {
            "_setup_locked": True,
            "kind": "Hidden",
            "item": "Detailed agent roles",
            "detail": (
                "Detailed agent roles, insights, and next-action lists appear after "
                "real scan evidence exists."
            ),
        },
    ]
    if runtime:
        rows.append(
            {
                "_setup_locked": True,
                "kind": "Runtime",
                "item": _agent_runtime_name(runtime.get("orchestrator")),
                "detail": _agent_runtime_label(runtime),
            }
        )
    return rows


def _help_lines(width: int) -> list[str]:
    lines = [_rule("Help", width)]
    lines.extend(
        _wrap(
            (
                "First commands: start opens the walkthrough; inbox shows scan "
                "messages; setup shows the first setup command; evidence gaps "
                "shows blockers; safe run reviews the call budget; q exits."
            ),
            width,
        )
    )
    lines.extend(
        _wrap(
            (
                "Browsing, clicking, filtering, and refresh make 0 provider calls. "
                "Commands with execute are deliberate actions."
            ),
            width,
        )
    )
    lines.append("")
    lines.append(_rule("Command Reference", width))
    commands = [
        (
            "0..9, Ctrl+A, or page name",
            "Switch page; Ctrl+A opens Agent Coach.",
        ),
        ("features", "List current Market Radar features and where they live in the TUI."),
        ("setup / first", "Show the first setup command and where to run it."),
        ("open #|TICKER", "Open a candidate from Candidate Review."),
        ("open #|ALERT_ID", "Open an alert from the alerts page."),
        ("ticker SYMBOL|all", "Filter candidate-adjacent pages by ticker where supported."),
        ("available-at ISO|latest", "Set or clear the point-in-time data cutoff."),
        ("ready", "Show only decision-useful not-priced-in rows from the full scan."),
        ("now", "Show the single next priced-in action, response, and cost."),
        ("usefulness STATUS|all", "Filter Inbox by usefulness verdict."),
        ("source-gap SOURCE|all", "Filter Inbox by missing/stale data source."),
        ("batch SOURCE", "Plan full-scan source fill and show the next safe chunk."),
        ("batch SOURCE execute", "Run only the next guarded source-fill chunk."),
        ("batch SOURCE execute 3", "Run a capped source-fill batch set."),
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
        ("agent", "Preview real Agents SDK gates with zero OpenAI calls."),
        ("agent execute", "Run one credit-gated OpenAI Agents SDK brief."),
        ("decision-gap GAP|all", "Filter Inbox by missing decision evidence."),
        ("next / prev", "Page through the current Inbox scan rows."),
        ("offset ROW", "Jump to a 1-based full-scan row number."),
        ("limit 1-200", "Change Inbox rows per page."),
        ("alert-status STATUS|all", "Filter alerts by status."),
        ("alert-route ROUTE|all", "Filter alerts by route."),
        ("refresh", "Reload the local database snapshot."),
        ("run", "Show the guarded run instruction on the run page."),
        ("run execute", "Start one capped radar cycle after reviewing the call plan."),
        ("json", "Print the redacted JSON snapshot used by the TUI."),
        ("action TICKER ACTION [notes]", "Save watch/ready/simulate_entry/dismiss."),
        ("trigger TICKER TYPE OP THRESHOLD", "Save a market trigger."),
        ("eval-triggers [ticker]", "Evaluate saved triggers against stored market context."),
        ("ticket TICKER SIDE ENTRY STOP", "Save a blocked order-preview ticket."),
        (
            "feedback ROW|ALERT_ID LABEL",
            "Record alert feedback from current alert rows.",
        ),
        ("ledger coverage", "Show Warning/manual-review rows missing value-ledger entries."),
        (
            "ledger record #|id|TICKER LABEL ACTION DECISION VALUE CONFIDENCE",
            "Preview a value-ledger entry; add --execute to write it.",
        ),
        ("outcome coverage", "Show value-ledger rows missing forward outcomes."),
        (
            "outcome update LEDGER_ID AVAILABLE_AT|filter",
            "Preview a deterministic outcome; add --execute to write it.",
        ),
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


def _readiness_ready_label(value: object) -> str:
    return "ready" if value is True else "not ready"


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


def _footer_lines(
    width: int,
    *,
    payload: Mapping[str, object] | None = None,
    page: str = "overview",
) -> list[str]:
    snapshot = _mapping(payload)
    action = _footer_next_action(snapshot, page)
    lines = [_rule("Next Safe Action", width)]
    lines.extend(_footer_next_action_lines(action, width))
    lines.extend(_wrap(_cost_boundary_summary(snapshot), width))
    lines.append(_rule("Last Response", width))
    lines.extend(_wrap("LAST RESPONSE: Ready. No command has run in this view.", width))
    lines.append(_rule("Commands", width))
    lines.extend(_wrap(_footer_command_hint(snapshot), width))
    return lines


def _footer_next_action_lines(action: str, width: int) -> list[str]:
    segments = [segment for segment in str(action).splitlines() if segment.strip()]
    if not segments:
        segments = ["No next action is available. Refresh the dashboard snapshot."]
    lines: list[str] = []
    for index, segment in enumerate(segments):
        prefix = "NEXT SAFE ACTION: " if index == 0 else "  "
        lines.extend(_wrap(f"{prefix}{segment}", width))
    return lines


def _footer_command_hint(payload: Mapping[str, object]) -> str:
    if _real_results_empty(payload):
        return (
            "Type setup for the first setup command, 2 for Evidence Gaps, "
            "3 for Safe Run, refresh, help, or q."
        )
    return "Type a page name, number, filter command, refresh, json, help, or q."


def _footer_next_action(payload: Mapping[str, object], page: str) -> str:
    if page.startswith("candidate:"):
        return _candidate_case_next_safe_action(payload, page.split(":", 1)[1])
    if page.startswith("alert:"):
        return (
            "Alert detail is a research notification, not trade approval. "
            "Review evidence, then record local feedback."
        )
    setup_footer = _setup_command_footer_action(payload)
    if setup_footer and page in {
        "readiness",
        "run",
        "candidates",
        "alerts",
        "review",
        "broker",
        "ops",
        "agent",
        "themes",
        "tutorial",
        "start",
    }:
        return setup_footer
    if page == "tutorial":
        if _real_results_empty(payload):
            return _no_real_result_next_action(
                payload,
                _mapping(payload.get("real_results")),
            )
        return (
            "Start with Inbox: press 1 or click Inbox. Browsing tutorial makes "
            "0 provider calls."
        )
    if page == "overview":
        if _market_inbox_rows(payload):
            return _market_inbox_next_safe_action(payload)
        next_step = _priced_in_operator_step(payload) or _mapping(
            payload.get("operator_next_step")
        )
        return str(
            next_step.get("action")
            or _mapping(payload.get("priced_in_answer")).get("next_action")
            or "Open Inbox and inspect messages."
        )
    if page == "readiness":
        return _readiness_next_safe_action(payload)
    if page == "run":
        return _run_page_next_safe_action(payload)
    if page == "candidates":
        return _candidates_next_safe_action(payload)
    if page == "alerts":
        alerts = _mapping(payload.get("alerts"))
        count = int(_number_or_zero(alerts.get("count"))) or len(
            _rows(alerts.get("rows"))
        )
        if count:
            return (
                "Research alerts only; not trade signals. Open one, then record "
                "local feedback."
            )
        if _real_results_empty(payload):
            return _no_real_result_next_action(payload, _mapping(payload.get("real_results")))
        return "No alert rows yet. Alerts are research notifications, not trade signals."
    if page == "review":
        if _real_results_empty(payload):
            return _no_real_result_next_action(
                payload,
                _mapping(payload.get("real_results")),
            )
        if _priced_in_review_rows(payload):
            return (
                "Decision Review is not trade approval. Verify optional gaps, "
                "then open the top row manually."
            )
        return "No decision-ready review rows. Fix Evidence Gaps before any decision."
    if page == "ipo":
        ipo = _mapping(payload.get("ipo_s1"))
        count = int(_number_or_zero(ipo.get("count")))
        if count:
            return (
                "IPO/S-1 rows are catalyst evidence only. Open a filing row; "
                "browsing makes 0 SEC calls."
            )
        if _real_results_empty(payload):
            return setup_footer or (
                "Set up the market scan first; IPO/S-1 is optional catalyst "
                "evidence after that."
            )
        return "No IPO/S-1 rows in this snapshot. Continue with Inbox or Candidates."
    if page == "broker":
        return _broker_next_safe_action(payload)
    if page == "ops":
        return _ops_next_safe_action(payload)
    if page == "telemetry":
        return _telemetry_next_safe_action(payload)
    if page == "agent":
        if _real_results_missing(payload):
            real_results = _mapping(payload.get("real_results"))
            return (
                _no_real_result_next_action(payload, real_results)
                + " Agent preview is zero-call; execute stays blocked."
            )
        return "Use agent for a zero-call preview; agent execute spends OpenAI budget."
    if page == "themes":
        if _real_results_empty(payload):
            return _no_real_result_next_action(
                payload,
                _mapping(payload.get("real_results")),
            )
        themes = _mapping(payload.get("themes"))
        count = int(_number_or_zero(themes.get("count")))
        if count:
            return (
                "Themes are research clusters. Open Inbox or Candidates for ticker "
                "evidence before acting."
            )
        return "No theme clusters in this snapshot. Continue with Inbox or fill scan data."
    if page == "validation":
        validation = _mapping(payload.get("validation"))
        report = _mapping(validation.get("report"))
        if report:
            return (
                "Validation is the quality gate. Review false positives before "
                "trusting alert usefulness."
            )
        if _real_results_empty(payload):
            return setup_footer or (
                "Set up the market scan first; validation comes after real scan "
                "evidence and outcomes."
            )
        return "No validation report yet. Keep decisions research-only until evidence exists."
    if page == "costs":
        if _costs_waiting_for_first_scan(
            payload,
            _mapping(payload.get("costs")),
            _mapping(payload.get("value_ledger")),
            _mapping(payload.get("value_outcomes")),
        ):
            setup_footer = _setup_command_footer_action(payload)
            if setup_footer:
                return setup_footer
        value_report = _mapping(payload.get("value_report"))
        if value_report:
            return (
                "Costs prove whether radar is worth using. Review value, feedback, "
                "and outcomes before counting wins."
            )
        return "No value report yet. Record feedback and outcomes before judging usefulness."
    if page == "features":
        return "Use Features as the map of what exists. Press Enter on a row to jump there."
    if page == "help":
        return "Use Help as the command reference. Type a command below; browsing makes 0 calls."
    return "Use the workflow navigation or open the highlighted row."


def _execution_cost_summary(payload: Mapping[str, object]) -> str:
    call_plan = _mapping(payload.get("call_plan"))
    agent = _mapping(payload.get("agent_brief"))
    credit = _mapping(agent.get("credit_gate"))
    provider_calls = int(_number_or_zero(call_plan.get("max_external_call_count")))
    openai_cap = int(_number_or_zero(credit.get("max_openai_calls")))
    estimated_cost = credit.get("estimated_cost_usd", 0)
    return (
        f"Guarded command budget: provider calls {provider_calls}; "
        f"OpenAI execute cap {openai_cap}; "
        f"estimated OpenAI cost {_format_usd_amount(estimated_cost)}; "
        "DB writes shown by each command."
    )


def _format_usd_amount(value: object) -> str:
    return f"${_number_or_zero(value):,.2f}"


def _format_percentage_amount(value: object) -> str:
    return f"{_number_or_zero(value):,.1f}%"


def _cost_boundary_summary(payload: Mapping[str, object]) -> str:
    return (
        "Browsing cost: 0 provider calls, 0 OpenAI calls. "
        f"{_execution_cost_summary(payload)}"
    )


def _modern_cost_boundary_summary() -> str:
    return "Browsing cost: 0 provider calls, 0 OpenAI calls."


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


_STATUS_LABELS: Mapping[str, str] = {
    "addtowatchlist": "add to watchlist",
    "agent_review": "agent review",
    "approval_required": "approval required",
    "blocked_run_steps": "blocked run steps",
    "candidate_ledger_coverage": "Candidate ledger coverage",
    "candidate_packet": "candidate packet",
    "catalyst_events": "catalyst events",
    "decision_card": "decision card",
    "decision_ready": "decision ready",
    "dry_run": "dry run",
    "expected_gate": "expected gate",
    "incomplete_daily_bar_coverage": "incomplete daily-bar coverage",
    "insufficient_evidence": "Insufficient evidence",
    "live_call_planned": "live call planned",
    "live_calls_planned": "live calls planned",
    "llm_real_mode_disabled": "Premium LLM safety gate",
    "local_text": "local text",
    "manual_review_ready": "manual review ready",
    "market_bars": "market bars",
    "market_momentum": "market momentum",
    "needs_auth": "needs auth",
    "no_candidate_packets": "no candidate packets",
    "no_validation_runs": "No validation runs yet",
    "not_started": "not started",
    "partial_success": "partial success",
    "read_only": "read only",
    "read_only_decision_support": "read-only decision support",
    "read_only_research": "read-only research",
    "research_only": "research only",
    "safe_read_only": "safe read-only",
    "setup_blocked": "setup blocked",
    "theme_peer_sector": "theme/peer/sector",
}

_DASHBOARD_TEXT_REPLACEMENTS: tuple[tuple[str, str], ...] = (
    (
        "Raise CATALYST_POLYGON_TICKERS_MAX_PAGES if needed, then seed tickers.",
        "Seed the ticker universe before calling this a full-market scan.",
    ),
    ("Run plan status=", "Run plan: "),
    ("decision_ready=false", "decision ready: no"),
    ("decision_ready=true", "decision ready: yes"),
    ("latest_bars_stale=no", "latest bars stale: no"),
    ("latest_bars_stale=yes", "latest bars stale: yes"),
    ("live_call_planned", "live call planned"),
    ("live_calls_planned", "live calls planned"),
    ("expected_gate", "expected gate"),
    ("fixture_events", "sample events"),
    ("fixture_market_data", "sample market data"),
    ("max_external_call_count", "max external calls"),
    ("no_run", "no run"),
    ("order_submission_enabled=False", "orders enabled: no"),
    ("order_submission_enabled=True", "orders enabled: yes"),
    ("partial_success", "partial success"),
    ("read_only=False", "read-only: no"),
    ("read_only=True", "read-only: yes"),
    ("shadow alerts", "dry-run alerts"),
    ("shadow mode", "dry-run mode"),
    ("shadow scans", "dry-run scans"),
    ("shadow setup", "dry-run setup"),
    ("source_live=no", "source live: no"),
    ("source_live=yes", "source live: yes"),
    ("setup_blocked", "setup blocked"),
    ("manual_review_ready", "manual review ready"),
    ("market_momentum", "market momentum"),
    ("research_only", "research only"),
)


def _human_status_label(value: object) -> str:
    text = _text(value)
    if text == "n/a":
        return text
    normalized = text.strip().lower()
    return _STATUS_LABELS.get(normalized, _human_label(text))


def _humanize_dashboard_text(value: object) -> str:
    text = _text(value)
    for raw, replacement in _DASHBOARD_TEXT_REPLACEMENTS:
        text = text.replace(raw, replacement)
    return text


def _human_source_name(value: object) -> str:
    raw = "" if value is None else str(value).strip()
    if not raw:
        return ""
    return _human_status_label(raw)


def _human_source_status_text(value: object) -> str:
    text = _humanize_dashboard_text(value)
    source_replacements = (
        ("market_bars", "market bars"),
        ("catalyst_events", "catalyst events"),
        ("local_text", "local text"),
        ("theme_peer_sector", "theme/peer/sector"),
        ("broker_context", "broker context"),
        ("agent_review", "agent review"),
        ("missing_cik", "missing CIK"),
    )
    for raw, replacement in source_replacements:
        text = text.replace(raw, replacement)
    command_replacements = (
        ("batch market bars", "batch market_bars"),
        ("batch catalyst events", "batch catalyst_events"),
        ("batch local text", "batch local_text"),
        ("batch broker context", "batch broker_context"),
    )
    for human_command, command in command_replacements:
        text = text.replace(human_command, command)
    return text


def _human_agent_text(value: object) -> str:
    text = _human_source_status_text(value)
    replacements = (
        ("AddToWatchlist", "add to watchlist"),
        ("residual_universe_review", "residual universe review"),
        ("manual_csv", "manual CSV"),
        ("selected_universe", "selected universe"),
        ("full_market", "full market"),
        ("latest_run=", "latest run "),
        ("status=", "status "),
        ("calls=", "calls "),
        ("command=", "command "),
        ("next=", "next "),
        ("rows=", "rows "),
        ("steps=", "steps "),
        ("coverage-first=", "coverage-first "),
    )
    for raw, replacement in replacements:
        text = text.replace(raw, replacement)
    return text


def _human_readiness_evidence(value: object) -> str:
    text = _human_source_status_text(value)
    replacements = (
        ("snapshot_status=", "snapshot: "),
        ("blockers=", "blockers: "),
        ("latest bars stale: no", "bars fresh: yes"),
        ("latest bars stale: yes", "bars fresh: no"),
        ("source live: yes", "live data: yes"),
        ("source live: no", "live data: no"),
        ("snapshot: blocked", "snapshot blocked"),
        ("bars fresh: yes", "bars fresh"),
        ("bars fresh: no", "bars stale"),
        ("live data: yes", "live data"),
        ("live data: no", "live data missing"),
        ("incomplete_daily_bar_coverage", "daily-bar coverage"),
        ("blocked_run_steps", "run steps"),
        ("no_candidate_packets", "candidate packets"),
    )
    for raw, replacement in replacements:
        text = text.replace(raw, replacement)
    return text


def _yes_no_label(value: object) -> str:
    if isinstance(value, bool):
        return "Yes" if value else "No"
    return _text(value)


def _enabled_label(value: object) -> str:
    if isinstance(value, bool):
        return "Enabled" if value else "Disabled"
    return _human_status_label(value)


def _human_label(value: object) -> str:
    return _text(value).replace("_", " ").strip()


def _indexed(rows: Sequence[Mapping[str, object]]) -> list[dict[str, object]]:
    return [{"index": index, **dict(row)} for index, row in enumerate(rows, start=1)]


def _mapping_items(value: Mapping[str, object]) -> list[dict[str, object]]:
    return [{"key": key, "value": item} for key, item in sorted(value.items())]


def _kv_lines(items: Sequence[tuple[str, object]], *, width: int) -> list[str]:
    raw_label_width = max((len(_text(label)) for label, _ in items), default=14)
    max_label_width = max(14, width // 3)
    label_width = min(max_label_width, max(14, width // 5, raw_label_width))
    value_width = max(20, width - label_width - 3)
    lines: list[str] = []
    for label, value in items:
        label_text = _clip(label, label_width)
        text = _text(value)
        wrapped = _wrap(text, value_width)
        first, *rest = wrapped or [""]
        lines.append(f"{label_text:<{label_width}} : {first}")
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
    overflow = sum(widths) + (3 * (len(columns) - 1)) - width
    while overflow > 0:
        shrink_index = max(range(len(widths)), key=widths.__getitem__)
        if widths[shrink_index] <= 4:
            break
        widths[shrink_index] -= 1
        overflow -= 1
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
    raw = (value or "overview").strip().lower()
    if raw.startswith("candidate:") or raw.startswith("alert:"):
        return raw
    text = "-".join(raw.replace("_", " ").split())
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


def _optional_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
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

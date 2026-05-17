from __future__ import annotations

import json
import shutil
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from uuid import uuid4

from sqlalchemy.engine import Engine
from textual import events
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Grid, Horizontal, Vertical
from textual.widgets import DataTable, Footer, Header, Input, Static

from catalyst_radar.brokers.interactive import (
    create_blocked_order_ticket,
    create_trigger,
    evaluate_triggers,
    opportunity_action_payload,
    order_ticket_payload,
    record_opportunity_action,
    trigger_payload,
)
from catalyst_radar.core.config import AppConfig
from catalyst_radar.dashboard import data as dashboard_data
from catalyst_radar.feedback.service import (
    FeedbackError,
)
from catalyst_radar.feedback.service import (
    record_feedback as record_alert_feedback,
)
from catalyst_radar.jobs.scheduler import SchedulerConfig, run_once, scheduler_run_payload
from catalyst_radar.security.licenses import redact_restricted_external_payload
from catalyst_radar.storage.broker_repositories import BrokerRepository
from catalyst_radar.storage.job_repositories import JobLockRepository

RADAR_RUN_COOLDOWN_LOCK_NAME = "manual_radar_run_cooldown"


@dataclass(frozen=True)
class DashboardFilters:
    ticker: str | None = None
    available_at: datetime | None = None
    alert_status: str | None = None
    alert_route: str | None = None
    telemetry_limit: int = 8

    def normalized(self) -> DashboardFilters:
        ticker = (self.ticker or "").strip().upper() or None
        alert_status = (self.alert_status or "").strip() or None
        alert_route = (self.alert_route or "").strip() or None
        return replace(
            self,
            ticker=ticker,
            alert_status=alert_status,
            alert_route=alert_route,
            telemetry_limit=max(1, int(self.telemetry_limit)),
        )


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
    "tut": "tutorial",
    "tutorial": "tutorial",
    "1": "overview",
    "home": "overview",
    "o": "overview",
    "overview": "overview",
    "start": "overview",
    "2": "readiness",
    "ready": "readiness",
    "readiness": "readiness",
    "3": "run",
    "run": "run",
    "plan": "run",
    "4": "candidates",
    "c": "candidates",
    "candidates": "candidates",
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
    "themes": "themes",
    "validation": "validation",
    "costs": "costs",
    "features": "features",
    "help": "help",
}

NAVIGATION_TEXT = (
    "0 Tutorial | 1 Start | 2 Readiness | 3 Run | 4 Candidates | 5 Alerts | "
    "6 IPO/S-1 | 7 Broker | 8 Ops | 9 Telemetry | features | help | q"
)

MODERN_PAGES: tuple[tuple[str, str, str], ...] = (
    ("tutorial", "0", "Tutorial"),
    ("overview", "1", "Start"),
    ("readiness", "2", "Readiness"),
    ("run", "3", "Run"),
    ("candidates", "4", "Candidates"),
    ("alerts", "5", "Alerts"),
    ("ipo", "6", "IPO/S-1"),
    ("broker", "7", "Broker"),
    ("ops", "8", "Ops"),
    ("telemetry", "9", "Telemetry"),
    ("features", "F", "Features"),
    ("help", "?", "Help"),
)


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
        dashboard_data.load_radar_run_candidate_rows(engine, latest_run)
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
    operator_next_step = dashboard_data.operator_next_step_payload(operator_work_queue)
    telemetry = dashboard_data.telemetry_tape_payload(
        ops_health,
        limit=filters.telemetry_limit,
    )
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
            "telemetry_limit": filters.telemetry_limit,
        },
        "runtime_context": runtime_context,
        "readiness": dashboard_data.radar_readiness_payload(engine, config),
        "radar_run_cooldown": dashboard_data.radar_run_cooldown_payload(engine, config),
        "latest_run": latest_run,
        "discovery_snapshot": discovery_snapshot,
        "actionability_breakdown": actionability,
        "investment_readiness": investment_readiness,
        "operator_work_queue": operator_work_queue,
        "operator_next_step": operator_next_step,
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
    redacted = redact_restricted_external_payload(payload)
    return redacted if isinstance(redacted, dict) else payload


def run_dashboard_tui(
    *,
    engine: Engine,
    config: AppConfig,
    dotenv_loaded: bool,
    filters: DashboardFilters,
    initial_page: str = "tutorial",
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
    initial_page: str = "tutorial",
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
        Binding("1", "go('overview')", "Start", priority=True),
        Binding("2", "go('readiness')", "Readiness", priority=True),
        Binding("3", "go('run')", "Run", priority=True),
        Binding("4", "go('candidates')", "Candidates", priority=True),
        Binding("5", "go('alerts')", "Alerts", priority=True),
        Binding("6", "go('ipo')", "IPO/S-1", priority=True),
        Binding("7", "go('broker')", "Broker", priority=True),
        Binding("8", "go('ops')", "Ops", priority=True),
        Binding("9", "go('telemetry')", "Telemetry", priority=True),
        ("f", "go('features')", "Features"),
        ("?", "go('help')", "Help"),
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
        self.filters = filters.normalized()
        self.page = _normalize_page(initial_page)
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
                        "Type a command or click a row. Try: 2, 4, run, refresh, help, q"
                    ),
                    id="command",
                )
        yield Footer()

    def on_mount(self) -> None:
        self.reload_snapshot()
        self.refresh_view()
        if self.page == "tutorial":
            self.query_one("#nav-tutorial", FocusRow).focus()
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
        self.status_message = ""
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
        if self.page == "candidates":
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

    def _row_by_key(self, key: object) -> Mapping[str, object]:
        key_text = str(key)
        for row in self._current_rows():
            if str(row.get("_row_key") or "") == key_text:
                return row
        return {}

    def _refresh_nav(self) -> None:
        active = self.page.split(":", 1)[0]
        for page_key, shortcut, label in MODERN_PAGES:
            item = self.query_one(f"#nav-{page_key}", FocusRow)
            item.set_class(page_key == active, "active")
            item.update(self._nav_label(page_key, shortcut, label))

    def _nav_label(self, page_key: str, shortcut: str, label: str) -> str:
        active = self.page.split(":", 1)[0] == page_key
        marker = ">>" if active else "  "
        counts = self._nav_count_suffix(page_key)
        return f"{marker} {shortcut:<2} {label}{counts}"

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
        active = self.page.split(":", 1)[0]
        page_keys = [page_key for page_key, _, _ in MODERN_PAGES]
        try:
            current = page_keys.index(active)
        except ValueError:
            current = 0
        self.action_go(page_keys[(current + delta) % len(page_keys)])

    def _move_sidebar_focus(self, delta: int) -> None:
        focus_ids = [f"nav-{page_key}" for page_key, _, _ in MODERN_PAGES]
        focus_ids.extend(["action-refresh", "action-run-page"])
        focused_id = self.focused.id if self.focused else ""
        if focused_id not in focus_ids:
            focused_id = f"nav-{self.page.split(':', 1)[0]}"
        index = focus_ids.index(focused_id)
        self.query_one(f"#{focus_ids[(index + delta) % len(focus_ids)]}", FocusRow).focus()

    def _navigation_text(self) -> str:
        return (
            "[bold #58a6ff]KEYS[/] 0 tutorial | 1 start | 2 readiness | 4 candidates | "
            "Ctrl+N/P page\n"
            "[bold #58a6ff]MOUSE[/] click sidebar or table rows | "
            "Tab focus | Up/Down on sidebar | Enter open | Esc command | q quit\n"
        )

    def _response_text(self) -> str:
        response = self.status_message or "Ready. No command has run in this view."
        return f"[bold #58a6ff]LAST RESPONSE[/]\n{response}"

    def _action_text(self) -> str:
        page = self.page.split(":", 1)[0]
        page_action = {
            "tutorial": "Follow the numbered rows. Press 1 when you are ready for the dashboard.",
            "overview": (
                "Start with the highlighted questions. If Can I act? says No, "
                "open Readiness or Candidates before running anything."
            ),
            "run": "Review call budget, then type run execute only if intended.",
            "candidates": "Click or focus a row and press Enter to open a candidate.",
            "alerts": "Click or focus a row and press Enter to open an alert.",
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
        next_step = _mapping(self.payload.get("operator_next_step"))
        next_action = next_step.get("action") or readiness.get("next_action")
        can_act = _decision_label(readiness)
        active_page = self.page.split(":", 1)[0]
        page_title = (
            "TUTORIAL"
            if active_page == "tutorial"
            else "START HERE"
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
                        "[bold #58a6ff]Do next[/] Read the rows below, then press 1 for Start.",
                    ]
                )
            )
            self.query_one("#metric-readiness", Static).update(
                _metric_text("Step 1", "Learn controls", "mouse, keys, commands")
            )
            self.query_one("#metric-market", Static).update(
                _metric_text("Step 2", "Open Start", "press 1")
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
                        f"[dim]{readiness.get('status') or 'unknown'} | "
                        f"{can_act} | "
                        f"{self.payload.get('external_calls_made', 0)} calls while viewing[/dim]"
                    ),
                    (
                        f"[bold]Can I act?[/] {can_act}. "
                        f"{readiness.get('headline') or 'No readiness headline.'} "
                        f"[dim]Build {(_nested(runtime, 'build', 'commit') or 'n/a')} | "
                        f"Ticker {controls.get('ticker') or 'all'}[/dim]"
                    ),
                    f"[bold #58a6ff]Do next[/] {next_action or 'No operator action.'}",
                ]
            )
        )
        self.query_one("#metric-readiness", Static).update(
            _metric_text(
                "Can I act?",
                can_act,
                readiness.get("status"),
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
        next_step = _mapping(self.payload.get("operator_next_step"))
        can_act = _decision_label(readiness)
        next_action = next_step.get("action") or readiness.get("next_action") or "Open Readiness."
        usefulness = _mapping(readiness.get("market_radar_usefulness"))
        blocked_layers = usefulness.get("blocked_layers")
        if page == "tutorial":
            return "\n".join(
                [
                    "[bold #7ee787]TUTORIAL[/]  Do these in order. Nothing external runs here.",
                    "[bold]1.[/] Press 1 or click Start to see whether Market Radar is usable.",
                    "[bold]2.[/] Press 2 to see blockers. Press 4 to inspect candidate research.",
                    (
                        "[bold]3.[/] Press 3 only to review the run plan; "
                        "type run execute only by intent."
                    ),
                ]
            )
        if page == "overview":
            return "\n".join(
                [
                    "[bold #7ee787]START HERE[/]  This is a control desk, not investment advice.",
                    f"[bold]1. Can I act?[/] {can_act}.",
                    (
                        f"[bold]2. Why?[/] {blocked_layers or 0} useful layer(s) blocked. "
                        "Readiness lists each blocker."
                    ),
                    (
                        "[bold]3. Best next click:[/] "
                        "2 Readiness for blockers, 4 Candidates for research, 3 Run for call plan."
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
                    "[bold #7ee787]USE THIS PAGE[/] Diagnose stale or broken data.",
                    "[bold]Look for:[/] unhealthy providers, stale market bars, database gaps.",
                    "[bold]Do next:[/] refresh after fixing data, then return to Start.",
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
                "[bold]Do next:[/] click rows when available, or return to Start with 1.",
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
                {**dict(row), "_row_key": str(row.get("ticker") or index)}
                for index, row in enumerate(_candidate_rows(self.payload), start=1)
            ]
            return (
                "Candidates - click a row or press Enter to open",
                [
                    ("ticker", "Ticker", 8),
                    ("state", "State", 20),
                    ("decision_status", "Decision", 16),
                    ("score", "Score", 8),
                    ("risk_or_gap", "Risk / gap", 34),
                    ("next_step", "Next step", 52),
                ],
                rows,
                "Commands: action <ticker> watch, trigger <ticker> ..., ticket <ticker> ...",
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
            return (
                "Provider health",
                [
                    ("provider", "Provider", 16),
                    ("status", "Status", 14),
                    ("checked_at", "Checked", 24),
                    ("reason", "Reason", 72),
                ],
                _rows(_mapping(self.payload.get("ops_health")).get("providers")),
                _ops_detail(self.payload),
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
        rows = [
            {
                "step": "1",
                "do": "Press 1 or click Start",
                "result": "See the plain-language answer: can I act, why, and next click.",
            },
            {
                "step": "2",
                "do": "Press 2 or click Readiness",
                "result": "See exactly what blocks a decision-useful workflow.",
            },
            {
                "step": "3",
                "do": "Press 4 or click Candidates",
                "result": "Review companies. These are research rows, not trade signals.",
            },
            {
                "step": "4",
                "do": "Press 3 or click Run",
                "result": "Review external-call budget before running anything.",
            },
            {
                "step": "5",
                "do": "Use the bottom command box",
                "result": "Try ticker AAPL, refresh, help, or q. Esc focuses the box.",
            },
        ]
        return (
            "Tutorial - your first 90 seconds",
            [("step", "Step", 6), ("do", "Do this", 34), ("result", "What happens", 96)],
            rows,
            "Safe rule: clicking, filtering, tutorial, and refresh make 0 provider calls.",
        )

    def _overview_model(
        self,
    ) -> tuple[str, Sequence[tuple[str, str, int]], list[Mapping[str, object]], str]:
        readiness = _mapping(self.payload.get("readiness"))
        usefulness = _mapping(readiness.get("market_radar_usefulness"))
        latest_run = _mapping(self.payload.get("latest_run"))
        freshness = _mapping(_mapping(readiness.get("discovery_snapshot")).get("freshness"))
        database = _mapping(_mapping(self.payload.get("ops_health")).get("database"))
        call_plan = _mapping(self.payload.get("call_plan"))
        can_act = _decision_label(readiness)
        next_action = readiness.get("next_action") or "Open Readiness."
        counts = _mapping(self.payload.get("candidates")).get("count") or 0
        alerts = _mapping(self.payload.get("alerts")).get("count") or 0
        rows = [
            {
                "question": "Can I act on this?",
                "answer": can_act,
                "do_this": (
                    "Use it for research only."
                    if readiness.get("safe_to_make_investment_decision") is not True
                    else "Manual review is still required before any action."
                ),
            },
            {
                "question": "Why not?",
                "answer": readiness.get("headline") or usefulness.get("headline"),
                "do_this": next_action,
            },
            {
                "question": "What should I click first?",
                "answer": "Readiness or Candidates",
                "do_this": f"Press 2 for blockers; press 4 to review {counts} candidate(s).",
            },
            {
                "question": "What changed recently?",
                "answer": (
                    f"Run {latest_run.get('status') or 'unknown'}; "
                    f"bars {database.get('latest_daily_bar_date') or 'n/a'}"
                ),
                "do_this": (
                    f"Run coverage {freshness.get('active_security_with_as_of_bar_count')}/"
                    f"{freshness.get('active_security_count')}; use Ops if this looks stale."
                ),
            },
            {
                "question": "Will this spend calls?",
                "answer": "Not while browsing",
                "do_this": (
                    f"Run page shows: {call_plan.get('status') or 'unknown'}, "
                    f"max {call_plan.get('max_external_call_count')} external calls."
                ),
            },
            {
                "question": "What else needs review?",
                "answer": (
                    f"{alerts} alert(s); "
                    f"{usefulness.get('ready_layers')}/"
                    f"{usefulness.get('total_layers')} layers ready"
                ),
                "do_this": "Press 5 for alerts or ? for the command reference.",
            },
        ]
        return (
            "Start here - answer these in order",
            [
                ("question", "Question", 28),
                ("answer", "Answer", 52),
                ("do_this", "Do this", 60),
            ],
            rows,
            (
                "Beginner path: 2 Readiness -> 4 Candidates -> 3 Run plan. "
                "Browsing and filtering make 0 provider calls."
            ),
        )

    def _candidate_detail_model(
        self,
        ticker: str,
    ) -> tuple[str, Sequence[tuple[str, str, int]], list[Mapping[str, object]], str]:
        ticker = ticker.upper()
        row = next(
            (item for item in _candidate_rows(self.payload) if item.get("ticker") == ticker),
            {},
        )
        rows = _mapping_items(_compact_detail(row))
        return (
            f"Candidate {ticker}",
            [("key", "Field", 24), ("value", "Value", 110)],
            rows,
            "Local commands can save watch actions, triggers, and blocked order-preview tickets.",
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
            {"command": "0, 1..9, f, ?", "meaning": "Keyboard page shortcuts."},
            {
                "command": "tutorial / start",
                "meaning": "Open the tutorial or the first dashboard page.",
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
    page: str = "tutorial",
    width: int | None = None,
) -> str:
    resolved_width = _resolve_width(width)
    page = _normalize_page(page)
    lines = _header_lines(payload, page, resolved_width)
    if page == "tutorial":
        lines.extend(_tutorial_lines(resolved_width))
    elif page == "overview":
        lines.extend(_overview_lines(payload, resolved_width))
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
            filters=DashboardFilters(telemetry_limit=filters.telemetry_limit),
            message="Filters cleared.",
        )
    if command in {"ticker", "tkr"}:
        ticker = value.upper()
        next_filters = replace(filters, ticker=None if ticker in {"", "ALL", "NONE"} else ticker)
        return _CommandUpdate(page=page, filters=next_filters, message="Ticker filter updated.")
    if command in {"available-at", "cutoff"}:
        if value.lower() in {"", "latest", "all", "none"}:
            return _CommandUpdate(
                page=page,
                filters=replace(filters, available_at=None),
                message="Available-at filter cleared.",
            )
        parsed = _datetime_or_none(value)
        if parsed is None:
            return _CommandUpdate(page=page, filters=filters, message="Invalid timestamp.")
        return _CommandUpdate(
            page=page,
            filters=replace(filters, available_at=parsed),
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
        return _CommandUpdate(page=next_page, filters=filters)
    return _CommandUpdate(
        page=page,
        filters=filters,
        message=f"Unknown command: {raw}. Type help for commands.",
    )


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
    return [
        _rule("Market Radar Terminal Dashboard", width, char="="),
        (
            f"Page: {page} | "
            f"Status: {_text(readiness.get('status'))} | "
            f"Decision safe: {_text(readiness.get('safe_to_make_investment_decision'))} | "
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


def _tutorial_lines(width: int) -> list[str]:
    lines = [_rule("Tutorial - your first 90 seconds", width)]
    lines.extend(
        _table_lines(
            [
                {
                    "step": "1",
                    "do": "Press 1 / click Start",
                    "result": "See whether Market Radar is usable and what to do next.",
                },
                {
                    "step": "2",
                    "do": "Press 2 / click Readiness",
                    "result": "See blockers before trusting output.",
                },
                {
                    "step": "3",
                    "do": "Press 4 / click Candidates",
                    "result": "Review company research rows.",
                },
                {
                    "step": "4",
                    "do": "Press 3 / click Run",
                    "result": "Review the external-call plan before running anything.",
                },
                {
                    "step": "5",
                    "do": "Use command box",
                    "result": "Try ticker AAPL, refresh, help, or q.",
                },
            ],
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
                ("Safe rule", "Tutorial, clicking, filtering, and refresh make 0 provider calls."),
                ("Orders", "Real order submission is disabled."),
                ("Exit", "Press q."),
            ),
            width=width,
        )
    )
    return lines


def _overview_lines(payload: Mapping[str, object], width: int) -> list[str]:
    readiness = _mapping(payload.get("readiness"))
    usefulness = _mapping(readiness.get("market_radar_usefulness"))
    next_step = _mapping(payload.get("operator_next_step"))
    latest_run = _mapping(payload.get("latest_run"))
    freshness = _mapping(_mapping(readiness.get("discovery_snapshot")).get("freshness"))
    database = _mapping(_mapping(payload.get("ops_health")).get("database"))
    call_plan = _mapping(payload.get("call_plan"))
    lines = [_rule("Overview", width)]
    lines.extend(
        _kv_lines(
            (
                ("Readiness", readiness.get("headline")),
                ("Next", readiness.get("next_action")),
                ("Usefulness", usefulness.get("headline")),
                (
                    "Useful layers",
                    f"{usefulness.get('ready_layers')}/{usefulness.get('total_layers')}"
                    f"; blocked={usefulness.get('blocked_layers')}"
                    f"; research={usefulness.get('research_layers')}",
                ),
                (
                    "Latest run",
                    f"{latest_run.get('status') or 'unknown'}; "
                    f"required={latest_run.get('required_completed_count')}/"
                    f"{latest_run.get('required_step_count')}; "
                    f"as_of={_nested(readiness, 'radar_run', 'as_of') or 'n/a'}",
                ),
                (
                    "Market freshness",
                    f"stale={freshness.get('latest_bars_older_than_as_of')}; "
                    f"latest_bar={freshness.get('latest_daily_bar_date') or 'n/a'}; "
                    f"run_as_of={_nested(readiness, 'radar_run', 'as_of') or 'n/a'}",
                ),
                (
                    "Run as-of coverage",
                    f"active={_text(freshness.get('active_security_count'))}; "
                    f"with_as_of_bar="
                    f"{_text(freshness.get('active_security_with_as_of_bar_count'))}; "
                    f"missing={_text(freshness.get('missing_as_of_daily_bar_count'))}",
                ),
                (
                    "Latest-bar coverage",
                    f"active={_text(database.get('active_security_count'))}; "
                    f"with_latest_bar="
                    f"{_text(database.get('active_security_with_latest_daily_bar_count'))}; "
                    f"latest_bar={database.get('latest_daily_bar_date') or 'n/a'}",
                ),
                (
                    "Call plan",
                    f"{call_plan.get('status') or 'unknown'}; "
                    f"will_call_external={call_plan.get('will_call_external_providers')}; "
                    f"max_external_calls={call_plan.get('max_external_call_count')}",
                ),
                (
                    "Operator next",
                    f"{next_step.get('priority') or 'n/a'}: "
                    f"{next_step.get('action') or 'No operator action.'}",
                ),
            ),
            width=width,
        )
    )
    lines.extend(_dashboard_count_lines(payload, width))
    return lines


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
    lines = [_rule("Radar Run And Call Plan", width)]
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
    lines.append("")
    lines.append(
        "Operational note: execute live runs only after this call plan matches intent. "
        "Type `run execute` to start one capped cycle."
    )
    return lines


def _candidates_lines(payload: Mapping[str, object], width: int) -> list[str]:
    rows = _candidate_rows(payload)
    lines = [_rule("Candidates", width)]
    lines.extend(
        _table_lines(
            _indexed(rows),
            [
                ("index", "#", 4),
                ("ticker", "Ticker", 8),
                ("state", "State", 20),
                ("decision_status", "Decision", 16),
                ("score", "Score", 8),
                ("risk_or_gap", "Risk / Gap", 38),
                ("next_step", "Next Step", 42),
            ],
            width=width,
            limit=16,
        )
    )
    lines.append("Use `open <#|ticker>` to inspect a candidate.")
    return lines


def _candidate_detail_lines(
    payload: Mapping[str, object],
    ticker: str,
    width: int,
) -> list[str]:
    ticker = ticker.strip().upper()
    row = next(
        (candidate for candidate in _candidate_rows(payload) if candidate.get("ticker") == ticker),
        {},
    )
    lines = [_rule(f"Candidate {ticker or 'n/a'}", width)]
    if not row:
        lines.append("Candidate not found for the current filters.")
        return lines
    lines.extend(
        _kv_lines(
            (
                ("State", row.get("state")),
                ("Decision", row.get("decision_status")),
                ("Score", row.get("score") or row.get("final_score")),
                ("Setup", row.get("setup") or row.get("setup_type")),
                ("Top catalyst", row.get("top_catalyst") or row.get("top_event_title")),
                ("Risk / gap", row.get("risk_or_gap")),
                ("Next step", row.get("next_step") or row.get("decision_next_step")),
                ("Readiness gate", row.get("readiness_gate") or row.get("decision_readiness_gate")),
                ("Schwab context", row.get("schwab_context_status")),
                ("Decision card", row.get("decision_card_id") or row.get("card")),
            ),
            width=width,
        )
    )
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


def _help_lines(width: int) -> list[str]:
    lines = [_rule("Help", width)]
    commands = [
        ("1..9 or page name", "Switch page."),
        ("features", "List current Market Radar features and where they live in the TUI."),
        ("open <#|ticker>", "Open a candidate from the candidates page."),
        ("open <#|alert-id>", "Open an alert from the alerts page."),
        ("ticker <SYMBOL|all>", "Filter candidate-adjacent pages by ticker where supported."),
        ("available-at <ISO|latest>", "Set or clear the point-in-time data cutoff."),
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
                              limit=20))
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
    readiness = _mapping(payload.get("readiness"))
    labeled = _rows(readiness.get("candidate_decision_labels"))
    if labeled:
        return labeled
    return _rows(_mapping(payload.get("candidates")).get("rows"))


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

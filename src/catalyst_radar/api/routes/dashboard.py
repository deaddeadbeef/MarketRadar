from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, ConfigDict, Field

from catalyst_radar.core.config import AppConfig
from catalyst_radar.dashboard.tui import (
    PAGE_ALIASES,
    DashboardFilters,
    apply_dashboard_command,
    dashboard_filters_for_page,
    dashboard_snapshot_payload,
)
from catalyst_radar.security.access import Role, require_role
from catalyst_radar.storage.db import create_schema, engine_from_url

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])

TRADING_WORKBENCH_TITLE = "MarketRadar Trading Workbench"


@dataclass(frozen=True)
class DashboardPageRequest:
    snapshot_page: str
    selected_page: str
    detail_ticker: str | None = None


class DashboardCommandRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    command: str = Field(min_length=1)
    page: str = "overview"
    ticker: str | None = Field(default=None, min_length=1, max_length=12)
    available_at: datetime | None = None
    alert_status: str | None = None
    alert_route: str | None = None
    priced_in_status: str = "all"
    usefulness: str | None = None
    source_gap: list[str] = Field(default_factory=list)
    decision_gap: list[str] = Field(default_factory=list)
    stocks_only: bool = False
    scan_limit: int = Field(default=50, ge=1, le=200)
    scan_offset: int = Field(default=0, ge=0)
    telemetry_limit: int = Field(default=8, ge=1, le=200)


DASHBOARD_DESKTOP_PAGES: tuple[dict[str, str], ...] = (
    {
        "key": "tutorial",
        "label": "0 Start",
        "shortcut": "0",
        "description": "First-run path and safe operating boundary.",
    },
    {
        "key": "overview",
        "label": "1 Command Center",
        "shortcut": "1",
        "description": "Trading workbench command center, account state, and next safe action.",
    },
    {
        "key": "portfolio",
        "label": "Portfolio",
        "shortcut": "portfolio",
        "description": "Positions, exposure, cash, and portfolio context.",
    },
    {
        "key": "market-radar",
        "label": "Market Radar",
        "shortcut": "radar",
        "description": "MarketRadar catalyst scout, mispricing queue, and evidence gaps.",
    },
    {
        "key": "trade-planner",
        "label": "Trade Planner",
        "shortcut": "planner",
        "description": "Trade thesis, sizing, reward/risk, and decision-card planning.",
    },
    {
        "key": "risk-desk",
        "label": "Risk Desk",
        "shortcut": "risk",
        "description": "Policy gates, portfolio impact, concentration, and hard blocks.",
    },
    {
        "key": "paper-trading",
        "label": "Paper Trading",
        "shortcut": "paper",
        "description": "Paper-only tickets, fills, and shadow validation.",
    },
    {
        "key": "backtest",
        "label": "Backtest",
        "shortcut": "replay",
        "description": "Replay, backtest, and validation evidence.",
    },
    {
        "key": "readiness",
        "label": "2 Evidence Gaps",
        "shortcut": "2",
        "description": "Evidence gaps and setup blockers before relying on output.",
    },
    {
        "key": "run",
        "label": "3 Safe Run",
        "shortcut": "3",
        "description": "Safe run plan, provider-call budget, and execution gates.",
    },
    {
        "key": "candidates",
        "label": "4 Candidate Review",
        "shortcut": "4",
        "description": "Candidate queue with source and decision gaps.",
    },
    {
        "key": "review",
        "label": "Review",
        "shortcut": "D",
        "description": "Decision-ready rows filtered to useful review candidates.",
    },
    {
        "key": "alerts",
        "label": "5 Alerts",
        "shortcut": "5",
        "description": "Research alerts and routing status.",
    },
    {
        "key": "ipo",
        "label": "6 IPO/S-1",
        "shortcut": "6",
        "description": "IPO/S-1 catalyst evidence rows.",
    },
    {
        "key": "broker",
        "label": "7 Broker",
        "shortcut": "7",
        "description": "Read-only broker and portfolio context.",
    },
    {
        "key": "ops",
        "label": "8 Ops",
        "shortcut": "8",
        "description": "Provider health, runtime context, and run diagnostics.",
    },
    {
        "key": "telemetry",
        "label": "9 Telemetry",
        "shortcut": "9",
        "description": "Audit tape and telemetry coverage.",
    },
    {
        "key": "agent",
        "label": "Ctrl+A Agent",
        "shortcut": "Ctrl+A",
        "description": "Zero-call agent preview and gated OpenAI execution status.",
    },
    {
        "key": "themes",
        "label": "Themes",
        "shortcut": "theme",
        "description": "Clustered catalyst patterns and repeated theme context.",
    },
    {
        "key": "validation",
        "label": "Validation",
        "shortcut": "valid",
        "description": "Shadow, paper, and value validation evidence.",
    },
    {
        "key": "costs",
        "label": "Costs",
        "shortcut": "V",
        "description": "Value ledger, outcomes, validation, and cost evidence.",
    },
    {
        "key": "features",
        "label": "F Features",
        "shortcut": "F",
        "description": "Feature inventory and where each feature lives.",
    },
    {
        "key": "journal",
        "label": "Journal",
        "shortcut": "journal",
        "description": "Decision journal, feedback, value ledger, and outcome review.",
    },
    {
        "key": "help",
        "label": "? Help",
        "shortcut": "?",
        "description": "Keyboard, automation, and command reference.",
    },
)

TRADING_PLATFORM_MODULES: tuple[dict[str, str], ...] = (
    {
        "key": "command-center",
        "label": "Command Center",
        "role": "Operating home for account state, safe action, and agent handoff.",
        "source": "local dashboard snapshot",
        "status": "active",
        "page": "overview",
        "test_id": "platform-tool-command-center",
        "next_action": "Review the safe action and route work to a tool.",
    },
    {
        "key": "portfolio",
        "label": "Portfolio",
        "role": "Positions, exposure, cash, watch intent, and broker context.",
        "source": "read-only broker and local portfolio records",
        "status": "route_ready",
        "page": "portfolio",
        "test_id": "platform-tool-portfolio",
        "next_action": "Inspect exposure before any trade plan.",
    },
    {
        "key": "market-radar",
        "label": "Market Radar",
        "role": "Scouted catalysts, mispricing queues, evidence gaps, and watchlists.",
        "source": "priced-in queue and catalyst evidence",
        "status": "active",
        "page": "market-radar",
        "test_id": "platform-tool-market-radar",
        "next_action": "Open the top evidence row or fill missing sources.",
    },
    {
        "key": "trade-planner",
        "label": "Trade Planner",
        "role": "Candidate sizing, thesis, reward/risk, and decision-card assembly.",
        "source": "decision cards and validation evidence",
        "status": "route_ready",
        "page": "trade-planner",
        "test_id": "platform-tool-trade-planner",
        "next_action": "Draft a plan from a decision-ready candidate.",
    },
    {
        "key": "risk-desk",
        "label": "Risk Desk",
        "role": "Policy gates, portfolio impact, concentration, and hard blocks.",
        "source": "policy scan, broker context, and validation artifacts",
        "status": "route_ready",
        "page": "risk-desk",
        "test_id": "platform-tool-risk-desk",
        "next_action": "Resolve hard blocks before paper or live consideration.",
    },
    {
        "key": "paper-trading",
        "label": "Paper Trading",
        "role": "Paper-only tickets, fills, outcomes, and shadow validation.",
        "source": "paper trades and value outcomes",
        "status": "preview_only",
        "page": "paper-trading",
        "test_id": "platform-tool-paper-trading",
        "next_action": "Use paper execution only after risk approval.",
    },
    {
        "key": "broker-desk",
        "label": "Broker Desk",
        "role": "Read-only broker connection, order-ticket previews, and sync boundaries.",
        "source": "broker snapshot and local order-ticket records",
        "status": "read_only",
        "page": "broker",
        "test_id": "platform-tool-broker-desk",
        "next_action": "Authenticate only for portfolio context; order submission is disabled.",
    },
    {
        "key": "backtest",
        "label": "Backtest / Replay",
        "role": "Historical replay, shadow-mode validation, and strategy evidence.",
        "source": "validation runs and backtest artifacts",
        "status": "route_ready",
        "page": "backtest",
        "test_id": "platform-tool-backtest",
        "next_action": "Compare candidate logic against replay evidence.",
    },
    {
        "key": "alerts",
        "label": "Alerts",
        "role": "Research notifications, watch triggers, and operator routing.",
        "source": "local alert rows",
        "status": "active",
        "page": "alerts",
        "test_id": "platform-tool-alerts",
        "next_action": "Open an alert as research context, not trade approval.",
    },
    {
        "key": "journal",
        "label": "Journal",
        "role": "Decision notes, feedback, value ledger, and outcome review.",
        "source": "local feedback and value ledger records",
        "status": "route_ready",
        "page": "journal",
        "test_id": "platform-tool-journal",
        "next_action": "Record feedback and outcome evidence locally.",
    },
    {
        "key": "agent-cockpit",
        "label": "Agent Cockpit",
        "role": "Agent brief, proposed tool use, budget gates, and execution review.",
        "source": "agent brief and runtime context",
        "status": "preview_only",
        "page": "agent",
        "test_id": "platform-tool-agent-cockpit",
        "next_action": "Preview agent reasoning; execute remains gated.",
    },
)

TRADING_PLATFORM_BOUNDARY: dict[str, object] = {
    "live_trading_enabled": False,
    "broker_order_submission": "disabled",
    "autonomous_execution": "disabled",
    "paper_trading": "preview_only",
    "provider_calls_for_browsing": 0,
}

DASHBOARD_COMMAND_BOX_COMMANDS: tuple[dict[str, str], ...] = (
    {
        "command": "0..9, Ctrl+A, Ctrl+N/P, Tab, J/K, V, F, ?, or page name",
        "meaning": "Switch pages; Ctrl+A opens Agent and V opens Costs.",
        "safety": "zero_provider_calls",
        "route": "local_navigation",
    },
    {
        "command": "themes / validation / costs / features",
        "meaning": (
            "Open local evidence pages for clustered themes, validation, costs, "
            "and feature inventory."
        ),
        "safety": "zero_provider_calls",
        "route": "local_navigation",
    },
    {
        "command": "setup / first",
        "meaning": "Show the first setup command and where to run it.",
        "safety": "zero_provider_calls",
        "route": "local_navigation",
    },
    {
        "command": "open #|TICKER",
        "meaning": "Open a row from Candidate Review or show its next command.",
        "safety": "zero_provider_calls",
        "route": "local_detail",
    },
    {
        "command": "ticker SYMBOL|all",
        "meaning": "Filter ticker-aware pages.",
        "safety": "zero_provider_calls",
        "route": "snapshot_refresh",
    },
    {
        "command": "available-at ISO|latest",
        "meaning": "Set or clear the point-in-time cutoff.",
        "safety": "zero_provider_calls",
        "route": "snapshot_refresh",
    },
    {
        "command": "ready / full / mismatches / stocks",
        "meaning": (
            "Apply decision-useful, full universe, mismatch, and stock-only "
            "scan filters."
        ),
        "safety": "zero_provider_calls",
        "route": "local_filter",
    },
    {
        "command": "usefulness STATUS|all",
        "meaning": "Filter Inbox by usefulness verdict.",
        "safety": "zero_provider_calls",
        "route": "local_filter",
    },
    {
        "command": "source-gap SOURCE|all",
        "meaning": "Filter Inbox by missing or stale source evidence.",
        "safety": "zero_provider_calls",
        "route": "local_filter",
    },
    {
        "command": "decision-gap GAP|all",
        "meaning": "Filter Inbox by missing decision evidence.",
        "safety": "zero_provider_calls",
        "route": "local_filter",
    },
    {
        "command": "next / prev / offset ROW / limit 1-200",
        "meaning": "Page through current Inbox scan rows.",
        "safety": "zero_provider_calls",
        "route": "local_pagination",
    },
    {
        "command": "export full / export current",
        "meaning": "Show JSON export commands without running them.",
        "safety": "external_boundary",
        "route": "command_preview",
    },
    {
        "command": "batch SOURCE / batch SOURCE all / batch SOURCE execute 3",
        "meaning": "Plan source fills or show the external execution boundary.",
        "safety": "plan_only_execute_external",
        "route": "local_plan",
    },
    {
        "command": "catalyst-radar COMMAND",
        "meaning": (
            "Show where to run full CLI commands without executing them in the "
            "dashboard."
        ),
        "safety": "external_boundary",
        "route": "powershell_boundary",
    },
    {
        "command": "bars manual template/import",
        "meaning": (
            "Preview market-bar repair commands through the dashboard backend; "
            "execute stays external."
        ),
        "safety": "preview_only_execute_external",
        "route": "dashboard_backend",
    },
    {
        "command": "bars saved capture/validate/import",
        "meaning": (
            "Preview saved grouped-daily commands through the dashboard backend; "
            "confirm/execute stays external."
        ),
        "safety": "preview_only_confirm_execute_external",
        "route": "dashboard_backend",
    },
    {
        "command": "options template/validate/import",
        "meaning": (
            "Preview point-in-time options commands through the dashboard backend; "
            "execute stays external."
        ),
        "safety": "preview_only_execute_external",
        "route": "dashboard_backend",
    },
    {
        "command": "cik template/validate/import",
        "meaning": (
            "Preview SEC CIK override commands through the dashboard backend; "
            "execute stays external."
        ),
        "safety": "preview_only_execute_external",
        "route": "dashboard_backend",
    },
    {
        "command": "agent / agent execute",
        "meaning": "Preview agent gates through the dashboard backend; execute stays external.",
        "safety": "preview_only_execute_external",
        "route": "dashboard_backend",
    },
    {
        "command": "alert-status STATUS|all / alert-route ROUTE|all",
        "meaning": "Filter alerts.",
        "safety": "zero_provider_calls",
        "route": "local_filter",
    },
    {
        "command": "run / run execute",
        "meaning": "Open Safe Run or show the capped run execution boundary.",
        "safety": "guarded_execution",
        "route": "dashboard_backend",
    },
    {
        "command": "action / trigger / ticket / feedback",
        "meaning": "Run guarded local Broker or Alert commands through the dashboard backend.",
        "safety": "local_db_only",
        "route": "dashboard_backend",
    },
    {
        "command": "paper-decision preview / execute",
        "meaning": (
            "Preview or record the active trading plan as a local paper decision; "
            "broker order submission stays disabled."
        ),
        "safety": "local_db_only_no_broker_order",
        "route": "dashboard_backend",
    },
    {
        "command": "ledger coverage / record",
        "meaning": "Run guarded local value-ledger commands through the dashboard backend.",
        "safety": "local_db_only",
        "route": "dashboard_backend",
    },
    {
        "command": "outcome coverage / update",
        "meaning": "Run guarded local value-outcome commands through the dashboard backend.",
        "safety": "local_db_only",
        "route": "dashboard_backend",
    },
    {
        "command": "json",
        "meaning": "Open and focus the raw JSON snapshot.",
        "safety": "zero_provider_calls",
        "route": "local_snapshot_view",
    },
    {
        "command": "clear-filters / refresh / q",
        "meaning": "Reset filters, reload, or close the native window.",
        "safety": "zero_provider_calls",
        "route": "local_navigation",
    },
)


def _engine():
    engine = engine_from_url(AppConfig.from_env().database_url)
    create_schema(engine)
    return engine


def _desktop_pages() -> list[dict[str, str]]:
    return [
        {
            **page,
            "test_id": f"nav-page-{page['key']}",
        }
        for page in DASHBOARD_DESKTOP_PAGES
    ]


def _command_box_commands() -> list[dict[str, str]]:
    return [dict(command) for command in DASHBOARD_COMMAND_BOX_COMMANDS]


def _trading_platform_manifest() -> dict[str, object]:
    return {
        "schema_version": "trading-platform-manifest-v1",
        "name": TRADING_WORKBENCH_TITLE,
        "primary_tool": "market-radar",
        "modules": [dict(module) for module in TRADING_PLATFORM_MODULES],
        "execution_boundary": dict(TRADING_PLATFORM_BOUNDARY),
    }


def _automation_recipe() -> dict[str, object]:
    return {
        "schema_version": "dashboard-computer-use-recipe-v1",
        "launch": {
            "executable": "target\\release\\radar-desktop.exe",
            "window_title": TRADING_WORKBENCH_TITLE,
        },
        "state_sources": {
            "page": "automation-state",
            "filters": "filter-state",
            "command": "command-state",
            "json": "automation-json",
        },
        "expected_json_keys": [
            "contract_version",
            "page",
            "nav",
            "status",
            "provider_calls",
            "last_command",
            "filters",
        ],
        "expected_filter_keys": [
            "ticker",
            "scan_mode",
            "stocks_only",
            "limit",
            "offset",
            "usefulness",
            "source_gap",
            "decision_gap",
            "available_at",
            "alert_status",
            "alert_route",
        ],
        "actions": [
            {
                "id": "focus-command",
                "input_kind": "key",
                "input": "Escape",
                "target_test_id": "command-input",
                "route": "local_navigation",
                "expected_page": "overview",
                "expected_nav": "overview",
                "expected_provider_calls": 0,
                "expected_state": [
                    "command-state contains command box focused",
                    "automation-json.provider_calls=0",
                ],
                "requires_review": False,
            },
            {
                "id": "filter-ticker",
                "input_kind": "command",
                "input": "ticker MSFT",
                "target_test_id": "command-input",
                "route": "snapshot_refresh",
                "expected_page": "overview",
                "expected_nav": "overview",
                "expected_provider_calls": 0,
                "expected_state": [
                    "automation-json.filters.ticker=MSFT",
                    "filter-state contains ticker=MSFT",
                ],
                "requires_review": False,
            },
            {
                "id": "reject-invalid-source-gap",
                "input_kind": "command",
                "input": "source-gap nonsense",
                "target_test_id": "command-input",
                "route": "local_filter_validation",
                "expected_page": "overview",
                "expected_nav": "overview",
                "expected_provider_calls": 0,
                "expected_state": [
                    "command-state contains Unsupported source-gap value",
                    "automation-json.filters.source_gap remains unchanged",
                ],
                "requires_review": False,
            },
            {
                "id": "ready-review-filter",
                "input_kind": "command",
                "input": "ready",
                "target_test_id": "command-input",
                "route": "local_filter",
                "expected_page": "review",
                "expected_nav": "review",
                "expected_provider_calls": 0,
                "expected_state": [
                    "automation-json.filters.scan_mode=actionable",
                    "automation-json.filters.usefulness=decision_useful",
                    "filter-state contains scan_mode=actionable",
                ],
                "requires_review": False,
            },
            {
                "id": "open-review-page",
                "input_kind": "command",
                "input": "review",
                "target_test_id": "command-input",
                "route": "local_navigation",
                "expected_page": "review",
                "expected_nav": "review",
                "expected_provider_calls": 0,
                "expected_state": [
                    "dashboard-page data-current-page=review",
                    "automation-json.page=review",
                ],
                "requires_review": False,
            },
            {
                "id": "open-row",
                "input_kind": "command",
                "input": "open 1",
                "target_test_id": "command-input",
                "route": "local_detail",
                "expected_page": "candidate:<TICKER>|alert:<ID>",
                "expected_nav": "candidates|alerts",
                "expected_provider_calls": 0,
                "expected_state": [
                    "dashboard-page exposes candidate-detail or alert-detail",
                    "automation-json.nav is candidates or alerts",
                ],
                "requires_review": False,
            },
            {
                "id": "source-plan",
                "input_kind": "command",
                "input": "batch catalyst_events",
                "target_test_id": "command-input",
                "route": "local_plan",
                "expected_page": "ops",
                "expected_nav": "ops",
                "expected_provider_calls": 0,
                "expected_state": [
                    "command-state contains source plan",
                    "automation-json.provider_calls=0",
                ],
                "requires_review": False,
            },
            {
                "id": "source-execute-boundary",
                "input_kind": "command",
                "input": "batch catalyst_events execute 3",
                "target_test_id": "command-input",
                "route": "powershell_boundary",
                "expected_page": "ops",
                "expected_nav": "ops",
                "expected_provider_calls": 0,
                "expected_state": [
                    "command-state contains --execute-batches 3",
                    "command-state contains provider_calls=0 in the desktop app",
                ],
                "requires_review": True,
            },
            {
                "id": "provider-preview",
                "input_kind": "command",
                "input": "bars status",
                "target_test_id": "command-input",
                "route": "dashboard_backend",
                "expected_page": "run",
                "expected_nav": "run",
                "expected_provider_calls": 0,
                "expected_state": [
                    "command-state contains Market-bar status",
                    "automation-json.provider_calls=0",
                ],
                "requires_review": False,
            },
            {
                "id": "safe-run-execute",
                "input_kind": "command",
                "input": "run execute",
                "target_test_id": "command-input",
                "route": "guarded_dashboard_backend",
                "expected_page": "run",
                "expected_nav": "run",
                "expected_provider_calls": None,
                "expected_state": [
                    "command-state contains Radar run finished, blocked, or rate limited",
                    "backend result includes radar_run telemetry",
                ],
                "requires_review": True,
            },
            {
                "id": "powershell-boundary",
                "input_kind": "command",
                "input": "catalyst-radar priced-in-queue --full-scan --all --json",
                "target_test_id": "command-input",
                "route": "powershell_boundary",
                "expected_page": None,
                "expected_nav": None,
                "expected_provider_calls": 0,
                "expected_state": [
                    "command-state says PowerShell command, not a dashboard command",
                    "automation-json.provider_calls=0",
                ],
                "requires_review": True,
            },
            {
                "id": "open-json",
                "input_kind": "command",
                "input": "json",
                "target_test_id": "command-input",
                "route": "local_snapshot_view",
                "expected_page": None,
                "expected_nav": None,
                "expected_provider_calls": 0,
                "expected_state": [
                    "snapshot-json-output is focused",
                    "automation-json remains parseable",
                ],
                "requires_review": False,
            },
            {
                "id": "close-window",
                "input_kind": "command",
                "input": "q",
                "target_test_id": "command-input",
                "route": "local_window_control",
                "expected_page": None,
                "expected_nav": None,
                "expected_provider_calls": 0,
                "expected_state": [
                    "native MarketRadar Trading Workbench window closes",
                ],
                "requires_review": True,
            },
        ],
    }


@router.get("/manifest", dependencies=[Depends(require_role(Role.VIEWER))])
def manifest() -> dict[str, object]:
    return {
        "schema_version": "dashboard-ui-manifest-v1",
        "external_calls_made": 0,
        "app_name": TRADING_WORKBENCH_TITLE,
        "surfaces": {
            "default": "tauri_desktop",
            "terminal": "rust_tui",
            "legacy": "python_textual",
        },
        "pages": _desktop_pages(),
        "platform": _trading_platform_manifest(),
        "automation": {
            "contract_version": "market-radar-desktop-automation-v1",
            "landmarks": [
                "desktop-shell",
                "workflow-nav",
                "dashboard-toolbar",
                "dashboard-page",
                "command-form",
                "command-input",
                "command-status",
                "command-state",
                "automation-state",
                "automation-json",
                "filter-state",
                "attention-queue",
                "loading-dashboard",
                "loading-metric-strip",
                "loading-preview-queue",
                "next-safe-action",
                "keys-panel",
                "keys-list",
                "snapshot-panel",
                "snapshot-source",
                "snapshot-refresh",
                "snapshot-page",
                "snapshot-mode",
                "snapshot-json",
                "snapshot-json-output",
            ],
            "keyboard_shortcuts": [
                "0-9 jump to numbered workflow pages",
                "Ctrl+A opens Agent",
                "Ctrl+N moves forward; Ctrl+P moves backward",
                "Type themes or validation to open evidence pages",
                "V opens Costs",
                "F opens Features",
                "? opens Help",
                "ArrowRight/ArrowDown/Tab/J moves forward",
                "ArrowLeft/ArrowUp/Shift+Tab/K moves backward",
                "F5 or R refreshes the local snapshot",
                "Home opens Start, End opens Help",
                "Esc focuses the command box",
                "next and prev page through scan rows without walking past the end",
                "clear-filters resets filters while preserving the row limit",
                (
                    "usefulness clears with all, any, none, or blank; alert "
                    "filters clear with all, none, or blank"
                ),
                "Command box accepts safe page, filter, refresh, help, and JSON commands",
                (
                    "offset, limit, and available-at commands reject invalid "
                    "values before refreshing"
                ),
                (
                    "source-gap and decision-gap commands reject unsupported "
                    "values before refreshing"
                ),
                (
                    "ready applies the decision-ready scan filter; review "
                    "opens the Review page"
                ),
                (
                    "batch SOURCE opens an Ops source plan; batch SOURCE all "
                    "and batch SOURCE execute N show PowerShell boundaries"
                ),
                (
                    "run opens Safe Run; run execute starts the guarded "
                    "radar-run API/CLI backend path"
                ),
                (
                    "action, trigger, ticket, feedback, paper-decision, "
                    "ledger, and outcome commands use the guarded dashboard "
                    "backend for local DB-only operations"
                ),
                (
                    "agent, bars, options, and cik/sec planning commands use "
                    "the guarded dashboard backend for preview/status output; "
                    "execute and confirm variants stay external boundaries"
                ),
                (
                    "Q closes the native desktop window; q, quit, or exit "
                    "also close from the command box"
                ),
                (
                    "Full catalyst-radar commands show a PowerShell boundary "
                    "instead of executing in-app"
                ),
            ],
            "command_box_commands": _command_box_commands(),
            "automation_recipe": _automation_recipe(),
            "native_window_title": TRADING_WORKBENCH_TITLE,
            "native_executable": "target\\release\\radar-desktop.exe",
            "computer_use_steps": [
                {
                    "step": "launch",
                    "action": (
                        "Launch the app by executable path through Computer Use, "
                        "then select the returned window object."
                    ),
                    "target": "target\\release\\radar-desktop.exe",
                    "expected": (
                        "A native window titled MarketRadar Trading Workbench is targetable."
                    ),
                },
                {
                    "step": "capture",
                    "action": "Capture screenshot and accessibility text for the selected window.",
                    "target": "MarketRadar Trading Workbench",
                    "expected": (
                        "The window exposes MarketRadar workflow tabs, dashboard-page, "
                        "command-input, command-state, automation-state, "
                        "automation-json, filter-state, loading-dashboard "
                        "before first data, next-safe-action, "
                        "keys-panel, snapshot-panel, and page=<PAGE>, "
                        "nav=<WORKFLOW_PAGE>, snapshot-page=<PAGE>, and "
                        "provider_calls=0."
                    ),
                },
                {
                    "step": "focus-command",
                    "action": "Press Escape in the dashboard window.",
                    "target": "command-input",
                    "expected": (
                        "The command box receives focus and command-state reports "
                        "command box focused."
                    ),
                },
                {
                    "step": "filter-command",
                    "action": "Type ticker MSFT and press Return.",
                    "target": "command-input",
                    "expected": (
                        "filter-ticker is MSFT, automation-state remains page=overview, "
                        "and provider_calls=0."
                    ),
                },
                {
                    "step": "filter-validation-command",
                    "action": "Type source-gap nonsense and press Return.",
                    "target": "command-input",
                    "expected": (
                        "command-status reports Unsupported source-gap value, "
                        "the filter is unchanged, and provider_calls=0."
                    ),
                },
                {
                    "step": "numeric-validation-command",
                    "action": "Type limit 1.5 and press Return.",
                    "target": "command-input",
                    "expected": (
                        "command-status reports Usage: limit 1-200, "
                        "the scan limit is unchanged, and provider_calls=0."
                    ),
                },
                {
                    "step": "time-validation-command",
                    "action": "Type available-at nonsense and press Return.",
                    "target": "command-input",
                    "expected": (
                        "command-status reports Invalid timestamp, "
                        "available_at is unchanged, and provider_calls=0."
                    ),
                },
                {
                    "step": "pagination-boundary-command",
                    "action": (
                        "When the current scan page is at the end, type next "
                        "and press Return."
                    ),
                    "target": "command-input",
                    "expected": (
                        "command-status reports Already at the end of the "
                        "current scan filter and provider_calls=0."
                    ),
                },
                {
                    "step": "clear-filters-command",
                    "action": (
                        "Type limit 25, press Return, then type clear-filters "
                        "and press Return."
                    ),
                    "target": "command-input",
                    "expected": (
                        "filter-limit remains 25, non-limit filters are reset, "
                        "scan_offset returns to 0, and provider_calls=0."
                    ),
                },
                {
                    "step": "optional-filter-clear-command",
                    "action": "Type usefulness ANY and press Return.",
                    "target": "command-input",
                    "expected": (
                        "usefulness is cleared case-insensitively, "
                        "command-status reports Usefulness filter cleared, "
                        "and provider_calls=0."
                    ),
                },
                {
                    "step": "ready-filter-command",
                    "action": "Type ready and press Return.",
                    "target": "filter-state",
                    "expected": (
                        "filter-state reports scan_mode=actionable and "
                        "usefulness=decision_useful, automation-json reports "
                        "last_command=ready, filters.scan_mode=actionable, "
                        "filters.usefulness=decision_useful, page=review, "
                        "and provider_calls=0."
                    ),
                },
                {
                    "step": "page-command",
                    "action": "Type review and press Return.",
                    "target": "command-input",
                    "expected": (
                        "dashboard-page reports page=review, the selected tab is "
                        "Review, filter-state is still exposed, and "
                        "provider_calls=0."
                    ),
                },
                {
                    "step": "row-open",
                    "action": (
                        "Focus a queue-row and press Return, or type open 1 and "
                        "press Return."
                    ),
                    "target": "queue-row",
                    "expected": (
                        "dashboard-page reports page=candidate:<TICKER> or "
                        "page=alert:<ID>, automation-state reports nav=candidates "
                        "or nav=alerts, the detail panel is visible, and provider_calls=0."
                    ),
                },
                {
                    "step": "guarded-command",
                    "action": "Type batch catalyst_events and press Return.",
                    "target": "command-input",
                    "expected": (
                        "dashboard-page reports page=ops, command-status shows "
                        "a source-specific Ops plan or workflow status, and "
                        "provider_calls=0."
                    ),
                },
                {
                    "step": "source-batch-execute-boundary",
                    "action": (
                        "Type batch catalyst_events execute 3 and press Return."
                    ),
                    "target": "command-input",
                    "expected": (
                        "dashboard-page reports page=ops, command-status shows "
                        "the PowerShell command with --execute-batches 3 and "
                        "provider_calls=0."
                    ),
                },
                {
                    "step": "local-dashboard-command",
                    "action": (
                        "Type action ACME watch Codex smoke and press Return "
                        "only after intentional local write validation."
                    ),
                    "target": "command-input",
                    "expected": (
                        "dashboard-page reports page=broker, command-status "
                        "reports Local only, db_writes=1, and no provider, "
                        "OpenAI, broker, order, or external calls occur after "
                        "refresh."
                    ),
                },
                {
                    "step": "paper-decision-command",
                    "action": (
                        "Type paper-decision preview and press Return, then "
                        "type paper-decision execute only after reviewing the "
                        "active plan."
                    ),
                    "target": "command-input",
                    "expected": (
                        "dashboard-page reports page=paper-trading, "
                        "command-status reports paper_decision, "
                        "external_calls=0, no_execution=true, "
                        "broker_order_submitted=false, and any DB write is a "
                        "local paper-trade/audit record only."
                    ),
                },
                {
                    "step": "provider-preview-command",
                    "action": "Type bars status and press Return.",
                    "target": "command-input",
                    "expected": (
                        "dashboard-page reports page=run, command-status "
                        "reports Market-bar status from the dashboard backend, "
                        "and provider_calls=0 after refresh."
                    ),
                },
                {
                    "step": "safe-run-execute-command",
                    "action": (
                        "Type run execute and press Return only after reviewing "
                        "the Safe Run call plan."
                    ),
                    "target": "command-input",
                    "expected": (
                        "dashboard-page reports page=run, command-status reports "
                        "Radar run finished, blocked, or rate limited, and the "
                        "backend returns the radar_run telemetry contract."
                    ),
                },
                {
                    "step": "powershell-command",
                    "action": (
                        "Type catalyst-radar priced-in-queue --full-scan "
                        "--all --json and press Return."
                    ),
                    "target": "command-input",
                    "expected": (
                        "command-status says it is a PowerShell command, not a "
                        "dashboard command, and provider_calls=0."
                    ),
                },
                {
                    "step": "json-command",
                    "action": "Type json and press Return.",
                    "target": "snapshot-json-output",
                    "expected": (
                        "Raw JSON snapshot opens, focus moves to "
                        "snapshot-json-output, and provider_calls=0."
                    ),
                },
                {
                    "step": "close-command",
                    "action": (
                        "Type q and press Return only when the automation "
                        "session is finished."
                    ),
                    "target": "command-input",
                    "expected": (
                        "The native MarketRadar Trading Workbench window closes "
                        "without provider, OpenAI, broker, or DB-write actions."
                    ),
                },
            ],
            "zero_call_assertions": [
                (
                    "Dashboard browsing, command-box navigation, filtering, copy, "
                    "and raw JSON inspection must leave provider_calls=0."
                ),
                (
                    "Local broker, feedback, paper-decision, value-ledger, "
                    "and outcome commands may write the local DB through the "
                    "guarded dashboard backend, but must not make provider, "
                    "OpenAI, broker, order, or external calls unless the "
                    "command explicitly reports an external-call budget."
                ),
                (
                    "Agent, market-bar, options, and SEC CIK preview/status "
                    "commands may use the dashboard backend, but execute or "
                    "confirm variants must remain external PowerShell "
                    "boundaries unless the backend command explicitly reports "
                    "an accepted external-call budget."
                ),
                (
                    "Source batch plan commands may read the current snapshot, "
                    "but execute variants must remain external PowerShell "
                    "boundaries and leave provider_calls=0."
                ),
                (
                    "Invalid source-gap or decision-gap filter commands must "
                    "not refresh the snapshot or change filters."
                ),
                (
                    "Invalid offset, limit, or available-at commands must not "
                    "refresh the snapshot or change filters."
                ),
                (
                    "ready must update filter-state to scan_mode=actionable "
                    "and usefulness=decision_useful while opening Review "
                    "without provider calls; review must open the Review page "
                    "without changing filters."
                ),
                (
                    "Pagination commands must not advance scan_offset beyond "
                    "priced_in_queue.total_count."
                ),
                (
                    "clear-filters must preserve the chosen row limit while "
                    "clearing ticker, source, decision, availability, alert, "
                    "usefulness, and offset filters."
                ),
                (
                    "Optional usefulness filters must clear case-insensitively "
                    "for all, any, none, or blank input; alert-status and "
                    "alert-route clear for all, none, or blank input."
                ),
                (
                    "Full catalyst-radar commands typed into the desktop command box "
                    "must stay external and leave provider_calls=0."
                ),
                (
                    "Clicking or pressing Enter on queue rows must open local "
                    "candidate/alert detail without provider calls."
                ),
                (
                    "Dynamic detail pages must expose both page=<candidate|alert detail> "
                    "and nav=<parent workflow page> for automation."
                ),
                (
                    "q, quit, and exit close the native window through the Tauri "
                    "window API and must not run provider, OpenAI, broker, or "
                    "DB-write actions."
                ),
            ],
            "notes": [
                "Every workflow button has role=tab, aria-selected, and a nav-page-* data-testid.",
                "The current page title is exposed through data-testid=page-title.",
                (
                    "The latest command, current page/nav, provider-call count, "
                    "and command result are exposed through data-testid=command-state."
                ),
                (
                    "The exact selected page, parent nav page, and provider-call "
                    "count are exposed through data-testid=automation-state."
                ),
                (
                    "The aggregate automation state is exposed as machine-readable "
                    "JSON through data-testid=automation-json."
                ),
                (
                    "The active ticker, scan, availability, alert, source-gap, "
                    "decision-gap, usefulness, limit, and offset filters are "
                    "exposed through data-testid=filter-state."
                ),
                (
                    "The dashboard main region exposes data-current-page and "
                    "data-current-nav-page for dynamic detail pages."
                ),
                (
                    "Before the first snapshot loads, the main region exposes "
                    "loading-dashboard, loading-metric-strip, and "
                    "loading-preview-queue instead of a blank box."
                ),
                (
                    "The right rail exposes keys-panel and snapshot-panel, "
                    "including snapshot-source, snapshot-refresh, "
                    "snapshot-page, and snapshot-mode."
                ),
                (
                    "Candidate detail pages keep nav-page-candidates selected; "
                    "alert detail pages keep nav-page-alerts selected."
                ),
                (
                    "Rows use data-testid=queue-row, are keyboard focusable, "
                    "and include ticker-specific labels when available."
                ),
                (
                    "Refreshing reads the existing dashboard JSON contract and "
                    "makes zero provider calls."
                ),
                (
                    "Local broker, feedback, paper-decision, value-ledger, "
                    "and outcome commands use the guarded dashboard backend; "
                    "source-batch execute and provider execute/confirm "
                    "commands remain external PowerShell boundaries; "
                    "provider preview/status commands use the guarded "
                    "dashboard backend; run execute uses the guarded "
                    "radar-run API/CLI backend path."
                ),
            ],
        },
        "data_contract": {
            "snapshot_endpoint": "/api/dashboard/snapshot?fast=true",
            "snapshot_command": "catalyst-radar dashboard-snapshot --json --fast",
            "provider_calls_for_browsing": 0,
        },
    }


def _dashboard_page_request(page: str) -> DashboardPageRequest:
    raw_page = page.strip() or "overview"
    if ticker := _detail_page_suffix(raw_page, "candidate:"):
        ticker = ticker.upper()
        return DashboardPageRequest(
            snapshot_page="overview",
            selected_page=f"candidate:{ticker}",
            detail_ticker=ticker,
        )
    if alert_id := _detail_page_suffix(raw_page, "alert:"):
        return DashboardPageRequest(
            snapshot_page="alerts",
            selected_page=f"alert:{alert_id}",
        )
    canonical_page = _canonical_dashboard_page(raw_page)
    return DashboardPageRequest(
        snapshot_page=canonical_page,
        selected_page=canonical_page,
    )


def _canonical_dashboard_page(page: str) -> str:
    normalized = "-".join(page.strip().lower().replace("_", " ").split())
    return PAGE_ALIASES.get(normalized, page)


def _detail_page_suffix(page: str, prefix: str) -> str | None:
    if page[: len(prefix)].lower() != prefix:
        return None
    suffix = page[len(prefix) :].strip()
    return suffix or None


def _dashboard_filters_from_values(
    page_request: DashboardPageRequest,
    *,
    ticker: str | None,
    available_at: datetime | None,
    alert_status: str | None,
    alert_route: str | None,
    priced_in_status: str,
    usefulness: str | None,
    source_gap: list[str] | None,
    decision_gap: list[str] | None,
    stocks_only: bool,
    scan_limit: int,
    scan_offset: int,
    telemetry_limit: int,
) -> DashboardFilters:
    filters = DashboardFilters(
        ticker=page_request.detail_ticker or ticker,
        available_at=available_at,
        alert_status=alert_status,
        alert_route=alert_route,
        priced_in_status=priced_in_status,
        priced_in_usefulness=usefulness,
        priced_in_source_gap=source_gap,
        priced_in_decision_gap=decision_gap,
        priced_in_stocks_only=stocks_only,
        priced_in_limit=scan_limit,
        priced_in_offset=0 if page_request.detail_ticker else scan_offset,
        telemetry_limit=telemetry_limit,
    )
    return dashboard_filters_for_page(filters, page_request.snapshot_page)


@router.get("/snapshot", dependencies=[Depends(require_role(Role.VIEWER))])
def snapshot(
    page: str = "overview",
    ticker: Annotated[str | None, Query(min_length=1, max_length=12)] = None,
    available_at: datetime | None = None,
    alert_status: str | None = None,
    alert_route: str | None = None,
    priced_in_status: str = "all",
    usefulness: str | None = None,
    source_gap: Annotated[list[str] | None, Query()] = None,
    decision_gap: Annotated[list[str] | None, Query()] = None,
    stocks_only: bool = False,
    scan_limit: Annotated[int, Query(ge=1, le=200)] = 50,
    scan_offset: Annotated[int, Query(ge=0)] = 0,
    telemetry_limit: Annotated[int, Query(ge=1, le=200)] = 8,
    fast: bool = True,
) -> dict[str, object]:
    page_request = _dashboard_page_request(page)
    filters = _dashboard_filters_from_values(
        page_request,
        ticker=ticker,
        available_at=available_at,
        alert_status=alert_status,
        alert_route=alert_route,
        priced_in_status=priced_in_status,
        usefulness=usefulness,
        source_gap=source_gap,
        decision_gap=decision_gap,
        stocks_only=stocks_only,
        scan_limit=scan_limit,
        scan_offset=scan_offset,
        telemetry_limit=telemetry_limit,
    )
    payload = dashboard_snapshot_payload(
        engine=_engine(),
        config=AppConfig.from_env(),
        dotenv_loaded=True,
        filters=filters,
        fast_view=fast,
    )
    payload["selected_page"] = page_request.selected_page
    return payload


@router.post("/command", dependencies=[Depends(require_role(Role.ANALYST))])
def command(request: DashboardCommandRequest) -> dict[str, object]:
    page_request = _dashboard_page_request(request.page)
    filters = _dashboard_filters_from_values(
        page_request,
        ticker=request.ticker,
        available_at=request.available_at,
        alert_status=request.alert_status,
        alert_route=request.alert_route,
        priced_in_status=request.priced_in_status,
        usefulness=request.usefulness,
        source_gap=request.source_gap,
        decision_gap=request.decision_gap,
        stocks_only=request.stocks_only,
        scan_limit=request.scan_limit,
        scan_offset=request.scan_offset,
        telemetry_limit=request.telemetry_limit,
    )
    engine = _engine()
    config = AppConfig.from_env()
    payload = dashboard_snapshot_payload(
        engine=engine,
        config=config,
        dotenv_loaded=True,
        filters=filters,
        fast_view=True,
    )
    payload["selected_page"] = page_request.selected_page
    update = apply_dashboard_command(
        request.command,
        payload,
        page_request.selected_page,
        filters,
        engine=engine,
        config=config,
    )
    return {
        "schema_version": "dashboard-command-result-v1",
        "command": request.command,
        "page": update.page,
        "exit_requested": update.exit_requested,
        "message": update.message,
        "filters": asdict(update.filters),
        "snapshot": payload,
    }


__all__ = ["router"]

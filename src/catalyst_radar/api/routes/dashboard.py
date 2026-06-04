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
        "label": "1 Inbox",
        "shortcut": "1",
        "description": "Inbox, status, first blocker, and next safe action.",
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
        "key": "help",
        "label": "? Help",
        "shortcut": "?",
        "description": "Keyboard, automation, and command reference.",
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


@router.get("/manifest", dependencies=[Depends(require_role(Role.VIEWER))])
def manifest() -> dict[str, object]:
    return {
        "schema_version": "dashboard-ui-manifest-v1",
        "external_calls_made": 0,
        "surfaces": {
            "default": "tauri_desktop",
            "terminal": "rust_tui",
            "legacy": "python_textual",
        },
        "pages": _desktop_pages(),
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
                "automation-state",
                "attention-queue",
                "next-safe-action",
                "snapshot-json",
                "snapshot-json-output",
            ],
            "keyboard_shortcuts": [
                "0-9 jump to numbered workflow pages",
                "Ctrl+A opens Agent",
                "Type themes or validation to open evidence pages",
                "V opens Costs",
                "F opens Features",
                "? opens Help",
                "Arrow keys move through workflow pages",
                "F5 refreshes the local snapshot",
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
                    "batch SOURCE opens an Ops source plan; batch SOURCE all "
                    "and batch SOURCE execute N show PowerShell boundaries"
                ),
                (
                    "run opens Safe Run; run execute starts the guarded "
                    "radar-run API/CLI backend path"
                ),
                (
                    "action, trigger, ticket, feedback, ledger, and outcome "
                    "commands use the guarded dashboard backend for local "
                    "DB-only operations"
                ),
                (
                    "agent, bars, options, and cik/sec planning commands use "
                    "the guarded dashboard backend for preview/status output; "
                    "execute and confirm variants stay external boundaries"
                ),
                "q, quit, or exit closes the native desktop window",
                (
                    "Full catalyst-radar commands show a PowerShell boundary "
                    "instead of executing in-app"
                ),
            ],
            "native_window_title": "MarketRadar Command Center",
            "native_executable": "target\\release\\radar-desktop.exe",
            "computer_use_steps": [
                {
                    "step": "launch",
                    "action": (
                        "Launch the app by executable path through Computer Use, "
                        "then select the returned window object."
                    ),
                    "target": "target\\release\\radar-desktop.exe",
                    "expected": "A native window titled MarketRadar Command Center is targetable.",
                },
                {
                    "step": "capture",
                    "action": "Capture screenshot and accessibility text for the selected window.",
                    "target": "MarketRadar Command Center",
                    "expected": (
                        "The window exposes MarketRadar workflow tabs, dashboard-page, "
                        "command-input, automation-state, next-safe-action, and "
                        "page=<PAGE>, nav=<WORKFLOW_PAGE>, and provider_calls=0."
                    ),
                },
                {
                    "step": "focus-command",
                    "action": "Press Escape in the dashboard window.",
                    "target": "command-input",
                    "expected": (
                        "The command box receives focus and command-status reports "
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
                    "step": "page-command",
                    "action": "Type ready and press Return.",
                    "target": "command-input",
                    "expected": (
                        "dashboard-page reports page=review and the selected tab is "
                        "Review."
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
                        "The native MarketRadar Command Center window closes "
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
                    "Local broker, feedback, value-ledger, and outcome commands "
                    "may write the local DB through the guarded dashboard "
                    "backend, but must not make provider, OpenAI, broker, "
                    "order, or external calls unless the command explicitly "
                    "reports an external-call budget."
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
                    "The exact selected page, parent nav page, and provider-call "
                    "count are exposed through data-testid=automation-state."
                ),
                (
                    "The dashboard main region exposes data-current-page and "
                    "data-current-nav-page for dynamic detail pages."
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
                    "Local broker, feedback, value-ledger, and outcome commands "
                    "use the guarded dashboard backend; source-batch execute "
                    "and provider execute/confirm commands remain external "
                    "PowerShell boundaries; provider preview/status commands "
                    "use the guarded dashboard backend; run execute uses the "
                    "guarded radar-run API/CLI backend path."
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

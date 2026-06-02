from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Query

from catalyst_radar.core.config import AppConfig
from catalyst_radar.dashboard.tui import (
    DashboardFilters,
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
                "Command box accepts safe page, filter, refresh, help, and JSON commands",
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
                        "provider_calls=0."
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
                        "page=alert:<ID>, the detail panel is visible, and "
                        "provider_calls=0."
                    ),
                },
                {
                    "step": "guarded-command",
                    "action": "Type batch catalyst_events and press Return.",
                    "target": "command-input",
                    "expected": (
                        "dashboard-page reports page=ops, command-status shows an "
                        "external command boundary, and provider_calls=0."
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
            ],
            "zero_call_assertions": [
                (
                    "Dashboard browsing, command-box navigation, filtering, copy, "
                    "and raw JSON inspection must leave provider_calls=0."
                ),
                (
                    "Execute-class commands must show the external PowerShell command "
                    "boundary instead of running provider, OpenAI, broker, or DB-write "
                    "actions from the desktop command box."
                ),
                (
                    "Full catalyst-radar commands typed into the desktop command box "
                    "must stay external and leave provider_calls=0."
                ),
                (
                    "Clicking or pressing Enter on queue rows must open local "
                    "candidate/alert detail without provider calls."
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
    return DashboardPageRequest(snapshot_page=raw_page, selected_page=raw_page)


def _detail_page_suffix(page: str, prefix: str) -> str | None:
    if page[: len(prefix)].lower() != prefix:
        return None
    suffix = page[len(prefix) :].strip()
    return suffix or None


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
    filters = dashboard_filters_for_page(filters, page_request.snapshot_page)
    payload = dashboard_snapshot_payload(
        engine=_engine(),
        config=AppConfig.from_env(),
        dotenv_loaded=True,
        filters=filters,
        fast_view=fast,
    )
    payload["selected_page"] = page_request.selected_page
    return payload


__all__ = ["router"]

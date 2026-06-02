from __future__ import annotations

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

DASHBOARD_DESKTOP_PAGES: tuple[dict[str, str], ...] = (
    {"key": "tutorial", "label": "0 Start", "shortcut": "0"},
    {"key": "overview", "label": "1 Inbox", "shortcut": "1"},
    {"key": "readiness", "label": "2 Evidence Gaps", "shortcut": "2"},
    {"key": "run", "label": "3 Safe Run", "shortcut": "3"},
    {"key": "candidates", "label": "4 Candidate Review", "shortcut": "4"},
    {"key": "review", "label": "Review", "shortcut": "D"},
    {"key": "alerts", "label": "5 Alerts", "shortcut": "5"},
    {"key": "ipo", "label": "6 IPO/S-1", "shortcut": "6"},
    {"key": "broker", "label": "7 Broker", "shortcut": "7"},
    {"key": "ops", "label": "8 Ops", "shortcut": "8"},
    {"key": "telemetry", "label": "9 Telemetry", "shortcut": "9"},
    {"key": "agent", "label": "Ctrl+A Agent", "shortcut": "Ctrl+A"},
    {"key": "features", "label": "F Features", "shortcut": "F"},
    {"key": "help", "label": "? Help", "shortcut": "?"},
)


def _engine():
    engine = engine_from_url(AppConfig.from_env().database_url)
    create_schema(engine)
    return engine



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
        "pages": list(DASHBOARD_DESKTOP_PAGES),
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
                "F opens Features",
                "? opens Help",
                "Arrow keys move through workflow pages",
                "F5 refreshes the local snapshot",
                "Esc focuses the command box",
                "Command box accepts safe page, filter, refresh, help, and JSON commands",
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
                    "step": "guarded-command",
                    "action": "Type batch catalyst_events and press Return.",
                    "target": "command-input",
                    "expected": (
                        "dashboard-page reports page=ops, command-status shows an "
                        "external command boundary, and provider_calls=0."
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
            ],
        },
        "data_contract": {
            "snapshot_endpoint": "/api/dashboard/snapshot?fast=true",
            "snapshot_command": "catalyst-radar dashboard-snapshot --json --fast",
            "provider_calls_for_browsing": 0,
        },
    }

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
    filters = DashboardFilters(
        ticker=ticker,
        available_at=available_at,
        alert_status=alert_status,
        alert_route=alert_route,
        priced_in_status=priced_in_status,
        priced_in_usefulness=usefulness,
        priced_in_source_gap=source_gap,
        priced_in_decision_gap=decision_gap,
        priced_in_stocks_only=stocks_only,
        priced_in_limit=scan_limit,
        priced_in_offset=scan_offset,
        telemetry_limit=telemetry_limit,
    )
    filters = dashboard_filters_for_page(filters, page)
    payload = dashboard_snapshot_payload(
        engine=_engine(),
        config=AppConfig.from_env(),
        dotenv_loaded=True,
        filters=filters,
        fast_view=fast,
    )
    payload["selected_page"] = page
    return payload


__all__ = ["router"]

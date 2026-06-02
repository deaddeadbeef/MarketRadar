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
                "attention-queue",
                "next-safe-action",
                "snapshot-json",
            ],
            "keyboard_shortcuts": [
                "0-9 jump to numbered workflow pages",
                "Ctrl+A opens Agent",
                "F opens Features",
                "? opens Help",
                "Arrow keys move through workflow pages",
                "F5 refreshes the local snapshot",
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

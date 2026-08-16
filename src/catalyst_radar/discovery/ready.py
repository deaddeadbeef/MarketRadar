"""Product ship gate for event-first discovery."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from pathlib import Path

from sqlalchemy.engine import Engine

from catalyst_radar.discovery.brief import (
    DISCOVERY_BARS_NEXT_COMMAND,
    FRESHNESS_STALE_HOURS,
    build_discovery_brief,
    default_events_path,
)

READY_SCHEMA = "discovery-ready-v1"
JOIN_TARGET_PCT = 50.0
TOP_N = 20


def build_discovery_readiness(
    *,
    events_path: str | Path | None = None,
    engine: Engine | None = None,
    now: datetime | None = None,
    theme_peers_path: str | Path | None = Path("config/theme_peers.yaml"),
) -> dict[str, object]:
    path = Path(events_path) if events_path else default_events_path()
    if not path.is_file():
        return {
            "schema_version": READY_SCHEMA,
            "ready": False,
            "status": "missing_events",
            "first_blocker": "missing_events",
            "canonical_next_action": (
                "Install a fresh world-events-v1 JSON into data/local/world_events.json."
            ),
            "canonical_next_command": (
                "powershell -ExecutionPolicy Bypass -File "
                "scripts/refresh-world-events.ps1 "
                "-EventsPath <fresh-world-events.json> -Execute"
            ),
            "investment_advice": False,
            "external_calls_made": 0,
            "db_writes_made": 0,
        }

    brief = build_discovery_brief(
        events_path=path,
        theme_peers_path=theme_peers_path,
        engine=engine,
        limit=max(TOP_N, 25),
        now=now,
    )
    discoveries = [
        row
        for row in brief.get("discoveries") or []
        if isinstance(row, Mapping)
    ][:TOP_N]
    joined = sum(1 for row in discoveries if row.get("join_status") == "joined")
    join_pct = round((100.0 * joined / len(discoveries)) if discoveries else 0.0, 1)
    age_hours = float(brief.get("events_age_hours") or 0.0)
    freshness_ok = (
        str(brief.get("freshness_status")) == "fresh"
        and age_hours <= FRESHNESS_STALE_HOURS
    )
    join_ok = bool(discoveries) and join_pct >= JOIN_TARGET_PCT
    advice_ok = brief.get("investment_advice") is False
    ready = bool(freshness_ok and join_ok and advice_ok)

    if not path.is_file():
        blocker = "missing_events"
    elif not freshness_ok:
        blocker = "stale_events"
    elif not discoveries:
        blocker = "no_discoveries"
    elif not join_ok:
        blocker = "event_join_coverage"
    elif not advice_ok:
        blocker = "investment_advice_flag"
    else:
        blocker = None

    next_action = str(brief.get("next_action") or "")
    next_command = str(brief.get("next_command") or "")
    if blocker == "event_join_coverage":
        next_command = DISCOVERY_BARS_NEXT_COMMAND
    if ready:
        next_action = (
            "Review the top-20 event-time leads, open a case, and label from Proof."
        )
        next_command = f"catalyst-radar discovery-brief --events {path} --json"

    return {
        "schema_version": READY_SCHEMA,
        "ready": ready,
        "status": "ready" if ready else "blocked",
        "first_blocker": blocker,
        "freshness_ok": freshness_ok,
        "freshness_status": brief.get("freshness_status"),
        "events_age_hours": age_hours,
        "freshness_limit_hours": FRESHNESS_STALE_HOURS,
        "join_coverage_pct": join_pct,
        "join_target_pct": JOIN_TARGET_PCT,
        "join_target_met": join_ok,
        "joined": joined,
        "top_n": len(discoveries),
        "discovery_count": int(brief.get("discovery_count") or 0),
        "event_count": int(brief.get("event_count") or 0),
        "investment_advice": False,
        "decision_support_only": True,
        "canonical_next_action": next_action,
        "canonical_next_command": next_command,
        "events_path": str(path),
        "goal_status": brief.get("goal_status"),
        "external_calls_made": 0,
        "db_writes_made": 0,
    }

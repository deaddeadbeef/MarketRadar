"""Lightweight zero-call snapshot for World Events desktop browsing."""
from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from pathlib import Path

# Ensure repo src is importable when launched from radar-desktop.
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from catalyst_radar.discovery.brief import build_discovery_brief, default_events_path
from catalyst_radar.discovery.case_file import build_discovery_case_file


def main() -> int:
    events_path = default_events_path()
    if not Path(events_path).is_file():
        sample = ROOT / "data" / "sample" / "world_events.json"
        events_path = sample if sample.is_file() else events_path

    try:
        brief = build_discovery_brief(
            events_path=events_path,
            engine=None,
            limit=25,
        )
        brief["status"] = "ready"
        focus = ""
        discoveries = brief.get("discoveries") or []
        if discoveries and isinstance(discoveries[0], dict):
            focus = str(discoveries[0].get("ticker") or "")
        if focus:
            brief["case_file"] = build_discovery_case_file(
                ticker=focus,
                events_path=events_path,
                engine=None,
            )
    except Exception as exc:
        brief = {
            "schema_version": "discovery-brief-v1",
            "status": "error",
            "error": str(exc),
            "events": [],
            "discoveries": [],
            "headline": f"Discovery snapshot failed: {exc}",
            "external_calls_made": 0,
            "db_writes_made": 0,
            "investment_advice": False,
        }

    now = datetime.now(tz=UTC).isoformat()
    payload = {
        "schema_version": "dashboard-cli-snapshot-v1",
        "snapshot_mode": "discovery_fast",
        "generated_at": now,
        "status": "discovery_ready",
        "first_blocker": None,
        "next_action": brief.get("next_action")
        or "Review World Events discovery queue as research-only leads.",
        "next_command": brief.get("next_command")
        or f"catalyst-radar discovery-brief --events {events_path} --json",
        "canonical_next_action": brief.get("canonical_next_action") or brief.get("next_action"),
        "canonical_next_command": brief.get("canonical_next_command")
        or brief.get("next_command"),
        "external_calls_made": 0,
        "event_discovery": brief,
        "candidates": {"count": 0, "rows": []},
        "alerts": {"count": 0, "rows": []},
        "themes": {"count": 0, "rows": []},
        "investment_advice": False,
        "can_make_investment_decision": False,
        "decision_support_only": True,
    }
    print(json.dumps(payload, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

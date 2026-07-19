"""Lightweight zero-call snapshot for World Events desktop browsing.

The desktop client appends CLI-style flags to whatever snapshot command is
configured (e.g. --page world-events --ticker MU --scan-limit 50). Accept and
honor the useful ones; ignore the rest so the process never dies on unknown args.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

# Ensure repo src is importable when launched from radar-desktop.
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from catalyst_radar.discovery.brief import build_discovery_brief, default_events_path
from catalyst_radar.discovery.case_file import build_discovery_case_file
from catalyst_radar.security.secrets import load_app_dotenv


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument("--page", default="world-events")
    parser.add_argument("--ticker")
    parser.add_argument("--events", type=Path)
    parser.add_argument("--available-at")
    parser.add_argument("--alert-status")
    parser.add_argument("--alert-route")
    parser.add_argument("--priced-in-status")
    parser.add_argument("--usefulness")
    parser.add_argument("--source-gap", action="append", default=[])
    parser.add_argument("--decision-gap", action="append", default=[])
    parser.add_argument("--stocks-only", action="store_true")
    parser.add_argument("--scan-limit", type=int, default=25)
    parser.add_argument("--scan-offset", type=int, default=0)
    parser.add_argument("--telemetry-limit", type=int, default=8)
    parser.add_argument("--database-url")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--fast", action="store_true")
    # Desktop may pass extra unknown flags as the client evolves.
    args, _unknown = parser.parse_known_args(argv)
    return args


def _local_engine(database_url: str | None = None):
    """Best-effort read-only local DB for priced-in reaction join (no external calls)."""
    try:
        from catalyst_radar.core.config import AppConfig
        from catalyst_radar.storage.db import create_schema, engine_from_url

        # Prefer .env.local (same as CLI) so desktop joins the live local DB.
        load_app_dotenv()
        config = AppConfig.from_env()
        url = (database_url or config.database_url or "").strip()
        if not url:
            return None
        engine = engine_from_url(url)
        create_schema(engine)
        return engine
    except Exception:
        return None


def main(argv: list[str] | None = None) -> int:
    # Load operator env before resolving default events/database paths.
    load_app_dotenv()
    args = parse_args(argv)
    events_path = args.events or default_events_path()
    if not Path(events_path).is_file():
        sample = ROOT / "data" / "sample" / "world_events.json"
        events_path = sample if sample.is_file() else events_path

    limit = max(1, min(int(args.scan_limit or 25), 50))
    focus = (args.ticker or "").strip().upper()
    engine = _local_engine(args.database_url)

    try:
        brief = build_discovery_brief(
            events_path=events_path,
            engine=engine,
            limit=limit,
        )
        brief["status"] = "ready"
        discoveries = brief.get("discoveries") or []
        if not focus and discoveries and isinstance(discoveries[0], dict):
            focus = str(discoveries[0].get("ticker") or "").strip().upper()
        if focus:
            brief["case_file"] = build_discovery_case_file(
                ticker=focus,
                events_path=events_path,
                engine=engine,
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
        "selected_page": args.page or "world-events",
    }
    # Print JSON only — no logging on stdout (desktop parses the whole stream).
    sys.stdout.write(json.dumps(payload, sort_keys=True, default=str))
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

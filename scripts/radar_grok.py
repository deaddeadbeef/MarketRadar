"""Thin Grok Build helpers. Zero hidden provider calls except bars + confirm."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


def _events_path() -> Path:
    local = ROOT / "data" / "local" / "world_events.json"
    return local if local.is_file() else ROOT / "data" / "sample" / "world_events.json"


def cmd_status() -> int:
    path = ROOT / "data" / "local" / "world_events.json"
    payload = {
        "schema_version": "radar-grok-status-v1",
        "investment_advice": False,
        "events_path": str(path),
        "events_present": path.is_file(),
        "inbox": str(ROOT / "data" / "local" / "inbox"),
    }
    if path.is_file():
        raw = json.loads(path.read_text(encoding="utf-8"))
        events = raw.get("events") or []
        payload["event_count"] = len(events)
        payload["generated_at"] = raw.get("generated_at")
        payload["source"] = raw.get("source")
        payload["titles"] = [str(e.get("title") or "") for e in events[:8] if isinstance(e, dict)]
    print(json.dumps(payload, indent=2, default=str))
    return 0


def cmd_brief() -> int:
    from catalyst_radar.discovery.brief import build_discovery_brief
    from catalyst_radar.discovery.ux import apply_novice_ux
    from catalyst_radar.security.secrets import load_app_dotenv

    load_app_dotenv()
    engine = None
    try:
        from catalyst_radar.core.config import AppConfig
        from catalyst_radar.storage.db import engine_from_url

        url = (AppConfig.from_env().database_url or "").strip()
        if url:
            engine = engine_from_url(url)
    except Exception:
        engine = None
    brief = build_discovery_brief(events_path=_events_path(), engine=engine, limit=25)
    novice = apply_novice_ux(brief)
    stories = (novice.get("novice") or {}).get("events") or []
    out = {
        "schema_version": "radar-grok-brief-v1",
        "investment_advice": False,
        "headline": novice.get("headline"),
        "next_action": novice.get("next_action"),
        "freshness_status": novice.get("freshness_status"),
        "story_count": len(stories),
        "stories": [
            {
                "title": row.get("title"),
                "mood": row.get("mood"),
                "names": row.get("names"),
            }
            for row in stories
            if isinstance(row, dict)
        ],
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "external_calls_made": 0,
    }
    print(json.dumps(out, indent=2, default=str))
    return 0


def cmd_convert(posts: Path, execute: bool) -> int:
    from catalyst_radar.discovery.from_posts import convert_posts_file

    dest = ROOT / "data" / "local" / "world_events.json"
    payload = convert_posts_file(
        posts_path=posts,
        destination=dest,
        execute=execute,
    )
    print(json.dumps(payload, indent=2, default=str))
    return 0 if payload.get("status") in {"preview", "executed"} else 1


def cmd_ready() -> int:
    from catalyst_radar.discovery.ready import build_discovery_readiness
    from catalyst_radar.security.secrets import load_app_dotenv

    load_app_dotenv()
    engine = None
    try:
        from catalyst_radar.core.config import AppConfig
        from catalyst_radar.storage.db import engine_from_url

        url = (AppConfig.from_env().database_url or "").strip()
        if url:
            engine = engine_from_url(url)
    except Exception:
        engine = None
    payload = build_discovery_readiness(events_path=_events_path(), engine=engine)
    print(json.dumps(payload, indent=2, default=str))
    return 0 if payload.get("ready") else 1


def cmd_bars(*, confirm: bool, execute: bool) -> int:
    from catalyst_radar.core.config import AppConfig
    from catalyst_radar.discovery.polygon_bars import (
        default_bar_window,
        mapped_tickers_from_events,
        write_polygon_bars,
    )
    from catalyst_radar.security.secrets import load_app_dotenv
    from catalyst_radar.storage.db import engine_from_url

    load_app_dotenv()
    config = AppConfig.from_env()
    engine = engine_from_url(config.database_url)
    tickers = mapped_tickers_from_events(str(_events_path()))
    start, end = default_bar_window()
    payload = write_polygon_bars(
        engine=engine,
        api_key=str(config.polygon_api_key or ""),
        tickers=tickers,
        start=start,
        end=end,
        confirm_external_call=confirm,
        execute=execute,
    )
    payload.pop("bars", None)
    print(json.dumps(payload, indent=2, default=str))
    return 0 if payload.get("status") != "error" else 2


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("status")
    sub.add_parser("brief")
    sub.add_parser("ready")
    conv = sub.add_parser("convert")
    conv.add_argument("--posts", type=Path, required=True)
    conv.add_argument("--execute", action="store_true")
    bars = sub.add_parser("bars")
    bars.add_argument("--confirm-external-call", action="store_true")
    bars.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    if args.cmd == "status":
        return cmd_status()
    if args.cmd == "brief":
        return cmd_brief()
    if args.cmd == "ready":
        return cmd_ready()
    if args.cmd == "convert":
        return cmd_convert(args.posts, bool(args.execute))
    if args.cmd == "bars":
        return cmd_bars(
            confirm=bool(args.confirm_external_call),
            execute=bool(args.execute),
        )
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

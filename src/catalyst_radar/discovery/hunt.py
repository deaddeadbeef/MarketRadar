"""Headless hunt status. Mining X is the Grok skill, not this CLI."""

from __future__ import annotations

from datetime import date
from pathlib import Path

HUNT_SCHEMA = "discovery-hunt-v1"


def build_hunt_status(
    *,
    root: Path | str | None = None,
    today: date | None = None,
) -> dict[str, object]:
    """Return JSON for `catalyst-radar hunt`. Does not search X or call providers."""
    base = Path(root) if root is not None else Path(".")
    day = (today or date.today()).isoformat()
    inbox = base / "data" / "local" / "inbox"
    posts = inbox / f"x_posts_{day}.json"
    events = base / "data" / "local" / "world_events.json"
    posts_present = posts.is_file()
    return {
        "schema_version": HUNT_SCHEMA,
        "investment_advice": False,
        "decision_support_only": True,
        "can_make_investment_decision": False,
        "status": "skill_required",
        "receive_surface": "cli",
        "message": (
            "Hunt mines X via the Grok skill. This CLI cannot search X. "
            'Run grok -p "/market-radar hunt", then convert and brief.'
        ),
        "skill": "/market-radar hunt",
        "headless_command": 'grok -p "/market-radar hunt"',
        "inbox_dir": str(inbox),
        "today_posts": str(posts),
        "today_posts_present": posts_present,
        "events_path": str(events),
        "events_present": events.is_file(),
        "next_command": (
            f"catalyst-radar convert --posts {posts} --execute"
            if posts_present
            else 'grok -p "/market-radar hunt"'
        ),
        "external_calls_made": 0,
        "db_writes_made": 0,
    }


__all__ = ["HUNT_SCHEMA", "build_hunt_status"]

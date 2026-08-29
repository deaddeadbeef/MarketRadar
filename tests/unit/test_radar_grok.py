from __future__ import annotations

import json
import sys
from io import StringIO
from pathlib import Path

import pytest

pytestmark = pytest.mark.discovery

ROOT = Path(__file__).resolve().parents[2]


def test_grok_radar_skill_and_commands_exist() -> None:
    skill = ROOT / ".grok" / "skills" / "market-radar" / "SKILL.md"
    assert skill.is_file()
    text = skill.read_text(encoding="utf-8")
    assert "name: market-radar" in text
    assert "argument-hint" in text
    assert "/market-radar hunt" in text
    assert "grok -p" in text
    assert "tell the user to press" not in text.casefold()
    assert "World Events is the **receive surface**" not in text
    assert "receive surface is the CLI" in text
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    assert "CLI discovery is primary" in readme
    assert "Open the desktop app on **World Events**" not in readme
    agents = (ROOT / "AGENTS.md").read_text(encoding="utf-8")
    assert "press R" not in agents
    assert "human receive surface is the CLI" in agents
    rules = (ROOT / ".grok" / "rules" / "market-radar.md").read_text(encoding="utf-8")
    assert "Press **R**" not in rules
    assert "receives** that briefing from the CLI" in rules or "CLI" in rules
    assert (ROOT / ".grok" / "skills" / "market-radar" / "references" / "hunt.md").is_file()
    assert (ROOT / ".grok" / "rules" / "market-radar.md").is_file()
    assert not (ROOT / ".grok" / "skills" / "radar").exists()
    commands = ROOT / ".grok" / "commands"
    if commands.is_dir():
        leftovers = {p.name for p in commands.glob("*.md")}
        assert leftovers == set()


def test_hunt_playbook_calendars_first_distinct_event_cap() -> None:
    """Contract: hunt.md is calendars-first; 16 = distinct event_id, not post count."""
    hunt = (
        ROOT / ".grok" / "skills" / "market-radar" / "references" / "hunt.md"
    ).read_text(encoding="utf-8")
    lower = hunt.casefold()
    assert "official calendars" in lower or "calendars / ir" in lower
    assert "distinct" in lower and "event_id" in lower
    assert "16" in hunt
    assert "never pad" in lower
    assert "url host" in lower or "source class comes from the **url host**" in lower
    # X is last, not the lead search instruction
    assert lower.index("official") < lower.index("x") or "then x" in lower or "x — last" in lower or "x — last" in hunt.casefold()
    assert "do not spend a story slot on an x mirror" in lower or "clustering" in lower
    assert "do **not** drop a still-open dated primary" in lower or "still-open dated primary" in lower

    skill = (ROOT / ".grok" / "skills" / "market-radar" / "SKILL.md").read_text(
        encoding="utf-8"
    )
    assert "calendars" in skill.casefold() or "official" in skill.casefold()
    pending = (ROOT / "docs" / "missions" / "pending-binaries.md").read_text(
        encoding="utf-8"
    )
    assert "distinct" in pending.casefold()
    assert "calendars" in pending.casefold() or "regulator" in pending.casefold()


def test_radar_grok_status_json() -> None:
    sys.path.insert(0, str(ROOT / "scripts"))
    import radar_grok  # type: ignore[import-not-found]

    buf = StringIO()
    old = sys.stdout
    sys.stdout = buf
    try:
        code = radar_grok.cmd_status()
    finally:
        sys.stdout = old
    assert code == 0
    payload = json.loads(buf.getvalue())
    assert payload["schema_version"] == "radar-grok-status-v1"
    assert payload["investment_advice"] is False


def test_radar_grok_hunt_json() -> None:
    sys.path.insert(0, str(ROOT / "scripts"))
    import radar_grok  # type: ignore[import-not-found]

    buf = StringIO()
    old = sys.stdout
    sys.stdout = buf
    try:
        code = radar_grok.cmd_hunt()
    finally:
        sys.stdout = old
    assert code == 0
    payload = json.loads(buf.getvalue())
    assert payload["schema_version"] == "discovery-hunt-v1"
    assert payload["investment_advice"] is False
    assert payload["receive_surface"] == "cli"

from __future__ import annotations

import json
import sys
from io import StringIO
from pathlib import Path

import pytest

pytestmark = pytest.mark.discovery

ROOT = Path(__file__).resolve().parents[2]


def test_grok_radar_skill_and_commands_exist() -> None:
    skill = ROOT / ".grok" / "skills" / "radar" / "SKILL.md"
    assert skill.is_file()
    text = skill.read_text(encoding="utf-8")
    assert "name: radar" in text
    assert "/radar-hunt" in text
    assert (ROOT / ".grok" / "skills" / "radar" / "references" / "hunt.md").is_file()
    assert (ROOT / ".grok" / "rules" / "radar.md").is_file()
    for name in ("radar.md", "radar-hunt.md", "radar-brief.md", "radar-ready.md"):
        assert (ROOT / ".grok" / "commands" / name).is_file()


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

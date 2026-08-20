from __future__ import annotations

import json
import subprocess
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
    assert "/market-radar open" in text
    assert "install-market-radar-skill.ps1" in text
    assert (ROOT / ".grok" / "skills" / "market-radar" / "references" / "hunt.md").is_file()
    assert (ROOT / ".grok" / "rules" / "market-radar.md").is_file()
    assert (ROOT / "scripts" / "open-market-radar.ps1").is_file()
    assert (ROOT / "scripts" / "install-market-radar-skill.ps1").is_file()
    assert not (ROOT / ".grok" / "skills" / "radar").exists()
    commands = ROOT / ".grok" / "commands"
    if commands.is_dir():
        leftovers = {p.name for p in commands.glob("*.md")}
        assert leftovers == set()


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


def test_open_market_radar_script_is_idempotent_and_job_safe() -> None:
    text = (ROOT / "scripts" / "open-market-radar.ps1").read_text(encoding="utf-8")
    assert "status=already_running" in text
    assert "Win32_Process" in text
    assert "--page world-events" in text
    radar = (ROOT / "scripts" / "radar-grok.ps1").read_text(encoding="utf-8")
    assert '"open"' in radar


def test_install_market_radar_skill_copies_into_grok_home(
    tmp_path: Path,
) -> None:
    grok_home = tmp_path / "grok"
    completed = subprocess.run(
        [
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(ROOT / "scripts" / "install-market-radar-skill.ps1"),
            "-RepoRoot",
            str(ROOT),
            "-GrokHome",
            str(grok_home),
            "-LiveInstall",
            str(tmp_path / "missing-live"),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
    payload = json.loads(completed.stdout.strip().splitlines()[-1])
    dest = grok_home / "skills" / "market-radar"
    assert payload["status"] == "installed"
    assert (dest / "SKILL.md").is_file()
    assert (dest / "references" / "hunt.md").is_file()
    assert (dest / "scripts" / "open-market-radar.ps1").is_file()
    installed = (dest / "SKILL.md").read_text(encoding="utf-8")
    assert "/market-radar open" in installed

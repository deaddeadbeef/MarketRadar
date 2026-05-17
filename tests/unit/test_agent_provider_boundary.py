from __future__ import annotations

import tomllib
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_runtime_agent_stack_has_no_copilot_dependency_or_source_reference() -> None:
    pyproject = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text())
    dependencies = pyproject["project"]["dependencies"]
    dependency_text = "\n".join(dependencies).lower()

    assert "copilot" not in dependency_text
    assert "@github" not in dependency_text

    runtime_source = "\n".join(
        path.read_text(encoding="utf-8")
        for path in (PROJECT_ROOT / "src" / "catalyst_radar").rglob("*.py")
    ).lower()

    assert "copilot" not in runtime_source
    assert "@github/copilot" not in runtime_source
    assert "github.copilot" not in runtime_source


def test_runtime_agent_stack_uses_openai_responses_client() -> None:
    source = (
        PROJECT_ROOT / "src" / "catalyst_radar" / "agents" / "openai_client.py"
    ).read_text(encoding="utf-8")

    assert "from openai import OpenAI" in source
    assert ".responses.create(" in source

"""Lock: World Events HTML does not mount discovery-proof dollars."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.discovery

FRONTEND_APP = Path("apps/radar-desktop/frontend/app.js")
FUNCTION_RE = re.compile(r"^function ([A-Za-z_][A-Za-z0-9_]*)\(", re.MULTILINE)
CALL_RE = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\(")
PROOF_TOKENS = ("discovery-proof", "claimable_value_usd")


def _function_bodies(source: str) -> dict[str, str]:
    matches = list(FUNCTION_RE.finditer(source))
    bodies: dict[str, str] = {}
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(source)
        bodies[match.group(1)] = source[match.start() : end]
    return bodies


def _reachable_renderers(bodies: dict[str, str], root: str) -> dict[str, str]:
    seen: dict[str, str] = {}
    stack = [root]
    while stack:
        name = stack.pop()
        if name in seen or name not in bodies:
            continue
        body = bodies[name]
        seen[name] = body
        for callee in CALL_RE.findall(body):
            if callee.startswith("render") and callee in bodies and callee not in seen:
                stack.append(callee)
    return seen


def test_world_events_html_has_no_discovery_proof_dollars() -> None:
    source = FRONTEND_APP.read_text(encoding="utf-8")
    assert "'world-events': renderWorldEvents" in source

    bodies = _function_bodies(source)
    assert "renderWorldEvents" in bodies
    path = _reachable_renderers(bodies, "renderWorldEvents")
    combined = "\n".join(path.values())

    assert "renderDiscoveryProof" not in path
    assert "renderDiscoveryProof(" not in combined
    for token in PROOF_TOKENS:
        assert token not in combined

"""D3 guard: discovery package must not depend on deprecated product surfaces."""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.discovery

FORBIDDEN = (
    "catalyst_radar.brokers",
    "catalyst_radar.trading",
    "catalyst_radar.ipo",
    "catalyst_radar.alerts",
    "catalyst_radar.portfolio",
    "catalyst_radar.decision_cards",
    "catalyst_radar.universe",
    "catalyst_radar.dashboard.data",
    "catalyst_radar.textint",
    "load_candidate_rows",
)


def test_discovery_modules_avoid_deprecated_package_imports() -> None:
    root = Path("src/catalyst_radar/discovery")
    offenders: list[str] = []
    for path in root.glob("*.py"):
        text = path.read_text(encoding="utf-8")
        for token in FORBIDDEN:
            if token in text:
                offenders.append(f"{path.name}:{token}")
    assert offenders == []

"""Shared pytest fixtures for MarketRadar.

Legacy workbench CLI commands are hard-blocked unless
CATALYST_ENABLE_LEGACY_WORKBENCH is truthy. Integration and unit tests that
still exercise deprecated CLI surfaces (agent-brief, alerts, broker, etc.)
need the flag set so they keep validating quarantined code paths until D4/D5
deletion. Product-path tests that assert the default lockdown must explicitly
clear the env var with monkeypatch.delenv.
"""

from __future__ import annotations

import os

import pytest

from catalyst_radar.deprecation import LEGACY_WORKBENCH_ENV


@pytest.fixture(autouse=True)
def _enable_legacy_workbench_for_tests(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep legacy CLI/UI reachable in the full test suite by default.

    Why: D3 hard-blocks deprecated CLI commands when the env flag is off.
    Most existing integration tests still call those commands. The product
    lockdown is covered by dedicated tests that delenv the flag.
    """
    monkeypatch.setenv(LEGACY_WORKBENCH_ENV, "true")
    # Ensure the process env is also visible to any os.environ reads that
    # bypass monkeypatch (defensive; monkeypatch normally covers this).
    os.environ.setdefault(LEGACY_WORKBENCH_ENV, "true")

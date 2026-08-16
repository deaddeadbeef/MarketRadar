from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from catalyst_radar.deprecation import block_if_deprecated_cli
from catalyst_radar.discovery.persist import persist_discovery_brief
from catalyst_radar.discovery.ready import build_discovery_readiness

pytestmark = pytest.mark.discovery


def test_discovery_ready_blocks_on_stale_sample() -> None:
    payload = build_discovery_readiness(
        events_path=Path("data/sample/world_events.json"),
        engine=None,
        now=datetime(2026, 8, 13, tzinfo=UTC),
    )
    assert payload["ready"] is False
    assert payload["first_blocker"] == "stale_events"
    assert payload["investment_advice"] is False
    assert "refresh-world-events" in str(payload["canonical_next_command"])


def test_discovery_ready_fresh_without_db_blocks_on_join() -> None:
    payload = build_discovery_readiness(
        events_path=Path("data/sample/world_events.json"),
        engine=None,
        now=datetime(2026, 7, 19, 12, tzinfo=UTC),
    )
    assert payload["freshness_ok"] is True
    assert payload["ready"] is False
    assert payload["first_blocker"] == "event_join_coverage"
    assert payload["canonical_next_command"] == (
        "catalyst-radar discovery-bars --polygon --confirm-external-call"
    )
    assert "fill-discovery-gaps" not in str(payload["canonical_next_command"])


def test_deprecated_cli_blocked_without_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CATALYST_ENABLE_LEGACY_WORKBENCH", raising=False)
    blocked = block_if_deprecated_cli("agent-brief")
    assert blocked is not None
    assert blocked.startswith("BLOCKED")
    monkeypatch.setenv("CATALYST_ENABLE_LEGACY_WORKBENCH", "true")
    assert block_if_deprecated_cli("agent-brief") is None
    assert block_if_deprecated_cli("discovery-brief") is None


def test_persist_discovery_brief_writes_dated_file(tmp_path: Path) -> None:
    result = persist_discovery_brief(
        {"schema_version": "discovery-brief-v1", "ready": False},
        dest_dir=tmp_path,
        clock=datetime(2026, 8, 13, tzinfo=UTC),
    )
    path = Path(str(result["path"]))
    assert path.name == "2026-08-13.json"
    assert path.is_file()
    assert result["file_writes_made"] == 1

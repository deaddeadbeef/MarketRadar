from __future__ import annotations

import pytest

pytestmark = pytest.mark.discovery

from pathlib import Path

from catalyst_radar.discovery.label import build_discovery_label_payload
from catalyst_radar.discovery.proof import build_discovery_proof
from catalyst_radar.storage.db import create_schema, engine_from_url


def test_discovery_proof_empty_without_db() -> None:
    proof = build_discovery_proof(engine=None)
    assert proof["schema_version"] == "discovery-proof-v1"
    assert proof["status"] == "no_db"
    assert proof["count"] == 0
    assert proof["investment_advice"] is False


def test_discovery_label_and_proof_roundtrip(tmp_path: Path) -> None:
    db_path = tmp_path / "proof.db"
    engine = engine_from_url(f"sqlite:///{db_path}")
    create_schema(engine)
    events = Path("data/sample/world_events.json")

    preview = build_discovery_label_payload(
        engine=engine,
        ticker="MU",
        label="good-research",
        events_path=events,
        execute=False,
    )
    assert preview["label_status"] == "preview"
    assert preview["db_writes_made"] == 0

    written = build_discovery_label_payload(
        engine=engine,
        ticker="MU",
        label="good-research",
        events_path=events,
        execute=True,
    )
    assert written["label_status"] == "written"
    assert written["db_writes_made"] == 1
    assert written["artifact_type"] == "discovery_row"

    proof = build_discovery_proof(engine=engine, limit=20)
    assert proof["status"] == "ready"
    assert proof["count"] >= 1
    assert proof["summary"]["claimable_count"] >= 1
    labels = {str(row.get("label")) for row in proof["entries"]}
    assert "good-research" in labels

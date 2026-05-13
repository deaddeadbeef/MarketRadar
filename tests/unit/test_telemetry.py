from __future__ import annotations

import logging
from datetime import UTC, datetime

from sqlalchemy import create_engine, select

from catalyst_radar.ops.telemetry import record_telemetry_event
from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.schema import audit_events


def test_record_telemetry_event_appends_audit_row_and_structured_log(tmp_path, caplog):
    engine = create_engine(f"sqlite:///{tmp_path / 'telemetry.db'}", future=True)
    create_schema(engine)
    occurred_at = datetime(2026, 5, 13, 1, 0, tzinfo=UTC)

    with caplog.at_level(logging.INFO, logger="catalyst_radar.telemetry"):
        event = record_telemetry_event(
            engine,
            event_name="radar_run.completed",
            status="success",
            actor_source="api",
            actor_id="user-1",
            actor_role="analyst",
            artifact_type="radar_run",
            artifact_id="daily-run",
            metadata={"step_counts": {"success": 2}},
            after_payload={"daily_result": {"status": "success"}},
            occurred_at=occurred_at,
        )

    with engine.connect() as conn:
        row = conn.execute(select(audit_events)).one()._mapping

    assert event.id == row["id"]
    assert row["event_type"] == "telemetry.radar_run.completed"
    assert row["actor_source"] == "api"
    assert row["actor_id"] == "user-1"
    assert row["actor_role"] == "analyst"
    assert row["artifact_type"] == "radar_run"
    assert row["artifact_id"] == "daily-run"
    assert row["status"] == "success"
    assert row["metadata"]["schema_version"] == "telemetry-v1"
    assert row["metadata"]["step_counts"] == {"success": 2}
    assert row["after_payload"] == {"daily_result": {"status": "success"}}
    assert len(caplog.records) == 1
    assert caplog.records[0].message == "telemetry_event"
    assert caplog.records[0].catalyst_telemetry["event_type"] == (
        "telemetry.radar_run.completed"
    )

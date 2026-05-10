from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, inspect

from catalyst_radar.security.audit import AuditEvent, AuditLogRepository
from catalyst_radar.storage.db import create_schema

OCCURRED_AT = datetime(2026, 5, 10, 14, tzinfo=UTC)


def test_audit_schema_creates_table_and_indexes(tmp_path) -> None:
    engine = create_engine(f"sqlite:///{tmp_path / 'audit-schema.db'}", future=True)
    create_schema(engine)

    inspector = inspect(engine)
    columns = {column["name"]: column for column in inspector.get_columns("audit_events")}
    assert set(columns) >= {
        "id",
        "event_type",
        "actor_source",
        "actor_id",
        "actor_role",
        "artifact_type",
        "artifact_id",
        "ticker",
        "candidate_state_id",
        "candidate_packet_id",
        "decision_card_id",
        "budget_ledger_id",
        "status",
        "metadata",
        "before_payload",
        "after_payload",
        "occurred_at",
        "available_at",
        "created_at",
    }
    for name in (
        "event_type",
        "actor_source",
        "status",
        "metadata",
        "before_payload",
        "after_payload",
        "occurred_at",
        "created_at",
    ):
        assert columns[name]["nullable"] is False

    index_names = {index["name"] for index in inspector.get_indexes("audit_events")}
    assert {
        "ix_audit_events_event_type_occurred",
        "ix_audit_events_artifact_occurred",
        "ix_audit_events_ticker_occurred",
        "ix_audit_events_candidate_packet",
    } <= index_names


def test_audit_repository_appends_repeated_events_for_same_artifact(tmp_path) -> None:
    engine = create_engine(f"sqlite:///{tmp_path / 'audit-events.db'}", future=True)
    create_schema(engine)
    repo = AuditLogRepository(engine)

    first = repo.append(
        event_type="decision.approved",
        actor_source="operator",
        actor_id="user-1",
        actor_role="portfolio_manager",
        artifact_type="decision_card",
        artifact_id="decision-card-1",
        ticker="msft",
        decision_card_id="decision-card-1",
        status="accepted",
        metadata={"ordinal": 1, "ordered_keys": ["first", "second"]},
        before_payload={"state": "review"},
        after_payload={"state": "approved"},
        occurred_at=OCCURRED_AT,
        created_at=OCCURRED_AT,
    )
    second = repo.append(
        event_type="decision.approved",
        actor_source="operator",
        actor_id="user-1",
        actor_role="portfolio_manager",
        artifact_type="decision_card",
        artifact_id="decision-card-1",
        ticker="MSFT",
        decision_card_id="decision-card-1",
        status="accepted",
        metadata={"ordinal": 2, "ordered_keys": ["third", "fourth"]},
        before_payload={"state": "review"},
        after_payload={"state": "approved"},
        occurred_at=OCCURRED_AT,
        created_at=OCCURRED_AT + timedelta(microseconds=1),
    )

    assert first.id != second.id

    events = repo.list_events(artifact_type="decision_card", artifact_id="decision-card-1")
    assert [event.id for event in events] == [first.id, second.id]
    assert [event.metadata["ordinal"] for event in events] == [1, 2]
    assert [list(event.metadata) for event in events] == [
        ["ordinal", "ordered_keys"],
        ["ordinal", "ordered_keys"],
    ]
    assert events[0].metadata["ordered_keys"] == ("first", "second")
    assert events[1].metadata["ordered_keys"] == ("third", "fourth")
    assert [event.ticker for event in events] == ["MSFT", "MSFT"]
    assert repo.list_events(ticker="msft", event_type="decision.approved") == events
    assert repo.list_events(artifact_type="candidate_packet") == []


def test_audit_event_validates_required_fields_json_and_datetimes() -> None:
    event = AuditEvent(
        event_type="candidate.generated",
        actor_source="pipeline",
        artifact_type="candidate_packet",
        artifact_id="packet-1",
        ticker="aapl",
        status="completed",
        metadata={"nested": {"at": OCCURRED_AT}},
        before_payload={},
        after_payload={"score": 91.5},
        occurred_at=datetime(2026, 5, 10, 22, tzinfo=timezone(timedelta(hours=8))),
    )

    assert event.ticker == "AAPL"
    assert event.occurred_at == OCCURRED_AT
    assert event.metadata["nested"]["at"] == "2026-05-10T14:00:00+00:00"

    with pytest.raises(ValueError, match="event_type must not be blank"):
        AuditEvent(
            event_type=" ",
            actor_source="pipeline",
            status="completed",
            occurred_at=OCCURRED_AT,
        )

    with pytest.raises(ValueError, match="occurred_at must be timezone-aware"):
        AuditEvent(
            event_type="candidate.generated",
            actor_source="pipeline",
            status="completed",
            occurred_at=datetime(2026, 5, 10, 14),
        )

    with pytest.raises(TypeError, match="metadata.bad must be JSON-serializable"):
        AuditEvent(
            event_type="candidate.generated",
            actor_source="pipeline",
            status="completed",
            metadata={"bad": object()},
            occurred_at=OCCURRED_AT,
        )

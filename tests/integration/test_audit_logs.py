from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import DBAPIError

from catalyst_radar.security.audit import AuditEvent, AuditLogRepository
from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.schema import audit_events

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
        "paper_trade_id",
        "alert_id",
        "decision",
        "reason",
        "hard_blocks",
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
        "hard_blocks",
        "metadata",
        "before_payload",
        "after_payload",
        "occurred_at",
        "created_at",
    ):
        assert columns[name]["nullable"] is False

    indexes = {
        index["name"]: index["column_names"] for index in inspector.get_indexes("audit_events")
    }
    assert {
        "ix_audit_events_event_type_occurred",
        "ix_audit_events_artifact",
        "ix_audit_events_artifact_occurred",
        "ix_audit_events_ticker_occurred",
        "ix_audit_events_candidate_packet",
    } <= set(indexes)
    assert indexes["ix_audit_events_artifact"] == ["artifact_type", "artifact_id"]


def test_audit_migrations_define_dialect_specific_append_only_contract() -> None:
    sqlite_migration = Path("sql/migrations/013_security_audit.sql").read_text(
        encoding="utf-8"
    )
    postgres_migration = Path("sql/migrations/013_security_audit.postgres.sql").read_text(
        encoding="utf-8"
    )

    assert "hard_blocks JSON NOT NULL DEFAULT '[]'" in sqlite_migration
    assert "CREATE INDEX IF NOT EXISTS ix_audit_events_artifact" in sqlite_migration
    assert "ON audit_events (artifact_type, artifact_id);" in sqlite_migration
    assert "CREATE TRIGGER IF NOT EXISTS trg_audit_events_no_update" in sqlite_migration
    assert "CREATE TRIGGER IF NOT EXISTS trg_audit_events_no_delete" in sqlite_migration
    assert "::jsonb" not in sqlite_migration
    assert "CREATE OR REPLACE FUNCTION reject_audit_events_mutation()" in postgres_migration
    assert "CREATE TRIGGER trg_audit_events_no_update" in postgres_migration
    assert "CREATE TRIGGER trg_audit_events_no_delete" in postgres_migration


def test_sqlite_audit_migration_executes_locally(tmp_path) -> None:
    migration = Path("sql/migrations/013_security_audit.sql").read_text(encoding="utf-8")
    db_path = tmp_path / "migration.db"

    with sqlite3.connect(db_path) as conn:
        conn.executescript(migration)
        columns = {row[1] for row in conn.execute("PRAGMA table_info(audit_events)")}
        indexes = {row[1] for row in conn.execute("PRAGMA index_list(audit_events)")}
        triggers = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type = 'trigger' AND tbl_name = 'audit_events'"
            )
        }

    assert {
        "paper_trade_id",
        "alert_id",
        "decision",
        "reason",
        "hard_blocks",
    } <= columns
    assert "ix_audit_events_artifact" in indexes
    assert {
        "trg_audit_events_no_update",
        "trg_audit_events_no_delete",
    } <= triggers


def test_audit_repository_appends_repeated_events_for_same_artifact(tmp_path) -> None:
    engine = create_engine(f"sqlite:///{tmp_path / 'audit-events.db'}", future=True)
    create_schema(engine)
    repo = AuditLogRepository(engine)

    first = repo.append_event(
        event_type="decision.approved",
        actor_source="operator",
        actor_id="user-1",
        actor_role="portfolio_manager",
        artifact_type="decision_card",
        artifact_id="decision-card-1",
        ticker="msft",
        decision_card_id="decision-card-1",
        paper_trade_id="paper-trade-1",
        alert_id="alert-1",
        decision="buy",
        reason="approved after review",
        hard_blocks=["risk_hard_block"],
        status="accepted",
        metadata={"ordinal": 1, "ordered_keys": ["first", "second"]},
        before_payload={"state": "review"},
        after_payload={"state": "approved"},
        occurred_at=OCCURRED_AT,
        created_at=OCCURRED_AT,
    )
    second = repo.append_event(
        event_type="decision.approved",
        actor_source="operator",
        actor_id="user-1",
        actor_role="portfolio_manager",
        artifact_type="decision_card",
        artifact_id="decision-card-1",
        ticker="MSFT",
        decision_card_id="decision-card-1",
        paper_trade_id="paper-trade-1",
        alert_id="alert-1",
        decision="buy",
        reason="approval recorded again",
        hard_blocks=["risk_hard_block", "liquidity_hard_block"],
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
    assert len({event.id for event in events}) == 2
    assert [event.metadata["ordinal"] for event in events] == [1, 2]
    assert [list(event.metadata) for event in events] == [
        ["ordinal", "ordered_keys"],
        ["ordinal", "ordered_keys"],
    ]
    assert events[0].metadata["ordered_keys"] == ("first", "second")
    assert events[1].metadata["ordered_keys"] == ("third", "fourth")
    assert [event.ticker for event in events] == ["MSFT", "MSFT"]
    assert [event.paper_trade_id for event in events] == ["paper-trade-1", "paper-trade-1"]
    assert [event.alert_id for event in events] == ["alert-1", "alert-1"]
    assert [event.decision for event in events] == ["buy", "buy"]
    assert events[0].reason == "approved after review"
    assert events[1].reason == "approval recorded again"
    assert events[0].hard_blocks == ("risk_hard_block",)
    assert events[1].hard_blocks == ("risk_hard_block", "liquidity_hard_block")
    assert repo.list_events(ticker="msft", event_type="decision.approved") == events
    assert repo.list_events(artifact_type="candidate_packet") == []


def test_audit_repository_append_event_keyword_api_defaults_timestamps(tmp_path) -> None:
    engine = create_engine(f"sqlite:///{tmp_path / 'audit-keyword-api.db'}", future=True)
    create_schema(engine)
    repo = AuditLogRepository(engine)

    before = datetime.now(UTC)
    event = repo.append_event(
        event_type="alert.sent",
        actor_source="pipeline",
        artifact_type="alert",
        artifact_id="alert-1",
        alert_id="alert-1",
    )
    after = datetime.now(UTC)

    assert event.status == "success"
    assert before <= event.occurred_at <= after
    assert before <= event.created_at <= after
    assert event.hard_blocks == ()

    events = repo.list_events(artifact_type="alert", artifact_id="alert-1")
    assert events == [event]


def test_audit_events_reject_direct_update_and_delete(tmp_path) -> None:
    engine = create_engine(f"sqlite:///{tmp_path / 'audit-append-only.db'}", future=True)
    create_schema(engine)
    repo = AuditLogRepository(engine)
    event = repo.append_event(
        event_type="alert.sent",
        actor_source="pipeline",
        artifact_type="alert",
        artifact_id="alert-1",
        alert_id="alert-1",
    )

    with pytest.raises(DBAPIError, match="audit_events is append-only"):
        with engine.begin() as conn:
            conn.execute(
                audit_events.update()
                .where(audit_events.c.id == event.id)
                .values(status="mutated")
            )

    with pytest.raises(DBAPIError, match="audit_events is append-only"):
        with engine.begin() as conn:
            conn.execute(audit_events.delete().where(audit_events.c.id == event.id))

    assert repo.list_events(artifact_type="alert", artifact_id="alert-1") == [event]


def test_audit_repository_rejects_non_positive_limit(tmp_path) -> None:
    engine = create_engine(f"sqlite:///{tmp_path / 'audit-limit.db'}", future=True)
    create_schema(engine)
    repo = AuditLogRepository(engine)

    with pytest.raises(ValueError, match="limit must be positive"):
        repo.list_events(limit=0)

    with pytest.raises(ValueError, match="limit must be positive"):
        repo.list_events(limit=-1)


def test_audit_event_validates_required_fields_json_and_datetimes() -> None:
    event = AuditEvent(
        event_type="candidate.generated",
        actor_source="pipeline",
        artifact_type="candidate_packet",
        artifact_id="packet-1",
        ticker="aapl",
        status="completed",
        hard_blocks=("risk_hard_block",),
        metadata={"nested": {"at": OCCURRED_AT}},
        before_payload={},
        after_payload={"score": 91.5},
        occurred_at=datetime(2026, 5, 10, 22, tzinfo=timezone(timedelta(hours=8))),
    )

    assert event.ticker == "AAPL"
    assert event.occurred_at == OCCURRED_AT
    assert event.hard_blocks == ("risk_hard_block",)
    assert event.metadata["nested"]["at"] == "2026-05-10T14:00:00+00:00"

    for field_name in ("event_type", "actor_source", "status"):
        kwargs = {
            "event_type": "candidate.generated",
            "actor_source": "pipeline",
            "status": "completed",
            "occurred_at": OCCURRED_AT,
            field_name: None,
        }
        with pytest.raises(ValueError, match=f"{field_name} must not be blank"):
            AuditEvent(**kwargs)

        kwargs[field_name] = object()
        with pytest.raises(TypeError, match=f"{field_name} must be a string"):
            AuditEvent(**kwargs)

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

    with pytest.raises(TypeError, match="hard_blocks must be a JSON array"):
        AuditEvent(
            event_type="candidate.generated",
            actor_source="pipeline",
            status="completed",
            hard_blocks="risk_hard_block",
            occurred_at=OCCURRED_AT,
        )

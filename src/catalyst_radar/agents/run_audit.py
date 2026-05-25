from __future__ import annotations

import uuid
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import Engine, select

from catalyst_radar.security.audit import AuditLogRepository
from catalyst_radar.storage.db import create_schema, engine_from_url
from catalyst_radar.storage.schema import audit_events

AGENT_RUN_AUDIT_EVENT_TYPE = "agent_run_recorded"


def record_agent_run_audit(
    *,
    database_url: str | None = None,
    engine: Engine | None = None,
    mode: str,
    model: str | None,
    snapshot_hash: str,
    external_calls_made: Mapping[str, int],
    status: str,
    operator_goal: str | None = None,
    redaction_version: str = "market-radar-agent-snapshot-v1",
    external_calls_planned: Mapping[str, int] | None = None,
    token_usage: Mapping[str, int] | None = None,
    final_output_summary: str | None = None,
    safety_verdict: str | None = None,
) -> str:
    resolved_engine = _resolve_engine(database_url=database_url, engine=engine)
    run_id = f"agent-run-v1:{uuid.uuid4().hex}"
    now = datetime.now(UTC)
    AuditLogRepository(resolved_engine).append_event(
        event_type=AGENT_RUN_AUDIT_EVENT_TYPE,
        actor_source="openai_agents_sdk",
        status=status,
        artifact_type="agent_run",
        artifact_id=run_id,
        metadata={
            "mode": mode,
            "model": model,
            "operator_goal": operator_goal,
            "snapshot_hash": snapshot_hash,
            "redaction_version": redaction_version,
            "external_calls_planned": dict(external_calls_planned or {}),
            "external_calls_made": dict(external_calls_made),
            "token_usage": dict(token_usage or {}),
            "final_output_summary": final_output_summary,
            "safety_verdict": safety_verdict,
        },
        after_payload={
            "status": status,
            "external_calls_made": dict(external_calls_made),
        },
        occurred_at=now,
        created_at=now,
    )
    return run_id


def load_agent_run_audit(
    database_url: str,
    run_id: str,
) -> dict[str, object]:
    engine = engine_from_url(database_url)
    create_schema(engine)
    stmt = select(audit_events).where(
        audit_events.c.event_type == AGENT_RUN_AUDIT_EVENT_TYPE,
        audit_events.c.artifact_type == "agent_run",
        audit_events.c.artifact_id == run_id,
    )
    with engine.connect() as conn:
        row = conn.execute(stmt).mappings().first()
    if row is None:
        raise KeyError(run_id)
    metadata = _mapping(row.get("metadata"))
    return {
        "id": row["artifact_id"],
        "created_at": row["created_at"],
        "mode": metadata.get("mode"),
        "model": metadata.get("model"),
        "operator_goal": metadata.get("operator_goal"),
        "snapshot_hash": metadata.get("snapshot_hash"),
        "redaction_version": metadata.get("redaction_version"),
        "external_calls_planned": _mapping(metadata.get("external_calls_planned")),
        "external_calls_made": _mapping(metadata.get("external_calls_made")),
        "token_usage": _mapping(metadata.get("token_usage")),
        "status": row["status"],
        "final_output_summary": metadata.get("final_output_summary"),
        "safety_verdict": metadata.get("safety_verdict"),
    }


def _resolve_engine(
    *,
    database_url: str | None,
    engine: Engine | None,
) -> Engine:
    if engine is not None:
        return engine
    if not database_url:
        msg = "database_url or engine is required"
        raise ValueError(msg)
    resolved = engine_from_url(database_url)
    create_schema(resolved)
    return resolved


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


__all__ = [
    "AGENT_RUN_AUDIT_EVENT_TYPE",
    "load_agent_run_audit",
    "record_agent_run_audit",
]

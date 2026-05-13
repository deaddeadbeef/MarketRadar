from __future__ import annotations

import logging
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import Engine

from catalyst_radar.security.audit import AuditEvent, AuditLogRepository

TELEMETRY_PREFIX = "telemetry."
TELEMETRY_SCHEMA_VERSION = "telemetry-v1"

logger = logging.getLogger("catalyst_radar.telemetry")


def record_telemetry_event(
    engine: Engine,
    *,
    event_name: str,
    status: str,
    actor_source: str,
    actor_id: str | None = None,
    actor_role: str | None = None,
    artifact_type: str | None = None,
    artifact_id: str | None = None,
    reason: str | None = None,
    metadata: Mapping[str, Any] | None = None,
    before_payload: Mapping[str, Any] | None = None,
    after_payload: Mapping[str, Any] | None = None,
    occurred_at: datetime | None = None,
    available_at: datetime | None = None,
) -> AuditEvent:
    event_type = _telemetry_event_type(event_name)
    resolved_at = occurred_at or datetime.now(UTC)
    event_metadata = {
        "schema_version": TELEMETRY_SCHEMA_VERSION,
        **dict(metadata or {}),
    }
    log_payload = {
        "event_type": event_type,
        "status": status,
        "actor_source": actor_source,
        "artifact_type": artifact_type,
        "artifact_id": artifact_id,
        "reason": reason,
        "metadata": event_metadata,
        "occurred_at": resolved_at.isoformat(),
    }
    logger.info(
        "telemetry_event",
        extra={"catalyst_telemetry": log_payload},
    )
    return AuditLogRepository(engine).append_event(
        event_type=event_type,
        actor_source=actor_source,
        actor_id=actor_id,
        actor_role=actor_role,
        artifact_type=artifact_type,
        artifact_id=artifact_id,
        status=status,
        reason=reason,
        metadata=event_metadata,
        before_payload=dict(before_payload or {}),
        after_payload=dict(after_payload or {}),
        occurred_at=resolved_at,
        available_at=available_at,
    )


def _telemetry_event_type(event_name: str) -> str:
    text = str(event_name or "").strip()
    if not text:
        msg = "event_name must not be blank"
        raise ValueError(msg)
    return text if text.startswith(TELEMETRY_PREFIX) else f"{TELEMETRY_PREFIX}{text}"


__all__ = [
    "TELEMETRY_PREFIX",
    "TELEMETRY_SCHEMA_VERSION",
    "record_telemetry_event",
]

from __future__ import annotations

import json
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from enum import Enum
from typing import Any

from sqlalchemy import Engine, insert, select

from catalyst_radar.core.immutability import freeze_json_value, thaw_json_value
from catalyst_radar.storage.schema import audit_events


@dataclass(frozen=True)
class AuditEvent:
    event_type: str
    actor_source: str
    status: str
    occurred_at: datetime
    id: str = field(default_factory=lambda: f"audit-event-v1:{uuid.uuid4().hex}")
    actor_id: str | None = None
    actor_role: str | None = None
    artifact_type: str | None = None
    artifact_id: str | None = None
    ticker: str | None = None
    candidate_state_id: str | None = None
    candidate_packet_id: str | None = None
    decision_card_id: str | None = None
    budget_ledger_id: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    before_payload: Mapping[str, Any] = field(default_factory=dict)
    after_payload: Mapping[str, Any] = field(default_factory=dict)
    available_at: datetime | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", _required_text(self.id, "id"))
        object.__setattr__(self, "event_type", _required_text(self.event_type, "event_type"))
        object.__setattr__(
            self,
            "actor_source",
            _required_text(self.actor_source, "actor_source"),
        )
        object.__setattr__(self, "status", _required_text(self.status, "status"))
        object.__setattr__(self, "actor_id", _optional_text(self.actor_id, "actor_id"))
        object.__setattr__(self, "actor_role", _optional_text(self.actor_role, "actor_role"))
        object.__setattr__(
            self,
            "artifact_type",
            _optional_text(self.artifact_type, "artifact_type"),
        )
        object.__setattr__(
            self,
            "artifact_id",
            _optional_text(self.artifact_id, "artifact_id"),
        )
        object.__setattr__(self, "ticker", _optional_ticker(self.ticker))
        object.__setattr__(
            self,
            "candidate_state_id",
            _optional_text(self.candidate_state_id, "candidate_state_id"),
        )
        object.__setattr__(
            self,
            "candidate_packet_id",
            _optional_text(self.candidate_packet_id, "candidate_packet_id"),
        )
        object.__setattr__(
            self,
            "decision_card_id",
            _optional_text(self.decision_card_id, "decision_card_id"),
        )
        object.__setattr__(
            self,
            "budget_ledger_id",
            _optional_text(self.budget_ledger_id, "budget_ledger_id"),
        )
        object.__setattr__(
            self,
            "metadata",
            freeze_json_value(_json_ready_mapping(self.metadata, "metadata")),
        )
        object.__setattr__(
            self,
            "before_payload",
            freeze_json_value(_json_ready_mapping(self.before_payload, "before_payload")),
        )
        object.__setattr__(
            self,
            "after_payload",
            freeze_json_value(_json_ready_mapping(self.after_payload, "after_payload")),
        )
        object.__setattr__(
            self,
            "occurred_at",
            _to_utc_datetime(self.occurred_at, "occurred_at"),
        )
        if self.available_at is not None:
            object.__setattr__(
                self,
                "available_at",
                _to_utc_datetime(self.available_at, "available_at"),
            )
        object.__setattr__(
            self,
            "created_at",
            _to_utc_datetime(self.created_at, "created_at"),
        )


class AuditLogRepository:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def append_event(self, event: AuditEvent) -> AuditEvent:
        with self.engine.begin() as conn:
            conn.execute(insert(audit_events).values(**_event_row(event)))
        return event

    def append(
        self,
        *,
        event_type: str,
        actor_source: str,
        status: str,
        occurred_at: datetime,
        actor_id: str | None = None,
        actor_role: str | None = None,
        artifact_type: str | None = None,
        artifact_id: str | None = None,
        ticker: str | None = None,
        candidate_state_id: str | None = None,
        candidate_packet_id: str | None = None,
        decision_card_id: str | None = None,
        budget_ledger_id: str | None = None,
        metadata: Mapping[str, Any] | None = None,
        before_payload: Mapping[str, Any] | None = None,
        after_payload: Mapping[str, Any] | None = None,
        available_at: datetime | None = None,
        created_at: datetime | None = None,
    ) -> AuditEvent:
        event = AuditEvent(
            event_type=event_type,
            actor_source=actor_source,
            status=status,
            occurred_at=occurred_at,
            actor_id=actor_id,
            actor_role=actor_role,
            artifact_type=artifact_type,
            artifact_id=artifact_id,
            ticker=ticker,
            candidate_state_id=candidate_state_id,
            candidate_packet_id=candidate_packet_id,
            decision_card_id=decision_card_id,
            budget_ledger_id=budget_ledger_id,
            metadata=metadata or {},
            before_payload=before_payload or {},
            after_payload=after_payload or {},
            available_at=available_at,
            created_at=created_at or datetime.now(UTC),
        )
        return self.append_event(event)

    def list_events(
        self,
        *,
        artifact_type: str | None = None,
        artifact_id: str | None = None,
        ticker: str | None = None,
        event_type: str | None = None,
        limit: int = 200,
    ) -> list[AuditEvent]:
        stmt = (
            select(audit_events)
            .where(
                *_event_filters(
                    artifact_type=artifact_type,
                    artifact_id=artifact_id,
                    ticker=ticker,
                    event_type=event_type,
                )
            )
            .order_by(
                audit_events.c.occurred_at,
                audit_events.c.created_at,
                audit_events.c.id,
            )
            .limit(_positive_limit(limit))
        )
        with self.engine.connect() as conn:
            return [_event_from_row(row._mapping) for row in conn.execute(stmt)]


def _event_row(event: AuditEvent) -> dict[str, Any]:
    return {
        "id": event.id,
        "event_type": event.event_type,
        "actor_source": event.actor_source,
        "actor_id": event.actor_id,
        "actor_role": event.actor_role,
        "artifact_type": event.artifact_type,
        "artifact_id": event.artifact_id,
        "ticker": event.ticker,
        "candidate_state_id": event.candidate_state_id,
        "candidate_packet_id": event.candidate_packet_id,
        "decision_card_id": event.decision_card_id,
        "budget_ledger_id": event.budget_ledger_id,
        "status": event.status,
        "metadata": thaw_json_value(event.metadata),
        "before_payload": thaw_json_value(event.before_payload),
        "after_payload": thaw_json_value(event.after_payload),
        "occurred_at": event.occurred_at,
        "available_at": event.available_at,
        "created_at": event.created_at,
    }


def _event_from_row(row: Any) -> AuditEvent:
    return AuditEvent(
        id=row["id"],
        event_type=row["event_type"],
        actor_source=row["actor_source"],
        actor_id=row["actor_id"],
        actor_role=row["actor_role"],
        artifact_type=row["artifact_type"],
        artifact_id=row["artifact_id"],
        ticker=row["ticker"],
        candidate_state_id=row["candidate_state_id"],
        candidate_packet_id=row["candidate_packet_id"],
        decision_card_id=row["decision_card_id"],
        budget_ledger_id=row["budget_ledger_id"],
        status=row["status"],
        metadata=row["metadata"],
        before_payload=row["before_payload"],
        after_payload=row["after_payload"],
        occurred_at=_as_datetime(row["occurred_at"]),
        available_at=_as_optional_datetime(row["available_at"]),
        created_at=_as_datetime(row["created_at"]),
    )


def _event_filters(
    *,
    artifact_type: str | None,
    artifact_id: str | None,
    ticker: str | None,
    event_type: str | None,
) -> list[Any]:
    filters = []
    if artifact_type is not None and artifact_type.strip():
        filters.append(audit_events.c.artifact_type == artifact_type.strip())
    if artifact_id is not None and artifact_id.strip():
        filters.append(audit_events.c.artifact_id == artifact_id.strip())
    if ticker is not None and ticker.strip():
        filters.append(audit_events.c.ticker == ticker.strip().upper())
    if event_type is not None and event_type.strip():
        filters.append(audit_events.c.event_type == event_type.strip())
    return filters


def _required_text(value: object, field_name: str) -> str:
    text = str(value).strip()
    if not text:
        msg = f"{field_name} must not be blank"
        raise ValueError(msg)
    return text


def _optional_text(value: object | None, field_name: str) -> str | None:
    if value is None:
        return None
    return _required_text(value, field_name)


def _optional_ticker(value: object | None) -> str | None:
    if value is None:
        return None
    return _required_text(value, "ticker").upper()


def _json_ready_mapping(value: Mapping[str, Any], field_name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        msg = f"{field_name} must be a mapping"
        raise TypeError(msg)
    normalized = {
        str(key): _json_ready(item, f"{field_name}.{key}") for key, item in value.items()
    }
    json.dumps(normalized, allow_nan=False, separators=(",", ":"), sort_keys=True)
    return normalized


def _json_ready(value: Any, field_name: str) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _json_ready(item, f"{field_name}.{key}")
            for key, item in value.items()
        }
    if isinstance(value, list | tuple):
        return [_json_ready(item, field_name) for item in value]
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return _to_utc_datetime(value, field_name).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if value is None or isinstance(value, str | int | bool):
        return value
    if isinstance(value, float):
        json.dumps(value, allow_nan=False)
        return value
    msg = f"{field_name} must be JSON-serializable"
    raise TypeError(msg)


def _to_utc_datetime(value: datetime | str, field_name: str) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if not isinstance(value, datetime):
        msg = f"{field_name} must be a datetime"
        raise TypeError(msg)
    if value.tzinfo is None or value.utcoffset() is None:
        msg = f"{field_name} must be timezone-aware"
        raise ValueError(msg)
    return value.astimezone(UTC)


def _as_datetime(value: datetime | str) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _as_optional_datetime(value: datetime | str | None) -> datetime | None:
    if value is None:
        return None
    return _as_datetime(value)


def _positive_limit(value: int) -> int:
    return max(1, int(value))


__all__ = ["AuditEvent", "AuditLogRepository"]

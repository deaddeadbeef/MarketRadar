from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime
from typing import Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select

from catalyst_radar.core.config import AppConfig
from catalyst_radar.dashboard import data as dashboard_data
from catalyst_radar.security.access import Role, require_role
from catalyst_radar.security.redaction import redact_value
from catalyst_radar.storage.db import engine_from_url
from catalyst_radar.storage.schema import audit_events

router = APIRouter(prefix="/api/ops", tags=["ops"])


def _engine():
    return engine_from_url(AppConfig.from_env().database_url)


def _dashboard_helper(name: str) -> Callable[..., Any]:
    try:
        return getattr(dashboard_data, name)
    except AttributeError as exc:
        msg = f"dashboard data helper is unavailable: {name}"
        raise RuntimeError(msg) from exc


@router.get("/health", dependencies=[Depends(require_role(Role.VIEWER))])
def health() -> dict[str, object]:
    load_ops_health = _dashboard_helper("load_ops_health")
    return load_ops_health(_engine())


@router.get("/telemetry", dependencies=[Depends(require_role(Role.VIEWER))])
def telemetry(
    limit: int = Query(default=8, ge=1, le=100),
) -> dict[str, object]:
    load_ops_health = _dashboard_helper("load_ops_health")
    telemetry_tape_payload = _dashboard_helper("telemetry_tape_payload")
    return telemetry_tape_payload(load_ops_health(_engine()), limit=limit)


@router.get("/telemetry/raw", dependencies=[Depends(require_role(Role.VIEWER))])
def raw_telemetry(
    limit: int = Query(default=25, ge=1, le=500),
    event_type: str | None = Query(default=None),
    status: str | None = Query(default=None),
    artifact_type: str | None = Query(default=None),
    artifact_id: str | None = Query(default=None),
    ticker: str | None = Query(default=None),
) -> dict[str, object]:
    filters = [audit_events.c.event_type.like("telemetry.%")]
    normalized_event_type = _normalize_telemetry_event_type(event_type)
    if normalized_event_type:
        filters.append(audit_events.c.event_type == normalized_event_type)
    if _present(status):
        filters.append(audit_events.c.status == status.strip())
    if _present(artifact_type):
        filters.append(audit_events.c.artifact_type == artifact_type.strip())
    if _present(artifact_id):
        filters.append(audit_events.c.artifact_id == artifact_id.strip())
    if _present(ticker):
        filters.append(audit_events.c.ticker == ticker.strip().upper())

    stmt = (
        select(audit_events)
        .where(*filters)
        .order_by(
            audit_events.c.occurred_at.desc(),
            audit_events.c.created_at.desc(),
            audit_events.c.id.desc(),
        )
        .limit(limit)
    )
    with _engine().connect() as conn:
        rows = [_raw_audit_event(row._mapping) for row in conn.execute(stmt)]
    return {
        "schema_version": "ops-telemetry-raw-v1",
        "external_calls_made": 0,
        "limit": limit,
        "count": len(rows),
        "filters": {
            "event_type": normalized_event_type,
            "status": _clean_filter(status),
            "artifact_type": _clean_filter(artifact_type),
            "artifact_id": _clean_filter(artifact_id),
            "ticker": _clean_filter(ticker.upper() if ticker else None),
        },
        "events": rows,
    }


def _raw_audit_event(row: Any) -> dict[str, object]:
    payload = {
        "id": row["id"],
        "event_type": row["event_type"],
        "actor_source": row["actor_source"],
        "actor_id": row["actor_id"],
        "actor_role": row["actor_role"],
        "artifact_type": row["artifact_type"],
        "artifact_id": row["artifact_id"],
        "ticker": row["ticker"],
        "candidate_state_id": row["candidate_state_id"],
        "candidate_packet_id": row["candidate_packet_id"],
        "decision_card_id": row["decision_card_id"],
        "budget_ledger_id": row["budget_ledger_id"],
        "paper_trade_id": row["paper_trade_id"],
        "alert_id": row["alert_id"],
        "decision": row["decision"],
        "reason": row["reason"],
        "hard_blocks": row["hard_blocks"],
        "status": row["status"],
        "metadata": row["metadata"],
        "before_payload": row["before_payload"],
        "after_payload": row["after_payload"],
        "occurred_at": row["occurred_at"],
        "available_at": row["available_at"],
        "created_at": row["created_at"],
    }
    redacted = redact_value(_json_safe(payload))
    return redacted if isinstance(redacted, dict) else {}


def _json_safe(value: object) -> object:
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_json_safe(item) for item in value]
    return value


def _normalize_telemetry_event_type(value: str | None) -> str | None:
    text = _clean_filter(value)
    if text is None:
        return None
    return text if text.startswith("telemetry.") else f"telemetry.{text}"


def _clean_filter(value: str | None) -> str | None:
    if value is None:
        return None
    text = value.strip()
    return text or None


def _present(value: str | None) -> bool:
    return _clean_filter(value) is not None

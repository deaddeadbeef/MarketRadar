from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel, ConfigDict

from catalyst_radar.alerts.models import Alert, AlertRoute, AlertStatus
from catalyst_radar.core.config import AppConfig
from catalyst_radar.core.immutability import thaw_json_value
from catalyst_radar.feedback.service import (
    InvalidFeedbackError,
    MissingArtifactError,
    TickerMismatchError,
    record_feedback,
)
from catalyst_radar.security.access import Role, require_role
from catalyst_radar.storage.alert_repositories import AlertRepository
from catalyst_radar.storage.db import engine_from_url

router = APIRouter(prefix="/api/alerts", tags=["alerts"])


class AlertFeedbackRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    label: str
    ticker: str | None = None
    notes: str | None = None


def _engine():
    return engine_from_url(AppConfig.from_env().database_url)


@router.get("", dependencies=[Depends(require_role(Role.VIEWER))])
def alerts(
    ticker: str | None = None,
    status: str | None = None,
    route: str | None = None,
    limit: int = Query(default=200, ge=1, le=500),
) -> dict[str, list[dict[str, Any]]]:
    resolved_status = _enum_filter(AlertStatus, status, "status")
    resolved_route = _enum_filter(AlertRoute, route, "route")
    repo = AlertRepository(_engine())
    rows = repo.list_alerts(
        available_at=datetime.now(UTC),
        ticker=ticker,
        status=resolved_status,
        route=resolved_route,
        limit=limit,
    )
    return {"items": [_alert_payload(row) for row in rows]}


@router.get("/{alert_id}", dependencies=[Depends(require_role(Role.VIEWER))])
def alert_detail(alert_id: str) -> dict[str, Any]:
    alert = AlertRepository(_engine()).alert_by_id(
        alert_id,
        available_at=datetime.now(UTC),
    )
    if alert is None:
        raise HTTPException(status_code=404, detail="alert not found")
    return _alert_payload(alert)


@router.post("/{alert_id}/feedback", dependencies=[Depends(require_role(Role.ANALYST))])
def alert_feedback(
    alert_id: str,
    request: AlertFeedbackRequest,
    x_catalyst_actor: str | None = Header(default=None),
    x_catalyst_role: str | None = Header(default=None),
) -> dict[str, str]:
    engine = _engine()
    alert = AlertRepository(engine).alert_by_id(alert_id, available_at=datetime.now(UTC))
    if alert is None:
        raise HTTPException(status_code=404, detail="alert not found")
    if request.ticker is not None and request.ticker.strip().upper() != alert.ticker:
        raise HTTPException(
            status_code=422,
            detail="ticker must match the referenced artifact",
        )

    try:
        result = record_feedback(
            engine,
            artifact_type="alert",
            artifact_id=alert.id,
            ticker=alert.ticker,
            label=request.label,
            notes=request.notes,
            source="api",
            actor_id=x_catalyst_actor,
            actor_role=x_catalyst_role,
        )
    except MissingArtifactError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (InvalidFeedbackError, TickerMismatchError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    useful_label = result.useful_label
    return {
        "id": useful_label.id,
        "artifact_type": useful_label.artifact_type,
        "artifact_id": useful_label.artifact_id,
        "ticker": useful_label.ticker,
        "label": useful_label.label,
    }


def _alert_payload(alert: Alert) -> dict[str, Any]:
    return {
        "id": alert.id,
        "ticker": alert.ticker,
        "as_of": alert.as_of,
        "source_ts": alert.source_ts,
        "available_at": alert.available_at,
        "candidate_state_id": alert.candidate_state_id,
        "candidate_packet_id": alert.candidate_packet_id,
        "decision_card_id": alert.decision_card_id,
        "action_state": alert.action_state,
        "route": alert.route.value,
        "channel": alert.channel.value,
        "priority": alert.priority.value,
        "status": alert.status.value,
        "dedupe_key": alert.dedupe_key,
        "trigger_kind": alert.trigger_kind,
        "trigger_fingerprint": alert.trigger_fingerprint,
        "title": alert.title,
        "summary": alert.summary,
        "feedback_url": alert.feedback_url,
        "payload": thaw_json_value(alert.payload),
        "created_at": alert.created_at,
        "sent_at": alert.sent_at,
    }


def _enum_filter(enum_type: type[Any], value: str | None, field_name: str) -> str | None:
    if value is None or not value.strip():
        return None
    try:
        return enum_type(value.strip()).value
    except ValueError as exc:
        allowed = ", ".join(sorted(item.value for item in enum_type))
        raise HTTPException(
            status_code=422,
            detail=f"{field_name} must be one of: {allowed}",
        ) from exc

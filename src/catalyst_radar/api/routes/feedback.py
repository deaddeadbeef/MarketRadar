from __future__ import annotations

from datetime import UTC, datetime

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from sqlalchemy import Engine, select

from catalyst_radar.core.config import AppConfig
from catalyst_radar.storage.db import engine_from_url
from catalyst_radar.storage.schema import (
    candidate_packets,
    candidate_states,
    decision_cards,
    paper_trades,
)
from catalyst_radar.storage.validation_repositories import ValidationRepository
from catalyst_radar.validation.models import UsefulAlertLabel, useful_alert_label_id

ALLOWED_ARTIFACT_TYPES = frozenset(
    {"candidate_packet", "decision_card", "paper_trade", "alert"}
)
ALLOWED_LABELS = frozenset(
    {"useful", "noisy", "too_late", "too_early", "ignored", "acted"}
)

router = APIRouter(prefix="/api/feedback", tags=["feedback"])


class FeedbackRequest(BaseModel):
    artifact_type: str
    artifact_id: str
    ticker: str
    label: str
    notes: str | None = None


def _engine():
    return engine_from_url(AppConfig.from_env().database_url)


@router.post("")
def record_feedback(request: FeedbackRequest) -> dict[str, str]:
    artifact_type = _allowed_value(
        request.artifact_type,
        allowed=ALLOWED_ARTIFACT_TYPES,
        field_name="artifact_type",
    )
    label = _allowed_value(request.label, allowed=ALLOWED_LABELS, field_name="label")
    artifact_id = _required_text(request.artifact_id, "artifact_id")
    ticker = _required_text(request.ticker, "ticker").upper()
    notes = _optional_notes(request.notes)
    engine = _engine()
    artifact_ticker = _artifact_ticker(engine, artifact_type, artifact_id)
    if artifact_ticker != ticker:
        raise HTTPException(
            status_code=422,
            detail="ticker must match the referenced artifact",
        )

    useful_label = UsefulAlertLabel(
        id=useful_alert_label_id(
            artifact_type=artifact_type,
            artifact_id=artifact_id,
            label=label,
        ),
        artifact_type=artifact_type,
        artifact_id=artifact_id,
        ticker=ticker,
        label=label,
        notes=notes,
        created_at=datetime.now(UTC),
    )
    ValidationRepository(engine).insert_useful_alert_label(useful_label)
    return {
        "id": useful_label.id,
        "artifact_type": useful_label.artifact_type,
        "artifact_id": useful_label.artifact_id,
        "ticker": useful_label.ticker,
        "label": useful_label.label,
    }


def _allowed_value(value: str, *, allowed: frozenset[str], field_name: str) -> str:
    text = _required_text(value, field_name)
    if text not in allowed:
        allowed_values = ", ".join(sorted(allowed))
        raise HTTPException(
            status_code=422,
            detail=f"{field_name} must be one of: {allowed_values}",
        )
    return text


def _required_text(value: object, field_name: str) -> str:
    text = str(value).strip()
    if not text:
        raise HTTPException(status_code=422, detail=f"{field_name} must not be blank")
    return text


def _optional_notes(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _artifact_ticker(engine: Engine, artifact_type: str, artifact_id: str) -> str:
    table_by_artifact = {
        "candidate_packet": candidate_packets,
        "decision_card": decision_cards,
        "paper_trade": paper_trades,
        "alert": candidate_states,
    }
    table = table_by_artifact[artifact_type]
    with engine.connect() as conn:
        row = conn.execute(
            select(table.c.ticker).where(table.c.id == artifact_id).limit(1)
        ).first()
    if row is None:
        raise HTTPException(status_code=404, detail="referenced artifact not found")
    return str(row[0]).upper()

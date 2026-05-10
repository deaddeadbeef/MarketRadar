from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, ConfigDict

from catalyst_radar.core.config import AppConfig
from catalyst_radar.feedback.service import (
    InvalidFeedbackError,
    MissingArtifactError,
    TickerMismatchError,
)
from catalyst_radar.feedback.service import (
    record_feedback as record_feedback_service,
)
from catalyst_radar.storage.db import engine_from_url

router = APIRouter(prefix="/api/feedback", tags=["feedback"])


class FeedbackRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    artifact_type: str
    artifact_id: str
    ticker: str
    label: str
    notes: str | None = None


def _engine():
    return engine_from_url(AppConfig.from_env().database_url)


@router.post("")
def record_feedback(request: FeedbackRequest) -> dict[str, str]:
    try:
        result = record_feedback_service(
            _engine(),
            artifact_type=request.artifact_type,
            artifact_id=request.artifact_id,
            ticker=request.ticker,
            label=request.label,
            notes=request.notes,
            source="api",
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

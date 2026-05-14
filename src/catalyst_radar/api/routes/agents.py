from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field

from catalyst_radar.agents.review_service import run_agent_review
from catalyst_radar.agents.tasks import DEFAULT_TASKS
from catalyst_radar.core.config import AppConfig
from catalyst_radar.dashboard import data as dashboard_data
from catalyst_radar.security.access import Role, require_role
from catalyst_radar.storage.db import create_schema, engine_from_url

router = APIRouter(prefix="/api/agents", tags=["agents"])


class AgentReviewRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ticker: str = Field(min_length=1, max_length=12)
    as_of: date
    available_at: datetime | None = None
    task: str = "skeptic_review"
    mode: Literal["dry_run", "fake", "real"] = "dry_run"


def _engine():
    engine = engine_from_url(AppConfig.from_env().database_url)
    create_schema(engine)
    return engine


@router.post("/review", dependencies=[Depends(require_role(Role.ANALYST))])
def review_candidate(request: AgentReviewRequest) -> dict[str, object]:
    if request.task not in DEFAULT_TASKS:
        raise HTTPException(
            status_code=422,
            detail={
                "error": "unsupported_agent_review_task",
                "supported_tasks": sorted(DEFAULT_TASKS),
            },
        )
    available_at = _aware_utc(request.available_at or datetime.now(UTC))
    try:
        result = run_agent_review(
            _engine(),
            config=AppConfig.from_env(),
            ticker=request.ticker.upper(),
            as_of=request.as_of,
            available_at=available_at,
            task_name=request.task,
            mode=request.mode,
            actor_source="api",
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    if result.status_code >= 400:
        raise HTTPException(status_code=result.status_code, detail=result.payload)
    return result.payload


@router.get("/reviews", dependencies=[Depends(require_role(Role.VIEWER))])
def review_history(
    ticker: Annotated[str | None, Query(min_length=1, max_length=12)] = None,
    task: str | None = None,
    status: str | None = None,
    available_at: datetime | None = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
) -> dict[str, object]:
    try:
        return dashboard_data.load_agent_review_history(
            _engine(),
            available_at=available_at,
            ticker=ticker.upper() if ticker is not None else None,
            task=task,
            status=status,
            limit=limit,
        )
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        msg = "available_at must include timezone information"
        raise ValueError(msg)
    return value.astimezone(UTC)


__all__ = ["router"]

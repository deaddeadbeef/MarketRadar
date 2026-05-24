from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field

from catalyst_radar.agents.review_service import run_agent_review
from catalyst_radar.agents.sdk_orchestrator import run_market_radar_agents
from catalyst_radar.agents.tasks import DEFAULT_TASKS
from catalyst_radar.core.config import AppConfig
from catalyst_radar.dashboard import data as dashboard_data
from catalyst_radar.dashboard.tui import DashboardFilters, dashboard_snapshot_payload
from catalyst_radar.security.access import Role, require_role
from catalyst_radar.storage.budget_repositories import BudgetLedgerRepository
from catalyst_radar.storage.db import create_schema, engine_from_url

router = APIRouter(prefix="/api/agents", tags=["agents"])


class AgentReviewRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ticker: str = Field(min_length=1, max_length=12)
    as_of: date
    available_at: datetime | None = None
    task: str = "skeptic_review"
    mode: Literal["dry_run", "fake", "real"] = "dry_run"


class AgentBriefRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: Literal["dry_run", "real"] = "dry_run"
    execute: bool = False
    ticker: str | None = Field(default=None, min_length=1, max_length=12)
    available_at: datetime | None = None
    priced_in_status: str = "all"
    usefulness: str | None = None
    source_gap: str | None = None
    decision_gap: str | None = None
    scan_limit: int = Field(default=50, ge=1, le=200)
    scan_offset: int = Field(default=0, ge=0)
    telemetry_limit: int = Field(default=8, ge=1, le=200)
    goal: str | None = None
    max_openai_calls: int = Field(default=3, ge=1, le=8)


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


@router.get("/brief", dependencies=[Depends(require_role(Role.VIEWER))])
def agent_brief(
    ticker: Annotated[str | None, Query(min_length=1, max_length=12)] = None,
    available_at: datetime | None = None,
    alert_status: str | None = None,
    alert_route: str | None = None,
    priced_in_status: str = "all",
    usefulness: str | None = None,
    source_gap: str | None = None,
    decision_gap: str | None = None,
    scan_limit: Annotated[int, Query(ge=1, le=200)] = 50,
    scan_offset: Annotated[int, Query(ge=0)] = 0,
    telemetry_limit: Annotated[int, Query(ge=1, le=200)] = 8,
    goal: str | None = None,
) -> dict[str, object]:
    config = AppConfig.from_env()
    filters = DashboardFilters(
        ticker=ticker,
        available_at=available_at,
        alert_status=alert_status,
        alert_route=alert_route,
        priced_in_status=priced_in_status,
        priced_in_usefulness=usefulness,
        priced_in_source_gap=source_gap,
        priced_in_decision_gap=decision_gap,
        priced_in_limit=scan_limit,
        priced_in_offset=scan_offset,
        telemetry_limit=telemetry_limit,
    )
    snapshot = dashboard_snapshot_payload(
        engine=_engine(),
        config=config,
        dotenv_loaded=True,
        filters=filters,
    )
    return run_market_radar_agents(
        snapshot,
        config,
        real=False,
        operator_goal=goal,
    )


@router.post("/brief/run", dependencies=[Depends(require_role(Role.ANALYST))])
def agent_brief_run(request: AgentBriefRunRequest) -> dict[str, object]:
    if request.execute and request.mode != "real":
        raise HTTPException(status_code=422, detail="execute requires mode=real")
    config = AppConfig.from_env()
    engine = _engine()
    filters = DashboardFilters(
        ticker=request.ticker,
        available_at=request.available_at,
        priced_in_status=request.priced_in_status,
        priced_in_usefulness=request.usefulness,
        priced_in_source_gap=request.source_gap,
        priced_in_decision_gap=request.decision_gap,
        priced_in_limit=request.scan_limit,
        priced_in_offset=request.scan_offset,
        telemetry_limit=request.telemetry_limit,
    )
    snapshot = dashboard_snapshot_payload(
        engine=engine,
        config=config,
        dotenv_loaded=True,
        filters=filters,
    )
    return run_market_radar_agents(
        snapshot,
        config,
        real=request.mode == "real",
        operator_goal=request.goal,
        execute=request.execute,
        max_openai_calls=request.max_openai_calls,
        ledger_repo=BudgetLedgerRepository(engine) if request.mode == "real" else None,
    )


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

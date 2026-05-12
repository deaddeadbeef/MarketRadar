from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from catalyst_radar.core.config import AppConfig
from catalyst_radar.dashboard import data as dashboard_data
from catalyst_radar.jobs.scheduler import (
    SchedulerConfig,
    run_once,
    scheduler_run_payload,
)
from catalyst_radar.security.access import Role, require_role
from catalyst_radar.security.licenses import redact_restricted_external_payload
from catalyst_radar.storage.db import create_schema, engine_from_url

router = APIRouter(prefix="/api/radar", tags=["radar"])


def _engine():
    engine = engine_from_url(AppConfig.from_env().database_url)
    create_schema(engine)
    return engine


class RadarRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    as_of: date | None = None
    decision_available_at: datetime | None = None
    outcome_available_at: datetime | None = None
    provider: str | None = None
    universe: str | None = None
    tickers: list[str] = Field(default_factory=list)
    run_llm: bool = False
    llm_dry_run: bool = True
    dry_run_alerts: bool = True


def _dashboard_helper(name: str) -> Callable[..., Any]:
    try:
        return getattr(dashboard_data, name)
    except AttributeError as exc:
        msg = f"dashboard data helper is unavailable: {name}"
        raise RuntimeError(msg) from exc


@router.get("/candidates", dependencies=[Depends(require_role(Role.VIEWER))])
def candidates() -> dict[str, object]:
    load_candidate_rows = _dashboard_helper("load_candidate_rows")
    return {
        "items": redact_restricted_external_payload(load_candidate_rows(_engine()))
    }


@router.get("/candidates/{ticker}", dependencies=[Depends(require_role(Role.VIEWER))])
def candidate_detail(ticker: str) -> dict[str, object]:
    load_ticker_detail = _dashboard_helper("load_ticker_detail")
    detail = load_ticker_detail(_engine(), ticker.upper())
    if detail is None:
        raise HTTPException(status_code=404, detail="candidate not found")
    return redact_restricted_external_payload(detail)


@router.post("/runs", dependencies=[Depends(require_role(Role.ANALYST))])
def run_radar(request: RadarRunRequest) -> dict[str, object]:
    try:
        config = SchedulerConfig(
            owner="api-radar-run",
            as_of=request.as_of,
            decision_available_at=request.decision_available_at,
            outcome_available_at=request.outcome_available_at,
            provider=request.provider,
            universe=request.universe,
            tickers=tuple(request.tickers),
            run_llm=request.run_llm,
            llm_dry_run=request.llm_dry_run,
            dry_run_alerts=request.dry_run_alerts,
        )
        result = run_once(engine=_engine(), config=config)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    payload = scheduler_run_payload(result)
    if not result.acquired_lock:
        raise HTTPException(status_code=409, detail=payload)
    return payload

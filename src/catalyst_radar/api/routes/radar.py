from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from catalyst_radar.core.config import AppConfig
from catalyst_radar.dashboard import data as dashboard_data
from catalyst_radar.jobs.scheduler import (
    SchedulerConfig,
    run_once,
    scheduler_run_payload,
)
from catalyst_radar.jobs.step_outcomes import classify_step_outcome
from catalyst_radar.ops.telemetry import record_telemetry_event
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
def run_radar(
    request: RadarRunRequest,
    x_catalyst_actor: str | None = Header(default=None),
    x_catalyst_role: str | None = Header(default=None),
) -> dict[str, object]:
    engine = _engine()
    run_artifact_id = f"radar-run-api:{uuid4().hex}"
    request_metadata = _radar_run_request_metadata(request)
    record_telemetry_event(
        engine,
        event_name="radar_run.requested",
        status="received",
        actor_source="api",
        actor_id=x_catalyst_actor,
        actor_role=x_catalyst_role,
        artifact_type="radar_run",
        artifact_id=run_artifact_id,
        metadata=request_metadata,
    )
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
        result = run_once(engine=engine, config=config)
    except ValueError as exc:
        record_telemetry_event(
            engine,
            event_name="radar_run.rejected",
            status="rejected",
            actor_source="api",
            actor_id=x_catalyst_actor,
            actor_role=x_catalyst_role,
            artifact_type="radar_run",
            artifact_id=run_artifact_id,
            reason=str(exc),
            metadata=request_metadata,
        )
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        record_telemetry_event(
            engine,
            event_name="radar_run.error",
            status="failed",
            actor_source="api",
            actor_id=x_catalyst_actor,
            actor_role=x_catalyst_role,
            artifact_type="radar_run",
            artifact_id=run_artifact_id,
            reason=str(exc) or exc.__class__.__name__,
            metadata={**request_metadata, "error_type": exc.__class__.__name__},
        )
        raise

    payload = scheduler_run_payload(result)
    if not result.acquired_lock:
        record_telemetry_event(
            engine,
            event_name="radar_run.lock_contention",
            status="blocked",
            actor_source="api",
            actor_id=x_catalyst_actor,
            actor_role=x_catalyst_role,
            artifact_type="radar_run",
            artifact_id=run_artifact_id,
            reason=result.reason,
            metadata={
                **request_metadata,
                "lock_expires_at": (
                    result.lock_expires_at.isoformat()
                    if result.lock_expires_at is not None
                    else None
                ),
            },
            after_payload=payload,
        )
        raise HTTPException(status_code=409, detail=payload)
    record_telemetry_event(
        engine,
        event_name="radar_run.completed",
        status=(result.daily_result.status if result.daily_result is not None else "success"),
        actor_source="api",
        actor_id=x_catalyst_actor,
        actor_role=x_catalyst_role,
        artifact_type="radar_run",
        artifact_id=run_artifact_id,
        metadata={
            **request_metadata,
            **_radar_run_result_metadata(payload),
        },
        after_payload=payload,
    )
    return payload


@router.get("/runs/latest", dependencies=[Depends(require_role(Role.VIEWER))])
def latest_radar_run() -> dict[str, object]:
    load_radar_run_summary = _dashboard_helper("load_radar_run_summary")
    return load_radar_run_summary(_engine())


def _radar_run_request_metadata(request: RadarRunRequest) -> dict[str, object]:
    return {
        "lock_name": "daily-run",
        "as_of": request.as_of.isoformat() if request.as_of is not None else None,
        "decision_available_at": (
            request.decision_available_at.isoformat()
            if request.decision_available_at is not None
            else None
        ),
        "outcome_available_at": (
            request.outcome_available_at.isoformat()
            if request.outcome_available_at is not None
            else None
        ),
        "provider": request.provider,
        "universe": request.universe,
        "tickers": [ticker.upper() for ticker in request.tickers],
        "run_llm": request.run_llm,
        "llm_dry_run": request.llm_dry_run,
        "dry_run_alerts": request.dry_run_alerts,
    }


def _radar_run_result_metadata(payload: dict[str, object]) -> dict[str, object]:
    daily_result = payload.get("daily_result")
    if not isinstance(daily_result, dict):
        return {
            "daily_status": None,
            "step_counts": {},
            "outcome_category_counts": {},
            "skip_reason_counts": {},
            "blocked_steps": [],
            "expected_gate_steps": [],
            "skipped_steps": [],
        }
    steps = daily_result.get("steps")
    if not isinstance(steps, dict):
        return {
            "daily_status": daily_result.get("status"),
            "step_counts": {},
            "outcome_category_counts": {},
            "skip_reason_counts": {},
            "blocked_steps": [],
            "expected_gate_steps": [],
            "skipped_steps": [],
        }
    counts: dict[str, int] = {}
    outcome_category_counts: dict[str, int] = {}
    skip_reason_counts: dict[str, int] = {}
    skipped_steps: list[dict[str, object]] = []
    blocked_steps: list[dict[str, object]] = []
    expected_gate_steps: list[dict[str, object]] = []
    for step_name, step in steps.items():
        status = str(step.get("status") if isinstance(step, dict) else "unknown")
        counts[status] = counts.get(status, 0) + 1
        if isinstance(step, dict):
            reason = str(step.get("reason") or "unspecified")
            category = str(step.get("category") or "")
            classification = (
                classify_step_outcome(status, None if reason == "unspecified" else reason)
                if not category
                else None
            )
            outcome_category = category or classification.category
            outcome_category_counts[outcome_category] = (
                outcome_category_counts.get(outcome_category, 0) + 1
            )
            step_summary = {
                "step": str(step.get("name") or step_name),
                "reason": None if reason == "unspecified" else reason,
                "category": outcome_category,
                "label": step.get("label")
                or (classification.label if classification is not None else None),
                "requested_count": step.get("requested_count"),
                "raw_count": step.get("raw_count"),
                "normalized_count": step.get("normalized_count"),
            }
            if bool(
                step.get("blocks_reliance")
                if "blocks_reliance" in step
                else (
                    classification.blocks_reliance
                    if classification is not None
                    else outcome_category in {"blocked_input", "failed", "needs_review"}
                )
            ):
                blocked_steps.append(step_summary)
            if outcome_category == "expected_gate":
                expected_gate_steps.append(step_summary)
        if status == "skipped" and isinstance(step, dict):
            skip_reason_counts[reason] = skip_reason_counts.get(reason, 0) + 1
            skipped_steps.append(step_summary)
    return {
        "daily_status": daily_result.get("status"),
        "step_counts": dict(sorted(counts.items())),
        "outcome_category_counts": dict(sorted(outcome_category_counts.items())),
        "skip_reason_counts": dict(sorted(skip_reason_counts.items())),
        "blocked_steps": blocked_steps,
        "expected_gate_steps": expected_gate_steps,
        "skipped_steps": skipped_steps,
    }

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from datetime import date as Date
from math import ceil
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
from catalyst_radar.storage.job_repositories import JobLockRepository
from catalyst_radar.universe.seed import seed_polygon_tickers

router = APIRouter(prefix="/api/radar", tags=["radar"])
RADAR_RUN_COOLDOWN_LOCK_NAME = "manual_radar_run_cooldown"
UNIVERSE_SEED_LOCK_NAME = "polygon_ticker_seed"


def _engine():
    engine = engine_from_url(AppConfig.from_env().database_url)
    create_schema(engine)
    return engine


class RadarRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    as_of: Date | None = None
    decision_available_at: datetime | None = None
    outcome_available_at: datetime | None = None
    provider: str | None = None
    universe: str | None = None
    tickers: list[str] = Field(default_factory=list)
    run_llm: bool = False
    llm_dry_run: bool = True
    dry_run_alerts: bool = True


class UniverseSeedRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    provider: str = "polygon"
    date: Date | None = None
    max_pages: int | None = Field(default=None, ge=1)


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
    app_config = AppConfig.from_env()
    run_artifact_id = f"radar-run-api:{uuid4().hex}"
    request_metadata = _radar_run_request_metadata(request, app_config)
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
        _acquire_radar_run_slot(engine, config=app_config, metadata=request_metadata)
        result = run_once(engine=engine, config=config)
    except _RadarRunRateLimited as exc:
        payload = exc.as_payload()
        record_telemetry_event(
            engine,
            event_name="radar_run.rate_limited",
            status="blocked",
            actor_source="api",
            actor_id=x_catalyst_actor,
            actor_role=x_catalyst_role,
            artifact_type="radar_run",
            artifact_id=run_artifact_id,
            reason="rate_limited",
            metadata={**request_metadata, **payload},
        )
        raise HTTPException(
            status_code=429,
            detail=payload,
            headers={"Retry-After": str(payload["retry_after_seconds"])},
        ) from exc
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
    payload = _with_discovery_snapshot(engine, config=app_config, payload=payload)
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
    engine = _engine()
    summary = load_radar_run_summary(engine)
    return _with_discovery_snapshot(
        engine,
        config=AppConfig.from_env(),
        payload=summary,
        radar_run_summary=summary,
    )


@router.post("/universe/seed", dependencies=[Depends(require_role(Role.ANALYST))])
def seed_universe(
    request: UniverseSeedRequest,
    x_catalyst_actor: str | None = Header(default=None),
    x_catalyst_role: str | None = Header(default=None),
) -> dict[str, object]:
    engine = _engine()
    config = AppConfig.from_env()
    artifact_id = f"universe-seed-api:{uuid4().hex}"
    metadata = _universe_seed_request_metadata(request, config)
    record_telemetry_event(
        engine,
        event_name="universe_seed.requested",
        status="received",
        actor_source="api",
        actor_id=x_catalyst_actor,
        actor_role=x_catalyst_role,
        artifact_type="universe_seed",
        artifact_id=artifact_id,
        metadata=metadata,
    )
    try:
        _validate_universe_seed_request(request, config)
        _acquire_universe_seed_slot(engine, config=config, metadata=metadata)
        result = seed_polygon_tickers(
            engine,
            config=config,
            max_pages=request.max_pages,
            date_value=request.date,
        )
    except _UniverseSeedRateLimited as exc:
        payload = exc.as_payload()
        record_telemetry_event(
            engine,
            event_name="universe_seed.rate_limited",
            status="blocked",
            actor_source="api",
            actor_id=x_catalyst_actor,
            actor_role=x_catalyst_role,
            artifact_type="universe_seed",
            artifact_id=artifact_id,
            reason="rate_limited",
            metadata={**metadata, **payload},
        )
        raise HTTPException(
            status_code=429,
            detail=payload,
            headers={"Retry-After": str(payload["retry_after_seconds"])},
        ) from exc
    except ValueError as exc:
        record_telemetry_event(
            engine,
            event_name="universe_seed.rejected",
            status="rejected",
            actor_source="api",
            actor_id=x_catalyst_actor,
            actor_role=x_catalyst_role,
            artifact_type="universe_seed",
            artifact_id=artifact_id,
            reason=str(exc),
            metadata=metadata,
        )
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        reason = str(exc) or exc.__class__.__name__
        record_telemetry_event(
            engine,
            event_name="universe_seed.rejected",
            status="failed",
            actor_source="api",
            actor_id=x_catalyst_actor,
            actor_role=x_catalyst_role,
            artifact_type="universe_seed",
            artifact_id=artifact_id,
            reason=reason,
            metadata={**metadata, "error_type": exc.__class__.__name__},
        )
        raise HTTPException(status_code=503, detail=reason) from exc

    payload = result.as_payload()
    record_telemetry_event(
        engine,
        event_name="universe_seed.completed",
        status="success",
        actor_source="api",
        actor_id=x_catalyst_actor,
        actor_role=x_catalyst_role,
        artifact_type="universe_seed",
        artifact_id=artifact_id,
        metadata={**metadata, "job_id": result.job_id},
        after_payload=payload,
    )
    return payload


def _radar_run_request_metadata(
    request: RadarRunRequest,
    config: AppConfig,
) -> dict[str, object]:
    return {
        "lock_name": "daily-run",
        "cooldown_lock_name": RADAR_RUN_COOLDOWN_LOCK_NAME,
        "min_interval_seconds": config.radar_run_min_interval_seconds,
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


def _with_discovery_snapshot(
    engine,
    *,
    config: AppConfig,
    payload: dict[str, object],
    radar_run_summary: dict[str, object] | None = None,
) -> dict[str, object]:
    summary = (
        radar_run_summary
        if radar_run_summary is not None
        else dashboard_data.load_radar_run_summary(engine)
    )
    return {
        **payload,
        "discovery_snapshot": redact_restricted_external_payload(
            dashboard_data.radar_discovery_snapshot_payload(
                engine,
                config,
                radar_run_summary=summary,
            )
        ),
    }


def _universe_seed_request_metadata(
    request: UniverseSeedRequest,
    config: AppConfig,
) -> dict[str, object]:
    return {
        "provider": request.provider,
        "date": request.date.isoformat() if request.date is not None else None,
        "requested_max_pages": request.max_pages,
        "configured_max_pages": config.polygon_tickers_max_pages,
        "min_interval_seconds": config.polygon_ticker_seed_min_interval_seconds,
    }


def _validate_universe_seed_request(
    request: UniverseSeedRequest,
    config: AppConfig,
) -> None:
    provider = str(request.provider or "").strip().lower()
    if provider != "polygon":
        msg = "only provider=polygon is supported for universe seed"
        raise ValueError(msg)
    if (
        request.max_pages is not None
        and request.max_pages > config.polygon_tickers_max_pages
    ):
        msg = (
            "max_pages exceeds configured cap "
            f"CATALYST_POLYGON_TICKERS_MAX_PAGES={config.polygon_tickers_max_pages}"
        )
        raise ValueError(msg)


class _UniverseSeedRateLimited(RuntimeError):
    def __init__(self, *, retry_after_seconds: int, reset_at: datetime | None) -> None:
        self.retry_after_seconds = retry_after_seconds
        self.reset_at = reset_at
        super().__init__(f"universe seed is rate limited for {retry_after_seconds}s")

    def as_payload(self) -> dict[str, object]:
        return {
            "operation": "polygon_ticker_seed",
            "retry_after_seconds": self.retry_after_seconds,
            "reset_at": self.reset_at.isoformat() if self.reset_at is not None else None,
        }


class _RadarRunRateLimited(RuntimeError):
    def __init__(self, *, retry_after_seconds: int, reset_at: datetime | None) -> None:
        self.retry_after_seconds = retry_after_seconds
        self.reset_at = reset_at
        super().__init__(f"radar run is rate limited for {retry_after_seconds}s")

    def as_payload(self) -> dict[str, object]:
        return {
            "operation": "manual_radar_run",
            "retry_after_seconds": self.retry_after_seconds,
            "reset_at": self.reset_at.isoformat() if self.reset_at is not None else None,
        }


def _acquire_radar_run_slot(
    engine,
    *,
    config: AppConfig,
    metadata: dict[str, object],
) -> None:
    now = datetime.now(UTC)
    result = JobLockRepository(engine).acquire(
        RADAR_RUN_COOLDOWN_LOCK_NAME,
        owner=f"api-radar-run-cooldown:{uuid4().hex}",
        ttl=timedelta(seconds=config.radar_run_min_interval_seconds),
        now=now,
        metadata={
            "operation": "manual_radar_run",
            **metadata,
        },
    )
    if result.acquired:
        return
    raise _RadarRunRateLimited(
        retry_after_seconds=_retry_after_seconds(result.expires_at, now),
        reset_at=result.expires_at,
    )


def _acquire_universe_seed_slot(
    engine,
    *,
    config: AppConfig,
    metadata: dict[str, object],
) -> None:
    now = datetime.now(UTC)
    result = JobLockRepository(engine).acquire(
        UNIVERSE_SEED_LOCK_NAME,
        owner=f"api-universe-seed:{uuid4().hex}",
        ttl=timedelta(seconds=config.polygon_ticker_seed_min_interval_seconds),
        now=now,
        metadata={
            "operation": "polygon_ticker_seed",
            **metadata,
        },
    )
    if result.acquired:
        return
    raise _UniverseSeedRateLimited(
        retry_after_seconds=_retry_after_seconds(result.expires_at, now),
        reset_at=result.expires_at,
    )


def _retry_after_seconds(reset_at: datetime | None, now: datetime) -> int:
    if reset_at is None:
        return 1
    return max(1, int(ceil((reset_at.astimezone(UTC) - now).total_seconds())))


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

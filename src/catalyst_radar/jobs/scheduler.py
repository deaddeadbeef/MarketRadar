from __future__ import annotations

import os
import socket
import time as time_module
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from enum import Enum
from typing import Any

from sqlalchemy import Engine

from catalyst_radar.jobs.tasks import DailyRunResult, DailyRunSpec, JobStepResult, run_daily
from catalyst_radar.storage.job_repositories import JobLockRepository


@dataclass(frozen=True)
class SchedulerConfig:
    owner: str
    lock_name: str = "daily-run"
    lock_ttl: timedelta = timedelta(minutes=45)
    run_interval: timedelta = timedelta(hours=24)
    as_of: date | None = None
    decision_available_at: datetime | None = None
    outcome_available_at: datetime | None = None
    run_llm: bool = False
    llm_dry_run: bool = True
    dry_run_alerts: bool = True

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> SchedulerConfig:
        source = os.environ if environ is None else environ
        return cls(
            owner=_optional_text(source, "CATALYST_WORKER_OWNER") or _default_owner(),
            lock_name=_optional_text(source, "CATALYST_WORKER_LOCK_NAME") or "daily-run",
            lock_ttl=_duration_seconds(
                source,
                "CATALYST_WORKER_LOCK_TTL_SECONDS",
                timedelta(minutes=45),
            ),
            run_interval=_duration_seconds(
                source,
                "CATALYST_WORKER_INTERVAL_SECONDS",
                timedelta(hours=24),
            ),
            as_of=_optional_date(source, "CATALYST_DAILY_AS_OF"),
            decision_available_at=_optional_datetime(
                source,
                "CATALYST_DECISION_AVAILABLE_AT",
            ),
            outcome_available_at=_optional_datetime(
                source,
                "CATALYST_OUTCOME_AVAILABLE_AT",
            ),
            run_llm=_bool(source, "CATALYST_RUN_LLM", False),
            llm_dry_run=_bool(source, "CATALYST_LLM_DRY_RUN", True),
            dry_run_alerts=_bool(source, "CATALYST_DRY_RUN_ALERTS", True),
        )


@dataclass(frozen=True)
class SchedulerRunResult:
    acquired_lock: bool
    reason: str | None
    daily_result: DailyRunResult | None


def build_daily_spec(
    config: SchedulerConfig,
    *,
    now: datetime | None = None,
) -> DailyRunSpec:
    resolved_now = _to_utc(now or datetime.now(UTC), "now")
    return DailyRunSpec(
        as_of=config.as_of or resolved_now.date(),
        decision_available_at=config.decision_available_at or resolved_now,
        outcome_available_at=config.outcome_available_at,
        dry_run_alerts=config.dry_run_alerts,
        run_llm=config.run_llm,
        llm_dry_run=config.llm_dry_run,
    )


def run_once(
    *,
    engine: Engine,
    config: SchedulerConfig,
    now: datetime | None = None,
) -> SchedulerRunResult:
    resolved_now = _to_utc(now or datetime.now(UTC), "now")
    spec = build_daily_spec(config, now=resolved_now)
    repo = JobLockRepository(engine)
    lock = repo.acquire(
        config.lock_name,
        owner=config.owner,
        ttl=config.lock_ttl,
        now=resolved_now,
        metadata=_lock_metadata(config, spec),
    )
    if not lock.acquired:
        return SchedulerRunResult(
            acquired_lock=False,
            reason="lock_held",
            daily_result=None,
        )

    try:
        daily_result = run_daily(spec, engine=engine)
        return SchedulerRunResult(
            acquired_lock=True,
            reason=None,
            daily_result=daily_result,
        )
    finally:
        repo.release(config.lock_name, owner=config.owner)


def run_forever(*, engine: Engine, config: SchedulerConfig) -> None:
    interval_seconds = config.run_interval.total_seconds()
    if interval_seconds <= 0:
        run_once(engine=engine, config=config)
        return
    while True:
        run_once(engine=engine, config=config)
        time_module.sleep(interval_seconds)


def scheduler_run_payload(result: SchedulerRunResult) -> dict[str, Any]:
    return {
        "acquired_lock": result.acquired_lock,
        "reason": result.reason,
        "daily_result": (
            daily_run_payload(result.daily_result)
            if result.daily_result is not None
            else None
        ),
    }


def daily_run_payload(result: DailyRunResult) -> dict[str, Any]:
    return {
        "status": result.status,
        "spec": daily_spec_payload(result.spec),
        "steps": {step.name: step_payload(step) for step in result.steps},
    }


def daily_spec_payload(spec: DailyRunSpec) -> dict[str, Any]:
    return {
        "as_of": spec.as_of.isoformat(),
        "decision_available_at": spec.decision_available_at.isoformat(),
        "outcome_available_at": (
            spec.outcome_available_at.isoformat()
            if spec.outcome_available_at is not None
            else None
        ),
        "provider": spec.provider,
        "universe": spec.universe,
        "tickers": list(spec.tickers),
        "dry_run_alerts": spec.dry_run_alerts,
        "run_llm": spec.run_llm,
        "llm_dry_run": spec.llm_dry_run,
    }


def step_payload(step: JobStepResult) -> dict[str, Any]:
    return {
        "name": step.name,
        "status": step.status,
        "job_id": step.job_id,
        "requested_count": step.requested_count,
        "raw_count": step.raw_count,
        "normalized_count": step.normalized_count,
        "reason": step.reason,
        "payload": _json_safe(step.payload),
    }


def _lock_metadata(config: SchedulerConfig, spec: DailyRunSpec) -> dict[str, Any]:
    return {
        "owner": config.owner,
        "as_of": spec.as_of.isoformat(),
        "decision_available_at": spec.decision_available_at.isoformat(),
        "outcome_available_at": (
            spec.outcome_available_at.isoformat()
            if spec.outcome_available_at is not None
            else None
        ),
        "dry_run_alerts": spec.dry_run_alerts,
        "run_llm": spec.run_llm,
        "llm_dry_run": spec.llm_dry_run,
    }


def _optional_text(source: Mapping[str, str], key: str) -> str | None:
    raw = source.get(key)
    if raw is None:
        return None
    value = raw.strip()
    return value or None


def _duration_seconds(
    source: Mapping[str, str],
    key: str,
    default: timedelta,
) -> timedelta:
    raw = source.get(key)
    if raw is None or raw.strip() == "":
        return default
    try:
        return timedelta(seconds=float(raw))
    except ValueError as exc:
        msg = f"{key} must be a number of seconds"
        raise ValueError(msg) from exc


def _optional_date(source: Mapping[str, str], key: str) -> date | None:
    raw = source.get(key)
    if raw is None or raw.strip() == "":
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        msg = f"{key} must be an ISO date"
        raise ValueError(msg) from exc


def _optional_datetime(source: Mapping[str, str], key: str) -> datetime | None:
    raw = source.get(key)
    if raw is None or raw.strip() == "":
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        msg = f"{key} must be an ISO datetime"
        raise ValueError(msg) from exc
    return _to_utc(parsed, key)


def _bool(source: Mapping[str, str], key: str, default: bool) -> bool:
    raw = source.get(key)
    if raw is None or raw.strip() == "":
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    msg = f"{key} must be one of 1/true/yes/on or 0/false/no/off"
    raise ValueError(msg)


def _to_utc(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        msg = f"{field_name} must be timezone-aware"
        raise ValueError(msg)
    return value.astimezone(UTC)


def _default_owner() -> str:
    hostname = socket.gethostname() or "localhost"
    return f"{hostname}:{os.getpid()}"


def _json_safe(value: Any) -> Any:
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, tuple | list):
        return [_json_safe(item) for item in value]
    return value


__all__ = [
    "SchedulerConfig",
    "SchedulerRunResult",
    "build_daily_spec",
    "daily_run_payload",
    "daily_spec_payload",
    "run_forever",
    "run_once",
    "scheduler_run_payload",
    "step_payload",
]

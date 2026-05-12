from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from math import ceil
from typing import Any
from uuid import uuid4

from sqlalchemy import Engine, select

from catalyst_radar.core.config import AppConfig
from catalyst_radar.storage.job_repositories import JobLockRepository
from catalyst_radar.storage.schema import job_locks

SCHWAB_PORTFOLIO_SYNC_OPERATION = "portfolio_sync"
SCHWAB_MARKET_SYNC_OPERATION = "market_context_sync"


@dataclass(frozen=True)
class SchwabRateLimitState:
    operation: str
    allowed: bool
    min_interval_seconds: int
    retry_after_seconds: int
    reset_at: datetime | None

    def as_payload(self) -> dict[str, object]:
        return {
            "operation": self.operation,
            "allowed": self.allowed,
            "min_interval_seconds": self.min_interval_seconds,
            "retry_after_seconds": self.retry_after_seconds,
            "reset_at": self.reset_at.isoformat() if self.reset_at is not None else None,
        }


class SchwabRateLimitExceeded(RuntimeError):
    def __init__(self, state: SchwabRateLimitState) -> None:
        self.state = state
        super().__init__(
            f"Schwab {state.operation} is rate limited; retry after "
            f"{state.retry_after_seconds} seconds"
        )


def acquire_schwab_api_slot(
    engine: Engine,
    *,
    operation: str,
    min_interval_seconds: int,
    now: datetime | None = None,
    metadata: dict[str, Any] | None = None,
) -> SchwabRateLimitState:
    resolved_now = _to_utc(now or datetime.now(UTC))
    _validate_interval(min_interval_seconds)
    owner = f"{operation}:{uuid4()}"
    result = JobLockRepository(engine).acquire(
        _lock_name(operation),
        owner=owner,
        ttl=timedelta(seconds=min_interval_seconds),
        now=resolved_now,
        metadata={
            "broker": "schwab",
            "operation": operation,
            "min_interval_seconds": min_interval_seconds,
            **dict(metadata or {}),
        },
    )
    if result.acquired:
        return SchwabRateLimitState(
            operation=operation,
            allowed=True,
            min_interval_seconds=min_interval_seconds,
            retry_after_seconds=0,
            reset_at=result.expires_at,
        )
    state = SchwabRateLimitState(
        operation=operation,
        allowed=False,
        min_interval_seconds=min_interval_seconds,
        retry_after_seconds=_retry_after_seconds(result.expires_at, resolved_now),
        reset_at=result.expires_at,
    )
    raise SchwabRateLimitExceeded(state)


def schwab_rate_limit_status(
    engine: Engine,
    *,
    config: AppConfig,
    now: datetime | None = None,
) -> list[dict[str, object]]:
    resolved_now = _to_utc(now or datetime.now(UTC))
    settings = {
        SCHWAB_PORTFOLIO_SYNC_OPERATION: config.schwab_sync_min_interval_seconds,
        SCHWAB_MARKET_SYNC_OPERATION: config.schwab_market_sync_min_interval_seconds,
    }
    locks = _active_locks(engine, settings.keys(), now=resolved_now)
    rows: list[dict[str, object]] = []
    for operation, interval_seconds in settings.items():
        _validate_interval(interval_seconds)
        reset_at = locks.get(operation)
        allowed = reset_at is None or reset_at <= resolved_now
        rows.append(
            SchwabRateLimitState(
                operation=operation,
                allowed=allowed,
                min_interval_seconds=interval_seconds,
                retry_after_seconds=(
                    0 if allowed else _retry_after_seconds(reset_at, resolved_now)
                ),
                reset_at=reset_at,
            ).as_payload()
        )
    return rows


def schwab_rate_limit_config_payload(config: AppConfig) -> dict[str, object]:
    return {
        "portfolio_sync_min_interval_seconds": config.schwab_sync_min_interval_seconds,
        "market_sync_min_interval_seconds": config.schwab_market_sync_min_interval_seconds,
        "market_sync_max_tickers": config.schwab_market_sync_max_tickers,
    }


def _active_locks(
    engine: Engine,
    operations: Any,
    *,
    now: datetime,
) -> dict[str, datetime]:
    names = {_lock_name(str(operation)): str(operation) for operation in operations}
    if not names:
        return {}
    with engine.connect() as conn:
        rows = conn.execute(
            select(job_locks.c.lock_name, job_locks.c.expires_at).where(
                job_locks.c.lock_name.in_(names.keys()),
                job_locks.c.expires_at > now,
            )
        )
    return {names[str(row.lock_name)]: _to_utc(row.expires_at) for row in rows}


def _lock_name(operation: str) -> str:
    normalized = str(operation or "").strip().lower()
    if not normalized:
        raise ValueError("operation is required")
    return f"schwab_api:{normalized}"


def _retry_after_seconds(reset_at: datetime | None, now: datetime) -> int:
    if reset_at is None:
        return 1
    return max(1, int(ceil((_to_utc(reset_at) - now).total_seconds())))


def _to_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _validate_interval(seconds: int) -> None:
    if int(seconds) <= 0:
        raise ValueError("Schwab API rate-limit interval must be greater than zero")

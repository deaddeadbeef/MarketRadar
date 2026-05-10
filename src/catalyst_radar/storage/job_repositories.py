from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import Engine, delete, select, update
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from catalyst_radar.core.immutability import thaw_json_value
from catalyst_radar.storage.schema import job_locks


@dataclass(frozen=True)
class JobLockAcquireResult:
    lock_name: str
    owner: str
    acquired: bool
    current_owner: str | None
    expires_at: datetime | None


class JobLockRepository:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def acquire(
        self,
        lock_name: str,
        *,
        owner: str,
        ttl: timedelta,
        now: datetime | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> JobLockAcquireResult:
        resolved_now = _to_utc(now or datetime.now(UTC), "now")
        expires_at = resolved_now + ttl
        payload = thaw_json_value(metadata or {})
        with self.engine.begin() as conn:
            result = conn.execute(
                _acquire_statement(
                    dialect_name=conn.dialect.name,
                    lock_name=lock_name,
                    owner=owner,
                    acquired_at=resolved_now,
                    heartbeat_at=resolved_now,
                    expires_at=expires_at,
                    metadata=payload,
                )
            )
            if result.rowcount == 1:
                return JobLockAcquireResult(
                    lock_name=lock_name,
                    owner=owner,
                    acquired=True,
                    current_owner=owner,
                    expires_at=expires_at,
                )
            row = conn.execute(
                select(job_locks).where(job_locks.c.lock_name == lock_name).limit(1)
            ).one()
        current = row._mapping
        return JobLockAcquireResult(
            lock_name=lock_name,
            owner=owner,
            acquired=False,
            current_owner=str(current["owner"]),
            expires_at=_as_utc(current["expires_at"]),
        )

    def heartbeat(
        self,
        lock_name: str,
        *,
        owner: str,
        ttl: timedelta,
        now: datetime | None = None,
    ) -> bool:
        resolved_now = _to_utc(now or datetime.now(UTC), "now")
        with self.engine.begin() as conn:
            result = conn.execute(
                update(job_locks)
                .where(job_locks.c.lock_name == lock_name, job_locks.c.owner == owner)
                .values(heartbeat_at=resolved_now, expires_at=resolved_now + ttl)
            )
        return result.rowcount == 1

    def release(self, lock_name: str, *, owner: str) -> bool:
        with self.engine.begin() as conn:
            result = conn.execute(
                delete(job_locks).where(
                    job_locks.c.lock_name == lock_name,
                    job_locks.c.owner == owner,
                )
            )
        return result.rowcount == 1


def _acquire_statement(
    *,
    dialect_name: str,
    lock_name: str,
    owner: str,
    acquired_at: datetime,
    heartbeat_at: datetime,
    expires_at: datetime,
    metadata: Any,
):
    insert_factory = postgresql_insert if dialect_name == "postgresql" else sqlite_insert
    stmt = insert_factory(job_locks).values(
        lock_name=lock_name,
        owner=owner,
        acquired_at=acquired_at,
        heartbeat_at=heartbeat_at,
        expires_at=expires_at,
        metadata=metadata,
    )
    return stmt.on_conflict_do_update(
        index_elements=[job_locks.c.lock_name],
        set_={
            "owner": owner,
            "acquired_at": acquired_at,
            "heartbeat_at": heartbeat_at,
            "expires_at": expires_at,
            "metadata": metadata,
        },
        where=job_locks.c.expires_at <= acquired_at,
    )


def _to_utc(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        msg = f"{field_name} must be timezone-aware"
        raise ValueError(msg)
    return value.astimezone(UTC)


def _as_utc(value: datetime | str) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


__all__ = ["JobLockAcquireResult", "JobLockRepository"]

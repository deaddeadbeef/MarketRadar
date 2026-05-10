from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import create_engine, inspect

from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.job_repositories import JobLockRepository


def _engine():
    engine = create_engine("sqlite:///:memory:", future=True)
    create_schema(engine)
    return engine


def test_create_schema_adds_job_locks_table():
    engine = _engine()

    assert "job_locks" in inspect(engine).get_table_names()


def test_job_lock_rejects_unexpired_owner_and_allows_expired_takeover():
    engine = _engine()
    repo = JobLockRepository(engine)
    now = datetime(2026, 5, 10, 1, 0, tzinfo=UTC)

    first = repo.acquire(
        "daily-run",
        owner="worker-a",
        ttl=timedelta(minutes=10),
        now=now,
        metadata={"as_of": "2026-05-09"},
    )
    blocked = repo.acquire(
        "daily-run",
        owner="worker-b",
        ttl=timedelta(minutes=10),
        now=now + timedelta(minutes=1),
    )
    stolen = repo.acquire(
        "daily-run",
        owner="worker-b",
        ttl=timedelta(minutes=10),
        now=now + timedelta(minutes=11),
    )

    assert first.acquired is True
    assert blocked.acquired is False
    assert blocked.current_owner == "worker-a"
    assert stolen.acquired is True
    assert stolen.current_owner == "worker-b"


def test_job_lock_heartbeat_and_release_require_matching_owner():
    engine = _engine()
    repo = JobLockRepository(engine)
    now = datetime(2026, 5, 10, 1, 0, tzinfo=UTC)
    repo.acquire("daily-run", owner="worker-a", ttl=timedelta(minutes=10), now=now)

    assert (
        repo.heartbeat(
            "daily-run",
            owner="worker-b",
            ttl=timedelta(minutes=10),
            now=now,
        )
        is False
    )
    assert repo.release("daily-run", owner="worker-b") is False
    assert (
        repo.heartbeat(
            "daily-run",
            owner="worker-a",
            ttl=timedelta(minutes=10),
            now=now,
        )
        is True
    )
    assert repo.release("daily-run", owner="worker-a") is True
    assert (
        repo.acquire(
            "daily-run",
            owner="worker-b",
            ttl=timedelta(minutes=10),
            now=now,
        ).acquired
        is True
    )

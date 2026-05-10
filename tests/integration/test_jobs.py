from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pytest
from sqlalchemy import create_engine, inspect, select

from catalyst_radar.jobs.tasks import DAILY_STEP_ORDER, DailyRunSpec, run_daily
from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.job_repositories import JobLockRepository
from catalyst_radar.storage.schema import job_locks, job_runs, validation_runs


def _engine():
    engine = create_engine("sqlite:///:memory:", future=True)
    create_schema(engine)
    return engine


def test_create_schema_adds_job_locks_table():
    engine = _engine()

    assert "job_locks" in inspect(engine).get_table_names()


def test_daily_run_requires_timezone_aware_available_at():
    with pytest.raises(
        ValueError,
        match="decision_available_at must be timezone-aware",
    ):
        DailyRunSpec(
            as_of=date(2026, 5, 9),
            decision_available_at=datetime(2026, 5, 10, 1, 0),
        )


def test_daily_run_records_skipped_steps_without_llm_or_inputs():
    engine = _engine()
    spec = DailyRunSpec(
        as_of=date(2026, 5, 9),
        decision_available_at=datetime(2026, 5, 10, 1, 0, tzinfo=UTC),
        run_llm=False,
        dry_run_alerts=True,
    )

    result = run_daily(spec, engine=engine)

    assert result.status in {"success", "partial_success"}
    assert result.step("daily_bar_ingest").status == "skipped"
    assert result.step("llm_review").status == "skipped"
    assert result.step("digest").status in {"success", "skipped"}

    with engine.connect() as conn:
        rows = conn.execute(
            select(
                job_runs.c.job_type,
                job_runs.c.status,
                job_runs.c.metadata,
            )
        ).all()

    persisted = {row.job_type: row for row in rows}
    assert set(persisted) == set(DAILY_STEP_ORDER)
    assert persisted["daily_bar_ingest"].status == "skipped"
    assert persisted["llm_review"].status == "skipped"
    assert persisted["digest"].status in {"success", "skipped"}
    assert persisted["daily_bar_ingest"].metadata["as_of"] == "2026-05-09"
    assert (
        persisted["daily_bar_ingest"].metadata["decision_available_at"]
        == "2026-05-10T01:00:00+00:00"
    )


def test_daily_run_runs_validation_update_when_outcome_cutoff_is_supplied():
    engine = _engine()
    outcome_available_at = datetime(2026, 6, 10, 1, 0, tzinfo=UTC)
    spec = DailyRunSpec(
        as_of=date(2026, 5, 9),
        decision_available_at=datetime(2026, 5, 10, 1, 0, tzinfo=UTC),
        outcome_available_at=outcome_available_at,
        run_llm=False,
        dry_run_alerts=True,
    )

    result = run_daily(spec, engine=engine)

    validation_step = result.step("validation_update")
    assert validation_step.status == "success"
    assert validation_step.reason is None
    assert validation_step.payload["candidate_count"] == 0
    with engine.connect() as conn:
        run = conn.execute(select(validation_runs)).one()

    assert run.status == "success"
    assert run.config["outcome_available_at"] == outcome_available_at.isoformat()
    assert run.metrics["candidate_count"] == 0


def test_daily_run_marks_validation_run_failed_when_validation_update_fails(monkeypatch):
    engine = _engine()
    outcome_available_at = datetime(2026, 6, 10, 1, 0, tzinfo=UTC)
    spec = DailyRunSpec(
        as_of=date(2026, 5, 9),
        decision_available_at=datetime(2026, 5, 10, 1, 0, tzinfo=UTC),
        outcome_available_at=outcome_available_at,
        run_llm=False,
        dry_run_alerts=True,
    )

    def fail_replay(*args, **kwargs):
        del args, kwargs
        raise RuntimeError("forced replay failure")

    monkeypatch.setattr("catalyst_radar.jobs.tasks.build_replay_results", fail_replay)

    result = run_daily(spec, engine=engine)

    validation_step = result.step("validation_update")
    assert validation_step.status == "failed"
    assert validation_step.reason == "forced replay failure"
    with engine.connect() as conn:
        run = conn.execute(select(validation_runs)).one()

    assert run.status == "failed"
    assert run.metrics == {
        "error": "forced replay failure",
        "error_type": "RuntimeError",
    }


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

    blocked_after_takeover = repo.acquire(
        "daily-run",
        owner="worker-c",
        ttl=timedelta(minutes=10),
        now=now + timedelta(minutes=12),
    )

    assert blocked_after_takeover.acquired is False
    assert blocked_after_takeover.current_owner == "worker-b"
    with engine.connect() as conn:
        row = conn.execute(
            select(job_locks.c.owner, job_locks.c.expires_at).where(
                job_locks.c.lock_name == "daily-run"
            )
        ).one()
    assert row.owner == "worker-b"
    assert row.expires_at == stolen.expires_at.replace(tzinfo=None)


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

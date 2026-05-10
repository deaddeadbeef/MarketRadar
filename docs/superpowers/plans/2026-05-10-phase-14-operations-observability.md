# Phase 14 Operations, Scheduling, and Observability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Catalyst Radar run as a reliable daily research assistant with non-overlapping jobs, visible operational health, degraded-mode controls, local worker runtime, Docker packaging, and runbooks.

**Architecture:** Reuse the existing durable `job_runs`, `provider_health`, `data_quality_incidents`, `budget_ledger`, `validation_runs`, and alert tables as the source of truth. Add one missing concurrency primitive (`job_locks`), a small jobs package that orchestrates existing pipeline APIs, and an ops package that computes health, metrics, degraded mode, and drift from persisted rows. The worker and CLI call the same scheduler functions so local, Docker, and VM execution have identical behavior.

**Tech Stack:** Python 3.12, SQLAlchemy Core, SQLite/Postgres-compatible schema, existing CLI/API/dashboard stack, FastAPI, Streamlit, Docker Compose.

---

## Current Baseline

- Branch: `feature/phase-14-operations-observability`
- Worktree: `.worktrees/phase-14-operations-observability`
- Base commit: `1c3d039 docs: update phase 13 verification`
- Existing ops storage already present:
  - `provider_health`
  - `job_runs`
  - `data_quality_incidents`
  - `budget_ledger`
  - `validation_runs`
  - `validation_results`
  - `useful_alert_labels`
  - `alerts`
  - `alert_suppressions`
  - `user_feedback`
- Missing storage:
  - `job_locks`
- OpenAI API note:
  - Phase 14 must not require an OpenAI key. The worker defaults to deterministic, dry-run-safe behavior. Real OpenAI provider smoke remains gated on `OPENAI_API_KEY`, and the implementation should continue to fail closed when it is missing.

## File Structure

### Create

- `apps/worker/main.py`
  - Thin runtime entry point for Docker/VM worker execution.
- `src/catalyst_radar/jobs/__init__.py`
  - Package exports for scheduler and task APIs.
- `src/catalyst_radar/jobs/tasks.py`
  - Daily run dataclasses and orchestration of deterministic pipeline steps.
- `src/catalyst_radar/jobs/scheduler.py`
  - Lock acquisition, run-once, run-loop, and environment parsing.
- `src/catalyst_radar/ops/__init__.py`
  - Package exports for health and metrics APIs.
- `src/catalyst_radar/ops/health.py`
  - Provider banners, stale data detection, degraded mode, and runbook links.
- `src/catalyst_radar/ops/metrics.py`
  - Stage counts, cost metrics, useful-alert metrics, incident metrics, unsupported-claim rate, false-positive rate, score drift.
- `src/catalyst_radar/ops/runbooks.py`
  - Stable runbook IDs and relative doc paths for API/dashboard display.
- `src/catalyst_radar/storage/job_repositories.py`
  - Job lock model and repository.
- `infra/docker/Dockerfile`
  - Shared Python image for API/dashboard/worker.
- `infra/docker/docker-compose.prod.yml`
  - Production-style compose using externally supplied env files and persisted Postgres volume.
- `docs/runbooks/provider-failure.md`
- `docs/runbooks/llm-failure.md`
- `docs/runbooks/score-drift.md`
- `docs/phase-14-review.md`
- `tests/integration/test_jobs.py`
- `tests/integration/test_ops_health.py`

### Modify

- `src/catalyst_radar/storage/schema.py`
  - Add `job_locks` table and indexes.
- `src/catalyst_radar/storage/db.py`
  - Add SQLite upgrade path for `job_locks`.
- `src/catalyst_radar/cli.py`
  - Add `run-daily` command wired to the scheduler.
- `src/catalyst_radar/dashboard/data.py`
  - Delegate ops health loading to `catalyst_radar.ops.health`.
- `src/catalyst_radar/api/routes/ops.py`
  - Keep `/api/ops/health` stable while returning the richer payload.
- `apps/dashboard/pages/5_Ops.py`
  - Render provider banners, degraded mode, metrics, incidents, drift, and job status.
- `docker-compose.yml`
  - Extend local compose from Postgres-only to Postgres/API/dashboard/worker.
- `docs/superpowers/plans/2026-05-09-full-product-implementation.md`
  - Mark Phase 14 complete after verification.

---

## Behavioral Contracts

### Job Locks

- A lock is identified by `lock_name`.
- A lock has `owner`, `acquired_at`, `heartbeat_at`, `expires_at`, and JSON `metadata`.
- `acquire()` succeeds when no row exists or `expires_at <= now`.
- `acquire()` fails when an unexpired row exists for a different owner.
- `heartbeat()` updates only when `(lock_name, owner)` match.
- `release()` deletes only when `(lock_name, owner)` match.
- Stale locks are intentionally stealable so a crashed worker does not block future runs.

### Daily Run

- `DailyRunSpec.decision_available_at` must be timezone-aware UTC.
- `DailyRunSpec.as_of` is a market date, not the current date.
- The deterministic scan timestamp is `as_of` at 21:00 UTC.
- Downstream steps use the same `decision_available_at` so historical/replay semantics remain clean.
- LLM review is disabled unless `run_llm=True`.
- LLM provider calls are fake/dry-run unless explicitly configured otherwise.
- External alert delivery remains dry-run unless existing alert channels are separately configured.
- A step that has no eligible inputs returns `status="skipped"` with a reason and still records a job row.

### Degraded Mode

- Core data is stale when latest candidate state `as_of` is older than the configured freshness window or when a provider health row reports `stale`, `unhealthy`, `degraded`, `down`, `failed`, or `error`.
- When core data is stale, ops health returns:
  - `degraded_mode.enabled == true`
  - `degraded_mode.max_action_state == "AddToWatchlist"`
  - `degraded_mode.disabled_states == ["Warning", "EligibleForManualBuyReview", "ThesisWeakening", "ExitInvalidateReview"]`
- This phase exposes the degraded-mode control in ops payload/dashboard. Enforcement inside scoring policy is covered by the same stale-data check where the daily job decides whether to run LLM/card escalation.

### Metrics

- Stage counts come from `job_runs` and current candidate state distribution.
- Cost metrics come from `BudgetLedgerRepository.summary()`.
- Useful alerts come from `ValidationRepository.list_useful_alert_labels()`.
- Stale incidents and schema failures come from `data_quality_incidents`.
- Unsupported-claim rate comes from `budget_ledger` rows with schema-rejected source-faithfulness or schema-validation reasons divided by total LLM attempts.
- False-positive rate comes from latest validation run metrics when available.
- Score drift compares latest candidate score distribution with the previous `as_of` distribution.

---

## Task 1: Baseline Verification and Plan Commit

**Files:**
- Create: `docs/superpowers/plans/2026-05-10-phase-14-operations-observability.md`

- [ ] **Step 1: Run focused baseline tests**

Run:

```powershell
python -m pytest tests/integration/test_dashboard_data.py::test_load_ops_health_reports_provider_status_and_database tests/integration/test_api_routes.py::test_get_ops_health tests/integration/test_provider_storage.py::test_job_run_transitions_from_running_to_terminal_status -q
```

Expected:

```text
3 passed
```

If a test name has drifted, find the exact current names with:

```powershell
rg "load_ops_health|ops_health|records_job_lifecycle" tests/integration -n
```

Then run the matching focused tests before moving on.

- [ ] **Step 2: Run focused lint baseline**

Run:

```powershell
python -m ruff check src/catalyst_radar/dashboard/data.py src/catalyst_radar/api/routes/ops.py src/catalyst_radar/storage/provider_repositories.py tests/integration/test_dashboard_data.py tests/integration/test_api_routes.py tests/integration/test_provider_storage.py
```

Expected:

```text
All checks passed!
```

- [ ] **Step 3: Commit this plan**

Run:

```powershell
git add docs/superpowers/plans/2026-05-10-phase-14-operations-observability.md
git commit -m "docs: add phase 14 operations plan"
```

Expected: one commit containing only the phase plan.

---

## Task 2: Job Lock Storage

**Files:**
- Modify: `src/catalyst_radar/storage/schema.py`
- Modify: `src/catalyst_radar/storage/db.py`
- Create: `src/catalyst_radar/storage/job_repositories.py`
- Test: `tests/integration/test_jobs.py`

- [ ] **Step 1: Write failing lock acquisition tests**

Add to `tests/integration/test_jobs.py`:

```python
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

    assert repo.heartbeat("daily-run", owner="worker-b", ttl=timedelta(minutes=10), now=now) is False
    assert repo.release("daily-run", owner="worker-b") is False
    assert repo.heartbeat("daily-run", owner="worker-a", ttl=timedelta(minutes=10), now=now) is True
    assert repo.release("daily-run", owner="worker-a") is True
    assert repo.acquire("daily-run", owner="worker-b", ttl=timedelta(minutes=10), now=now).acquired is True
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```powershell
python -m pytest tests/integration/test_jobs.py -q
```

Expected: fail because `catalyst_radar.storage.job_repositories` or `job_locks` does not exist.

- [ ] **Step 3: Add `job_locks` table**

In `src/catalyst_radar/storage/schema.py`, add imports only if needed and define:

```python
job_locks = Table(
    "job_locks",
    metadata,
    Column("lock_name", String, primary_key=True),
    Column("owner", String, nullable=False),
    Column("acquired_at", DateTime(timezone=True), nullable=False),
    Column("heartbeat_at", DateTime(timezone=True), nullable=False),
    Column("expires_at", DateTime(timezone=True), nullable=False),
    Column("metadata", json_type, nullable=False),
)
```

Add indexes near the existing job/provider indexes:

```python
Index("ix_job_locks_expires_at", job_locks.c.expires_at)
Index("ix_job_locks_owner", job_locks.c.owner)
```

- [ ] **Step 4: Add SQLite schema upgrade**

In `src/catalyst_radar/storage/db.py`, import `job_locks` and add a helper:

```python
def _upgrade_sqlite_job_locks(engine: Engine) -> None:
    if engine.dialect.name != "sqlite":
        return
    inspector = inspect(engine)
    if "job_locks" in inspector.get_table_names():
        return
    job_locks.create(engine)
```

Call `_upgrade_sqlite_job_locks(engine)` from `create_schema()` after `metadata.create_all(engine)`.

- [ ] **Step 5: Implement `JobLockRepository`**

Create `src/catalyst_radar/storage/job_repositories.py` with:

```python
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Mapping

from sqlalchemy import Engine, delete, insert, select, update

from catalyst_radar.storage.schema import job_locks
from catalyst_radar.storage.types import thaw_json_value


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
            row = conn.execute(
                select(job_locks).where(job_locks.c.lock_name == lock_name).limit(1)
            ).first()
            if row is not None:
                current = row._mapping
                current_expires_at = _as_utc(current["expires_at"])
                if current_expires_at > resolved_now:
                    current_owner = str(current["owner"])
                    return JobLockAcquireResult(
                        lock_name=lock_name,
                        owner=owner,
                        acquired=False,
                        current_owner=current_owner,
                        expires_at=current_expires_at,
                    )
                conn.execute(delete(job_locks).where(job_locks.c.lock_name == lock_name))
            conn.execute(
                insert(job_locks).values(
                    lock_name=lock_name,
                    owner=owner,
                    acquired_at=resolved_now,
                    heartbeat_at=resolved_now,
                    expires_at=expires_at,
                    metadata=payload,
                )
            )
        return JobLockAcquireResult(
            lock_name=lock_name,
            owner=owner,
            acquired=True,
            current_owner=owner,
            expires_at=expires_at,
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
```

- [ ] **Step 6: Run focused tests**

Run:

```powershell
python -m pytest tests/integration/test_jobs.py -q
python -m ruff check src/catalyst_radar/storage/schema.py src/catalyst_radar/storage/db.py src/catalyst_radar/storage/job_repositories.py tests/integration/test_jobs.py
```

Expected: tests pass and ruff is clean.

- [ ] **Step 7: Commit job lock storage**

Run:

```powershell
git add src/catalyst_radar/storage/schema.py src/catalyst_radar/storage/db.py src/catalyst_radar/storage/job_repositories.py tests/integration/test_jobs.py
git commit -m "feat: add scheduler job locks"
```

---

## Task 3: Ops Metrics and Health Payload

**Files:**
- Create: `src/catalyst_radar/ops/__init__.py`
- Create: `src/catalyst_radar/ops/runbooks.py`
- Create: `src/catalyst_radar/ops/metrics.py`
- Create: `src/catalyst_radar/ops/health.py`
- Modify: `src/catalyst_radar/dashboard/data.py`
- Test: `tests/integration/test_ops_health.py`
- Test: `tests/integration/test_dashboard_data.py`

- [ ] **Step 1: Write failing ops health tests**

Add `tests/integration/test_ops_health.py` with:

```python
from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import create_engine, insert

from catalyst_radar.core.models import ActionState
from catalyst_radar.ops.health import load_ops_health
from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.schema import (
    budget_ledger,
    candidate_states,
    data_quality_incidents,
    job_runs,
    provider_health,
    validation_runs,
)


def _engine():
    engine = create_engine("sqlite:///:memory:", future=True)
    create_schema(engine)
    return engine


def test_ops_health_enables_degraded_mode_for_stale_core_data():
    engine = _engine()
    now = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
    stale_as_of = now - timedelta(days=3)
    with engine.begin() as conn:
        conn.execute(
            insert(candidate_states).values(
                id="state-old",
                ticker="AAA",
                as_of=stale_as_of,
                state=ActionState.WARNING.value,
                previous_state=None,
                final_score=88.0,
                policy_version="test",
                reason="old data",
                source_feature_ids=[],
                created_at=stale_as_of,
                payload={"candidate": {"metadata": {}}},
            )
        )

    health = load_ops_health(engine, now=now, stale_after=timedelta(hours=36))

    assert health["degraded_mode"]["enabled"] is True
    assert health["degraded_mode"]["max_action_state"] == ActionState.ADD_TO_WATCHLIST.value
    assert ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW.value in health["degraded_mode"]["disabled_states"]
    assert health["stale_data"]["detected"] is True


def test_ops_health_reports_metrics_banners_incidents_and_score_drift():
    engine = _engine()
    now = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
    prev = datetime(2026, 5, 8, 21, 0, tzinfo=UTC)
    latest = datetime(2026, 5, 9, 21, 0, tzinfo=UTC)
    with engine.begin() as conn:
        conn.execute(
            insert(provider_health),
            [
                {
                    "id": "ph-1",
                    "provider": "polygon",
                    "status": "healthy",
                    "checked_at": now - timedelta(minutes=5),
                    "reason": "ok",
                    "latency_ms": 25.0,
                },
                {
                    "id": "ph-2",
                    "provider": "sec",
                    "status": "degraded",
                    "checked_at": now - timedelta(minutes=4),
                    "reason": "rate limited",
                    "latency_ms": 1200.0,
                },
            ],
        )
        conn.execute(
            insert(job_runs),
            [
                {
                    "id": "job-1",
                    "job_type": "feature_scan",
                    "provider": None,
                    "status": "success",
                    "started_at": now - timedelta(minutes=30),
                    "finished_at": now - timedelta(minutes=25),
                    "requested_count": 2,
                    "raw_count": 2,
                    "normalized_count": 2,
                    "error_summary": None,
                    "metadata": {},
                }
            ],
        )
        conn.execute(
            insert(data_quality_incidents).values(
                id="incident-1",
                provider="sec",
                severity="warning",
                kind="stale_data",
                affected_tickers=["AAA"],
                reason="fixture stale",
                fail_closed_action="disable action states above AddToWatchlist",
                payload={"field": "available_at"},
                detected_at=now - timedelta(minutes=3),
                source_ts=now - timedelta(days=2),
                available_at=now - timedelta(days=2),
            )
        )
        for idx, score in enumerate([40.0, 42.0, 43.0], start=1):
            conn.execute(
                insert(candidate_states).values(
                    id=f"prev-{idx}",
                    ticker=f"P{idx}",
                    as_of=prev,
                    state=ActionState.RESEARCH_ONLY.value,
                    previous_state=None,
                    final_score=score,
                    policy_version="test",
                    reason="previous",
                    source_feature_ids=[],
                    created_at=prev,
                    payload={"candidate": {"metadata": {}}},
                )
            )
        for idx, score in enumerate([91.0, 93.0, 94.0], start=1):
            conn.execute(
                insert(candidate_states).values(
                    id=f"latest-{idx}",
                    ticker=f"L{idx}",
                    as_of=latest,
                    state=ActionState.WARNING.value,
                    previous_state=None,
                    final_score=score,
                    policy_version="test",
                    reason="latest",
                    source_feature_ids=[],
                    created_at=latest,
                    payload={"candidate": {"metadata": {}}},
                )
            )
        conn.execute(
            insert(validation_runs).values(
                id="validation-1",
                run_type="replay",
                as_of_start=prev,
                as_of_end=latest,
                decision_available_at=latest,
                outcome_available_at=now,
                status="completed",
                metrics={"candidate_count": 4, "false_positive_count": 1},
                started_at=now - timedelta(hours=1),
                finished_at=now - timedelta(minutes=10),
                payload={},
            )
        )
        conn.execute(
            insert(budget_ledger).values(
                id="llm-1",
                ts=now - timedelta(minutes=15),
                available_at=now - timedelta(minutes=15),
                ticker="AAA",
                candidate_state_id=None,
                candidate_packet_id=None,
                decision_card_id=None,
                task="evidence_review",
                model="gpt-5.1",
                provider="openai",
                status="schema_rejected",
                skip_reason="schema_validation_failed",
                input_tokens=100,
                cached_input_tokens=0,
                output_tokens=20,
                tool_calls=[],
                estimated_cost=0.01,
                actual_cost=0.01,
                currency="USD",
                candidate_state=None,
                prompt_version="test",
                schema_version="test",
                outcome_label=None,
                payload={},
                created_at=now - timedelta(minutes=15),
            )
        )

    health = load_ops_health(engine, now=now)

    assert health["provider_banners"][0]["provider"] == "sec"
    assert health["metrics"]["stage_counts"]["feature_scan"]["success"] == 1
    assert health["metrics"]["stale_incident_count"] == 1
    assert health["metrics"]["unsupported_claim_rate"] == 1.0
    assert health["metrics"]["false_positive_rate"] == 0.25
    assert health["score_drift"]["detected"] is True
    assert health["runbooks"]["provider_failure"].endswith("provider-failure.md")
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```powershell
python -m pytest tests/integration/test_ops_health.py -q
```

Expected: fail because `catalyst_radar.ops.health` does not exist.

- [ ] **Step 3: Implement runbook registry**

Create `src/catalyst_radar/ops/runbooks.py`:

```python
from __future__ import annotations


RUNBOOKS = {
    "provider_failure": "docs/runbooks/provider-failure.md",
    "llm_failure": "docs/runbooks/llm-failure.md",
    "score_drift": "docs/runbooks/score-drift.md",
}


def runbook_links() -> dict[str, str]:
    return dict(RUNBOOKS)


__all__ = ["RUNBOOKS", "runbook_links"]
```

- [ ] **Step 4: Implement metrics helpers**

Create `src/catalyst_radar/ops/metrics.py` with these public functions:

```python
def load_ops_metrics(engine: Engine, *, now: datetime | None = None) -> dict[str, object]: ...
def detect_score_drift(engine: Engine, *, mean_delta_threshold: float = 25.0, count_delta_ratio: float = 0.75) -> dict[str, object]: ...
```

Implementation details:

- Query `job_runs` ordered by recent `started_at`.
- Build `stage_counts` as `{job_type: {status: count}}`.
- Read cost from `BudgetLedgerRepository(engine).summary(available_at=now)`.
- Count latest useful labels through `ValidationRepository(engine).list_useful_alert_labels(available_at=now)`.
- Count incidents from `data_quality_incidents`.
- Count unsupported claim rows from `budget_ledger` where:
  - `status == "schema_rejected"`
  - `skip_reason` is one of `schema_validation_failed`, `source_faithfulness_failed`, `unsupported_claim`
- Divide unsupported claim rows by all rows in `budget_ledger`.
- Compute latest validation false-positive rate from newest `validation_runs.finished_at` with `metrics.false_positive_count` and `metrics.candidate_count`.
- Detect score drift by comparing `candidate_states.final_score` for latest `as_of` versus previous `as_of`.

- [ ] **Step 5: Implement health payload**

Create `src/catalyst_radar/ops/health.py` with:

```python
UNHEALTHY_PROVIDER_STATUSES = {"stale", "unhealthy", "degraded", "down", "failed", "error"}
DISABLED_DEGRADED_STATES = [
    ActionState.WARNING.value,
    ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW.value,
    ActionState.THESIS_WEAKENING.value,
    ActionState.EXIT_INVALIDATE_REVIEW.value,
]


def load_ops_health(
    engine: Engine,
    *,
    now: datetime | None = None,
    stale_after: timedelta = timedelta(hours=36),
) -> dict[str, object]:
    ...
```

Return a dict with all pre-existing keys plus the new keys:

```python
{
    "providers": providers,
    "provider_banners": provider_banners,
    "jobs": jobs,
    "database": database,
    "stale_data": stale_data,
    "degraded_mode": degraded_mode,
    "metrics": load_ops_metrics(engine, now=resolved_now),
    "score_drift": detect_score_drift(engine),
    "incidents": incidents,
    "runbooks": runbook_links(),
}
```

Keep the old payload fields and naming stable so existing API/dashboard tests continue to pass.

- [ ] **Step 6: Delegate dashboard data function**

In `src/catalyst_radar/dashboard/data.py`, replace the body of `load_ops_health(engine)` with:

```python
from catalyst_radar.ops.health import load_ops_health as _load_ops_health


def load_ops_health(engine: Engine) -> dict[str, object]:
    return _load_ops_health(engine)
```

Keep imports that other functions still use. Remove `job_runs` and `provider_health` imports only if ruff reports them unused.

- [ ] **Step 7: Run focused ops tests**

Run:

```powershell
python -m pytest tests/integration/test_ops_health.py tests/integration/test_dashboard_data.py::test_load_ops_health_reports_provider_status_and_database tests/integration/test_api_routes.py::test_get_ops_health -q
python -m ruff check src/catalyst_radar/ops src/catalyst_radar/dashboard/data.py tests/integration/test_ops_health.py
```

Expected: focused tests pass and ruff is clean.

- [ ] **Step 8: Commit ops metrics and health**

Run:

```powershell
git add src/catalyst_radar/ops src/catalyst_radar/dashboard/data.py tests/integration/test_ops_health.py
git commit -m "feat: add operations health metrics"
```

---

## Task 4: Daily Task Orchestration

**Files:**
- Create: `src/catalyst_radar/jobs/__init__.py`
- Create: `src/catalyst_radar/jobs/tasks.py`
- Modify: `tests/integration/test_jobs.py`

- [ ] **Step 1: Add failing daily-run tests**

Extend `tests/integration/test_jobs.py`:

```python
from datetime import date

import pytest

from catalyst_radar.jobs.tasks import DailyRunSpec, run_daily


def test_daily_run_requires_timezone_aware_available_at():
    engine = _engine()

    with pytest.raises(ValueError, match="decision_available_at must be timezone-aware"):
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
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```powershell
python -m pytest tests/integration/test_jobs.py -q
```

Expected: fail because `catalyst_radar.jobs.tasks` does not exist.

- [ ] **Step 3: Implement task dataclasses**

Create `src/catalyst_radar/jobs/tasks.py` with:

```python
@dataclass(frozen=True)
class DailyRunSpec:
    as_of: date
    decision_available_at: datetime
    outcome_available_at: datetime | None = None
    provider: str | None = None
    universe: str | None = None
    tickers: tuple[str, ...] = ()
    dry_run_alerts: bool = True
    run_llm: bool = False
    llm_dry_run: bool = True

    def __post_init__(self) -> None:
        if self.decision_available_at.tzinfo is None or self.decision_available_at.utcoffset() is None:
            msg = "decision_available_at must be timezone-aware"
            raise ValueError(msg)
        object.__setattr__(self, "decision_available_at", self.decision_available_at.astimezone(UTC))
        if self.outcome_available_at is not None:
            if self.outcome_available_at.tzinfo is None or self.outcome_available_at.utcoffset() is None:
                msg = "outcome_available_at must be timezone-aware"
                raise ValueError(msg)
            object.__setattr__(self, "outcome_available_at", self.outcome_available_at.astimezone(UTC))
        object.__setattr__(self, "tickers", tuple(ticker.upper() for ticker in self.tickers))


@dataclass(frozen=True)
class JobStepResult:
    name: str
    status: str
    job_id: str | None = None
    requested_count: int = 0
    raw_count: int = 0
    normalized_count: int = 0
    reason: str | None = None
    payload: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DailyRunResult:
    status: str
    spec: DailyRunSpec
    steps: tuple[JobStepResult, ...]

    def step(self, name: str) -> JobStepResult:
        for step in self.steps:
            if step.name == name:
                return step
        msg = f"unknown step: {name}"
        raise KeyError(msg)
```

- [ ] **Step 4: Implement `run_daily` with durable step rows**

Implement:

```python
DAILY_STEP_ORDER = (
    "daily_bar_ingest",
    "event_ingest",
    "local_text_triage",
    "feature_scan",
    "scoring_policy",
    "candidate_packets",
    "decision_cards",
    "llm_review",
    "digest",
    "validation_update",
)


def run_daily(spec: DailyRunSpec, *, engine: Engine) -> DailyRunResult:
    provider_repo = ProviderRepository(engine)
    steps = []
    for step_name in DAILY_STEP_ORDER:
        steps.append(_run_step(step_name, spec, provider_repo=provider_repo, engine=engine))
    status = _daily_status(steps)
    return DailyRunResult(status=status, spec=spec, steps=tuple(steps))
```

Step behavior:

- `daily_bar_ingest`: record skipped unless this phase adds configured provider input. Reason: `no_scheduled_provider_input`.
- `event_ingest`: record skipped unless event connectors are configured. Reason: `no_scheduled_event_provider`.
- `local_text_triage`: call existing text pipeline only when events/snippets exist for the cutoff; otherwise skipped.
- `feature_scan`: call `run_scan` when data exists for the requested tickers/as_of; otherwise skipped.
- `scoring_policy`: record success when scan created or updated candidate states; skipped when no candidate inputs.
- `candidate_packets`: call `build_candidate_packet` for Warning-or-higher rows available at cutoff; skipped when none.
- `decision_cards`: call `build_decision_card` for eligible manual buy-review states; skipped when none.
- `llm_review`: skipped when `spec.run_llm is False`; fake/dry-run when `spec.run_llm is True and spec.llm_dry_run is True`; fail closed without an API key when real provider is requested.
- `digest`: call `build_alert_digest` from existing alerting code when alerts exist; skipped when none.
- `validation_update`: skipped unless `outcome_available_at` is supplied; when supplied, call existing validation update/replay helpers.

Each step must call `ProviderRepository.start_job(step_name, spec.provider, metadata)` before work and `finish_job(...)` after work.

- [ ] **Step 5: Run focused jobs tests**

Run:

```powershell
python -m pytest tests/integration/test_jobs.py -q
python -m ruff check src/catalyst_radar/jobs src/catalyst_radar/storage/job_repositories.py tests/integration/test_jobs.py
```

Expected: tests pass and ruff is clean.

- [ ] **Step 6: Commit daily orchestration**

Run:

```powershell
git add src/catalyst_radar/jobs tests/integration/test_jobs.py
git commit -m "feat: add daily job orchestration"
```

---

## Task 5: Scheduler, Worker, and CLI

**Files:**
- Create: `src/catalyst_radar/jobs/scheduler.py`
- Create: `apps/worker/main.py`
- Modify: `src/catalyst_radar/cli.py`
- Modify: `tests/integration/test_jobs.py`

- [ ] **Step 1: Add failing scheduler tests**

Extend `tests/integration/test_jobs.py`:

```python
from catalyst_radar.jobs.scheduler import SchedulerConfig, build_daily_spec, run_once


def test_scheduler_run_once_uses_lock_and_releases_it():
    engine = _engine()
    config = SchedulerConfig(
        owner="worker-test",
        lock_name="daily-run",
        lock_ttl=timedelta(minutes=10),
        run_interval=timedelta(minutes=30),
        as_of=date(2026, 5, 9),
        decision_available_at=datetime(2026, 5, 10, 1, 0, tzinfo=UTC),
        run_llm=False,
        llm_dry_run=True,
        dry_run_alerts=True,
    )

    result = run_once(engine=engine, config=config)

    assert result.acquired_lock is True
    assert result.daily_result is not None
    assert result.daily_result.step("llm_review").status == "skipped"


def test_scheduler_run_once_skips_when_lock_is_held():
    engine = _engine()
    repo = JobLockRepository(engine)
    now = datetime(2026, 5, 10, 1, 0, tzinfo=UTC)
    repo.acquire("daily-run", owner="other-worker", ttl=timedelta(minutes=10), now=now)
    config = SchedulerConfig(
        owner="worker-test",
        lock_name="daily-run",
        lock_ttl=timedelta(minutes=10),
        run_interval=timedelta(minutes=30),
        as_of=date(2026, 5, 9),
        decision_available_at=now,
    )

    result = run_once(engine=engine, config=config, now=now)

    assert result.acquired_lock is False
    assert result.daily_result is None
    assert result.reason == "lock_held"


def test_build_daily_spec_from_environment_values():
    config = SchedulerConfig.from_env(
        {
            "CATALYST_DAILY_AS_OF": "2026-05-09",
            "CATALYST_DECISION_AVAILABLE_AT": "2026-05-10T01:00:00+00:00",
            "CATALYST_RUN_LLM": "0",
            "CATALYST_LLM_DRY_RUN": "1",
            "CATALYST_DRY_RUN_ALERTS": "1",
        }
    )

    spec = build_daily_spec(config)

    assert spec.as_of == date(2026, 5, 9)
    assert spec.decision_available_at == datetime(2026, 5, 10, 1, 0, tzinfo=UTC)
    assert spec.run_llm is False
    assert spec.llm_dry_run is True
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```powershell
python -m pytest tests/integration/test_jobs.py -q
```

Expected: fail because `catalyst_radar.jobs.scheduler` does not exist.

- [ ] **Step 3: Implement scheduler**

Create `src/catalyst_radar/jobs/scheduler.py` with:

```python
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
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "SchedulerConfig":
        ...


@dataclass(frozen=True)
class SchedulerRunResult:
    acquired_lock: bool
    reason: str | None
    daily_result: DailyRunResult | None
```

Implement:

```python
def build_daily_spec(config: SchedulerConfig, *, now: datetime | None = None) -> DailyRunSpec: ...
def run_once(*, engine: Engine, config: SchedulerConfig, now: datetime | None = None) -> SchedulerRunResult: ...
def run_forever(*, engine: Engine, config: SchedulerConfig) -> None: ...
```

`run_once` must:

1. Acquire `JobLockRepository(engine).acquire(...)`.
2. Return `SchedulerRunResult(False, "lock_held", None)` if the lock is held.
3. Call `run_daily(...)` if acquired.
4. Release the lock in a `finally` block.

- [ ] **Step 4: Implement worker entrypoint**

Create `apps/worker/main.py`:

```python
from __future__ import annotations

from catalyst_radar.config import load_config
from catalyst_radar.jobs.scheduler import SchedulerConfig, run_forever, run_once
from catalyst_radar.storage.db import create_engine_from_config, create_schema


def main() -> int:
    config = load_config()
    engine = create_engine_from_config(config)
    create_schema(engine)
    scheduler_config = SchedulerConfig.from_env()
    if scheduler_config.run_interval.total_seconds() <= 0:
        result = run_once(engine=engine, config=scheduler_config)
        return 0 if result.reason in {None, "lock_held"} else 1
    run_forever(engine=engine, config=scheduler_config)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

If the existing config helper names differ, use the established config and engine helper names from the repo.

- [ ] **Step 5: Add CLI command**

In `src/catalyst_radar/cli.py`, add parser:

```python
run_daily_parser = subparsers.add_parser("run-daily")
run_daily_parser.add_argument("--as-of", required=True)
run_daily_parser.add_argument("--available-at", required=True)
run_daily_parser.add_argument("--outcome-available-at")
run_daily_parser.add_argument("--run-llm", action="store_true")
run_daily_parser.add_argument("--real-llm", action="store_true")
run_daily_parser.add_argument("--deliver-alerts", action="store_true")
run_daily_parser.add_argument("--json", action="store_true")
```

Handler must build `SchedulerConfig`, call `run_once`, print stable JSON when `--json` is passed, and return non-zero only on real failures. A held lock is a clean no-op.

- [ ] **Step 6: Run focused tests and CLI smoke**

Run:

```powershell
python -m pytest tests/integration/test_jobs.py -q
python -m ruff check src/catalyst_radar/jobs apps/worker/main.py src/catalyst_radar/cli.py tests/integration/test_jobs.py
python -m catalyst_radar.cli init-db --database-url sqlite:///data/local/phase14-smoke.db
python -m catalyst_radar.cli run-daily --database-url sqlite:///data/local/phase14-smoke.db --as-of 2026-05-09 --available-at 2026-05-10T01:00:00+00:00 --json
```

Expected:

- Tests pass.
- Ruff clean.
- CLI JSON includes `acquired_lock`, `daily_result`, and a skipped `llm_review` step.

- [ ] **Step 7: Commit scheduler and worker**

Run:

```powershell
git add src/catalyst_radar/jobs/scheduler.py apps/worker/main.py src/catalyst_radar/cli.py tests/integration/test_jobs.py
git commit -m "feat: add daily scheduler worker"
```

---

## Task 6: Ops Dashboard and API Surface

**Files:**
- Modify: `apps/dashboard/pages/5_Ops.py`
- Modify: `src/catalyst_radar/api/routes/ops.py`
- Modify: `tests/integration/test_api_routes.py`
- Modify: `tests/integration/test_dashboard_data.py`

- [ ] **Step 1: Add focused API/dashboard assertions**

Update `tests/integration/test_api_routes.py::test_get_ops_health` so the mocked or real payload asserts the new keys:

```python
assert "degraded_mode" in response.json()
assert "metrics" in response.json()
assert "score_drift" in response.json()
assert "runbooks" in response.json()
```

Update the dashboard data ops test to assert:

```python
assert "provider_banners" in health
assert "degraded_mode" in health
assert "metrics" in health
assert "score_drift" in health
```

- [ ] **Step 2: Run tests to verify current surface**

Run:

```powershell
python -m pytest tests/integration/test_api_routes.py::test_get_ops_health tests/integration/test_dashboard_data.py::test_load_ops_health_reports_provider_status_and_database -q
```

Expected: fail until API/dashboard tests and payloads agree.

- [ ] **Step 3: Keep API route stable**

`src/catalyst_radar/api/routes/ops.py` should continue to expose:

```python
@router.get("/health")
def get_ops_health() -> dict[str, object]:
    return dashboard_data.load_ops_health(_engine())
```

Only adjust typing/imports if required by ruff.

- [ ] **Step 4: Render richer dashboard sections**

In `apps/dashboard/pages/5_Ops.py`, render:

- Provider banners at the top when `health["provider_banners"]` is non-empty.
- Degraded mode status with disabled states.
- Metrics columns:
  - total LLM actual cost
  - cost per useful alert
  - stale incident count
  - unsupported-claim rate
  - false-positive rate
- Score drift status with latest/previous average score.
- Recent data-quality incidents table.
- Existing provider and job tables unchanged.

Do not add decorative marketing sections. Keep this page operational and dense.

- [ ] **Step 5: Run tests and lint**

Run:

```powershell
python -m pytest tests/integration/test_api_routes.py::test_get_ops_health tests/integration/test_dashboard_data.py::test_load_ops_health_reports_provider_status_and_database tests/integration/test_ops_health.py -q
python -m ruff check apps/dashboard/pages/5_Ops.py src/catalyst_radar/api/routes/ops.py tests/integration/test_api_routes.py tests/integration/test_dashboard_data.py
```

Expected: tests pass and ruff is clean.

- [ ] **Step 6: Commit ops UI/API polish**

Run:

```powershell
git add apps/dashboard/pages/5_Ops.py src/catalyst_radar/api/routes/ops.py tests/integration/test_api_routes.py tests/integration/test_dashboard_data.py
git commit -m "feat: show operations controls in dashboard"
```

---

## Task 7: Docker Runtime and Runbooks

**Files:**
- Create: `infra/docker/Dockerfile`
- Create: `infra/docker/docker-compose.prod.yml`
- Modify: `docker-compose.yml`
- Create: `docs/runbooks/provider-failure.md`
- Create: `docs/runbooks/llm-failure.md`
- Create: `docs/runbooks/score-drift.md`

- [ ] **Step 1: Add shared Dockerfile**

Create `infra/docker/Dockerfile`:

```dockerfile
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential curl \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY src ./src
COPY apps ./apps
COPY docs ./docs

RUN pip install --upgrade pip \
    && pip install -e .

CMD ["python", "-m", "catalyst_radar.cli", "--help"]
```

- [ ] **Step 2: Extend local compose**

Modify `docker-compose.yml` to include:

- `postgres`
- `api`
- `dashboard`
- `worker`

Use this shape:

```yaml
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: catalyst
      POSTGRES_PASSWORD: catalyst
      POSTGRES_DB: catalyst_radar
    ports:
      - "54321:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U catalyst -d catalyst_radar"]
      interval: 10s
      timeout: 5s
      retries: 5

  api:
    build:
      context: .
      dockerfile: infra/docker/Dockerfile
    command: python -m uvicorn apps.api.main:app --host 0.0.0.0 --port 8000
    environment:
      CATALYST_DATABASE_URL: postgresql+psycopg://catalyst:catalyst@postgres:5432/catalyst_radar
    ports:
      - "8000:8000"
    depends_on:
      postgres:
        condition: service_healthy

  dashboard:
    build:
      context: .
      dockerfile: infra/docker/Dockerfile
    command: streamlit run apps/dashboard/Home.py --server.address 0.0.0.0 --server.port 8501
    environment:
      CATALYST_DATABASE_URL: postgresql+psycopg://catalyst:catalyst@postgres:5432/catalyst_radar
    ports:
      - "8501:8501"
    depends_on:
      postgres:
        condition: service_healthy

  worker:
    build:
      context: .
      dockerfile: infra/docker/Dockerfile
    command: python -m apps.worker.main
    environment:
      CATALYST_DATABASE_URL: postgresql+psycopg://catalyst:catalyst@postgres:5432/catalyst_radar
      CATALYST_WORKER_INTERVAL_SECONDS: "86400"
      CATALYST_RUN_LLM: "0"
      CATALYST_LLM_DRY_RUN: "1"
      CATALYST_DRY_RUN_ALERTS: "1"
    depends_on:
      postgres:
        condition: service_healthy

volumes:
  postgres_data:
```

- [ ] **Step 3: Add production-style compose**

Create `infra/docker/docker-compose.prod.yml` with the same services but:

- no default OpenAI key
- app services use `env_file: ../../.env.prod`
- no source bind mount
- Postgres password and database URL come from env
- worker interval remains configurable through env

- [ ] **Step 4: Add provider failure runbook**

Create `docs/runbooks/provider-failure.md` with sections:

```markdown
# Provider Failure Runbook

## Trigger

Provider health is `degraded`, `down`, `failed`, `error`, or stale beyond the freshness window.

## Immediate Controls

- Confirm the Ops dashboard degraded-mode banner is present.
- Confirm states above `AddToWatchlist` are disabled for the affected run.
- Keep deterministic scans available only when source freshness is acceptable.

## Diagnosis

- Inspect `/api/ops/health`.
- Check latest `provider_health` rows.
- Check latest `data_quality_incidents` rows for affected providers and tickers.
- Check recent `job_runs` for `daily_bar_ingest`, `event_ingest`, and `feature_scan`.

## Recovery

- Retry the affected provider job after rate-limit or outage clears.
- Use backup provider inputs only when licensing and freshness are known.
- Re-run `run-daily` with explicit `--as-of` and `--available-at`.

## Closeout

- Confirm a healthy provider row after recovery.
- Confirm stale-data banner clears.
- Record whether the incident created false alerts or missed opportunities.
```

- [ ] **Step 5: Add LLM failure runbook**

Create `docs/runbooks/llm-failure.md` with sections:

- Trigger
- Immediate Controls
- Diagnosis
- Recovery
- Closeout

Mandatory content:

- deterministic scanner continues
- premium model calls fail closed without key or budget
- eligible candidates stay reviewable without synthetic model claims
- inspect `budget_ledger` status and skip reasons
- rerun LLM review only for candidates with valid source-linked evidence packets

- [ ] **Step 6: Add score drift runbook**

Create `docs/runbooks/score-drift.md` with sections:

- Trigger
- Immediate Controls
- Diagnosis
- Recovery
- Closeout

Mandatory content:

- freeze new buy-review states
- inspect latest versus previous score distribution
- inspect provider freshness and schema failures
- run replay validation before re-enabling escalation
- record whether drift was data, scoring, or regime-driven

- [ ] **Step 7: Validate compose config**

Run:

```powershell
docker compose config
```

Expected: compose prints resolved services without YAML errors.

If Docker is unavailable on the machine, run:

```powershell
python - <<'PY'
from pathlib import Path
import yaml
for path in [Path("docker-compose.yml"), Path("infra/docker/docker-compose.prod.yml")]:
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    assert "services" in data
    assert {"postgres", "api", "dashboard", "worker"} <= set(data["services"])
print("compose yaml parsed")
PY
```

- [ ] **Step 8: Commit Docker and runbooks**

Run:

```powershell
git add infra/docker docker-compose.yml docs/runbooks
git commit -m "feat: add operations runtime packaging"
```

---

## Task 8: Final Phase Review, Verification, and Merge

**Files:**
- Create: `docs/phase-14-review.md`
- Modify: `docs/superpowers/plans/2026-05-09-full-product-implementation.md`

- [ ] **Step 1: Update master plan**

In `docs/superpowers/plans/2026-05-09-full-product-implementation.md`, mark every Phase 14 implementation task complete.

- [ ] **Step 2: Add phase review note**

Create `docs/phase-14-review.md`:

```markdown
# Phase 14 Review

## Completed

- Added scheduler job locks.
- Added daily run orchestration and worker runtime.
- Added operations health payload, degraded mode, provider banners, metrics, and score drift.
- Added dashboard/API operations surface.
- Added local and production-style Docker Compose runtime.
- Added provider, LLM, and score-drift runbooks.

## Verification

- `python -m pytest`
- `python -m ruff check src tests apps`
- `git diff --check`
- `docker compose config`

## Residual Risk

- Real provider scheduling depends on configured provider credentials and licensed data sources.
- Real OpenAI provider smoke requires `OPENAI_API_KEY`; without it the system continues to fail closed.
- Alert delivery remains dry-run unless a delivery channel is explicitly enabled.
```

- [ ] **Step 3: Run full verification**

Run:

```powershell
python -m pytest
python -m ruff check src tests apps
git diff --check
docker compose config
```

Expected:

- all tests pass
- ruff clean
- diff check clean
- compose config valid

- [ ] **Step 4: Commit docs**

Run:

```powershell
git add docs/phase-14-review.md docs/superpowers/plans/2026-05-09-full-product-implementation.md
git commit -m "docs: review phase 14 operations"
```

- [ ] **Step 5: Request implementation reviews**

Use subagents for:

- spec compliance review against this plan and the master Phase 14 checklist
- code quality review of the full branch
- operations/runtime review of worker and Docker paths

Fix all Important or Critical findings before merging.

- [ ] **Step 6: Merge to main**

Run:

```powershell
git switch main
git merge --ff-only feature/phase-14-operations-observability
python -m pytest
python -m ruff check src tests apps
git diff --check
```

Expected: main has Phase 14 commits and the full suite remains green.

---

## Acceptance Checklist

- [ ] Scheduled daily execution exists through CLI, worker entrypoint, and scheduler module.
- [ ] Job locks prevent overlapping daily runs and recover after expiration.
- [ ] `job_runs` records every scheduled step with stable status, counts, and metadata.
- [ ] Provider-health banners appear in ops payload/dashboard.
- [ ] Degraded mode disables states above `AddToWatchlist` when core data is stale.
- [ ] Ops payload includes stage counts, cost, useful-alert metrics, stale incidents, unsupported-claim rate, false-positive rate, and score drift.
- [ ] API route `/api/ops/health` remains backward-compatible and includes richer fields.
- [ ] Dashboard Ops page shows provider health, job status, stale data, schema failures/incidents, degraded mode, metrics, and drift.
- [ ] Docker Compose can run Postgres, worker, API, and dashboard.
- [ ] Runbooks exist for provider failure, LLM failure, and score drift.
- [ ] Phase 14 remains deterministic and does not require an OpenAI key.
- [ ] Full tests and ruff pass on feature branch and main after merge.

## Spec Coverage Check

- Job schedule from Engineering Spec section 14: covered by `DailyRunSpec`, `run_daily`, `SchedulerConfig`, worker, and CLI.
- Provider failure runbook: covered by degraded mode, provider banners, metrics, and `docs/runbooks/provider-failure.md`.
- LLM failure runbook: covered by dry-run/default-disabled LLM step, budget ledger visibility, fail-closed API key behavior, and `docs/runbooks/llm-failure.md`.
- Score distribution abnormal shift: covered by `detect_score_drift`, dashboard/API payload, and `docs/runbooks/score-drift.md`.
- Observability telemetry from Architecture Spec section 11: covered by ops metrics from existing durable tables.
- Deployment architecture local/VM: covered by worker entrypoint and Docker Compose.
- No automated trading: unchanged; alerts remain dry-run by default.

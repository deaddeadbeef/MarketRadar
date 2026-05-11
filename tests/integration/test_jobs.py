from __future__ import annotations

import json
import threading
import time
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, insert, inspect, select

from apps.worker import main as worker_main
from catalyst_radar.brokers.models import (
    BrokerAccount,
    BrokerBalanceSnapshot,
    BrokerConnection,
    BrokerConnectionStatus,
    BrokerPosition,
    broker_account_id,
    broker_balance_snapshot_id,
    broker_connection_id,
    broker_position_id,
)
from catalyst_radar.cli import main as cli_main
from catalyst_radar.core.models import (
    ActionState,
    CandidateSnapshot,
    MarketFeatures,
    PolicyResult,
)
from catalyst_radar.jobs.scheduler import (
    SchedulerConfig,
    SchedulerRunResult,
    _next_sleep_seconds,
    build_daily_spec,
    run_once,
)
from catalyst_radar.jobs.tasks import DAILY_STEP_ORDER, DailyRunSpec, run_daily
from catalyst_radar.pipeline.scan import ScanResult
from catalyst_radar.storage.broker_repositories import BrokerRepository
from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.job_repositories import JobLockRepository
from catalyst_radar.storage.schema import (
    candidate_states,
    decision_cards,
    job_locks,
    job_runs,
    provider_health,
    securities,
    validation_runs,
)


def _engine():
    engine = create_engine("sqlite:///:memory:", future=True)
    create_schema(engine)
    return engine


def _file_engine(tmp_path):
    engine = create_engine(f"sqlite:///{(tmp_path / 'jobs.db').as_posix()}", future=True)
    create_schema(engine)
    return engine


def _insert_active_security(engine, now: datetime) -> None:
    with engine.begin() as conn:
        conn.execute(
            insert(securities).values(
                ticker="MSFT",
                name="Microsoft",
                exchange="NASDAQ",
                sector="Technology",
                industry="Software",
                market_cap=3_000_000_000_000.0,
                avg_dollar_volume_20d=5_000_000_000.0,
                has_options=True,
                is_active=True,
                updated_at=now,
                metadata={},
            )
        )


def _insert_broker_position(engine, *, ticker: str, now: datetime) -> None:
    repo = BrokerRepository(engine)
    connection_id = broker_connection_id()
    account_id = broker_account_id("schwab", "jobs-account-hash")
    repo.upsert_connection(
        BrokerConnection(
            id=connection_id,
            broker="schwab",
            user_id="local",
            status=BrokerConnectionStatus.CONNECTED,
            created_at=now,
            updated_at=now,
            last_successful_sync_at=now,
            metadata={"mode": "read_only"},
        )
    )
    repo.upsert_accounts(
        [
            BrokerAccount(
                id=account_id,
                connection_id=connection_id,
                broker="schwab",
                broker_account_id="12345678",
                account_hash="jobs-account-hash",
                created_at=now,
                updated_at=now,
                display_name="MARGIN ending 5678",
            )
        ]
    )
    repo.upsert_balance_snapshots(
        [
            BrokerBalanceSnapshot(
                id=broker_balance_snapshot_id(account_id, now),
                account_id=account_id,
                as_of=now,
                cash=50_000.0,
                buying_power=100_000.0,
                liquidation_value=250_000.0,
                equity=250_000.0,
                raw_payload={},
                created_at=now,
            )
        ]
    )
    repo.replace_positions(
        account_id,
        now,
        [
            BrokerPosition(
                id=broker_position_id(account_id, ticker, now),
                account_id=account_id,
                as_of=now,
                ticker=ticker,
                quantity=100,
                market_value=9500.0,
                raw_payload={},
                created_at=now,
            )
        ],
    )


def _high_score_scan_result(
    as_of: date,
    *,
    available_at: datetime | None = None,
) -> ScanResult:
    as_of_dt = datetime(as_of.year, as_of.month, as_of.day, 21, tzinfo=UTC)
    features = MarketFeatures(
        ticker="MSFT",
        as_of=as_of_dt,
        ret_5d=0.05,
        ret_20d=0.12,
        rs_20_sector=0.2,
        rs_60_spy=0.3,
        near_52w_high=1.0,
        ma_regime=1.0,
        rel_volume_5d=2.0,
        dollar_volume_z=1.0,
        atr_pct=0.03,
        extension_20d=0.05,
        liquidity_score=95.0,
        feature_version="test-features-v1",
    )
    candidate = CandidateSnapshot(
        ticker="MSFT",
        as_of=as_of_dt,
        features=features,
        final_score=91.0,
        strong_pillars=4,
        risk_penalty=0.0,
        portfolio_penalty=0.0,
        data_stale=False,
        entry_zone=(100.0, 105.0),
        invalidation_price=95.0,
        reward_risk=3.0,
        metadata={
            "available_at": (available_at or as_of_dt).isoformat(),
            "pillar_scores": {"price_strength": 90.0, "volume_liquidity": 88.0},
            "position_size": {
                "risk_per_trade_pct": 0.004,
                "shares": 40,
                "notional": 4160.0,
                "cash_check": "pass",
            },
            "portfolio_impact": {
                "ticker": "MSFT",
                "proposed_notional": 4160.0,
                "max_loss": 400.0,
                "single_name_before_pct": 0.05,
                "single_name_after_pct": 0.09,
                "sector_before_pct": 0.22,
                "sector_after_pct": 0.26,
                "theme_before_pct": 0.12,
                "theme_after_pct": 0.16,
                "correlated_before_pct": 0.18,
                "correlated_after_pct": 0.22,
                "portfolio_penalty": 1.0,
                "hard_blocks": [],
            },
        },
    )
    return ScanResult(
        ticker="MSFT",
        candidate=candidate,
        policy=PolicyResult(
            state=ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW,
            reasons=("all_buy_review_gates_passed",),
        ),
    )


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

    assert result.status == "success"
    assert {step.status for step in result.steps} == {"skipped"}
    assert result.step("daily_bar_ingest").status == "skipped"
    assert result.step("local_text_triage").status == "skipped"
    assert result.step("llm_review").status == "skipped"
    assert result.step("digest").status == "skipped"

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


def test_daily_run_caps_high_states_and_blocks_decision_work_when_degraded(monkeypatch):
    engine = _engine()
    decision_available_at = datetime(2026, 5, 10, 1, 0, tzinfo=UTC)
    _insert_active_security(engine, decision_available_at)
    with engine.begin() as conn:
        conn.execute(
            insert(provider_health).values(
                id="provider-health-down",
                provider="polygon",
                status="down",
                checked_at=decision_available_at,
                reason="provider outage",
                latency_ms=None,
            )
        )

    def fake_run_scan(*args, **kwargs):
        del args, kwargs
        return [_high_score_scan_result(date(2026, 5, 9))]

    monkeypatch.setattr("catalyst_radar.jobs.tasks.run_scan", fake_run_scan)
    spec = DailyRunSpec(
        as_of=date(2026, 5, 9),
        decision_available_at=decision_available_at,
        run_llm=True,
        llm_dry_run=True,
        dry_run_alerts=True,
    )

    result = run_daily(spec, engine=engine)

    assert result.step("scoring_policy").status == "success"
    assert result.step("scoring_policy").payload["degraded_state_cap_count"] == 1
    assert result.step("candidate_packets").status == "skipped"
    assert result.step("candidate_packets").reason == "degraded_mode_blocks_high_state_work"
    assert result.step("decision_cards").reason == "degraded_mode_blocks_decision_cards"
    assert result.step("llm_review").reason == "degraded_mode_blocks_llm_review"

    with engine.connect() as conn:
        state = conn.execute(
            select(
                candidate_states.c.state,
                candidate_states.c.transition_reasons,
            )
        ).one()

    assert state.state == ActionState.ADD_TO_WATCHLIST.value
    assert "degraded_mode_state_cap" in state.transition_reasons


def test_daily_run_decision_cards_include_broker_context(monkeypatch):
    engine = _engine()
    decision_available_at = datetime.now(UTC).replace(microsecond=0) + timedelta(minutes=1)
    _insert_active_security(engine, decision_available_at)
    _insert_broker_position(engine, ticker="MSFT", now=decision_available_at)

    def fake_run_scan(*args, **kwargs):
        del args, kwargs
        return [
            _high_score_scan_result(
                date(2026, 5, 9),
                available_at=decision_available_at,
            )
        ]

    monkeypatch.setattr("catalyst_radar.jobs.tasks.run_scan", fake_run_scan)
    spec = DailyRunSpec(
        as_of=date(2026, 5, 9),
        decision_available_at=decision_available_at,
        run_llm=False,
        dry_run_alerts=True,
    )

    result = run_daily(spec, engine=engine)

    assert result.step("decision_cards").status == "success"
    with engine.connect() as conn:
        payload = conn.execute(select(decision_cards.c.payload)).scalar_one()
    context = payload["broker_portfolio_context"]
    assert context["broker_connected"] is True
    assert context["existing_position"]["ticker"] == "MSFT"
    assert context["existing_position"]["market_value"] == 9500.0


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


def test_daily_run_blocks_downstream_steps_after_failed_feature_scan(monkeypatch):
    engine = _engine()
    decision_available_at = datetime(2026, 5, 10, 1, 0, tzinfo=UTC)
    _insert_active_security(engine, decision_available_at)

    def fail_scan(*args, **kwargs):
        del args, kwargs
        raise RuntimeError("scan unavailable")

    monkeypatch.setattr("catalyst_radar.jobs.tasks.run_scan", fail_scan)
    spec = DailyRunSpec(
        as_of=date(2026, 5, 9),
        decision_available_at=decision_available_at,
        outcome_available_at=datetime(2026, 6, 10, 1, 0, tzinfo=UTC),
    )

    result = run_daily(spec, engine=engine)

    assert result.step("feature_scan").status == "failed"
    assert result.step("candidate_packets").status == "skipped"
    assert result.step("candidate_packets").reason == "blocked_by_failed_dependency:feature_scan"
    assert result.step("decision_cards").status == "skipped"
    assert result.step("decision_cards").reason == "blocked_by_failed_dependency:candidate_packets"
    assert result.step("digest").status == "skipped"
    assert result.step("digest").reason == "blocked_by_failed_dependency:candidate_packets"
    assert result.step("validation_update").status == "skipped"
    assert (
        result.step("validation_update").reason
        == "blocked_by_failed_dependency:candidate_packets"
    )


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


def test_job_lock_rejects_nonpositive_ttl():
    engine = _engine()
    repo = JobLockRepository(engine)

    with pytest.raises(ValueError, match="ttl must be greater than 0"):
        repo.acquire(
            "daily-run",
            owner="worker-a",
            ttl=timedelta(seconds=0),
            now=datetime(2026, 5, 10, 1, 0, tzinfo=UTC),
        )


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
    assert result.reason is None
    assert result.daily_result is not None
    assert result.daily_result.step("llm_review").status == "skipped"

    repo = JobLockRepository(engine)
    reacquired = repo.acquire(
        "daily-run",
        owner="another-worker",
        ttl=timedelta(minutes=10),
        now=datetime(2026, 5, 10, 1, 1, tzinfo=UTC),
    )
    assert reacquired.acquired is True


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


def test_scheduler_run_once_heartbeats_lock_during_active_run(monkeypatch, tmp_path):
    engine = _file_engine(tmp_path)
    competitor_attempt: dict[str, bool] = {}
    release_worker = threading.Event()

    def slow_run_daily(spec, *, engine, abort_event=None):
        time.sleep(0.75)
        assert _wait_for_lock_heartbeat(engine, "daily-run", "worker-a")
        competitor = JobLockRepository(engine).acquire(
            "daily-run",
            owner="worker-b",
            ttl=timedelta(seconds=5),
            now=datetime.now(UTC),
        )
        competitor_attempt["acquired"] = competitor.acquired
        release_worker.set()
        return run_daily(spec, engine=engine, abort_event=abort_event)

    monkeypatch.setattr("catalyst_radar.jobs.scheduler.run_daily", slow_run_daily)
    config = SchedulerConfig(
        owner="worker-a",
        lock_name="daily-run",
        lock_ttl=timedelta(milliseconds=500),
        run_interval=timedelta(minutes=30),
        as_of=date(2026, 5, 9),
        decision_available_at=datetime(2026, 5, 10, 1, 0, tzinfo=UTC),
    )

    result = run_once(engine=engine, config=config)

    assert release_worker.is_set()
    assert result.acquired_lock is True
    assert competitor_attempt["acquired"] is False


def _wait_for_lock_heartbeat(engine, lock_name: str, owner: str) -> bool:
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        with engine.connect() as conn:
            row = conn.execute(
                select(
                    job_locks.c.acquired_at,
                    job_locks.c.heartbeat_at,
                ).where(
                    job_locks.c.lock_name == lock_name,
                    job_locks.c.owner == owner,
                )
            ).first()
        if row is not None and row.heartbeat_at > row.acquired_at:
            return True
        time.sleep(0.02)
    return False


def test_scheduler_run_once_reports_lost_heartbeat(monkeypatch, tmp_path):
    engine = _file_engine(tmp_path)

    def slow_run_daily(spec, *, engine, abort_event=None):
        time.sleep(0.25)
        return run_daily(spec, engine=engine, abort_event=abort_event)

    def lose_heartbeat(self, *args, **kwargs):
        del self, args, kwargs
        return False

    monkeypatch.setattr("catalyst_radar.jobs.scheduler.run_daily", slow_run_daily)
    monkeypatch.setattr(JobLockRepository, "heartbeat", lose_heartbeat)
    config = SchedulerConfig(
        owner="worker-a",
        lock_name="daily-run",
        lock_ttl=timedelta(milliseconds=150),
        run_interval=timedelta(minutes=30),
        as_of=date(2026, 5, 9),
        decision_available_at=datetime(2026, 5, 10, 1, 0, tzinfo=UTC),
    )

    result = run_once(engine=engine, config=config)

    assert result.acquired_lock is True
    assert result.reason == "lock_heartbeat_lost"
    assert result.daily_result is not None
    assert result.daily_result.step("daily_bar_ingest").status == "failed"


def test_scheduler_retries_near_lock_expiry_instead_of_full_interval():
    now = datetime(2026, 5, 10, 1, 0, tzinfo=UTC)
    result = SchedulerRunResult(
        acquired_lock=False,
        reason="lock_held",
        daily_result=None,
        lock_expires_at=now + timedelta(minutes=5),
    )

    sleep_seconds = _next_sleep_seconds(timedelta(hours=24), result, now=now)

    assert sleep_seconds == pytest.approx(301.0)


def test_scheduler_config_rejects_nonpositive_lock_ttl_from_env():
    with pytest.raises(ValueError, match="lock_ttl must be greater than 0"):
        SchedulerConfig.from_env({"CATALYST_WORKER_LOCK_TTL_SECONDS": "0"})


def test_build_daily_spec_from_environment_values():
    outcome_available_at = datetime(2026, 6, 10, 1, 0, tzinfo=UTC)
    config = SchedulerConfig.from_env(
        {
            "CATALYST_DAILY_AS_OF": "2026-05-09",
            "CATALYST_DECISION_AVAILABLE_AT": " 2026-05-10T01:00:00+00:00 ",
            "CATALYST_OUTCOME_AVAILABLE_AT": outcome_available_at.isoformat(),
            "CATALYST_RUN_LLM": "0",
            "CATALYST_LLM_DRY_RUN": "1",
            "CATALYST_DRY_RUN_ALERTS": "1",
        }
    )

    spec = build_daily_spec(config)

    assert spec.as_of == date(2026, 5, 9)
    assert spec.decision_available_at == datetime(2026, 5, 10, 1, 0, tzinfo=UTC)
    assert spec.outcome_available_at == outcome_available_at
    assert spec.run_llm is False
    assert spec.llm_dry_run is True
    assert spec.dry_run_alerts is True


def test_scheduler_config_rejects_unsupported_real_llm_and_alert_delivery():
    with pytest.raises(ValueError, match="real daily LLM review is not supported"):
        SchedulerConfig(run_llm=True, llm_dry_run=False, owner="worker-test")
    with pytest.raises(ValueError, match="daily alert delivery is not supported"):
        SchedulerConfig(dry_run_alerts=False, owner="worker-test")


def test_worker_one_shot_returns_failure_for_partial_daily_result(monkeypatch, tmp_path):
    monkeypatch.setenv(
        "CATALYST_DATABASE_URL",
        f"sqlite:///{(tmp_path / 'worker.db').as_posix()}",
    )
    monkeypatch.setenv("CATALYST_WORKER_INTERVAL_SECONDS", "0")

    def partial_run_once(*, engine, config):
        del engine, config
        return SchedulerRunResult(
            acquired_lock=True,
            reason=None,
            daily_result=SimpleNamespace(status="partial_success"),
        )

    monkeypatch.setattr(worker_main, "run_once", partial_run_once)

    assert worker_main.main() == 1


def test_cli_run_daily_json_smoke(monkeypatch, tmp_path, capsys):
    database_url = f"sqlite:///{(tmp_path / 'scheduler-cli.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    exit_code = cli_main(
        [
            "run-daily",
            "--as-of",
            "2026-05-09",
            "--available-at",
            "2026-05-10T01:00:00+00:00",
            "--json",
        ]
    )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["acquired_lock"] is True
    assert payload["reason"] is None
    assert payload["daily_result"]["status"] == "success"
    assert payload["daily_result"]["steps"]["llm_review"]["status"] == "skipped"


def test_cli_run_daily_rejects_unsupported_real_llm_and_delivery(monkeypatch, tmp_path, capsys):
    database_url = f"sqlite:///{(tmp_path / 'scheduler-cli.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    real_llm_exit = cli_main(
        [
            "run-daily",
            "--as-of",
            "2026-05-09",
            "--available-at",
            "2026-05-10T01:00:00+00:00",
            "--run-llm",
            "--real-llm",
        ]
    )
    delivery_exit = cli_main(
        [
            "run-daily",
            "--as-of",
            "2026-05-09",
            "--available-at",
            "2026-05-10T01:00:00+00:00",
            "--deliver-alerts",
        ]
    )

    captured = capsys.readouterr()
    assert real_llm_exit == 2
    assert delivery_exit == 2
    assert "run-daily --real-llm is not supported" in captured.err
    assert "run-daily --deliver-alerts is not supported" in captured.err

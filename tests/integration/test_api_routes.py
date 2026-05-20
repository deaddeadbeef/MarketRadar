from __future__ import annotations

import csv
import json
from datetime import UTC, date, datetime
from pathlib import Path

from fastapi.testclient import TestClient
from sqlalchemy import func, insert, select

import apps.api.main as api_main
from apps.api.main import create_app
from catalyst_radar.alerts.models import Alert, alert_id
from catalyst_radar.api.routes import agents as agent_routes
from catalyst_radar.api.routes import radar as radar_routes
from catalyst_radar.core.models import ActionState, DailyBar, Security
from catalyst_radar.dashboard import data as dashboard_data
from catalyst_radar.jobs.scheduler import SchedulerRunResult
from catalyst_radar.jobs.tasks import DailyRunResult, DailyRunSpec, JobStepResult
from catalyst_radar.market.manual_bars import MANUAL_BAR_COLUMNS
from catalyst_radar.security.audit import AuditLogRepository
from catalyst_radar.storage.alert_repositories import AlertRepository
from catalyst_radar.storage.db import create_schema, engine_from_url
from catalyst_radar.storage.repositories import MarketRepository
from catalyst_radar.storage.schema import (
    alerts,
    audit_events,
    budget_ledger,
    candidate_packets,
    candidate_states,
    daily_bars,
    data_quality_incidents,
    decision_cards,
    job_runs,
    normalized_provider_records,
    raw_provider_records,
    securities,
    signal_features,
    useful_alert_labels,
    user_feedback,
)
from catalyst_radar.universe.seed import UniverseSeedResult

AS_OF = datetime(2026, 5, 1, 21, tzinfo=UTC)
SOURCE_TS = datetime(2026, 5, 1, 20, 30, tzinfo=UTC)
AVAILABLE_AT = datetime(2026, 5, 1, 21, 5, tzinfo=UTC)


def test_api_health() -> None:
    client = TestClient(create_app())

    response = client.get("/api/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["service"] == "catalyst-radar"
    assert payload["build"]["version"] == "0.1.0"
    assert payload["build"]["commit"]
    assert "secret" not in str(payload).lower()


def test_api_app_loads_local_dotenv(monkeypatch) -> None:
    calls = []
    monkeypatch.setattr(api_main, "load_app_dotenv", lambda: calls.append(True) or True)

    create_app()

    assert calls == [True]


def test_get_candidates_returns_rows(tmp_path, monkeypatch) -> None:
    database_url = _database_url(tmp_path, "candidates.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = _create_database(database_url)
    _insert_candidate(engine)

    client = TestClient(create_app())

    response = client.get("/api/radar/candidates")

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["items"]) == 1
    item = payload["items"][0]
    assert item["ticker"] == "MSFT"
    assert item["state"] == ActionState.WARNING.value
    assert item["final_score"] == 78.0
    assert item["setup_type"] == "breakout"


def test_get_candidates_uses_latest_radar_run_scope(tmp_path, monkeypatch) -> None:
    database_url = _database_url(tmp_path, "candidates-latest-run.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)
    latest_run = {
        "as_of": "2026-05-01",
        "decision_available_at": AVAILABLE_AT.isoformat(),
        "finished_at": "2026-05-01T21:06:00+00:00",
    }
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        dashboard_data,
        "load_radar_run_summary",
        lambda _engine: latest_run,
        raising=False,
    )

    def load_radar_run_candidate_rows(
        _engine,
        summary,
        *,
        include_post_run_artifacts: bool = False,
    ) -> list[dict[str, object]]:
        captured["summary"] = summary
        captured["include_post_run_artifacts"] = include_post_run_artifacts
        return [{"ticker": "RUN", "state": ActionState.WARNING.value}]

    monkeypatch.setattr(
        dashboard_data,
        "load_radar_run_candidate_rows",
        load_radar_run_candidate_rows,
        raising=False,
    )
    monkeypatch.setattr(
        dashboard_data,
        "load_candidate_rows",
        lambda _engine: (_ for _ in ()).throw(AssertionError("fallback called")),
        raising=False,
    )
    client = TestClient(create_app())

    response = client.get("/api/radar/candidates")

    assert response.status_code == 200
    assert captured["summary"] == latest_run
    assert captured["include_post_run_artifacts"] is True
    assert response.json()["scope"] == {
        "source": "latest_radar_run",
        "as_of": "2026-05-01",
        "decision_available_at": AVAILABLE_AT.isoformat(),
        "finished_at": "2026-05-01T21:06:00+00:00",
    }
    assert response.json()["items"] == [
        {"ticker": "RUN", "state": ActionState.WARNING.value}
    ]


def test_auth_required_when_enabled(tmp_path, monkeypatch) -> None:
    database_url = _database_url(tmp_path, "auth-required.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("CATALYST_API_AUTH_MODE", "header")
    _create_database(database_url)

    client = TestClient(create_app())

    response = client.get("/api/radar/candidates")

    assert response.status_code == 401
    assert response.json() == {"detail": "role is required"}


def test_viewer_can_read_but_cannot_post_feedback(tmp_path, monkeypatch) -> None:
    database_url = _database_url(tmp_path, "auth-viewer.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("CATALYST_API_AUTH_MODE", "header")
    engine = _create_database(database_url)
    _insert_candidate(engine)

    client = TestClient(create_app())

    read_response = client.get(
        "/api/radar/candidates",
        headers={"X-Catalyst-Role": "viewer"},
    )
    write_response = client.post(
        "/api/feedback",
        headers={"X-Catalyst-Role": "viewer"},
        json={
            "artifact_type": "decision_card",
            "artifact_id": "card-MSFT",
            "ticker": "MSFT",
            "label": "useful",
        },
    )
    analyst_write_response = client.post(
        "/api/feedback",
        headers={"X-Catalyst-Role": "analyst"},
        json={
            "artifact_type": "decision_card",
            "artifact_id": "card-MSFT",
            "ticker": "MSFT",
            "label": "useful",
        },
    )

    assert read_response.status_code == 200
    assert write_response.status_code == 403
    assert write_response.json() == {"detail": "insufficient role"}
    assert analyst_write_response.status_code == 200


def test_post_agent_review_dry_run_logs_budget_and_audit(tmp_path, monkeypatch) -> None:
    database_url = _database_url(tmp_path, "agent-review.db")
    _configure_fake_safe_llm(monkeypatch, database_url)
    engine = _create_database(database_url)
    _insert_candidate(engine)

    client = TestClient(create_app())

    response = client.post(
        "/api/agents/review",
        json={
            "ticker": "MSFT",
            "as_of": AS_OF.date().isoformat(),
            "available_at": AVAILABLE_AT.isoformat(),
            "task": "mid_review",
            "mode": "dry_run",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "dry_run"
    assert payload["mode"] == "dry_run"
    assert payload["ticker"] == "MSFT"
    assert payload["candidate_packet_id"] == "packet-MSFT"
    assert payload["ledger"]["status"] == "dry_run"
    assert payload["ledger"]["candidate_packet_id"] == "packet-MSFT"
    with engine.connect() as conn:
        rows = [dict(row._mapping) for row in conn.execute(select(budget_ledger))]
    assert [(row["status"], row["candidate_packet_id"]) for row in rows] == [
        ("dry_run", "packet-MSFT")
    ]
    events = AuditLogRepository(engine).list_events(event_type="model_call_recorded")
    assert len(events) == 1
    assert events[0].ticker == "MSFT"
    assert events[0].candidate_packet_id == "packet-MSFT"
    assert events[0].status == "dry_run"


def test_get_agent_reviews_returns_budget_ledger_history(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "agent-review-history.db")
    _configure_fake_safe_llm(monkeypatch, database_url)
    engine = _create_database(database_url)
    _insert_candidate(engine)

    client = TestClient(create_app())

    post_response = client.post(
        "/api/agents/review",
        json={
            "ticker": "MSFT",
            "as_of": AS_OF.date().isoformat(),
            "available_at": AVAILABLE_AT.isoformat(),
            "task": "mid_review",
            "mode": "dry_run",
        },
    )
    history_response = client.get(
        "/api/agents/reviews",
        params={
            "ticker": "MSFT",
            "task": "mid_review",
            "available_at": AVAILABLE_AT.isoformat(),
        },
    )

    assert post_response.status_code == 200
    assert history_response.status_code == 200
    payload = history_response.json()
    assert payload["source"] == "budget_ledger"
    assert payload["schema_version"] == "agent-review-history-v1"
    assert payload["attempt_count"] == 1
    assert payload["filters"]["ticker"] == "MSFT"
    assert payload["rows"][0]["ticker"] == "MSFT"
    assert payload["rows"][0]["task"] == "mid_review"
    assert payload["rows"][0]["status"] == "dry_run"
    assert payload["rows"][0]["candidate_packet_id"] == "packet-MSFT"


def test_get_agent_brief_returns_zero_call_market_radar_brief(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "agent-brief-api.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)
    captured: dict[str, object] = {}

    def fake_dashboard_snapshot_payload(**kwargs):
        captured["snapshot_kwargs"] = kwargs
        return {
            "schema_version": "dashboard-cli-snapshot-v1",
            "priced_in_answer": {
                "schema_version": "priced-in-answer-v1",
                "status": "research_only",
                "answer": "Not fully priced yet.",
                "external_calls_made": 0,
            },
        }

    def fake_run_market_radar_agents(snapshot, _config, **kwargs):
        captured["snapshot"] = snapshot
        captured["agent_kwargs"] = kwargs
        return {
            "schema_version": "market-radar-agent-brief-v1",
            "mode": "dry_run",
            "status": "dry_run",
            "insights": ["Priced-in answer is research_only."],
            "external_calls_made": {"openai": 0, "market_data": 0, "broker": 0},
        }

    monkeypatch.setattr(
        agent_routes,
        "dashboard_snapshot_payload",
        fake_dashboard_snapshot_payload,
    )
    monkeypatch.setattr(
        agent_routes,
        "run_market_radar_agents",
        fake_run_market_radar_agents,
    )
    client = TestClient(create_app())

    response = client.get(
        "/api/agents/brief",
        params={
            "ticker": "msft",
            "available_at": "2026-05-18T16:00:00+00:00",
            "priced_in_status": "actionable",
            "usefulness": "research_useful",
            "source_gap": "options,local_text",
            "decision_gap": "decision_card",
            "scan_limit": 12,
            "scan_offset": 24,
            "telemetry_limit": 5,
            "goal": "Find unpriced expectation gaps.",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["schema_version"] == "market-radar-agent-brief-v1"
    assert payload["external_calls_made"] == {
        "broker": 0,
        "market_data": 0,
        "openai": 0,
    }
    filters = captured["snapshot_kwargs"]["filters"].normalized()
    assert filters.ticker == "MSFT"
    assert filters.available_at.isoformat() == "2026-05-18T16:00:00+00:00"
    assert filters.priced_in_status == "actionable"
    assert filters.priced_in_usefulness == "research_useful"
    assert filters.priced_in_source_gap == ("options", "local_text")
    assert filters.priced_in_decision_gap == ("decision_card",)
    assert filters.priced_in_limit == 12
    assert filters.priced_in_offset == 24
    assert filters.telemetry_limit == 5
    assert captured["agent_kwargs"] == {
        "real": False,
        "operator_goal": "Find unpriced expectation gaps.",
    }


def test_post_agent_review_requires_analyst_when_auth_enabled(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "agent-review-auth.db")
    _configure_fake_safe_llm(monkeypatch, database_url)
    engine = _create_database(database_url)
    _insert_candidate(engine)
    monkeypatch.setenv("CATALYST_API_AUTH_MODE", "header")

    client = TestClient(create_app())

    viewer_response = client.post(
        "/api/agents/review",
        headers={"X-Catalyst-Role": "viewer"},
        json={
            "ticker": "MSFT",
            "as_of": AS_OF.date().isoformat(),
            "available_at": AVAILABLE_AT.isoformat(),
        },
    )
    analyst_response = client.post(
        "/api/agents/review",
        headers={"X-Catalyst-Role": "analyst"},
        json={
            "ticker": "MSFT",
            "as_of": AS_OF.date().isoformat(),
            "available_at": AVAILABLE_AT.isoformat(),
            "task": "mid_review",
            "mode": "dry_run",
        },
    )

    assert viewer_response.status_code == 403
    assert analyst_response.status_code == 200


def test_post_radar_run_builds_scheduler_config(tmp_path, monkeypatch) -> None:
    database_url = _database_url(tmp_path, "radar-run.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("CATALYST_DAILY_MARKET_PROVIDER", "csv")
    monkeypatch.setenv("CATALYST_DAILY_PROVIDER", "csv")
    engine = _create_database(database_url)
    captured: dict[str, object] = {}

    def fake_run_once(*, engine, config):
        captured["engine_url"] = str(engine.url)
        captured["config"] = config
        return SchedulerRunResult(
            acquired_lock=True,
            reason=None,
            daily_result=None,
        )

    monkeypatch.setattr(radar_routes, "run_once", fake_run_once)
    client = TestClient(create_app())

    response = client.post(
        "/api/radar/runs",
        headers={"X-Catalyst-Actor": "tester", "X-Catalyst-Role": "analyst"},
        json={
            "as_of": "2026-05-09",
            "decision_available_at": "2026-05-10T01:00:00Z",
            "outcome_available_at": "2026-05-15T01:00:00Z",
            "provider": "csv",
            "universe": "liquid-us",
            "tickers": ["msft", "NVDA", "MSFT"],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload == {
        "acquired_lock": True,
        "reason": None,
        "lock_expires_at": None,
        "daily_result": None,
        "discovery_snapshot": payload["discovery_snapshot"],
    }
    assert payload["discovery_snapshot"]["status"] == "attention"
    assert payload["discovery_snapshot"]["blockers"][0]["code"] == "no_run"
    config = captured["config"]
    assert captured["engine_url"] == database_url
    assert config.owner == "api-radar-run"
    assert config.as_of == date(2026, 5, 9)
    assert config.decision_available_at == datetime(2026, 5, 10, 1, 0, tzinfo=UTC)
    assert config.outcome_available_at == datetime(2026, 5, 15, 1, 0, tzinfo=UTC)
    assert config.provider == "csv"
    assert config.universe == "liquid-us"
    assert config.tickers == ("MSFT", "NVDA")
    assert config.run_llm is False
    assert config.llm_dry_run is True
    assert config.dry_run_alerts is True
    telemetry = _audit_event_rows(engine)
    assert [row["event_type"] for row in telemetry] == [
        "telemetry.radar_run.requested",
        "telemetry.radar_run.completed",
    ]
    assert telemetry[0]["actor_id"] == "tester"
    assert telemetry[0]["actor_role"] == "analyst"
    assert telemetry[0]["artifact_type"] == "radar_run"
    assert telemetry[0]["artifact_id"].startswith("radar-run-api:")
    assert telemetry[1]["artifact_id"] == telemetry[0]["artifact_id"]
    assert telemetry[0]["metadata"]["lock_name"] == "daily-run"
    assert telemetry[0]["metadata"]["tickers"] == ["MSFT", "NVDA", "MSFT"]
    assert telemetry[1]["status"] == "success"
    assert telemetry[1]["metadata"]["daily_status"] is None
    assert telemetry[1]["after_payload"]["acquired_lock"] is True


def test_post_radar_run_telemetry_summarizes_skipped_steps(tmp_path, monkeypatch) -> None:
    database_url = _database_url(tmp_path, "radar-run-skip-telemetry.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = _create_database(database_url)

    def fake_run_once(**_kwargs):
        spec = DailyRunSpec(
            as_of=date(2026, 5, 9),
            decision_available_at=datetime(2026, 5, 10, 1, 0, tzinfo=UTC),
            run_llm=False,
            dry_run_alerts=True,
        )
        daily_result = DailyRunResult(
            status="success",
            spec=spec,
            steps=(
                JobStepResult(
                    name="daily_bar_ingest",
                    status="success",
                    job_id="job-daily",
                    requested_count=43,
                    raw_count=43,
                    normalized_count=43,
                ),
                JobStepResult(
                    name="event_ingest",
                    status="skipped",
                    job_id="job-events",
                    reason="no_scheduled_event_provider",
                ),
                JobStepResult(
                    name="local_text_triage",
                    status="skipped",
                    job_id="job-text",
                    reason="no_text_inputs",
                ),
            ),
        )
        return SchedulerRunResult(
            acquired_lock=True,
            reason=None,
            daily_result=daily_result,
        )

    monkeypatch.setattr(radar_routes, "run_once", fake_run_once)
    client = TestClient(create_app())

    response = client.post("/api/radar/runs", json={})

    assert response.status_code == 200
    telemetry = _audit_event_rows(engine)
    completed = telemetry[1]
    assert completed["event_type"] == "telemetry.radar_run.completed"
    assert completed["metadata"]["step_counts"] == {"skipped": 2, "success": 1}
    assert completed["metadata"]["outcome_category_counts"] == {
        "blocked_input": 1,
        "completed": 1,
        "not_ready": 1,
    }
    assert completed["metadata"]["skip_reason_counts"] == {
        "no_scheduled_event_provider": 1,
        "no_text_inputs": 1,
    }
    assert completed["metadata"]["blocked_steps"] == [
        {
            "step": "event_ingest",
            "reason": "no_scheduled_event_provider",
            "category": "blocked_input",
            "label": "Blocked input",
            "requested_count": 0,
            "raw_count": 0,
            "normalized_count": 0,
        },
    ]
    assert completed["metadata"]["expected_gate_steps"] == []
    assert completed["metadata"]["skipped_steps"] == [
        {
            "step": "event_ingest",
            "reason": "no_scheduled_event_provider",
            "category": "blocked_input",
            "label": "Blocked input",
            "requested_count": 0,
            "raw_count": 0,
            "normalized_count": 0,
        },
        {
            "step": "local_text_triage",
            "reason": "no_text_inputs",
            "category": "not_ready",
            "label": "Not ready",
            "requested_count": 0,
            "raw_count": 0,
            "normalized_count": 0,
        },
    ]


def test_post_radar_run_requires_analyst_when_auth_enabled(tmp_path, monkeypatch) -> None:
    database_url = _database_url(tmp_path, "radar-run-auth.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("CATALYST_API_AUTH_MODE", "header")
    _create_database(database_url)
    monkeypatch.setattr(
        radar_routes,
        "run_once",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("run_once called")),
    )
    client = TestClient(create_app())

    response = client.post(
        "/api/radar/runs",
        headers={"X-Catalyst-Role": "viewer"},
        json={},
    )

    assert response.status_code == 403
    assert response.json() == {"detail": "insufficient role"}


def test_post_radar_run_rejects_unsupported_real_llm(tmp_path, monkeypatch) -> None:
    database_url = _database_url(tmp_path, "radar-run-real-llm.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = _create_database(database_url)
    monkeypatch.setattr(
        radar_routes,
        "run_once",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("run_once called")),
    )
    client = TestClient(create_app())

    response = client.post(
        "/api/radar/runs",
        json={"run_llm": True, "llm_dry_run": False},
    )

    assert response.status_code == 422
    assert response.json() == {
        "detail": "real daily LLM review is not supported; use run-llm-review per candidate"
    }
    telemetry = _audit_event_rows(engine)
    assert [row["event_type"] for row in telemetry] == [
        "telemetry.radar_run.requested",
        "telemetry.radar_run.rejected",
    ]
    assert telemetry[1]["status"] == "rejected"
    assert telemetry[1]["reason"] == (
        "real daily LLM review is not supported; use run-llm-review per candidate"
    )


def test_post_radar_run_reports_lock_contention(tmp_path, monkeypatch) -> None:
    database_url = _database_url(tmp_path, "radar-run-lock.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = _create_database(database_url)
    expires_at = datetime(2026, 5, 10, 1, 5, tzinfo=UTC)

    def fake_run_once(*, engine, config):
        del engine, config
        return SchedulerRunResult(
            acquired_lock=False,
            reason="lock_held",
            daily_result=None,
            lock_expires_at=expires_at,
        )

    monkeypatch.setattr(radar_routes, "run_once", fake_run_once)
    client = TestClient(create_app())

    response = client.post("/api/radar/runs", json={})

    assert response.status_code == 409
    assert response.json() == {
        "detail": {
            "acquired_lock": False,
            "reason": "lock_held",
            "lock_expires_at": expires_at.isoformat(),
            "daily_result": None,
        }
    }
    telemetry = _audit_event_rows(engine)
    assert [row["event_type"] for row in telemetry] == [
        "telemetry.radar_run.requested",
        "telemetry.radar_run.lock_contention",
    ]
    assert telemetry[1]["status"] == "blocked"
    assert telemetry[1]["reason"] == "lock_held"
    assert telemetry[1]["after_payload"]["acquired_lock"] is False


def test_post_radar_run_rate_limits_repeated_manual_requests(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "radar-run-cooldown.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("CATALYST_RADAR_RUN_MIN_INTERVAL_SECONDS", "60")
    engine = _create_database(database_url)
    calls = []

    def fake_run_once(*, engine, config):
        calls.append({"engine_url": str(engine.url), "config": config})
        return SchedulerRunResult(
            acquired_lock=True,
            reason=None,
            daily_result=None,
        )

    monkeypatch.setattr(radar_routes, "run_once", fake_run_once)
    client = TestClient(create_app())

    first = client.post("/api/radar/runs", json={})
    second = client.post("/api/radar/runs", json={})

    assert first.status_code == 200
    assert second.status_code == 429
    assert second.json()["detail"]["operation"] == "manual_radar_run"
    assert int(second.headers["Retry-After"]) > 0
    assert len(calls) == 1
    telemetry = _audit_event_rows(engine)
    assert [row["event_type"] for row in telemetry] == [
        "telemetry.radar_run.requested",
        "telemetry.radar_run.completed",
        "telemetry.radar_run.requested",
        "telemetry.radar_run.rate_limited",
    ]
    assert telemetry[0]["metadata"]["min_interval_seconds"] == 60
    assert telemetry[3]["status"] == "blocked"
    assert telemetry[3]["reason"] == "rate_limited"


def test_post_universe_seed_uses_capped_polygon_ingest(tmp_path, monkeypatch) -> None:
    database_url = _database_url(tmp_path, "universe-seed.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("CATALYST_POLYGON_API_KEY", "fixture-key")
    monkeypatch.setenv("CATALYST_POLYGON_TICKERS_MAX_PAGES", "2")
    engine = _create_database(database_url)
    calls = []

    def fake_seed(engine_arg, *, config, max_pages, date_value):
        calls.append(
            {
                "database_url": str(engine_arg.url),
                "max_pages": max_pages,
                "date_value": date_value,
                "configured_cap": config.polygon_tickers_max_pages,
            }
        )
        return UniverseSeedResult(
            provider="polygon",
            job_id="job-seed",
            max_pages=max_pages,
            date=date_value,
            requested_count=2,
            raw_count=2,
            normalized_count=2,
            security_count=2,
            daily_bar_count=0,
            holding_count=0,
            rejected_count=0,
        )

    monkeypatch.setattr(radar_routes, "seed_polygon_tickers", fake_seed)
    client = TestClient(create_app())

    response = client.post(
        "/api/radar/universe/seed",
        headers={"X-Catalyst-Actor": "tester", "X-Catalyst-Role": "analyst"},
        json={"provider": "polygon", "max_pages": 2, "date": "2026-05-08"},
    )

    assert response.status_code == 200
    assert response.json()["security_count"] == 2
    assert calls == [
        {
            "database_url": str(engine.url),
            "max_pages": 2,
            "date_value": date(2026, 5, 8),
            "configured_cap": 2,
        }
    ]
    telemetry = _audit_event_rows(engine)
    assert [row["event_type"] for row in telemetry] == [
        "telemetry.universe_seed.requested",
        "telemetry.universe_seed.completed",
    ]
    assert telemetry[0]["actor_id"] == "tester"
    assert telemetry[0]["actor_role"] == "analyst"
    assert telemetry[0]["metadata"]["configured_max_pages"] == 2
    assert telemetry[1]["after_payload"]["job_id"] == "job-seed"


def test_post_universe_seed_rejects_max_pages_above_configured_cap(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "universe-seed-cap.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("CATALYST_POLYGON_API_KEY", "fixture-key")
    monkeypatch.setenv("CATALYST_POLYGON_TICKERS_MAX_PAGES", "1")
    engine = _create_database(database_url)
    monkeypatch.setattr(
        radar_routes,
        "seed_polygon_tickers",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("seed called")),
    )
    client = TestClient(create_app())

    response = client.post(
        "/api/radar/universe/seed",
        json={"provider": "polygon", "max_pages": 2},
    )

    assert response.status_code == 422
    assert "max_pages exceeds configured cap" in response.json()["detail"]
    telemetry = _audit_event_rows(engine)
    assert [row["event_type"] for row in telemetry] == [
        "telemetry.universe_seed.requested",
        "telemetry.universe_seed.rejected",
    ]


def test_post_universe_seed_rate_limits_repeated_requests(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "universe-seed-rate.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("CATALYST_POLYGON_API_KEY", "fixture-key")
    monkeypatch.setenv("CATALYST_POLYGON_TICKER_SEED_MIN_INTERVAL_SECONDS", "60")
    engine = _create_database(database_url)
    calls = []

    def fake_seed(*_args, **kwargs):
        calls.append(kwargs)
        return UniverseSeedResult(
            provider="polygon",
            job_id="job-seed",
            max_pages=1,
            date=None,
            requested_count=1,
            raw_count=1,
            normalized_count=1,
            security_count=1,
            daily_bar_count=0,
            holding_count=0,
            rejected_count=0,
        )

    monkeypatch.setattr(radar_routes, "seed_polygon_tickers", fake_seed)
    client = TestClient(create_app())

    first = client.post("/api/radar/universe/seed", json={"provider": "polygon"})
    second = client.post("/api/radar/universe/seed", json={"provider": "polygon"})

    assert first.status_code == 200
    assert second.status_code == 429
    assert int(second.headers["Retry-After"]) > 0
    assert len(calls) == 1
    telemetry = _audit_event_rows(engine)
    assert [row["event_type"] for row in telemetry] == [
        "telemetry.universe_seed.requested",
        "telemetry.universe_seed.completed",
        "telemetry.universe_seed.requested",
        "telemetry.universe_seed.rate_limited",
    ]


def test_post_universe_seed_requires_analyst_when_auth_enabled(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "universe-seed-auth.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("CATALYST_API_AUTH_MODE", "header")
    _create_database(database_url)
    monkeypatch.setattr(
        radar_routes,
        "seed_polygon_tickers",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("seed called")),
    )
    client = TestClient(create_app())

    response = client.post(
        "/api/radar/universe/seed",
        headers={"X-Catalyst-Role": "viewer"},
        json={"provider": "polygon"},
    )

    assert response.status_code == 403
    assert response.json() == {"detail": "insufficient role"}


def test_get_latest_radar_run_returns_summary(tmp_path, monkeypatch) -> None:
    database_url = _database_url(tmp_path, "radar-run-latest.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)
    monkeypatch.setattr(
        dashboard_data,
        "load_radar_run_summary",
        lambda _engine: {"status": "success", "step_count": 10},
        raising=False,
    )
    monkeypatch.setattr(
        dashboard_data,
        "radar_discovery_snapshot_payload",
        lambda *_args, **_kwargs: {
            "status": "fixture",
            "yield": {"candidate_states": 2},
        },
        raising=False,
    )
    client = TestClient(create_app())

    response = client.get("/api/radar/runs/latest")

    assert response.status_code == 200
    assert response.json() == {
        "status": "success",
        "step_count": 10,
        "discovery_snapshot": {
            "status": "fixture",
            "yield": {"candidate_states": 2},
        },
    }


def test_get_latest_radar_run_redacts_restricted_discovery_snapshot(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "radar-run-latest-redacted.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)
    monkeypatch.setattr(
        dashboard_data,
        "load_radar_run_summary",
        lambda _engine: {"status": "success", "step_count": 10},
        raising=False,
    )
    monkeypatch.setattr(
        dashboard_data,
        "radar_discovery_snapshot_payload",
        lambda *_args, **_kwargs: {
            "status": "fixture",
            "top_discoveries": [
                {
                    "ticker": "MSFT",
                    "why_now": "restricted fixture catalyst",
                    "audit": {
                        "provider_license_policy": {
                            "license_tags": ["local-csv-fixture"],
                        }
                    },
                }
            ],
            "evidence_plan": {
                "schema_version": "priced-in-evidence-plan-v1",
                "status": "blocked",
                "external_calls_made": 0,
                "steps": [{"priority": 1, "area": "universe", "status": "blocked"}],
            },
        },
        raising=False,
    )
    client = TestClient(create_app())

    response = client.get("/api/radar/runs/latest")

    assert response.status_code == 200
    assert response.json()["discovery_snapshot"]["top_discoveries"] == [
        {
            "external_export_blocked": True,
            "license_tags": ["local-csv-fixture"],
            "attribution_required": False,
        }
    ]


def test_get_radar_readiness_returns_decision_contract(tmp_path, monkeypatch) -> None:
    database_url = _database_url(tmp_path, "radar-readiness.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)
    monkeypatch.setattr(
        dashboard_data,
        "radar_readiness_payload",
        lambda _engine, _config: {
            "schema_version": "radar-readiness-v1",
            "status": "research_only",
            "decision_mode": "research_only",
            "safe_to_make_investment_decision": False,
            "next_action": "Configure live sources.",
            "operator_next_step": {
                "schema_version": "operator-next-step-v1",
                "status": "blocked",
                "priority": "must_fix",
                "area": "Live market scan",
                "action": "Configure live sources.",
                "external_calls_made": 0,
            },
            "operator_work_queue": {
                "schema_version": "operator-work-queue-v1",
                "status": "blocked",
                "safe_to_make_investment_decision": False,
                "rows": [{"priority": "must_fix", "area": "Live market scan"}],
            },
            "candidate_decision_labels": [
                {"ticker": "MSFT", "decision_status": "research_only"}
            ],
        },
        raising=False,
    )
    client = TestClient(create_app())

    response = client.get("/api/radar/readiness")

    assert response.status_code == 200
    assert response.json() == {
        "schema_version": "radar-readiness-v1",
        "status": "research_only",
        "decision_mode": "research_only",
        "safe_to_make_investment_decision": False,
        "next_action": "Configure live sources.",
        "operator_next_step": {
            "schema_version": "operator-next-step-v1",
            "status": "blocked",
            "priority": "must_fix",
            "area": "Live market scan",
            "action": "Configure live sources.",
            "external_calls_made": 0,
        },
        "operator_work_queue": {
            "schema_version": "operator-work-queue-v1",
            "status": "blocked",
            "safe_to_make_investment_decision": False,
            "rows": [{"priority": "must_fix", "area": "Live market scan"}],
        },
        "candidate_decision_labels": [
            {"ticker": "MSFT", "decision_status": "research_only"}
        ],
    }


def test_get_radar_readiness_redacts_restricted_discovery_snapshot(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "radar-readiness-redacted.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)
    monkeypatch.setattr(
        dashboard_data,
        "radar_readiness_payload",
        lambda _engine, _config: {
            "schema_version": "radar-readiness-v1",
            "status": "research_only",
            "discovery_snapshot": {
                "top_discoveries": [
                    {
                        "ticker": "MSFT",
                        "why_now": "restricted fixture catalyst",
                        "audit": {
                            "provider_license_policy": {
                                "license_tags": ["local-csv-fixture"],
                            }
                        },
                    }
                ],
            },
            "candidate_decision_labels": [
                {
                    "ticker": "MSFT",
                    "top_catalyst": "restricted fixture catalyst",
                    "audit": {
                        "provider_license_policy": {
                            "license_tags": ["local-csv-fixture"],
                        }
                    },
                }
            ],
        },
        raising=False,
    )
    client = TestClient(create_app())

    response = client.get("/api/radar/readiness")

    assert response.status_code == 200
    assert response.json()["discovery_snapshot"]["top_discoveries"] == [
        {
            "external_export_blocked": True,
            "license_tags": ["local-csv-fixture"],
            "attribution_required": False,
        }
    ]
    assert response.json()["candidate_decision_labels"] == [
        {
            "external_export_blocked": True,
            "license_tags": ["local-csv-fixture"],
            "attribution_required": False,
        }
    ]


def test_get_radar_live_activation_returns_read_only_contract(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "radar-live-activation.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)
    monkeypatch.setattr(
        dashboard_data,
        "load_radar_run_summary",
        lambda _engine: {"status": "success", "steps": []},
        raising=False,
    )
    monkeypatch.setattr(
        dashboard_data,
        "load_broker_summary",
        lambda _engine: {"status": "not_connected"},
        raising=False,
    )
    monkeypatch.setattr(
        dashboard_data,
        "live_data_activation_contract_payload",
        lambda _config, *, radar_run_summary, broker_summary: {
            "schema_version": "live-data-activation-contract-v1",
            "status": "blocked",
            "read_only": True,
            "makes_external_calls": False,
            "evidence": (
                f"run={radar_run_summary['status']}; "
                f"broker={broker_summary['status']}"
            ),
        },
        raising=False,
    )
    client = TestClient(create_app())

    response = client.get("/api/radar/live-activation")

    assert response.status_code == 200
    assert response.json() == {
        "schema_version": "live-data-activation-contract-v1",
        "status": "blocked",
        "read_only": True,
        "makes_external_calls": False,
        "evidence": "run=success; broker=not_connected",
    }


def test_get_radar_research_shortlist_returns_redacted_rows(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "radar-research-shortlist.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)
    monkeypatch.setattr(
        dashboard_data,
        "radar_research_shortlist_payload",
        lambda _engine, _config, *, limit: {
            "schema_version": "research-shortlist-v1",
            "status": "research",
            "count": 1,
            "rows": [
                {
                    "priority": "research_now",
                    "ticker": "MSFT",
                    "decision_status": "research_only",
                    "state": "Warning",
                    "score": 84.0,
                    "setup": "guidance_raise",
                    "why_now": "restricted fixture catalyst",
                    "top_catalyst": "restricted fixture guide raise",
                    "risk_or_gap": "restricted fixture gap",
                    "next_step": "Open restricted fixture source.",
                    "decision_card_id": "n/a",
                    "audit": {
                        "provider_license_policy": {
                            "license_tags": ["local-csv-fixture"],
                        }
                    },
                }
            ],
        },
        raising=False,
    )
    client = TestClient(create_app())

    response = client.get("/api/radar/research-shortlist?limit=3")

    assert response.status_code == 200
    assert response.json()["rows"] == [
        {
            "priority": "research_now",
            "ticker": "MSFT",
            "decision_status": "research_only",
            "state": "Warning",
            "score": 84.0,
            "setup": "guidance_raise",
            "decision_card_id": "n/a",
            "next_step": (
                "Review source details in the local dashboard; provider text is "
                "withheld by export policy."
            ),
            "external_export_blocked": True,
            "license_tags": ["local-csv-fixture"],
            "attribution_required": False,
            "restricted_fields": ["why_now", "top_catalyst", "risk_or_gap"],
        }
    ]


def test_get_radar_priced_in_queue_returns_cli_ready_rows(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "radar-priced-in.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)

    def fake_priced_in_queue_payload(
        _engine,
        _config,
        *,
        limit,
        offset,
        available_at,
        status,
        usefulness,
        source_gap,
        decision_gap,
        min_gap,
        stocks_only,
    ) -> dict[str, object]:
        return {
            "schema_version": "priced-in-queue-v1",
            "status": "ready",
            "external_calls_made": 0,
            "filters": {
                "limit": limit,
                "offset": offset,
                "status": status,
                "usefulness": usefulness,
                "source_gap": [source_gap],
                "decision_gap": [decision_gap],
                "min_gap": min_gap,
                "stocks_only": stocks_only,
                "available_at": available_at.isoformat() if available_at else None,
            },
            "count": 1,
            "total_count": 25,
            "offset": offset,
            "has_more": True,
            "usefulness_counts": {"research_useful": 1},
            "source_coverage": {
                "schema_version": "priced-in-source-coverage-v1",
                "row_count": 1,
                "weak_sources": ["options"],
                "summary": "options 0/1 (1 missing)",
                "actions": [
                    {
                        "source": "options",
                        "status": "missing",
                        "coverage_pct": 0.0,
                        "next_action": "Treat options as absent.",
                        "command": "catalyst-radar ingest-options --fixture <options-summary.json>",
                    }
                ],
            },
            "rows": [
                {
                    "ticker": "MSFT",
                    "priced_in_status": "bullish_not_priced_in",
                    "emotion_reaction_gap": 42.0,
                    "why_now": "Emotion is ahead of price reaction.",
                    "data_sources": {
                        "available": ["market_bars", "catalyst_events"],
                        "missing": ["options"],
                        "stale": [],
                        "summary": "available: market_bars, catalyst_events; missing: options",
                    },
                    "usefulness": {
                        "schema_version": "priced-in-usefulness-verdict-v1",
                        "status": "research_useful",
                        "decision_ready": False,
                        "missing_for_decision": ["decision_card", "options"],
                    },
                    "next_step": "Open candidate detail.",
                }
            ],
        }

    monkeypatch.setattr(
        dashboard_data,
        "priced_in_queue_payload",
        fake_priced_in_queue_payload,
        raising=False,
    )
    client = TestClient(create_app())

    response = client.get(
        "/api/radar/priced-in?limit=3&offset=6"
        "&available_at=2026-05-18T16:00:00%2B00:00"
        "&status=bullish_not_priced_in"
        "&usefulness=research_useful&source_gap=options&decision_gap=decision_card"
        "&min_gap=10&stocks_only=true"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["schema_version"] == "priced-in-queue-v1"
    assert payload["external_calls_made"] == 0
    assert payload["filters"] == {
        "limit": 3,
        "offset": 6,
        "status": "bullish_not_priced_in",
        "usefulness": "research_useful",
        "source_gap": ["options"],
        "decision_gap": ["decision_card"],
        "min_gap": 10.0,
        "stocks_only": True,
        "available_at": "2026-05-18T16:00:00+00:00",
    }
    assert payload["usefulness_counts"] == {"research_useful": 1}
    assert payload["total_count"] == 25
    assert payload["offset"] == 6
    assert payload["has_more"] is True
    assert payload["source_coverage"]["actions"][0]["source"] == "options"
    assert payload["source_coverage"]["actions"][0]["status"] == "missing"
    assert payload["rows"][0]["ticker"] == "MSFT"
    assert payload["rows"][0]["usefulness"]["status"] == "research_useful"
    assert payload["rows"][0]["data_sources"]["available"] == [
        "market_bars",
        "catalyst_events",
    ]

    response = client.get("/api/radar/priced-in?all_rows=true&limit=3&offset=6")

    assert response.status_code == 200
    payload = response.json()
    assert payload["filters"]["limit"] == 1_000_000
    assert payload["filters"]["offset"] == 0

    response = client.get("/api/radar/priced-in?decision_ready=true&limit=5")

    assert response.status_code == 200
    payload = response.json()
    assert payload["filters"]["status"] == "actionable"
    assert payload["filters"]["usefulness"] == "decision_useful"
    assert payload["filters"]["limit"] == 5


def test_get_radar_priced_in_preflight_returns_zero_call_steps(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "radar-priced-in-preflight.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)
    monkeypatch.setattr(
        dashboard_data,
        "priced_in_preflight_payload",
        lambda _engine, _config: {
            "schema_version": "priced-in-preflight-v1",
            "status": "blocked",
            "external_calls_made": 0,
            "rows": [
                {
                    "area": "universe",
                    "status": "blocked",
                    "finding": "tiny universe",
                    "next_action": "seed tickers",
                }
            ],
            "evidence_plan": {
                "schema_version": "priced-in-evidence-plan-v1",
                "status": "blocked",
                "external_calls_made": 0,
                "steps": [{"priority": 1, "area": "universe", "status": "blocked"}],
            },
        },
        raising=False,
    )
    client = TestClient(create_app())

    response = client.get("/api/radar/priced-in/preflight")

    assert response.status_code == 200
    payload = response.json()
    assert payload["schema_version"] == "priced-in-preflight-v1"
    assert payload["external_calls_made"] == 0
    assert payload["rows"][0]["area"] == "universe"
    assert payload["evidence_plan"]["schema_version"] == "priced-in-evidence-plan-v1"


def test_get_radar_priced_in_answer_returns_current_scan_answer(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "radar-priced-in-answer.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)
    captured: dict[str, object] = {}

    def fake_priced_in_answer_payload(_engine, _config, **kwargs) -> dict[str, object]:
        captured.update(kwargs)
        return {
            "schema_version": "priced-in-answer-v1",
            "status": "research_only",
            "question": "Has price fully matched market expectations?",
            "answer": "Not fully priced for 1 research lead, but not decision-ready.",
            "decision_ready": False,
            "can_make_investment_decision": False,
            "external_calls_made": 0,
            "counts": {"research_lead_rows": 1},
            "top_rows": [{"ticker": "MSFT", "usefulness": "research_useful"}],
        }

    monkeypatch.setattr(
        dashboard_data,
        "priced_in_answer_payload",
        fake_priced_in_answer_payload,
        raising=False,
    )
    client = TestClient(create_app())

    response = client.get(
        "/api/radar/priced-in/answer?limit=3"
        "&available_at=2026-05-18T16:00:00%2B00:00"
        "&status=actionable&usefulness=research_useful&source_gap=options"
        "&decision_gap=decision_card&min_gap=10"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["schema_version"] == "priced-in-answer-v1"
    assert payload["status"] == "research_only"
    assert payload["decision_ready"] is False
    assert payload["can_make_investment_decision"] is False
    assert payload["external_calls_made"] == 0
    assert payload["top_rows"][0]["ticker"] == "MSFT"
    assert captured["limit"] == 3
    assert captured["available_at"].isoformat() == "2026-05-18T16:00:00+00:00"
    assert captured["status"] == "actionable"
    assert captured["usefulness"] == "research_useful"
    assert captured["source_gap"] == "options"
    assert captured["decision_gap"] == "decision_card"
    assert captured["min_gap"] == 10.0


def test_get_radar_priced_in_audit_returns_zero_call_audit(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "radar-priced-in-audit.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)
    captured: dict[str, object] = {}

    def fake_priced_in_audit_payload(_engine, _config, **kwargs) -> dict[str, object]:
        captured.update(kwargs)
        return {
            "schema_version": "priced-in-full-scan-audit-v1",
            "status": "attention",
            "question": "Can MarketRadar answer whether price matches market expectations?",
            "answer": "Partially.",
            "external_calls_made": 0,
            "scope": {"mode": "full_scan", "ranked_rows": 12_087},
            "sources": [],
        }

    monkeypatch.setattr(
        dashboard_data,
        "priced_in_full_scan_audit_payload",
        fake_priced_in_audit_payload,
        raising=False,
    )
    client = TestClient(create_app())

    response = client.get(
        "/api/radar/priced-in/audit?"
        "available_at=2026-05-18T16:00:00%2B00:00&"
        "source_gap=options&limit=7&offset=11"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["schema_version"] == "priced-in-full-scan-audit-v1"
    assert payload["external_calls_made"] == 0
    assert payload["scope"]["mode"] == "full_scan"
    assert captured["available_at"].isoformat() == "2026-05-18T16:00:00+00:00"
    assert captured["source_gap"] == "options"
    assert captured["preview_limit"] == 7
    assert captured["preview_offset"] == 11
    assert captured["all_rows"] is False

    captured.clear()
    response = client.get("/api/radar/priced-in/audit?limit=7&offset=11&all_rows=true")

    assert response.status_code == 200
    assert captured["preview_limit"] == 1_000_000
    assert captured["preview_offset"] == 0
    assert captured["all_rows"] is True


def test_get_radar_priced_in_source_batches_returns_zero_call_plan(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "radar-priced-in-source-batches.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)
    captured: dict[str, object] = {}

    def fake_source_batches_payload(_engine, _config, **kwargs) -> dict[str, object]:
        captured.update(kwargs)
        return {
            "schema_version": "priced-in-source-batches-v1",
            "status": "ready",
            "source": kwargs["source"],
            "external_calls_made": 0,
            "total_gap_rows": 2,
            "batch_count": 1,
            "count": 1,
            "batches": [
                {
                    "number": 1,
                    "tickers": ["MSFT", "AAPL"],
                    "command": (
                        "catalyst-radar schwab-market-sync "
                        "--ticker MSFT --ticker AAPL"
                    ),
                }
            ],
        }

    monkeypatch.setattr(
        dashboard_data,
        "priced_in_source_gap_batches_payload",
        fake_source_batches_payload,
        raising=False,
    )
    client = TestClient(create_app())

    response = client.get(
        "/api/radar/priced-in/source-batches?source=options&batch_limit=2"
        "&batch_offset=1&batch_size=5&available_at=2026-05-18T16:00:00%2B00:00"
        "&all_batches=true&status=all&usefulness=research_useful"
        "&decision_gap=options&min_gap=12&stocks_only=true"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["schema_version"] == "priced-in-source-batches-v1"
    assert payload["external_calls_made"] == 0
    assert payload["batches"][0]["tickers"] == ["MSFT", "AAPL"]
    assert captured["source"] == "options"
    assert captured["batch_limit"] == 2
    assert captured["batch_offset"] == 1
    assert captured["batch_size"] == 5
    assert captured["all_batches"] is True
    assert captured["available_at"].isoformat() == "2026-05-18T16:00:00+00:00"
    assert captured["status"] == "all"
    assert captured["usefulness"] == "research_useful"
    assert captured["decision_gap"] == "options"
    assert captured["min_gap"] == 12.0
    assert captured["stocks_only"] is True


def test_get_radar_priced_in_source_batches_can_return_all_source_overview(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "radar-priced-in-source-overview.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)
    captured: dict[str, object] = {}

    def fake_overview_payload(_engine, _config, **kwargs) -> dict[str, object]:
        captured.update(kwargs)
        return {
            "schema_version": "priced-in-source-batch-overview-v1",
            "status": "ready",
            "external_calls_made": 0,
            "source_count": 2,
            "coverage_first_recommendation": {
                "source": "catalyst_events",
                "mode": "coverage_first",
            },
            "decision_shortcut_recommendation": {
                "source": "options",
                "mode": "decision_shortcut",
            },
            "sources": [
                {
                    "source": "options",
                    "status": "ready",
                    "execute_next_command": (
                        "catalyst-radar priced-in-source-batches "
                        "--source options --execute-next"
                    ),
                }
            ],
        }

    monkeypatch.setattr(
        dashboard_data,
        "priced_in_all_source_gap_batches_payload",
        fake_overview_payload,
        raising=False,
    )
    client = TestClient(create_app())

    response = client.get(
        "/api/radar/priced-in/source-batches?source=all&batch_size=5"
        "&available_at=2026-05-18T16:00:00%2B00:00&status=all"
        "&usefulness=decision_useful&decision_gap=options&min_gap=12"
        "&stocks_only=true"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["schema_version"] == "priced-in-source-batch-overview-v1"
    assert payload["external_calls_made"] == 0
    assert payload["coverage_first_recommendation"]["source"] == "catalyst_events"
    assert payload["decision_shortcut_recommendation"]["source"] == "options"
    assert payload["sources"][0]["source"] == "options"
    assert captured["batch_size"] == 5
    assert captured["available_at"].isoformat() == "2026-05-18T16:00:00+00:00"
    assert captured["status"] == "all"
    assert captured["usefulness"] == "decision_useful"
    assert captured["decision_gap"] == "options"
    assert captured["min_gap"] == 12.0
    assert captured["stocks_only"] is True


def test_post_radar_priced_in_source_batch_execute_next_runs_one_chunk(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "radar-priced-in-source-execute.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)
    captured: dict[str, object] = {}

    def fake_execute(_engine, _config, **kwargs) -> dict[str, object]:
        captured.update(kwargs)
        return {
            "schema_version": "priced-in-source-batch-execution-v1",
            "source": kwargs["source"],
            "status": "executed",
            "external_calls_made": 0,
            "plan": {"status": "ready", "batch_count": 1},
            "batch": {"number": 1, "tickers": ["MSFT"]},
            "result": {
                "provider": "local_text",
                "endpoint": "features-batch",
                "ticker_count": 1,
                "feature_count": 1,
                "snippet_count": 2,
                "external_calls_made": 0,
            },
            "post_execution": {
                "schema_version": "priced-in-source-batch-post-execution-v1",
                "source": kwargs["source"],
                "status": "improved",
                "external_calls_made": 0,
                "before_gap_rows": 10,
                "after_gap_rows": 9,
                "gap_rows_resolved": 1,
                "before_plannable_rows": 10,
                "after_plannable_rows": 9,
                "plannable_rows_resolved": 1,
                "before_batch_count": 2,
                "after_batch_count": 2,
                "next_action": "Review the updated next batch.",
            },
        }

    monkeypatch.setattr(
        radar_routes,
        "execute_priced_in_source_batch",
        fake_execute,
        raising=False,
    )
    client = TestClient(create_app())

    response = client.post(
        "/api/radar/priced-in/source-batches/execute-next",
        json={
            "source": "local_text",
            "available_at": "2026-05-18T16:00:00+00:00",
            "status": "all",
            "usefulness": "research_useful",
            "decision_gap": ["candidate_packet"],
            "min_gap": 12,
            "stocks_only": True,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["schema_version"] == "priced-in-source-batch-execution-v1"
    assert payload["source"] == "local_text"
    assert payload["status"] == "executed"
    assert payload["external_calls_made"] == 0
    assert payload["post_execution"]["status"] == "improved"
    assert payload["post_execution"]["gap_rows_resolved"] == 1
    assert captured["source"] == "local_text"
    assert captured["available_at"].isoformat() == "2026-05-18T16:00:00+00:00"
    assert captured["status"] == "all"
    assert captured["usefulness"] == "research_useful"
    assert captured["decision_gap"] == ["candidate_packet"]
    assert captured["min_gap"] == 12.0
    assert captured["stocks_only"] is True


def test_post_radar_priced_in_source_batch_execute_next_can_run_capped_batches(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "radar-priced-in-source-execute-batches.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)
    captured: dict[str, object] = {}

    def fake_execute_batches(_engine, _config, **kwargs) -> dict[str, object]:
        captured.update(kwargs)
        return {
            "schema_version": "priced-in-source-batch-run-v1",
            "source": kwargs["source"],
            "status": "executed",
            "requested_batches": kwargs["max_batches"],
            "executed_batches": 3,
            "external_calls_made": 3,
            "before_plan": {"total_gap_rows": 10, "plannable_gap_rows": 10},
            "after_plan": {"total_gap_rows": 7, "plannable_gap_rows": 7},
            "gap_rows_resolved": 3,
            "plannable_rows_resolved": 3,
            "executions": [],
            "next_action": "Review the next batch plan before continuing.",
        }

    monkeypatch.setattr(
        radar_routes,
        "execute_priced_in_source_batches",
        fake_execute_batches,
        raising=False,
    )
    client = TestClient(create_app())

    response = client.post(
        "/api/radar/priced-in/source-batches/execute-next",
        json={
            "source": "catalyst_events",
            "available_at": "2026-05-18T16:00:00+00:00",
            "status": "all",
            "usefulness": "research_useful",
            "decision_gap": ["candidate_packet"],
            "min_gap": 12,
            "stocks_only": True,
            "max_batches": 3,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["schema_version"] == "priced-in-source-batch-run-v1"
    assert payload["source"] == "catalyst_events"
    assert payload["executed_batches"] == 3
    assert payload["gap_rows_resolved"] == 3
    assert captured["source"] == "catalyst_events"
    assert captured["max_batches"] == 3
    assert captured["available_at"].isoformat() == "2026-05-18T16:00:00+00:00"
    assert captured["status"] == "all"
    assert captured["usefulness"] == "research_useful"
    assert captured["decision_gap"] == ["candidate_packet"]
    assert captured["min_gap"] == 12.0
    assert captured["stocks_only"] is True


def test_post_radar_market_bars_template_and_import_use_database_universe(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "radar-market-bars.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = _create_database(database_url)
    _insert_active_securities(engine, ["AAA", "BBB", "ZZZ"])
    template_path = tmp_path / "api-manual-bars.csv"
    bars_path = tmp_path / "api-filled-bars.csv"
    client = TestClient(create_app())

    template_response = client.post(
        "/api/radar/market-bars/template",
        json={
            "expected_as_of": "2026-05-11",
            "output_path": str(template_path),
            "missing_only": True,
        },
    )

    assert template_response.status_code == 200
    template_payload = template_response.json()
    assert template_payload["schema_version"] == "manual-market-bars-template-v1"
    assert template_payload["status"] == "ready"
    assert template_payload["row_count"] == 3
    assert template_payload["template_scope"] == "missing_as_of_bars"
    assert template_payload["missing_only"] is True
    assert template_payload["external_calls_made"] == 0
    assert [row["ticker"] for row in _read_csv_rows(template_path)] == [
        "AAA",
        "BBB",
        "ZZZ",
    ]

    invalid_preview_response = client.post(
        "/api/radar/market-bars/import",
        json={
            "daily_bars_path": str(template_path),
            "expected_as_of": "2026-05-11",
        },
    )

    assert invalid_preview_response.status_code == 200
    invalid_preview = invalid_preview_response.json()
    assert invalid_preview["status"] == "invalid"
    assert invalid_preview["invalid_row_count"] == 3
    assert invalid_preview["blank_required_count"] > 0
    assert invalid_preview["blank_required_field_counts"]["open"] == 3
    assert invalid_preview["blank_required_field_counts"]["vwap"] == 3
    assert invalid_preview["fill_progress"] == {
        "complete_rows": 0,
        "partial_rows": 0,
        "empty_rows": 3,
        "filled_rows": 0,
    }
    assert invalid_preview["external_calls_made"] == 0
    assert invalid_preview["executed"] is False

    _write_manual_bar_csv(template_path, ["AAA"], as_of="2026-05-11")
    guarded_template_response = client.post(
        "/api/radar/market-bars/template",
        json={
            "expected_as_of": "2026-05-11",
            "output_path": str(template_path),
            "missing_only": True,
        },
    )
    assert guarded_template_response.status_code == 422
    assert "refusing to overwrite manual market-bar template" in str(
        guarded_template_response.json()["detail"]
    )

    overwrite_template_response = client.post(
        "/api/radar/market-bars/template",
        json={
            "expected_as_of": "2026-05-11",
            "output_path": str(template_path),
            "missing_only": True,
            "overwrite": True,
        },
    )
    assert overwrite_template_response.status_code == 200
    assert overwrite_template_response.json()["status"] == "ready"

    _write_manual_bar_csv(bars_path, ["AAA", "BBB", "ZZZ"], as_of="2026-05-11")
    preview_response = client.post(
        "/api/radar/market-bars/import",
        json={
            "daily_bars_path": str(bars_path),
            "expected_as_of": "2026-05-11",
        },
    )

    assert preview_response.status_code == 200
    preview_payload = preview_response.json()
    assert preview_payload["status"] == "ready"
    assert preview_payload["fill_progress"] == {
        "complete_rows": 3,
        "partial_rows": 0,
        "empty_rows": 0,
        "filled_rows": 3,
    }
    assert preview_payload["executed"] is False
    assert preview_payload["external_calls_made"] == 0

    execute_response = client.post(
        "/api/radar/market-bars/import",
        json={
            "daily_bars_path": str(bars_path),
            "expected_as_of": "2026-05-11",
            "execute": True,
        },
    )

    assert execute_response.status_code == 200
    execute_payload = execute_response.json()
    assert execute_payload["status"] == "imported"
    assert execute_payload["executed"] is True
    bars = MarketRepository(engine).daily_bars(
        "BBB",
        end=date(2026, 5, 11),
        lookback=1,
    )
    assert len(bars) == 1
    assert bars[0].date == date(2026, 5, 11)


def test_post_radar_market_bars_import_complete_rows_only_is_incremental(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "radar-market-bars-incremental.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = _create_database(database_url)
    _insert_active_securities(engine, ["AAA", "BBB", "ZZZ"])
    bars_path = tmp_path / "api-incremental-bars.csv"
    _write_mixed_manual_bar_csv(
        bars_path,
        complete_tickers=["AAA"],
        empty_tickers=["BBB"],
        partial_tickers=[],
        as_of="2026-05-11",
    )
    client = TestClient(create_app())

    preview_response = client.post(
        "/api/radar/market-bars/import",
        json={
            "daily_bars_path": str(bars_path),
            "expected_as_of": "2026-05-11",
            "complete_rows_only": True,
        },
    )

    assert preview_response.status_code == 200
    preview_payload = preview_response.json()
    assert preview_payload["status"] == "ready_partial"
    assert preview_payload["complete_rows_only"] is True
    assert preview_payload["coverage_after_import_count"] == 1
    assert preview_payload["missing_expected_count"] == 2
    assert preview_payload["fill_progress"] == {
        "complete_rows": 1,
        "partial_rows": 0,
        "empty_rows": 1,
        "filled_rows": 1,
    }
    assert preview_payload["executed"] is False
    assert preview_payload["external_calls_made"] == 0

    execute_response = client.post(
        "/api/radar/market-bars/import",
        json={
            "daily_bars_path": str(bars_path),
            "expected_as_of": "2026-05-11",
            "complete_rows_only": True,
            "execute": True,
        },
    )

    assert execute_response.status_code == 200
    execute_payload = execute_response.json()
    assert execute_payload["status"] == "partial_imported"
    assert execute_payload["executed"] is True
    with engine.connect() as conn:
        imported = {
            str(row._mapping["ticker"])
            for row in conn.execute(
                select(daily_bars.c.ticker).where(daily_bars.c.date == date(2026, 5, 11))
            )
        }
    assert "AAA" in imported
    assert "BBB" not in imported


def test_post_radar_market_bars_template_and_import_can_scope_to_stocks(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "radar-stock-market-bars.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("CATALYST_POLYGON_API_KEY", "fixture-key")
    engine = _create_database(database_url)
    MarketRepository(engine).upsert_securities(
        [
            Security(
                ticker="AADR",
                name="Alpha ADR",
                exchange="NYSE",
                sector="Financials",
                industry="Banks",
                market_cap=1_000_000_000,
                avg_dollar_volume_20d=20_000_000,
                has_options=True,
                is_active=True,
                updated_at=AVAILABLE_AT,
                metadata={"type": "ADRC"},
            ),
            Security(
                ticker="BSTK",
                name="Beta Stock",
                exchange="NASDAQ",
                sector="Technology",
                industry="Software",
                market_cap=1_000_000_000,
                avg_dollar_volume_20d=20_000_000,
                has_options=True,
                is_active=True,
                updated_at=AVAILABLE_AT,
                metadata={"type": "CS"},
            ),
            Security(
                ticker="EETF",
                name="Example ETF",
                exchange="NYSE",
                sector="ETF",
                industry="ETF",
                market_cap=1_000_000_000,
                avg_dollar_volume_20d=20_000_000,
                has_options=True,
                is_active=True,
                updated_at=AVAILABLE_AT,
                metadata={"type": "ETF"},
            ),
        ]
    )
    MarketRepository(engine).upsert_daily_bars(
        [_daily_bar("BSTK", date(2026, 5, 11))]
    )
    template_path = tmp_path / "api-stock-bars.csv"
    bars_path = tmp_path / "api-filled-stock-bars.csv"
    client = TestClient(create_app())

    template_response = client.post(
        "/api/radar/market-bars/template",
        json={
            "expected_as_of": "2026-05-11",
            "output_path": str(template_path),
            "missing_only": True,
            "stocks_only": True,
        },
    )

    assert template_response.status_code == 200
    template_payload = template_response.json()
    assert template_payload["status"] == "ready"
    assert template_payload["row_count"] == 1
    assert template_payload["stocks_only"] is True
    assert template_payload["template_scope"] == "stock_like_missing_as_of_bars"
    assert "name" in template_payload["template_columns"]
    assert template_payload["active_security_count"] == 2
    assert template_payload["existing_as_of_bar_count"] == 1
    assert template_payload["missing_as_of_bar_count"] == 1
    assert [row["ticker"] for row in _read_csv_rows(template_path)] == ["AADR"]

    _write_manual_bar_csv(bars_path, ["AADR"], as_of="2026-05-11")
    preview_response = client.post(
        "/api/radar/market-bars/import",
        json={
            "daily_bars_path": str(bars_path),
            "expected_as_of": "2026-05-11",
            "stocks_only": True,
        },
    )

    assert preview_response.status_code == 200
    preview_payload = preview_response.json()
    assert preview_payload["status"] == "ready"
    assert preview_payload["stocks_only"] is True
    assert preview_payload["coverage_scope"] == "stock_like"
    assert preview_payload["coverage_after_import_count"] == 2
    assert preview_payload["missing_expected_count"] == 0
    assert preview_payload["external_calls_made"] == 0

    repair_response = client.post(
        "/api/radar/market-bars/repair-plan",
        json={
            "expected_as_of": "2026-05-11",
            "stocks_only": True,
        },
    )

    assert repair_response.status_code == 200
    repair_payload = repair_response.json()
    assert repair_payload["schema_version"] == "manual-market-bars-repair-plan-v1"
    assert repair_payload["status"] == "attention"
    assert repair_payload["coverage_scope"] == "stock_like"
    assert repair_payload["active_security_count"] == 2
    assert repair_payload["existing_as_of_bar_count"] == 1
    assert repair_payload["missing_as_of_bar_count"] == 1
    assert repair_payload["missing_security_type_counts"] == {"ADRC": 1}
    assert repair_payload["manual_template_command"].endswith(
        "--missing-only --stocks-only"
    )
    assert repair_payload["required_fill_fields"] == [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "vwap",
    ]
    assert repair_payload["blank_required_field_counts_if_new_template"] == {
        "open": 1,
        "high": 1,
        "low": 1,
        "close": 1,
        "volume": 1,
        "vwap": 1,
    }
    assert repair_payload["template_row_count"] == 1
    assert repair_payload["provider_fill_status"] == "ready_for_approval"
    assert repair_payload["provider_fill_external_call_count"] == 1
    assert repair_payload["provider_key_configured"] is True
    assert repair_payload["external_calls_made"] == 0


def test_post_radar_market_bars_provider_fixture_preview_is_zero_write(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "radar-provider-fixture-preview.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = _create_database(database_url)
    MarketRepository(engine).upsert_securities(
        [
            _security_with_type("AAPL", "CS"),
            _security_with_type("MSFT", "CS"),
            _security_with_type("GOOG", "CS"),
        ]
    )
    client = TestClient(create_app())

    response = client.post(
        "/api/radar/market-bars/provider-fixture-preview",
        json={
            "expected_as_of": "2026-05-08",
            "fixture_path": "tests/fixtures/polygon/grouped_daily_2026-05-08.json",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["schema_version"] == "polygon-grouped-daily-fixture-preview-v1"
    assert payload["status"] == "ready_with_rejections"
    assert payload["raw_count"] == 6
    assert payload["normalized_count"] == 6
    assert payload["daily_bar_count"] == 6
    assert payload["rejected_count"] == 1
    assert payload["external_calls_made"] == 0
    assert payload["db_writes_made"] == 0
    assert payload["coverage"]["active_security_count"] == 3
    assert payload["coverage"]["fixture_active_match_count"] == 2
    assert payload["coverage"]["missing_covered_by_fixture_count"] == 2
    assert payload["coverage"]["missing_after_import_count"] == 1
    assert payload["coverage"]["stock_like_covered_by_fixture_count"] == 2
    assert payload["coverage"]["stock_like_missing_after_import_count"] == 1

    with engine.connect() as conn:
        assert conn.execute(select(func.count()).select_from(job_runs)).scalar_one() == 0
        assert (
            conn.execute(select(func.count()).select_from(raw_provider_records)).scalar_one()
            == 0
        )
        assert (
            conn.execute(
                select(func.count()).select_from(normalized_provider_records)
            ).scalar_one()
            == 0
        )
        assert conn.execute(select(func.count()).select_from(daily_bars)).scalar_one() == 0
        assert (
            conn.execute(select(func.count()).select_from(data_quality_incidents)).scalar_one()
            == 0
        )


def test_post_radar_market_bars_provider_fixture_preview_rejects_missing_file(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "radar-provider-fixture-missing.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)
    client = TestClient(create_app())

    response = client.post(
        "/api/radar/market-bars/provider-fixture-preview",
        json={
            "expected_as_of": "2026-05-08",
            "fixture_path": str(tmp_path / "missing-grouped-daily.json"),
        },
    )

    assert response.status_code == 422


def test_post_radar_market_bars_provider_fixture_capture_requires_approval(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "radar-provider-fixture-capture-plan.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)
    output_path = tmp_path / "polygon-grouped-daily-2026-05-08.json"
    client = TestClient(create_app())

    response = client.post(
        "/api/radar/market-bars/provider-fixture-capture",
        json={
            "expected_as_of": "2026-05-08",
            "output_path": str(output_path),
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["schema_version"] == "polygon-grouped-daily-response-capture-v1"
    assert payload["status"] == "approval_required"
    assert payload["capture_external_call_count"] == 1
    assert payload["external_calls_made"] == 0
    assert payload["db_writes_made"] == 0
    assert "--confirm-external-call" in payload["capture_command"]
    assert not output_path.exists()


def test_post_radar_market_bars_provider_fixture_capture_uses_fixture_without_db_writes(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "radar-provider-fixture-capture.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = _create_database(database_url)
    output_path = tmp_path / "polygon-grouped-daily-2026-05-08.json"
    client = TestClient(create_app())

    response = client.post(
        "/api/radar/market-bars/provider-fixture-capture",
        json={
            "expected_as_of": "2026-05-08",
            "output_path": str(output_path),
            "fixture_path": "tests/fixtures/polygon/grouped_daily_2026-05-08.json",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["schema_version"] == "polygon-grouped-daily-response-capture-v1"
    assert payload["status"] == "ready"
    assert payload["source"] == "fixture"
    assert payload["external_calls_made"] == 0
    assert payload["db_writes_made"] == 0
    assert output_path.read_bytes() == Path(
        "tests/fixtures/polygon/grouped_daily_2026-05-08.json"
    ).read_bytes()

    with engine.connect() as conn:
        assert conn.execute(select(func.count()).select_from(job_runs)).scalar_one() == 0
        assert (
            conn.execute(select(func.count()).select_from(raw_provider_records)).scalar_one()
            == 0
        )
        assert conn.execute(select(func.count()).select_from(daily_bars)).scalar_one() == 0


def test_post_radar_market_bars_provider_fixture_import_previews_without_writes(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "radar-provider-fixture-import-preview.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = _create_database(database_url)
    MarketRepository(engine).upsert_securities(
        [
            _security_with_type("AAPL", "CS"),
            _security_with_type("MSFT", "CS"),
            _security_with_type("GOOG", "CS"),
        ]
    )
    client = TestClient(create_app())

    response = client.post(
        "/api/radar/market-bars/provider-fixture-import",
        json={
            "expected_as_of": "2026-05-08",
            "fixture_path": "tests/fixtures/polygon/grouped_daily_2026-05-08.json",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["schema_version"] == "polygon-grouped-daily-fixture-import-v1"
    assert payload["status"] == "ready_with_rejections"
    assert payload["executed"] is False
    assert payload["external_calls_made"] == 0
    assert payload["db_writes_made"] == 0
    assert "execute=true" in payload["write_boundary"]

    with engine.connect() as conn:
        assert conn.execute(select(func.count()).select_from(job_runs)).scalar_one() == 0
        assert (
            conn.execute(select(func.count()).select_from(raw_provider_records)).scalar_one()
            == 0
        )
        assert (
            conn.execute(
                select(func.count()).select_from(normalized_provider_records)
            ).scalar_one()
            == 0
        )
        assert conn.execute(select(func.count()).select_from(daily_bars)).scalar_one() == 0
        assert (
            conn.execute(select(func.count()).select_from(data_quality_incidents)).scalar_one()
            == 0
        )


def test_post_radar_market_bars_provider_fixture_import_executes_saved_fixture(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "radar-provider-fixture-import.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = _create_database(database_url)
    MarketRepository(engine).upsert_securities(
        [
            _security_with_type("AAPL", "CS"),
            _security_with_type("MSFT", "CS"),
            _security_with_type("GOOG", "CS"),
        ]
    )
    client = TestClient(create_app())

    response = client.post(
        "/api/radar/market-bars/provider-fixture-import",
        json={
            "expected_as_of": "2026-05-08",
            "fixture_path": "tests/fixtures/polygon/grouped_daily_2026-05-08.json",
            "execute": True,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["schema_version"] == "polygon-grouped-daily-fixture-import-v1"
    assert payload["status"] == "imported_with_rejections"
    assert payload["executed"] is True
    assert payload["requested_count"] == 7
    assert payload["raw_count"] == 6
    assert payload["normalized_count"] == 6
    assert payload["daily_bar_count"] == 6
    assert payload["rejected_count"] == 1
    assert payload["external_calls_made"] == 0
    assert payload["db_writes_made"] == 1
    assert payload["preview"]["schema_version"] == (
        "polygon-grouped-daily-fixture-preview-v1"
    )
    assert "0 provider calls" in payload["write_boundary"]

    with engine.connect() as conn:
        assert conn.execute(select(func.count()).select_from(job_runs)).scalar_one() == 1
        assert (
            conn.execute(select(func.count()).select_from(raw_provider_records)).scalar_one()
            == 6
        )
        assert (
            conn.execute(
                select(func.count()).select_from(normalized_provider_records)
            ).scalar_one()
            == 6
        )
        assert conn.execute(select(func.count()).select_from(daily_bars)).scalar_one() == 6
        assert (
            conn.execute(select(func.count()).select_from(data_quality_incidents)).scalar_one()
            == 1
        )
        assert (
            conn.execute(
                select(func.count()).where(
                    daily_bars.c.ticker == "AAPL",
                    daily_bars.c.date == date(2026, 5, 8),
                )
            ).scalar_one()
            == 1
        )


def test_post_radar_market_bars_provider_fixture_import_rejects_missing_file(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "radar-provider-fixture-import-missing.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)
    client = TestClient(create_app())

    response = client.post(
        "/api/radar/market-bars/provider-fixture-import",
        json={
            "expected_as_of": "2026-05-08",
            "fixture_path": str(tmp_path / "missing-grouped-daily.json"),
            "execute": True,
        },
    )

    assert response.status_code == 422


def test_post_radar_sec_submissions_batch_calls_capped_sec_executor(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "radar-sec-submissions-batch.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("CATALYST_SEC_DAILY_MAX_TICKERS", "2")
    _create_database(database_url)
    captured: dict[str, object] = {}

    class FakeSecBatchResult:
        def as_payload(self) -> dict[str, object]:
            return {
                "schema_version": "sec-submissions-batch-result-v1",
                "provider": "sec",
                "endpoint": "submissions-batch",
                "live": True,
                "target_count": 1,
                "targets": [{"ticker": "MSFT", "cik": "0000000789"}],
                "external_calls_made": 1,
                "raw_count": 1,
                "normalized_count": 1,
                "security_count": 0,
                "daily_bar_count": 0,
                "holding_count": 0,
                "event_count": 1,
                "rejected_count": 0,
                "job_ids": ["job-1"],
            }

    def fake_ingest_sec_submissions_batch(**kwargs) -> FakeSecBatchResult:
        captured.update(kwargs)
        return FakeSecBatchResult()

    monkeypatch.setattr(
        radar_routes,
        "ingest_sec_submissions_batch",
        fake_ingest_sec_submissions_batch,
    )
    client = TestClient(create_app())

    response = client.post(
        "/api/radar/sec/submissions-batch",
        json={"targets": [{"ticker": "msft", "cik": "789"}]},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["schema_version"] == "sec-submissions-batch-result-v1"
    assert payload["provider"] == "sec"
    assert payload["external_calls_made"] == 1
    targets = captured["targets"]
    assert len(targets) == 1
    assert targets[0].ticker == "MSFT"
    assert targets[0].cik == "0000000789"


def test_post_radar_sec_company_tickers_refreshes_cik_metadata(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "radar-sec-company-tickers.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)
    called: dict[str, object] = {}

    class FakeCikRefreshResult:
        def as_payload(self) -> dict[str, object]:
            return {
                "schema_version": "sec-cik-metadata-refresh-v1",
                "provider": "sec",
                "endpoint": "company-tickers",
                "live": True,
                "external_calls_made": 1,
                "active_security_count": 2,
                "missing_before_count": 1,
                "matched_missing_count": 1,
                "updated_count": 1,
                "missing_after_count": 0,
                "updated_tickers": ["AAPL"],
                "unmatched_tickers": [],
                "next_action": "Recheck catalyst_events source batches.",
            }

    def fake_refresh_sec_cik_metadata(*args, **kwargs) -> FakeCikRefreshResult:
        called["args"] = args
        called["kwargs"] = kwargs
        return FakeCikRefreshResult()

    monkeypatch.setattr(
        radar_routes,
        "refresh_sec_cik_metadata",
        fake_refresh_sec_cik_metadata,
    )
    client = TestClient(create_app())

    response = client.post("/api/radar/sec/company-tickers")

    assert response.status_code == 200
    payload = response.json()
    assert payload["schema_version"] == "sec-cik-metadata-refresh-v1"
    assert payload["updated_count"] == 1
    assert payload["external_calls_made"] == 1
    assert called["args"]
    assert called["kwargs"] == {}


def test_get_radar_sec_cik_overrides_template_returns_zero_call_rows(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "radar-sec-cik-template.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)
    captured: dict[str, object] = {}

    def fake_template_payload(_engine, _config, *, stocks_only):
        captured["stocks_only"] = stocks_only
        return {
            "schema_version": "sec-cik-override-template-v1",
            "status": "ready",
            "provider": "manual",
            "live": False,
            "external_calls_made": 0,
            "source": "catalyst_events",
            "stocks_only": stocks_only,
            "row_count": 1,
            "rows": [
                {
                    "ticker": "FRBA",
                    "cik": "",
                    "sec_company_name": "",
                    "security_type": "CS",
                    "template_reason": (
                        "missing_sec_cik_for_catalyst_events_source_gap"
                    ),
                }
            ],
        }

    monkeypatch.setattr(
        dashboard_data,
        "sec_cik_override_template_payload",
        fake_template_payload,
        raising=False,
    )
    client = TestClient(create_app())

    response = client.get("/api/radar/sec/cik-overrides-template?stocks_only=true")

    assert response.status_code == 200
    payload = response.json()
    assert payload["schema_version"] == "sec-cik-override-template-v1"
    assert payload["external_calls_made"] == 0
    assert payload["stocks_only"] is True
    assert payload["rows"][0]["ticker"] == "FRBA"
    assert captured["stocks_only"] is True


def test_get_radar_options_fixture_template_returns_zero_call_fixture(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "radar-options-template.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)
    captured: dict[str, object] = {}

    def fake_template_payload(_engine, _config, *, stocks_only):
        captured["stocks_only"] = stocks_only
        return {
            "schema_version": "options-fixture-template-v1",
            "status": "ready",
            "provider": "manual",
            "live": False,
            "external_calls_made": 0,
            "source": "options",
            "stocks_only": stocks_only,
            "row_count": 1,
            "fixture": {
                "as_of": "2026-05-10T21:00:00+00:00",
                "source_ts": "2026-05-10T21:00:00+00:00",
                "available_at": "2026-05-10T21:00:00+00:00",
                "provider": "options_fixture",
                "results": [{"ticker": "MSFT", "call_volume": ""}],
            },
        }

    monkeypatch.setattr(
        dashboard_data,
        "options_fixture_template_payload",
        fake_template_payload,
        raising=False,
    )
    client = TestClient(create_app())

    response = client.get("/api/radar/options/fixture-template?stocks_only=true")

    assert response.status_code == 200
    payload = response.json()
    assert payload["schema_version"] == "options-fixture-template-v1"
    assert payload["external_calls_made"] == 0
    assert payload["stocks_only"] is True
    assert payload["fixture"]["results"][0]["ticker"] == "MSFT"
    assert captured["stocks_only"] is True


def test_post_radar_options_fixture_validate_returns_zero_call_result(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "radar-options-validate.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)
    fixture = tmp_path / "point-in-time-options.json"
    fixture.write_text(
        json.dumps(
            {
                "as_of": "2026-05-10T21:00:00+00:00",
                "source_ts": "2026-05-10T21:00:00+00:00",
                "available_at": "2026-05-10T21:00:00+00:00",
                "provider": "options_fixture",
                "results": [
                    {
                        "ticker": "MSFT",
                        "call_volume": 100,
                        "put_volume": 50,
                        "call_open_interest": 1000,
                        "put_open_interest": 700,
                        "iv_percentile": 0.55,
                        "skew": 0.1,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    client = TestClient(create_app())

    response = client.post(
        "/api/radar/options/fixture-validate",
        json={"fixture_path": str(fixture), "expected_as_of": "2026-05-10"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["schema_version"] == "options-fixture-validation-v1"
    assert payload["status"] == "ready"
    assert payload["row_count"] == 1
    assert payload["external_calls_made"] == 0
    assert payload["import_command"].endswith(str(fixture))


def test_post_radar_sec_cik_overrides_imports_manual_metadata(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "radar-sec-cik-overrides.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = _create_database(database_url)
    _insert_active_securities(engine, ["AAPL", "MSFT"])
    client = TestClient(create_app())

    response = client.post(
        "/api/radar/sec/cik-overrides",
        json={
            "overrides": [
                {"ticker": "AAPL", "cik": "320193", "sec_company_name": "Apple Inc."},
                {"ticker": "MSFT", "cik": "789019"},
                {"ticker": "MISS", "cik": "123456"},
            ]
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["schema_version"] == "sec-cik-override-import-v1"
    assert payload["external_calls_made"] == 0
    assert payload["requested_count"] == 3
    assert payload["updated_count"] == 2
    assert payload["unmatched_count"] == 1
    assert payload["updated_tickers"] == ["AAPL", "MSFT"]
    assert payload["unmatched_tickers"] == ["MISS"]

    with engine.connect() as conn:
        rows = {
            str(row.ticker): dict(row._mapping["metadata"] or {})
            for row in conn.execute(select(securities.c.ticker, securities.c.metadata))
        }
    assert rows["AAPL"]["cik"] == "0000320193"
    assert rows["AAPL"]["sec_company_name"] == "Apple Inc."
    assert rows["AAPL"]["cik_source"] == "manual_cik_override"
    assert rows["MSFT"]["cik"] == "0000789019"


def test_post_radar_sec_cik_overrides_validate_returns_zero_call_plan(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "radar-sec-cik-validate.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = _create_database(database_url)
    _insert_active_securities(engine, ["AAPL", "MSFT"])
    client = TestClient(create_app())

    response = client.post(
        "/api/radar/sec/cik-overrides/validate",
        json={
            "overrides": [
                {"ticker": "AAPL", "cik": "320193", "sec_company_name": "Apple Inc."},
                {"ticker": "MSFT", "cik": "789019"},
                {"ticker": "MISS", "cik": "123456"},
            ]
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["schema_version"] == "sec-cik-override-validation-v1"
    assert payload["status"] == "attention"
    assert payload["external_calls_made"] == 0
    assert payload["requested_count"] == 3
    assert payload["valid_count"] == 3
    assert payload["update_candidate_count"] == 2
    assert payload["unmatched_count"] == 1
    assert payload["update_candidate_tickers"] == ["AAPL", "MSFT"]
    assert payload["unmatched_tickers"] == ["MISS"]

    with engine.connect() as conn:
        rows = {
            str(row.ticker): dict(row._mapping["metadata"] or {})
            for row in conn.execute(select(securities.c.ticker, securities.c.metadata))
        }
    assert "cik" not in rows["AAPL"]
    assert "cik" not in rows["MSFT"]


def test_post_radar_sec_submissions_batch_rejects_too_many_targets(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "radar-sec-submissions-too-many.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("CATALYST_SEC_DAILY_MAX_TICKERS", "1")
    _create_database(database_url)
    called = False

    def fake_ingest_sec_submissions_batch(**_kwargs):
        nonlocal called
        called = True
        raise AssertionError("ingest should not be called")

    monkeypatch.setattr(
        radar_routes,
        "ingest_sec_submissions_batch",
        fake_ingest_sec_submissions_batch,
    )
    client = TestClient(create_app())

    response = client.post(
        "/api/radar/sec/submissions-batch",
        json={
            "targets": [
                {"ticker": "MSFT", "cik": "0000789019"},
                {"ticker": "AAPL", "cik": "0000320193"},
            ]
        },
    )

    assert response.status_code == 400
    assert "maximum is 1" in response.json()["detail"]
    assert called is False


def test_post_radar_sec_submissions_batch_rejects_empty_targets(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "radar-sec-submissions-empty.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)
    client = TestClient(create_app())

    response = client.post("/api/radar/sec/submissions-batch", json={"targets": []})

    assert response.status_code == 400
    assert "At least one SEC target" in response.json()["detail"]


def test_post_radar_sec_submissions_batch_rejects_blank_target_fields(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "radar-sec-submissions-blank.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)
    client = TestClient(create_app())

    response = client.post(
        "/api/radar/sec/submissions-batch",
        json={"targets": [{"ticker": "MSFT", "cik": "  "}]},
    )

    assert response.status_code == 422
    assert "ticker and CIK" in response.json()["detail"]


def test_post_radar_text_features_batch_runs_local_text_pipeline(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "radar-text-features-batch.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)
    captured: dict[str, object] = {}

    class FakeTextPipelineResult:
        feature_count = 2
        snippet_count = 4

    def fake_run_text_pipeline(_event_repo, _text_repo, **kwargs):
        captured.update(kwargs)
        return FakeTextPipelineResult()

    monkeypatch.setattr(radar_routes, "run_text_pipeline", fake_run_text_pipeline)
    client = TestClient(create_app())

    response = client.post(
        "/api/radar/text/features-batch",
        json={
            "as_of": "2026-05-15",
            "available_at": "2026-05-18T16:00:00+00:00",
            "tickers": ["msft", "MSFT", "aapl"],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["schema_version"] == "text-features-batch-result-v1"
    assert payload["provider"] == "local_text"
    assert payload["external_calls_made"] == 0
    assert payload["feature_count"] == 2
    assert payload["snippet_count"] == 4
    assert payload["tickers"] == ["MSFT", "AAPL"]
    assert captured["as_of"].isoformat() == "2026-05-15T21:00:00+00:00"
    assert captured["available_at"].isoformat() == "2026-05-18T16:00:00+00:00"
    assert captured["tickers"] == ("MSFT", "AAPL")


def test_post_radar_text_features_batch_rejects_empty_tickers(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "radar-text-features-empty.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)
    client = TestClient(create_app())

    response = client.post(
        "/api/radar/text/features-batch",
        json={"as_of": "2026-05-15", "tickers": []},
    )

    assert response.status_code == 400
    assert "At least one local text ticker" in response.json()["detail"]


def test_post_radar_run_call_plan_returns_read_only_call_budget(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "radar-call-plan.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)
    captured: dict[str, object] = {}

    def _fake_call_plan(_engine, _config, **kwargs):
        captured.update(kwargs)
        return {
            "schema_version": "radar-run-call-plan-v1",
            "status": "local_or_dry_run_only",
            "will_call_external_providers": False,
            "max_external_call_count": 0,
        }

    monkeypatch.setattr(
        dashboard_data,
        "radar_run_call_plan_payload",
        _fake_call_plan,
        raising=False,
    )
    monkeypatch.setattr(
        radar_routes,
        "run_once",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("run called")),
    )
    client = TestClient(create_app())

    response = client.post(
        "/api/radar/runs/call-plan",
        json={"tickers": ["msft", "nvda"], "run_llm": True, "llm_dry_run": True},
    )

    assert response.status_code == 200
    assert response.json()["max_external_call_count"] == 0
    assert captured["tickers"] == ["msft", "nvda"]
    assert captured["run_llm"] is True
    assert captured["llm_dry_run"] is True


def test_post_radar_run_call_plan_blocks_provider_override_mismatch(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "radar-call-plan-mismatch.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("CATALYST_DAILY_MARKET_PROVIDER", "csv")
    _create_database(database_url)
    client = TestClient(create_app())

    response = client.post("/api/radar/runs/call-plan", json={"provider": "polygon"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "blocked"
    assert payload["max_external_call_count"] == 0
    by_layer = {str(row["layer"]): row for row in payload["rows"]}
    assert by_layer["Scan provider"]["status"] == "blocked"
    assert by_layer["Scan provider"]["provider"] == "polygon"
    assert "scheduled market provider csv" in by_layer["Scan provider"]["detail"]


def test_post_radar_run_rejects_provider_override_mismatch_before_running(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "radar-run-mismatch.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("CATALYST_DAILY_MARKET_PROVIDER", "csv")
    _create_database(database_url)
    monkeypatch.setattr(
        radar_routes,
        "run_once",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("run called")),
    )
    client = TestClient(create_app())

    response = client.post("/api/radar/runs", json={"provider": "polygon"})

    assert response.status_code == 422
    assert "provider override polygon does not match" in response.json()["detail"]


def test_get_candidate_detail_returns_404_for_missing_ticker(tmp_path, monkeypatch) -> None:
    database_url = _database_url(tmp_path, "missing-detail.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)
    monkeypatch.setattr(
        dashboard_data,
        "load_ticker_detail",
        lambda _engine, _ticker: None,
        raising=False,
    )

    client = TestClient(create_app())

    response = client.get("/api/radar/candidates/MSFT")

    assert response.status_code == 404
    assert response.json() == {"detail": "candidate not found"}


def test_get_candidate_detail_returns_payload(tmp_path, monkeypatch) -> None:
    database_url = _database_url(tmp_path, "detail.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)

    def load_ticker_detail(_engine, ticker: str) -> dict[str, object]:
        return {
            "ticker": ticker,
            "latest_candidate": {"ticker": ticker, "state": ActionState.WARNING.value},
            "manual_review_only": True,
        }

    monkeypatch.setattr(
        dashboard_data,
        "load_ticker_detail",
        load_ticker_detail,
        raising=False,
    )
    client = TestClient(create_app())

    response = client.get("/api/radar/candidates/MSFT")

    assert response.status_code == 200
    assert response.json() == {
        "ticker": "MSFT",
        "latest_candidate": {"ticker": "MSFT", "state": ActionState.WARNING.value},
        "manual_review_only": True,
    }


def test_get_candidate_detail_uses_latest_run_cutoff(tmp_path, monkeypatch) -> None:
    database_url = _database_url(tmp_path, "detail-latest-run.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)
    latest_run = {
        "as_of": "2026-05-01",
        "decision_available_at": AVAILABLE_AT.isoformat(),
        "finished_at": "2026-05-01T21:06:00+00:00",
    }
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        dashboard_data,
        "load_radar_run_summary",
        lambda _engine: latest_run,
        raising=False,
    )

    def load_ticker_detail(
        _engine,
        ticker: str,
        *,
        available_at: datetime | None = None,
    ) -> dict[str, object]:
        captured["ticker"] = ticker
        captured["available_at"] = available_at
        return {"ticker": ticker, "cutoff": available_at.isoformat() if available_at else None}

    monkeypatch.setattr(
        dashboard_data,
        "load_ticker_detail",
        load_ticker_detail,
        raising=False,
    )
    client = TestClient(create_app())

    response = client.get("/api/radar/candidates/msft")

    assert response.status_code == 200
    assert captured["ticker"] == "MSFT"
    assert captured["available_at"] == datetime(2026, 5, 1, 21, 6, tzinfo=UTC)
    assert response.json() == {
        "ticker": "MSFT",
        "cutoff": "2026-05-01T21:06:00+00:00",
    }


def test_get_candidate_detail_redacts_external_export_blocked_payload(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "detail-export-block.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)

    def load_ticker_detail(_engine, ticker: str) -> dict[str, object]:
        return {
            "ticker": ticker,
            "candidate_packet": {
                "id": "packet-MSFT",
                "payload": {
                    "supporting_evidence": [{"summary": "restricted"}],
                    "audit": {
                        "provider_license_policy": {
                            "license_tags": ["local-csv-fixture"],
                            "metadata_complete": True,
                            "prompt_allowed": True,
                            "external_export_allowed": False,
                            "attribution_required": False,
                            "policies": [],
                        }
                    },
                },
            },
        }

    monkeypatch.setattr(
        dashboard_data,
        "load_ticker_detail",
        load_ticker_detail,
        raising=False,
    )
    client = TestClient(create_app())

    response = client.get("/api/radar/candidates/MSFT")

    assert response.status_code == 200
    assert response.json()["candidate_packet"]["payload"] == {
        "external_export_blocked": True,
        "license_tags": ["local-csv-fixture"],
        "attribution_required": False,
    }


def test_get_candidates_redacts_restricted_research_brief(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "candidate-brief-export-block.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)

    def load_candidate_rows(_engine) -> list[dict[str, object]]:
        return [
            {
                "ticker": "MSFT",
                "research_brief": {
                    "why_now": "restricted catalyst",
                    "audit": {
                        "provider_license_policy": {
                            "license_tags": ["local-csv-fixture"],
                            "metadata_complete": True,
                            "prompt_allowed": True,
                            "external_export_allowed": False,
                            "attribution_required": False,
                            "policies": [],
                        }
                    },
                },
            }
        ]

    monkeypatch.setattr(
        dashboard_data,
        "load_candidate_rows",
        load_candidate_rows,
        raising=False,
    )
    client = TestClient(create_app())

    response = client.get("/api/radar/candidates")

    assert response.status_code == 200
    assert response.json()["items"][0]["research_brief"] == {
        "external_export_blocked": True,
        "license_tags": ["local-csv-fixture"],
        "attribution_required": False,
    }


def test_get_ops_health(tmp_path, monkeypatch) -> None:
    database_url = _database_url(tmp_path, "ops.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)
    monkeypatch.setattr(
        dashboard_data,
        "load_ops_health",
        lambda _engine: {
            "providers": [{"provider": "csv", "status": "ok"}],
            "jobs": [],
            "database": {"status": "ok"},
            "stale_data": [],
            "provider_banners": [],
            "degraded_mode": {
                "enabled": False,
                "max_action_state": ActionState.ADD_TO_WATCHLIST.value,
                "disabled_states": [],
                "reasons": [],
            },
            "metrics": {
                "cost": {"total_actual_cost_usd": 0.0, "cost_per_useful_alert": 0.0},
                "stale_incident_count": 0,
                "unsupported_claim_rate": 0.0,
                "false_positive_rate": None,
            },
            "score_drift": {"detected": False, "latest": None, "previous": None},
            "runbooks": {},
        },
        raising=False,
    )
    client = TestClient(create_app())

    response = client.get("/api/ops/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["database"] == {"status": "ok"}
    assert payload["providers"] == [{"provider": "csv", "status": "ok"}]
    assert "degraded_mode" in payload
    assert "metrics" in payload
    assert "score_drift" in payload
    assert "runbooks" in payload


def test_get_ops_telemetry_returns_summarized_tape(tmp_path, monkeypatch) -> None:
    database_url = _database_url(tmp_path, "ops-telemetry.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)
    monkeypatch.setattr(
        dashboard_data,
        "load_ops_health",
        lambda _engine: {
            "telemetry": {
                "event_count": 1,
                "latest_event_at": AVAILABLE_AT.isoformat(),
                "status_counts": {"success": 1},
                "events": [
                    {
                        "event_type": "telemetry.radar_run.completed",
                        "status": "success",
                        "artifact_type": "radar_run",
                        "artifact_id": "radar-run-api:abc123",
                        "occurred_at": AVAILABLE_AT.isoformat(),
                        "metadata": {
                            "daily_status": "success",
                            "step_counts": {"success": 8, "skipped": 3},
                            "outcome_category_counts": {
                                "completed": 8,
                                "expected_gate": 3,
                            },
                            "blocked_steps": [],
                            "expected_gate_steps": [
                                {"step": "decision_cards"},
                                {"step": "digest"},
                                {"step": "validation_update"},
                            ],
                        },
                    }
                ],
            }
        },
        raising=False,
    )
    client = TestClient(create_app())

    response = client.get("/api/ops/telemetry?limit=1")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ready"
    assert payload["attention_count"] == 0
    assert payload["guarded_count"] == 0
    assert payload["headline"] == "Latest telemetry event is healthy."
    assert payload["event_count"] == 1
    assert payload["status_counts"] == {"success": 1}
    assert payload["events"][0]["event"] == "radar_run.completed"
    assert payload["events"][0]["summary"] == (
        "daily_status=success; required=8/8; blocked=0; "
        "expected_gates=3; audit_raw_skips=3"
    )


def test_get_ops_telemetry_coverage_returns_zero_call_readiness(
    tmp_path, monkeypatch
) -> None:
    database_url = _database_url(tmp_path, "ops-telemetry-coverage.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)
    captured: dict[str, object] = {}

    def coverage_payload(engine) -> dict[str, object]:
        captured["engine"] = engine
        return {
            "schema_version": "ops-telemetry-coverage-v1",
            "external_calls_made": 0,
            "status": "attention",
            "missing_required_count": 1,
            "domains": [
                {
                    "domain": "Radar run step telemetry",
                    "status": "attention",
                    "required": True,
                }
            ],
        }

    monkeypatch.setattr(
        dashboard_data,
        "telemetry_coverage_payload",
        coverage_payload,
        raising=False,
    )
    client = TestClient(create_app())

    response = client.get("/api/ops/telemetry/coverage")

    assert response.status_code == 200
    payload = response.json()
    assert payload["schema_version"] == "ops-telemetry-coverage-v1"
    assert payload["external_calls_made"] == 0
    assert payload["status"] == "attention"
    assert payload["missing_required_count"] == 1
    assert payload["domains"][0]["domain"] == "Radar run step telemetry"
    assert captured["engine"] is not None


def test_get_ops_raw_telemetry_exports_redacted_audit_evidence(
    tmp_path, monkeypatch
) -> None:
    database_url = _database_url(tmp_path, "ops-telemetry-raw.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = _create_database(database_url)
    repo = AuditLogRepository(engine)
    repo.append_event(
        event_type="telemetry.radar_run.completed",
        actor_source="api",
        actor_id="tester",
        actor_role="analyst",
        artifact_type="radar_run",
        artifact_id="run-1",
        ticker="msft",
        status="success",
        reason="done",
        metadata={
            "source_url": "https://api.polygon.io/v2/aggs?apiKey=secret-key",
            "access_token": "plain-token",
        },
        before_payload={"authorization": "Bearer secret-token"},
        after_payload={"completed": True},
        occurred_at=AVAILABLE_AT,
    )
    repo.append_event(
        event_type="telemetry.radar_run.rate_limited",
        actor_source="api",
        artifact_type="radar_run",
        artifact_id="run-2",
        ticker="NVDA",
        status="blocked",
        reason="rate_limited",
        occurred_at=datetime(2026, 5, 1, 21, 6, tzinfo=UTC),
    )
    client = TestClient(create_app())

    response = client.get("/api/ops/telemetry/raw?event_type=radar_run.completed&limit=5")

    assert response.status_code == 200
    payload = response.json()
    assert payload["schema_version"] == "ops-telemetry-raw-v1"
    assert payload["external_calls_made"] == 0
    assert payload["count"] == 1
    assert payload["filters"]["event_type"] == "telemetry.radar_run.completed"
    event = payload["events"][0]
    assert event["event_type"] == "telemetry.radar_run.completed"
    assert event["ticker"] == "MSFT"
    assert event["metadata"]["access_token"] == "<redacted>"
    assert event["metadata"]["source_url"] == (
        "https://api.polygon.io/v2/aggs?apiKey=<redacted>"
    )
    assert event["before_payload"]["authorization"] == "<redacted>"
    assert "secret-key" not in str(payload)
    assert "plain-token" not in str(payload)
    assert "secret-token" not in str(payload)


def test_get_cost_summary(tmp_path, monkeypatch) -> None:
    database_url = _database_url(tmp_path, "costs.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _isolate_llm_config_env(monkeypatch)
    _create_database(database_url)
    client = TestClient(create_app())

    response = client.get("/api/costs/summary")

    assert response.status_code == 200
    payload = response.json()
    assert payload["source"] == "budget_ledger"
    assert payload["total_actual_cost_usd"] == 0.0
    assert payload["status_counts"] == {}


def _isolate_llm_config_env(monkeypatch) -> None:
    for key in (
        "CATALYST_ENABLE_PREMIUM_LLM",
        "CATALYST_LLM_DAILY_BUDGET_USD",
        "CATALYST_LLM_MONTHLY_BUDGET_USD",
        "CATALYST_LLM_TASK_DAILY_CAPS",
    ):
        monkeypatch.delenv(key, raising=False)


def test_post_feedback_records_useful_alert_label(tmp_path, monkeypatch) -> None:
    database_url = _database_url(tmp_path, "feedback.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = _create_database(database_url)
    _insert_candidate(engine)
    with engine.connect() as conn:
        candidate_before = conn.execute(select(candidate_states).limit(1)).first()
    assert candidate_before is not None

    client = TestClient(create_app())

    response = client.post(
        "/api/feedback",
        headers={"X-Catalyst-Actor": "analyst-1", "X-Catalyst-Role": "analyst"},
        json={
            "artifact_type": "decision_card",
            "artifact_id": "card-MSFT",
            "ticker": "msft",
            "label": "useful",
            "notes": "worth review apikey=note-secret",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload == {
        "id": "useful-alert-label-v1:decision_card:card-MSFT:useful",
        "artifact_type": "decision_card",
        "artifact_id": "card-MSFT",
        "ticker": "MSFT",
        "label": "useful",
    }
    with engine.connect() as conn:
        labels = list(conn.execute(select(useful_alert_labels)))
        candidate_after = conn.execute(select(candidate_states).limit(1)).first()
    assert len(labels) == 1
    stored_label = labels[0]._mapping
    assert stored_label["ticker"] == "MSFT"
    assert stored_label["label"] == "useful"
    assert stored_label["notes"] == "worth review apikey=note-secret"
    assert candidate_after is not None
    assert dict(candidate_after._mapping) == dict(candidate_before._mapping)
    events = AuditLogRepository(engine).list_events(
        artifact_type="decision_card",
        artifact_id="card-MSFT",
    )
    assert [event.event_type for event in events] == ["feedback_recorded"]
    assert events[0].actor_source == "api"
    assert events[0].actor_id == "analyst-1"
    assert events[0].actor_role == "analyst"
    assert events[0].metadata["label"] == "useful"
    assert "note-secret" not in events[0].after_payload["notes"]
    assert events[0].after_payload["notes"] == "worth review apikey=<redacted>"

    assert response.status_code == 200
    assert (
        client.post(
            "/api/feedback",
            json={
                "artifact_type": "decision_card",
                "artifact_id": "card-MSFT",
                "ticker": "MSFT",
                "label": "useful",
            },
        ).status_code
        == 200
    )
    repeated_events = AuditLogRepository(engine).list_events(
        artifact_type="decision_card",
        artifact_id="card-MSFT",
        event_type="feedback_recorded",
    )
    assert len(repeated_events) == 2
    assert repeated_events[0].id != repeated_events[1].id


def test_post_feedback_rejects_unknown_label(tmp_path, monkeypatch) -> None:
    database_url = _database_url(tmp_path, "unknown-label.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = _create_database(database_url)

    client = TestClient(create_app())

    response = client.post(
        "/api/feedback",
        json={
            "artifact_type": "decision_card",
            "artifact_id": "card-MSFT",
            "ticker": "MSFT",
            "label": "maybe",
            "notes": "worth review",
        },
    )

    assert response.status_code == 422
    with engine.connect() as conn:
        assert conn.execute(select(func.count()).select_from(useful_alert_labels)).scalar_one() == 0
        assert conn.execute(select(func.count()).select_from(user_feedback)).scalar_one() == 0
        assert conn.execute(select(func.count()).select_from(audit_events)).scalar_one() == 0


def test_post_feedback_rejects_missing_artifact(tmp_path, monkeypatch) -> None:
    database_url = _database_url(tmp_path, "missing-artifact.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = _create_database(database_url)

    client = TestClient(create_app())

    response = client.post(
        "/api/feedback",
        json={
            "artifact_type": "decision_card",
            "artifact_id": "missing-card",
            "ticker": "MSFT",
            "label": "useful",
        },
    )

    assert response.status_code == 404
    assert response.json() == {"detail": "referenced artifact not found"}
    with engine.connect() as conn:
        assert conn.execute(select(func.count()).select_from(useful_alert_labels)).scalar_one() == 0


def test_post_feedback_rejects_ticker_mismatch(tmp_path, monkeypatch) -> None:
    database_url = _database_url(tmp_path, "ticker-mismatch.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = _create_database(database_url)
    _insert_candidate(engine)

    client = TestClient(create_app())

    response = client.post(
        "/api/feedback",
        json={
            "artifact_type": "decision_card",
            "artifact_id": "card-MSFT",
            "ticker": "AAPL",
            "label": "useful",
        },
    )

    assert response.status_code == 422
    assert response.json() == {"detail": "ticker must match the referenced artifact"}
    with engine.connect() as conn:
        assert conn.execute(select(func.count()).select_from(useful_alert_labels)).scalar_one() == 0


def test_post_feedback_rejects_unknown_artifact_type(tmp_path, monkeypatch) -> None:
    database_url = _database_url(tmp_path, "unknown-artifact.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = _create_database(database_url)

    client = TestClient(create_app())

    response = client.post(
        "/api/feedback",
        json={
            "artifact_type": "order",
            "artifact_id": "card-MSFT",
            "ticker": "MSFT",
            "label": "useful",
        },
    )

    assert response.status_code == 422
    with engine.connect() as conn:
        assert conn.execute(select(func.count()).select_from(useful_alert_labels)).scalar_one() == 0


def test_post_feedback_records_alert_feedback_for_real_alert_id(tmp_path, monkeypatch) -> None:
    database_url = _database_url(tmp_path, "generic-feedback-alert.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = _create_database(database_url)
    alert = _insert_alert(engine)

    client = TestClient(create_app())

    response = client.post(
        "/api/feedback",
        json={
            "artifact_type": "alert",
            "artifact_id": alert.id,
            "ticker": "msft",
            "label": "acted",
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "id": f"useful-alert-label-v1:alert:{alert.id}:acted",
        "artifact_type": "alert",
        "artifact_id": alert.id,
        "ticker": "MSFT",
        "label": "acted",
    }
    with engine.connect() as conn:
        assert conn.execute(select(func.count()).select_from(user_feedback)).scalar_one() == 1
        assert conn.execute(select(func.count()).select_from(useful_alert_labels)).scalar_one() == 1


def test_post_feedback_rejects_missing_alert_id(tmp_path, monkeypatch) -> None:
    database_url = _database_url(tmp_path, "generic-feedback-missing-alert.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = _create_database(database_url)
    _insert_candidate(engine)

    client = TestClient(create_app())

    response = client.post(
        "/api/feedback",
        json={
            "artifact_type": "alert",
            "artifact_id": "state-msft",
            "ticker": "MSFT",
            "label": "useful",
        },
    )

    assert response.status_code == 404
    assert response.json() == {"detail": "referenced artifact not found"}
    with engine.connect() as conn:
        assert conn.execute(select(func.count()).select_from(alerts)).scalar_one() == 0
        assert conn.execute(select(func.count()).select_from(user_feedback)).scalar_one() == 0
        assert conn.execute(select(func.count()).select_from(useful_alert_labels)).scalar_one() == 0


def _database_url(tmp_path, name: str) -> str:
    return f"sqlite:///{(tmp_path / name).as_posix()}"


def _create_database(database_url: str):
    engine = engine_from_url(database_url)
    create_schema(engine)
    return engine


def _insert_active_securities(engine, tickers: list[str]) -> None:
    MarketRepository(engine).upsert_securities(
        [
            Security(
                ticker=ticker,
                name=f"{ticker} Inc.",
                exchange="NASDAQ",
                sector="Technology",
                industry="Software",
                market_cap=1_000_000_000,
                avg_dollar_volume_20d=20_000_000,
                has_options=True,
                is_active=True,
                updated_at=AVAILABLE_AT,
            )
            for ticker in tickers
        ]
    )


def _security_with_type(ticker: str, security_type: str) -> Security:
    return Security(
        ticker=ticker,
        name=f"{ticker} Inc.",
        exchange="NASDAQ",
        sector="Technology",
        industry="Software",
        market_cap=1_000_000_000,
        avg_dollar_volume_20d=20_000_000,
        has_options=True,
        is_active=True,
        updated_at=AVAILABLE_AT,
        metadata={"type": security_type},
    )


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_manual_bar_csv(path: Path, tickers: list[str], *, as_of: str) -> None:
    stamp = datetime(2026, 5, 11, 21, tzinfo=UTC).isoformat()
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=MANUAL_BAR_COLUMNS)
        writer.writeheader()
        for index, ticker in enumerate(tickers):
            writer.writerow(
                {
                    "ticker": ticker,
                    "date": as_of,
                    "open": "100",
                    "high": "101",
                    "low": "99",
                    "close": f"{100 + (index / 100):.2f}",
                    "volume": "1000000",
                    "vwap": "100",
                    "adjusted": "true",
                    "provider": "manual_csv",
                    "source_ts": stamp,
                    "available_at": stamp,
                }
            )


def _write_mixed_manual_bar_csv(
    path: Path,
    *,
    complete_tickers: list[str],
    empty_tickers: list[str],
    partial_tickers: list[str],
    as_of: str,
) -> None:
    stamp = datetime(2026, 5, 11, 21, tzinfo=UTC).isoformat()
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=MANUAL_BAR_COLUMNS)
        writer.writeheader()
        for index, ticker in enumerate(complete_tickers):
            writer.writerow(
                {
                    "ticker": ticker,
                    "date": as_of,
                    "open": "100",
                    "high": "101",
                    "low": "99",
                    "close": f"{100 + (index / 100):.2f}",
                    "volume": "1000000",
                    "vwap": "100",
                    "adjusted": "true",
                    "provider": "manual_csv",
                    "source_ts": stamp,
                    "available_at": stamp,
                }
            )
        for ticker in empty_tickers:
            writer.writerow(
                {
                    "ticker": ticker,
                    "date": as_of,
                    "open": "",
                    "high": "",
                    "low": "",
                    "close": "",
                    "volume": "",
                    "vwap": "",
                    "adjusted": "true",
                    "provider": "manual_csv",
                    "source_ts": stamp,
                    "available_at": stamp,
                }
            )
        for ticker in partial_tickers:
            writer.writerow(
                {
                    "ticker": ticker,
                    "date": as_of,
                    "open": "100",
                    "high": "",
                    "low": "",
                    "close": "",
                    "volume": "",
                    "vwap": "",
                    "adjusted": "true",
                    "provider": "manual_csv",
                    "source_ts": stamp,
                    "available_at": stamp,
                }
            )


def _daily_bar(ticker: str, bar_date: date) -> DailyBar:
    return DailyBar(
        ticker=ticker,
        date=bar_date,
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.0,
        volume=1_000_000,
        vwap=100.0,
        adjusted=True,
        provider="manual_csv",
        source_ts=datetime(2026, 5, 11, 21, tzinfo=UTC),
        available_at=datetime(2026, 5, 11, 21, tzinfo=UTC),
    )


def _configure_fake_safe_llm(monkeypatch, database_url: str) -> None:
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("CATALYST_ENABLE_PREMIUM_LLM", "true")
    monkeypatch.setenv("CATALYST_LLM_PROVIDER", "fake")
    monkeypatch.setenv("CATALYST_LLM_EVIDENCE_MODEL", "fake")
    monkeypatch.setenv("CATALYST_LLM_SKEPTIC_MODEL", "fake")
    monkeypatch.setenv("CATALYST_LLM_DECISION_CARD_MODEL", "fake")
    monkeypatch.setenv("CATALYST_LLM_INPUT_COST_PER_1M", "0")
    monkeypatch.setenv("CATALYST_LLM_CACHED_INPUT_COST_PER_1M", "0")
    monkeypatch.setenv("CATALYST_LLM_OUTPUT_COST_PER_1M", "0")
    monkeypatch.setenv("CATALYST_LLM_PRICING_UPDATED_AT", "2026-05-10")
    monkeypatch.setenv("CATALYST_LLM_DAILY_BUDGET_USD", "1")
    monkeypatch.setenv("CATALYST_LLM_MONTHLY_BUDGET_USD", "10")


def _audit_event_rows(engine) -> list[dict[str, object]]:
    with engine.connect() as conn:
        return [
            dict(row._mapping)
            for row in conn.execute(
                select(audit_events).order_by(
                    audit_events.c.occurred_at,
                    audit_events.c.created_at,
                    audit_events.c.id,
                )
            )
        ]


def _insert_candidate(engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            insert(candidate_states).values(
                id="state-msft",
                ticker="MSFT",
                as_of=AS_OF,
                state=ActionState.WARNING.value,
                previous_state=None,
                final_score=78.0,
                score_delta_5d=4.0,
                hard_blocks=[],
                transition_reasons=["score_requires_manual_review"],
                feature_version="score-v4-options-theme",
                policy_version="policy-v2-events",
                created_at=AVAILABLE_AT,
            )
        )
        conn.execute(
            insert(signal_features).values(
                ticker="MSFT",
                as_of=AS_OF,
                feature_version="score-v4-options-theme",
                price_strength=82.0,
                volume_score=74.0,
                liquidity_score=91.0,
                risk_penalty=4.0,
                portfolio_penalty=1.0,
                final_score=78.0,
                payload={
                    "candidate": {
                        "ticker": "MSFT",
                        "as_of": AS_OF.isoformat(),
                        "entry_zone": [100.0, 104.0],
                        "invalidation_price": 94.0,
                        "metadata": {
                            "source_ts": SOURCE_TS.isoformat(),
                            "available_at": AVAILABLE_AT.isoformat(),
                            "setup_type": "breakout",
                            "portfolio_impact": {"hard_blocks": []},
                        },
                    },
                    "policy": {
                        "state": ActionState.WARNING.value,
                        "hard_blocks": [],
                        "reasons": ["score_requires_manual_review"],
                    },
                },
            )
        )
        conn.execute(
            insert(candidate_packets).values(
                id="packet-MSFT",
                ticker="MSFT",
                as_of=AS_OF,
                candidate_state_id="state-msft",
                state=ActionState.WARNING.value,
                final_score=78.0,
                schema_version="candidate-packet-v1",
                source_ts=SOURCE_TS,
                available_at=AVAILABLE_AT,
                payload={
                    "supporting_evidence": [
                        {
                            "kind": "news",
                            "title": "MSFT evidence update",
                            "summary": "MSFT reported a material product catalyst.",
                            "polarity": "supporting",
                            "strength": 0.81,
                            "source_id": "event-msft",
                            "source_quality": 0.9,
                            "source_ts": SOURCE_TS.isoformat(),
                            "available_at": AVAILABLE_AT.isoformat(),
                        }
                    ],
                    "disconfirming_evidence": [
                        {
                            "kind": "risk",
                            "title": "MSFT valuation risk",
                            "summary": "Valuation remains extended versus recent growth.",
                            "polarity": "disconfirming",
                            "strength": 0.42,
                            "computed_feature_id": "risk-msft",
                            "source_quality": 0.7,
                            "source_ts": SOURCE_TS.isoformat(),
                            "available_at": AVAILABLE_AT.isoformat(),
                        }
                    ],
                    "conflicts": [],
                    "hard_blocks": [],
                    "trade_plan": {
                        "entry_zone": [100.0, 104.0],
                        "invalidation_price": 94.0,
                    },
                },
                created_at=AVAILABLE_AT,
            )
        )
        conn.execute(
            insert(decision_cards).values(
                id="card-MSFT",
                ticker="MSFT",
                as_of=AS_OF,
                candidate_packet_id="packet-MSFT",
                action_state=ActionState.WARNING.value,
                setup_type="breakout",
                final_score=78.0,
                schema_version="decision-card-v1",
                source_ts=SOURCE_TS,
                available_at=AVAILABLE_AT,
                next_review_at=AVAILABLE_AT,
                user_decision=None,
                payload={"manual_review_only": True, "disclaimer": "Manual review only."},
                created_at=AVAILABLE_AT,
            )
        )


def _insert_alert(engine) -> Alert:
    alert = Alert(
        id=alert_id(
            ticker="MSFT",
            route="immediate_manual_review",
            dedupe_key=_alert_dedupe_key(),
            available_at=AVAILABLE_AT,
        ),
        ticker="MSFT",
        as_of=AS_OF,
        source_ts=SOURCE_TS,
        available_at=AVAILABLE_AT,
        candidate_state_id="state-msft",
        candidate_packet_id="packet-MSFT",
        decision_card_id="card-MSFT",
        action_state="EligibleForManualBuyReview",
        route="immediate_manual_review",
        channel="dashboard",
        priority="high",
        status="planned",
        dedupe_key=_alert_dedupe_key(),
        trigger_kind="state_transition",
        trigger_fingerprint="ResearchOnly->EligibleForManualBuyReview",
        title="MSFT manual review alert",
        summary="MSFT candidate is ready for manual review.",
        feedback_url="/api/alerts/alert-msft/feedback",
        payload={"score": 92.5},
        created_at=AVAILABLE_AT,
        sent_at=None,
    )
    AlertRepository(engine).upsert_alert(alert)
    return alert


def _alert_dedupe_key() -> str:
    return (
        "alert-dedupe-v1:MSFT:immediate_manual_review:"
        "EligibleForManualBuyReview:state_transition:"
        "ResearchOnly->EligibleForManualBuyReview"
    )

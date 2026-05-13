from __future__ import annotations

from datetime import UTC, date, datetime

from fastapi.testclient import TestClient
from sqlalchemy import func, insert, select

import apps.api.main as api_main
from apps.api.main import create_app
from catalyst_radar.alerts.models import Alert, alert_id
from catalyst_radar.api.routes import radar as radar_routes
from catalyst_radar.core.models import ActionState
from catalyst_radar.dashboard import data as dashboard_data
from catalyst_radar.jobs.scheduler import SchedulerRunResult
from catalyst_radar.jobs.tasks import DailyRunResult, DailyRunSpec, JobStepResult
from catalyst_radar.security.audit import AuditLogRepository
from catalyst_radar.storage.alert_repositories import AlertRepository
from catalyst_radar.storage.db import create_schema, engine_from_url
from catalyst_radar.storage.schema import (
    alerts,
    audit_events,
    candidate_packets,
    candidate_states,
    decision_cards,
    signal_features,
    useful_alert_labels,
    user_feedback,
)

AS_OF = datetime(2026, 5, 1, 21, tzinfo=UTC)
SOURCE_TS = datetime(2026, 5, 1, 20, 30, tzinfo=UTC)
AVAILABLE_AT = datetime(2026, 5, 1, 21, 5, tzinfo=UTC)


def test_api_health() -> None:
    client = TestClient(create_app())

    response = client.get("/api/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "catalyst-radar"}


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


def test_post_radar_run_builds_scheduler_config(tmp_path, monkeypatch) -> None:
    database_url = _database_url(tmp_path, "radar-run.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
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
    assert response.json() == {
        "acquired_lock": True,
        "reason": None,
        "lock_expires_at": None,
        "daily_result": None,
    }
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
    client = TestClient(create_app())

    response = client.get("/api/radar/runs/latest")

    assert response.status_code == 200
    assert response.json() == {"status": "success", "step_count": 10}


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
                    "supporting_evidence": [],
                    "disconfirming_evidence": [],
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

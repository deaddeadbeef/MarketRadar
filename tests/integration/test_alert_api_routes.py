from __future__ import annotations

from datetime import UTC, datetime

from fastapi.testclient import TestClient
from sqlalchemy import func, select

from apps.api.main import create_app
from catalyst_radar.alerts.models import Alert, alert_id
from catalyst_radar.storage.alert_repositories import AlertRepository
from catalyst_radar.storage.db import create_schema, engine_from_url
from catalyst_radar.storage.schema import user_feedback
from catalyst_radar.storage.validation_repositories import ValidationRepository

AS_OF = datetime(2026, 5, 1, 21, tzinfo=UTC)
SOURCE_TS = datetime(2026, 5, 1, 20, 30, tzinfo=UTC)
AVAILABLE_AT = datetime(2026, 5, 1, 21, 5, tzinfo=UTC)
FUTURE_AT = datetime(2099, 1, 1, 21, 5, tzinfo=UTC)


def test_get_alerts_returns_rows(tmp_path, monkeypatch) -> None:
    database_url = _database_url(tmp_path, "alerts-list.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = _create_database(database_url)
    alert = _insert_alert(engine)

    client = TestClient(create_app())

    response = client.get("/api/alerts")

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["items"]) == 1
    item = payload["items"][0]
    assert item["id"] == alert.id
    assert item["ticker"] == "MSFT"
    assert item["route"] == "immediate_manual_review"
    assert item["payload"] == {"score": 92.5, "evidence": ["visible"]}


def test_get_alerts_rejects_invalid_filters(tmp_path, monkeypatch) -> None:
    database_url = _database_url(tmp_path, "alerts-invalid-filters.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)

    client = TestClient(create_app())

    bad_status = client.get("/api/alerts?status=bad")
    bad_route = client.get("/api/alerts?route=bad")

    assert bad_status.status_code == 422
    assert bad_status.json()["detail"].startswith("status must be one of:")
    assert bad_route.status_code == 422
    assert bad_route.json()["detail"].startswith("route must be one of:")


def test_get_alert_detail_returns_404_for_missing_alert(tmp_path, monkeypatch) -> None:
    database_url = _database_url(tmp_path, "missing-alert.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)

    client = TestClient(create_app())

    response = client.get("/api/alerts/missing-alert")

    assert response.status_code == 404
    assert response.json() == {"detail": "alert not found"}


def test_get_alert_detail_returns_payload(tmp_path, monkeypatch) -> None:
    database_url = _database_url(tmp_path, "alert-detail.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = _create_database(database_url)
    alert = _insert_alert(engine)

    client = TestClient(create_app())

    response = client.get(f"/api/alerts/{alert.id}")

    assert response.status_code == 200
    payload = response.json()
    assert payload["id"] == alert.id
    assert payload["ticker"] == "MSFT"
    assert payload["title"] == "MSFT manual review alert"
    assert payload["payload"] == {"score": 92.5, "evidence": ["visible"]}


def test_post_alert_feedback_records_user_feedback_and_useful_label(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _database_url(tmp_path, "alert-feedback.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = _create_database(database_url)
    alert = _insert_alert(engine)

    client = TestClient(create_app())

    response = client.post(
        f"/api/alerts/{alert.id}/feedback",
        json={"label": "useful", "notes": "Good review prompt"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload == {
        "id": f"useful-alert-label-v1:alert:{alert.id}:useful",
        "artifact_type": "alert",
        "artifact_id": alert.id,
        "ticker": "MSFT",
        "label": "useful",
    }
    feedback = AlertRepository(engine).latest_feedback(
        artifact_type="alert",
        artifact_id=alert.id,
    )
    useful_label = ValidationRepository(engine).latest_useful_alert_label(
        artifact_type="alert",
        artifact_id=alert.id,
    )
    assert feedback is not None
    assert feedback.ticker == "MSFT"
    assert feedback.label == "useful"
    assert feedback.notes == "Good review prompt"
    assert feedback.source == "api"
    assert useful_label is not None
    assert useful_label.ticker == "MSFT"
    assert useful_label.label == "useful"
    assert useful_label.notes == "Good review prompt"


def test_generic_feedback_validates_alert_table(tmp_path, monkeypatch) -> None:
    database_url = _database_url(tmp_path, "generic-alert-feedback.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = _create_database(database_url)
    alert = _insert_alert(engine)

    client = TestClient(create_app())

    missing_response = client.post(
        "/api/feedback",
        json={
            "artifact_type": "alert",
            "artifact_id": "candidate-state-MSFT",
            "ticker": "MSFT",
            "label": "useful",
        },
    )
    response = client.post(
        "/api/feedback",
        json={
            "artifact_type": "alert",
            "artifact_id": alert.id,
            "ticker": "MSFT",
            "label": "acted",
        },
    )

    assert missing_response.status_code == 404
    assert missing_response.json() == {"detail": "referenced artifact not found"}
    assert response.status_code == 200
    assert response.json()["artifact_id"] == alert.id
    with engine.connect() as conn:
        assert conn.execute(select(func.count()).select_from(user_feedback)).scalar_one() == 1


def test_generic_feedback_rejects_future_alert_artifact(tmp_path, monkeypatch) -> None:
    database_url = _database_url(tmp_path, "generic-alert-future-feedback.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = _create_database(database_url)
    alert = _insert_alert(engine, available_at=FUTURE_AT, created_at=FUTURE_AT)

    client = TestClient(create_app())

    alert_specific = client.post(
        f"/api/alerts/{alert.id}/feedback",
        json={"label": "useful"},
    )
    generic = client.post(
        "/api/feedback",
        json={
            "artifact_type": "alert",
            "artifact_id": alert.id,
            "ticker": "MSFT",
            "label": "useful",
        },
    )

    assert alert_specific.status_code == 404
    assert generic.status_code == 404
    assert generic.json() == {"detail": "referenced artifact not found"}
    with engine.connect() as conn:
        assert conn.execute(select(func.count()).select_from(user_feedback)).scalar_one() == 0


def test_alert_feedback_rejects_unknown_fields(tmp_path, monkeypatch) -> None:
    database_url = _database_url(tmp_path, "alert-feedback-extra.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = _create_database(database_url)
    alert = _insert_alert(engine)

    client = TestClient(create_app())

    response = client.post(
        f"/api/alerts/{alert.id}/feedback",
        json={"label": "useful", "note": "typo"},
    )

    assert response.status_code == 422
    with engine.connect() as conn:
        assert conn.execute(select(func.count()).select_from(user_feedback)).scalar_one() == 0


def test_alert_feedback_rejects_ticker_mismatch(tmp_path, monkeypatch) -> None:
    database_url = _database_url(tmp_path, "alert-ticker-mismatch.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = _create_database(database_url)
    alert = _insert_alert(engine)

    client = TestClient(create_app())

    response = client.post(
        f"/api/alerts/{alert.id}/feedback",
        json={"label": "useful", "ticker": "AAPL"},
    )

    assert response.status_code == 422
    assert response.json() == {"detail": "ticker must match the referenced artifact"}
    with engine.connect() as conn:
        assert conn.execute(select(func.count()).select_from(user_feedback)).scalar_one() == 0


def _database_url(tmp_path, name: str) -> str:
    return f"sqlite:///{(tmp_path / name).as_posix()}"


def _create_database(database_url: str):
    engine = engine_from_url(database_url)
    create_schema(engine)
    return engine


def _insert_alert(
    engine,
    *,
    available_at: datetime = AVAILABLE_AT,
    created_at: datetime = AVAILABLE_AT,
) -> Alert:
    alert = Alert(
        id=alert_id(
            ticker="MSFT",
            route="immediate_manual_review",
            dedupe_key=_dedupe_key(),
            available_at=available_at,
        ),
        ticker="MSFT",
        as_of=AS_OF,
        source_ts=SOURCE_TS,
        available_at=available_at,
        candidate_state_id="candidate-state-MSFT",
        candidate_packet_id="candidate-packet-MSFT",
        decision_card_id="decision-card-MSFT",
        action_state="EligibleForManualBuyReview",
        route="immediate_manual_review",
        channel="dashboard",
        priority="high",
        status="planned",
        dedupe_key=_dedupe_key(),
        trigger_kind="state_transition",
        trigger_fingerprint="ResearchOnly->EligibleForManualBuyReview",
        title="MSFT manual review alert",
        summary="MSFT candidate is ready for manual review.",
        feedback_url="/api/alerts/alert-msft/feedback",
        payload={"score": 92.5, "evidence": ["visible"]},
        created_at=created_at,
        sent_at=None,
    )
    AlertRepository(engine).upsert_alert(alert)
    return alert


def _dedupe_key() -> str:
    return (
        "alert-dedupe-v1:MSFT:immediate_manual_review:"
        "EligibleForManualBuyReview:state_transition:"
        "ResearchOnly->EligibleForManualBuyReview"
    )

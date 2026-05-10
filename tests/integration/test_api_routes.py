from __future__ import annotations

from datetime import UTC, datetime

from fastapi.testclient import TestClient
from sqlalchemy import func, insert, select

from apps.api.main import create_app
from catalyst_radar.alerts.models import Alert, alert_id
from catalyst_radar.core.models import ActionState
from catalyst_radar.dashboard import data as dashboard_data
from catalyst_radar.storage.alert_repositories import AlertRepository
from catalyst_radar.storage.db import create_schema, engine_from_url
from catalyst_radar.storage.schema import (
    alerts,
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
        },
        raising=False,
    )
    client = TestClient(create_app())

    response = client.get("/api/ops/health")

    assert response.status_code == 200
    assert response.json()["database"] == {"status": "ok"}
    assert response.json()["providers"] == [{"provider": "csv", "status": "ok"}]


def test_get_cost_summary(tmp_path, monkeypatch) -> None:
    database_url = _database_url(tmp_path, "costs.db")
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _create_database(database_url)
    client = TestClient(create_app())

    response = client.get("/api/costs/summary")

    assert response.status_code == 200
    payload = response.json()
    assert payload["source"] == "budget_ledger"
    assert payload["total_actual_cost_usd"] == 0.0
    assert payload["status_counts"] == {}


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
        json={
            "artifact_type": "decision_card",
            "artifact_id": "card-MSFT",
            "ticker": "msft",
            "label": "useful",
            "notes": "worth review",
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
    assert stored_label["notes"] == "worth review"
    assert candidate_after is not None
    assert dict(candidate_after._mapping) == dict(candidate_before._mapping)


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

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import create_engine

from catalyst_radar.alerts.models import (
    Alert,
    AlertSuppression,
    UserFeedback,
    alert_id,
    alert_suppression_id,
    user_feedback_id,
)
from catalyst_radar.storage.alert_repositories import AlertRepository
from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.validation_repositories import ValidationRepository
from catalyst_radar.validation.models import UsefulAlertLabel, useful_alert_label_id

AS_OF = datetime(2026, 5, 8, 21, tzinfo=UTC)
SOURCE_TS = datetime(2026, 5, 8, 20, 30, tzinfo=UTC)
AVAILABLE_AT = datetime(2026, 5, 10, 14, tzinfo=UTC)
FUTURE_AT = AVAILABLE_AT + timedelta(hours=2)


def test_alert_repository_upserts_and_lists_visible_alerts(
    seeded_repo,
) -> None:
    repo, alert, _, _, _ = seeded_repo

    repo.upsert_alert(alert)

    rows = repo.list_alerts(available_at=AVAILABLE_AT)
    assert rows == [alert]
    assert rows[0].ticker == "MSFT"
    assert repo.alert_by_id(alert.id, available_at=AVAILABLE_AT) == alert
    assert repo.latest_alert_by_dedupe_key(alert.dedupe_key, AVAILABLE_AT) == alert


def test_alert_repository_filters_future_alerts(seeded_repo) -> None:
    repo, visible, future, _, _ = seeded_repo

    assert repo.list_alerts(available_at=AVAILABLE_AT) == [visible]
    assert repo.alert_by_id(future.id, available_at=AVAILABLE_AT) is None
    assert repo.alert_by_id(future.id, available_at=FUTURE_AT) == future
    assert repo.list_alerts(available_at=FUTURE_AT, ticker="aapl") == [future]


def test_alert_repository_records_suppression(seeded_repo) -> None:
    repo, _, _, suppression, _ = seeded_repo

    assert repo.list_suppressions(available_at=AVAILABLE_AT) == [suppression]
    assert repo.list_suppressions(available_at=AVAILABLE_AT - timedelta(minutes=1)) == []


def test_alert_repository_records_user_feedback_and_useful_label(seeded_repo) -> None:
    repo, alert, _, _, feedback = seeded_repo
    validation_repo = ValidationRepository(repo.engine)
    useful_label = UsefulAlertLabel(
        id=useful_alert_label_id(
            artifact_type="alert",
            artifact_id=alert.id,
            label="useful",
        ),
        artifact_type="alert",
        artifact_id=alert.id,
        ticker="msft",
        label="useful",
        notes="Good review prompt",
        created_at=AVAILABLE_AT,
    )

    validation_repo.insert_useful_alert_label(useful_label)

    assert repo.latest_feedback(artifact_type="alert", artifact_id=alert.id) == feedback
    assert validation_repo.latest_useful_alert_label(
        artifact_type="alert",
        artifact_id=alert.id,
    ) == useful_label


@pytest.fixture
def seeded_repo(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'alerts.db'}", future=True)
    create_schema(engine)
    repo = AlertRepository(engine)
    visible = _alert(ticker="msft", available_at=AVAILABLE_AT, created_at=AVAILABLE_AT)
    future = _alert(
        ticker="AAPL",
        available_at=FUTURE_AT,
        created_at=FUTURE_AT,
        dedupe_key="alert-dedupe-v1:AAPL:daily_digest:ResearchOnly:event:press",
    )
    suppression = _suppression()
    feedback = _feedback(visible)
    repo.upsert_alert(visible)
    repo.upsert_alert(future)
    repo.insert_suppression(suppression)
    repo.insert_user_feedback(feedback)
    return repo, visible, future, suppression, feedback


def _alert(
    *,
    ticker: str,
    available_at: datetime,
    created_at: datetime,
    dedupe_key: str = (
        "alert-dedupe-v1:MSFT:immediate_manual_review:"
        "EligibleForManualBuyReview:state_transition:"
        "ResearchOnly->EligibleForManualBuyReview"
    ),
) -> Alert:
    return Alert(
        id=alert_id(
            ticker=ticker,
            route="immediate_manual_review",
            dedupe_key=dedupe_key,
            available_at=available_at,
        ),
        ticker=ticker,
        as_of=AS_OF,
        source_ts=SOURCE_TS,
        available_at=available_at,
        candidate_state_id=f"candidate-state-{ticker.upper()}",
        candidate_packet_id=f"candidate-packet-{ticker.upper()}",
        decision_card_id=f"decision-card-{ticker.upper()}",
        action_state="EligibleForManualBuyReview",
        route="immediate_manual_review",
        channel="dashboard",
        priority="high",
        status="planned",
        dedupe_key=dedupe_key,
        trigger_kind="state_transition",
        trigger_fingerprint="ResearchOnly->EligibleForManualBuyReview",
        title=f"{ticker.upper()} manual review alert",
        summary=f"{ticker.upper()} candidate is ready for manual review.",
        feedback_url=f"/api/alerts/feedback/{ticker.upper()}",
        payload={"score": 92.5, "evidence": ["visible"]},
        created_at=created_at,
        sent_at=None,
    )


def _suppression() -> AlertSuppression:
    dedupe_key = (
        "alert-dedupe-v1:MSFT:immediate_manual_review:"
        "EligibleForManualBuyReview:state_transition:"
        "ResearchOnly->EligibleForManualBuyReview"
    )
    return AlertSuppression(
        id=alert_suppression_id(
            dedupe_key=dedupe_key,
            reason="duplicate_trigger",
            available_at=AVAILABLE_AT,
        ),
        ticker="msft",
        as_of=AS_OF,
        available_at=AVAILABLE_AT,
        candidate_state_id="candidate-state-MSFT",
        decision_card_id="decision-card-MSFT",
        route="immediate_manual_review",
        dedupe_key=dedupe_key,
        trigger_kind="state_transition",
        trigger_fingerprint="ResearchOnly->EligibleForManualBuyReview",
        reason="duplicate_trigger",
        payload={"existing_alert_id": "alert-msft"},
        created_at=AVAILABLE_AT,
    )


def _feedback(alert: Alert) -> UserFeedback:
    return UserFeedback(
        id=user_feedback_id(
            artifact_type="alert",
            artifact_id=alert.id,
            label="useful",
            created_at=AVAILABLE_AT,
        ),
        artifact_type="alert",
        artifact_id=alert.id,
        ticker="msft",
        label="useful",
        notes="Good review prompt",
        source="test",
        payload={"alert_id": alert.id, "route": alert.route.value},
        created_at=AVAILABLE_AT,
    )

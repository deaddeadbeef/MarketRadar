from __future__ import annotations

from datetime import UTC, datetime

from catalyst_radar.alerts.models import AlertChannel, AlertPriority, AlertRoute
from catalyst_radar.alerts.routing import (
    AlertCandidate,
    alert_summary,
    alert_title,
    route_alert,
)
from catalyst_radar.core.models import ActionState

AS_OF = datetime(2026, 5, 8, 21, 0, tzinfo=UTC)
AVAILABLE_AT = datetime(2026, 5, 10, 14, 0, tzinfo=UTC)
FORBIDDEN_COPY = ("buy now", "sell now", "execute", "place order", "automatic trade")


def test_routes_eligible_manual_review_to_immediate_alert() -> None:
    candidate = _candidate(
        state=ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW,
        previous_state=ActionState.WARNING,
        decision_card_id="card-msft",
    )

    decision = route_alert(candidate)

    assert decision.should_alert is True
    assert decision.route == AlertRoute.IMMEDIATE_MANUAL_REVIEW
    assert decision.channel == AlertChannel.DASHBOARD
    assert decision.priority == AlertPriority.HIGH
    assert decision.trigger_kind == "state_transition"
    assert (
        decision.trigger_fingerprint
        == "state_transition:Warning->EligibleForManualBuyReview"
    )
    copy = f"{alert_title(candidate, decision)} {alert_summary(candidate, decision)}".lower()
    assert "manual review" in copy
    assert not any(phrase in copy for phrase in FORBIDDEN_COPY)


def test_routes_high_delta_warning_to_digest() -> None:
    candidate = _candidate(state=ActionState.WARNING, score_delta_5d=12.8)

    decision = route_alert(candidate)

    assert decision.should_alert is True
    assert decision.route == AlertRoute.WARNING_DIGEST
    assert decision.channel == AlertChannel.DIGEST
    assert decision.priority == AlertPriority.HIGH
    assert decision.trigger_kind == "score_delta"
    assert decision.trigger_fingerprint == "score_delta:10"


def test_suppresses_low_delta_warning() -> None:
    candidate = _candidate(state=ActionState.WARNING, score_delta_5d=9.9)

    decision = route_alert(candidate)

    assert decision.should_alert is False
    assert decision.reason == "warning_delta_below_threshold"
    assert decision.route is None
    assert decision.channel is None
    assert decision.priority is None


def test_routes_research_and_watchlist_to_daily_digest() -> None:
    for state in (ActionState.RESEARCH_ONLY, ActionState.ADD_TO_WATCHLIST):
        candidate = _candidate(state=state)

        decision = route_alert(candidate)

        assert decision.should_alert is True
        assert decision.route == AlertRoute.DAILY_DIGEST
        assert decision.channel == AlertChannel.DIGEST
        assert decision.priority == AlertPriority.NORMAL
        assert decision.trigger_kind == "event"


def test_routes_thesis_weakening_and_exit_review_to_position_watch() -> None:
    thesis = route_alert(_candidate(state=ActionState.THESIS_WEAKENING))
    exit_review = route_alert(_candidate(state=ActionState.EXIT_INVALIDATE_REVIEW))

    assert thesis.should_alert is True
    assert thesis.route == AlertRoute.POSITION_WATCH
    assert thesis.channel == AlertChannel.DASHBOARD
    assert thesis.priority == AlertPriority.HIGH
    assert thesis.trigger_kind == "invalidation"

    assert exit_review.should_alert is True
    assert exit_review.route == AlertRoute.POSITION_WATCH
    assert exit_review.channel == AlertChannel.DASHBOARD
    assert exit_review.priority == AlertPriority.CRITICAL
    assert exit_review.trigger_kind == "invalidation"


def test_suppresses_blocked_and_no_action() -> None:
    for state in (ActionState.BLOCKED, ActionState.NO_ACTION):
        decision = route_alert(_candidate(state=state))

        assert decision.should_alert is False
        assert decision.reason == "state_not_alertable"
        assert decision.route is None


def test_routing_requires_decision_card_for_manual_review() -> None:
    candidate = _candidate(
        state=ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW,
        decision_card_id=None,
    )

    decision = route_alert(candidate)

    assert decision.should_alert is False
    assert decision.reason == "manual_review_missing_decision_card"
    assert decision.route is None


def _candidate(
    *,
    state: ActionState,
    previous_state: ActionState | None = ActionState.RESEARCH_ONLY,
    score_delta_5d: float = 14.0,
    decision_card_id: str | None = "card-msft",
    evidence: dict[str, object] | None = None,
) -> AlertCandidate:
    return AlertCandidate(
        ticker="msft",
        as_of=AS_OF,
        source_ts=AS_OF,
        available_at=AVAILABLE_AT,
        candidate_state_id="state-msft",
        action_state=state,
        previous_state=previous_state,
        final_score=82.4,
        score_delta_5d=score_delta_5d,
        candidate_packet_id="packet-msft",
        decision_card_id=decision_card_id,
        top_supporting_evidence=evidence
        or {"source_id": "event-msft", "title": "Evidence strengthened"},
        entry_zone=(100.0, 105.0),
        invalidation_price=94.5,
        payload={"audit": "unit"},
    )

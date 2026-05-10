from __future__ import annotations

from datetime import UTC, datetime

from catalyst_radar.alerts.dedupe import alert_dedupe_key, decide_dedupe
from catalyst_radar.alerts.models import Alert, AlertStatus
from catalyst_radar.alerts.routing import AlertCandidate, route_alert
from catalyst_radar.core.models import ActionState

AS_OF = datetime(2026, 5, 8, 21, 0, tzinfo=UTC)
AVAILABLE_AT = datetime(2026, 5, 10, 14, 0, tzinfo=UTC)


def test_dedupe_key_is_stable_for_same_trigger() -> None:
    first = _candidate(state=ActionState.WARNING, candidate_state_id="state-1")
    rebuilt = _candidate(state=ActionState.WARNING, candidate_state_id="state-2")

    first_key = alert_dedupe_key(first, route_alert(first))
    rebuilt_key = alert_dedupe_key(rebuilt, route_alert(rebuilt))

    assert first_key == rebuilt_key
    assert first_key.startswith("alert-dedupe-v1:MSFT:warning_digest:Warning:score_delta:")


def test_state_change_produces_distinct_dedupe_key() -> None:
    first = _candidate(
        state=ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW,
        previous_state=ActionState.WARNING,
    )
    changed = _candidate(
        state=ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW,
        previous_state=ActionState.RESEARCH_ONLY,
    )

    assert alert_dedupe_key(first, route_alert(first)) != alert_dedupe_key(
        changed,
        route_alert(changed),
    )


def test_new_evidence_produces_distinct_dedupe_key() -> None:
    first = _candidate(
        state=ActionState.RESEARCH_ONLY,
        evidence={"source_id": "event-1", "title": "First evidence"},
    )
    changed = _candidate(
        state=ActionState.RESEARCH_ONLY,
        evidence={"source_id": "event-2", "title": "Second evidence"},
    )

    assert alert_dedupe_key(first, route_alert(first)) != alert_dedupe_key(
        changed,
        route_alert(changed),
    )


def test_evidence_url_fingerprint_is_canonical_and_hash_only() -> None:
    first = _candidate(
        state=ActionState.RESEARCH_ONLY,
        evidence={
            "source_url": "HTTPS://Example.com/path?a=1&utm_source=news",
            "title": "Evidence",
        },
    )
    same_without_tracking = _candidate(
        state=ActionState.RESEARCH_ONLY,
        evidence={"source_url": "https://example.com/path?a=1", "title": "Evidence"},
    )

    first_key = alert_dedupe_key(first, route_alert(first))
    second_key = alert_dedupe_key(same_without_tracking, route_alert(same_without_tracking))

    assert first_key == second_key
    assert "https://" not in first_key
    assert "utm_source" not in first_key


def test_score_delta_bucket_produces_distinct_key_when_threshold_moves() -> None:
    first = _candidate(state=ActionState.WARNING, score_delta_5d=12.0)
    changed = _candidate(state=ActionState.WARNING, score_delta_5d=17.0)

    assert alert_dedupe_key(first, route_alert(first)) != alert_dedupe_key(
        changed,
        route_alert(changed),
    )


def test_duplicate_existing_alert_returns_suppression_decision() -> None:
    candidate = _candidate(state=ActionState.WARNING, score_delta_5d=12.0)
    route_decision = route_alert(candidate)
    dedupe_key = alert_dedupe_key(candidate, route_decision)

    decision = decide_dedupe(
        Alert(
            id="alert-existing",
            ticker=candidate.ticker,
            as_of=candidate.as_of,
            source_ts=candidate.source_ts,
            available_at=candidate.available_at,
            action_state=candidate.action_state.value,
            route=route_decision.route,
            channel=route_decision.channel,
            priority=route_decision.priority,
            status=AlertStatus.PLANNED,
            dedupe_key=dedupe_key,
            trigger_kind=route_decision.trigger_kind,
            trigger_fingerprint=route_decision.trigger_fingerprint,
            title="MSFT warning evidence digest",
            summary="MSFT candidate review prompt",
            created_at=AVAILABLE_AT,
        ),
        dedupe_key,
    )

    assert decision.emit is False
    assert decision.dedupe_key == dedupe_key
    assert decision.reason == "duplicate_trigger"


def _candidate(
    *,
    state: ActionState,
    previous_state: ActionState | None = ActionState.RESEARCH_ONLY,
    score_delta_5d: float = 14.0,
    candidate_state_id: str = "state-msft",
    evidence: dict[str, object] | None = None,
) -> AlertCandidate:
    return AlertCandidate(
        ticker="MSFT",
        as_of=AS_OF,
        source_ts=AS_OF,
        available_at=AVAILABLE_AT,
        candidate_state_id=candidate_state_id,
        action_state=state,
        previous_state=previous_state,
        final_score=82.4,
        score_delta_5d=score_delta_5d,
        candidate_packet_id="packet-msft",
        decision_card_id="card-msft",
        top_supporting_evidence=evidence
        or {"source_id": "event-msft", "title": "Evidence strengthened"},
        entry_zone=(100.0, 105.0),
        invalidation_price=94.5,
        payload={"audit": "unit"},
    )

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from catalyst_radar.core.models import ActionState
from catalyst_radar.pipeline.candidate_packet import (
    CANDIDATE_PACKET_SCHEMA_VERSION,
    CandidatePacket,
    EvidenceItem,
    build_candidate_packet,
    canonical_packet_json,
    packet_payload,
)

AS_OF = datetime(2026, 5, 10, 21, tzinfo=UTC)
SOURCE_TS = datetime(2026, 5, 10, 12, tzinfo=UTC)
AVAILABLE_AT = datetime(2026, 5, 10, 13, tzinfo=UTC)


def test_warning_packet_gets_supporting_and_disconfirming_evidence() -> None:
    packet = build_candidate_packet(
        candidate_state=_candidate_state(
            state=ActionState.WARNING,
            transition_reasons=("trade_plan_required",),
        ),
        signal_features_payload=_signal_payload(
            candidate_overrides={
                "entry_zone": None,
                "invalidation_price": None,
                "reward_risk": 0.0,
            },
            policy_overrides={"missing_trade_plan": ["entry_zone", "invalidation_price"]},
        ),
        events=[_event()],
        snippets=[_snippet(sentiment=0.45)],
        requested_available_at=AVAILABLE_AT,
    )

    assert packet.schema_version == CANDIDATE_PACKET_SCHEMA_VERSION
    assert packet.state == ActionState.WARNING
    assert {item.kind for item in packet.supporting_evidence} >= {
        "material_event",
        "text_snippet",
        "computed_feature",
    }
    assert any(item.kind == "missing_trade_plan" for item in packet.disconfirming_evidence)
    assert all(
        item.source_id or item.source_url or item.computed_feature_id
        for item in (*packet.supporting_evidence, *packet.disconfirming_evidence)
    )
    payload = packet_payload(packet)
    assert payload["audit"]["score_recomputed"] is False
    assert payload["audit"]["llm_calls"] is False


def test_warning_packet_adds_explicit_evidence_gap_when_no_bear_case_exists() -> None:
    packet = build_candidate_packet(
        candidate_state=_candidate_state(state=ActionState.WARNING),
        signal_features_payload=_signal_payload(),
        requested_available_at=AVAILABLE_AT,
    )

    assert [item.kind for item in packet.disconfirming_evidence] == ["evidence_gap"]
    assert packet.disconfirming_evidence[0].computed_feature_id == (
        "candidate_states:state-msft:evidence_gap"
    )


def test_warning_or_higher_non_buy_states_require_two_sided_evidence() -> None:
    packet = build_candidate_packet(
        candidate_state=_candidate_state(state=ActionState.THESIS_WEAKENING),
        signal_features_payload=_signal_payload(),
        requested_available_at=AVAILABLE_AT,
    )

    assert packet.state == ActionState.THESIS_WEAKENING
    assert packet.supporting_evidence
    assert packet.disconfirming_evidence
    assert packet.payload["escalation"]["packet_required"] is True


def test_blocked_packet_carries_hard_blocks_and_reasons() -> None:
    packet = build_candidate_packet(
        candidate_state=_candidate_state(
            state=ActionState.BLOCKED,
            hard_blocks=("liquidity_hard_block",),
            transition_reasons=("liquidity score below policy floor",),
        ),
        signal_features_payload=_signal_payload(
            candidate_overrides={"risk_penalty": 22.0},
            policy_overrides={
                "hard_blocks": ["liquidity_hard_block"],
                "reasons": ["liquidity score below policy floor"],
            },
        ),
        requested_available_at=AVAILABLE_AT,
    )

    assert packet.state == ActionState.BLOCKED
    assert packet.hard_blocks == ("liquidity_hard_block",)
    payload = packet_payload(packet)
    assert payload["policy"]["transition_reasons"] == [
        "liquidity score below policy floor"
    ]
    assert any(item.kind == "hard_block" for item in packet.disconfirming_evidence)


def test_eligible_packet_contains_trade_plan_and_portfolio_impact() -> None:
    packet = build_candidate_packet(
        candidate_state=_candidate_state(
            state=ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW,
            final_score=88.0,
            transition_reasons=("all_buy_review_gates_passed",),
        ),
        signal_features_payload=_signal_payload(final_score=88.0),
        portfolio_row=_portfolio_row(),
        requested_available_at=AVAILABLE_AT,
    )

    assert packet.state == ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW
    payload = packet_payload(packet)
    assert payload["trade_plan"] == {
        "setup_type": "breakout",
        "entry_zone": [100.0, 104.0],
        "invalidation_price": 94.0,
        "target_price": 125.0,
        "reward_risk": 2.7,
        "missing_fields": [],
        "position_size": {
            "shares": 40,
            "notional": 4160.0,
            "position_pct": 0.0416,
            "risk_amount": 400.0,
            "is_capped": False,
        },
    }
    assert payload["portfolio_impact"]["proposed_notional"] == 4160.0
    assert {item.kind for item in packet.supporting_evidence} >= {
        "setup_plan",
        "portfolio_impact",
    }
    assert payload["escalation"]["decision_card_required"] is True
    assert payload["escalation"]["no_trade_execution"] is True


def test_evidence_validation_rejects_unsupported_or_naive_claims() -> None:
    with pytest.raises(ValueError, match="source_id"):
        EvidenceItem(
            kind="claim",
            title="Unsupported",
            summary="No source link.",
            polarity="supporting",
            strength=0.5,
        )

    with pytest.raises(ValueError, match="timezone-aware"):
        EvidenceItem(
            kind="claim",
            title="Naive timestamp",
            summary="Naive timestamp is not point in time safe.",
            polarity="supporting",
            strength=0.5,
            computed_feature_id="signal_features:MSFT:x",
            source_ts=datetime(2026, 5, 10, 12),
        )


def test_builder_rejects_missing_signal_feature_payload() -> None:
    with pytest.raises(ValueError, match="signal_features payload"):
        build_candidate_packet(
            candidate_state=_candidate_state(state=ActionState.WARNING),
            signal_features_payload={},
            requested_available_at=AVAILABLE_AT,
        )


def test_candidate_packet_validation_rejects_manual_packet_without_both_sides() -> None:
    support = EvidenceItem(
        kind="computed_feature",
        title="Support",
        summary="Linked support.",
        polarity="supporting",
        strength=0.5,
        computed_feature_id="signal_features:MSFT:x",
        source_ts=SOURCE_TS,
        available_at=AVAILABLE_AT,
    )

    with pytest.raises(ValueError, match="disconfirming evidence"):
        CandidatePacket(
            id="packet",
            ticker="MSFT",
            as_of=AS_OF,
            candidate_state_id="state-msft",
            state=ActionState.WARNING,
            final_score=77.0,
            supporting_evidence=(support,),
            disconfirming_evidence=(),
            conflicts=(),
            hard_blocks=(),
            payload={},
            source_ts=SOURCE_TS,
            available_at=AVAILABLE_AT,
        )


def test_rebuilding_same_inputs_yields_same_id_and_payload() -> None:
    kwargs = {
        "candidate_state": _candidate_state(state=ActionState.WARNING),
        "signal_features_payload": _signal_payload(),
        "events": [_event()],
        "snippets": [_snippet()],
        "requested_available_at": AVAILABLE_AT,
    }

    first = build_candidate_packet(**kwargs)
    second = build_candidate_packet(**kwargs)

    assert first.id == second.id
    assert canonical_packet_json(first) == canonical_packet_json(second)


def test_future_optional_records_are_excluded_from_point_in_time_packet() -> None:
    future_event = {
        **_event(event_id="event-future", title="Future event"),
        "available_at": datetime(2026, 5, 11, 13, tzinfo=UTC).isoformat(),
    }

    packet = build_candidate_packet(
        candidate_state=_candidate_state(state=ActionState.WARNING),
        signal_features_payload=_signal_payload(),
        events=[future_event],
        requested_available_at=AVAILABLE_AT,
    )

    assert "Future event" not in canonical_packet_json(packet)


def _candidate_state(
    *,
    state: ActionState,
    final_score: float = 78.0,
    hard_blocks: tuple[str, ...] = (),
    transition_reasons: tuple[str, ...] = ("score_requires_manual_review",),
) -> dict[str, object]:
    return {
        "id": "state-msft",
        "ticker": "MSFT",
        "as_of": AS_OF,
        "state": state.value,
        "final_score": final_score,
        "score_delta_5d": 3.2,
        "hard_blocks": list(hard_blocks),
        "transition_reasons": list(transition_reasons),
        "feature_version": "score-v4-options-theme",
        "policy_version": "policy-v2-events",
        "created_at": AVAILABLE_AT,
    }


def _signal_payload(
    *,
    final_score: float = 78.0,
    candidate_overrides: dict[str, object] | None = None,
    metadata_overrides: dict[str, object] | None = None,
    policy_overrides: dict[str, object] | None = None,
) -> dict[str, object]:
    metadata = {
        "setup_type": "breakout",
        "target_price": 125.0,
        "pillar_scores": {
            "price_strength": 86.0,
            "relative_strength": 81.0,
            "volume_liquidity": 72.0,
            "trend_quality": 66.0,
        },
        "event_bonus": 4.8,
        "local_narrative_score": 72.0,
        "local_narrative_bonus": 4.3,
        "source_quality_score": 87.0,
        "theme_match_score": 74.0,
        "theme_hits": [{"theme_id": "ai_infrastructure", "terms": ["cloud", "ai"]}],
        "selected_snippet_ids": ["snippet-1"],
        "options_flow_score": 68.0,
        "options_bonus": 2.7,
        "options_risk_score": 20.0,
        "options_risk_penalty": 0.8,
        "call_put_ratio": 2.4,
        "iv_percentile": 0.55,
        "sector_rotation_score": 62.0,
        "theme_velocity_score": 58.0,
        "peer_readthrough_score": 35.0,
        "sector_theme_bonus": 4.2,
        "position_size": {
            "shares": 40,
            "notional": 4160.0,
            "position_pct": 0.0416,
            "risk_amount": 400.0,
            "is_capped": False,
        },
        "portfolio_impact": _portfolio_impact(),
        "source_ts": SOURCE_TS.isoformat(),
        "available_at": AVAILABLE_AT.isoformat(),
    }
    metadata.update(metadata_overrides or {})
    candidate = {
        "ticker": "MSFT",
        "as_of": AS_OF.isoformat(),
        "features": {
            "ticker": "MSFT",
            "as_of": AS_OF.isoformat(),
            "feature_version": "score-v4-options-theme",
            "ret_20d": 0.13,
            "rs_20_sector": 83.0,
        },
        "final_score": final_score,
        "strong_pillars": 3,
        "risk_penalty": 8.0,
        "portfolio_penalty": 1.0,
        "data_stale": False,
        "entry_zone": [100.0, 104.0],
        "invalidation_price": 94.0,
        "reward_risk": 2.7,
        "metadata": metadata,
    }
    candidate.update(candidate_overrides or {})
    policy = {
        "state": "Warning",
        "hard_blocks": [],
        "reasons": ["score_requires_manual_review"],
        "missing_trade_plan": [],
        "policy_version": "policy-v2-events",
    }
    policy.update(policy_overrides or {})
    return {
        "ticker": "MSFT",
        "as_of": AS_OF,
        "feature_version": "score-v4-options-theme",
        "final_score": final_score,
        "payload": {"candidate": candidate, "policy": policy},
    }


def _event(
    *,
    event_id: str = "event-1",
    title: str = "Cloud revenue guidance raised",
) -> dict[str, object]:
    return {
        "id": event_id,
        "source_id": event_id,
        "event_type": "guidance",
        "title": title,
        "source": "company_release",
        "source_url": "https://example.com/msft-guidance",
        "source_quality": 0.95,
        "materiality": 0.90,
        "source_ts": SOURCE_TS.isoformat(),
        "available_at": AVAILABLE_AT.isoformat(),
    }


def _snippet(*, sentiment: float = 0.6) -> dict[str, object]:
    return {
        "id": "snippet-1",
        "event_id": "event-1",
        "section": "management commentary",
        "text": "Management cited accelerating cloud demand and durable AI infrastructure demand.",
        "source": "company_release",
        "source_url": "https://example.com/msft-guidance",
        "source_quality": 0.90,
        "materiality": 0.80,
        "sentiment": sentiment,
        "source_ts": SOURCE_TS.isoformat(),
        "available_at": AVAILABLE_AT.isoformat(),
    }


def _portfolio_row() -> dict[str, object]:
    return {
        "id": "portfolio-impact-1",
        "ticker": "MSFT",
        "as_of": AS_OF,
        "setup_type": "breakout",
        "payload": {"portfolio_impact": _portfolio_impact()},
        "source_ts": SOURCE_TS,
        "available_at": AVAILABLE_AT,
    }


def _portfolio_impact() -> dict[str, object]:
    return {
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
    }

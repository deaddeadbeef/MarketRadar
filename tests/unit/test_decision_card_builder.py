from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from catalyst_radar.core.immutability import thaw_json_value
from catalyst_radar.core.models import ActionState
from catalyst_radar.decision_cards import (
    DECISION_CARD_SCHEMA_VERSION,
    FORBIDDEN_EXECUTION_PHRASES,
    attach_llm_review_to_decision_card,
    build_decision_card,
    serialize_decision_card_payload,
)

AS_OF = datetime(2026, 5, 10, 21, tzinfo=UTC)
SOURCE_TS = datetime(2026, 5, 10, 20, 30, tzinfo=UTC)
AVAILABLE_AT = datetime(2026, 5, 10, 21, 5, tzinfo=UTC)


def test_eligible_manual_buy_review_card_includes_required_contract_fields() -> None:
    card = build_decision_card(_packet())
    payload = thaw_json_value(card.payload)

    assert card.id == build_decision_card(_packet()).id
    assert card.schema_version == DECISION_CARD_SCHEMA_VERSION
    assert card.ticker == "AAA"
    assert card.action_state == ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW
    assert card.next_review_at == datetime(2026, 5, 11, 13, 30, tzinfo=UTC)
    assert set(payload) == {
        "identity",
        "scores",
        "trade_plan",
        "position_sizing",
        "portfolio_impact",
        "evidence",
        "disconfirming_evidence",
        "controls",
        "disclaimer",
        "audit",
    }
    assert payload["identity"]["card_type"] == "eligible_manual_review"
    assert payload["identity"]["company"] == "Acme Analytics"
    assert payload["trade_plan"]["entry_zone"] == [100.0, 104.0]
    assert payload["trade_plan"]["invalidation_price"] == 94.0
    assert payload["trade_plan"]["reward_risk"] == 2.6
    assert payload["position_sizing"]["shares"] == 25.0
    assert payload["portfolio_impact"]["single_name"]["after_pct"] == 4.3
    assert payload["evidence"][0]["computed_feature_id"].endswith(":pillar_scores")
    assert payload["disconfirming_evidence"][0]["computed_feature_id"].endswith(":risk_penalty")
    assert payload["controls"]["hard_blocks"] == []
    assert payload["controls"]["next_review_at"] == card.next_review_at.isoformat()
    assert payload["audit"]["candidate_packet_id"] == "packet-aaa-20260510"

    serialized = serialize_decision_card_payload(payload).lower()
    assert "manual review only" in serialized
    for phrase in FORBIDDEN_EXECUTION_PHRASES:
        assert phrase not in serialized
    assert serialize_decision_card_payload(payload) == serialize_decision_card_payload(
        thaw_json_value(build_decision_card(_packet()).payload)
    )


def test_warning_card_is_research_card_and_not_buy_review_eligible() -> None:
    packet = _packet(
        state=ActionState.WARNING,
        final_score=78.0,
        trade_plan={
            "setup_type": "breakout",
            "entry_zone": None,
            "invalidation_price": None,
            "max_loss_if_wrong": None,
            "reward_risk": None,
            "missing_fields": ["entry_zone", "invalidation_price", "reward_risk"],
        },
    )

    card = build_decision_card(packet)
    payload = thaw_json_value(card.payload)

    assert card.action_state == ActionState.WARNING
    assert payload["identity"]["card_type"] == "research"
    assert payload["controls"]["missing_trade_plan"] == [
        "entry_zone",
        "invalidation_price",
        "reward_risk",
    ]
    assert payload["trade_plan"]["entry_zone"] is None
    assert card.next_review_at == AS_OF + timedelta(days=2)


def test_missing_required_trade_plan_blocks_eligible_card_generation() -> None:
    packet = _packet(
        trade_plan={
            "setup_type": "breakout",
            "entry_zone": None,
            "invalidation_price": 94.0,
            "max_loss_if_wrong": 150.0,
            "reward_risk": 2.6,
            "missing_fields": ["entry_zone"],
        }
    )

    with pytest.raises(ValueError, match="EligibleForManualBuyReview.*entry_zone"):
        build_decision_card(packet)


def test_hard_blocked_candidate_produces_blocked_research_card_only() -> None:
    card = build_decision_card(_packet(hard_blocks=["liquidity_hard_block"]))
    payload = thaw_json_value(card.payload)

    assert card.action_state == ActionState.BLOCKED
    assert payload["identity"]["card_type"] == "blocked_research"
    assert payload["controls"]["hard_blocks"] == ["liquidity_hard_block"]
    assert payload["portfolio_impact"]["hard_blocks"] == ["liquidity_hard_block"]
    assert card.next_review_at == AS_OF + timedelta(days=7)


def test_position_sizing_is_copied_from_scan_metadata_without_recalculation() -> None:
    sizing = {
        "risk_per_trade_pct": 0.37,
        "shares": 17.5,
        "notional": 1234.56,
        "cash_check": "pass",
        "sizing_notes": ["copied from deterministic scan metadata"],
    }

    card = build_decision_card(_packet(position_sizing=sizing))
    payload = thaw_json_value(card.payload)

    assert payload["position_sizing"] == sizing


def test_real_packet_score_field_names_are_preserved() -> None:
    packet = _packet()
    packet["payload"]["scores"] = {
        "final": 91.5,
        "pillars": {"price_strength": 94.0},
        "risk_penalty": 3.0,
        "portfolio_penalty": 1.5,
        "score_delta_5d": 4.0,
    }

    card = build_decision_card(packet)
    payload = thaw_json_value(card.payload)

    assert payload["scores"]["pillar_scores"] == {"price_strength": 94.0}
    assert payload["scores"]["score_delta"] == 4.0


def test_duplicate_packet_attribute_and_payload_evidence_is_deduped() -> None:
    evidence = _supporting_evidence()
    disconfirming = _disconfirming_evidence()
    packet = _packet(
        supporting_evidence=[evidence],
        disconfirming_evidence=[disconfirming],
    )
    packet["supporting_evidence"] = [evidence]
    packet["disconfirming_evidence"] = [disconfirming]

    card = build_decision_card(packet)
    payload = thaw_json_value(card.payload)

    assert len(payload["evidence"]) == 1
    assert len(payload["disconfirming_evidence"]) == 1


def test_forbidden_execution_wording_is_rejected() -> None:
    packet = _packet(
        supporting_evidence=[
            {
                "kind": "computed_feature",
                "title": "Unsafe language",
                "summary": "The system can execute this plan.",
                "polarity": "supporting",
                "strength": 0.9,
                "computed_feature_id": "signal_features:AAA:2026-05-10:market-v1:pillar_scores",
            }
        ]
    )

    with pytest.raises(ValueError, match="forbidden execution wording"):
        build_decision_card(packet)


def test_attach_llm_review_adds_narrative_without_changing_deterministic_fields() -> None:
    card = build_decision_card(_packet())
    original_payload = thaw_json_value(card.payload)
    deterministic_keys = {
        "identity",
        "scores",
        "trade_plan",
        "position_sizing",
        "portfolio_impact",
        "evidence",
        "disconfirming_evidence",
        "controls",
        "disclaimer",
        "audit",
    }
    draft = _llm_review_draft()

    updated = attach_llm_review_to_decision_card(card, draft)
    updated_payload = thaw_json_value(updated.payload)

    assert updated is not card
    assert updated.id == card.id
    assert updated.ticker == card.ticker
    assert updated.as_of == card.as_of
    assert updated.candidate_packet_id == card.candidate_packet_id
    assert updated.action_state == card.action_state
    assert updated.setup_type == card.setup_type
    assert updated.final_score == card.final_score
    assert updated.next_review_at == card.next_review_at
    assert updated.schema_version == card.schema_version
    assert updated.source_ts == card.source_ts
    assert updated.available_at == card.available_at
    assert updated.user_decision == card.user_decision
    assert set(updated_payload) == deterministic_keys | {"llm_review"}
    for key in deterministic_keys:
        assert updated_payload[key] == original_payload[key]
    assert updated_payload["llm_review"] == draft
    assert "llm_review" not in original_payload


def test_attach_llm_review_rejects_execution_language() -> None:
    card = build_decision_card(_packet())
    draft = _llm_review_draft(summary="The system should execute the setup.")

    with pytest.raises(ValueError, match="forbidden execution wording"):
        attach_llm_review_to_decision_card(card, draft)


def test_attach_llm_review_requires_manual_review_only() -> None:
    card = build_decision_card(_packet())
    draft = _llm_review_draft()
    draft["manual_review_only"] = False

    with pytest.raises(ValueError, match="manual_review_only"):
        attach_llm_review_to_decision_card(card, draft)


def test_attach_llm_review_rejects_unlinked_claims() -> None:
    card = build_decision_card(_packet())
    draft = _llm_review_draft()
    del draft["supporting_points"][0]["computed_feature_id"]

    with pytest.raises(ValueError, match="supporting_points\\[0\\].*source_id"):
        attach_llm_review_to_decision_card(card, draft)


def test_attach_llm_review_rejects_unknown_evidence_reference() -> None:
    card = build_decision_card(_packet())
    draft = _llm_review_draft()
    draft["risks"][0]["computed_feature_id"] = "feature-unknown"

    with pytest.raises(ValueError, match="computed_feature_id.*decision card evidence"):
        attach_llm_review_to_decision_card(card, draft)


def test_attach_llm_review_rejects_deterministic_fields() -> None:
    card = build_decision_card(_packet())
    draft = _llm_review_draft()
    draft["scores"] = {"final_score": 100.0}

    with pytest.raises(ValueError, match="deterministic fields"):
        attach_llm_review_to_decision_card(card, draft)


def test_attach_llm_review_rejects_extra_claim_fields() -> None:
    card = build_decision_card(_packet())
    draft = _llm_review_draft()
    draft["claims"] = [{"source_id": "unknown-source", "claim": "Unvalidated claim."}]

    with pytest.raises(ValueError, match="unexpected fields"):
        attach_llm_review_to_decision_card(card, draft)


def test_attach_llm_review_rejects_extra_unresolved_conflicts() -> None:
    card = build_decision_card(_packet())
    draft = _llm_review_draft()
    draft["unresolved_conflicts"] = [
        {"source_id": "unknown-source", "claim": "Unvalidated conflict."}
    ]

    with pytest.raises(ValueError, match="unexpected fields"):
        attach_llm_review_to_decision_card(card, draft)


def _packet(
    *,
    state: ActionState = ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW,
    final_score: float = 91.5,
    trade_plan: dict[str, object] | None = None,
    hard_blocks: list[str] | None = None,
    position_sizing: dict[str, object] | None = None,
    supporting_evidence: list[dict[str, object]] | None = None,
    disconfirming_evidence: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    hard_blocks = hard_blocks or []
    return {
        "id": "packet-aaa-20260510",
        "ticker": "aaa",
        "as_of": AS_OF,
        "state": state,
        "final_score": final_score,
        "source_ts": SOURCE_TS,
        "available_at": AVAILABLE_AT,
        "payload": {
            "identity": {
                "ticker": "AAA",
                "company": "Acme Analytics",
                "state": state.value,
                "setup_type": "breakout",
            },
            "scores": {
                "final_score": final_score,
                "pillar_scores": {
                    "price_strength": 94.0,
                    "relative_strength": 88.0,
                    "volume_liquidity": 83.0,
                    "trend_quality": 90.0,
                },
                "risk_penalty": 3.0,
                "portfolio_penalty": 1.5,
                "score_delta": 4.0,
            },
            "trade_plan": trade_plan or _trade_plan(),
            "portfolio_impact": _portfolio_impact(hard_blocks),
            "supporting_evidence": supporting_evidence or [_supporting_evidence()],
            "disconfirming_evidence": disconfirming_evidence or [_disconfirming_evidence()],
            "conflicts": [],
            "hard_blocks": hard_blocks,
            "metadata": {
                "position_sizing": position_sizing or _position_sizing(),
            },
            "audit": {
                "source_ts": SOURCE_TS.isoformat(),
                "available_at": AVAILABLE_AT.isoformat(),
            },
        },
    }


def _trade_plan() -> dict[str, object]:
    return {
        "setup_type": "breakout",
        "entry_zone": [100.0, 104.0],
        "invalidation_price": 94.0,
        "max_loss_if_wrong": 150.0,
        "reward_risk": 2.6,
        "missing_fields": [],
    }


def _position_sizing() -> dict[str, object]:
    return {
        "risk_per_trade_pct": 0.5,
        "shares": 25.0,
        "notional": 2550.0,
        "cash_check": "pass",
        "sizing_notes": ["copied from deterministic scan metadata"],
    }


def _portfolio_impact(hard_blocks: list[str]) -> dict[str, object]:
    return {
        "single_name_before_pct": 2.1,
        "single_name_after_pct": 4.3,
        "sector_before_pct": 12.0,
        "sector_after_pct": 14.2,
        "theme_before_pct": 5.0,
        "theme_after_pct": 6.8,
        "correlated_before_pct": 9.5,
        "correlated_after_pct": 10.9,
        "proposed_notional": 2550.0,
        "max_loss": 150.0,
        "portfolio_penalty": 1.5,
        "hard_blocks": hard_blocks,
    }


def _supporting_evidence() -> dict[str, object]:
    return {
        "kind": "computed_feature",
        "title": "Relative strength confirmed",
        "summary": "Relative strength and price pillars are above deterministic thresholds.",
        "polarity": "supporting",
        "strength": 0.91,
        "computed_feature_id": "signal_features:AAA:2026-05-10:market-v1:pillar_scores",
        "source_quality": 0.8,
    }


def _disconfirming_evidence() -> dict[str, object]:
    return {
        "kind": "computed_feature",
        "title": "Risk penalty still present",
        "summary": "Volatility and extension create a non-zero deterministic risk penalty.",
        "polarity": "disconfirming",
        "strength": 0.42,
        "computed_feature_id": "signal_features:AAA:2026-05-10:market-v1:risk_penalty",
        "source_quality": 0.7,
    }


def _llm_review_draft(
    *,
    summary: str = "Manual-review setup with source-linked evidence notes.",
) -> dict[str, object]:
    return {
        "ticker": "AAA",
        "as_of": AS_OF.isoformat(),
        "schema_version": DECISION_CARD_SCHEMA_VERSION,
        "summary": summary,
        "supporting_points": [
            {
                "text": "Relative strength is above the deterministic threshold.",
                "computed_feature_id": "signal_features:AAA:2026-05-10:market-v1:pillar_scores",
            }
        ],
        "risks": [
            {
                "text": "Volatility and extension create a non-zero deterministic risk penalty.",
                "computed_feature_id": "signal_features:AAA:2026-05-10:market-v1:risk_penalty",
            }
        ],
        "questions_for_human": ["Is source-linked setup durability confirmed?"],
        "manual_review_only": True,
    }

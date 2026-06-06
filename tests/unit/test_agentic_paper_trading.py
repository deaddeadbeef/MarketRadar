from __future__ import annotations

from datetime import UTC, datetime

from catalyst_radar.agents.paper_trading import build_agentic_paper_trade_intent
from catalyst_radar.core.models import ActionState

AS_OF = datetime(2026, 5, 10, 21, tzinfo=UTC)
SOURCE_TS = datetime(2026, 5, 10, 20, 30, tzinfo=UTC)
AVAILABLE_AT = datetime(2026, 5, 10, 21, 5, tzinfo=UTC)


def test_builds_ready_agentic_paper_trade_intent_from_manual_review_card() -> None:
    payload = build_agentic_paper_trade_intent(
        _decision_card(),
        available_at=AVAILABLE_AT,
        entry_price=100.0,
    ).to_payload()

    assert payload["schema_version"] == "agentic-paper-trade-intent-v1"
    assert payload["status"] == "ready"
    assert payload["recommended_paper_decision"] == "approved"
    assert payload["hard_blocks"] == []
    assert payload["requires_override_for_approval"] is False
    assert payload["external_calls_required"] == 0
    assert payload["external_calls_made"] == 0
    assert payload["broker_order_submitted"] is False
    assert payload["order_submission_allowed"] is False
    assert payload["no_execution"] is True
    assert payload["paper_decision"]["decision_card_id"] == "card-MSFT"
    assert payload["paper_decision"]["entry_price"] == 100.0
    assert "--preview" in payload["paper_decision"]["preview_command"]
    assert "--execute" in payload["paper_decision"]["execute_command"]
    assert [item["agent"] for item in payload["specialist_rationale"]] == [
        "Catalyst Analyst",
        "Skeptic",
        "Market Structure Analyst",
        "Portfolio Manager",
        "Execution Planner",
        "Risk Governor",
    ]


def test_blocks_agentic_paper_trade_intent_when_card_is_not_manual_review_ready() -> None:
    card = _decision_card(
        action_state=ActionState.WARNING.value,
        payload_overrides={
            "trade_plan": {
                "entry_zone": [101.0, 103.0],
                "invalidation_price": None,
                "reward_risk": None,
                "missing_fields": ["invalidation_price", "reward_risk"],
            },
            "hard_blocks": ["source_coverage_gap"],
        },
    )

    payload = build_agentic_paper_trade_intent(
        card,
        available_at=AVAILABLE_AT,
    ).to_payload()

    assert payload["status"] == "blocked"
    assert payload["recommended_paper_decision"] == "deferred"
    assert payload["requires_override_for_approval"] is True
    assert payload["hard_blocks"] == [
        "source_coverage_gap",
        "action_state_not_manual_review_eligible",
        "missing_trade_plan:invalidation_price",
        "missing_trade_plan:reward_risk",
    ]
    assert payload["paper_decision"]["entry_price"] is None
    assert "--decision deferred" in payload["paper_decision"]["preview_command"]
    assert "no broker order" in payload["next_action"].lower()


def _decision_card(
    *,
    action_state: str = ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW.value,
    payload_overrides: dict[str, object] | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "identity": {
            "action_state": action_state,
            "setup_type": "breakout_continuation",
        },
        "scores": {
            "final_score": 84.0,
            "reward_risk": 2.4,
        },
        "trade_plan": {
            "entry_zone": [99.0, 102.0],
            "invalidation_price": 94.0,
            "reward_risk": 2.4,
            "max_loss_if_wrong": 200.0,
            "missing_fields": [],
        },
        "position_sizing": {
            "shares": 20.0,
            "notional": 2000.0,
            "risk_per_trade_pct": 0.005,
        },
        "portfolio_impact": {
            "max_loss": 200.0,
            "hard_blocks": [],
        },
        "evidence": [
            {
                "title": "Cloud guidance raised",
                "summary": "Company raised cloud revenue guidance.",
            },
        ],
        "disconfirming_evidence": [
            {
                "title": "Risk penalty elevated",
                "summary": "Risk penalty remains non-zero.",
            },
        ],
        "controls": {
            "hard_blocks": [],
            "next_review_at": "2026-05-12T21:00:00+00:00",
        },
        "disclaimer": "Manual review only.",
        "audit": {
            "source": "test",
        },
    }
    if payload_overrides:
        payload.update(payload_overrides)
    return {
        "id": "card-MSFT",
        "ticker": "MSFT",
        "as_of": AS_OF,
        "source_ts": SOURCE_TS,
        "available_at": AVAILABLE_AT,
        "action_state": action_state,
        "setup_type": "breakout_continuation",
        "final_score": 84.0,
        "payload": payload,
    }

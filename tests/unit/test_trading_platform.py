from __future__ import annotations

from datetime import UTC, datetime

from catalyst_radar.core.config import AppConfig
from catalyst_radar.core.models import ActionState
from catalyst_radar.trading.platform import build_trading_platform_plan

AS_OF = datetime(2026, 5, 10, 21, tzinfo=UTC)
SOURCE_TS = datetime(2026, 5, 10, 20, 30, tzinfo=UTC)
AVAILABLE_AT = datetime(2026, 5, 10, 21, 5, tzinfo=UTC)


def test_builds_ready_supervised_paper_trading_plan() -> None:
    payload = build_trading_platform_plan(
        _decision_card(),
        available_at=AVAILABLE_AT,
        entry_price=100.0,
        config=AppConfig(portfolio_value=25_000.0, portfolio_cash=12_000.0),
    ).to_payload()

    assert payload["schema_version"] == "agentic-trading-platform-plan-v1"
    assert payload["status"] == "ready_for_paper_trade"
    assert payload["autonomy_level"] == "L2_paper_supervised"
    assert payload["decision_card_id"] == "card-MSFT"
    assert payload["ticker"] == "MSFT"
    assert payload["external_calls_required"] == 0
    assert payload["external_calls_made"] == 0
    assert payload["db_writes_required"] == 0
    assert payload["db_writes_made"] == 0
    assert payload["broker_order_submitted"] is False
    assert payload["order_submission_allowed"] is False
    assert payload["no_execution"] is True

    risk = payload["risk_approval"]
    assert risk["approved_for_paper_trade"] is True
    assert risk["approved_for_live_submission"] is False
    assert risk["paper_trade_blocks"] == []
    assert "broker_submission_disabled" in risk["live_submission_blocks"]
    assert risk["limits"]["risk_per_trade_pct"] == 0.005
    assert risk["limits"]["max_single_name_pct"] == 0.08

    order = payload["order_intent"]
    assert order["route"] == "paper_trade_only"
    assert order["side"] == "buy"
    assert order["quantity"] == 20
    assert order["limit_price"] == 100.0
    assert order["stop_price"] == 94.0
    assert order["submission_allowed"] is False
    assert order["broker_order_submitted"] is False
    assert order["disabled_order_preview"]["submission_allowed"] is False

    controls = payload["execution_controls"]
    assert controls["external_calls_required"] == 0
    assert controls["external_calls_made"] == 0
    assert controls["db_writes_required"] == 0
    assert controls["db_writes_made"] == 0
    assert controls["broker_order_submitted"] is False
    assert controls["order_submission_allowed"] is False
    assert controls["live_trading_kill_switch"] == "engaged"

    assert payload["agentic_paper_intent"]["status"] == "ready"
    assert payload["supervision"]["requires_manual_approval"] is True
    assert "--preview" in payload["supervision"]["paper_decision_preview_command"]
    assert "--execute" in payload["supervision"]["paper_decision_execute_command"]
    assert payload["capability_map"][-1]["status"] == "out_of_scope"


def test_blocks_trading_plan_when_decision_card_is_not_manual_review_ready() -> None:
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

    payload = build_trading_platform_plan(
        card,
        available_at=AVAILABLE_AT,
        entry_price=100.0,
        config=AppConfig(portfolio_value=25_000.0),
    ).to_payload()

    assert payload["status"] == "blocked"
    assert payload["autonomy_level"] == "L1_agentic_review"
    assert payload["risk_approval"]["approved_for_paper_trade"] is False
    assert payload["risk_approval"]["approved_for_live_submission"] is False
    assert payload["order_intent"]["submission_allowed"] is False
    assert payload["execution_controls"]["no_execution"] is True
    assert payload["agentic_paper_intent"]["recommended_paper_decision"] == "deferred"
    assert payload["risk_approval"]["paper_trade_blocks"] == [
        "source_coverage_gap",
        "action_state_not_manual_review_eligible",
        "missing_trade_plan:invalidation_price",
        "missing_trade_plan:reward_risk",
        "missing_order_intent:invalidation_price",
    ]


def test_uses_trade_plan_entry_zone_when_explicit_entry_price_is_missing() -> None:
    payload = build_trading_platform_plan(
        _decision_card(),
        available_at=AVAILABLE_AT,
        config=AppConfig(portfolio_value=25_000.0),
    ).to_payload()

    assert payload["status"] == "ready_for_paper_trade"
    assert payload["strategy_proposal"]["entry_price"] == 99.0
    assert payload["strategy_proposal"]["entry_price_source"] == "trade_plan_entry_zone_low"
    assert payload["order_intent"]["limit_price"] == 99.0
    assert "--entry-price 99" in payload["supervision"]["paper_decision_preview_command"]


def _decision_card(
    *,
    action_state: str = ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW.value,
    payload_overrides: dict[str, object] | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "identity": {
            "action_state": action_state,
            "setup_type": "breakout_continuation",
            "sector": "Technology",
            "theme": "Cloud AI",
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
            "target_price": 116.0,
            "time_stop_days": 20,
            "missing_fields": [],
        },
        "position_sizing": {
            "shares": 20.0,
            "notional": 2000.0,
            "risk_per_trade_pct": 0.005,
        },
        "portfolio_impact": {
            "single_name_after_pct": 4.0,
            "sector_after_pct": 14.0,
            "theme_after_pct": 6.0,
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
        "audit": {
            "source": "test",
            "candidate_packet_id": "packet-MSFT",
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
        "next_review_at": datetime(2026, 5, 12, 21, tzinfo=UTC),
        "action_state": action_state,
        "setup_type": "breakout_continuation",
        "final_score": 84.0,
        "payload": payload,
    }

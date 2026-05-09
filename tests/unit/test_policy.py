from dataclasses import replace
from datetime import UTC, datetime

from catalyst_radar.core.models import ActionState, MarketFeatures
from catalyst_radar.scoring.policy import POLICY_VERSION, evaluate_policy
from catalyst_radar.scoring.score import candidate_from_features


def test_stale_data_is_blocked() -> None:
    candidate = candidate_from_features(
        _features(),
        portfolio_penalty=0.0,
        data_stale=True,
        entry_zone=(100.0, 103.0),
        invalidation_price=94.0,
        reward_risk=2.5,
    )

    result = evaluate_policy(candidate)

    assert result.state == ActionState.BLOCKED
    assert "data_stale" in result.hard_blocks
    assert result.is_blocked is True


def test_mid_scores_are_added_to_watchlist() -> None:
    candidate = candidate_from_features(
        _features(ret_5d=0.03, ret_20d=0.04, rs_20_sector=58, rs_60_spy=57, ma_regime=60),
        portfolio_penalty=0.0,
        data_stale=False,
        entry_zone=(100.0, 103.0),
        invalidation_price=94.0,
        reward_risk=2.5,
    )

    result = evaluate_policy(candidate)

    assert result.state == ActionState.ADD_TO_WATCHLIST
    assert result.hard_blocks == ()


def test_policy_blocks_low_liquidity_even_with_high_score() -> None:
    candidate = candidate_from_features(
        _features(liquidity_score=49),
        portfolio_penalty=0.0,
        data_stale=False,
        entry_zone=(100.0, 103.0),
        invalidation_price=94.0,
        reward_risk=2.5,
    )

    result = evaluate_policy(candidate)

    assert candidate.final_score >= 85
    assert result.state == ActionState.BLOCKED
    assert "liquidity_hard_block" in result.hard_blocks


def test_policy_blocks_high_risk_penalty_even_with_high_score() -> None:
    candidate = candidate_from_features(
        _features(),
        portfolio_penalty=0.0,
        data_stale=False,
        entry_zone=(100.0, 103.0),
        invalidation_price=94.0,
        reward_risk=2.5,
    )
    candidate = replace(candidate, risk_penalty=20.0)

    result = evaluate_policy(candidate)

    assert candidate.final_score >= 85
    assert result.state == ActionState.BLOCKED
    assert "risk_penalty_hard_block" in result.hard_blocks


def test_policy_blocks_portfolio_penalty_at_plan_threshold() -> None:
    candidate = candidate_from_features(
        _features(),
        portfolio_penalty=20.0,
        data_stale=False,
        entry_zone=(100.0, 103.0),
        invalidation_price=94.0,
        reward_risk=2.5,
    )

    result = evaluate_policy(candidate)

    assert result.state == ActionState.BLOCKED
    assert "portfolio_hard_block" in result.hard_blocks


def test_policy_uses_exact_portfolio_hard_blocks_from_impact_metadata() -> None:
    candidate = candidate_from_features(
        _features(),
        portfolio_penalty=25.0,
        data_stale=False,
        entry_zone=(100.0, 103.0),
        invalidation_price=94.0,
        reward_risk=2.5,
        metadata={
            "portfolio_impact": {
                "hard_blocks": [
                    "single_name_exposure_hard_block",
                    "sector_exposure_hard_block",
                ]
            }
        },
    )

    result = evaluate_policy(candidate)

    assert result.state == ActionState.BLOCKED
    assert result.hard_blocks == (
        "single_name_exposure_hard_block",
        "sector_exposure_hard_block",
    )


def test_policy_requires_trade_plan_for_buy_review() -> None:
    candidate = candidate_from_features(
        _features(),
        portfolio_penalty=0.0,
        data_stale=False,
        entry_zone=None,
        invalidation_price=None,
        reward_risk=2.5,
    )

    result = evaluate_policy(candidate)

    assert result.state == ActionState.WARNING
    assert result.missing_trade_plan == (
        "entry_zone",
        "invalidation_price",
        "portfolio_impact_missing",
    )


def test_policy_keeps_low_reward_risk_candidate_in_warning() -> None:
    candidate = candidate_from_features(
        _features(),
        portfolio_penalty=0.0,
        data_stale=False,
        entry_zone=(100.0, 103.0),
        invalidation_price=94.0,
        reward_risk=1.5,
    )

    result = evaluate_policy(candidate)

    assert candidate.final_score >= 85
    assert result.state == ActionState.WARNING
    assert result.missing_trade_plan == ("reward_risk_too_low", "portfolio_impact_missing")


def test_policy_requires_portfolio_impact_for_buy_review() -> None:
    candidate = candidate_from_features(
        _features(),
        portfolio_penalty=0.0,
        data_stale=False,
        entry_zone=(100.0, 103.0),
        invalidation_price=94.0,
        reward_risk=2.5,
    )

    result = evaluate_policy(candidate)

    assert candidate.final_score >= 85
    assert result.state == ActionState.WARNING
    assert result.missing_trade_plan == ("portfolio_impact_missing",)


def test_policy_treats_chase_block_as_buy_review_blocker() -> None:
    candidate = candidate_from_features(
        _features(),
        portfolio_penalty=0.0,
        data_stale=False,
        entry_zone=(100.0, 103.0),
        invalidation_price=94.0,
        reward_risk=2.5,
        metadata={"portfolio_impact": {"hard_blocks": []}, "chase_block": True},
    )

    result = evaluate_policy(candidate)

    assert result.state == ActionState.WARNING
    assert result.hard_blocks == ()
    assert result.missing_trade_plan == ("chase_block",)


def test_policy_hard_blocks_chase_when_extension_is_extreme() -> None:
    candidate = candidate_from_features(
        _features(extension_20d=0.25),
        portfolio_penalty=0.0,
        data_stale=False,
        entry_zone=(100.0, 103.0),
        invalidation_price=94.0,
        reward_risk=2.5,
        metadata={"portfolio_impact": {"hard_blocks": []}, "chase_block": True},
    )

    result = evaluate_policy(candidate)

    assert result.state == ActionState.BLOCKED
    assert "chase_overextension_hard_block" in result.hard_blocks


def test_eligible_manual_buy_review_when_all_gates_pass() -> None:
    candidate = candidate_from_features(
        _features(),
        portfolio_penalty=0.0,
        data_stale=False,
        entry_zone=(100.0, 103.0),
        invalidation_price=94.0,
        reward_risk=2.5,
        metadata={"portfolio_impact": {"hard_blocks": []}},
    )

    result = evaluate_policy(candidate)

    assert POLICY_VERSION == "policy-v2-events"
    assert result.state == ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW
    assert result.hard_blocks == ()
    assert result.missing_trade_plan == ()


def test_event_conflict_downgrades_buy_review_candidate_to_research_only() -> None:
    candidate = candidate_from_features(
        _features(),
        portfolio_penalty=0.0,
        data_stale=False,
        entry_zone=(100.0, 103.0),
        invalidation_price=94.0,
        reward_risk=2.5,
        metadata={
            "portfolio_impact": {"hard_blocks": []},
            "has_event_conflict": True,
            "event_conflicts": [
                {
                    "ticker": "AAA",
                    "conflict_type": "guidance_direction_conflict",
                    "source_event_ids": ["raise", "cut"],
                }
            ],
        },
    )

    result = evaluate_policy(candidate)

    assert candidate.final_score >= 85
    assert result.state == ActionState.RESEARCH_ONLY
    assert result.reasons == ("event_conflict_requires_manual_resolution",)


def _features(
    *,
    ret_5d: float = 0.11,
    ret_20d: float = 0.22,
    rs_20_sector: float = 86,
    rs_60_spy: float = 82,
    ma_regime: float = 92,
    liquidity_score: float = 95,
    extension_20d: float = 0.07,
) -> MarketFeatures:
    as_of = datetime(2026, 5, 8, 21, tzinfo=UTC)
    return MarketFeatures(
        ticker="AAA",
        as_of=as_of,
        ret_5d=ret_5d,
        ret_20d=ret_20d,
        rs_20_sector=rs_20_sector,
        rs_60_spy=rs_60_spy,
        near_52w_high=0.98,
        ma_regime=ma_regime,
        rel_volume_5d=2.1,
        dollar_volume_z=2.0,
        atr_pct=0.035,
        extension_20d=extension_20d,
        liquidity_score=liquidity_score,
        feature_version="market-v1",
    )

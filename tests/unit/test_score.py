import math
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from catalyst_radar.core.models import ActionState, CandidateSnapshot, MarketFeatures
from catalyst_radar.scoring.policy import evaluate_policy
from catalyst_radar.scoring.score import candidate_from_features, score_market_features


def test_strong_market_features_score_as_buy_review_candidate() -> None:
    features = _strong_features()

    result = score_market_features(features, portfolio_penalty=3.0)

    assert result.final_score >= 72
    assert result.strong_pillars >= 3
    assert result.risk_penalty < 12


def test_candidate_from_features_preserves_score_and_trade_plan() -> None:
    features = _strong_features()

    candidate = candidate_from_features(
        features,
        portfolio_penalty=2.0,
        data_stale=False,
        entry_zone=(100.0, 104.0),
        invalidation_price=94.0,
        reward_risk=2.4,
    )

    assert isinstance(candidate, CandidateSnapshot)
    assert candidate.ticker == "AAA"
    assert candidate.final_score >= 72
    assert candidate.entry_zone == (100.0, 104.0)
    assert candidate.invalidation_price == 94.0
    assert candidate.reward_risk == 2.4
    assert candidate.metadata["policy_version_input"] == "score-v2-events"


def test_candidate_from_features_merges_extra_metadata() -> None:
    candidate = candidate_from_features(
        _strong_features(),
        portfolio_penalty=2.0,
        data_stale=False,
        entry_zone=(100.0, 104.0),
        invalidation_price=94.0,
        reward_risk=2.4,
        metadata={"setup_type": "breakout", "chase_block": False},
    )

    assert candidate.metadata["setup_type"] == "breakout"
    assert candidate.metadata["chase_block"] is False
    assert candidate.metadata["policy_version_input"] == "score-v2-events"
    assert "pillar_scores" in candidate.metadata


def test_candidate_from_features_keeps_score_owned_metadata_authoritative() -> None:
    candidate = candidate_from_features(
        _strong_features(),
        portfolio_penalty=2.0,
        data_stale=False,
        entry_zone=(100.0, 104.0),
        invalidation_price=94.0,
        reward_risk=2.4,
        metadata={"policy_version_input": "external", "pillar_scores": {"bad": 0}},
    )

    assert candidate.metadata["policy_version_input"] == "score-v2-events"
    assert "price_strength" in candidate.metadata["pillar_scores"]
    assert "bad" not in candidate.metadata["pillar_scores"]


def test_score_market_features_sanitizes_non_finite_inputs() -> None:
    features = replace(
        _strong_features(),
        ret_5d=float("nan"),
        ret_20d=float("inf"),
        rs_20_sector=float("-inf"),
        rs_60_spy=float("nan"),
        near_52w_high=float("inf"),
        ma_regime=float("nan"),
        rel_volume_5d=float("inf"),
        dollar_volume_z=float("-inf"),
        atr_pct=float("nan"),
        extension_20d=float("inf"),
        liquidity_score=float("nan"),
    )

    result = score_market_features(features, portfolio_penalty=float("nan"))

    assert math.isfinite(result.final_score)
    assert math.isfinite(result.risk_penalty)
    assert all(math.isfinite(score) for score in result.pillar_scores.values())
    assert result.final_score < 72


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("ret_5d", float("nan")),
        ("dollar_volume_z", float("inf")),
        ("atr_pct", float("-inf")),
        ("extension_20d", float("nan")),
        ("liquidity_score", float("inf")),
    ],
)
def test_score_market_features_fails_closed_for_any_non_finite_input(
    field_name: str,
    value: float,
) -> None:
    features = replace(_strong_features(), **{field_name: value})

    result = score_market_features(features, portfolio_penalty=0.0)

    assert math.isfinite(result.final_score)
    assert math.isfinite(result.risk_penalty)
    assert all(math.isfinite(score) for score in result.pillar_scores.values())
    assert result.final_score < 72
    assert result.strong_pillars == 0
    assert result.risk_penalty >= 20


def test_candidate_from_non_finite_features_is_blocked_by_policy() -> None:
    candidate = candidate_from_features(
        replace(_strong_features(), atr_pct=float("nan")),
        portfolio_penalty=0.0,
        data_stale=False,
        entry_zone=(100.0, 104.0),
        invalidation_price=94.0,
        reward_risk=2.4,
    )

    result = evaluate_policy(candidate)

    assert candidate.final_score < 72
    assert candidate.risk_penalty >= 20
    assert result.state == ActionState.BLOCKED
    assert "risk_penalty_hard_block" in result.hard_blocks


def test_event_support_is_bounded_and_cannot_override_stale_data_policy() -> None:
    candidate = candidate_from_features(
        _strong_features(),
        portfolio_penalty=0.0,
        data_stale=True,
        entry_zone=(100.0, 104.0),
        invalidation_price=94.0,
        reward_risk=2.4,
        event_support_score=100.0,
    )

    result = evaluate_policy(candidate)

    assert candidate.final_score <= 100.0
    assert candidate.metadata["event_support_score"] == 100.0
    assert candidate.metadata["event_bonus"] == 8.0
    assert result.state == ActionState.BLOCKED
    assert "data_stale" in result.hard_blocks


def _strong_features() -> MarketFeatures:
    as_of = datetime(2026, 5, 8, 21, tzinfo=UTC)
    return MarketFeatures(
        ticker="AAA",
        as_of=as_of,
        ret_5d=0.11,
        ret_20d=0.22,
        rs_20_sector=86,
        rs_60_spy=82,
        near_52w_high=0.98,
        ma_regime=92,
        rel_volume_5d=2.1,
        dollar_volume_z=2.0,
        atr_pct=0.035,
        extension_20d=0.07,
        liquidity_score=95,
        feature_version="market-v1",
    )

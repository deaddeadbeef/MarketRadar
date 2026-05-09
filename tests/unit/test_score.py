from datetime import UTC, datetime

from catalyst_radar.core.models import CandidateSnapshot, MarketFeatures
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
    assert candidate.metadata["policy_version_input"] == "score-v1"


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

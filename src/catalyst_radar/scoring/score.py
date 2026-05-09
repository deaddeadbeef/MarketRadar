from __future__ import annotations

from dataclasses import dataclass

from catalyst_radar.core.models import CandidateSnapshot, MarketFeatures

SCORE_VERSION = "score-v1"


@dataclass(frozen=True)
class ScoreResult:
    final_score: float
    strong_pillars: int
    risk_penalty: float
    pillar_scores: dict[str, float]


def score_market_features(features: MarketFeatures, portfolio_penalty: float) -> ScoreResult:
    pillar_scores = {
        "price_strength": _price_strength(features),
        "relative_strength": _relative_strength(features),
        "volume_liquidity": _volume_liquidity(features),
        "trend_quality": _trend_quality(features),
    }
    raw_score = sum(pillar_scores.values()) / len(pillar_scores)
    risk_penalty = _risk_penalty(features)
    final_score = _clamp(raw_score - risk_penalty - max(0.0, portfolio_penalty), 0, 100)
    strong_pillars = sum(score >= 70 for score in pillar_scores.values())
    return ScoreResult(
        final_score=round(final_score, 2),
        strong_pillars=strong_pillars,
        risk_penalty=round(risk_penalty, 2),
        pillar_scores={key: round(value, 2) for key, value in pillar_scores.items()},
    )


def candidate_from_features(
    features: MarketFeatures,
    portfolio_penalty: float,
    data_stale: bool,
    entry_zone: tuple[float, float] | None,
    invalidation_price: float | None,
    reward_risk: float,
) -> CandidateSnapshot:
    score = score_market_features(features, portfolio_penalty)
    return CandidateSnapshot(
        ticker=features.ticker,
        as_of=features.as_of,
        features=features,
        final_score=score.final_score,
        strong_pillars=score.strong_pillars,
        risk_penalty=score.risk_penalty,
        portfolio_penalty=max(0.0, float(portfolio_penalty)),
        data_stale=data_stale,
        entry_zone=entry_zone,
        invalidation_price=invalidation_price,
        reward_risk=reward_risk,
        metadata={
            "policy_version_input": SCORE_VERSION,
            "pillar_scores": score.pillar_scores,
        },
    )


def _price_strength(features: MarketFeatures) -> float:
    return _clamp((features.ret_20d * 250) + (features.ret_5d * 150) + 25, 0, 100)


def _relative_strength(features: MarketFeatures) -> float:
    return _clamp((features.rs_20_sector * 0.55) + (features.rs_60_spy * 0.45), 0, 100)


def _volume_liquidity(features: MarketFeatures) -> float:
    volume_score = min(features.rel_volume_5d / 2.0, 1.0) * 55
    z_score = _clamp((features.dollar_volume_z + 1.0) * 12.5, 0, 25)
    liquidity_score = _clamp(features.liquidity_score, 0, 100) * 0.20
    return _clamp(volume_score + z_score + liquidity_score, 0, 100)


def _trend_quality(features: MarketFeatures) -> float:
    near_high_score = features.near_52w_high * 100
    return _clamp((features.ma_regime * 0.55) + (near_high_score * 0.45), 0, 100)


def _risk_penalty(features: MarketFeatures) -> float:
    volatility_penalty = max(0.0, features.atr_pct - 0.04) * 250
    extension_penalty = max(0.0, features.extension_20d - 0.10) * 120
    liquidity_penalty = max(0.0, 40 - features.liquidity_score) * 0.15
    return _clamp(volatility_penalty + extension_penalty + liquidity_penalty, 0, 30)


def _clamp(value: float, minimum: float, maximum: float) -> float:
    return float(max(minimum, min(maximum, value)))

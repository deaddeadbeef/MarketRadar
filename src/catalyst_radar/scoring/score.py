from __future__ import annotations

import math
from dataclasses import dataclass

from catalyst_radar.core.models import CandidateSnapshot, MarketFeatures

SCORE_VERSION = "score-v1"
_PILLAR_NAMES = (
    "price_strength",
    "relative_strength",
    "volume_liquidity",
    "trend_quality",
)
_NUMERIC_FEATURE_FIELDS = (
    "ret_5d",
    "ret_20d",
    "rs_20_sector",
    "rs_60_spy",
    "near_52w_high",
    "ma_regime",
    "rel_volume_5d",
    "dollar_volume_z",
    "atr_pct",
    "extension_20d",
    "liquidity_score",
)


@dataclass(frozen=True)
class ScoreResult:
    final_score: float
    strong_pillars: int
    risk_penalty: float
    pillar_scores: dict[str, float]


def score_market_features(features: MarketFeatures, portfolio_penalty: float) -> ScoreResult:
    if _has_non_finite_feature_input(features):
        return _fail_closed_score()

    pillar_scores = {
        "price_strength": _price_strength(features),
        "relative_strength": _relative_strength(features),
        "volume_liquidity": _volume_liquidity(features),
        "trend_quality": _trend_quality(features),
    }
    raw_score = sum(pillar_scores.values()) / len(pillar_scores)
    risk_penalty = _risk_penalty(features)
    final_score = _clamp(raw_score - risk_penalty - _non_negative(portfolio_penalty), 0, 100)
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
        portfolio_penalty=_non_negative(portfolio_penalty),
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
    return _clamp(
        (_finite(features.ret_20d) * 250) + (_finite(features.ret_5d) * 150) + 25,
        0,
        100,
    )


def _relative_strength(features: MarketFeatures) -> float:
    return _clamp(
        (_finite(features.rs_20_sector) * 0.55) + (_finite(features.rs_60_spy) * 0.45),
        0,
        100,
    )


def _volume_liquidity(features: MarketFeatures) -> float:
    volume_score = min(_non_negative(features.rel_volume_5d) / 2.0, 1.0) * 55
    z_score = _clamp((_finite(features.dollar_volume_z) + 1.0) * 12.5, 0, 25)
    liquidity_score = _clamp(_finite(features.liquidity_score), 0, 100) * 0.20
    return _clamp(volume_score + z_score + liquidity_score, 0, 100)


def _trend_quality(features: MarketFeatures) -> float:
    near_high_score = _finite(features.near_52w_high) * 100
    return _clamp((_finite(features.ma_regime) * 0.55) + (near_high_score * 0.45), 0, 100)


def _risk_penalty(features: MarketFeatures) -> float:
    volatility_penalty = max(0.0, _finite(features.atr_pct) - 0.04) * 250
    extension_penalty = max(0.0, _finite(features.extension_20d) - 0.10) * 120
    liquidity_penalty = max(0.0, 40 - _finite(features.liquidity_score)) * 0.15
    return _clamp(volatility_penalty + extension_penalty + liquidity_penalty, 0, 30)


def _clamp(value: float, minimum: float, maximum: float) -> float:
    return float(max(minimum, min(maximum, _finite(value))))


def _finite(value: float) -> float:
    result = float(value)
    if not math.isfinite(result):
        return 0.0
    return result


def _non_negative(value: float) -> float:
    return max(0.0, _finite(value))


def _has_non_finite_feature_input(features: MarketFeatures) -> bool:
    return any(
        not math.isfinite(float(getattr(features, field)))
        for field in _NUMERIC_FEATURE_FIELDS
    )


def _fail_closed_score() -> ScoreResult:
    return ScoreResult(
        final_score=0.0,
        strong_pillars=0,
        risk_penalty=100.0,
        pillar_scores={name: 0.0 for name in _PILLAR_NAMES},
    )

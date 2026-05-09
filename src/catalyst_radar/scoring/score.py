from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from catalyst_radar.core.models import CandidateSnapshot, MarketFeatures

SCORE_VERSION = "score-v4-options-theme"
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
    event_bonus: float
    local_narrative_bonus: float
    options_bonus: float
    sector_theme_bonus: float
    options_risk_penalty: float
    pillar_scores: dict[str, float]


def score_market_features(
    features: MarketFeatures,
    portfolio_penalty: float,
    event_support_score: float = 0.0,
    local_narrative_score: float = 0.0,
    options_flow_score: float = 0.0,
    options_risk_score: float = 0.0,
    sector_rotation_score: float = 0.0,
    theme_velocity_score: float = 0.0,
    peer_readthrough_score: float = 0.0,
) -> ScoreResult:
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
    event_bonus = _event_bonus(event_support_score)
    local_narrative_bonus = _local_narrative_bonus(local_narrative_score)
    options_bonus = _options_bonus(options_flow_score)
    sector_theme_bonus = _sector_theme_bonus(
        sector_rotation_score=sector_rotation_score,
        theme_velocity_score=theme_velocity_score,
        peer_readthrough_score=peer_readthrough_score,
    )
    options_risk_penalty = _options_risk_penalty(options_risk_score)
    final_score = _clamp(
        raw_score
        + event_bonus
        + local_narrative_bonus
        + options_bonus
        + sector_theme_bonus
        - risk_penalty
        - options_risk_penalty
        - _non_negative(portfolio_penalty),
        0,
        100,
    )
    strong_pillars = sum(score >= 70 for score in pillar_scores.values())
    return ScoreResult(
        final_score=round(final_score, 2),
        strong_pillars=strong_pillars,
        risk_penalty=round(risk_penalty, 2),
        event_bonus=round(event_bonus, 2),
        local_narrative_bonus=round(local_narrative_bonus, 2),
        options_bonus=round(options_bonus, 2),
        sector_theme_bonus=round(sector_theme_bonus, 2),
        options_risk_penalty=round(options_risk_penalty, 2),
        pillar_scores={key: round(value, 2) for key, value in pillar_scores.items()},
    )


def candidate_from_features(
    features: MarketFeatures,
    portfolio_penalty: float,
    data_stale: bool,
    entry_zone: tuple[float, float] | None,
    invalidation_price: float | None,
    reward_risk: float,
    metadata: Mapping[str, Any] | None = None,
    event_support_score: float = 0.0,
    local_narrative_score: float = 0.0,
    options_flow_score: float = 0.0,
    options_risk_score: float = 0.0,
    sector_rotation_score: float = 0.0,
    theme_velocity_score: float = 0.0,
    peer_readthrough_score: float = 0.0,
) -> CandidateSnapshot:
    score = score_market_features(
        features=features,
        portfolio_penalty=portfolio_penalty,
        event_support_score=event_support_score,
        local_narrative_score=local_narrative_score,
        options_flow_score=options_flow_score,
        options_risk_score=options_risk_score,
        sector_rotation_score=sector_rotation_score,
        theme_velocity_score=theme_velocity_score,
        peer_readthrough_score=peer_readthrough_score,
    )
    candidate_metadata = dict(metadata or {})
    candidate_metadata.update(
        {
            "policy_version_input": SCORE_VERSION,
            "pillar_scores": score.pillar_scores,
            "event_support_score": _non_negative(event_support_score),
            "event_bonus": score.event_bonus,
            "local_narrative_score": _non_negative(local_narrative_score),
            "local_narrative_bonus": score.local_narrative_bonus,
            "options_flow_score": _non_negative(options_flow_score),
            "options_bonus": score.options_bonus,
            "options_risk_score": _non_negative(options_risk_score),
            "options_risk_penalty": score.options_risk_penalty,
            "sector_rotation_score": _non_negative(sector_rotation_score),
            "theme_velocity_score": _non_negative(theme_velocity_score),
            "peer_readthrough_score": _non_negative(peer_readthrough_score),
            "sector_theme_bonus": score.sector_theme_bonus,
        }
    )
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
        metadata=candidate_metadata,
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


def _event_bonus(event_support_score: float) -> float:
    return min(8.0, _non_negative(event_support_score) * 0.08)


def _local_narrative_bonus(local_narrative_score: float) -> float:
    return min(6.0, _non_negative(local_narrative_score) * 0.06)


def _options_bonus(options_flow_score: float) -> float:
    return min(4.0, _non_negative(options_flow_score) * 0.04)


def _sector_theme_bonus(
    *,
    sector_rotation_score: float,
    theme_velocity_score: float,
    peer_readthrough_score: float,
) -> float:
    return min(
        6.0,
        (_non_negative(sector_rotation_score) * 0.02)
        + (_non_negative(theme_velocity_score) * 0.02)
        + (_non_negative(peer_readthrough_score) * 0.02),
    )


def _options_risk_penalty(options_risk_score: float) -> float:
    return min(4.0, _non_negative(options_risk_score) * 0.04)


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
        event_bonus=0.0,
        local_narrative_bonus=0.0,
        options_bonus=0.0,
        sector_theme_bonus=0.0,
        options_risk_penalty=0.0,
        pillar_scores={name: 0.0 for name in _PILLAR_NAMES},
    )

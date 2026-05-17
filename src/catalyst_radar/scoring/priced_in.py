from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from catalyst_radar.core.models import MarketFeatures

PRICED_IN_VERSION = "priced-in-v1"


@dataclass(frozen=True)
class PricedInResult:
    status: str
    direction: str
    emotion_score: float
    reaction_score: float
    opposite_reaction_score: float
    emotion_reaction_gap: float
    priced_in_score: float
    reason: str
    next_step: str
    evidence: Mapping[str, object]
    schema_version: str = PRICED_IN_VERSION

    def as_payload(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "status": self.status,
            "direction": self.direction,
            "emotion_score": self.emotion_score,
            "reaction_score": self.reaction_score,
            "opposite_reaction_score": self.opposite_reaction_score,
            "emotion_reaction_gap": self.emotion_reaction_gap,
            "priced_in_score": self.priced_in_score,
            "reason": self.reason,
            "next_step": self.next_step,
            "evidence": dict(self.evidence),
        }


def evaluate_priced_in(
    features: MarketFeatures,
    metadata: Mapping[str, Any],
    *,
    data_stale: bool = False,
    hard_blocks: Sequence[str] = (),
) -> PricedInResult:
    """Compare catalyst emotion with price reaction using local scan features only."""
    sentiment = _finite(metadata.get("sentiment_score"))
    direction = _emotion_direction(sentiment, features)
    direction_label = "bearish" if direction < 0 else "bullish"
    emotion_score = _emotion_score(metadata)
    reaction_score = _reaction_score(features, direction=direction)
    opposite_reaction_score = _reaction_score(features, direction=-direction)
    gap = emotion_score - reaction_score
    priced_in_score = _priced_in_score(emotion_score, reaction_score)
    has_conflict = bool(metadata.get("has_event_conflict"))

    status = _status(
        emotion_score=emotion_score,
        reaction_score=reaction_score,
        opposite_reaction_score=opposite_reaction_score,
        gap=gap,
        direction=direction,
        data_stale=data_stale,
        hard_blocks=hard_blocks,
        has_conflict=has_conflict,
    )
    return PricedInResult(
        status=status,
        direction=direction_label,
        emotion_score=round(emotion_score, 2),
        reaction_score=round(reaction_score, 2),
        opposite_reaction_score=round(opposite_reaction_score, 2),
        emotion_reaction_gap=round(gap, 2),
        priced_in_score=round(priced_in_score, 2),
        reason=_reason(status, direction_label, emotion_score, reaction_score, gap),
        next_step=_next_step(status),
        evidence={
            "event_support_score": round(_finite(metadata.get("event_support_score")), 2),
            "material_event_count": int(max(0.0, _finite(metadata.get("material_event_count")))),
            "local_narrative_score": round(_finite(metadata.get("local_narrative_score")), 2),
            "sentiment_score": round(sentiment, 2),
            "novelty_score": round(_finite(metadata.get("novelty_score")), 2),
            "source_quality_score": round(_finite(metadata.get("source_quality_score")), 2),
            "theme_match_score": round(_finite(metadata.get("theme_match_score")), 2),
            "options_flow_score": round(_finite(metadata.get("options_flow_score")), 2),
            "theme_velocity_score": round(_finite(metadata.get("theme_velocity_score")), 2),
            "ret_5d_pct": round(features.ret_5d * 100, 2),
            "ret_20d_pct": round(features.ret_20d * 100, 2),
            "rs_20_sector": round(_finite(features.rs_20_sector), 2),
            "rs_60_spy": round(_finite(features.rs_60_spy), 2),
            "rel_volume_5d": round(_finite(features.rel_volume_5d), 2),
            "dollar_volume_z": round(_finite(features.dollar_volume_z), 2),
            "extension_20d_pct": round(features.extension_20d * 100, 2),
            "data_stale": bool(data_stale),
            "hard_blocks": list(hard_blocks),
            "has_event_conflict": has_conflict,
        },
    )


def _emotion_score(metadata: Mapping[str, Any]) -> float:
    event_support = _finite(metadata.get("event_support_score"))
    event_count_bonus = min(12.0, _finite(metadata.get("material_event_count")) * 6.0)
    local_narrative = _finite(metadata.get("local_narrative_score"))
    sentiment_strength = min(100.0, abs(_finite(metadata.get("sentiment_score"))))
    novelty = _finite(metadata.get("novelty_score"))
    source_quality = _finite(metadata.get("source_quality_score"))
    theme_match = _finite(metadata.get("theme_match_score"))
    options_flow = _finite(metadata.get("options_flow_score"))
    theme_velocity = _finite(metadata.get("theme_velocity_score"))

    score = (
        event_support * 0.24
        + local_narrative * 0.24
        + sentiment_strength * 0.14
        + novelty * 0.10
        + source_quality * 0.10
        + theme_match * 0.07
        + options_flow * 0.06
        + theme_velocity * 0.05
        + event_count_bonus
    )
    return _clamp(score, 0.0, 100.0)


def _reaction_score(features: MarketFeatures, *, direction: int) -> float:
    directional_5d = direction * _finite(features.ret_5d)
    directional_20d = direction * _finite(features.ret_20d)
    relative_20 = direction * (_finite(features.rs_20_sector) - 50.0)
    relative_60 = direction * (_finite(features.rs_60_spy) - 50.0)
    extension = direction * _finite(features.extension_20d)

    return_score = _clamp((directional_5d * 280.0) + (directional_20d * 170.0), 0.0, 55.0)
    relative_score = _clamp((relative_20 * 0.75) + (relative_60 * 0.45), 0.0, 30.0)
    volume_score = _clamp(
        max(0.0, _finite(features.rel_volume_5d) - 1.0) * 8.0
        + max(0.0, _finite(features.dollar_volume_z)) * 3.0,
        0.0,
        15.0,
    )
    extension_score = _clamp(extension * 120.0, 0.0, 10.0)
    return _clamp(return_score + relative_score + volume_score + extension_score, 0.0, 100.0)


def _emotion_direction(sentiment: float, features: MarketFeatures) -> int:
    if sentiment <= -15.0:
        return -1
    if sentiment >= 15.0:
        return 1
    if features.ret_20d < -0.05 and features.rs_20_sector < 45:
        return -1
    return 1


def _priced_in_score(emotion_score: float, reaction_score: float) -> float:
    if emotion_score < 1.0:
        return 100.0 if reaction_score >= 60.0 else 0.0
    return _clamp((reaction_score / emotion_score) * 100.0, 0.0, 100.0)


def _status(
    *,
    emotion_score: float,
    reaction_score: float,
    opposite_reaction_score: float,
    gap: float,
    direction: int,
    data_stale: bool,
    hard_blocks: Sequence[str],
    has_conflict: bool,
) -> str:
    if data_stale:
        return "stale"
    if hard_blocks:
        return "blocked"
    if has_conflict or (emotion_score >= 45.0 and opposite_reaction_score >= 45.0):
        return "conflicted"
    if emotion_score < 25.0 and reaction_score < 30.0:
        return "neutral"
    if reaction_score >= 55.0 and reaction_score >= emotion_score + 20.0:
        return "overextended_hype"
    if emotion_score >= 55.0 and gap >= 20.0:
        return "bearish_not_priced_in" if direction < 0 else "bullish_not_priced_in"
    if emotion_score >= 45.0 and gap >= 10.0:
        return "bearish_not_priced_in" if direction < 0 else "bullish_not_priced_in"
    if emotion_score >= 45.0 and reaction_score >= max(40.0, emotion_score - 10.0):
        return "fully_priced"
    if reaction_score >= 60.0 and emotion_score < 45.0:
        return "overextended_hype"
    return "neutral"


def _reason(
    status: str,
    direction_label: str,
    emotion_score: float,
    reaction_score: float,
    gap: float,
) -> str:
    if status == "stale":
        return "Market bars are stale, so the priced-in read is not reliable yet."
    if status == "blocked":
        return "Policy or portfolio blocks exist; resolve them before using the signal."
    if status == "conflicted":
        return (
            f"{direction_label.capitalize()} evidence conflicts with price action or sources; "
            "do not escalate until the conflict is explained."
        )
    if status in {"bullish_not_priced_in", "bearish_not_priced_in"}:
        return (
            f"{direction_label.capitalize()} emotion {emotion_score:.0f} is ahead of "
            f"price reaction {reaction_score:.0f} by {gap:.0f} points."
        )
    if status == "fully_priced":
        return (
            f"Price reaction {reaction_score:.0f} has mostly caught up to "
            f"{direction_label} emotion {emotion_score:.0f}."
        )
    if status == "overextended_hype":
        return (
            f"Price reaction {reaction_score:.0f} is stronger than "
            f"supporting emotion {emotion_score:.0f}; chase risk is high."
        )
    return "No clear emotion-versus-price mismatch is visible from current local inputs."


def _next_step(status: str) -> str:
    if status in {"bullish_not_priced_in", "bearish_not_priced_in"}:
        return "Open the candidate, verify source links, then refresh or build the Decision Card."
    if status == "fully_priced":
        return "Treat as watchlist unless new evidence appears or the setup resets."
    if status == "overextended_hype":
        return "Check disconfirming evidence and avoid chasing without a fresh primary source."
    if status == "conflicted":
        return "Resolve the source or price-action conflict before escalation."
    if status == "stale":
        return "Refresh latest market bars, then re-run the radar."
    if status == "blocked":
        return "Clear readiness, policy, or portfolio blocks before using this signal."
    return "Monitor; no useful priced-in mismatch is visible yet."


def _finite(value: object) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(result):
        return 0.0
    return result


def _clamp(value: float, minimum: float, maximum: float) -> float:
    return float(max(minimum, min(maximum, _finite(value))))

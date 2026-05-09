from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from catalyst_radar.core.models import ActionState, CandidateSnapshot, PolicyResult

POLICY_VERSION = "policy-v1"

MIN_BUY_REVIEW_SCORE = 72
MIN_ELIGIBLE_BUY_REVIEW_SCORE = 85
MIN_BUY_REVIEW_PILLARS = 3
MIN_REWARD_RISK = 2.0
MIN_WATCHLIST_SCORE = 60
MIN_RESEARCH_ONLY_SCORE = 50
CHASE_HARD_BLOCK_EXTENSION = 0.20


def evaluate_policy(candidate: CandidateSnapshot) -> PolicyResult:
    hard_blocks = []
    reasons = []

    if candidate.data_stale:
        hard_blocks.append("data_stale")
        reasons.append("candidate data is stale")

    portfolio_blocks = _portfolio_hard_blocks(candidate)
    if portfolio_blocks:
        hard_blocks.extend(portfolio_blocks)
        reasons.append("portfolio impact exceeds hard policy limits")
    elif candidate.portfolio_penalty >= 20:
        hard_blocks.append("portfolio_hard_block")
        reasons.append("portfolio impact exceeds hard policy limits")

    if candidate.features.liquidity_score < 50:
        hard_blocks.append("liquidity_hard_block")
        reasons.append("liquidity score below policy floor")

    if candidate.risk_penalty >= 20:
        hard_blocks.append("risk_penalty_hard_block")
        reasons.append("risk penalty exceeds policy ceiling")

    if _chase_block(candidate) and candidate.features.extension_20d >= CHASE_HARD_BLOCK_EXTENSION:
        hard_blocks.append("chase_overextension_hard_block")
        reasons.append("setup is extended beyond chase risk ceiling")

    if hard_blocks:
        return PolicyResult(
            state=ActionState.BLOCKED,
            hard_blocks=tuple(hard_blocks),
            reasons=tuple(reasons),
        )

    missing_trade_plan = _manual_review_blockers(candidate)
    eligible_for_buy_review = (
        candidate.final_score >= MIN_ELIGIBLE_BUY_REVIEW_SCORE
        and candidate.strong_pillars >= MIN_BUY_REVIEW_PILLARS
        and not missing_trade_plan
    )
    if eligible_for_buy_review:
        return PolicyResult(
            state=ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW,
            reasons=("all_buy_review_gates_passed",),
        )

    if candidate.final_score >= MIN_BUY_REVIEW_SCORE:
        return PolicyResult(
            state=ActionState.WARNING,
            reasons=_warning_reasons(missing_trade_plan),
            missing_trade_plan=missing_trade_plan,
        )

    if candidate.final_score >= MIN_WATCHLIST_SCORE:
        return PolicyResult(
            state=ActionState.ADD_TO_WATCHLIST,
            reasons=("score_above_watchlist_floor",),
            missing_trade_plan=missing_trade_plan,
        )

    if candidate.final_score >= MIN_RESEARCH_ONLY_SCORE:
        return PolicyResult(
            state=ActionState.RESEARCH_ONLY,
            reasons=("score_above_research_floor",),
        )

    return PolicyResult(state=ActionState.NO_ACTION, reasons=("score_below_research_floor",))


def _missing_trade_plan(candidate: CandidateSnapshot) -> tuple[str, ...]:
    missing = []
    if candidate.entry_zone is None:
        missing.append("entry_zone")
    if candidate.invalidation_price is None:
        missing.append("invalidation_price")
    if candidate.reward_risk <= 0:
        missing.append("reward_risk")
    elif candidate.reward_risk < MIN_REWARD_RISK:
        missing.append("reward_risk_too_low")
    return tuple(missing)


def _manual_review_blockers(candidate: CandidateSnapshot) -> tuple[str, ...]:
    missing = list(_missing_trade_plan(candidate))
    if (
        candidate.final_score >= MIN_ELIGIBLE_BUY_REVIEW_SCORE
        and candidate.strong_pillars >= MIN_BUY_REVIEW_PILLARS
    ):
        if _portfolio_impact(candidate) is None:
            missing.append("portfolio_impact_missing")
        if _chase_block(candidate):
            missing.append("chase_block")
    return tuple(missing)


def _warning_reasons(missing_trade_plan: tuple[str, ...]) -> tuple[str, ...]:
    if missing_trade_plan:
        return ("trade_plan_required",)
    return ("score_requires_manual_review",)


def _portfolio_impact(candidate: CandidateSnapshot) -> Mapping[str, Any] | None:
    impact = candidate.metadata.get("portfolio_impact")
    if isinstance(impact, Mapping):
        return impact
    return None


def _portfolio_hard_blocks(candidate: CandidateSnapshot) -> list[str]:
    impact = _portfolio_impact(candidate)
    if impact is None:
        return []
    hard_blocks = impact.get("hard_blocks", ())
    if not isinstance(hard_blocks, (list, tuple)):
        return []
    return [str(block) for block in hard_blocks if str(block)]


def _chase_block(candidate: CandidateSnapshot) -> bool:
    return candidate.metadata.get("chase_block") is True

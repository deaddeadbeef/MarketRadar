from __future__ import annotations

from collections.abc import Sequence

from catalyst_radar.core.models import DailyBar, MarketFeatures
from catalyst_radar.scoring.setups import SetupPlan, SetupType

MIN_REWARD_RISK = 2.0
CHASE_EXTENSION_THRESHOLD = 0.12


def breakout_plan(bars: Sequence[DailyBar], features: MarketFeatures) -> SetupPlan:
    latest = _latest_bar(bars)
    if latest is None or latest.close <= 0:
        return _empty_plan(SetupType.BREAKOUT, "missing_valid_price_data")

    recent_high = max(bar.high for bar in bars[-60:])
    entry_high = max(latest.close, recent_high)
    entry_zone = (round(latest.close * 0.99, 2), round(entry_high * 1.01, 2))
    invalidation_price = _invalidation_price(latest.close, features.atr_pct, floor_pct=0.06)
    target_price, reward_risk = _target_and_reward_risk(
        entry_price=latest.close,
        invalidation_price=invalidation_price,
        target_pct=_target_pct(features.ret_20d, minimum=0.14, maximum=0.22),
    )
    chase_block = _is_chasing(latest.close, recent_high, features.extension_20d)
    return SetupPlan(
        setup_type=SetupType.BREAKOUT,
        entry_zone=entry_zone,
        invalidation_price=invalidation_price,
        target_price=target_price,
        reward_risk=reward_risk,
        chase_block=chase_block,
        reasons=tuple(
            reason
            for reason in (
                "near_52w_high_breakout",
                "relative_strength_confirmed" if features.rs_60_spy >= 70 else "",
                "chase_block" if chase_block else "",
            )
            if reason
        ),
        metadata={
            "recent_high": round(recent_high, 2),
            "extension_20d": round(features.extension_20d, 4),
        },
    )


def pullback_plan(bars: Sequence[DailyBar], features: MarketFeatures) -> SetupPlan:
    latest = _latest_bar(bars)
    if latest is None or latest.close <= 0:
        return _empty_plan(SetupType.PULLBACK, "missing_valid_price_data")

    recent_low = min(bar.low for bar in bars[-20:])
    entry_zone = (round(latest.close * 0.97, 2), round(latest.close * 1.00, 2))
    invalidation_price = round(min(recent_low * 0.98, latest.close * 0.94), 2)
    target_price, reward_risk = _target_and_reward_risk(
        entry_price=latest.close,
        invalidation_price=invalidation_price,
        target_pct=_target_pct(features.ret_20d, minimum=0.10, maximum=0.18),
    )
    chase_block = features.extension_20d > CHASE_EXTENSION_THRESHOLD
    return SetupPlan(
        setup_type=SetupType.PULLBACK,
        entry_zone=entry_zone,
        invalidation_price=invalidation_price,
        target_price=target_price,
        reward_risk=reward_risk,
        chase_block=chase_block,
        reasons=tuple(
            reason
            for reason in (
                "trend_pullback",
                "above_moving_average_regime" if features.ma_regime >= 60 else "",
                "chase_block" if chase_block else "",
            )
            if reason
        ),
        metadata={"recent_low": round(recent_low, 2)},
    )


def post_earnings_plan(bars: Sequence[DailyBar], features: MarketFeatures) -> SetupPlan:
    return _event_placeholder_plan(
        bars,
        features,
        setup_type=SetupType.POST_EARNINGS,
        reason="post_earnings_requires_event_ingest",
    )


def sector_rotation_plan(bars: Sequence[DailyBar], features: MarketFeatures) -> SetupPlan:
    latest = _latest_bar(bars)
    if latest is None or latest.close <= 0:
        return _empty_plan(SetupType.SECTOR_ROTATION, "missing_valid_price_data")

    entry_zone = (round(latest.close * 0.98, 2), round(latest.close * 1.01, 2))
    invalidation_price = _invalidation_price(latest.close, features.atr_pct, floor_pct=0.055)
    target_price, reward_risk = _target_and_reward_risk(
        entry_price=latest.close,
        invalidation_price=invalidation_price,
        target_pct=_target_pct(features.ret_20d, minimum=0.11, maximum=0.19),
    )
    return SetupPlan(
        setup_type=SetupType.SECTOR_ROTATION,
        entry_zone=entry_zone,
        invalidation_price=invalidation_price,
        target_price=target_price,
        reward_risk=reward_risk,
        chase_block=_is_chasing(latest.close, latest.high, features.extension_20d),
        reasons=("sector_relative_strength", "rotation_without_event_dependency"),
        metadata={"rs_20_sector": round(features.rs_20_sector, 2)},
    )


def filings_catalyst_plan(bars: Sequence[DailyBar], features: MarketFeatures) -> SetupPlan:
    return _event_placeholder_plan(
        bars,
        features,
        setup_type=SetupType.FILINGS_CATALYST,
        reason="filings_catalyst_requires_event_ingest",
    )


def select_setup_plan(bars: Sequence[DailyBar], features: MarketFeatures) -> SetupPlan:
    if features.near_52w_high >= 0.90 and features.rs_60_spy >= 70:
        return breakout_plan(bars, features)
    if features.rs_20_sector >= 75 and features.ret_20d > 0:
        return sector_rotation_plan(bars, features)
    if features.ma_regime >= 60 and features.ret_20d > 0:
        return pullback_plan(bars, features)
    return _market_momentum_plan(bars, features)


def _market_momentum_plan(bars: Sequence[DailyBar], features: MarketFeatures) -> SetupPlan:
    latest = _latest_bar(bars)
    if latest is None or latest.close <= 0:
        return _empty_plan(SetupType.MARKET_MOMENTUM, "missing_valid_price_data")
    entry_zone = (round(latest.close * 0.98, 2), round(latest.close * 1.02, 2))
    invalidation_price = _invalidation_price(latest.close, features.atr_pct, floor_pct=0.08)
    target_price, reward_risk = _target_and_reward_risk(
        entry_price=latest.close,
        invalidation_price=invalidation_price,
        target_pct=_target_pct(features.ret_20d, minimum=0.10, maximum=0.16),
    )
    return SetupPlan(
        setup_type=SetupType.MARKET_MOMENTUM,
        entry_zone=entry_zone,
        invalidation_price=invalidation_price,
        target_price=target_price,
        reward_risk=reward_risk,
        chase_block=_is_chasing(latest.close, latest.high, features.extension_20d),
        reasons=("market_momentum_default",),
        metadata={},
    )


def _event_placeholder_plan(
    bars: Sequence[DailyBar],
    features: MarketFeatures,
    *,
    setup_type: SetupType,
    reason: str,
) -> SetupPlan:
    base = _market_momentum_plan(bars, features)
    return SetupPlan(
        setup_type=setup_type,
        entry_zone=base.entry_zone,
        invalidation_price=base.invalidation_price,
        target_price=base.target_price,
        reward_risk=base.reward_risk,
        chase_block=base.chase_block,
        reasons=(reason, "not_selected_without_event_evidence"),
        metadata={"placeholder": True, "event_confirmed": False},
    )


def _empty_plan(setup_type: SetupType, reason: str) -> SetupPlan:
    return SetupPlan(
        setup_type=setup_type,
        entry_zone=None,
        invalidation_price=None,
        target_price=None,
        reward_risk=0.0,
        chase_block=False,
        reasons=(reason,),
        metadata={},
    )


def _latest_bar(bars: Sequence[DailyBar]) -> DailyBar | None:
    if not bars:
        return None
    return bars[-1]


def _invalidation_price(close: float, atr_pct: float, *, floor_pct: float) -> float:
    atr_buffer = max(float(atr_pct), 0.0) * 2.0
    invalidation_pct = max(floor_pct, min(0.15, atr_buffer))
    return round(close * (1 - invalidation_pct), 2)


def _target_and_reward_risk(
    *,
    entry_price: float,
    invalidation_price: float,
    target_pct: float,
) -> tuple[float, float]:
    downside = entry_price - invalidation_price
    if downside <= 0:
        return round(entry_price, 2), 0.0
    target_price = entry_price * (1 + target_pct)
    reward_risk = (target_price - entry_price) / downside
    return round(target_price, 2), round(reward_risk, 2)


def _target_pct(ret_20d: float, *, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, float(ret_20d)))


def _is_chasing(close: float, recent_high: float, extension_20d: float) -> bool:
    if extension_20d > CHASE_EXTENSION_THRESHOLD:
        return True
    if recent_high <= 0:
        return False
    return close > recent_high * 1.04

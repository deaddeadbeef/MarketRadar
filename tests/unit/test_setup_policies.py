from datetime import UTC, date, datetime

from catalyst_radar.core.models import DailyBar, MarketFeatures
from catalyst_radar.scoring.setup_policies import (
    breakout_plan,
    filings_catalyst_plan,
    post_earnings_plan,
    select_setup_plan,
)
from catalyst_radar.scoring.setups import SetupType


def test_breakout_near_highs_generates_entry_and_invalidation() -> None:
    plan = breakout_plan(_bars(), _features())

    assert plan.setup_type == SetupType.BREAKOUT
    assert plan.entry_zone is not None
    assert plan.invalidation_price is not None
    assert plan.invalidation_price < plan.entry_zone[0]
    assert plan.target_price is not None
    assert plan.target_price > plan.entry_zone[1]
    assert plan.reward_risk >= 2.0
    assert "near_52w_high_breakout" in plan.reasons


def test_extended_breakout_is_marked_as_chase_block() -> None:
    plan = breakout_plan(_bars(), _features(extension_20d=0.18))

    assert plan.chase_block is True
    assert "chase_block" in plan.reasons


def test_low_reward_risk_plan_can_be_generated_for_later_policy_block() -> None:
    weak_features = _features(atr_pct=0.12)

    plan = breakout_plan(_bars(), weak_features)

    assert plan.reward_risk > 0
    assert plan.reward_risk < 2.5


def test_event_dependent_placeholders_do_not_promote_without_events() -> None:
    features = _features()

    selected = select_setup_plan(_bars(), features)
    earnings = post_earnings_plan(_bars(), features)
    filings = filings_catalyst_plan(_bars(), features)

    assert selected.setup_type == SetupType.BREAKOUT
    assert earnings.metadata["placeholder"] is True
    assert earnings.metadata["event_confirmed"] is False
    assert "post_earnings_requires_event_ingest" in earnings.reasons
    assert filings.metadata["placeholder"] is True
    assert filings.metadata["event_confirmed"] is False
    assert "filings_catalyst_requires_event_ingest" in filings.reasons


def _bars() -> list[DailyBar]:
    source_ts = datetime(2026, 5, 8, 20, tzinfo=UTC)
    return [
        DailyBar(
            ticker="AAA",
            date=date(2026, 5, 4),
            open=96,
            high=100,
            low=95,
            close=99,
            volume=800_000,
            vwap=98,
            adjusted=True,
            provider="sample",
            source_ts=source_ts,
            available_at=datetime(2026, 5, 4, 21, tzinfo=UTC),
        ),
        DailyBar(
            ticker="AAA",
            date=date(2026, 5, 5),
            open=99,
            high=103,
            low=98,
            close=102,
            volume=900_000,
            vwap=101,
            adjusted=True,
            provider="sample",
            source_ts=source_ts,
            available_at=datetime(2026, 5, 5, 21, tzinfo=UTC),
        ),
        DailyBar(
            ticker="AAA",
            date=date(2026, 5, 6),
            open=102,
            high=106,
            low=101,
            close=105,
            volume=950_000,
            vwap=104,
            adjusted=True,
            provider="sample",
            source_ts=source_ts,
            available_at=datetime(2026, 5, 6, 21, tzinfo=UTC),
        ),
        DailyBar(
            ticker="AAA",
            date=date(2026, 5, 7),
            open=105,
            high=109,
            low=104,
            close=108,
            volume=1_100_000,
            vwap=107,
            adjusted=True,
            provider="sample",
            source_ts=source_ts,
            available_at=datetime(2026, 5, 7, 21, tzinfo=UTC),
        ),
        DailyBar(
            ticker="AAA",
            date=date(2026, 5, 8),
            open=108,
            high=112,
            low=107,
            close=111,
            volume=1_500_000,
            vwap=110,
            adjusted=True,
            provider="sample",
            source_ts=source_ts,
            available_at=datetime(2026, 5, 8, 21, tzinfo=UTC),
        ),
    ]


def _features(*, extension_20d: float = 0.07, atr_pct: float = 0.035) -> MarketFeatures:
    return MarketFeatures(
        ticker="AAA",
        as_of=datetime(2026, 5, 8, 21, tzinfo=UTC),
        ret_5d=0.11,
        ret_20d=0.22,
        rs_20_sector=86,
        rs_60_spy=82,
        near_52w_high=0.98,
        ma_regime=92,
        rel_volume_5d=2.1,
        dollar_volume_z=2.0,
        atr_pct=atr_pct,
        extension_20d=extension_20d,
        liquidity_score=95,
        feature_version="market-v1",
    )

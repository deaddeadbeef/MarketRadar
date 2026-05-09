from datetime import UTC, datetime

from catalyst_radar.core.models import (
    ActionState,
    CandidateSnapshot,
    MarketFeatures,
    PolicyResult,
)


def test_action_state_values_are_stable() -> None:
    assert ActionState.NO_ACTION.value == "NoAction"
    assert ActionState.ADD_TO_WATCHLIST.value == "AddToWatchlist"
    assert ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW.value == "EligibleForManualBuyReview"
    assert ActionState.BLOCKED.value == "Blocked"


def test_policy_result_records_block_reasons() -> None:
    result = PolicyResult(
        state=ActionState.BLOCKED,
        hard_blocks=("liquidity_hard_block",),
        reasons=("avg dollar volume below floor",),
    )

    assert result.is_blocked is True
    assert result.hard_blocks == ("liquidity_hard_block",)


def test_candidate_snapshot_keeps_availability_time() -> None:
    as_of = datetime(2026, 5, 8, 21, tzinfo=UTC)
    features = MarketFeatures(
        ticker="AAA",
        as_of=as_of,
        ret_5d=0.12,
        ret_20d=0.12,
        rs_20_sector=82,
        rs_60_spy=80,
        near_52w_high=0.98,
        ma_regime=90,
        rel_volume_5d=2.0,
        dollar_volume_z=2.5,
        atr_pct=0.04,
        extension_20d=0.08,
        liquidity_score=90,
        feature_version="market-v1",
    )
    snapshot = CandidateSnapshot(
        ticker="AAA",
        as_of=as_of,
        features=features,
        final_score=78.0,
        strong_pillars=3,
        risk_penalty=5.0,
        portfolio_penalty=0.0,
        data_stale=False,
    )

    assert snapshot.ticker == "AAA"
    assert snapshot.features.feature_version == "market-v1"

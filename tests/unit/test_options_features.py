from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta

import pytest

from catalyst_radar.features.options import (
    OptionFeatureInput,
    compute_option_feature_score,
)


def test_positive_call_put_ratio_scores_when_volume_is_meaningful() -> None:
    score = compute_option_feature_score(
        _option_input(call_volume=12_000, put_volume=4_000, iv_percentile=0.72)
    )

    assert score.ticker == "AAA"
    assert score.call_put_ratio == 3.0
    assert score.call_oi_ratio > 1.0
    assert score.abnormality_score > 50.0
    assert score.options_flow_score > 50.0


def test_high_iv_percentile_and_skew_create_finite_risk_penalty() -> None:
    score = compute_option_feature_score(_option_input(iv_percentile=0.96, skew=1.75))

    assert math.isfinite(score.options_flow_score)
    assert math.isfinite(score.options_risk_score)
    assert score.iv_percentile == 0.96
    assert score.options_risk_score > 50.0


def test_zero_volume_returns_neutral_flow_score() -> None:
    score = compute_option_feature_score(
        _option_input(call_volume=0, put_volume=0, iv_percentile=0.75, skew=1.25)
    )

    assert score.call_put_ratio == 0.0
    assert score.options_flow_score == 0.0
    assert math.isfinite(score.options_risk_score)


def test_nan_and_inf_inputs_are_finite_safe_and_degraded() -> None:
    score = compute_option_feature_score(
        _option_input(
            call_volume=math.nan,
            put_volume=math.inf,
            call_open_interest=-math.inf,
            put_open_interest=math.nan,
            iv_percentile=math.inf,
            skew=-math.inf,
        )
    )

    assert score.call_put_ratio == 0.0
    assert score.call_oi_ratio == 0.0
    assert score.iv_percentile == 0.0
    assert score.skew == 0.0
    assert score.options_flow_score == 0.0
    assert all(
        math.isfinite(value)
        for value in (
            score.call_put_ratio,
            score.call_oi_ratio,
            score.iv_percentile,
            score.skew,
            score.abnormality_score,
            score.options_flow_score,
            score.options_risk_score,
        )
    )


def test_option_input_validation_normalizes_and_rejects_invalid_availability() -> None:
    base = _option_input(ticker="aaa", payload={"nested": {"items": [1, 2]}})

    assert base.ticker == "AAA"
    assert base.payload["nested"]["items"] == (1, 2)

    with pytest.raises(ValueError, match="provider"):
        _option_input(provider=" ")

    with pytest.raises(ValueError, match="available_at"):
        _option_input(available_at=base.source_ts - timedelta(seconds=1))

    with pytest.raises(ValueError, match="as_of"):
        _option_input(as_of=datetime(2026, 5, 8, 21, 0))


def _option_input(
    *,
    ticker: str = "AAA",
    as_of: datetime = datetime(2026, 5, 8, 21, 0, tzinfo=UTC),
    provider: str = "options_fixture",
    call_volume: float = 12_000.0,
    put_volume: float = 4_000.0,
    call_open_interest: float = 50_000.0,
    put_open_interest: float = 30_000.0,
    iv_percentile: float = 0.72,
    skew: float = 0.2,
    source_ts: datetime = datetime(2026, 5, 8, 20, 45, tzinfo=UTC),
    available_at: datetime = datetime(2026, 5, 8, 21, 0, tzinfo=UTC),
    payload: dict[str, object] | None = None,
) -> OptionFeatureInput:
    return OptionFeatureInput(
        ticker=ticker,
        as_of=as_of,
        provider=provider,
        call_volume=call_volume,
        put_volume=put_volume,
        call_open_interest=call_open_interest,
        put_open_interest=put_open_interest,
        iv_percentile=iv_percentile,
        skew=skew,
        source_ts=source_ts,
        available_at=available_at,
        payload=payload or {"source": "fixture"},
    )

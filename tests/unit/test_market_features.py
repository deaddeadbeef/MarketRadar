import math
from dataclasses import fields
from datetime import UTC, datetime

import pandas as pd

from catalyst_radar.features.market import compute_market_features


def test_market_features_score_strong_relative_move() -> None:
    as_of = datetime(2026, 5, 8, 21, tzinfo=UTC)
    ticker_bars = _bars(
        "AAA",
        [94, 98, 100, 102, 105, 109],
        [500000, 700000, 800000, 850000, 1100000, 1500000],
    )
    spy_bars = _bars(
        "SPY",
        [504, 507, 506, 509, 510, 512],
        [70000000, 72000000, 71000000, 73000000, 74000000, 76000000],
    )
    sector_bars = _bars(
        "XLK",
        [202, 205, 206, 209, 211, 215],
        [10000000, 11000000, 10500000, 12000000, 13000000, 14000000],
    )

    features = compute_market_features("AAA", as_of, ticker_bars, spy_bars, sector_bars)

    assert features.ticker == "AAA"
    assert features.ret_5d > 0.10
    assert features.rs_20_sector > 50
    assert features.rs_60_spy > 50
    assert 0 < features.near_52w_high <= 1
    assert features.rel_volume_5d > 1
    assert features.liquidity_score == 100


def test_market_features_penalize_illiquid_name() -> None:
    as_of = datetime(2026, 5, 8, 21, tzinfo=UTC)
    ticker_bars = _bars(
        "CCC",
        [8.1, 8.0, 8.0, 7.9, 7.8, 7.6],
        [250000, 180000, 160000, 150000, 140000, 130000],
    )
    spy_bars = _bars(
        "SPY",
        [504, 507, 506, 509, 510, 512],
        [70000000, 72000000, 71000000, 73000000, 74000000, 76000000],
    )
    sector_bars = _bars(
        "XLK",
        [202, 205, 206, 209, 211, 215],
        [10000000, 11000000, 10500000, 12000000, 13000000, 14000000],
    )

    features = compute_market_features("CCC", as_of, ticker_bars, spy_bars, sector_bars)

    assert features.ret_5d < 0
    assert features.liquidity_score < 50


def test_market_features_near_52w_high_is_ratio() -> None:
    as_of = datetime(2026, 5, 8, 21, tzinfo=UTC)
    ticker_bars = _bars(
        "AAA",
        [94, 98, 100, 102, 105, 109],
        [500000, 700000, 800000, 850000, 1100000, 1500000],
    )
    spy_bars = _bars(
        "SPY",
        [504, 507, 506, 509, 510, 512],
        [70000000, 72000000, 71000000, 73000000, 74000000, 76000000],
    )
    sector_bars = _bars(
        "XLK",
        [202, 205, 206, 209, 211, 215],
        [10000000, 11000000, 10500000, 12000000, 13000000, 14000000],
    )

    features = compute_market_features("AAA", as_of, ticker_bars, spy_bars, sector_bars)

    assert 0 < features.near_52w_high <= 1
    assert math.isclose(features.near_52w_high, 109 / (109 * 1.01))


def test_market_features_returns_finite_values_for_non_finite_input() -> None:
    as_of = datetime(2026, 5, 8, 21, tzinfo=UTC)
    ticker_bars = _bars(
        "BAD",
        [10, 11, float("inf"), 12, 13, float("nan")],
        [100000, 120000, 140000, 160000, float("inf"), 180000],
    )
    spy_bars = _bars(
        "SPY",
        [504, float("nan"), 506, 509, 510, 512],
        [70000000, 72000000, 71000000, 73000000, 74000000, 76000000],
    )
    sector_bars = _bars(
        "XLK",
        [202, 205, float("inf"), 209, 211, 215],
        [10000000, 11000000, 10500000, 12000000, 13000000, 14000000],
    )

    features = compute_market_features("BAD", as_of, ticker_bars, spy_bars, sector_bars)

    for field in fields(features):
        value = getattr(features, field.name)
        if isinstance(value, float):
            assert math.isfinite(value), field.name


def _bars(ticker: str, closes: list[float], volumes: list[int]) -> pd.DataFrame:
    dates = pd.date_range("2026-05-01", periods=len(closes), freq="B")
    return pd.DataFrame(
        {
            "ticker": ticker,
            "date": dates,
            "open": closes,
            "high": [value * 1.01 for value in closes],
            "low": [value * 0.99 for value in closes],
            "close": closes,
            "volume": volumes,
            "vwap": closes,
        }
    )

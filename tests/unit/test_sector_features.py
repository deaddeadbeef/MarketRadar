from __future__ import annotations

import math

import pandas as pd

from catalyst_radar.features.sector import sector_rotation_score


def test_sector_rotation_scores_ticker_and_sector_outperformance() -> None:
    score = sector_rotation_score(
        ticker_bars=_bars("AAA", [94, 98, 100, 102, 105, 109]),
        spy_bars=_bars("SPY", [504, 507, 506, 509, 510, 512]),
        sector_bars=_bars("XLK", [202, 205, 206, 209, 211, 215]),
    )

    assert score.score > 60.0
    assert score.ticker_vs_sector > 0.0
    assert score.sector_vs_spy > 0.0


def test_sector_rotation_is_neutral_for_missing_data() -> None:
    score = sector_rotation_score(
        ticker_bars=pd.DataFrame(),
        spy_bars=pd.DataFrame(),
        sector_bars=pd.DataFrame(),
    )

    assert score.score == 50.0
    assert score.ticker_return_20d == 0.0
    assert score.sector_return_20d == 0.0
    assert score.spy_return_20d == 0.0


def test_sector_rotation_returns_finite_values_for_bad_inputs() -> None:
    score = sector_rotation_score(
        ticker_bars=_bars("BAD", [10, 11, float("inf"), 12, 13, float("nan")]),
        spy_bars=_bars("SPY", [504, float("nan"), 506, 509, 510, 512]),
        sector_bars=_bars("XLK", [202, 205, float("inf"), 209, 211, 215]),
    )

    for value in score.__dict__.values():
        assert math.isfinite(value)


def _bars(ticker: str, closes: list[float]) -> pd.DataFrame:
    dates = pd.date_range("2026-05-01", periods=len(closes), freq="B")
    return pd.DataFrame(
        {
            "ticker": ticker,
            "date": dates,
            "open": closes,
            "high": [value * 1.01 for value in closes],
            "low": [value * 0.99 for value in closes],
            "close": closes,
            "volume": [1_000_000] * len(closes),
            "vwap": closes,
        }
    )

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from catalyst_radar.features.market import _period_return, _prepare_bars

SECTOR_FEATURE_VERSION = "sector-v1"


@dataclass(frozen=True)
class SectorRotationScore:
    score: float
    ticker_return_20d: float
    sector_return_20d: float
    spy_return_20d: float
    ticker_vs_sector: float
    sector_vs_spy: float


def sector_rotation_score(
    ticker_bars: pd.DataFrame,
    spy_bars: pd.DataFrame,
    sector_bars: pd.DataFrame,
) -> SectorRotationScore:
    ticker = _prepare_bars(ticker_bars)
    spy = _prepare_bars(spy_bars)
    sector = _prepare_bars(sector_bars)
    ticker_return = _finite(_period_return(ticker, 20))
    sector_return = _finite(_period_return(sector, 20))
    spy_return = _finite(_period_return(spy, 20))
    ticker_vs_sector = ticker_return - sector_return
    sector_vs_spy = sector_return - spy_return
    score = 50.0 + (ticker_vs_sector * 180.0) + (sector_vs_spy * 220.0)
    return SectorRotationScore(
        score=round(_clamp(score, 0.0, 100.0), 2),
        ticker_return_20d=round(ticker_return, 6),
        sector_return_20d=round(sector_return, 6),
        spy_return_20d=round(spy_return, 6),
        ticker_vs_sector=round(ticker_vs_sector, 6),
        sector_vs_spy=round(sector_vs_spy, 6),
    )


def _finite(value: float) -> float:
    number = float(value)
    if number != number or number in (float("inf"), float("-inf")):
        return 0.0
    return number


def _clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, _finite(value)))


__all__ = ["SECTOR_FEATURE_VERSION", "SectorRotationScore", "sector_rotation_score"]

from __future__ import annotations

from datetime import datetime

import numpy as np
import pandas as pd

from catalyst_radar.core.models import MarketFeatures

FEATURE_VERSION = "market-v1"


def compute_market_features(
    ticker: str,
    as_of: datetime,
    ticker_bars: pd.DataFrame,
    spy_bars: pd.DataFrame,
    sector_bars: pd.DataFrame,
) -> MarketFeatures:
    bars = _prepare_bars(ticker_bars)
    spy = _prepare_bars(spy_bars)
    sector = _prepare_bars(sector_bars)

    ret_5d = _period_return(bars, 5)
    ret_20d = _period_return(bars, 20)
    sector_ret_20d = _period_return(sector, 20)
    spy_ret_60d = _period_return(spy, 60)
    ticker_ret_60d = _period_return(bars, 60)

    return MarketFeatures(
        ticker=ticker,
        as_of=as_of,
        ret_5d=ret_5d,
        ret_20d=ret_20d,
        rs_20_sector=_relative_strength_score(ret_20d, sector_ret_20d),
        rs_60_spy=_relative_strength_score(ticker_ret_60d, spy_ret_60d),
        near_52w_high=_near_high_score(bars, 252),
        ma_regime=_ma_regime_score(bars),
        rel_volume_5d=_relative_volume(bars, 5, 20),
        dollar_volume_z=_dollar_volume_z(bars),
        atr_pct=_atr_pct(bars, 14),
        extension_20d=_extension_from_ma(bars, 20),
        liquidity_score=_liquidity_score(bars, 20),
        feature_version=FEATURE_VERSION,
    )


def _prepare_bars(bars: pd.DataFrame) -> pd.DataFrame:
    if bars.empty:
        return bars.copy()

    prepared = bars.copy()
    prepared["date"] = pd.to_datetime(prepared["date"])
    return prepared.sort_values("date").reset_index(drop=True)


def _period_return(bars: pd.DataFrame, sessions: int) -> float:
    if bars.empty or "close" not in bars:
        return 0.0

    closes = bars["close"].astype(float).to_numpy()
    if len(closes) < 2:
        return 0.0

    start_index = max(0, len(closes) - sessions - 1)
    start = closes[start_index]
    end = closes[-1]
    if start <= 0:
        return 0.0

    return float((end / start) - 1)


def _relative_strength_score(ticker_return: float, benchmark_return: float) -> float:
    return _clamp(50 + ((ticker_return - benchmark_return) * 250), 0, 100)


def _near_high_score(bars: pd.DataFrame, sessions: int) -> float:
    if bars.empty or "close" not in bars:
        return 0.0

    closes = bars["close"].astype(float)
    close = float(closes.iloc[-1])
    high = float(closes.tail(sessions).max())
    if high <= 0:
        return 0.0

    return _clamp((close / high) * 100, 0, 100)


def _ma_regime_score(bars: pd.DataFrame) -> float:
    if bars.empty or "close" not in bars:
        return 0.0

    closes = bars["close"].astype(float)
    close = float(closes.iloc[-1])
    ma_20 = float(closes.tail(20).mean())
    ma_50 = float(closes.tail(50).mean())

    if ma_20 <= 0 or ma_50 <= 0:
        return 0.0

    score = 50.0
    score += 25.0 if close >= ma_20 else -25.0
    score += 25.0 if ma_20 >= ma_50 else -25.0
    return _clamp(score, 0, 100)


def _relative_volume(bars: pd.DataFrame, recent_sessions: int, baseline_sessions: int) -> float:
    if bars.empty or "volume" not in bars:
        return 0.0

    volumes = bars["volume"].astype(float)
    recent = float(volumes.tail(recent_sessions).mean())
    baseline_window = volumes.iloc[: max(0, len(volumes) - recent_sessions)].tail(baseline_sessions)
    if baseline_window.empty:
        baseline = float(volumes.mean())
    else:
        baseline = float(baseline_window.mean())

    if baseline <= 0:
        return 0.0

    return float(recent / baseline)


def _dollar_volume_z(bars: pd.DataFrame) -> float:
    dollar_volume = _dollar_volume(bars)
    if dollar_volume.empty:
        return 0.0

    recent = float(dollar_volume.tail(5).mean())
    baseline = dollar_volume.tail(20)
    std = float(baseline.std(ddof=0))
    if std <= 0:
        return 0.0

    return float((recent - float(baseline.mean())) / std)


def _atr_pct(bars: pd.DataFrame, sessions: int) -> float:
    if bars.empty or not {"high", "low", "close"}.issubset(bars.columns):
        return 0.0

    high = bars["high"].astype(float)
    low = bars["low"].astype(float)
    close = bars["close"].astype(float)
    previous_close = close.shift(1)
    true_range = pd.concat(
        [
            high - low,
            (high - previous_close).abs(),
            (low - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    last_close = float(close.iloc[-1])
    if last_close <= 0:
        return 0.0

    return float(true_range.tail(sessions).mean() / last_close)


def _extension_from_ma(bars: pd.DataFrame, sessions: int) -> float:
    if bars.empty or "close" not in bars:
        return 0.0

    closes = bars["close"].astype(float)
    moving_average = float(closes.tail(sessions).mean())
    if moving_average <= 0:
        return 0.0

    return float((float(closes.iloc[-1]) / moving_average) - 1)


def _liquidity_score(bars: pd.DataFrame, sessions: int) -> float:
    dollar_volume = _dollar_volume(bars)
    if dollar_volume.empty:
        return 0.0

    avg_dollar_volume = float(dollar_volume.tail(sessions).mean())
    return _clamp((avg_dollar_volume / 10_000_000) * 100, 0, 100)


def _dollar_volume(bars: pd.DataFrame) -> pd.Series:
    if bars.empty or not {"close", "volume"}.issubset(bars.columns):
        return pd.Series(dtype=float)

    closes = bars["close"].astype(float).replace([np.inf, -np.inf], np.nan)
    volumes = bars["volume"].astype(float).replace([np.inf, -np.inf], np.nan)
    return (closes * volumes).dropna()


def _clamp(value: float, minimum: float, maximum: float) -> float:
    return float(max(minimum, min(maximum, value)))

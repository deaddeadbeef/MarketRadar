from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from catalyst_radar.core.models import DailyBar, Security

_BENCHMARK_OR_SECTOR_ETFS = frozenset({"SPY", "XLK", "XLI"})


@dataclass(frozen=True)
class UniverseFilterConfig:
    min_price: float
    min_avg_dollar_volume: float
    require_sector: bool = False
    include_etfs: bool = False
    include_adrs: bool = True


@dataclass(frozen=True)
class UniverseDecision:
    ticker: str
    included: bool
    reason: str
    rank: int | None
    avg_dollar_volume_20d: float
    latest_close: float
    exclusion_reasons: tuple[str, ...]


def evaluate_universe_member(
    security: Security,
    bars: list[DailyBar],
    config: UniverseFilterConfig,
    *,
    as_of: date | None = None,
) -> UniverseDecision:
    exclusion_reasons: list[str] = []
    ticker = security.ticker.upper()
    latest_close = 0.0
    avg_dollar_volume = 0.0

    if not security.is_active:
        exclusion_reasons.append("inactive")
    if not bars:
        exclusion_reasons.append("missing_bars")
    else:
        latest = bars[-1]
        latest_close = float(latest.close)
        if as_of is not None and latest.date < as_of:
            exclusion_reasons.append("stale_bars")
        avg_dollar_volume = _avg_dollar_volume(bars[-20:])

    if bars and latest_close < config.min_price:
        exclusion_reasons.append("low_price")
    if bars and avg_dollar_volume < config.min_avg_dollar_volume:
        exclusion_reasons.append("low_avg_dollar_volume")
    if config.require_sector and _missing_sector(security.sector):
        exclusion_reasons.append("missing_sector")
    if not config.include_etfs and _is_etf(security):
        exclusion_reasons.append("etf_excluded")
    if not config.include_adrs and _is_adr(security):
        exclusion_reasons.append("adr_excluded")

    included = not exclusion_reasons
    return UniverseDecision(
        ticker=ticker,
        included=included,
        reason="eligible" if included else ",".join(exclusion_reasons),
        rank=None,
        avg_dollar_volume_20d=avg_dollar_volume,
        latest_close=latest_close,
        exclusion_reasons=tuple(exclusion_reasons),
    )


def _avg_dollar_volume(bars: list[DailyBar]) -> float:
    if not bars:
        return 0.0
    return sum(float(bar.close) * float(bar.volume) for bar in bars) / len(bars)


def _missing_sector(sector: str) -> bool:
    return not sector.strip() or sector.strip().lower() == "unknown"


def _is_etf(security: Security) -> bool:
    return security.ticker.upper() in _BENCHMARK_OR_SECTOR_ETFS or "etf" in security.name.lower()


def _is_adr(security: Security) -> bool:
    haystack = f"{security.name} {security.industry}".lower()
    return " adr" in haystack or "american deposit" in haystack

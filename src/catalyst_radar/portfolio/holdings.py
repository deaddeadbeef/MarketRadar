from __future__ import annotations

import math
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from catalyst_radar.core.models import HoldingSnapshot


@dataclass(frozen=True)
class PortfolioState:
    as_of: datetime
    portfolio_value: float
    cash: float
    holdings: tuple[HoldingSnapshot, ...]
    source: str


def latest_portfolio_state(
    holdings: Sequence[HoldingSnapshot],
    as_of: datetime,
    fallback_value: float,
    fallback_cash: float,
) -> PortfolioState:
    eligible = [holding for holding in holdings if holding.as_of <= as_of]
    if not eligible:
        return PortfolioState(
            as_of=as_of,
            portfolio_value=_positive_or_zero(fallback_value),
            cash=_positive_or_zero(fallback_cash),
            holdings=(),
            source="config_fallback",
        )

    snapshot_as_of = max(holding.as_of for holding in eligible)
    snapshot_holdings = tuple(holding for holding in eligible if holding.as_of == snapshot_as_of)
    snapshot_value = _first_positive(holding.portfolio_value for holding in snapshot_holdings)
    snapshot_cash = _first_positive(holding.cash for holding in snapshot_holdings)
    fallback_portfolio_value = _positive_or_zero(fallback_value)
    fallback_cash_value = _positive_or_zero(fallback_cash)

    return PortfolioState(
        as_of=snapshot_as_of,
        portfolio_value=snapshot_value or fallback_portfolio_value,
        cash=snapshot_cash or fallback_cash_value,
        holdings=snapshot_holdings,
        source="holdings_snapshot",
    )


def positions_by_ticker(state: PortfolioState) -> dict[str, dict[str, Any]]:
    return {
        holding.ticker: {
            "notional": holding.market_value,
            "sector": holding.sector,
            "theme": holding.theme,
            "shares": holding.shares,
        }
        for holding in state.holdings
    }


def _first_positive(values: Iterable[float]) -> float:
    for value in values:
        positive = _positive_or_zero(value)
        if positive > 0:
            return positive
    return 0.0


def _positive_or_zero(value: float) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(result) or result <= 0:
        return 0.0
    return result

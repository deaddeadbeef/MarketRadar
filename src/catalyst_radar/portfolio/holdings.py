from __future__ import annotations

import math
from collections.abc import Sequence
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
    input_warnings: tuple[str, ...] = ()


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

    snapshot_holdings = _latest_holding_by_ticker(eligible)
    snapshot_as_of = max(holding.as_of for holding in snapshot_holdings)
    snapshot_value, value_warning = _account_value(
        eligible,
        "portfolio_value",
        fallback_value,
        "inconsistent_portfolio_value",
    )
    snapshot_cash, cash_warning = _account_value(
        eligible,
        "cash",
        fallback_cash,
        "inconsistent_cash",
    )
    input_warnings = tuple(
        warning
        for warning in (value_warning, cash_warning)
        if warning is not None
    )

    return PortfolioState(
        as_of=snapshot_as_of,
        portfolio_value=snapshot_value,
        cash=snapshot_cash,
        holdings=snapshot_holdings,
        source="holdings_latest_by_ticker",
        input_warnings=input_warnings,
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
        for holding in state.holdings
        if holding.market_value > 0 or holding.shares > 0
    }


def _latest_holding_by_ticker(
    holdings: Sequence[HoldingSnapshot],
) -> tuple[HoldingSnapshot, ...]:
    latest: dict[str, HoldingSnapshot] = {}
    for holding in holdings:
        current = latest.get(holding.ticker)
        if current is None or holding.as_of > current.as_of:
            latest[holding.ticker] = holding
    return tuple(latest[ticker] for ticker in sorted(latest))


def _account_value(
    holdings: Sequence[HoldingSnapshot],
    field_name: str,
    fallback_value: float,
    warning_name: str,
) -> tuple[float, str | None]:
    rows_with_value = [
        holding
        for holding in holdings
        if _positive_or_zero(getattr(holding, field_name)) > 0
    ]
    if not rows_with_value:
        return _positive_or_zero(fallback_value), None
    account_as_of = max(holding.as_of for holding in rows_with_value)
    positive_values = [
        _positive_or_zero(getattr(holding, field_name))
        for holding in rows_with_value
        if holding.as_of == account_as_of
    ]
    unique_values = {round(value, 2) for value in positive_values}
    if len(unique_values) > 1:
        return min(positive_values), warning_name
    return positive_values[0], None


def _positive_or_zero(value: float) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(result) or result <= 0:
        return 0.0
    return result

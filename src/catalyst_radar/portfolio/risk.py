from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

from catalyst_radar.core.models import PortfolioImpact


@dataclass(frozen=True)
class PortfolioPolicy:
    max_position_pct: float = 0.10
    risk_per_trade_pct: float = 0.01
    max_sector_pct: float = 0.30
    max_theme_pct: float = 0.40


@dataclass(frozen=True)
class PositionSize:
    shares: int
    notional: float
    position_pct: float
    risk_amount: float
    is_capped: bool


def compute_position_size(
    account_equity: float,
    entry_price: float,
    invalidation_price: float,
    policy: PortfolioPolicy | None = None,
) -> PositionSize:
    active_policy = policy or PortfolioPolicy()
    account_equity = _finite_positive(account_equity)
    entry_price = _finite_positive(entry_price)
    invalidation_price = _finite_positive(invalidation_price)
    risk_per_trade_pct = _finite_positive(active_policy.risk_per_trade_pct)
    max_position_pct = _finite_positive(active_policy.max_position_pct)

    if account_equity <= 0 or entry_price <= 0 or invalidation_price <= 0:
        return PositionSize(
            shares=0,
            notional=0.0,
            position_pct=0.0,
            risk_amount=0.0,
            is_capped=False,
        )

    risk_per_share = entry_price - invalidation_price
    if risk_per_share <= 0 or risk_per_trade_pct <= 0 or max_position_pct <= 0:
        return PositionSize(
            shares=0,
            notional=0.0,
            position_pct=0.0,
            risk_amount=0.0,
            is_capped=False,
        )

    risk_budget = account_equity * risk_per_trade_pct
    max_position_notional = account_equity * max_position_pct
    risk_based_shares = int(risk_budget // risk_per_share)
    max_position_shares = int(max_position_notional // entry_price)
    shares = max(0, min(risk_based_shares, max_position_shares))
    notional = shares * entry_price
    risk_amount = shares * risk_per_share
    return PositionSize(
        shares=shares,
        notional=round(notional, 2),
        position_pct=round(notional / account_equity, 4),
        risk_amount=round(risk_amount, 2),
        is_capped=max_position_shares < risk_based_shares,
    )


def evaluate_portfolio_impact(
    ticker: str,
    sector: str,
    theme: str,
    account_equity: float,
    current_positions: dict[str, dict[str, Any]],
    proposed_notional: float,
    policy: PortfolioPolicy | None = None,
) -> PortfolioImpact:
    active_policy = policy or PortfolioPolicy()
    account_equity = _finite_positive(account_equity)
    proposed_notional = _finite_positive(proposed_notional)
    max_position_pct = _finite_positive(active_policy.max_position_pct)
    max_sector_pct = _finite_positive(active_policy.max_sector_pct)
    max_theme_pct = _finite_positive(active_policy.max_theme_pct)

    if account_equity <= 0:
        return PortfolioImpact(
            ticker=ticker,
            single_name_after_pct=0.0,
            sector_after_pct=0.0,
            theme_after_pct=0.0,
            portfolio_penalty=25.0,
            hard_blocks=("invalid_account_equity",),
        )

    hard_blocks = []
    penalty = 0.0
    if proposed_notional <= 0:
        hard_blocks.append("invalid_portfolio_input")
        penalty += 25.0

    existing_name, _ = _position_notional(current_positions.get(ticker, {}))

    sector_notional = 0.0
    theme_notional = 0.0
    for position in current_positions.values():
        notional, invalid = _position_notional(position)
        if invalid:
            hard_blocks.append("invalid_portfolio_input")
            penalty += 25.0
            continue
        if position.get("sector") == sector:
            sector_notional += notional
        if position.get("theme") == theme:
            theme_notional += notional

    single_name_after_pct = (existing_name + proposed_notional) / account_equity
    sector_after_pct = (sector_notional + proposed_notional) / account_equity
    theme_after_pct = (theme_notional + proposed_notional) / account_equity

    if single_name_after_pct > max_position_pct:
        hard_blocks.append("single_name_overexposure")
        penalty += 25.0
    if sector_after_pct > max_sector_pct:
        hard_blocks.append("sector_overexposure")
        penalty += 25.0
    if theme_after_pct > max_theme_pct:
        hard_blocks.append("theme_overexposure")
        penalty += 25.0

    return PortfolioImpact(
        ticker=ticker,
        single_name_after_pct=round(single_name_after_pct, 4),
        sector_after_pct=round(sector_after_pct, 4),
        theme_after_pct=round(theme_after_pct, 4),
        portfolio_penalty=penalty,
        hard_blocks=tuple(dict.fromkeys(hard_blocks)),
    )


def _position_notional(position: dict[str, Any]) -> tuple[float, bool]:
    try:
        value = float(position.get("notional", 0.0))
    except (TypeError, ValueError):
        return 0.0, True
    if not math.isfinite(value) or value < 0:
        return 0.0, True
    return value, False


def _finite_positive(value: float) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(result) or result <= 0:
        return 0.0
    return result

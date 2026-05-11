from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

from catalyst_radar.core.config import AppConfig


@dataclass(frozen=True)
class OrderPreviewRequest:
    ticker: str
    side: str
    entry_price: float
    invalidation_price: float
    risk_per_trade_pct: float
    account_id: str | None = None


def build_disabled_order_preview(
    request: OrderPreviewRequest,
    *,
    portfolio_context: dict[str, Any],
    config: AppConfig,
) -> dict[str, object]:
    equity = _positive_float(portfolio_context.get("portfolio_equity"))
    entry = _positive_float(request.entry_price)
    invalidation = _positive_float(request.invalidation_price)
    risk_pct = _positive_float(request.risk_per_trade_pct)
    stop_distance = abs(entry - invalidation) if entry and invalidation else 0.0
    risk_budget = equity * risk_pct
    shares_by_risk = math.floor(risk_budget / stop_distance) if stop_distance > 0 else 0
    max_value = equity * config.max_single_name_pct
    shares_by_exposure = math.floor(max_value / entry) if entry > 0 else 0
    proposed_shares = max(0, min(shares_by_risk, shares_by_exposure))
    estimated_value = proposed_shares * entry
    hard_blocks = ["broker_submission_disabled"]
    if config.schwab_order_submission_enabled:
        hard_blocks.append("broker_read_only_integration")
    if portfolio_context.get("broker_data_stale"):
        hard_blocks.append("stale_broker_data")
    if stop_distance <= 0:
        hard_blocks.append("invalid_invalidation")
    if proposed_shares <= 0:
        hard_blocks.append("zero_preview_size")
    return {
        "action": "preview_only",
        "ticker": request.ticker.upper(),
        "side": request.side.lower(),
        "entry_price": entry,
        "invalidation_price": invalidation,
        "portfolio_equity": round(equity, 2),
        "risk_budget_dollars": round(risk_budget, 2),
        "stop_distance": round(stop_distance, 2),
        "max_shares_by_risk": shares_by_risk,
        "max_shares_by_exposure": shares_by_exposure,
        "proposed_shares": proposed_shares,
        "estimated_position_value": round(estimated_value, 2),
        "hard_blocks": hard_blocks,
        "warnings": ["Broker routing is disabled; this is a manual-review preview only."],
        "requires_manual_approval": True,
        "submission_allowed": False,
        "submission_enabled": bool(config.schwab_order_submission_enabled),
    }


def _positive_float(value: object) -> float:
    try:
        number = float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
    return number if math.isfinite(number) and number > 0 else 0.0

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from catalyst_radar.validation.models import (
    PaperDecision,
    PaperTrade,
    PaperTradeState,
    paper_trade_id,
)


def create_paper_trade_from_card(
    card: Mapping[str, Any] | object,
    decision: PaperDecision | str,
    available_at: datetime,
    *,
    entry_price: float | None = None,
    entry_at: datetime | None = None,
) -> PaperTrade:
    """Create a simulated paper trade from a decision card.

    This function records research workflow state only. It does not place orders
    and does not call any external execution API.
    """

    decision_value = PaperDecision(decision)
    card_payload = _mapping(_read(card, "payload", {}))
    trade_plan = _mapping(card_payload.get("trade_plan"))
    sizing = _mapping(card_payload.get("position_sizing"))
    portfolio_impact = _mapping(card_payload.get("portfolio_impact"))
    card_id = str(_read(card, "id", ""))
    if not card_id:
        msg = "decision card id is required"
        raise ValueError(msg)

    if decision_value == PaperDecision.REJECTED:
        state = PaperTradeState.REJECTED
    elif decision_value == PaperDecision.DEFERRED:
        state = PaperTradeState.DEFERRED
    elif entry_price is not None:
        state = PaperTradeState.OPEN
    else:
        state = PaperTradeState.PENDING_ENTRY

    next_review_at = _optional_datetime(
        _read(card, "next_review_at", None)
        or _mapping(card_payload.get("controls")).get("next_review_at"),
        "next_review_at",
    )

    return PaperTrade(
        id=paper_trade_id(card_id, decision_value),
        decision_card_id=card_id,
        ticker=str(_read(card, "ticker", "")).upper(),
        as_of=_aware_datetime(_read(card, "as_of"), "as_of"),
        decision=decision_value,
        state=state,
        entry_price=entry_price,
        entry_at=entry_at,
        invalidation_price=_optional_float(trade_plan.get("invalidation_price")),
        shares=_float_value(sizing.get("shares"), default=0.0),
        notional=_float_value(sizing.get("notional"), default=0.0),
        max_loss=_float_value(
            trade_plan.get("max_loss_if_wrong"),
            default=_float_value(portfolio_impact.get("max_loss"), default=0.0),
        ),
        outcome_labels={},
        source_ts=_aware_datetime(_read(card, "source_ts"), "source_ts"),
        available_at=_aware_datetime(available_at, "available_at"),
        payload={
            "manual_review_only": True,
            "no_execution": True,
            "source": "decision_card",
            "decision_card_id": card_id,
            "next_review_at": next_review_at.isoformat() if next_review_at else None,
        },
    )


def mark_simulated_entry(
    trade: PaperTrade,
    *,
    entry_price: float,
    entry_at: datetime,
) -> PaperTrade:
    if trade.state not in {PaperTradeState.PENDING_ENTRY, PaperTradeState.OPEN}:
        msg = f"cannot mark entry for paper trade in state {trade.state.value}"
        raise ValueError(msg)
    return PaperTrade(
        id=trade.id,
        decision_card_id=trade.decision_card_id,
        ticker=trade.ticker,
        as_of=trade.as_of,
        decision=trade.decision,
        state=PaperTradeState.OPEN,
        entry_price=entry_price,
        entry_at=entry_at,
        invalidation_price=trade.invalidation_price,
        shares=trade.shares,
        notional=trade.notional,
        max_loss=trade.max_loss,
        outcome_labels=trade.outcome_labels,
        source_ts=trade.source_ts,
        available_at=trade.available_at,
        payload=trade.payload,
        created_at=trade.created_at,
        updated_at=_aware_datetime(entry_at, "entry_at"),
    )


def update_trade_outcome(
    trade: PaperTrade,
    outcome_labels: Mapping[str, Any],
    updated_at: datetime,
) -> PaperTrade:
    invalidated = bool(outcome_labels.get("invalidated"))
    state = PaperTradeState.INVALIDATED if invalidated else PaperTradeState.CLOSED
    if trade.state in {PaperTradeState.REJECTED, PaperTradeState.DEFERRED}:
        state = trade.state
    resolved_updated_at = _aware_datetime(updated_at, "updated_at")
    return PaperTrade(
        id=f"{trade.id}:outcome:{resolved_updated_at.isoformat()}",
        decision_card_id=trade.decision_card_id,
        ticker=trade.ticker,
        as_of=trade.as_of,
        decision=trade.decision,
        state=state,
        entry_price=trade.entry_price,
        entry_at=trade.entry_at,
        invalidation_price=trade.invalidation_price,
        shares=trade.shares,
        notional=trade.notional,
        max_loss=trade.max_loss,
        outcome_labels=outcome_labels,
        source_ts=trade.source_ts,
        available_at=resolved_updated_at,
        payload=trade.payload,
        created_at=trade.created_at,
        updated_at=resolved_updated_at,
    )


def _read(source: Mapping[str, Any] | object, key: str, default: Any = None) -> Any:
    if isinstance(source, Mapping):
        return source.get(key, default)
    return getattr(source, key, default)


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _aware_datetime(value: object, field_name: str) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if not isinstance(value, datetime):
        msg = f"{field_name} must be a datetime"
        raise TypeError(msg)
    if value.tzinfo is None or value.utcoffset() is None:
        msg = f"{field_name} must be timezone-aware"
        raise ValueError(msg)
    return value.astimezone(UTC)


def _optional_datetime(value: object, field_name: str) -> datetime | None:
    if value is None:
        return None
    return _aware_datetime(value, field_name)


def _float_value(value: object, *, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    return _float_value(value, default=0.0)


__all__ = [
    "create_paper_trade_from_card",
    "mark_simulated_entry",
    "update_trade_outcome",
]

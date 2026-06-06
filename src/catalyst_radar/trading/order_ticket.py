from __future__ import annotations

from datetime import datetime

from sqlalchemy.engine import Engine

from catalyst_radar.brokers.interactive import (
    create_blocked_order_ticket,
    order_ticket_payload,
)
from catalyst_radar.brokers.order_preview import (
    OrderPreviewRequest,
    build_disabled_order_preview,
)
from catalyst_radar.brokers.portfolio_context import latest_broker_portfolio_context
from catalyst_radar.core.config import AppConfig
from catalyst_radar.storage.broker_repositories import BrokerRepository


class OrderTicketExecutionError(ValueError):
    """Raised when an active-plan order ticket cannot be safely resolved."""


def run_workbench_order_ticket(
    engine: Engine,
    *,
    ticker: str,
    side: str,
    entry_price: float,
    invalidation_price: float,
    config: AppConfig,
    available_at: datetime | None = None,
    account_id: str | None = None,
    risk_per_trade_pct: float | None = None,
    notes: str | None = None,
    execute: bool = False,
    actor_source: str = "cli",
    actor_id: str | None = None,
    actor_role: str | None = None,
) -> dict[str, object]:
    request = _order_preview_request(
        ticker=ticker,
        side=side,
        entry_price=entry_price,
        invalidation_price=invalidation_price,
        risk_per_trade_pct=risk_per_trade_pct,
        account_id=account_id,
        config=config,
    )
    if execute:
        row = create_blocked_order_ticket(
            repo=BrokerRepository(engine),
            ticker=request.ticker,
            side=request.side,
            entry_price=request.entry_price,
            invalidation_price=request.invalidation_price,
            risk_per_trade_pct=request.risk_per_trade_pct,
            notes=notes,
            account_id=request.account_id,
            config=config,
            now=available_at,
            actor_source=actor_source,
            actor_id=actor_id,
            actor_role=actor_role,
        )
        preview = dict(row.preview_payload)
        ticket = order_ticket_payload(row)
    else:
        preview = build_disabled_order_preview(
            request,
            portfolio_context=latest_broker_portfolio_context(
                engine,
                ticker=request.ticker,
                available_at=available_at,
                config=config,
            ),
            config=config,
        )
        ticket = None
    return order_ticket_result_payload(
        preview=preview,
        ticket=ticket,
        execute=execute,
    )


def order_ticket_result_payload(
    *,
    preview: dict[str, object],
    ticket: dict[str, object] | None,
    execute: bool,
) -> dict[str, object]:
    return {
        "schema_version": "workbench-order-ticket-v1",
        "mode": "recorded" if execute else "preview",
        "external_calls_required": 0,
        "external_calls_made": 0,
        "db_writes_required": 1,
        "db_writes_made": 1 if execute else 0,
        "broker_order_submitted": False,
        "submission_allowed": False,
        "no_execution": True,
        "preview": preview,
        "ticket": ticket,
        "next_action": (
            "Blocked order ticket saved locally; no broker order was submitted."
            if execute
            else "Preview only. Re-run with record to save a blocked local ticket."
        ),
    }


def _order_preview_request(
    *,
    ticker: str,
    side: str,
    entry_price: float,
    invalidation_price: float,
    risk_per_trade_pct: float | None,
    account_id: str | None,
    config: AppConfig,
) -> OrderPreviewRequest:
    ticker_text = str(ticker or "").strip().upper()
    side_text = str(side or "").strip().lower()
    if not ticker_text:
        raise OrderTicketExecutionError("ticker is required")
    if side_text not in {"buy", "sell"}:
        raise OrderTicketExecutionError("side must be buy or sell")
    try:
        entry = float(entry_price)
        invalidation = float(invalidation_price)
    except (TypeError, ValueError) as exc:
        raise OrderTicketExecutionError(
            "entry and invalidation prices must be numbers"
        ) from exc
    risk_pct = (
        float(risk_per_trade_pct)
        if risk_per_trade_pct is not None
        else float(config.risk_per_trade_pct)
    )
    return OrderPreviewRequest(
        ticker=ticker_text,
        side=side_text,
        entry_price=entry,
        invalidation_price=invalidation,
        risk_per_trade_pct=risk_pct,
        account_id=str(account_id).strip() if account_id else None,
    )


__all__ = [
    "OrderTicketExecutionError",
    "order_ticket_result_payload",
    "run_workbench_order_ticket",
]

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta

from sqlalchemy import Engine

from catalyst_radar.brokers.models import (
    BrokerBalanceSnapshot,
    BrokerConnectionStatus,
    BrokerPosition,
)
from catalyst_radar.core.config import AppConfig
from catalyst_radar.storage.broker_repositories import BrokerRepository


def latest_broker_portfolio_context(
    engine: Engine,
    *,
    ticker: str | None = None,
    available_at: datetime | None = None,
    stale_after: timedelta = timedelta(hours=24),
    config: AppConfig | None = None,
) -> dict[str, object]:
    repo = BrokerRepository(engine)
    accounts = repo.list_accounts()
    connection = repo.latest_connection()
    balance = repo.latest_balance()
    positions = repo.latest_positions()
    as_of = _latest_as_of(balance, positions)
    now = _as_utc(available_at) if available_at is not None else datetime.now(UTC)
    stale = as_of is None or as_of < now - stale_after
    equity = balance.equity if balance is not None else 0.0
    cash = balance.cash if balance is not None else 0.0
    buying_power = balance.buying_power if balance is not None else 0.0
    normalized_ticker = str(ticker or "").strip().upper()
    existing = _existing_position(normalized_ticker, positions, equity)
    exposure = _exposure_summary(positions, equity)
    active_config = config or AppConfig.from_env()
    connected = (
        connection is not None
        and connection.status == BrokerConnectionStatus.CONNECTED
        and bool(accounts)
    )
    hard_blocks = []
    if stale and accounts:
        hard_blocks.append("stale_broker_data")
    if connection is not None and connection.status != BrokerConnectionStatus.CONNECTED:
        hard_blocks.append(f"broker_{connection.status.value}")
    return {
        "broker": "schwab",
        "broker_connected": connected,
        "connection_status": connection.status.value if connection is not None else "missing",
        "read_only": True,
        "order_submission_enabled": bool(active_config.schwab_order_submission_enabled),
        "order_submission_available": False,
        "snapshot_as_of": as_of.isoformat() if as_of is not None else None,
        "broker_data_stale": stale,
        "account_count": len(accounts),
        "position_count": len(positions),
        "portfolio_equity": round(equity, 2),
        "cash": round(cash, 2),
        "buying_power": round(buying_power, 2),
        "existing_position": existing,
        "exposure_before": exposure,
        "hard_blocks": hard_blocks,
    }


def portfolio_snapshot_payload(engine: Engine) -> dict[str, object]:
    repo = BrokerRepository(engine)
    connection = repo.latest_connection()
    accounts = repo.list_accounts()
    balance = repo.latest_balance()
    positions = repo.latest_positions()
    orders = repo.list_open_orders()
    return {
        "broker": "schwab",
        "connection_status": connection.status.value if connection is not None else "missing",
        "last_successful_sync_at": (
            connection.last_successful_sync_at.isoformat()
            if connection is not None and connection.last_successful_sync_at is not None
            else None
        ),
        "account_count": len(accounts),
        "position_count": len(positions),
        "open_order_count": len(orders),
        "latest_balance": _balance_payload(balance),
    }


def positions_payload(engine: Engine) -> list[dict[str, object]]:
    return [_position_payload(row) for row in BrokerRepository(engine).latest_positions()]


def balances_payload(engine: Engine) -> list[dict[str, object]]:
    repo = BrokerRepository(engine)
    balances = []
    for account in repo.list_accounts():
        balance = repo.latest_balance(account_id=account.id)
        if balance is not None:
            item = _balance_payload(balance)
            item["account_id"] = account.id
            item["display_name"] = account.display_name
            balances.append(item)
    return balances


def open_orders_payload(engine: Engine) -> list[dict[str, object]]:
    return [
        {
            "id": row.id,
            "account_id": row.account_id,
            "ticker": row.ticker,
            "side": row.side,
            "order_type": row.order_type,
            "quantity": row.quantity,
            "limit_price": row.limit_price,
            "status": row.status,
            "submitted_at": row.submitted_at.isoformat() if row.submitted_at else None,
        }
        for row in BrokerRepository(engine).list_open_orders()
    ]


def exposure_payload(engine: Engine) -> dict[str, object]:
    return latest_broker_portfolio_context(engine)


def _latest_as_of(
    balance: BrokerBalanceSnapshot | None,
    positions: Sequence[BrokerPosition],
) -> datetime | None:
    candidates = []
    if balance is not None:
        candidates.append(balance.as_of)
    candidates.extend(position.as_of for position in positions)
    return max(candidates) if candidates else None


def _existing_position(
    ticker: str,
    positions: Sequence[BrokerPosition],
    equity: float,
) -> Mapping[str, object] | None:
    if not ticker:
        return None
    matches = [position for position in positions if position.ticker == ticker]
    if not matches:
        return None
    quantity = sum(position.quantity for position in matches)
    market_value = sum(position.market_value for position in matches)
    return {
        "ticker": ticker,
        "quantity": round(quantity, 4),
        "market_value": round(market_value, 2),
        "exposure_pct": round(market_value / equity, 4) if equity > 0 else 0.0,
    }


def _exposure_summary(
    positions: Sequence[BrokerPosition],
    equity: float,
) -> Mapping[str, object]:
    gross = sum(max(0.0, position.market_value) for position in positions)
    top_positions = sorted(
        positions,
        key=lambda position: (-position.market_value, position.ticker),
    )[:10]
    return {
        "gross_exposure_pct": round(gross / equity, 4) if equity > 0 else 0.0,
        "single_name": {
            position.ticker: round(position.market_value / equity, 4) if equity > 0 else 0.0
            for position in top_positions
        },
    }


def _balance_payload(balance: BrokerBalanceSnapshot | None) -> dict[str, object] | None:
    if balance is None:
        return None
    return {
        "as_of": balance.as_of.isoformat(),
        "cash": round(balance.cash, 2),
        "buying_power": round(balance.buying_power, 2),
        "liquidation_value": round(balance.liquidation_value, 2),
        "equity": round(balance.equity, 2),
    }


def _position_payload(row: BrokerPosition) -> dict[str, object]:
    return {
        "account_id": row.account_id,
        "as_of": row.as_of.isoformat(),
        "ticker": row.ticker,
        "quantity": row.quantity,
        "average_price": row.average_price,
        "market_value": round(row.market_value, 2),
        "unrealized_pnl": row.unrealized_pnl,
        "sector": row.sector,
        "theme": row.theme,
    }


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)

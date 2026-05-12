from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import Engine, delete, func, insert, select, update

from catalyst_radar.brokers.models import (
    BrokerAccount,
    BrokerBalanceSnapshot,
    BrokerConnection,
    BrokerConnectionStatus,
    BrokerMarketSnapshot,
    BrokerOpportunityAction,
    BrokerOrder,
    BrokerOrderTicket,
    BrokerPosition,
    BrokerPositionSnapshot,
    BrokerToken,
    BrokerTrigger,
    BrokerTriggerStatus,
    broker_position_snapshot_id,
)
from catalyst_radar.core.immutability import thaw_json_value
from catalyst_radar.storage.schema import (
    broker_accounts,
    broker_balance_snapshots,
    broker_connections,
    broker_market_snapshots,
    broker_opportunity_actions,
    broker_order_tickets,
    broker_orders,
    broker_position_snapshots,
    broker_positions,
    broker_tokens,
    broker_triggers,
)

OPEN_ORDER_STATUSES = ("AWAITING_PARENT_ORDER", "QUEUED", "WORKING")


class BrokerRepository:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def upsert_connection(self, connection: BrokerConnection) -> BrokerConnection:
        with self.engine.begin() as conn:
            conn.execute(delete(broker_connections).where(broker_connections.c.id == connection.id))
            conn.execute(insert(broker_connections).values(**_connection_row(connection)))
        return connection

    def latest_connection(
        self,
        broker: str = "schwab",
        *,
        user_id: str = "local",
    ) -> BrokerConnection | None:
        stmt = (
            select(broker_connections)
            .where(
                broker_connections.c.broker == broker,
                broker_connections.c.user_id == user_id,
            )
            .order_by(broker_connections.c.updated_at.desc(), broker_connections.c.id.desc())
            .limit(1)
        )
        with self.engine.connect() as conn:
            row = conn.execute(stmt).first()
        return _connection_from_row(row._mapping) if row is not None else None

    def mark_connection_disconnected(self, connection_id: str, *, now: datetime) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                update(broker_connections)
                .where(broker_connections.c.id == connection_id)
                .values(
                    status=BrokerConnectionStatus.DISCONNECTED.value,
                    updated_at=_as_utc(now),
                )
            )
            conn.execute(
                delete(broker_tokens).where(broker_tokens.c.connection_id == connection_id)
            )

    def upsert_token(self, token: BrokerToken) -> BrokerToken:
        with self.engine.begin() as conn:
            conn.execute(delete(broker_tokens).where(broker_tokens.c.id == token.id))
            conn.execute(insert(broker_tokens).values(**_token_row(token)))
        return token

    def latest_token(self, connection_id: str) -> BrokerToken | None:
        stmt = (
            select(broker_tokens)
            .where(broker_tokens.c.connection_id == connection_id)
            .order_by(broker_tokens.c.updated_at.desc(), broker_tokens.c.id.desc())
            .limit(1)
        )
        with self.engine.connect() as conn:
            row = conn.execute(stmt).first()
        return _token_from_row(row._mapping) if row is not None else None

    def upsert_accounts(self, accounts: Iterable[BrokerAccount]) -> int:
        count = 0
        with self.engine.begin() as conn:
            for account in accounts:
                conn.execute(delete(broker_accounts).where(broker_accounts.c.id == account.id))
                conn.execute(insert(broker_accounts).values(**_account_row(account)))
                count += 1
        return count

    def list_accounts(
        self,
        *,
        broker: str = "schwab",
        active_only: bool = True,
    ) -> list[BrokerAccount]:
        filters = [broker_accounts.c.broker == broker]
        if active_only:
            filters.append(broker_accounts.c.is_active.is_(True))
        stmt = select(broker_accounts).where(*filters).order_by(broker_accounts.c.display_name)
        with self.engine.connect() as conn:
            return [_account_from_row(row._mapping) for row in conn.execute(stmt)]

    def account_by_id(self, account_id: str) -> BrokerAccount | None:
        stmt = select(broker_accounts).where(broker_accounts.c.id == account_id).limit(1)
        with self.engine.connect() as conn:
            row = conn.execute(stmt).first()
        return _account_from_row(row._mapping) if row is not None else None

    def account_by_hash(self, account_hash: str) -> BrokerAccount | None:
        stmt = (
            select(broker_accounts)
            .where(broker_accounts.c.account_hash == account_hash)
            .limit(1)
        )
        with self.engine.connect() as conn:
            row = conn.execute(stmt).first()
        return _account_from_row(row._mapping) if row is not None else None

    def upsert_balance_snapshots(self, rows: Iterable[BrokerBalanceSnapshot]) -> int:
        count = 0
        with self.engine.begin() as conn:
            for row in rows:
                conn.execute(
                    delete(broker_balance_snapshots).where(
                        broker_balance_snapshots.c.id == row.id
                    )
                )
                conn.execute(insert(broker_balance_snapshots).values(**_balance_row(row)))
                count += 1
        return count

    def latest_balance(self, *, account_id: str | None = None) -> BrokerBalanceSnapshot | None:
        filters = []
        if account_id is not None:
            filters.append(broker_balance_snapshots.c.account_id == account_id)
        stmt = (
            select(broker_balance_snapshots)
            .where(*filters)
            .order_by(
                broker_balance_snapshots.c.as_of.desc(),
                broker_balance_snapshots.c.created_at.desc(),
                broker_balance_snapshots.c.id.desc(),
            )
            .limit(1)
        )
        with self.engine.connect() as conn:
            row = conn.execute(stmt).first()
        return _balance_from_row(row._mapping) if row is not None else None

    def upsert_positions(self, rows: Iterable[BrokerPosition]) -> int:
        count = 0
        with self.engine.begin() as conn:
            for row in rows:
                conn.execute(delete(broker_positions).where(broker_positions.c.id == row.id))
                conn.execute(insert(broker_positions).values(**_position_row(row)))
                count += 1
        return count

    def replace_positions(
        self,
        account_id: str,
        as_of: datetime,
        rows: Iterable[BrokerPosition],
    ) -> int:
        normalized_as_of = _as_utc(as_of)
        position_rows = list(rows)
        snapshot = BrokerPositionSnapshot(
            id=broker_position_snapshot_id(account_id, normalized_as_of),
            account_id=account_id,
            as_of=normalized_as_of,
            position_count=len(position_rows),
            raw_payload={"source": "schwab", "position_count": len(position_rows)},
            created_at=normalized_as_of,
        )
        with self.engine.begin() as conn:
            conn.execute(
                delete(broker_positions).where(
                    broker_positions.c.account_id == account_id,
                    broker_positions.c.as_of == normalized_as_of,
                )
            )
            conn.execute(
                delete(broker_position_snapshots).where(
                    broker_position_snapshots.c.id == snapshot.id
                )
            )
            conn.execute(
                insert(broker_position_snapshots).values(**_position_snapshot_row(snapshot))
            )
            for row in position_rows:
                conn.execute(insert(broker_positions).values(**_position_row(row)))
        return len(position_rows)

    def latest_positions(self, *, account_id: str | None = None) -> list[BrokerPosition]:
        latest_as_of = self.latest_positions_as_of(account_id=account_id)
        if latest_as_of is None:
            return []
        filters = [broker_positions.c.as_of == latest_as_of]
        if account_id is not None:
            filters.append(broker_positions.c.account_id == account_id)
        stmt = select(broker_positions).where(*filters).order_by(broker_positions.c.ticker)
        with self.engine.connect() as conn:
            return [_position_from_row(row._mapping) for row in conn.execute(stmt)]

    def latest_positions_as_of(self, *, account_id: str | None = None) -> datetime | None:
        snapshot_filters = []
        if account_id is not None:
            snapshot_filters.append(broker_position_snapshots.c.account_id == account_id)
        snapshot_stmt = select(func.max(broker_position_snapshots.c.as_of)).where(
            *snapshot_filters
        )
        with self.engine.connect() as conn:
            snapshot_value = conn.execute(snapshot_stmt).scalar_one_or_none()
        if isinstance(snapshot_value, datetime):
            return _as_utc(snapshot_value)

        filters = []
        if account_id is not None:
            filters.append(broker_positions.c.account_id == account_id)
        stmt = select(func.max(broker_positions.c.as_of)).where(*filters)
        with self.engine.connect() as conn:
            value = conn.execute(stmt).scalar_one_or_none()
        return _as_utc(value) if isinstance(value, datetime) else None

    def upsert_orders(self, rows: Iterable[BrokerOrder]) -> int:
        count = 0
        with self.engine.begin() as conn:
            for row in rows:
                conn.execute(delete(broker_orders).where(broker_orders.c.id == row.id))
                conn.execute(insert(broker_orders).values(**_order_row(row)))
                count += 1
        return count

    def replace_open_orders(self, account_id: str, rows: Iterable[BrokerOrder]) -> int:
        count = 0
        with self.engine.begin() as conn:
            conn.execute(delete(broker_orders).where(broker_orders.c.account_id == account_id))
            for row in rows:
                conn.execute(insert(broker_orders).values(**_order_row(row)))
                count += 1
        return count

    def list_open_orders(self, *, account_id: str | None = None) -> list[BrokerOrder]:
        filters = [broker_orders.c.status.in_(OPEN_ORDER_STATUSES)]
        if account_id is not None:
            filters.append(broker_orders.c.account_id == account_id)
        stmt = (
            select(broker_orders)
            .where(*filters)
            .order_by(broker_orders.c.submitted_at.desc(), broker_orders.c.id.desc())
        )
        with self.engine.connect() as conn:
            return [_order_from_row(row._mapping) for row in conn.execute(stmt)]

    def upsert_opportunity_action(
        self,
        action: BrokerOpportunityAction,
    ) -> BrokerOpportunityAction:
        with self.engine.begin() as conn:
            conn.execute(
                delete(broker_opportunity_actions).where(
                    broker_opportunity_actions.c.id == action.id
                )
            )
            conn.execute(
                insert(broker_opportunity_actions).values(**_opportunity_action_row(action))
            )
        return action

    def list_opportunity_actions(
        self,
        *,
        ticker: str | None = None,
        limit: int = 100,
    ) -> list[BrokerOpportunityAction]:
        filters = []
        if ticker is not None:
            filters.append(broker_opportunity_actions.c.ticker == ticker.upper())
        stmt = (
            select(broker_opportunity_actions)
            .where(*filters)
            .order_by(
                broker_opportunity_actions.c.created_at.desc(),
                broker_opportunity_actions.c.id.desc(),
            )
            .limit(limit)
        )
        with self.engine.connect() as conn:
            return [_opportunity_action_from_row(row._mapping) for row in conn.execute(stmt)]

    def upsert_market_snapshots(self, rows: Iterable[BrokerMarketSnapshot]) -> int:
        count = 0
        with self.engine.begin() as conn:
            for row in rows:
                conn.execute(
                    delete(broker_market_snapshots).where(broker_market_snapshots.c.id == row.id)
                )
                conn.execute(insert(broker_market_snapshots).values(**_market_snapshot_row(row)))
                count += 1
        return count

    def latest_market_snapshots(
        self,
        *,
        tickers: Iterable[str] | None = None,
        limit: int = 100,
    ) -> list[BrokerMarketSnapshot]:
        requested = [ticker.upper() for ticker in tickers or [] if ticker]
        filters = []
        if requested:
            filters.append(broker_market_snapshots.c.ticker.in_(requested))
        stmt = (
            select(broker_market_snapshots)
            .where(*filters)
            .order_by(
                broker_market_snapshots.c.ticker.asc(),
                broker_market_snapshots.c.as_of.desc(),
                broker_market_snapshots.c.id.desc(),
            )
        )
        latest: dict[str, BrokerMarketSnapshot] = {}
        with self.engine.connect() as conn:
            for row in conn.execute(stmt):
                snapshot = _market_snapshot_from_row(row._mapping)
                latest.setdefault(snapshot.ticker, snapshot)
                if len(latest) >= limit and not requested:
                    break
        return list(latest.values())

    def latest_market_snapshot(self, ticker: str) -> BrokerMarketSnapshot | None:
        snapshots = self.latest_market_snapshots(tickers=[ticker], limit=1)
        return snapshots[0] if snapshots else None

    def upsert_trigger(self, trigger: BrokerTrigger) -> BrokerTrigger:
        with self.engine.begin() as conn:
            conn.execute(delete(broker_triggers).where(broker_triggers.c.id == trigger.id))
            conn.execute(insert(broker_triggers).values(**_trigger_row(trigger)))
        return trigger

    def list_triggers(
        self,
        *,
        ticker: str | None = None,
        active_only: bool = False,
        limit: int = 100,
    ) -> list[BrokerTrigger]:
        filters = []
        if ticker is not None:
            filters.append(broker_triggers.c.ticker == ticker.upper())
        if active_only:
            filters.append(broker_triggers.c.status == BrokerTriggerStatus.ACTIVE.value)
        stmt = (
            select(broker_triggers)
            .where(*filters)
            .order_by(broker_triggers.c.created_at.desc(), broker_triggers.c.id.desc())
            .limit(limit)
        )
        with self.engine.connect() as conn:
            return [_trigger_from_row(row._mapping) for row in conn.execute(stmt)]

    def upsert_order_ticket(self, ticket: BrokerOrderTicket) -> BrokerOrderTicket:
        with self.engine.begin() as conn:
            conn.execute(delete(broker_order_tickets).where(broker_order_tickets.c.id == ticket.id))
            conn.execute(insert(broker_order_tickets).values(**_order_ticket_row(ticket)))
        return ticket

    def list_order_tickets(
        self,
        *,
        ticker: str | None = None,
        limit: int = 100,
    ) -> list[BrokerOrderTicket]:
        filters = []
        if ticker is not None:
            filters.append(broker_order_tickets.c.ticker == ticker.upper())
        stmt = (
            select(broker_order_tickets)
            .where(*filters)
            .order_by(
                broker_order_tickets.c.created_at.desc(),
                broker_order_tickets.c.id.desc(),
            )
            .limit(limit)
        )
        with self.engine.connect() as conn:
            return [_order_ticket_from_row(row._mapping) for row in conn.execute(stmt)]


def _connection_row(connection: BrokerConnection) -> dict[str, Any]:
    return {
        "id": connection.id,
        "broker": connection.broker,
        "user_id": connection.user_id,
        "status": connection.status.value,
        "created_at": connection.created_at,
        "updated_at": connection.updated_at,
        "last_successful_sync_at": connection.last_successful_sync_at,
        "metadata": thaw_json_value(connection.metadata),
    }


def _token_row(token: BrokerToken) -> dict[str, Any]:
    return {
        "id": token.id,
        "connection_id": token.connection_id,
        "access_token_encrypted": token.access_token_encrypted,
        "refresh_token_encrypted": token.refresh_token_encrypted,
        "access_token_expires_at": token.access_token_expires_at,
        "refresh_token_expires_at": token.refresh_token_expires_at,
        "created_at": token.created_at,
        "updated_at": token.updated_at,
    }


def _account_row(account: BrokerAccount) -> dict[str, Any]:
    return {
        "id": account.id,
        "connection_id": account.connection_id,
        "broker": account.broker,
        "broker_account_id": account.broker_account_id,
        "account_hash": account.account_hash,
        "account_type": account.account_type,
        "display_name": account.display_name,
        "is_active": account.is_active,
        "created_at": account.created_at,
        "updated_at": account.updated_at,
    }


def _balance_row(row: BrokerBalanceSnapshot) -> dict[str, Any]:
    return {
        "id": row.id,
        "account_id": row.account_id,
        "as_of": row.as_of,
        "cash": row.cash,
        "buying_power": row.buying_power,
        "liquidation_value": row.liquidation_value,
        "equity": row.equity,
        "raw_payload": thaw_json_value(row.raw_payload),
        "created_at": row.created_at,
    }


def _position_row(row: BrokerPosition) -> dict[str, Any]:
    return {
        "id": row.id,
        "account_id": row.account_id,
        "as_of": row.as_of,
        "ticker": row.ticker,
        "quantity": row.quantity,
        "average_price": row.average_price,
        "market_value": row.market_value,
        "unrealized_pnl": row.unrealized_pnl,
        "sector": row.sector,
        "theme": row.theme,
        "raw_payload": thaw_json_value(row.raw_payload),
        "created_at": row.created_at,
    }


def _position_snapshot_row(row: BrokerPositionSnapshot) -> dict[str, Any]:
    return {
        "id": row.id,
        "account_id": row.account_id,
        "as_of": row.as_of,
        "position_count": row.position_count,
        "raw_payload": thaw_json_value(row.raw_payload),
        "created_at": row.created_at,
    }


def _order_row(row: BrokerOrder) -> dict[str, Any]:
    return {
        "id": row.id,
        "account_id": row.account_id,
        "broker_order_id": row.broker_order_id,
        "ticker": row.ticker,
        "side": row.side,
        "order_type": row.order_type,
        "quantity": row.quantity,
        "limit_price": row.limit_price,
        "status": row.status,
        "submitted_at": row.submitted_at,
        "raw_payload": thaw_json_value(row.raw_payload),
        "created_at": row.created_at,
    }


def _opportunity_action_row(row: BrokerOpportunityAction) -> dict[str, Any]:
    return {
        "id": row.id,
        "ticker": row.ticker,
        "action": row.action.value,
        "status": row.status,
        "thesis": row.thesis,
        "notes": row.notes,
        "payload": thaw_json_value(row.payload),
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def _market_snapshot_row(row: BrokerMarketSnapshot) -> dict[str, Any]:
    return {
        "id": row.id,
        "ticker": row.ticker,
        "as_of": row.as_of,
        "last_price": row.last_price,
        "bid_price": row.bid_price,
        "ask_price": row.ask_price,
        "mark_price": row.mark_price,
        "day_change_percent": row.day_change_percent,
        "total_volume": row.total_volume,
        "relative_volume": row.relative_volume,
        "high_52_week": row.high_52_week,
        "low_52_week": row.low_52_week,
        "price_trend_5d_percent": row.price_trend_5d_percent,
        "option_call_put_ratio": row.option_call_put_ratio,
        "option_iv_percentile": row.option_iv_percentile,
        "raw_payload": thaw_json_value(row.raw_payload),
        "created_at": row.created_at,
    }


def _trigger_row(row: BrokerTrigger) -> dict[str, Any]:
    return {
        "id": row.id,
        "ticker": row.ticker,
        "trigger_type": row.trigger_type,
        "operator": row.operator,
        "threshold": row.threshold,
        "latest_value": row.latest_value,
        "status": row.status.value,
        "notes": row.notes,
        "payload": thaw_json_value(row.payload),
        "created_at": row.created_at,
        "updated_at": row.updated_at,
        "fired_at": row.fired_at,
    }


def _order_ticket_row(row: BrokerOrderTicket) -> dict[str, Any]:
    return {
        "id": row.id,
        "ticker": row.ticker,
        "side": row.side,
        "quantity": row.quantity,
        "limit_price": row.limit_price,
        "stop_price": row.stop_price,
        "invalidation_price": row.invalidation_price,
        "risk_budget": row.risk_budget,
        "status": row.status.value,
        "submission_allowed": row.submission_allowed,
        "notes": row.notes,
        "preview_payload": thaw_json_value(row.preview_payload),
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def _connection_from_row(row: Mapping[str, Any]) -> BrokerConnection:
    return BrokerConnection(
        id=row["id"],
        broker=row["broker"],
        user_id=row["user_id"],
        status=BrokerConnectionStatus(row["status"]),
        created_at=_as_utc(row["created_at"]),
        updated_at=_as_utc(row["updated_at"]),
        last_successful_sync_at=(
            _as_utc(row["last_successful_sync_at"])
            if row["last_successful_sync_at"] is not None
            else None
        ),
        metadata=row["metadata"],
    )


def _token_from_row(row: Mapping[str, Any]) -> BrokerToken:
    return BrokerToken(
        id=row["id"],
        connection_id=row["connection_id"],
        access_token_encrypted=row["access_token_encrypted"],
        refresh_token_encrypted=row["refresh_token_encrypted"],
        access_token_expires_at=_as_utc(row["access_token_expires_at"]),
        refresh_token_expires_at=(
            _as_utc(row["refresh_token_expires_at"])
            if row["refresh_token_expires_at"] is not None
            else None
        ),
        created_at=_as_utc(row["created_at"]),
        updated_at=_as_utc(row["updated_at"]),
    )


def _account_from_row(row: Mapping[str, Any]) -> BrokerAccount:
    return BrokerAccount(
        id=row["id"],
        connection_id=row["connection_id"],
        broker=row["broker"],
        broker_account_id=row["broker_account_id"],
        account_hash=row["account_hash"],
        account_type=row["account_type"],
        display_name=row["display_name"],
        is_active=bool(row["is_active"]),
        created_at=_as_utc(row["created_at"]),
        updated_at=_as_utc(row["updated_at"]),
    )


def _balance_from_row(row: Mapping[str, Any]) -> BrokerBalanceSnapshot:
    return BrokerBalanceSnapshot(
        id=row["id"],
        account_id=row["account_id"],
        as_of=_as_utc(row["as_of"]),
        cash=row["cash"],
        buying_power=row["buying_power"],
        liquidation_value=row["liquidation_value"],
        equity=row["equity"],
        raw_payload=row["raw_payload"],
        created_at=_as_utc(row["created_at"]),
    )


def _position_from_row(row: Mapping[str, Any]) -> BrokerPosition:
    return BrokerPosition(
        id=row["id"],
        account_id=row["account_id"],
        as_of=_as_utc(row["as_of"]),
        ticker=row["ticker"],
        quantity=row["quantity"],
        average_price=row["average_price"],
        market_value=row["market_value"],
        unrealized_pnl=row["unrealized_pnl"],
        sector=row["sector"],
        theme=row["theme"],
        raw_payload=row["raw_payload"],
        created_at=_as_utc(row["created_at"]),
    )


def _order_from_row(row: Mapping[str, Any]) -> BrokerOrder:
    return BrokerOrder(
        id=row["id"],
        account_id=row["account_id"],
        broker_order_id=row["broker_order_id"],
        ticker=row["ticker"],
        side=row["side"],
        order_type=row["order_type"],
        quantity=row["quantity"],
        limit_price=row["limit_price"],
        status=row["status"],
        submitted_at=_as_utc(row["submitted_at"]) if row["submitted_at"] is not None else None,
        raw_payload=row["raw_payload"],
        created_at=_as_utc(row["created_at"]),
    )


def _opportunity_action_from_row(row: Mapping[str, Any]) -> BrokerOpportunityAction:
    return BrokerOpportunityAction(
        id=row["id"],
        ticker=row["ticker"],
        action=row["action"],
        status=row["status"],
        thesis=row["thesis"],
        notes=row["notes"],
        payload=row["payload"],
        created_at=_as_utc(row["created_at"]),
        updated_at=_as_utc(row["updated_at"]),
    )


def _market_snapshot_from_row(row: Mapping[str, Any]) -> BrokerMarketSnapshot:
    return BrokerMarketSnapshot(
        id=row["id"],
        ticker=row["ticker"],
        as_of=_as_utc(row["as_of"]),
        last_price=row["last_price"],
        bid_price=row["bid_price"],
        ask_price=row["ask_price"],
        mark_price=row["mark_price"],
        day_change_percent=row["day_change_percent"],
        total_volume=row["total_volume"],
        relative_volume=row["relative_volume"],
        high_52_week=row["high_52_week"],
        low_52_week=row["low_52_week"],
        price_trend_5d_percent=row["price_trend_5d_percent"],
        option_call_put_ratio=row["option_call_put_ratio"],
        option_iv_percentile=row["option_iv_percentile"],
        raw_payload=row["raw_payload"],
        created_at=_as_utc(row["created_at"]),
    )


def _trigger_from_row(row: Mapping[str, Any]) -> BrokerTrigger:
    return BrokerTrigger(
        id=row["id"],
        ticker=row["ticker"],
        trigger_type=row["trigger_type"],
        operator=row["operator"],
        threshold=row["threshold"],
        latest_value=row["latest_value"],
        status=row["status"],
        notes=row["notes"],
        payload=row["payload"],
        created_at=_as_utc(row["created_at"]),
        updated_at=_as_utc(row["updated_at"]),
        fired_at=_as_utc(row["fired_at"]) if row["fired_at"] is not None else None,
    )


def _order_ticket_from_row(row: Mapping[str, Any]) -> BrokerOrderTicket:
    return BrokerOrderTicket(
        id=row["id"],
        ticker=row["ticker"],
        side=row["side"],
        quantity=row["quantity"],
        limit_price=row["limit_price"],
        stop_price=row["stop_price"],
        invalidation_price=row["invalidation_price"],
        risk_budget=row["risk_budget"],
        status=row["status"],
        submission_allowed=bool(row["submission_allowed"]),
        notes=row["notes"],
        preview_payload=row["preview_payload"],
        created_at=_as_utc(row["created_at"]),
        updated_at=_as_utc(row["updated_at"]),
    )


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


__all__ = ["BrokerRepository"]

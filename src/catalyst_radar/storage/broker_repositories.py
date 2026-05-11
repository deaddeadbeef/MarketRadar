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
    BrokerOrder,
    BrokerPosition,
    BrokerPositionSnapshot,
    BrokerToken,
    broker_position_snapshot_id,
)
from catalyst_radar.core.immutability import thaw_json_value
from catalyst_radar.storage.schema import (
    broker_accounts,
    broker_balance_snapshots,
    broker_connections,
    broker_orders,
    broker_position_snapshots,
    broker_positions,
    broker_tokens,
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
            conn.execute(insert(broker_position_snapshots).values(**_position_snapshot_row(snapshot)))
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


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


__all__ = ["BrokerRepository"]

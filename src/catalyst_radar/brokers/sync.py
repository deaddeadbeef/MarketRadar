from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

from catalyst_radar.brokers.models import (
    LOCAL_USER_ID,
    SCHWAB_BROKER,
    BrokerAccount,
    BrokerBalanceSnapshot,
    BrokerConnection,
    BrokerConnectionStatus,
    BrokerOrder,
    BrokerPosition,
    BrokerSyncResult,
    broker_account_id,
    broker_balance_snapshot_id,
    broker_connection_id,
    broker_order_id,
    broker_position_id,
)
from catalyst_radar.brokers.schwab import SchwabClient
from catalyst_radar.core.models import HoldingSnapshot
from catalyst_radar.storage.broker_repositories import BrokerRepository
from catalyst_radar.storage.repositories import MarketRepository


def sync_schwab_read_only(
    *,
    client: SchwabClient,
    broker_repo: BrokerRepository,
    market_repo: MarketRepository | None = None,
    now: datetime | None = None,
    user_id: str = LOCAL_USER_ID,
) -> BrokerSyncResult:
    synced_at = (now or datetime.now(UTC)).astimezone(UTC)
    connection_id = broker_connection_id(SCHWAB_BROKER, user_id)
    previous = broker_repo.latest_connection(SCHWAB_BROKER, user_id=user_id)
    balances: list[BrokerBalanceSnapshot] = []
    positions: list[BrokerPosition] = []
    open_orders: list[BrokerOrder] = []
    holdings: list[HoldingSnapshot] = []
    previous_holding_tickers = (
        {holding.ticker for holding in market_repo.list_holdings()}
        if market_repo is not None
        else set()
    )
    positions_by_account: dict[str, list[BrokerPosition]] = {}
    orders_by_account: dict[str, list[BrokerOrder]] = {}
    try:
        account_number_rows = client.get_account_numbers()
        account_detail_rows = client.get_accounts_with_positions()
        accounts = _accounts_from_schwab(
            connection_id=connection_id,
            account_number_rows=account_number_rows,
            detail_rows=account_detail_rows,
            synced_at=synced_at,
        )
        detail_by_hash = {
            _account_hash_from_detail(row): row
            for row in account_detail_rows
            if _account_hash_from_detail(row)
        }
        for account in accounts:
            detail = detail_by_hash.get(account.account_hash, {})
            securities_account = _mapping(detail.get("securitiesAccount"))
            balance_payload = _mapping(securities_account.get("currentBalances"))
            balance = _balance_from_schwab(account, balance_payload, synced_at)
            balances.append(balance)
            account_positions = _positions_from_schwab(
                account,
                _records(securities_account.get("positions")),
                synced_at,
            )
            positions_by_account[account.id] = account_positions
            positions.extend(account_positions)
            holdings.extend(_holdings_from_positions(account_positions, balance))
            account_open_orders = [
                _order_from_schwab(account, order_payload, synced_at)
                for order_payload in client.get_open_orders(account.account_hash, now=synced_at)
            ]
            orders_by_account[account.id] = account_open_orders
            open_orders.extend(account_open_orders)
    except Exception as exc:
        _record_sync_error(
            broker_repo=broker_repo,
            connection_id=connection_id,
            previous=previous,
            synced_at=synced_at,
            error=exc,
            user_id=user_id,
        )
        raise

    created_at = previous.created_at if previous is not None else synced_at
    connection = BrokerConnection(
        id=connection_id,
        broker=SCHWAB_BROKER,
        user_id=user_id,
        status=BrokerConnectionStatus.CONNECTED,
        created_at=created_at,
        updated_at=synced_at,
        last_successful_sync_at=synced_at,
        metadata={"mode": "read_only", "source": "schwab"},
    )
    broker_repo.upsert_connection(connection)
    broker_repo.upsert_accounts(accounts)
    broker_repo.upsert_balance_snapshots(balances)
    for account in accounts:
        broker_repo.replace_positions(account.id, synced_at, positions_by_account[account.id])
        broker_repo.replace_open_orders(account.id, orders_by_account[account.id])
    if market_repo is not None:
        latest_balance = max(balances, key=lambda row: row.equity, default=None)
        market_repo.replace_holdings_snapshot(
            as_of=synced_at,
            rows=holdings,
            previous_tickers=previous_holding_tickers,
            portfolio_value=latest_balance.equity if latest_balance is not None else 0.0,
            cash=latest_balance.cash if latest_balance is not None else 0.0,
        )
    return BrokerSyncResult(
        connection_id=connection_id,
        account_count=len(accounts),
        balance_count=len(balances),
        position_count=len(positions),
        open_order_count=len(open_orders),
        synced_at=synced_at,
    )


def _record_sync_error(
    *,
    broker_repo: BrokerRepository,
    connection_id: str,
    previous: BrokerConnection | None,
    synced_at: datetime,
    error: Exception,
    user_id: str,
) -> None:
    broker_repo.upsert_connection(
        BrokerConnection(
            id=connection_id,
            broker=SCHWAB_BROKER,
            user_id=user_id,
            status=BrokerConnectionStatus.ERROR,
            created_at=previous.created_at if previous is not None else synced_at,
            updated_at=synced_at,
            last_successful_sync_at=(
                previous.last_successful_sync_at if previous is not None else None
            ),
            metadata={
                "mode": "read_only",
                "source": "schwab",
                "last_error": str(error),
                "error_type": type(error).__name__,
            },
        )
    )


def _accounts_from_schwab(
    *,
    connection_id: str,
    account_number_rows: Sequence[Mapping[str, Any]],
    detail_rows: Sequence[Mapping[str, Any]],
    synced_at: datetime,
) -> list[BrokerAccount]:
    detail_by_hash = {
        _account_hash_from_detail(row): _mapping(row.get("securitiesAccount"))
        for row in detail_rows
        if _account_hash_from_detail(row)
    }
    accounts = []
    for row in account_number_rows:
        broker_account_id = str(row.get("accountNumber") or "").strip()
        account_hash = str(row.get("hashValue") or "").strip()
        if not broker_account_id or not account_hash:
            continue
        detail = detail_by_hash.get(account_hash, {})
        account_type = _optional_text(detail.get("type"))
        accounts.append(
            BrokerAccount(
                id=broker_account_id_fn(SCHWAB_BROKER, account_hash),
                connection_id=connection_id,
                broker=SCHWAB_BROKER,
                broker_account_id=broker_account_id,
                account_hash=account_hash,
                account_type=account_type,
                display_name=_display_name(broker_account_id, account_type),
                is_active=True,
                created_at=synced_at,
                updated_at=synced_at,
            )
        )
    return accounts


def broker_account_id_fn(broker: str, account_hash: str) -> str:
    return broker_account_id(broker, account_hash)


def _balance_from_schwab(
    account: BrokerAccount,
    payload: Mapping[str, Any],
    synced_at: datetime,
) -> BrokerBalanceSnapshot:
    equity = _float(
        _first_present(
            payload.get("liquidationValue"),
            payload.get("accountValue"),
            payload.get("equity"),
        )
    )
    return BrokerBalanceSnapshot(
        id=broker_balance_snapshot_id(account.id, synced_at),
        account_id=account.id,
        as_of=synced_at,
        cash=_float(_first_present(payload.get("cashBalance"), payload.get("cash"))),
        buying_power=_float(
            _first_present(payload.get("buyingPower"), payload.get("stockBuyingPower"))
        ),
        liquidation_value=equity,
        equity=equity,
        raw_payload=dict(payload),
        created_at=synced_at,
    )


def _positions_from_schwab(
    account: BrokerAccount,
    rows: Sequence[Mapping[str, Any]],
    synced_at: datetime,
) -> list[BrokerPosition]:
    positions = []
    for row in rows:
        instrument = _mapping(row.get("instrument"))
        ticker = _optional_text(instrument.get("symbol"))
        if ticker is None:
            continue
        positions.append(
            BrokerPosition(
                id=broker_position_id(account.id, ticker, synced_at),
                account_id=account.id,
                as_of=synced_at,
                ticker=ticker,
                quantity=_float(
                    _first_present(
                        row.get("longQuantity"),
                        row.get("shortQuantity"),
                        row.get("quantity"),
                    )
                ),
                average_price=_optional_float(row.get("averagePrice")),
                market_value=_float(row.get("marketValue")),
                unrealized_pnl=_optional_float(
                    _first_present(row.get("longOpenProfitLoss"), row.get("currentDayProfitLoss"))
                ),
                sector=str(instrument.get("sector") or "unclassified"),
                theme=str(instrument.get("theme") or "broker_synced"),
                raw_payload=dict(row),
                created_at=synced_at,
            )
        )
    return positions


def _order_from_schwab(
    account: BrokerAccount,
    payload: Mapping[str, Any],
    synced_at: datetime,
) -> BrokerOrder:
    legs = _records(payload.get("orderLegCollection"))
    first_leg = legs[0] if legs else {}
    instrument = _mapping(first_leg.get("instrument"))
    entered_time = _parse_datetime(payload.get("enteredTime"))
    broker_id = _optional_text(_first_present(payload.get("orderId"), payload.get("order_id")))
    return BrokerOrder(
        id=broker_order_id(account.id, broker_id, payload),
        account_id=account.id,
        broker_order_id=broker_id,
        ticker=_optional_text(instrument.get("symbol")),
        side=_optional_text(first_leg.get("instruction")),
        order_type=_optional_text(payload.get("orderType")),
        quantity=_optional_float(
            _first_present(first_leg.get("quantity"), payload.get("quantity"))
        ),
        limit_price=_optional_float(payload.get("price")),
        status=str(payload.get("status") or "UNKNOWN"),
        submitted_at=entered_time,
        raw_payload=dict(payload),
        created_at=synced_at,
    )


def _holdings_from_positions(
    positions: Sequence[BrokerPosition],
    balance: BrokerBalanceSnapshot,
) -> list[HoldingSnapshot]:
    return [
        HoldingSnapshot(
            ticker=position.ticker,
            shares=position.quantity,
            market_value=position.market_value,
            sector=position.sector,
            theme=position.theme,
            as_of=position.as_of,
            portfolio_value=balance.equity,
            cash=balance.cash,
        )
        for position in positions
    ]


def _account_hash_from_detail(row: Mapping[str, Any]) -> str | None:
    securities_account = _mapping(row.get("securitiesAccount"))
    return _optional_text(
        _first_present(
            row.get("hashValue"),
            securities_account.get("accountHash"),
            securities_account.get("encryptedAccountNumber"),
        )
    )


def _display_name(account_id: str, account_type: str | None) -> str:
    suffix = account_id[-4:] if len(account_id) >= 4 else account_id
    prefix = account_type or "Schwab account"
    return f"{prefix} ending {suffix}"


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _records(value: object) -> list[Mapping[str, Any]]:
    return [row for row in value if isinstance(row, Mapping)] if isinstance(value, list) else []


def _first_present(*values: object) -> object:
    for value in values:
        if value not in (None, ""):
            return value
    return None


def _float(value: object) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _optional_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    return _float(value)


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _parse_datetime(value: object) -> datetime | None:
    text = _optional_text(value)
    if text is None:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)

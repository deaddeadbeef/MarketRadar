from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from catalyst_radar.core.immutability import freeze_json_value

SCHWAB_BROKER = "schwab"
LOCAL_USER_ID = "local"


class BrokerConnectionStatus(StrEnum):
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    NEEDS_AUTH = "needs_auth"
    ERROR = "error"


@dataclass(frozen=True)
class BrokerConnection:
    id: str
    broker: str
    user_id: str
    status: BrokerConnectionStatus
    created_at: datetime
    updated_at: datetime
    last_successful_sync_at: datetime | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "broker", _required_text(self.broker, "broker"))
        object.__setattr__(self, "user_id", _required_text(self.user_id, "user_id"))
        object.__setattr__(self, "status", BrokerConnectionStatus(self.status))
        object.__setattr__(self, "created_at", _to_utc_datetime(self.created_at))
        object.__setattr__(self, "updated_at", _to_utc_datetime(self.updated_at))
        if self.last_successful_sync_at is not None:
            object.__setattr__(
                self,
                "last_successful_sync_at",
                _to_utc_datetime(self.last_successful_sync_at),
            )
        object.__setattr__(self, "metadata", freeze_json_value(dict(self.metadata)))


@dataclass(frozen=True)
class BrokerToken:
    id: str
    connection_id: str
    access_token_encrypted: str
    access_token_expires_at: datetime
    created_at: datetime
    updated_at: datetime
    refresh_token_encrypted: str | None = None
    refresh_token_expires_at: datetime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "connection_id", _required_text(self.connection_id, "connection_id")
        )
        object.__setattr__(
            self,
            "access_token_encrypted",
            _required_text(self.access_token_encrypted, "access_token_encrypted"),
        )
        object.__setattr__(
            self,
            "refresh_token_encrypted",
            _optional_text(self.refresh_token_encrypted),
        )
        object.__setattr__(
            self,
            "access_token_expires_at",
            _to_utc_datetime(self.access_token_expires_at),
        )
        if self.refresh_token_expires_at is not None:
            object.__setattr__(
                self,
                "refresh_token_expires_at",
                _to_utc_datetime(self.refresh_token_expires_at),
            )
        object.__setattr__(self, "created_at", _to_utc_datetime(self.created_at))
        object.__setattr__(self, "updated_at", _to_utc_datetime(self.updated_at))


@dataclass(frozen=True)
class BrokerAccount:
    id: str
    connection_id: str
    broker: str
    broker_account_id: str
    account_hash: str
    created_at: datetime
    updated_at: datetime
    account_type: str | None = None
    display_name: str | None = None
    is_active: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "connection_id", _required_text(self.connection_id, "connection_id")
        )
        object.__setattr__(self, "broker", _required_text(self.broker, "broker"))
        object.__setattr__(
            self,
            "broker_account_id",
            _required_text(self.broker_account_id, "broker_account_id"),
        )
        object.__setattr__(
            self, "account_hash", _required_text(self.account_hash, "account_hash")
        )
        object.__setattr__(self, "account_type", _optional_text(self.account_type))
        object.__setattr__(self, "display_name", _optional_text(self.display_name))
        object.__setattr__(self, "created_at", _to_utc_datetime(self.created_at))
        object.__setattr__(self, "updated_at", _to_utc_datetime(self.updated_at))


@dataclass(frozen=True)
class BrokerBalanceSnapshot:
    id: str
    account_id: str
    as_of: datetime
    cash: float
    buying_power: float
    liquidation_value: float
    equity: float
    raw_payload: Mapping[str, Any]
    created_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "account_id", _required_text(self.account_id, "account_id"))
        object.__setattr__(self, "as_of", _to_utc_datetime(self.as_of))
        for field_name in ("cash", "buying_power", "liquidation_value", "equity"):
            object.__setattr__(self, field_name, _float(getattr(self, field_name)))
        object.__setattr__(self, "raw_payload", freeze_json_value(dict(self.raw_payload)))
        object.__setattr__(self, "created_at", _to_utc_datetime(self.created_at))


@dataclass(frozen=True)
class BrokerPosition:
    id: str
    account_id: str
    as_of: datetime
    ticker: str
    quantity: float
    market_value: float
    raw_payload: Mapping[str, Any]
    created_at: datetime
    average_price: float | None = None
    unrealized_pnl: float | None = None
    sector: str = "unclassified"
    theme: str = "broker_synced"

    def __post_init__(self) -> None:
        object.__setattr__(self, "account_id", _required_text(self.account_id, "account_id"))
        object.__setattr__(self, "as_of", _to_utc_datetime(self.as_of))
        object.__setattr__(self, "ticker", _required_text(self.ticker, "ticker").upper())
        object.__setattr__(self, "quantity", _float(self.quantity))
        object.__setattr__(self, "market_value", _float(self.market_value))
        object.__setattr__(self, "average_price", _optional_float(self.average_price))
        object.__setattr__(self, "unrealized_pnl", _optional_float(self.unrealized_pnl))
        object.__setattr__(self, "sector", _required_text(self.sector, "sector"))
        object.__setattr__(self, "theme", _required_text(self.theme, "theme"))
        object.__setattr__(self, "raw_payload", freeze_json_value(dict(self.raw_payload)))
        object.__setattr__(self, "created_at", _to_utc_datetime(self.created_at))


@dataclass(frozen=True)
class BrokerPositionSnapshot:
    id: str
    account_id: str
    as_of: datetime
    position_count: int
    raw_payload: Mapping[str, Any]
    created_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "account_id", _required_text(self.account_id, "account_id"))
        object.__setattr__(self, "as_of", _to_utc_datetime(self.as_of))
        object.__setattr__(self, "position_count", int(self.position_count))
        object.__setattr__(self, "raw_payload", freeze_json_value(dict(self.raw_payload)))
        object.__setattr__(self, "created_at", _to_utc_datetime(self.created_at))


@dataclass(frozen=True)
class BrokerOrder:
    id: str
    account_id: str
    status: str
    raw_payload: Mapping[str, Any]
    created_at: datetime
    broker_order_id: str | None = None
    ticker: str | None = None
    side: str | None = None
    order_type: str | None = None
    quantity: float | None = None
    limit_price: float | None = None
    submitted_at: datetime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "account_id", _required_text(self.account_id, "account_id"))
        object.__setattr__(self, "status", _required_text(self.status, "status"))
        object.__setattr__(self, "broker_order_id", _optional_text(self.broker_order_id))
        object.__setattr__(self, "ticker", _optional_ticker(self.ticker))
        object.__setattr__(self, "side", _optional_text(self.side))
        object.__setattr__(self, "order_type", _optional_text(self.order_type))
        object.__setattr__(self, "quantity", _optional_float(self.quantity))
        object.__setattr__(self, "limit_price", _optional_float(self.limit_price))
        if self.submitted_at is not None:
            object.__setattr__(self, "submitted_at", _to_utc_datetime(self.submitted_at))
        object.__setattr__(self, "raw_payload", freeze_json_value(dict(self.raw_payload)))
        object.__setattr__(self, "created_at", _to_utc_datetime(self.created_at))


@dataclass(frozen=True)
class BrokerSyncResult:
    connection_id: str
    account_count: int
    balance_count: int
    position_count: int
    open_order_count: int
    synced_at: datetime
    status: BrokerConnectionStatus = BrokerConnectionStatus.CONNECTED


def broker_connection_id(broker: str = SCHWAB_BROKER, user_id: str = LOCAL_USER_ID) -> str:
    return f"broker-connection-v1:{_digest({'broker': broker, 'user_id': user_id})}"


def broker_token_id(connection_id: str) -> str:
    return f"broker-token-v1:{_digest({'connection_id': connection_id})}"


def broker_account_id(broker: str, account_hash: str) -> str:
    return f"broker-account-v1:{_digest({'broker': broker, 'account_hash': account_hash})}"


def broker_balance_snapshot_id(account_id: str, as_of: datetime) -> str:
    return f"broker-balance-v1:{_digest({'account_id': account_id, 'as_of': as_of.isoformat()})}"


def broker_position_id(account_id: str, ticker: str, as_of: datetime) -> str:
    payload = {
        "account_id": account_id,
        "ticker": ticker.upper(),
        "as_of": as_of.isoformat(),
    }
    return f"broker-position-v1:{_digest(payload)}"


def broker_position_snapshot_id(account_id: str, as_of: datetime) -> str:
    payload = {"account_id": account_id, "as_of": as_of.isoformat()}
    return f"broker-position-snapshot-v1:{_digest(payload)}"


def broker_order_id(account_id: str, raw_order_id: str | None, payload: Mapping[str, Any]) -> str:
    identity = raw_order_id or _digest(payload)
    return f"broker-order-v1:{_digest({'account_id': account_id, 'order': identity})}"


def _digest(value: Mapping[str, Any]) -> str:
    serialized = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()[:24]


def _required_text(value: object, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        msg = f"{field_name} must not be blank"
        raise ValueError(msg)
    return text


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_ticker(value: object) -> str | None:
    text = _optional_text(value)
    return text.upper() if text else None


def _float(value: object) -> float:
    return float(value or 0.0)


def _optional_float(value: object) -> float | None:
    return None if value in (None, "") else float(value)


def _to_utc_datetime(value: datetime) -> datetime:
    if not isinstance(value, datetime):
        msg = "value must be a datetime"
        raise TypeError(msg)
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)

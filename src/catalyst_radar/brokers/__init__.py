"""DEPRECATED product surface: broker integrations.

Event-first MarketRadar does not ship a broker/order product path.
Scheduled for phased removal — see docs/PRODUCT_SCOPE.md and docs/DEPRECATION.md.
"""

from catalyst_radar.brokers.models import (
    BrokerAccount,
    BrokerBalanceSnapshot,
    BrokerConnection,
    BrokerConnectionStatus,
    BrokerOrder,
    BrokerPosition,
    BrokerPositionSnapshot,
    BrokerSyncResult,
    BrokerToken,
    broker_account_id,
    broker_balance_snapshot_id,
    broker_connection_id,
    broker_order_id,
    broker_position_id,
    broker_position_snapshot_id,
    broker_token_id,
)

__all__ = [
    "BrokerAccount",
    "BrokerBalanceSnapshot",
    "BrokerConnection",
    "BrokerConnectionStatus",
    "BrokerOrder",
    "BrokerPosition",
    "BrokerPositionSnapshot",
    "BrokerSyncResult",
    "BrokerToken",
    "broker_account_id",
    "broker_balance_snapshot_id",
    "broker_connection_id",
    "broker_order_id",
    "broker_position_id",
    "broker_position_snapshot_id",
    "broker_token_id",
]

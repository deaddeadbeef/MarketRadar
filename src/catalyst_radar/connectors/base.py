from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any, Protocol

from catalyst_radar.core.immutability import freeze_mapping


class ConnectorRecordKind(StrEnum):
    SECURITY = "security"
    DAILY_BAR = "daily_bar"
    HOLDING = "holding"
    UNIVERSE_MEMBER = "universe_member"


class ConnectorHealthStatus(StrEnum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    DOWN = "down"


def validate_required_text(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        msg = f"{field_name} must be non-empty"
        raise ValueError(msg)
    return value


def validate_aware_datetime(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        msg = f"{field_name} must be a timezone-aware datetime"
        raise ValueError(msg)
    if value.utcoffset() is None:
        msg = f"{field_name} must be a timezone-aware datetime"
        raise ValueError(msg)
    return value


def immutable_mapping(value: Mapping[str, Any], field_name: str) -> Mapping[str, Any]:
    return freeze_mapping(value, field_name)


def validate_not_before(
    value: datetime,
    minimum: datetime,
    field_name: str,
    minimum_field_name: str,
) -> None:
    if value < minimum:
        msg = f"{field_name} must not be earlier than {minimum_field_name}"
        raise ValueError(msg)


def validate_non_negative(value: int | float, field_name: str) -> None:
    if value < 0:
        msg = f"{field_name} must be non-negative"
        raise ValueError(msg)


@dataclass(frozen=True)
class ConnectorRequest:
    provider: str
    endpoint: str
    params: Mapping[str, Any]
    requested_at: datetime
    idempotency_key: str | None = None

    def __post_init__(self) -> None:
        validate_required_text(self.provider, "provider")
        validate_required_text(self.endpoint, "endpoint")
        validate_aware_datetime(self.requested_at, "requested_at")
        object.__setattr__(self, "params", immutable_mapping(self.params, "params"))


@dataclass(frozen=True)
class RawRecord:
    provider: str
    kind: ConnectorRecordKind
    request_hash: str
    payload_hash: str
    payload: Mapping[str, Any]
    source_ts: datetime
    fetched_at: datetime
    available_at: datetime
    license_tag: str
    retention_policy: str

    def __post_init__(self) -> None:
        validate_required_text(self.provider, "provider")
        validate_required_text(self.request_hash, "request_hash")
        validate_required_text(self.payload_hash, "payload_hash")
        validate_required_text(self.license_tag, "license_tag")
        validate_required_text(self.retention_policy, "retention_policy")
        source_ts = validate_aware_datetime(self.source_ts, "source_ts")
        fetched_at = validate_aware_datetime(self.fetched_at, "fetched_at")
        available_at = validate_aware_datetime(self.available_at, "available_at")
        validate_not_before(fetched_at, source_ts, "fetched_at", "source_ts")
        validate_not_before(available_at, source_ts, "available_at", "source_ts")
        object.__setattr__(self, "payload", immutable_mapping(self.payload, "payload"))


@dataclass(frozen=True)
class NormalizedRecord:
    provider: str
    kind: ConnectorRecordKind
    identity: str
    payload: Mapping[str, Any]
    source_ts: datetime
    available_at: datetime
    raw_payload_hash: str

    def __post_init__(self) -> None:
        validate_required_text(self.provider, "provider")
        validate_required_text(self.identity, "identity")
        validate_required_text(self.raw_payload_hash, "raw_payload_hash")
        source_ts = validate_aware_datetime(self.source_ts, "source_ts")
        available_at = validate_aware_datetime(self.available_at, "available_at")
        validate_not_before(available_at, source_ts, "available_at", "source_ts")
        object.__setattr__(self, "payload", immutable_mapping(self.payload, "payload"))


@dataclass(frozen=True)
class ConnectorHealth:
    provider: str
    status: ConnectorHealthStatus
    checked_at: datetime
    reason: str
    latency_ms: float | None = None

    def __post_init__(self) -> None:
        validate_required_text(self.provider, "provider")
        validate_aware_datetime(self.checked_at, "checked_at")


@dataclass(frozen=True)
class ProviderCostEstimate:
    provider: str
    request_count: int
    estimated_cost_usd: float
    currency: str = "USD"

    def __post_init__(self) -> None:
        validate_required_text(self.provider, "provider")
        validate_required_text(self.currency, "currency")
        validate_non_negative(self.request_count, "request_count")
        validate_non_negative(self.estimated_cost_usd, "estimated_cost_usd")


class MarketDataConnector(Protocol):
    def fetch(self, request: ConnectorRequest) -> list[RawRecord]:
        ...

    def normalize(self, records: Sequence[RawRecord]) -> list[NormalizedRecord]:
        ...

    def healthcheck(self) -> ConnectorHealth:
        ...

    def estimate_cost(self, request: ConnectorRequest) -> ProviderCostEstimate:
        ...


__all__ = [
    "ConnectorHealth",
    "ConnectorHealthStatus",
    "ConnectorRecordKind",
    "ConnectorRequest",
    "MarketDataConnector",
    "NormalizedRecord",
    "ProviderCostEstimate",
    "RawRecord",
    "immutable_mapping",
    "validate_aware_datetime",
    "validate_non_negative",
    "validate_not_before",
    "validate_required_text",
]

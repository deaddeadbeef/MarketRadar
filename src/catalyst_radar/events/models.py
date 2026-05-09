from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from catalyst_radar.core.immutability import freeze_mapping


class EventType(StrEnum):
    EARNINGS = "earnings"
    GUIDANCE = "guidance"
    SEC_FILING = "sec_filing"
    INSIDER = "insider"
    ANALYST_REVISION = "analyst_revision"
    SECTOR_READ_THROUGH = "sector_read_through"
    PRODUCT_CUSTOMER = "product_customer"
    LEGAL_REGULATORY = "legal_regulatory"
    FINANCING = "financing"
    CORPORATE_ACTION = "corporate_action"
    NEWS = "news"


class SourceCategory(StrEnum):
    PRIMARY_SOURCE = "primary_source"
    REGULATORY = "regulatory"
    REPUTABLE_NEWS = "reputable_news"
    COMPANY_PRESS_RELEASE = "company_press_release"
    ANALYST_PROVIDER = "analyst_provider"
    AGGREGATOR = "aggregator"
    SOCIAL = "social"
    PROMOTIONAL = "promotional"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class RawEvent:
    ticker: str
    provider: str
    source: str
    source_category: SourceCategory
    title: str
    body: str
    url: str | None
    source_ts: datetime
    available_at: datetime
    payload: Mapping[str, Any]

    def __post_init__(self) -> None:
        object.__setattr__(self, "ticker", _required_text(self.ticker, "ticker").upper())
        object.__setattr__(self, "provider", _required_text(self.provider, "provider"))
        object.__setattr__(self, "source", _required_text(self.source, "source"))
        object.__setattr__(self, "title", _required_text(self.title, "title"))
        object.__setattr__(self, "source_category", SourceCategory(self.source_category))
        object.__setattr__(self, "source_ts", _require_aware_utc(self.source_ts, "source_ts"))
        object.__setattr__(
            self,
            "available_at",
            _require_aware_utc(self.available_at, "available_at"),
        )
        if self.available_at < self.source_ts:
            msg = "available_at must be greater than or equal to source_ts"
            raise ValueError(msg)
        object.__setattr__(self, "payload", freeze_mapping(self.payload, "payload"))


@dataclass(frozen=True)
class EventClassification:
    event_type: EventType
    source_quality: float
    materiality: float
    reasons: Sequence[str] = ()
    requires_confirmation: bool = False
    requires_text_triage: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "event_type", EventType(self.event_type))
        object.__setattr__(self, "source_quality", _clamp_score(self.source_quality))
        object.__setattr__(self, "materiality", _clamp_score(self.materiality))
        object.__setattr__(self, "reasons", tuple(str(reason) for reason in self.reasons))


@dataclass(frozen=True)
class CanonicalEvent:
    id: str
    ticker: str
    event_type: EventType
    provider: str
    source: str
    source_category: SourceCategory
    source_url: str | None
    title: str
    body_hash: str
    dedupe_key: str
    source_quality: float
    materiality: float
    source_ts: datetime
    available_at: datetime
    payload: Mapping[str, Any]

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", _required_text(self.id, "id"))
        object.__setattr__(self, "ticker", _required_text(self.ticker, "ticker").upper())
        object.__setattr__(self, "event_type", EventType(self.event_type))
        object.__setattr__(self, "provider", _required_text(self.provider, "provider"))
        object.__setattr__(self, "source", _required_text(self.source, "source"))
        object.__setattr__(self, "source_category", SourceCategory(self.source_category))
        object.__setattr__(self, "title", _required_text(self.title, "title"))
        object.__setattr__(self, "body_hash", _required_text(self.body_hash, "body_hash"))
        object.__setattr__(self, "dedupe_key", _required_text(self.dedupe_key, "dedupe_key"))
        object.__setattr__(self, "source_quality", _clamp_score(self.source_quality))
        object.__setattr__(self, "materiality", _clamp_score(self.materiality))
        object.__setattr__(self, "source_ts", _require_aware_utc(self.source_ts, "source_ts"))
        object.__setattr__(
            self,
            "available_at",
            _require_aware_utc(self.available_at, "available_at"),
        )
        if self.available_at < self.source_ts:
            msg = "available_at must be greater than or equal to source_ts"
            raise ValueError(msg)
        object.__setattr__(self, "payload", freeze_mapping(self.payload, "payload"))


@dataclass(frozen=True)
class EventEvidenceSummary:
    ticker: str
    event_count: int
    top_event_type: EventType | None
    top_event_title: str | None
    top_event_source: str | None
    top_event_source_url: str | None
    top_event_source_quality: float | None
    top_event_materiality: float | None
    event_ids: Sequence[str] = ()
    has_conflict: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "ticker", _required_text(self.ticker, "ticker").upper())
        if self.event_count < 0:
            msg = "event_count must be non-negative"
            raise ValueError(msg)
        if self.top_event_type is not None:
            object.__setattr__(self, "top_event_type", EventType(self.top_event_type))
        object.__setattr__(self, "event_ids", tuple(str(event_id) for event_id in self.event_ids))


def _required_text(value: str, field_name: str) -> str:
    text = str(value).strip()
    if not text:
        msg = f"{field_name} must not be blank"
        raise ValueError(msg)
    return text


def _require_aware_utc(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        msg = f"{field_name} must be a datetime"
        raise TypeError(msg)
    if value.tzinfo is None or value.utcoffset() is None:
        msg = f"{field_name} must be timezone-aware"
        raise ValueError(msg)
    return value.astimezone(UTC)


def _clamp_score(value: float) -> float:
    return min(1.0, max(0.0, float(value)))

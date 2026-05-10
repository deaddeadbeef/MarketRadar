from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from enum import Enum, StrEnum
from typing import Any

from catalyst_radar.core.immutability import freeze_mapping

ALLOWED_USER_FEEDBACK_LABELS = frozenset(
    {"useful", "noisy", "too_late", "too_early", "ignored", "acted"}
)


class AlertRoute(StrEnum):
    IMMEDIATE_MANUAL_REVIEW = "immediate_manual_review"
    WARNING_DIGEST = "warning_digest"
    DAILY_DIGEST = "daily_digest"
    POSITION_WATCH = "position_watch"


class AlertChannel(StrEnum):
    DASHBOARD = "dashboard"
    DIGEST = "digest"
    EMAIL = "email"
    WEBHOOK = "webhook"


class AlertStatus(StrEnum):
    PLANNED = "planned"
    DRY_RUN = "dry_run"
    SENT = "sent"
    FAILED = "failed"


class AlertPriority(StrEnum):
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass(frozen=True)
class Alert:
    id: str
    ticker: str
    as_of: datetime
    source_ts: datetime
    available_at: datetime
    action_state: str
    route: AlertRoute
    channel: AlertChannel
    priority: AlertPriority
    status: AlertStatus
    dedupe_key: str
    trigger_kind: str
    trigger_fingerprint: str
    title: str
    summary: str
    payload: Mapping[str, Any] = field(default_factory=dict)
    candidate_state_id: str | None = None
    candidate_packet_id: str | None = None
    decision_card_id: str | None = None
    feedback_url: str | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    sent_at: datetime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", _required_text(self.id, "id"))
        object.__setattr__(self, "ticker", _required_text(self.ticker, "ticker").upper())
        object.__setattr__(self, "as_of", _require_aware_utc(self.as_of, "as_of"))
        object.__setattr__(
            self,
            "source_ts",
            _require_aware_utc(self.source_ts, "source_ts"),
        )
        object.__setattr__(
            self,
            "available_at",
            _require_aware_utc(self.available_at, "available_at"),
        )
        object.__setattr__(
            self,
            "action_state",
            _required_text(self.action_state, "action_state"),
        )
        object.__setattr__(self, "route", AlertRoute(self.route))
        object.__setattr__(self, "channel", AlertChannel(self.channel))
        object.__setattr__(self, "priority", AlertPriority(self.priority))
        object.__setattr__(self, "status", AlertStatus(self.status))
        object.__setattr__(
            self,
            "dedupe_key",
            _required_text(self.dedupe_key, "dedupe_key"),
        )
        object.__setattr__(
            self,
            "trigger_kind",
            _required_text(self.trigger_kind, "trigger_kind"),
        )
        object.__setattr__(
            self,
            "trigger_fingerprint",
            _required_text(self.trigger_fingerprint, "trigger_fingerprint"),
        )
        object.__setattr__(self, "title", _required_text(self.title, "title"))
        object.__setattr__(self, "summary", _required_text(self.summary, "summary"))
        object.__setattr__(
            self,
            "candidate_state_id",
            _optional_text(self.candidate_state_id, "candidate_state_id"),
        )
        object.__setattr__(
            self,
            "candidate_packet_id",
            _optional_text(self.candidate_packet_id, "candidate_packet_id"),
        )
        object.__setattr__(
            self,
            "decision_card_id",
            _optional_text(self.decision_card_id, "decision_card_id"),
        )
        object.__setattr__(
            self,
            "feedback_url",
            _optional_text(self.feedback_url, "feedback_url"),
        )
        object.__setattr__(
            self,
            "payload",
            freeze_mapping(_json_ready_mapping(self.payload, "payload"), "payload"),
        )
        object.__setattr__(
            self,
            "created_at",
            _require_aware_utc(self.created_at, "created_at"),
        )
        if self.sent_at is not None:
            object.__setattr__(
                self,
                "sent_at",
                _require_aware_utc(self.sent_at, "sent_at"),
            )


@dataclass(frozen=True)
class AlertSuppression:
    id: str
    ticker: str
    as_of: datetime
    available_at: datetime
    route: AlertRoute
    dedupe_key: str
    trigger_kind: str
    trigger_fingerprint: str
    reason: str
    payload: Mapping[str, Any] = field(default_factory=dict)
    candidate_state_id: str | None = None
    decision_card_id: str | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", _required_text(self.id, "id"))
        object.__setattr__(self, "ticker", _required_text(self.ticker, "ticker").upper())
        object.__setattr__(self, "as_of", _require_aware_utc(self.as_of, "as_of"))
        object.__setattr__(
            self,
            "available_at",
            _require_aware_utc(self.available_at, "available_at"),
        )
        object.__setattr__(
            self,
            "candidate_state_id",
            _optional_text(self.candidate_state_id, "candidate_state_id"),
        )
        object.__setattr__(
            self,
            "decision_card_id",
            _optional_text(self.decision_card_id, "decision_card_id"),
        )
        object.__setattr__(self, "route", AlertRoute(self.route))
        object.__setattr__(
            self,
            "dedupe_key",
            _required_text(self.dedupe_key, "dedupe_key"),
        )
        object.__setattr__(
            self,
            "trigger_kind",
            _required_text(self.trigger_kind, "trigger_kind"),
        )
        object.__setattr__(
            self,
            "trigger_fingerprint",
            _required_text(self.trigger_fingerprint, "trigger_fingerprint"),
        )
        object.__setattr__(self, "reason", _required_text(self.reason, "reason"))
        object.__setattr__(
            self,
            "payload",
            freeze_mapping(_json_ready_mapping(self.payload, "payload"), "payload"),
        )
        object.__setattr__(
            self,
            "created_at",
            _require_aware_utc(self.created_at, "created_at"),
        )


@dataclass(frozen=True)
class UserFeedback:
    id: str
    artifact_type: str
    artifact_id: str
    ticker: str
    label: str
    source: str
    payload: Mapping[str, Any] = field(default_factory=dict)
    notes: str | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", _required_text(self.id, "id"))
        object.__setattr__(
            self,
            "artifact_type",
            _required_text(self.artifact_type, "artifact_type"),
        )
        object.__setattr__(
            self,
            "artifact_id",
            _required_text(self.artifact_id, "artifact_id"),
        )
        object.__setattr__(self, "ticker", _required_text(self.ticker, "ticker").upper())
        label = _required_text(self.label, "label")
        if label not in ALLOWED_USER_FEEDBACK_LABELS:
            msg = f"label must be one of {sorted(ALLOWED_USER_FEEDBACK_LABELS)}"
            raise ValueError(msg)
        object.__setattr__(self, "label", label)
        object.__setattr__(self, "source", _required_text(self.source, "source"))
        object.__setattr__(
            self,
            "payload",
            freeze_mapping(_json_ready_mapping(self.payload, "payload"), "payload"),
        )
        if self.notes is not None:
            object.__setattr__(self, "notes", str(self.notes))
        object.__setattr__(
            self,
            "created_at",
            _require_aware_utc(self.created_at, "created_at"),
        )


def alert_id(
    *,
    ticker: str,
    route: str,
    dedupe_key: str,
    available_at: datetime,
) -> str:
    digest = _stable_digest(
        "alert-v1",
        _required_text(ticker, "ticker").upper(),
        AlertRoute(route).value,
        _required_text(dedupe_key, "dedupe_key"),
        _require_aware_utc(available_at, "available_at").isoformat(),
    )
    return f"alert-v1:{digest}"


def alert_suppression_id(
    *,
    dedupe_key: str,
    reason: str,
    available_at: datetime,
) -> str:
    digest = _stable_digest(
        "alert-suppression-v1",
        _required_text(dedupe_key, "dedupe_key"),
        _required_text(reason, "reason"),
        _require_aware_utc(available_at, "available_at").isoformat(),
    )
    return f"alert-suppression-v1:{digest}"


def user_feedback_id(
    *,
    artifact_type: str,
    artifact_id: str,
    label: str,
    created_at: datetime,
) -> str:
    digest = _stable_digest(
        "user-feedback-v1",
        _required_text(artifact_type, "artifact_type"),
        _required_text(artifact_id, "artifact_id"),
        _required_text(label, "label"),
        _require_aware_utc(created_at, "created_at").isoformat(),
    )
    return f"user-feedback-v1:{digest}"


def _required_text(value: object, field_name: str) -> str:
    text = str(value).strip()
    if not text:
        msg = f"{field_name} must not be blank"
        raise ValueError(msg)
    return text


def _optional_text(value: object | None, field_name: str) -> str | None:
    if value is None:
        return None
    return _required_text(value, field_name)


def _require_aware_utc(value: datetime, field_name: str) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if not isinstance(value, datetime):
        msg = f"{field_name} must be a datetime"
        raise TypeError(msg)
    if value.tzinfo is None or value.utcoffset() is None:
        msg = f"{field_name} must be timezone-aware"
        raise ValueError(msg)
    return value.astimezone(UTC)


def _stable_digest(*parts: object) -> str:
    canonical = json.dumps(
        [_json_ready(part, "digest_part") for part in parts],
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:32]


def _json_ready_mapping(value: Mapping[str, Any], field_name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        msg = f"{field_name} must be a mapping"
        raise TypeError(msg)
    normalized = {str(key): _json_ready(item, f"{field_name}.{key}") for key, item in value.items()}
    json.dumps(normalized, allow_nan=False, separators=(",", ":"), sort_keys=True)
    return normalized


def _json_ready(value: Any, field_name: str) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_ready(item, f"{field_name}.{key}") for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_json_ready(item, field_name) for item in value]
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return _require_aware_utc(value, field_name).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if value is None or isinstance(value, str | int | bool):
        return value
    if isinstance(value, float):
        json.dumps(value, allow_nan=False)
        return value
    msg = f"{field_name} must be JSON-serializable"
    raise TypeError(msg)


__all__ = [
    "ALLOWED_USER_FEEDBACK_LABELS",
    "Alert",
    "AlertChannel",
    "AlertPriority",
    "AlertRoute",
    "AlertStatus",
    "AlertSuppression",
    "UserFeedback",
    "alert_id",
    "alert_suppression_id",
    "user_feedback_id",
]

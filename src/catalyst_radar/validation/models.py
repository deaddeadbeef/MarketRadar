from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from catalyst_radar.core.immutability import freeze_mapping
from catalyst_radar.core.models import ActionState


class ValidationRunStatus(StrEnum):
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


class PaperDecision(StrEnum):
    APPROVED = "approved"
    REJECTED = "rejected"
    DEFERRED = "deferred"


class PaperTradeState(StrEnum):
    PENDING_ENTRY = "pending_entry"
    OPEN = "open"
    INVALIDATED = "invalidated"
    CLOSED = "closed"
    REJECTED = "rejected"
    DEFERRED = "deferred"


@dataclass(frozen=True)
class ValidationRun:
    id: str
    run_type: str
    as_of_start: datetime
    as_of_end: datetime
    decision_available_at: datetime
    status: ValidationRunStatus
    config: Mapping[str, Any] = field(default_factory=dict)
    metrics: Mapping[str, Any] = field(default_factory=dict)
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    finished_at: datetime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", _required_text(self.id, "id"))
        object.__setattr__(self, "run_type", _required_text(self.run_type, "run_type"))
        object.__setattr__(
            self,
            "as_of_start",
            _require_aware_utc(self.as_of_start, "as_of_start"),
        )
        object.__setattr__(
            self,
            "as_of_end",
            _require_aware_utc(self.as_of_end, "as_of_end"),
        )
        object.__setattr__(
            self,
            "decision_available_at",
            _require_aware_utc(
                self.decision_available_at,
                "decision_available_at",
            ),
        )
        if self.as_of_end < self.as_of_start:
            msg = "as_of_end must be greater than or equal to as_of_start"
            raise ValueError(msg)
        object.__setattr__(self, "status", ValidationRunStatus(self.status))
        object.__setattr__(self, "config", freeze_mapping(self.config, "config"))
        object.__setattr__(self, "metrics", freeze_mapping(self.metrics, "metrics"))
        object.__setattr__(
            self,
            "started_at",
            _require_aware_utc(self.started_at, "started_at"),
        )
        if self.finished_at is not None:
            object.__setattr__(
                self,
                "finished_at",
                _require_aware_utc(self.finished_at, "finished_at"),
            )


@dataclass(frozen=True)
class ValidationResult:
    id: str
    run_id: str
    ticker: str
    as_of: datetime
    available_at: datetime
    state: ActionState
    final_score: float
    candidate_state_id: str | None = None
    candidate_packet_id: str | None = None
    decision_card_id: str | None = None
    baseline: str | None = None
    labels: Mapping[str, Any] = field(default_factory=dict)
    leakage_flags: Sequence[str] = ()
    payload: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", _required_text(self.id, "id"))
        object.__setattr__(self, "run_id", _required_text(self.run_id, "run_id"))
        object.__setattr__(self, "ticker", _required_text(self.ticker, "ticker").upper())
        object.__setattr__(self, "as_of", _require_aware_utc(self.as_of, "as_of"))
        object.__setattr__(
            self,
            "available_at",
            _require_aware_utc(self.available_at, "available_at"),
        )
        object.__setattr__(self, "state", ActionState(self.state))
        object.__setattr__(self, "final_score", float(self.final_score))
        if self.candidate_state_id is not None:
            object.__setattr__(
                self,
                "candidate_state_id",
                _required_text(self.candidate_state_id, "candidate_state_id"),
            )
        if self.candidate_packet_id is not None:
            object.__setattr__(
                self,
                "candidate_packet_id",
                _required_text(self.candidate_packet_id, "candidate_packet_id"),
            )
        if self.decision_card_id is not None:
            object.__setattr__(
                self,
                "decision_card_id",
                _required_text(self.decision_card_id, "decision_card_id"),
            )
        if self.baseline is not None:
            object.__setattr__(self, "baseline", _required_text(self.baseline, "baseline"))
        object.__setattr__(self, "labels", freeze_mapping(self.labels, "labels"))
        object.__setattr__(
            self,
            "leakage_flags",
            tuple(str(flag) for flag in self.leakage_flags if str(flag)),
        )
        object.__setattr__(self, "payload", freeze_mapping(self.payload, "payload"))


@dataclass(frozen=True)
class PaperTrade:
    id: str
    decision_card_id: str
    ticker: str
    as_of: datetime
    decision: PaperDecision
    state: PaperTradeState
    source_ts: datetime
    available_at: datetime
    entry_price: float | None = None
    entry_at: datetime | None = None
    invalidation_price: float | None = None
    shares: float = 0.0
    notional: float = 0.0
    max_loss: float = 0.0
    outcome_labels: Mapping[str, Any] = field(default_factory=dict)
    payload: Mapping[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", _required_text(self.id, "id"))
        object.__setattr__(
            self,
            "decision_card_id",
            _required_text(self.decision_card_id, "decision_card_id"),
        )
        object.__setattr__(self, "ticker", _required_text(self.ticker, "ticker").upper())
        object.__setattr__(self, "as_of", _require_aware_utc(self.as_of, "as_of"))
        object.__setattr__(self, "decision", PaperDecision(self.decision))
        object.__setattr__(self, "state", PaperTradeState(self.state))
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
        if self.available_at < self.source_ts:
            msg = "available_at must be greater than or equal to source_ts"
            raise ValueError(msg)
        if self.entry_at is not None:
            object.__setattr__(
                self,
                "entry_at",
                _require_aware_utc(self.entry_at, "entry_at"),
            )
        for field_name in ("shares", "notional", "max_loss"):
            object.__setattr__(self, field_name, float(getattr(self, field_name)))
        if self.entry_price is not None:
            object.__setattr__(self, "entry_price", float(self.entry_price))
        if self.invalidation_price is not None:
            object.__setattr__(
                self,
                "invalidation_price",
                float(self.invalidation_price),
            )
        object.__setattr__(
            self,
            "outcome_labels",
            freeze_mapping(self.outcome_labels, "outcome_labels"),
        )
        object.__setattr__(self, "payload", freeze_mapping(self.payload, "payload"))
        object.__setattr__(
            self,
            "created_at",
            _require_aware_utc(self.created_at, "created_at"),
        )
        object.__setattr__(
            self,
            "updated_at",
            _require_aware_utc(self.updated_at, "updated_at"),
        )


@dataclass(frozen=True)
class UsefulAlertLabel:
    id: str
    artifact_type: str
    artifact_id: str
    ticker: str
    label: str
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
        object.__setattr__(self, "label", _required_text(self.label, "label"))
        if self.notes is not None:
            object.__setattr__(self, "notes", str(self.notes))
        object.__setattr__(
            self,
            "created_at",
            _require_aware_utc(self.created_at, "created_at"),
        )


def validation_run_id(
    *,
    run_type: str,
    as_of_start: datetime,
    as_of_end: datetime,
    decision_available_at: datetime,
) -> str:
    return (
        f"validation-{_required_text(run_type, 'run_type')}:"
        f"{_require_aware_utc(as_of_start, 'as_of_start').isoformat()}:"
        f"{_require_aware_utc(as_of_end, 'as_of_end').isoformat()}:"
        f"{_require_aware_utc(decision_available_at, 'decision_available_at').isoformat()}"
    )


def validation_result_id(
    *,
    run_id: str,
    ticker: str,
    as_of: datetime,
    state: ActionState | str,
    baseline: str | None = None,
) -> str:
    suffix = baseline or "candidate"
    return (
        f"validation-result-v1:{_required_text(run_id, 'run_id')}:"
        f"{_required_text(ticker, 'ticker').upper()}:"
        f"{_require_aware_utc(as_of, 'as_of').isoformat()}:"
        f"{ActionState(state).value}:{suffix}"
    )


def paper_trade_id(
    decision_card_id: str,
    decision: PaperDecision | str | None = None,
) -> str:
    suffix = "" if decision is None else f":{PaperDecision(decision).value}"
    return f"paper-trade-v1:{_required_text(decision_card_id, 'decision_card_id')}{suffix}"


def useful_alert_label_id(
    *,
    artifact_type: str,
    artifact_id: str,
    label: str,
) -> str:
    return (
        f"useful-alert-label-v1:{_required_text(artifact_type, 'artifact_type')}:"
        f"{_required_text(artifact_id, 'artifact_id')}:{_required_text(label, 'label')}"
    )


def _required_text(value: object, field_name: str) -> str:
    text = str(value).strip()
    if not text:
        msg = f"{field_name} must not be blank"
        raise ValueError(msg)
    return text


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


__all__ = [
    "PaperDecision",
    "PaperTrade",
    "PaperTradeState",
    "UsefulAlertLabel",
    "ValidationResult",
    "ValidationRun",
    "ValidationRunStatus",
    "paper_trade_id",
    "useful_alert_label_id",
    "validation_result_id",
    "validation_run_id",
]

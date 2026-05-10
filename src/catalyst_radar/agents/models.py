from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import Enum, StrEnum
from typing import Any

from catalyst_radar.core.immutability import freeze_json_value


class LLMTaskName(StrEnum):
    MINI_EXTRACTION = "mini_extraction"
    MID_REVIEW = "mid_review"
    SKEPTIC_REVIEW = "skeptic_review"
    GPT55_DECISION_CARD = "gpt55_decision_card"
    FULL_TRANSCRIPT_DEEP_DIVE = "full_transcript_deep_dive"


class LLMCallStatus(StrEnum):
    PLANNED = "planned"
    SKIPPED = "skipped"
    DRY_RUN = "dry_run"
    COMPLETED = "completed"
    FAILED = "failed"
    SCHEMA_REJECTED = "schema_rejected"


class LLMSkipReason(StrEnum):
    PREMIUM_LLM_DISABLED = "premium_llm_disabled"
    CANDIDATE_STATE_NOT_ELIGIBLE = "candidate_state_not_eligible"
    TASK_DAILY_CAP_EXCEEDED = "task_daily_cap_exceeded"
    DAILY_BUDGET_EXCEEDED = "daily_budget_exceeded"
    MONTHLY_BUDGET_EXCEEDED = "monthly_budget_exceeded"
    MONTHLY_SOFT_CAP_REQUIRES_HIGH_SCORE = "monthly_soft_cap_requires_high_score"
    MODEL_NOT_CONFIGURED = "model_not_configured"
    PRICING_MISSING = "pricing_missing"
    PRICING_STALE = "pricing_stale"
    MANUAL_TASK_REQUIRES_OPERATOR = "manual_task_requires_operator"
    CANDIDATE_PACKET_MISSING = "candidate_packet_missing"
    SCHEMA_VALIDATION_FAILED = "schema_validation_failed"
    CLIENT_ERROR = "client_error"


@dataclass(frozen=True)
class TokenUsage:
    input_tokens: int = 0
    cached_input_tokens: int = 0
    output_tokens: int = 0

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "input_tokens",
            _nonnegative_int(self.input_tokens, "input_tokens"),
        )
        object.__setattr__(
            self,
            "cached_input_tokens",
            _nonnegative_int(self.cached_input_tokens, "cached_input_tokens"),
        )
        object.__setattr__(
            self,
            "output_tokens",
            _nonnegative_int(self.output_tokens, "output_tokens"),
        )


@dataclass(frozen=True)
class BudgetLedgerEntry:
    id: str
    ts: datetime
    available_at: datetime
    task: LLMTaskName
    status: LLMCallStatus
    estimated_cost: float
    actual_cost: float
    currency: str = "USD"
    ticker: str | None = None
    candidate_state_id: str | None = None
    candidate_packet_id: str | None = None
    decision_card_id: str | None = None
    model: str | None = None
    provider: str = "none"
    skip_reason: LLMSkipReason | None = None
    token_usage: TokenUsage = field(default_factory=TokenUsage)
    tool_calls: Sequence[Mapping[str, Any]] = ()
    candidate_state: str | None = None
    prompt_version: str | None = None
    schema_version: str | None = None
    outcome_label: str | None = None
    payload: Mapping[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", _required_text(self.id, "id"))
        object.__setattr__(self, "ts", _require_aware_utc(self.ts, "ts"))
        object.__setattr__(
            self,
            "available_at",
            _require_aware_utc(self.available_at, "available_at"),
        )
        object.__setattr__(self, "task", LLMTaskName(self.task))
        object.__setattr__(self, "status", LLMCallStatus(self.status))
        object.__setattr__(
            self,
            "estimated_cost",
            _nonnegative_finite_float(self.estimated_cost, "estimated_cost"),
        )
        object.__setattr__(
            self,
            "actual_cost",
            _nonnegative_finite_float(self.actual_cost, "actual_cost"),
        )
        object.__setattr__(self, "currency", _required_text(self.currency, "currency"))
        object.__setattr__(self, "ticker", _optional_ticker(self.ticker))
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
        object.__setattr__(self, "model", _optional_text(self.model, "model"))
        object.__setattr__(self, "provider", _required_text(self.provider, "provider"))
        if self.skip_reason is not None:
            object.__setattr__(
                self,
                "skip_reason",
                LLMSkipReason(self.skip_reason),
            )
        if not isinstance(self.token_usage, TokenUsage):
            object.__setattr__(self, "token_usage", TokenUsage(**self.token_usage))
        object.__setattr__(
            self,
            "tool_calls",
            _json_ready_tool_calls(self.tool_calls, "tool_calls"),
        )
        object.__setattr__(
            self,
            "candidate_state",
            _optional_text(self.candidate_state, "candidate_state"),
        )
        object.__setattr__(
            self,
            "prompt_version",
            _optional_text(self.prompt_version, "prompt_version"),
        )
        object.__setattr__(
            self,
            "schema_version",
            _optional_text(self.schema_version, "schema_version"),
        )
        object.__setattr__(
            self,
            "outcome_label",
            _optional_text(self.outcome_label, "outcome_label"),
        )
        object.__setattr__(
            self,
            "payload",
            freeze_json_value(_json_ready_mapping(self.payload, "payload")),
        )
        object.__setattr__(
            self,
            "created_at",
            _require_aware_utc(self.created_at, "created_at"),
        )


def budget_ledger_id(
    *,
    task: str,
    ticker: str | None,
    candidate_packet_id: str | None,
    status: str,
    available_at: datetime,
    prompt_version: str | None = None,
) -> str:
    normalized = [
        "budget-ledger-v1",
        LLMTaskName(task).value,
        ticker.upper() if ticker else None,
        candidate_packet_id,
        LLMCallStatus(status).value,
        _require_aware_utc(available_at, "available_at").isoformat(),
        prompt_version,
    ]
    canonical = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:32]
    return f"budget-ledger-v1:{digest}"


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


def _optional_ticker(value: object | None) -> str | None:
    if value is None:
        return None
    return _required_text(value, "ticker").upper()


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


def _nonnegative_int(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        msg = f"{field_name} must be an integer"
        raise TypeError(msg)
    if value < 0:
        msg = f"{field_name} must be greater than or equal to zero"
        raise ValueError(msg)
    return value


def _nonnegative_finite_float(value: object, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float | Decimal):
        msg = f"{field_name} must be a number"
        raise TypeError(msg)
    cost = float(value)
    if not math.isfinite(cost) or cost < 0:
        msg = f"{field_name} must be finite and greater than or equal to zero"
        raise ValueError(msg)
    return cost


def _json_ready_tool_calls(
    value: Sequence[Mapping[str, Any]],
    field_name: str,
) -> tuple[Mapping[str, Any], ...]:
    if isinstance(value, str) or not isinstance(value, Sequence):
        msg = f"{field_name} must be a sequence of mappings"
        raise TypeError(msg)
    return tuple(
        freeze_json_value(_json_ready_mapping(item, f"{field_name}.{index}"))
        for index, item in enumerate(value)
    )


def _json_ready_mapping(value: Mapping[str, Any], field_name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        msg = f"{field_name} must be a mapping"
        raise TypeError(msg)
    normalized = {
        str(key): _json_ready(item, f"{field_name}.{key}")
        for key, item in value.items()
    }
    json.dumps(normalized, allow_nan=False, separators=(",", ":"), sort_keys=True)
    return normalized


def _json_ready(value: Any, field_name: str) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _json_ready(item, f"{field_name}.{key}")
            for key, item in value.items()
        }
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
    "BudgetLedgerEntry",
    "LLMCallStatus",
    "LLMSkipReason",
    "LLMTaskName",
    "TokenUsage",
    "budget_ledger_id",
]

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from typing import Literal

from sqlalchemy.engine import Engine

from catalyst_radar.agents.budget import BudgetController
from catalyst_radar.agents.models import (
    BudgetLedgerEntry,
    LLMCallStatus,
    LLMSkipReason,
    TokenUsage,
    budget_ledger_id,
)
from catalyst_radar.agents.openai_client import OpenAIResponsesClient
from catalyst_radar.agents.router import (
    FakeLLMClient,
    LLMClientRequest,
    LLMClientResult,
    LLMRouter,
)
from catalyst_radar.agents.tasks import DEFAULT_TASKS
from catalyst_radar.core.config import AppConfig
from catalyst_radar.core.immutability import thaw_json_value
from catalyst_radar.security.audit import AuditLogRepository
from catalyst_radar.security.redaction import redact_text, redact_value
from catalyst_radar.storage.budget_repositories import BudgetLedgerRepository
from catalyst_radar.storage.candidate_packet_repositories import CandidatePacketRepository

AgentReviewMode = Literal["dry_run", "fake", "real"]


@dataclass(frozen=True)
class AgentReviewServiceResult:
    status_code: int
    payload: dict[str, object]


def run_agent_review(
    engine: Engine,
    *,
    config: AppConfig,
    ticker: str,
    as_of: date,
    available_at: datetime,
    task_name: str = "skeptic_review",
    mode: AgentReviewMode = "dry_run",
    actor_source: str = "api",
) -> AgentReviewServiceResult:
    task = DEFAULT_TASKS[task_name]
    attempted_at = datetime.now(UTC)
    packet_repo = CandidatePacketRepository(engine)
    ledger_repo = BudgetLedgerRepository(engine)
    packet = packet_repo.latest_candidate_packet(
        ticker,
        as_of=_scan_timestamp(as_of),
        available_at=_aware_utc(available_at),
    )
    if packet is None:
        entry = _missing_packet_entry(
            config=config,
            task_name=task_name,
            ticker=ticker,
            available_at=_aware_utc(available_at),
            attempted_at=attempted_at,
        )
        ledger_repo.upsert_entry(entry)
        append_model_call_audit_event(
            engine,
            entry=entry,
            actor_source=actor_source,
            artifact_type="candidate_packet",
            artifact_id=None,
        )
        return AgentReviewServiceResult(
            status_code=404,
            payload={
                "status": "missing_candidate_packet",
                "message": f"Candidate packet not found for {ticker.upper()}.",
                "ledger": llm_ledger_payload(entry),
            },
        )

    budget = BudgetController(
        config=config,
        ledger_repo=ledger_repo,
        now=lambda: attempted_at,
    )
    router = LLMRouter(
        budget=budget,
        client=_llm_client_for_mode(config=config, mode=mode),
        now=lambda: attempted_at,
    )
    result = router.review_candidate(
        task=task,
        candidate=packet,
        available_at=_aware_utc(available_at),
        dry_run=mode == "dry_run",
    )
    payload = {
        "status": result.status.value,
        "mode": mode,
        "ticker": packet.ticker,
        "candidate_packet_id": packet.id,
        **llm_review_payload(result),
    }
    status_code = 200
    if result.status in {LLMCallStatus.FAILED, LLMCallStatus.SCHEMA_REJECTED}:
        status_code = 422
    return AgentReviewServiceResult(status_code=status_code, payload=payload)


def append_model_call_audit_event(
    engine: Engine,
    *,
    entry: BudgetLedgerEntry,
    actor_source: str,
    artifact_type: str,
    artifact_id: str | None,
) -> None:
    AuditLogRepository(engine).append_event(
        event_type="model_call_recorded",
        actor_source=actor_source,
        artifact_type=artifact_type,
        artifact_id=artifact_id,
        ticker=entry.ticker,
        candidate_state_id=entry.candidate_state_id,
        candidate_packet_id=entry.candidate_packet_id,
        budget_ledger_id=entry.id,
        status=entry.status.value,
        metadata={
            "task": entry.task.value,
            "provider": entry.provider,
            "model": entry.model,
            "skip_reason": entry.skip_reason.value if entry.skip_reason else None,
            "prompt_version": entry.prompt_version,
            "schema_version": entry.schema_version,
        },
        available_at=entry.available_at,
        occurred_at=entry.created_at,
    )


def llm_review_payload(result) -> dict[str, object]:
    entry = result.ledger_entry
    return {
        "result": {
            "status": result.status.value,
            "error": redact_text(result.error) if result.error is not None else None,
            "payload": (
                redact_value(thaw_json_value(result.payload))
                if result.payload is not None
                else None
            ),
        },
        "route": {
            "skip": result.decision.skip,
            "reason": result.decision.reason.value if result.decision.reason else None,
            "task": result.decision.task.name.value,
            "model": result.decision.model,
            "estimated_cost_usd": result.decision.estimated_cost,
            "max_tokens": result.decision.max_tokens,
            "estimated_usage": {
                "input_tokens": result.decision.estimated_usage.input_tokens,
                "cached_input_tokens": result.decision.estimated_usage.cached_input_tokens,
                "output_tokens": result.decision.estimated_usage.output_tokens,
            },
        },
        "ledger": llm_ledger_payload(entry),
    }


def llm_ledger_payload(entry) -> dict[str, object]:
    return {
        "id": entry.id,
        "ts": entry.ts.isoformat(),
        "available_at": entry.available_at.isoformat(),
        "ticker": entry.ticker,
        "task": entry.task.value,
        "model": entry.model,
        "provider": entry.provider,
        "status": entry.status.value,
        "skip_reason": entry.skip_reason.value if entry.skip_reason else None,
        "input_tokens": entry.token_usage.input_tokens,
        "cached_input_tokens": entry.token_usage.cached_input_tokens,
        "output_tokens": entry.token_usage.output_tokens,
        "estimated_cost_usd": entry.estimated_cost,
        "actual_cost_usd": entry.actual_cost,
        "currency": entry.currency,
        "candidate_state": entry.candidate_state,
        "candidate_state_id": entry.candidate_state_id,
        "candidate_packet_id": entry.candidate_packet_id,
        "prompt_version": entry.prompt_version,
        "schema_version": entry.schema_version,
        "outcome_label": entry.outcome_label,
        "payload": redact_value(thaw_json_value(entry.payload)),
    }


def _missing_packet_entry(
    *,
    config: AppConfig,
    task_name: str,
    ticker: str,
    available_at: datetime,
    attempted_at: datetime,
) -> BudgetLedgerEntry:
    task = DEFAULT_TASKS[task_name]
    model = getattr(config, task.model_config_key)
    return BudgetLedgerEntry(
        id=budget_ledger_id(
            task=task.name.value,
            ticker=ticker,
            candidate_packet_id=None,
            status=LLMCallStatus.SKIPPED.value,
            available_at=available_at,
            prompt_version=task.prompt_version,
            attempted_at=attempted_at,
        ),
        ts=attempted_at,
        available_at=available_at,
        task=task.name,
        status=LLMCallStatus.SKIPPED,
        estimated_cost=0.0,
        actual_cost=0.0,
        ticker=ticker,
        model=str(model).strip() if model else None,
        provider=config.llm_provider,
        skip_reason=LLMSkipReason.CANDIDATE_PACKET_MISSING,
        token_usage=TokenUsage(),
        prompt_version=task.prompt_version,
        schema_version=task.schema_version,
        payload={"error": "candidate packet not found"},
        created_at=attempted_at,
    )


def _llm_client_for_mode(*, config: AppConfig, mode: AgentReviewMode):
    provider = config.llm_provider.strip().lower()
    if mode == "fake" or provider == "fake":
        return FakeLLMClient()
    if mode == "real" and provider == "openai":
        return OpenAIResponsesClient(api_key=config.openai_api_key)
    return _SafeDisabledLLMClient()


def _scan_timestamp(value: date) -> datetime:
    return datetime.combine(value, time(21), tzinfo=UTC)


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        msg = "available_at must include timezone information"
        raise ValueError(msg)
    return value.astimezone(UTC)


class _SafeDisabledLLMClient:
    def complete(self, request: LLMClientRequest) -> LLMClientResult:
        del request
        raise RuntimeError("real_llm_provider_disabled")


__all__ = [
    "AgentReviewMode",
    "AgentReviewServiceResult",
    "append_model_call_audit_event",
    "llm_ledger_payload",
    "llm_review_payload",
    "run_agent_review",
]

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from catalyst_radar.agents.budget import BudgetController
from catalyst_radar.agents.evidence import build_agent_evidence_packet
from catalyst_radar.agents.models import (
    BudgetLedgerEntry,
    LLMCallStatus,
    LLMSkipReason,
    TokenUsage,
    budget_ledger_id,
)
from catalyst_radar.agents.schemas import (
    AgentSchemaError,
    validate_decision_card_draft_output,
    validate_evidence_review_output,
    validate_skeptic_review_output,
)
from catalyst_radar.agents.tasks import LLMTask
from catalyst_radar.pipeline.candidate_packet import (
    CandidatePacket,
    EvidenceItem,
    canonical_packet_json,
)


@dataclass(frozen=True)
class LLMClientRequest:
    task: LLMTask
    candidate: CandidatePacket
    candidate_json: str
    evidence_packet: Mapping[str, Any]
    prompt_version: str
    schema_version: str
    model: str
    max_output_tokens: int
    estimated_usage: TokenUsage


@dataclass(frozen=True)
class LLMClientResult:
    payload: Mapping[str, Any]
    token_usage: TokenUsage
    model: str
    provider: str = "fake"
    tool_calls: tuple[Mapping[str, Any], ...] = ()


class LLMClient(Protocol):
    def complete(self, request: LLMClientRequest) -> LLMClientResult:
        raise NotImplementedError(
            "client implementations return schema candidate and token usage"
        )


@dataclass(frozen=True)
class LLMRouteDecision:
    skip: bool
    reason: LLMSkipReason | None
    task: LLMTask
    model: str | None
    estimated_cost: float
    max_tokens: int
    estimated_usage: TokenUsage
    ledger_entry: BudgetLedgerEntry | None = None


@dataclass(frozen=True)
class LLMReviewResult:
    decision: LLMRouteDecision
    status: LLMCallStatus
    ledger_entry: BudgetLedgerEntry
    payload: Mapping[str, Any] | None = None
    error: str | None = None


class FakeLLMClient:
    def complete(self, request: LLMClientRequest) -> LLMClientResult:
        if request.schema_version == "skeptic-review-v1":
            payload = _fake_skeptic_review_payload(request)
        elif request.schema_version == "decision-card-v1":
            payload = _fake_decision_card_draft_payload(request)
        else:
            payload = _fake_evidence_review_payload(request)
        return LLMClientResult(
            payload=payload,
            token_usage=TokenUsage(
                input_tokens=request.estimated_usage.input_tokens,
                cached_input_tokens=request.estimated_usage.cached_input_tokens,
                output_tokens=min(180, request.max_output_tokens),
            ),
            model=request.model,
            provider="fake",
        )


def _fake_evidence_review_payload(request: LLMClientRequest) -> Mapping[str, Any]:
    evidence = request.candidate.supporting_evidence[0]
    source_quality = evidence.source_quality
    if source_quality is None:
        source_quality = min(max(evidence.strength, 0.0), 1.0)
    claim: dict[str, Any] = {
        "claim": evidence.summary,
        "source_quality": source_quality,
        "evidence_type": evidence.kind,
        "sentiment": _sentiment_from_polarity(evidence.polarity, evidence.strength),
        "confidence": min(max(evidence.strength, 0.0), 1.0),
        "uncertainty_notes": "Deterministic fake review from first supporting evidence item.",
    }
    if evidence.source_id:
        claim["source_id"] = evidence.source_id
    elif evidence.computed_feature_id:
        claim["computed_feature_id"] = evidence.computed_feature_id

    return {
        "ticker": request.candidate.ticker,
        "as_of": request.candidate.as_of.isoformat(),
        "claims": [claim],
        "bear_case": [
            _fake_evidence_review_note(item)
            for item in request.candidate.disconfirming_evidence[:3]
        ],
        "unresolved_conflicts": _fake_unresolved_conflicts(request),
        "recommended_policy_downgrade": bool(request.candidate.hard_blocks),
    }


def _fake_skeptic_review_payload(request: LLMClientRequest) -> Mapping[str, Any]:
    evidence = _first_linked_evidence(
        (*request.candidate.disconfirming_evidence, *request.candidate.supporting_evidence)
    )
    item: dict[str, Any] = {
        "claim": evidence.summary,
        "severity": _severity_from_strength(evidence.strength),
        "confidence": min(max(evidence.strength, 0.0), 1.0),
        "why_it_matters": "This evidence can weaken confidence in the setup.",
    }
    _add_source_link(item, evidence)
    return {
        "ticker": request.candidate.ticker,
        "as_of": request.candidate.as_of.isoformat(),
        "schema_version": request.schema_version,
        "bear_case": [item],
        "missing_evidence": ["No additional fake-client evidence was supplied."],
        "contradictions": [
            str(conflict) for conflict in request.candidate.conflicts[:3]
        ],
        "recommended_policy_downgrade": bool(request.candidate.hard_blocks),
        "manual_review_notes": "Human reviewer should inspect evidence durability.",
    }


def _fake_decision_card_draft_payload(request: LLMClientRequest) -> Mapping[str, Any]:
    supporting = _first_linked_evidence(request.candidate.supporting_evidence)
    risk = _first_linked_evidence(
        (*request.candidate.disconfirming_evidence, *request.candidate.supporting_evidence)
    )
    supporting_point: dict[str, Any] = {"text": supporting.summary}
    risk_point: dict[str, Any] = {"text": risk.summary}
    _add_source_link(supporting_point, supporting)
    _add_source_link(risk_point, risk)
    return {
        "ticker": request.candidate.ticker,
        "as_of": request.candidate.as_of.isoformat(),
        "schema_version": request.schema_version,
        "summary": "Manual-review setup with source-linked evidence notes.",
        "supporting_points": [supporting_point],
        "risks": [risk_point],
        "questions_for_human": ["Is the source-linked setup still durable?"],
        "manual_review_only": True,
    }


def _first_linked_evidence(items: Sequence[EvidenceItem]) -> EvidenceItem:
    for item in items:
        if item.source_id or item.source_url or item.computed_feature_id:
            return item
    msg = "fake client requires at least one source-linked evidence item"
    raise ValueError(msg)


def _fake_evidence_review_note(evidence: EvidenceItem) -> Mapping[str, Any]:
    item: dict[str, Any] = {
        "claim": evidence.summary,
        "confidence": min(max(evidence.strength, 0.0), 1.0),
    }
    _add_source_link(item, evidence)
    return item


def _fake_unresolved_conflicts(request: LLMClientRequest) -> list[Mapping[str, Any]]:
    items: list[Mapping[str, Any]] = []
    allowed_reference_ids = set(request.evidence_packet.get("allowed_reference_ids", ()))
    allowed_computed_feature_ids = set(
        request.evidence_packet.get("allowed_computed_feature_ids", ())
    )
    for conflict in request.candidate.conflicts[:3]:
        item: dict[str, Any] = {
            "claim": str(conflict.get("kind") or "Unresolved evidence conflict."),
            "confidence": 0.5,
        }
        source_id = conflict.get("source_id") or conflict.get("source_url")
        computed_feature_id = conflict.get("computed_feature_id")
        if isinstance(source_id, str) and source_id in allowed_reference_ids:
            item["source_id"] = source_id
        elif (
            isinstance(computed_feature_id, str)
            and computed_feature_id in allowed_computed_feature_ids
        ):
            item["computed_feature_id"] = computed_feature_id
        else:
            continue
        items.append(item)
    return items


def _add_source_link(target: dict[str, Any], evidence: EvidenceItem) -> None:
    if evidence.source_id:
        target["source_id"] = evidence.source_id
    elif evidence.source_url:
        target["source_id"] = evidence.source_url
    elif evidence.computed_feature_id:
        target["computed_feature_id"] = evidence.computed_feature_id


def _severity_from_strength(strength: float) -> str:
    if strength >= 0.75:
        return "high"
    if strength >= 0.4:
        return "medium"
    return "low"


class LLMRouter:
    def __init__(
        self,
        *,
        budget: BudgetController,
        client: LLMClient,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.budget = budget
        self.client = client
        self.now = now or (lambda: datetime.now(UTC))

    def route(
        self,
        *,
        task: LLMTask,
        candidate: CandidatePacket,
        available_at: datetime,
    ) -> LLMRouteDecision:
        estimated_usage = _estimate_usage(candidate, task)
        model = self._model_for_task(task)
        attempted_at = datetime.now(UTC)
        budget_decision = self.budget.allow_llm_call(
            task=task,
            ticker=candidate.ticker,
            candidate_state=candidate.state,
            final_score=candidate.final_score,
            estimated_usage=estimated_usage,
            available_at=available_at,
        )
        if not budget_decision.allowed:
            entry = self._ledger_entry(
                task=task,
                candidate=candidate,
                available_at=available_at,
                status=LLMCallStatus.SKIPPED,
                estimated_cost=budget_decision.estimated_cost,
                actual_cost=0.0,
                model=model,
                provider=self.budget.config.llm_provider,
                skip_reason=budget_decision.reason,
                token_usage=estimated_usage,
                attempted_at=attempted_at,
            )
            self.budget.ledger_repo.upsert_entry(entry)
            return LLMRouteDecision(
                skip=True,
                reason=budget_decision.reason,
                task=task,
                model=model,
                estimated_cost=budget_decision.estimated_cost,
                max_tokens=task.max_output_tokens,
                estimated_usage=estimated_usage,
                ledger_entry=entry,
            )
        return LLMRouteDecision(
            skip=False,
            reason=None,
            task=task,
            model=model,
            estimated_cost=budget_decision.estimated_cost,
            max_tokens=task.max_output_tokens,
            estimated_usage=estimated_usage,
        )

    def review_candidate(
        self,
        *,
        task: LLMTask,
        candidate: CandidatePacket,
        available_at: datetime,
        dry_run: bool = False,
    ) -> LLMReviewResult:
        decision = self.route(task=task, candidate=candidate, available_at=available_at)
        if decision.skip:
            if decision.ledger_entry is None:
                msg = "skipped route must include a ledger entry"
                raise RuntimeError(msg)
            return LLMReviewResult(
                decision=decision,
                status=LLMCallStatus.SKIPPED,
                ledger_entry=decision.ledger_entry,
            )

        model = decision.model or "none"
        attempted_at = datetime.now(UTC)
        if dry_run:
            entry = self._ledger_entry(
                task=task,
                candidate=candidate,
                available_at=available_at,
                status=LLMCallStatus.DRY_RUN,
                estimated_cost=decision.estimated_cost,
                actual_cost=0.0,
                model=model,
                provider=self.budget.config.llm_provider,
                token_usage=decision.estimated_usage,
                attempted_at=attempted_at,
            )
            self.budget.ledger_repo.upsert_entry(entry)
            return LLMReviewResult(
                decision=decision,
                status=LLMCallStatus.DRY_RUN,
                ledger_entry=entry,
            )

        request = LLMClientRequest(
            task=task,
            candidate=candidate,
            candidate_json=canonical_packet_json(candidate),
            evidence_packet=build_agent_evidence_packet(candidate),
            prompt_version=task.prompt_version,
            schema_version=task.schema_version,
            model=model,
            max_output_tokens=task.max_output_tokens,
            estimated_usage=decision.estimated_usage,
        )
        try:
            client_result = self.client.complete(request)
        except Exception as exc:  # noqa: BLE001
            return self._failed_result(
                decision=decision,
                task=task,
                candidate=candidate,
                available_at=available_at,
                model=model,
                error=str(exc),
            )

        actual_cost = self.budget.estimate_cost(client_result.token_usage)
        try:
            payload = _validate_output(
                task=task,
                payload=client_result.payload,
                candidate=candidate,
                evidence_packet=request.evidence_packet,
            )
        except AgentSchemaError as exc:
            entry = self._ledger_entry(
                task=task,
                candidate=candidate,
                available_at=available_at,
                status=LLMCallStatus.SCHEMA_REJECTED,
                estimated_cost=decision.estimated_cost,
                actual_cost=actual_cost,
                model=client_result.model,
                provider=client_result.provider,
                skip_reason=LLMSkipReason.SCHEMA_VALIDATION_FAILED,
                token_usage=client_result.token_usage,
                tool_calls=client_result.tool_calls,
                payload={"error": str(exc)},
                attempted_at=attempted_at,
            )
            self.budget.ledger_repo.upsert_entry(entry)
            return LLMReviewResult(
                decision=decision,
                status=LLMCallStatus.SCHEMA_REJECTED,
                ledger_entry=entry,
                error=str(exc),
            )

        entry = self._ledger_entry(
            task=task,
            candidate=candidate,
            available_at=available_at,
            status=LLMCallStatus.COMPLETED,
            estimated_cost=decision.estimated_cost,
            actual_cost=actual_cost,
            model=client_result.model,
            provider=client_result.provider,
            token_usage=client_result.token_usage,
            tool_calls=client_result.tool_calls,
            outcome_label=_outcome_label(task),
            payload=payload,
            attempted_at=attempted_at,
        )
        self.budget.ledger_repo.upsert_entry(entry)
        return LLMReviewResult(
            decision=decision,
            status=LLMCallStatus.COMPLETED,
            ledger_entry=entry,
            payload=payload,
        )

    def _failed_result(
        self,
        *,
        decision: LLMRouteDecision,
        task: LLMTask,
        candidate: CandidatePacket,
        available_at: datetime,
        model: str,
        error: str,
    ) -> LLMReviewResult:
        entry = self._ledger_entry(
            task=task,
            candidate=candidate,
            available_at=available_at,
            status=LLMCallStatus.FAILED,
            estimated_cost=decision.estimated_cost,
            actual_cost=0.0,
            model=model,
            provider=self.budget.config.llm_provider,
            skip_reason=LLMSkipReason.CLIENT_ERROR,
            token_usage=decision.estimated_usage,
            payload={"error": error},
            attempted_at=datetime.now(UTC),
        )
        self.budget.ledger_repo.upsert_entry(entry)
        return LLMReviewResult(
            decision=decision,
            status=LLMCallStatus.FAILED,
            ledger_entry=entry,
            error=error,
        )

    def _ledger_entry(
        self,
        *,
        task: LLMTask,
        candidate: CandidatePacket,
        available_at: datetime,
        status: LLMCallStatus,
        estimated_cost: float,
        actual_cost: float,
        model: str | None,
        provider: str,
        skip_reason: LLMSkipReason | None = None,
        token_usage: TokenUsage | None = None,
        tool_calls: tuple[Mapping[str, Any], ...] = (),
        outcome_label: str | None = None,
        payload: Mapping[str, Any] | None = None,
        attempted_at: datetime | None = None,
    ) -> BudgetLedgerEntry:
        created_at = _aware_utc(attempted_at or datetime.now(UTC), "attempted_at")
        return BudgetLedgerEntry(
            id=budget_ledger_id(
                task=task.name.value,
                ticker=candidate.ticker,
                candidate_packet_id=candidate.id,
                status=status.value,
                available_at=available_at,
                prompt_version=task.prompt_version,
                attempted_at=created_at,
            ),
            ts=_aware_utc(self.now(), "now"),
            available_at=_aware_utc(available_at, "available_at"),
            task=task.name,
            status=status,
            estimated_cost=estimated_cost,
            actual_cost=actual_cost,
            ticker=candidate.ticker,
            candidate_state_id=candidate.candidate_state_id,
            candidate_packet_id=candidate.id,
            model=model,
            provider=provider,
            skip_reason=skip_reason,
            token_usage=token_usage or TokenUsage(),
            tool_calls=tool_calls,
            candidate_state=candidate.state.value,
            prompt_version=task.prompt_version,
            schema_version=task.schema_version,
            outcome_label=outcome_label,
            payload=payload or {},
            created_at=created_at,
        )

    def _model_for_task(self, task: LLMTask) -> str | None:
        model = getattr(self.budget.config, task.model_config_key)
        if model is None:
            return None
        text = str(model).strip()
        return text or None


def _estimate_usage(candidate: CandidatePacket, task: LLMTask) -> TokenUsage:
    canonical = canonical_packet_json(candidate)
    input_tokens = min(_token_estimate(canonical), task.max_input_tokens)
    return TokenUsage(input_tokens=input_tokens, output_tokens=task.max_output_tokens)


def _token_estimate(value: str) -> int:
    return max(1, (len(value.encode("utf-8")) + 3) // 4)


def _validate_output(
    *,
    task: LLMTask,
    payload: Mapping[str, Any],
    candidate: CandidatePacket,
    evidence_packet: Mapping[str, Any],
) -> Mapping[str, Any]:
    if task.schema_version == "evidence-review-v1":
        return validate_evidence_review_output(
            payload,
            ticker=candidate.ticker,
            as_of=candidate.as_of,
            evidence_packet=evidence_packet,
        )
    if task.schema_version == "skeptic-review-v1":
        return validate_skeptic_review_output(
            payload,
            ticker=candidate.ticker,
            as_of=candidate.as_of,
            evidence_packet=evidence_packet,
        )
    if task.schema_version == "decision-card-v1":
        return validate_decision_card_draft_output(
            payload,
            ticker=candidate.ticker,
            as_of=candidate.as_of,
            evidence_packet=evidence_packet,
        )
    msg = f"unsupported schema version: {task.schema_version}"
    raise AgentSchemaError(msg)


def _outcome_label(task: LLMTask) -> str:
    if task.schema_version == "skeptic-review-v1":
        return "skeptic_review"
    if task.schema_version == "decision-card-v1":
        return "decision_card_draft"
    return "evidence_review"


def _sentiment_from_polarity(polarity: str, strength: float) -> float:
    bounded = min(max(strength, 0.0), 1.0)
    if polarity == "disconfirming":
        return -bounded
    if polarity == "neutral":
        return 0.0
    return bounded


def _aware_utc(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        msg = f"{field_name} must be timezone-aware"
        raise ValueError(msg)
    return value.astimezone(UTC)


__all__ = [
    "FakeLLMClient",
    "LLMClient",
    "LLMClientRequest",
    "LLMClientResult",
    "LLMReviewResult",
    "LLMRouteDecision",
    "LLMRouter",
]

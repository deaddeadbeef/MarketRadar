from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from pydantic import BaseModel, ConfigDict, Field

from catalyst_radar.core.config import AppConfig
from catalyst_radar.security.access import Role, require_role
from catalyst_radar.storage.db import engine_from_url
from catalyst_radar.storage.validation_repositories import ValidationRepository
from catalyst_radar.validation.value_ledger import (
    build_value_ledger_entry,
    load_value_ledger_entries_payload,
    load_value_ledger_entry_payload,
    load_value_ledger_summary_payload,
    value_ledger_artifact_context,
    value_ledger_write_payload,
)

router = APIRouter(prefix="/api/value-ledger", tags=["value-ledger"])


class ValueLedgerEntryRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    artifact_type: str
    artifact_id: str
    label: str
    estimated_value_usd: float = Field(ge=0)
    confidence: float = Field(ge=0, le=1)
    as_of: date | None = None
    scan_run_id: str | None = None
    candidate_state_id: str | None = None
    candidate_packet_id: str | None = None
    decision_card_id: str | None = None
    ticker: str | None = None
    action_state: str | None = None
    priced_in_status: str | None = None
    priced_in_direction: str | None = None
    emotion_score: float | None = None
    reaction_score: float | None = None
    emotion_reaction_gap: float | None = None
    final_score: float | None = None
    setup_type: str | None = None
    supported_action: str | None = None
    user_decision: str | None = None
    cost_to_produce_usd: float = Field(default=0.0, ge=0)
    provider_call_count: int = Field(default=0, ge=0)
    llm_call_count: int = Field(default=0, ge=0)
    outcome_status: str = "pending"
    notes: str | None = None
    entry_date: date | None = None
    available_at: datetime | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
    execute: bool = False


def _engine():
    return engine_from_url(AppConfig.from_env().database_url)


@router.get("/summary", dependencies=[Depends(require_role(Role.VIEWER))])
def value_ledger_summary(
    available_at: Annotated[datetime | None, Query()] = None,
    period_start: Annotated[date | None, Query()] = None,
    period_end: Annotated[date | None, Query()] = None,
) -> dict[str, object]:
    try:
        return load_value_ledger_summary_payload(
            _engine(),
            available_at=available_at,
            period_start=period_start,
            period_end=period_end,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.get("/entries", dependencies=[Depends(require_role(Role.VIEWER))])
def value_ledger_entries(
    available_at: Annotated[datetime | None, Query()] = None,
    period_start: Annotated[date | None, Query()] = None,
    period_end: Annotated[date | None, Query()] = None,
    ticker: Annotated[str | None, Query()] = None,
    label: Annotated[str | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=1000)] = 200,
) -> dict[str, object]:
    return load_value_ledger_entries_payload(
        _engine(),
        available_at=available_at,
        period_start=period_start,
        period_end=period_end,
        ticker=ticker,
        label=label,
        limit=limit,
    )


@router.get("/entries/{entry_id}", dependencies=[Depends(require_role(Role.VIEWER))])
def value_ledger_entry(
    entry_id: Annotated[str, Path(min_length=1)],
) -> dict[str, object]:
    try:
        return load_value_ledger_entry_payload(_engine(), entry_id=entry_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/entries", dependencies=[Depends(require_role(Role.ANALYST))])
def value_ledger_record(request: ValueLedgerEntryRequest) -> dict[str, object]:
    try:
        engine = _engine()
        available_at = request.available_at or datetime.now(UTC)
        artifact_context = value_ledger_artifact_context(
            engine,
            artifact_type=request.artifact_type,
            artifact_id=request.artifact_id,
            available_at=available_at,
        )
        entry = build_value_ledger_entry(
            artifact_type=request.artifact_type,
            artifact_id=request.artifact_id,
            label=request.label,
            estimated_value_usd=request.estimated_value_usd,
            confidence=request.confidence,
            as_of=request.as_of,
            scan_run_id=request.scan_run_id,
            candidate_state_id=request.candidate_state_id,
            candidate_packet_id=request.candidate_packet_id,
            decision_card_id=request.decision_card_id,
            source="api",
            entry_date=request.entry_date,
            available_at=available_at,
            ticker=request.ticker,
            action_state=request.action_state,
            priced_in_status=request.priced_in_status,
            priced_in_direction=request.priced_in_direction,
            emotion_score=request.emotion_score,
            reaction_score=request.reaction_score,
            emotion_reaction_gap=request.emotion_reaction_gap,
            final_score=request.final_score,
            setup_type=request.setup_type,
            supported_action=request.supported_action,
            user_decision=request.user_decision,
            cost_to_produce_usd=request.cost_to_produce_usd,
            provider_call_count=request.provider_call_count,
            llm_call_count=request.llm_call_count,
            outcome_status=request.outcome_status,
            notes=request.notes,
            payload=request.payload,
            artifact_context=artifact_context,
        )
        if request.execute:
            ValidationRepository(engine).upsert_value_ledger_entry(entry)
        return value_ledger_write_payload(entry, execute=request.execute)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

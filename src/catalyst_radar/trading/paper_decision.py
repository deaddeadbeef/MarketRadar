from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime

from sqlalchemy.engine import Engine

from catalyst_radar.agents.paper_trading import paper_decision_command
from catalyst_radar.core.immutability import thaw_json_value
from catalyst_radar.core.models import ActionState
from catalyst_radar.security.audit import AuditLogRepository
from catalyst_radar.security.redaction import redact_text
from catalyst_radar.storage.validation_repositories import ValidationRepository
from catalyst_radar.validation.models import PaperDecision, PaperTrade
from catalyst_radar.validation.paper import create_paper_trade_from_card


class PaperDecisionExecutionError(ValueError):
    """Raised when a paper-decision request cannot be safely resolved."""


def run_paper_decision(
    engine: Engine,
    *,
    decision_card_id: str,
    decision: PaperDecision | str,
    available_at: datetime,
    entry_price: float | None = None,
    entry_at: datetime | None = None,
    override_reason: str | None = None,
    execute: bool = False,
    actor_source: str = "cli",
    actor_id: str | None = None,
    actor_role: str | None = None,
) -> dict[str, object]:
    validation_repo = ValidationRepository(engine)
    card = validation_repo.decision_card_payload(
        decision_card_id,
        available_at=available_at,
    )
    if card is None:
        raise PaperDecisionExecutionError(
            f"decision card not found: {decision_card_id}"
        )
    decision_value = PaperDecision(decision)
    hard_blocks = card_hard_blocks(card)
    if (
        decision_value == PaperDecision.APPROVED
        and hard_blocks
        and not _optional_text(override_reason)
    ):
        raise PaperDecisionExecutionError(
            "--override-reason is required to approve a blocked card"
        )
    resolved_entry_at = entry_at or (available_at if entry_price is not None else None)
    trade = create_paper_trade_from_card(
        card,
        decision_value,
        available_at=available_at,
        entry_price=entry_price,
        entry_at=resolved_entry_at,
    )
    payload = paper_decision_payload(
        trade=trade,
        decision_card_id=decision_card_id,
        decision=decision_value,
        available_at=available_at,
        entry_price=entry_price,
        entry_at=resolved_entry_at,
        override_reason=_optional_text(override_reason),
        hard_blocks=hard_blocks,
        execute=execute,
    )
    if execute:
        validation_repo.upsert_paper_trade(trade)
        append_paper_decision_audit_events(
            engine,
            card=card,
            trade=trade,
            decision=decision_value,
            hard_blocks=hard_blocks,
            override_reason=_optional_text(override_reason),
            occurred_at=available_at,
            actor_source=actor_source,
            actor_id=actor_id,
            actor_role=actor_role,
        )
    return payload


def paper_decision_payload(
    *,
    trade: PaperTrade,
    decision_card_id: str,
    decision: PaperDecision,
    available_at: datetime,
    entry_price: float | None,
    entry_at: datetime | None,
    override_reason: str | None,
    hard_blocks: Sequence[str],
    execute: bool,
) -> dict[str, object]:
    write_count = paper_decision_db_write_count(
        decision=decision,
        hard_blocks=hard_blocks,
    )
    return {
        "schema_version": "paper-decision-v1",
        "mode": "executed" if execute else "preview",
        "external_calls_required": 0,
        "external_calls_made": 0,
        "db_writes_required": write_count,
        "db_writes_made": write_count if execute else 0,
        "broker_order_submitted": False,
        "no_execution": True,
        "preview_command": paper_decision_command(
            decision_card_id=decision_card_id,
            decision=decision,
            available_at=available_at,
            entry_price=entry_price,
            entry_at=entry_at,
            override_reason=override_reason,
            execute=False,
        ),
        "execute_command": (
            paper_decision_command(
                decision_card_id=decision_card_id,
                decision=decision,
                available_at=available_at,
                entry_price=entry_price,
                entry_at=entry_at,
                override_reason=override_reason,
                execute=True,
            )
            if not execute
            else None
        ),
        "hard_blocks": list(hard_blocks),
        "trade": paper_trade_payload(trade),
        "next_action": (
            "Paper decision saved locally; no broker order was submitted."
            if execute
            else "Preview only. Re-run with --execute to write the paper-trade log row."
        ),
    }


def paper_trade_payload(trade: PaperTrade) -> dict[str, object]:
    return {
        "id": trade.id,
        "decision_card_id": trade.decision_card_id,
        "ticker": trade.ticker,
        "as_of": trade.as_of.isoformat(),
        "decision": trade.decision.value,
        "state": trade.state.value,
        "entry_price": trade.entry_price,
        "entry_at": trade.entry_at.isoformat() if trade.entry_at else None,
        "invalidation_price": trade.invalidation_price,
        "shares": trade.shares,
        "notional": trade.notional,
        "max_loss": trade.max_loss,
        "outcome_labels": thaw_json_value(trade.outcome_labels),
        "source_ts": trade.source_ts.isoformat(),
        "available_at": trade.available_at.isoformat(),
        "payload": thaw_json_value(trade.payload),
        "created_at": trade.created_at.isoformat(),
        "updated_at": trade.updated_at.isoformat(),
    }


def paper_decision_db_write_count(
    *,
    decision: PaperDecision,
    hard_blocks: Sequence[str],
) -> int:
    return 3 if decision == PaperDecision.APPROVED and hard_blocks else 2


def append_paper_decision_audit_events(
    engine: Engine,
    *,
    card: Mapping[str, object],
    trade: PaperTrade,
    decision: PaperDecision,
    hard_blocks: Sequence[str],
    override_reason: str | None,
    occurred_at: datetime,
    actor_source: str,
    actor_id: str | None = None,
    actor_role: str | None = None,
) -> None:
    repo = AuditLogRepository(engine)
    common_actor = {
        "actor_source": actor_source,
        "actor_id": actor_id,
        "actor_role": actor_role,
    }
    repo.append_event(
        event_type="paper_decision_recorded",
        artifact_type="decision_card",
        artifact_id=trade.decision_card_id,
        ticker=trade.ticker,
        decision_card_id=trade.decision_card_id,
        paper_trade_id=trade.id,
        decision=decision.value,
        reason=redact_text(override_reason) if override_reason is not None else None,
        hard_blocks=tuple(hard_blocks),
        status="success",
        metadata={
            "state": trade.state.value,
            "action_state": str(card.get("action_state") or ""),
            "manual_review_only": bool(trade.payload.get("manual_review_only")),
            "no_execution": bool(trade.payload.get("no_execution")),
        },
        after_payload={"paper_trade_id": trade.id, "state": trade.state.value},
        available_at=trade.available_at,
        occurred_at=occurred_at,
        **common_actor,
    )
    if decision == PaperDecision.APPROVED and hard_blocks:
        repo.append_event(
            event_type="hard_block_bypass_recorded",
            artifact_type="decision_card",
            artifact_id=trade.decision_card_id,
            ticker=trade.ticker,
            decision_card_id=trade.decision_card_id,
            paper_trade_id=trade.id,
            decision=decision.value,
            reason=redact_text(override_reason) if override_reason is not None else None,
            hard_blocks=tuple(hard_blocks),
            status="success",
            metadata={"state": trade.state.value},
            after_payload={"paper_trade_id": trade.id},
            available_at=trade.available_at,
            occurred_at=occurred_at,
            **common_actor,
        )


def card_hard_blocks(card: Mapping[str, object]) -> tuple[str, ...]:
    payload = _mapping(card.get("payload"))
    controls = _mapping(payload.get("controls"))
    portfolio = _mapping(payload.get("portfolio_impact"))
    values = [
        *_sequence(payload.get("hard_blocks")),
        *_sequence(controls.get("hard_blocks")),
        *_sequence(portfolio.get("hard_blocks")),
    ]
    hard_blocks = tuple(
        dict.fromkeys(str(value) for value in values if str(value).strip())
    )
    if hard_blocks:
        return hard_blocks
    if str(card.get("action_state") or "") == ActionState.BLOCKED.value:
        return ("blocked_action_state",)
    return ()


def _mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


def _sequence(value: object) -> list[object]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list | tuple):
        return list(value)
    return [value]


def _optional_text(value: str | None) -> str | None:
    if value is None:
        return None
    text = value.strip()
    return text or None


__all__ = [
    "PaperDecisionExecutionError",
    "append_paper_decision_audit_events",
    "card_hard_blocks",
    "paper_decision_db_write_count",
    "paper_decision_payload",
    "paper_trade_payload",
    "run_paper_decision",
]

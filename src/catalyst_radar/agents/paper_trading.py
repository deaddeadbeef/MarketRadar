from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from catalyst_radar.core.models import ActionState
from catalyst_radar.validation.models import PaperDecision

SCHEMA_VERSION = "agentic-paper-trade-intent-v1"


@dataclass(frozen=True)
class AgenticPaperTradeIntent:
    decision_card_id: str
    ticker: str
    action_state: ActionState
    status: str
    recommended_paper_decision: PaperDecision
    available_at: datetime
    entry_price: float | None
    entry_at: datetime | None
    hard_blocks: tuple[str, ...]
    specialist_rationale: tuple[Mapping[str, object], ...]
    preview_command: str
    execute_command: str

    def to_payload(self) -> dict[str, object]:
        return {
            "schema_version": SCHEMA_VERSION,
            "status": self.status,
            "decision_card_id": self.decision_card_id,
            "ticker": self.ticker,
            "action_state": self.action_state.value,
            "recommended_paper_decision": self.recommended_paper_decision.value,
            "requires_manual_approval": True,
            "requires_override_for_approval": bool(self.hard_blocks),
            "external_calls_required": 0,
            "external_calls_made": 0,
            "broker_order_submitted": False,
            "order_submission_allowed": False,
            "no_execution": True,
            "db_writes_required": 0,
            "db_writes_made": 0,
            "hard_blocks": list(self.hard_blocks),
            "specialist_rationale": [dict(item) for item in self.specialist_rationale],
            "paper_decision": {
                "decision_card_id": self.decision_card_id,
                "decision": self.recommended_paper_decision.value,
                "available_at": self.available_at.isoformat(),
                "entry_price": self.entry_price,
                "entry_at": self.entry_at.isoformat() if self.entry_at else None,
                "preview_command": self.preview_command,
                "execute_command": self.execute_command,
            },
            "next_action": _next_action(self.status, self.recommended_paper_decision),
        }


def build_agentic_paper_trade_intent(
    card: Mapping[str, Any] | object,
    *,
    available_at: datetime,
    entry_price: float | None = None,
    entry_at: datetime | None = None,
    override_reason: str | None = None,
) -> AgenticPaperTradeIntent:
    """Build a zero-execution paper-trade intent from one decision card.

    The manager consumes the already-stored decision-card evidence only. It does
    not call providers, brokers, OpenAI, shell, filesystem, or web tools.
    """

    payload = _mapping(_read(card, "payload", {}))
    decision_card_id = _required_text(_read(card, "id", ""), "decision_card_id")
    ticker = _required_text(_read(card, "ticker", ""), "ticker").upper()
    action_state = _action_state(card, payload)
    resolved_available_at = _aware_datetime(available_at, "available_at")
    resolved_entry_at = (
        _aware_datetime(entry_at, "entry_at")
        if entry_at is not None
        else resolved_available_at
        if entry_price is not None
        else None
    )
    resolved_entry_price = _optional_positive_float(entry_price)
    hard_blocks = _hard_blocks(card, payload, action_state)
    recommended_decision = (
        PaperDecision.APPROVED
        if action_state == ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW and not hard_blocks
        else PaperDecision.DEFERRED
    )
    preview_command = paper_decision_command(
        decision_card_id=decision_card_id,
        decision=recommended_decision,
        available_at=resolved_available_at,
        entry_price=resolved_entry_price,
        entry_at=resolved_entry_at,
        override_reason=override_reason if recommended_decision == PaperDecision.APPROVED else None,
        execute=False,
    )
    execute_command = paper_decision_command(
        decision_card_id=decision_card_id,
        decision=recommended_decision,
        available_at=resolved_available_at,
        entry_price=resolved_entry_price,
        entry_at=resolved_entry_at,
        override_reason=override_reason if recommended_decision == PaperDecision.APPROVED else None,
        execute=True,
    )
    status = "ready" if recommended_decision == PaperDecision.APPROVED else "blocked"
    return AgenticPaperTradeIntent(
        decision_card_id=decision_card_id,
        ticker=ticker,
        action_state=action_state,
        status=status,
        recommended_paper_decision=recommended_decision,
        available_at=resolved_available_at,
        entry_price=resolved_entry_price,
        entry_at=resolved_entry_at,
        hard_blocks=hard_blocks,
        specialist_rationale=_specialist_rationale(
            card=card,
            payload=payload,
            action_state=action_state,
            hard_blocks=hard_blocks,
            recommended_decision=recommended_decision,
            entry_price=resolved_entry_price,
            preview_command=preview_command,
        ),
        preview_command=preview_command,
        execute_command=execute_command,
    )


def paper_decision_command(
    *,
    decision_card_id: str,
    decision: PaperDecision,
    available_at: datetime,
    entry_price: float | None,
    entry_at: datetime | None,
    override_reason: str | None,
    execute: bool,
) -> str:
    parts = [
        "catalyst-radar",
        "paper-decision",
        "--decision-card-id",
        decision_card_id,
        "--decision",
        decision.value,
        "--available-at",
        _aware_datetime(available_at, "available_at").isoformat(),
    ]
    if entry_price is not None:
        parts.extend(["--entry-price", _number_text(entry_price)])
    if entry_at is not None:
        parts.extend(["--entry-at", _aware_datetime(entry_at, "entry_at").isoformat()])
    if override_reason is not None:
        parts.extend(["--override-reason", override_reason])
    parts.append("--execute" if execute else "--preview")
    parts.append("--json")
    return " ".join(_quote_cli_part(part) for part in parts)


def _hard_blocks(
    card: Mapping[str, Any] | object,
    payload: Mapping[str, Any],
    action_state: ActionState,
) -> tuple[str, ...]:
    controls = _mapping(payload.get("controls"))
    portfolio = _mapping(payload.get("portfolio_impact"))
    trade_plan = _mapping(payload.get("trade_plan"))
    values = [
        *_sequence_texts(payload.get("hard_blocks")),
        *_sequence_texts(controls.get("hard_blocks")),
        *_sequence_texts(portfolio.get("hard_blocks")),
    ]
    if str(_read(card, "action_state", "") or "") == ActionState.BLOCKED.value:
        values.append("blocked_action_state")
    if action_state != ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW:
        values.append("action_state_not_manual_review_eligible")
    values.extend(
        f"missing_trade_plan:{field}"
        for field in _missing_trade_plan_fields(trade_plan)
    )
    return tuple(dict.fromkeys(value for value in values if value))


def _missing_trade_plan_fields(trade_plan: Mapping[str, Any]) -> tuple[str, ...]:
    fields = list(_sequence_texts(trade_plan.get("missing_fields")))
    if not trade_plan.get("entry_zone"):
        fields.append("entry_zone")
    invalidation = _optional_positive_float(trade_plan.get("invalidation_price"))
    if invalidation is None:
        fields.append("invalidation_price")
    reward_risk = _optional_positive_float(trade_plan.get("reward_risk"))
    if reward_risk is None:
        fields.append("reward_risk")
    return tuple(dict.fromkeys(field for field in fields if field))


def _specialist_rationale(
    *,
    card: Mapping[str, Any] | object,
    payload: Mapping[str, Any],
    action_state: ActionState,
    hard_blocks: Sequence[str],
    recommended_decision: PaperDecision,
    entry_price: float | None,
    preview_command: str,
) -> tuple[Mapping[str, object], ...]:
    del card
    trade_plan = _mapping(payload.get("trade_plan"))
    sizing = _mapping(payload.get("position_sizing"))
    portfolio = _mapping(payload.get("portfolio_impact"))
    max_loss = portfolio.get("max_loss") or trade_plan.get("max_loss_if_wrong") or 0
    portfolio_blocks = ", ".join(_sequence_texts(portfolio.get("hard_blocks"))) or "none"
    evidence = _sequence_mappings(payload.get("evidence"))
    disconfirming = _sequence_mappings(payload.get("disconfirming_evidence"))
    score = _mapping(payload.get("scores")).get("final_score")
    setup_type = _mapping(payload.get("identity")).get("setup_type")
    return (
        {
            "agent": "Catalyst Analyst",
            "role": "Source-linked opportunity thesis",
            "summary": _evidence_summary(evidence, fallback="No supporting evidence was attached."),
            "confidence": "medium",
        },
        {
            "agent": "Skeptic",
            "role": "Disconfirming evidence and evidence gaps",
            "summary": _evidence_summary(
                disconfirming,
                fallback="No disconfirming evidence was attached.",
            ),
            "confidence": "high" if hard_blocks else "medium",
        },
        {
            "agent": "Market Structure Analyst",
            "role": "Entry, invalidation, reward/risk, and setup shape",
            "summary": (
                f"setup={setup_type or 'unknown'}; score={score or 'unknown'}; "
                f"entry_zone={trade_plan.get('entry_zone') or 'missing'}; "
                f"entry_price={entry_price if entry_price is not None else 'pending'}; "
                f"invalidation={trade_plan.get('invalidation_price') or 'missing'}; "
                f"reward_risk={trade_plan.get('reward_risk') or 'missing'}."
            ),
            "confidence": "medium",
        },
        {
            "agent": "Portfolio Manager",
            "role": "Sizing and portfolio impact",
            "summary": (
                f"shares={sizing.get('shares', 0)}; notional={sizing.get('notional', 0)}; "
                f"max_loss={max_loss}; portfolio_blocks={portfolio_blocks}."
            ),
            "confidence": "high",
        },
        {
            "agent": "Execution Planner",
            "role": "Paper-trade route only",
            "summary": (
                f"Recommended paper decision is {recommended_decision.value}; "
                f"preview command is `{preview_command}`."
            ),
            "confidence": "high",
        },
        {
            "agent": "Risk Governor",
            "role": "Deterministic permission boundary",
            "summary": (
                f"action_state={action_state.value}; "
                f"hard_blocks={', '.join(hard_blocks) or 'none'}; "
                "no broker order submission is allowed."
            ),
            "confidence": "high",
        },
    )


def _next_action(status: str, decision: PaperDecision) -> str:
    if status == "ready" and decision == PaperDecision.APPROVED:
        return (
            "Preview only. If the paper-trade thesis matches your intent, run the "
            "execute command to write a local paper-trade row; no broker order is submitted."
        )
    return (
        "No broker order is submitted. Resolve hard blocks or keep this as a deferred "
        "paper decision for follow-up evidence."
    )


def _action_state(card: Mapping[str, Any] | object, payload: Mapping[str, Any]) -> ActionState:
    raw = _read(card, "action_state", None) or _mapping(payload.get("identity")).get(
        "action_state"
    )
    return ActionState(str(raw))


def _read(source: Mapping[str, Any] | object, key: str, default: Any = None) -> Any:
    if isinstance(source, Mapping):
        return source.get(key, default)
    return getattr(source, key, default)


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _sequence_texts(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value.strip()] if value.strip() else []
    if isinstance(value, list | tuple):
        return [str(item).strip() for item in value if str(item).strip()]
    return [str(value).strip()] if str(value).strip() else []


def _sequence_mappings(value: object) -> list[Mapping[str, Any]]:
    if not isinstance(value, list | tuple):
        return []
    return [item for item in value if isinstance(item, Mapping)]


def _evidence_summary(rows: Sequence[Mapping[str, Any]], *, fallback: str) -> str:
    if not rows:
        return fallback
    first = rows[0]
    title = str(first.get("title") or "Evidence").strip()
    summary = str(first.get("summary") or "").strip()
    return f"{title}: {summary}" if summary else title


def _aware_datetime(value: object, field_name: str) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if not isinstance(value, datetime):
        msg = f"{field_name} must be a datetime"
        raise TypeError(msg)
    if value.tzinfo is None or value.utcoffset() is None:
        msg = f"{field_name} must be timezone-aware"
        raise ValueError(msg)
    return value.astimezone(UTC)


def _optional_positive_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number <= 0 or number != number:
        return None
    return number


def _required_text(value: object, field_name: str) -> str:
    text = str(value).strip()
    if not text:
        msg = f"{field_name} must not be blank"
        raise ValueError(msg)
    return text


def _number_text(value: float) -> str:
    return str(int(value)) if float(value).is_integer() else str(value)


def _quote_cli_part(value: object) -> str:
    text = str(value)
    if not text:
        return '""'
    if any(char.isspace() for char in text) or '"' in text:
        return '"' + text.replace('"', '\\"') + '"'
    return text


__all__ = [
    "AgenticPaperTradeIntent",
    "build_agentic_paper_trade_intent",
    "paper_decision_command",
]

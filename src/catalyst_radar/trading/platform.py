from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from catalyst_radar.agents.paper_trading import (
    AgenticPaperTradeIntent,
    build_agentic_paper_trade_intent,
)
from catalyst_radar.brokers.order_preview import (
    OrderPreviewRequest,
    build_disabled_order_preview,
)
from catalyst_radar.core.config import AppConfig

SCHEMA_VERSION = "agentic-trading-platform-plan-v1"


@dataclass(frozen=True)
class TradingPlatformPlan:
    decision_card_id: str
    ticker: str
    status: str
    autonomy_level: str
    available_at: datetime
    strategy_proposal: Mapping[str, object]
    risk_approval: Mapping[str, object]
    order_intent: Mapping[str, object]
    execution_controls: Mapping[str, object]
    supervision: Mapping[str, object]
    capability_map: Sequence[Mapping[str, object]]
    agentic_intent: AgenticPaperTradeIntent
    audit: Mapping[str, object]
    next_action: str

    def to_payload(self) -> dict[str, object]:
        return {
            "schema_version": SCHEMA_VERSION,
            "status": self.status,
            "autonomy_level": self.autonomy_level,
            "decision_card_id": self.decision_card_id,
            "ticker": self.ticker,
            "available_at": self.available_at.isoformat(),
            "external_calls_required": 0,
            "external_calls_made": 0,
            "db_writes_required": 0,
            "db_writes_made": 0,
            "broker_order_submitted": False,
            "order_submission_allowed": False,
            "no_execution": True,
            "strategy_proposal": dict(self.strategy_proposal),
            "risk_approval": dict(self.risk_approval),
            "order_intent": dict(self.order_intent),
            "execution_controls": dict(self.execution_controls),
            "supervision": dict(self.supervision),
            "capability_map": [dict(item) for item in self.capability_map],
            "agentic_paper_intent": self.agentic_intent.to_payload(),
            "audit": dict(self.audit),
            "next_action": self.next_action,
        }


def build_trading_platform_plan(
    card: Mapping[str, Any] | object,
    *,
    available_at: datetime,
    entry_price: float | None = None,
    entry_at: datetime | None = None,
    override_reason: str | None = None,
    config: AppConfig | None = None,
    broker_data_stale: bool = False,
) -> TradingPlatformPlan:
    """Build a zero-execution trading platform plan from a stored Decision Card."""

    resolved_config = config or AppConfig()
    resolved_available_at = _aware_datetime(available_at, "available_at")
    payload = _mapping(_read(card, "payload", {}))
    trade_plan = _mapping(payload.get("trade_plan"))
    sizing = _mapping(payload.get("position_sizing"))
    portfolio = _mapping(payload.get("portfolio_impact"))
    identity = _mapping(payload.get("identity"))

    resolved_entry_price, entry_price_source = _resolve_entry_price(
        explicit_entry_price=entry_price,
        trade_plan=trade_plan,
    )
    agentic_intent = build_agentic_paper_trade_intent(
        card,
        available_at=resolved_available_at,
        entry_price=resolved_entry_price,
        entry_at=entry_at,
        override_reason=override_reason,
    )
    invalidation_price = _optional_positive_float(trade_plan.get("invalidation_price"))
    reward_risk = _optional_positive_float(
        trade_plan.get("reward_risk") or _mapping(payload.get("scores")).get("reward_risk")
    )
    shares = _optional_positive_float(sizing.get("shares")) or 0.0
    notional = _resolved_notional(
        shares=shares,
        entry_price=resolved_entry_price,
        sizing=sizing,
    )
    max_loss = _optional_positive_float(
        portfolio.get("max_loss") or trade_plan.get("max_loss_if_wrong")
    )
    direction = _strategy_direction(payload)
    side = "buy" if direction == "bullish" else "unsupported"
    risk_per_trade_pct = (
        _optional_positive_float(sizing.get("risk_per_trade_pct"))
        or resolved_config.risk_per_trade_pct
    )
    order_preview = build_disabled_order_preview(
        OrderPreviewRequest(
            ticker=agentic_intent.ticker,
            side="buy",
            entry_price=resolved_entry_price or 0.0,
            invalidation_price=invalidation_price or 0.0,
            risk_per_trade_pct=risk_per_trade_pct,
        ),
        portfolio_context={
            "portfolio_equity": _portfolio_equity(
                payload=payload,
                config=resolved_config,
                notional=notional,
            ),
            "broker_data_stale": broker_data_stale,
        },
        config=resolved_config,
    )
    paper_trade_blocks = _paper_trade_blocks(
        agentic_blocks=agentic_intent.hard_blocks,
        side=side,
        shares=shares,
        entry_price=resolved_entry_price,
        invalidation_price=invalidation_price,
    )
    live_submission_blocks = tuple(_sequence_texts(order_preview.get("hard_blocks")))
    approved_for_paper_trade = agentic_intent.status == "ready" and not paper_trade_blocks
    status = "ready_for_paper_trade" if approved_for_paper_trade else "blocked"
    autonomy_level = "L2_paper_supervised" if approved_for_paper_trade else "L1_agentic_review"

    strategy_proposal = {
        "decision_card_id": agentic_intent.decision_card_id,
        "ticker": agentic_intent.ticker,
        "action_state": agentic_intent.action_state.value,
        "setup_type": _text(identity.get("setup_type") or _read(card, "setup_type", "")),
        "direction": direction,
        "final_score": _optional_positive_float(_read(card, "final_score", None)),
        "entry_zone": _entry_zone(trade_plan),
        "entry_price": resolved_entry_price,
        "entry_price_source": entry_price_source,
        "invalidation_price": invalidation_price,
        "reward_risk": reward_risk,
        "target_price": _optional_positive_float(trade_plan.get("target_price")),
        "time_stop_days": _optional_positive_int(trade_plan.get("time_stop_days")),
        "next_review_at": _datetime_text(
            _read(card, "next_review_at", None)
            or _mapping(payload.get("controls")).get("next_review_at")
        ),
        "supporting_evidence_count": len(_sequence_mappings(payload.get("evidence"))),
        "disconfirming_evidence_count": len(
            _sequence_mappings(payload.get("disconfirming_evidence"))
        ),
    }
    risk_approval = {
        "approved_for_paper_trade": approved_for_paper_trade,
        "approved_for_live_submission": False,
        "paper_trade_blocks": list(paper_trade_blocks),
        "live_submission_blocks": list(live_submission_blocks),
        "portfolio_hard_blocks": _sequence_texts(portfolio.get("hard_blocks")),
        "limits": {
            "risk_per_trade_pct": risk_per_trade_pct,
            "max_single_name_pct": resolved_config.max_single_name_pct,
            "max_sector_pct": resolved_config.max_sector_pct,
            "max_theme_pct": resolved_config.max_theme_pct,
        },
        "estimated_max_loss": max_loss,
        "requires_manual_approval": True,
        "requires_override_for_paper_approval": bool(paper_trade_blocks),
        "live_submission_reason": "broker submission is disabled by the platform kill switch",
    }
    order_intent = {
        "route": "paper_trade_only",
        "side": side,
        "quantity": shares,
        "limit_price": resolved_entry_price,
        "stop_price": invalidation_price,
        "estimated_notional": notional,
        "estimated_max_loss": max_loss,
        "submission_allowed": False,
        "broker_order_submitted": False,
        "disabled_order_preview": order_preview,
    }
    execution_controls = {
        "external_calls_required": 0,
        "external_calls_made": 0,
        "db_writes_required": 0,
        "db_writes_made": 0,
        "broker_order_submitted": False,
        "order_submission_allowed": False,
        "no_execution": True,
        "paper_trade_write_allowed_by_this_command": False,
        "live_trading_kill_switch": "engaged",
        "broker_adapter_mode": "read_only",
        "schwab_order_submission_enabled": bool(
            resolved_config.schwab_order_submission_enabled
        ),
    }
    supervision = {
        "requires_manual_approval": True,
        "requires_override_for_approval": bool(paper_trade_blocks),
        "human_actions_required": _human_actions_required(
            approved_for_paper_trade=approved_for_paper_trade,
            paper_trade_blocks=paper_trade_blocks,
        ),
        "paper_decision_preview_command": agentic_intent.preview_command,
        "paper_decision_execute_command": agentic_intent.execute_command,
        "audit_required": True,
        "no_autonomous_execution": True,
    }
    audit = {
        "source": "trading_platform_plan",
        "decision_card_available_at": _datetime_text(_read(card, "available_at", None)),
        "candidate_packet_id": _mapping(payload.get("audit")).get("candidate_packet_id"),
        "provider_calls": 0,
        "model_calls": 0,
        "broker_calls": 0,
        "database_writes": 0,
    }
    return TradingPlatformPlan(
        decision_card_id=agentic_intent.decision_card_id,
        ticker=agentic_intent.ticker,
        status=status,
        autonomy_level=autonomy_level,
        available_at=resolved_available_at,
        strategy_proposal=strategy_proposal,
        risk_approval=risk_approval,
        order_intent=order_intent,
        execution_controls=execution_controls,
        supervision=supervision,
        capability_map=_capability_map(
            agentic_status=agentic_intent.status,
            paper_status=status,
        ),
        agentic_intent=agentic_intent,
        audit=audit,
        next_action=_next_action(approved_for_paper_trade, paper_trade_blocks),
    )


def _paper_trade_blocks(
    *,
    agentic_blocks: Sequence[str],
    side: str,
    shares: float,
    entry_price: float | None,
    invalidation_price: float | None,
) -> tuple[str, ...]:
    blocks = list(agentic_blocks)
    if side != "buy":
        blocks.append("unsupported_order_side")
    if shares <= 0:
        blocks.append("missing_position_sizing:shares")
    if entry_price is None:
        blocks.append("missing_order_intent:entry_price")
    if invalidation_price is None:
        blocks.append("missing_order_intent:invalidation_price")
    return tuple(dict.fromkeys(block for block in blocks if block))


def _resolve_entry_price(
    *,
    explicit_entry_price: float | None,
    trade_plan: Mapping[str, Any],
) -> tuple[float | None, str]:
    explicit = _optional_positive_float(explicit_entry_price)
    if explicit is not None:
        return explicit, "explicit"
    entry_zone = _entry_zone(trade_plan)
    if entry_zone:
        return entry_zone[0], "trade_plan_entry_zone_low"
    return None, "missing"


def _entry_zone(trade_plan: Mapping[str, Any]) -> list[float]:
    raw = trade_plan.get("entry_zone")
    if not isinstance(raw, list | tuple):
        return []
    values = [_optional_positive_float(item) for item in raw]
    return [value for value in values if value is not None]


def _resolved_notional(
    *,
    shares: float,
    entry_price: float | None,
    sizing: Mapping[str, Any],
) -> float:
    if shares > 0 and entry_price is not None:
        return round(shares * entry_price, 2)
    return _optional_positive_float(sizing.get("notional")) or 0.0


def _portfolio_equity(
    *,
    payload: Mapping[str, Any],
    config: AppConfig,
    notional: float,
) -> float:
    portfolio = _mapping(payload.get("portfolio_impact"))
    configured = _optional_positive_float(config.portfolio_value)
    if configured is not None:
        return configured
    for key in ("portfolio_equity", "portfolio_value", "account_equity"):
        value = _optional_positive_float(portfolio.get(key))
        if value is not None:
            return value
    if notional > 0 and config.max_single_name_pct > 0:
        return round(notional / config.max_single_name_pct, 2)
    return 0.0


def _strategy_direction(payload: Mapping[str, Any]) -> str:
    identity = _mapping(payload.get("identity"))
    priced_in = _mapping(payload.get("priced_in"))
    values = [
        identity.get("direction"),
        identity.get("setup_type"),
        priced_in.get("direction"),
        priced_in.get("status"),
    ]
    text = " ".join(str(value).lower() for value in values if value)
    return "bearish" if "bearish" in text else "bullish"


def _capability_map(
    *,
    agentic_status: str,
    paper_status: str,
) -> tuple[Mapping[str, object], ...]:
    return (
        {
            "level": "L0",
            "name": "market_scout",
            "status": "available",
            "description": "Stored MarketRadar scan, packet, and decision-card evidence.",
        },
        {
            "level": "L1",
            "name": "agentic_review",
            "status": agentic_status,
            "description": "Specialist rationale and guarded paper-decision commands.",
        },
        {
            "level": "L2",
            "name": "supervised_paper_trade",
            "status": paper_status,
            "description": "Human-approved local paper-trade workflow.",
        },
        {
            "level": "L3",
            "name": "broker_order_ticket_preview",
            "status": "disabled",
            "description": "Order-ticket writes are outside this read-only command.",
        },
        {
            "level": "L4",
            "name": "supervised_live_submission",
            "status": "disabled",
            "description": "Broker submission kill switch remains engaged.",
        },
        {
            "level": "L5",
            "name": "autonomous_live_trading",
            "status": "out_of_scope",
            "description": "Autonomous live trading is not implemented.",
        },
    )


def _human_actions_required(
    *,
    approved_for_paper_trade: bool,
    paper_trade_blocks: Sequence[str],
) -> list[str]:
    if approved_for_paper_trade:
        return [
            "Review the strategy proposal and risk approval.",
            "Run the paper-decision preview command.",
            "Run the paper-decision execute command only for a local paper-trade write.",
        ]
    blocks = ", ".join(paper_trade_blocks) or "unknown block"
    return [
        f"Resolve or explicitly defer paper-trade blocks: {blocks}.",
        "Do not submit broker orders.",
    ]


def _next_action(approved_for_paper_trade: bool, paper_trade_blocks: Sequence[str]) -> str:
    if approved_for_paper_trade:
        return (
            "Review the paper-trade-only plan, then run the paper-decision preview "
            "command before any local paper-trade write."
        )
    blocks = ", ".join(paper_trade_blocks) or "risk controls"
    return f"No trade action is allowed by this plan. Resolve or defer: {blocks}."


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


def _text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _datetime_text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return value
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            value = value.replace(tzinfo=UTC)
        return value.astimezone(UTC).isoformat()
    return str(value)


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


def _optional_positive_int(value: object) -> int | None:
    number = _optional_positive_float(value)
    return int(number) if number is not None else None


__all__ = [
    "TradingPlatformPlan",
    "build_trading_platform_plan",
]

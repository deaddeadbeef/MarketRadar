from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta
from typing import Any

from sqlalchemy import Engine, select

from catalyst_radar.core.immutability import thaw_json_value
from catalyst_radar.storage.schema import (
    alerts,
    candidate_packets,
    candidate_states,
    decision_cards,
    paper_trades,
)
from catalyst_radar.storage.validation_repositories import ValidationRepository
from catalyst_radar.validation.models import ValueLedgerEntry, value_ledger_entry_id

CHATGPT_PRO_MONTHLY_COST_USD = 200.0
TARGET_MONTHLY_VALUE_USD = 40.0
ALLOWED_FEEDBACK_LABELS = frozenset(
    {
        "useful",
        "noisy",
        "acted",
        "ignored",
        "avoided-loss",
        "missed",
        "false-positive",
        "false-negative",
        "too-late",
        "too-early",
        "good-research",
        "duplicate",
        "not-understandable",
        "blocked-correctly",
    }
)
ALLOWED_SUPPORTED_ACTIONS = frozenset(
    {"watch", "research", "avoid", "paper_trade", "reject", "live_review", "no_action"}
)
ALLOWED_USER_DECISIONS = frozenset(
    {"accepted", "rejected", "wait", "ignored", "paper-only", "avoided", "unknown"}
)
ALLOWED_ARTIFACT_TYPES = frozenset(
    {
        "candidate_state",
        "candidate_packet",
        "decision_card",
        "paper_trade",
        "alert",
        "priced_in_answer",
        "shadow_run",
        "manual_note",
    }
)
USEFUL_DEFINITION = (
    "Useful means a logged MarketRadar artifact changed a manual review decision, "
    "saved research time, avoided a bad action, or created a forward-testable "
    "market-emotion/priced-in hypothesis."
)


def build_value_ledger_entry(
    *,
    artifact_type: str,
    artifact_id: str,
    label: str,
    estimated_value_usd: float,
    confidence: float,
    source: str,
    entry_date: date | None = None,
    available_at: datetime | None = None,
    as_of: date | None = None,
    scan_run_id: str | None = None,
    candidate_state_id: str | None = None,
    candidate_packet_id: str | None = None,
    decision_card_id: str | None = None,
    ticker: str | None = None,
    action_state: str | None = None,
    priced_in_status: str | None = None,
    priced_in_direction: str | None = None,
    emotion_score: float | None = None,
    reaction_score: float | None = None,
    emotion_reaction_gap: float | None = None,
    final_score: float | None = None,
    setup_type: str | None = None,
    supported_action: str | None = None,
    user_decision: str | None = None,
    cost_to_produce_usd: float = 0.0,
    provider_call_count: int = 0,
    llm_call_count: int = 0,
    outcome_status: str = "pending",
    notes: str | None = None,
    payload: Mapping[str, Any] | None = None,
    artifact_context: Mapping[str, Any] | None = None,
    created_at: datetime | None = None,
) -> ValueLedgerEntry:
    resolved_available_at = _to_utc_datetime(available_at or datetime.now(UTC), "available_at")
    resolved_entry_date = entry_date or resolved_available_at.date()
    resolved_source = _required_text(source, "source")
    resolved_artifact_type = _allowed_value(
        artifact_type,
        allowed=ALLOWED_ARTIFACT_TYPES,
        field_name="artifact_type",
    )
    resolved_label = _allowed_value(
        label,
        allowed=ALLOWED_FEEDBACK_LABELS,
        field_name="label",
    )
    context = dict(artifact_context or {})
    resolved_ticker = ticker or _optional_text(context.get("ticker"))
    resolved_as_of = as_of or _date_from_context(context.get("as_of"))
    resolved_candidate_state_id = (
        candidate_state_id
        or _optional_text(context.get("candidate_state_id"))
        or (artifact_id if resolved_artifact_type == "candidate_state" else None)
    )
    resolved_candidate_packet_id = (
        candidate_packet_id
        or _optional_text(context.get("candidate_packet_id"))
        or (artifact_id if resolved_artifact_type == "candidate_packet" else None)
    )
    resolved_decision_card_id = (
        decision_card_id
        or _optional_text(context.get("decision_card_id"))
        or (artifact_id if resolved_artifact_type == "decision_card" else None)
    )
    resolved_action_state = action_state or _optional_text(
        context.get("action_state") or context.get("state")
    )
    resolved_final_score = final_score
    if resolved_final_score is None and context.get("final_score") is not None:
        resolved_final_score = float(context["final_score"])
    resolved_setup_type = setup_type or _optional_text(context.get("setup_type"))
    resolved_supported_action = _optional_allowed_value(
        supported_action,
        allowed=ALLOWED_SUPPORTED_ACTIONS,
        field_name="supported_action",
    )
    resolved_user_decision = _optional_allowed_value(
        user_decision,
        allowed=ALLOWED_USER_DECISIONS,
        field_name="user_decision",
    )
    return ValueLedgerEntry(
        id=value_ledger_entry_id(
            artifact_type=resolved_artifact_type,
            artifact_id=artifact_id,
            label=resolved_label,
            entry_date=resolved_entry_date,
            source=resolved_source,
        ),
        entry_date=resolved_entry_date,
        as_of=resolved_as_of,
        scan_run_id=scan_run_id,
        candidate_state_id=resolved_candidate_state_id,
        candidate_packet_id=resolved_candidate_packet_id,
        decision_card_id=resolved_decision_card_id,
        artifact_type=resolved_artifact_type,
        artifact_id=artifact_id,
        ticker=resolved_ticker,
        label=resolved_label,
        action_state=resolved_action_state,
        priced_in_status=priced_in_status,
        priced_in_direction=priced_in_direction,
        emotion_score=emotion_score,
        reaction_score=reaction_score,
        emotion_reaction_gap=emotion_reaction_gap,
        final_score=resolved_final_score,
        setup_type=resolved_setup_type,
        supported_action=resolved_supported_action,
        user_decision=resolved_user_decision,
        estimated_value_usd=estimated_value_usd,
        confidence=confidence,
        cost_to_produce_usd=cost_to_produce_usd,
        provider_call_count=provider_call_count,
        llm_call_count=llm_call_count,
        outcome_status=outcome_status,
        source=resolved_source,
        notes=notes,
        available_at=resolved_available_at,
        payload=payload or {},
        created_at=_to_utc_datetime(created_at or resolved_available_at, "created_at"),
        updated_at=_to_utc_datetime(created_at or resolved_available_at, "updated_at"),
    )


def value_ledger_write_payload(
    entry: ValueLedgerEntry,
    *,
    execute: bool,
) -> dict[str, object]:
    return {
        "schema_version": "value-ledger-entry-plan-v1",
        "mode": "executed" if execute else "preview",
        "external_calls_required": 0,
        "external_calls_made": 0,
        "db_writes_required": 1,
        "db_writes_made": 1 if execute else 0,
        "entry": value_ledger_entry_payload(entry),
        "next_action": (
            "Value ledger entry saved."
            if execute
            else "Preview only. Re-run with --execute to write this value entry."
        ),
        "useful_definition": USEFUL_DEFINITION,
        "allowed_feedback_labels": sorted(ALLOWED_FEEDBACK_LABELS),
    }


def value_ledger_entry_payload(entry: ValueLedgerEntry) -> dict[str, object]:
    return {
        "id": entry.id,
        "entry_date": entry.entry_date.isoformat(),
        "as_of": entry.as_of.isoformat() if entry.as_of is not None else None,
        "scan_run_id": entry.scan_run_id,
        "candidate_state_id": entry.candidate_state_id,
        "candidate_packet_id": entry.candidate_packet_id,
        "decision_card_id": entry.decision_card_id,
        "artifact_type": entry.artifact_type,
        "artifact_id": entry.artifact_id,
        "ticker": entry.ticker,
        "label": entry.label,
        "action_state": entry.action_state,
        "priced_in_status": entry.priced_in_status,
        "priced_in_direction": entry.priced_in_direction,
        "emotion_score": entry.emotion_score,
        "reaction_score": entry.reaction_score,
        "emotion_reaction_gap": entry.emotion_reaction_gap,
        "final_score": entry.final_score,
        "setup_type": entry.setup_type,
        "supported_action": entry.supported_action,
        "user_decision": entry.user_decision,
        "estimated_value_usd": entry.estimated_value_usd,
        "attribution_estimate_usd": entry.estimated_value_usd,
        "confidence": entry.confidence,
        "confidence_weighted_value_usd": entry.estimated_value_usd * entry.confidence,
        "cost_to_produce_usd": entry.cost_to_produce_usd,
        "provider_call_count": entry.provider_call_count,
        "llm_call_count": entry.llm_call_count,
        "outcome_status": entry.outcome_status,
        "source": entry.source,
        "notes": entry.notes,
        "available_at": entry.available_at.isoformat(),
        "payload": thaw_json_value(entry.payload),
        "created_at": entry.created_at.isoformat(),
        "updated_at": entry.updated_at.isoformat(),
    }


def load_value_ledger_entries_payload(
    engine: Engine,
    *,
    available_at: datetime | None = None,
    period_start: date | None = None,
    period_end: date | None = None,
    ticker: str | None = None,
    label: str | None = None,
    limit: int = 200,
) -> dict[str, object]:
    cutoff = _to_utc_datetime(available_at or datetime.now(UTC), "available_at")
    entries = ValidationRepository(engine).list_value_ledger_entries(
        available_at=cutoff,
        period_start=period_start,
        period_end=period_end,
        ticker=ticker,
        label=label,
        limit=limit,
    )
    return {
        "schema_version": "value-ledger-entries-v1",
        "external_calls_made": 0,
        "db_writes_made": 0,
        "available_at": cutoff.isoformat(),
        "filters": {
            "period_start": period_start.isoformat() if period_start else None,
            "period_end": period_end.isoformat() if period_end else None,
            "ticker": ticker.upper() if ticker is not None and ticker.strip() else None,
            "label": label if label is not None and str(label).strip() else None,
            "limit": limit,
        },
        "count": len(entries),
        "entries": [value_ledger_entry_payload(entry) for entry in entries],
    }


def load_value_ledger_entry_payload(
    engine: Engine,
    *,
    entry_id: str,
) -> dict[str, object]:
    resolved_id = _required_text(entry_id, "entry_id")
    entry = ValidationRepository(engine).value_ledger_entry(resolved_id)
    if entry is None:
        msg = f"value ledger entry not found: {resolved_id}"
        raise ValueError(msg)
    return {
        "schema_version": "value-ledger-entry-v1",
        "external_calls_made": 0,
        "db_writes_made": 0,
        "entry": value_ledger_entry_payload(entry),
    }


def load_value_ledger_summary_payload(
    engine: Engine,
    *,
    available_at: datetime | None = None,
    period_start: date | None = None,
    period_end: date | None = None,
    target_monthly_value_usd: float = TARGET_MONTHLY_VALUE_USD,
) -> dict[str, object]:
    cutoff = _to_utc_datetime(available_at or datetime.now(UTC), "available_at")
    resolved_start, resolved_end = _resolved_period(
        cutoff.date(),
        period_start=period_start,
        period_end=period_end,
    )
    target = _positive_float(target_monthly_value_usd, "target_monthly_value_usd")
    entries = ValidationRepository(engine).list_value_ledger_entries(
        available_at=cutoff,
        period_start=resolved_start,
        period_end=resolved_end,
        limit=1000,
    )
    total_value = sum(entry.estimated_value_usd for entry in entries)
    weighted_value = sum(
        entry.estimated_value_usd * entry.confidence for entry in entries
    )
    total_cost = sum(entry.cost_to_produce_usd for entry in entries)
    provider_calls = sum(entry.provider_call_count for entry in entries)
    llm_calls = sum(entry.llm_call_count for entry in entries)
    useful_entries = [entry for entry in entries if entry.estimated_value_usd > 0]
    return {
        "schema_version": "value-ledger-summary-v1",
        "source": "value_ledger",
        "external_calls_made": 0,
        "db_writes_made": 0,
        "available_at": cutoff.isoformat(),
        "period_start": resolved_start.isoformat(),
        "period_end": resolved_end.isoformat(),
        "currency": "USD",
        "entry_count": len(entries),
        "useful_entry_count": len(useful_entries),
        "total_estimated_value_usd": round(total_value, 4),
        "confidence_weighted_value_usd": round(weighted_value, 4),
        "cost_to_produce_usd": round(total_cost, 4),
        "net_confidence_weighted_value_usd": round(weighted_value - total_cost, 4),
        "provider_call_count": provider_calls,
        "llm_call_count": llm_calls,
        "target_monthly_value_usd": target,
        "target_coverage_pct": round((weighted_value / target) * 100, 2)
        if target > 0
        else None,
        "chatgpt_pro_monthly_cost_usd": CHATGPT_PRO_MONTHLY_COST_USD,
        "chatgpt_pro_offset_pct": round(
            (weighted_value / CHATGPT_PRO_MONTHLY_COST_USD) * 100,
            2,
        ),
        "useful_definition": USEFUL_DEFINITION,
        "labels": _label_totals(entries),
        "outcome_status_counts": _string_counts(
            entry.outcome_status for entry in entries
        ),
        "user_decision_counts": _string_counts(
            entry.user_decision or "unknown" for entry in entries
        ),
        "top_entries": [
            value_ledger_entry_payload(entry)
            for entry in sorted(
                entries,
                key=lambda row: (
                    row.estimated_value_usd * row.confidence,
                    row.entry_date,
                    row.id,
                ),
                reverse=True,
            )[:10]
        ],
    }


def value_ledger_artifact_context(
    engine: Engine,
    *,
    artifact_type: str,
    artifact_id: str,
    available_at: datetime,
) -> dict[str, Any]:
    resolved_type = _allowed_value(
        artifact_type,
        allowed=ALLOWED_ARTIFACT_TYPES,
        field_name="artifact_type",
    )
    table = {
        "candidate_state": candidate_states,
        "candidate_packet": candidate_packets,
        "decision_card": decision_cards,
        "paper_trade": paper_trades,
        "alert": alerts,
    }.get(resolved_type)
    if table is None:
        return {}
    cutoff = _to_utc_datetime(available_at, "available_at")
    filters = [table.c.id == _required_text(artifact_id, "artifact_id")]
    if "available_at" in table.c:
        filters.append(table.c.available_at <= cutoff)
    elif "created_at" in table.c:
        filters.append(table.c.created_at <= cutoff)
    with engine.connect() as conn:
        row = conn.execute(select(table).where(*filters).limit(1)).first()
    if row is None:
        msg = f"referenced {resolved_type} artifact not found"
        raise ValueError(msg)
    return dict(row._mapping)


def _resolved_period(
    reference_date: date,
    *,
    period_start: date | None,
    period_end: date | None,
) -> tuple[date, date]:
    start = period_start or reference_date.replace(day=1)
    end = period_end or _month_end(start)
    if end < start:
        msg = "period_end must be greater than or equal to period_start"
        raise ValueError(msg)
    return start, end


def _month_end(start: date) -> date:
    if start.month == 12:
        return start.replace(year=start.year + 1, month=1, day=1) - timedelta(days=1)
    return start.replace(month=start.month + 1, day=1) - timedelta(days=1)


def _label_totals(entries: list[ValueLedgerEntry]) -> list[dict[str, object]]:
    totals: dict[str, dict[str, float | int | str]] = {}
    for entry in entries:
        row = totals.setdefault(
            entry.label,
            {
                "label": entry.label,
                "entry_count": 0,
                "total_estimated_value_usd": 0.0,
                "confidence_weighted_value_usd": 0.0,
            },
        )
        row["entry_count"] = int(row["entry_count"]) + 1
        row["total_estimated_value_usd"] = (
            float(row["total_estimated_value_usd"]) + entry.estimated_value_usd
        )
        row["confidence_weighted_value_usd"] = (
            float(row["confidence_weighted_value_usd"])
            + entry.estimated_value_usd * entry.confidence
        )
    return [
        {
            **row,
            "total_estimated_value_usd": round(
                float(row["total_estimated_value_usd"]),
                4,
            ),
            "confidence_weighted_value_usd": round(
                float(row["confidence_weighted_value_usd"]),
                4,
            ),
        }
        for row in sorted(
            totals.values(),
            key=lambda item: float(item["confidence_weighted_value_usd"]),
            reverse=True,
        )
    ]


def _string_counts(values: object) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        key = str(value).strip() or "unknown"
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


def _allowed_value(
    value: object,
    *,
    allowed: frozenset[str],
    field_name: str,
) -> str:
    text = _required_text(value, field_name)
    if text not in allowed:
        msg = f"{field_name} must be one of: {', '.join(sorted(allowed))}"
        raise ValueError(msg)
    return text


def _optional_allowed_value(
    value: object | None,
    *,
    allowed: frozenset[str],
    field_name: str,
) -> str | None:
    if value is None:
        return None
    return _allowed_value(value, allowed=allowed, field_name=field_name)


def _required_text(value: object, field_name: str) -> str:
    text = str(value).strip()
    if not text:
        msg = f"{field_name} must not be blank"
        raise ValueError(msg)
    return text


def _optional_text(value: object | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _date_from_context(value: object | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.date()
    return None


def _positive_float(value: object, field_name: str) -> float:
    number = float(value)
    if number <= 0 or number != number or number in {float("inf"), float("-inf")}:
        msg = f"{field_name} must be a finite positive number"
        raise ValueError(msg)
    return number


def _to_utc_datetime(value: datetime, field_name: str) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if not isinstance(value, datetime):
        msg = f"{field_name} must be a datetime"
        raise TypeError(msg)
    if value.tzinfo is None or value.utcoffset() is None:
        msg = f"{field_name} must be timezone-aware"
        raise ValueError(msg)
    return value.astimezone(UTC)

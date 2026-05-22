from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping
from datetime import UTC, date, datetime, timedelta
from math import isfinite

from sqlalchemy.engine import Engine

from catalyst_radar.core.immutability import thaw_json_value
from catalyst_radar.storage.validation_repositories import ValidationRepository
from catalyst_radar.validation.models import ValueLedgerEntry, ValueOutcome
from catalyst_radar.validation.value_ledger import (
    CHATGPT_PRO_MONTHLY_COST_USD,
    TARGET_MONTHLY_VALUE_USD,
    USEFUL_DEFINITION,
    value_ledger_entry_payload,
)
from catalyst_radar.validation.value_outcomes import value_outcome_payload

USEFUL_LABELS = frozenset(
    {"useful", "good-research", "acted", "avoided-loss", "blocked-correctly"}
)
NOISY_LABELS = frozenset(
    {
        "noisy",
        "duplicate",
        "not-understandable",
        "too-late",
        "too-early",
        "false-positive",
    }
)
MISSED_LABELS = frozenset({"missed", "false-negative"})
AVOIDED_LABELS = frozenset({"avoided-loss"})
FALSE_POSITIVE_LABELS = frozenset({"false-positive"})
DEFAULT_MIN_USEFUL_EVIDENCE_COUNT = 2


def monthly_value_report_payload(
    engine: Engine,
    *,
    month: str,
    available_at: datetime | None = None,
    target_monthly_value_usd: float = TARGET_MONTHLY_VALUE_USD,
    min_useful_evidence_count: int = DEFAULT_MIN_USEFUL_EVIDENCE_COUNT,
) -> dict[str, object]:
    start, end = month_bounds(month)
    cutoff = _to_utc_datetime(available_at or datetime.now(UTC), "available_at")
    target = _positive_float(target_monthly_value_usd, "target_monthly_value_usd")
    min_evidence = max(1, int(min_useful_evidence_count))
    repo = ValidationRepository(engine)
    entries = repo.list_value_ledger_entries(
        available_at=cutoff,
        period_start=start,
        period_end=end,
        limit=10_000,
    )
    entry_ids = {entry.id for entry in entries}
    outcomes = [
        outcome
        for outcome in repo.list_value_outcomes(available_at=cutoff, limit=10_000)
        if outcome.value_ledger_entry_id in entry_ids
    ]
    label_counts = Counter(entry.label for entry in entries)
    user_decision_counts = Counter(entry.user_decision or "unknown" for entry in entries)
    useful_entries = [entry for entry in entries if _is_useful_entry(entry)]
    noisy_entries = [entry for entry in entries if entry.label in NOISY_LABELS]
    false_positive_entries = [
        entry for entry in entries if entry.label in FALSE_POSITIVE_LABELS
    ]
    missed_entries = [entry for entry in entries if entry.label in MISSED_LABELS]
    avoided_entries = [entry for entry in entries if _is_avoided_entry(entry)]
    acted_entries = [entry for entry in entries if _is_acted_entry(entry)]
    ignored_entries = [entry for entry in entries if _is_ignored_entry(entry)]
    useful_decision_cards = [
        entry for entry in useful_entries if entry.artifact_type == "decision_card"
    ]
    total_estimated_value = sum(entry.estimated_value_usd for entry in useful_entries)
    weighted_value = sum(_claimable_weighted_value(entry) for entry in entries)
    ledger_cost = sum(entry.cost_to_produce_usd for entry in entries)
    operating_time_cost = sum(_payload_float(entry, "operating_time_cost_usd") for entry in entries)
    provider_api_model_cost = ledger_cost
    total_cost = provider_api_model_cost + operating_time_cost
    net_value = weighted_value - total_cost
    uncertainty = _uncertainty_band(entries, total_cost=total_cost)
    verdict = _monthly_value_verdict(
        net_value=net_value,
        target=target,
        useful_count=len(useful_entries),
        min_useful_evidence_count=min_evidence,
    )
    report = {
        "schema_version": "monthly-value-report-v1",
        "source": "value_ledger_and_outcomes",
        "month": month,
        "period_start": start.isoformat(),
        "period_end": end.isoformat(),
        "available_at": cutoff.isoformat(),
        "currency": "USD",
        "verdict": verdict,
        "target_monthly_value_usd": target,
        "threshold_monthly_value_usd": target,
        "threshold_met": net_value >= target,
        "plausibly_met_40_usd_threshold": verdict == "pass",
        "plausibly_earned_at_least_40_usd": verdict == "pass",
        "decision_support_value_not_profit": True,
        "profit_calculation_included": False,
        "profit_usd": None,
        "investment_advice": False,
        "entry_count": len(entries),
        "useful_insights_count": len(useful_entries),
        "noisy_insights_count": len(noisy_entries),
        "acted_insights_count": len(acted_entries),
        "ignored_insights_count": len(ignored_entries),
        "avoided_bad_entries_count": len(avoided_entries),
        "paper_trade_outcome_count": _paper_trade_outcome_count(entries, outcomes),
        "false_positive_count": len(false_positive_entries),
        "missed_signal_count": len(missed_entries),
        "estimated_research_time_saved_hours": round(
            sum(
                _payload_float(entry, "research_time_saved_minutes")
                for entry in entries
            )
            / 60.0,
            4,
        ),
        "estimated_research_time_saved_usd": round(
            sum(_payload_float(entry, "research_time_saved_usd") for entry in entries),
            4,
        ),
        "estimated_avoided_loss_usd": round(
            sum(_claimable_weighted_value(entry) for entry in avoided_entries),
            4,
        ),
        "estimated_opportunity_value_usd": round(
            sum(
                _claimable_weighted_value(entry)
                for entry in entries
                if not _is_avoided_entry(entry)
            ),
            4,
        ),
        "total_estimated_value_usd": round(total_estimated_value, 4),
        "confidence_weighted_value_usd": round(weighted_value, 4),
        "provider_api_model_costs_usd": round(provider_api_model_cost, 4),
        "operating_time_cost_usd": round(operating_time_cost, 4),
        "total_cost_usd": round(total_cost, 4),
        "net_decision_support_value_usd": round(net_value, 4),
        "cost_per_useful_alert": _ratio(total_cost, len(useful_entries)),
        "cost_per_useful_decision_card": _ratio(total_cost, len(useful_decision_cards)),
        "provider_call_count": sum(entry.provider_call_count for entry in entries),
        "llm_call_count": sum(entry.llm_call_count for entry in entries),
        "chatgpt_pro_monthly_cost_usd": CHATGPT_PRO_MONTHLY_COST_USD,
        "chatgpt_pro_offset_pct": _ratio(weighted_value, CHATGPT_PRO_MONTHLY_COST_USD, scale=100),
        "confidence_uncertainty_band_usd": uncertainty,
        "label_counts": dict(sorted(label_counts.items())),
        "user_decision_counts": dict(sorted(user_decision_counts.items())),
        "outcome_status_counts": _outcome_status_counts(outcomes),
        "best_useful_insights": [
            value_ledger_entry_payload(entry)
            for entry in _top_entries(useful_entries, reverse=True)[:5]
        ],
        "noisy_or_false_positive_insights": [
            value_ledger_entry_payload(entry)
            for entry in _top_entries(_dedupe_entries(noisy_entries + false_positive_entries))[
                :5
            ]
        ],
        "linked_outcomes": [value_outcome_payload(outcome) for outcome in outcomes[:20]],
        "useful_definition": USEFUL_DEFINITION,
        "decision_support_note": (
            "This report measures attributed decision-support value, not realized "
            "trading profit or investment advice."
        ),
        "verdict_reason": _verdict_reason(
            verdict,
            net_value=net_value,
            target=target,
            useful_count=len(useful_entries),
            min_useful_evidence_count=min_evidence,
        ),
        "external_calls_made": 0,
        "db_writes_made": 0,
    }
    return report


def month_bounds(month: str) -> tuple[date, date]:
    text = str(month).strip()
    if len(text) != 7 or text[4] != "-":
        msg = "month must be formatted as YYYY-MM"
        raise ValueError(msg)
    try:
        start = date.fromisoformat(f"{text}-01")
    except ValueError as exc:
        msg = "month must be formatted as YYYY-MM"
        raise ValueError(msg) from exc
    if start.month == 12:
        end = start.replace(year=start.year + 1, month=1, day=1) - timedelta(days=1)
    else:
        end = start.replace(month=start.month + 1, day=1) - timedelta(days=1)
    return start, end


def _monthly_value_verdict(
    *,
    net_value: float,
    target: float,
    useful_count: int,
    min_useful_evidence_count: int,
) -> str:
    if useful_count < min_useful_evidence_count:
        return "insufficient_evidence"
    return "pass" if net_value >= target else "fail"


def _verdict_reason(
    verdict: str,
    *,
    net_value: float,
    target: float,
    useful_count: int,
    min_useful_evidence_count: int,
) -> str:
    if verdict == "insufficient_evidence":
        return (
            f"Only {useful_count} useful evidence row(s); "
            f"{min_useful_evidence_count} required before pass/fail."
        )
    if verdict == "pass":
        return f"Net decision-support value {net_value:.2f} meets target {target:.2f}."
    return f"Net decision-support value {net_value:.2f} is below target {target:.2f}."


def _is_useful_entry(entry: ValueLedgerEntry) -> bool:
    return entry.label in USEFUL_LABELS


def _is_avoided_entry(entry: ValueLedgerEntry) -> bool:
    return entry.label in AVOIDED_LABELS or entry.user_decision == "avoided"


def _is_acted_entry(entry: ValueLedgerEntry) -> bool:
    return entry.label == "acted" or entry.user_decision in {
        "accepted",
        "paper-only",
        "avoided",
    }


def _is_ignored_entry(entry: ValueLedgerEntry) -> bool:
    return entry.label == "ignored" or entry.user_decision == "ignored"


def _paper_trade_outcome_count(
    entries: Iterable[ValueLedgerEntry],
    outcomes: Iterable[ValueOutcome],
) -> int:
    paper_trade_entry_ids = {
        entry.id
        for entry in entries
        if entry.artifact_type == "paper_trade" or entry.supported_action == "paper_trade"
    }
    return sum(1 for outcome in outcomes if outcome.value_ledger_entry_id in paper_trade_entry_ids)


def _outcome_status_counts(outcomes: Iterable[ValueOutcome]) -> dict[str, int]:
    counts = Counter(outcome.status for outcome in outcomes)
    return dict(sorted(counts.items()))


def _weighted_value(entry: ValueLedgerEntry) -> float:
    return entry.estimated_value_usd * entry.confidence


def _claimable_weighted_value(entry: ValueLedgerEntry) -> float:
    if not _is_useful_entry(entry):
        return 0.0
    return _weighted_value(entry)


def _payload_float(entry: ValueLedgerEntry, key: str) -> float:
    payload = thaw_json_value(entry.payload)
    if not isinstance(payload, Mapping):
        return 0.0
    value = payload.get(key)
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    return number if isfinite(number) and number > 0 else 0.0


def _uncertainty_band(
    entries: Iterable[ValueLedgerEntry],
    *,
    total_cost: float,
) -> dict[str, object]:
    rows = list(entries)
    lower = sum(
        entry.estimated_value_usd * entry.confidence * entry.confidence
        for entry in rows
        if _is_useful_entry(entry)
    )
    upper = sum(
        entry.estimated_value_usd * min(1.0, entry.confidence + ((1.0 - entry.confidence) / 2))
        for entry in rows
        if _is_useful_entry(entry)
    )
    return {
        "method": "confidence_squared_lower_and_half_uncertainty_upper",
        "lower_net_value_usd": round(lower - total_cost, 4),
        "base_net_value_usd": round(
            sum(_claimable_weighted_value(entry) for entry in rows) - total_cost,
            4,
        ),
        "upper_net_value_usd": round(upper - total_cost, 4),
    }


def _top_entries(
    entries: Iterable[ValueLedgerEntry],
    *,
    reverse: bool = True,
) -> list[ValueLedgerEntry]:
    return sorted(
        entries,
        key=lambda entry: (_weighted_value(entry), entry.entry_date, entry.id),
        reverse=reverse,
    )


def _dedupe_entries(entries: Iterable[ValueLedgerEntry]) -> list[ValueLedgerEntry]:
    seen: set[str] = set()
    unique: list[ValueLedgerEntry] = []
    for entry in entries:
        if entry.id in seen:
            continue
        seen.add(entry.id)
        unique.append(entry)
    return unique


def _ratio(numerator: float, denominator: float, *, scale: float = 1.0) -> float | None:
    if denominator <= 0:
        return None
    return round((numerator / denominator) * scale, 4)


def _positive_float(value: object, field_name: str) -> float:
    number = float(value)
    if number <= 0 or not isfinite(number):
        msg = f"{field_name} must be a positive finite number"
        raise ValueError(msg)
    return number


def _to_utc_datetime(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        msg = f"{field_name} must be a datetime"
        raise TypeError(msg)
    if value.tzinfo is None or value.utcoffset() is None:
        msg = f"{field_name} must be timezone-aware"
        raise ValueError(msg)
    return value.astimezone(UTC)

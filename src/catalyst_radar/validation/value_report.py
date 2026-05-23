from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping
from datetime import UTC, date, datetime, timedelta
from math import isfinite

from sqlalchemy.engine import Engine

from catalyst_radar.core.immutability import thaw_json_value
from catalyst_radar.storage.validation_repositories import ValidationRepository
from catalyst_radar.validation.baselines import (
    NEWS_EVENT_ONLY_SCREENER,
    RANDOM_SECTOR_MATCHED_BASKET,
    RELATIVE_STRENGTH_SCREENER,
    SECTOR_ETF_ROTATION_SCREENER,
    VOLUME_BREAKOUT_SCREENER,
)
from catalyst_radar.validation.models import ValidationRun, ValueLedgerEntry, ValueOutcome
from catalyst_radar.validation.reports import build_validation_report, validation_report_payload
from catalyst_radar.validation.value_ledger import (
    CHATGPT_PRO_MONTHLY_COST_USD,
    CLAIMABLE_VALUE_LABELS,
    TARGET_MONTHLY_VALUE_USD,
    USEFUL_DEFINITION,
    load_value_ledger_candidate_coverage_payload,
    value_ledger_entry_payload,
)
from catalyst_radar.validation.value_outcomes import (
    load_value_outcome_coverage_payload,
    value_outcome_payload,
)

USEFUL_LABELS = CLAIMABLE_VALUE_LABELS
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
MISSION_BRIEF_BASELINES = (
    RELATIVE_STRENGTH_SCREENER,
    VOLUME_BREAKOUT_SCREENER,
    SECTOR_ETF_ROTATION_SCREENER,
    NEWS_EVENT_ONLY_SCREENER,
    RANDOM_SECTOR_MATCHED_BASKET,
)


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
    candidate_coverage = load_value_ledger_candidate_coverage_payload(
        engine,
        available_at=cutoff,
        period_start=start,
        period_end=end,
        limit=20,
    )
    outcome_coverage = load_value_outcome_coverage_payload(
        engine,
        available_at=cutoff,
        period_start=start,
        period_end=end,
        limit=20,
    )
    entry_ids = {entry.id for entry in entries}
    outcomes = [
        outcome
        for outcome in repo.list_value_outcomes(available_at=cutoff, limit=10_000)
        if outcome.value_ledger_entry_id in entry_ids
    ]
    outcomes_by_entry_id = _latest_outcomes_by_entry_id(outcomes)
    validation_evidence = _validation_evidence_summary(
        repo,
        available_at=cutoff,
        period_start=start,
        period_end=end,
    )
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
    llm_reviewed_entries = [entry for entry in entries if entry.llm_call_count > 0]
    useful_llm_reviewed_entries = [
        entry for entry in llm_reviewed_entries if _is_useful_entry(entry)
    ]
    useful_decision_cards = [
        entry for entry in useful_entries if entry.artifact_type == "decision_card"
    ]
    useful_evidence_examples = _value_evidence_examples(
        _top_entries(useful_entries, reverse=True),
        outcomes_by_entry_id=outcomes_by_entry_id,
        category="useful",
        limit=5,
    )
    noisy_evidence_examples = _value_evidence_examples(
        _top_entries(_dedupe_entries(noisy_entries + false_positive_entries)),
        outcomes_by_entry_id=outcomes_by_entry_id,
        category="noisy_or_false_positive",
        limit=5,
    )
    total_estimated_value = sum(entry.estimated_value_usd for entry in useful_entries)
    weighted_value = sum(_claimable_weighted_value(entry) for entry in entries)
    ledger_cost = sum(entry.cost_to_produce_usd for entry in entries)
    operating_time_cost = sum(_payload_float(entry, "operating_time_cost_usd") for entry in entries)
    provider_api_model_cost = ledger_cost
    llm_reviewed_cost = sum(entry.cost_to_produce_usd for entry in llm_reviewed_entries)
    total_cost = provider_api_model_cost + operating_time_cost
    net_value = weighted_value - total_cost
    uncertainty = _uncertainty_band(entries, total_cost=total_cost)
    raw_value_verdict = _monthly_value_verdict(
        net_value=net_value,
        target=target,
        useful_count=len(useful_entries),
        min_useful_evidence_count=min_evidence,
    )
    candidate_ledger_coverage = _candidate_ledger_coverage_summary(candidate_coverage)
    value_outcome_coverage = _value_outcome_coverage_summary(outcome_coverage)
    first_evidence_gap = _monthly_value_first_evidence_gap(
        candidate_ledger_coverage=candidate_ledger_coverage,
        value_outcome_coverage=value_outcome_coverage,
        validation_evidence=validation_evidence,
        verdict=raw_value_verdict,
        useful_count=len(useful_entries),
        min_useful_evidence_count=min_evidence,
    )
    verdict = (
        raw_value_verdict
        if first_evidence_gap["first_blocker"] is None
        else "insufficient_evidence"
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
        "first_blocker": first_evidence_gap["first_blocker"],
        "first_gap_count": first_evidence_gap["first_gap_count"],
        "canonical_next_action": first_evidence_gap["canonical_next_action"],
        "canonical_next_command": first_evidence_gap["canonical_next_command"],
        "next_action": first_evidence_gap["canonical_next_action"],
        "entry_count": len(entries),
        "candidate_ledger_coverage": candidate_ledger_coverage,
        "value_outcome_coverage": value_outcome_coverage,
        "validation_evidence": validation_evidence,
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
        "llm_reviewed_costs_usd": round(llm_reviewed_cost, 4),
        "operating_time_cost_usd": round(operating_time_cost, 4),
        "total_cost_usd": round(total_cost, 4),
        "net_decision_support_value_usd": round(net_value, 4),
        "cost_per_useful_alert": _ratio(total_cost, len(useful_entries)),
        "cost_per_useful_decision_card": _ratio(total_cost, len(useful_decision_cards)),
        "llm_reviewed_entry_count": len(llm_reviewed_entries),
        "useful_llm_reviewed_entry_count": len(useful_llm_reviewed_entries),
        "cost_per_useful_llm_reviewed_candidate": _ratio(
            llm_reviewed_cost,
            len(useful_llm_reviewed_entries),
        ),
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
        "best_useful_evidence_examples": useful_evidence_examples,
        "noisy_or_false_positive_evidence_examples": noisy_evidence_examples,
        "value_evidence_examples": useful_evidence_examples + noisy_evidence_examples,
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
            first_blocker=first_evidence_gap["first_blocker"],
            first_gap_count=int(first_evidence_gap["first_gap_count"] or 0),
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


def _candidate_ledger_coverage_summary(
    coverage: Mapping[str, object],
) -> dict[str, object]:
    rows = coverage.get("rows")
    rows = rows if isinstance(rows, Iterable) else ()
    return {
        "schema_version": "monthly-value-candidate-ledger-coverage-v1",
        "status": coverage.get("status") or "unknown",
        "surfaced_candidate_count": int(coverage.get("surfaced_candidate_count") or 0),
        "logged_candidate_count": int(coverage.get("logged_candidate_count") or 0),
        "missing_ledger_count": int(coverage.get("missing_ledger_count") or 0),
        "coverage_pct": coverage.get("coverage_pct"),
        "first_missing_candidate_state_id": coverage.get(
            "first_missing_candidate_state_id"
        ),
        "first_missing_ticker": coverage.get("first_missing_ticker"),
        "canonical_next_command": coverage.get("canonical_next_command"),
        "rows": [row for row in rows if isinstance(row, Mapping)],
        "next_action": coverage.get("next_action"),
        "external_calls_made": int(coverage.get("external_calls_made") or 0),
        "db_writes_made": int(coverage.get("db_writes_made") or 0),
    }


def _monthly_value_first_evidence_gap(
    *,
    candidate_ledger_coverage: Mapping[str, object],
    value_outcome_coverage: Mapping[str, object],
    validation_evidence: Mapping[str, object],
    verdict: str,
    useful_count: int,
    min_useful_evidence_count: int,
) -> dict[str, object]:
    candidate_status = str(candidate_ledger_coverage.get("status") or "")
    if candidate_status == "gaps":
        action = (
            "Record the first missing value-ledger row before claiming monthly "
            "value evidence."
        )
        return _monthly_value_gap(
            first_blocker="candidate_ledger_coverage",
            first_gap_count=int(candidate_ledger_coverage.get("missing_ledger_count") or 0),
            action=action,
            command=candidate_ledger_coverage.get("canonical_next_command"),
        )

    outcome_status = str(value_outcome_coverage.get("status") or "")
    outcome_entry_count = int(value_outcome_coverage.get("ledger_entry_count") or 0)
    if outcome_entry_count > 0 and outcome_status != "ready":
        return _monthly_value_gap(
            first_blocker="value_outcome_coverage",
            first_gap_count=int(value_outcome_coverage.get("missing_outcome_count") or 0),
            action=str(
                value_outcome_coverage.get("next_action")
                or "Compute missing value outcomes before claiming monthly value evidence."
            ),
            command=value_outcome_coverage.get("canonical_next_command"),
        )

    if verdict == "insufficient_evidence":
        missing = max(0, min_useful_evidence_count - useful_count)
        if missing > 0:
            return _monthly_value_gap(
                first_blocker="useful_evidence",
                first_gap_count=missing,
                action=(
                    "Record enough useful/noisy value-ledger evidence for a monthly "
                    "pass/fail verdict."
                ),
                command=None,
            )

    if validation_evidence.get("ready") is not True:
        missing_baselines = validation_evidence.get("missing_baselines")
        insufficient_baselines = validation_evidence.get("insufficient_baselines")
        baseline_gaps: set[str] = set()
        if isinstance(missing_baselines, list):
            baseline_gaps.update(str(name) for name in missing_baselines)
        if isinstance(insufficient_baselines, list):
            baseline_gaps.update(str(name) for name in insufficient_baselines)
        return _monthly_value_gap(
            first_blocker="validation_evidence",
            first_gap_count=len(baseline_gaps),
            action=str(
                validation_evidence.get("next_action")
                or "Run validation evidence before claiming monthly value proof."
            ),
            command=validation_evidence.get("canonical_next_command"),
        )

    return _monthly_value_gap(
        first_blocker=None,
        first_gap_count=0,
        action="Monthly value evidence is ready for review.",
        command=None,
    )


def _monthly_value_gap(
    *,
    first_blocker: str | None,
    first_gap_count: int,
    action: str,
    command: object,
) -> dict[str, object]:
    resolved_command = command if isinstance(command, str) and command.strip() else None
    return {
        "first_blocker": first_blocker,
        "first_gap_count": max(0, int(first_gap_count)),
        "canonical_next_action": action,
        "canonical_next_command": resolved_command,
    }


def _value_outcome_coverage_summary(
    coverage: Mapping[str, object],
) -> dict[str, object]:
    rows = coverage.get("rows")
    rows = rows if isinstance(rows, Iterable) else ()
    return {
        "schema_version": "monthly-value-outcome-coverage-v1",
        "status": coverage.get("status") or "unknown",
        "ledger_entry_count": int(coverage.get("ledger_entry_count") or 0),
        "linked_outcome_count": int(coverage.get("linked_outcome_count") or 0),
        "missing_outcome_count": int(coverage.get("missing_outcome_count") or 0),
        "first_missing_value_ledger_entry_id": coverage.get(
            "first_missing_value_ledger_entry_id"
        ),
        "first_missing_ticker": coverage.get("first_missing_ticker"),
        "canonical_next_command": coverage.get("canonical_next_command"),
        "computed_outcome_count": int(coverage.get("computed_outcome_count") or 0),
        "insufficient_data_count": int(coverage.get("insufficient_data_count") or 0),
        "coverage_pct": coverage.get("coverage_pct"),
        "rows": [row for row in rows if isinstance(row, Mapping)],
        "next_action": coverage.get("next_action"),
        "external_calls_made": int(coverage.get("external_calls_made") or 0),
        "db_writes_made": int(coverage.get("db_writes_made") or 0),
    }


def _validation_evidence_summary(
    repo: ValidationRepository,
    *,
    available_at: datetime,
    period_start: date,
    period_end: date,
) -> dict[str, object]:
    replay_command = _validation_replay_preview_command(
        period_start=period_start,
        period_end=period_end,
        available_at=available_at,
    )
    run = repo.latest_successful_validation_run(
        available_at=available_at,
        period_start=period_start,
        period_end=period_end,
    )
    if run is None:
        latest_run = repo.latest_successful_validation_run(available_at=available_at)
        if latest_run is not None:
            return _validation_period_mismatch_evidence(
                run=latest_run,
                period_start=period_start,
                period_end=period_end,
                canonical_next_command=replay_command,
            )
        return _missing_validation_evidence(
            status="no_validation_runs",
            selected_run_id=None,
            next_action=(
                "Run validation-replay after candidate outcomes are available, "
                "then rerun validation-report."
            ),
            canonical_next_command=replay_command,
        )
    results = repo.list_validation_results(run.id, available_at=available_at)
    if not results:
        return _missing_validation_evidence(
            status="validation_results_not_found",
            selected_run_id=run.id,
            next_action=(
                "Check the validation run id or run validation-replay to create "
                "stored validation results."
            ),
            canonical_next_command=replay_command,
        )
    report = validation_report_payload(
        build_validation_report(
            run.id,
            results,
            useful_alert_labels=repo.list_useful_alert_labels(available_at=available_at),
        )
    )
    comparison = report.get("baseline_comparison")
    comparison = comparison if isinstance(comparison, Mapping) else {}
    measured = [
        name
        for name in MISSION_BRIEF_BASELINES
        if _baseline_sample_status(comparison, name) == "measured"
    ]
    missing = [name for name in MISSION_BRIEF_BASELINES if name not in comparison]
    insufficient = [
        name
        for name in MISSION_BRIEF_BASELINES
        if name not in measured and name not in missing
    ]
    baseline_results = _baseline_result_rows(comparison)
    baseline_result_counts = Counter(
        str(row.get("result_vs_market_radar") or "unknown")
        for row in baseline_results
    )
    precision_at_5 = _first_baseline_metric(comparison, "marketradar_precision_at_5")
    precision_at_10 = _first_baseline_metric(comparison, "marketradar_precision_at_10")
    ready = (
        not missing
        and not insufficient
        and precision_at_5 is not None
        and precision_at_10 is not None
    )
    return {
        "schema_version": "monthly-value-validation-evidence-v1",
        "status": "ready" if ready else "insufficient_evidence",
        "ready": ready,
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "selected_run_id": run.id,
        "validation_run": _validation_run_payload(run),
        "candidate_result_count": sum(1 for result in results if result.baseline is None),
        "baseline_result_count": sum(1 for result in results if result.baseline is not None),
        "required_baselines": list(MISSION_BRIEF_BASELINES),
        "measured_baselines": measured,
        "insufficient_baselines": insufficient,
        "missing_baselines": missing,
        "baseline_result_counts": dict(sorted(baseline_result_counts.items())),
        "baseline_results": baseline_results,
        "precision_at_5": precision_at_5,
        "precision_at_10": precision_at_10,
        "external_calls_made": 0,
        "db_writes_made": 0,
        "canonical_next_command": None if ready else replay_command,
        "next_action": (
            "Validation and baseline evidence is measured for the latest successful run."
            if ready
            else (
                "Run validation-replay with outcome labels until every mission "
                "baseline is measured."
            )
        ),
    }


def _validation_period_mismatch_evidence(
    *,
    run: ValidationRun,
    period_start: date,
    period_end: date,
    canonical_next_command: str,
) -> dict[str, object]:
    return {
        "schema_version": "monthly-value-validation-evidence-v1",
        "status": "run_period_mismatch",
        "ready": False,
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "selected_run_id": run.id,
        "validation_run": _validation_run_payload(run),
        "candidate_result_count": 0,
        "baseline_result_count": 0,
        "required_baselines": list(MISSION_BRIEF_BASELINES),
        "measured_baselines": [],
        "insufficient_baselines": list(MISSION_BRIEF_BASELINES),
        "missing_baselines": [],
        "precision_at_5": None,
        "precision_at_10": None,
        "external_calls_made": 0,
        "db_writes_made": 0,
        "canonical_next_command": canonical_next_command,
        "next_action": (
            "Run validation-replay for this report month before claiming monthly "
            "value proof."
        ),
    }


def _validation_run_payload(run: ValidationRun) -> dict[str, object]:
    return {
        "id": run.id,
        "run_type": run.run_type,
        "as_of_start": run.as_of_start.isoformat(),
        "as_of_end": run.as_of_end.isoformat(),
        "decision_available_at": run.decision_available_at.isoformat(),
        "started_at": run.started_at.isoformat(),
        "finished_at": run.finished_at.isoformat() if run.finished_at else None,
    }


def _missing_validation_evidence(
    *,
    status: str,
    selected_run_id: str | None,
    next_action: str,
    canonical_next_command: str | None,
) -> dict[str, object]:
    return {
        "schema_version": "monthly-value-validation-evidence-v1",
        "status": status,
        "ready": False,
        "selected_run_id": selected_run_id,
        "candidate_result_count": 0,
        "baseline_result_count": 0,
        "required_baselines": list(MISSION_BRIEF_BASELINES),
        "measured_baselines": [],
        "insufficient_baselines": list(MISSION_BRIEF_BASELINES),
        "missing_baselines": list(MISSION_BRIEF_BASELINES),
        "precision_at_5": None,
        "precision_at_10": None,
        "external_calls_made": 0,
        "db_writes_made": 0,
        "canonical_next_command": canonical_next_command,
        "next_action": next_action,
    }


def _validation_replay_preview_command(
    *,
    period_start: date,
    period_end: date,
    available_at: datetime,
) -> str:
    return (
        "catalyst-radar validation-replay "
        f"--as-of-start {period_start.isoformat()} "
        f"--as-of-end {period_end.isoformat()} "
        f"--available-at {available_at.isoformat()} "
        f"--outcome-available-at {available_at.isoformat()} "
        "--preview --json"
    )


def _baseline_sample_status(
    comparison: Mapping[str, object],
    name: str,
) -> str | None:
    row = comparison.get(name)
    if not isinstance(row, Mapping):
        return None
    status = row.get("sample_status")
    return str(status) if status is not None else None


def _first_baseline_metric(
    comparison: Mapping[str, object],
    metric: str,
) -> float | None:
    for name in MISSION_BRIEF_BASELINES:
        row = comparison.get(name)
        if not isinstance(row, Mapping):
            continue
        value = row.get(metric)
        if isinstance(value, int | float):
            return float(value)
    return None


def _baseline_result_rows(comparison: Mapping[str, object]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for name in MISSION_BRIEF_BASELINES:
        row = comparison.get(name)
        if not isinstance(row, Mapping):
            rows.append(
                {
                    "baseline": name,
                    "sample_status": "missing",
                    "result_vs_market_radar": "missing",
                    "marketradar_precision_at_5": None,
                    "marketradar_precision_at_10": None,
                    "baseline_precision_at_5": None,
                    "baseline_precision_at_10": None,
                    "missed_opportunity_count": 0,
                }
            )
            continue
        rows.append(
            {
                "baseline": name,
                "sample_status": _optional_string(row.get("sample_status")),
                "result_vs_market_radar": _optional_string(
                    row.get("result_vs_market_radar")
                ),
                "marketradar_precision_at_5": _optional_number(
                    row.get("marketradar_precision_at_5")
                ),
                "marketradar_precision_at_10": _optional_number(
                    row.get("marketradar_precision_at_10")
                ),
                "baseline_precision_at_5": _optional_number(
                    row.get("baseline_precision_at_5")
                ),
                "baseline_precision_at_10": _optional_number(
                    row.get("baseline_precision_at_10")
                ),
                "missed_opportunity_count": int(
                    row.get("missed_opportunity_count") or 0
                ),
            }
        )
    return rows


def _optional_string(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_number(value: object) -> float | None:
    if isinstance(value, int | float) and isfinite(float(value)):
        return float(value)
    return None


def _verdict_reason(
    verdict: str,
    *,
    net_value: float,
    target: float,
    useful_count: int,
    min_useful_evidence_count: int,
    first_blocker: object = None,
    first_gap_count: int = 0,
) -> str:
    if verdict == "insufficient_evidence":
        blocker = str(first_blocker or "")
        if blocker and blocker != "useful_evidence":
            return (
                f"Evidence incomplete: {blocker} has {first_gap_count} gap(s) "
                "before pass/fail."
            )
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


def _latest_outcomes_by_entry_id(
    outcomes: Iterable[ValueOutcome],
) -> dict[str, ValueOutcome]:
    latest: dict[str, ValueOutcome] = {}
    for outcome in outcomes:
        existing = latest.get(outcome.value_ledger_entry_id)
        if existing is None or outcome.outcome_available_at > existing.outcome_available_at:
            latest[outcome.value_ledger_entry_id] = outcome
    return latest


def _value_evidence_examples(
    entries: Iterable[ValueLedgerEntry],
    *,
    outcomes_by_entry_id: Mapping[str, ValueOutcome],
    category: str,
    limit: int,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for entry in entries:
        if len(rows) >= limit:
            break
        rows.append(
            _value_evidence_example(
                entry,
                outcome=outcomes_by_entry_id.get(entry.id),
                category=category,
            )
        )
    return rows


def _value_evidence_example(
    entry: ValueLedgerEntry,
    *,
    outcome: ValueOutcome | None,
    category: str,
) -> dict[str, object]:
    outcome_payload = value_outcome_payload(outcome) if outcome is not None else None
    weighted_value = _claimable_weighted_value(entry)
    return {
        "category": category,
        "ledger_entry_id": entry.id,
        "ticker": entry.ticker,
        "as_of": entry.as_of.isoformat() if entry.as_of is not None else None,
        "entry_date": entry.entry_date.isoformat(),
        "artifact_type": entry.artifact_type,
        "artifact_id": entry.artifact_id,
        "feedback_label": entry.label,
        "supported_action": entry.supported_action,
        "user_decision": entry.user_decision,
        "what_found": _what_found(entry),
        "what_happened": _what_happened(outcome),
        "outcome_status": outcome.status if outcome is not None else "pending",
        "outcome_available_at": (
            outcome.outcome_available_at.isoformat() if outcome is not None else None
        ),
        "trading_days_observed": (
            outcome.trading_days_observed if outcome is not None else None
        ),
        "primary_return": _primary_outcome_return(outcome),
        "primary_return_text": _primary_outcome_return_text(outcome),
        "return_5d": outcome.return_5d if outcome is not None else None,
        "return_10d": outcome.return_10d if outcome is not None else None,
        "return_20d": outcome.return_20d if outcome is not None else None,
        "return_60d": outcome.return_60d if outcome is not None else None,
        "spy_relative_return_20d": (
            outcome.spy_relative_return_20d if outcome is not None else None
        ),
        "sector_relative_return_20d": (
            outcome.sector_relative_return_20d if outcome is not None else None
        ),
        "max_adverse_excursion": (
            outcome.max_adverse_excursion if outcome is not None else None
        ),
        "max_favorable_excursion": (
            outcome.max_favorable_excursion if outcome is not None else None
        ),
        "invalidation_touched": (
            outcome.invalidation_touched if outcome is not None else None
        ),
        "setup_follow_through": (
            outcome_payload.get("setup_follow_through")
            if isinstance(outcome_payload, Mapping)
            else None
        ),
        "estimated_value_usd": round(entry.estimated_value_usd, 4),
        "confidence": entry.confidence,
        "attributed_value_usd": round(weighted_value, 4),
        "cost_to_produce_usd": round(entry.cost_to_produce_usd, 4),
        "provider_call_count": entry.provider_call_count,
        "llm_call_count": entry.llm_call_count,
        "summary": _value_evidence_summary(
            entry,
            what_found=_what_found(entry),
            what_happened=_what_happened(outcome),
            attributed_value_usd=weighted_value,
        ),
    }


def _what_found(entry: ValueLedgerEntry) -> str:
    parts = [
        entry.priced_in_status,
        entry.priced_in_direction,
        entry.action_state,
        entry.setup_type,
    ]
    text = " / ".join(str(part) for part in parts if part)
    return text or f"{entry.artifact_type}:{entry.artifact_id}"


def _what_happened(outcome: ValueOutcome | None) -> str:
    if outcome is None:
        return "Outcome pending."
    primary = _primary_outcome_return(outcome)
    if primary is None:
        return f"Outcome {outcome.status}; {outcome.trading_days_observed} trading day(s) observed."
    return (
        f"Outcome {outcome.status}; {primary['horizon']} return "
        f"{primary['return']:.4f} after {outcome.trading_days_observed} trading day(s)."
    )


def _primary_outcome_return(outcome: ValueOutcome | None) -> dict[str, object] | None:
    if outcome is None:
        return None
    for horizon, value in (
        ("60d", outcome.return_60d),
        ("20d", outcome.return_20d),
        ("10d", outcome.return_10d),
        ("5d", outcome.return_5d),
    ):
        if value is not None:
            return {"horizon": horizon, "return": value}
    return None


def _primary_outcome_return_text(outcome: ValueOutcome | None) -> str:
    primary = _primary_outcome_return(outcome)
    if primary is None:
        return "n/a"
    return f"{primary['horizon']}:{primary['return']:.4f}"


def _value_evidence_summary(
    entry: ValueLedgerEntry,
    *,
    what_found: str,
    what_happened: str,
    attributed_value_usd: float,
) -> str:
    ticker = entry.ticker or "UNKNOWN"
    action = entry.supported_action or "unknown action"
    decision = entry.user_decision or "unknown decision"
    return (
        f"{ticker}: found {what_found}; supported {action}; user chose "
        f"{decision}; {what_happened} Attributed decision-support value "
        f"${attributed_value_usd:.2f}."
    )


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

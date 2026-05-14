from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import fields, is_dataclass
from datetime import UTC, date, datetime, timedelta
from enum import Enum
from math import ceil, isfinite
from typing import Any

from sqlalchemy import Engine, and_, func, select

from catalyst_radar.brokers.interactive import (
    market_snapshot_payload,
    opportunity_action_payload,
    order_ticket_payload,
    trigger_payload,
)
from catalyst_radar.brokers.portfolio_context import (
    balances_payload,
    exposure_payload,
    open_orders_payload,
    portfolio_snapshot_payload,
    positions_payload,
)
from catalyst_radar.brokers.rate_limit import (
    schwab_rate_limit_config_payload,
    schwab_rate_limit_status,
)
from catalyst_radar.core.config import AppConfig
from catalyst_radar.core.models import ActionState
from catalyst_radar.jobs.step_outcomes import (
    StepOutcomeClassification,
    classify_step_outcome,
)
from catalyst_radar.jobs.tasks import DAILY_STEP_ORDER
from catalyst_radar.storage.broker_repositories import BrokerRepository
from catalyst_radar.storage.budget_repositories import BudgetLedgerRepository
from catalyst_radar.storage.schema import (
    alert_suppressions,
    alerts,
    candidate_packets,
    candidate_states,
    decision_cards,
    events,
    job_locks,
    job_runs,
    paper_trades,
    securities,
    signal_features,
    text_snippets,
    user_feedback,
    validation_results,
    validation_runs,
)
from catalyst_radar.storage.validation_repositories import ValidationRepository
from catalyst_radar.validation.reports import (
    USEFUL_ALERT_LABELS,
    build_validation_report,
    validation_report_payload,
)

RADAR_RUN_COOLDOWN_LOCK_NAME = "manual_radar_run_cooldown"
DAILY_WORKER_LOCK_NAME = "daily-run"

ALERT_SUPPRESSION_EXPLANATIONS = {
    "duplicate_trigger": "A prior alert already covers the same trigger.",
    "manual_review_missing_decision_card": (
        "Manual review candidates need a Decision Card before alerting."
    ),
    "state_not_alertable": "The candidate state is not alertable.",
    "warning_delta_below_threshold": (
        "Warning score movement did not clear the alert delta threshold."
    ),
}

ALERT_SUPPRESSION_ACTIONS = {
    "duplicate_trigger": "Use the existing alert instead of creating a duplicate.",
    "manual_review_missing_decision_card": (
        "Generate or review the Decision Card before manual-buy escalation."
    ),
    "state_not_alertable": "Keep this candidate in research/watchlist flow.",
    "warning_delta_below_threshold": (
        "Review the candidate manually or lower alert thresholds if this is too quiet."
    ),
}


def load_candidate_rows(
    engine: Engine,
    *,
    available_at: datetime | None = None,
    as_of_date: date | None = None,
) -> list[dict[str, object]]:
    cutoff = _as_utc_datetime_or_none(available_at)
    run_date = _parse_date(as_of_date)
    ranked_state_stmt = select(
        candidate_states.c.id.label("candidate_state_id"),
        func.row_number()
        .over(
            partition_by=candidate_states.c.ticker,
            order_by=(
                candidate_states.c.as_of.desc(),
                candidate_states.c.created_at.desc(),
                candidate_states.c.id.desc(),
            ),
        )
        .label("state_rank"),
    )
    if cutoff is not None:
        ranked_state_stmt = ranked_state_stmt.where(
            candidate_states.c.created_at <= cutoff,
        )
    if run_date is not None:
        start, end = _date_window(run_date)
        ranked_state_stmt = ranked_state_stmt.where(
            candidate_states.c.as_of >= start,
            candidate_states.c.as_of < end,
        )
    elif cutoff is not None:
        ranked_state_stmt = ranked_state_stmt.where(candidate_states.c.as_of <= cutoff)
    ranked_states = ranked_state_stmt.subquery()

    ranked_packet_stmt = (
        select(
            candidate_packets.c.id,
            candidate_packets.c.candidate_state_id,
            candidate_packets.c.available_at,
            candidate_packets.c.created_at,
            candidate_packets.c.payload,
            func.row_number()
            .over(
                partition_by=candidate_packets.c.candidate_state_id,
                order_by=(
                    candidate_packets.c.available_at.desc(),
                    candidate_packets.c.created_at.desc(),
                    candidate_packets.c.id.desc(),
                ),
            )
            .label("packet_rank"),
        )
        .where(candidate_packets.c.candidate_state_id.is_not(None))
    )
    if cutoff is not None:
        ranked_packet_stmt = ranked_packet_stmt.where(candidate_packets.c.available_at <= cutoff)
    ranked_packets = ranked_packet_stmt.subquery()

    ranked_card_stmt = select(
        decision_cards.c.id,
        decision_cards.c.candidate_packet_id,
        decision_cards.c.available_at,
        decision_cards.c.next_review_at,
        decision_cards.c.payload,
        func.row_number()
        .over(
            partition_by=decision_cards.c.candidate_packet_id,
            order_by=(
                decision_cards.c.available_at.desc(),
                decision_cards.c.created_at.desc(),
                decision_cards.c.id.desc(),
            ),
        )
        .label("card_rank"),
    )
    if cutoff is not None:
        ranked_card_stmt = ranked_card_stmt.where(decision_cards.c.available_at <= cutoff)
    ranked_cards = ranked_card_stmt.subquery()

    stmt = (
        select(
            candidate_states,
            signal_features.c.payload.label("signal_payload"),
            ranked_packets.c.id.label("candidate_packet_id"),
            ranked_packets.c.available_at.label("candidate_packet_available_at"),
            ranked_packets.c.created_at.label("candidate_packet_created_at"),
            ranked_packets.c.payload.label("candidate_packet_payload"),
            ranked_cards.c.id.label("decision_card_id"),
            ranked_cards.c.available_at.label("decision_card_available_at"),
            ranked_cards.c.next_review_at.label("next_review_at"),
            ranked_cards.c.payload.label("decision_card_payload"),
        )
        .join(
            ranked_states,
            and_(
                ranked_states.c.candidate_state_id == candidate_states.c.id,
                ranked_states.c.state_rank == 1,
            ),
        )
        .join(
            signal_features,
            and_(
                signal_features.c.ticker == candidate_states.c.ticker,
                signal_features.c.as_of == candidate_states.c.as_of,
                signal_features.c.feature_version == candidate_states.c.feature_version,
            ),
            isouter=True,
        )
        .join(
            ranked_packets,
            and_(
                ranked_packets.c.candidate_state_id == candidate_states.c.id,
                ranked_packets.c.packet_rank == 1,
            ),
            isouter=True,
        )
        .join(
            ranked_cards,
            and_(
                ranked_cards.c.candidate_packet_id == ranked_packets.c.id,
                ranked_cards.c.card_rank == 1,
            ),
            isouter=True,
        )
        .order_by(candidate_states.c.final_score.desc(), candidate_states.c.as_of.desc())
        .limit(200)
    )
    with engine.connect() as conn:
        return [_candidate_row(row._mapping) for row in conn.execute(stmt)]


def load_radar_run_candidate_rows(
    engine: Engine,
    radar_run_summary: Mapping[str, object],
) -> list[dict[str, object]]:
    summary = _row_dict(radar_run_summary)
    cutoff = _parse_utc_datetime(summary.get("finished_at")) or _parse_utc_datetime(
        summary.get("decision_available_at")
    )
    return load_candidate_rows(
        engine,
        available_at=cutoff,
        as_of_date=_parse_date(summary.get("as_of")),
    )


def opportunity_focus_payload(
    candidate_rows: Sequence[Mapping[str, object]],
    *,
    limit: int = 5,
) -> list[dict[str, object]]:
    if limit <= 0:
        return []
    rows: list[dict[str, object]] = []
    for rank, candidate in enumerate(candidate_rows[:limit], start=1):
        brief = _mapping_value(candidate, "research_brief")
        support = _mapping_value(candidate, "top_supporting_evidence")
        row = {
            "rank": rank,
            "ticker": candidate.get("ticker"),
            "focus": brief.get("focus") or _research_focus(str(candidate.get("state") or "")),
            "state": candidate.get("state"),
            "score": _finite_float(candidate.get("final_score")),
            "why_now": brief.get("why_now"),
            "top_catalyst": brief.get("top_catalyst"),
            "evidence": brief.get("supporting_evidence") or support.get("title"),
            "risk_or_gap": brief.get("risk_or_gap"),
            "next_step": brief.get("next_step"),
            "card": candidate.get("decision_card_id") or "n/a",
        }
        if "schwab_context_status" in candidate:
            row.update(
                {
                    "schwab_last_price": candidate.get("schwab_last_price"),
                    "schwab_day_change_percent": candidate.get(
                        "schwab_day_change_percent"
                    ),
                    "schwab_relative_volume": candidate.get("schwab_relative_volume"),
                    "schwab_context_status": candidate.get("schwab_context_status"),
                }
            )
        rows.append(row)
    return rows


def candidate_rows_with_market_context(
    candidate_rows: Sequence[Mapping[str, object]],
    market_context: Sequence[Mapping[str, object]] | object,
) -> list[dict[str, object]]:
    """Attach latest stored Schwab market context to candidate rows without API calls."""
    context_by_ticker = _latest_market_context_by_ticker(market_context)
    enriched: list[dict[str, object]] = []
    for row in candidate_rows:
        if not isinstance(row, Mapping):
            continue
        values = _row_dict(row)
        ticker = str(values.get("ticker") or "").strip().upper()
        context = context_by_ticker.get(ticker)
        if context is None:
            values.update(_empty_candidate_market_context())
        else:
            values.update(_candidate_market_context_fields(context))
        enriched.append(values)
    return enriched


def candidate_delta_payload(
    engine: Engine,
    *,
    radar_run_summary: Mapping[str, object] | None = None,
    candidate_rows: Sequence[Mapping[str, object]] | None = None,
    available_at: datetime | None = None,
    limit: int = 8,
    score_move_threshold: float = 5.0,
) -> dict[str, object]:
    summary = radar_run_summary if isinstance(radar_run_summary, Mapping) else {}
    cutoff = (
        available_at
        or _parse_utc_datetime(summary.get("finished_at"))
        or _parse_utc_datetime(summary.get("decision_available_at"))
    )
    run_as_of = _parse_date(summary.get("as_of"))
    if candidate_rows is not None:
        current_rows = [
            _row_dict(row) for row in candidate_rows if isinstance(row, Mapping)
        ]
        current_run_rows = [
            row for row in current_rows if _parse_date(row.get("as_of")) == run_as_of
        ]
        stale_context_count = max(0, len(current_rows) - len(current_run_rows))
    elif run_as_of is None:
        current_run_rows = []
        stale_context_count = len(load_candidate_rows(engine, available_at=cutoff))
    else:
        current_run_rows = load_candidate_rows(
            engine,
            available_at=cutoff,
            as_of_date=run_as_of,
        )
        context_rows = load_candidate_rows(engine, available_at=cutoff)
        current_tickers = {
            str(row.get("ticker") or "").strip().upper()
            for row in current_run_rows
            if str(row.get("ticker") or "").strip()
        }
        stale_context_count = sum(
            1
            for row in context_rows
            if str(row.get("ticker") or "").strip().upper() not in current_tickers
        )
    changes: list[dict[str, object]] = []
    with engine.connect() as conn:
        for row in current_run_rows:
            previous = _previous_candidate_state_row(
                conn,
                row,
                available_at=cutoff,
            )
            change = _candidate_delta_row(
                row,
                previous,
                score_move_threshold=score_move_threshold,
            )
            if change is not None:
                changes.append(change)

    all_changes = changes
    changes = sorted(
        changes,
        key=lambda row: (
            -int(row["severity"]),
            -abs(_finite_float(row.get("score_change"))),
            -_finite_float(row.get("current_score")),
            str(row.get("ticker") or ""),
        ),
    )[: _positive_limit(limit)]
    summary_counts = _candidate_delta_counts(all_changes)
    if all_changes:
        status = "changed"
        headline = f"{len(all_changes)} candidate change(s) need review."
        next_action = "Review state, score, and blocker changes before acting on the queue."
    elif current_run_rows:
        status = "unchanged"
        headline = "No material candidate changes in the latest run."
        next_action = "Keep monitoring or adjust thresholds only after live inputs are ready."
    else:
        status = "no_current_candidates"
        headline = "No current-run candidate changes are available."
        next_action = (
            "Configure live inputs or inspect stale candidate context before relying on deltas."
        )
    return {
        "schema_version": "candidate-delta-v1",
        "status": status,
        "headline": headline,
        "next_action": next_action,
        "as_of": summary.get("as_of"),
        "latest_run_cutoff": cutoff.isoformat() if cutoff is not None else None,
        "score_move_threshold": float(score_move_threshold),
        "summary": {
            "current_run_candidates": len(current_run_rows),
            "stale_context_candidates": stale_context_count,
            "changed_candidates": len(all_changes),
            **summary_counts,
        },
        "evidence": (
            f"as_of={summary.get('as_of') or 'n/a'}; "
            f"current_run_candidates={len(current_run_rows)}; "
            f"stale_context_candidates={stale_context_count}; "
            f"score_move_threshold={float(score_move_threshold):.2f}"
        ),
        "rows": changes,
    }


def research_shortlist_payload(
    candidate_rows: Sequence[Mapping[str, object]],
    investment_readiness: Mapping[str, object] | None = None,
    *,
    limit: int = 8,
    market_context: Sequence[Mapping[str, object]] | object = (),
) -> dict[str, object]:
    readiness = investment_readiness if isinstance(investment_readiness, Mapping) else {}
    enriched_rows = candidate_rows_with_market_context(candidate_rows, market_context)
    labeled_rows = candidate_decision_labels_payload(enriched_rows, readiness)
    rows = [
        _research_shortlist_row(row)
        for row in sorted(
            (row for row in labeled_rows if isinstance(row, Mapping)),
            key=_research_shortlist_sort_key,
        )[: _positive_limit(limit)]
    ]
    actionable_count = sum(
        1 for row in rows if str(row.get("priority") or "") == "manual_review"
    )
    research_count = sum(
        1 for row in rows if str(row.get("priority") or "") == "research_now"
    )
    if actionable_count:
        status = "manual_review"
        headline = f"{actionable_count} candidate(s) are ready for manual review."
        next_action = "Review cards, exposure, hard blocks, and source freshness."
    elif research_count:
        status = "research"
        headline = f"{research_count} candidate(s) should be researched first."
        next_action = "Start with the top catalyst and risk/gap columns."
    elif rows:
        status = "monitor"
        headline = "No shortlist item is ready for manual review yet."
        next_action = "Use the listed verification steps before changing thresholds."
    else:
        status = "empty"
        headline = "No candidates are available for the research shortlist."
        next_action = "Run the radar after live inputs are configured."
    return {
        "schema_version": "research-shortlist-v1",
        "status": status,
        "headline": headline,
        "next_action": next_action,
        "decision_mode": readiness.get("decision_mode") or "unknown",
        "safe_to_make_investment_decision": bool(
            readiness.get("manual_buy_review_ready")
        ),
        "count": len(rows),
        "rows": rows,
    }


def operator_work_queue_payload(
    config: AppConfig,
    *,
    radar_run_summary: Mapping[str, object] | None = None,
    broker_summary: Mapping[str, object] | None = None,
    discovery_snapshot: Mapping[str, object] | None = None,
    candidate_rows: Sequence[Mapping[str, object]] | None = None,
    limit: int = 8,
) -> dict[str, object]:
    """Build the operator-facing next-action queue from existing diagnostics."""
    candidates = [row for row in candidate_rows or () if isinstance(row, Mapping)]
    actionability = actionability_breakdown_payload(candidates, limit=limit)
    investment = investment_readiness_payload(
        discovery_snapshot,
        actionability,
        candidates,
    )
    shortlist = research_shortlist_payload(candidates, investment, limit=limit)
    readiness_rows = readiness_checklist_payload(
        config,
        radar_run_summary=radar_run_summary,
        broker_summary=broker_summary,
    )

    queue_rows: list[dict[str, object]] = []
    sequence = 0
    for row in readiness_rows:
        status = str(row.get("status") or "")
        if status not in {"blocked", "attention"}:
            continue
        sequence += 1
        queue_rows.append(
            _operator_work_queue_row(
                sequence=sequence,
                severity=100 if status == "blocked" else 80,
                priority="must_fix" if status == "blocked" else "attention",
                area=str(row.get("area") or "Readiness"),
                item=str(row.get("finding") or row.get("area") or "Readiness item"),
                status=status,
                next_action=str(row.get("next_action") or "Review readiness details."),
                evidence=str(row.get("evidence") or "n/a"),
                source="readiness_checklist",
            )
        )

    for row in _sequence_value(shortlist.get("rows")):
        if not isinstance(row, Mapping):
            continue
        priority = str(row.get("priority") or "monitor")
        if priority == "manual_review":
            severity = 70
            queue_priority = "review_now"
        elif priority in {"research_now", "missing_card"}:
            severity = 55
            queue_priority = "research"
        elif priority == "blocked":
            severity = 35
            queue_priority = "blocked_candidate"
        else:
            severity = 20
            queue_priority = "monitor"
        sequence += 1
        ticker = str(row.get("ticker") or "n/a")
        queue_rows.append(
            _operator_work_queue_row(
                sequence=sequence,
                severity=severity,
                priority=queue_priority,
                area="Candidate",
                item=f"{ticker}: {row.get('why_now') or row.get('setup') or 'candidate review'}",
                status=priority,
                next_action=str(row.get("next_step") or "Review research brief."),
                evidence=str(row.get("risk_or_gap") or row.get("evidence") or "n/a"),
                source="research_shortlist",
                ticker=ticker,
            )
        )

    queue_rows = sorted(
        queue_rows,
        key=lambda row: (
            -int(row["severity"]),
            int(row["sequence"]),
            str(row.get("ticker") or ""),
        ),
    )[: _positive_limit(limit)]
    blocking_count = sum(
        1 for row in queue_rows if str(row.get("priority") or "") == "must_fix"
    )
    review_count = sum(
        1 for row in queue_rows if str(row.get("priority") or "") == "review_now"
    )
    research_count = sum(
        1 for row in queue_rows if str(row.get("priority") or "") == "research"
    )
    if blocking_count:
        status = "blocked"
        headline = f"{blocking_count} setup blocker(s) must be cleared first."
        next_action = str(queue_rows[0].get("next_action") or "Review the top queue item.")
    elif review_count:
        status = "review"
        headline = f"{review_count} candidate(s) need manual review."
        next_action = "Open the top candidate and save an operator action."
    elif research_count:
        status = "research"
        headline = f"{research_count} candidate(s) need research before review."
        next_action = "Work the research rows from top to bottom."
    elif queue_rows:
        status = "monitor"
        headline = "No urgent operator action is required."
        next_action = "Monitor the listed context items."
    else:
        status = "empty"
        headline = "No operator work queue items are available."
        next_action = "Run the radar after live inputs are configured."
    return {
        "schema_version": "operator-work-queue-v1",
        "status": status,
        "headline": headline,
        "next_action": next_action,
        "counts": {
            "blocking": blocking_count,
            "review": review_count,
            "research": research_count,
            "total": len(queue_rows),
        },
        "investment_mode": investment.get("decision_mode") or "unknown",
        "safe_to_make_investment_decision": bool(
            investment.get("manual_buy_review_ready")
        ),
        "rows": queue_rows,
    }


def candidate_decision_labels_payload(
    candidate_rows: Sequence[Mapping[str, object]],
    investment_readiness: Mapping[str, object] | None = None,
) -> list[dict[str, object]]:
    readiness = investment_readiness if isinstance(investment_readiness, Mapping) else {}
    return [
        {
            **_row_dict(candidate),
            **_candidate_decision_label(candidate, readiness),
        }
        for candidate in candidate_rows
        if isinstance(candidate, Mapping)
    ]


def actionability_breakdown_payload(
    candidate_rows: Sequence[Mapping[str, object]],
    *,
    limit: int = 5,
) -> dict[str, object]:
    rows = [row for row in candidate_rows if isinstance(row, Mapping)]
    buckets = {
        "Buy-review ready": 0,
        "Research now": 0,
        "Watchlist": 0,
        "Blocked or risk review": 0,
        "Monitor": 0,
    }
    risk_counts: dict[str, dict[str, object]] = {}
    action_rows: list[dict[str, object]] = []
    for candidate in rows:
        state = str(candidate.get("state") or "")
        bucket = _actionability_bucket(state)
        buckets[bucket] += 1
        brief = _mapping_value(candidate, "research_brief")
        risk_or_gap = str(
            _first_present(
                brief.get("risk_or_gap"),
                _mapping_value(candidate, "top_disconfirming_evidence").get("title"),
                "No explicit risk/gap captured.",
            )
        )
        risk_entry = risk_counts.setdefault(
            risk_or_gap,
            {"risk_or_gap": risk_or_gap, "count": 0, "sample_tickers": []},
        )
        risk_entry["count"] = int(risk_entry["count"]) + 1
        sample_tickers = risk_entry["sample_tickers"]
        if isinstance(sample_tickers, list) and len(sample_tickers) < 4:
            sample_tickers.append(candidate.get("ticker"))
        if len(action_rows) < limit:
            action_rows.append(
                {
                    "ticker": candidate.get("ticker"),
                    "state": candidate.get("state"),
                    "score": _finite_float(candidate.get("final_score")),
                    "focus": brief.get("focus") or _research_focus(state),
                    "risk_or_gap": risk_or_gap,
                    "next_step": brief.get("next_step")
                    or _research_next_step(
                        state,
                        has_decision_card=bool(candidate.get("decision_card_id")),
                    ),
                    "card": candidate.get("decision_card_id") or "n/a",
                }
            )
    counts = [
        {"bucket": bucket, "count": count}
        for bucket, count in buckets.items()
        if count
    ]
    blockers = sorted(
        (
            {
                "risk_or_gap": str(value["risk_or_gap"]),
                "count": int(value["count"]),
                "sample_tickers": ", ".join(
                    str(item) for item in value["sample_tickers"] if item
                ),
            }
            for value in risk_counts.values()
        ),
        key=lambda row: (-int(row["count"]), str(row["risk_or_gap"])),
    )[:limit]
    status, headline, next_action = _actionability_status(buckets, total=len(rows))
    return {
        "status": status,
        "headline": headline,
        "next_action": next_action,
        "total_candidates": len(rows),
        "counts": counts,
        "top_blockers": blockers,
        "next_actions": action_rows,
    }


def investment_readiness_payload(
    discovery_snapshot: Mapping[str, object] | None,
    actionability_breakdown: Mapping[str, object] | None,
    candidate_rows: Sequence[Mapping[str, object]] | None = None,
) -> dict[str, object]:
    snapshot = discovery_snapshot if isinstance(discovery_snapshot, Mapping) else {}
    actionability = (
        actionability_breakdown if isinstance(actionability_breakdown, Mapping) else {}
    )
    blocker_rows = [
        _row_dict(row)
        for row in _sequence_value(snapshot.get("blockers"))
        if isinstance(row, Mapping)
    ]
    blocker_codes = [str(row.get("code") or "") for row in blocker_rows]
    counts_by_bucket = _actionability_counts_by_bucket(actionability.get("counts"))
    buy_ready_count = counts_by_bucket.get("Buy-review ready", 0)
    buy_ready_with_card_count = _buy_review_ready_with_card_count(candidate_rows or ())
    research_count = counts_by_bucket.get("Research now", 0)
    snapshot_status = str(snapshot.get("status") or "unknown")
    source_modes = _mapping_value(snapshot, "source_modes")
    freshness = _mapping_value(snapshot, "freshness")
    yield_payload = _mapping_value(snapshot, "yield")
    packet_count = int(_finite_float(yield_payload.get("candidate_packets")))
    card_count = int(_finite_float(yield_payload.get("decision_cards")))
    hard_blockers = [
        code
        for code in blocker_codes
        if code
        and code
        in {
            "no_run",
            "fixture_market_data",
            "fixture_events",
            "market_missing_credentials",
            "market_disabled",
            "events_missing_credentials",
            "events_disabled",
            "thin_universe",
            "stale_daily_bars",
            "blocked_run_steps",
            "no_candidate_packets",
        }
    ]
    bars_stale = bool(freshness.get("latest_bars_older_than_as_of"))
    source_live = (
        source_modes.get("market") == "live" and source_modes.get("events") == "live"
    )
    buy_review_ready = (
        source_live
        and not hard_blockers
        and not bars_stale
        and packet_count > 0
        and card_count > 0
        and buy_ready_with_card_count > 0
    )

    if not snapshot:
        status = "blocked"
        decision_mode = "not_ready"
        headline = "No run has produced decision evidence yet."
        next_action = "Run one capped radar cycle after live inputs are configured."
    elif buy_review_ready:
        status = "ready"
        decision_mode = "manual_buy_review"
        headline = f"{buy_ready_with_card_count} candidate(s) can enter manual buy review."
        next_action = "Open Decision Cards and verify hard blocks, exposure, and source freshness."
    elif snapshot_status in {"fixture", "blocked"} or hard_blockers or bars_stale:
        status = "research_only"
        decision_mode = "research_only"
        headline = "Current candidates are research-only, not investment-decision ready."
        next_action = _first_blocker_action(
            blocker_rows,
            default="Configure live sources, refresh stale data, then rerun the radar.",
        )
    elif buy_ready_count and buy_ready_with_card_count == 0:
        status = "research_only"
        decision_mode = "research_only"
        headline = "Buy-review candidates are missing Decision Cards."
        next_action = "Build Decision Cards before opening any manual buy-review workflow."
    elif research_count:
        status = "research_only"
        decision_mode = "research_only"
        headline = f"{research_count} candidate(s) need research before buy review."
        next_action = str(
            actionability.get("next_action")
            or "Review research gaps before changing thresholds."
        )
    else:
        status = "monitor"
        decision_mode = "monitor"
        headline = "No current candidate is ready for manual buy review."
        next_action = str(
            actionability.get("next_action")
            or "Keep monitoring until a fresher catalyst or stronger score appears."
        )

    return {
        "status": status,
        "decision_mode": decision_mode,
        "manual_buy_review_ready": buy_review_ready,
        "headline": headline,
        "detail": (
            f"market={source_modes.get('market') or 'unknown'}; "
            f"events={source_modes.get('events') or 'unknown'}; "
            f"packets={packet_count}; cards={card_count}; "
            f"buy_review={buy_ready_count}; "
            f"buy_review_with_card={buy_ready_with_card_count}"
        ),
        "next_action": next_action,
        "evidence": (
            f"snapshot_status={snapshot_status}; "
            f"blockers={', '.join(code for code in blocker_codes if code) or 'none'}; "
            f"latest_bars_stale={'yes' if bars_stale else 'no'}; "
            f"source_live={'yes' if source_live else 'no'}"
        ),
        "counts": [
            {"bucket": bucket, "count": count}
            for bucket, count in counts_by_bucket.items()
            if count
        ],
        "blocking_reasons": blocker_rows,
    }


def radar_readiness_payload(
    engine: Engine,
    config: AppConfig,
    *,
    candidate_limit: int = 25,
) -> dict[str, object]:
    radar_run_summary = load_radar_run_summary(engine)
    latest_run_cutoff = _parse_utc_datetime(radar_run_summary.get("decision_available_at"))
    candidate_rows = load_radar_run_candidate_rows(engine, radar_run_summary)
    broker_summary = load_broker_summary(engine)
    market_candidate_rows = candidate_rows_with_market_context(
        candidate_rows,
        _mapping_value(broker_summary, "market_context"),
    )
    ops_health = load_ops_health(engine)
    discovery_snapshot = radar_discovery_snapshot_payload(
        engine,
        config,
        radar_run_summary=radar_run_summary,
        ops_health=ops_health,
    )
    actionability = actionability_breakdown_payload(market_candidate_rows)
    investment = investment_readiness_payload(
        discovery_snapshot,
        actionability,
        market_candidate_rows,
    )
    activation = activation_summary_payload(
        config,
        radar_run_summary=radar_run_summary,
        broker_summary=broker_summary,
    )
    live_plan = live_activation_plan_payload(
        config,
        radar_run_summary=radar_run_summary,
        broker_summary=broker_summary,
    )
    checklist = readiness_checklist_payload(
        config,
        radar_run_summary=radar_run_summary,
        broker_summary=broker_summary,
    )
    alert_diagnostics = alert_planning_diagnostics_payload(
        engine,
        radar_run_summary=radar_run_summary,
    )
    telemetry = telemetry_tape_payload(ops_health)
    labeled_candidates = candidate_decision_labels_payload(
        market_candidate_rows,
        investment,
    )
    candidate_delta = candidate_delta_payload(
        engine,
        radar_run_summary=radar_run_summary,
    )
    operator_queue = operator_work_queue_payload(
        config,
        radar_run_summary=radar_run_summary,
        broker_summary=broker_summary,
        discovery_snapshot=discovery_snapshot,
        candidate_rows=market_candidate_rows,
    )
    safe_to_decide = bool(investment.get("manual_buy_review_ready"))
    return {
        "schema_version": "radar-readiness-v1",
        "status": investment.get("status") or "unknown",
        "decision_mode": investment.get("decision_mode") or "unknown",
        "safe_to_make_investment_decision": safe_to_decide,
        "headline": investment.get("headline") or "Investment readiness unavailable.",
        "next_action": investment.get("next_action") or "Review readiness inputs.",
        "evidence": investment.get("evidence") or "",
        "latest_run_cutoff": (
            latest_run_cutoff.isoformat() if latest_run_cutoff is not None else None
        ),
        "run_path": _radar_run_path_summary(radar_run_summary),
        "radar_run": _readiness_radar_run_summary(radar_run_summary),
        "activation_summary": activation,
        "live_activation_plan": live_plan,
        "readiness_checklist": checklist,
        "alert_planning_diagnostics": alert_diagnostics,
        "discovery_snapshot": discovery_snapshot,
        "actionability_breakdown": actionability,
        "investment_readiness": investment,
        "candidate_delta": candidate_delta,
        "operator_work_queue": operator_queue,
        "candidate_decision_labels": [
            _readiness_candidate_label(row)
            for row in labeled_candidates[: _positive_limit(candidate_limit)]
        ],
        "telemetry_tape": telemetry,
    }


def radar_research_shortlist_payload(
    engine: Engine,
    config: AppConfig,
    *,
    limit: int = 8,
) -> dict[str, object]:
    radar_run_summary = load_radar_run_summary(engine)
    latest_run_cutoff = _parse_utc_datetime(radar_run_summary.get("decision_available_at"))
    candidate_rows = load_radar_run_candidate_rows(engine, radar_run_summary)
    broker_summary = load_broker_summary(engine)
    market_candidate_rows = candidate_rows_with_market_context(
        candidate_rows,
        _mapping_value(broker_summary, "market_context"),
    )
    discovery_snapshot = radar_discovery_snapshot_payload(
        engine,
        config,
        radar_run_summary=radar_run_summary,
    )
    actionability = actionability_breakdown_payload(market_candidate_rows)
    readiness = investment_readiness_payload(
        discovery_snapshot,
        actionability,
        market_candidate_rows,
    )
    shortlist = research_shortlist_payload(
        market_candidate_rows,
        readiness,
        limit=limit,
        market_context=_mapping_value(broker_summary, "market_context"),
    )
    return {
        **shortlist,
        "latest_run_cutoff": (
            latest_run_cutoff.isoformat() if latest_run_cutoff is not None else None
        ),
        "radar_status": radar_run_summary.get("status") or "unknown",
        "readiness_status": readiness.get("status") or "unknown",
    }


def load_ticker_detail(
    engine: Engine,
    ticker: str,
    *,
    available_at: datetime | None = None,
) -> dict[str, object] | None:
    symbol = str(ticker).strip().upper()
    if not symbol:
        return None

    requested_cutoff = _as_utc_datetime_or_none(available_at)
    with engine.connect() as conn:
        latest_state = _latest_state_row(conn, symbol, available_at=requested_cutoff)
        if latest_state is None:
            return None

        signal_row = _signal_feature_row(conn, latest_state)
        packet_row = _latest_packet_row(
            conn,
            symbol,
            candidate_state_id=str(latest_state["id"]),
            available_at=requested_cutoff,
        ) or _latest_packet_row(conn, symbol, available_at=requested_cutoff)
        packet_id = str(packet_row["id"]) if packet_row is not None else None
        card_row = _latest_card_row(
            conn,
            symbol,
            packet_id=packet_id,
            available_at=requested_cutoff,
        ) or _latest_card_row(
            conn,
            symbol,
            available_at=requested_cutoff,
        )
        detail_cutoff = _detail_cutoff(
            requested_cutoff,
            latest_state=latest_state,
            packet_row=packet_row,
            card_row=card_row,
        )

        latest_candidate = _candidate_row(
            _candidate_detail_mapping(latest_state, signal_row, packet_row, card_row)
        )
        state_history = [
            _row_dict(row._mapping)
            for row in conn.execute(
                select(candidate_states)
                .where(
                    candidate_states.c.ticker == symbol,
                    candidate_states.c.as_of <= latest_state["as_of"],
                    *(
                        [candidate_states.c.created_at <= detail_cutoff]
                        if detail_cutoff is not None
                        else []
                    ),
                )
                .order_by(
                    candidate_states.c.as_of.desc(),
                    candidate_states.c.created_at.desc(),
                    candidate_states.c.id.desc(),
                )
            )
        ]
        event_rows = [
            _row_dict(row._mapping)
            for row in conn.execute(
                select(events)
                .where(
                    events.c.ticker == symbol,
                    *(
                        [events.c.available_at <= detail_cutoff]
                        if detail_cutoff is not None
                        else []
                    ),
                )
                .order_by(
                    events.c.available_at.desc(),
                    events.c.materiality.desc(),
                    events.c.created_at.desc(),
                    events.c.id.desc(),
                )
                .limit(25)
            )
        ]
        snippet_rows = [
            _row_dict(row._mapping)
            for row in conn.execute(
                select(text_snippets)
                .where(
                    text_snippets.c.ticker == symbol,
                    *(
                        [text_snippets.c.available_at <= detail_cutoff]
                        if detail_cutoff is not None
                        else []
                    ),
                )
                .order_by(
                    text_snippets.c.available_at.desc(),
                    text_snippets.c.materiality.desc(),
                    text_snippets.c.created_at.desc(),
                    text_snippets.c.id.desc(),
                )
                .limit(25)
            )
        ]
        validation_rows = [
            _row_dict(row._mapping)
            for row in conn.execute(
                select(validation_results)
                .where(
                    validation_results.c.ticker == symbol,
                    *(
                        [validation_results.c.available_at <= detail_cutoff]
                        if detail_cutoff is not None
                        else []
                    ),
                )
                .order_by(
                    validation_results.c.available_at.desc(),
                    validation_results.c.as_of.desc(),
                    validation_results.c.created_at.desc(),
                    validation_results.c.id.desc(),
                )
                .limit(50)
            )
        ]
        trade_rows = [
            _row_dict(row._mapping)
            for row in conn.execute(
                select(paper_trades)
                .where(
                    paper_trades.c.ticker == symbol,
                    *(
                        [paper_trades.c.available_at <= detail_cutoff]
                        if detail_cutoff is not None
                        else []
                    ),
                )
                .order_by(
                    paper_trades.c.available_at.desc(),
                    paper_trades.c.updated_at.desc(),
                    paper_trades.c.id.desc(),
                )
                .limit(50)
            )
        ]

    signal_payload = signal_row.get("payload") if signal_row is not None else None
    packet_payload = packet_row.get("payload") if packet_row is not None else None
    card_payload = card_row.get("payload") if card_row is not None else None
    candidate_payload = _mapping_value(signal_payload, "candidate")
    candidate_metadata = _mapping_value(candidate_payload, "metadata")

    return {
        "ticker": symbol,
        "latest_candidate": latest_candidate,
        "state_history": state_history,
        "features": _row_dict(signal_row) if signal_row is not None else None,
        "events": event_rows,
        "snippets": snippet_rows,
        "candidate_packet": _row_dict(packet_row) if packet_row is not None else None,
        "decision_card": _row_dict(card_row) if card_row is not None else None,
        "setup_plan": _first_mapping(
            _mapping_value(card_payload, "setup_plan"),
            _mapping_value(card_payload, "trade_plan"),
            _mapping_value(packet_payload, "setup_plan"),
            _mapping_value(packet_payload, "trade_plan"),
            {"setup_type": candidate_metadata.get("setup_type")}
            if candidate_metadata.get("setup_type") is not None
            else None,
        ),
        "portfolio_impact": _first_mapping(
            _mapping_value(card_payload, "portfolio_impact"),
            _mapping_value(packet_payload, "portfolio_impact"),
            _mapping_value(candidate_metadata, "portfolio_impact"),
        ),
        "validation_results": validation_rows,
        "paper_trades": trade_rows,
        "manual_review_only": card_row is not None,
    }


def load_theme_rows(
    engine: Engine,
    *,
    available_at: datetime | None = None,
) -> list[dict[str, object]]:
    cutoff = _as_utc_datetime_or_none(available_at)
    ranked_state_stmt = select(
        candidate_states.c.id.label("candidate_state_id"),
        func.row_number()
        .over(
            partition_by=candidate_states.c.ticker,
            order_by=(
                candidate_states.c.as_of.desc(),
                candidate_states.c.created_at.desc(),
                candidate_states.c.id.desc(),
            ),
        )
        .label("state_rank"),
    )
    if cutoff is not None:
        ranked_state_stmt = ranked_state_stmt.where(
            candidate_states.c.created_at <= cutoff,
            candidate_states.c.as_of <= cutoff,
        )
    ranked_states = ranked_state_stmt.subquery()

    stmt = (
        select(
            candidate_states,
            signal_features.c.payload.label("signal_payload"),
        )
        .join(
            ranked_states,
            and_(
                ranked_states.c.candidate_state_id == candidate_states.c.id,
                ranked_states.c.state_rank == 1,
            ),
        )
        .join(
            signal_features,
            and_(
                signal_features.c.ticker == candidate_states.c.ticker,
                signal_features.c.as_of == candidate_states.c.as_of,
                signal_features.c.feature_version == candidate_states.c.feature_version,
            ),
            isouter=True,
        )
    )
    groups: defaultdict[str, list[dict[str, object]]] = defaultdict(list)
    with engine.connect() as conn:
        for row in conn.execute(stmt):
            values = _row_dict(row._mapping)
            signal_payload = values.pop("signal_payload", None)
            metadata = _mapping_value(_mapping_value(signal_payload, "candidate"), "metadata")
            theme = _theme_name(metadata) or "unclassified"
            groups[theme].append(values)

    rows = []
    for theme, items in groups.items():
        sorted_items = sorted(
            items,
            key=lambda item: (
                -_finite_float(item.get("final_score")),
                str(item.get("ticker") or ""),
            ),
        )
        scores = [_finite_float(item.get("final_score")) for item in items]
        states = Counter(str(item.get("state") or "unknown") for item in items)
        rows.append(
            {
                "theme": theme,
                "candidate_count": len(items),
                "avg_score": sum(scores) / len(scores) if scores else 0.0,
                "top_tickers": [item["ticker"] for item in sorted_items[:5]],
                "states": dict(sorted(states.items())),
                "latest_as_of": max(item["as_of"] for item in items),
            }
        )

    return sorted(
        rows,
        key=lambda row: (
            -int(row["candidate_count"]),
            -_finite_float(row["avg_score"]),
            str(row["theme"]),
        ),
    )


def load_alert_rows(
    engine: Engine,
    *,
    available_at: datetime | None = None,
    ticker: str | None = None,
    status: str | None = None,
    route: str | None = None,
    limit: int = 200,
) -> list[dict[str, object]]:
    cutoff = _as_utc_datetime_or_none(available_at) or datetime.now(UTC)
    filters = [alerts.c.available_at <= cutoff]
    if ticker is not None and ticker.strip():
        filters.append(alerts.c.ticker == ticker.strip().upper())
    if status is not None and status.strip():
        filters.append(alerts.c.status == status.strip())
    if route is not None and route.strip():
        filters.append(alerts.c.route == route.strip())

    ranked_feedback = _ranked_alert_feedback(cutoff)
    stmt = (
        select(
            alerts,
            ranked_feedback.c.feedback_id,
            ranked_feedback.c.feedback_label,
            ranked_feedback.c.feedback_notes,
            ranked_feedback.c.feedback_source,
            ranked_feedback.c.feedback_created_at,
        )
        .join(
            ranked_feedback,
            and_(
                ranked_feedback.c.artifact_id == alerts.c.id,
                ranked_feedback.c.feedback_rank == 1,
            ),
            isouter=True,
        )
        .where(*filters)
        .order_by(
            alerts.c.available_at.desc(),
            alerts.c.created_at.desc(),
            alerts.c.id.desc(),
        )
        .limit(_positive_limit(limit))
    )
    with engine.connect() as conn:
        return [_alert_row(row._mapping) for row in conn.execute(stmt)]


def load_alert_detail(
    engine: Engine,
    alert_id: str,
    *,
    available_at: datetime | None = None,
) -> dict[str, object] | None:
    resolved_id = str(alert_id).strip()
    if not resolved_id:
        return None
    cutoff = _as_utc_datetime_or_none(available_at) or datetime.now(UTC)
    filters = [alerts.c.id == resolved_id, alerts.c.available_at <= cutoff]

    ranked_feedback = _ranked_alert_feedback(cutoff)
    stmt = (
        select(
            alerts,
            ranked_feedback.c.feedback_id,
            ranked_feedback.c.feedback_label,
            ranked_feedback.c.feedback_notes,
            ranked_feedback.c.feedback_source,
            ranked_feedback.c.feedback_created_at,
        )
        .join(
            ranked_feedback,
            and_(
                ranked_feedback.c.artifact_id == alerts.c.id,
                ranked_feedback.c.feedback_rank == 1,
            ),
            isouter=True,
        )
        .where(*filters)
        .limit(1)
    )
    with engine.connect() as conn:
        row = conn.execute(stmt).first()
    return _alert_row(row._mapping) if row is not None else None


def alert_planning_diagnostics_payload(
    engine: Engine,
    radar_run_summary: Mapping[str, object] | None = None,
    *,
    limit: int = 50,
) -> dict[str, object]:
    summary = _row_dict(radar_run_summary) if isinstance(radar_run_summary, Mapping) else {}
    steps = _radar_steps_by_name(summary)
    planning_step = steps.get("alert_planning", {})
    digest_step = steps.get("digest", {})
    planning_status = str(planning_step.get("status") or "")
    planning_category = str(planning_step.get("category") or "")
    planning_payload = _mapping_value(planning_step, "payload")
    digest_reason = str(digest_step.get("reason") or "")
    cutoff = _parse_utc_datetime(summary.get("decision_available_at"))
    run_as_of = _parse_date(summary.get("as_of"))
    if not summary:
        return {
            "status": "unknown",
            "headline": "No radar run is available for alert diagnostics.",
            "next_action": "Run the radar once before reviewing alert planning.",
            "evidence": "no latest run",
            "counts": [],
            "rows": [],
        }
    if planning_status != "success":
        reason = str(planning_step.get("reason") or "n/a")
        return {
            "status": planning_category or planning_status or "unknown",
            "headline": "Alert planning did not complete for the latest run.",
            "next_action": str(
                planning_step.get("operator_action")
                or "Resolve alert-planning telemetry before expecting digest alerts."
            ),
            "evidence": _step_evidence("alert_planning", planning_step),
            "counts": [],
            "rows": [],
            "reason": reason,
        }

    rows = _alert_suppression_diagnostic_rows(
        engine,
        available_at=cutoff,
        as_of_date=run_as_of,
        limit=limit,
    )
    reason_counts = Counter(str(row.get("reason") or "unknown") for row in rows)
    planned_alert_count = int(_finite_float(planning_payload.get("alert_count")))
    digest_alert_count = int(_finite_float(planning_payload.get("digest_alert_count")))
    suppression_count = int(
        _finite_float(planning_payload.get("suppression_count"))
        or len(rows)
    )
    status = "ready" if digest_alert_count else "suppressed"
    headline = (
        f"Alert planning produced {digest_alert_count} digest alert(s)."
        if digest_alert_count
        else (
            "Alert planning ran, but every candidate was suppressed or routed away "
            "from digest alerts."
        )
    )
    next_action = (
        "Review planned alerts in the Alerts tab."
        if digest_alert_count
        else "Review suppression reasons before changing alert thresholds."
    )
    if digest_reason == "no_alerts" and rows:
        next_action = (
            "Start with the top suppression reason; adjust thresholds only if the "
            "suppressed candidates should have paged you."
        )
    return {
        "status": status,
        "headline": headline,
        "next_action": next_action,
        "evidence": (
            f"alert_count={planned_alert_count}; digest_alert_count={digest_alert_count}; "
            f"suppression_count={suppression_count}; "
            f"reasons={_count_map_label(dict(sorted(reason_counts.items())))}"
        ),
        "counts": [
            {"reason": reason, "count": count}
            for reason, count in sorted(
                reason_counts.items(),
                key=lambda item: (-item[1], item[0]),
            )
        ],
        "rows": rows,
    }


def load_ipo_s1_rows(
    engine: Engine,
    *,
    ticker: str | None = None,
    available_at: datetime | None = None,
    limit: int = 50,
) -> list[dict[str, object]]:
    cutoff = _as_utc_datetime_or_none(available_at) or datetime.now(UTC)
    filters = [events.c.available_at <= cutoff]
    if ticker is not None and ticker.strip():
        filters.append(events.c.ticker == ticker.strip().upper())
    stmt = (
        select(events)
        .where(*filters)
        .order_by(
            events.c.source_ts.desc(),
            events.c.available_at.desc(),
            events.c.materiality.desc(),
            events.c.id.desc(),
        )
        .limit(_positive_limit(limit) * 4)
    )
    rows: list[dict[str, object]] = []
    with engine.connect() as conn:
        for row in conn.execute(stmt):
            candidate = _ipo_s1_row(row._mapping)
            if candidate is not None:
                rows.append(candidate)
            if len(rows) >= _positive_limit(limit):
                break
    return rows


def load_validation_summary(engine: Engine) -> dict[str, object]:
    repo = ValidationRepository(engine)
    latest_run_id = _latest_validation_run_id(engine)
    if latest_run_id is None:
        return {
            "latest_run": None,
            "report": None,
            "paper_trades": [_dataclass_dict(row) for row in repo.list_paper_trades()],
            "useful_labels": [
                _dataclass_dict(row) for row in repo.list_useful_alert_labels()
            ],
        }

    latest_run = repo.latest_validation_run(latest_run_id)
    if latest_run is None:
        return {
            "latest_run": None,
            "report": None,
            "paper_trades": [_dataclass_dict(row) for row in repo.list_paper_trades()],
            "useful_labels": [
                _dataclass_dict(row) for row in repo.list_useful_alert_labels()
            ],
        }

    summary_cutoff = latest_run.finished_at or latest_run.started_at
    result_rows = repo.list_validation_results(latest_run.id, available_at=summary_cutoff)
    raw_labels = repo.list_useful_alert_labels(available_at=summary_cutoff)
    with engine.connect() as conn:
        useful_labels = [
            label
            for label in raw_labels
            if _label_matches_validation_results(conn, label, result_rows)
        ]
    report = build_validation_report(
        latest_run.id,
        result_rows,
        useful_alert_labels=useful_labels,
        total_cost=_total_cost_from_metrics(latest_run.metrics),
    )
    return {
        "latest_run": _dataclass_dict(latest_run),
        "report": validation_report_payload(report),
        "paper_trades": [
            _dataclass_dict(row)
            for row in repo.list_paper_trades(available_at=summary_cutoff)
        ],
        "useful_labels": [_dataclass_dict(row) for row in useful_labels],
    }


def load_cost_summary(
    engine: Engine,
    *,
    available_at: datetime | None = None,
) -> dict[str, object]:
    requested_cutoff = _as_utc_datetime_or_none(available_at)
    repo = ValidationRepository(engine)
    latest_run_id = _latest_validation_run_id(engine, available_at=requested_cutoff)
    latest_run = repo.latest_validation_run(latest_run_id) if latest_run_id is not None else None
    latest_result_rows = (
        repo.list_validation_results(
            latest_run.id,
            available_at=latest_run.finished_at or latest_run.started_at,
        )
        if latest_run is not None
        else []
    )
    label_cutoff = (
        (latest_run.finished_at or latest_run.started_at)
        if latest_run
        else requested_cutoff
    )
    raw_labels = repo.list_useful_alert_labels(available_at=label_cutoff)
    with engine.connect() as conn:
        useful_labels = [
            label
            for label in raw_labels
            if str(label.label).lower() in USEFUL_ALERT_LABELS
            and (
                latest_run is None
                or _label_matches_validation_results(conn, label, latest_result_rows)
            )
        ]
    validation_total_cost = (
        _total_cost_from_metrics(latest_run.metrics) if latest_run is not None else 0.0
    )
    ledger_summary = BudgetLedgerRepository(engine).summary(
        available_at=requested_cutoff or datetime.now(UTC)
    )
    total_actual_cost = _finite_float(ledger_summary.get("total_actual_cost_usd"))
    useful_count = len(useful_labels)
    cost_per_useful_alert = (
        0.0
        if total_actual_cost <= 0
        else total_actual_cost / useful_count
        if useful_count > 0
        else None
    )
    config = AppConfig.from_env()
    return {
        "currency": ledger_summary.get("currency", "USD"),
        "total_actual_cost_usd": total_actual_cost,
        "total_estimated_cost_usd": _finite_float(
            ledger_summary.get("total_estimated_cost_usd")
        ),
        "validation_total_cost_usd": validation_total_cost,
        "useful_alert_count": useful_count,
        "cost_per_useful_alert": cost_per_useful_alert,
        "attempt_count": int(_finite_float(ledger_summary.get("attempt_count"))),
        "status_counts": _string_int_mapping(ledger_summary.get("status_counts")),
        "by_task": ledger_summary.get("by_task", []),
        "by_model": ledger_summary.get("by_model", []),
        "rows": ledger_summary.get("rows", []),
        "caps": {
            "premium_llm_enabled": config.enable_premium_llm,
            "daily_budget_usd": config.llm_daily_budget_usd,
            "monthly_budget_usd": config.llm_monthly_budget_usd,
            "task_daily_caps": dict(sorted(config.llm_task_daily_caps.items())),
        },
        "useful_labels": [_dataclass_dict(label) for label in useful_labels],
        "source": "budget_ledger",
    }


def load_ops_health(
    engine: Engine,
    *,
    now: datetime | None = None,
    stale_after: timedelta = timedelta(hours=36),
) -> dict[str, object]:
    from catalyst_radar.ops.health import load_ops_health as _load_ops_health

    return _load_ops_health(engine, now=now, stale_after=stale_after)


def load_radar_run_summary(engine: Engine, *, limit: int = 250) -> dict[str, object]:
    step_names = set(DAILY_STEP_ORDER)
    with engine.connect() as conn:
        rows = [
            _row_dict(row._mapping)
            for row in conn.execute(
                select(job_runs)
                .where(job_runs.c.job_type.in_(step_names))
                .order_by(job_runs.c.started_at.desc(), job_runs.c.id.desc())
                .limit(_positive_limit(limit))
            )
        ]
    if not rows:
        return {}

    run_key = _radar_run_key(rows[0])
    run_rows_by_step: dict[str, dict[str, object]] = {}
    for row in rows:
        step_name = str(row.get("job_type") or "")
        if step_name in run_rows_by_step or _radar_run_key(row) != run_key:
            continue
        run_rows_by_step[step_name] = row

    ordered_steps = [
        run_rows_by_step[step]
        for step in DAILY_STEP_ORDER
        if step in run_rows_by_step
    ]
    classified_steps = [
        (row, _radar_run_step_classification(row)) for row in ordered_steps
    ]
    started_values = [
        value
        for value in (_as_utc_datetime_or_none(row.get("started_at")) for row in ordered_steps)
        if value is not None
    ]
    finished_values = [
        value
        for value in (_as_utc_datetime_or_none(row.get("finished_at")) for row in ordered_steps)
        if value is not None
    ]
    status_counts = Counter(str(row.get("status") or "unknown") for row in ordered_steps)
    outcome_counts = Counter(classification.category for _, classification in classified_steps)
    expected_gate_count = outcome_counts.get("expected_gate", 0)
    required_step_count = max(0, len(ordered_steps) - expected_gate_count)
    required_completed_count = min(
        sum(1 for _, classification in classified_steps if classification.category == "completed"),
        required_step_count,
    )
    blocking_step_count = sum(
        1 for _, classification in classified_steps if classification.blocks_reliance
    )
    first_metadata = rows[0].get("metadata")
    metadata = _row_dict(first_metadata) if isinstance(first_metadata, Mapping) else {}
    return {
        "status": _radar_run_status(ordered_steps),
        "as_of": metadata.get("as_of"),
        "decision_available_at": metadata.get("decision_available_at"),
        "outcome_available_at": metadata.get("outcome_available_at"),
        "provider": metadata.get("provider"),
        "universe": metadata.get("universe"),
        "tickers": metadata.get("tickers") or [],
        "started_at": min(started_values).isoformat() if started_values else None,
        "finished_at": (
            max(finished_values).isoformat()
            if len(finished_values) == len(ordered_steps) and finished_values
            else None
        ),
        "step_count": len(ordered_steps),
        "required_step_count": required_step_count,
        "required_completed_count": required_completed_count,
        "required_incomplete_count": max(
            0, required_step_count - required_completed_count
        ),
        "optional_expected_gate_count": expected_gate_count,
        "action_needed_count": blocking_step_count,
        "run_path_status": _radar_run_path_status(
            required_completed_count=required_completed_count,
            required_step_count=required_step_count,
            blocking_step_count=blocking_step_count,
        ),
        "status_counts": dict(sorted(status_counts.items())),
        "outcome_category_counts": dict(sorted(outcome_counts.items())),
        "blocking_step_count": blocking_step_count,
        "expected_gate_count": expected_gate_count,
        "requested_count": sum(
            int(_finite_float(row.get("requested_count"))) for row in ordered_steps
        ),
        "raw_count": sum(int(_finite_float(row.get("raw_count"))) for row in ordered_steps),
        "normalized_count": sum(
            int(_finite_float(row.get("normalized_count"))) for row in ordered_steps
        ),
        "steps": [
            {
                "id": row.get("id"),
                "step": row.get("job_type"),
                "status": row.get("status"),
                "category": _radar_run_step_classification(row).category,
                "label": _radar_run_step_classification(row).label,
                "started_at": row.get("started_at"),
                "finished_at": row.get("finished_at"),
                "requested_count": row.get("requested_count"),
                "raw_count": row.get("raw_count"),
                "normalized_count": row.get("normalized_count"),
                "error_summary": row.get("error_summary"),
                "reason": _radar_run_step_reason(row),
                "meaning": classification.meaning,
                "operator_action": classification.operator_action,
                "trigger_condition": classification.trigger_condition,
                "blocks_reliance": classification.blocks_reliance,
                "payload": _radar_run_step_payload(row),
            }
            for row, classification in classified_steps
        ],
    }


def load_broker_summary(engine: Engine) -> dict[str, object]:
    broker_repo = BrokerRepository(engine)
    config = AppConfig.from_env()
    return {
        "snapshot": portfolio_snapshot_payload(engine),
        "positions": positions_payload(engine),
        "balances": balances_payload(engine),
        "open_orders": open_orders_payload(engine),
        "exposure": exposure_payload(engine),
        "market_context": [
            market_snapshot_payload(row) for row in broker_repo.latest_market_snapshots()
        ],
        "opportunity_actions": [
            opportunity_action_payload(row) for row in broker_repo.list_opportunity_actions()
        ],
        "triggers": [trigger_payload(row) for row in broker_repo.list_triggers()],
        "order_tickets": [
            order_ticket_payload(row) for row in broker_repo.list_order_tickets()
        ],
        "rate_limits": schwab_rate_limit_status(engine, config=config),
        "rate_limit_config": schwab_rate_limit_config_payload(config),
    }


def data_source_coverage_payload(
    config: AppConfig,
    *,
    broker_summary: Mapping[str, object] | None = None,
) -> list[dict[str, object]]:
    broker = broker_summary if isinstance(broker_summary, Mapping) else {}
    broker_snapshot = _mapping_value(broker, "snapshot")
    broker_exposure = _mapping_value(broker, "exposure")
    rate_limit_config = _mapping_value(broker, "rate_limit_config")
    market_provider = _provider_name(config.daily_market_provider, default="csv")
    event_provider = _provider_name(config.daily_event_provider, default="news_fixture")
    return [
        {
            "layer": "Market data",
            "mode": _market_source_mode(config, market_provider),
            "provider": market_provider,
            "detail": _market_data_detail(config, market_provider),
            "guardrail": f"universe={config.universe_name}; batch={config.scan_batch_size}",
        },
        {
            "layer": "News/events",
            "mode": _event_source_mode(config, event_provider),
            "provider": event_provider,
            "detail": _event_source_detail(config, event_provider),
            "guardrail": _event_source_guardrail(config, event_provider),
        },
        {
            "layer": "Schwab portfolio",
            "mode": _broker_mode(broker_snapshot, broker_exposure),
            "provider": "schwab",
            "detail": (
                f"accounts={broker_snapshot.get('account_count', 0)}; "
                f"positions={broker_snapshot.get('position_count', 0)}"
            ),
            "guardrail": (
                f"read_only=true; stale={bool(broker_exposure.get('broker_data_stale'))}; "
                f"sync_min={rate_limit_config.get('portfolio_sync_min_interval_seconds', 'n/a')}s"
            ),
        },
        {
            "layer": "LLM review",
            "mode": _llm_mode(config),
            "provider": config.llm_provider or "none",
            "detail": ", ".join(
                item
                for item in (
                    config.llm_evidence_model,
                    config.llm_skeptic_model,
                    config.llm_decision_card_model,
                )
                if item
            )
            or "no model configured",
            "guardrail": (
                f"daily_budget={config.llm_daily_budget_usd}; "
                f"monthly_budget={config.llm_monthly_budget_usd}"
            ),
        },
        {
            "layer": "Order submission",
            "mode": "disabled"
            if not config.schwab_order_submission_enabled
            else "blocked_by_policy",
            "provider": "schwab",
            "detail": "order preview only",
            "guardrail": "real order submission is disabled by kill switch",
        },
    ]


def provider_preflight_payload(
    config: AppConfig,
    *,
    radar_run_summary: Mapping[str, object] | None = None,
    broker_summary: Mapping[str, object] | None = None,
) -> list[dict[str, object]]:
    coverage_rows = data_source_coverage_payload(config, broker_summary=broker_summary)
    coverage = {str(row.get("layer") or ""): row for row in coverage_rows}
    steps = _radar_steps_by_name(radar_run_summary)
    market_provider = _provider_name(config.daily_market_provider, default="csv")
    event_provider = _provider_name(config.daily_event_provider, default="news_fixture")
    return [
        _market_preflight_row(config, market_provider, coverage.get("Market data", {})),
        _event_preflight_row(
            config,
            event_provider,
            coverage.get("News/events", {}),
            steps.get("event_ingest", {}),
        ),
        _schwab_preflight_row(config, coverage.get("Schwab portfolio", {})),
        _llm_preflight_row(config, coverage.get("LLM review", {})),
    ]


def radar_run_call_plan_payload(
    engine: Engine,
    config: AppConfig,
    *,
    as_of: date | str | None = None,
    provider: str | None = None,
    universe: str | None = None,
    tickers: Sequence[str] | None = None,
    run_llm: bool = False,
    llm_dry_run: bool = True,
    dry_run_alerts: bool = True,
) -> dict[str, object]:
    normalized_tickers = _call_plan_tickers(tickers)
    sec_targets = _sec_call_plan_targets(
        engine,
        tickers=normalized_tickers,
        limit=config.sec_daily_max_tickers,
    )
    rows = [
        _market_call_plan_row(config),
        _event_call_plan_row(config, sec_targets=sec_targets),
        _llm_call_plan_row(run_llm=run_llm, llm_dry_run=llm_dry_run),
        _alert_call_plan_row(dry_run_alerts=dry_run_alerts),
        _schwab_call_plan_row(),
    ]
    max_external_calls = sum(
        int(_finite_float(row.get("external_call_count_max"))) for row in rows
    )
    blocked_rows = [row for row in rows if str(row.get("status") or "") == "blocked"]
    if blocked_rows:
        status = "blocked"
        headline = "Radar run call plan has blocked live steps."
        next_action = str(blocked_rows[0].get("next_action") or "Review blocked call rows.")
    elif max_external_calls:
        status = "live_calls_planned"
        headline = f"Radar run may make up to {max_external_calls} external call(s)."
        next_action = "Run only if the caps and providers match your intent."
    else:
        status = "local_or_dry_run_only"
        headline = "Radar run has no planned external provider calls."
        next_action = "Safe to use for local fixture/dry-run validation."
    return {
        "schema_version": "radar-run-call-plan-v1",
        "status": status,
        "headline": headline,
        "next_action": next_action,
        "will_call_external_providers": max_external_calls > 0,
        "max_external_call_count": max_external_calls,
        "scope": {
            "as_of": as_of.isoformat() if isinstance(as_of, date) else as_of,
            "provider_override": provider,
            "universe_override": universe,
            "tickers": normalized_tickers,
            "ticker_count": len(normalized_tickers),
        },
        "guardrails": {
            "manual_run_cooldown_seconds": config.radar_run_min_interval_seconds,
            "sec_daily_max_tickers": config.sec_daily_max_tickers,
            "polygon_ticker_seed_max_pages": config.polygon_tickers_max_pages,
            "daily_real_llm_supported": False,
            "daily_real_alert_delivery_supported": False,
            "schwab_called_by_radar_run": False,
        },
        "rows": rows,
    }


def radar_run_default_scope_payload(
    engine: Engine,
    config: AppConfig,
    *,
    radar_run_summary: Mapping[str, object] | None = None,
    ops_health: Mapping[str, object] | None = None,
    now: datetime | None = None,
) -> dict[str, object]:
    """Return the implicit manual-run scope the dashboard should use."""
    resolved_now = _as_utc_datetime_or_none(now) or datetime.now(UTC)
    summary = _row_dict(radar_run_summary) if isinstance(radar_run_summary, Mapping) else {}
    health = (
        _row_dict(ops_health)
        if isinstance(ops_health, Mapping)
        else load_ops_health(engine, now=resolved_now)
    )
    database = _mapping_value(health, "database")
    market_provider = _provider_name(config.daily_market_provider, default="csv")
    market_mode = _market_source_mode(config, market_provider)
    latest_bar_date = _parse_date(database.get("latest_daily_bar_date"))
    current_date = resolved_now.date()
    previous_as_of = _parse_date(summary.get("as_of"))

    payload: dict[str, object] = {
        "schema_version": "radar-run-default-scope-v1",
        "status": "current_default",
        "scope": {},
        "market_provider": market_provider,
        "market_mode": market_mode,
        "current_date": current_date.isoformat(),
        "latest_daily_bar_date": _date_iso_or_none(latest_bar_date),
        "previous_run_as_of": _date_iso_or_none(previous_as_of),
        "headline": "Manual run defaults to the current date.",
        "detail": "Live market runs should use the current as-of date.",
    }

    if market_mode != "fixture":
        return payload
    if latest_bar_date is None:
        payload.update(
            {
                "status": "no_local_bars",
                "headline": "No local daily bars are available for a default scope.",
                "detail": "Refresh or load local market data before running offline discovery.",
            }
        )
        return payload

    payload.update(
        {
            "status": "suggested",
            "scope": {"as_of": latest_bar_date.isoformat()},
            "headline": (
                "Fixture run default uses the latest local daily bar "
                f"{latest_bar_date.isoformat()}."
            ),
            "detail": (
                "No external calls are needed to choose this scope; it avoids "
                "false stale-bar blocks during offline dashboard validation."
            ),
        }
    )
    return payload


def agent_review_summary_payload(
    radar_run_summary: Mapping[str, object] | None,
    candidate_rows: Sequence[Mapping[str, object]] | None = None,
) -> dict[str, object]:
    """Summarize the latest agentic review step for dashboard triage."""
    summary = _row_dict(radar_run_summary) if isinstance(radar_run_summary, Mapping) else {}
    steps = _radar_steps_by_name(summary)
    llm_step = _row_dict(steps.get("llm_review"))
    llm_payload = _mapping_value(llm_step, "payload")
    reviewed_tickers = _call_plan_tickers(
        [str(value) for value in _sequence_value(llm_payload.get("reviewed_tickers"))]
    )
    reviewed_packet_count = int(
        _finite_float(
            _first_present(
                llm_payload.get("reviewed_packet_count"),
                llm_step.get("normalized_count"),
            )
        )
    )
    requested_count = int(_finite_float(llm_step.get("requested_count")))
    reason = str(llm_step.get("reason") or "")
    step_status = str(llm_step.get("status") or "not_run")
    dry_run = bool(llm_payload.get("dry_run")) or reason == "dry_run_only"
    mode = "dry_run" if dry_run else "live" if step_status == "success" else "not_run"
    remaining_gates = [
        _agent_review_gate_row(name, steps[name])
        for name in DAILY_STEP_ORDER
        if name in steps
        and name != "llm_review"
        and str(steps[name].get("category") or "") == "expected_gate"
    ]
    candidate_by_ticker = {
        str(row.get("ticker") or "").strip().upper(): _row_dict(row)
        for row in candidate_rows or ()
        if isinstance(row, Mapping) and str(row.get("ticker") or "").strip()
    }
    reviewed_candidates = [
        _agent_review_candidate_row(ticker, candidate_by_ticker.get(ticker))
        for ticker in reviewed_tickers
    ]
    ticker_text = ", ".join(reviewed_tickers[:5])

    if step_status == "success":
        status = "dry_run_reviewed" if dry_run else "reviewed"
        headline = (
            f"Agent review checked {reviewed_packet_count} packet(s)"
            + (f" for {ticker_text}." if ticker_text else ".")
        )
        next_action = (
            "Open reviewed tickers, compare the research brief with source evidence, "
            "then decide whether the candidate deserves manual follow-up."
        )
    elif reason == "no_llm_review_inputs":
        status = "no_review_inputs"
        headline = "Agent review had no eligible packets."
        next_action = str(
            llm_step.get("trigger_condition")
            or "At least one Warning or manual-review candidate packet must exist."
        )
    elif reason == "llm_disabled":
        status = "disabled"
        headline = "Agent review is disabled for the latest run."
        next_action = (
            "Enable dry-run review first; use real model calls only after budgets are set."
        )
    elif str(llm_step.get("category") or "") == "expected_gate":
        status = "expected_gate"
        headline = "Agent review did not trigger for the latest run."
        next_action = str(llm_step.get("trigger_condition") or "Review the run gate reason.")
    elif llm_step:
        status = "attention"
        headline = "Agent review needs attention."
        next_action = str(llm_step.get("operator_action") or "Inspect LLM step telemetry.")
    else:
        status = "not_available"
        headline = "No agent review step is available yet."
        next_action = "Run Radar with LLM dry run enabled to produce an auditable review step."

    return {
        "schema_version": "agent-review-summary-v1",
        "status": status,
        "mode": mode,
        "headline": headline,
        "next_action": next_action,
        "as_of": summary.get("as_of"),
        "step_status": step_status,
        "reason": reason or None,
        "requested_count": requested_count,
        "reviewed_packet_count": reviewed_packet_count,
        "review_task": llm_payload.get("review_task"),
        "reviewed_tickers": reviewed_tickers,
        "reviewed_candidates": reviewed_candidates,
        "candidate_packet_ids": [
            str(value)
            for value in _sequence_value(llm_payload.get("candidate_packet_ids"))
            if str(value).strip()
        ],
        "candidate_packet_state_counts": _string_int_mapping(
            llm_payload.get("candidate_packet_state_counts")
        ),
        "eligible_states": [
            str(value)
            for value in _sequence_value(llm_payload.get("eligible_states"))
            if str(value).strip()
        ],
        "remaining_expected_gates": remaining_gates,
        "evidence": _step_evidence("llm_review", llm_step) if llm_step else "n/a",
    }


def _agent_review_candidate_row(
    ticker: str,
    row: Mapping[str, object] | None,
) -> dict[str, object]:
    candidate = _row_dict(row) if isinstance(row, Mapping) else {}
    brief = _mapping_value(candidate, "research_brief")
    support = _mapping_value(candidate, "top_supporting_evidence")
    if not candidate:
        return {
            "ticker": ticker,
            "state": "n/a",
            "score": None,
            "setup": "n/a",
            "why_now": "Reviewed ticker is not in the current candidate table.",
            "evidence": None,
            "risk_or_gap": None,
            "next_step": "Refresh the dashboard data and rerun the radar if needed.",
        }
    return {
        "ticker": ticker,
        "state": candidate.get("state"),
        "score": candidate.get("final_score"),
        "setup": candidate.get("setup_type"),
        "why_now": brief.get("why_now"),
        "evidence": brief.get("supporting_evidence") or support.get("title"),
        "risk_or_gap": brief.get("risk_or_gap"),
        "next_step": _first_present(
            candidate.get("decision_next_step"),
            brief.get("next_step"),
        ),
    }


def _agent_review_gate_row(
    step: str,
    row: Mapping[str, object],
) -> dict[str, object]:
    return {
        "step": step,
        "reason": row.get("reason"),
        "trigger_condition": row.get("trigger_condition"),
        "meaning": row.get("meaning"),
        "operator_action": row.get("operator_action"),
    }


def activation_summary_payload(
    config: AppConfig,
    *,
    radar_run_summary: Mapping[str, object] | None = None,
    broker_summary: Mapping[str, object] | None = None,
) -> dict[str, object]:
    coverage_rows = data_source_coverage_payload(config, broker_summary=broker_summary)
    coverage = {str(row.get("layer") or ""): row for row in coverage_rows}
    readiness_rows = readiness_checklist_payload(
        config,
        radar_run_summary=radar_run_summary,
        broker_summary=broker_summary,
    )
    blocked_rows = [
        row for row in readiness_rows if str(row.get("status") or "") == "blocked"
    ]
    attention_rows = [
        row for row in readiness_rows if str(row.get("status") or "") == "attention"
    ]
    market = coverage.get("Market data", {})
    events = coverage.get("News/events", {})
    market_mode = str(market.get("mode") or "unknown")
    event_mode = str(events.get("mode") or "unknown")
    run_path = _radar_run_path_summary(radar_run_summary)

    if blocked_rows and (
        "missing_credentials" in {market_mode, event_mode}
        or market_mode == "disabled"
        or event_mode == "disabled"
    ):
        status = "blocked"
        headline = "Live activation is blocked."
        detail = _activation_blocker_detail(blocked_rows)
        next_action = _activation_next_action(blocked_rows)
    elif market_mode == "fixture" or event_mode == "fixture":
        status = "fixture"
        headline = "Fixture mode: not a live US-market scan yet."
        detail = (
            f"Market data is {market.get('provider') or market_mode}; "
            f"news/events are {events.get('provider') or event_mode}."
        )
        next_action = (
            "Set CATALYST_DAILY_MARKET_PROVIDER=polygon, CATALYST_POLYGON_API_KEY, "
            "CATALYST_DAILY_EVENT_PROVIDER=sec, CATALYST_SEC_ENABLE_LIVE=1, and "
            "CATALYST_SEC_USER_AGENT; then run one capped radar cycle."
        )
    elif blocked_rows:
        status = "blocked"
        headline = "Radar output has blockers."
        detail = _activation_blocker_detail(blocked_rows)
        next_action = _activation_next_action(blocked_rows)
    elif attention_rows:
        status = "attention"
        headline = "Radar is usable with attention items."
        detail = _activation_blocker_detail(attention_rows)
        next_action = _activation_next_action(attention_rows)
    else:
        status = "ready"
        headline = "Live radar inputs are ready."
        detail = (
            f"Market data provider {market.get('provider') or market_mode}; "
            f"event provider {events.get('provider') or event_mode}."
        )
        next_action = (
            "Run one radar cycle, inspect provider health and rejected counts, "
            "then scale the universe cautiously."
        )

    return {
        "status": status,
        "headline": headline,
        "detail": detail,
        "next_action": next_action,
        "evidence": (
            f"market={market.get('provider') or 'unknown'}/{market_mode}; "
            f"events={events.get('provider') or 'unknown'}/{event_mode}; "
            f"required_path={run_path['required_complete']}/{run_path['required_total']}; "
            f"action_needed={run_path['blocking_count']}; "
            f"optional_gates={run_path['expected_gate_count']}"
        ),
    }


def live_activation_plan_payload(
    config: AppConfig,
    *,
    radar_run_summary: Mapping[str, object] | None = None,
    broker_summary: Mapping[str, object] | None = None,
) -> dict[str, object]:
    coverage_rows = data_source_coverage_payload(config, broker_summary=broker_summary)
    coverage = {str(row.get("layer") or ""): row for row in coverage_rows}
    preflight_rows = provider_preflight_payload(
        config,
        radar_run_summary=radar_run_summary,
        broker_summary=broker_summary,
    )
    run_path = _radar_run_path_summary(radar_run_summary)
    market_missing_env = _market_activation_missing_env(config)
    event_missing_env = _event_activation_missing_env(config)
    missing_env = [*market_missing_env, *event_missing_env]
    action_needed = run_path["blocking_count"]
    required_complete = run_path["required_complete"]
    required_total = run_path["required_total"]
    market = coverage.get("Market data", {})
    events = coverage.get("News/events", {})
    market_mode = str(market.get("mode") or "unknown")
    event_mode = str(events.get("mode") or "unknown")

    if missing_env:
        status = "blocked"
        headline = "Live market activation is not configured yet."
        next_action = "Set the missing environment variables, then run one capped radar cycle."
    elif action_needed:
        status = "attention"
        headline = "Live inputs are configured, but the last run needs action."
        next_action = "Fix the blocked run step before relying on the output."
    elif required_total and required_complete >= required_total:
        status = "ready"
        headline = "Live activation inputs and required run path are ready."
        next_action = "Run one capped radar cycle and inspect discovery blockers before scaling."
    else:
        status = "attention"
        headline = "Live activation needs a fresh run."
        next_action = "Run one capped radar cycle and inspect required-path telemetry."

    task_rows = [
        {
            "area": "Required run path",
            "status": (
                "ready"
                if required_total and required_complete >= required_total and not action_needed
                else "attention"
            ),
            "current_state": f"{required_complete}/{required_total} completed",
            "missing_env": "",
            "safe_next_action": (
                "No run-step action needed."
                if not action_needed
                else "Review the blocked run-step table before using candidates."
            ),
        },
        _activation_task_row(
            "Live market data",
            market,
            missing=market_missing_env,
            ready_modes={"live"},
            safe_next_action=(
                "Set Polygon provider/key, then keep the first run to one grouped-daily request."
            ),
        ),
        _activation_task_row(
            "SEC catalyst feed",
            events,
            missing=event_missing_env,
            ready_modes={"live"},
            safe_next_action=(
                "Set SEC live mode/User-Agent, then keep submissions capped per run."
            ),
        ),
        _activation_task_row(
            "Schwab portfolio context",
            coverage.get("Schwab portfolio", {}),
            missing=[],
            ready_modes={"read_only_connected", "stale_read_only_connected"},
            safe_next_action="Use read-only sync as context; order submission stays disabled.",
            optional=True,
        ),
        _activation_task_row(
            "Agentic LLM review",
            coverage.get("LLM review", {}),
            missing=_llm_activation_missing_env(config),
            ready_modes={"enabled"},
            safe_next_action=(
                "Enable only after provider, skeptic model, key, pricing, budget caps, "
                "and a low task cap are set."
            ),
            optional=True,
        ),
    ]
    call_budget_rows = [
        {
            "layer": str(row.get("layer") or ""),
            "status": str(row.get("status") or ""),
            "call_budget": str(row.get("call_budget") or ""),
            "guardrail": str(row.get("guardrail") or ""),
        }
        for row in preflight_rows
    ]
    return {
        "status": status,
        "headline": headline,
        "next_action": next_action,
        "missing_env": missing_env,
        "tasks": task_rows,
        "call_budgets": call_budget_rows,
        "evidence": (
            f"run_path={required_complete}/{required_total}; "
            f"optional_expected_gates={run_path['expected_gate_count']}; "
            f"action_needed={action_needed}; "
            f"market={market.get('provider') or 'unknown'}/{market_mode}; "
            f"events={events.get('provider') or 'unknown'}/{event_mode}"
        ),
    }


def live_data_activation_contract_payload(
    config: AppConfig,
    *,
    radar_run_summary: Mapping[str, object] | None = None,
    broker_summary: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Return exact operator steps for moving from fixture mode to capped live inputs."""
    plan = live_activation_plan_payload(
        config,
        radar_run_summary=radar_run_summary,
        broker_summary=broker_summary,
    )
    missing_env = [str(item) for item in _sequence_value(plan.get("missing_env"))]
    status = "blocked" if missing_env else "ready"
    headline = (
        "Live data activation needs environment edits."
        if missing_env
        else "Live data activation inputs are configured."
    )
    next_action = (
        "Fill the template values in .env.local, restart services, then inspect the call plan."
        if missing_env
        else "Inspect the call plan, seed the universe once if needed, then run one capped cycle."
    )
    return {
        "schema_version": "live-data-activation-contract-v1",
        "status": status,
        "headline": headline,
        "next_action": next_action,
        "read_only": True,
        "makes_external_calls": False,
        "missing_env": missing_env,
        "minimum_env_lines": _live_data_minimum_env_lines(config),
        "env_template": _live_data_env_template(config),
        "safe_limits": _live_data_safe_limits(config),
        "operator_steps": _live_data_operator_steps(config, missing_env=missing_env),
        "worker_env_lines": _live_data_worker_env_lines(),
        "worker_commands": _live_data_worker_commands(),
        "call_budget_if_activated": _live_data_call_budget_if_activated(config),
        "evidence": (
            f"contract_calls_external=no; missing_env={len(missing_env)}; "
            f"polygon_ticker_pages={config.polygon_tickers_max_pages}; "
            f"sec_daily_tickers={config.sec_daily_max_tickers}; "
            f"manual_run_cooldown={config.radar_run_min_interval_seconds}s"
        ),
    }


def telemetry_tape_payload(
    ops_health: Mapping[str, object],
    *,
    limit: int = 8,
) -> dict[str, object]:
    telemetry = _mapping_value(ops_health, "telemetry")
    status_counts = _string_int_mapping(telemetry.get("status_counts"))
    rows: list[dict[str, object]] = []
    for event in _sequence_value(telemetry.get("events"))[: _positive_limit(limit)]:
        if not isinstance(event, Mapping):
            continue
        row = _row_dict(event)
        event_type = str(row.get("event_type") or "unknown")
        metadata = _mapping_value(row, "metadata")
        after_payload = _mapping_value(row, "after_payload")
        outcome = _telemetry_step_outcome_fields(event_type, row, metadata)
        rows.append(
            {
                "occurred_at": row.get("occurred_at"),
                "event": event_type.removeprefix("telemetry."),
                "status": outcome.get("status") or row.get("status") or "unknown",
                "reason": row.get("reason") or "",
                "artifact": _telemetry_artifact_label(row),
                **{
                    key: value
                    for key, value in outcome.items()
                    if key != "status"
                },
                "summary": _telemetry_event_summary(
                    event_type,
                    event=row,
                    metadata=metadata,
                    after_payload=after_payload,
                ),
            }
        )
    return {
        "status": _telemetry_tape_status(status_counts),
        "event_count": int(_finite_float(telemetry.get("event_count"))),
        "latest_event_at": telemetry.get("latest_event_at"),
        "status_counts": status_counts,
        "events": rows,
    }


def radar_run_cooldown_payload(
    engine: Engine,
    config: AppConfig,
    *,
    now: datetime | None = None,
) -> dict[str, object]:
    resolved_now = _as_utc_datetime_or_none(now) or datetime.now(UTC)
    min_interval_seconds = max(1, int(config.radar_run_min_interval_seconds))
    with engine.connect() as conn:
        lock_row = conn.execute(
            select(
                job_locks.c.lock_name,
                job_locks.c.owner,
                job_locks.c.acquired_at,
                job_locks.c.expires_at,
                job_locks.c.metadata,
            )
            .where(job_locks.c.lock_name == RADAR_RUN_COOLDOWN_LOCK_NAME)
            .limit(1)
        ).mappings().first()

    lock = _row_dict(lock_row) if lock_row is not None else {}
    reset_at = _parse_utc_datetime(lock.get("expires_at"))
    acquired_at = _parse_utc_datetime(lock.get("acquired_at"))
    active = reset_at is not None and reset_at > resolved_now
    retry_after_seconds = (
        _retry_after_seconds(reset_at, resolved_now) if active else 0
    )

    if active:
        status = "cooldown"
        headline = "Manual radar run is cooling down."
        detail = f"Next run is allowed in {retry_after_seconds} second(s)."
        next_action = "Wait for the cooldown to expire before starting another manual run."
    else:
        status = "ready"
        headline = "Manual radar run is ready."
        detail = f"Minimum interval is {min_interval_seconds} second(s)."
        next_action = "Start one capped radar cycle when you are ready to refresh signals."

    return {
        "status": status,
        "allowed": not active,
        "operation": "manual_radar_run",
        "lock_name": RADAR_RUN_COOLDOWN_LOCK_NAME,
        "min_interval_seconds": min_interval_seconds,
        "retry_after_seconds": retry_after_seconds,
        "reset_at": _iso_or_none(reset_at),
        "acquired_at": _iso_or_none(acquired_at),
        "headline": headline,
        "detail": detail,
        "next_action": next_action,
        "evidence": (
            f"lock={'active' if active else 'inactive'}; "
            f"expires_at={_iso_or_na(reset_at)}; "
            f"min_interval_seconds={min_interval_seconds}"
        ),
    }


def worker_status_payload(
    engine: Engine,
    *,
    now: datetime | None = None,
) -> dict[str, object]:
    resolved_now = _as_utc_datetime_or_none(now) or datetime.now(UTC)
    step_names = set(DAILY_STEP_ORDER)
    with engine.connect() as conn:
        lock_row = conn.execute(
            select(
                job_locks.c.lock_name,
                job_locks.c.owner,
                job_locks.c.acquired_at,
                job_locks.c.heartbeat_at,
                job_locks.c.expires_at,
                job_locks.c.metadata,
            )
            .where(job_locks.c.lock_name == DAILY_WORKER_LOCK_NAME)
            .limit(1)
        ).mappings().first()
        latest_run = conn.execute(
            select(
                job_runs.c.job_type,
                job_runs.c.status,
                job_runs.c.started_at,
                job_runs.c.finished_at,
                job_runs.c.metadata,
            )
            .where(job_runs.c.job_type.in_(step_names))
            .order_by(job_runs.c.started_at.desc(), job_runs.c.id.desc())
            .limit(1)
        ).mappings().first()

    lock = _row_dict(lock_row) if lock_row is not None else {}
    expires_at = _parse_utc_datetime(lock.get("expires_at"))
    heartbeat_at = _parse_utc_datetime(lock.get("heartbeat_at"))
    active = expires_at is not None and expires_at > resolved_now
    run = _row_dict(latest_run) if latest_run is not None else {}
    latest_started_at = _parse_utc_datetime(run.get("started_at"))
    latest_finished_at = _parse_utc_datetime(run.get("finished_at"))
    if active:
        status = "running"
        headline = "Daily worker lock is active."
        next_action = "Let the current worker cycle finish before starting another one."
    elif run:
        status = "idle"
        headline = "No daily worker lock is active, but radar job history exists."
        next_action = (
            "Start the one-shot smoke or daily worker loop after live inputs are configured."
        )
    else:
        status = "not_seen"
        headline = "No daily worker activity has been recorded yet."
        next_action = "Use the worker handoff only after the live data contract is ready."
    return {
        "schema_version": "worker-status-v1",
        "status": status,
        "headline": headline,
        "next_action": next_action,
        "lock_name": DAILY_WORKER_LOCK_NAME,
        "lock_active": active,
        "lock_owner": lock.get("owner"),
        "lock_acquired_at": _iso_or_none(_parse_utc_datetime(lock.get("acquired_at"))),
        "lock_heartbeat_at": _iso_or_none(heartbeat_at),
        "lock_expires_at": _iso_or_none(expires_at),
        "latest_job_type": run.get("job_type"),
        "latest_job_status": run.get("status"),
        "latest_started_at": _iso_or_none(latest_started_at),
        "latest_finished_at": _iso_or_none(latest_finished_at),
        "evidence": (
            f"lock={'active' if active else 'inactive'}; "
            f"owner={lock.get('owner') or 'n/a'}; "
            f"heartbeat_at={_iso_or_na(heartbeat_at)}; "
            f"expires_at={_iso_or_na(expires_at)}; "
            f"latest_job={run.get('job_type') or 'n/a'}"
        ),
    }


def universe_coverage_payload(
    config: AppConfig,
    ops_health: Mapping[str, object],
) -> dict[str, object]:
    database = _mapping_value(ops_health, "database")
    active_count = int(_finite_float(database.get("active_security_count")))
    with_bars_count = int(
        _finite_float(database.get("active_security_with_daily_bar_count"))
    )
    target_count = max(1, int(config.scan_batch_size))
    thin_floor = min(100, target_count)

    if active_count == 0:
        status = "blocked"
        headline = "No active scan universe is loaded."
        detail = "The radar has no active securities to scan."
    elif active_count < thin_floor:
        status = "thin"
        headline = f"Thin scan universe: {active_count} active securities."
        detail = "This is useful for smoke tests, not broad US-market discovery."
    elif active_count < target_count:
        status = "partial"
        headline = f"Partial scan universe: {active_count} active securities."
        detail = f"The configured scan batch target is {target_count}."
    elif with_bars_count < active_count:
        status = "attention"
        headline = "Scan universe is loaded, but daily bars are incomplete."
        detail = f"{with_bars_count} of {active_count} active securities have daily bars."
    else:
        status = "ready"
        headline = f"Scan universe is loaded: {active_count} active securities."
        detail = "Active securities and daily bars are available for scanning."

    return {
        "status": status,
        "headline": headline,
        "detail": detail,
        "next_action": (
            "Seed or refresh the universe with "
            f"`python -m catalyst_radar.cli ingest-polygon tickers --max-pages "
            f"{config.polygon_tickers_max_pages}` before relying on broad discovery."
            if status in {"blocked", "thin", "partial"}
            else "Monitor daily-bar coverage and rejected provider records after each run."
        ),
        "evidence": (
            f"active={active_count}; with_daily_bars={with_bars_count}; "
            f"target={target_count}; "
            f"latest_daily_bar={database.get('latest_daily_bar_date') or 'n/a'}"
        ),
    }


def radar_discovery_snapshot_payload(
    engine: Engine,
    config: AppConfig,
    *,
    radar_run_summary: Mapping[str, object] | None = None,
    ops_health: Mapping[str, object] | None = None,
    candidate_rows: Sequence[Mapping[str, object]] | None = None,
    limit: int = 5,
) -> dict[str, object]:
    """Summarize latest persisted discovery yield without touching providers."""
    summary = (
        _row_dict(radar_run_summary)
        if isinstance(radar_run_summary, Mapping)
        else load_radar_run_summary(engine)
    )
    as_of_date = _parse_date(summary.get("as_of"))
    cutoff = _parse_utc_datetime(summary.get("decision_available_at"))
    artifact_cutoff = _parse_utc_datetime(summary.get("finished_at")) or cutoff
    health = (
        _row_dict(ops_health)
        if isinstance(ops_health, Mapping)
        else load_ops_health(engine, now=cutoff)
    )
    if candidate_rows is not None:
        run_candidate_rows = [_row_dict(row) for row in candidate_rows]
        context_candidate_rows = run_candidate_rows
    else:
        run_candidate_rows = load_candidate_rows(
            engine,
            available_at=artifact_cutoff,
            as_of_date=as_of_date,
        )
        context_candidate_rows = run_candidate_rows or load_candidate_rows(
            engine,
            available_at=artifact_cutoff,
        )
    steps = _radar_steps_by_name(summary)
    scoped_candidates = _discovery_scoped_candidates(run_candidate_rows, summary)
    context_candidates = _discovery_scoped_candidates(context_candidate_rows, summary)
    candidates = _discovery_run_candidates(scoped_candidates, summary)
    coverage_rows = _discovery_source_coverage_payload(
        config,
        summary=summary,
        steps=steps,
    )
    coverage = {str(row.get("layer") or ""): row for row in coverage_rows}
    market = coverage.get("Market data", {})
    events = coverage.get("News/events", {})
    database = _mapping_value(health, "database")
    run_path = _radar_run_path_summary(summary)
    latest_bar_date = _parse_date(database.get("latest_daily_bar_date"))
    latest_candidate_at = _latest_candidate_as_of(context_candidates)
    latest_candidate_session_date = _date_iso_or_none(latest_candidate_at)

    candidate_count = len(candidates)
    packet_count = _step_metric(
        steps,
        "candidate_packets",
        "normalized_count",
        default=0,
    )
    card_count = _step_metric(
        steps,
        "decision_cards",
        "normalized_count",
        default=0,
    )
    packet_candidates = _latest_run_packet_candidates(candidates, summary)
    latest_candidate_context = _latest_candidate_context_payload(
        context_candidates,
        summary,
        cutoff=cutoff,
        limit=limit,
    )
    requested_count = _step_metric(
        steps,
        "daily_bar_ingest",
        "requested_count",
        default=_step_metric(
            steps,
            "feature_scan",
            "requested_count",
            default=int(_finite_float(database.get("active_security_count"))),
        ),
    )
    scanned_count = _step_metric(
        steps,
        "feature_scan",
        "normalized_count",
        default=candidate_count,
    )
    blockers = _discovery_blockers(
        summary=summary,
        market=market,
        events=events,
        database=database,
        run_path=run_path,
        as_of_date=as_of_date,
        latest_bar_date=latest_bar_date,
        packet_count=packet_count,
    )
    status = _discovery_status(
        has_run=bool(summary),
        market_mode=str(market.get("mode") or "unknown"),
        event_mode=str(events.get("mode") or "unknown"),
        run_path=run_path,
        blockers=blockers,
        packet_count=packet_count,
    )

    return {
        "status": status,
        "headline": _discovery_headline(status, candidate_count),
        "detail": _discovery_detail(
            market=market,
            events=events,
            packet_count=packet_count,
            card_count=card_count,
        ),
        "next_action": _discovery_next_action(status, blockers),
        "evidence": (
            f"as_of={summary.get('as_of') or 'n/a'}; "
            f"decision_available_at={summary.get('decision_available_at') or 'n/a'}; "
            f"market={market.get('provider') or 'unknown'}/{market.get('mode') or 'unknown'}; "
            f"events={events.get('provider') or 'unknown'}/{events.get('mode') or 'unknown'}; "
            f"latest_daily_bar={database.get('latest_daily_bar_date') or 'n/a'}; "
            f"latest_candidate_session_date={latest_candidate_session_date or 'n/a'}; "
            f"latest_candidate_as_of={_iso_or_na(latest_candidate_at)}"
        ),
        "run": {
            "status": summary.get("status"),
            "as_of": summary.get("as_of"),
            "decision_available_at": summary.get("decision_available_at"),
            "provider": summary.get("provider"),
            "universe": summary.get("universe"),
            "required_complete": run_path["required_complete"],
            "required_total": run_path["required_total"],
            "blocking_count": run_path["blocking_count"],
            "expected_gate_count": run_path["expected_gate_count"],
        },
        "source_modes": {
            "market": market.get("mode") or "unknown",
            "market_provider": market.get("provider") or "unknown",
            "events": events.get("mode") or "unknown",
            "event_provider": events.get("provider") or "unknown",
        },
        "freshness": {
            "latest_daily_bar_date": (
                latest_bar_date.isoformat() if latest_bar_date is not None else None
            ),
            "latest_bars_older_than_as_of": bool(
                as_of_date is not None
                and latest_bar_date is not None
                and latest_bar_date < as_of_date
            ),
            "latest_candidate_as_of": _iso_or_none(latest_candidate_at),
            "latest_candidate_session_date": latest_candidate_session_date,
            "latest_candidate_age_days": _age_days(cutoff, latest_candidate_at),
        },
        "yield": {
            "requested_securities": requested_count,
            "scanned_securities": scanned_count,
            "candidate_states": candidate_count,
            "candidate_packets": packet_count,
            "decision_cards": card_count,
        },
        "latest_candidate_context": latest_candidate_context,
        "blockers": blockers,
        "top_discoveries": [
            _discovery_candidate(row)
            for row in (
                packet_candidates[: max(0, int(limit))]
                if packet_count > 0 and summary
                else []
            )
        ],
    }


def readiness_checklist_payload(
    config: AppConfig,
    *,
    radar_run_summary: Mapping[str, object] | None = None,
    broker_summary: Mapping[str, object] | None = None,
) -> list[dict[str, object]]:
    coverage_rows = data_source_coverage_payload(config, broker_summary=broker_summary)
    coverage = {str(row.get("layer") or ""): row for row in coverage_rows}
    steps = _radar_steps_by_name(radar_run_summary)
    rows: list[dict[str, object]] = []

    market = coverage.get("Market data", {})
    market_mode = str(market.get("mode") or "unknown")
    if market.get("provider") == "polygon" and not config.polygon_api_key:
        rows.append(
            _readiness_row(
                "Live market scan",
                "blocked",
                "Polygon daily market data is selected, but the API key is missing.",
                "Set CATALYST_POLYGON_API_KEY before using Polygon from the dashboard run.",
                _coverage_evidence(market),
            )
        )
    elif market_mode == "live":
        rows.append(
            _readiness_row(
                "Live market scan",
                "ready",
                f"Market scan is configured for live provider {market.get('provider')}.",
                "Watch provider health, rejected counts, and rate-limit telemetry.",
                _coverage_evidence(market),
            )
        )
    elif market_mode == "disabled":
        rows.append(
            _readiness_row(
                "Live market scan",
                "blocked",
                "Market scan has no enabled daily market provider.",
                "Configure a daily market provider before treating radar output as current.",
                _coverage_evidence(market),
            )
        )
    else:
        rows.append(
            _readiness_row(
                "Live market scan",
                "blocked",
                "Market scan is using local fixture data, not fresh US-market coverage.",
                "Configure a live daily market provider and keep batch/rate limits enabled.",
                _coverage_evidence(market),
            )
        )

    events = coverage.get("News/events", {})
    event_mode = str(events.get("mode") or "unknown")
    if events.get("provider") in {"sec", "sec_submissions"} and event_mode == "missing_credentials":
        rows.append(
            _readiness_row(
                "Catalyst feed",
                "blocked",
                "SEC catalyst ingestion is selected, but live SEC settings are incomplete.",
                (
                    "Set CATALYST_SEC_ENABLE_LIVE=1 and CATALYST_SEC_USER_AGENT before "
                    "using SEC scheduled ingest."
                ),
                _coverage_evidence(events),
            )
        )
    elif event_mode == "live":
        rows.append(
            _readiness_row(
                "Catalyst feed",
                "ready",
                f"News/event ingestion is configured for provider {events.get('provider')}.",
                "Monitor source freshness and event rejection telemetry.",
                _coverage_evidence(events),
            )
        )
    elif event_mode == "disabled":
        rows.append(
            _readiness_row(
                "Catalyst feed",
                "blocked",
                "No news or catalyst provider is enabled.",
                "Configure a scheduled event provider so the radar can detect new catalysts.",
                _coverage_evidence(events),
            )
        )
    else:
        rows.append(
            _readiness_row(
                "Catalyst feed",
                "blocked",
                "Catalyst ingestion is using fixture events, not fresh news or filings.",
                "Configure a live news/SEC event source before relying on early-discovery output.",
                _coverage_evidence(events),
            )
        )

    research_steps = (
        "event_ingest",
        "local_text_triage",
        "feature_scan",
        "scoring_policy",
        "candidate_packets",
    )
    missing_or_unsuccessful = [
        step
        for step in research_steps
        if str(steps.get(step, {}).get("status") or "") != "success"
    ]
    if not steps:
        rows.append(
            _readiness_row(
                "Research loop",
                "attention",
                "No radar run telemetry is available yet.",
                "Run the radar once and review step-level telemetry.",
                "no latest run",
            )
        )
    elif not missing_or_unsuccessful:
        rows.append(
            _readiness_row(
                "Research loop",
                "ready",
                "The latest run produced features, scores, and candidate packets.",
                "Use the Candidate Queue and Research Briefs for triage.",
                _steps_evidence(steps, research_steps),
            )
        )
    else:
        rows.append(
            _readiness_row(
                "Research loop",
                "blocked",
                "The latest run did not complete the full research packet path.",
                (
                    "Fix the first skipped/failed upstream step before treating candidates "
                    "as complete."
                ),
                _steps_evidence(steps, missing_or_unsuccessful),
            )
        )

    decision_step = steps.get("decision_cards", {})
    decision_reason = str(decision_step.get("reason") or "")
    decision_category = str(decision_step.get("category") or "")
    if str(decision_step.get("status") or "") == "success":
        rows.append(
            _readiness_row(
                "Decision Cards",
                "ready",
                "Decision Cards were generated in the latest run.",
                "Review card assumptions and next-review dates before acting.",
                _step_evidence("decision_cards", decision_step),
            )
        )
    elif decision_reason == "no_manual_buy_review_inputs":
        rows.append(
            _readiness_row(
                "Decision Cards",
                "optional",
                "No candidate reached the manual buy-review gate in the latest run.",
                (
                    "Review research briefs manually, or adjust policy thresholds if this "
                    "gate is too conservative."
                ),
                _step_evidence("decision_cards", decision_step),
            )
        )
    elif decision_reason == "no_candidate_packets":
        rows.append(
            _readiness_row(
                "Decision Cards",
                "blocked",
                "Decision Cards did not run because no candidate packets were produced.",
                "Fix packet generation or loosen the warning threshold before expecting cards.",
                _step_evidence("decision_cards", decision_step),
            )
        )
    elif decision_category == "expected_gate":
        rows.append(
            _readiness_row(
                "Decision Cards",
                "optional",
                "Decision Cards were not needed for the latest run.",
                "No action required unless you want this optional gate to run.",
                _step_evidence("decision_cards", decision_step),
            )
        )
    else:
        rows.append(
            _readiness_row(
                "Decision Cards",
                "blocked",
                "Decision Cards were not generated.",
                "Resolve the listed run-step reason before using cards as an action layer.",
                _step_evidence("decision_cards", decision_step),
            )
        )

    llm_step = steps.get("llm_review", {})
    llm_mode = str(coverage.get("LLM review", {}).get("mode") or "unknown")
    llm_missing = _llm_missing_env(config)
    if str(llm_step.get("status") or "") == "success":
        rows.append(
            _readiness_row(
                "LLM review",
                "ready",
                "LLM review ran for the latest candidate set.",
                "Audit cost and evidence citations before increasing automation.",
                _step_evidence("llm_review", llm_step),
            )
        )
    elif llm_missing:
        rows.append(
            _readiness_row(
                "LLM review",
                "blocked",
                "OpenAI review is enabled, but setup is incomplete.",
                (
                    "Set missing model, key, pricing, and budget variables before "
                    "running real agentic LLM review."
                ),
                _coverage_evidence(coverage.get("LLM review", {})),
            )
        )
    elif llm_mode == "disabled" or str(llm_step.get("reason") or "") == "llm_disabled":
        rows.append(
            _readiness_row(
                "LLM review",
                "optional",
                (
                    "Premium LLM review is disabled; deterministic research briefs are "
                    "still available."
                ),
                (
                    "Configure OpenAI models, budgets, and credentials only when you want "
                    "agentic review."
                ),
                _coverage_evidence(coverage.get("LLM review", {})),
            )
        )
    else:
        rows.append(
            _readiness_row(
                "LLM review",
                "attention",
                "LLM review did not run in the latest radar cycle.",
                "Check model configuration, budget caps, and candidate-card availability.",
                _step_evidence("llm_review", llm_step),
            )
        )

    broker = coverage.get("Schwab portfolio", {})
    broker_mode = str(broker.get("mode") or "unknown")
    if broker_mode == "read_only_connected":
        rows.append(
            _readiness_row(
                "Portfolio context",
                "ready",
                "Schwab read-only portfolio context is connected and fresh.",
                "Use exposure warnings as context; order submission remains disabled.",
                _coverage_evidence(broker),
            )
        )
    elif broker_mode == "stale_read_only_connected":
        rows.append(
            _readiness_row(
                "Portfolio context",
                "attention",
                "Schwab portfolio context is connected but stale.",
                (
                    "Run one portfolio sync from the Broker tab; the minimum sync interval "
                    "is enforced."
                ),
                _coverage_evidence(broker),
            )
        )
    else:
        rows.append(
            _readiness_row(
                "Portfolio context",
                "optional",
                "Schwab portfolio context is not connected.",
                "Connect Schwab when you want position-aware exposure checks.",
                _coverage_evidence(broker),
            )
        )

    digest_step = steps.get("digest", {})
    digest_reason = str(digest_step.get("reason") or "")
    digest_category = str(digest_step.get("category") or "")
    if str(digest_step.get("status") or "") == "success":
        rows.append(
            _readiness_row(
                "Alerting",
                "ready",
                "The latest run generated an alert digest.",
                "Review alert route and dry-run status before delivery automation.",
                _step_evidence("digest", digest_step),
            )
        )
    elif digest_reason == "no_alerts":
        alert_planning_step = steps.get("alert_planning", {})
        alert_planning_status = str(alert_planning_step.get("status") or "")
        if alert_planning_status == "success":
            finding = "Alert planning ran, but no digest-routed alerts were produced."
            next_action = (
                "Use candidates/research briefs for manual triage, or review alert "
                "thresholds and suppressions if you expect digest alerts."
            )
            evidence = _steps_evidence(steps, ("alert_planning", "digest"))
        else:
            finding = "No digest-routed alerts were available for the latest digest step."
            next_action = (
                "Use candidates/research briefs for manual triage, or fix alert "
                "planning before expecting digest automation."
            )
            evidence = _step_evidence("digest", digest_step)
        rows.append(
            _readiness_row(
                "Alerting",
                "optional",
                finding,
                next_action,
                evidence,
            )
        )
    elif digest_category == "expected_gate":
        rows.append(
            _readiness_row(
                "Alerting",
                "optional",
                "Alerting was not needed for the latest run.",
                "No action required unless you want alert delivery to run.",
                _step_evidence("digest", digest_step),
            )
        )
    else:
        rows.append(
            _readiness_row(
                "Alerting",
                "attention",
                "Alert digest did not complete.",
                "Check alert policy and upstream candidate-card availability.",
                _step_evidence("digest", digest_step),
            )
        )

    validation_step = steps.get("validation_update", {})
    if str(validation_step.get("status") or "") == "success":
        rows.append(
            _readiness_row(
                "Outcome validation",
                "ready",
                "Outcome validation updated in the latest run.",
                "Use validation labels to tighten future scoring thresholds.",
                _step_evidence("validation_update", validation_step),
            )
        )
    else:
        rows.append(
            _readiness_row(
                "Outcome validation",
                "optional",
                (
                    "Outcome validation needs a future outcome cutoff, so it is expected "
                    "to skip during live triage."
                ),
                "Run validation later with an outcome timestamp after the review window closes.",
                _step_evidence("validation_update", validation_step),
            )
        )

    orders = coverage.get("Order submission", {})
    order_mode = str(orders.get("mode") or "unknown")
    rows.append(
        _readiness_row(
            "Order safety",
            "safe" if order_mode == "disabled" else "blocked",
            "Real order submission is disabled; the dashboard can preview orders only."
            if order_mode == "disabled"
            else "Order submission configuration is not in the expected disabled state.",
            "Keep the kill switch off until an explicit trading-safety review is complete.",
            _coverage_evidence(orders),
        )
    )
    return rows


def _candidate_row(row: Any) -> dict[str, object]:
    values = dict(row)
    for key in (
        "as_of",
        "created_at",
        "candidate_packet_available_at",
        "candidate_packet_created_at",
        "decision_card_available_at",
        "next_review_at",
    ):
        if key in values and values[key] is not None:
            values[key] = _as_utc_datetime(values[key])
    signal_payload = values.pop("signal_payload", None)
    candidate_packet_payload = values.pop("candidate_packet_payload", None)
    decision_card_payload = values.pop("decision_card_payload", None)
    candidate_payload = (
        signal_payload.get("candidate", {}) if isinstance(signal_payload, dict) else {}
    )
    candidate_metadata = candidate_payload.get("metadata", {})
    if not isinstance(candidate_metadata, dict):
        candidate_metadata = {}
    portfolio_impact = candidate_metadata.get("portfolio_impact", {})
    if not isinstance(portfolio_impact, dict):
        portfolio_impact = {}

    values["setup_type"] = candidate_metadata.get("setup_type")
    values["portfolio_hard_blocks"] = portfolio_impact.get("hard_blocks", [])
    values["entry_zone"] = candidate_payload.get("entry_zone")
    values["invalidation_price"] = candidate_payload.get("invalidation_price")
    values["material_event_count"] = candidate_metadata.get("material_event_count", 0)
    values["top_event_type"] = candidate_metadata.get("top_event_type")
    values["top_event_title"] = candidate_metadata.get("top_event_title")
    values["top_event_source"] = candidate_metadata.get("top_event_source")
    values["top_event_source_url"] = candidate_metadata.get("top_event_source_url")
    values["top_event_source_quality"] = candidate_metadata.get("top_event_source_quality")
    values["top_event_materiality"] = candidate_metadata.get("top_event_materiality")
    values["has_event_conflict"] = candidate_metadata.get("has_event_conflict", False)
    values["event_conflicts"] = candidate_metadata.get("event_conflicts", [])
    values["local_narrative_score"] = candidate_metadata.get("local_narrative_score", 0.0)
    values["local_narrative_bonus"] = candidate_metadata.get("local_narrative_bonus", 0.0)
    values["novelty_score"] = candidate_metadata.get("novelty_score", 0.0)
    values["sentiment_score"] = candidate_metadata.get("sentiment_score", 0.0)
    values["source_quality_score"] = candidate_metadata.get("source_quality_score", 0.0)
    values["theme_match_score"] = candidate_metadata.get("theme_match_score", 0.0)
    values["theme_hits"] = candidate_metadata.get("theme_hits", [])
    values["selected_snippet_ids"] = candidate_metadata.get("selected_snippet_ids", [])
    values["selected_snippet_count"] = candidate_metadata.get("selected_snippet_count", 0)
    values["text_feature_version"] = candidate_metadata.get("text_feature_version")
    values["options_flow_score"] = candidate_metadata.get("options_flow_score", 0.0)
    values["options_risk_score"] = candidate_metadata.get("options_risk_score", 0.0)
    values["call_put_ratio"] = candidate_metadata.get("call_put_ratio", 0.0)
    values["iv_percentile"] = candidate_metadata.get("iv_percentile", 0.0)
    values["sector_rotation_score"] = candidate_metadata.get("sector_rotation_score", 0.0)
    values["theme_velocity_score"] = candidate_metadata.get("theme_velocity_score", 0.0)
    values["peer_readthrough_score"] = candidate_metadata.get("peer_readthrough_score", 0.0)
    values["candidate_theme"] = candidate_metadata.get("candidate_theme")
    values["theme_feature_version"] = candidate_metadata.get("theme_feature_version")
    values["options_feature_version"] = candidate_metadata.get("options_feature_version")
    packet_payload = (
        candidate_packet_payload if isinstance(candidate_packet_payload, dict) else {}
    )
    card_payload = decision_card_payload if isinstance(decision_card_payload, dict) else {}
    values["supporting_evidence_count"] = len(
        packet_payload.get("supporting_evidence", [])
    )
    values["disconfirming_evidence_count"] = len(
        packet_payload.get("disconfirming_evidence", [])
    )
    values["top_supporting_evidence"] = _top_evidence_summary(
        packet_payload.get("supporting_evidence", [])
    )
    values["top_disconfirming_evidence"] = _top_evidence_summary(
        packet_payload.get("disconfirming_evidence", [])
    )
    values["manual_review_disclaimer"] = card_payload.get("disclaimer")
    values["research_brief"] = _candidate_research_brief(values, packet_payload)
    return values


def _previous_candidate_state_row(
    conn: Any,
    current: Mapping[str, object],
    *,
    available_at: datetime | None,
) -> dict[str, object] | None:
    ticker = str(current.get("ticker") or "").strip().upper()
    current_as_of = _as_utc_datetime_or_none(current.get("as_of"))
    if not ticker or current_as_of is None:
        return None
    filters = [
        candidate_states.c.ticker == ticker,
        candidate_states.c.as_of < current_as_of,
    ]
    if available_at is not None:
        filters.append(candidate_states.c.created_at <= available_at)
    row = conn.execute(
        select(candidate_states)
        .where(*filters)
        .order_by(
            candidate_states.c.as_of.desc(),
            candidate_states.c.created_at.desc(),
            candidate_states.c.id.desc(),
        )
        .limit(1)
    ).first()
    return _row_dict(row._mapping) if row is not None else None


def _candidate_delta_row(
    current: Mapping[str, object],
    previous: Mapping[str, object] | None,
    *,
    score_move_threshold: float,
) -> dict[str, object] | None:
    ticker = str(current.get("ticker") or "").strip().upper()
    current_score = _finite_float(current.get("final_score"))
    current_state = str(current.get("state") or "unknown")
    current_blocks = _candidate_delta_blockers(current)
    if previous is None:
        return {
            "ticker": ticker,
            "change_type": "new_candidate",
            "severity": _candidate_delta_severity("new_candidate"),
            "is_new_candidate": True,
            "state_changed": False,
            "score_moved": False,
            "blocker_changed": bool(current_blocks),
            "previous_state": None,
            "current_state": current_state,
            "previous_score": None,
            "current_score": current_score,
            "score_change": None,
            "blockers_added": current_blocks,
            "blockers_removed": [],
            "current_blockers": current_blocks,
            "as_of": current.get("as_of"),
            "action": "Open the research brief and verify source freshness before escalation.",
        }

    previous_state = str(previous.get("state") or "unknown")
    previous_score = _finite_float(previous.get("final_score"))
    score_change = current_score - previous_score
    previous_blocks = _candidate_delta_blockers(previous)
    blockers_added = [block for block in current_blocks if block not in previous_blocks]
    blockers_removed = [block for block in previous_blocks if block not in current_blocks]
    state_changed = current_state != previous_state
    blocker_changed = bool(blockers_added or blockers_removed)
    score_moved = abs(score_change) >= float(score_move_threshold)
    if not (state_changed or blocker_changed or score_moved):
        return None
    change_type = _candidate_delta_change_type(
        state_changed=state_changed,
        blocker_changed=blocker_changed,
        score_moved=score_moved,
    )
    return {
        "ticker": ticker,
        "change_type": change_type,
        "severity": _candidate_delta_severity(change_type),
        "is_new_candidate": False,
        "state_changed": state_changed,
        "score_moved": score_moved,
        "blocker_changed": blocker_changed,
        "previous_state": previous_state,
        "current_state": current_state,
        "previous_score": previous_score,
        "current_score": current_score,
        "score_change": round(score_change, 2),
        "blockers_added": blockers_added,
        "blockers_removed": blockers_removed,
        "current_blockers": current_blocks,
        "as_of": current.get("as_of"),
        "action": _candidate_delta_action(
            change_type,
            blockers_added=blockers_added,
            blockers_removed=blockers_removed,
            current_state=current_state,
        ),
    }


def _candidate_delta_blockers(row: Mapping[str, object]) -> list[str]:
    blockers: list[str] = []
    for key in ("hard_blocks", "portfolio_hard_blocks"):
        for item in _sequence_value(row.get(key)):
            text = str(item or "").strip()
            if text and text not in blockers:
                blockers.append(text)
    return blockers


def _candidate_delta_change_type(
    *,
    state_changed: bool,
    blocker_changed: bool,
    score_moved: bool,
) -> str:
    if state_changed:
        return "state_changed"
    if blocker_changed:
        return "blocker_changed"
    if score_moved:
        return "score_moved"
    return "unchanged"


def _candidate_delta_severity(change_type: str) -> int:
    return {
        "new_candidate": 4,
        "state_changed": 3,
        "blocker_changed": 2,
        "score_moved": 1,
    }.get(change_type, 0)


def _candidate_delta_action(
    change_type: str,
    *,
    blockers_added: Sequence[str],
    blockers_removed: Sequence[str],
    current_state: str,
) -> str:
    if blockers_added:
        return "Review newly added blockers before escalating this candidate."
    if blockers_removed:
        return "Check whether cleared blockers make this candidate eligible for deeper review."
    if change_type == "state_changed" and current_state == ActionState.BLOCKED.value:
        return "Open blocker diagnostics before treating the high score as actionable."
    if change_type == "state_changed":
        return "Review the state transition and the evidence that caused it."
    if change_type == "score_moved":
        return "Inspect the research brief and top evidence behind the score move."
    return "Review the candidate history before acting."


def _candidate_delta_counts(rows: Sequence[Mapping[str, object]]) -> dict[str, int]:
    return {
        "new_candidates": sum(1 for row in rows if bool(row.get("is_new_candidate"))),
        "state_changes": sum(1 for row in rows if bool(row.get("state_changed"))),
        "score_moves": sum(1 for row in rows if bool(row.get("score_moved"))),
        "blocker_changes": sum(1 for row in rows if bool(row.get("blocker_changed"))),
    }


def _alert_row(row: Any) -> dict[str, object]:
    values = _row_dict(dict(row))
    payload = values.get("payload")
    if not isinstance(payload, Mapping):
        payload = {}

    values["score_trigger"] = _first_present(
        payload.get("score_trigger"),
        payload.get("score"),
        payload.get("final_score"),
        payload.get("trigger_score"),
    )
    values["feedback"] = values.get("feedback_label")
    return values


def _alert_suppression_diagnostic_rows(
    engine: Engine,
    *,
    available_at: datetime | None,
    as_of_date: date | None,
    limit: int,
) -> list[dict[str, object]]:
    if available_at is None:
        return []
    filters = [alert_suppressions.c.available_at == available_at]
    if as_of_date is not None:
        start, end = _date_window(as_of_date)
        filters.extend(
            [
                alert_suppressions.c.as_of >= start,
                alert_suppressions.c.as_of < end,
            ]
        )
    stmt = (
        select(
            alert_suppressions.c.ticker,
            alert_suppressions.c.reason,
            alert_suppressions.c.route,
            alert_suppressions.c.trigger_kind,
            alert_suppressions.c.trigger_fingerprint,
            alert_suppressions.c.candidate_state_id,
            alert_suppressions.c.decision_card_id,
            alert_suppressions.c.available_at,
            alert_suppressions.c.created_at,
        )
        .where(*filters)
        .order_by(
            alert_suppressions.c.reason,
            alert_suppressions.c.ticker,
            alert_suppressions.c.created_at.desc(),
            alert_suppressions.c.id.desc(),
        )
        .limit(_positive_limit(limit))
    )
    rows: list[dict[str, object]] = []
    with engine.connect() as conn:
        for row in conn.execute(stmt):
            values = _row_dict(row._mapping)
            reason = str(values.get("reason") or "unknown")
            rows.append(
                {
                    "ticker": values.get("ticker"),
                    "reason": reason,
                    "route": values.get("route"),
                    "trigger": values.get("trigger_kind"),
                    "meaning": ALERT_SUPPRESSION_EXPLANATIONS.get(
                        reason,
                        "Alert planner suppressed this candidate.",
                    ),
                    "next_action": ALERT_SUPPRESSION_ACTIONS.get(
                        reason,
                        "Review the candidate and alert policy before changing thresholds.",
                    ),
                    "candidate_state_id": values.get("candidate_state_id"),
                    "decision_card_id": values.get("decision_card_id") or "n/a",
                    "available_at": values.get("available_at"),
                }
            )
    return rows


def _ipo_s1_row(row: Any) -> dict[str, object] | None:
    values = _row_dict(dict(row))
    payload = values.get("payload")
    if not isinstance(payload, Mapping):
        return None
    analysis = payload.get("ipo_analysis")
    if not isinstance(analysis, Mapping):
        return None
    return {
        "id": values.get("id"),
        "ticker": values.get("ticker"),
        "event_type": values.get("event_type"),
        "title": values.get("title"),
        "source": values.get("source"),
        "source_url": values.get("source_url"),
        "source_ts": values.get("source_ts"),
        "available_at": values.get("available_at"),
        "materiality": values.get("materiality"),
        "form_type": payload.get("form_type"),
        "filing_date": payload.get("filing_date"),
        "accession_number": payload.get("accession_number"),
        "document_url": payload.get("document_url") or values.get("source_url"),
        "document_text_hash": payload.get("document_text_hash"),
        "summary": payload.get("summary"),
        "proposed_ticker": analysis.get("proposed_ticker"),
        "exchange": analysis.get("exchange"),
        "shares_offered": analysis.get("shares_offered"),
        "price_range_low": analysis.get("price_range_low"),
        "price_range_high": analysis.get("price_range_high"),
        "price_range_midpoint": analysis.get("price_range_midpoint"),
        "estimated_gross_proceeds": analysis.get("estimated_gross_proceeds"),
        "underwriters": _json_safe(analysis.get("underwriters", [])),
        "use_of_proceeds_summary": analysis.get("use_of_proceeds_summary"),
        "risk_flags": _json_safe(analysis.get("risk_flags", [])),
        "sections_found": _json_safe(analysis.get("sections_found", [])),
        "analysis": _json_safe(analysis),
    }


def _ranked_alert_feedback(cutoff: datetime | None) -> Any:
    filters = [user_feedback.c.artifact_type == "alert"]
    if cutoff is not None:
        filters.append(user_feedback.c.created_at <= cutoff)
    return (
        select(
            user_feedback.c.id.label("feedback_id"),
            user_feedback.c.artifact_id.label("artifact_id"),
            user_feedback.c.label.label("feedback_label"),
            user_feedback.c.notes.label("feedback_notes"),
            user_feedback.c.source.label("feedback_source"),
            user_feedback.c.created_at.label("feedback_created_at"),
            func.row_number()
            .over(
                partition_by=(
                    user_feedback.c.artifact_type,
                    user_feedback.c.artifact_id,
                ),
                order_by=(user_feedback.c.created_at.desc(), user_feedback.c.id.desc()),
            )
            .label("feedback_rank"),
        )
        .where(*filters)
        .subquery()
    )


def _latest_state_row(
    conn: Any,
    ticker: str,
    *,
    available_at: datetime | None = None,
) -> dict[str, object] | None:
    filters = [candidate_states.c.ticker == ticker]
    if available_at is not None:
        filters.append(candidate_states.c.created_at <= available_at)
        filters.append(candidate_states.c.as_of <= available_at)
    row = conn.execute(
        select(candidate_states)
        .where(*filters)
        .order_by(
            candidate_states.c.as_of.desc(),
            candidate_states.c.created_at.desc(),
            candidate_states.c.id.desc(),
        )
        .limit(1)
    ).first()
    return dict(row._mapping) if row is not None else None


def _signal_feature_row(
    conn: Any,
    state_row: Mapping[str, object],
) -> dict[str, object] | None:
    row = conn.execute(
        select(signal_features)
        .where(
            signal_features.c.ticker == state_row["ticker"],
            signal_features.c.as_of == state_row["as_of"],
            signal_features.c.feature_version == state_row["feature_version"],
        )
        .limit(1)
    ).first()
    return dict(row._mapping) if row is not None else None


def _latest_packet_row(
    conn: Any,
    ticker: str,
    *,
    candidate_state_id: str | None = None,
    available_at: datetime | None = None,
) -> dict[str, object] | None:
    filters = [candidate_packets.c.ticker == ticker]
    if candidate_state_id is not None:
        filters.append(candidate_packets.c.candidate_state_id == candidate_state_id)
    if available_at is not None:
        filters.append(candidate_packets.c.available_at <= available_at)
    row = conn.execute(
        select(candidate_packets)
        .where(*filters)
        .order_by(
            candidate_packets.c.available_at.desc(),
            candidate_packets.c.created_at.desc(),
            candidate_packets.c.id.desc(),
        )
        .limit(1)
    ).first()
    return dict(row._mapping) if row is not None else None


def _latest_card_row(
    conn: Any,
    ticker: str,
    *,
    packet_id: str | None = None,
    available_at: datetime | None = None,
) -> dict[str, object] | None:
    filters = [decision_cards.c.ticker == ticker]
    if packet_id is not None:
        filters.append(decision_cards.c.candidate_packet_id == packet_id)
    if available_at is not None:
        filters.append(decision_cards.c.available_at <= available_at)
    row = conn.execute(
        select(decision_cards)
        .where(*filters)
        .order_by(
            decision_cards.c.available_at.desc(),
            decision_cards.c.created_at.desc(),
            decision_cards.c.id.desc(),
        )
        .limit(1)
    ).first()
    return dict(row._mapping) if row is not None else None


def _candidate_detail_mapping(
    state_row: Mapping[str, object],
    signal_row: Mapping[str, object] | None,
    packet_row: Mapping[str, object] | None,
    card_row: Mapping[str, object] | None,
) -> dict[str, object]:
    values = dict(state_row)
    values["signal_payload"] = signal_row.get("payload") if signal_row is not None else None
    values["candidate_packet_id"] = packet_row.get("id") if packet_row is not None else None
    values["candidate_packet_available_at"] = (
        packet_row.get("available_at") if packet_row is not None else None
    )
    values["candidate_packet_payload"] = (
        packet_row.get("payload") if packet_row is not None else None
    )
    values["decision_card_id"] = card_row.get("id") if card_row is not None else None
    values["decision_card_available_at"] = (
        card_row.get("available_at") if card_row is not None else None
    )
    values["next_review_at"] = card_row.get("next_review_at") if card_row is not None else None
    values["decision_card_payload"] = card_row.get("payload") if card_row is not None else None
    return values


def _top_evidence_summary(value: object) -> dict[str, object] | None:
    if not isinstance(value, list) or not value or not isinstance(value[0], dict):
        return None
    item = value[0]
    return {
        "kind": item.get("kind"),
        "title": item.get("title"),
        "source_id": item.get("source_id"),
        "source_url": item.get("source_url"),
        "computed_feature_id": item.get("computed_feature_id"),
        "strength": item.get("strength"),
    }


def _candidate_research_brief(
    candidate: Mapping[str, object],
    packet_payload: Mapping[str, object],
) -> dict[str, object]:
    state = str(candidate.get("state") or "")
    support = _mapping_value(candidate, "top_supporting_evidence")
    risk = _mapping_value(candidate, "top_disconfirming_evidence")
    top_event = _first_present(candidate.get("top_event_title"), support.get("title"))
    source = _first_present(
        candidate.get("top_event_source"),
        support.get("source_id"),
        support.get("kind"),
    )
    source_url = _first_present(candidate.get("top_event_source_url"), support.get("source_url"))
    risk_or_gap = _first_present(
        risk.get("title"),
        _first_item(candidate.get("portfolio_hard_blocks")),
        "No disconfirming evidence captured yet.",
    )
    decision_card_id = str(candidate.get("decision_card_id") or "").strip()
    return {
        "focus": _research_focus(state),
        "why_now": _research_why_now(candidate, top_event=top_event),
        "top_catalyst": top_event,
        "source": source,
        "source_url": source_url,
        "supporting_evidence": support.get("title"),
        "risk_or_gap": risk_or_gap,
        "next_step": _research_next_step(state, has_decision_card=bool(decision_card_id)),
        "decision_card_status": (
            f"available: {decision_card_id}"
            if decision_card_id
            else "not generated; candidate is not in manual-buy-review state"
        ),
        "audit": _research_brief_audit(packet_payload),
    }


def _research_focus(state: str) -> str:
    normalized = state.strip().lower()
    if normalized == "eligibleformanualbuyreview":
        return "Manual buy review"
    if normalized == "warning":
        return "Research now"
    if normalized == "addtowatchlist":
        return "Watchlist"
    if normalized == "blocked":
        return "Blocked"
    if normalized in {"thesisweakening", "exitinvalidatereview"}:
        return "Risk review"
    return "Monitor"


def _actionability_bucket(state: str) -> str:
    normalized = state.strip()
    if normalized == ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW.value:
        return "Buy-review ready"
    if normalized == ActionState.WARNING.value:
        return "Research now"
    if normalized == ActionState.ADD_TO_WATCHLIST.value:
        return "Watchlist"
    if normalized in {
        ActionState.BLOCKED.value,
        ActionState.THESIS_WEAKENING.value,
        ActionState.EXIT_INVALIDATE_REVIEW.value,
    }:
        return "Blocked or risk review"
    return "Monitor"


def _candidate_decision_label(
    candidate: Mapping[str, object],
    readiness: Mapping[str, object],
) -> dict[str, object]:
    decision_mode = str(readiness.get("decision_mode") or "unknown")
    state = str(candidate.get("state") or "")
    has_card = bool(str(candidate.get("decision_card_id") or "").strip())
    readiness_gate = _readiness_gate_next_action(readiness)
    if decision_mode == "manual_buy_review":
        if state == ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW.value and has_card:
            return {
                "decision_status": "manual_buy_review",
                "decision_next_step": "Review card, exposure, and hard blocks.",
                "decision_readiness_gate": None,
            }
        return {
            "decision_status": "research_only",
            "decision_next_step": "Not in manual buy-review state.",
            "decision_readiness_gate": None,
        }
    if decision_mode == "research_only":
        if state == ActionState.BLOCKED.value:
            return {
                "decision_status": "blocked",
                "decision_next_step": "Clear hard blocks before escalation.",
                "decision_readiness_gate": readiness_gate,
            }
        if state == ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW.value and not has_card:
            return {
                "decision_status": "missing_card",
                "decision_next_step": "Build a Decision Card first.",
                "decision_readiness_gate": readiness_gate,
            }
        return {
            "decision_status": "research_only",
            "decision_next_step": _candidate_specific_next_step(
                candidate,
                has_decision_card=has_card,
            ),
            "decision_readiness_gate": readiness_gate,
        }
    if decision_mode == "monitor":
        return {
            "decision_status": "monitor",
            "decision_next_step": "Wait for stronger evidence or a fresher catalyst.",
            "decision_readiness_gate": readiness_gate,
        }
    return {
        "decision_status": "not_ready",
        "decision_next_step": str(
            readiness.get("next_action") or "Complete live readiness first."
        ),
        "decision_readiness_gate": readiness_gate,
    }


def _readiness_gate_next_action(readiness: Mapping[str, object]) -> str | None:
    next_action = str(readiness.get("next_action") or "").strip()
    if not next_action:
        return None
    decision_mode = str(readiness.get("decision_mode") or "unknown")
    if decision_mode == "manual_buy_review":
        return None
    return next_action


def _candidate_specific_next_step(
    candidate: Mapping[str, object],
    *,
    has_decision_card: bool,
) -> str:
    brief = _mapping_value(candidate, "research_brief")
    brief_next_step = str(brief.get("next_step") or "").strip()
    if brief_next_step:
        return brief_next_step
    return _research_next_step(
        str(candidate.get("state") or ""),
        has_decision_card=has_decision_card,
    )


def _research_shortlist_sort_key(row: Mapping[str, object]) -> tuple[int, float, str]:
    priority = _research_shortlist_priority(row)
    return (
        {
            "manual_review": 0,
            "research_now": 1,
            "missing_card": 2,
            "watchlist": 3,
            "blocked": 4,
            "monitor": 5,
        }.get(priority, 6),
        -_finite_float(row.get("final_score")),
        str(row.get("ticker") or ""),
    )


def _research_shortlist_priority(row: Mapping[str, object]) -> str:
    decision_status = str(row.get("decision_status") or "")
    state = str(row.get("state") or "")
    if decision_status == "manual_buy_review":
        return "manual_review"
    if state == ActionState.WARNING.value:
        return "research_now"
    if decision_status == "missing_card":
        return "missing_card"
    if state == ActionState.ADD_TO_WATCHLIST.value:
        return "watchlist"
    if decision_status == "blocked" or state in {
        ActionState.BLOCKED.value,
        ActionState.THESIS_WEAKENING.value,
        ActionState.EXIT_INVALIDATE_REVIEW.value,
    }:
        return "blocked"
    return "monitor"


def _research_shortlist_row(row: Mapping[str, object]) -> dict[str, object]:
    brief = _mapping_value(row, "research_brief")
    support = _mapping_value(row, "top_supporting_evidence")
    risk = _mapping_value(row, "top_disconfirming_evidence")
    priority = _research_shortlist_priority(row)
    return {
        "priority": priority,
        "ticker": row.get("ticker"),
        "decision_status": row.get("decision_status") or "unknown",
        "state": row.get("state"),
        "score": _finite_float(row.get("final_score")),
        "setup": row.get("setup_type") or "n/a",
        "why_now": brief.get("why_now") or row.get("top_event_title"),
        "top_catalyst": brief.get("top_catalyst") or support.get("title"),
        "evidence": brief.get("supporting_evidence") or support.get("title"),
        "risk_or_gap": brief.get("risk_or_gap") or risk.get("title"),
        "next_step": row.get("decision_next_step")
        or brief.get("next_step")
        or _research_next_step(
            str(row.get("state") or ""),
            has_decision_card=bool(row.get("decision_card_id")),
        ),
        "readiness_gate": row.get("decision_readiness_gate"),
        "decision_card_id": row.get("decision_card_id") or "n/a",
        "source": brief.get("source") or support.get("source_id") or support.get("kind"),
        "schwab_last_price": row.get("schwab_last_price"),
        "schwab_day_change_percent": row.get("schwab_day_change_percent"),
        "schwab_relative_volume": row.get("schwab_relative_volume"),
        "schwab_price_trend_5d_percent": row.get("schwab_price_trend_5d_percent"),
        "schwab_option_call_put_ratio": row.get("schwab_option_call_put_ratio"),
        "schwab_context_status": row.get("schwab_context_status"),
        "schwab_market_as_of": row.get("schwab_market_as_of"),
        "audit": _mapping_value(brief, "audit"),
    }


def _actionability_status(
    buckets: Mapping[str, int],
    *,
    total: int,
) -> tuple[str, str, str]:
    if total == 0:
        return (
            "empty",
            "No candidates are currently queued.",
            "Run the radar after live inputs are configured.",
        )
    if buckets.get("Buy-review ready", 0):
        return (
            "ready",
            f"{buckets['Buy-review ready']} candidate(s) are ready for manual buy review.",
            "Review Decision Cards, hard blocks, exposure, and source freshness.",
        )
    if buckets.get("Research now", 0):
        return (
            "research",
            f"{buckets['Research now']} candidate(s) need research before buy review.",
            "Review the top risk/gap and confirm whether policy thresholds are too conservative.",
        )
    if buckets.get("Watchlist", 0):
        return (
            "watchlist",
            f"{buckets['Watchlist']} candidate(s) belong on the watchlist.",
            "Wait for stronger score, volume confirmation, or a fresher catalyst.",
        )
    return (
        "blocked",
        "No candidate is actionable yet.",
        "Use the top blocker list to decide whether inputs, thresholds, or candidates need work.",
    )


def _actionability_counts_by_bucket(value: object) -> dict[str, int]:
    rows = _sequence_value(value)
    counts: dict[str, int] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        bucket = str(row.get("bucket") or "").strip()
        if not bucket:
            continue
        counts[bucket] = int(_finite_float(row.get("count")))
    return counts


def _buy_review_ready_with_card_count(
    candidate_rows: Sequence[Mapping[str, object]],
) -> int:
    count = 0
    for candidate in candidate_rows:
        if not isinstance(candidate, Mapping):
            continue
        if (
            str(candidate.get("state") or "")
            != ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW.value
        ):
            continue
        if str(candidate.get("decision_card_id") or "").strip():
            count += 1
    return count


def _first_blocker_action(
    blockers: Sequence[Mapping[str, object]],
    *,
    default: str,
) -> str:
    for blocker in blockers:
        action = blocker.get("next_action")
        if action not in (None, ""):
            return str(action)
    return default


def _research_why_now(candidate: Mapping[str, object], *, top_event: object) -> str:
    if top_event not in (None, ""):
        return str(top_event)
    setup = candidate.get("setup_type")
    score = candidate.get("final_score")
    if setup not in (None, "") and score not in (None, ""):
        return f"{setup} setup with score {score}"
    if score not in (None, ""):
        return f"Candidate score {score}"
    return "Candidate is in the current radar queue."


def _research_next_step(state: str, *, has_decision_card: bool) -> str:
    normalized = state.strip().lower()
    if has_decision_card:
        return "Review the Decision Card before any trade action."
    if normalized == "warning":
        return "Review catalyst, evidence, and missing trade-plan items before escalation."
    if normalized == "addtowatchlist":
        return "Track for stronger score, volume confirmation, or a fresh catalyst."
    if normalized == "blocked":
        return "Do not escalate until hard blocks or policy gaps clear."
    if normalized in {"thesisweakening", "exitinvalidatereview"}:
        return "Check position risk and thesis invalidation evidence."
    return "Continue monitoring; no buy workflow has been opened."


def _first_item(value: object) -> object:
    if isinstance(value, list | tuple) and value:
        return value[0]
    return None


def _research_brief_audit(packet_payload: Mapping[str, object]) -> dict[str, object]:
    audit = _mapping_value(packet_payload, "audit")
    provider_policy = _mapping_value(audit, "provider_license_policy")
    if not provider_policy:
        return {}
    return {"provider_license_policy": provider_policy}


def _as_utc_datetime(value: object) -> object:
    if not isinstance(value, datetime):
        return value
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _as_utc_datetime_or_none(value: object) -> datetime | None:
    converted = _as_utc_datetime(value)
    return converted if isinstance(converted, datetime) else None


def _detail_cutoff(
    requested_cutoff: datetime | None,
    *,
    latest_state: Mapping[str, object],
    packet_row: Mapping[str, object] | None,
    card_row: Mapping[str, object] | None,
) -> datetime | None:
    if requested_cutoff is not None:
        return requested_cutoff
    candidates: list[datetime] = []
    for row, key in (
        (card_row, "available_at"),
        (packet_row, "available_at"),
        (latest_state, "created_at"),
        (latest_state, "as_of"),
    ):
        if row is None:
            continue
        cutoff = _as_utc_datetime_or_none(row.get(key))
        if cutoff is not None:
            candidates.append(cutoff)
    return max(candidates) if candidates else None


def _row_dict(row: Mapping[str, object] | None) -> dict[str, object]:
    if row is None:
        return {}
    return {str(key): _json_safe(value) for key, value in row.items()}


def _dataclass_dict(value: object) -> dict[str, object]:
    if not is_dataclass(value) or isinstance(value, type):
        return _row_dict(value) if isinstance(value, Mapping) else {}
    return {
        field.name: _json_safe(getattr(value, field.name))
        for field in fields(value)
    }


def _json_safe(value: object) -> object:
    if isinstance(value, datetime):
        return _as_utc_datetime(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, tuple | list):
        return [_json_safe(item) for item in value]
    return value


def _mapping_value(source: object, key: str) -> dict[str, object]:
    if not isinstance(source, Mapping):
        return {}
    value = source.get(key)
    return _row_dict(value) if isinstance(value, Mapping) else {}


def _readiness_row(
    area: str,
    status: str,
    finding: str,
    next_action: str,
    evidence: str,
) -> dict[str, object]:
    return {
        "area": area,
        "status": status,
        "finding": finding,
        "next_action": next_action,
        "evidence": evidence,
    }


def _operator_work_queue_row(
    *,
    sequence: int,
    severity: int,
    priority: str,
    area: str,
    item: str,
    status: str,
    next_action: str,
    evidence: str,
    source: str,
    ticker: str | None = None,
) -> dict[str, object]:
    row: dict[str, object] = {
        "sequence": sequence,
        "severity": severity,
        "priority": priority,
        "area": area,
        "item": item,
        "status": status,
        "next_action": next_action,
        "evidence": evidence,
        "source": source,
    }
    if ticker:
        row["ticker"] = ticker
    return row


def _activation_blocker_detail(rows: Sequence[Mapping[str, object]]) -> str:
    if not rows:
        return "No blocking readiness rows."
    labels = [str(row.get("area") or "Unknown") for row in rows[:3]]
    suffix = f" plus {len(rows) - 3} more" if len(rows) > 3 else ""
    return f"{', '.join(labels)} need attention{suffix}."


def _activation_next_action(rows: Sequence[Mapping[str, object]]) -> str:
    if not rows:
        return "No operator action required."
    return str(rows[0].get("next_action") or "Review the readiness checklist.")


def _activation_missing_env(config: AppConfig) -> list[str]:
    return [*_market_activation_missing_env(config), *_event_activation_missing_env(config)]


def _market_activation_missing_env(config: AppConfig) -> list[str]:
    items: list[str] = []
    market_provider = _provider_name(config.daily_market_provider, default="csv")
    if market_provider != "polygon":
        items.append("CATALYST_DAILY_MARKET_PROVIDER=polygon")
        items.append("CATALYST_DAILY_PROVIDER=polygon")
    if not config.polygon_api_key:
        items.append("CATALYST_POLYGON_API_KEY")
    return items


def _event_activation_missing_env(config: AppConfig) -> list[str]:
    items: list[str] = []
    event_provider = _provider_name(config.daily_event_provider, default="news_fixture")
    if event_provider not in {"sec", "sec_submissions"}:
        items.append("CATALYST_DAILY_EVENT_PROVIDER=sec")
    if not config.sec_enable_live:
        items.append("CATALYST_SEC_ENABLE_LIVE=1")
    if not config.sec_user_agent:
        items.append("CATALYST_SEC_USER_AGENT")
    return items


def _llm_missing_env(config: AppConfig) -> list[str]:
    provider = _provider_name(config.llm_provider, default="none")
    if not config.enable_premium_llm or provider in {"none", "off", "disabled"}:
        return []
    items: list[str] = []
    if provider != "openai":
        items.append("CATALYST_LLM_PROVIDER=openai")
    if not config.openai_api_key:
        items.append("OPENAI_API_KEY")
    if not config.llm_skeptic_model:
        items.append("CATALYST_LLM_SKEPTIC_MODEL")
    if not _llm_pricing_configured(config):
        items.append(
            "CATALYST_LLM_INPUT_COST_PER_1M / "
            "CATALYST_LLM_CACHED_INPUT_COST_PER_1M / "
            "CATALYST_LLM_OUTPUT_COST_PER_1M / "
            "CATALYST_LLM_PRICING_UPDATED_AT"
        )
    if config.llm_daily_budget_usd <= 0 or config.llm_monthly_budget_usd <= 0:
        items.append("CATALYST_LLM_DAILY_BUDGET_USD / CATALYST_LLM_MONTHLY_BUDGET_USD")
    return items


def _llm_activation_missing_env(config: AppConfig) -> list[str]:
    provider = _provider_name(config.llm_provider, default="none")
    items: list[str] = []
    if not config.enable_premium_llm:
        items.append("CATALYST_ENABLE_PREMIUM_LLM=1")
    if provider != "openai":
        items.append("CATALYST_LLM_PROVIDER=openai")
    items.extend(_llm_missing_env(config))
    if provider in {"none", "off", "disabled"}:
        if not config.openai_api_key:
            items.append("OPENAI_API_KEY")
        if not config.llm_skeptic_model:
            items.append("CATALYST_LLM_SKEPTIC_MODEL")
        if not _llm_pricing_configured(config):
            items.append(
                "CATALYST_LLM_INPUT_COST_PER_1M / "
                "CATALYST_LLM_CACHED_INPUT_COST_PER_1M / "
                "CATALYST_LLM_OUTPUT_COST_PER_1M / "
                "CATALYST_LLM_PRICING_UPDATED_AT"
            )
        if config.llm_daily_budget_usd <= 0 or config.llm_monthly_budget_usd <= 0:
            items.append(
                "CATALYST_LLM_DAILY_BUDGET_USD / CATALYST_LLM_MONTHLY_BUDGET_USD"
            )
    return list(dict.fromkeys(items))


def _llm_pricing_configured(config: AppConfig) -> bool:
    return (
        config.llm_input_cost_per_1m is not None
        and config.llm_cached_input_cost_per_1m is not None
        and config.llm_output_cost_per_1m is not None
        and bool(config.llm_pricing_updated_at)
    )


def _activation_task_row(
    area: str,
    coverage: Mapping[str, object],
    *,
    missing: Sequence[str],
    ready_modes: set[str],
    safe_next_action: str,
    optional: bool = False,
) -> dict[str, object]:
    mode = str(coverage.get("mode") or "unknown")
    provider = str(coverage.get("provider") or "unknown")
    missing_text = ", ".join(dict.fromkeys(missing))
    if missing_text:
        status = "optional_setup" if optional else "blocked"
    elif mode in ready_modes:
        status = "ready" if not optional or mode != "stale_read_only_connected" else "attention"
    elif optional:
        status = "optional"
    else:
        status = "attention"
    return {
        "area": area,
        "status": status,
        "current_state": f"{provider}/{mode}",
        "missing_env": missing_text,
        "safe_next_action": safe_next_action,
    }


def _live_data_env_template(config: AppConfig) -> list[dict[str, object]]:
    return [
        _activation_env_row(
            "CATALYST_DAILY_MARKET_PROVIDER",
            "polygon",
            configured=_provider_name(config.daily_market_provider, default="csv")
            == "polygon",
            current=_provider_name(config.daily_market_provider, default="csv"),
        ),
        _activation_env_row(
            "CATALYST_DAILY_PROVIDER",
            "polygon",
            configured=_provider_name(config.daily_market_provider, default="csv")
            == "polygon",
            current="aligned with CATALYST_DAILY_MARKET_PROVIDER",
        ),
        _activation_env_row(
            "CATALYST_POLYGON_API_KEY",
            "<your Polygon API key>",
            configured=bool(config.polygon_api_key),
            secret=True,
        ),
        _activation_env_row(
            "CATALYST_POLYGON_TICKERS_MAX_PAGES",
            str(max(1, int(config.polygon_tickers_max_pages))),
            configured=True,
            current=str(max(1, int(config.polygon_tickers_max_pages))),
        ),
        _activation_env_row(
            "CATALYST_DAILY_EVENT_PROVIDER",
            "sec",
            configured=_provider_name(config.daily_event_provider, default="news_fixture")
            in {"sec", "sec_submissions"},
            current=_provider_name(config.daily_event_provider, default="news_fixture"),
        ),
        _activation_env_row(
            "CATALYST_SEC_ENABLE_LIVE",
            "1",
            configured=bool(config.sec_enable_live),
            current="1" if config.sec_enable_live else "0",
        ),
        _activation_env_row(
            "CATALYST_SEC_USER_AGENT",
            "MarketRadar/0.1 your-email@example.com",
            configured=bool(config.sec_user_agent),
            secret=True,
        ),
        _activation_env_row(
            "CATALYST_SEC_DAILY_MAX_TICKERS",
            str(max(1, int(config.sec_daily_max_tickers))),
            configured=True,
            current=str(max(1, int(config.sec_daily_max_tickers))),
        ),
        _activation_env_row(
            "CATALYST_RADAR_RUN_MIN_INTERVAL_SECONDS",
            str(max(1, int(config.radar_run_min_interval_seconds))),
            configured=True,
            current=str(max(1, int(config.radar_run_min_interval_seconds))),
        ),
        _activation_env_row(
            "CATALYST_ENABLE_PREMIUM_LLM",
            "1",
            configured=bool(config.enable_premium_llm),
            current="1" if config.enable_premium_llm else "0",
        ),
        _activation_env_row(
            "CATALYST_LLM_PROVIDER",
            "openai",
            configured=_provider_name(config.llm_provider, default="none") == "openai",
            current=_provider_name(config.llm_provider, default="none"),
        ),
        _activation_env_row(
            "CATALYST_LLM_SKEPTIC_MODEL",
            "<OpenAI model for skeptic_review>",
            configured=bool(config.llm_skeptic_model),
            current=config.llm_skeptic_model or "missing",
        ),
        _activation_env_row(
            "OPENAI_API_KEY",
            "<your OpenAI API key>",
            configured=bool(config.openai_api_key),
            secret=True,
        ),
        _activation_env_row(
            "CATALYST_LLM_INPUT_COST_PER_1M",
            "<input dollars per 1M tokens>",
            configured=config.llm_input_cost_per_1m is not None,
            current=_activation_float_current(config.llm_input_cost_per_1m),
        ),
        _activation_env_row(
            "CATALYST_LLM_CACHED_INPUT_COST_PER_1M",
            "<cached-input dollars per 1M tokens>",
            configured=config.llm_cached_input_cost_per_1m is not None,
            current=_activation_float_current(config.llm_cached_input_cost_per_1m),
        ),
        _activation_env_row(
            "CATALYST_LLM_OUTPUT_COST_PER_1M",
            "<output dollars per 1M tokens>",
            configured=config.llm_output_cost_per_1m is not None,
            current=_activation_float_current(config.llm_output_cost_per_1m),
        ),
        _activation_env_row(
            "CATALYST_LLM_PRICING_UPDATED_AT",
            "YYYY-MM-DD",
            configured=bool(config.llm_pricing_updated_at),
            current=config.llm_pricing_updated_at or "missing",
        ),
        _activation_env_row(
            "CATALYST_LLM_DAILY_BUDGET_USD",
            "1.00",
            configured=config.llm_daily_budget_usd > 0,
            current=str(config.llm_daily_budget_usd),
        ),
        _activation_env_row(
            "CATALYST_LLM_MONTHLY_BUDGET_USD",
            "20.00",
            configured=config.llm_monthly_budget_usd > 0,
            current=str(config.llm_monthly_budget_usd),
        ),
        _activation_env_row(
            "CATALYST_LLM_TASK_DAILY_CAPS",
            "skeptic_review=3",
            configured="skeptic_review" in config.llm_task_daily_caps,
            current=str(dict(config.llm_task_daily_caps))
            if config.llm_task_daily_caps
            else "default caps",
        ),
    ]


def _live_data_minimum_env_lines(config: AppConfig) -> list[str]:
    required_names = {
        "CATALYST_DAILY_MARKET_PROVIDER",
        "CATALYST_DAILY_PROVIDER",
        "CATALYST_POLYGON_API_KEY",
        "CATALYST_POLYGON_TICKERS_MAX_PAGES",
        "CATALYST_DAILY_EVENT_PROVIDER",
        "CATALYST_SEC_ENABLE_LIVE",
        "CATALYST_SEC_USER_AGENT",
        "CATALYST_SEC_DAILY_MAX_TICKERS",
        "CATALYST_RADAR_RUN_MIN_INTERVAL_SECONDS",
    }
    return [
        f"{row['name']}={row['value_template']}"
        for row in _live_data_env_template(config)
        if str(row["name"]) in required_names
    ]


def _live_data_worker_env_lines() -> list[str]:
    return [
        "CATALYST_WORKER_INTERVAL_SECONDS=86400",
        "CATALYST_WORKER_LOCK_TTL_SECONDS=2700",
        "CATALYST_RUN_LLM=false",
        "CATALYST_LLM_DRY_RUN=true",
        "CATALYST_DRY_RUN_ALERTS=true",
    ]


def _live_data_worker_commands() -> list[dict[str, object]]:
    return [
        {
            "mode": "one-shot smoke",
            "when": "after .env.local is edited and services are restarted",
            "external_calls": "one capped radar cycle",
            "command": (
                "$env:CATALYST_WORKER_INTERVAL_SECONDS='0'; "
                "python -m apps.worker.main"
            ),
        },
        {
            "mode": "daily worker loop",
            "when": "after the one-shot smoke succeeds",
            "external_calls": "one capped radar cycle per interval",
            "command": (
                "$env:CATALYST_WORKER_INTERVAL_SECONDS='86400'; "
                "python -m apps.worker.main"
            ),
        },
    ]


def _activation_float_current(value: float | None) -> str:
    return "missing" if value is None else str(value)


def _activation_env_row(
    name: str,
    value_template: str,
    *,
    configured: bool,
    current: str | None = None,
    secret: bool = False,
) -> dict[str, object]:
    return {
        "name": name,
        "value_template": value_template,
        "configured": configured,
        "current": "set" if secret and configured else ("missing" if secret else current),
        "secret": secret,
    }


def _live_data_safe_limits(config: AppConfig) -> list[dict[str, object]]:
    return [
        {
            "guardrail": "Polygon universe seed cap",
            "value": f"{max(1, int(config.polygon_tickers_max_pages))} page(s)",
            "reason": "Bounds ticker-reference requests before broad discovery.",
        },
        {
            "guardrail": "SEC daily ticker cap",
            "value": f"{max(1, int(config.sec_daily_max_tickers))} ticker(s)",
            "reason": "Bounds SEC submissions requests per radar run.",
        },
        {
            "guardrail": "Manual radar cooldown",
            "value": f"{max(1, int(config.radar_run_min_interval_seconds))} second(s)",
            "reason": "Prevents repeated dashboard/API runs from hammering providers.",
        },
        {
            "guardrail": "Schwab isolation",
            "value": "not called by radar runs",
            "reason": "Portfolio sync remains behind separate broker controls and rate guards.",
        },
        {
            "guardrail": "Daily LLM safety",
            "value": "real daily LLM disabled",
            "reason": "Use per-candidate review after live data quality is acceptable.",
        },
    ]


def _live_data_operator_steps(
    config: AppConfig,
    *,
    missing_env: Sequence[str],
) -> list[dict[str, object]]:
    env_status = "blocked" if missing_env else "ready"
    seed_pages = max(1, int(config.polygon_tickers_max_pages))
    return [
        {
            "step": 1,
            "status": env_status,
            "action": "Edit .env.local using the template below; do not paste keys into chat.",
            "external_calls": 0,
            "command": "notepad .env.local",
        },
        {
            "step": 2,
            "status": "manual",
            "action": "Restart the local API and dashboard so the new env is loaded.",
            "external_calls": 0,
            "command": "restart local Market Radar services",
        },
        {
            "step": 3,
            "status": "manual",
            "action": "Seed the active universe with the configured Polygon page cap.",
            "external_calls": seed_pages,
            "command": (
                "Invoke-RestMethod -Method Post "
                "-Uri https://127.0.0.1:8443/api/radar/universe/seed "
                "-SkipCertificateCheck -ContentType 'application/json' "
                f"-Body '{{\"provider\":\"polygon\",\"max_pages\":{seed_pages}}}'"
            ),
        },
        {
            "step": 4,
            "status": "safe_check",
            "action": "Inspect the radar call plan before running live ingestion.",
            "external_calls": 0,
            "command": (
                "Invoke-RestMethod -Method Post "
                "-Uri https://127.0.0.1:8443/api/radar/runs/call-plan "
                "-SkipCertificateCheck -ContentType 'application/json' -Body '{}'"
            ),
        },
        {
            "step": 5,
            "status": "manual",
            "action": "Run one capped radar cycle only after the call plan matches intent.",
            "external_calls": 1 + max(1, int(config.sec_daily_max_tickers)),
            "command": (
                "Invoke-RestMethod -Method Post "
                "-Uri https://127.0.0.1:8443/api/radar/runs "
                "-SkipCertificateCheck -ContentType 'application/json' -Body '{}'"
            ),
        },
        {
            "step": 6,
            "status": "safe_check",
            "action": "Review readiness and the research shortlist before any investment work.",
            "external_calls": 0,
            "command": (
                "Invoke-RestMethod "
                "-Uri https://127.0.0.1:8443/api/radar/readiness "
                "-SkipCertificateCheck"
            ),
        },
    ]


def _live_data_call_budget_if_activated(config: AppConfig) -> list[dict[str, object]]:
    return [
        {
            "operation": "read this activation contract",
            "max_external_calls": 0,
            "provider": "none",
        },
        {
            "operation": "seed universe once",
            "max_external_calls": max(1, int(config.polygon_tickers_max_pages)),
            "provider": "polygon",
        },
        {
            "operation": "run one radar cycle",
            "max_external_calls": 1 + max(1, int(config.sec_daily_max_tickers)),
            "provider": "polygon + sec",
        },
    ]


def _telemetry_tape_status(status_counts: Mapping[str, int]) -> str:
    if not status_counts:
        return "empty"
    if any(status_counts.get(status, 0) for status in ("failed", "rejected", "blocked")):
        return "attention"
    return "ready"


def _telemetry_artifact_label(event: Mapping[str, object]) -> str:
    artifact_type = str(event.get("artifact_type") or "").strip()
    artifact_id = str(event.get("artifact_id") or "").strip()
    if artifact_type and artifact_id:
        return f"{artifact_type}:{artifact_id[:24].rstrip('-:_')}"
    return artifact_type or artifact_id or "n/a"


def _telemetry_step_outcome_fields(
    event_type: str,
    event: Mapping[str, object],
    metadata: Mapping[str, object],
) -> dict[str, object]:
    if event_type.removeprefix("telemetry.") != "radar_run.step_finished":
        return {}
    raw_status = str(
        _first_present(
            event.get("status"),
            metadata.get("result_status"),
            "unknown",
        )
    )
    reason = _first_present(event.get("reason"), metadata.get("result_reason"))
    reason_text = str(reason) if reason not in (None, "") else None
    classification = classify_step_outcome(raw_status, reason_text)
    return {
        "status": classification.category,
        "step": metadata.get("step") or "n/a",
        "outcome": classification.label,
        "raw_status": raw_status,
        "blocks_reliance": "yes" if classification.blocks_reliance else "no",
    }


def _telemetry_event_summary(
    event_type: str,
    *,
    event: Mapping[str, object] | None = None,
    metadata: Mapping[str, object],
    after_payload: Mapping[str, object],
) -> str:
    short_event = event_type.removeprefix("telemetry.")
    if short_event == "radar_run.step_finished":
        event_mapping = event if isinstance(event, Mapping) else {}
        raw_status = str(
            _first_present(
                metadata.get("result_status"),
                event_mapping.get("status"),
                "unknown",
            )
        )
        reason_value = _first_present(
            metadata.get("result_reason"),
            event_mapping.get("reason"),
        )
        reason = str(reason_value) if reason_value not in (None, "") else "n/a"
        classification = classify_step_outcome(
            raw_status,
            None if reason == "n/a" else reason,
        )
        category = classification.category
        label = classification.label
        action = str(classification.operator_action or metadata.get("operator_action") or "")
        trigger_condition = str(
            classification.trigger_condition or metadata.get("trigger_condition") or ""
        )
        parts = [
            f"step={metadata.get('step') or 'n/a'}",
            f"outcome={label}",
            f"category={category}",
            f"raw_status={raw_status}",
            f"reason={reason}",
        ]
        if trigger_condition:
            parts.append(f"trigger={trigger_condition}")
        if action:
            parts.append(f"action={action}")
        return "; ".join(parts)
    if short_event == "radar_run.step_started":
        return (
            f"step={metadata.get('step') or 'n/a'}; "
            f"job_id={metadata.get('job_id') or 'n/a'}; "
            f"provider={metadata.get('provider') or 'default'}; "
            f"universe={metadata.get('universe') or 'default'}"
        )
    if short_event == "radar_run.completed":
        step_counts = _string_int_mapping(metadata.get("step_counts"))
        blocked_count = len(_sequence_value(metadata.get("blocked_steps")))
        expected_gate_count = len(_sequence_value(metadata.get("expected_gate_steps")))
        outcome_counts = _string_int_mapping(metadata.get("outcome_category_counts"))
        required_total = max(0, sum(step_counts.values()) - expected_gate_count)
        required_complete = min(
            outcome_counts.get("completed", step_counts.get("success", 0)),
            required_total,
        )
        raw_skipped_count = step_counts.get("skipped", 0)
        required_label = (
            f"required={required_complete}/{required_total}; "
            if required_total
            else ""
        )
        raw_skips_label = (
            f"; audit_raw_skips={raw_skipped_count}" if raw_skipped_count else ""
        )
        return (
            f"daily_status={metadata.get('daily_status') or 'unknown'}; "
            f"{required_label}"
            f"blocked={blocked_count}; expected_gates={expected_gate_count}"
            f"{raw_skips_label}"
        )
    if short_event in {
        "radar_run.rejected",
        "radar_run.lock_contention",
        "radar_run.rate_limited",
    }:
        return (
            f"provider={metadata.get('provider') or 'default'}; "
            f"universe={metadata.get('universe') or 'default'}"
        )
    if short_event == "universe_seed.completed":
        return (
            f"job_id={metadata.get('job_id') or after_payload.get('job_id') or 'n/a'}; "
            f"max_pages={metadata.get('max_pages') or after_payload.get('max_pages') or 'n/a'}; "
            f"normalized={after_payload.get('normalized_count') or 'n/a'}; "
            f"rejected={after_payload.get('rejected_count') or 0}"
        )
    if short_event.startswith("universe_seed."):
        return (
            f"provider={metadata.get('provider') or 'polygon'}; "
            f"max_pages={metadata.get('max_pages') or 'n/a'}"
        )
    return _count_map_label(_string_int_mapping(metadata.get("step_counts"))) or "n/a"


def _count_map_label(values: Mapping[str, int]) -> str:
    return ", ".join(f"{key}={value}" for key, value in sorted(values.items()))


def _boolish(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int | float):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _radar_run_path_summary(
    radar_run_summary: Mapping[str, object] | None,
) -> dict[str, int]:
    summary = radar_run_summary if isinstance(radar_run_summary, Mapping) else {}
    outcome_counts = _string_int_mapping(summary.get("outcome_category_counts"))
    step_count = int(_finite_float(summary.get("step_count")))
    expected_gate_count = int(
        _finite_float(summary.get("expected_gate_count"))
        or outcome_counts.get("expected_gate", 0)
    )
    required_total_value = summary.get("required_step_count")
    required_total = (
        int(_finite_float(required_total_value))
        if required_total_value is not None
        else max(0, step_count - expected_gate_count)
    )
    required_complete_value = summary.get("required_completed_count")
    required_complete = (
        int(_finite_float(required_complete_value))
        if required_complete_value is not None
        else min(outcome_counts.get("completed", 0), required_total)
    )
    blocking_value = summary.get("action_needed_count")
    blocking_count = (
        int(_finite_float(blocking_value))
        if blocking_value is not None
        else int(_finite_float(summary.get("blocking_step_count")))
    )
    return {
        "required_total": required_total,
        "required_complete": min(required_complete, required_total),
        "blocking_count": blocking_count,
        "expected_gate_count": expected_gate_count,
    }


def _readiness_radar_run_summary(
    radar_run_summary: Mapping[str, object] | None,
) -> dict[str, object]:
    summary = radar_run_summary if isinstance(radar_run_summary, Mapping) else {}
    return {
        "status": summary.get("status") or "unknown",
        "run_path_status": summary.get("run_path_status") or "unknown",
        "as_of": summary.get("as_of"),
        "decision_available_at": summary.get("decision_available_at"),
        "started_at": summary.get("started_at"),
        "finished_at": summary.get("finished_at"),
        "provider": summary.get("provider"),
        "universe": summary.get("universe"),
        "tickers": summary.get("tickers") or [],
        "required_step_count": int(_finite_float(summary.get("required_step_count"))),
        "required_completed_count": int(
            _finite_float(summary.get("required_completed_count"))
        ),
        "blocking_step_count": int(_finite_float(summary.get("blocking_step_count"))),
        "expected_gate_count": int(_finite_float(summary.get("expected_gate_count"))),
        "requested_count": int(_finite_float(summary.get("requested_count"))),
        "raw_count": int(_finite_float(summary.get("raw_count"))),
        "normalized_count": int(_finite_float(summary.get("normalized_count"))),
    }


def _readiness_candidate_label(row: Mapping[str, object]) -> dict[str, object]:
    brief = _mapping_value(row, "research_brief")
    support = _mapping_value(row, "top_supporting_evidence")
    risk = _mapping_value(row, "top_disconfirming_evidence")
    return {
        "ticker": row.get("ticker"),
        "decision_status": row.get("decision_status") or "unknown",
        "state": row.get("state"),
        "score": _finite_float(row.get("final_score")),
        "setup": row.get("setup_type") or "n/a",
        "top_catalyst": brief.get("top_catalyst")
        or row.get("top_event_title")
        or support.get("title"),
        "risk_or_gap": brief.get("risk_or_gap") or risk.get("title"),
        "decision_card_id": row.get("decision_card_id"),
        "next_step": row.get("decision_next_step") or brief.get("next_step"),
        "readiness_gate": row.get("decision_readiness_gate"),
        "schwab_last_price": row.get("schwab_last_price"),
        "schwab_day_change_percent": row.get("schwab_day_change_percent"),
        "schwab_relative_volume": row.get("schwab_relative_volume"),
        "schwab_context_status": row.get("schwab_context_status"),
        "audit": _mapping_value(brief, "audit"),
    }


def _latest_market_context_by_ticker(
    market_context: Sequence[Mapping[str, object]] | object,
) -> dict[str, dict[str, object]]:
    rows = [
        _row_dict(row)
        for row in _sequence_value(market_context)
        if isinstance(row, Mapping)
    ]
    rows = sorted(
        rows,
        key=lambda row: (
            str(row.get("ticker") or "").strip().upper(),
            _parse_utc_datetime(row.get("as_of")) or datetime.min.replace(tzinfo=UTC),
            _parse_utc_datetime(row.get("created_at"))
            or datetime.min.replace(tzinfo=UTC),
            str(row.get("id") or ""),
        ),
        reverse=True,
    )
    context_by_ticker: dict[str, dict[str, object]] = {}
    for row in rows:
        ticker = str(row.get("ticker") or "").strip().upper()
        if ticker and ticker not in context_by_ticker:
            context_by_ticker[ticker] = row
    return context_by_ticker


def _candidate_market_context_fields(context: Mapping[str, object]) -> dict[str, object]:
    return {
        "schwab_context_status": "available",
        "schwab_market_as_of": context.get("as_of"),
        "schwab_last_price": _optional_float(context.get("last_price")),
        "schwab_bid_price": _optional_float(context.get("bid_price")),
        "schwab_ask_price": _optional_float(context.get("ask_price")),
        "schwab_mark_price": _optional_float(context.get("mark_price")),
        "schwab_day_change_percent": _optional_float(context.get("day_change_percent")),
        "schwab_total_volume": _optional_float(context.get("total_volume")),
        "schwab_relative_volume": _optional_float(context.get("relative_volume")),
        "schwab_price_trend_5d_percent": _optional_float(
            context.get("price_trend_5d_percent")
        ),
        "schwab_option_call_put_ratio": _optional_float(
            context.get("option_call_put_ratio")
        ),
        "schwab_option_iv_percentile": _optional_float(
            context.get("option_iv_percentile")
        ),
    }


def _empty_candidate_market_context() -> dict[str, object]:
    return {
        "schwab_context_status": "missing",
        "schwab_market_as_of": None,
        "schwab_last_price": None,
        "schwab_bid_price": None,
        "schwab_ask_price": None,
        "schwab_mark_price": None,
        "schwab_day_change_percent": None,
        "schwab_total_volume": None,
        "schwab_relative_volume": None,
        "schwab_price_trend_5d_percent": None,
        "schwab_option_call_put_ratio": None,
        "schwab_option_iv_percentile": None,
    }


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not isfinite(number):
        return None
    return number


def _radar_run_path_status(
    *,
    required_completed_count: int,
    required_step_count: int,
    blocking_step_count: int,
) -> str:
    if blocking_step_count:
        return "action_needed"
    if required_step_count == 0:
        return "no_run"
    if required_completed_count >= required_step_count:
        return "complete"
    return "incomplete"


def _discovery_candidate(row: Mapping[str, object]) -> dict[str, object]:
    brief = _mapping_value(row, "research_brief")
    support = _mapping_value(row, "top_supporting_evidence")
    risk = _mapping_value(row, "top_disconfirming_evidence")
    return {
        "ticker": row.get("ticker"),
        "score": _finite_float(row.get("final_score")),
        "state": row.get("state"),
        "setup": row.get("setup_type") or "n/a",
        "why_now": brief.get("why_now"),
        "top_catalyst": brief.get("top_catalyst") or row.get("top_event_title"),
        "evidence": brief.get("supporting_evidence") or support.get("title"),
        "risk_or_gap": brief.get("risk_or_gap") or risk.get("title"),
        "packet": row.get("candidate_packet_id") or "n/a",
        "card": row.get("decision_card_id") or "n/a",
        "next_step": brief.get("next_step"),
        "audit": _mapping_value(brief, "audit"),
    }


def _discovery_scoped_candidates(
    candidates: Sequence[Mapping[str, object]],
    summary: Mapping[str, object],
) -> list[dict[str, object]]:
    if not summary:
        return []
    tickers = {
        str(ticker).strip().upper()
        for ticker in _sequence_value(summary.get("tickers"))
        if str(ticker).strip()
    }
    rows: list[dict[str, object]] = []
    for row in candidates:
        ticker = str(row.get("ticker") or "").strip().upper()
        if tickers and ticker not in tickers:
            continue
        rows.append(_row_dict(row))
    return rows


def _discovery_run_candidates(
    candidates: Sequence[Mapping[str, object]],
    summary: Mapping[str, object],
) -> list[dict[str, object]]:
    if not summary:
        return []
    as_of_date = _parse_date(summary.get("as_of"))
    tickers = {
        str(ticker).strip().upper()
        for ticker in _sequence_value(summary.get("tickers"))
        if str(ticker).strip()
    }
    rows: list[dict[str, object]] = []
    for row in candidates:
        ticker = str(row.get("ticker") or "").strip().upper()
        if tickers and ticker not in tickers:
            continue
        if as_of_date is not None and _parse_date(row.get("as_of")) != as_of_date:
            continue
        rows.append(_row_dict(row))
    return rows


def _latest_candidate_context_payload(
    candidates: Sequence[Mapping[str, object]],
    summary: Mapping[str, object],
    *,
    cutoff: datetime | None,
    limit: int,
) -> dict[str, object]:
    latest_candidate_at = _latest_candidate_as_of(candidates)
    run_as_of = _parse_date(summary.get("as_of"))
    latest_candidate_date = latest_candidate_at.date() if latest_candidate_at else None
    top_candidates = sorted(
        (_row_dict(row) for row in candidates),
        key=lambda row: (-_finite_float(row.get("final_score")), str(row.get("ticker") or "")),
    )[: max(0, int(limit))]
    return {
        "candidate_states": len(candidates),
        "latest_candidate_as_of": _iso_or_none(latest_candidate_at),
        "latest_candidate_session_date": _date_iso_or_none(latest_candidate_at),
        "latest_candidate_age_days": _age_days(cutoff, latest_candidate_at),
        "stale_relative_to_run": bool(
            run_as_of is not None
            and latest_candidate_date is not None
            and latest_candidate_date < run_as_of
        ),
        "top_candidates": [_discovery_candidate(row) for row in top_candidates],
    }


def _latest_run_packet_candidates(
    candidates: Sequence[Mapping[str, object]],
    summary: Mapping[str, object],
) -> list[dict[str, object]]:
    if not summary:
        return []
    run_lower_bound = _parse_utc_datetime(
        summary.get("decision_available_at")
    ) or _parse_utc_datetime(summary.get("started_at"))
    run_finished_at = _parse_utc_datetime(summary.get("finished_at"))
    rows: list[dict[str, object]] = []
    for row in candidates:
        if not row.get("candidate_packet_id"):
            continue
        packet_produced_at = _parse_utc_datetime(
            row.get("candidate_packet_created_at")
        ) or _parse_utc_datetime(row.get("candidate_packet_available_at"))
        if packet_produced_at is None:
            continue
        if run_lower_bound is not None and packet_produced_at < run_lower_bound:
            continue
        if run_finished_at is not None and packet_produced_at > run_finished_at:
            continue
        rows.append(_row_dict(row))
    return rows


def _discovery_source_coverage_payload(
    config: AppConfig,
    *,
    summary: Mapping[str, object],
    steps: Mapping[str, Mapping[str, object]],
) -> list[dict[str, object]]:
    market_provider = _run_market_provider(config, summary=summary, steps=steps)
    event_provider = _run_event_provider(config, steps=steps)
    market_mode = _run_market_source_mode(
        config,
        provider=market_provider,
        step=steps.get("daily_bar_ingest", {}),
    )
    event_mode = _run_event_source_mode(
        config,
        provider=event_provider,
        step=steps.get("event_ingest", {}),
    )
    return [
        {
            "layer": "Market data",
            "mode": market_mode,
            "provider": market_provider,
            "detail": _market_data_detail(config, market_provider),
            "guardrail": f"universe={config.universe_name}; batch={config.scan_batch_size}",
        },
        {
            "layer": "News/events",
            "mode": event_mode,
            "provider": event_provider,
            "detail": _event_source_detail(config, event_provider),
            "guardrail": _event_source_guardrail(config, event_provider),
        },
    ]


def _run_market_provider(
    config: AppConfig,
    *,
    summary: Mapping[str, object],
    steps: Mapping[str, Mapping[str, object]],
) -> str:
    return _provider_name(
        _step_provider(steps, "daily_bar_ingest")
        or summary.get("provider")
        or config.daily_market_provider,
        default="csv",
    )


def _run_event_provider(
    config: AppConfig,
    *,
    steps: Mapping[str, Mapping[str, object]],
) -> str:
    return _provider_name(
        _step_provider(steps, "event_ingest") or config.daily_event_provider,
        default="news_fixture",
    )


def _step_provider(
    steps: Mapping[str, Mapping[str, object]],
    step_name: str,
) -> object:
    payload = _mapping_value(steps.get(step_name, {}), "payload")
    return (
        payload.get("provider")
        or payload.get("scheduled_provider")
        or payload.get("scheduled_event_provider")
    )


def _run_market_source_mode(
    config: AppConfig,
    *,
    provider: str,
    step: Mapping[str, object],
) -> str:
    if provider == "polygon":
        if str(step.get("status") or "") == "success":
            return "live"
        reason = str(step.get("reason") or step.get("error_summary") or "")
        if "CATALYST_POLYGON_API_KEY" in reason or not config.polygon_api_key:
            return "missing_credentials"
    return _market_source_mode(config, provider)


def _run_event_source_mode(
    config: AppConfig,
    *,
    provider: str,
    step: Mapping[str, object],
) -> str:
    if provider in {"sec", "sec_submissions"}:
        if str(step.get("status") or "") == "success":
            return "live"
        reason = str(step.get("reason") or step.get("error_summary") or "")
        if "CATALYST_SEC_ENABLE_LIVE" in reason or "CATALYST_SEC_USER_AGENT" in reason:
            return "missing_credentials"
        if reason == "no_sec_cik_targets":
            return "live"
    return _event_source_mode(config, provider)


def _discovery_blockers(
    *,
    summary: Mapping[str, object],
    market: Mapping[str, object],
    events: Mapping[str, object],
    database: Mapping[str, object],
    run_path: Mapping[str, int],
    as_of_date: date | None,
    latest_bar_date: date | None,
    packet_count: int,
) -> list[dict[str, object]]:
    blockers: list[dict[str, object]] = []
    if not summary:
        blockers.append(
            _discovery_blocker(
                "no_run",
                "No radar run has been recorded yet.",
                "Run one capped radar cycle from the dashboard.",
            )
        )
    market_mode = str(market.get("mode") or "unknown")
    event_mode = str(events.get("mode") or "unknown")
    if market_mode == "fixture":
        blockers.append(
            _discovery_blocker(
                "fixture_market_data",
                "Market data is still fixture-backed.",
                "Configure Polygon before relying on broad US-market discovery.",
            )
        )
    elif market_mode in {"missing_credentials", "disabled"}:
        blockers.append(
            _discovery_blocker(
                f"market_{market_mode}",
                "Live market data is not available.",
                "Set the market provider credentials and rerun preflight.",
            )
        )
    if event_mode == "fixture":
        blockers.append(
            _discovery_blocker(
                "fixture_events",
                "Catalyst events are still fixture-backed.",
                "Configure SEC/live event ingest before relying on fresh catalysts.",
            )
        )
    elif event_mode in {"missing_credentials", "disabled"}:
        blockers.append(
            _discovery_blocker(
                f"events_{event_mode}",
                "Live catalyst ingestion is not available.",
                "Set live event provider settings and rerun preflight.",
            )
        )
    active_count = int(_finite_float(database.get("active_security_count")))
    if active_count and active_count < 100:
        blockers.append(
            _discovery_blocker(
                "thin_universe",
                f"Only {active_count} active securities are loaded.",
                "Seed or refresh the universe before treating discovery as broad.",
            )
        )
    if as_of_date is not None and latest_bar_date is not None and latest_bar_date < as_of_date:
        blockers.append(
            _discovery_blocker(
                "stale_daily_bars",
                f"Latest daily bars are {latest_bar_date.isoformat()}, older than run as-of.",
                "Refresh market bars for the selected as-of date before acting.",
            )
        )
    if int(run_path.get("blocking_count") or 0) > 0:
        blockers.append(
            _discovery_blocker(
                "blocked_run_steps",
                f"{run_path['blocking_count']} required run step(s) need attention.",
                "Open the run-step action table and fix the first blocked input.",
            )
        )
    if summary and packet_count == 0:
        blockers.append(
            _discovery_blocker(
                "no_candidate_packets",
                "No candidate packets were produced for the latest run.",
                "Treat scores as incomplete until candidate packet generation succeeds.",
            )
        )
    return blockers


def _discovery_blocker(
    code: str,
    finding: str,
    next_action: str,
) -> dict[str, object]:
    return {"code": code, "finding": finding, "next_action": next_action}


def _discovery_status(
    *,
    has_run: bool,
    market_mode: str,
    event_mode: str,
    run_path: Mapping[str, int],
    blockers: Sequence[Mapping[str, object]],
    packet_count: int,
) -> str:
    if not has_run:
        return "attention"
    if market_mode in {"missing_credentials", "disabled"} or event_mode in {
        "missing_credentials",
        "disabled",
    }:
        return "blocked"
    if int(run_path.get("blocking_count") or 0) > 0:
        return "blocked"
    if market_mode == "fixture" or event_mode == "fixture":
        return "fixture"
    if packet_count == 0 or blockers:
        return "attention"
    return "ready"


def _discovery_headline(status: str, candidate_count: int) -> str:
    if status == "ready":
        return f"Latest run surfaced {candidate_count} current candidate(s)."
    if status == "fixture":
        return f"Fixture discovery snapshot: {candidate_count} candidate(s)."
    if status == "blocked":
        return "Latest discovery has blocking input gaps."
    return f"Discovery needs attention: {candidate_count} candidate(s) visible."


def _discovery_detail(
    *,
    market: Mapping[str, object],
    events: Mapping[str, object],
    packet_count: int,
    card_count: int,
) -> str:
    return (
        f"Market {market.get('provider') or 'unknown'}/"
        f"{market.get('mode') or 'unknown'}; "
        f"events {events.get('provider') or 'unknown'}/"
        f"{events.get('mode') or 'unknown'}; "
        f"candidate packets={packet_count}; decision cards={card_count}."
    )


def _discovery_next_action(
    status: str,
    blockers: Sequence[Mapping[str, object]],
) -> str:
    if blockers:
        return str(blockers[0].get("next_action") or "Review discovery blockers.")
    if status == "ready":
        return "Review top discoveries, then open ticker detail before any trade action."
    if status == "fixture":
        return "Switch market and event providers to live mode before relying on timing."
    return "Run one radar cycle and review discovery yield before changing thresholds."


def _step_metric(
    steps: Mapping[str, Mapping[str, object]],
    step_name: str,
    field: str,
    *,
    default: int,
) -> int:
    step = steps.get(step_name, {})
    if field not in step:
        return int(default)
    return int(_finite_float(step.get(field)))


def _latest_candidate_as_of(
    candidates: Sequence[Mapping[str, object]],
) -> datetime | None:
    values: list[datetime] = []
    for row in candidates:
        parsed = _parse_utc_datetime(row.get("as_of"))
        if parsed is None and (parsed_date := _parse_date(row.get("as_of"))) is not None:
            parsed = datetime.combine(parsed_date, datetime.min.time(), tzinfo=UTC)
        if parsed is not None:
            values.append(parsed)
    return max(values) if values else None


def _date_window(value: date) -> tuple[datetime, datetime]:
    start = datetime.combine(value, datetime.min.time(), tzinfo=UTC)
    return start, start + timedelta(days=1)


def _age_days(later: datetime | None, earlier: datetime | None) -> int | None:
    if later is None or earlier is None:
        return None
    return max(0, (later.date() - earlier.date()).days)


def _parse_utc_datetime(value: object) -> datetime | None:
    parsed = _as_utc_datetime_or_none(value)
    if parsed is not None:
        return parsed
    if isinstance(value, str) and value.strip():
        try:
            return _as_utc_datetime_or_none(
                datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
            )
        except ValueError:
            return None
    return None


def _parse_date(value: object) -> date | None:
    if isinstance(value, datetime):
        converted = _as_utc_datetime(value)
        return converted.date() if isinstance(converted, datetime) else None
    if isinstance(value, date):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return date.fromisoformat(value.strip()[:10])
        except ValueError:
            return None
    return None


def _iso_or_none(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _date_iso_or_none(value: date | datetime | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    return value.isoformat()


def _iso_or_na(value: datetime | None) -> str:
    return _iso_or_none(value) or "n/a"


def _retry_after_seconds(reset_at: datetime | None, now: datetime) -> int:
    resolved_reset_at = _as_utc_datetime_or_none(reset_at)
    if resolved_reset_at is None:
        return 1
    return max(1, int(ceil((resolved_reset_at - now).total_seconds())))


def _radar_steps_by_name(
    radar_run_summary: Mapping[str, object] | None,
) -> dict[str, dict[str, object]]:
    if not isinstance(radar_run_summary, Mapping):
        return {}
    raw_steps = radar_run_summary.get("steps")
    steps: dict[str, dict[str, object]] = {}
    if isinstance(raw_steps, Mapping):
        for name, value in raw_steps.items():
            if not isinstance(value, Mapping):
                continue
            row = _row_dict(value)
            row.setdefault("step", str(name))
            steps[str(name)] = row
        return steps
    if isinstance(raw_steps, Sequence) and not isinstance(raw_steps, str | bytes):
        for value in raw_steps:
            if not isinstance(value, Mapping):
                continue
            row = _row_dict(value)
            name = str(row.get("step") or row.get("name") or row.get("job_type") or "")
            if name:
                steps[name] = row
    return steps


def _call_plan_tickers(tickers: Sequence[str] | None) -> list[str]:
    values: list[str] = []
    seen: set[str] = set()
    for value in tickers or ():
        ticker = str(value or "").strip().upper()
        if ticker and ticker not in seen:
            values.append(ticker)
            seen.add(ticker)
    return values


def _sec_call_plan_targets(
    engine: Engine,
    *,
    tickers: Sequence[str],
    limit: int,
) -> list[dict[str, object]]:
    filters = [securities.c.is_active.is_(True)]
    if tickers:
        filters.append(securities.c.ticker.in_(tuple(tickers)))
    stmt = (
        select(securities.c.ticker, securities.c.metadata)
        .where(*filters)
        .order_by(securities.c.ticker)
    )
    rows: list[dict[str, object]] = []
    cap = max(0, int(limit))
    with engine.connect() as conn:
        for row in conn.execute(stmt):
            if len(rows) >= cap:
                break
            metadata = row._mapping["metadata"]
            cik = _security_metadata_cik(metadata if isinstance(metadata, Mapping) else {})
            if cik is not None:
                rows.append({"ticker": row.ticker, "cik": cik})
    return rows


def _security_metadata_cik(metadata: Mapping[str, object]) -> str | None:
    for key in ("cik", "cik_str", "central_index_key"):
        value = metadata.get(key)
        if value is not None and str(value).strip():
            return str(value).strip().zfill(10)
    return None


def _coverage_evidence(row: Mapping[str, object]) -> str:
    parts = [
        str(row.get("provider") or "").strip(),
        str(row.get("mode") or "").strip(),
        str(row.get("detail") or "").strip(),
        str(row.get("guardrail") or "").strip(),
    ]
    return "; ".join(part for part in parts if part) or "n/a"


def _market_data_detail(config: AppConfig, provider: str) -> str:
    if provider in {"csv", "sample"}:
        return config.csv_daily_bars_path
    if provider == "polygon":
        return f"grouped daily; base_url={config.polygon_base_url}"
    return config.market_provider


def _event_source_detail(config: AppConfig, provider: str) -> str:
    if provider in {"news_fixture", "sample", "fixture"}:
        return config.news_fixture_path
    if provider in {"sec", "sec_submissions"}:
        return f"submissions; base_url={config.sec_base_url}"
    return provider


def _event_source_guardrail(config: AppConfig, provider: str) -> str:
    if provider in {"sec", "sec_submissions"}:
        return (
            "point-in-time event cutoff enforced; "
            f"max_tickers={config.sec_daily_max_tickers}"
        )
    return "point-in-time event cutoff enforced"


def _preflight_row(
    layer: str,
    status: str,
    provider: str,
    call_budget: str,
    guardrail: str,
    next_action: str,
    evidence: str,
) -> dict[str, object]:
    return {
        "layer": layer,
        "status": status,
        "provider": provider,
        "call_budget": call_budget,
        "guardrail": guardrail,
        "next_action": next_action,
        "evidence": evidence,
    }


def _market_preflight_row(
    config: AppConfig,
    provider: str,
    coverage: Mapping[str, object],
) -> dict[str, object]:
    mode = str(coverage.get("mode") or "unknown")
    if provider == "polygon" and mode == "missing_credentials":
        return _preflight_row(
            "Market data",
            "blocked",
            provider,
            "0 live calls until CATALYST_POLYGON_API_KEY is set",
            (
            "Polygon grouped daily is capped at 1 request per radar run; "
            f"manual run cooldown={config.radar_run_min_interval_seconds}s; "
            f"ticker reference seed cap={config.polygon_tickers_max_pages} page(s)."
        ),
            "Set the Polygon API key, then run one radar cycle and inspect rejected_count.",
            _coverage_evidence(coverage),
        )
    if provider == "polygon" and mode == "live":
        return _preflight_row(
            "Market data",
            "ready",
            provider,
            "1 grouped-daily request per radar run",
            (
                "No ticker-by-ticker price polling in daily radar runs; "
                f"scanner batch={config.scan_batch_size}; "
                f"manual run cooldown={config.radar_run_min_interval_seconds}s; "
                f"ticker reference seed cap={config.polygon_tickers_max_pages} page(s)."
            ),
            "Run one radar cycle and verify provider health plus rejected_count before scaling.",
            _coverage_evidence(coverage),
        )
    if mode == "disabled":
        return _preflight_row(
            "Market data",
            "blocked",
            provider,
            "0 live calls",
            "No scheduled market provider is enabled.",
            "Set CATALYST_DAILY_MARKET_PROVIDER=polygon for live grouped daily data.",
            _coverage_evidence(coverage),
        )
    return _preflight_row(
        "Market data",
        "fixture",
        provider,
        "0 live calls",
        "Local fixture data only; no external market-data requests.",
        "Switch to Polygon when you are ready for fresh US-market coverage.",
        _coverage_evidence(coverage),
    )


def _event_preflight_row(
    config: AppConfig,
    provider: str,
    coverage: Mapping[str, object],
    event_step: Mapping[str, object],
) -> dict[str, object]:
    mode = str(coverage.get("mode") or "unknown")
    sec_budget = f"up to {config.sec_daily_max_tickers} SEC submissions requests per radar run"
    if provider in {"sec", "sec_submissions"} and mode == "missing_credentials":
        return _preflight_row(
            "News/events",
            "blocked",
            provider,
            "0 live calls until SEC live flag and User-Agent are set",
            f"{sec_budget}; SEC live ingest fails closed without required settings.",
            "Set CATALYST_SEC_ENABLE_LIVE=1 and CATALYST_SEC_USER_AGENT.",
            _coverage_evidence(coverage),
        )
    if provider in {"sec", "sec_submissions"}:
        if str(event_step.get("reason") or "") == "no_sec_cik_targets":
            return _preflight_row(
                "News/events",
                "attention",
                provider,
                sec_budget,
                "SEC calls are capped and require active securities with CIK metadata.",
                "Seed active securities with CIKs through Polygon reference data or CSV metadata.",
                _step_evidence("event_ingest", event_step),
            )
        return _preflight_row(
            "News/events",
            "ready",
            provider,
            sec_budget,
            "SEC live ingest requires User-Agent and point-in-time cutoff.",
            "Run one radar cycle and verify target_count, event_count, and rejected_count.",
            _coverage_evidence(coverage),
        )
    if mode == "disabled":
        return _preflight_row(
            "News/events",
            "blocked",
            provider,
            "0 live calls",
            "No scheduled catalyst provider is enabled.",
            "Set CATALYST_DAILY_EVENT_PROVIDER=sec when you want SEC catalyst polling.",
            _coverage_evidence(coverage),
        )
    return _preflight_row(
        "News/events",
        "fixture",
        provider,
        "0 live calls",
        "Local fixture events only; no external news or filing requests.",
        "Switch to SEC scheduled ingest when you are ready for live catalyst discovery.",
        _coverage_evidence(coverage),
    )


def _market_call_plan_row(config: AppConfig) -> dict[str, object]:
    provider = _provider_name(config.daily_market_provider, default="csv")
    if provider in {"", "none", "off", "disabled"}:
        return _call_plan_row(
            "Market data",
            "skipped",
            provider,
            "none",
            0,
            "Scheduled market provider is disabled.",
            "Set CATALYST_DAILY_MARKET_PROVIDER before expecting fresh bars.",
        )
    if provider in {"csv", "sample"}:
        return _call_plan_row(
            "Market data",
            "local_only",
            provider,
            "scheduled_csv_ingest",
            0,
            "Reads local fixture/CSV market files only.",
            "Use for dry-run validation; configure Polygon for live discovery.",
        )
    if provider == "polygon":
        if not config.polygon_api_key:
            return _call_plan_row(
                "Market data",
                "blocked",
                provider,
                "grouped_daily",
                0,
                "Polygon is selected, but CATALYST_POLYGON_API_KEY is missing.",
                "Set CATALYST_POLYGON_API_KEY before running live market ingest.",
            )
        return _call_plan_row(
            "Market data",
            "live_call_planned",
            provider,
            "grouped_daily",
            1,
            "One Polygon grouped-daily request for the selected as-of date.",
            "Keep the manual cooldown active and inspect rejected_count after the run.",
        )
    return _call_plan_row(
        "Market data",
        "blocked",
        provider,
        "unknown",
        0,
        "Scheduled market provider is not supported by daily radar runs.",
        "Use csv, sample, polygon, none, off, or disabled.",
    )


def _event_call_plan_row(
    config: AppConfig,
    *,
    sec_targets: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    provider = _provider_name(config.daily_event_provider, default="news_fixture")
    if provider in {"", "none", "off", "disabled"}:
        return _call_plan_row(
            "News/events",
            "skipped",
            provider,
            "none",
            0,
            "Scheduled event provider is disabled.",
            "Set CATALYST_DAILY_EVENT_PROVIDER before expecting fresh catalysts.",
        )
    if provider in {"news_fixture", "sample", "fixture"}:
        return _call_plan_row(
            "News/events",
            "local_only",
            provider,
            "scheduled_news_fixture_ingest",
            0,
            "Reads local fixture/news files only.",
            "Configure SEC live mode for live filing catalysts.",
        )
    if provider in {"sec", "sec_submissions"}:
        if not config.sec_enable_live:
            return _call_plan_row(
                "News/events",
                "blocked",
                provider,
                "submissions",
                0,
                "SEC provider is selected, but CATALYST_SEC_ENABLE_LIVE=1 is missing.",
                "Set CATALYST_SEC_ENABLE_LIVE=1 before scheduled SEC ingest.",
            )
        if not config.sec_user_agent:
            return _call_plan_row(
                "News/events",
                "blocked",
                provider,
                "submissions",
                0,
                "SEC provider is selected, but CATALYST_SEC_USER_AGENT is missing.",
                "Set a compliant SEC User-Agent before scheduled SEC ingest.",
            )
        target_count = len(sec_targets)
        if target_count == 0:
            return _call_plan_row(
                "News/events",
                "expected_gate",
                provider,
                "submissions",
                0,
                "No active securities with CIK metadata are available for SEC polling.",
                "Seed/refresh Polygon tickers so securities include CIK metadata.",
            )
        return _call_plan_row(
            "News/events",
            "live_call_planned",
            provider,
            "submissions",
            target_count,
            (
                f"SEC submissions polling is capped at {config.sec_daily_max_tickers} "
                f"ticker(s); this scope has {target_count} target(s)."
            ),
            "Run only when this target count matches your intended call budget.",
        )
    return _call_plan_row(
        "News/events",
        "blocked",
        provider,
        "unknown",
        0,
        "Scheduled event provider is not supported by daily radar runs.",
        "Use news_fixture, sample, fixture, sec, sec_submissions, none, off, or disabled.",
    )


def _llm_call_plan_row(*, run_llm: bool, llm_dry_run: bool) -> dict[str, object]:
    if not run_llm:
        return _call_plan_row(
            "LLM review",
            "expected_gate",
            "none",
            "daily_llm_review",
            0,
            "Daily LLM review is not requested.",
            "Use per-candidate LLM review after live data quality is acceptable.",
        )
    if llm_dry_run:
        return _call_plan_row(
            "LLM review",
            "dry_run",
            "configured",
            "daily_llm_review",
            0,
            "Daily LLM review is requested in dry-run mode; no model call is made.",
            "Inspect budget estimates before enabling per-candidate model calls.",
        )
    return _call_plan_row(
        "LLM review",
        "blocked",
        "configured",
        "daily_llm_review",
        0,
        "Real daily LLM review is not supported.",
        "Use run-llm-review per candidate instead of daily real LLM mode.",
    )


def _alert_call_plan_row(*, dry_run_alerts: bool) -> dict[str, object]:
    if dry_run_alerts:
        return _call_plan_row(
            "Alert delivery",
            "dry_run",
            "internal",
            "daily_digest",
            0,
            "Daily alert delivery is locked to dry-run mode.",
            "Use explicit alert delivery workflows after review.",
        )
    return _call_plan_row(
        "Alert delivery",
        "blocked",
        "internal",
        "daily_digest",
        0,
        "Real daily alert delivery is not supported.",
        "Keep daily radar runs in dry-run alert mode.",
    )


def _schwab_call_plan_row() -> dict[str, object]:
    return _call_plan_row(
        "Schwab",
        "not_called",
        "schwab",
        "portfolio_sync",
        0,
        "Radar runs do not call Schwab APIs.",
        "Use separate broker sync controls with their own rate guards.",
    )


def _call_plan_row(
    layer: str,
    status: str,
    provider: str,
    endpoint: str,
    external_call_count_max: int,
    detail: str,
    next_action: str,
) -> dict[str, object]:
    return {
        "layer": layer,
        "status": status,
        "provider": provider or "n/a",
        "endpoint": endpoint,
        "external_call_count_max": max(0, int(external_call_count_max)),
        "detail": detail,
        "next_action": next_action,
    }


def _schwab_preflight_row(
    config: AppConfig,
    coverage: Mapping[str, object],
) -> dict[str, object]:
    mode = str(coverage.get("mode") or "unknown")
    status = "attention" if mode == "stale_read_only_connected" else "ready"
    if mode not in {"read_only_connected", "stale_read_only_connected"}:
        status = "optional"
    return _preflight_row(
        "Schwab portfolio",
        status,
        "schwab",
        (
            f"portfolio sync min {config.schwab_sync_min_interval_seconds}s; "
            f"market context max {config.schwab_market_sync_max_tickers} tickers "
            f"per {config.schwab_market_sync_min_interval_seconds}s"
        ),
        "Read-only sync only; real order submission remains disabled by kill switch.",
        (
            "Run one sync from the Broker tab."
            if status == "attention"
            else "Use synced positions as context for candidate exposure checks."
        ),
        _coverage_evidence(coverage),
    )


def _llm_preflight_row(
    config: AppConfig,
    coverage: Mapping[str, object],
) -> dict[str, object]:
    mode = str(coverage.get("mode") or "unknown")
    missing = _llm_missing_env(config)
    if mode == "disabled":
        status = "optional"
        setup = ", ".join(_llm_activation_missing_env(config))
        call_budget = f"0 LLM calls; setup requires {setup}" if setup else "0 LLM calls"
        next_action = (
            "Enable OpenAI review only after model, key, pricing, budget, and task-cap "
            "setup."
        )
    elif missing:
        status = "blocked"
        call_budget = f"0 LLM calls until {', '.join(missing)} are set"
        next_action = "Complete OpenAI setup, then run one dry-run review before real review."
    else:
        status = "ready"
        daily_cap = config.llm_task_daily_caps or {}
        call_budget = (
            f"daily_budget={config.llm_daily_budget_usd}; "
            f"monthly_budget={config.llm_monthly_budget_usd}; "
            f"task_caps={dict(daily_cap)}"
        )
        next_action = "Keep dry-run review on until card quality and cost telemetry are acceptable."
    return _preflight_row(
        "LLM review",
        status,
        str(config.llm_provider or "none"),
        call_budget,
        "Budget caps and task caps gate agentic review.",
        next_action,
        _coverage_evidence(coverage),
    )


def _step_evidence(name: str, step: Mapping[str, object]) -> str:
    if not step:
        return f"{name}: missing"
    status = str(step.get("status") or "unknown")
    reason = str(step.get("reason") or "n/a")
    classification = classify_step_outcome(status, None if reason == "n/a" else reason)
    outcome = str(step.get("label") or classification.label)
    category = str(step.get("category") or classification.category)
    requested = int(_finite_float(step.get("requested_count")))
    raw = int(_finite_float(step.get("raw_count")))
    normalized = int(_finite_float(step.get("normalized_count")))
    return (
        f"{name}: outcome={outcome}; category={category}; requested={requested}; "
        f"raw={raw}; normalized={normalized}; reason={reason}"
    )


def _steps_evidence(
    steps: Mapping[str, Mapping[str, object]],
    names: Sequence[str],
) -> str:
    return " | ".join(_step_evidence(name, steps.get(name, {})) for name in names)


def _provider_name(value: object, *, default: str) -> str:
    text = str(value or "").strip().lower()
    return text or default


def _source_mode(provider: str, *, fixture_names: set[str]) -> str:
    if provider in {"none", "off", "disabled", ""}:
        return "disabled"
    if provider in fixture_names or "fixture" in provider:
        return "fixture"
    return "live"


def _market_source_mode(config: AppConfig, provider: str) -> str:
    if provider == "polygon" and not config.polygon_api_key:
        return "missing_credentials"
    return _source_mode(provider, fixture_names={"csv", "sample"})


def _event_source_mode(config: AppConfig, provider: str) -> str:
    if provider in {"sec", "sec_submissions"} and (
        not config.sec_enable_live or not config.sec_user_agent
    ):
        return "missing_credentials"
    return _source_mode(provider, fixture_names={"news_fixture", "sample", "fixture"})


def _broker_mode(
    snapshot: Mapping[str, object],
    exposure: Mapping[str, object],
) -> str:
    status = str(snapshot.get("connection_status") or "unknown").strip().lower()
    if status == "connected":
        if bool(exposure.get("broker_data_stale")):
            return "stale_read_only_connected"
        return "read_only_connected"
    return status or "unknown"


def _llm_mode(config: AppConfig) -> str:
    provider = str(config.llm_provider or "").strip().lower()
    if not config.enable_premium_llm or provider in {"", "none", "off", "disabled"}:
        return "disabled"
    if provider == "openai" and not config.openai_api_key:
        return "missing_credentials"
    return "enabled"


def _first_mapping(*values: object) -> dict[str, object]:
    for value in values:
        if isinstance(value, Mapping) and value:
            return _row_dict(value)
    return {}


def _sequence_value(value: object) -> tuple[object, ...]:
    if isinstance(value, Mapping) or isinstance(value, str | bytes):
        return ()
    if isinstance(value, Sequence):
        return tuple(value)
    return ()


def _first_present(*values: object) -> object:
    for value in values:
        if value is not None and value != "" and value != [] and value != {}:
            return value
    return None


def _theme_name(metadata: Mapping[str, object]) -> str | None:
    value = metadata.get("candidate_theme")
    if value is not None and str(value).strip():
        return str(value).strip()
    theme_hits = metadata.get("theme_hits")
    if not isinstance(theme_hits, list):
        return None
    for hit in theme_hits:
        if not isinstance(hit, Mapping):
            continue
        for key in ("theme_id", "theme", "name"):
            value = hit.get(key)
            if value is not None and str(value).strip():
                return str(value).strip()
    return None


def _finite_float(value: object) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    return number if isfinite(number) else 0.0


def _string_int_mapping(value: object) -> dict[str, int]:
    if not isinstance(value, Mapping):
        return {}
    return {str(key): int(_finite_float(item)) for key, item in value.items()}


def _positive_limit(value: int) -> int:
    return max(1, int(value))


def _radar_run_key(row: Mapping[str, object]) -> tuple[object, ...]:
    metadata = row.get("metadata")
    if not isinstance(metadata, Mapping):
        metadata = {}
    tickers = metadata.get("tickers")
    normalized_tickers = tuple(tickers) if isinstance(tickers, list | tuple) else ()
    return (
        metadata.get("as_of"),
        metadata.get("decision_available_at"),
        metadata.get("outcome_available_at"),
        metadata.get("provider"),
        metadata.get("universe"),
        normalized_tickers,
    )


def _radar_run_status(rows: Sequence[Mapping[str, object]]) -> str:
    statuses = tuple(str(row.get("status") or "unknown") for row in rows)
    if not statuses:
        return "unknown"
    if any(status == "running" for status in statuses):
        return "running"
    if any(status == "failed" for status in statuses):
        if any(status != "failed" for status in statuses):
            return "partial_success"
        return "failed"
    if any(_radar_run_step_classification(row).blocks_reliance for row in rows):
        return "partial_success"
    return "success"


def _radar_run_step_classification(
    row: Mapping[str, object],
) -> StepOutcomeClassification:
    inferred = classify_step_outcome(
        str(row.get("status") or ""),
        _radar_run_step_reason(row),
    )
    metadata = row.get("metadata")
    if isinstance(metadata, Mapping):
        return StepOutcomeClassification(
            category=inferred.category,
            label=inferred.label,
            meaning=(
                inferred.meaning
                if inferred.meaning is not None
                else (
                    str(metadata.get("outcome_meaning"))
                    if metadata.get("outcome_meaning") is not None
                    else None
                )
            ),
            operator_action=(
                inferred.operator_action
                if inferred.operator_action is not None
                else (
                    str(metadata.get("operator_action"))
                    if metadata.get("operator_action") is not None
                    else None
                )
            ),
            trigger_condition=(
                inferred.trigger_condition
                if inferred.trigger_condition is not None
                else (
                    str(metadata.get("trigger_condition"))
                    if metadata.get("trigger_condition") is not None
                    else None
                )
            ),
            blocks_reliance=inferred.blocks_reliance,
        )
    return inferred


def _radar_run_step_reason(row: Mapping[str, object]) -> str | None:
    metadata = row.get("metadata")
    reason = metadata.get("result_reason") if isinstance(metadata, Mapping) else None
    if reason is None:
        reason = row.get("error_summary")
    return str(reason) if reason else None


def _radar_run_step_payload(row: Mapping[str, object]) -> object:
    metadata = row.get("metadata")
    if not isinstance(metadata, Mapping):
        return {}
    payload = metadata.get("result_payload")
    return payload if payload is not None else {}


def _latest_validation_run_id(
    engine: Engine,
    *,
    available_at: datetime | None = None,
) -> str | None:
    filters = [
        validation_runs.c.status == "success",
        validation_runs.c.finished_at.is_not(None),
    ]
    if available_at is not None:
        filters.append(validation_runs.c.finished_at <= available_at)
    with engine.connect() as conn:
        row = conn.execute(
            select(validation_runs.c.id)
            .where(*filters)
            .order_by(
                validation_runs.c.finished_at.desc(),
                validation_runs.c.started_at.desc(),
                validation_runs.c.created_at.desc(),
                validation_runs.c.id.desc(),
            )
            .limit(1)
        ).first()
    return str(row[0]) if row is not None else None


def _total_cost_from_metrics(metrics: Mapping[str, object]) -> float:
    for key in (
        "total_cost_usd",
        "total_cost",
        "actual_cost_usd",
        "estimated_cost_usd",
    ):
        value = metrics.get(key)
        if value is not None:
            return _finite_float(value)
    return 0.0


def _label_matches_validation_results(conn: Any, label: object, rows: list[object]) -> bool:
    keys = _validation_result_artifact_keys(rows)
    artifact_id = str(getattr(label, "artifact_id", "") or "")
    artifact_type = str(getattr(label, "artifact_type", "") or "")
    if artifact_id in keys:
        return True
    if artifact_type != "alert":
        return False
    row = conn.execute(
        select(
            alerts.c.candidate_state_id,
            alerts.c.candidate_packet_id,
            alerts.c.decision_card_id,
        )
        .where(alerts.c.id == artifact_id)
        .limit(1)
    ).first()
    if row is None:
        return False
    return any(
        value is not None and str(value).strip() in keys for value in row._mapping.values()
    )


def _validation_result_artifact_keys(rows: list[object]) -> set[str]:
    keys: set[str] = set()
    for row in rows:
        for attr in ("id", "candidate_state_id", "candidate_packet_id", "decision_card_id"):
            value = getattr(row, attr, None)
            if value is not None and str(value).strip():
                keys.add(str(value))
    return keys

from __future__ import annotations

import json
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from copy import deepcopy
from dataclasses import fields, is_dataclass
from datetime import UTC, date, datetime, timedelta
from enum import Enum
from hashlib import sha256
from math import ceil, isfinite
from pathlib import Path
from time import monotonic
from typing import Any

from sqlalchemy import Engine, and_, func, select
from sqlalchemy.exc import SQLAlchemyError

from catalyst_radar.agents.models import BudgetLedgerEntry
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
from catalyst_radar.connectors.options import (
    OPTIONS_FIXTURE_NUMERIC_FIELDS,
    OPTIONS_FIXTURE_TEMPLATE_RESULT_FIELDS,
    validate_options_fixture_json,
)
from catalyst_radar.core.config import AppConfig
from catalyst_radar.core.models import ActionState, MarketFeatures
from catalyst_radar.core.runtime import build_info
from catalyst_radar.jobs.step_outcomes import (
    StepOutcomeClassification,
    classify_step_outcome,
)
from catalyst_radar.jobs.tasks import DAILY_STEP_ORDER
from catalyst_radar.market.manual_bars import (
    MANUAL_BAR_REQUIRED_FILL_FIELDS,
    manual_bar_provider_health_gate,
    manual_market_bars_repair_plan,
    provider_saved_file_capture_approval_packet,
)
from catalyst_radar.scoring.priced_in import evaluate_priced_in
from catalyst_radar.security.redaction import redact_text
from catalyst_radar.storage.broker_repositories import BrokerRepository
from catalyst_radar.storage.budget_repositories import BudgetLedgerRepository
from catalyst_radar.storage.provider_repositories import ProviderRepository
from catalyst_radar.storage.schema import (
    alert_suppressions,
    alerts,
    audit_events,
    broker_market_snapshots,
    candidate_packets,
    candidate_states,
    daily_bars,
    decision_cards,
    events,
    job_locks,
    job_runs,
    option_features,
    paper_trades,
    securities,
    signal_features,
    text_features,
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
PRICED_IN_SOURCE_CLASSES = (
    "market_bars",
    "catalyst_events",
    "local_text",
    "options",
    "theme_peer_sector",
    "broker_context",
)
PRICED_IN_BATCHABLE_SOURCES = frozenset(
    {"catalyst_events", "local_text", "options", "broker_context"}
)
PRICED_IN_SCHWAB_BATCH_SOURCES = frozenset({"options", "broker_context"})
PRICED_IN_OPTIONAL_CONTEXT_SOURCES = frozenset(
    {"options", "theme_peer_sector", "broker_context"}
)
PRICED_IN_LOCAL_BATCH_MAX_TICKERS = 50
PRICED_IN_COMPANY_LIKE_SECURITY_TYPES = frozenset({"CS", "ADRC"})
PRICED_IN_FUND_LIKE_SECURITY_TYPES = frozenset({"ETF", "ETN", "ETS", "ETV", "FUND"})
PRICED_IN_WRAPPER_SECURITY_TYPES = frozenset({"WARRANT", "RIGHT", "UNIT", "PFD", "SP"})
PRICED_IN_NON_COMPANY_SECURITY_TYPES = (
    PRICED_IN_FUND_LIKE_SECURITY_TYPES | PRICED_IN_WRAPPER_SECURITY_TYPES
)
PRICED_IN_SCAN_EXCLUDED_TICKERS = frozenset({"SPY", "XLK", "XLI"})
_ARTIFACT_CUTOFF_UNSET = object()
_PRICED_IN_AUDIT_CACHE_TTL_SECONDS = 180.0
_PRICED_IN_AUDIT_CACHE_MAX_ITEMS = 12
_PRICED_IN_AUDIT_CACHE: dict[tuple[object, ...], tuple[float, dict[str, object]]] = {}
PRICED_IN_SOURCE_ALIASES = {
    "bars": "market_bars",
    "market": "market_bars",
    "market_data": "market_bars",
    "events": "catalyst_events",
    "filings": "catalyst_events",
    "news": "catalyst_events",
    "text": "local_text",
    "textint": "local_text",
    "options_flow": "options",
    "schwab": "broker_context",
    "broker": "broker_context",
    "theme": "theme_peer_sector",
    "themes": "theme_peer_sector",
    "peer": "theme_peer_sector",
    "peers": "theme_peer_sector",
    "sector": "theme_peer_sector",
}
PRICED_IN_ACTIONABLE_STATUSES = frozenset(
    {
        "bullish_not_priced_in",
        "bearish_not_priced_in",
    }
)
PRICED_IN_ACTIONABLE_FILTERS = frozenset(
    {
        "actionable",
        "mismatch",
        "not_priced_in",
        "not-priced-in",
    }
)
PRICED_IN_SOURCE_ACTION_TICKER_LIMIT = 5
PRICED_IN_FULL_SCAN_PREVIEW_LIMIT = 25
PRICED_IN_USEFULNESS_STATUSES = frozenset(
    {
        "research_useful",
        "decision_useful",
        "blocked",
        "monitor_only",
        "not_useful",
    }
)
PRICED_IN_USEFULNESS_FILTERS: Mapping[str, frozenset[str]] = {
    "useful": frozenset({"research_useful", "decision_useful"}),
    "research": frozenset({"research_useful"}),
    "research_useful": frozenset({"research_useful"}),
    "decision": frozenset({"decision_useful"}),
    "ready": frozenset({"decision_useful"}),
    "decision_useful": frozenset({"decision_useful"}),
    "blocked": frozenset({"blocked"}),
    "monitor": frozenset({"monitor_only"}),
    "monitor_only": frozenset({"monitor_only"}),
    "not_useful": frozenset({"not_useful"}),
}
FULL_SCAN_MARKET_BLOCKER_CODES = frozenset(
    {"stale_daily_bars", "incomplete_daily_bar_coverage"}
)
FULL_SCAN_DERIVATIVE_READINESS_AREAS = frozenset(
    {"Research loop", "Decision Cards"}
)

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
    artifact_available_at: datetime | None | object = _ARTIFACT_CUTOFF_UNSET,
    as_of_date: date | None = None,
    limit: int | None = 200,
    include_artifacts: bool = True,
    include_briefs: bool = True,
) -> list[dict[str, object]]:
    cutoff = _as_utc_datetime_or_none(available_at)
    artifact_cutoff = (
        cutoff
        if artifact_available_at is _ARTIFACT_CUTOFF_UNSET
        else _as_utc_datetime_or_none(artifact_available_at)
    )
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
        .order_by(candidate_states.c.final_score.desc(), candidate_states.c.as_of.desc())
    )
    if include_artifacts:
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
        if artifact_cutoff is not None:
            ranked_packet_stmt = ranked_packet_stmt.where(
                candidate_packets.c.available_at <= artifact_cutoff
            )
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
        if artifact_cutoff is not None:
            ranked_card_stmt = ranked_card_stmt.where(
                decision_cards.c.available_at <= artifact_cutoff
            )
        ranked_cards = ranked_card_stmt.subquery()

        stmt = (
            stmt.add_columns(
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
        )
    if limit is not None:
        stmt = stmt.limit(_positive_limit(limit))
    with engine.connect() as conn:
        return [
            _candidate_row(row._mapping, include_briefs=include_briefs)
            for row in conn.execute(stmt)
        ]


def load_radar_run_candidate_rows(
    engine: Engine,
    radar_run_summary: Mapping[str, object],
    *,
    limit: int | None = 200,
    include_artifacts: bool = True,
    include_post_run_artifacts: bool = False,
    include_briefs: bool = True,
) -> list[dict[str, object]]:
    summary = _row_dict(radar_run_summary)
    cutoff = _parse_utc_datetime(summary.get("finished_at")) or _parse_utc_datetime(
        summary.get("decision_available_at")
    )
    run_has_universe = bool(str(summary.get("universe") or "").strip())
    rows = load_candidate_rows(
        engine,
        available_at=cutoff,
        artifact_available_at=None
        if include_post_run_artifacts
        else _ARTIFACT_CUTOFF_UNSET,
        as_of_date=_parse_date(summary.get("as_of")),
        limit=None if run_has_universe else limit,
        include_artifacts=include_artifacts,
        include_briefs=include_briefs,
    )
    filtered_rows = _filter_rows_to_run_universe(
        engine,
        rows,
        radar_run_summary=summary,
        cutoff=cutoff,
    )
    if limit is None:
        return filtered_rows
    return filtered_rows[: _positive_limit(limit)]


def _should_use_previous_priced_in_scan(
    radar_run_summary: Mapping[str, object],
    candidate_rows: Sequence[Mapping[str, object]],
) -> bool:
    if candidate_rows or not radar_run_summary:
        return False
    steps = _radar_steps_by_name(radar_run_summary)
    feature_scan = steps.get("feature_scan", {})
    feature_status = str(feature_scan.get("status") or "").strip().lower()
    if feature_status in {"failed", "skipped"}:
        return True
    run_path = _radar_run_path_summary(radar_run_summary)
    return (
        str(radar_run_summary.get("status") or "").strip().lower()
        in {"failed", "partial_success"}
        and int(_finite_float(run_path.get("blocking_count"))) > 0
    )


def _load_previous_populated_priced_in_scan_rows(
    engine: Engine,
    *,
    include_artifacts: bool = True,
) -> list[dict[str, object]]:
    scan_date = _previous_populated_priced_in_scan_date(engine)
    if scan_date is None:
        return []
    return load_candidate_rows(
        engine,
        as_of_date=scan_date,
        limit=None,
        include_artifacts=include_artifacts,
    )


def _previous_populated_priced_in_scan_date(engine: Engine) -> date | None:
    date_expr = func.date(candidate_states.c.as_of)
    stmt = (
        select(date_expr.label("scan_date"), func.count().label("row_count"))
        .group_by(date_expr)
        .order_by(date_expr.desc())
    )
    counts: list[tuple[date, int]] = []
    with engine.connect() as conn:
        for row in conn.execute(stmt):
            scan_date = _parse_date(row.scan_date)
            if scan_date is None:
                continue
            counts.append((scan_date, int(_finite_float(row.row_count))))
    if not counts:
        return None
    max_count = max(count for _, count in counts)
    threshold = max(1, int(max_count * 0.9))
    return max(scan_date for scan_date, count in counts if count >= threshold)


def _priced_in_scan_selection_payload(
    *,
    mode: str,
    candidate_rows: Sequence[Mapping[str, object]],
    latest_run: Mapping[str, object] | None = None,
    reason: str | None = None,
) -> dict[str, object]:
    latest = _row_dict(latest_run) if isinstance(latest_run, Mapping) else {}
    selected_at = _latest_candidate_as_of(candidate_rows)
    selected_as_of = _date_iso_or_none(selected_at)
    return {
        "schema_version": "priced-in-scan-selection-v1",
        "mode": mode,
        "reason": reason,
        "latest_run_status": latest.get("status"),
        "latest_run_as_of": latest.get("as_of"),
        "latest_run_cutoff": latest.get("decision_available_at")
        or latest.get("finished_at"),
        "selected_candidate_as_of": selected_as_of,
        "selected_row_count": len(candidate_rows),
    }


def _filter_rows_to_run_universe(
    engine: Engine,
    rows: Sequence[Mapping[str, object]],
    *,
    radar_run_summary: Mapping[str, object],
    cutoff: datetime | None,
) -> list[dict[str, object]]:
    universe = str(radar_run_summary.get("universe") or "").strip()
    run_date = _parse_date(radar_run_summary.get("as_of"))
    if not universe or run_date is None or cutoff is None:
        return [_shallow_row_dict(row) for row in rows if isinstance(row, Mapping)]
    snapshot = ProviderRepository(engine).latest_universe_snapshot(
        name=universe,
        as_of=datetime(run_date.year, run_date.month, run_date.day, 21, tzinfo=UTC),
        available_at=cutoff,
    )
    if snapshot is None:
        return [_shallow_row_dict(row) for row in rows if isinstance(row, Mapping)]
    tickers = {
        member.ticker.upper()
        for member in ProviderRepository(engine).list_universe_member_rows(snapshot.id)
    }
    if not tickers:
        return []
    return [
        _shallow_row_dict(row)
        for row in rows
        if isinstance(row, Mapping)
        and str(row.get("ticker") or "").strip().upper() in tickers
    ]


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
        values = _shallow_row_dict(row)
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
        if current_rows or run_as_of is None:
            stale_context_count = max(0, len(current_rows) - len(current_run_rows))
        else:
            stale_context_count = len(load_candidate_rows(engine, available_at=cutoff))
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


def priced_in_queue_payload(
    engine: Engine,
    config: AppConfig,
    *,
    limit: int = 50,
    offset: int = 0,
    available_at: datetime | None = None,
    status: str | None = None,
    usefulness: str | None = None,
    source_gap: str | Sequence[str] | None = None,
    decision_gap: str | Sequence[str] | None = None,
    min_gap: float | None = None,
    stocks_only: bool = False,
    candidate_rows: Sequence[Mapping[str, object]] | None = None,
    total_count: int | None = None,
    include_planning_rows: bool = False,
    latest_run_summary: Mapping[str, object] | None = None,
    broker_summary: Mapping[str, object] | None = None,
    discovery_snapshot: Mapping[str, object] | None = None,
) -> dict[str, object]:
    latest_run = (
        _shallow_row_dict(latest_run_summary)
        if isinstance(latest_run_summary, Mapping)
        else load_radar_run_summary(engine)
    )
    using_supplied_rows = candidate_rows is not None
    scan_selection_mode = "latest_run"
    scan_selection_reason: str | None = None
    if candidate_rows is not None:
        queue_candidate_rows = [_shallow_row_dict(row) for row in candidate_rows]
        scan_selection_mode = "supplied_rows"
    elif available_at is not None:
        queue_candidate_rows = load_candidate_rows(
            engine,
            available_at=available_at,
            as_of_date=_parse_date(latest_run.get("as_of")) if latest_run else None,
            limit=None,
            include_artifacts=True,
            include_briefs=False,
        )
        scan_selection_mode = "requested_cutoff"
    elif latest_run:
        queue_candidate_rows = load_radar_run_candidate_rows(
            engine,
            latest_run,
            limit=None,
            include_artifacts=True,
            include_post_run_artifacts=True,
            include_briefs=False,
        )
        if _should_use_previous_priced_in_scan(latest_run, queue_candidate_rows):
            previous_rows = _load_previous_populated_priced_in_scan_rows(
                engine,
                include_artifacts=True,
            )
            if previous_rows:
                queue_candidate_rows = previous_rows
                scan_selection_mode = "previous_useful_scan"
                scan_selection_reason = "latest_run_without_priced_in_rows"
    else:
        queue_candidate_rows = load_candidate_rows(
            engine,
            limit=None,
            include_artifacts=True,
            include_briefs=False,
        )
        scan_selection_mode = "latest_candidate_rows"
    resolved_broker_summary = (
        _shallow_row_dict(broker_summary)
        if isinstance(broker_summary, Mapping)
        else load_broker_summary(engine)
    )
    queue_candidate_rows = candidate_rows_with_market_context(
        queue_candidate_rows,
        _market_context_value(resolved_broker_summary),
    )
    discovery = (
        _shallow_row_dict(discovery_snapshot)
        if isinstance(discovery_snapshot, Mapping)
        else radar_discovery_snapshot_payload(
            engine,
            config,
            radar_run_summary=latest_run,
            candidate_rows=queue_candidate_rows,
        )
    )
    wanted_status = str(status or "").strip().lower()
    wanted_usefulness, usefulness_matches = _priced_in_usefulness_filter(usefulness)
    wanted_source_gaps = _priced_in_source_gap_filter(source_gap)
    wanted_decision_gaps = _priced_in_decision_gap_filter(decision_gap)
    security_meta = _security_metadata_by_ticker(
        engine,
        [
            str(row.get("ticker") or "").strip().upper()
            for row in queue_candidate_rows
            if isinstance(row, Mapping)
        ],
    )
    rows = [
        _priced_in_queue_row(
            row,
            security_metadata=security_meta.get(
                str(row.get("ticker") or "").strip().upper()
            ),
        )
        for row in queue_candidate_rows
        if isinstance(row, Mapping)
    ]
    if wanted_status and wanted_status != "all":
        rows = [row for row in rows if _priced_in_status_matches(row, wanted_status)]
    if min_gap is not None:
        threshold = abs(float(min_gap))
        rows = [
            row
            for row in rows
            if abs(_finite_float(row.get("emotion_reaction_gap"))) >= threshold
        ]
    if usefulness_matches:
        rows = [
            row
            for row in rows
            if _priced_in_usefulness_matches(row, usefulness_matches)
        ]
    if wanted_source_gaps:
        rows = [
            row
            for row in rows
            if _priced_in_source_gap_matches(row, wanted_source_gaps)
        ]
    if wanted_decision_gaps:
        rows = [
            row
            for row in rows
            if _priced_in_decision_gap_matches(row, wanted_decision_gaps)
        ]
    if stocks_only:
        rows = [row for row in rows if _priced_in_row_is_stock_like(row)]
    rows = sorted(rows, key=_priced_in_queue_sort_key)
    loaded_total_count = len(rows)
    if (
        using_supplied_rows
        and not wanted_status
        and wanted_usefulness == "all"
        and not wanted_source_gaps
        and not wanted_decision_gaps
        and min_gap is None
        and total_count is not None
    ):
        total_count = max(_positive_offset(total_count), loaded_total_count)
    else:
        total_count = loaded_total_count
    resolved_limit = _positive_limit(limit)
    resolved_offset = _positive_offset(offset)
    page_rows = rows[resolved_offset : resolved_offset + resolved_limit]
    instrument_scope = _priced_in_instrument_scope_payload(engine, rows)
    source_coverage = priced_in_source_coverage_summary(rows, stocks_only=stocks_only)
    source_coverage = _priced_in_source_coverage_with_instrument_routes(
        engine,
        rows,
        source_coverage,
    )
    source_coverage = _priced_in_source_coverage_with_option_diagnostic(
        engine,
        rows,
        source_coverage,
    )
    status_counts = dict(Counter(str(row.get("priced_in_status") or "unknown") for row in rows))
    scan_status = _priced_in_scan_status(discovery)
    if scan_selection_mode == "previous_useful_scan":
        scan_status = "previous_scan"
    scan_selection = _priced_in_scan_selection_payload(
        mode=scan_selection_mode,
        reason=scan_selection_reason,
        latest_run=latest_run,
        candidate_rows=queue_candidate_rows,
    )
    preflight = priced_in_preflight_payload(
        engine,
        config,
        latest_run=latest_run,
        discovery_snapshot=discovery,
        source_coverage=source_coverage,
        stocks_only=stocks_only,
    )
    scan_exclusions = _priced_in_scan_exclusions_payload(engine)
    payload = {
        "schema_version": "priced-in-queue-v1",
        "status": scan_status,
        "headline": _priced_in_queue_headline(
            scan_status,
            total_count=total_count,
            returned_count=len(page_rows),
            offset=resolved_offset,
            status_filter=wanted_status or "all",
            filtered=(
                wanted_status not in {"", "all"}
                or wanted_usefulness != "all"
                or bool(wanted_source_gaps)
                or bool(wanted_decision_gaps)
                or min_gap is not None
            ),
        ),
        "next_action": _priced_in_queue_next_action(scan_status),
        "external_calls_made": 0,
        "preflight": preflight,
        "scan_selection": scan_selection,
        "scan_exclusions": scan_exclusions,
        "latest_run": _row_dict(_mapping_value(discovery, "run")),
        "scan": {
            **_row_dict(_mapping_value(discovery, "yield")),
            "freshness": _row_dict(_mapping_value(discovery, "freshness")),
        },
        "filters": {
            "status": wanted_status or "all",
            "usefulness": wanted_usefulness,
            "source_gap": list(wanted_source_gaps),
            "decision_gap": list(wanted_decision_gaps),
            "min_gap": min_gap,
            "stocks_only": bool(stocks_only),
            "limit": resolved_limit,
            "offset": resolved_offset,
            "available_at": available_at.isoformat() if available_at else None,
        },
        "count": len(page_rows),
        "returned_count": len(page_rows),
        "total_count": total_count,
        "offset": resolved_offset,
        "has_more": resolved_offset + len(page_rows) < total_count,
        "status_counts": status_counts,
        "usefulness_counts": _priced_in_usefulness_counts(rows),
        "decision_gap_counts": _priced_in_decision_gap_counts(rows),
        "source_coverage": source_coverage,
        "instrument_scope": instrument_scope,
        "rows": page_rows,
    }
    if include_planning_rows:
        payload["planning_rows"] = rows
    return payload


def _priced_in_scan_exclusions_payload(engine: Engine):
    rows: list[dict[str, object]] = []
    try:
        with engine.connect() as conn:
            rows = [
                dict(row)
                for row in conn.execute(
                    select(securities.c.ticker, securities.c.name)
                    .where(
                        securities.c.is_active.is_(True),
                        securities.c.ticker.in_(
                            sorted(PRICED_IN_SCAN_EXCLUDED_TICKERS)
                        ),
                    )
                    .order_by(securities.c.ticker)
                ).mappings()
            ]
    except SQLAlchemyError:
        rows = []
    tickers = [str(row.get("ticker") or "").strip().upper() for row in rows]
    tickers = [ticker for ticker in tickers if ticker]
    return {
        "schema_version": "priced-in-scan-exclusions-v1",
        "reason": "benchmark_reference_tickers",
        "count": len(tickers),
        "tickers": tickers,
        "rows": rows,
        "operator_note": (
            "Benchmark ETFs are used for relative-strength context and are "
            "intentionally excluded from candidate scoring. They should not "
            "block the trusted-answer gate when every real evidence gap is clear."
        ),
        "external_calls_made": 0,
    }


def priced_in_source_gap_batches_payload(
    engine: Engine,
    config: AppConfig,
    *,
    source: str,
    batch_limit: int = 5,
    batch_offset: int = 0,
    batch_size: int | None = None,
    all_batches: bool = False,
    available_at: datetime | None = None,
    status: str | None = None,
    usefulness: str | None = None,
    decision_gap: str | Sequence[str] | None = None,
    min_gap: float | None = None,
    stocks_only: bool = False,
    queue: Mapping[str, object] | None = None,
) -> dict[str, object]:
    source_name = _single_priced_in_source(source)
    max_batch_size = _priced_in_source_max_batch_size(source_name, config)
    requested_batch_size = (
        max_batch_size if batch_size is None else _positive_limit(batch_size)
    )
    resolved_batch_size = min(requested_batch_size, max_batch_size)
    requested_limit = _positive_limit(batch_limit)
    requested_offset = _positive_offset(batch_offset)
    resolved_offset = 0 if all_batches else requested_offset
    using_supplied_queue = isinstance(queue, Mapping)
    resolved_queue = (
        _row_dict(queue)
        if using_supplied_queue
        else priced_in_queue_payload(
            engine,
            config,
            limit=1_000_000,
            offset=0,
            available_at=available_at,
            status=status,
            usefulness=usefulness,
            source_gap=None if source_name == "market_bars" else source_name,
            decision_gap=decision_gap,
            min_gap=min_gap,
            stocks_only=stocks_only,
        )
    )
    resolved_filters = _mapping_value(resolved_queue, "filters")
    resolved_stocks_only = bool(stocks_only or resolved_filters.get("stocks_only"))
    if source_name == "market_bars":
        return _priced_in_market_bar_source_gap_plan(
            engine,
            config,
            queue=resolved_queue,
            stocks_only=resolved_stocks_only,
            batch_limit=requested_limit,
            batch_offset=requested_offset,
            batch_size=resolved_batch_size,
            requested_batch_size=requested_batch_size,
            max_batch_size=max_batch_size,
            all_batches=all_batches,
        )
    planning_rows = (
        resolved_queue.get("planning_rows")
        if using_supplied_queue
        and isinstance(resolved_queue.get("planning_rows"), list | tuple)
        else resolved_queue.get("rows", [])
    )
    rows = [
        row
        for row in planning_rows
        if isinstance(row, Mapping) and str(row.get("ticker") or "").strip()
    ]
    if using_supplied_queue:
        rows = [
            row
            for row in rows
            if _priced_in_source_gap_matches(row, (source_name,))
        ]
    all_gap_tickers = [str(row["ticker"]).strip().upper() for row in rows]
    batchable = source_name in PRICED_IN_BATCHABLE_SOURCES
    plan_rows, diagnostic = _priced_in_source_plannable_rows(
        engine,
        source_name=source_name,
        rows=rows,
        stocks_only=resolved_stocks_only,
    )
    plan_rows = sorted(plan_rows, key=_priced_in_source_row_priority_key)
    tickers = [str(row["ticker"]).strip().upper() for row in plan_rows]
    batch_count = ceil(len(tickers) / resolved_batch_size) if batchable and tickers else 0
    scan_as_of = _priced_in_batch_as_of(rows)
    planned_at = datetime.now(UTC).replace(microsecond=0)
    planned_available_at = planned_at.isoformat()
    resolved_limit = max(batch_count, 1) if all_batches else requested_limit
    batches = []
    if batchable:
        for index in range(
            resolved_offset,
            min(batch_count, resolved_offset + resolved_limit),
        ):
            row_start = index * resolved_batch_size
            row_end = min(row_start + resolved_batch_size, len(tickers))
            batch_tickers = tickers[row_start:row_end]
            batch_targets = _priced_in_source_batch_targets(
                engine,
                source_name=source_name,
                tickers=batch_tickers,
            )
            call_budget = _priced_in_source_batch_call_budget(
                config,
                source_name=source_name,
                ticker_count=len(batch_tickers),
            )
            batches.append(
                {
                    "index": index,
                    "number": index + 1,
                    "row_start": row_start + 1,
                    "row_end": row_end,
                    "tickers": batch_tickers,
                    "targets": batch_targets,
                    "command": _priced_in_source_batch_command(
                        source_name,
                        batch_tickers,
                        scan_as_of=scan_as_of,
                        planned_available_at=planned_available_at,
                        targets=batch_targets,
                    ),
                    "api": _priced_in_source_batch_api(source_name),
                    "api_payload": _priced_in_source_batch_api_payload(
                        source_name,
                        batch_tickers,
                        scan_as_of=scan_as_of,
                        planned_available_at=planned_available_at,
                        targets=batch_targets,
                    ),
                    **call_budget,
                }
            )
    returned_ticker_count = sum(
        len(_sequence_value(batch.get("tickers")))
        for batch in batches
        if isinstance(batch, Mapping)
    )
    returned_batch_start = resolved_offset + 1 if batches else None
    returned_batch_end = resolved_offset + len(batches) if batches else None
    status_value = (
        "ready"
        if batchable and tickers
        else "no_gaps"
        if not all_gap_tickers
        else "routed"
        if (
            source_name == "catalyst_events"
            and str(diagnostic.get("status") or "") == "routed"
        )
        else "blocked"
        if batchable
        else "not_batchable"
    )
    next_action = _priced_in_source_batches_next_action(
        source_name=source_name,
        batchable=batchable,
        total_gap_rows=len(all_gap_tickers),
        plannable_gap_rows=len(tickers),
    )
    if status_value in {"blocked", "routed"}:
        diagnostic_action = str(diagnostic.get("next_action") or "").strip()
        if diagnostic_action:
            next_action = diagnostic_action
    review_rows_command = _priced_in_queue_source_gap_command(
        source_name,
        stocks_only=resolved_stocks_only,
        limit=50,
    )
    export_rows_command = _priced_in_queue_source_gap_command(
        source_name,
        stocks_only=resolved_stocks_only,
        all_rows=True,
    )
    all_batches_command = (
        _priced_in_source_batches_command(
            source_name,
            stocks_only=resolved_stocks_only,
            all_batches=True,
            json=True,
        )
        if batch_count > 0
        else None
    )
    execute_next_command = (
        _priced_in_source_batches_command(
            source_name,
            stocks_only=resolved_stocks_only,
            execute_next=True,
        )
        if batches
        else None
    )
    execute_batches_command = (
        _priced_in_source_batches_command(
            source_name,
            stocks_only=resolved_stocks_only,
            execute_batches=3,
        )
        if batches
        else None
    )
    execute_batches_api = (
        (
            "POST /api/radar/priced-in/source-batches/execute-next "
            '{"source":"'
            f'{source_name}","max_batches":3'
        )
        + (',"stocks_only":true' if resolved_stocks_only else "")
        + "}"
        if batches
        else None
    )
    all_batches_api = (
        _priced_in_source_batches_api(
            source_name,
            stocks_only=resolved_stocks_only,
            all_batches=True,
        )
        if batch_count > 0
        else None
    )
    plan_command = all_batches_command or (
        review_rows_command if all_gap_tickers else None
    )
    plan_api = all_batches_api if all_batches_command else None
    if source_name == "catalyst_events" and diagnostic.get("manual_fix_command"):
        diagnostic = {
            **diagnostic,
            "manual_template_command": (
                "catalyst-radar ingest-sec cik-overrides-template "
                "--out data\\local\\cik-overrides-template.csv"
                + (" --stocks-only" if resolved_stocks_only else "")
            ),
            "manual_template_api": (
                "GET /api/radar/sec/cik-overrides-template"
                + ("?stocks_only=true" if resolved_stocks_only else "")
            ),
        }
    first_batch = next((batch for batch in batches if isinstance(batch, Mapping)), None)
    returned_tickers_are_batch_sample = returned_ticker_count < len(tickers)
    batch_preview_note = (
        f"Returned tickers are the next source-fill batch preview: "
        f"{returned_ticker_count} of {len(tickers)} plannable row(s), from "
        f"{len(all_gap_tickers)} full-scan {source_name} gap row(s). This is not "
        "the scan universe."
        if returned_tickers_are_batch_sample
        else (
            f"Returned tickers cover every currently returned provider batch for "
            f"{source_name}. Full-scan gap rows: {len(all_gap_tickers)}. This "
            "is not the scan universe."
        )
    )
    approval_checklist = _priced_in_source_batch_approval_checklist(
        source_name=source_name,
        status=status_value,
        total_gap_rows=len(all_gap_tickers),
        batch_count=batch_count,
        batch_size=resolved_batch_size,
        first_batch=first_batch,
        review_rows_command=review_rows_command,
        all_batches_command=all_batches_command,
        execute_next_command=execute_next_command,
        execute_batches_command=execute_batches_command,
    )
    return {
        "schema_version": "priced-in-source-batches-v1",
        "status": status_value,
        "source": source_name,
        "external_calls_made": 0,
        "planned_at": planned_at.isoformat(),
        "execution_boundary": (
            "Plan only. This command does not call providers; each listed batch command "
            "is an explicit read-only sync and remains rate-limited."
        ),
        "scan_scope": {
            "mode": "full_scan",
            "source_gap": source_name,
            "stocks_only": resolved_stocks_only,
            "instrument_filter": (
                "stocks_only" if resolved_stocks_only else "all_instruments"
            ),
            "full_scan_gap_rows": len(all_gap_tickers),
            "plannable_rows": len(tickers),
            "planned_batches": batch_count,
            "batch_size": resolved_batch_size,
            "returned_batches": len(batches),
            "returned_batch_start": returned_batch_start,
            "returned_batch_end": returned_batch_end,
            "returned_tickers": returned_ticker_count,
            "tickers_are_batch_sample": returned_tickers_are_batch_sample,
            "returned_ticker_scope": (
                "next_provider_batch_preview"
                if returned_tickers_are_batch_sample
                else "returned_provider_batches"
            ),
            "batch_preview_note": batch_preview_note,
            "explanation": (
                "The full scan covers every matching ranked row. The tickers shown "
                "here are only the returned rate-limited source-fill batch(es); use "
                "all_batches_command to list the complete full-scan batch plan."
            ),
        },
        "headline": _priced_in_source_batches_headline(
            source_name=source_name,
            batchable=batchable,
            total_gap_rows=len(all_gap_tickers),
            plannable_gap_rows=len(tickers),
            batch_count=batch_count,
            batch_size=resolved_batch_size,
        ),
        "next_action": next_action,
        "filters": {
            **_row_dict(_mapping_value(resolved_queue, "filters")),
            "source_gap": [source_name],
            "batch_limit": resolved_limit,
            "batch_offset": resolved_offset,
            "requested_batch_limit": requested_limit,
            "requested_batch_offset": requested_offset,
            "all_batches": all_batches,
            "batch_size": resolved_batch_size,
            "requested_batch_size": requested_batch_size,
            "max_batch_size": max_batch_size,
        },
        "total_gap_rows": len(all_gap_tickers),
        "plannable_gap_rows": len(tickers),
        "unplannable_gap_rows": max(0, len(all_gap_tickers) - len(tickers)),
        "routed_gap_rows": int(_finite_float(diagnostic.get("routed_non_company_rows"))),
        "blocked_gap_rows": max(
            0,
            max(0, len(all_gap_tickers) - len(tickers))
            - int(_finite_float(diagnostic.get("routed_non_company_rows"))),
        ),
        "diagnostic": diagnostic,
        "batch_size": resolved_batch_size,
        "batch_count": batch_count,
        "batch_offset": resolved_offset,
        "batch_limit": resolved_limit,
        "all_batches": all_batches,
        "approval_checklist": approval_checklist,
        "count": len(batches),
        "has_more": resolved_offset + len(batches) < batch_count,
        "review_rows_command": review_rows_command,
        "export_rows_command": export_rows_command,
        "all_batches_command": all_batches_command,
        "execute_next_command": execute_next_command,
        "execute_batches_command": execute_batches_command,
        "execute_batches_api": execute_batches_api,
        "all_batches_api": all_batches_api,
        "command": plan_command,
        "plan_command": plan_command,
        "plan_api": plan_api,
        "next_batch_command": _priced_in_source_next_batch_command(
            source_name=source_name,
            stocks_only=resolved_stocks_only,
            batch_limit=resolved_limit,
            batch_offset=resolved_offset + len(batches),
            batch_count=batch_count,
            all_batches=all_batches,
        ),
        "batches": batches,
    }


def _priced_in_market_bar_source_gap_plan(
    engine: Engine,
    config: AppConfig,
    *,
    queue: Mapping[str, object],
    stocks_only: bool,
    batch_limit: int,
    batch_offset: int,
    batch_size: int,
    requested_batch_size: int,
    max_batch_size: int,
    all_batches: bool,
) -> dict[str, object]:
    source_name = "market_bars"
    latest_run = _mapping_value(queue, "latest_run")
    freshness = _mapping_value(_mapping_value(queue, "scan"), "freshness")
    planning_rows = _sequence_value(queue.get("planning_rows")) or _sequence_value(
        queue.get("rows")
    )
    target_as_of = (
        _parse_date(latest_run.get("as_of"))
        or _parse_date(freshness.get("latest_candidate_session_date"))
        or _parse_date(freshness.get("latest_daily_bar_date"))
        or _parse_date(_priced_in_batch_as_of(planning_rows))
    )
    planned_at = datetime.now(UTC).replace(microsecond=0)
    if stocks_only:
        stock_scope = _priced_in_market_bar_stock_scope(
            engine,
            target_as_of=target_as_of,
        )
        active = int(_finite_float(stock_scope.get("stock_like_active")))
        available = int(_finite_float(stock_scope.get("stock_like_with_as_of_bar")))
        missing = int(_finite_float(stock_scope.get("stock_like_missing_as_of_bar")))
        sample_tickers = [
            str(ticker).strip().upper()
            for ticker in _sequence_value(
                stock_scope.get("sample_missing_stock_like_tickers")
            )
            if str(ticker).strip()
        ]
        coverage_basis = "stock_like_active_as_of_bars"
        blocked_reason = "missing_stock_like_as_of_bars"
        next_action = (
            str(stock_scope.get("next_action") or "").strip()
            or "Fill stock-like missing as-of bars before trusting the stocks-only scan."
        )
        headline = (
            "Stock-like market bars are complete for the stocks-only scan."
            if missing <= 0
            else f"{missing} stock-like active row(s) are missing as-of market bars."
        )
        explanation = (
            "The stocks-only full scan requires an as-of market bar for each "
            "active common stock or ADR before the source-batch overview can "
            "claim complete price-reaction coverage."
        )
    else:
        active_scope = _priced_in_active_market_bar_scope(
            engine,
            queue=queue,
            target_as_of=target_as_of,
        )
        active = int(_finite_float(active_scope.get("active_securities")))
        available = int(_finite_float(active_scope.get("with_as_of_bar")))
        missing = int(_finite_float(active_scope.get("missing_as_of_bar")))
        sample_tickers = [
            str(ticker).strip().upper()
            for ticker in _sequence_value(active_scope.get("sample_missing_tickers"))
            if str(ticker).strip()
        ]
        coverage_basis = "active_universe_as_of_bars"
        blocked_reason = "missing_active_as_of_bars"
        next_action = (
            "Fill missing as-of bars for the active universe; then rerun the "
            "full priced-in scan."
            if missing
            else "Active-universe rows have as-of market bars."
        )
        headline = (
            "Active-universe market bars are complete for the full scan."
            if missing <= 0
            else f"{missing} active row(s) are missing as-of market bars."
        )
        explanation = (
            "The full scan requires an as-of market bar for each active security "
            "before the source-batch overview can claim complete price-reaction "
            "coverage."
        )
    provider_plan = _priced_in_market_bar_provider_fill_plan(
        engine,
        config,
        target_as_of=target_as_of,
        missing=missing,
        active_security_count=active,
        existing_as_of_bar_count=available,
        coverage_scope="stock_like" if stocks_only else "active_universe",
        missing_as_of_bar_ticker_sample=sample_tickers,
    )
    template_command = _csv_market_template_command(
        target_as_of,
        missing_only=True,
        stocks_only=stocks_only,
    )
    template_path = _csv_market_template_path(
        target_as_of,
        stocks_only=stocks_only,
    )
    preview_command = _csv_market_refresh_command(
        target_as_of,
        daily_bars_path=template_path,
        execute=False,
        stocks_only=stocks_only,
    )
    import_command = _csv_market_refresh_command(
        target_as_of,
        daily_bars_path=template_path,
        execute=True,
        stocks_only=stocks_only,
    )
    repair_context = _priced_in_market_bar_source_repair_context(
        engine,
        config,
        target_as_of=target_as_of,
        stocks_only=stocks_only,
        missing=missing,
    )
    status_value = (
        "no_gaps"
        if missing <= 0
        else "blocked"
        if target_as_of is None or active <= 0
        else "attention"
    )
    diagnostic = {
        "schema_version": "priced-in-market-bar-source-gap-diagnostic-v1",
        "status": status_value,
        "eligible_rows": 0,
        "blocked_rows": missing,
        "blocked_reason": blocked_reason if missing else None,
        "reason": (
            f"{available}/{active} "
            f"{'stock-like active' if stocks_only else 'active'} row(s) have an as-of "
            "market bar for the scan date."
            if active
            else (
                "No stock-like active universe is available for market-bar planning."
                if stocks_only
                else "No active universe is available for market-bar planning."
            )
        ),
        "sample_blocked_tickers": sample_tickers,
        "fix_command": template_command if missing else None,
        "manual_template_command": template_command if missing else None,
        "manual_validate_command": preview_command if missing else None,
        "manual_fix_command": import_command if missing else None,
        "required_fill_fields": list(MANUAL_BAR_REQUIRED_FILL_FIELDS),
        "blank_required_field_counts_if_new_template": {
            field_name: missing for field_name in MANUAL_BAR_REQUIRED_FILL_FIELDS
        }
        if missing
        else {},
        "template_row_count": missing,
        "provider_fill_command": provider_plan.get("provider_call_command"),
        "provider_fill_status": provider_plan.get("status"),
        "provider_fill_external_call_count": provider_plan.get(
            "execute_external_call_count"
        ),
        "provider_saved_file_path": provider_plan.get("provider_saved_file_path"),
        "provider_saved_file_exists": provider_plan.get("provider_saved_file_exists"),
        "provider_saved_file_status": provider_plan.get("provider_saved_file_status"),
        "provider_saved_file_next_action": provider_plan.get(
            "provider_saved_file_next_action"
        ),
        "provider_saved_file_capture_command": provider_plan.get(
            "provider_saved_file_capture_command"
        ),
        "provider_saved_file_capture_external_call_count": provider_plan.get(
            "provider_saved_file_capture_external_call_count"
        ),
        "provider_saved_file_capture_approval_packet": provider_plan.get(
            "provider_saved_file_capture_approval_packet"
        ),
        "provider_saved_file_validate_command": provider_plan.get(
            "provider_saved_file_validate_command"
        ),
        "provider_saved_file_import_command": provider_plan.get(
            "provider_saved_file_import_command"
        ),
        "provider_saved_file_external_call_count": provider_plan.get(
            "provider_saved_file_external_call_count"
        ),
        "provider_saved_file_boundary": provider_plan.get(
            "provider_saved_file_boundary"
        ),
        **repair_context,
        "external_calls_made": 0,
    }
    approval_checklist = _priced_in_source_batch_approval_checklist(
        source_name=source_name,
        status=status_value,
        total_gap_rows=missing,
        batch_count=0,
        batch_size=batch_size,
        first_batch=None,
        review_rows_command=template_command,
        all_batches_command=None,
        execute_next_command=None,
        execute_batches_command=None,
    )
    return {
        "schema_version": "priced-in-source-batches-v1",
        "status": status_value,
        "source": source_name,
        "coverage_basis": coverage_basis,
        "external_calls_made": 0,
        "planned_at": planned_at.isoformat(),
        "execution_boundary": (
            "Plan only. This market-bar source plan makes no provider calls. "
            "Template generation writes a local CSV; import preview makes no DB "
            "writes; import --execute writes local daily bars only."
        ),
        "scan_scope": {
            "mode": "full_scan",
            "source_gap": source_name,
            "stocks_only": stocks_only,
            "instrument_filter": "stocks_only" if stocks_only else "all_instruments",
            "full_scan_gap_rows": missing,
            "plannable_rows": 0,
            "planned_batches": 0,
            "batch_size": batch_size,
            "returned_batches": 0,
            "returned_batch_start": None,
            "returned_batch_end": None,
            "returned_tickers": 0,
            "tickers_are_batch_sample": False,
            "returned_ticker_scope": "manual_repair_template",
            "stock_like_active": active,
            "stock_like_with_as_of_bar": available if stocks_only else None,
            "stock_like_missing_as_of_bar": missing if stocks_only else None,
            "active_securities": active,
            "with_as_of_bar": available,
            "missing_as_of_bar": missing,
            "coverage_basis": coverage_basis,
            "batch_preview_note": (
                "Market-bar gaps are missing scan-universe rows, not provider "
                "source-fill batches."
            ),
            "explanation": explanation,
        },
        "headline": headline,
        "next_action": next_action,
        "filters": {
            **_row_dict(_mapping_value(queue, "filters")),
            "source_gap": [source_name],
            "batch_limit": batch_limit,
            "batch_offset": batch_offset,
            "requested_batch_limit": batch_limit,
            "requested_batch_offset": batch_offset,
            "all_batches": all_batches,
            "batch_size": batch_size,
            "requested_batch_size": requested_batch_size,
            "max_batch_size": max_batch_size,
        },
        "total_gap_rows": missing,
        "plannable_gap_rows": 0,
        "unplannable_gap_rows": missing,
        "routed_gap_rows": 0,
        "blocked_gap_rows": missing,
        "diagnostic": diagnostic,
        "batch_size": batch_size,
        "batch_count": 0,
        "batch_offset": batch_offset,
        "batch_limit": batch_limit,
        "all_batches": all_batches,
        "approval_checklist": approval_checklist,
        "count": 0,
        "has_more": False,
        "review_rows_command": template_command if missing else None,
        "export_rows_command": template_command if missing else None,
        "all_batches_command": None,
        "execute_next_command": None,
        "execute_batches_command": None,
        "execute_batches_api": None,
        "all_batches_api": None,
        "command": template_command if missing else None,
        "plan_command": template_command if missing else None,
        "plan_api": None,
        "next_batch_command": None,
        "batches": [],
    }


def _priced_in_active_market_bar_scope(
    engine: Engine,
    *,
    queue: Mapping[str, object],
    target_as_of: date | None,
) -> dict[str, object]:
    freshness = _mapping_value(_mapping_value(queue, "scan"), "freshness")
    fallback_active = int(_finite_float(freshness.get("active_security_count")))
    if fallback_active <= 0:
        fallback_active = int(_finite_float(queue.get("total_count")))
    fallback_available = int(
        _finite_float(freshness.get("active_security_with_as_of_bar_count"))
    )
    fallback_missing = int(_finite_float(freshness.get("missing_as_of_daily_bar_count")))
    if fallback_available <= 0 and fallback_missing <= 0 and fallback_active > 0:
        fallback_available = fallback_active
    fallback_samples = [
        str(ticker).strip().upper()
        for ticker in _sequence_value(freshness.get("missing_as_of_daily_bar_tickers"))
        if str(ticker).strip()
    ]
    if target_as_of is None:
        return {
            "active_securities": fallback_active,
            "with_as_of_bar": fallback_available,
            "missing_as_of_bar": fallback_missing,
            "sample_missing_tickers": _sample_tickers(fallback_samples),
        }
    try:
        with engine.connect() as conn:
            active_tickers = {
                str(row._mapping["ticker"]).strip().upper()
                for row in conn.execute(
                    select(securities.c.ticker).where(securities.c.is_active.is_(True))
                )
                if str(row._mapping["ticker"]).strip()
            }
            covered = {
                str(row._mapping["ticker"]).strip().upper()
                for row in conn.execute(
                    select(daily_bars.c.ticker).where(
                        daily_bars.c.date == target_as_of
                    )
                )
                if str(row._mapping["ticker"]).strip()
            }
    except SQLAlchemyError:
        return {
            "active_securities": fallback_active,
            "with_as_of_bar": fallback_available,
            "missing_as_of_bar": fallback_missing,
            "sample_missing_tickers": _sample_tickers(fallback_samples),
        }
    missing_tickers = sorted(active_tickers - covered)
    return {
        "active_securities": len(active_tickers),
        "with_as_of_bar": len(active_tickers) - len(missing_tickers),
        "missing_as_of_bar": len(missing_tickers),
        "sample_missing_tickers": _sample_tickers(missing_tickers),
    }


def sec_cik_override_template_payload(
    engine: Engine,
    config: AppConfig,
    *,
    available_at: datetime | None = None,
    status: str | None = None,
    usefulness: str | None = None,
    decision_gap: str | Sequence[str] | None = None,
    min_gap: float | None = None,
    stocks_only: bool = False,
) -> dict[str, object]:
    source_name = "catalyst_events"
    queue = priced_in_queue_payload(
        engine,
        config,
        limit=1_000_000,
        offset=0,
        available_at=available_at,
        status=status,
        usefulness=usefulness,
        source_gap=source_name,
        decision_gap=decision_gap,
        min_gap=min_gap,
        stocks_only=stocks_only,
    )
    rows = [
        row
        for row in _sequence_value(queue.get("rows"))
        if isinstance(row, Mapping)
        and str(row.get("ticker") or "").strip()
        and _priced_in_source_gap_matches(row, (source_name,))
    ]
    rows = sorted(rows, key=_priced_in_source_row_priority_key)
    tickers = [
        str(row.get("ticker") or "").strip().upper()
        for row in rows
        if str(row.get("ticker") or "").strip()
    ]
    security_meta = _security_metadata_by_ticker(engine, tickers)
    cik_by_ticker = _security_cik_by_ticker(engine, tickers)
    template_rows: list[dict[str, object]] = []
    skipped_with_cik: list[str] = []
    routed_non_company: list[str] = []
    unknown_type: list[str] = []
    for ticker in dict.fromkeys(tickers):
        security_type = _security_type_for_scope(security_meta.get(ticker))
        if ticker in cik_by_ticker:
            skipped_with_cik.append(ticker)
            continue
        if _is_non_company_instrument_type(security_type):
            routed_non_company.append(ticker)
            continue
        if security_type == "UNKNOWN":
            unknown_type.append(ticker)
        template_rows.append(
            {
                "ticker": ticker,
                "cik": "",
                "sec_company_name": "",
                "security_type": security_type,
                "template_reason": (
                    "missing_sec_cik_for_catalyst_events_source_gap"
                ),
            }
        )
    row_count = len(template_rows)
    status_value = "ready" if row_count else "empty"
    command = (
        "catalyst-radar ingest-sec cik-overrides-template "
        "--out data\\local\\cik-overrides-template.csv"
        + (" --stocks-only" if stocks_only else "")
    )
    api = "GET /api/radar/sec/cik-overrides-template" + (
        "?stocks_only=true" if stocks_only else ""
    )
    return {
        "schema_version": "sec-cik-override-template-v1",
        "status": status_value,
        "provider": "manual",
        "live": False,
        "external_calls_made": 0,
        "source": source_name,
        "stocks_only": bool(stocks_only),
        "source_gap_rows": len(rows),
        "row_count": row_count,
        "columns": [
            "ticker",
            "cik",
            "sec_company_name",
            "security_type",
            "template_reason",
        ],
        "rows": template_rows,
        "sample_tickers": _sample_tickers(
            [str(row.get("ticker") or "") for row in template_rows]
        ),
        "skipped_with_cik_count": len(skipped_with_cik),
        "sample_skipped_with_cik_tickers": _sample_tickers(skipped_with_cik),
        "routed_non_company_count": len(routed_non_company),
        "sample_routed_non_company_tickers": _sample_tickers(routed_non_company),
        "unknown_type_count": len(unknown_type),
        "sample_unknown_type_tickers": _sample_tickers(unknown_type),
        "command": command,
        "api": api,
        "import_command": (
            "catalyst-radar ingest-sec cik-overrides "
            "--csv data\\local\\cik-overrides-template.csv"
        ),
        "validate_command": (
            "catalyst-radar ingest-sec cik-overrides "
            "--csv data\\local\\cik-overrides-template.csv --validate-only"
        ),
        "next_action": (
            "Fill cik and optional sec_company_name for each row, validate the "
            "completed CSV, then import it before replanning catalyst_events."
            if row_count
            else "No missing company-like SEC CIK blockers need a manual template."
        ),
        "boundary": (
            "Template/export is zero-call. Do not guess CIKs; use exact SEC CIKs "
            "or an explicitly approved SEC company-tickers refresh."
        ),
    }


def options_fixture_template_payload(
    engine: Engine,
    config: AppConfig,
    *,
    available_at: datetime | None = None,
    status: str | None = None,
    usefulness: str | None = None,
    decision_gap: str | Sequence[str] | None = None,
    min_gap: float | None = None,
    stocks_only: bool = False,
) -> dict[str, object]:
    source_name = "options"
    queue = priced_in_queue_payload(
        engine,
        config,
        limit=1_000_000,
        offset=0,
        available_at=available_at,
        status=status,
        usefulness=usefulness,
        source_gap=source_name,
        decision_gap=decision_gap,
        min_gap=min_gap,
        stocks_only=stocks_only,
    )
    rows = [
        row
        for row in _sequence_value(queue.get("rows"))
        if isinstance(row, Mapping)
        and str(row.get("ticker") or "").strip()
        and _priced_in_source_gap_matches(row, (source_name,))
    ]
    rows = sorted(rows, key=_priced_in_source_row_priority_key)
    unique_rows = list(
        {
            str(row.get("ticker") or "").strip().upper(): row
            for row in rows
            if str(row.get("ticker") or "").strip()
        }.items()
    )
    coverage = _mapping_value(queue, "source_coverage")
    diagnostic = _mapping_value(coverage, "options_gap_diagnostic")
    if not diagnostic and rows:
        diagnostic = _priced_in_option_gap_diagnostic(engine, rows)
    target_as_of = _options_fixture_template_target_as_of(rows, diagnostic)
    target_date = _options_fixture_template_target_date(target_as_of, diagnostic)
    default_path = f"data\\local\\point-in-time-options-{target_date}.json"
    result_rows = [
        {
            "ticker": ticker,
            "call_volume": "",
            "put_volume": "",
            "call_open_interest": "",
            "put_open_interest": "",
            "iv_percentile": "",
            "skew": "",
        }
        for ticker, _row in unique_rows
    ]
    fixture = {
        "as_of": target_as_of,
        "source_ts": target_as_of,
        "available_at": target_as_of,
        "provider": "options_fixture",
        "results": result_rows,
    }
    row_count = len(result_rows)
    query = "?stocks_only=true" if stocks_only else ""
    return {
        "schema_version": "options-fixture-template-v1",
        "status": "ready" if row_count else "empty",
        "provider": "manual",
        "live": False,
        "external_calls_made": 0,
        "source": source_name,
        "stocks_only": bool(stocks_only),
        "source_gap_rows": len(rows),
        "row_count": row_count,
        "target_as_of": target_as_of,
        "target_date": target_date,
        "columns": list(OPTIONS_FIXTURE_TEMPLATE_RESULT_FIELDS),
        "fixture": fixture,
        "sample_tickers": _sample_tickers([ticker for ticker, _row in unique_rows]),
        "command": _options_point_in_time_template_command(
            diagnostic,
            stocks_only=stocks_only,
        ),
        "api": f"GET /api/radar/options/fixture-template{query}",
        "validation_command": (
            f"catalyst-radar ingest-options --fixture {default_path} "
            f"--validate-only --expected-as-of {target_date}"
        ),
        "validation_api": "POST /api/radar/options/fixture-validate",
        "import_command": f"catalyst-radar ingest-options --fixture {default_path}",
        "next_action": (
            "Fill the aggregate option fields for each ticker, then import the "
            "completed point-in-time fixture before replanning options."
            if row_count
            else "No options source-gap rows need a point-in-time fixture template."
        ),
        "boundary": (
            "Template/export is zero-call. Values must describe option context "
            "available at the scan date; do not backfill current chains into an "
            "older scan."
        ),
    }


def priced_in_all_source_gap_batches_payload(
    engine: Engine,
    config: AppConfig,
    *,
    batch_size: int | None = None,
    available_at: datetime | None = None,
    status: str | None = None,
    usefulness: str | None = None,
    decision_gap: str | Sequence[str] | None = None,
    min_gap: float | None = None,
    stocks_only: bool = False,
) -> dict[str, object]:
    queue = priced_in_queue_payload(
        engine,
        config,
        limit=1_000_000,
        offset=0,
        available_at=available_at,
        status=status,
        usefulness=usefulness,
        decision_gap=decision_gap,
        min_gap=min_gap,
        stocks_only=stocks_only,
    )
    priority_counts = _priced_in_source_gap_priority_counts(
        _sequence_value(queue.get("rows"))
    )
    rows: list[dict[str, object]] = []
    for source in PRICED_IN_SOURCE_CLASSES:
        plan = priced_in_source_gap_batches_payload(
            engine,
            config,
            source=source,
            batch_limit=1,
            batch_size=batch_size,
            available_at=available_at,
            status=status,
            usefulness=usefulness,
            decision_gap=decision_gap,
            min_gap=min_gap,
            stocks_only=stocks_only,
            queue=queue,
        )
        rows.append(
            _priced_in_all_source_batch_row(
                plan,
                priority_counts=priority_counts.get(source),
            )
        )
    market_bars_for_scope = _priced_in_market_bars_scope_from_source_rows(rows)
    total_gap_rows = sum(int(_finite_float(row.get("total_gap_rows"))) for row in rows)
    ready_rows = [row for row in rows if str(row.get("status") or "") == "ready"]
    blocked_rows = [
        row
        for row in rows
        if int(_finite_float(row.get("total_gap_rows"))) > 0
        and str(row.get("status") or "") != "ready"
    ]
    has_market_bar_blocker = any(
        str(row.get("source") or "") == "market_bars"
        and int(_finite_float(row.get("total_gap_rows"))) > 0
        for row in blocked_rows
    )
    status_value = (
        "attention"
        if has_market_bar_blocker
        else "ready"
        if ready_rows
        else "blocked"
        if blocked_rows
        else "complete"
    )
    coverage_recommendation = _priced_in_all_source_coverage_recommendation(
        status=status_value,
        ready_rows=ready_rows,
        blocked_rows=blocked_rows,
    )
    decision_shortcut_blocker = _priced_in_decision_shortcut_blocker(
        coverage_recommendation
    )
    decision_recommendation = (
        None
        if decision_shortcut_blocker
        else _priced_in_all_source_decision_recommendation(
            status=status_value,
            ready_rows=ready_rows,
            blocked_rows=blocked_rows,
        )
    )
    goal_alignment = _priced_in_all_source_goal_alignment(
        queue,
        total_gap_rows=total_gap_rows,
        coverage_recommendation=coverage_recommendation,
        decision_recommendation=decision_recommendation or {},
        stocks_only=stocks_only,
        market_bars=market_bars_for_scope,
    )
    scan_scope = _priced_in_all_source_overview_scan_scope(
        queue,
        source_count=len(rows),
        total_gap_rows=total_gap_rows,
        stocks_only=stocks_only,
        market_bars=market_bars_for_scope,
    )
    mission_brief = _priced_in_all_source_mission_brief(
        status=status_value,
        scan_scope=scan_scope,
        goal_alignment=goal_alignment,
        coverage_recommendation=coverage_recommendation,
        decision_shortcut_blocker=decision_shortcut_blocker,
        rows=rows,
    )
    source_execution_gate = _priced_in_all_source_execution_gate(
        coverage_recommendation,
        ready_rows=ready_rows,
    )
    return {
        "schema_version": "priced-in-source-batch-overview-v1",
        "status": status_value,
        "headline": _priced_in_all_source_batches_headline(
            source_count=len(rows),
            ready_count=len(ready_rows),
            blocked_count=len(blocked_rows),
            total_gap_rows=total_gap_rows,
        ),
        "next_action": coverage_recommendation.get("action"),
        "scan_scope": scan_scope,
        "mission_brief": mission_brief,
        "goal_alignment": goal_alignment,
        "coverage_first_recommendation": coverage_recommendation,
        "decision_shortcut_recommendation": decision_recommendation,
        "decision_shortcut_blocker": decision_shortcut_blocker,
        "source_execution_gate": source_execution_gate,
        "external_calls_made": 0,
        "execution_boundary": _priced_in_all_source_execution_boundary(
            source_execution_gate
        ),
        "source_count": len(rows),
        "ready_source_count": len(ready_rows),
        "blocked_source_count": len(blocked_rows),
        "total_gap_rows": total_gap_rows,
        "sources": rows,
    }



def _priced_in_all_source_execution_boundary(
    source_execution_gate: Mapping[str, object],
) -> str:
    if str(source_execution_gate.get("status") or "") == "blocked":
        blocked_by = str(source_execution_gate.get("blocked_by") or "source gate")
        return (
            "Plan only. This overview makes no provider calls and does not "
            f"execute sources. Source execution is blocked by {blocked_by} "
            "until the gate clears."
        )
    return (
        "Plan only. This overview makes no provider calls and never executes "
        "every source. Pick one source and run its execute_next_command when "
        "the call budget matches your intent."
    )



def _priced_in_all_source_execution_gate(
    coverage_recommendation: Mapping[str, object],
    *,
    ready_rows: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    source = str(coverage_recommendation.get("source") or "").strip()
    gaps = int(_finite_float(coverage_recommendation.get("total_gap_rows")))
    if source == "market_bars" and gaps:
        return {
            "schema_version": "priced-in-source-execution-gate-v1",
            "status": "blocked",
            "execute_next_allowed": False,
            "execute_batches_allowed": False,
            "blocked_by": "market_bars",
            "blocked_gap_rows": gaps,
            "reason": (
                "Source chunks may be planned, but execution is blocked until "
                "scan-date market bars are complete."
            ),
            "next_action": coverage_recommendation.get("action"),
            "command": coverage_recommendation.get("command"),
            "external_calls_required": int(
                _finite_float(coverage_recommendation.get("first_batch_external_calls"))
            ),
            "external_calls_made": 0,
        }
    runnable = any(
        str(row.get("execute_next_command") or "").strip() for row in ready_rows
    )
    return {
        "schema_version": "priced-in-source-execution-gate-v1",
        "status": "ready" if runnable else "complete",
        "execute_next_allowed": bool(runnable),
        "execute_batches_allowed": bool(runnable),
        "blocked_by": None,
        "blocked_gap_rows": 0,
        "reason": (
            "Review one source chunk and provider budget before execution."
            if runnable
            else "No source chunk execution is currently needed."
        ),
        "external_calls_made": 0,
    }



def _priced_in_all_source_mission_brief(
    *,
    status: str,
    scan_scope: Mapping[str, object],
    goal_alignment: Mapping[str, object],
    coverage_recommendation: Mapping[str, object],
    decision_shortcut_blocker: Mapping[str, object] | None,
    rows: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    active = int(_finite_float(scan_scope.get("active_securities")))
    scanned = int(_finite_float(scan_scope.get("scanned_rows")))
    ranked = int(_finite_float(scan_scope.get("ranked_rows")))
    gap_rows = int(_finite_float(goal_alignment.get("source_gap_rows")))
    next_source = str(coverage_recommendation.get("source") or "").strip()
    next_gaps = int(_finite_float(coverage_recommendation.get("total_gap_rows")))
    next_calls = int(
        _finite_float(coverage_recommendation.get("first_batch_external_calls"))
    )
    answer = _priced_in_mission_answer_text(
        status=status,
        active=active,
        scanned=scanned,
        ranked=ranked,
        gap_rows=gap_rows,
        next_source=next_source,
        next_gaps=next_gaps,
        decision_shortcut_blocker=decision_shortcut_blocker,
    )
    next_unblock_options = _priced_in_mission_unblock_options(
        coverage_recommendation
    )
    recommended_unblock = _priced_in_mission_recommended_unblock_action(
        coverage_recommendation,
        next_unblock_options,
    )
    return {
        "schema_version": "priced-in-mission-brief-v1",
        "question": (
            "Which stocks have market emotion that price has not fully matched?"
        ),
        "current_answer": answer,
        "useful_definition": goal_alignment.get("useful_definition"),
        "scan_progress": {
            "active_securities": active,
            "scanned_rows": scanned,
            "ranked_rows": ranked,
            "source_gap_rows": gap_rows,
        },
        "next_source": next_source or None,
        "next_gap_rows": next_gaps,
        "next_operator_action": coverage_recommendation.get("action"),
        "next_command": coverage_recommendation.get("command"),
        "next_external_calls_required": next_calls,
        **(
            {"recommended_unblock_action": recommended_unblock}
            if recommended_unblock
            else {}
        ),
        "next_unblock_options": next_unblock_options,
        "operator_boundary": (
            "Viewing this brief is zero-call. Execute only one reviewed source "
            "or repair action when the provider, date, and call budget are "
            "intentional."
        ),
        "roadmap": _priced_in_mission_source_roadmap(rows),
    }


def _priced_in_mission_answer_text(
    *,
    status: str,
    active: int,
    scanned: int,
    ranked: int,
    gap_rows: int,
    next_source: str,
    next_gaps: int,
    decision_shortcut_blocker: Mapping[str, object] | None,
) -> str:
    if status == "complete" and gap_rows <= 0:
        return (
            f"Trusted scan coverage is complete for {ranked or scanned} ranked "
            "row(s); review the ranked queue for expectation-price gaps."
        )
    if isinstance(decision_shortcut_blocker, Mapping) and next_source == "market_bars":
        return (
            "Not trusted yet: market_bars is missing scan-date price reaction "
            f"for {next_gaps} active row(s), so MarketRadar cannot claim a "
            "full-market priced-in answer."
        )
    if next_source:
        return (
            f"Not complete yet: {next_source} still has {next_gaps} gap row(s). "
            f"The current scan has {ranked or scanned} ranked row(s) from "
            f"{active or scanned} active security row(s)."
        )
    return (
        f"Not complete yet: {gap_rows} source evidence gap row(s) remain across "
        f"{ranked or scanned} ranked row(s)."
    )


def _priced_in_mission_source_roadmap(
    rows: Sequence[Mapping[str, object]],
) -> list[dict[str, object]]:
    roadmap: list[dict[str, object]] = []
    for row in rows:
        source = str(row.get("source") or "").strip()
        gaps = int(_finite_float(row.get("total_gap_rows")))
        if not source or gaps <= 0:
            continue
        first_batch = _mapping_value(row, "first_batch")
        roadmap.append(
            {
                "source": source,
                "status": row.get("status"),
                "gap_rows": gaps,
                "plannable_gap_rows": int(_finite_float(row.get("plannable_gap_rows"))),
                "unplannable_gap_rows": int(_finite_float(row.get("unplannable_gap_rows"))),
                "routed_gap_rows": int(_finite_float(row.get("routed_gap_rows"))),
                "blocked_gap_rows": int(
                    _finite_float(row.get("blocked_gap_rows"))
                    or max(
                        0,
                        int(_finite_float(row.get("unplannable_gap_rows")))
                        - int(_finite_float(row.get("routed_gap_rows"))),
                    )
                ),
                "next_chunk_external_calls": int(
                    _finite_float(first_batch.get("external_calls_required"))
                )
                if first_batch
                else 0,
                "next_action": row.get("next_action"),
            }
        )
    return roadmap


def _priced_in_mission_recommended_unblock_action(
    coverage_recommendation,
    options,
):
    source = str(coverage_recommendation.get("source") or "").strip()
    gaps = int(_finite_float(coverage_recommendation.get("total_gap_rows")))
    if source != "market_bars" or gaps <= 0:
        return None
    by_kind = {
        str(option.get("kind") or ""): option
        for option in options
        if isinstance(option, Mapping)
    }
    diagnostic = _mapping_value(coverage_recommendation, "diagnostic")
    packet = _mapping_value(
        diagnostic,
        "provider_saved_file_capture_approval_packet",
    )
    saved_status = str(
        packet.get("saved_file_status")
        or diagnostic.get("provider_saved_file_status")
        or ""
    ).strip()
    validate_option = by_kind.get("validate_saved_file")
    if saved_status == "available" and validate_option:
        return _priced_in_market_bar_recommended_unblock_from_option(
            validate_option,
            reason="Validate the saved grouped-daily file before import.",
        )
    saved_capture = by_kind.get("saved_provider_capture")
    if (
        saved_capture
        and bool(packet.get("approval_required"))
        and str(packet.get("status") or "") == "approval_required"
    ):
        return _priced_in_market_bar_recommended_unblock_from_option(
            saved_capture,
            request_body_key="confirm_request_body",
            reason=saved_capture.get("question")
            or "Capture one saved grouped-daily provider response for review.",
        )
    manual_option = by_kind.get("manual_csv")
    if manual_option:
        return _priced_in_market_bar_recommended_unblock_from_option(
            manual_option,
            reason=manual_option.get("next_action")
            or coverage_recommendation.get("action"),
        )
    first_option = next(iter(by_kind.values()), None)
    if first_option:
        return _priced_in_market_bar_recommended_unblock_from_option(first_option)
    return None



def _priced_in_mission_unblock_options(
    coverage_recommendation: Mapping[str, object],
) -> list[dict[str, object]]:
    source = str(coverage_recommendation.get("source") or "").strip()
    gaps = int(_finite_float(coverage_recommendation.get("total_gap_rows")))
    if source != "market_bars" or gaps <= 0:
        return []
    diagnostic = _row_dict(_mapping_value(coverage_recommendation, "diagnostic"))
    options: list[dict[str, object]] = []

    manual_template = str(diagnostic.get("manual_template_command") or "").strip()
    manual_preview = str(diagnostic.get("manual_validate_command") or "").strip()
    manual_execute = str(diagnostic.get("manual_fix_command") or "").strip()
    if manual_template:
        options.append(
            {
                "kind": "manual_csv",
                "status": "available",
                "label": "Manual CSV",
                "external_calls_required": 0,
                "db_writes_before_execute": 0,
                "command": manual_template,
                "api": diagnostic.get("template_api")
                or "POST /api/radar/market-bars/template",
                "request_body": _market_bar_template_request_body(diagnostic),
                "preview_api": diagnostic.get("import_api")
                or "POST /api/radar/market-bars/import",
                "preview_request_body": _market_bar_import_request_body(
                    diagnostic,
                    execute=False,
                ),
                "execute_api": diagnostic.get("import_api")
                or "POST /api/radar/market-bars/import",
                "execute_request_body": _market_bar_import_request_body(
                    diagnostic,
                    execute=True,
                ),
                "preview_command": manual_preview or None,
                "execute_command": manual_execute or None,
                "next_action": (
                    "Create or fill the missing-bar CSV, preview complete rows, "
                    "then execute the import only after review."
                ),
            }
        )

    packet = _row_dict(
        _mapping_value(diagnostic, "provider_saved_file_capture_approval_packet")
    )
    if packet:
        options.append(
            {
                "kind": "saved_provider_capture",
                "status": packet.get("status"),
                "label": "Saved provider capture",
                "approval_required": bool(packet.get("approval_required")),
                "external_calls_required": int(
                    _finite_float(packet.get("external_calls_if_approved"))
                ),
                "db_writes_during_step": int(
                    _finite_float(packet.get("db_writes_during_capture"))
                ),
                "command": packet.get("tui_confirm_command")
                or packet.get("capture_cli_command"),
                "cli_command": packet.get("capture_cli_command"),
                "tui_command": packet.get("tui_confirm_command"),
                "api": packet.get("capture_api"),
                "request_body": packet.get("capture_request_body"),
                "confirm_request_body": packet.get("capture_confirm_request_body"),
                "question": packet.get("question"),
                "next_action": packet.get("next_action"),
            }
        )

        for step in _sequence_value(packet.get("post_capture_zero_call_steps")):
            if not isinstance(step, Mapping):
                continue
            step_name = str(step.get("step") or "").strip()
            if step_name not in {"validate_saved_file", "preview_import"}:
                continue
            options.append(
                {
                    "kind": step_name,
                    "status": packet.get("saved_file_status"),
                    "label": step_name.replace("_", " ").title(),
                    "external_calls_required": int(
                        _finite_float(step.get("external_calls_made"))
                    ),
                    "db_writes_during_step": int(
                        _finite_float(step.get("db_writes_made"))
                    ),
                    "command": step.get("tui_command") or step.get("cli_command"),
                    "cli_command": step.get("cli_command"),
                    "tui_command": step.get("tui_command"),
                    "api": step.get("api"),
                    "request_body": step.get("request_body"),
                    "next_action": (
                        "Use after the saved provider response exists on disk."
                    ),
                }
            )
    return options


def _priced_in_all_source_goal_alignment(
    queue: Mapping[str, object],
    *,
    total_gap_rows: int,
    coverage_recommendation: Mapping[str, object],
    decision_recommendation: Mapping[str, object],
    stocks_only: bool,
    market_bars: Mapping[str, object] | None = None,
) -> dict[str, object]:
    full_scan = _priced_in_answer_full_scan_summary(queue, market_bars=market_bars)
    ranked_rows = int(_finite_float(full_scan.get("ranked_rows")))
    scanned_rows = int(_finite_float(full_scan.get("scanned_rows")))
    active_rows = int(_finite_float(full_scan.get("active_securities")))
    denominator = ranked_rows or scanned_rows or active_rows
    instrument_filter = "stocks_only" if stocks_only else "all_instruments"
    scan_name = "stock scan" if stocks_only else "ranked scan"
    coverage_source = str(coverage_recommendation.get("source") or "").strip()
    coverage_gaps = int(_finite_float(coverage_recommendation.get("total_gap_rows")))
    coverage_calls = int(
        _finite_float(coverage_recommendation.get("first_batch_external_calls"))
    )
    coverage_command = coverage_recommendation.get("command")
    decision_source = str(decision_recommendation.get("source") or "").strip()
    decision_rows = int(
        _finite_float(decision_recommendation.get("decision_useful_gap_rows"))
    )
    coverage_blocker_detail = str(
        coverage_recommendation.get("blocker_detail") or ""
    ).strip()
    current_blocker = (
        (
            f"{coverage_source} evidence has {coverage_gaps} gap row(s); "
            f"{coverage_blocker_detail}."
        )
        if coverage_source and coverage_blocker_detail
        else (
            f"{coverage_source} evidence has {coverage_gaps} gap row(s)."
            if coverage_source
            else "No source coverage blocker is currently runnable."
        )
    )
    next_step = str(
        coverage_recommendation.get("action")
        or "Review the source coverage plan before adding more data."
    )
    if stocks_only and active_rows:
        current_state = (
            f"The current {scan_name} has {scanned_rows}/{active_rows} "
            f"stock-like active row(s) with as-of price reaction and "
            f"{total_gap_rows} source evidence gap row(s)."
        )
    else:
        current_state = (
            f"The current {scan_name} covers {denominator} ranked row(s) and "
            f"has {total_gap_rows} source evidence gap row(s)."
        )
    return {
        "schema_version": "priced-in-goal-alignment-v1",
        "status": "aligned",
        "goal": (
            "Find stocks where market emotion has not yet been matched by "
            "price reaction."
        ),
        "useful_definition": (
            "Useful means a ranked stock row has fresh price reaction plus "
            "enough catalyst/context evidence to judge the emotion-price gap."
        ),
        "instrument_filter": instrument_filter,
        "stocks_only": bool(stocks_only),
        "ranked_rows": ranked_rows,
        "scanned_rows": scanned_rows,
        "active_securities": active_rows,
        "source_gap_rows": total_gap_rows,
        "current_state": current_state,
        "current_blocker": current_blocker,
        "next_useful_step": next_step,
        "next_source": coverage_source or None,
        "next_command": coverage_command,
        "next_external_calls_required": coverage_calls,
        "decision_shortcut_source": decision_source or None,
        "decision_shortcut_rows": decision_rows,
        "provider_boundary": (
            "This is a zero-call plan. Execute only one reviewed source chunk "
            "when the provider and call budget are intentional."
        ),
    }


def _priced_in_market_bars_scope_from_source_rows(
    rows: Sequence[Mapping[str, object]],
) -> dict[str, object] | None:
    market_row = next(
        (
            row
            for row in rows
            if str(row.get("source") or "").strip() == "market_bars"
        ),
        None,
    )
    if not market_row:
        return None
    scope = _mapping_value(market_row, "scan_scope")
    coverage_basis = str(scope.get("coverage_basis") or "").strip()
    active = int(
        _finite_float(
            scope.get("stock_like_active")
            if coverage_basis == "stock_like_active_as_of_bars"
            else scope.get("active_securities")
        )
    )
    if active <= 0:
        return None
    if coverage_basis != "stock_like_active_as_of_bars":
        return {
            "repair": {
                "stocks_only": False,
                "target_as_of": _parse_date(_mapping_value(scope, "target").get("as_of")),
                "active_securities": active,
                "with_as_of_bar": int(_finite_float(scope.get("with_as_of_bar"))),
                "missing_as_of_bar": int(_finite_float(scope.get("missing_as_of_bar"))),
            }
        }
    stock_scope = {
        "schema_version": "priced-in-market-bar-stock-scope-v1",
        "status": market_row.get("status"),
        "target_as_of": _parse_date(_mapping_value(scope, "target").get("as_of")),
        "stock_like_active": active,
        "stock_like_with_as_of_bar": int(
            _finite_float(scope.get("stock_like_with_as_of_bar"))
        ),
        "stock_like_missing_as_of_bar": int(
            _finite_float(scope.get("stock_like_missing_as_of_bar"))
        ),
        "sample_missing_stock_like_tickers": list(
            _sequence_value(
                _mapping_value(market_row, "diagnostic").get(
                    "sample_blocked_tickers"
                )
            )
        ),
    }
    return {
        "repair": {
            "stocks_only": True,
            "stock_scope": stock_scope,
        }
    }


def _priced_in_all_source_overview_scan_scope(
    queue: Mapping[str, object],
    *,
    source_count: int,
    total_gap_rows: int,
    stocks_only: bool = False,
    market_bars: Mapping[str, object] | None = None,
) -> dict[str, object]:
    full_scan = _priced_in_answer_full_scan_summary(queue, market_bars=market_bars)
    active = int(_finite_float(full_scan.get("active_securities")))
    scanned = int(_finite_float(full_scan.get("scanned_rows")))
    unscanned = int(_finite_float(full_scan.get("unscanned_rows")))
    ranked = int(_finite_float(full_scan.get("ranked_rows")))
    mode = str(full_scan.get("mode") or "full_scan")
    denominator = ranked or scanned or active
    explanation = (
        (
            f"The stocks-only full scan has as-of price reaction for "
            f"{scanned}/{active} stock-like active row(s). Source rows, first "
            "batches, and example tickers are coverage summaries or "
            "provider-safe chunks, not the scan universe."
        )
        if stocks_only and active
        else (
            f"The full scan covers {denominator} ranked row(s). Source rows, "
            "first batches, and example tickers are coverage summaries or "
            "provider-safe chunks, not the scan universe."
        )
    )
    return {
        "schema_version": "priced-in-source-overview-scan-scope-v1",
        "mode": mode,
        "is_all_active_scan": bool(full_scan.get("is_all_active_scan")),
        "stocks_only": bool(stocks_only),
        "instrument_filter": "stocks_only" if stocks_only else "all_instruments",
        "active_securities": active,
        "scanned_rows": scanned,
        "unscanned_rows": unscanned,
        "ranked_rows": ranked,
        "scan_scope_basis": full_scan.get("scan_scope_basis"),
        "source_classes": source_count,
        "source_gap_rows": total_gap_rows,
        "examples_are_samples": True,
        "explanation": explanation,
        "review_full_scan_command": _priced_in_queue_full_scan_command(
            stocks_only=stocks_only,
            limit=50,
        ),
        "export_full_scan_command": _priced_in_queue_full_scan_command(
            stocks_only=stocks_only,
            all_rows=True,
        ),
    }


def _priced_in_all_source_batch_row(
    plan: Mapping[str, object],
    *,
    priority_counts: Mapping[str, object] | None = None,
) -> dict[str, object]:
    source = str(plan.get("source") or "").strip()
    batches = _sequence_value(plan.get("batches"))
    first_batch = next((batch for batch in batches if isinstance(batch, Mapping)), None)
    first_batch_payload = _priced_in_first_source_batch_payload(first_batch)
    status = str(plan.get("status") or "unknown")
    executable = status == "ready" and first_batch is not None
    priority = _row_dict(priority_counts or {})
    total_gap_rows = int(_finite_float(plan.get("total_gap_rows")))
    plannable_gap_rows = int(_finite_float(plan.get("plannable_gap_rows")))
    unplannable_gap_rows = int(_finite_float(plan.get("unplannable_gap_rows")))
    routed_gap_rows = int(_finite_float(plan.get("routed_gap_rows")))
    blocked_gap_rows = int(
        _finite_float(plan.get("blocked_gap_rows"))
        or max(0, unplannable_gap_rows - routed_gap_rows)
    )
    batch_count = int(_finite_float(plan.get("batch_count")))
    batch_size = int(_finite_float(plan.get("batch_size")))
    all_batches_command = plan.get("all_batches_command")
    all_batches_api = plan.get("all_batches_api")
    review_rows_command = plan.get("review_rows_command")
    plan_command = all_batches_command or (
        review_rows_command if total_gap_rows else None
    )
    plan_api = all_batches_api if all_batches_command else None
    return {
        "source": source,
        "status": status,
        "headline": plan.get("headline"),
        "next_action": plan.get("next_action"),
        "total_gap_rows": total_gap_rows,
        "plannable_gap_rows": plannable_gap_rows,
        "unplannable_gap_rows": unplannable_gap_rows,
        "routed_gap_rows": routed_gap_rows,
        "blocked_gap_rows": blocked_gap_rows,
        "decision_useful_gap_rows": int(
            _finite_float(priority.get("decision_useful_gap_rows"))
        ),
        "research_useful_gap_rows": int(
            _finite_float(priority.get("research_useful_gap_rows"))
        ),
        "actionable_gap_rows": int(_finite_float(priority.get("actionable_gap_rows"))),
        "priority_sample_tickers": list(
            _sequence_value(priority.get("priority_sample_tickers"))
        ),
        "batch_count": batch_count,
        "batch_size": batch_size,
        "scan_scope": _row_dict(_mapping_value(plan, "scan_scope")),
        "coverage_basis": plan.get("coverage_basis")
        or _mapping_value(plan, "scan_scope").get("coverage_basis"),
        "first_batch": first_batch_payload,
        "approval_checklist": _row_dict(
            _mapping_value(plan, "approval_checklist")
        ),
        "all_batches_command": all_batches_command,
        "all_batches_api": all_batches_api,
        "review_rows_command": review_rows_command,
        "export_rows_command": plan.get("export_rows_command"),
        "command": plan_command,
        "plan_command": plan_command,
        "plan_api": plan_api,
        "execute_next_command": (
            plan.get("execute_next_command")
            if executable
            else None
        ),
        "execute_batches_command": (
            plan.get("execute_batches_command")
            if executable
            else None
        ),
        "execute_next_api": (
            "POST /api/radar/priced-in/source-batches/execute-next"
            if executable
            else None
        ),
        "diagnostic": _row_dict(_mapping_value(plan, "diagnostic")),
    }


def _priced_in_first_source_batch_payload(
    batch: Mapping[str, object] | None,
) -> dict[str, object] | None:
    if batch is None:
        return None
    return {
        "number": batch.get("number"),
        "row_start": batch.get("row_start"),
        "row_end": batch.get("row_end"),
        "tickers": list(_sequence_value(batch.get("tickers"))),
        "external_calls_required": int(
            _finite_float(batch.get("external_calls_required"))
        ),
        "external_call_breakdown": _row_dict(
            _mapping_value(batch, "external_call_breakdown")
        ),
        "call_plan_status": batch.get("call_plan_status"),
        "call_plan_headline": batch.get("call_plan_headline"),
        "call_plan_next_action": batch.get("call_plan_next_action"),
        "command": batch.get("command"),
        "api": batch.get("api"),
        "api_payload": _row_dict(_mapping_value(batch, "api_payload")),
    }


def _priced_in_market_bar_source_repair_context(
    engine: Engine,
    config: AppConfig,
    *,
    target_as_of: date | None,
    stocks_only: bool,
    missing: int,
) -> dict[str, object]:
    if target_as_of is None or missing <= 0:
        return {}
    try:
        repair_plan = manual_market_bars_repair_plan(
            engine,
            expected_as_of=target_as_of,
            stocks_only=stocks_only,
            provider_key_configured=config.polygon_api_key_configured,
            **_manual_repair_provider_health_kwargs(engine),
        ).as_payload()
    except ValueError as exc:
        return {
            "repair_context_status": "invalid",
            "repair_context_error": str(exc),
            "repair_context_external_calls_made": 0,
        }
    missing_universe = _mapping_value(repair_plan, "missing_universe_diagnostic")
    return {
        "target_as_of": repair_plan.get("target_as_of")
        or repair_plan.get("expected_as_of"),
        "stocks_only": repair_plan.get("stocks_only"),
        "local_template_path": repair_plan.get("local_template_path"),
        "template_api": repair_plan.get("template_api")
        or repair_plan.get("manual_template_api"),
        "import_api": repair_plan.get("import_api")
        or repair_plan.get("manual_import_api"),
        "local_bar_history": {
            "missing_with_history": int(
                _finite_float(repair_plan.get("missing_with_local_history_count"))
            ),
            "missing_without_history": int(
                _finite_float(repair_plan.get("missing_without_local_history_count"))
            ),
        },
        "missing_universe": _row_dict(missing_universe),
    }


def _priced_in_source_batch_approval_checklist(
    *,
    source_name: str,
    status: str,
    total_gap_rows: int,
    batch_count: int,
    batch_size: int,
    first_batch: Mapping[str, object] | None,
    review_rows_command: str,
    all_batches_command: str | None,
    execute_next_command: str | None,
    execute_batches_command: str | None,
) -> dict[str, object]:
    batch = _row_dict(first_batch or {})
    tickers = [
        str(ticker).strip().upper()
        for ticker in _sequence_value(batch.get("tickers"))
        if str(ticker).strip()
    ]
    calls = int(_finite_float(batch.get("external_calls_required")))
    breakdown = _row_dict(_mapping_value(batch, "external_call_breakdown"))
    approval_required = bool(execute_next_command)
    provider = _priced_in_source_execution_provider(source_name)
    if not approval_required:
        return {
            "schema_version": "priced-in-source-batch-approval-checklist-v1",
            "source": source_name,
            "status": status,
            "approval_required": False,
            "provider": provider,
            "external_calls_required": 0,
            "external_call_breakdown": {},
            "trade_order_submission_allowed": False,
            "execute_next_command": None,
            "execute_batches_command": None,
            "summary": "No executable provider/source batch is currently available.",
            "items": [
                {
                    "item": "No execution command",
                    "status": "blocked",
                    "detail": (
                        "Review the diagnostic and source-gap rows before attempting "
                        "source-fill execution."
                    ),
                }
            ],
        }
    batch_range = (
        f"rows {batch.get('row_start')}-{batch.get('row_end')}"
        if batch.get("row_start") and batch.get("row_end")
        else "the first returned batch"
    )
    call_summary = (
        _priced_in_source_call_breakdown_text(breakdown)
        if breakdown
        else "0 external calls"
    )
    return {
        "schema_version": "priced-in-source-batch-approval-checklist-v1",
        "source": source_name,
        "status": status,
        "approval_required": True,
        "provider": provider,
        "external_calls_required": calls,
        "external_call_breakdown": breakdown,
        "trade_order_submission_allowed": False,
        "execute_next_command": execute_next_command,
        "execute_batches_command": execute_batches_command,
        "summary": (
            f"Approve only if {source_name} source fill for {len(tickers)} ticker(s) "
            f"and {calls} external call(s) is intentional."
        ),
        "items": [
            {
                "item": "Full-scan gap reviewed",
                "status": "required",
                "detail": (
                    f"{total_gap_rows} full-scan {source_name} gap row(s). "
                    f"Review rows first: {review_rows_command}"
                ),
            },
            {
                "item": "Batch scope accepted",
                "status": "required",
                "detail": (
                    f"Executing next runs only {batch_range}: {len(tickers)} "
                    f"ticker(s), batch size {batch_size}, out of {batch_count} "
                    "planned batch(es)."
                ),
            },
            {
                "item": "Provider budget accepted",
                "status": "required",
                "detail": f"{calls} external call(s): {call_summary}.",
            },
            {
                "item": "No trading permission",
                "status": "required",
                "detail": (
                    "This is source-fill/read-only context only; it must not submit "
                    "orders or change broker positions."
                ),
            },
            {
                "item": "Exact command confirmed",
                "status": "required",
                "detail": str(execute_next_command),
            },
            {
                "item": "Repeat execution capped",
                "status": "required",
                "detail": (
                    f"Use {execute_batches_command} only after reviewing the full "
                    f"batch list: {all_batches_command or 'no batch list available'}"
                ),
            },
        ],
    }


def _priced_in_source_execution_provider(source_name: str) -> str:
    if source_name == "catalyst_events":
        return "sec"
    if source_name in PRICED_IN_SCHWAB_BATCH_SOURCES:
        return "schwab"
    if source_name == "local_text":
        return "local_text"
    return "none"


def _priced_in_source_call_breakdown_text(counts: Mapping[str, object]) -> str:
    parts = [
        f"{key}={int(_finite_float(value))}"
        for key, value in sorted(counts.items())
        if int(_finite_float(value)) > 0
    ]
    return ", ".join(parts) if parts else "0 external calls"


def _priced_in_source_gap_priority_counts(
    rows: Sequence[object],
) -> dict[str, dict[str, object]]:
    counts: dict[str, dict[str, object]] = {
        source: {
            "decision_useful_gap_rows": 0,
            "research_useful_gap_rows": 0,
            "actionable_gap_rows": 0,
            "priority_sample_tickers": [],
        }
        for source in PRICED_IN_SOURCE_CLASSES
    }
    for raw_row in rows:
        if not isinstance(raw_row, Mapping):
            continue
        row = raw_row
        ticker = str(row.get("ticker") or "").strip().upper()
        priced_status = str(row.get("priced_in_status") or "").strip().lower()
        usefulness = _mapping_value(row, "usefulness")
        usefulness_status = str(usefulness.get("status") or "").strip().lower()
        for source in PRICED_IN_SOURCE_CLASSES:
            if not _priced_in_source_gap_matches(row, (source,)):
                continue
            source_counts = counts[source]
            if priced_status in PRICED_IN_ACTIONABLE_STATUSES:
                source_counts["actionable_gap_rows"] = int(
                    _finite_float(source_counts.get("actionable_gap_rows"))
                ) + 1
            if usefulness_status == "decision_useful":
                source_counts["decision_useful_gap_rows"] = int(
                    _finite_float(source_counts.get("decision_useful_gap_rows"))
                ) + 1
                _append_priority_sample_ticker(source_counts, ticker)
            elif usefulness_status == "research_useful":
                source_counts["research_useful_gap_rows"] = int(
                    _finite_float(source_counts.get("research_useful_gap_rows"))
                ) + 1
                _append_priority_sample_ticker(source_counts, ticker)
    return counts


def _append_priority_sample_ticker(
    source_counts: dict[str, object],
    ticker: str,
) -> None:
    if not ticker:
        return
    samples = source_counts.get("priority_sample_tickers")
    if not isinstance(samples, list):
        samples = []
        source_counts["priority_sample_tickers"] = samples
    if ticker not in samples and len(samples) < PRICED_IN_SOURCE_ACTION_TICKER_LIMIT:
        samples.append(ticker)


def _priced_in_all_source_batches_headline(
    *,
    source_count: int,
    ready_count: int,
    blocked_count: int,
    total_gap_rows: int,
) -> str:
    if total_gap_rows <= 0:
        return f"All {source_count} priced-in source classes are covered."
    return (
        f"{total_gap_rows} source gap row(s) remain across {source_count} "
        f"source class(es); {ready_count} source(s) have a runnable next chunk "
        f"and {blocked_count} source(s) are blocked."
    )


def _priced_in_all_source_batches_next_action(
    *,
    status: str,
    ready_rows: Sequence[Mapping[str, object]],
    blocked_rows: Sequence[Mapping[str, object]],
) -> str:
    if status == "complete":
        return "No source batch action is needed."
    if ready_rows:
        first = sorted(ready_rows, key=_priced_in_source_batch_priority_key)[0]
        source = str(first.get("source") or "source")
        decision_rows = int(_finite_float(first.get("decision_useful_gap_rows")))
        research_rows = int(_finite_float(first.get("research_useful_gap_rows")))
        actionable_rows = int(_finite_float(first.get("actionable_gap_rows")))
        samples = [
            str(ticker)
            for ticker in _sequence_value(first.get("priority_sample_tickers"))
            if str(ticker).strip()
        ]
        sample_text = f" Example: {', '.join(samples)}." if samples else ""
        if decision_rows:
            return (
                f"Start with {source}; it fills context for {decision_rows} "
                "decision-ready row(s). Inspect first_batch, then run "
                f"execute_next_command only if the provider budget is intentional."
                f"{sample_text}"
            )
        if research_rows:
            return (
                f"Start with {source}; it clears evidence for {research_rows} "
                "research-useful mismatch row(s). Inspect first_batch, then run "
                f"execute_next_command only if the provider budget is intentional."
                f"{sample_text}"
            )
        if actionable_rows:
            return (
                f"Start with {source}; it covers {actionable_rows} actionable "
                "mismatch row(s). Inspect first_batch, then run execute_next_command "
                f"only if the provider budget is intentional.{sample_text}"
            )
        return (
            f"Start with {source}; inspect plan_command, then run "
            "execute_next_command only if the provider budget is intentional."
        )
    first_blocked = blocked_rows[0] if blocked_rows else {}
    return str(first_blocked.get("next_action") or "Resolve blocked source gaps first.")


def _priced_in_all_source_coverage_recommendation(
    *,
    status: str,
    ready_rows: Sequence[Mapping[str, object]],
    blocked_rows: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    if status == "complete":
        return {
            "schema_version": "priced-in-source-recommendation-v1",
            "mode": "coverage_first",
            "source": None,
            "action": "No source batch action is needed.",
            "command": None,
            "rationale": "Every tracked source class is covered for the current scan.",
        }
    market_bar_blocker = next(
        (
            row
            for row in blocked_rows
            if str(row.get("source") or "") == "market_bars"
            and int(_finite_float(row.get("total_gap_rows"))) > 0
        ),
        None,
    )
    if market_bar_blocker:
        coverage_basis = str(market_bar_blocker.get("coverage_basis") or "")
        active_scope = coverage_basis == "active_universe_as_of_bars"
        return _priced_in_source_recommendation(
            market_bar_blocker,
            mode="coverage_first",
            action=str(
                market_bar_blocker.get("next_action")
                or "Fill missing market bars before expanding source coverage."
            ),
            rationale=(
                "Fresh price reaction defines the scan universe; clear active "
                "market-bar gaps before claiming full-market coverage."
                if active_scope
                else (
                    "Fresh price reaction defines the scan universe; clear stock-like "
                    "market-bar gaps before claiming full-stock coverage."
                )
            ),
        )
    if ready_rows:
        first = sorted(ready_rows, key=_priced_in_source_batch_coverage_key)[0]
        source = str(first.get("source") or "source")
        gap_rows = int(_finite_float(first.get("total_gap_rows")))
        return _priced_in_source_recommendation(
            first,
            mode="coverage_first",
            action=(
                f"Start full-scan coverage with {source}; it has {gap_rows} "
                "remaining gap row(s). Inspect first_batch, then run "
                "execute_next_command only if the provider budget is intentional."
            ),
            rationale="Prioritizes broad evidence coverage across the whole scan.",
        )
    first_blocked = blocked_rows[0] if blocked_rows else {}
    return _priced_in_source_recommendation(
        first_blocked,
        mode="coverage_first",
        action=str(
            first_blocked.get("next_action") or "Resolve blocked source gaps first."
        ),
        rationale="No ready source chunk exists; clear the first blocked source.",
    )


def _priced_in_all_source_decision_recommendation(
    *,
    status: str,
    ready_rows: Sequence[Mapping[str, object]],
    blocked_rows: Sequence[Mapping[str, object]],
) -> dict[str, object] | None:
    if status == "complete":
        return None
    if ready_rows:
        first = sorted(ready_rows, key=_priced_in_source_batch_priority_key)[0]
        action = _priced_in_all_source_batches_next_action(
            status=status,
            ready_rows=[first],
            blocked_rows=blocked_rows,
        )
        return _priced_in_source_recommendation(
            first,
            mode="decision_shortcut",
            action=action,
            rationale=(
                "Prioritizes the currently decision-useful or actionable subset "
                "inside the full scan."
            ),
        )
    return None


def _priced_in_decision_shortcut_blocker(
    coverage_recommendation: Mapping[str, object],
) -> dict[str, object] | None:
    source = str(coverage_recommendation.get("source") or "").strip()
    gaps = int(_finite_float(coverage_recommendation.get("total_gap_rows")))
    if source != "market_bars" or gaps <= 0:
        return None
    coverage_basis = str(coverage_recommendation.get("coverage_basis") or "")
    row_label = (
        "active row"
        if coverage_basis == "active_universe_as_of_bars"
        else "stock-like row"
    )
    return {
        "schema_version": "priced-in-decision-shortcut-blocker-v1",
        "status": "blocked",
        "blocked_by": "market_bars",
        "blocked_gap_rows": gaps,
        "action": (
            "Clear market_bars first; decision shortcuts are hidden until every "
            f"{row_label} has scan-date price reaction."
        ),
        "command": coverage_recommendation.get("command"),
        "external_calls_required": 0,
    }


def _priced_in_source_recommendation(
    row: Mapping[str, object],
    *,
    mode: str,
    action: str,
    rationale: str,
) -> dict[str, object]:
    first_batch = _mapping_value(row, "first_batch")
    diagnostic = _row_dict(_mapping_value(row, "diagnostic"))
    repair_command = (
        diagnostic.get("fix_command")
        or diagnostic.get("manual_template_command")
        or diagnostic.get("manual_fix_command")
    )
    return {
        "schema_version": "priced-in-source-recommendation-v1",
        "mode": mode,
        "source": row.get("source"),
        "status": row.get("status"),
        "action": action,
        "rationale": rationale,
        "command": (
            row.get("command")
            or row.get("plan_command")
            or row.get("all_batches_command")
            or repair_command
        ),
        "capped_command": row.get("execute_batches_command"),
        "api": row.get("plan_api") or row.get("all_batches_api"),
        "total_gap_rows": int(_finite_float(row.get("total_gap_rows"))),
        "coverage_basis": row.get("coverage_basis"),
        "decision_useful_gap_rows": int(
            _finite_float(row.get("decision_useful_gap_rows"))
        ),
        "research_useful_gap_rows": int(
            _finite_float(row.get("research_useful_gap_rows"))
        ),
        "actionable_gap_rows": int(_finite_float(row.get("actionable_gap_rows"))),
        "sample_tickers": list(_sequence_value(row.get("priority_sample_tickers"))),
        "diagnostic": diagnostic,
        "blocker_detail": _priced_in_source_blocker_detail(diagnostic),
        "blocked_rows": int(_finite_float(diagnostic.get("blocked_rows"))),
        "blocked_reason": diagnostic.get("blocked_reason"),
        "sample_blocked_tickers": list(
            _sequence_value(diagnostic.get("sample_blocked_tickers"))
        ),
        "fix_command": diagnostic.get("fix_command"),
        "manual_fix_command": diagnostic.get("manual_fix_command"),
        "manual_template_command": diagnostic.get("manual_template_command"),
        "first_batch_external_calls": int(
            _finite_float(first_batch.get("external_calls_required"))
        )
        if first_batch
        else 0,
    }


def _priced_in_source_blocker_detail(
    diagnostic: Mapping[str, object],
) -> str | None:
    blocked_rows = int(_finite_float(diagnostic.get("blocked_rows")))
    eligible_rows = int(_finite_float(diagnostic.get("eligible_rows")))
    blocked_reason = str(diagnostic.get("blocked_reason") or "").strip()
    samples = [
        str(ticker).strip().upper()
        for ticker in _sequence_value(diagnostic.get("sample_blocked_tickers"))
        if str(ticker).strip()
    ]
    if blocked_rows <= 0 and not blocked_reason and not samples:
        return None
    parts: list[str] = []
    if eligible_rows or blocked_rows:
        parts.append(f"{eligible_rows} eligible row(s), {blocked_rows} blocked row(s)")
    if blocked_reason:
        parts.append(f"blocked_reason={blocked_reason}")
    if samples:
        parts.append(f"examples={', '.join(samples)}")
    return "; ".join(parts)


def _priced_in_source_batch_coverage_key(row: Mapping[str, object]) -> tuple[int, int]:
    source = str(row.get("source") or "")
    try:
        source_order = PRICED_IN_SOURCE_CLASSES.index(source)
    except ValueError:
        source_order = len(PRICED_IN_SOURCE_CLASSES)
    optional = 1 if source in PRICED_IN_OPTIONAL_CONTEXT_SOURCES else 0
    return (optional, source_order)


def _priced_in_source_batch_priority_key(row: Mapping[str, object]) -> tuple[int, int, int]:
    decision_rows = int(_finite_float(row.get("decision_useful_gap_rows")))
    research_rows = int(_finite_float(row.get("research_useful_gap_rows")))
    actionable_rows = int(_finite_float(row.get("actionable_gap_rows")))
    source = str(row.get("source") or "")
    try:
        source_order = PRICED_IN_SOURCE_CLASSES.index(source)
    except ValueError:
        source_order = len(PRICED_IN_SOURCE_CLASSES)
    if decision_rows:
        return (0, -decision_rows, source_order)
    if research_rows:
        return (1, -research_rows, source_order)
    if actionable_rows:
        return (2, -actionable_rows, source_order)
    return (3, 0, source_order)


def _market_bar_template_request_body(market_bar_repair):
    target = str(market_bar_repair.get("target_as_of") or "").strip()
    output_path = str(market_bar_repair.get("local_template_path") or "").strip()
    if not target or not output_path:
        return None
    return {
        "expected_as_of": target,
        "output_path": output_path,
        "provider": "manual_csv",
        "missing_only": True,
        "stocks_only": bool(market_bar_repair.get("stocks_only")),
        "overwrite": False,
    }


def _market_bar_import_request_body(market_bar_repair, *, execute: bool):
    target = str(market_bar_repair.get("target_as_of") or "").strip()
    daily_bars_path = str(market_bar_repair.get("local_template_path") or "").strip()
    if not target or not daily_bars_path:
        return None
    return {
        "daily_bars_path": daily_bars_path,
        "expected_as_of": target,
        "stocks_only": bool(market_bar_repair.get("stocks_only")),
        "complete_rows_only": True,
        "execute": execute,
    }


def _priced_in_market_bar_blocker_unblock_options(market_bar_repair, provider_plan):
    options = []
    manual_template = str(market_bar_repair.get("template_command") or "").strip()
    manual_preview = str(market_bar_repair.get("import_preview_command") or "").strip()
    manual_execute = str(market_bar_repair.get("import_execute_command") or "").strip()
    if manual_template:
        options.append(
            {
                "kind": "manual_csv",
                "status": "available",
                "label": "Manual CSV",
                "external_calls_required": 0,
                "db_writes_before_execute": 0,
                "command": manual_template,
                "api": market_bar_repair.get("template_api")
                or "POST /api/radar/market-bars/template",
                "request_body": _market_bar_template_request_body(market_bar_repair),
                "preview_api": market_bar_repair.get("import_api")
                or "POST /api/radar/market-bars/import",
                "preview_request_body": _market_bar_import_request_body(
                    market_bar_repair,
                    execute=False,
                ),
                "execute_api": market_bar_repair.get("import_api")
                or "POST /api/radar/market-bars/import",
                "execute_request_body": _market_bar_import_request_body(
                    market_bar_repair,
                    execute=True,
                ),
                "preview_command": manual_preview or None,
                "execute_command": manual_execute or None,
                "next_action": (
                    "Fill the missing-bar CSV, preview complete rows, then "
                    "execute only after review."
                ),
            }
        )

    packet = _mapping_value(provider_plan, "provider_saved_file_capture_approval_packet")
    if packet:
        options.append(
            {
                "kind": "saved_provider_capture",
                "status": packet.get("status"),
                "label": "Saved provider capture",
                "approval_required": bool(packet.get("approval_required")),
                "external_calls_required": int(
                    _finite_float(packet.get("external_calls_if_approved"))
                ),
                "db_writes_during_step": int(
                    _finite_float(packet.get("db_writes_during_capture"))
                ),
                "command": packet.get("tui_confirm_command")
                or packet.get("capture_cli_command"),
                "cli_command": packet.get("capture_cli_command"),
                "tui_command": packet.get("tui_confirm_command"),
                "api": packet.get("capture_api"),
                "request_body": packet.get("capture_request_body"),
                "confirm_request_body": packet.get("capture_confirm_request_body"),
                "question": packet.get("question"),
                "next_action": packet.get("next_action"),
            }
        )
        for step in _sequence_value(packet.get("post_capture_zero_call_steps")):
            if not isinstance(step, Mapping):
                continue
            step_name = str(step.get("step") or "").strip()
            if step_name not in {"validate_saved_file", "preview_import"}:
                continue
            options.append(
                {
                    "kind": step_name,
                    "status": packet.get("saved_file_status"),
                    "label": step_name.replace("_", " ").title(),
                    "external_calls_required": int(
                        _finite_float(step.get("external_calls_made"))
                    ),
                    "db_writes_during_step": int(
                        _finite_float(step.get("db_writes_made"))
                    ),
                    "command": step.get("tui_command") or step.get("cli_command"),
                    "cli_command": step.get("cli_command"),
                    "tui_command": step.get("tui_command"),
                    "api": step.get("api"),
                    "request_body": step.get("request_body"),
                    "next_action": (
                        "Use after the saved provider response exists on disk."
                    ),
                }
            )
    return options


def _priced_in_market_bar_recommended_unblock_action(blocker_detail):
    if not blocker_detail:
        return None
    options = {
        str(option.get("kind") or ""): option
        for option in _sequence_value(blocker_detail.get("unblock_options"))
        if isinstance(option, Mapping)
    }
    saved_capture = _mapping_value(blocker_detail, "saved_provider_capture")
    saved_file_status = str(saved_capture.get("saved_file_status") or "").strip()
    validate_option = options.get("validate_saved_file")
    if saved_file_status == "available" and validate_option:
        return _priced_in_market_bar_recommended_unblock_from_option(
            validate_option,
            reason="Validate the saved grouped-daily file before import.",
        )
    saved_capture_option = options.get("saved_provider_capture")
    if (
        saved_capture_option
        and saved_capture.get("approval_required")
        and str(saved_capture.get("status") or "") == "approval_required"
    ):
        return _priced_in_market_bar_recommended_unblock_from_option(
            saved_capture_option,
            request_body_key="confirm_request_body",
            reason=saved_capture_option.get("question")
            or "Capture one saved grouped-daily provider response for review.",
        )
    manual_csv = _mapping_value(blocker_detail, "manual_csv")
    manual_option = options.get("manual_csv")
    if manual_csv:
        file_exists = bool(manual_csv.get("exists"))
        complete_rows = int(_finite_float(manual_csv.get("complete_rows")))
        command = (
            manual_csv.get("preview_command")
            if file_exists and complete_rows != 0
            else None
        )
        command = command or manual_csv.get("template_command")
        api = None
        request_body = None
        if manual_option:
            if file_exists:
                api = manual_option.get("preview_api") or manual_option.get("api")
                request_body = manual_option.get("preview_request_body")
            else:
                api = manual_option.get("api")
                request_body = manual_option.get("request_body")
        return _priced_in_market_bar_recommended_unblock_payload(
            kind="manual_csv",
            label="Manual CSV",
            status="available" if command else "attention",
            reason=blocker_detail.get("next_action") or manual_csv.get("next_action"),
            command=command,
            api=api,
            request_body=request_body,
            approval_required=False,
            external_calls_required=0,
            db_writes_required=0,
        )
    first_option = next(iter(options.values()), None)
    if first_option:
        return _priced_in_market_bar_recommended_unblock_from_option(first_option)
    return None


def _priced_in_market_bar_recommended_unblock_from_option(
    option,
    *,
    request_body_key: str = "request_body",
    reason=None,
):
    writes = option.get("db_writes_during_step")
    if writes is None:
        writes = option.get("db_writes_before_execute")
    return _priced_in_market_bar_recommended_unblock_payload(
        kind=str(option.get("kind") or "option"),
        label=option.get("label") or str(option.get("kind") or "option"),
        status=option.get("status") or "unknown",
        reason=reason or option.get("next_action") or option.get("question"),
        command=option.get("command"),
        tui_command=option.get("tui_command") or option.get("command"),
        cli_command=option.get("cli_command") or option.get("command"),
        api=option.get("api"),
        request_body=option.get(request_body_key) or option.get("request_body"),
        approval_required=bool(option.get("approval_required")),
        external_calls_required=int(_finite_float(option.get("external_calls_required"))),
        db_writes_required=int(_finite_float(writes)),
    )


def _priced_in_market_bar_recommended_unblock_payload(
    *,
    kind: str,
    label,
    status,
    reason,
    command,
    tui_command=None,
    cli_command=None,
    api=None,
    request_body,
    approval_required: bool,
    external_calls_required: int,
    db_writes_required: int,
):
    return {
        "schema_version": "priced-in-market-bar-recommended-unblock-v1",
        "kind": kind,
        "label": label,
        "status": status,
        "reason": reason,
        "command": command,
        "cli_command": cli_command or command,
        "tui_command": tui_command or command,
        "api": api,
        "request_body": request_body,
        "approval_required": approval_required,
        "external_calls_required": external_calls_required,
        "db_writes_required": db_writes_required,
        "external_calls_made": 0,
    }



def _priced_in_answer_operator_next_step(
    *,
    answer_status: str,
    answer_text: str,
    full_market_trust_gate: Mapping[str, object],
    next_action: str,
    next_command: str,
    stocks_only: bool,
):
    """Return the one operator action that advances the priced-in answer."""
    gate = _row_dict(full_market_trust_gate)
    recommended = _mapping_value(gate, "recommended_action")
    trusted = bool(gate.get("trusted_full_market_answer"))
    first_blocker = str(gate.get("first_blocker") or "").strip()
    first_gap_count = int(_finite_float(gate.get("first_gap_count")))
    command = str(next_command or "").strip() or None
    tui_command = command
    api = None
    request_body = None
    approval_required = False
    calls_required = 0
    writes_required = 0
    action_kind = "review_priced_in_answer"
    action_label = "Review priced-in answer"
    action_reason = next_action or answer_text
    response_after_action = (
        "Review the visible priced-in rows. This is still not trade approval."
    )

    if recommended:
        action_kind = str(recommended.get("kind") or "unblock_priced_in_answer")
        action_label = str(recommended.get("label") or action_kind).strip()
        action_reason = str(
            recommended.get("reason")
            or recommended.get("next_action")
            or action_label
        ).strip()
        command = str(
            recommended.get("cli_command")
            or recommended.get("command")
            or ""
        ).strip() or None
        tui_command = str(
            recommended.get("tui_command")
            or recommended.get("command")
            or command
            or ""
        ).strip() or None
        api = recommended.get("api")
        request_body = recommended.get("request_body")
        approval_required = bool(recommended.get("approval_required"))
        calls_required = int(_finite_float(recommended.get("external_calls_required")))
        writes_required = int(_finite_float(recommended.get("db_writes_required")))
        response_after_action = _priced_in_operator_response_after_action(action_kind)
    elif trusted:
        action_reason = next_action or "Open decision-ready priced-in rows."
        command = command or "catalyst-radar priced-in-queue --decision-ready"
        tui_command = "ready"
    elif first_blocker:
        action_kind = f"clear_{first_blocker}"
        action_label = f"Clear {first_blocker}"
        action_reason = next_action or gate.get("next_action") or answer_text
        command = command or str(gate.get("next_command") or "").strip() or None
        tui_command = command
        response_after_action = (
            "Rerun `catalyst-radar priced-in-answer` after the blocker changes."
        )

    status = "ready" if trusted else "blocked"
    priority = "review" if trusted else "must_fix"
    if answer_status in {"none_visible", "monitor"} and not first_blocker:
        priority = "monitor"

    return {
        "schema_version": "priced-in-operator-next-step-v1",
        "question": "What should the operator do next for the priced-in scan?",
        "status": status,
        "answer_status": answer_status,
        "trusted_priced_in_answer": trusted,
        "can_use_for_investment_decision": False,
        "investment_decision_boundary": (
            "This is decision support only. It never approves a trade."
        ),
        "priority": priority,
        "scope": "stock_like" if stocks_only else "full_market",
        "first_blocker": first_blocker or None,
        "first_gap_count": first_gap_count,
        "action_kind": action_kind,
        "action_label": action_label,
        "action": action_reason,
        "command": command,
        "tui_command": tui_command,
        "api": api,
        "request_body": request_body,
        "approval_required": approval_required,
        "external_calls_required": calls_required,
        "db_writes_required": writes_required,
        "response_after_action": response_after_action,
        "external_calls_made": 0,
        "db_writes_made": 0,
    }


def _priced_in_operator_response_after_action(action_kind: str):
    if action_kind == "saved_provider_capture":
        return (
            "A saved provider response should exist locally. Validate it before "
            "previewing or executing any import."
        )
    if action_kind == "validate_saved_file":
        return "If validation passes, preview the saved import before executing it."
    if action_kind == "preview_import":
        return "If the preview fills the intended missing bars, execute the import."
    if action_kind == "manual_csv":
        return (
            "Fill or generate the local CSV, preview the import, then execute only "
            "after the preview matches the intended bars."
        )
    return "Rerun `catalyst-radar priced-in-answer` after the action completes."

def priced_in_answer_payload(
    engine: Engine,
    config: AppConfig,
    *,
    limit: int = 5,
    available_at: datetime | None = None,
    status: str | None = None,
    usefulness: str | None = None,
    source_gap: str | Sequence[str] | None = None,
    decision_gap: str | Sequence[str] | None = None,
    min_gap: float | None = None,
    stocks_only: bool = False,
    queue: Mapping[str, object] | None = None,
    preflight: Mapping[str, object] | None = None,
    market_bars: Mapping[str, object] | None = None,
) -> dict[str, object]:
    resolved_limit = _positive_limit(limit)
    resolved_queue = (
        _row_dict(queue)
        if isinstance(queue, Mapping)
        else priced_in_queue_payload(
            engine,
            config,
            limit=resolved_limit,
            offset=0,
            available_at=available_at,
            status=status or "all",
            usefulness=usefulness,
            source_gap=source_gap,
            decision_gap=decision_gap,
            min_gap=min_gap,
            stocks_only=stocks_only,
            include_planning_rows=True,
        )
    )
    queue_preflight = _mapping_value(resolved_queue, "preflight")
    resolved_preflight = (
        _row_dict(preflight)
        if isinstance(preflight, Mapping)
        else (
            _row_dict(queue_preflight)
            if isinstance(queue_preflight, Mapping) and queue_preflight
            else priced_in_preflight_payload(engine, config, stocks_only=stocks_only)
        )
    )
    status_counts = _mapping_value(resolved_queue, "status_counts")
    usefulness_counts = _mapping_value(resolved_queue, "usefulness_counts")
    actionable_count = sum(
        int(_finite_float(status_counts.get(item)))
        for item in PRICED_IN_ACTIONABLE_STATUSES
    )
    decision_ready_count = int(_finite_float(usefulness_counts.get("decision_useful")))
    research_lead_count = int(_finite_float(usefulness_counts.get("research_useful")))
    blocked_count = int(_finite_float(usefulness_counts.get("blocked")))
    top_rows = _priced_in_answer_rows(
        _sequence_value(resolved_queue.get("rows")),
        stocks_only=stocks_only,
    )
    source_coverage = _mapping_value(resolved_queue, "source_coverage")
    resolved_market_bars = (
        _row_dict(market_bars)
        if isinstance(market_bars, Mapping)
        else _priced_in_audit_market_bars(
            engine,
            config,
            resolved_queue,
            resolved_preflight,
        )
    )
    source_coverage = _priced_in_source_coverage_with_market_bar_scope(
        source_coverage,
        resolved_market_bars,
    )
    market_bar_gap = _priced_in_answer_market_bar_gap(source_coverage)
    market_bar_gap_count = int(_finite_float(market_bar_gap.get("count")))
    evidence_completeness = _priced_in_answer_evidence_completeness(source_coverage)
    core_evidence_gap = _priced_in_answer_core_evidence_gap(
        evidence_completeness,
        skip_market_bars=market_bar_gap_count > 0,
    )
    core_evidence_gap_count = int(_finite_float(core_evidence_gap.get("count")))
    answer_status = _priced_in_answer_status(
        queue_status=str(resolved_queue.get("status") or "unknown"),
        actionable_count=actionable_count,
        decision_ready_count=decision_ready_count,
        research_lead_count=research_lead_count,
        blocked_count=blocked_count,
        market_bar_gap_count=market_bar_gap_count,
        core_evidence_gap_count=core_evidence_gap_count,
    )
    decision_readiness = _priced_in_answer_decision_readiness(
        _mapping_value(resolved_queue, "decision_gap_counts"),
        source_coverage=source_coverage,
        decision_ready_count=decision_ready_count,
        scan_as_of=str(_mapping_value(resolved_queue, "latest_run").get("as_of") or ""),
        market_bar_gap=market_bar_gap,
        core_evidence_gap=core_evidence_gap,
    )
    next_action, next_command = _priced_in_answer_next_step(
        answer_status=answer_status,
        preflight=resolved_preflight,
        top_rows=top_rows,
        decision_readiness=decision_readiness,
        stocks_only=stocks_only,
    )
    full_scan_summary = _priced_in_answer_full_scan_summary(
        resolved_queue,
        market_bars=resolved_market_bars,
    )
    market_bar_blocker_detail: dict[str, object] | None = None
    if str(evidence_completeness.get("first_gap_source") or "") == "market_bars":
        market_bar_repair = _mapping_value(resolved_market_bars, "repair")
        if market_bar_repair:
            local_progress = _mapping_value(
                market_bar_repair,
                "local_template_fill_progress",
            )
            operator_step = _mapping_value(market_bar_repair, "operator_step")
            provider_plan = _mapping_value(market_bar_repair, "provider_fill_plan")
            missing_universe = _mapping_value(
                market_bar_repair,
                "missing_universe_diagnostic",
            )
            if not missing_universe:
                missing_universe = _mapping_value(
                    _mapping_value(market_bar_repair, "diagnostic"),
                    "missing_universe",
                )
            market_bar_blocker_detail = {
                "schema_version": "priced-in-market-bar-blocker-detail-v1",
                "source": "market_bars",
                "status": market_bar_repair.get("status")
                or resolved_market_bars.get("status"),
                "missing_as_of_bar": market_bar_repair.get("missing_as_of_bar")
                or resolved_market_bars.get("missing_as_of_bar"),
                "local_template_path": market_bar_repair.get("local_template_path"),
                "local_template_exists": bool(
                    market_bar_repair.get("local_template_exists")
                ),
                "complete_rows": int(_finite_float(local_progress.get("complete_rows"))),
                "partial_rows": int(_finite_float(local_progress.get("partial_rows"))),
                "empty_rows": int(_finite_float(local_progress.get("empty_rows"))),
                "provider_saved_file_status": provider_plan.get(
                    "provider_saved_file_status"
                ),
                "provider_saved_file_path": provider_plan.get(
                    "provider_saved_file_path"
                ),
                "next_action": operator_step.get("action")
                or market_bar_repair.get("next_action"),
                "preview_command": operator_step.get("after_manual_command")
                or market_bar_repair.get("import_preview_command"),
                "external_calls_made": 0,
            }
            if missing_universe:
                market_bar_blocker_detail["missing_universe"] = {
                    "schema_version": "priced-in-market-bar-missing-universe-v1",
                    "summary": missing_universe.get("summary"),
                    "active_metadata_rows": missing_universe.get(
                        "active_metadata_rows"
                    ),
                    "acquisition_or_spac_name_count": missing_universe.get(
                        "acquisition_or_spac_name_count"
                    ),
                    "no_composite_figi_count": missing_universe.get(
                        "no_composite_figi_count"
                    ),
                    "zero_avg_dollar_volume_20d_count": missing_universe.get(
                        "zero_avg_dollar_volume_20d_count"
                    ),
                    "operator_note": missing_universe.get("operator_note"),
                    "external_calls_made": 0,
                }
            manual_csv_context = _priced_in_market_bar_manual_csv_context(
                market_bar_repair,
                local_progress,
                operator_step,
            )
            if manual_csv_context:
                market_bar_blocker_detail["manual_csv"] = manual_csv_context
            saved_capture_context = (
                _priced_in_market_bar_saved_provider_capture_context(provider_plan)
            )
            if saved_capture_context:
                market_bar_blocker_detail["saved_provider_capture"] = (
                    saved_capture_context
                )
            unblock_options = _priced_in_market_bar_blocker_unblock_options(
                market_bar_repair,
                provider_plan,
            )
            if unblock_options:
                market_bar_blocker_detail["unblock_options"] = unblock_options
            recommended_action = _priced_in_market_bar_recommended_unblock_action(
                market_bar_blocker_detail
            )
            if recommended_action:
                market_bar_blocker_detail["recommended_action"] = recommended_action
    unscanned_blocker_rows = int(
        _finite_float(
            full_scan_summary.get("unscanned_blocker_rows")
            if "unscanned_blocker_rows" in full_scan_summary
            else full_scan_summary.get("unscanned_rows")
        )
    )
    trust_gate_trusted = bool(
        bool(evidence_completeness.get("all_sources_ready"))
        and unscanned_blocker_rows <= 0
    )
    full_market_trust_gate = {
        "schema_version": "priced-in-full-market-trust-gate-v1",
        "question": "Can MarketRadar trust a full-market priced-in answer right now?",
        "status": "ready" if trust_gate_trusted else "blocked",
        "trusted_full_market_answer": trust_gate_trusted,
        "answer": evidence_completeness.get("summary"),
        "first_blocker": evidence_completeness.get("first_gap_source"),
        "first_gap_count": evidence_completeness.get("first_gap_count"),
        "active_securities": full_scan_summary.get("active_securities"),
        "scanned_rows": full_scan_summary.get("scanned_rows"),
        "unscanned_rows": full_scan_summary.get("unscanned_rows"),
        "unscanned_blocker_rows": full_scan_summary.get(
            "unscanned_blocker_rows"
        ),
        "scan_excluded_rows": full_scan_summary.get("scan_excluded_rows"),
        "scan_excluded_tickers": full_scan_summary.get("scan_excluded_tickers"),
        "scan_excluded_reason": full_scan_summary.get("scan_excluded_reason"),
        "ranked_rows": full_scan_summary.get("ranked_rows"),
        "next_action": evidence_completeness.get("next_action"),
        "next_command": evidence_completeness.get("command"),
        "blocker_detail": market_bar_blocker_detail,
        "operator_boundary": "This gate is zero-call and cannot run providers.",
        "external_calls_made": 0,
    }
    if market_bar_blocker_detail and market_bar_blocker_detail.get(
        "recommended_action"
    ):
        full_market_trust_gate["recommended_action"] = market_bar_blocker_detail[
            "recommended_action"
        ]
    reviewable_subset = {
        "schema_version": "priced-in-reviewable-subset-v1",
        "row_count": decision_ready_count,
        "sample_tickers": [
            row.get("ticker") for row in top_rows if row.get("decision_ready")
        ],
        "external_calls_made": 0,
    }
    decision_ready = answer_status == "decision_ready"
    trust_blockers = _priced_in_prioritized_trust_blockers(
        _priced_in_answer_trust_blockers(
            resolved_preflight,
            answer_status=answer_status,
            source_coverage=source_coverage,
        ),
        primary_area="market_bars" if market_bar_gap_count > 0 else None,
    )
    blocker_ladder = _priced_in_answer_blocker_ladder(
        trust_blockers,
        stocks_only=stocks_only,
    )
    full_market_trust_gate["blocker_ladder"] = blocker_ladder
    after_current_blocker = _priced_in_answer_after_current_blocker(
        blocker_ladder,
        engine=engine,
        config=config,
        queue=resolved_queue,
        stocks_only=stocks_only,
    )
    if after_current_blocker:
        full_market_trust_gate["after_current_blocker"] = after_current_blocker
    investment_decision_boundary = (
        "Priced-in answer readiness is not trade approval. Use the separate "
        "radar readiness/manual_buy_review gate before any investment decision."
    )
    answer_text = _priced_in_answer_text(
        answer_status=answer_status,
        actionable_count=actionable_count,
        decision_ready_count=decision_ready_count,
        research_lead_count=research_lead_count,
        blocked_count=blocked_count,
        market_bar_gap_count=market_bar_gap_count,
        core_evidence_gap=core_evidence_gap,
        stocks_only=stocks_only,
    )
    headline_text = _priced_in_answer_headline(
        answer_status=answer_status,
        total_count=int(_finite_float(resolved_queue.get("total_count"))),
        actionable_count=actionable_count,
        decision_ready_count=decision_ready_count,
        research_lead_count=research_lead_count,
        blocked_count=blocked_count,
        market_bar_gap_count=market_bar_gap_count,
        core_evidence_gap=core_evidence_gap,
        stocks_only=stocks_only,
    )
    operator_next_step = _priced_in_answer_operator_next_step(
        answer_status=answer_status,
        answer_text=answer_text,
        full_market_trust_gate=full_market_trust_gate,
        next_action=next_action,
        next_command=next_command,
        stocks_only=stocks_only,
    )
    return {
        "schema_version": "priced-in-answer-v1",
        "status": answer_status,
        "question": "Has price fully matched market expectations?",
        "answer": answer_text,
        "headline": headline_text,
        "decision_ready": decision_ready,
        "priced_in_answer_ready": decision_ready,
        "can_make_investment_decision": False,
        "manual_investment_decision_ready": False,
        "investment_decision_boundary": investment_decision_boundary,
        "external_calls_made": 0,
        "counts": {
            "total_rows": int(_finite_float(resolved_queue.get("total_count"))),
            "visible_rows": int(_finite_float(resolved_queue.get("count"))),
            "actionable_mismatch_rows": actionable_count,
            "decision_ready_rows": decision_ready_count,
            "research_lead_rows": research_lead_count,
            "blocked_rows": blocked_count,
        },
        "scan_scope": _priced_in_answer_scan_scope(resolved_queue),
        "full_scan": full_scan_summary,
        "full_market_trust_gate": full_market_trust_gate,
        "operator_next_step": operator_next_step,
        "reviewable_subset": reviewable_subset,
        "decision_readiness": decision_readiness,
        "evidence_completeness": evidence_completeness,
        "filters": _row_dict(_mapping_value(resolved_queue, "filters")),
        "source_coverage": {
            "summary": source_coverage.get("summary"),
            "weak_sources": list(_sequence_value(source_coverage.get("weak_sources"))),
        },
        "trust_blockers": trust_blockers,
        "next_action": next_action,
        "next_command": next_command,
        "top_rows": top_rows[:resolved_limit],
    }


def priced_in_full_scan_audit_payload(
    engine: Engine,
    config: AppConfig,
    *,
    available_at: datetime | None = None,
    source_gap: str | Sequence[str] | None = None,
    queue: Mapping[str, object] | None = None,
    preflight: Mapping[str, object] | None = None,
    preview_limit: int = PRICED_IN_FULL_SCAN_PREVIEW_LIMIT,
    preview_offset: int = 0,
    all_rows: bool = False,
    stocks_only: bool = False,
    market_bars: Mapping[str, object] | None = None,
) -> dict[str, object]:
    if queue is not None or preflight is not None or market_bars is not None:
        return _priced_in_full_scan_audit_payload_uncached(
            engine,
            config,
            available_at=available_at,
            source_gap=source_gap,
            queue=queue,
            preflight=preflight,
            preview_limit=preview_limit,
            preview_offset=preview_offset,
            all_rows=all_rows,
            stocks_only=stocks_only,
            market_bars=market_bars,
        )

    resolved_all_rows = bool(all_rows)
    resolved_preview_limit = 1_000_000 if resolved_all_rows else _positive_limit(
        preview_limit
    )
    resolved_preview_offset = 0 if resolved_all_rows else _positive_offset(
        preview_offset
    )
    wanted_source_gaps = tuple(_priced_in_source_gap_filter(source_gap))
    state_token = _priced_in_audit_cache_state_token(engine)
    if state_token is None:
        return _priced_in_full_scan_audit_payload_uncached(
            engine,
            config,
            available_at=available_at,
            source_gap=source_gap,
            preview_limit=preview_limit,
            preview_offset=preview_offset,
            all_rows=all_rows,
            stocks_only=stocks_only,
        )

    cache_key = (
        str(engine.url),
        available_at.isoformat() if available_at is not None else "",
        wanted_source_gaps,
        resolved_preview_limit,
        resolved_preview_offset,
        resolved_all_rows,
        bool(stocks_only),
        state_token,
    )
    now = monotonic()
    cached = _PRICED_IN_AUDIT_CACHE.get(cache_key)
    if cached is not None and now - cached[0] <= _PRICED_IN_AUDIT_CACHE_TTL_SECONDS:
        payload = deepcopy(cached[1])
        _priced_in_audit_cache_performance(
            payload,
            cache_status="hit",
            cache_age_ms=(now - cached[0]) * 1000,
        )
        return payload
    if cached is not None:
        _PRICED_IN_AUDIT_CACHE.pop(cache_key, None)

    build_started = monotonic()
    payload = _priced_in_full_scan_audit_payload_uncached(
        engine,
        config,
        available_at=available_at,
        source_gap=source_gap,
        preview_limit=preview_limit,
        preview_offset=preview_offset,
        all_rows=all_rows,
        stocks_only=stocks_only,
    )
    _priced_in_audit_cache_performance(
        payload,
        cache_status="miss",
        build_elapsed_ms=(monotonic() - build_started) * 1000,
    )
    _priced_in_audit_cache_store(cache_key, payload)
    return payload


def _priced_in_audit_cache_state_token(engine: Engine) -> tuple[object, ...] | None:
    tables = (
        (securities, securities.c.updated_at),
        (daily_bars, daily_bars.c.available_at),
        (signal_features, signal_features.c.as_of),
        (candidate_states, candidate_states.c.created_at),
        (candidate_packets, candidate_packets.c.created_at),
        (decision_cards, decision_cards.c.created_at),
        (events, events.c.created_at),
        (text_snippets, text_snippets.c.created_at),
        (text_features, text_features.c.created_at),
        (option_features, option_features.c.created_at),
        (broker_market_snapshots, broker_market_snapshots.c.created_at),
        (job_runs, job_runs.c.finished_at),
    )
    try:
        with engine.connect() as conn:
            token_parts = []
            for table, freshness_column in tables:
                row = conn.execute(
                    select(
                        func.count().label("row_count"),
                        func.max(freshness_column).label("latest_value"),
                    ).select_from(table)
                ).one()
                token_parts.append(
                    (
                        table.name,
                        int(row[0] or 0),
                        str(row[1] or ""),
                    )
                )
    except SQLAlchemyError:
        return None
    return tuple(token_parts)


def _priced_in_audit_cache_store(
    cache_key: tuple[object, ...],
    payload: Mapping[str, object],
) -> None:
    if len(_PRICED_IN_AUDIT_CACHE) >= _PRICED_IN_AUDIT_CACHE_MAX_ITEMS:
        oldest_key = min(
            _PRICED_IN_AUDIT_CACHE,
            key=lambda key: _PRICED_IN_AUDIT_CACHE[key][0],
        )
        _PRICED_IN_AUDIT_CACHE.pop(oldest_key, None)
    _PRICED_IN_AUDIT_CACHE[cache_key] = (monotonic(), deepcopy(dict(payload)))


def _priced_in_audit_cache_performance(
    payload: dict[str, object],
    *,
    cache_status: str,
    build_elapsed_ms: float | None = None,
    cache_age_ms: float | None = None,
) -> None:
    performance = _row_dict(payload.get("performance"))
    performance.update(
        {
            "cache_status": cache_status,
            "cache_ttl_seconds": int(_PRICED_IN_AUDIT_CACHE_TTL_SECONDS),
            "cache_key_scope": "database_state_and_audit_filters",
        }
    )
    if build_elapsed_ms is not None:
        performance["build_elapsed_ms"] = round(build_elapsed_ms, 1)
    if cache_age_ms is not None:
        performance["cache_age_ms"] = round(cache_age_ms, 1)
    payload["performance"] = performance


def _priced_in_full_scan_audit_payload_uncached(
    engine: Engine,
    config: AppConfig,
    *,
    available_at: datetime | None = None,
    source_gap: str | Sequence[str] | None = None,
    queue: Mapping[str, object] | None = None,
    preflight: Mapping[str, object] | None = None,
    preview_limit: int = PRICED_IN_FULL_SCAN_PREVIEW_LIMIT,
    preview_offset: int = 0,
    all_rows: bool = False,
    stocks_only: bool = False,
    market_bars: Mapping[str, object] | None = None,
) -> dict[str, object]:
    resolved_all_rows = bool(all_rows)
    resolved_preview_limit = 1_000_000 if resolved_all_rows else _positive_limit(preview_limit)
    resolved_preview_offset = 0 if resolved_all_rows else _positive_offset(preview_offset)
    wanted_source_gaps = _priced_in_source_gap_filter(source_gap)
    audit_page_command = _priced_in_audit_command(
        limit=resolved_preview_limit,
        offset=resolved_preview_offset,
        available_at=available_at,
        source_gap=wanted_source_gaps,
        all_rows=resolved_all_rows,
        stocks_only=stocks_only,
    )
    resolved_queue = (
        _row_dict(queue)
        if isinstance(queue, Mapping)
        else priced_in_queue_payload(
            engine,
            config,
            limit=resolved_preview_limit,
            offset=resolved_preview_offset,
            available_at=available_at,
            status="all",
            include_planning_rows=True,
            stocks_only=stocks_only,
        )
    )
    resolved_preflight = (
        _row_dict(preflight)
        if isinstance(preflight, Mapping)
        else _row_dict(_mapping_value(resolved_queue, "preflight"))
    )
    if not resolved_preflight:
        resolved_preflight = priced_in_preflight_payload(
            engine,
            config,
            stocks_only=stocks_only,
        )
    full_scan = _priced_in_answer_full_scan_summary(resolved_queue)
    preview_queue = resolved_queue
    if wanted_source_gaps:
        preview_queue = _priced_in_audit_preview_queue_from_planning_rows(
            resolved_queue,
            source_gaps=wanted_source_gaps,
            limit=resolved_preview_limit,
            offset=resolved_preview_offset,
        ) or priced_in_queue_payload(
            engine,
            config,
            limit=resolved_preview_limit,
            offset=resolved_preview_offset,
            available_at=available_at,
            status="all",
            source_gap=wanted_source_gaps,
            stocks_only=stocks_only,
        )
    preview_scan = _priced_in_answer_full_scan_summary(preview_queue)
    preview_rows = _priced_in_full_scan_preview_rows(
        _sequence_value(preview_queue.get("rows"))
    )
    has_more = bool(preview_scan.get("has_more"))
    next_audit_page_command = (
        _priced_in_audit_command(
            limit=resolved_preview_limit,
            offset=resolved_preview_offset + max(1, resolved_preview_limit),
            available_at=available_at,
            source_gap=wanted_source_gaps,
            all_rows=False,
            stocks_only=stocks_only,
        )
        if has_more
        else None
    )
    audit_full_export_command = _priced_in_audit_command(
        limit=resolved_preview_limit,
        offset=0,
        available_at=available_at,
        source_gap=wanted_source_gaps,
        all_rows=True,
        json=True,
        stocks_only=stocks_only,
    )
    source_coverage = _mapping_value(resolved_queue, "source_coverage")
    instrument_scope = _mapping_value(resolved_queue, "instrument_scope")
    planning_rows = _sequence_value(resolved_queue.get("planning_rows"))
    if not planning_rows:
        planning_rows = _sequence_value(resolved_queue.get("rows"))
    resolved_market_bars = (
        _row_dict(market_bars)
        if isinstance(market_bars, Mapping)
        else _priced_in_audit_market_bars(
            engine,
            config,
            resolved_queue,
            resolved_preflight,
        )
    )
    source_coverage = _priced_in_source_coverage_with_market_bar_scope(
        source_coverage,
        resolved_market_bars,
    )
    priority_counts = _priced_in_source_gap_priority_counts(planning_rows)
    source_rows = []
    for row in _sequence_value(source_coverage.get("actions")):
        if not isinstance(row, Mapping):
            continue
        source = str(row.get("source") or "").strip()
        source_rows.append(
            _priced_in_audit_source_row(
                row,
                priority_counts=priority_counts.get(source),
            )
        )
    source_gap_actions = _priced_in_audit_source_gap_actions(
        engine,
        config,
        source_rows,
        wanted_source_gaps,
        available_at=available_at,
        queue=resolved_queue,
        stocks_only=stocks_only,
    )
    status_counts = _mapping_value(resolved_queue, "status_counts")
    usefulness_counts = _mapping_value(resolved_queue, "usefulness_counts")
    actionable_count = sum(
        int(_finite_float(status_counts.get(item)))
        for item in PRICED_IN_ACTIONABLE_STATUSES
    )
    decision_ready_count = int(_finite_float(usefulness_counts.get("decision_useful")))
    research_ready_count = int(_finite_float(usefulness_counts.get("research_useful")))
    blocked_count = int(_finite_float(usefulness_counts.get("blocked")))
    next_action, next_command = _priced_in_audit_next_step(
        resolved_preflight,
        source_rows,
    )
    recommended_source_gap = _priced_in_audit_recommended_source_gap(
        source_rows,
        available_at=available_at,
        stocks_only=stocks_only,
    )
    status = _priced_in_audit_status(
        preflight_status=str(resolved_preflight.get("status") or ""),
        decision_ready_count=decision_ready_count,
        research_ready_count=research_ready_count,
    )
    ranked_rows = int(_finite_float(full_scan.get("ranked_rows")))
    active = int(_finite_float(full_scan.get("active_securities"))) or ranked_rows
    answer_rows = [
        row
        for row in planning_rows
        if isinstance(row, Mapping)
        and (
            not wanted_source_gaps
            or _priced_in_source_gap_matches(row, wanted_source_gaps)
        )
    ]
    answer_shortlist = _priced_in_audit_answer_shortlist(
        answer_rows,
        source_gaps=wanted_source_gaps,
        full_scan_rows=ranked_rows,
        limit=10,
        full_scan_review_command=audit_page_command,
        full_scan_export_command=audit_full_export_command,
        stocks_only=stocks_only,
    )
    primary_scan = _priced_in_audit_primary_full_scan(
        full_scan,
        preview_scan=preview_scan,
        ranked_rows=ranked_rows,
        active_securities=active,
        all_rows_requested=resolved_all_rows,
        source_gaps=wanted_source_gaps,
        audit_page_command=audit_page_command,
        audit_next_page_command=next_audit_page_command,
        audit_full_export_command=audit_full_export_command,
        stocks_only=stocks_only,
    )
    trust_blockers = _priced_in_answer_trust_blockers(
        resolved_preflight,
        answer_status=status,
        source_coverage=source_coverage,
    )
    return {
        "schema_version": "priced-in-full-scan-audit-v1",
        "status": status,
        "headline": (
            f"Full scan ranks {ranked_rows} row(s) from {active} active "
            f"securities; {research_ready_count} research lead(s), "
            f"{decision_ready_count} decision-ready row(s)."
        ),
        "question": "Can MarketRadar answer whether price matches market expectations?",
        "answer": _priced_in_audit_answer(
            status=status,
            actionable_count=actionable_count,
            research_ready_count=research_ready_count,
            decision_ready_count=decision_ready_count,
            blocked_count=blocked_count,
        ),
        "external_calls_made": 0,
        "scope": {
            "mode": full_scan.get("mode"),
            "instrument_filter": "stocks_only" if stocks_only else "all",
            "stocks_only": bool(stocks_only),
            "is_all_active_scan": full_scan.get("is_all_active_scan"),
            "active_securities": active,
            "scanned_rows": full_scan.get("scanned_rows"),
            "ranked_rows": ranked_rows,
            "visible_row_start": full_scan.get("visible_row_start"),
            "visible_row_end": full_scan.get("visible_row_end"),
            "visible_rows": full_scan.get("visible_rows"),
            "has_more": full_scan.get("has_more"),
            "visible_tickers_are_sample": full_scan.get(
                "visible_tickers_are_sample"
            ),
            "review_command": full_scan.get("review_command"),
            "next_page_command": full_scan.get("next_page_command"),
            "export_command": full_scan.get("full_export_command")
            or full_scan.get("export_command"),
            "sample_explanation": full_scan.get("sample_explanation"),
            "all_rows_requested": resolved_all_rows,
            "audit_page_command": audit_page_command,
            "audit_next_page_command": next_audit_page_command,
            "audit_full_export_command": audit_full_export_command,
        },
        "preview": {
            "schema_version": "priced-in-full-scan-preview-v1",
            "row_start": preview_scan.get("visible_row_start"),
            "row_end": preview_scan.get("visible_row_end"),
            "visible_rows": preview_scan.get("visible_rows"),
            "total_rows": preview_scan.get("ranked_rows"),
            "has_more": preview_scan.get("has_more"),
            "all_rows": resolved_all_rows,
            "sample_explanation": _priced_in_audit_preview_note(
                preview_scan.get("sample_explanation"),
                source_gaps=wanted_source_gaps,
            ),
            "filter": {
                "source_gap": list(wanted_source_gaps),
                "stocks_only": bool(stocks_only),
            },
            "source_gap_actions": source_gap_actions,
            "review_command": preview_scan.get("review_command"),
            "next_page_command": preview_scan.get("next_page_command"),
            "export_command": preview_scan.get("full_export_command")
            or preview_scan.get("export_command"),
            "audit_page_command": audit_page_command,
            "audit_next_page_command": next_audit_page_command,
            "audit_full_export_command": audit_full_export_command,
        },
        "preview_rows": preview_rows,
        "primary_scan": primary_scan,
        "answer_shortlist": answer_shortlist,
        "counts": {
            "actionable_mismatch_rows": actionable_count,
            "research_lead_rows": research_ready_count,
            "decision_ready_rows": decision_ready_count,
            "blocked_rows": blocked_count,
        },
        "market_bars": resolved_market_bars,
        "source_coverage": {
            "summary": source_coverage.get("summary"),
            "weak_sources": list(_sequence_value(source_coverage.get("weak_sources"))),
            "ready_source_count": sum(
                1 for row in source_rows if str(row.get("status")) == "ready"
            ),
            "source_count": len(source_rows),
            "trust_gap_count": len(trust_blockers),
        },
        "instrument_scope": instrument_scope,
        "sources": source_rows,
        "recommended_source_gap": recommended_source_gap,
        "trust_blockers": trust_blockers,
        "evidence_plan": _row_dict(_mapping_value(resolved_preflight, "evidence_plan")),
        "next_action": next_action,
        "next_command": next_command,
        "useful_definition": (
            "Useful means full-scan price reaction is covered, catalyst/text "
            "evidence explains the emotion side, decision artifacts exist for "
            "candidate rows, and optional broker/options context is point-in-time "
            "or explicitly labeled supporting evidence."
        ),
        "commands": {
            "answer": "catalyst-radar priced-in-answer"
            + (" --stocks-only" if stocks_only else ""),
            "queue": _priced_in_queue_full_scan_command(
                stocks_only=stocks_only,
                limit=50,
            ),
            "preflight": "catalyst-radar priced-in-preflight",
            "source_overview": _priced_in_source_batches_command(
                "all",
                stocks_only=stocks_only,
            ),
            "export_full_scan": _priced_in_queue_full_scan_command(
                stocks_only=stocks_only,
                all_rows=True,
            ),
            "audit_full_scan": audit_full_export_command,
        },
    }


def _priced_in_audit_primary_full_scan(
    full_scan: Mapping[str, object],
    *,
    preview_scan: Mapping[str, object],
    ranked_rows: int,
    active_securities: int,
    all_rows_requested: bool,
    source_gaps: Sequence[str],
    audit_page_command: str,
    audit_next_page_command: str | None,
    audit_full_export_command: str,
    stocks_only: bool = False,
) -> dict[str, object]:
    visible_rows = int(_finite_float(preview_scan.get("visible_rows")))
    row_start = int(_finite_float(preview_scan.get("visible_row_start")))
    row_end = int(_finite_float(preview_scan.get("visible_row_end")))
    display_mode = "complete_full_scan" if all_rows_requested else "page_preview"
    source_gap_text = ", ".join(source_gaps)
    focus = (
        f"full scan rows with source gap(s): {source_gap_text}"
        if source_gaps
        else "full active-universe scan"
    )
    visible_note = (
        f"Visible rows cover all {ranked_rows} ranked full-scan row(s)."
        if all_rows_requested
        else (
            f"Visible rows {row_start}-{row_end} are one page from "
            f"{ranked_rows} ranked full-scan row(s). Use export_command for "
            "the complete local scan."
        )
    )
    return {
        "schema_version": "priced-in-primary-full-scan-v1",
        "scope": "stocks_only" if stocks_only else "full_active_universe",
        "stocks_only": bool(stocks_only),
        "instrument_filter": "stocks_only" if stocks_only else "all_instruments",
        "focus": focus,
        "mode": full_scan.get("mode") or "full_scan",
        "active_securities": active_securities,
        "scanned_rows": full_scan.get("scanned_rows") or ranked_rows,
        "ranked_rows": ranked_rows,
        "visible_row_start": row_start,
        "visible_row_end": row_end,
        "visible_rows": visible_rows,
        "display_mode": display_mode,
        "all_rows_requested": bool(all_rows_requested),
        "visible_rows_are_full_scan": bool(all_rows_requested),
        "visible_rows_are_page": not bool(all_rows_requested),
        "source_gap_filter": list(source_gaps),
        "shortlist_role": "priority_lens_not_scan_scope",
        "source_batch_role": "provider_fill_logistics_not_scan_scope",
        "summary": (
            f"MarketRadar scanned and ranked {ranked_rows} row(s) from "
            f"{active_securities} active securities."
        ),
        "visible_rows_note": visible_note,
        "scope_boundary": (
            "The full scan is the ranked universe. Shortlists are priority lenses; "
            "provider batches are evidence-fill chunks."
        ),
        "review_command": audit_page_command,
        "next_page_command": audit_next_page_command,
        "export_command": audit_full_export_command,
        "queue_export_command": _priced_in_queue_full_scan_command(
            stocks_only=stocks_only,
            all_rows=True,
        ),
        "external_calls_made": 0,
    }


def _priced_in_audit_answer_shortlist(
    rows: Sequence[object],
    *,
    source_gaps: Sequence[str] = (),
    full_scan_rows: int = 0,
    limit: int = 10,
    full_scan_review_command: str | None = None,
    full_scan_export_command: str | None = None,
    stocks_only: bool = False,
) -> dict[str, object]:
    answer_rows = _priced_in_answer_rows(rows, stocks_only=stocks_only)
    decision_rows = [row for row in answer_rows if bool(row.get("decision_ready"))]
    selected = decision_rows or answer_rows
    resolved_limit = _positive_limit(limit)
    visible_rows = [
        {
            "rank": index,
            **_row_dict(row),
        }
        for index, row in enumerate(selected[:resolved_limit], start=1)
    ]
    decision_count = len(decision_rows)
    actionable_count = len(answer_rows)
    blocked_count = max(0, actionable_count - decision_count)
    focus = (
        f"source_gap:{','.join(source_gaps)}"
        if source_gaps
        else "full_scan"
    )
    if decision_count > 0:
        status = "decision_ready"
        summary = (
            f"Showing {len(visible_rows)} of {decision_count} decision-ready "
            "not-priced-in row(s)."
        )
    elif actionable_count > 0:
        status = "needs_evidence"
        summary = (
            f"No decision-ready not-priced-in rows in this focus; showing "
            f"{len(visible_rows)} actionable row(s) that still need evidence."
        )
    else:
        status = "none_visible"
        summary = "No actionable not-priced-in rows are visible in this focus."
    scope_text = (
        f"Filtered to source gap(s): {', '.join(source_gaps)}."
        if source_gaps
        else "Full active-universe ranked scan."
    )
    selected_count = len(selected)
    selection_note = (
        f"These {len(visible_rows)} visible row(s) are a priority lens over "
        f"{full_scan_rows} ranked full-scan row(s), not the scan universe."
        if full_scan_rows
        else "These visible rows are a priority lens, not the scan universe."
    )
    return {
        "schema_version": "priced-in-answer-shortlist-v1",
        "status": status,
        "lens": "market_expectation_priority_lens",
        "focus": focus,
        "source_gap_filter": list(source_gaps),
        "summary": summary,
        "scope": scope_text,
        "selection_scope": "priority_lens_not_scan_universe",
        "selection_note": selection_note,
        "full_scan_rows": full_scan_rows,
        "actionable_mismatch_rows": actionable_count,
        "decision_ready_rows": decision_count,
        "needs_evidence_rows": blocked_count,
        "visible_rows": len(visible_rows),
        "selected_priority_rows": selected_count,
        "visible_rows_are_sample": len(visible_rows) < selected_count,
        "full_scan_review_command": full_scan_review_command,
        "full_scan_export_command": full_scan_export_command,
        "external_calls_made": 0,
        "investment_decision_boundary": (
            "This shortlist ranks market-expectation mismatch evidence only; it is "
            "not trade approval."
        ),
        "rows": visible_rows,
    }


def _priced_in_audit_source_gap_actions(
    engine: Engine,
    config: AppConfig,
    source_rows: Sequence[Mapping[str, object]],
    source_gaps: Sequence[str],
    available_at: datetime | None,
    queue: Mapping[str, object] | None,
    stocks_only: bool = False,
) -> list[dict[str, object]]:
    if not source_gaps:
        return []
    selected = set(source_gaps)
    actions: list[dict[str, object]] = []
    for row in source_rows:
        source = str(row.get("source") or "").strip()
        if source not in selected:
            continue
        plan = priced_in_source_gap_batches_payload(
            engine,
            config,
            source=source,
            batch_limit=1,
            available_at=available_at,
            status="all",
            stocks_only=stocks_only,
            queue=queue,
        )
        actions.append(
            {
                "source": source,
                "status": row.get("status"),
                "gap_count": int(_finite_float(row.get("gap_count"))),
                "coverage_pct": row.get("coverage_pct"),
                "next_action": row.get("next_action"),
                "plan_command": row.get("command"),
                **_priced_in_audit_source_gap_batch_action(plan),
                "execution_boundary": (
                    "Planning and browsing make 0 provider calls; execute source "
                    "batches only after approving provider calls."
                ),
            }
        )
    return actions


def _priced_in_audit_preview_queue_from_planning_rows(
    queue: Mapping[str, object],
    *,
    source_gaps: Sequence[str],
    limit: int,
    offset: int,
) -> dict[str, object] | None:
    planning_rows = queue.get("planning_rows")
    if not isinstance(planning_rows, list | tuple):
        return None
    filtered_rows = [
        row
        for row in planning_rows
        if isinstance(row, Mapping) and _priced_in_source_gap_matches(row, source_gaps)
    ]
    resolved_limit = _positive_limit(limit)
    resolved_offset = _positive_offset(offset)
    page_rows = filtered_rows[resolved_offset : resolved_offset + resolved_limit]
    filters = {
        **_row_dict(_mapping_value(queue, "filters")),
        "status": "all",
        "source_gap": list(source_gaps),
        "limit": resolved_limit,
        "offset": resolved_offset,
    }
    return {
        **_row_dict(queue),
        "filters": filters,
        "count": len(page_rows),
        "returned_count": len(page_rows),
        "total_count": len(filtered_rows),
        "offset": resolved_offset,
        "has_more": resolved_offset + len(page_rows) < len(filtered_rows),
        "rows": page_rows,
    }


def _priced_in_audit_source_gap_batch_action(
    plan: Mapping[str, object],
) -> dict[str, object]:
    batches = _sequence_value(plan.get("batches"))
    first_batch = next((batch for batch in batches if isinstance(batch, Mapping)), None)
    first_batch_payload = _priced_in_first_source_batch_payload(first_batch)
    diagnostic = _mapping_value(plan, "diagnostic")
    scan_scope = _mapping_value(plan, "scan_scope")
    first_batch_tickers = (
        list(_sequence_value(first_batch_payload.get("tickers")))
        if isinstance(first_batch_payload, Mapping)
        else []
    )
    first_batch_external_calls = (
        int(_finite_float(first_batch_payload.get("external_calls_required")))
        if isinstance(first_batch_payload, Mapping)
        else None
    )
    first_batch_command = (
        first_batch_payload.get("command")
        if isinstance(first_batch_payload, Mapping)
        else None
    )
    batch_count = int(_finite_float(plan.get("batch_count")))
    total_gap_rows = int(_finite_float(plan.get("total_gap_rows")))
    batch_scope = (
        f"First provider batch only; full scan has {total_gap_rows} "
        f"gap row(s) and {batch_count} planned batch(es)."
        if first_batch_tickers
        else str(scan_scope.get("explanation") or "").strip()
    )
    return {
        "batch_status": plan.get("status"),
        "full_scan_gap_rows": total_gap_rows,
        "plannable_gap_rows": int(_finite_float(plan.get("plannable_gap_rows"))),
        "unplannable_gap_rows": int(_finite_float(plan.get("unplannable_gap_rows"))),
        "blocked_gap_rows": int(
            _finite_float(plan.get("blocked_gap_rows"))
            or max(
                0,
                int(_finite_float(plan.get("unplannable_gap_rows")))
                - int(_finite_float(plan.get("routed_gap_rows"))),
            )
        ),
        "provider_batch_count": batch_count,
        "batch_size": int(_finite_float(plan.get("batch_size"))),
        "first_batch_scope": "first_provider_batch" if first_batch_tickers else None,
        "first_batch_tickers": first_batch_tickers,
        "first_batch_external_calls": first_batch_external_calls,
        "first_batch_command": first_batch_command,
        "execute_next_command": plan.get("execute_next_command"),
        "execute_batches_command": plan.get("execute_batches_command"),
        "review_rows_command": plan.get("review_rows_command"),
        "export_rows_command": plan.get("export_rows_command"),
        "all_batches_command": plan.get("all_batches_command"),
        "all_batches_api": plan.get("all_batches_api"),
        "approval_checklist": _row_dict(_mapping_value(plan, "approval_checklist")),
        "diagnostic_status": diagnostic.get("status"),
        "blocked_reason": diagnostic.get("blocked_reason"),
        "diagnostic_next_action": diagnostic.get("next_action"),
        "batch_scope": batch_scope,
        "batch_preview_note": scan_scope.get("batch_preview_note"),
    }


def _priced_in_audit_command(
    *,
    limit: int,
    offset: int,
    available_at: datetime | None,
    source_gap: Sequence[str],
    all_rows: bool = False,
    json: bool = False,
    stocks_only: bool = False,
) -> str:
    parts = ["catalyst-radar", "priced-in-audit"]
    if available_at is not None:
        parts.extend(["--available-at", available_at.isoformat()])
    if stocks_only:
        parts.append("--stocks-only")
    for source in source_gap:
        parts.extend(["--source-gap", str(source)])
    if all_rows:
        parts.append("--all")
    else:
        parts.extend(["--limit", str(_positive_limit(limit))])
        offset_value = _positive_offset(offset)
        if offset_value:
            parts.extend(["--offset", str(offset_value)])
    if json:
        parts.append("--json")
    return " ".join(parts)


def _priced_in_audit_preview_note(
    sample_explanation: object,
    *,
    source_gaps: Sequence[str],
) -> str:
    base = str(sample_explanation or "").strip()
    if not source_gaps:
        return base
    filter_note = (
        "This audit row page is filtered to rows missing or stale for "
        f"{', '.join(source_gaps)}."
    )
    return f"{filter_note} {base}".strip()


def _priced_in_full_scan_preview_rows(rows: Sequence[object]) -> list[dict[str, object]]:
    preview: list[dict[str, object]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        usefulness = _mapping_value(row, "usefulness")
        data_sources = _mapping_value(row, "data_sources")
        missing_sources = [
            str(item)
            for item in _sequence_value(data_sources.get("missing"))
            if str(item).strip()
        ]
        stale_sources = [
            str(item)
            for item in _sequence_value(data_sources.get("stale"))
            if str(item).strip()
        ]
        preview.append(
            {
                "ticker": row.get("ticker"),
                "status": row.get("priced_in_status"),
                "usefulness": usefulness.get("status"),
                "decision_ready": bool(usefulness.get("decision_ready")),
                "direction": row.get("priced_in_direction"),
                "emotion_reaction_gap": row.get("emotion_reaction_gap"),
                "emotion_score": row.get("emotion_score"),
                "reaction_score": row.get("reaction_score"),
                "priced_in_score": row.get("priced_in_score"),
                "missing_sources": missing_sources,
                "stale_sources": stale_sources,
                "why_now": row.get("why_now") or row.get("top_catalyst"),
                "next_step": row.get("next_step"),
            }
        )
    return preview


def _priced_in_audit_status(
    *,
    preflight_status: str,
    decision_ready_count: int,
    research_ready_count: int,
) -> str:
    normalized = preflight_status.strip().lower()
    if normalized == "blocked":
        return "blocked"
    if normalized == "attention":
        return "attention"
    if decision_ready_count > 0:
        return "decision_ready"
    if research_ready_count > 0:
        return "research_only"
    return "monitor_only"


def _priced_in_audit_answer(
    *,
    status: str,
    actionable_count: int,
    research_ready_count: int,
    decision_ready_count: int,
    blocked_count: int,
) -> str:
    if status == "blocked":
        return "No. Full-scan prerequisites still block a trustworthy answer."
    if status == "attention":
        return (
            "Partially. MarketRadar has research output, but source or coverage "
            "gaps still need attention before trusting the answer."
        )
    if decision_ready_count:
        return (
            f"Yes for review: {decision_ready_count} row(s) have a priced-in "
            "answer ready for human decision review."
        )
    if research_ready_count:
        return (
            f"Research only: {research_ready_count} not-priced-in lead(s) need "
            "more evidence before decision review."
        )
    if actionable_count:
        return (
            f"Not yet. {actionable_count} actionable mismatch row(s) remain "
            f"but {blocked_count} row(s) are blocked or incomplete."
        )
    return "No actionable emotion-versus-price mismatch is currently ready."


def _priced_in_audit_market_bars(
    engine: Engine,
    config: AppConfig,
    queue: Mapping[str, object],
    preflight: Mapping[str, object],
) -> dict[str, object]:
    scan = _mapping_value(queue, "scan")
    freshness = _mapping_value(scan, "freshness")
    latest_run = _mapping_value(queue, "latest_run")
    active = int(_finite_float(freshness.get("active_security_count")))
    if active <= 0:
        active = int(_finite_float(queue.get("total_count")))
    with_as_of_bar = int(
        _finite_float(freshness.get("active_security_with_as_of_bar_count"))
    )
    missing = int(_finite_float(freshness.get("missing_as_of_daily_bar_count")))
    if with_as_of_bar <= 0 and missing <= 0 and active > 0:
        with_as_of_bar = active
    rows_by_area = {
        str(row.get("area") or ""): row
        for row in _sequence_value(preflight.get("rows"))
        if isinstance(row, Mapping)
    }
    filters = _mapping_value(queue, "filters")
    stocks_only = bool(filters.get("stocks_only"))
    market_row = _row_dict(rows_by_area.get("market_bars", {}))
    target_as_of = (
        _parse_date(latest_run.get("as_of"))
        or _parse_date(freshness.get("latest_candidate_session_date"))
        or _parse_date(freshness.get("latest_daily_bar_date"))
    )
    repair = _priced_in_audit_market_bar_repair(
        engine=engine,
        config=config,
        active=active,
        with_as_of_bar=with_as_of_bar,
        missing=missing,
        target_as_of=target_as_of,
        market_row=market_row,
        missing_tickers=_sequence_value(
            freshness.get("missing_as_of_daily_bar_tickers")
        ),
        stocks_only=stocks_only,
    )
    return {
        "status": market_row.get("status") or ("ready" if missing == 0 else "attention"),
        "active_securities": active,
        "with_as_of_bar": with_as_of_bar,
        "missing_as_of_bar": missing,
        "coverage_pct": round((with_as_of_bar / active) * 100, 1) if active else 0.0,
        "target_as_of": _date_iso_or_none(target_as_of),
        "missing_as_of_bar_tickers": repair.get("missing_as_of_bar_tickers"),
        "finding": market_row.get("finding"),
        "next_action": market_row.get("next_action"),
        "command": repair.get("template_command") or market_row.get("command"),
        "api": repair.get("template_api") or market_row.get("api"),
        "repair": repair,
    }


def _priced_in_audit_market_bar_repair(
    *,
    engine: Engine,
    config: AppConfig,
    active: int,
    with_as_of_bar: int,
    missing: int,
    target_as_of: date | None,
    market_row: Mapping[str, object],
    missing_tickers: Sequence[object],
    stocks_only: bool,
) -> dict[str, object]:
    missing_sample = [
        str(ticker).strip().upper()
        for ticker in missing_tickers
        if str(ticker).strip()
    ]
    template_command = _csv_market_template_command(
        target_as_of,
        missing_only=True,
        stocks_only=stocks_only,
    )
    import_preview_command = _csv_market_refresh_command(
        target_as_of,
        execute=False,
        stocks_only=stocks_only,
    )
    import_execute_command = _csv_market_refresh_command(
        target_as_of,
        execute=True,
        stocks_only=stocks_only,
    )
    diagnostic = _priced_in_market_bar_missing_diagnostic(
        engine,
        target_as_of=target_as_of,
        missing_ticker_fallback=missing_sample,
    )
    stock_scope = _priced_in_market_bar_stock_scope(
        engine,
        target_as_of=target_as_of,
    )
    effective_missing = (
        int(_finite_float(stock_scope.get("stock_like_missing_as_of_bar")))
        if stocks_only
        else missing
    )
    provider_active = (
        int(_finite_float(stock_scope.get("stock_like_active")))
        if stocks_only
        else active
    )
    provider_existing = (
        int(_finite_float(stock_scope.get("stock_like_with_as_of_bar")))
        if stocks_only
        else with_as_of_bar
    )
    provider_fill_plan = _priced_in_market_bar_provider_fill_plan(
        engine,
        config,
        target_as_of=target_as_of,
        missing=effective_missing,
        active_security_count=provider_active,
        existing_as_of_bar_count=provider_existing,
        coverage_scope="stock_like" if stocks_only else "active_universe",
        missing_as_of_bar_ticker_sample=missing_sample,
        missing_security_type_counts=_mapping_value(diagnostic, "type_counts"),
        missing_universe_diagnostic=diagnostic,
    )
    manual_repair_plan: dict[str, object] = {}
    stock_manual_repair_plan: dict[str, object] = {}
    provider_health_kwargs = _manual_repair_provider_health_kwargs(engine)
    if target_as_of is not None:
        try:
            manual_repair_plan = manual_market_bars_repair_plan(
                engine,
                expected_as_of=target_as_of,
                stocks_only=stocks_only,
                provider_key_configured=config.polygon_api_key_configured,
                **provider_health_kwargs,
            ).as_payload()
        except ValueError as exc:
            manual_repair_plan = {
                "schema_version": "manual-market-bars-repair-plan-v1",
                "status": "invalid",
                "expected_as_of": target_as_of.isoformat(),
                "stocks_only": stocks_only,
                "error": str(exc),
                "external_calls_made": 0,
            }
        if stocks_only:
            stock_manual_repair_plan = manual_repair_plan
        else:
            try:
                stock_manual_repair_plan = manual_market_bars_repair_plan(
                    engine,
                    expected_as_of=target_as_of,
                    stocks_only=True,
                    provider_key_configured=config.polygon_api_key_configured,
                    **provider_health_kwargs,
                ).as_payload()
            except ValueError as exc:
                stock_manual_repair_plan = {
                    "schema_version": "manual-market-bars-repair-plan-v1",
                    "status": "invalid",
                    "expected_as_of": target_as_of.isoformat(),
                    "stocks_only": True,
                    "error": str(exc),
                    "external_calls_made": 0,
                }
    stock_scope = _priced_in_stock_scope_with_manual_repair(
        stock_scope,
        stock_manual_repair_plan,
    )
    local_template_preview = _mapping_value(
        manual_repair_plan,
        "local_template_preview",
    )
    local_template_fill_progress = _mapping_value(
        local_template_preview,
        "fill_progress",
    )
    status = (
        "ready"
        if effective_missing <= 0
        else str(market_row.get("status") or "attention")
    )
    if effective_missing <= 0:
        next_action = (
            "Stock-like rows have as-of market bars."
            if stocks_only
            else "As-of market bars cover the active universe."
        )
    elif stocks_only:
        next_action = (
            "Generate the DB-backed stock-like missing-bar template, fill only "
            "the missing common stock and ADR rows for the scan date, preview "
            "the import, then execute the local DB import only when stock-like "
            "coverage is complete."
        )
    else:
        next_action = (
            "Generate the DB-backed missing-bar template, fill only the missing "
            "ticker rows for the scan date, preview the import, then execute "
            "the local DB import only when coverage is complete."
        )
    return {
        "schema_version": "priced-in-market-bar-repair-v1",
        "status": status,
        "target_as_of": _date_iso_or_none(target_as_of),
        "active_securities": active,
        "with_as_of_bar": with_as_of_bar,
        "missing_as_of_bar": missing,
        "stocks_only": stocks_only,
        "coverage_scope": "stock_like" if stocks_only else "active_universe",
        "missing_as_of_bar_tickers": missing_sample,
        "missing_as_of_bar_ticker_sample": missing_sample[:12],
        "template_command": template_command,
        "import_preview_command": import_preview_command,
        "import_execute_command": import_execute_command,
        "required_fill_fields": list(MANUAL_BAR_REQUIRED_FILL_FIELDS),
        "dashboard_manual_template_command": manual_repair_plan.get(
            "dashboard_manual_template_command"
        ),
        "dashboard_manual_template_regenerate_command": manual_repair_plan.get(
            "dashboard_manual_template_regenerate_command"
        ),
        "dashboard_manual_import_preview_command": manual_repair_plan.get(
            "dashboard_manual_import_preview_command"
        ),
        "dashboard_manual_import_execute_command": manual_repair_plan.get(
            "dashboard_manual_import_execute_command"
        ),
        "blank_required_field_counts_if_new_template": {
            field_name: effective_missing
            for field_name in MANUAL_BAR_REQUIRED_FILL_FIELDS
        }
        if effective_missing > 0
        else {},
        "template_row_count": effective_missing,
        "local_template_path": manual_repair_plan.get("local_template_path"),
        "local_template_exists": bool(manual_repair_plan.get("local_template_exists")),
        "local_template_preview": (
            _row_dict(local_template_preview) if local_template_preview else None
        ),
        "local_template_fill_progress": _row_dict(local_template_fill_progress),
        "missing_universe_diagnostic": _row_dict(
            _mapping_value(manual_repair_plan, "missing_universe_diagnostic")
        ),
        "operator_step": _row_dict(
            _mapping_value(manual_repair_plan, "operator_step"),
        ),
        "template_api": "POST /api/radar/market-bars/template",
        "import_api": "POST /api/radar/market-bars/import",
        "diagnostic": diagnostic,
        "stock_scope": stock_scope,
        "provider_fill_plan": provider_fill_plan,
        "external_calls_made": 0,
        "write_boundary": (
            "Template generation writes a local CSV. Import preview makes no DB "
            "writes. Import --execute writes local daily bars only; none of these "
            "commands call market providers."
        ),
        "next_action": next_action,
    }


def _priced_in_stock_scope_with_manual_repair(
    stock_scope: Mapping[str, object],
    repair_plan: Mapping[str, object],
) -> dict[str, object]:
    enriched = _row_dict(stock_scope)
    if not repair_plan:
        return enriched
    operator_step = _row_dict(_mapping_value(repair_plan, "operator_step"))
    local_template_preview = _mapping_value(repair_plan, "local_template_preview")
    local_template_fill_progress = _mapping_value(
        local_template_preview,
        "fill_progress",
    )
    local_template_schema = _mapping_value(repair_plan, "local_template_schema")
    enriched.update(
        {
            "manual_template_regenerate_command": repair_plan.get(
                "manual_template_regenerate_command",
            ),
            "dashboard_manual_template_command": repair_plan.get(
                "dashboard_manual_template_command"
            ),
            "dashboard_manual_template_regenerate_command": repair_plan.get(
                "dashboard_manual_template_regenerate_command"
            ),
            "dashboard_manual_import_preview_command": repair_plan.get(
                "dashboard_manual_import_preview_command"
            ),
            "dashboard_manual_import_execute_command": repair_plan.get(
                "dashboard_manual_import_execute_command"
            ),
            "local_template_path": repair_plan.get("local_template_path"),
            "local_template_exists": bool(repair_plan.get("local_template_exists")),
            "local_template_schema": (
                _row_dict(local_template_schema) if local_template_schema else {}
            ),
            "local_template_preview": (
                _row_dict(local_template_preview) if local_template_preview else None
            ),
            "local_template_fill_progress": _row_dict(
                local_template_fill_progress,
            ),
            "operator_step": operator_step,
        }
    )
    action = str(operator_step.get("action") or "").strip()
    if action:
        enriched["next_action"] = action
    return enriched


def _priced_in_market_bar_stock_scope(
    engine: Engine,
    *,
    target_as_of: date | None,
) -> dict[str, object]:
    answer_boundary = (
        "This is the market-bar boundary for a stocks-only priced-in answer. "
        "It does not make the full active-universe answer complete; funds, "
        "wrappers, preferreds, rights, warrants, and unknown instruments still "
        "need their own bars or an explicit route."
    )
    if target_as_of is None:
        return {
            "schema_version": "priced-in-market-bar-stock-scope-v1",
            "status": "unknown_as_of",
            "target_as_of": None,
            "stock_like_security_types": sorted(PRICED_IN_COMPANY_LIKE_SECURITY_TYPES),
            "stock_like_active": 0,
            "stock_like_with_as_of_bar": 0,
            "stock_like_missing_as_of_bar": 0,
            "stock_like_coverage_pct": 0.0,
            "non_stock_active": 0,
            "non_stock_missing_as_of_bar": 0,
            "unknown_type_active": 0,
            "unknown_type_missing_as_of_bar": 0,
            "sample_missing_stock_like_tickers": [],
            "external_calls_made": 0,
            "answer_boundary": answer_boundary,
            "next_action": "Resolve the scan date before judging stock-only coverage.",
        }

    try:
        with engine.connect() as conn:
            active_rows = conn.execute(
                select(
                    securities.c.ticker,
                    securities.c.metadata,
                ).where(securities.c.is_active.is_(True))
            ).all()
            covered = {
                str(row._mapping["ticker"]).strip().upper()
                for row in conn.execute(
                    select(daily_bars.c.ticker).where(
                        daily_bars.c.date == target_as_of
                    )
                )
                if str(row._mapping["ticker"]).strip()
            }
    except SQLAlchemyError:
        active_rows = []
        covered = set()

    stock_like_active = 0
    stock_like_with_bar = 0
    missing_stock_like: list[str] = []
    non_stock_active = 0
    non_stock_missing = 0
    unknown_active = 0
    unknown_missing = 0
    for row in active_rows:
        ticker = str(row._mapping["ticker"] or "").strip().upper()
        if not ticker:
            continue
        metadata = row._mapping["metadata"] or {}
        if not isinstance(metadata, Mapping):
            metadata = {}
        security_type = str(metadata.get("type") or "").strip().upper() or "UNKNOWN"
        has_bar = ticker in covered
        if _is_sec_company_like_type(security_type):
            stock_like_active += 1
            if has_bar:
                stock_like_with_bar += 1
            else:
                missing_stock_like.append(ticker)
            continue
        if security_type == "UNKNOWN":
            unknown_active += 1
            if not has_bar:
                unknown_missing += 1
            continue
        non_stock_active += 1
        if not has_bar:
            non_stock_missing += 1

    stock_like_missing = len(missing_stock_like)
    if stock_like_active <= 0:
        status = "blocked"
        next_action = "Seed or classify active securities before claiming a stock scan."
    elif stock_like_missing <= 0:
        status = "ready"
        next_action = (
            "Stock-like rows have as-of bars. A stocks-only priced-in answer can "
            "be separated while full active-universe repair continues for "
            "fund/wrapper/unknown rows."
        )
    else:
        status = "attention"
        next_action = (
            "Fill stock-like missing as-of bars first; they are required before "
            "the system can claim a complete stocks-only priced-in answer."
        )
    manual_template_command = _csv_market_template_command(
        target_as_of,
        missing_only=True,
        stocks_only=True,
    )
    manual_import_preview_command = _csv_market_refresh_command(
        target_as_of,
        execute=False,
        stocks_only=True,
    )
    manual_import_execute_command = _csv_market_refresh_command(
        target_as_of,
        execute=True,
        stocks_only=True,
    )

    return {
        "schema_version": "priced-in-market-bar-stock-scope-v1",
        "status": status,
        "target_as_of": _date_iso_or_none(target_as_of),
        "stock_like_security_types": sorted(PRICED_IN_COMPANY_LIKE_SECURITY_TYPES),
        "stock_like_active": stock_like_active,
        "stock_like_with_as_of_bar": stock_like_with_bar,
        "stock_like_missing_as_of_bar": stock_like_missing,
        "stock_like_coverage_pct": _source_coverage_pct(
            available=stock_like_with_bar,
            stale=0,
            missing=stock_like_missing,
        ),
        "non_stock_active": non_stock_active,
        "non_stock_missing_as_of_bar": non_stock_missing,
        "unknown_type_active": unknown_active,
        "unknown_type_missing_as_of_bar": unknown_missing,
        "sample_missing_stock_like_tickers": _sample_tickers(missing_stock_like),
        "external_calls_made": 0,
        "answer_boundary": answer_boundary,
        "next_action": next_action,
        "manual_template_command": manual_template_command,
        "manual_import_preview_command": manual_import_preview_command,
        "manual_import_execute_command": manual_import_execute_command,
    }


def _manual_repair_provider_health_kwargs(engine: Engine) -> dict[str, object]:
    health = _latest_provider_health_payload(engine, "polygon")
    return {
        "provider_health_status": health.get("status"),
        "provider_health_reason": health.get("reason"),
        "provider_health_checked_at": _as_utc_datetime_or_none(
            health.get("checked_at"),
        ),
    }


def _latest_provider_health_payload(
    engine: Engine,
    provider: str,
) -> dict[str, object]:
    try:
        health = ProviderRepository(engine).latest_health(provider)
    except SQLAlchemyError:
        return {}
    if health is None:
        return {}
    return {
        "provider": provider,
        "status": health.status.value,
        "reason": health.reason,
        "checked_at": health.checked_at.isoformat(),
    }


def _priced_in_market_bar_provider_fill_plan(
    engine: Engine,
    config: AppConfig,
    *,
    target_as_of: date | None,
    missing: int,
    active_security_count: int | None = None,
    existing_as_of_bar_count: int | None = None,
    coverage_scope: str = "active_universe",
    missing_as_of_bar_ticker_sample: Sequence[object] | None = None,
    missing_as_of_bar_ticker_more: int | None = None,
    missing_security_type_counts: Mapping[str, object] | None = None,
    missing_universe_diagnostic: Mapping[str, object] | None = None,
) -> dict[str, object]:
    provider_health = _latest_provider_health_payload(engine, "polygon")
    target_value = _date_iso_or_none(target_as_of)
    provider_command = (
        "catalyst-radar ingest-polygon grouped-daily "
        f"--date {target_value} --confirm-external-call"
        if target_value
        else None
    )
    saved_file_path = (
        Path("data") / "local" / f"polygon-grouped-daily-{target_value}.json"
        if target_value
        else None
    )
    saved_file_import_command = (
        "catalyst-radar market-bars saved-import "
        f"--expected-as-of {target_value} --fixture {saved_file_path}"
        if target_value and saved_file_path is not None
        else None
    )
    saved_file_capture_command = (
        "catalyst-radar market-bars saved-capture "
        f"--expected-as-of {target_value} --out {saved_file_path} "
        f"--expect-active-count {active_security_count} "
        f"--expect-existing-count {existing_as_of_bar_count} "
        f"--expect-missing-count {max(0, int(missing))} "
        "--confirm-external-call"
        if target_value and saved_file_path is not None
        else None
    )
    saved_file_validate_command = (
        "catalyst-radar market-bars saved-validate "
        f"--expected-as-of {target_value} --fixture {saved_file_path}"
        if target_value and saved_file_path is not None
        else None
    )
    saved_file_capture_request_body = (
        {
            "expected_as_of": target_value,
            "output_path": str(saved_file_path),
            "confirm_external_call": False,
            "expected_active_security_count": active_security_count,
            "expected_existing_as_of_bar_count": existing_as_of_bar_count,
            "expected_missing_as_of_bar_count": max(0, int(missing)),
        }
        if target_value and saved_file_path is not None
        else None
    )
    saved_file_capture_confirm_request_body = (
        {**saved_file_capture_request_body, "confirm_external_call": True}
        if saved_file_capture_request_body
        else None
    )
    saved_file_validate_request_body = (
        {"expected_as_of": target_value, "fixture_path": str(saved_file_path)}
        if target_value and saved_file_path is not None
        else None
    )
    saved_file_import_preview_request_body = (
        {**saved_file_validate_request_body, "execute": False}
        if saved_file_validate_request_body
        else None
    )
    saved_file_import_request_body = (
        {**saved_file_validate_request_body, "execute": True}
        if saved_file_validate_request_body
        else None
    )
    saved_file_exists = bool(saved_file_path is not None and saved_file_path.exists())
    saved_file_status = (
        "available"
        if saved_file_exists
        else ("missing" if target_value and missing > 0 else "not_needed")
    )
    if saved_file_status == "available":
        saved_file_next_action = (
            "Validate the saved grouped-daily JSON response, then import it. "
            "Both saved-file steps make 0 provider calls."
        )
    elif saved_file_status == "missing":
        saved_file_next_action = (
            "Capture or obtain the saved grouped-daily JSON response before "
            "running saved-file validate/import."
        )
    else:
        saved_file_next_action = "No saved-file import is needed."
    execute_call_count = 1 if target_value and missing > 0 else 0
    key_configured = bool(config.polygon_api_key_configured)
    provider_health_gate = (
        manual_bar_provider_health_gate(
            status=provider_health.get("status"),
            reason=provider_health.get("reason"),
            checked_at=_as_utc_datetime_or_none(provider_health.get("checked_at")),
            target_as_of=target_as_of,
        )
        if target_as_of is not None
        else {
            "blocks_provider_fill": False,
            "warning": None,
            "external_calls_made": 0,
        }
    )
    provider_health_blocks_fill = bool(
        provider_health_gate.get("blocks_provider_fill"),
    )
    provider_health_warning = (
        str(provider_health_gate.get("warning") or "").strip() or None
    )
    missing_sample = _sample_tickers(
        [
            str(ticker).strip().upper()
            for ticker in _sequence_value(missing_as_of_bar_ticker_sample or [])
            if str(ticker).strip()
        ]
    )
    if missing_as_of_bar_ticker_more is None:
        missing_more = max(0, int(missing) - len(missing_sample))
    else:
        missing_more = max(0, int(_finite_float(missing_as_of_bar_ticker_more)))
    missing_diagnostic = _row_dict(missing_universe_diagnostic or {})
    if not missing_diagnostic and target_as_of is not None and missing > 0:
        missing_diagnostic = _priced_in_market_bar_missing_diagnostic(
            engine,
            target_as_of=target_as_of,
            missing_ticker_fallback=missing_sample,
        )
    missing_type_counts = {
        str(security_type).strip().upper(): int(_finite_float(count))
        for security_type, count in dict(missing_security_type_counts or {}).items()
        if str(security_type).strip() and int(_finite_float(count)) > 0
    }
    if not missing_type_counts:
        missing_type_counts = {
            str(security_type).strip().upper(): int(_finite_float(count))
            for security_type, count in _mapping_value(
                missing_diagnostic,
                "type_counts",
            ).items()
            if str(security_type).strip() and int(_finite_float(count)) > 0
        }
    if missing <= 0:
        status = "not_needed"
        next_action = "No market-bar provider fill is needed."
    elif target_as_of is None:
        status = "blocked"
        next_action = "Resolve the scan date before planning a provider bar fill."
    elif provider_health_blocks_fill:
        status = "blocked_by_provider_health"
        next_action = (
            "Stored Polygon/Massive health is down; use the manual CSV path or "
            "import a saved grouped-daily JSON response before requesting the "
            "grouped-daily fill."
        )
    elif key_configured:
        status = (
            "ready_for_approval_with_health_warning"
            if provider_health_warning
            else "ready_for_approval"
        )
        if provider_health_warning:
            next_action = (
                "Stored Polygon/Massive health was a stale same-day EOD denial; "
                "import a saved grouped-daily JSON response, or run the grouped-daily "
                "command only if you approve one historical market-data provider call."
            )
        else:
            next_action = (
                "Import a saved grouped-daily JSON response, or if you approve one "
                "market-data provider call, run the grouped-daily command, then rerun "
                "the scan/audit from the updated local bars."
            )
    else:
        status = "blocked"
        next_action = (
            "Use the missing-only manual CSV template, import a saved grouped-daily "
            "JSON response, or set a real Polygon/Massive API key; do not run the "
            "provider command until explicitly approved."
        )
    saved_file_capture_approval_packet = (
        provider_saved_file_capture_approval_packet(
            expected_as_of=target_as_of,
            coverage_scope=coverage_scope,
            active_security_count=active_security_count,
            existing_as_of_bar_count=existing_as_of_bar_count,
            missing_as_of_bar_count=max(0, int(missing)),
            missing_as_of_bar_ticker_sample=missing_sample,
            missing_as_of_bar_ticker_more=missing_more,
            missing_security_type_counts=missing_type_counts,
            missing_universe_diagnostic=missing_diagnostic,
            provider_key_configured=key_configured,
            provider_fill_status=status,
            provider_health_blocks_fill=provider_health_blocks_fill,
            provider_health_warning=provider_health_warning,
            provider_saved_file_path=saved_file_path,
            provider_saved_file_status=saved_file_status,
            provider_saved_file_capture_command=saved_file_capture_command,
            provider_saved_file_capture_request_body=saved_file_capture_request_body,
            provider_saved_file_capture_confirm_request_body=(
                saved_file_capture_confirm_request_body
            ),
            provider_saved_file_validate_command=saved_file_validate_command,
            provider_saved_file_validate_request_body=saved_file_validate_request_body,
            provider_saved_file_import_command=saved_file_import_command,
            provider_saved_file_import_preview_request_body=(
                saved_file_import_preview_request_body
            ),
            provider_saved_file_import_request_body=saved_file_import_request_body,
        )
        if target_as_of is not None and saved_file_path is not None
        else None
    )
    return {
        "schema_version": "priced-in-market-bar-provider-fill-plan-v1",
        "status": status,
        "provider": "polygon",
        "provider_label": "Polygon/Massive grouped daily",
        "target_as_of": target_value,
        "coverage_scope": coverage_scope,
        "active_security_count": active_security_count,
        "existing_as_of_bar_count": existing_as_of_bar_count,
        "missing_as_of_bar": max(0, int(missing)),
        "provider_key_configured": key_configured,
        "provider_health": provider_health or None,
        "provider_health_blocks_fill": provider_health_blocks_fill,
        "provider_health_warning": provider_health_warning,
        "execute_external_call_count": execute_call_count,
        "external_calls_made": 0,
        "provider_call_command": provider_command,
        "provider_call_api": None,
        "provider_saved_file_path": str(saved_file_path) if saved_file_path else None,
        "provider_saved_file_exists": saved_file_exists,
        "provider_saved_file_status": saved_file_status,
        "provider_saved_file_next_action": saved_file_next_action,
        "provider_saved_file_capture_command": saved_file_capture_command,
        "provider_saved_file_capture_api": (
            "POST /api/radar/market-bars/provider-fixture-capture"
            if saved_file_capture_command
            else None
        ),
        "provider_saved_file_capture_request_body": (
            saved_file_capture_request_body if saved_file_capture_command else None
        ),
        "provider_saved_file_capture_confirm_request_body": (
            saved_file_capture_confirm_request_body if saved_file_capture_command else None
        ),
        "provider_saved_file_capture_external_call_count": 1
        if saved_file_capture_command and missing > 0
        else 0,
        "provider_saved_file_capture_approval_packet": (
            saved_file_capture_approval_packet
        ),
        "provider_saved_file_capture_approval_guard": _mapping_value(
            saved_file_capture_approval_packet,
            "approval_guard",
        ),
        "provider_saved_file_import_command": saved_file_import_command,
        "provider_saved_file_validate_command": saved_file_validate_command,
        "provider_saved_file_validate_api": (
            "POST /api/radar/market-bars/provider-fixture-preview"
            if saved_file_validate_command
            else None
        ),
        "provider_saved_file_validate_request_body": (
            saved_file_validate_request_body if saved_file_validate_command else None
        ),
        "provider_saved_file_import_api": (
            "POST /api/radar/market-bars/provider-fixture-import"
            if saved_file_import_command
            else None
        ),
        "provider_saved_file_import_preview_request_body": (
            saved_file_import_preview_request_body if saved_file_import_command else None
        ),
        "provider_saved_file_import_request_body": (
            saved_file_import_request_body if saved_file_import_command else None
        ),
        "provider_saved_file_external_call_count": 0,
        "provider_saved_file_boundary": (
            "Capture or obtain the saved Polygon/Massive grouped-daily JSON response "
            "before import. Capture makes one provider call only with explicit "
            "approval. Validation and import read from disk and make 0 provider calls."
        ),
        "manual_template_command": _csv_market_template_command(
            target_as_of,
            missing_only=True,
        ),
        "manual_import_preview_command": _csv_market_refresh_command(
            target_as_of,
            execute=False,
        ),
        "approval_boundary": (
            "This plan makes 0 provider calls. The provider command makes one "
            "Polygon/Massive grouped-daily request and must only be run after "
            "explicit operator approval."
        ),
        "point_in_time_boundary": (
            "Grouped-daily ingest writes local bars from the fetch context. After "
            "provider fill, rerun the scan/audit instead of treating an older "
            "audit as automatically revalidated."
        ),
        "next_action": next_action,
    }


def _priced_in_market_bar_missing_diagnostic(
    engine: Engine,
    *,
    target_as_of: date | None,
    missing_ticker_fallback: Sequence[str],
) -> dict[str, object]:
    route_boundary = (
        "Market bars are required for price-reaction scoring. Non-company "
        "instruments can stay in a full active-universe scan only if their "
        "own bars are present; otherwise route or exclude them from a "
        "stocks-only answer."
    )
    if target_as_of is None:
        fallback_rows = [
            (str(ticker).strip().upper(), "UNKNOWN")
            for ticker in missing_ticker_fallback
            if str(ticker).strip()
        ]
        return _priced_in_market_bar_missing_diagnostic_payload(
            target_as_of=None,
            missing_rows=fallback_rows,
            status="attention" if fallback_rows else "unknown_as_of",
            route_boundary=route_boundary,
            next_action=(
                "Cannot classify missing bars by instrument type until the scan "
                "or latest daily-bar date is known."
            ),
        )
    try:
        with engine.connect() as conn:
            active_rows = conn.execute(
                select(
                    securities.c.ticker,
                    securities.c.metadata,
                ).where(securities.c.is_active.is_(True))
            ).all()
            covered = {
                str(row._mapping["ticker"]).strip().upper()
                for row in conn.execute(
                    select(daily_bars.c.ticker).where(
                        daily_bars.c.date == target_as_of
                    )
                )
                if str(row._mapping["ticker"]).strip()
            }
    except SQLAlchemyError:
        active_rows = []
        covered = set()

    missing_rows: list[tuple[str, str]] = []
    for row in active_rows:
        ticker = str(row._mapping["ticker"] or "").strip().upper()
        if not ticker or ticker in covered:
            continue
        metadata = row._mapping["metadata"] or {}
        if not isinstance(metadata, Mapping):
            metadata = {}
        security_type = str(metadata.get("type") or "").strip().upper() or "UNKNOWN"
        missing_rows.append((ticker, security_type))
    if not missing_rows and missing_ticker_fallback:
        missing_rows = [
            (str(ticker).strip().upper(), "UNKNOWN")
            for ticker in missing_ticker_fallback
            if str(ticker).strip()
        ]
    if not missing_rows:
        return _priced_in_market_bar_missing_diagnostic_payload(
            target_as_of=target_as_of,
            missing_rows=[],
            status="ready",
            route_boundary=route_boundary,
            next_action="No missing as-of bars to classify.",
        )
    next_action = (
        "Fill company-like as-of bars first; then decide whether fund/wrapper/"
        "unknown tickers belong in this full active-universe scan or should be "
        "routed/excluded from a stocks-only scan."
    )
    return _priced_in_market_bar_missing_diagnostic_payload(
        target_as_of=target_as_of,
        missing_rows=missing_rows,
        status="attention",
        route_boundary=route_boundary,
        next_action=next_action,
    )


def _priced_in_market_bar_missing_diagnostic_payload(
    *,
    target_as_of: date | None,
    missing_rows: Sequence[tuple[str, str]],
    status: str,
    route_boundary: str,
    next_action: str,
) -> dict[str, object]:
    normalized_rows = [
        (str(ticker).strip().upper(), str(security_type).strip().upper() or "UNKNOWN")
        for ticker, security_type in missing_rows
        if str(ticker).strip()
    ]
    type_counts = Counter(security_type for _ticker, security_type in normalized_rows)

    company_like = [
        ticker
        for ticker, security_type in normalized_rows
        if _is_sec_company_like_type(security_type)
    ]
    fund_like = [
        ticker
        for ticker, security_type in normalized_rows
        if security_type in PRICED_IN_FUND_LIKE_SECURITY_TYPES
    ]
    wrappers = [
        ticker
        for ticker, security_type in normalized_rows
        if security_type in PRICED_IN_WRAPPER_SECURITY_TYPES
    ]
    unknown = [
        ticker
        for ticker, security_type in normalized_rows
        if security_type == "UNKNOWN"
    ]
    if not company_like and (fund_like or wrappers):
        next_action = (
            "Missing bars are non-company instruments; fill their bars for a full "
            "active-universe scan, or route/exclude them from a stocks-only scan."
        )
    return {
        "schema_version": "priced-in-market-bar-missing-diagnostic-v1",
        "status": status,
        "target_as_of": _date_iso_or_none(target_as_of),
        "missing_count": len(normalized_rows),
        "type_counts": dict(sorted(type_counts.items())),
        "company_like_missing_count": len(company_like),
        "fund_like_missing_count": len(fund_like),
        "wrapper_missing_count": len(wrappers),
        "unknown_missing_count": len(unknown),
        "sample_company_like_tickers": _sample_tickers(company_like),
        "sample_fund_like_tickers": _sample_tickers(fund_like),
        "sample_wrapper_tickers": _sample_tickers(wrappers),
        "sample_unknown_tickers": _sample_tickers(unknown),
        "route_boundary": route_boundary,
        "external_calls_made": 0,
        "next_action": next_action,
    }


def _priced_in_audit_source_row(
    action: Mapping[str, object],
    *,
    priority_counts: Mapping[str, object] | None = None,
) -> dict[str, object]:
    priority = _row_dict(priority_counts or {})
    repair = _priced_in_audit_source_gap_repair(action)
    return {
        "source": action.get("source"),
        "status": action.get("status"),
        "available": int(_finite_float(action.get("available"))),
        "stale": int(_finite_float(action.get("stale"))),
        "missing": int(_finite_float(action.get("missing"))),
        "row_count": int(_finite_float(action.get("row_count"))),
        "gap_count": int(_finite_float(action.get("gap_count"))),
        "coverage_pct": action.get("coverage_pct"),
        "coverage_basis": action.get("coverage_basis"),
        "as_of_bar_scope": _row_dict(_mapping_value(action, "as_of_bar_scope")),
        "provider_fill_plan": _row_dict(_mapping_value(action, "provider_fill_plan")),
        "provider_fill_command": action.get("provider_fill_command"),
        "provider_fill_status": action.get("provider_fill_status"),
        "provider_fill_external_call_count": action.get(
            "provider_fill_external_call_count"
        ),
        "sample_tickers": list(_sequence_value(action.get("sample_tickers"))),
        "decision_useful_gap_rows": int(
            _finite_float(priority.get("decision_useful_gap_rows"))
        ),
        "research_useful_gap_rows": int(
            _finite_float(priority.get("research_useful_gap_rows"))
        ),
        "actionable_gap_rows": int(_finite_float(priority.get("actionable_gap_rows"))),
        "priority_sample_tickers": list(
            _sequence_value(priority.get("priority_sample_tickers"))
        ),
        "next_action": action.get("next_action"),
        "command": action.get("batch_plan_command") or action.get("command"),
        "repair": repair,
    }


def _priced_in_audit_source_gap_repair(
    action: Mapping[str, object],
) -> dict[str, object] | None:
    source = str(action.get("source") or "").strip()
    if source == "catalyst_events":
        return _priced_in_audit_catalyst_gap_repair(action)
    if source == "local_text":
        return _priced_in_audit_local_text_gap_repair(action)
    if source != "options":
        return None
    gap_count = int(_finite_float(action.get("gap_count")))
    if gap_count <= 0:
        return None
    diagnostic = _row_dict(_mapping_value(action, "diagnostic"))
    diagnostic_status = str(diagnostic.get("status") or "").strip()
    blocking_statuses = {
        "newer_than_scan",
        "after_decision_cutoff",
        "eligible_but_not_scored",
    }
    provider_batch_allowed = diagnostic_status not in blocking_statuses
    stocks_only = "--stocks-only" in str(
        action.get("batch_plan_command") or action.get("command") or ""
    )
    scan_dates = [
        str(value)
        for value in _sequence_value(diagnostic.get("scan_as_of_dates"))
        if str(value).strip()
    ]
    point_in_time_import_command = _options_point_in_time_import_command(diagnostic)
    point_in_time_template_command = _options_point_in_time_template_command(
        diagnostic,
        stocks_only=stocks_only,
    )
    point_in_time_validate_command = _options_point_in_time_validate_command(
        diagnostic
    )
    point_in_time_progress = _options_point_in_time_fixture_progress(
        diagnostic,
        stocks_only=stocks_only,
    )
    progress_action = (
        point_in_time_progress.get("next_action")
        if bool(point_in_time_progress.get("exists"))
        else None
    )
    sample_tickers = _option_gap_diagnostic_samples(diagnostic)
    if not sample_tickers:
        sample_tickers = [
            str(ticker).strip().upper()
            for ticker in _sequence_value(action.get("sample_tickers"))
            if str(ticker).strip()
        ][:PRICED_IN_SOURCE_ACTION_TICKER_LIMIT]
    next_action = str(
        progress_action
        or diagnostic.get("next_action")
        or action.get("next_action")
        or "Review options evidence before trusting decision-useful mismatch rows."
    ).strip()
    status = "blocked" if not provider_batch_allowed else "attention"
    return {
        "schema_version": "priced-in-source-gap-repair-v1",
        "source": source,
        "status": status,
        "diagnostic_status": diagnostic_status or None,
        "gap_count": gap_count,
        "scan_as_of_dates": scan_dates,
        "sample_tickers": sample_tickers,
        "provider_batch_allowed": provider_batch_allowed,
        "review_rows_command": action.get("full_scan_gap_review_command"),
        "export_rows_command": action.get("full_scan_export_command"),
        "batch_plan_command": action.get("batch_plan_command"),
        "point_in_time_template_command": point_in_time_template_command,
        "point_in_time_validate_command": point_in_time_validate_command,
        "point_in_time_import_command": point_in_time_import_command,
        "point_in_time_fixture_progress": point_in_time_progress,
        "current_context_boundary": (
            "Current Schwab option chains can support a current rerun, but must not "
            "be backfilled into an older scan as if they were available then."
        ),
        "write_boundary": (
            "Review/export/plan commands make 0 provider calls. Fixture import writes "
            "local option features only. Live Schwab source batches stay explicit, "
            "read-only, and rate-limited when they are allowed."
        ),
        "external_calls_made": 0,
        "next_action": next_action,
        "usefulness_impact": (
            "Options are supporting market-emotion evidence. This gap does not shrink "
            "the full-scan universe, but it lowers trust in decision-useful mismatch "
            "rows until point-in-time options are present or intentionally skipped."
        ),
    }


def _priced_in_audit_local_text_gap_repair(
    action: Mapping[str, object],
) -> dict[str, object] | None:
    gap_count = int(_finite_float(action.get("gap_count")))
    if gap_count <= 0:
        return None
    sample_tickers = [
        str(ticker).strip().upper()
        for ticker in _sequence_value(action.get("sample_tickers"))
        if str(ticker).strip()
    ][:PRICED_IN_SOURCE_ACTION_TICKER_LIMIT]
    next_action = (
        "Fill catalyst_events first; local text can only process rows with stored "
        "event text."
    )
    return {
        "schema_version": "priced-in-source-gap-repair-v1",
        "source": "local_text",
        "status": "blocked",
        "diagnostic_status": "missing_catalyst_events",
        "gap_count": gap_count,
        "sample_tickers": sample_tickers,
        "provider_batch_allowed": False,
        "prerequisite_source": "catalyst_events",
        "prerequisite_command": (
            "catalyst-radar priced-in-source-batches --source catalyst_events "
            "--all --json"
        ),
        "review_rows_command": action.get("full_scan_gap_review_command"),
        "export_rows_command": action.get("full_scan_export_command"),
        "batch_plan_command": action.get("batch_plan_command"),
        "current_context_boundary": (
            "Local text intelligence reads stored event text only. It cannot score "
            "market emotion for rows whose catalyst_events evidence has not been "
            "filled or routed yet."
        ),
        "write_boundary": (
            "Review/export/plan commands make 0 provider calls. Local text batches "
            "write local text features only after catalyst event text exists."
        ),
        "external_calls_made": 0,
        "next_action": next_action,
        "usefulness_impact": (
            "Local text turns catalyst documents into narrative strength. Until it "
            "runs, the scan can rank price reaction but has weak stored evidence for "
            "what the market is emotionally pricing."
        ),
    }


def _priced_in_audit_catalyst_gap_repair(
    action: Mapping[str, object],
) -> dict[str, object] | None:
    applicability = _row_dict(_mapping_value(action, "applicability"))
    if not applicability:
        return None
    company_like_gap_rows = int(_finite_float(applicability.get("applicable_gap_rows")))
    routed_rows = int(_finite_float(applicability.get("non_applicable_gap_rows")))
    if company_like_gap_rows <= 0 and routed_rows <= 0:
        return None
    provider_batch_allowed = company_like_gap_rows > 0
    status = "attention" if provider_batch_allowed else "routed"
    sample_company_like = [
        str(ticker).strip().upper()
        for ticker in _sequence_value(applicability.get("sample_applicable_gap_tickers"))
        if str(ticker).strip()
    ][:PRICED_IN_SOURCE_ACTION_TICKER_LIMIT]
    sample_routed = [
        str(ticker).strip().upper()
        for ticker in _sequence_value(
            applicability.get("sample_non_applicable_gap_tickers")
        )
        if str(ticker).strip()
    ][:PRICED_IN_SOURCE_ACTION_TICKER_LIMIT]
    next_action = str(
        applicability.get("next_action")
        or action.get("next_action")
        or "Fill catalyst evidence for company-like rows and route wrappers."
    ).strip()
    return {
        "schema_version": "priced-in-source-gap-repair-v1",
        "source": "catalyst_events",
        "status": status,
        "diagnostic_status": "company_like_sec_and_non_company_routes",
        "gap_count": int(_finite_float(action.get("gap_count"))),
        "company_like_gap_rows": company_like_gap_rows,
        "routed_non_company_gap_rows": routed_rows,
        "company_like_rows": int(_finite_float(applicability.get("applicable_rows"))),
        "non_company_rows": int(
            _finite_float(applicability.get("non_applicable_rows"))
        ),
        "sample_tickers": sample_company_like or sample_routed,
        "sample_company_like_gap_tickers": sample_company_like,
        "sample_routed_non_company_tickers": sample_routed,
        "provider_batch_allowed": provider_batch_allowed,
        "review_rows_command": action.get("full_scan_gap_review_command"),
        "export_rows_command": action.get("full_scan_export_command"),
        "batch_plan_command": action.get("batch_plan_command"),
        "batch_plan_api": action.get("batch_plan_api"),
        "non_company_route": (
            "Use fund, underlying, theme, sector, flow, or constituent evidence "
            "instead of SEC company filing batches."
        ),
        "current_context_boundary": (
            "SEC catalyst batches apply only to company-like or unknown-type rows. "
            "ETF, fund, ETN, right, warrant, and other wrapper rows stay in the "
            "full scan but use the non-company evidence route."
        ),
        "write_boundary": (
            "Review/export/plan commands make 0 provider calls. Executing a SEC "
            "source batch is explicit, capped, and read-only; routed non-company "
            "rows make no SEC company-filing calls."
        ),
        "external_calls_made": 0,
        "next_action": next_action,
        "usefulness_impact": (
            "Catalyst events explain the emotion side of price-vs-expectation. "
            "Local text intelligence stays blocked for rows without stored event "
            "text, so this gap limits trust in market-emotion scoring."
        ),
    }


def _priced_in_audit_recommended_source_gap(
    source_rows: Sequence[Mapping[str, object]],
    *,
    available_at: datetime | None,
    stocks_only: bool = False,
) -> dict[str, object] | None:
    candidates = [
        row
        for row in source_rows
        if int(_finite_float(row.get("gap_count"))) > 0
        and str(row.get("status") or "") != "ready"
    ]
    if not candidates:
        return None
    ranked = sorted(
        candidates,
        key=lambda row: (
            -int(_finite_float(row.get("decision_useful_gap_rows"))),
            -int(_finite_float(row.get("actionable_gap_rows"))),
            -int(_finite_float(row.get("research_useful_gap_rows"))),
            -int(_finite_float(row.get("gap_count"))),
            str(row.get("source") or ""),
        ),
    )
    top = ranked[0]
    source = str(top.get("source") or "").strip()
    decision_rows = int(_finite_float(top.get("decision_useful_gap_rows")))
    actionable_rows = int(_finite_float(top.get("actionable_gap_rows")))
    research_rows = int(_finite_float(top.get("research_useful_gap_rows")))
    gap_rows = int(_finite_float(top.get("gap_count")))
    next_action = str(top.get("next_action") or "Inspect this source gap.").strip()
    review_command = _priced_in_audit_command(
        limit=25,
        offset=0,
        available_at=available_at,
        source_gap=[source] if source else [],
        stocks_only=stocks_only,
    )
    full_scan_command = _priced_in_audit_command(
        limit=25,
        offset=0,
        available_at=available_at,
        source_gap=[source] if source else [],
        all_rows=True,
        json=True,
        stocks_only=stocks_only,
    )
    rationale = (
        f"{source} has the highest current payoff: {decision_rows} "
        f"decision-useful gap row(s), {actionable_rows} actionable gap row(s), "
        f"{research_rows} research-useful gap row(s), and {gap_rows} total gap row(s)."
    )
    sample_boundary = (
        "Example tickers are only a priority preview; the source gap itself covers "
        f"{gap_rows} full-scan row(s)."
    )
    result = {
        "schema_version": "priced-in-recommended-source-gap-v1",
        "source": source,
        "status": top.get("status"),
        "decision_useful_gap_rows": decision_rows,
        "actionable_gap_rows": actionable_rows,
        "research_useful_gap_rows": research_rows,
        "gap_count": gap_rows,
        "priority_sample_tickers": list(
            _sequence_value(top.get("priority_sample_tickers"))
        ),
        "rationale": rationale,
        "sample_boundary": sample_boundary,
        "next_action": f"Inspect {source} first. {next_action}",
        "review_command": review_command,
        "full_scan_command": full_scan_command,
        "plan_command": top.get("command"),
        "execution_boundary": (
            "Reviewing this recommendation makes 0 provider calls. Execute source "
            "batches only after explicitly approving provider calls."
        ),
    }
    repair = _row_dict(_mapping_value(top, "repair"))
    if repair:
        result["repair"] = repair
    return result


def _priced_in_audit_next_step(
    preflight: Mapping[str, object],
    source_rows: Sequence[Mapping[str, object]],
) -> tuple[str, str | None]:
    plan = _mapping_value(preflight, "evidence_plan")
    for step in _sequence_value(plan.get("steps")):
        if not isinstance(step, Mapping):
            continue
        if str(step.get("status") or "") == "ready":
            continue
        action = str(step.get("action") or step.get("next_action") or "").strip()
        command = str(step.get("command") or "").strip()
        if action:
            return action, command or None
    for row in source_rows:
        if int(_finite_float(row.get("gap_count"))) <= 0:
            continue
        action = str(row.get("next_action") or "").strip()
        command = str(row.get("command") or "").strip()
        if action:
            return action, command or None
    return (
        "Open the full priced-in queue and review the largest emotion/reaction gaps.",
        "catalyst-radar priced-in-queue --full-scan --limit 50",
    )


def _priced_in_answer_full_scan_summary(
    queue: Mapping[str, object],
    *,
    market_bars: Mapping[str, object] | None = None,
) -> dict[str, object]:
    scan_scope = _priced_in_answer_scan_scope(queue)
    scan = _mapping_value(queue, "scan")
    freshness = _mapping_value(scan, "freshness")
    filters = _mapping_value(queue, "filters")
    returned = int(
        _finite_float(queue.get("returned_count"))
        or _finite_float(queue.get("count"))
    )
    offset = int(_finite_float(queue.get("offset")))
    total = int(_finite_float(queue.get("total_count")))
    scan_total = int(
        _finite_float(
            _first_present(
                scan.get("scanned_candidate_states"),
                scan.get("candidate_states"),
                scan.get("scanned_securities"),
            )
        )
    )
    active = int(_finite_float(freshness.get("active_security_count")))
    stocks_only = bool(filters.get("stocks_only"))
    scan_scope_basis = "active_universe"
    raw_unscanned_rows = max(0, active - (scan_total or total))
    scan_exclusions = _mapping_value(queue, "scan_exclusions")
    scan_exclusion_reason = str(scan_exclusions.get("reason") or "").strip() or None
    scan_excluded_tickers = [
        str(ticker).strip().upper()
        for ticker in _sequence_value(scan_exclusions.get("tickers"))
        if str(ticker).strip()
    ]
    scan_excluded_rows = int(_finite_float(scan_exclusions.get("count")))
    if not scan_excluded_tickers and 0 < scan_excluded_rows:
        scan_excluded_tickers = sorted(PRICED_IN_SCAN_EXCLUDED_TICKERS)[
            :scan_excluded_rows
        ]
    if stocks_only:
        scan_excluded_rows = 0
        scan_excluded_tickers = []
        scan_exclusion_reason = None
    else:
        scan_excluded_rows = min(raw_unscanned_rows, max(0, scan_excluded_rows))
    unscanned_rows = raw_unscanned_rows
    unscanned_blocker_rows = max(0, raw_unscanned_rows - scan_excluded_rows)
    if stocks_only and isinstance(market_bars, Mapping):
        stock_scope = _mapping_value(
            _mapping_value(market_bars, "repair"),
            "stock_scope",
        )
        stock_like_active = int(_finite_float(stock_scope.get("stock_like_active")))
        stock_like_with_bar = int(
            _finite_float(stock_scope.get("stock_like_with_as_of_bar"))
        )
        if 0 < stock_like_active:
            active = stock_like_active
            scan_total = stock_like_with_bar or total
            scan_scope_basis = "stock_like_active_as_of_bars"
            unscanned_rows = max(0, active - scan_total)
            unscanned_blocker_rows = unscanned_rows
            scan_excluded_rows = 0
            scan_excluded_tickers = []
            scan_exclusion_reason = None
    start = offset + 1 if returned else 0
    end = offset + returned
    review_command = _priced_in_queue_command_from_filters(filters)
    export_command = _priced_in_queue_command_from_filters(filters, all_rows=True)
    mode = str(scan_scope.get("mode") or "full_scan")
    sample_text = (
        f"The tickers below are rows {start}-{end} from the current ranked page, "
        "not the "
        f"{'stocks-only active universe' if stocks_only else 'scan universe'} "
        f"of {active or total} row(s)."
        if total and returned and returned < total
        else "The visible tickers cover the current filtered result set."
    )
    return {
        "schema_version": "priced-in-full-scan-summary-v1",
        "mode": mode,
        "instrument_filter": "stocks_only" if stocks_only else "all",
        "stocks_only": stocks_only,
        "is_all_active_scan": mode == "full_scan",
        "active_securities": active,
        "scanned_rows": scan_total or total,
        "unscanned_rows": unscanned_rows,
        "unscanned_blocker_rows": unscanned_blocker_rows,
        "scan_excluded_rows": scan_excluded_rows,
        "scan_excluded_tickers": scan_excluded_tickers,
        "scan_excluded_reason": scan_exclusion_reason,
        "scan_scope_basis": scan_scope_basis,
        "ranked_rows": total,
        "visible_row_start": start,
        "visible_row_end": end,
        "visible_rows": returned,
        "has_more": bool(queue.get("has_more")),
        "visible_tickers_are_sample": bool(total and returned < total),
        "sample_explanation": sample_text,
        "review_command": review_command,
        "next_page_command": scan_scope.get("next_page_command"),
        "export_command": export_command,
        "full_export_command": (
            "catalyst-radar priced-in-queue --stocks-only --full-scan --all --json"
            if stocks_only
            else "catalyst-radar priced-in-queue --full-scan --all --json"
        ),
    }


def _priced_in_answer_scan_scope(queue: Mapping[str, object]) -> dict[str, object]:
    filters = _mapping_value(queue, "filters")
    queue_status = str(queue.get("status") or "").strip().lower()
    status = str(filters.get("status") or "all").strip().lower()
    stocks_only = bool(filters.get("stocks_only"))
    total = int(_finite_float(queue.get("total_count")))
    returned = int(
        _finite_float(queue.get("returned_count"))
        or _finite_float(queue.get("count"))
    )
    offset = int(_finite_float(queue.get("offset")))
    row_start = offset + 1 if returned else 0
    row_end = offset + returned
    has_more = bool(queue.get("has_more"))
    full_scan_mode = status in {"", "all"}
    mode = (
        "selected_universe"
        if queue_status == "selected_universe"
        else ("full_scan" if full_scan_mode else "filtered_scan")
    )
    if total <= 0:
        explanation = "No priced-in rows are visible in the current scan."
    elif queue_status == "selected_universe":
        latest_run = _mapping_value(queue, "latest_run")
        scan = _mapping_value(queue, "scan")
        freshness = _mapping_value(scan, "freshness")
        active = int(_finite_float(freshness.get("active_security_count")))
        universe = str(latest_run.get("universe") or "selected").strip()
        explanation = (
            f"Showing rows {row_start}-{row_end} of {total} from universe={universe}; "
            f"the latest run did not scan all {active or 'active'} active securities."
        )
    elif full_scan_mode:
        explanation = (
            f"Showing ranked rows {row_start}-{row_end} of {total}; "
            "the visible tickers are one page from the "
            f"{'stocks-only ' if stocks_only else ''}full scan, not the scan universe."
        )
    else:
        explanation = (
            f"Showing filtered rows {row_start}-{row_end} of {total}; "
            "switch to full scan to see neutral, blocked, stale, and fully-priced rows too."
        )
    next_offset = offset + max(1, int(_finite_float(filters.get("limit"))) or returned)
    return {
        "schema_version": "priced-in-scan-scope-v1",
        "mode": mode,
        "instrument_filter": "stocks_only" if stocks_only else "all",
        "stocks_only": stocks_only,
        "visible_row_start": row_start,
        "visible_row_end": row_end,
        "visible_rows": returned,
        "total_rows": total,
        "offset": offset,
        "has_more": has_more,
        "explanation": explanation,
        "current_page_command": _priced_in_queue_command_from_filters(filters),
        "next_page_command": _priced_in_queue_command_from_filters(
            filters,
            offset=next_offset,
        )
        if has_more
        else None,
        "current_filter_export_command": _priced_in_queue_command_from_filters(
            filters,
            all_rows=True,
        ),
        "full_scan_export_command": _priced_in_queue_full_scan_command(
            stocks_only=stocks_only,
            all_rows=True,
        ),
    }


def _priced_in_queue_command_from_filters(
    filters: Mapping[str, object],
    *,
    offset: int | None = None,
    all_rows: bool = False,
) -> str:
    parts = ["catalyst-radar", "priced-in-queue"]
    available_at = str(filters.get("available_at") or "").strip()
    if available_at:
        parts.extend(["--available-at", available_at])
    if bool(filters.get("stocks_only")):
        parts.append("--stocks-only")
    status = str(filters.get("status") or "all").strip().lower()
    if status in {"", "all"}:
        parts.append("--full-scan")
    elif status in PRICED_IN_ACTIONABLE_FILTERS:
        parts.append("--mismatches")
    else:
        parts.extend(["--status", status])
    usefulness = str(filters.get("usefulness") or "").strip()
    if usefulness and usefulness != "all":
        parts.extend(["--usefulness", usefulness])
    source_gap = filters.get("source_gap")
    if isinstance(source_gap, list | tuple):
        for source in source_gap:
            source_text = str(source or "").strip()
            if source_text:
                parts.extend(["--source-gap", source_text])
    decision_gap = filters.get("decision_gap")
    if isinstance(decision_gap, list | tuple):
        for gap in decision_gap:
            gap_text = str(gap or "").strip()
            if gap_text:
                parts.extend(["--decision-gap", gap_text])
    min_gap = filters.get("min_gap")
    if min_gap is not None:
        parts.extend(["--min-gap", str(min_gap)])
    if all_rows:
        parts.extend(["--all", "--json"])
    else:
        limit = int(_finite_float(filters.get("limit"))) or 50
        parts.extend(["--limit", str(limit), "--offset", str(offset or 0)])
    return " ".join(parts)


def _priced_in_answer_market_bar_gap(
    source_coverage: Mapping[str, object],
) -> dict[str, object]:
    for action in _sequence_value(source_coverage.get("actions")):
        if not isinstance(action, Mapping):
            continue
        if str(action.get("source") or "") != "market_bars":
            continue
        gap_count = int(_finite_float(action.get("gap_count")))
        status = str(action.get("status") or "").strip()
        if gap_count <= 0 or status in {"ready", "not_applicable"}:
            return {}
        return {
            "source": "market_bars",
            "gap": "market_bars",
            "count": gap_count,
            "status": status,
            "sample_tickers": list(_sequence_value(action.get("sample_tickers"))),
            "next_action": action.get("next_action"),
            "command": action.get("batch_plan_command") or action.get("command"),
        }
    return {}


def _priced_in_answer_decision_readiness(
    decision_gap_counts: Mapping[str, object],
    *,
    source_coverage: Mapping[str, object],
    decision_ready_count: int,
    scan_as_of: str = "",
    market_bar_gap: Mapping[str, object] | None = None,
    core_evidence_gap: Mapping[str, object] | None = None,
) -> dict[str, object]:
    row_count = int(_finite_float(decision_gap_counts.get("row_count")))
    count_values = _mapping_value(decision_gap_counts, "counts")
    actions = {
        str(action.get("source") or ""): action
        for action in _sequence_value(source_coverage.get("actions"))
        if isinstance(action, Mapping)
    }
    sample_tickers_by_gap = _mapping_value(decision_gap_counts, "sample_tickers")
    top_gaps = [
        _priced_in_decision_gap_row(
            gap,
            count,
            actions=actions,
            scan_as_of=scan_as_of,
            sample_tickers=_sequence_value(sample_tickers_by_gap.get(gap)),
        )
        for gap, count in sorted(
            count_values.items(),
            key=lambda item: (
                _priced_in_decision_gap_priority(str(item[0])),
                -int(_finite_float(item[1])),
                str(item[0]),
            ),
        )
        if int(_finite_float(count)) > 0
    ]
    market_gap = _row_dict(market_bar_gap or {})
    market_gap_count = int(_finite_float(market_gap.get("count")))
    if market_gap_count > 0:
        market_recommendation = {
            "gap": "market_bars",
            "source": "market_bars",
            "count": market_gap_count,
            "sample_tickers": list(_sequence_value(market_gap.get("sample_tickers"))),
            "next_action": market_gap.get("next_action"),
            "command": market_gap.get("command"),
        }
        top_gaps = [
            market_recommendation,
            *[gap for gap in top_gaps if gap.get("gap") != "market_bars"],
        ]
    core_gap = _row_dict(core_evidence_gap or {})
    core_gap_name = str(core_gap.get("gap") or "").strip()
    core_gap_count = int(_finite_float(core_gap.get("count")))
    if market_gap_count <= 0 and core_gap_count > 0 and core_gap_name:
        core_recommendation = {
            "gap": core_gap_name,
            "source": core_gap.get("source") or core_gap_name,
            "count": core_gap_count,
            "sample_tickers": list(_sequence_value(core_gap.get("sample_tickers"))),
            "next_action": core_gap.get("next_action"),
            "command": core_gap.get("command"),
        }
        top_gaps = [
            core_recommendation,
            *[gap for gap in top_gaps if gap.get("gap") != core_gap_name],
        ]
    recommended = top_gaps[0] if top_gaps else {}
    if market_gap_count > 0:
        status = "blocked"
        if decision_ready_count > 0:
            summary = (
                f"{decision_ready_count} row(s) look decision-ready inside the "
                f"scanned subset, but {market_gap_count} market-bar row(s) are "
                "missing from the full scan."
            )
        else:
            summary = (
                f"The full scan is blocked by {market_gap_count} missing "
                "market-bar row(s)."
            )
    elif core_gap_count > 0 and core_gap_name:
        status = "blocked"
        if decision_ready_count > 0:
            summary = (
                f"{decision_ready_count} row(s) look decision-ready inside the "
                f"scanned subset, but core evidence layer {core_gap_name} still "
                f"has {core_gap_count} gap row(s)."
            )
        else:
            summary = (
                f"The full scan is blocked by core evidence layer {core_gap_name} "
                f"({core_gap_count} gap row(s))."
            )
    elif decision_ready_count > 0:
        status = "ready"
        summary = f"{decision_ready_count} not-priced-in row(s) are decision-ready."
    elif row_count <= 0:
        status = "no_actionable_rows"
        summary = "No actionable mismatch rows are visible in the current scan filter."
    elif top_gaps:
        status = "blocked"
        summary = (
            f"0 of {row_count} actionable mismatch row(s) are decision-ready; "
            f"start with {recommended.get('gap')} "
            f"({recommended.get('count')} row(s))."
        )
    else:
        status = "blocked"
        summary = (
            f"0 of {row_count} actionable mismatch row(s) are decision-ready; "
            "open candidate detail for row-level blockers."
        )
    return {
        "schema_version": "priced-in-decision-readiness-v1",
        "status": status,
        "scope": decision_gap_counts.get("scope") or "actionable_mismatch_rows",
        "actionable_mismatch_rows": row_count,
        "decision_ready_rows": decision_ready_count,
        "summary": summary,
        "recommended_gap": recommended or None,
        "top_gaps": top_gaps[:8],
        "counts": _row_dict(count_values),
    }


def _priced_in_answer_evidence_completeness(
    source_coverage: Mapping[str, object],
) -> dict[str, object]:
    sources = _mapping_value(source_coverage, "sources")
    actions = {
        str(action.get("source") or ""): action
        for action in _sequence_value(source_coverage.get("actions"))
        if isinstance(action, Mapping)
    }
    source_names = (
        PRICED_IN_SOURCE_CLASSES
        if sources
        else tuple(
            source
            for source in PRICED_IN_SOURCE_CLASSES
            if source in actions
        )
    )
    layers: list[dict[str, object]] = []
    for source in source_names:
        values = _mapping_value(sources, source)
        action = _row_dict(actions.get(source, {}))
        row_count = int(_finite_float(values.get("row_count")))
        available = int(_finite_float(values.get("available")))
        stale = int(_finite_float(values.get("stale")))
        missing = int(_finite_float(values.get("missing")))
        action_gap = action.get("gap_count")
        gap_count = (
            int(_finite_float(action_gap))
            if action_gap is not None
            else max(0, stale + missing)
        )
        coverage_pct = _finite_float(values.get("coverage_pct"))
        if row_count <= 0 and available <= 0 and stale <= 0 and missing <= 0:
            status = "not_seen"
        elif gap_count <= 0 and stale <= 0 and missing <= 0:
            status = "ready"
        else:
            status = "attention"
        required = source not in PRICED_IN_OPTIONAL_CONTEXT_SOURCES
        layers.append(
            {
                "source": source,
                "status": status,
                "required_for_core_answer": required,
                "available": available,
                "stale": stale,
                "missing": missing,
                "gap_count": gap_count,
                "row_count": row_count,
                "coverage_pct": round(coverage_pct, 1),
                "next_action": action.get("next_action"),
                "command": action.get("batch_plan_command") or action.get("command"),
            }
        )

    total = len(layers)
    ready = sum(1 for layer in layers if layer["status"] == "ready")
    required_layers = [
        layer for layer in layers if bool(layer.get("required_for_core_answer"))
    ]
    required_total = len(required_layers)
    required_ready = sum(1 for layer in required_layers if layer["status"] == "ready")
    gap_layers = [layer for layer in layers if layer["status"] != "ready"]
    first_gap = gap_layers[0] if gap_layers else {}
    first_gap_label = str(first_gap.get("source") or "")
    first_gap_count = int(_finite_float(first_gap.get("gap_count")))
    gap_summary = [
        f"{layer['source']}:{int(_finite_float(layer.get('gap_count')))}"
        for layer in gap_layers
        if int(_finite_float(layer.get("gap_count"))) > 0
    ][:3]
    if total <= 0:
        status = "not_evaluated"
        summary = "No priced-in evidence layer coverage is available."
    elif ready == total:
        status = "ready"
        summary = f"All {total} priced-in evidence layer(s) are complete."
    elif required_ready < required_total:
        status = "blocked"
        gap_text = f"; first gaps {', '.join(gap_summary)}" if gap_summary else ""
        summary = (
            f"{ready}/{total} priced-in evidence layer(s) complete; core "
            f"{required_ready}/{required_total}{gap_text}."
        )
    else:
        status = "attention"
        gap_text = f"; optional gaps {', '.join(gap_summary)}" if gap_summary else ""
        summary = (
            f"Core evidence {required_ready}/{required_total} complete; all layers "
            f"{ready}/{total}{gap_text}."
        )
    return {
        "schema_version": "priced-in-evidence-completeness-v1",
        "status": status,
        "all_sources_ready": ready == total and total > 0,
        "core_sources_ready": required_ready == required_total and required_total > 0,
        "ready_source_count": ready,
        "total_source_count": total,
        "required_ready_source_count": required_ready,
        "required_source_count": required_total,
        "gap_source_count": len(gap_layers),
        "first_gap_source": first_gap_label or None,
        "first_gap_count": first_gap_count,
        "gap_summary": gap_summary,
        "next_action": first_gap.get("next_action"),
        "command": first_gap.get("command"),
        "summary": summary,
        "layers": layers,
        "external_calls_made": 0,
    }


def _priced_in_answer_core_evidence_gap(
    evidence_completeness: Mapping[str, object],
    *,
    skip_market_bars: bool = False,
) -> dict[str, object]:
    for layer in _sequence_value(evidence_completeness.get("layers")):
        if not isinstance(layer, Mapping):
            continue
        if not bool(layer.get("required_for_core_answer")):
            continue
        source = str(layer.get("source") or "").strip()
        if skip_market_bars and source == "market_bars":
            continue
        status = str(layer.get("status") or "").strip()
        gap_count = int(_finite_float(layer.get("gap_count")))
        if status == "ready" and gap_count <= 0:
            continue
        return {
            "gap": source,
            "source": source,
            "count": gap_count,
            "status": status,
            "next_action": layer.get("next_action"),
            "command": layer.get("command"),
        }
    return {}


def _priced_in_decision_gap_row(
    gap: object,
    count: object,
    *,
    actions: Mapping[str, Mapping[str, object]],
    scan_as_of: str = "",
    sample_tickers: Sequence[object] = (),
) -> dict[str, object]:
    gap_name = str(gap or "").strip()
    count_value = int(_finite_float(count))
    tickers = [
        str(ticker).strip().upper()
        for ticker in sample_tickers
        if str(ticker).strip()
    ][:PRICED_IN_LOCAL_BATCH_MAX_TICKERS]
    action = actions.get(gap_name, {})
    next_action = str(action.get("next_action") or "").strip()
    command = str(
        action.get("batch_plan_command")
        or action.get("command")
        or action.get("full_scan_gap_review_command")
        or ""
    ).strip()
    if gap_name == "candidate_packet":
        next_action = "Build Candidate Packets for research-useful mismatch rows."
        command = _priced_in_local_artifact_command(
            "build-packets",
            scan_as_of=scan_as_of,
            fallback_gap="candidate_packet",
            tickers=tickers,
        )
    elif gap_name == "decision_card":
        next_action = "Build Decision Cards after candidate packets exist."
        command = _priced_in_local_artifact_command(
            "build-decision-cards",
            scan_as_of=scan_as_of,
            fallback_gap="decision_card",
            tickers=tickers,
        )
    elif not next_action:
        next_action = "Review this decision gap before trusting not-priced-in output."
    return {
        "gap": gap_name,
        "count": count_value,
        "sample_tickers": tickers,
        "next_action": next_action,
        "command": command or None,
    }


def _priced_in_local_artifact_command(
    command: str,
    *,
    scan_as_of: str,
    fallback_gap: str,
    tickers: Sequence[str] = (),
) -> str:
    ticker_args = " ".join(
        f"--ticker {ticker}"
        for ticker in dict.fromkeys(
            str(ticker).strip().upper()
            for ticker in tickers
            if str(ticker).strip()
        )
    )
    if scan_as_of:
        ticker_part = f" {ticker_args}" if ticker_args else ""
        return (
            f"catalyst-radar {command} --as-of {scan_as_of}{ticker_part} "
            "--min-state ResearchOnly"
        )
    return (
        "catalyst-radar priced-in-queue --usefulness research_useful "
        f"--decision-gap {fallback_gap} --limit 50"
    )


def _priced_in_decision_gap_priority(gap: str) -> int:
    order = {
        "market_bars": 0,
        "catalyst_events": 1,
        "local_text": 2,
        "candidate_packet": 3,
        "decision_card": 4,
        "options": 5,
        "broker_context": 6,
        "theme_peer_sector": 7,
    }
    return order.get(gap, 99)


def _priced_in_answer_rows(
    rows: Sequence[object],
    *,
    stocks_only: bool = False,
) -> list[dict[str, object]]:
    answer_rows: list[dict[str, object]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        status = str(row.get("priced_in_status") or "").strip()
        if status not in PRICED_IN_ACTIONABLE_STATUSES:
            continue
        usefulness = _mapping_value(row, "usefulness")
        data_sources = _mapping_value(row, "data_sources")
        if not data_sources:
            data_sources = _priced_in_row_source_payload(row)
        answer_rows.append(
            {
                "ticker": row.get("ticker"),
                "status": status,
                "usefulness": usefulness.get("status"),
                "decision_ready": bool(usefulness.get("decision_ready")),
                "direction": row.get("priced_in_direction"),
                "emotion_reaction_gap": row.get("emotion_reaction_gap"),
                "emotion_score": row.get("emotion_score"),
                "reaction_score": row.get("reaction_score"),
                "priced_in_score": row.get("priced_in_score"),
                "why_now": row.get("why_now") or row.get("priced_in_reason"),
                "missing_sources": list(_sequence_value(data_sources.get("missing"))),
                "stale_sources": list(_sequence_value(data_sources.get("stale"))),
                "next_step": row.get("next_step"),
                "next_command": usefulness.get("next_command"),
                "drilldown": _priced_in_answer_row_drilldown(
                    row,
                    data_sources=data_sources,
                    usefulness=usefulness,
                    stocks_only=stocks_only,
                ),
            }
        )
    return answer_rows


def _priced_in_answer_row_drilldown(
    row: Mapping[str, object],
    *,
    data_sources: Mapping[str, object],
    usefulness: Mapping[str, object],
    stocks_only: bool = False,
) -> dict[str, object]:
    ticker = _priced_in_action_ticker(row)
    missing_sources = [
        str(source)
        for source in _sequence_value(data_sources.get("missing"))
        if str(source).strip()
    ]
    stale_sources = [
        str(source)
        for source in _sequence_value(data_sources.get("stale"))
        if str(source).strip()
    ]
    local_gaps = [
        str(gap)
        for gap in _sequence_value(usefulness.get("missing_for_decision"))
        if str(gap).strip()
    ]
    optional_gaps = [
        str(gap)
        for gap in _sequence_value(usefulness.get("optional_context_gaps"))
        if str(gap).strip()
    ]
    source_actions = [
        _priced_in_answer_source_gap_drilldown(
            source=source,
            status="missing",
            stocks_only=stocks_only,
        )
        for source in missing_sources
    ]
    source_actions.extend(
        _priced_in_answer_source_gap_drilldown(
            source=source,
            status="stale",
            stocks_only=stocks_only,
        )
        for source in stale_sources
    )
    gap_labels = []
    if missing_sources:
        gap_labels.append(f"missing sources: {', '.join(missing_sources)}")
    if stale_sources:
        gap_labels.append(f"stale sources: {', '.join(stale_sources)}")
    if local_gaps:
        gap_labels.append(f"local evidence gaps: {', '.join(local_gaps)}")
    if optional_gaps:
        gap_labels.append(f"optional context gaps: {', '.join(optional_gaps)}")
    return {
        "schema_version": "priced-in-answer-row-drilldown-v1",
        "ticker": ticker,
        "detail_command": f"catalyst-radar candidate-detail {ticker}",
        "detail_api": f"GET /api/radar/candidates/{ticker}",
        "source_gap_actions": source_actions,
        "missing_sources": missing_sources,
        "stale_sources": stale_sources,
        "local_evidence_gaps": local_gaps,
        "optional_context_gaps": optional_gaps,
        "evidence_gap_summary": (
            "; ".join(gap_labels) if gap_labels else "no row-level evidence gaps"
        ),
        "external_calls_made": 0,
        "action_boundary": (
            "Candidate detail and source-gap planning are zero-call review paths; "
            "provider execution still requires the separate approval checklist."
        ),
    }


def _priced_in_answer_source_gap_drilldown(
    *,
    source: str,
    status: str,
    stocks_only: bool = False,
) -> dict[str, object]:
    source_name = str(source or "").strip()
    plan_command = _priced_in_source_batch_plan_command(
        source_name,
        stocks_only=stocks_only,
    )
    return {
        "source": source_name,
        "status": status,
        "review_command": _priced_in_audit_command(
            limit=25,
            offset=0,
            available_at=None,
            source_gap=[source_name],
            stocks_only=stocks_only,
        ),
        "review_api": (
            f"GET /api/radar/priced-in/audit?source_gap={source_name}&limit=25"
            + ("&stocks_only=true" if stocks_only else "")
        ),
        "plan_command": plan_command,
        "plan_api": (
            _priced_in_source_batches_api(source_name, stocks_only=stocks_only)
            if plan_command
            else None
        ),
        "external_calls_made": 0,
    }


def _priced_in_answer_status(
    *,
    queue_status: str,
    actionable_count: int,
    decision_ready_count: int,
    research_lead_count: int,
    blocked_count: int,
    market_bar_gap_count: int = 0,
    core_evidence_gap_count: int = 0,
) -> str:
    if queue_status in {"universe_too_small", "partial_scan", "selected_universe"}:
        return "blocked"
    if market_bar_gap_count > 0:
        return "blocked"
    if core_evidence_gap_count > 0:
        return "blocked"
    if decision_ready_count > 0:
        return "decision_ready"
    if research_lead_count > 0:
        return "research_only"
    if actionable_count > 0 or blocked_count > 0:
        return "blocked"
    return "none_visible"


def _priced_in_answer_text(
    *,
    answer_status: str,
    actionable_count: int,
    decision_ready_count: int,
    research_lead_count: int,
    blocked_count: int,
    market_bar_gap_count: int = 0,
    core_evidence_gap: Mapping[str, object] | None = None,
    stocks_only: bool = False,
) -> str:
    if answer_status == "decision_ready":
        return (
            f"Not fully priced for {decision_ready_count} decision-ready row(s); "
            "review the top evidence before any action."
        )
    if answer_status == "research_only":
        return (
            f"Not fully priced for {research_lead_count} research lead(s), but "
            "none are decision-ready yet."
        )
    if answer_status == "blocked":
        if market_bar_gap_count > 0:
            scope = "Stocks-only" if stocks_only else "Full-market"
            suffix = (
                f" {decision_ready_count} scanned-subset row(s) still look "
                "reviewable, but the full scan must be repaired first."
                if decision_ready_count > 0
                else ""
            )
            return (
                f"{scope} priced-in answer is not ready: "
                f"{market_bar_gap_count} row(s) still lack scan-date price "
                f"reaction.{suffix}"
            )
        core_gap = _row_dict(core_evidence_gap or {})
        core_gap_name = str(core_gap.get("gap") or "").strip()
        core_gap_count = int(_finite_float(core_gap.get("count")))
        if core_gap_name and core_gap_count > 0:
            scope = "Stocks-only" if stocks_only else "Full-market"
            suffix = (
                f" {decision_ready_count} scanned-subset row(s) still look "
                "reviewable, but core evidence must be repaired first."
                if decision_ready_count > 0
                else ""
            )
            return (
                f"{scope} priced-in answer is not ready: core evidence layer "
                f"{core_gap_name} still has {core_gap_count} gap row(s).{suffix}"
            )
        return (
            f"{actionable_count or blocked_count} possible mismatch row(s) are blocked "
            "by missing evidence or scan readiness."
        )
    return "No useful not-priced-in mismatch is visible in the current scan."


def _priced_in_answer_headline(
    *,
    answer_status: str,
    total_count: int,
    actionable_count: int,
    decision_ready_count: int,
    research_lead_count: int,
    blocked_count: int,
    market_bar_gap_count: int = 0,
    core_evidence_gap: Mapping[str, object] | None = None,
    stocks_only: bool = False,
) -> str:
    if answer_status == "decision_ready":
        return (
            f"{decision_ready_count} decision-ready not-priced-in row(s) from "
            f"{total_count} scanned row(s)."
        )
    if answer_status == "research_only":
        return (
            f"{research_lead_count} research-useful not-priced-in lead(s), "
            f"{actionable_count} actionable mismatch row(s), {total_count} scanned row(s)."
        )
    if answer_status == "blocked":
        if market_bar_gap_count > 0:
            scope = "stock-like" if stocks_only else "active"
            return (
                f"Full scan blocked by {market_bar_gap_count} missing "
                f"{scope} market-bar row(s); {total_count} scanned row(s) are "
                "only a subset."
            )
        core_gap = _row_dict(core_evidence_gap or {})
        core_gap_name = str(core_gap.get("gap") or "").strip()
        core_gap_count = int(_finite_float(core_gap.get("count")))
        if core_gap_name and core_gap_count > 0:
            return (
                f"Full scan blocked by {core_gap_name} core evidence gap "
                f"({core_gap_count} row(s)); {total_count} scanned row(s)."
            )
        return (
            f"{blocked_count or actionable_count} row(s) need evidence cleanup before "
            f"the priced-in answer is trustworthy; {total_count} scanned row(s)."
        )
    return f"No useful mismatch among {total_count} scanned row(s)."


def _priced_in_answer_next_step(
    *,
    answer_status: str,
    preflight: Mapping[str, object],
    top_rows: Sequence[Mapping[str, object]],
    decision_readiness: Mapping[str, object] | None = None,
    stocks_only: bool = False,
) -> tuple[str, str | None]:
    recommended_gap = _mapping_value(decision_readiness or {}, "recommended_gap")
    recommended_action = str(recommended_gap.get("next_action") or "").strip()
    recommended_command = str(recommended_gap.get("command") or "").strip()
    if (
        answer_status in {"blocked", "research_only"}
        and recommended_action
        and recommended_command
    ):
        return recommended_action, recommended_command
    plan = _mapping_value(preflight, "evidence_plan")
    plan_action = str(plan.get("next_action") or "").strip()
    plan_command = str(plan.get("next_command") or "").strip()
    if answer_status in {"blocked", "research_only"} and plan_action:
        return plan_action, plan_command or None
    if answer_status == "decision_ready":
        scope_label = "stocks-only full scan" if stocks_only else "full-market scan"
        stocks_flag = " --stocks-only" if stocks_only else ""
        return (
            (
                f"Review the {scope_label}; decision-ready tickers are a "
                "filtered subset, not the scan universe."
            ),
            f"catalyst-radar priced-in-queue{stocks_flag} --full-scan --limit 50",
        )
    for row in top_rows:
        next_step = str(row.get("next_step") or "").strip()
        if next_step:
            command = str(row.get("next_command") or "").strip()
            return next_step, command or None
    return (
        "Review the priced-in queue and source coverage.",
        "catalyst-radar priced-in-queue --mismatches",
    )


def _priced_in_answer_trust_blockers(
    preflight: Mapping[str, object],
    *,
    answer_status: str,
    source_coverage: Mapping[str, object],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    actions = _sequence_value(source_coverage.get("actions"))
    actions_by_source = {
        str(action.get("source") or ""): action
        for action in actions
        if isinstance(action, Mapping)
    }
    plan = _mapping_value(preflight, "evidence_plan")
    for step in _sequence_value(plan.get("steps")):
        if not isinstance(step, Mapping):
            continue
        status = str(step.get("status") or "").strip()
        if status == "ready":
            continue
        area = str(step.get("area") or "").strip()
        source_action = _row_dict(actions_by_source.get(area, {}))
        rows.append(
            {
                "area": step.get("area"),
                "status": status,
                "gap_count": int(_finite_float(source_action.get("gap_count"))),
                "depends_on": list(_sequence_value(step.get("depends_on"))),
                "next_action": source_action.get("next_action")
                or step.get("action")
                or step.get("next_action"),
                "command": source_action.get("batch_plan_command")
                or source_action.get("command")
                or step.get("command"),
            }
        )
        if len(rows) >= 5:
            return rows
    for action in actions:
        if not isinstance(action, Mapping):
            continue
        status = str(action.get("status") or "").strip()
        if status in {"ready", "not_applicable"}:
            continue
        rows.append(
            {
                "area": action.get("source"),
                "status": status,
                "gap_count": int(_finite_float(action.get("gap_count"))),
                "next_action": action.get("next_action"),
                "command": action.get("batch_plan_command") or action.get("command"),
            }
        )
        if len(rows) >= 5:
            return rows
    return rows


def _priced_in_prioritized_trust_blockers(
    rows: Sequence[Mapping[str, object]],
    *,
    primary_area: str | None = None,
) -> list[dict[str, object]]:
    normalized = [_row_dict(row) for row in rows if isinstance(row, Mapping)]
    if not primary_area:
        return normalized
    return sorted(
        normalized,
        key=lambda row: 0 if str(row.get("area") or "") == primary_area else 1,
    )


def _priced_in_market_bar_manual_csv_context(
    market_bar_repair: Mapping[str, object],
    local_progress: Mapping[str, object],
    operator_step: Mapping[str, object],
):
    stock_scope = _mapping_value(market_bar_repair, "stock_scope")
    sample_source = (
        market_bar_repair.get("missing_as_of_bar_ticker_sample")
        or market_bar_repair.get("missing_as_of_bar_tickers")
        or stock_scope.get("sample_missing_stock_like_tickers")
        or stock_scope.get("sample_missing_tickers")
    )
    sample_tickers = [
        str(ticker).strip().upper()
        for ticker in _sequence_value(sample_source)
        if str(ticker).strip()
    ][:10]
    required_fields = [
        str(field).strip()
        for field in _sequence_value(market_bar_repair.get("required_fill_fields"))
        if str(field).strip()
    ]
    missing_count = int(
        _finite_float(
            market_bar_repair.get("missing_as_of_bar")
            or market_bar_repair.get("template_row_count")
        )
    )
    return {
        "schema_version": "priced-in-market-bar-manual-csv-v1",
        "path": market_bar_repair.get("local_template_path"),
        "exists": bool(market_bar_repair.get("local_template_exists")),
        "template_row_count": int(
            _finite_float(market_bar_repair.get("template_row_count"))
        ),
        "missing_row_count": missing_count,
        "complete_rows": int(_finite_float(local_progress.get("complete_rows"))),
        "partial_rows": int(_finite_float(local_progress.get("partial_rows"))),
        "empty_rows": int(_finite_float(local_progress.get("empty_rows"))),
        "required_fill_fields": required_fields,
        "sample_missing_tickers": sample_tickers,
        "template_command": market_bar_repair.get("dashboard_manual_template_command")
        or market_bar_repair.get("template_command"),
        "preview_command": operator_step.get("after_manual_command")
        or market_bar_repair.get("dashboard_manual_import_preview_command")
        or market_bar_repair.get("import_preview_command"),
        "execute_command": market_bar_repair.get(
            "dashboard_manual_import_execute_command"
        )
        or market_bar_repair.get("import_execute_command"),
        "next_action": (
            "Fill the required fields for each missing ticker, preview complete "
            "rows, then execute the local import only after review."
        ),
        "external_calls_made": 0,
    }


def _priced_in_market_bar_saved_provider_capture_context(
    provider_plan: Mapping[str, object],
):
    if not provider_plan:
        return None
    packet = _mapping_value(
        provider_plan,
        "provider_saved_file_capture_approval_packet",
    )
    if not packet:
        return None

    post_steps = []
    validate_command = None
    import_preview_command = None
    import_execute_command = None
    for step in _sequence_value(packet.get("post_capture_zero_call_steps")):
        if not isinstance(step, Mapping):
            continue
        step_name = str(step.get("step") or "").strip()
        if not step_name:
            continue
        request_body = step.get("request_body")
        step_payload = {
            "step": step_name,
            "command": step.get("tui_command") or step.get("cli_command"),
            "cli_command": step.get("cli_command"),
            "api": step.get("api"),
            "request_body": _row_dict(request_body)
            if isinstance(request_body, Mapping)
            else request_body,
            "external_calls_made": int(_finite_float(step.get("external_calls_made"))),
            "db_writes_made": int(_finite_float(step.get("db_writes_made"))),
            "db_writes_boundary": step.get("db_writes_boundary"),
        }
        post_steps.append(step_payload)
        if step_name == "validate_saved_file":
            validate_command = step_payload["command"]
        elif step_name == "preview_import":
            import_preview_command = step_payload["command"]
        elif step_name == "execute_import_after_preview":
            import_execute_command = step_payload["command"]

    capture_request_body = packet.get("capture_request_body")
    confirm_request_body = packet.get("capture_confirm_request_body")
    return {
        "schema_version": "priced-in-market-bar-saved-provider-capture-v1",
        "status": packet.get("status") or provider_plan.get("status"),
        "provider": packet.get("provider") or provider_plan.get("provider"),
        "provider_label": packet.get("provider_label")
        or provider_plan.get("provider_label"),
        "expected_as_of": packet.get("expected_as_of")
        or provider_plan.get("target_as_of"),
        "coverage_scope": packet.get("coverage_scope"),
        "active_security_count": packet.get("active_security_count")
        or provider_plan.get("active_security_count"),
        "existing_as_of_bar_count": packet.get("existing_as_of_bar_count")
        or provider_plan.get("existing_as_of_bar_count"),
        "missing_as_of_bar_count": packet.get("missing_as_of_bar_count")
        or provider_plan.get("missing_as_of_bar"),
        "provider_key_configured": bool(packet.get("provider_key_configured")),
        "provider_health_warning": packet.get("provider_health_warning"),
        "saved_file_status": packet.get("saved_file_status")
        or provider_plan.get("provider_saved_file_status"),
        "saved_file_path": packet.get("saved_file_path")
        or provider_plan.get("provider_saved_file_path"),
        "saved_file_exists": bool(provider_plan.get("provider_saved_file_exists")),
        "approval_required": bool(packet.get("approval_required")),
        "question": packet.get("question"),
        "purpose": packet.get("purpose"),
        "external_calls_without_approval": int(
            _finite_float(packet.get("external_calls_without_approval"))
        ),
        "external_calls_if_approved": int(
            _finite_float(packet.get("external_calls_if_approved"))
        ),
        "db_writes_during_capture": int(
            _finite_float(packet.get("db_writes_during_capture"))
        ),
        "capture_command": packet.get("tui_confirm_command")
        or packet.get("capture_cli_command"),
        "capture_cli_command": packet.get("capture_cli_command"),
        "capture_api": packet.get("capture_api"),
        "capture_request_body": _row_dict(capture_request_body)
        if isinstance(capture_request_body, Mapping)
        else capture_request_body,
        "capture_confirm_request_body": _row_dict(confirm_request_body)
        if isinstance(confirm_request_body, Mapping)
        else confirm_request_body,
        "post_capture_zero_call_steps": post_steps,
        "validate_command": validate_command,
        "import_preview_command": import_preview_command,
        "import_execute_command": import_execute_command,
        "guardrails": [
            str(item)
            for item in _sequence_value(packet.get("guardrails"))
            if str(item).strip()
        ],
        "next_action": packet.get("next_action"),
        "operator_note": (
            "This packet is descriptive and makes 0 provider calls. Capture "
            "requires explicit operator approval; validation and preview import "
            "read the saved file from disk."
        ),
        "external_calls_made": 0,
    }


def _priced_in_answer_blocker_ladder(rows, *, stocks_only: bool = False):
    ladder_rows = []
    for index, row in enumerate(rows, start=1):
        source = str(row.get("area") or "").strip()
        command = row.get("command")
        batchable = source in PRICED_IN_BATCHABLE_SOURCES
        plan_command = (
            _priced_in_source_batches_command(
                source,
                stocks_only=stocks_only,
                all_batches=True,
                json=True,
            )
            if batchable
            else command
        )
        plan_api = (
            _priced_in_source_batches_api(
                source,
                stocks_only=stocks_only,
                all_batches=True,
            )
            if batchable
            else None
        )
        execute_next_command = (
            _priced_in_source_batches_command(
                source,
                stocks_only=stocks_only,
                execute_next=True,
            )
            if batchable
            else None
        )
        execute_next_request_body = (
            {
                "source": source,
                "max_batches": 1,
                "stocks_only": stocks_only,
            }
            if batchable
            else None
        )
        ladder_rows.append(
            {
                "step": index,
                "source": source,
                "status": row.get("status"),
                "gap_count": int(_finite_float(row.get("gap_count"))),
                "depends_on": list(_sequence_value(row.get("depends_on"))),
                "next_action": row.get("next_action"),
                "command": command,
                "plan_command": plan_command,
                "plan_api": plan_api,
                "execute_next_command": execute_next_command,
                "execute_next_api": (
                    "POST /api/radar/priced-in/source-batches/execute-next"
                    if batchable
                    else None
                ),
                "execute_next_request_body": execute_next_request_body,
                "external_calls_made": 0,
            }
        )
    return {
        "schema_version": "priced-in-full-market-blocker-ladder-v1",
        "rows": ladder_rows,
        "operator_note": (
            "Clear blockers in order for a trusted full-market answer. This "
            "ladder is zero-call and does not execute source fills."
        ),
        "external_calls_made": 0,
    }


def _priced_in_answer_after_current_blocker(
    ladder: Mapping[str, object],
    *,
    engine: Engine | None = None,
    config: AppConfig | None = None,
    queue: Mapping[str, object] | None = None,
    stocks_only: bool = False,
):
    rows = [
        _row_dict(row)
        for row in _sequence_value(ladder.get("rows"))
        if isinstance(row, Mapping)
    ]
    if len(rows) < 2:
        return None
    current = rows[0]
    upcoming = rows[1]
    next_source = str(upcoming.get("source") or "").strip()
    if not next_source:
        return None
    next_status = str(upcoming.get("status") or "").strip()
    guidance = _priced_in_source_guidance(next_source, next_status)
    result = {
        "schema_version": "priced-in-after-current-blocker-v1",
        "current_blocker": current.get("source"),
        "current_gap_count": int(_finite_float(current.get("gap_count"))),
        "next_source": next_source,
        "next_status": next_status or None,
        "next_gap_count": int(_finite_float(upcoming.get("gap_count"))),
        "why_it_matters": guidance.get("meaning"),
        "next_action": upcoming.get("next_action"),
        "plan_command": upcoming.get("plan_command") or upcoming.get("command"),
        "plan_api": upcoming.get("plan_api"),
        "execute_next_command": upcoming.get("execute_next_command"),
        "execute_next_api": upcoming.get("execute_next_api"),
        "execute_next_request_body": upcoming.get("execute_next_request_body"),
        "operator_note": (
            "Preview only. Do not execute this source until the current blocker "
            "is cleared and the source batch plan matches your intended call budget."
        ),
        "external_calls_made": 0,
    }
    plan_summary = _priced_in_answer_next_source_plan_summary(
        engine,
        config,
        queue=queue,
        source=next_source,
        stocks_only=stocks_only,
    )
    if plan_summary:
        result["next_source_plan"] = plan_summary
    return result


def _priced_in_answer_next_source_plan_summary(
    engine: Engine | None,
    config: AppConfig | None,
    *,
    queue: Mapping[str, object] | None,
    source: str,
    stocks_only: bool = False,
):
    source_name = str(source or "").strip()
    if (
        engine is None
        or config is None
        or not isinstance(queue, Mapping)
        or not isinstance(queue.get("planning_rows"), (list, tuple))
        or source_name not in PRICED_IN_BATCHABLE_SOURCES
    ):
        return None
    try:
        payload = priced_in_source_gap_batches_payload(
            engine,
            config,
            source=source_name,
            batch_limit=1,
            stocks_only=stocks_only,
            queue=queue,
        )
    except (SQLAlchemyError, ValueError):
        return None
    diagnostic = _row_dict(_mapping_value(payload, "diagnostic"))
    batches = [
        _row_dict(batch)
        for batch in _sequence_value(payload.get("batches"))
        if isinstance(batch, Mapping)
    ]
    first_batch = batches[0] if batches else {}
    summary = {
        "schema_version": "priced-in-next-source-plan-summary-v1",
        "source": payload.get("source") or source_name,
        "status": payload.get("status"),
        "total_gap_rows": int(_finite_float(payload.get("total_gap_rows"))),
        "plannable_gap_rows": int(_finite_float(payload.get("plannable_gap_rows"))),
        "unplannable_gap_rows": int(_finite_float(payload.get("unplannable_gap_rows"))),
        "routed_gap_rows": int(_finite_float(payload.get("routed_gap_rows"))),
        "blocked_gap_rows": int(
            _finite_float(payload.get("blocked_gap_rows"))
            or max(
                0,
                int(_finite_float(payload.get("unplannable_gap_rows")))
                - int(_finite_float(payload.get("routed_gap_rows"))),
            )
        ),
        "blocked_rows": int(_finite_float(diagnostic.get("blocked_rows"))),
        "blocked_reason": diagnostic.get("blocked_reason"),
        "batch_count": int(_finite_float(payload.get("batch_count"))),
        "batch_size": int(_finite_float(payload.get("batch_size"))),
        "next_chunk_external_calls": int(
            _finite_float(first_batch.get("external_calls_required"))
        ),
        "next_chunk_sample_tickers": [
            str(ticker).strip().upper()
            for ticker in _sequence_value(first_batch.get("tickers"))
            if str(ticker).strip()
        ],
        "sample_blocked_tickers": [
            str(ticker).strip().upper()
            for ticker in _sequence_value(diagnostic.get("sample_blocked_tickers"))
            if str(ticker).strip()
        ],
        "sample_routed_non_company_tickers": [
            str(ticker).strip().upper()
            for ticker in _sequence_value(
                diagnostic.get("sample_routed_non_company_tickers")
            )
            if str(ticker).strip()
        ],
        "next_action": payload.get("next_action") or diagnostic.get("next_action"),
        "plan_command": payload.get("plan_command") or payload.get("command"),
        "plan_api": payload.get("plan_api"),
        "execute_next_command": payload.get("execute_next_command"),
        "execute_next_api": "POST /api/radar/priced-in/source-batches/execute-next",
        "execute_next_request_body": {
            "source": source_name,
            "max_batches": 1,
            "stocks_only": stocks_only,
        },
        "all_batches_command": payload.get("all_batches_command"),
        "all_batches_api": payload.get("all_batches_api"),
        "manual_template_command": diagnostic.get("manual_template_command"),
        "manual_template_api": diagnostic.get("manual_template_api"),
        "manual_validate_command": diagnostic.get("manual_validate_command"),
        "manual_validate_api": diagnostic.get("manual_validate_api"),
        "manual_fix_command": diagnostic.get("manual_fix_command"),
        "manual_fix_api": diagnostic.get("manual_fix_api"),
        "fix_command": diagnostic.get("fix_command"),
        "fix_api": diagnostic.get("fix_api"),
        "operator_boundary": (
            "This next-source plan is zero-call. It summarizes the reviewed batch "
            "plan only; execution remains a separate capped approval step."
        ),
        "external_calls_made": int(_finite_float(payload.get("external_calls_made"))),
    }
    missing_cik = _row_dict(
        {
            key: diagnostic.get(key)
            for key in (
                "missing_cik_company_like_rows",
                "missing_cik_non_company_rows",
                "missing_cik_unknown_type_rows",
                "missing_cik_type_counts",
                "sample_company_like_missing_cik_tickers",
                "sample_unknown_type_missing_cik_tickers",
            )
            if diagnostic.get(key) is not None
        }
    )
    if missing_cik:
        summary["missing_cik"] = missing_cik
    return summary

def priced_in_preflight_payload(
    engine: Engine,
    config: AppConfig,
    *,
    latest_run: Mapping[str, object] | None = None,
    discovery_snapshot: Mapping[str, object] | None = None,
    source_coverage: Mapping[str, object] | None = None,
    stocks_only: bool = False,
) -> dict[str, object]:
    run = (
        _row_dict(latest_run)
        if isinstance(latest_run, Mapping)
        else load_radar_run_summary(engine)
    )
    discovery = (
        _row_dict(discovery_snapshot)
        if isinstance(discovery_snapshot, Mapping)
        else radar_discovery_snapshot_payload(engine, config, radar_run_summary=run)
    )
    bar_universe = _latest_daily_bar_universe_payload(
        engine,
        available_at=datetime.now(UTC),
    )
    call_plan = radar_run_call_plan_payload(engine, config)
    provider_rows = provider_preflight_payload(config, radar_run_summary=run)
    resolved_source_coverage = (
        _row_dict(source_coverage)
        if isinstance(source_coverage, Mapping)
        else _priced_in_preflight_source_coverage(
            engine,
            run,
            stocks_only=stocks_only,
        )
    )
    freshness = _mapping_value(discovery, "freshness")
    scan_yield = _mapping_value(discovery, "yield")
    discovery_run = _mapping_value(discovery, "run")
    run_as_of = _date_iso_or_none(
        _parse_date(run.get("as_of"))
    ) or _date_iso_or_none(_parse_date(discovery_run.get("as_of")))
    latest_bar_as_of = str(bar_universe.get("latest_daily_bar_date") or "").strip()
    target_as_of = run_as_of or latest_bar_as_of or None
    if run_as_of:
        target_as_of_source = "run_as_of"
    elif latest_bar_as_of:
        target_as_of_source = "latest_daily_bar"
    else:
        target_as_of_source = None
    target_as_of_date = _parse_date(target_as_of)
    commands = _priced_in_preflight_commands(
        config,
        target_as_of=target_as_of_date,
        target_ticker_pages=_estimated_ticker_seed_pages(bar_universe),
        stocks_only=stocks_only,
    )
    stock_scope = (
        _priced_in_market_bar_stock_scope(engine, target_as_of=target_as_of_date)
        if stocks_only
        else None
    )
    market_bar_repair = _priced_in_preflight_market_bar_repair_scope(
        freshness=freshness,
        target_as_of=target_as_of_date,
        stocks_only=stocks_only,
        stock_scope=stock_scope,
    )
    manual_market_bar_repair = _priced_in_preflight_manual_market_bar_repair(
        engine,
        config,
        target_as_of=target_as_of_date,
        stocks_only=stocks_only,
    )
    if market_bar_repair:
        repair_missing = (
            int(
                _finite_float(
                    _mapping_value(market_bar_repair, "stock_scope").get(
                        "stock_like_missing_as_of_bar"
                    )
                )
            )
            if stocks_only
            else int(_finite_float(market_bar_repair.get("missing_as_of_bar")))
        )
        market_bar_repair = {
            **market_bar_repair,
            **manual_market_bar_repair,
            "provider_fill_plan": _priced_in_market_bar_provider_fill_plan(
                engine,
                config,
                target_as_of=target_as_of_date,
                missing=repair_missing,
                active_security_count=(
                    int(
                        _finite_float(
                            _mapping_value(market_bar_repair, "stock_scope").get(
                                "stock_like_active"
                            )
                        )
                    )
                    if stocks_only
                    else int(_finite_float(market_bar_repair.get("active_securities")))
                ),
                existing_as_of_bar_count=(
                    int(
                        _finite_float(
                            _mapping_value(market_bar_repair, "stock_scope").get(
                                "stock_like_with_as_of_bar"
                            )
                        )
                    )
                    if stocks_only
                    else int(_finite_float(market_bar_repair.get("with_as_of_bar")))
                ),
                coverage_scope="stock_like" if stocks_only else "active_universe",
                missing_as_of_bar_ticker_sample=(
                    manual_market_bar_repair.get("missing_as_of_bar_ticker_sample")
                    or market_bar_repair.get("missing_as_of_bar_ticker_sample")
                    or market_bar_repair.get("missing_as_of_bar_tickers")
                ),
                missing_security_type_counts=_mapping_value(
                    manual_market_bar_repair,
                    "missing_security_type_counts",
                ),
                missing_universe_diagnostic=_mapping_value(
                    manual_market_bar_repair,
                    "missing_universe_diagnostic",
                ),
            ),
        }
    resolved_source_coverage = _priced_in_source_coverage_with_market_bar_scope(
        resolved_source_coverage,
        {"repair": market_bar_repair},
    )
    provider_blocker = _latest_market_bar_provider_failure(
        engine,
        provider=_provider_name(config.daily_market_provider, default="csv"),
        target_as_of=target_as_of,
    )
    rows = _priced_in_preflight_rows(
        discovery,
        call_plan,
        provider_rows,
        commands,
        config,
        bar_universe,
        resolved_source_coverage,
        provider_blocker,
        stocks_only=stocks_only,
        stock_scope=stock_scope,
        market_bar_repair=market_bar_repair,
    )
    evidence_plan = _priced_in_evidence_plan(rows)
    first_blocker = _priced_in_preflight_first_blocker(
        evidence_plan,
        resolved_source_coverage,
    )
    operator_next_step = _priced_in_preflight_operator_next_step(
        first_blocker,
        evidence_plan,
    )
    blocked_rows = [row for row in rows if row["status"] == "blocked"]
    attention_rows = [row for row in rows if row["status"] == "attention"]
    if blocked_rows:
        status = "blocked"
        headline = f"{len(blocked_rows)} prerequisite(s) block a useful full-market scan."
        next_action = str(blocked_rows[0]["next_action"])
    elif attention_rows:
        status = "attention"
        headline = f"{len(attention_rows)} prerequisite(s) need attention before trusting output."
        next_action = str(attention_rows[0]["next_action"])
    else:
        status = "ready"
        headline = "Full-market priced-in scan prerequisites look ready."
        next_action = "Run one capped radar cycle, then review priced-in gaps."
    scan_scope = {
        "instrument_filter": "stocks_only" if stocks_only else "all_instruments",
        "active_security_count": int(
            _finite_float(freshness.get("active_security_count"))
        ),
        "requested_securities": int(
            _finite_float(scan_yield.get("requested_securities"))
        ),
        "scanned_securities": int(_finite_float(scan_yield.get("scanned_securities"))),
        "universe": discovery_run.get("universe"),
    }
    if stock_scope:
        scan_scope.update(
            {
                "stock_like_active": int(
                    _finite_float(stock_scope.get("stock_like_active")),
                ),
                "stock_like_with_as_of_bar": int(
                    _finite_float(stock_scope.get("stock_like_with_as_of_bar")),
                ),
                "stock_like_missing_as_of_bar": int(
                    _finite_float(stock_scope.get("stock_like_missing_as_of_bar")),
                ),
            }
        )
    return {
        "schema_version": "priced-in-preflight-v1",
        "stocks_only": bool(stocks_only),
        "instrument_filter": "stocks_only" if stocks_only else "all_instruments",
        "status": status,
        "headline": headline,
        "next_action": next_action,
        "first_gap": first_blocker.get("area"),
        "first_blocker": first_blocker,
        "operator_next_step": operator_next_step,
        "target_as_of": target_as_of,
        "target_as_of_source": target_as_of_source,
        "latest_run_as_of": run_as_of,
        "scan_scope": scan_scope,
        "provider_blocker": provider_blocker,
        "external_calls_made": 0,
        "scan_status": _priced_in_scan_status(discovery),
        "provider": {
            "market_provider": _provider_name(config.daily_market_provider, default="csv"),
            "ticker_seed_cap_pages": max(1, int(config.polygon_tickers_max_pages)),
            "ticker_page_delay_seconds": config.polygon_ticker_page_delay_seconds,
            **bar_universe,
        },
        "commands": commands,
        "api": {
            "seed_universe": "POST /api/radar/universe/seed",
            "call_plan": "POST /api/radar/runs/call-plan",
            "run": "POST /api/radar/runs",
            "queue": "GET /api/radar/priced-in"
            + ("?stocks_only=true" if stocks_only else ""),
        },
        "call_plan": {
            "status": call_plan.get("status"),
            "max_external_call_count": call_plan.get("max_external_call_count"),
            "next_action": call_plan.get("next_action"),
        },
        "evidence_plan": evidence_plan,
        "source_coverage": resolved_source_coverage,
        "rows": rows,
    }


def _priced_in_preflight_first_blocker(
    evidence_plan: Mapping[str, object],
    source_coverage: Mapping[str, object],
):
    steps = [
        _row_dict(step)
        for step in _sequence_value(evidence_plan.get("steps"))
        if isinstance(step, Mapping)
    ]
    if not steps:
        return {
            "schema_version": "priced-in-first-blocker-v1",
            "status": "ready",
            "area": None,
            "action": "Review the full-scan priced-in queue and candidate evidence.",
            "command": "catalyst-radar priced-in-queue --full-scan --limit 50",
            "api": "GET /api/radar/priced-in",
            "depends_on": [],
            "source_gap_count": 0,
            "source_row_count": 0,
            "source_available_count": 0,
            "source_blocked_reason": None,
            "external_calls_made": 0,
        }

    first = steps[0]
    area = str(first.get("area") or "").strip()
    source_action = next(
        (
            _row_dict(action)
            for action in _sequence_value(source_coverage.get("actions"))
            if isinstance(action, Mapping)
            and str(action.get("source") or "").strip() == area
        ),
        {},
    )
    gap_count = int(
        _finite_float(source_action.get("gap_count"))
        or _finite_float(source_action.get("missing"))
        + _finite_float(source_action.get("stale"))
    )
    row_count = int(_finite_float(source_action.get("row_count")))
    available_count = int(_finite_float(source_action.get("available")))
    source_diagnostic = _mapping_value(source_action, "diagnostic")
    blocked_reason = (
        source_action.get("blocked_reason")
        or source_diagnostic.get("blocked_reason")
        or source_diagnostic.get("reason")
    )
    return {
        "schema_version": "priced-in-first-blocker-v1",
        "status": first.get("status"),
        "area": area or None,
        "action": first.get("action"),
        "command": first.get("command"),
        "api": first.get("api"),
        "depends_on": list(_sequence_value(first.get("depends_on"))),
        "operator_step": _row_dict(_mapping_value(first, "operator_step")),
        "manual_step": bool(first.get("manual_step")),
        "after_manual_command": first.get("after_manual_command"),
        "source_gap_count": gap_count,
        "source_row_count": row_count,
        "source_available_count": available_count,
        "source_blocked_reason": blocked_reason,
        "external_calls_made": 0,
    }


def _priced_in_preflight_operator_next_step(
    first_blocker: Mapping[str, object],
    evidence_plan: Mapping[str, object],
):
    action = first_blocker.get("action") or evidence_plan.get("next_action")
    command = first_blocker.get("command") or evidence_plan.get("next_command")
    return {
        "schema_version": "priced-in-preflight-next-step-v1",
        "status": first_blocker.get("status") or evidence_plan.get("status"),
        "area": first_blocker.get("area"),
        "action": action,
        "command": command,
        "api": first_blocker.get("api"),
        "manual_step": bool(first_blocker.get("manual_step")),
        "after_manual_command": first_blocker.get("after_manual_command"),
        "operator_step": _row_dict(_mapping_value(first_blocker, "operator_step")),
        "external_calls_made": 0,
    }

def _priced_in_evidence_plan(rows: Sequence[Mapping[str, object]]) -> dict[str, object]:
    actionable_rows = [
        _row_dict(row)
        for row in rows
        if str(row.get("status") or "") in {"blocked", "attention"}
    ]
    by_area = {str(row.get("area") or ""): row for row in actionable_rows}
    blocked_order = (
        "universe",
        "scan_scope",
        "market_bars",
        "run_call_plan",
        "catalyst_events",
        "local_text",
        "options",
        "broker_context",
        "agent_review",
    )
    attention_order = (
        "scan_scope",
        "market_bars",
        "catalyst_events",
        "local_text",
        "options",
        "broker_context",
        "agent_review",
        "run_call_plan",
    )
    steps: list[dict[str, object]] = []
    added: set[str] = set()
    for area in blocked_order:
        row = by_area.get(area)
        if row is None or str(row.get("status") or "") != "blocked":
            continue
        steps.append(_priced_in_evidence_plan_step(len(steps) + 1, row))
        added.add(area)
    for area in attention_order:
        row = by_area.get(area)
        if row is None or area in added or str(row.get("status") or "") != "attention":
            continue
        steps.append(_priced_in_evidence_plan_step(len(steps) + 1, row))
        added.add(area)
    for row in actionable_rows:
        area = str(row.get("area") or "")
        if area not in added:
            steps.append(_priced_in_evidence_plan_step(len(steps) + 1, row))
            added.add(area)
    if steps:
        status = "blocked" if any(step["status"] == "blocked" for step in steps) else "attention"
        headline = (
            f"{len(steps)} evidence step(s) need attention before priced-in output "
            "is decision-useful."
        )
        next_action = str(steps[0].get("action") or "")
        next_command = str(steps[0].get("command") or "")
    else:
        status = "ready"
        headline = "Priced-in evidence plan is ready for review."
        next_action = "Review the full-scan priced-in queue and candidate evidence."
        next_command = "catalyst-radar priced-in-queue --full-scan --limit 50"
    return {
        "schema_version": "priced-in-evidence-plan-v1",
        "status": status,
        "headline": headline,
        "next_action": next_action,
        "next_command": next_command,
        "external_calls_made": 0,
        "steps": steps,
    }


def _priced_in_evidence_plan_step(
    priority: int,
    row: Mapping[str, object],
) -> dict[str, object]:
    area = str(row.get("area") or "")
    step = {
        "priority": priority,
        "area": area,
        "status": row.get("status"),
        "why": row.get("finding"),
        "action": row.get("next_action"),
        "command": row.get("command"),
        "api": row.get("api"),
        "depends_on": _priced_in_evidence_plan_dependencies(area),
    }
    operator_step = _mapping_value(row, "operator_step")
    if operator_step:
        step["operator_step"] = _row_dict(operator_step)
        step["manual_step"] = bool(operator_step.get("manual_step"))
        step["after_manual_command"] = operator_step.get("after_manual_command")
    return step


def _priced_in_evidence_plan_dependencies(area: str) -> list[str]:
    if area == "local_text":
        return ["catalyst_events"]
    if area in {"options", "broker_context", "agent_review"}:
        return ["market_bars", "catalyst_events", "local_text"]
    if area == "run_call_plan":
        return ["market_bars"]
    return []


def _priced_in_preflight_source_coverage(
    engine: Engine,
    latest_run: Mapping[str, object],
    *,
    stocks_only: bool = False,
) -> dict[str, object]:
    if latest_run:
        candidate_rows = load_radar_run_candidate_rows(
            engine,
            latest_run,
            limit=None,
            include_artifacts=True,
        )
    else:
        candidate_rows = load_candidate_rows(engine, limit=None, include_artifacts=True)
    broker_summary = load_broker_summary(engine)
    candidate_rows = candidate_rows_with_market_context(
        candidate_rows,
        _market_context_value(broker_summary),
    )
    security_meta = _security_metadata_by_ticker(
        engine,
        [
            str(row.get("ticker") or "").strip().upper()
            for row in candidate_rows
            if isinstance(row, Mapping)
        ],
    )
    queue_rows = [
        _priced_in_queue_row(
            row,
            security_metadata=security_meta.get(
                str(row.get("ticker") or "").strip().upper()
            ),
        )
        for row in candidate_rows
        if isinstance(row, Mapping)
    ]
    if stocks_only:
        queue_rows = [row for row in queue_rows if _priced_in_row_is_stock_like(row)]
    coverage = priced_in_source_coverage_summary(queue_rows, stocks_only=stocks_only)
    coverage = _priced_in_source_coverage_with_instrument_routes(
        engine,
        queue_rows,
        coverage,
    )
    return _priced_in_source_coverage_with_option_diagnostic(engine, queue_rows, coverage)


def _priced_in_preflight_manual_market_bar_repair(
    engine: Engine,
    config: AppConfig,
    *,
    target_as_of: date | None,
    stocks_only: bool,
):
    if target_as_of is None:
        return {}
    try:
        plan = manual_market_bars_repair_plan(
            engine,
            expected_as_of=target_as_of,
            stocks_only=stocks_only,
            provider_key_configured=config.polygon_api_key_configured,
            **_manual_repair_provider_health_kwargs(engine),
        ).as_payload()
    except ValueError as exc:
        return {
            "manual_repair_status": "invalid",
            "manual_repair_error": str(exc),
            "external_calls_made": 0,
        }
    return {
        "manual_repair_status": plan.get("status"),
        "operator_step": _row_dict(_mapping_value(plan, "operator_step")),
        "provider_saved_file_capture_command": plan.get(
            "provider_saved_file_capture_command"
        ),
        "provider_saved_file_capture_api": plan.get("provider_saved_file_capture_api"),
        "provider_saved_file_validate_command": plan.get(
            "provider_saved_file_validate_command"
        ),
        "provider_saved_file_capture_request_body": plan.get(
            "provider_saved_file_capture_request_body"
        ),
        "provider_saved_file_capture_confirm_request_body": plan.get(
            "provider_saved_file_capture_confirm_request_body"
        ),
        "provider_saved_file_validate_api": plan.get("provider_saved_file_validate_api"),
        "provider_saved_file_validate_request_body": plan.get(
            "provider_saved_file_validate_request_body"
        ),
        "provider_saved_file_import_command": plan.get(
            "provider_saved_file_import_command"
        ),
        "provider_saved_file_import_api": plan.get("provider_saved_file_import_api"),
        "provider_saved_file_import_preview_request_body": plan.get(
            "provider_saved_file_import_preview_request_body"
        ),
        "provider_saved_file_import_request_body": plan.get(
            "provider_saved_file_import_request_body"
        ),
        "manual_template_command": plan.get("manual_template_command"),
        "manual_import_preview_command": plan.get("manual_import_preview_command"),
        "manual_import_api": plan.get("manual_import_api"),
        "local_template_path": plan.get("local_template_path"),
        "local_template_exists": bool(plan.get("local_template_exists")),
        "local_template_preview": _row_dict(_mapping_value(plan, "local_template_preview")),
        "external_calls_made": 0,
    }


def _priced_in_preflight_market_bar_repair_scope(
    *,
    freshness: Mapping[str, object],
    target_as_of: date | None,
    stocks_only: bool,
    stock_scope: Mapping[str, object] | None,
) -> dict[str, object]:
    if stocks_only and stock_scope:
        return {"stocks_only": True, "stock_scope": _row_dict(stock_scope)}

    active = int(_finite_float(freshness.get("active_security_count")))
    if active <= 0:
        return {}
    available = int(
        _finite_float(freshness.get("active_security_with_as_of_bar_count"))
    )
    missing = int(_finite_float(freshness.get("missing_as_of_daily_bar_count")))
    sample_tickers = [
        str(ticker).strip().upper()
        for ticker in _sequence_value(freshness.get("missing_as_of_daily_bar_tickers"))
        if str(ticker).strip()
    ]
    return {
        "status": "ready" if missing <= 0 else "attention",
        "target_as_of": _date_iso_or_none(target_as_of),
        "active_securities": active,
        "with_as_of_bar": available,
        "missing_as_of_bar": missing,
        "stocks_only": False,
        "missing_as_of_bar_tickers": sample_tickers,
        "missing_as_of_bar_ticker_sample": _sample_tickers(sample_tickers),
    }


def priced_in_source_coverage_summary(
    rows: Sequence[Mapping[str, object]],
    *,
    stocks_only: bool = False,
) -> dict[str, object]:
    counts = {
        source: {"available": 0, "stale": 0, "missing": 0}
        for source in PRICED_IN_SOURCE_CLASSES
    }
    sample_tickers: dict[str, list[str]] = {source: [] for source in PRICED_IN_SOURCE_CLASSES}
    row_count = 0
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        row_count += 1
        ticker = _priced_in_action_ticker(row)
        sources = _priced_in_row_source_payload(row)
        seen: set[str] = set()
        for status in ("available", "stale", "missing"):
            for source in _sequence_value(sources.get(status)):
                normalized = str(source)
                if normalized not in counts:
                    continue
                counts[normalized][status] += 1
                seen.add(normalized)
                if status in {"missing", "stale"}:
                    _append_priced_in_action_ticker(sample_tickers[normalized], ticker)
        for source in PRICED_IN_SOURCE_CLASSES:
            if source not in seen:
                counts[source]["missing"] += 1
                _append_priced_in_action_ticker(sample_tickers[source], ticker)

    source_rows: dict[str, dict[str, object]] = {}
    for source, values in counts.items():
        available = values["available"]
        stale = values["stale"]
        missing = values["missing"]
        denominator = max(row_count, available + stale + missing)
        source_rows[source] = {
            "available": available,
            "stale": stale,
            "missing": missing,
            "row_count": row_count,
            "coverage_pct": round((available / denominator) * 100, 1)
            if denominator
            else 0.0,
            "sample_tickers": sample_tickers[source],
        }
    weak_sources = [
        source
        for source, _values in sorted(
            source_rows.items(),
            key=lambda item: (
                float(item[1]["coverage_pct"]),
                -int(item[1]["stale"]) - int(item[1]["missing"]),
                PRICED_IN_SOURCE_CLASSES.index(item[0])
                if item[0] in PRICED_IN_SOURCE_CLASSES
                else len(PRICED_IN_SOURCE_CLASSES),
            ),
        )
        if row_count and float(_values["coverage_pct"]) < 100.0
    ][:3]
    return {
        "schema_version": "priced-in-source-coverage-v1",
        "row_count": row_count,
        "stocks_only": bool(stocks_only),
        "instrument_filter": "stocks_only" if stocks_only else "all_instruments",
        "sources": source_rows,
        "weak_sources": weak_sources,
        "actions": _priced_in_source_action_rows(
            source_rows,
            row_count,
            stocks_only=stocks_only,
        ),
        "summary": _priced_in_source_coverage_summary_text(source_rows, row_count),
    }


def _priced_in_source_coverage_with_option_diagnostic(
    engine: Engine,
    rows: Sequence[Mapping[str, object]],
    source_coverage: Mapping[str, object],
) -> dict[str, object]:
    diagnostic = _priced_in_option_gap_diagnostic(engine, rows)
    if not diagnostic:
        return _row_dict(source_coverage)
    updated = _row_dict(source_coverage)
    stocks_only = bool(updated.get("stocks_only"))
    point_in_time_progress = _options_point_in_time_fixture_progress(
        diagnostic,
        stocks_only=stocks_only,
    )
    if point_in_time_progress:
        diagnostic = {
            **diagnostic,
            "point_in_time_fixture_progress": point_in_time_progress,
        }
    updated["options_gap_diagnostic"] = diagnostic
    updated_actions: list[dict[str, object]] = []
    for action in _sequence_value(updated.get("actions")):
        if not isinstance(action, Mapping):
            continue
        action_row = _row_dict(action)
        if str(action_row.get("source") or "") == "options":
            action_row["diagnostic"] = diagnostic
            progress_action = (
                point_in_time_progress.get("next_action")
                if bool(point_in_time_progress.get("exists"))
                else None
            )
            next_action = str(
                progress_action or diagnostic.get("next_action") or ""
            ).strip()
            if next_action:
                action_row["next_action"] = next_action
        updated_actions.append(action_row)
    updated["actions"] = updated_actions
    return updated


def _priced_in_source_coverage_with_instrument_routes(
    engine: Engine,
    rows: Sequence[Mapping[str, object]],
    source_coverage: Mapping[str, object],
) -> dict[str, object]:
    applicability = _catalyst_event_applicability_payload(engine, rows)
    if not applicability:
        return _row_dict(source_coverage)
    updated = _row_dict(source_coverage)
    source_rows = {
        str(source): _row_dict(values)
        for source, values in _mapping_value(updated, "sources").items()
        if isinstance(values, Mapping)
    }
    catalyst = source_rows.get("catalyst_events")
    if catalyst is None:
        return updated
    catalyst = {
        **catalyst,
        "raw_row_count": catalyst.get("row_count"),
        "raw_available": catalyst.get("available"),
        "raw_stale": catalyst.get("stale"),
        "raw_missing": catalyst.get("missing"),
        "available": applicability.get("applicable_available"),
        "stale": applicability.get("applicable_stale"),
        "missing": applicability.get("applicable_missing"),
        "row_count": applicability.get("applicable_rows"),
        "coverage_pct": _source_coverage_pct(
            available=int(_finite_float(applicability.get("applicable_available"))),
            stale=int(_finite_float(applicability.get("applicable_stale"))),
            missing=int(_finite_float(applicability.get("applicable_missing"))),
        ),
        "sample_tickers": list(
            _sequence_value(applicability.get("sample_applicable_gap_tickers"))
        ),
        "applicability": applicability,
        "non_applicable_rows": applicability.get("non_applicable_rows"),
        "routed_non_company_gap_rows": applicability.get(
            "non_applicable_gap_rows"
        ),
    }
    source_rows["catalyst_events"] = catalyst
    row_count = int(_finite_float(updated.get("row_count")))
    updated["sources"] = source_rows
    updated["weak_sources"] = _priced_in_source_coverage_weak_sources(source_rows)
    updated["actions"] = _priced_in_source_action_rows(
        source_rows,
        row_count,
        stocks_only=bool(updated.get("stocks_only")),
    )
    updated["summary"] = _priced_in_source_coverage_summary_text(source_rows, row_count)
    return updated


def _priced_in_source_coverage_with_market_bar_scope(
    source_coverage: Mapping[str, object],
    market_bars: Mapping[str, object],
) -> dict[str, object]:
    updated = _row_dict(source_coverage)
    repair = _mapping_value(market_bars, "repair")
    if not repair:
        return updated

    stocks_only = bool(repair.get("stocks_only"))
    if stocks_only:
        stock_scope = _mapping_value(repair, "stock_scope")
        active = int(_finite_float(stock_scope.get("stock_like_active")))
        available = int(_finite_float(stock_scope.get("stock_like_with_as_of_bar")))
        missing = int(_finite_float(stock_scope.get("stock_like_missing_as_of_bar")))
        sample_tickers = [
            str(ticker).strip().upper()
            for ticker in _sequence_value(
                stock_scope.get("sample_missing_stock_like_tickers")
            )
            if str(ticker).strip()
        ]
        coverage_basis = "stock_like_active_as_of_bars"
        scope_payload: Mapping[str, object] = stock_scope
    else:
        active = int(_finite_float(repair.get("active_securities")))
        available = int(_finite_float(repair.get("with_as_of_bar")))
        missing = int(_finite_float(repair.get("missing_as_of_bar")))
        sample_tickers = [
            str(ticker).strip().upper()
            for ticker in _sequence_value(
                repair.get("missing_as_of_bar_ticker_sample")
                or repair.get("missing_as_of_bar_tickers")
            )
            if str(ticker).strip()
        ]
        coverage_basis = "active_universe_as_of_bars"
        scope_payload = {
            "target_as_of": repair.get("target_as_of"),
            "active_securities": active,
            "with_as_of_bar": available,
            "missing_as_of_bar": missing,
        }
    if active <= 0:
        return updated

    provider_fill_plan = _mapping_value(repair, "provider_fill_plan")
    local_template_preview = _mapping_value(repair, "local_template_preview")
    source_rows = {
        str(source): _row_dict(values)
        for source, values in _mapping_value(updated, "sources").items()
        if isinstance(values, Mapping)
    }
    market_row = _row_dict(source_rows.get("market_bars", {}))
    market_row.update(
        {
            "raw_row_count": market_row.get("row_count"),
            "raw_available": market_row.get("available"),
            "raw_stale": market_row.get("stale"),
            "raw_missing": market_row.get("missing"),
            "available": available,
            "stale": 0,
            "missing": max(0, missing),
            "row_count": active,
            "coverage_pct": _source_coverage_pct(
                available=available,
                stale=0,
                missing=max(0, missing),
            ),
            "sample_tickers": _sample_tickers(sample_tickers),
            "coverage_basis": coverage_basis,
            "as_of_bar_scope": _row_dict(scope_payload),
            "repair_status": repair.get("status"),
            "dashboard_manual_template_command": repair.get(
                "dashboard_manual_template_command"
            ),
            "dashboard_manual_template_regenerate_command": repair.get(
                "dashboard_manual_template_regenerate_command"
            ),
            "dashboard_manual_import_preview_command": repair.get(
                "dashboard_manual_import_preview_command"
            ),
            "dashboard_manual_import_execute_command": repair.get(
                "dashboard_manual_import_execute_command"
            ),
            "provider_fill_plan": _row_dict(provider_fill_plan),
            "provider_fill_command": provider_fill_plan.get("provider_call_command"),
            "provider_fill_status": provider_fill_plan.get("status"),
            "provider_fill_external_call_count": provider_fill_plan.get(
                "execute_external_call_count"
            ),
            "local_template_status": local_template_preview.get("status"),
            "local_template_fill_progress": _row_dict(
                _mapping_value(repair, "local_template_fill_progress")
            ),
        }
    )
    source_rows["market_bars"] = market_row

    row_count = int(_finite_float(updated.get("row_count")))
    updated["sources"] = source_rows
    updated["weak_sources"] = _priced_in_source_coverage_weak_sources(source_rows)
    recomputed_actions = _priced_in_source_action_rows(
        source_rows,
        row_count,
        stocks_only=bool(updated.get("stocks_only")),
    )
    existing_actions = {
        str(action.get("source") or ""): _row_dict(action)
        for action in _sequence_value(updated.get("actions"))
        if isinstance(action, Mapping)
    }
    updated["actions"] = [
        action
        if str(action.get("source") or "") == "market_bars"
        else existing_actions.get(str(action.get("source") or ""), action)
        for action in recomputed_actions
    ]
    updated["summary"] = _priced_in_source_coverage_summary_text(source_rows, row_count)
    return updated


def _source_coverage_pct(*, available: int, stale: int, missing: int) -> float:
    denominator = max(available + stale + missing, 0)
    return round((available / denominator) * 100, 1) if denominator else 0.0


def _priced_in_source_coverage_weak_sources(
    source_rows: Mapping[str, Mapping[str, object]],
) -> list[str]:
    return [
        source
        for source, _values in sorted(
            source_rows.items(),
            key=lambda item: (
                float(_finite_float(item[1].get("coverage_pct"))),
                -int(_finite_float(item[1].get("stale")))
                - int(_finite_float(item[1].get("missing"))),
                PRICED_IN_SOURCE_CLASSES.index(item[0])
                if item[0] in PRICED_IN_SOURCE_CLASSES
                else len(PRICED_IN_SOURCE_CLASSES),
            ),
        )
        if int(_finite_float(_values.get("row_count"))) > 0
        and float(_finite_float(_values.get("coverage_pct"))) < 100.0
    ][:3]


def _priced_in_option_gap_diagnostic(
    engine: Engine,
    rows: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    missing_rows = [
        row
        for row in rows
        if isinstance(row, Mapping)
        and "options"
        in {
            str(item)
            for status in ("missing", "stale")
            for item in _sequence_value(_priced_in_row_source_payload(row).get(status))
        }
    ]
    if not missing_rows:
        return {}
    tickers = sorted(
        {
            str(row.get("ticker") or "").strip().upper()
            for row in missing_rows
            if str(row.get("ticker") or "").strip()
        }
    )
    if not tickers:
        return {}
    features_by_ticker = _option_feature_rows_by_ticker(engine, tickers)
    newer_than_scan: list[str] = []
    after_cutoff: list[str] = []
    no_stored_options: list[str] = []
    eligible_but_missing: list[str] = []
    scan_dates: list[str] = []
    for row in missing_rows:
        ticker = str(row.get("ticker") or "").strip().upper()
        if not ticker:
            continue
        row_as_of = _parse_utc_datetime(row.get("as_of"))
        row_cutoff = _parse_utc_datetime(row.get("available_at"))
        if row_as_of is not None:
            scan_dates.append(row_as_of.date().isoformat())
        features = features_by_ticker.get(ticker, [])
        if not features:
            no_stored_options.append(ticker)
            continue
        eligible = [
            feature
            for feature in features
            if (
                _parse_utc_datetime(feature.get("as_of")) is not None
                and row_as_of is not None
                and _parse_utc_datetime(feature.get("as_of")) <= row_as_of
                and (
                    row_cutoff is None
                    or (
                        _parse_utc_datetime(feature.get("available_at")) is not None
                        and _parse_utc_datetime(feature.get("available_at"))
                        <= row_cutoff
                    )
                )
            )
        ]
        if eligible:
            eligible_but_missing.append(ticker)
            continue
        if row_as_of is not None and any(
            _parse_utc_datetime(feature.get("as_of")) is not None
            and _parse_utc_datetime(feature.get("as_of")) > row_as_of
            for feature in features
        ):
            newer_than_scan.append(ticker)
            continue
        if row_cutoff is not None and any(
            _parse_utc_datetime(feature.get("available_at")) is not None
            and _parse_utc_datetime(feature.get("available_at")) > row_cutoff
            for feature in features
        ):
            after_cutoff.append(ticker)
            continue
        no_stored_options.append(ticker)

    status = "no_stored_options"
    if newer_than_scan:
        status = "newer_than_scan"
    elif after_cutoff:
        status = "after_decision_cutoff"
    elif eligible_but_missing:
        status = "eligible_but_not_scored"
    next_action = _option_gap_next_action(
        status=status,
        sample_newer=_sample_tickers(newer_than_scan),
        sample_missing=_sample_tickers(no_stored_options),
    )
    evidence = (
        f"missing={len(missing_rows)}; "
        f"newer_than_scan={len(set(newer_than_scan))}; "
        f"after_cutoff={len(set(after_cutoff))}; "
        f"no_stored_options={len(set(no_stored_options))}; "
        f"eligible_but_missing={len(set(eligible_but_missing))}"
    )
    return {
        "schema_version": "priced-in-options-gap-diagnostic-v1",
        "status": status,
        "missing_rows": len(missing_rows),
        "scan_as_of_dates": sorted(set(scan_dates)),
        "newer_than_scan_count": len(set(newer_than_scan)),
        "after_cutoff_count": len(set(after_cutoff)),
        "no_stored_options_count": len(set(no_stored_options)),
        "eligible_but_missing_count": len(set(eligible_but_missing)),
        "sample_newer_than_scan_tickers": _sample_tickers(newer_than_scan),
        "sample_no_stored_option_tickers": _sample_tickers(no_stored_options),
        "sample_after_cutoff_tickers": _sample_tickers(after_cutoff),
        "sample_eligible_but_missing_tickers": _sample_tickers(eligible_but_missing),
        "next_action": next_action,
        "evidence": evidence,
    }


def _option_feature_rows_by_ticker(
    engine: Engine,
    tickers: Sequence[str],
) -> dict[str, list[dict[str, object]]]:
    if not tickers:
        return {}
    stmt = (
        select(
            option_features.c.ticker,
            option_features.c.as_of,
            option_features.c.available_at,
            option_features.c.provider,
        )
        .where(option_features.c.ticker.in_(tickers))
        .order_by(
            option_features.c.ticker,
            option_features.c.as_of.desc(),
            option_features.c.available_at.desc(),
            option_features.c.provider,
        )
    )
    by_ticker: dict[str, list[dict[str, object]]] = defaultdict(list)
    with engine.connect() as conn:
        for row in conn.execute(stmt):
            values = _row_dict(row._mapping)
            ticker = str(values.get("ticker") or "").strip().upper()
            if ticker:
                by_ticker[ticker].append(values)
    return by_ticker


def _option_gap_next_action(
    *,
    status: str,
    sample_newer: Sequence[str],
    sample_missing: Sequence[str],
) -> str:
    if status == "newer_than_scan":
        sample = (
            f" Example tickers: {', '.join(sample_newer)}." if sample_newer else ""
        )
        return (
            f"Stored options exist after this scan date.{sample} Rerun only with a "
            "current scan date and current bars, or ingest point-in-time options for "
            "the original scan date."
        )
    if status == "after_decision_cutoff":
        return (
            "Stored options were not available at the scan cutoff; rerun with an "
            "appropriate cutoff or ingest point-in-time options available then."
        )
    if status == "eligible_but_not_scored":
        return "Rerun the scan so eligible stored option features enter priced-in scoring."
    sample = (
        f" Example tickers: {', '.join(sample_missing)}." if sample_missing else ""
    )
    return (
        f"No stored option features exist.{sample} Sync current Schwab options for a "
        "current rerun or ingest a point-in-time options fixture for the scan date."
    )


def _sample_tickers(values: Sequence[str]) -> list[str]:
    return list(dict.fromkeys(str(value).strip().upper() for value in values if value))[
        :PRICED_IN_SOURCE_ACTION_TICKER_LIMIT
    ]


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
    full_scan_market_blocker = _first_discovery_blocker_for_codes(
        discovery_snapshot,
        FULL_SCAN_MARKET_BLOCKER_CODES,
    )
    if full_scan_market_blocker:
        sequence += 1
        queue_rows.append(
            _operator_work_queue_row(
                sequence=sequence,
                severity=120,
                priority="must_fix",
                area="Full scan market bars",
                item=str(
                    full_scan_market_blocker.get("finding")
                    or "Fresh full-market bars are incomplete."
                ),
                status="blocked",
                next_action=str(
                    full_scan_market_blocker.get("next_action")
                    or "Refresh full-universe market bars before rerunning the scan."
                ),
                evidence=str(full_scan_market_blocker.get("code") or "market_bars"),
                source="discovery_snapshot",
            )
        )
    for row in readiness_rows:
        status = str(row.get("status") or "")
        if status not in {"blocked", "attention"}:
            continue
        if (
            full_scan_market_blocker
            and str(row.get("area") or "") in FULL_SCAN_DERIVATIVE_READINESS_AREAS
        ):
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
        next_action = _operator_blocking_next_action(queue_rows, blocking_count)
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


def _operator_blocking_next_action(
    queue_rows: Sequence[Mapping[str, object]],
    blocking_count: int,
) -> str:
    blocker_actions: list[str] = []
    seen: set[str] = set()
    for row in queue_rows:
        if str(row.get("priority") or "") != "must_fix":
            continue
        action = str(row.get("next_action") or "").strip()
        if not action or action in seen:
            continue
        seen.add(action)
        blocker_actions.append(action.rstrip("."))
        if len(blocker_actions) >= 3:
            break
    if blocking_count > 1 and blocker_actions:
        joined = "; ".join(blocker_actions)
        return f"Clear {blocking_count} setup blockers: {joined}."
    if blocker_actions:
        return f"{blocker_actions[0]}."
    return "Review the top queue item."


def operator_next_step_payload(
    operator_queue: Mapping[str, object] | None,
) -> dict[str, object]:
    """Return the single canonical operator next step from the work queue."""
    queue = _row_dict(operator_queue) if isinstance(operator_queue, Mapping) else {}
    rows = [
        _row_dict(row)
        for row in _sequence_value(queue.get("rows"))
        if isinstance(row, Mapping)
    ]
    top = rows[0] if rows else {}
    counts = _mapping_value(queue, "counts")
    blocking_count = int(_finite_float(counts.get("blocking")))
    action = str(
        _first_present(
            queue.get("next_action") if blocking_count > 1 else None,
            top.get("next_action"),
            queue.get("next_action"),
            "No operator action required.",
        )
    )
    return {
        "schema_version": "operator-next-step-v1",
        "status": top.get("status") or queue.get("status") or "empty",
        "priority": top.get("priority") or "none",
        "area": (
            "Setup blockers"
            if blocking_count > 1
            else top.get("area") or "Operator queue"
        ),
        "item": (
            queue.get("headline")
            if blocking_count > 1
            else top.get("item") or queue.get("headline") or "No queued operator item."
        ),
        "ticker": top.get("ticker"),
        "action": action,
        "evidence": top.get("evidence") or queue.get("headline") or "n/a",
        "source": top.get("source") or "operator_work_queue",
        "external_calls_made": 0,
    }


def market_radar_usefulness_payload(
    config: AppConfig,
    *,
    radar_run_summary: Mapping[str, object] | None = None,
    broker_summary: Mapping[str, object] | None = None,
    discovery_snapshot: Mapping[str, object] | None = None,
    candidate_rows: Sequence[Mapping[str, object]] | None = None,
    worker_status: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Summarize whether Market Radar is useful for scan, research, and decisions."""
    candidates = [row for row in candidate_rows or () if isinstance(row, Mapping)]
    snapshot = discovery_snapshot if isinstance(discovery_snapshot, Mapping) else {}
    worker = worker_status if isinstance(worker_status, Mapping) else {}
    source_modes = _mapping_value(snapshot, "source_modes")
    live_plan = live_activation_plan_payload(
        config,
        radar_run_summary=radar_run_summary,
        broker_summary=broker_summary,
    )
    coverage = {
        str(row.get("layer") or ""): row
        for row in data_source_coverage_payload(config, broker_summary=broker_summary)
    }
    actionability = actionability_breakdown_payload(candidates)
    investment = investment_readiness_payload(snapshot, actionability, candidates)
    queue = operator_work_queue_payload(
        config,
        radar_run_summary=radar_run_summary,
        broker_summary=broker_summary,
        discovery_snapshot=snapshot,
        candidate_rows=candidates,
    )
    steps = _radar_steps_by_name(radar_run_summary)
    llm_step = steps.get("llm_review", {})
    full_scan_market_blocker = _first_discovery_blocker_for_codes(
        snapshot,
        FULL_SCAN_MARKET_BLOCKER_CODES,
    )

    market_live = (
        source_modes.get("market") == "live" and source_modes.get("events") == "live"
    )
    market_scan_ready = market_live and not full_scan_market_blocker
    worker_state = str(worker.get("status") or "not_seen")
    decision_ready = bool(investment.get("manual_buy_review_ready"))
    research_available = bool(candidates) and str(investment.get("status") or "") in {
        "research_only",
        "ready",
        "monitor",
    }
    layers = [
        _usefulness_layer(
            "Automatic market scan",
            "ready" if market_scan_ready else "blocked",
            (
                "Market and catalyst sources are live."
                if market_scan_ready
                else str(
                    full_scan_market_blocker.get("finding")
                    if full_scan_market_blocker
                    else "Market Radar is still using fixture or incomplete live inputs."
                )
            ),
            (
                "Run one capped radar cycle and inspect rejected/provider counts."
                if market_scan_ready
                else str(full_scan_market_blocker.get("next_action"))
                if full_scan_market_blocker
                else str(
                    live_plan.get("next_action")
                    or "Configure live market and catalyst sources."
                )
            ),
            (
                f"market={source_modes.get('market') or 'unknown'}; "
                f"events={source_modes.get('events') or 'unknown'}"
            ),
        ),
        _usefulness_layer(
            "Agentic research loop",
            _agent_loop_usefulness_status(llm_step, coverage.get("LLM review", {})),
            _agent_loop_usefulness_current(llm_step, coverage.get("LLM review", {})),
            _agent_loop_usefulness_action(llm_step, coverage.get("LLM review", {})),
            _step_evidence("llm_review", llm_step) if llm_step else "no llm_review step",
        ),
        _usefulness_layer(
            "Worker automation",
            "ready" if worker_state in {"running", "idle"} else "blocked",
            str(worker.get("headline") or "No daily worker activity has been recorded."),
            str(
                worker.get("next_action")
                or "Start the worker after live input configuration is complete."
            ),
            str(worker.get("evidence") or "no worker evidence"),
        ),
        _usefulness_layer(
            "Investment decision support",
            "ready" if decision_ready else ("research" if research_available else "blocked"),
            str(
                investment.get("headline")
                or "Investment readiness has not been evaluated."
            ),
            str(investment.get("next_action") or "Review investment readiness."),
            str(investment.get("evidence") or "no investment readiness evidence"),
        ),
    ]
    ready_layers = sum(1 for row in layers if row["status"] == "ready")
    blocked_layers = sum(1 for row in layers if row["status"] == "blocked")
    research_layers = sum(1 for row in layers if row["status"] == "research")

    if decision_ready:
        status = "decision_ready"
        headline = "Market Radar is ready for manual investment review."
        next_action = str(
            investment.get("next_action")
            or "Open Decision Cards and verify exposure, source freshness, and hard blocks."
        )
    elif blocked_layers:
        status = "setup_blocked"
        headline = "Market Radar is not yet useful for live investment decisions."
        next_action = str(queue.get("next_action") or live_plan.get("next_action"))
    elif research_layers or research_available:
        status = "research_ready"
        headline = "Market Radar is useful for research triage, not buy decisions."
        next_action = str(
            investment.get("next_action") or "Work the research shortlist first."
        )
    else:
        status = "monitor"
        headline = "Market Radar has no actionable candidate insight yet."
        next_action = "Keep monitoring or run a fresh capped radar cycle."

    return {
        "schema_version": "market-radar-usefulness-v1",
        "status": status,
        "headline": headline,
        "next_action": next_action,
        "safe_to_make_investment_decision": decision_ready,
        "ready_layers": ready_layers,
        "total_layers": len(layers),
        "blocked_layers": blocked_layers,
        "research_layers": research_layers,
        "layers": layers,
        "evidence": (
            f"ready_layers={ready_layers}/{len(layers)}; "
            f"blocked_layers={blocked_layers}; "
            f"research_layers={research_layers}; "
            f"operator_queue={queue.get('status') or 'unknown'}; "
            f"live_plan={live_plan.get('status') or 'unknown'}"
        ),
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
            "incomplete_daily_bar_coverage",
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
    radar_run_summary: Mapping[str, object] | None = None,
    candidate_rows: Sequence[Mapping[str, object]] | None = None,
    broker_summary: Mapping[str, object] | None = None,
    ops_health: Mapping[str, object] | None = None,
    discovery_snapshot: Mapping[str, object] | None = None,
) -> dict[str, object]:
    radar_run_summary = (
        _row_dict(radar_run_summary)
        if isinstance(radar_run_summary, Mapping)
        else load_radar_run_summary(engine)
    )
    latest_run_cutoff = _parse_utc_datetime(radar_run_summary.get("decision_available_at"))
    candidate_rows = (
        [_row_dict(row) for row in candidate_rows]
        if candidate_rows is not None
        else load_radar_run_candidate_rows(engine, radar_run_summary)
    )
    broker_summary = (
        _row_dict(broker_summary)
        if isinstance(broker_summary, Mapping)
        else load_broker_summary(engine)
    )
    market_candidate_rows = candidate_rows_with_market_context(
        candidate_rows,
        _market_context_value(broker_summary),
    )
    ops_health = (
        _row_dict(ops_health) if isinstance(ops_health, Mapping) else load_ops_health(engine)
    )
    discovery_snapshot = (
        _row_dict(discovery_snapshot)
        if isinstance(discovery_snapshot, Mapping)
        else radar_discovery_snapshot_payload(
            engine,
            config,
            radar_run_summary=radar_run_summary,
            ops_health=ops_health,
        )
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
        candidate_rows=market_candidate_rows,
    )
    operator_queue = operator_work_queue_payload(
        config,
        radar_run_summary=radar_run_summary,
        broker_summary=broker_summary,
        discovery_snapshot=discovery_snapshot,
        candidate_rows=market_candidate_rows,
    )
    operator_next_step = operator_next_step_payload(operator_queue)
    usefulness = market_radar_usefulness_payload(
        config,
        radar_run_summary=radar_run_summary,
        broker_summary=broker_summary,
        discovery_snapshot=discovery_snapshot,
        candidate_rows=market_candidate_rows,
        worker_status=worker_status_payload(engine),
    )
    safe_to_decide = bool(investment.get("manual_buy_review_ready"))
    return {
        "schema_version": "radar-readiness-v1",
        "status": investment.get("status") or "unknown",
        "decision_mode": investment.get("decision_mode") or "unknown",
        "safe_to_make_investment_decision": safe_to_decide,
        "headline": investment.get("headline") or "Investment readiness unavailable.",
        "next_action": operator_next_step.get("action") or "Review readiness inputs.",
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
        "operator_next_step": operator_next_step,
        "market_radar_usefulness": usefulness,
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
        _market_context_value(broker_summary),
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
        market_context=_market_context_value(broker_summary),
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

    latest_candidate = candidate_rows_with_market_context(
        [latest_candidate],
        _market_context_value(load_broker_summary(engine)),
    )[0]
    security_metadata = _security_metadata_by_ticker(engine, [symbol]).get(symbol)
    signal_payload = signal_row.get("payload") if signal_row is not None else None
    packet_payload = packet_row.get("payload") if packet_row is not None else None
    card_payload = card_row.get("payload") if card_row is not None else None
    candidate_payload = _mapping_value(signal_payload, "candidate")
    candidate_metadata = _mapping_value(candidate_payload, "metadata")

    return {
        "ticker": symbol,
        "latest_candidate": latest_candidate,
        "priced_in_evidence_brief": _priced_in_evidence_brief(
            latest_candidate,
            events=event_rows,
            snippets=snippet_rows,
            packet_payload=packet_payload,
            security_metadata=security_metadata,
        ),
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
            candidate_states.c.ticker,
            candidate_states.c.as_of,
            candidate_states.c.state,
            candidate_states.c.final_score,
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
            values = row._mapping
            signal_payload = values.get("signal_payload")
            candidate_payload = (
                signal_payload.get("candidate")
                if isinstance(signal_payload, Mapping)
                else None
            )
            metadata = (
                candidate_payload.get("metadata")
                if isinstance(candidate_payload, Mapping)
                else {}
            )
            theme = _theme_name(metadata) or "unclassified"
            groups[theme].append(
                {
                    "ticker": values.get("ticker"),
                    "as_of": _as_utc_datetime_or_none(values.get("as_of")),
                    "state": values.get("state"),
                    "final_score": values.get("final_score"),
                }
            )

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


def load_agent_review_history(
    engine: Engine,
    *,
    available_at: datetime | None = None,
    ticker: str | None = None,
    task: str | None = None,
    status: str | None = None,
    limit: int = 50,
) -> dict[str, object]:
    cutoff = _as_utc_datetime_or_none(available_at)
    entries = BudgetLedgerRepository(engine).list_entries(
        available_at=cutoff or datetime.now(UTC),
        ticker=ticker,
        task=task,
        status=status,
        limit=limit,
    )
    return {
        "source": "budget_ledger",
        "schema_version": "agent-review-history-v1",
        "attempt_count": len(entries),
        "filters": {
            "available_at": cutoff.isoformat() if cutoff is not None else None,
            "ticker": ticker.upper() if ticker is not None and ticker.strip() else None,
            "task": task,
            "status": status,
            "limit": _positive_limit(limit),
        },
        "rows": [_budget_ledger_history_row(entry) for entry in entries],
    }


def agent_review_ledger_rows_payload(
    cost_summary: Mapping[str, object],
    ticker: object,
    *,
    task: str = "skeptic_review",
    limit: int = 5,
) -> list[dict[str, object]]:
    symbol = str(ticker or "").strip().upper()
    rows: list[dict[str, object]] = []
    for raw_row in _sequence_value(cost_summary.get("rows")):
        if not isinstance(raw_row, Mapping):
            continue
        row = _row_dict(raw_row)
        if symbol and str(row.get("ticker") or "").upper() != symbol:
            continue
        if task and str(row.get("task") or "") != task:
            continue
        rows.append(
            {
                "id": row.get("id"),
                "available_at": row.get("available_at"),
                "ticker": row.get("ticker"),
                "task": row.get("task"),
                "status": row.get("status"),
                "skip_reason": row.get("skip_reason"),
                "estimated_cost_usd": row.get("estimated_cost_usd"),
                "actual_cost_usd": row.get("actual_cost_usd"),
                "input_tokens": row.get("input_tokens"),
                "output_tokens": row.get("output_tokens"),
                "provider": row.get("provider"),
                "model": row.get("model"),
                "candidate_state": row.get("candidate_state"),
                "prompt_version": row.get("prompt_version"),
                "schema_version": row.get("schema_version"),
            }
        )
    return rows[: _positive_limit(limit)]


def agent_review_real_mode_gate_payload(config: AppConfig) -> dict[str, object]:
    missing = list(_llm_activation_missing_env(config))
    if int(config.llm_task_daily_caps.get("skeptic_review", 0)) <= 0:
        missing.append("CATALYST_LLM_TASK_DAILY_CAPS=skeptic_review=<low cap>")
    missing = list(dict.fromkeys(missing))
    ready = not missing
    return {
        "schema_version": "agent-review-real-mode-gate-v1",
        "status": "ready" if ready else "blocked",
        "headline": (
            "Real agent review is configured."
            if ready
            else "Real agent review is disabled until OpenAI review guardrails are set."
        ),
        "next_action": (
            "Run one real review only after dry-run evidence and call budget look right."
            if ready
            else "Set OpenAI provider, model, pricing, budgets, and a skeptic_review task cap."
        ),
        "missing_env": missing,
        "call_budget": (
            f"daily_budget={config.llm_daily_budget_usd}; "
            f"monthly_budget={config.llm_monthly_budget_usd}; "
            f"skeptic_review_cap={config.llm_task_daily_caps.get('skeptic_review')}"
            if ready
            else "0 OpenAI calls while blocked"
        ),
        "provider": _provider_name(config.llm_provider, default="none"),
        "model_configured": bool(config.llm_skeptic_model),
        "pricing_configured": _llm_pricing_configured(config),
        "budgets_configured": (
            config.llm_daily_budget_usd > 0 and config.llm_monthly_budget_usd > 0
        ),
        "task_cap_configured": (
            int(config.llm_task_daily_caps.get("skeptic_review", 0)) > 0
        ),
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


def radar_run_effective_status(
    run_payload: Mapping[str, object],
    *,
    fallback_status: object = None,
) -> str:
    status = str(run_payload.get("status") or fallback_status or "unknown").strip()
    status = status or "unknown"
    if status in {"running", "failed"}:
        return status
    steps = _radar_payload_step_rows(run_payload.get("steps"))
    if any(_radar_payload_step_blocks_reliance(step) for step in steps):
        return "partial_success"
    if any(str(step.get("status") or "") == "failed" for step in steps):
        return "partial_success"
    return status


def runtime_context_payload(
    config: AppConfig,
    *,
    radar_run_summary: Mapping[str, object] | None = None,
    dotenv_loaded: bool | None = None,
) -> dict[str, object]:
    """Return non-secret runtime context for dashboard/operator display."""
    summary = radar_run_summary if isinstance(radar_run_summary, Mapping) else {}
    run_path = _radar_run_path_summary(summary)
    database = _database_context_payload(config.database_url)
    build = build_info()
    return {
        "environment": config.environment,
        "env_file": ".env.local",
        "env_file_loaded": dotenv_loaded,
        "build": build,
        "database": database,
        "daily_market_provider": _provider_name(config.daily_market_provider, default="csv"),
        "daily_event_provider": _provider_name(
            config.daily_event_provider,
            default="news_fixture",
        ),
        "polygon_key_configured": config.polygon_api_key_configured,
        "sec_live_enabled": bool(config.sec_enable_live),
        "sec_user_agent_configured": config.sec_user_agent_configured,
        "openai_key_configured": bool(config.openai_api_key),
        "schwab_credentials_configured": bool(
            config.schwab_client_id
            and config.schwab_client_secret
            and config.schwab_redirect_uri
        ),
        "latest_run_as_of": summary.get("as_of"),
        "latest_run_cutoff": summary.get("decision_available_at")
        or summary.get("finished_at"),
        "run_path": run_path,
        "evidence": (
            f"build={build['commit']}; "
            f"db={database['name']}#{database['fingerprint']}; "
            f"providers={config.daily_market_provider or 'unset'}/"
            f"{config.daily_event_provider or 'unset'}; "
            f"required_path={run_path['required_complete']}/{run_path['required_total']}; "
            f"action_needed={run_path['blocking_count']}; "
            f"optional_gates={run_path['expected_gate_count']}"
        ),
    }


def radar_step_root_cause_rows(
    run_payload: Mapping[str, object],
    config: AppConfig,
) -> list[dict[str, object]]:
    rows = _radar_payload_step_rows(run_payload.get("steps"))
    grouped: dict[str, dict[str, object]] = {}
    for step in rows:
        reason = str(step.get("reason") or "")
        status = str(step.get("status") or "")
        classification = classify_step_outcome(status, reason or None)
        category = str(step.get("category") or classification.category)
        if category == "completed":
            continue
        group = _radar_step_root_cause_group(step, config)
        key = str(group["root_cause"])
        existing = grouped.setdefault(
            key,
            {
                **group,
                "affected_steps": [],
                "reasons": [],
            },
        )
        existing_steps = existing["affected_steps"]
        if isinstance(existing_steps, list):
            existing_steps.append(str(step.get("step") or step.get("name") or "unknown"))
        existing_reasons = existing["reasons"]
        if isinstance(existing_reasons, list) and reason:
            existing_reasons.append(reason)

    result: list[dict[str, object]] = []
    for row in grouped.values():
        steps = tuple(dict.fromkeys(str(value) for value in row.pop("affected_steps", [])))
        reasons = tuple(dict.fromkeys(str(value) for value in row.pop("reasons", [])))
        result.append(
            {
                "root_cause": row["root_cause"],
                "status": row["status"],
                "affected_steps": ", ".join(steps),
                "why": row["why"],
                "current_config": row["current_config"],
                "next_action": row["next_action"],
                "evidence": row["evidence"] if row["evidence"] != "n/a" else ", ".join(reasons),
            }
        )
    return sorted(
        result,
        key=lambda row: (
            _root_cause_rank(str(row.get("status") or "")),
            str(row.get("root_cause") or ""),
        ),
    )


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
        _scan_provider_alignment_call_plan_row(config, provider=provider),
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
        next_action = _call_plan_blocked_next_action(blocked_rows)
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


def _call_plan_blocked_next_action(rows: Sequence[Mapping[str, object]]) -> str:
    actions: list[str] = []
    seen: set[str] = set()
    for row in rows:
        action = str(row.get("next_action") or "").strip()
        if not action or action in seen:
            continue
        seen.add(action)
        actions.append(action.rstrip("."))
        if len(actions) >= 3:
            break
    if len(rows) > 1 and actions:
        return f"Clear {len(rows)} blocked call-plan rows: {'; '.join(actions)}."
    if actions:
        return f"{actions[0]}."
    return "Review blocked call rows."


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
            "Keep CATALYST_DAILY_MARKET_PROVIDER=csv for SEC-only smoke, set "
            "CATALYST_DAILY_EVENT_PROVIDER=sec, CATALYST_SEC_ENABLE_LIVE=1, and "
            "CATALYST_SEC_USER_AGENT; add Polygon later for live market bars."
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
        next_action = _missing_env_next_action(missing_env)
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
                "Use CSV for SEC-only smoke; add Polygon later for fresh broad-market bars."
            ),
        ),
        _activation_task_row(
            "SEC catalyst feed",
            events,
            missing=event_missing_env,
            ready_modes={"live"},
            safe_next_action=(
                _sec_activation_next_action(config)
                if event_missing_env
                else "Keep SEC submissions capped per run and watch rejected_count."
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
        f"Set {'; '.join(missing_env)} in .env.local, restart services, "
        "then inspect the call plan."
        if missing_env
        else (
            "Inspect the call plan, skip Polygon seeding unless configured, "
            "then run one capped cycle."
        )
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
        "dotenv_file": dotenv_activation_status_payload(config),
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


def dotenv_activation_status_payload(
    config: AppConfig,
    *,
    dotenv_path: str | Path = ".env.local",
) -> dict[str, object]:
    path = Path(dotenv_path)
    values: Mapping[str, object] = {}
    exists = path.is_file()
    updated_at: str | None = None
    if exists:
        from dotenv import dotenv_values

        values = dotenv_values(path)
        updated_at = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC).isoformat()

    specs = _dotenv_activation_specs(config)
    rows = [
        _dotenv_activation_row(config, values=values, key=key, required=required)
        for key, required in specs
    ]
    missing_rows = [row for row in rows if row["status"] == "missing"]
    restart_rows = [row for row in rows if row["status"] == "restart_required"]
    ready_rows = [row for row in rows if row["status"] == "loaded"]

    if not exists:
        status = "missing_file"
        headline = ".env.local was not found."
        next_action = "Create .env.local from the minimum block, then restart services."
    elif restart_rows:
        status = "restart_required"
        headline = ".env.local has values that are not loaded by the running process."
        next_action = "Restart the API, dashboard, and worker so .env.local is loaded."
    elif missing_rows:
        status = "missing_values"
        headline = ".env.local exists but required live activation values are missing."
        missing_keys = [str(row["key"]) for row in missing_rows]
        next_action = (
            f"Add {'; '.join(missing_keys)} to .env.local, then restart services."
        )
    else:
        status = "loaded"
        headline = ".env.local live activation values are loaded."
        next_action = "Inspect the call plan, then run one capped radar cycle."

    return {
        "schema_version": "dotenv-activation-status-v1",
        "status": status,
        "headline": headline,
        "next_action": next_action,
        "path": str(path),
        "exists": exists,
        "updated_at": updated_at,
        "required_count": len(specs),
        "loaded_count": len(ready_rows),
        "missing_count": len(missing_rows),
        "restart_required_count": len(restart_rows),
        "rows": rows,
        "evidence": (
            f"exists={'yes' if exists else 'no'}; "
            f"loaded={len(ready_rows)}/{len(specs)}; "
            f"missing={len(missing_rows)}; "
            f"restart_required={len(restart_rows)}"
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
    tape_status = _telemetry_tape_status(rows)
    return {
        "status": tape_status["status"],
        "headline": tape_status["headline"],
        "next_action": tape_status["next_action"],
        "attention_count": tape_status["attention_count"],
        "guarded_count": tape_status["guarded_count"],
        "evidence": tape_status["evidence"],
        "event_count": int(_finite_float(telemetry.get("event_count"))),
        "latest_event_at": telemetry.get("latest_event_at"),
        "status_counts": status_counts,
        "events": rows,
        "rollup": _telemetry_rollup_rows(rows),
    }


def telemetry_coverage_payload(
    engine: Engine,
    *,
    now: datetime | None = None,
) -> dict[str, object]:
    resolved_now = _as_utc_datetime_or_none(now) or datetime.now(UTC)
    filters = [
        audit_events.c.event_type.like("telemetry.%"),
        audit_events.c.occurred_at <= resolved_now,
    ]
    with engine.connect() as conn:
        summary = conn.execute(
            select(
                func.count(audit_events.c.id).label("event_count"),
                func.max(audit_events.c.occurred_at).label("latest_event_at"),
            ).where(*filters)
        ).mappings().first()
        event_rows = list(conn.execute(
            select(
                audit_events.c.event_type,
                func.count(audit_events.c.id).label("event_count"),
                func.max(audit_events.c.occurred_at).label("last_seen_at"),
            )
            .where(*filters)
            .group_by(audit_events.c.event_type)
        ).mappings())
        status_rows = list(conn.execute(
            select(
                audit_events.c.status,
                func.count(audit_events.c.id).label("event_count"),
            )
            .where(*filters)
            .group_by(audit_events.c.status)
        ).mappings())
        actor_rows = list(conn.execute(
            select(
                audit_events.c.actor_source,
                func.count(audit_events.c.id).label("event_count"),
            )
            .where(*filters)
            .group_by(audit_events.c.actor_source)
        ).mappings())
        artifact_rows = list(conn.execute(
            select(
                audit_events.c.artifact_type,
                func.count(audit_events.c.id).label("event_count"),
            )
            .where(*filters)
            .group_by(audit_events.c.artifact_type)
        ).mappings())
        recent_rows = [
            _row_dict(row._mapping)
            for row in conn.execute(
                select(
                    audit_events.c.event_type,
                    audit_events.c.status,
                    audit_events.c.reason,
                    audit_events.c.actor_source,
                    audit_events.c.artifact_type,
                    audit_events.c.artifact_id,
                    audit_events.c.ticker,
                    audit_events.c.occurred_at,
                )
                .where(*filters)
                .order_by(
                    audit_events.c.occurred_at.desc(),
                    audit_events.c.created_at.desc(),
                    audit_events.c.id.desc(),
                )
                .limit(8)
            )
        ]

    event_counts: dict[str, int] = {}
    last_seen_by_type: dict[str, datetime] = {}
    for row in event_rows:
        event_type = str(row.get("event_type") or "unknown")
        event_counts[event_type] = int(_finite_float(row.get("event_count")))
        last_seen = _parse_utc_datetime(row.get("last_seen_at"))
        if last_seen is not None:
            last_seen_by_type[event_type] = last_seen

    status_counts = {
        str(row.get("status") or "unknown"): int(_finite_float(row.get("event_count")))
        for row in status_rows
    }
    actor_source_counts = {
        str(row.get("actor_source") or "unknown"): int(
            _finite_float(row.get("event_count"))
        )
        for row in actor_rows
    }
    artifact_type_counts = {
        str(row.get("artifact_type") or "unknown"): int(
            _finite_float(row.get("event_count"))
        )
        for row in artifact_rows
    }

    radar_terminals = [
        "telemetry.radar_run.completed",
        "telemetry.radar_run.rejected",
        "telemetry.radar_run.error",
        "telemetry.radar_run.lock_contention",
        "telemetry.radar_run.rate_limited",
    ]
    universe_terminals = [
        "telemetry.universe_seed.completed",
        "telemetry.universe_seed.rejected",
        "telemetry.universe_seed.rate_limited",
    ]
    operator_events = sorted(
        event_type
        for event_type in event_counts
        if event_type.startswith("telemetry.operator.")
    )
    domains = [
        _telemetry_coverage_domain(
            name="Audit event store",
            required=True,
            status="ready" if event_counts else "missing",
            event_counts=event_counts,
            last_seen_by_type=last_seen_by_type,
            event_types=sorted(event_counts),
            missing_events=[] if event_counts else ["telemetry.*"],
            ready_action="Use the coverage panel and raw export as the audit source.",
            missing_action="Run a radar cycle to create the first telemetry event.",
        ),
        _telemetry_pair_domain(
            name="Radar run lifecycle",
            required=True,
            event_counts=event_counts,
            last_seen_by_type=last_seen_by_type,
            requested="telemetry.radar_run.requested",
            terminals=radar_terminals,
            ready_action="Radar run request and terminal-state telemetry are present.",
            missing_action="Start one capped radar run from the dashboard or API.",
        ),
        _telemetry_pair_domain(
            name="Radar run step telemetry",
            required=True,
            event_counts=event_counts,
            last_seen_by_type=last_seen_by_type,
            requested="telemetry.radar_run.step_started",
            terminals=["telemetry.radar_run.step_finished"],
            ready_action="Step start and finish events are present for run diagnosis.",
            missing_action=(
                "Run a radar cycle that reaches the worker path, then review skipped "
                "step reasons."
            ),
        ),
        _telemetry_pair_domain(
            name="Universe seed lifecycle",
            required=False,
            event_counts=event_counts,
            last_seen_by_type=last_seen_by_type,
            requested="telemetry.universe_seed.requested",
            terminals=universe_terminals,
            ready_action="Universe seeding request and terminal telemetry are present.",
            missing_action="Optional until you seed or refresh the market universe.",
        ),
        _telemetry_coverage_domain(
            name="Interactive dashboard actions",
            required=False,
            status="ready" if operator_events else "waiting",
            event_counts=event_counts,
            last_seen_by_type=last_seen_by_type,
            event_types=operator_events,
            missing_events=[] if operator_events else ["telemetry.operator.*"],
            ready_action="Operator action telemetry is present.",
            missing_action="Optional until you save an action, trigger, or order preview.",
        ),
    ]
    missing_required = [
        row
        for row in domains
        if row["required"] and row["status"] in {"missing", "attention"}
    ]
    attention_domains = [row for row in domains if row["status"] == "attention"]
    ready_domains = [row for row in domains if row["status"] == "ready"]
    total_event_count = int(_finite_float(summary.get("event_count") if summary else 0))
    latest_event_at = _iso_or_none(
        _parse_utc_datetime(summary.get("latest_event_at") if summary else None)
    )
    required_total = sum(1 for row in domains if row["required"])
    required_ready = sum(
        1 for row in domains if row["required"] and row["status"] == "ready"
    )
    if total_event_count == 0:
        status = "missing"
        headline = "Telemetry has not recorded any events yet."
        next_action = "Run one capped radar cycle before relying on operational status."
    elif missing_required or attention_domains:
        status = "attention"
        headline = "Telemetry coverage is incomplete."
        next_action = "Resolve required coverage gaps before relying on run diagnostics."
    else:
        status = "ready"
        headline = "Telemetry covers the core radar run path."
        next_action = "Use raw telemetry export when you need audit evidence."

    return {
        "schema_version": "ops-telemetry-coverage-v1",
        "external_calls_made": 0,
        "status": status,
        "headline": headline,
        "next_action": next_action,
        "generated_at": resolved_now.isoformat(),
        "total_event_count": total_event_count,
        "latest_event_at": latest_event_at,
        "required_domain_count": required_total,
        "ready_required_domain_count": required_ready,
        "ready_domain_count": len(ready_domains),
        "attention_domain_count": len(attention_domains),
        "missing_required_count": len(missing_required),
        "event_counts": event_counts,
        "status_counts": status_counts,
        "actor_source_counts": actor_source_counts,
        "artifact_type_counts": artifact_type_counts,
        "domains": domains,
        "recent_events": [
            {
                **row,
                "event": str(row.get("event_type") or "").removeprefix("telemetry."),
            }
            for row in recent_rows
        ],
        "evidence": (
            f"events={total_event_count}; required_ready={required_ready}/"
            f"{required_total}; missing_required={len(missing_required)}; "
            f"attention_domains={len(attention_domains)}; provider_calls=0"
        ),
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
            f"{config.polygon_tickers_max_pages} --confirm-external-call` "
            "before relying on broad discovery."
            if status in {"blocked", "thin", "partial"}
            else "Monitor daily-bar coverage and rejected provider records after each run."
        ),
        "evidence": (
            f"active={active_count}; with_daily_bars={with_bars_count}; "
            f"target={target_count}; "
            f"latest_daily_bar={database.get('latest_daily_bar_date') or 'n/a'}"
        ),
    }


def _daily_bar_coverage_for_date(
    engine: Engine,
    *,
    as_of_date: date | None,
    available_at: datetime | None,
    limit: int = 12,
) -> dict[str, object]:
    if as_of_date is None:
        return {
            "active_security_count": None,
            "with_as_of_bar_count": None,
            "missing_count": None,
            "missing_tickers": [],
        }

    cutoff = _as_utc_datetime_or_none(available_at)
    active_tickers = select(securities.c.ticker).where(securities.c.is_active.is_(True))
    bar_tickers = select(daily_bars.c.ticker).where(
        daily_bars.c.date == as_of_date,
        daily_bars.c.ticker.in_(active_tickers),
    )
    bar_count_filter = [
        daily_bars.c.date == as_of_date,
        daily_bars.c.ticker.in_(active_tickers),
    ]
    if cutoff is not None:
        bar_tickers = bar_tickers.where(daily_bars.c.available_at <= cutoff)
        bar_count_filter.append(daily_bars.c.available_at <= cutoff)

    with engine.connect() as conn:
        active_count = int(
            conn.scalar(
                select(func.count())
                .select_from(securities)
                .where(securities.c.is_active.is_(True))
            )
            or 0
        )
        with_as_of_bar_count = int(
            conn.scalar(
                select(func.count(func.distinct(daily_bars.c.ticker))).where(
                    *bar_count_filter
                )
            )
            or 0
        )
        missing_tickers = [
            str(row[0])
            for row in conn.execute(
                select(securities.c.ticker)
                .where(
                    securities.c.is_active.is_(True),
                    ~securities.c.ticker.in_(bar_tickers),
                )
                .order_by(securities.c.ticker.asc())
                .limit(limit)
            )
        ]

    return {
        "active_security_count": active_count,
        "with_as_of_bar_count": with_as_of_bar_count,
        "missing_count": max(active_count - with_as_of_bar_count, 0),
        "missing_tickers": missing_tickers,
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
        else load_ops_health(engine, now=artifact_cutoff or cutoff)
    )
    if candidate_rows is not None:
        run_candidate_rows = [
            _shallow_row_dict(row)
            for row in candidate_rows
            if isinstance(row, Mapping)
        ]
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
    as_of_bar_coverage = _daily_bar_coverage_for_date(
        engine,
        as_of_date=as_of_date,
        available_at=artifact_cutoff,
    )
    latest_candidate_at = _latest_candidate_as_of(context_candidates)
    latest_candidate_session_date = _date_iso_or_none(latest_candidate_at)

    candidate_count = len(candidates)
    scanned_candidate_count = _step_metric(
        steps,
        "feature_scan",
        "normalized_count",
        default=candidate_count,
    )
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
        as_of_bar_coverage=as_of_bar_coverage,
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
            "active_security_count": as_of_bar_coverage.get("active_security_count"),
            "active_security_with_as_of_bar_count": as_of_bar_coverage.get(
                "with_as_of_bar_count"
            ),
            "missing_as_of_daily_bar_count": as_of_bar_coverage.get("missing_count"),
            "missing_as_of_daily_bar_tickers": as_of_bar_coverage.get(
                "missing_tickers",
                [],
            ),
            "latest_candidate_as_of": _iso_or_none(latest_candidate_at),
            "latest_candidate_session_date": latest_candidate_session_date,
            "latest_candidate_age_days": _age_days(cutoff, latest_candidate_at),
        },
        "yield": {
            "requested_securities": requested_count,
            "scanned_securities": scanned_count,
            "candidate_states": candidate_count,
            "scanned_candidate_states": scanned_candidate_count,
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
    run_summary = _row_dict(radar_run_summary) if isinstance(radar_run_summary, Mapping) else {}
    run_as_of = _parse_date(run_summary.get("as_of"))
    steps = _radar_steps_by_name(radar_run_summary)
    rows: list[dict[str, object]] = []

    market = coverage.get("Market data", {})
    market_mode = str(market.get("mode") or "unknown")
    if market.get("provider") == "polygon" and not config.polygon_api_key_configured:
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
                "Market scan is using local CSV/fixture bars, not fresh market coverage.",
                _csv_market_refresh_next_action(run_as_of),
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
                _sec_activation_next_action(config),
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


def _candidate_row(row: Any, *, include_briefs: bool = True) -> dict[str, object]:
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
    features_payload = candidate_payload.get("features", {})
    if not isinstance(features_payload, dict):
        features_payload = {}
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
    for feature_key in (
        "ret_5d",
        "ret_20d",
        "rs_20_sector",
        "rs_60_spy",
        "rel_volume_5d",
        "dollar_volume_z",
        "extension_20d",
        "liquidity_score",
    ):
        values[feature_key] = features_payload.get(feature_key)
    priced_in = candidate_metadata.get("priced_in")
    if not isinstance(priced_in, Mapping):
        priced_in = _priced_in_from_candidate_payload(
            candidate_payload,
            candidate_metadata,
            hard_blocks=_sequence_value(values.get("hard_blocks")),
        )
    values["priced_in"] = _json_safe(priced_in)
    values["priced_in_status"] = _first_present(
        priced_in.get("status"),
        candidate_metadata.get("priced_in_status"),
    )
    values["priced_in_direction"] = _first_present(
        priced_in.get("direction"),
        candidate_metadata.get("priced_in_direction"),
    )
    values["priced_in_score"] = _first_present(
        priced_in.get("priced_in_score"),
        candidate_metadata.get("priced_in_score"),
    )
    values["emotion_score"] = _first_present(
        priced_in.get("emotion_score"),
        candidate_metadata.get("emotion_score"),
    )
    values["reaction_score"] = _first_present(
        priced_in.get("reaction_score"),
        candidate_metadata.get("reaction_score"),
    )
    values["emotion_reaction_gap"] = _first_present(
        priced_in.get("emotion_reaction_gap"),
        candidate_metadata.get("emotion_reaction_gap"),
    )
    values["priced_in_reason"] = _first_present(
        priced_in.get("reason"),
        candidate_metadata.get("priced_in_reason"),
    )
    values["priced_in_next_step"] = _first_present(
        priced_in.get("next_step"),
        candidate_metadata.get("priced_in_next_step"),
    )
    values["priced_in_data_sources"] = _priced_in_data_sources(values)
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
    if include_briefs:
        values["research_brief"] = _candidate_research_brief(values, packet_payload)
        values["priced_in_evidence_brief"] = _priced_in_evidence_brief(
            values,
            packet_payload=packet_payload,
        )
    return values


def _priced_in_from_candidate_payload(
    candidate_payload: Mapping[str, object],
    candidate_metadata: Mapping[str, object],
    *,
    hard_blocks: Sequence[object],
) -> dict[str, object]:
    features_payload = candidate_payload.get("features")
    if not isinstance(features_payload, Mapping):
        return {}
    as_of = _parse_utc_datetime(features_payload.get("as_of")) or _parse_utc_datetime(
        candidate_payload.get("as_of")
    )
    if as_of is None:
        return {}
    try:
        features = MarketFeatures(
            ticker=str(
                features_payload.get("ticker")
                or candidate_payload.get("ticker")
                or ""
            ).upper(),
            as_of=as_of,
            ret_5d=_finite_float(features_payload.get("ret_5d")),
            ret_20d=_finite_float(features_payload.get("ret_20d")),
            rs_20_sector=_finite_float(features_payload.get("rs_20_sector")),
            rs_60_spy=_finite_float(features_payload.get("rs_60_spy")),
            near_52w_high=_finite_float(features_payload.get("near_52w_high")),
            ma_regime=_finite_float(features_payload.get("ma_regime")),
            rel_volume_5d=_finite_float(features_payload.get("rel_volume_5d")),
            dollar_volume_z=_finite_float(features_payload.get("dollar_volume_z")),
            atr_pct=_finite_float(features_payload.get("atr_pct")),
            extension_20d=_finite_float(features_payload.get("extension_20d")),
            liquidity_score=_finite_float(features_payload.get("liquidity_score")),
            feature_version=str(features_payload.get("feature_version") or "unknown"),
        )
    except (TypeError, ValueError):
        return {}
    result = evaluate_priced_in(
        features,
        candidate_metadata,
        data_stale=bool(candidate_payload.get("data_stale")),
        hard_blocks=tuple(str(item) for item in hard_blocks if item not in (None, "")),
    )
    return result.as_payload()


def _priced_in_data_sources(row: Mapping[str, object]) -> dict[str, object]:
    available: list[str] = []
    missing: list[str] = []
    stale: list[str] = []

    data_stale = str(row.get("priced_in_status") or "").lower() == "stale" or (
        "data_stale" in _sequence_value(row.get("hard_blocks"))
    )
    if row.get("reaction_score") not in (None, ""):
        if data_stale:
            stale.append("market_bars")
        else:
            available.append("market_bars")
    else:
        missing.append("market_bars")

    if _finite_float(row.get("material_event_count")) > 0 or row.get("top_event_title"):
        available.append("catalyst_events")
    else:
        missing.append("catalyst_events")

    if (
        _finite_float(row.get("local_narrative_score")) > 0
        or _finite_float(row.get("selected_snippet_count")) > 0
    ):
        available.append("local_text")
    else:
        missing.append("local_text")

    if (
        row.get("options_feature_version")
        or _finite_float(row.get("options_flow_score")) > 0
        or _finite_float(row.get("call_put_ratio")) > 0
    ):
        available.append("options")
    else:
        missing.append("options")

    if (
        row.get("candidate_theme")
        or _finite_float(row.get("theme_velocity_score")) > 0
        or _finite_float(row.get("peer_readthrough_score")) > 0
        or _finite_float(row.get("sector_rotation_score")) > 0
    ):
        available.append("theme_peer_sector")
    else:
        missing.append("theme_peer_sector")

    if str(row.get("schwab_context_status") or "").lower() == "available":
        available.append("broker_context")
    else:
        missing.append("broker_context")

    parts = []
    if available:
        parts.append(f"available: {', '.join(available)}")
    if stale:
        parts.append(f"stale: {', '.join(stale)}")
    if missing:
        parts.append(f"missing: {', '.join(missing)}")
    return {
        "available": available,
        "stale": stale,
        "missing": missing,
        "summary": "; ".join(parts) if parts else "no source coverage",
    }


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


def _priced_in_evidence_brief(
    candidate: Mapping[str, object],
    *,
    events: Sequence[Mapping[str, object]] = (),
    snippets: Sequence[Mapping[str, object]] = (),
    packet_payload: Mapping[str, object] | None = None,
    security_metadata: Mapping[str, object] | None = None,
) -> dict[str, object]:
    packet = packet_payload if isinstance(packet_payload, Mapping) else {}
    blockers = _priced_in_row_blockers(candidate)
    data_sources = _priced_in_row_source_payload(candidate)
    instrument = _priced_in_row_instrument_payload(security_metadata)
    non_company_evidence = _priced_in_non_company_evidence_payload(
        candidate,
        instrument=instrument,
        security_metadata=security_metadata,
    )
    source_actions = _priced_in_source_actions_from_payload(
        data_sources,
        ticker=_priced_in_action_ticker(candidate),
    )
    usefulness = _priced_in_usefulness_verdict(
        candidate,
        blockers=blockers,
        data_sources=data_sources,
        source_actions=source_actions,
        instrument=instrument,
    )
    evidence = _priced_in_brief_evidence(
        candidate,
        events=events,
        snippets=snippets,
        packet_payload=packet,
    )
    status = str(candidate.get("priced_in_status") or "unknown").strip() or "unknown"
    return {
        "schema_version": "priced-in-evidence-brief-v1",
        "ticker": candidate.get("ticker"),
        "status": status,
        "direction": candidate.get("priced_in_direction") or "n/a",
        "emotion_score": candidate.get("emotion_score"),
        "reaction_score": candidate.get("reaction_score"),
        "emotion_reaction_gap": candidate.get("emotion_reaction_gap"),
        "priced_in_score": candidate.get("priced_in_score"),
        "blocked": bool(blockers),
        "blockers": blockers,
        "why_now": _display_priced_in_reason(candidate)
        or _research_why_now(candidate, top_event=candidate.get("top_event_title")),
        "top_catalyst": candidate.get("top_event_title")
        or _mapping_value(candidate, "top_supporting_evidence").get("title"),
        "source": candidate.get("top_event_source"),
        "source_url": candidate.get("top_event_source_url"),
        "instrument": instrument,
        **(
            {"non_company_evidence": non_company_evidence}
            if non_company_evidence
            else {}
        ),
        "data_sources": data_sources,
        "source_actions": source_actions,
        "usefulness": usefulness,
        "evidence": evidence,
        "next_step": _priced_in_brief_next_step(
            candidate,
            blockers,
            usefulness=usefulness,
        ),
    }


def _priced_in_brief_evidence(
    candidate: Mapping[str, object],
    *,
    events: Sequence[Mapping[str, object]],
    snippets: Sequence[Mapping[str, object]],
    packet_payload: Mapping[str, object],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for item in _sequence_value(packet_payload.get("supporting_evidence"))[:3]:
        if isinstance(item, Mapping):
            rows.append(
                {
                    "kind": item.get("kind") or "support",
                    "title": item.get("title") or item.get("summary"),
                    "source": item.get("source_id") or item.get("source"),
                    "source_url": item.get("source_url"),
                    "strength": item.get("strength"),
                }
            )
    for event in events[:3]:
        rows.append(
            {
                "kind": event.get("event_type") or "event",
                "title": event.get("title"),
                "source": event.get("source"),
                "source_url": event.get("source_url"),
                "strength": event.get("materiality"),
            }
        )
    for snippet in snippets[:2]:
        rows.append(
            {
                "kind": "local_text",
                "title": snippet.get("text"),
                "source": snippet.get("source"),
                "source_url": snippet.get("source_url"),
                "strength": snippet.get("materiality"),
            }
        )
    if not rows:
        support = _mapping_value(candidate, "top_supporting_evidence")
        title = support.get("title") or candidate.get("top_event_title")
        if title:
            rows.append(
                {
                    "kind": support.get("kind") or candidate.get("top_event_type") or "signal",
                    "title": title,
                    "source": support.get("source_id") or candidate.get("top_event_source"),
                    "source_url": support.get("source_url")
                    or candidate.get("top_event_source_url"),
                    "strength": support.get("strength"),
                }
            )
    return [row for row in rows if row.get("title")][:5]


def _priced_in_brief_next_step(
    candidate: Mapping[str, object],
    blockers: Sequence[str],
    *,
    usefulness: Mapping[str, object],
) -> str:
    if blockers:
        return "Clear blockers before treating this mismatch as actionable."
    if usefulness.get("status") in {"research_useful", "not_useful"}:
        next_action = str(usefulness.get("next_action") or "").strip()
        if next_action:
            return next_action
    return str(
        candidate.get("priced_in_next_step")
        or _mapping_value(candidate, "research_brief").get("next_step")
        or "Open candidate detail and verify evidence before acting."
    )


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
    priced_next_step = (
        str(candidate.get("priced_in_next_step") or "").strip()
        if _display_priced_in_reason(candidate)
        else ""
    )
    if priced_next_step:
        return priced_next_step
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


def _priced_in_queue_row(
    row: Mapping[str, object],
    *,
    security_metadata: Mapping[str, object] | None = None,
) -> dict[str, object]:
    brief = _mapping_value(row, "research_brief")
    status = str(row.get("priced_in_status") or "unknown").strip() or "unknown"
    blockers = _priced_in_row_blockers(row)
    data_sources = _priced_in_row_source_payload(row)
    instrument = _priced_in_row_instrument_payload(security_metadata)
    non_company_evidence = _priced_in_non_company_evidence_payload(
        row,
        instrument=instrument,
        security_metadata=security_metadata,
    )
    source_gaps = _priced_in_source_gap_names_from_payload(data_sources)
    usefulness = _priced_in_usefulness_verdict(
        row,
        blockers=blockers,
        data_sources=data_sources,
        source_gaps=source_gaps,
        instrument=instrument,
    )
    reason = str(
        _display_priced_in_reason(row)
        or brief.get("why_now")
        or row.get("top_event_title")
        or ""
    ).strip()
    next_step = (
        (_display_priced_in_reason(row) and row.get("priced_in_next_step"))
        or brief.get("next_step")
        or "Open candidate detail and review the evidence."
    )
    if blockers:
        next_step = "Clear blockers before treating this mismatch as actionable."
    elif usefulness.get("status") in {
        "research_useful",
        "decision_useful",
        "not_useful",
    }:
        next_step = str(usefulness.get("next_action") or next_step)
    return {
        "ticker": row.get("ticker"),
        "as_of": row.get("as_of"),
        "available_at": row.get("created_at"),
        "priced_in_status": status,
        "priced_in_direction": row.get("priced_in_direction"),
        "emotion_score": row.get("emotion_score"),
        "reaction_score": row.get("reaction_score"),
        "emotion_reaction_gap": row.get("emotion_reaction_gap"),
        "priced_in_score": row.get("priced_in_score"),
        "state": row.get("state"),
        "blocked": bool(blockers),
        "blockers": blockers,
        "score": _finite_float(row.get("final_score")),
        "setup": row.get("setup_type") or row.get("candidate_theme") or "n/a",
        "top_catalyst": brief.get("top_catalyst") or row.get("top_event_title"),
        "why_now": reason or "No priced-in reason is available.",
        "instrument": instrument,
        **(
            {"non_company_evidence": non_company_evidence}
            if non_company_evidence
            else {}
        ),
        "data_sources": data_sources,
        "usefulness": usefulness,
        "next_step": next_step,
        "source": brief.get("source") or row.get("top_event_source"),
        "source_url": brief.get("source_url") or row.get("top_event_source_url"),
        "data_stale": status.lower() == "stale" or "data_stale" in _sequence_value(
            row.get("hard_blocks")
        ),
    }


def _priced_in_row_blockers(row: Mapping[str, object]) -> list[str]:
    blockers = {
        str(item).strip()
        for key in ("hard_blocks", "portfolio_hard_blocks")
        for item in _sequence_value(row.get(key))
        if str(item).strip()
    }
    if str(row.get("state") or "").strip().lower() == "blocked" and not blockers:
        blockers.add("policy_blocked")
    return sorted(blockers)


def _priced_in_row_is_stock_like(row: Mapping[str, object]) -> bool:
    instrument = _mapping_value(row, "instrument")
    return str(instrument.get("category") or "").strip().lower() == "company_like"


def _priced_in_row_source_payload(row: Mapping[str, object]) -> dict[str, object]:
    for key in ("data_sources", "priced_in_data_sources"):
        value = row.get(key)
        if isinstance(value, Mapping):
            return _priced_in_source_payload_with_runtime_context(row, _row_dict(value))
    return _priced_in_source_payload_with_runtime_context(row, _priced_in_data_sources(row))


def _priced_in_source_payload_with_runtime_context(
    row: Mapping[str, object],
    payload: Mapping[str, object],
) -> dict[str, object]:
    status = str(row.get("schwab_context_status") or "").strip().lower()
    if status == "available":
        return _priced_in_source_payload_set_source(payload, "broker_context", "available")
    if status == "missing":
        return _priced_in_source_payload_set_source(payload, "broker_context", "missing")
    return _row_dict(payload)


def _priced_in_source_payload_set_source(
    payload: Mapping[str, object],
    source: str,
    target_status: str,
) -> dict[str, object]:
    statuses = ("available", "stale", "missing")
    values = {
        status: [
            str(item)
            for item in _sequence_value(payload.get(status))
            if str(item).strip() and str(item) != source
        ]
        for status in statuses
    }
    if target_status in values:
        values[target_status].append(source)
    parts = []
    for status in statuses:
        if values[status]:
            parts.append(f"{status}: {', '.join(values[status])}")
    return {
        "available": values["available"],
        "stale": values["stale"],
        "missing": values["missing"],
        "summary": "; ".join(parts) if parts else "no source coverage",
    }


def _priced_in_row_instrument_payload(
    security_metadata: Mapping[str, object] | None,
) -> dict[str, object]:
    security_type = _security_type_for_scope(security_metadata)
    if _is_sec_company_like_type(security_type):
        category = "company_like"
        evidence_route = "company_catalyst_text"
        sec_catalyst_applicable = True
    elif security_type == "UNKNOWN":
        category = "unknown"
        evidence_route = "company_catalyst_text"
        sec_catalyst_applicable = True
    else:
        category = "non_company"
        evidence_route = "market_theme_fund_or_flow"
        sec_catalyst_applicable = False
    return {
        "schema_version": "priced-in-row-instrument-v1",
        "security_type": security_type,
        "category": category,
        "evidence_route": evidence_route,
        "sec_catalyst_applicable": sec_catalyst_applicable,
    }


def _priced_in_non_company_evidence_payload(
    row: Mapping[str, object],
    *,
    instrument: Mapping[str, object],
    security_metadata: Mapping[str, object] | None,
) -> dict[str, object]:
    if str(instrument.get("category") or "").strip().lower() != "non_company":
        return {}
    security = _row_dict(security_metadata) if isinstance(security_metadata, Mapping) else {}
    metadata = _mapping_value(security, "metadata")
    security_type = str(instrument.get("security_type") or "UNKNOWN").strip().upper()
    name = str(security.get("name") or row.get("ticker") or "").strip()
    ticker = _priced_in_action_ticker(row)
    checkpoints = [
        _non_company_instrument_identity_evidence(
            ticker=ticker,
            name=name,
            security_type=security_type,
            security=security,
            metadata=metadata,
        ),
        _non_company_market_reaction_evidence(row),
        _non_company_theme_sector_evidence(row, security),
        _non_company_flow_volume_evidence(row),
    ]
    wrapper_hint = _non_company_underlying_or_objective_evidence(
        name=name,
        security_type=security_type,
        metadata=metadata,
    )
    if wrapper_hint:
        checkpoints.append(wrapper_hint)
    missing_required = [
        str(item.get("kind"))
        for item in checkpoints
        if bool(item.get("required")) and item.get("status") == "missing"
    ]
    available_count = sum(
        1
        for item in checkpoints
        if str(item.get("status") or "") in {"available", "inferred"}
    )
    status = (
        "available"
        if not missing_required
        else "partial"
        if available_count
        else "missing"
    )
    return {
        "schema_version": "priced-in-non-company-evidence-v1",
        "status": status,
        "route": "market_theme_fund_or_flow",
        "instrument_type": security_type,
        "name": name or ticker,
        "summary": _non_company_evidence_summary(checkpoints),
        "missing_required": missing_required,
        "checkpoints": checkpoints,
        "external_calls_made": 0,
    }


def _non_company_instrument_identity_evidence(
    *,
    ticker: str,
    name: str,
    security_type: str,
    security: Mapping[str, object],
    metadata: Mapping[str, object],
) -> dict[str, object]:
    exchange = str(security.get("exchange") or "").strip()
    figi = str(metadata.get("composite_figi") or "").strip()
    title = f"{ticker} is {security_type}"
    if name:
        title = f"{ticker}: {name}"
    detail = (
        f"Instrument type {security_type}; use non-company evidence rather than "
        "SEC operating-company catalysts."
    )
    return {
        "kind": "instrument_identity",
        "status": "available" if name or security_type != "UNKNOWN" else "missing",
        "required": True,
        "title": title,
        "detail": detail,
        "exchange": exchange or None,
        "figi": figi or None,
    }


def _non_company_market_reaction_evidence(
    row: Mapping[str, object],
) -> dict[str, object]:
    has_reaction = row.get("reaction_score") not in (None, "")
    gap = _finite_float(row.get("emotion_reaction_gap"))
    emotion = _finite_float(row.get("emotion_score"))
    reaction = _finite_float(row.get("reaction_score"))
    return {
        "kind": "market_reaction",
        "status": "available" if has_reaction else "missing",
        "required": True,
        "title": f"emotion {emotion:g} vs reaction {reaction:g}",
        "detail": (
            f"Gap {gap:g}; positive means market emotion is ahead of observed "
            "price reaction."
        )
        if has_reaction
        else "Market reaction score is not stored for this row.",
        "emotion_score": emotion if has_reaction else None,
        "reaction_score": reaction if has_reaction else None,
        "emotion_reaction_gap": gap if has_reaction else None,
    }


def _non_company_theme_sector_evidence(
    row: Mapping[str, object],
    security: Mapping[str, object],
) -> dict[str, object]:
    theme = _meaningful_text(row.get("candidate_theme"))
    sector = str(security.get("sector") or "").strip()
    industry = str(security.get("industry") or "").strip()
    metrics = {
        "theme_velocity_score": _finite_float(row.get("theme_velocity_score")),
        "peer_readthrough_score": _finite_float(row.get("peer_readthrough_score")),
        "sector_rotation_score": _finite_float(row.get("sector_rotation_score")),
    }
    useful_sector = [
        value
        for value in (sector, industry)
        if value and value.strip().lower() not in {"unknown", "n/a", "none"}
    ]
    has_metric = any(value != 0.0 for value in metrics.values())
    available = bool(theme or useful_sector or has_metric)
    detail_parts = []
    if theme:
        detail_parts.append(f"theme={theme}")
    if useful_sector:
        detail_parts.append(" / ".join(useful_sector))
    if has_metric:
        detail_parts.append(
            "theme/peer/sector scores="
            f"{metrics['theme_velocity_score']:g}/"
            f"{metrics['peer_readthrough_score']:g}/"
            f"{metrics['sector_rotation_score']:g}"
        )
    return {
        "kind": "theme_sector_context",
        "status": "available" if available else "missing",
        "required": True,
        "title": "theme/sector context",
        "detail": "; ".join(detail_parts)
        if detail_parts
        else "No theme, sector, or peer context is stored for this row.",
        "candidate_theme": theme or None,
        "sector": sector or None,
        "industry": industry or None,
        **metrics,
    }


def _non_company_flow_volume_evidence(
    row: Mapping[str, object],
) -> dict[str, object]:
    metrics = {
        "rel_volume_5d": _nullable_float(row.get("rel_volume_5d")),
        "dollar_volume_z": _nullable_float(row.get("dollar_volume_z")),
        "ret_5d": _nullable_float(row.get("ret_5d")),
        "ret_20d": _nullable_float(row.get("ret_20d")),
        "rs_20_sector": _nullable_float(row.get("rs_20_sector")),
    }
    available_metrics = {
        key: value for key, value in metrics.items() if value is not None
    }
    return {
        "kind": "flow_volume_context",
        "status": "available" if available_metrics else "missing",
        "required": False,
        "title": "flow/volume context",
        "detail": (
            ", ".join(f"{key}={value:g}" for key, value in available_metrics.items())
            if available_metrics
            else "No stored flow/volume feature snapshot is available."
        ),
        **metrics,
    }


def _non_company_underlying_or_objective_evidence(
    *,
    name: str,
    security_type: str,
    metadata: Mapping[str, object],
) -> dict[str, object]:
    description = str(
        _first_present(
            metadata.get("description"),
            metadata.get("objective"),
            metadata.get("fund_description"),
        )
        or ""
    ).strip()
    if description:
        return {
            "kind": "fund_objective",
            "status": "available",
            "required": False,
            "title": "stored fund objective",
            "detail": description,
        }
    if security_type in PRICED_IN_WRAPPER_SECURITY_TYPES and name:
        return {
            "kind": "underlying_hint",
            "status": "inferred",
            "required": False,
            "title": "underlying hint from instrument name",
            "detail": _non_company_underlying_hint_from_name(name),
        }
    if security_type in PRICED_IN_FUND_LIKE_SECURITY_TYPES and name:
        return {
            "kind": "fund_objective",
            "status": "inferred",
            "required": False,
            "title": "fund objective hint from instrument name",
            "detail": name,
        }
    return {}


def _non_company_underlying_hint_from_name(name: str) -> str:
    cleaned = name
    for suffix in (
        "Warrants",
        "Warrant",
        "Rights",
        "Right",
        "Units",
        "Unit",
        "Preferred",
    ):
        cleaned = cleaned.replace(suffix, "").strip(" -,.")
    return cleaned or name


def _non_company_evidence_summary(
    checkpoints: Sequence[Mapping[str, object]],
) -> str:
    parts = []
    for item in checkpoints:
        if str(item.get("status") or "") == "missing":
            continue
        title = str(item.get("title") or "").strip()
        detail = str(item.get("detail") or "").strip()
        if title and detail:
            parts.append(f"{title}: {detail}")
        elif title:
            parts.append(title)
        if len(parts) >= 3:
            break
    return "; ".join(parts) if parts else "No non-company evidence is available."


def _nullable_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    return _finite_float(value)


def _meaningful_text(value: object) -> str:
    text = str(value or "").strip()
    if text.lower() in {"", "unknown", "n/a", "none", "null"}:
        return ""
    return text


def _priced_in_source_coverage_summary_text(
    sources: Mapping[str, Mapping[str, object]],
    row_count: int,
) -> str:
    if row_count <= 0:
        return "no priced-in rows"
    parts = []
    for source in PRICED_IN_SOURCE_CLASSES:
        values = sources.get(source, {})
        available = int(_finite_float(values.get("available")))
        stale = int(_finite_float(values.get("stale")))
        missing = int(_finite_float(values.get("missing")))
        denominator = int(_finite_float(values.get("row_count"))) or row_count
        detail = f"{source} {available}/{denominator}"
        extras = []
        if stale:
            extras.append(f"{stale} stale")
        if missing:
            extras.append(f"{missing} missing")
        routed = int(_finite_float(values.get("routed_non_company_gap_rows")))
        if routed:
            extras.append(f"{routed} non-company routed")
        if extras:
            detail = f"{detail} ({', '.join(extras)})"
        parts.append(detail)
    return "; ".join(parts)


def _priced_in_source_action_rows(
    sources: Mapping[str, Mapping[str, object]],
    row_count: int,
    *,
    stocks_only: bool = False,
) -> list[dict[str, object]]:
    return [
        _priced_in_source_action_row(
            source,
            sources.get(source, {}),
            row_count,
            stocks_only=stocks_only,
        )
        for source in PRICED_IN_SOURCE_CLASSES
    ]


def _append_priced_in_action_ticker(sample_tickers: list[str], ticker: str) -> None:
    if (
        not ticker
        or ticker == "<TICKER>"
        or ticker in sample_tickers
        or len(sample_tickers) >= PRICED_IN_SOURCE_ACTION_TICKER_LIMIT
    ):
        return
    sample_tickers.append(ticker)


def _priced_in_action_ticker(candidate: Mapping[str, object]) -> str:
    return str(candidate.get("ticker") or "").strip().upper()


def _priced_in_source_actions_from_payload(
    data_sources: Mapping[str, object],
    *,
    ticker: str | None = None,
) -> list[dict[str, object]]:
    available = {str(item) for item in _sequence_value(data_sources.get("available"))}
    stale = {str(item) for item in _sequence_value(data_sources.get("stale"))}
    missing = {str(item) for item in _sequence_value(data_sources.get("missing"))}
    ticker_samples = [ticker.strip().upper()] if ticker and ticker.strip() else []
    source_rows: dict[str, dict[str, object]] = {}
    for source in PRICED_IN_SOURCE_CLASSES:
        is_available = source in available
        is_stale = source in stale
        is_missing = source in missing or (not is_available and not is_stale)
        source_rows[source] = {
            "available": 1 if is_available else 0,
            "stale": 1 if is_stale else 0,
            "missing": 1 if is_missing else 0,
            "row_count": 1,
            "coverage_pct": 100.0 if is_available else 0.0,
            "sample_tickers": ticker_samples if is_stale or is_missing else [],
        }
    return _priced_in_source_action_rows(source_rows, 1)


def _priced_in_source_gap_names_from_payload(
    data_sources: Mapping[str, object],
) -> list[str]:
    available = {
        str(item)
        for item in _sequence_value(data_sources.get("available"))
        if str(item).strip()
    }
    stale = {
        str(item)
        for item in _sequence_value(data_sources.get("stale"))
        if str(item).strip()
    }
    return [
        source
        for source in PRICED_IN_SOURCE_CLASSES
        if source in stale or source not in available
    ]


def _priced_in_core_sources_for_instrument(
    instrument: Mapping[str, object],
) -> set[str]:
    if str(instrument.get("category") or "").strip().lower() == "non_company":
        return {"market_bars", "theme_peer_sector"}
    return {"market_bars", "catalyst_events", "local_text"}


def _priced_in_optional_context_sources_for_instrument(
    instrument: Mapping[str, object],
) -> set[str]:
    optional_sources = set(PRICED_IN_OPTIONAL_CONTEXT_SOURCES)
    if str(instrument.get("category") or "").strip().lower() == "non_company":
        optional_sources.update({"catalyst_events", "local_text"})
    return optional_sources


def _priced_in_usefulness_verdict(
    candidate: Mapping[str, object],
    *,
    blockers: Sequence[str],
    data_sources: Mapping[str, object],
    source_actions: Sequence[Mapping[str, object]] = (),
    source_gaps: Sequence[str] | None = None,
    instrument: Mapping[str, object] | None = None,
) -> dict[str, object]:
    status = str(candidate.get("priced_in_status") or "").strip().lower()
    instrument_payload = _row_dict(instrument) if isinstance(instrument, Mapping) else {}
    non_company_route = (
        str(instrument_payload.get("category") or "").strip().lower()
        == "non_company"
    )
    available = {
        str(item)
        for item in _sequence_value(data_sources.get("available"))
        if str(item).strip()
    }
    stale = {
        str(item)
        for item in _sequence_value(data_sources.get("stale"))
        if str(item).strip()
    }
    core_sources = _priced_in_core_sources_for_instrument(instrument_payload)
    optional_context_sources = _priced_in_optional_context_sources_for_instrument(
        instrument_payload
    )
    missing_core = sorted(source for source in core_sources if source not in available)
    stale_core = sorted(source for source in core_sources if source in stale)
    resolved_source_gaps = (
        [str(source) for source in source_gaps if str(source).strip()]
        if source_gaps is not None
        else [
            str(action.get("source"))
            for action in source_actions
            if str(action.get("status") or "") not in {"ready", "not_applicable"}
        ]
    )
    optional_context_gaps = sorted(
        dict.fromkeys(
            source
            for source in resolved_source_gaps
            if source in optional_context_sources
        )
    )
    missing_for_decision = [
        source
        for source in resolved_source_gaps
        if source not in optional_context_sources
    ]
    candidate_packet_id = str(candidate.get("candidate_packet_id") or "").strip()
    if not candidate_packet_id:
        missing_for_decision.append("candidate_packet")
    decision_card_id = str(candidate.get("decision_card_id") or "").strip()
    if not decision_card_id:
        missing_for_decision.append("decision_card")
    missing_for_decision = sorted(dict.fromkeys(missing_for_decision))
    reasons: list[str] = []
    if blockers:
        verdict = "blocked"
        label = "Blocked mismatch"
        reasons.append("Policy or portfolio blockers must be cleared first.")
        next_action = "Clear blockers before treating this mismatch as actionable."
        next_command = "catalyst-radar candidate-detail <TICKER>"
    elif status not in PRICED_IN_ACTIONABLE_STATUSES:
        verdict = "monitor_only"
        label = "Monitor only"
        reasons.append("No bullish or bearish not-priced-in mismatch is visible.")
        next_action = "Keep this in monitoring until the priced-in signal changes."
        next_command = "catalyst-radar priced-in-queue --status all"
    elif missing_core or stale_core:
        verdict = "not_useful"
        label = "Not useful yet"
        if missing_core:
            if non_company_route:
                reasons.append(
                    "Missing non-company route source(s): "
                    f"{', '.join(missing_core)}."
                )
            else:
                reasons.append(f"Missing core source(s): {', '.join(missing_core)}.")
        if stale_core:
            if non_company_route:
                reasons.append(
                    "Stale non-company route source(s): "
                    f"{', '.join(stale_core)}."
                )
            else:
                reasons.append(f"Stale core source(s): {', '.join(stale_core)}.")
        next_action = (
            "Add market bars plus theme, underlying, fund, or flow evidence before review."
            if non_company_route
            else "Refresh core market, catalyst, or text data before review."
        )
        next_command = "catalyst-radar priced-in-preflight"
    elif missing_for_decision:
        verdict = "research_useful"
        label = "Research-useful mismatch"
        reasons.append(
            "Non-company market and theme evidence route is available."
            if non_company_route
            else "Core emotion-versus-reaction evidence is available."
        )
        reasons.append(
            "Decision evidence still missing: "
            f"{', '.join(missing_for_decision)}."
        )
        if "candidate_packet" in missing_for_decision:
            next_action = (
                "Build a Candidate Packet before Decision Card review."
            )
            next_command = _priced_in_build_packet_command(candidate)
        elif "decision_card" in missing_for_decision:
            next_action = "Build or refresh the Decision Card before decision review."
            next_command = _priced_in_build_decision_card_command(candidate)
        else:
            next_action = "Open candidate detail, verify evidence, then fill decision gaps."
            next_command = f"catalyst-radar candidate-detail {_priced_in_command_ticker(candidate)}"
    else:
        verdict = "decision_useful"
        label = "Priced-in answer ready"
        reasons.append(
            "Non-company market/theme route and local review artifacts are available."
            if non_company_route
            else (
                "Core emotion-versus-reaction evidence and local review artifacts "
                "are available."
            )
        )
        if optional_context_gaps:
            reasons.append(
                "Optional context still missing: "
                f"{', '.join(optional_context_gaps)}."
            )
        next_action = "Review the priced-in evidence and optional source gaps."
        next_command = (
            "catalyst-radar decision-card "
            f"--ticker {_priced_in_command_ticker(candidate)} "
            f"--as-of {_priced_in_command_as_of(candidate)}"
        )
    return {
        "schema_version": "priced-in-usefulness-verdict-v1",
        "status": verdict,
        "label": label,
        "decision_ready": verdict == "decision_useful",
        "reasons": reasons,
        "missing_for_decision": missing_for_decision,
        "optional_context_gaps": optional_context_gaps,
        "core_sources": sorted(core_sources),
        "evidence_route": instrument_payload.get("evidence_route")
        or "company_catalyst_text",
        "routed_optional_sources": sorted(
            source
            for source in optional_context_gaps
            if non_company_route and source in {"catalyst_events", "local_text"}
        ),
        "next_action": next_action,
        "next_command": next_command,
        "action_boundary": (
            "Research signal only until source gaps, blockers, and Decision Card are clear."
        )
        if verdict != "decision_useful"
        else (
            "Priced-in answer is ready for human review; optional context gaps "
            "may remain and real order submission remains disabled."
        ),
    }


def _priced_in_build_packet_command(candidate: Mapping[str, object]) -> str:
    return (
        "catalyst-radar build-packets "
        f"--as-of {_priced_in_command_as_of(candidate)} "
        f"--ticker {_priced_in_command_ticker(candidate)} "
        "--min-state ResearchOnly"
    )


def _priced_in_build_decision_card_command(candidate: Mapping[str, object]) -> str:
    return (
        "catalyst-radar build-decision-cards "
        f"--as-of {_priced_in_command_as_of(candidate)} "
        f"--ticker {_priced_in_command_ticker(candidate)} "
        "--min-state ResearchOnly"
    )


def _priced_in_command_ticker(candidate: Mapping[str, object]) -> str:
    return str(candidate.get("ticker") or "<TICKER>").strip().upper() or "<TICKER>"


def _priced_in_command_as_of(candidate: Mapping[str, object]) -> str:
    as_of = _parse_utc_datetime(candidate.get("as_of"))
    if as_of is None:
        return "<LATEST_TRADING_DATE>"
    return as_of.date().isoformat()


def _priced_in_source_action_row(
    source: str,
    values: Mapping[str, object],
    row_count: int,
    *,
    stocks_only: bool = False,
) -> dict[str, object]:
    available = int(_finite_float(values.get("available")))
    stale = int(_finite_float(values.get("stale")))
    missing = int(_finite_float(values.get("missing")))
    source_row_count = int(_finite_float(values.get("row_count"))) or row_count
    coverage_pct = round(float(_finite_float(values.get("coverage_pct"))), 1)
    sample_tickers = [
        str(ticker).strip().upper()
        for ticker in _sequence_value(values.get("sample_tickers"))
        if str(ticker).strip()
    ][:PRICED_IN_SOURCE_ACTION_TICKER_LIMIT]
    gap_count = stale + missing
    if source_row_count <= 0:
        status = "not_applicable"
    elif available == source_row_count and stale == 0 and missing == 0:
        status = "ready"
    elif available > 0 or stale > 0:
        status = "partial"
    else:
        status = "missing"
    guidance = _priced_in_source_guidance(source, status)
    if source == "market_bars" and status not in {"ready", "not_applicable"}:
        target_as_of = _parse_date(
            _mapping_value(values, "as_of_bar_scope").get("target_as_of")
        )
        stocks_scope = (
            str(values.get("coverage_basis") or "") == "stock_like_active_as_of_bars"
        )
        guidance = {
            **guidance,
            "next_action": (
                "Fill stock-like missing as-of bars first; then rerun the "
                "stocks-only priced-in scan."
                if stocks_scope
                else (
                    "Fill missing as-of bars for the active universe; then "
                    "rerun the full priced-in scan."
                )
            ),
            "command": _csv_market_template_command(
                target_as_of,
                missing_only=True,
                stocks_only=stocks_scope,
            ),
            "api": "POST /api/radar/market-bars/template",
        }
    applicability = _row_dict(_mapping_value(values, "applicability"))
    if source == "catalyst_events" and applicability:
        guidance = _priced_in_catalyst_guidance_with_applicability(
            guidance,
            applicability,
            status=status,
        )
    if status not in {"ready", "not_applicable"} and sample_tickers:
        ticker_guidance = _priced_in_source_guidance_for_tickers(
            source,
            guidance,
            sample_tickers,
        )
        batch_plan_command = _priced_in_source_batch_plan_command(
            source,
            stocks_only=stocks_only,
        )
        if batch_plan_command and gap_count > len(sample_tickers):
            sample_command = str(ticker_guidance.get("command") or "").strip()
            guidance = {
                **guidance,
                "command": batch_plan_command,
                "api": _priced_in_source_batches_api(
                    source,
                    stocks_only=stocks_only,
                ),
                "external_call_boundary": (
                    "Planning full-scan batches makes no provider calls; executing "
                    "a listed batch remains explicit and rate-limited."
                ),
            }
            if (
                source in PRICED_IN_SCHWAB_BATCH_SOURCES
                and sample_command
                and sample_command != batch_plan_command
            ):
                guidance["sample_command"] = sample_command
            if ticker_guidance.get("api_payload") is not None:
                guidance["sample_api_payload"] = ticker_guidance.get("api_payload")
        else:
            guidance = ticker_guidance
    batch_plan_command = (
        _priced_in_source_batch_plan_command(source, stocks_only=stocks_only)
        if gap_count > 0 and source in PRICED_IN_BATCHABLE_SOURCES
        else None
    )
    batch_plan_api = (
        _priced_in_source_batches_api(source, stocks_only=stocks_only)
        if batch_plan_command
        else None
    )
    return {
        "source": source,
        "status": status,
        "available": available,
        "stale": stale,
        "missing": missing,
        "gap_count": gap_count,
        "row_count": source_row_count,
        "raw_row_count": values.get("raw_row_count"),
        "routed_non_company_gap_rows": values.get("routed_non_company_gap_rows"),
        "applicability": applicability,
        "coverage_pct": coverage_pct,
        "coverage_basis": values.get("coverage_basis"),
        "as_of_bar_scope": _row_dict(_mapping_value(values, "as_of_bar_scope")),
        "repair_status": values.get("repair_status"),
        "provider_fill_plan": _row_dict(_mapping_value(values, "provider_fill_plan")),
        "provider_fill_command": values.get("provider_fill_command"),
        "provider_fill_status": values.get("provider_fill_status"),
        "provider_fill_external_call_count": values.get(
            "provider_fill_external_call_count"
        ),
        "dashboard_manual_template_command": values.get(
            "dashboard_manual_template_command"
        ),
        "dashboard_manual_template_regenerate_command": values.get(
            "dashboard_manual_template_regenerate_command"
        ),
        "dashboard_manual_import_preview_command": values.get(
            "dashboard_manual_import_preview_command"
        ),
        "dashboard_manual_import_execute_command": values.get(
            "dashboard_manual_import_execute_command"
        ),
        "sample_tickers": sample_tickers,
        "sample_scope": _priced_in_source_sample_scope(
            sample_count=len(sample_tickers),
            gap_count=gap_count,
            row_count=row_count,
        ),
        "full_scan_gap_review_command": (
            _priced_in_queue_source_gap_command(
                source,
                stocks_only=stocks_only,
                limit=50,
            )
            if gap_count > 0
            else None
        ),
        "full_scan_export_command": (
            _priced_in_queue_source_gap_command(
                source,
                stocks_only=stocks_only,
                all_rows=True,
            )
            if gap_count > 0
            else None
        ),
        "batch_plan_command": batch_plan_command,
        "batch_plan_api": batch_plan_api,
        **guidance,
    }


def _priced_in_source_sample_scope(
    *,
    sample_count: int,
    gap_count: int,
    row_count: int,
) -> str | None:
    if gap_count <= 0:
        return None
    if sample_count <= 0:
        return (
            f"No example tickers are attached; {gap_count} of {row_count} current "
            "filtered scan row(s) still have this source gap."
        )
    if sample_count >= gap_count:
        return (
            f"These are all {gap_count} missing/stale row(s) in the current "
            "filtered scan, not a separate scan universe."
        )
    return (
        f"These are the first {sample_count} of {gap_count} missing/stale row(s) "
        "in the current filtered scan; use full_scan_gap_review_command to page "
        "through the full scan."
    )


def _priced_in_catalyst_guidance_with_applicability(
    guidance: Mapping[str, object],
    applicability: Mapping[str, object],
    *,
    status: str,
) -> dict[str, object]:
    updated = dict(guidance)
    routed = int(_finite_float(applicability.get("non_applicable_gap_rows")))
    applicable_gap = int(_finite_float(applicability.get("applicable_gap_rows")))
    if routed:
        if applicable_gap:
            updated["next_action"] = (
                "Fill SEC catalyst events for company-like rows; route "
                "ETF/fund/wrapper rows to underlying, theme, fund-flow, or "
                "similar non-company evidence."
            )
        else:
            updated["next_action"] = (
                "No SEC company-event batch is needed for the routed "
                "non-company rows; use underlying, theme, fund-flow, or "
                "similar evidence instead."
            )
        updated["external_call_boundary"] = (
            "SEC catalyst batches are only for company-like or unknown-type "
            "rows. Routed ETF/fund/wrapper rows make no SEC company-filing calls."
        )
    if status == "ready" and routed:
        updated["meaning"] = (
            "SEC catalyst coverage is ready for applicable company rows; "
            "non-company instruments use a separate evidence route."
        )
    return updated


def _priced_in_source_guidance_for_tickers(
    source: str,
    guidance: Mapping[str, object],
    tickers: Sequence[str],
) -> dict[str, object]:
    updated = dict(guidance)
    if source in {"options", "broker_context"}:
        updated["command"] = _schwab_market_sync_command(tickers)
        updated["api_payload"] = {
            "tickers": list(tickers),
            "include_history": True,
            "include_options": True,
        }
    return updated


def _single_priced_in_source(value: str) -> str:
    sources = _priced_in_source_gap_filter(value)
    if len(sources) != 1:
        msg = (
            "source must name exactly one priced-in source: "
            f"{', '.join(PRICED_IN_SOURCE_CLASSES)}"
        )
        raise ValueError(msg)
    source = sources[0]
    if source not in PRICED_IN_SOURCE_CLASSES:
        msg = (
            "unsupported priced-in source "
            f"{source!r}; expected one of {', '.join(PRICED_IN_SOURCE_CLASSES)}"
        )
        raise ValueError(msg)
    return source


def _priced_in_source_max_batch_size(source_name: str, config: AppConfig) -> int:
    if source_name in PRICED_IN_SCHWAB_BATCH_SOURCES:
        return max(1, int(config.schwab_market_sync_max_tickers))
    if source_name == "catalyst_events":
        return max(1, int(config.sec_daily_max_tickers))
    if source_name == "local_text":
        return PRICED_IN_LOCAL_BATCH_MAX_TICKERS
    return PRICED_IN_LOCAL_BATCH_MAX_TICKERS


def _priced_in_source_plannable_rows(
    engine: Engine,
    *,
    source_name: str,
    rows: Sequence[Mapping[str, object]],
    stocks_only: bool = False,
) -> tuple[list[Mapping[str, object]], dict[str, object]]:
    if source_name in PRICED_IN_SCHWAB_BATCH_SOURCES:
        if source_name == "options":
            diagnostic = _priced_in_option_gap_diagnostic(engine, rows)
            diagnostic_status = str(diagnostic.get("status") or "").strip()
            blocking_statuses = {
                "newer_than_scan",
                "after_decision_cutoff",
                "eligible_but_not_scored",
            }
            if diagnostic_status in blocking_statuses:
                point_in_time_import_command = _options_point_in_time_import_command(
                    diagnostic
                )
                point_in_time_template_command = (
                    _options_point_in_time_template_command(
                        diagnostic,
                        stocks_only=stocks_only,
                    )
                )
                point_in_time_validate_command = _options_point_in_time_validate_command(
                    diagnostic
                )
                point_in_time_progress = _options_point_in_time_fixture_progress(
                    diagnostic,
                    stocks_only=stocks_only,
                )
                progress_action = (
                    point_in_time_progress.get("next_action")
                    if bool(point_in_time_progress.get("exists"))
                    else None
                )
                next_action = str(
                    progress_action
                    or diagnostic.get("next_action")
                    or "Options source fill is blocked for this scan date."
                )
                return [], {
                    "schema_version": "priced-in-source-batch-diagnostic-v1",
                    "status": "blocked",
                    "reason": next_action,
                    "eligible_rows": 0,
                    "blocked_rows": len(rows),
                    "blocked_reason": diagnostic_status,
                    "sample_blocked_tickers": _option_gap_diagnostic_samples(
                        diagnostic
                    ),
                    "next_action": next_action,
                    "point_in_time_template_command": point_in_time_template_command,
                    "point_in_time_validate_command": point_in_time_validate_command,
                    "point_in_time_import_command": point_in_time_import_command,
                    "point_in_time_fixture_progress": point_in_time_progress,
                    "option_gap_diagnostic": diagnostic,
                }
            return list(rows), {
                "schema_version": "priced-in-source-batch-diagnostic-v1",
                "status": "eligible",
                "reason": "Read-only Schwab option sync can be planned for these rows.",
                "eligible_rows": len(rows),
                "blocked_rows": 0,
                "option_gap_diagnostic": diagnostic,
            }
        return list(rows), {
            "schema_version": "priced-in-source-batch-diagnostic-v1",
            "status": "eligible",
            "reason": "Read-only Schwab market sync can be planned for these rows.",
            "eligible_rows": len(rows),
            "blocked_rows": 0,
        }
    if source_name == "catalyst_events":
        raw_tickers = [
            str(row.get("ticker") or "").strip().upper()
            for row in rows
            if str(row.get("ticker") or "").strip()
        ]
        security_meta = _security_metadata_by_ticker(engine, raw_tickers)
        sec_rows: list[Mapping[str, object]] = []
        routed_rows: list[Mapping[str, object]] = []
        for row in rows:
            ticker = str(row.get("ticker") or "").strip().upper()
            security_type = _security_type_for_scope(security_meta.get(ticker))
            if _is_non_company_instrument_type(security_type):
                routed_rows.append(row)
            else:
                sec_rows.append(row)
        tickers = [
            str(row.get("ticker") or "").strip().upper()
            for row in sec_rows
            if str(row.get("ticker") or "").strip()
        ]
        cik_by_ticker = _security_cik_by_ticker(engine, tickers)
        eligible = [
            row
            for row in sec_rows
            if str(row.get("ticker") or "").strip().upper() in cik_by_ticker
        ]
        missing_cik = [
            ticker for ticker in tickers if ticker and ticker not in cik_by_ticker
        ]
        missing_breakdown = _missing_cik_breakdown(
            missing_cik,
            security_meta=security_meta,
        )
        routed_tickers = [
            str(row.get("ticker") or "").strip().upper()
            for row in routed_rows
            if str(row.get("ticker") or "").strip()
        ]
        routed_payload = {
            "routed_non_company_rows": len(routed_rows),
            "sample_routed_non_company_tickers": _sample_tickers(routed_tickers),
            "non_company_evidence_route": (
                "Use fund, underlying, theme, sector, flow, or constituent evidence "
                "instead of SEC company filing batches."
            )
            if routed_rows
            else None,
        }
        diagnostic_status = (
            "eligible"
            if eligible
            else "blocked"
            if missing_cik
            else "routed"
            if routed_rows
            else "blocked"
        )
        return eligible, {
            "schema_version": "priced-in-source-batch-diagnostic-v1",
            "status": diagnostic_status,
            "reason": _missing_cik_reason(missing_breakdown)
            if missing_cik
            else (
                "Non-company instruments are routed away from SEC company filing "
                "batches."
                if routed_rows and not eligible
                else "SEC event batches can be planned for these CIK-backed rows."
            ),
            "eligible_rows": len(eligible),
            "blocked_rows": len(missing_cik),
            "blocked_reason": "missing_cik" if missing_cik else None,
            "sample_blocked_tickers": _sample_tickers(missing_cik),
            **missing_breakdown,
            **routed_payload,
            "next_action": _missing_cik_next_action(missing_breakdown)
            if missing_cik
            else (
                routed_payload["non_company_evidence_route"]
                if routed_rows and not eligible
                else None
            ),
            "fix_command": "catalyst-radar ingest-sec company-tickers"
            if _missing_cik_breakdown_can_refresh(missing_breakdown)
            else None,
            "manual_fix_command": (
                "catalyst-radar ingest-sec cik-overrides --csv <cik-overrides.csv>"
                if _missing_cik_breakdown_can_refresh(missing_breakdown)
                else None
            ),
            "manual_validate_command": (
                "catalyst-radar ingest-sec cik-overrides "
                "--csv <cik-overrides.csv> --validate-only"
                if _missing_cik_breakdown_can_refresh(missing_breakdown)
                else None
            ),
            "manual_fix_api": "POST /api/radar/sec/cik-overrides"
            if _missing_cik_breakdown_can_refresh(missing_breakdown)
            else None,
            "manual_validate_api": "POST /api/radar/sec/cik-overrides/validate"
            if _missing_cik_breakdown_can_refresh(missing_breakdown)
            else None,
            "fix_api": "POST /api/radar/sec/company-tickers"
            if _missing_cik_breakdown_can_refresh(missing_breakdown)
            else None,
        }
    if source_name == "local_text":
        eligible = [
            row
            for row in rows
            if "catalyst_events"
            in {
                str(item)
                for item in _sequence_value(
                    _priced_in_row_source_payload(row).get("available")
                )
            }
        ]
        blocked = max(0, len(rows) - len(eligible))
        return eligible, {
            "schema_version": "priced-in-source-batch-diagnostic-v1",
            "status": "eligible" if eligible else "blocked",
            "reason": (
                "Local text can run only after catalyst event text exists for a ticker."
                if blocked
                else "Local text batches can be planned for rows with event text."
            ),
            "eligible_rows": len(eligible),
            "blocked_rows": blocked,
            "blocked_reason": "missing_catalyst_events" if blocked else None,
            "sample_blocked_tickers": _sample_tickers(
                [
                    str(row.get("ticker") or "").strip().upper()
                    for row in rows
                    if row not in eligible
                ]
            ),
            "next_action": (
                "Fill catalyst_events first; local text can only process rows "
                "with stored event text."
            )
            if blocked
            else None,
        }
    return [], {
        "schema_version": "priced-in-source-batch-diagnostic-v1",
        "status": "not_batchable",
        "reason": f"{source_name} is not filled by ticker batch commands.",
        "eligible_rows": 0,
        "blocked_rows": len(rows),
    }


def _option_gap_diagnostic_samples(diagnostic: Mapping[str, object]) -> list[str]:
    for key in (
        "sample_newer_than_scan_tickers",
        "sample_after_cutoff_tickers",
        "sample_eligible_but_missing_tickers",
        "sample_no_stored_option_tickers",
    ):
        samples = [
            str(ticker).strip().upper()
            for ticker in _sequence_value(diagnostic.get(key))
            if str(ticker).strip()
        ]
        if samples:
            return samples
    return []


def _options_point_in_time_fixture_progress(
    diagnostic: Mapping[str, object],
    *,
    stocks_only: bool = False,
) -> dict[str, object]:
    target = _options_point_in_time_target_date(diagnostic)
    display_path = _options_point_in_time_fixture_display_path(target)
    base = {
        "schema_version": "options-point-in-time-fixture-progress-v1",
        "target_date": target,
        "path": display_path,
        "external_calls_made": 0,
        "validate_command": _options_point_in_time_validate_command(diagnostic),
        "import_command": _options_point_in_time_import_command(diagnostic),
        "template_command": _options_point_in_time_template_command(
            diagnostic,
            stocks_only=stocks_only,
        ),
    }
    if target == "<SCAN_DATE>":
        return {
            **base,
            "status": "unknown_scan_date",
            "exists": False,
            "row_count": 0,
            "complete": 0,
            "partial": 0,
            "empty": 0,
            "next_action": (
                "Pick one scan date before creating or importing a point-in-time "
                "options fixture."
            ),
        }
    path = _options_point_in_time_fixture_path(target)
    if not path.exists():
        return {
            **base,
            "status": "missing",
            "exists": False,
            "row_count": 0,
            "complete": 0,
            "partial": 0,
            "empty": 0,
            "next_action": (
                "Create the point-in-time options template, fill scan-date option "
                "context, then validate before import."
            ),
        }

    validation = validate_options_fixture_json(path, expected_as_of=target).as_payload()
    row_count = int(_finite_float(validation.get("row_count")))
    complete = int(_finite_float(validation.get("valid_row_count")))
    empty = _options_fixture_empty_row_count(path)
    partial = max(0, row_count - complete - empty)
    status = "ready_to_import" if validation.get("status") == "ready" else "needs_fill"
    if row_count <= 0 or int(_finite_float(validation.get("missing_field_count"))):
        status = "needs_fix"
    next_action = (
        f"Import the validated point-in-time options fixture at {display_path}."
        if status == "ready_to_import"
        else (
            f"Fill point-in-time option fields in {display_path}; "
            "then validate before import."
        )
    )
    return {
        **base,
        "status": status,
        "exists": True,
        "row_count": row_count,
        "complete": complete,
        "partial": partial,
        "empty": empty,
        "invalid_rows": int(_finite_float(validation.get("invalid_row_count"))),
        "blank_required_count": int(
            _finite_float(validation.get("blank_required_count"))
        ),
        "invalid_numeric_count": int(
            _finite_float(validation.get("invalid_numeric_count"))
        ),
        "missing_field_count": int(_finite_float(validation.get("missing_field_count"))),
        "duplicate_ticker_count": int(
            _finite_float(validation.get("duplicate_ticker_count"))
        ),
        "next_action": next_action,
    }


def _options_fixture_empty_row_count(path: Path) -> int:
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return 0
    if not isinstance(payload, Mapping):
        return 0
    results = payload.get("results")
    if not isinstance(results, list):
        return 0
    empty = 0
    for row in results:
        if not isinstance(row, Mapping):
            continue
        filled = [
            field
            for field in OPTIONS_FIXTURE_NUMERIC_FIELDS
            if str(row.get(field) or "").strip()
        ]
        if not filled:
            empty += 1
    return empty


def _options_point_in_time_fixture_path(target: str) -> Path:
    return Path("data") / "local" / f"point-in-time-options-{target}.json"


def _options_point_in_time_fixture_display_path(target: str) -> str:
    return f"data\\local\\point-in-time-options-{target}.json"


def _options_point_in_time_import_command(
    diagnostic: Mapping[str, object],
) -> str:
    target = _options_point_in_time_target_date(diagnostic)
    return (
        "catalyst-radar ingest-options --fixture "
        f"<point-in-time-options-{target}.json>"
    )


def _options_point_in_time_template_command(
    diagnostic: Mapping[str, object],
    *,
    stocks_only: bool = False,
) -> str:
    target = _options_point_in_time_target_date(diagnostic)
    return (
        "catalyst-radar ingest-options --fixture-template "
        f"--out data\\local\\point-in-time-options-{target}.json"
        + (" --stocks-only" if stocks_only else "")
    )


def _options_point_in_time_validate_command(
    diagnostic: Mapping[str, object],
) -> str:
    target = _options_point_in_time_target_date(diagnostic)
    return (
        "catalyst-radar ingest-options --fixture "
        f"data\\local\\point-in-time-options-{target}.json "
        f"--validate-only --expected-as-of {target}"
    )


def _options_point_in_time_target_date(
    diagnostic: Mapping[str, object],
) -> str:
    scan_dates = [
        str(value).strip()
        for value in _sequence_value(diagnostic.get("scan_as_of_dates"))
        if str(value).strip()
    ]
    return scan_dates[0] if len(scan_dates) == 1 else "<SCAN_DATE>"


def _options_fixture_template_target_as_of(
    rows: Sequence[Mapping[str, object]],
    diagnostic: Mapping[str, object],
) -> str:
    as_of_values = [
        _options_fixture_template_datetime_text(row.get("as_of"))
        for row in rows
        if str(row.get("as_of") or "").strip()
    ]
    unique_as_of = list(dict.fromkeys(as_of_values))
    if len(unique_as_of) == 1:
        return unique_as_of[0]
    target_date = _options_point_in_time_target_date(diagnostic)
    if target_date != "<SCAN_DATE>":
        return f"{target_date}T21:00:00+00:00"
    return "<SCAN_DATE>T21:00:00+00:00"


def _options_fixture_template_datetime_text(value: object) -> str:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    return str(value or "").strip()


def _options_fixture_template_target_date(
    target_as_of: str,
    diagnostic: Mapping[str, object],
) -> str:
    if len(target_as_of) >= 10 and target_as_of[4:5] == "-" and target_as_of[7:8] == "-":
        return target_as_of[:10]
    return _options_point_in_time_target_date(diagnostic)


def _priced_in_source_row_priority_key(row: Mapping[str, object]) -> tuple[int, float, str]:
    usefulness = _mapping_value(row, "usefulness")
    usefulness_status = str(usefulness.get("status") or "").strip().lower()
    priced_status = str(row.get("priced_in_status") or "").strip().lower()
    if usefulness_status == "decision_useful":
        rank = 0
    elif usefulness_status == "research_useful":
        rank = 1
    elif priced_status in PRICED_IN_ACTIONABLE_STATUSES:
        rank = 2
    elif usefulness_status == "monitor_only":
        rank = 3
    elif usefulness_status == "blocked":
        rank = 4
    else:
        rank = 5
    score = _finite_float(
        _first_present(
            row.get("emotion_reaction_gap"),
            row.get("score"),
            row.get("final_score"),
        )
    )
    return (rank, -abs(score), str(row.get("ticker") or ""))


def _security_cik_by_ticker(engine: Engine, tickers: Sequence[str]) -> dict[str, str]:
    normalized = sorted({str(ticker).strip().upper() for ticker in tickers if str(ticker).strip()})
    if not normalized:
        return {}
    stmt = select(securities.c.ticker, securities.c.metadata).where(
        securities.c.is_active.is_(True),
        securities.c.ticker.in_(normalized),
    )
    cik_by_ticker: dict[str, str] = {}
    with engine.connect() as conn:
        for row in conn.execute(stmt):
            metadata = row._mapping["metadata"] or {}
            if not isinstance(metadata, Mapping):
                continue
            cik = _metadata_cik(metadata)
            if cik:
                cik_by_ticker[str(row.ticker).strip().upper()] = cik
    return cik_by_ticker


def _security_metadata_by_ticker(
    engine: Engine,
    tickers: Sequence[str],
) -> dict[str, dict[str, object]]:
    normalized = sorted(
        {str(ticker).strip().upper() for ticker in tickers if str(ticker).strip()}
    )
    if not normalized:
        return {}
    stmt = (
        select(
            securities.c.ticker,
            securities.c.name,
            securities.c.exchange,
            securities.c.sector,
            securities.c.industry,
            securities.c.market_cap,
            securities.c.avg_dollar_volume_20d,
            securities.c.has_options,
            securities.c.metadata,
        )
        .where(
            securities.c.is_active.is_(True),
            securities.c.ticker.in_(normalized),
        )
    )
    rows: dict[str, dict[str, object]] = {}
    with engine.connect() as conn:
        for row in conn.execute(stmt):
            metadata = row._mapping["metadata"] or {}
            rows[str(row.ticker).strip().upper()] = {
                "name": str(row._mapping["name"] or ""),
                "exchange": str(row._mapping["exchange"] or ""),
                "sector": str(row._mapping["sector"] or ""),
                "industry": str(row._mapping["industry"] or ""),
                "market_cap": row._mapping["market_cap"],
                "avg_dollar_volume_20d": row._mapping["avg_dollar_volume_20d"],
                "has_options": bool(row._mapping["has_options"]),
                "metadata": metadata if isinstance(metadata, Mapping) else {},
            }
    return rows


def _priced_in_instrument_scope_payload(
    engine: Engine,
    rows: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    tickers: list[str] = []
    seen: set[str] = set()
    for row in rows:
        ticker = _priced_in_action_ticker(row)
        if not ticker or ticker in seen:
            continue
        seen.add(ticker)
        tickers.append(ticker)
    security_meta = _security_metadata_by_ticker(engine, tickers)
    type_counts: dict[str, int] = {}
    company_like: list[str] = []
    non_company: list[str] = []
    unknown_type: list[str] = []
    for ticker in tickers:
        security_type = _security_type_for_scope(security_meta.get(ticker))
        type_counts[security_type] = type_counts.get(security_type, 0) + 1
        if _is_sec_company_like_type(security_type):
            company_like.append(ticker)
        elif security_type == "UNKNOWN":
            unknown_type.append(ticker)
        else:
            non_company.append(ticker)
    row_count = len(tickers)
    sec_scope = {
        "schema_version": "priced-in-sec-catalyst-applicability-v1",
        "applicable_rows": len(company_like),
        "non_applicable_rows": len(non_company),
        "unknown_type_rows": len(unknown_type),
        "explanation": (
            "SEC company filings apply to operating-company rows. ETF, ETN, "
            "fund, right, warrant, and other wrapper rows stay in the full scan "
            "but need fund, underlying, theme, or flow evidence instead."
        ),
        "next_action": _instrument_scope_next_action(
            company_like_count=len(company_like),
            non_company_count=len(non_company),
            unknown_count=len(unknown_type),
        ),
    }
    return {
        "schema_version": "priced-in-instrument-scope-v1",
        "row_count": row_count,
        "company_like_rows": len(company_like),
        "non_company_rows": len(non_company),
        "unknown_type_rows": len(unknown_type),
        "type_counts": dict(sorted(type_counts.items())),
        "sample_company_like_tickers": _sample_tickers(company_like),
        "sample_non_company_tickers": _sample_tickers(non_company),
        "sample_unknown_type_tickers": _sample_tickers(unknown_type),
        "sec_catalyst_applicability": sec_scope,
        "explanation": (
            "The priced-in queue is still the full ranked universe. Instrument "
            "scope only explains which evidence route applies to each ticker."
        ),
    }


def _catalyst_event_applicability_payload(
    engine: Engine,
    rows: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    row_list = [row for row in rows if isinstance(row, Mapping)]
    tickers = [_priced_in_action_ticker(row) for row in row_list]
    security_meta = _security_metadata_by_ticker(engine, tickers)
    counters = {
        "applicable_available": 0,
        "applicable_stale": 0,
        "applicable_missing": 0,
        "non_applicable_available": 0,
        "non_applicable_stale": 0,
        "non_applicable_missing": 0,
        "unknown_type_rows": 0,
    }
    applicable_rows = 0
    non_applicable_rows = 0
    applicable_gap_tickers: list[str] = []
    non_applicable_gap_tickers: list[str] = []
    for row in row_list:
        ticker = _priced_in_action_ticker(row)
        security_type = _security_type_for_scope(security_meta.get(ticker))
        source_status = _priced_in_row_source_status(row, "catalyst_events")
        if _is_non_company_instrument_type(security_type):
            non_applicable_rows += 1
            counters[f"non_applicable_{source_status}"] += 1
            if source_status != "available":
                _append_priced_in_action_ticker(non_applicable_gap_tickers, ticker)
            continue
        applicable_rows += 1
        if security_type == "UNKNOWN":
            counters["unknown_type_rows"] += 1
        counters[f"applicable_{source_status}"] += 1
        if source_status != "available":
            _append_priced_in_action_ticker(applicable_gap_tickers, ticker)
    return {
        "schema_version": "priced-in-catalyst-applicability-v1",
        "applicable_rows": applicable_rows,
        "non_applicable_rows": non_applicable_rows,
        "unknown_type_rows": counters["unknown_type_rows"],
        "applicable_available": counters["applicable_available"],
        "applicable_stale": counters["applicable_stale"],
        "applicable_missing": counters["applicable_missing"],
        "applicable_gap_rows": counters["applicable_stale"]
        + counters["applicable_missing"],
        "non_applicable_available": counters["non_applicable_available"],
        "non_applicable_stale": counters["non_applicable_stale"],
        "non_applicable_missing": counters["non_applicable_missing"],
        "non_applicable_gap_rows": counters["non_applicable_stale"]
        + counters["non_applicable_missing"],
        "sample_applicable_gap_tickers": applicable_gap_tickers,
        "sample_non_applicable_gap_tickers": non_applicable_gap_tickers,
        "route": "sec_company_filings_for_company_like_rows",
        "non_company_route": "fund_underlying_theme_or_flow_evidence",
        "explanation": (
            "SEC company catalyst batches apply only to company-like or "
            "unknown-type rows. ETF, fund, ETN, right, warrant, and wrapper "
            "rows stay in the full scan but are routed away from SEC company "
            "filing batches."
        ),
        "next_action": (
            "Fill SEC catalyst events for company-like rows; route ETF/fund/"
            "wrapper rows to underlying, theme, fund-flow, or similar "
            "non-company evidence."
        ),
    }


def _priced_in_row_source_status(row: Mapping[str, object], source: str) -> str:
    sources = _priced_in_row_source_payload(row)
    if source in {str(item) for item in _sequence_value(sources.get("available"))}:
        return "available"
    if source in {str(item) for item in _sequence_value(sources.get("stale"))}:
        return "stale"
    return "missing"


def _security_type_for_scope(row: Mapping[str, object] | None) -> str:
    metadata = _mapping_value(row, "metadata")
    security_type = str(metadata.get("type") or "").strip().upper()
    if not security_type:
        return "UNKNOWN"
    return security_type


def _is_non_company_instrument_type(security_type: str) -> bool:
    normalized = str(security_type).strip().upper()
    return bool(
        normalized
        and normalized != "UNKNOWN"
        and not _is_sec_company_like_type(normalized)
    )


def _instrument_scope_next_action(
    *,
    company_like_count: int,
    non_company_count: int,
    unknown_count: int,
) -> str:
    if non_company_count and company_like_count:
        return (
            "Keep scanning all tickers, but fill SEC catalysts for company-like "
            "rows and route ETF/fund/wrapper rows to fund, underlying, theme, or "
            "flow evidence."
        )
    if non_company_count:
        return (
            "Keep these rows in the full scan, but do not wait on SEC company "
            "filings; use fund, underlying, theme, or flow evidence."
        )
    if unknown_count:
        return (
            "Classify unknown instrument types before deciding whether SEC company "
            "filings or a non-company evidence route applies."
        )
    if company_like_count:
        return "SEC company catalyst evidence applies to this scan scope."
    return "No priced-in rows are available to classify."


def _missing_cik_breakdown(
    tickers: Sequence[str],
    *,
    security_meta: Mapping[str, Mapping[str, object]],
) -> dict[str, object]:
    type_counts: dict[str, int] = {}
    company_like: list[str] = []
    non_company: list[str] = []
    unknown_type: list[str] = []
    for ticker in tickers:
        metadata = _mapping_value(security_meta.get(ticker), "metadata")
        security_type = str(metadata.get("type") or "").strip().upper() or "UNKNOWN"
        type_counts[security_type] = type_counts.get(security_type, 0) + 1
        if _is_sec_company_like_type(security_type):
            company_like.append(ticker)
        elif security_type == "UNKNOWN":
            unknown_type.append(ticker)
        else:
            non_company.append(ticker)
    return {
        "missing_cik_type_counts": dict(sorted(type_counts.items())),
        "missing_cik_company_like_rows": len(company_like),
        "missing_cik_non_company_rows": len(non_company),
        "missing_cik_unknown_type_rows": len(unknown_type),
        "sample_company_like_missing_cik_tickers": _sample_tickers(company_like),
        "sample_non_company_missing_cik_tickers": _sample_tickers(non_company),
        "sample_unknown_type_missing_cik_tickers": _sample_tickers(unknown_type),
    }


def _is_sec_company_like_type(security_type: str) -> bool:
    return str(security_type).strip().upper() in PRICED_IN_COMPANY_LIKE_SECURITY_TYPES


def _missing_cik_breakdown_can_refresh(breakdown: Mapping[str, object]) -> bool:
    return (
        int(_finite_float(breakdown.get("missing_cik_company_like_rows"))) > 0
        or int(_finite_float(breakdown.get("missing_cik_unknown_type_rows"))) > 0
    )


def _missing_cik_reason(breakdown: Mapping[str, object]) -> str:
    company_like = int(_finite_float(breakdown.get("missing_cik_company_like_rows")))
    non_company = int(_finite_float(breakdown.get("missing_cik_non_company_rows")))
    unknown = int(_finite_float(breakdown.get("missing_cik_unknown_type_rows")))
    if non_company and not company_like and not unknown:
        return (
            "SEC event batches require CIK metadata, but the missing rows are "
            "non-company instruments such as ETFs, ETNs, rights, or warrants."
        )
    if non_company:
        return (
            "SEC event batches require CIK metadata. Most missing rows are "
            "non-company instruments; only company-like or unknown-type rows may "
            "be fixed by SEC company tickers."
        )
    return "SEC event batches require CIK metadata for each ticker."


def _missing_cik_next_action(breakdown: Mapping[str, object]) -> str:
    company_like = int(_finite_float(breakdown.get("missing_cik_company_like_rows")))
    non_company = int(_finite_float(breakdown.get("missing_cik_non_company_rows")))
    unknown = int(_finite_float(breakdown.get("missing_cik_unknown_type_rows")))
    if non_company and not company_like and not unknown:
        return (
            "Do not expect SEC company-tickers refresh to clear these rows; route "
            "ETF/ETN/fund-like instruments to a fund, underlying, or theme evidence "
            "source, or scope SEC catalyst coverage to operating companies."
        )
    if non_company:
        return (
            "Refresh SEC company tickers only for the small company-like/unknown "
            "subset, then handle ETF/ETN/fund-like rows through fund, underlying, "
            "or theme evidence instead of SEC company filings."
        )
    return (
        "Add CIK metadata for blocked tickers or refresh security metadata with "
        "catalyst-radar ingest-sec company-tickers before expecting SEC catalyst "
        "coverage for those rows."
    )


def _metadata_cik(metadata: Mapping[str, object]) -> str | None:
    for key in ("cik", "cik_str", "central_index_key"):
        value = metadata.get(key)
        if value is not None and str(value).strip():
            return str(value).strip().zfill(10)
    return None


def _priced_in_batch_as_of(rows: Sequence[Mapping[str, object]]) -> str:
    for row in rows:
        as_of = _parse_utc_datetime(row.get("as_of"))
        if as_of is not None:
            return as_of.date().isoformat()
    return "<LATEST_TRADING_DATE>"


def _priced_in_source_batch_command(
    source_name: str,
    tickers: Sequence[str],
    *,
    scan_as_of: str,
    planned_available_at: str,
    targets: Sequence[Mapping[str, object]] = (),
) -> str:
    if source_name in PRICED_IN_SCHWAB_BATCH_SOURCES:
        return _schwab_market_sync_command(tickers)
    if source_name == "catalyst_events":
        target_args = " ".join(
            f"--target {target.get('ticker')}:{target.get('cik')}"
            for target in targets
            if target.get("ticker") and target.get("cik")
        )
        if target_args:
            return f"catalyst-radar ingest-sec submissions-batch {target_args}"
        ticker_args = _ticker_args(tickers)
        return (
            "catalyst-radar run-daily "
            f"--as-of {scan_as_of} --available-at {planned_available_at} "
            f"{ticker_args} --json"
        ).strip()
    if source_name == "local_text":
        ticker_args = _ticker_args(tickers)
        return (
            "catalyst-radar run-textint "
            f"--as-of {scan_as_of} {ticker_args}"
        ).strip()
    return "catalyst-radar priced-in-preflight --json"


def _ticker_args(tickers: Sequence[str]) -> str:
    return " ".join(f"--ticker {ticker}" for ticker in tickers if ticker)


def _priced_in_source_batch_api(source_name: str) -> str | None:
    if source_name in PRICED_IN_SCHWAB_BATCH_SOURCES:
        return "POST /api/brokers/schwab/market-sync"
    if source_name == "catalyst_events":
        return "POST /api/radar/sec/submissions-batch"
    if source_name == "local_text":
        return "POST /api/radar/text/features-batch"
    return None


def _priced_in_queue_source_gap_command(
    source_name: str,
    *,
    stocks_only: bool = False,
    limit: int = 50,
    all_rows: bool = False,
) -> str:
    parts = ["catalyst-radar", "priced-in-queue"]
    if stocks_only:
        parts.append("--stocks-only")
    parts.extend(["--full-scan", "--source-gap", source_name])
    if all_rows:
        parts.extend(["--all", "--json"])
    else:
        parts.extend(["--limit", str(_positive_limit(limit))])
    return " ".join(parts)


def _priced_in_queue_full_scan_command(
    *,
    stocks_only: bool = False,
    limit: int = 50,
    all_rows: bool = False,
) -> str:
    parts = ["catalyst-radar", "priced-in-queue"]
    if stocks_only:
        parts.append("--stocks-only")
    parts.append("--full-scan")
    if all_rows:
        parts.extend(["--all", "--json"])
    else:
        parts.extend(["--limit", str(_positive_limit(limit))])
    return " ".join(parts)


def _priced_in_source_batches_command(
    source_name: str,
    *,
    stocks_only: bool = False,
    all_batches: bool = False,
    json: bool = False,
    execute_next: bool = False,
    execute_batches: int | None = None,
    batch_limit: int | None = None,
    batch_offset: int | None = None,
) -> str:
    parts = ["catalyst-radar", "priced-in-source-batches", "--source", source_name]
    if stocks_only:
        parts.append("--stocks-only")
    if batch_limit is not None:
        parts.extend(["--batch-limit", str(_positive_limit(batch_limit))])
    if batch_offset is not None:
        parts.extend(["--batch-offset", str(_positive_offset(batch_offset))])
    if all_batches:
        parts.append("--all")
    if execute_next:
        parts.append("--execute-next")
    if execute_batches is not None:
        parts.extend(["--execute-batches", str(_positive_limit(execute_batches))])
    if json:
        parts.append("--json")
    return " ".join(parts)


def _priced_in_source_batches_api(
    source_name: str,
    *,
    stocks_only: bool = False,
    all_batches: bool = False,
) -> str:
    params = [f"source={source_name}"]
    if all_batches:
        params.append("all_batches=true")
    if stocks_only:
        params.append("stocks_only=true")
    return "GET /api/radar/priced-in/source-batches?" + "&".join(params)


def _priced_in_source_batch_api_payload(
    source_name: str,
    tickers: Sequence[str],
    *,
    scan_as_of: str,
    planned_available_at: str,
    targets: Sequence[Mapping[str, object]] = (),
) -> dict[str, object] | None:
    if source_name in PRICED_IN_SCHWAB_BATCH_SOURCES:
        return {
            "tickers": list(tickers),
            "include_history": True,
            "include_options": True,
        }
    if source_name == "catalyst_events":
        return {
            "targets": [
                {"ticker": target.get("ticker"), "cik": target.get("cik")}
                for target in targets
                if target.get("ticker") and target.get("cik")
            ]
        }
    if source_name == "local_text":
        return {
            "as_of": scan_as_of,
            "available_at": planned_available_at,
            "tickers": list(tickers),
        }
    return None


def _priced_in_source_batch_targets(
    engine: Engine,
    *,
    source_name: str,
    tickers: Sequence[str],
) -> list[dict[str, str]]:
    if source_name != "catalyst_events":
        return []
    cik_by_ticker = _security_cik_by_ticker(engine, tickers)
    return [
        {"ticker": ticker, "cik": cik_by_ticker[ticker]}
        for ticker in tickers
        if ticker in cik_by_ticker
    ]


def _priced_in_source_batch_call_budget(
    config: AppConfig,
    *,
    source_name: str,
    ticker_count: int,
) -> dict[str, object]:
    if source_name == "local_text":
        return {
            "external_calls_required": 0,
            "external_call_breakdown": {},
            "call_plan_status": "local_only",
            "call_plan_headline": "Local text intelligence makes no provider calls.",
        }
    if source_name == "catalyst_events":
        missing = _sec_batch_missing_env(config)
        if missing:
            return {
                "external_calls_required": 0,
                "external_call_breakdown": {},
                "call_plan_status": "blocked",
                "call_plan_headline": (
                    "SEC submissions batch is blocked by missing live SEC settings."
                ),
                "call_plan_next_action": (
                    f"Set {' and '.join(missing)} before running SEC batches."
                ),
            }
        return {
            "external_calls_required": ticker_count,
            "external_call_breakdown": {"catalyst_events": ticker_count},
            "call_plan_status": "live_calls_planned",
            "call_plan_headline": (
                f"SEC submissions batch may make {ticker_count} external call(s)."
            ),
            "call_plan_next_action": (
                "Run only when this target count matches your intended SEC budget."
            ),
        }
    if source_name in PRICED_IN_SCHWAB_BATCH_SOURCES:
        return {
            "external_calls_required": 1,
            "external_call_breakdown": {"schwab": 1},
            "call_plan_status": "live_calls_planned",
            "call_plan_headline": "Read-only Schwab market sync may make one external call.",
        }
    return {
        "external_calls_required": 0,
        "external_call_breakdown": {},
        "call_plan_status": "not_batchable",
        "call_plan_headline": "This source is not filled by ticker batch sync.",
    }


def _sec_batch_missing_env(config: AppConfig) -> list[str]:
    missing: list[str] = []
    if not config.sec_enable_live:
        missing.append("CATALYST_SEC_ENABLE_LIVE=1")
    if not config.sec_user_agent_configured:
        missing.append("CATALYST_SEC_USER_AGENT")
    return missing


def _priced_in_source_batches_headline(
    *,
    source_name: str,
    batchable: bool,
    total_gap_rows: int,
    plannable_gap_rows: int,
    batch_count: int,
    batch_size: int,
) -> str:
    if total_gap_rows <= 0:
        return f"No full-scan rows currently have a {source_name} gap."
    if not batchable:
        return (
            f"{total_gap_rows} full-scan row(s) have a {source_name} gap; "
            "this source is not filled by ticker batch sync."
        )
    if plannable_gap_rows <= 0:
        return (
            f"{total_gap_rows} full-scan row(s) have a {source_name} gap; "
            "no ticker batch is currently runnable for this source."
        )
    return (
        f"{total_gap_rows} full-scan row(s) have a {source_name} gap; "
        f"{plannable_gap_rows} eligible row(s) planned as {batch_count} "
        f"batch(es) of up to {batch_size} ticker(s)."
    )


def _priced_in_source_batches_next_action(
    *,
    source_name: str,
    batchable: bool,
    total_gap_rows: int,
    plannable_gap_rows: int,
) -> str:
    if total_gap_rows <= 0:
        return "No batch action is needed for this source."
    if batchable:
        if plannable_gap_rows <= 0 and source_name == "catalyst_events":
            return "Add CIK metadata or use an event provider that covers these tickers."
        if plannable_gap_rows <= 0 and source_name == "local_text":
            return "Fill catalyst events first; local text has no event text to process."
        return (
            "Full scan is split into provider-safe chunks. Review the full batch "
            "plan, then run one explicit chunk at a time only if the source, scan "
            "date, and call budget match your intent."
        )
    if source_name == "catalyst_events":
        return "Use the Run page call plan; event ingestion is governed by provider caps."
    if source_name == "local_text":
        return "Run local text intelligence after event text exists for the scan date."
    return "Review the source action on Ops; this source is not filled by ticker batch sync."


def _priced_in_source_next_batch_command(
    *,
    source_name: str,
    stocks_only: bool = False,
    batch_limit: int,
    batch_offset: int,
    batch_count: int,
    all_batches: bool = False,
) -> str | None:
    if all_batches:
        return None
    if batch_offset >= batch_count:
        return None
    return _priced_in_source_batches_command(
        source_name,
        stocks_only=stocks_only,
        batch_limit=batch_limit,
        batch_offset=batch_offset,
    )


def _priced_in_source_batch_plan_command(
    source: str,
    *,
    stocks_only: bool = False,
) -> str | None:
    if source not in PRICED_IN_BATCHABLE_SOURCES:
        return None
    return _priced_in_source_batches_command(
        source,
        stocks_only=stocks_only,
        all_batches=True,
        json=True,
    )


def _schwab_market_sync_command(tickers: Sequence[str]) -> str:
    ticker_args = " ".join(
        f"--ticker {ticker}"
        for ticker in tickers[:PRICED_IN_SOURCE_ACTION_TICKER_LIMIT]
        if ticker
    )
    return f"catalyst-radar schwab-market-sync {ticker_args}".strip()


def _priced_in_source_guidance(source: str, status: str) -> dict[str, object]:
    ready = status == "ready"
    if source == "market_bars":
        return {
            "meaning": "Price and volume reaction can be compared against emotion.",
            "next_action": "Use current bars in priced-in scoring."
            if ready
            else "Refresh latest market bars, then rerun the full-market scan.",
            "command": "catalyst-radar priced-in-preflight --json"
            if not ready
            else "catalyst-radar priced-in-queue --status actionable --json",
            "api": "GET /api/radar/priced-in/preflight"
            if not ready
            else "GET /api/radar/priced-in?status=actionable",
            "external_call_boundary": (
                "No calls while inspecting; market refresh calls depend on "
                "provider plan."
            ),
        }
    if source == "catalyst_events":
        return {
            "meaning": "Filings, news, or earnings events explain what the market is reacting to.",
            "next_action": "Use catalyst events in the emotion side of the mismatch."
            if ready
            else "Review the run call plan and refresh event ingestion before trusting emotion.",
            "command": "catalyst-radar dashboard-tui --once --page run"
            if not ready
            else "catalyst-radar candidate-detail <TICKER>",
            "api": "POST /api/radar/runs/call-plan"
            if not ready
            else "GET /api/radar/candidates/{ticker}",
            "external_call_boundary": (
                "Event refresh can call SEC/news providers only from an explicit run."
            ),
        }
    if source == "local_text":
        return {
            "meaning": "Local text intelligence turns event text into narrative strength.",
            "next_action": "Use local text features in candidate evidence."
            if ready
            else "Run text intelligence for the scan date before relying on narrative strength.",
            "command": "catalyst-radar run-textint --as-of <LATEST_TRADING_DATE>",
            "api": None,
            "external_call_boundary": (
                "Local text intelligence reads stored text and makes no provider calls."
            ),
        }
    if source == "options":
        return {
            "meaning": "Options flow can confirm or contradict the market-emotion signal.",
            "next_action": "Use options as a supporting signal only."
            if ready
            else (
                "Use point-in-time options for the scan date; for a current scan, "
                "sync Schwab option-chain context, then rerun."
            ),
            "command": "catalyst-radar schwab-market-sync --ticker <TICKER>",
            "api": "POST /api/brokers/schwab/market-sync",
            "external_call_boundary": (
                "Live Schwab options are explicit, read-only, and rate-limited; "
                "current option chains must not be used as score input for older "
                "scan dates."
            ),
        }
    if source == "theme_peer_sector":
        return {
            "meaning": "Theme, peer, and sector context shows whether the move is stock-specific.",
            "next_action": "Use theme and peer context as supporting evidence."
            if ready
            else "Rerun the scan after market bars and universe metadata are current.",
            "command": "catalyst-radar scan --as-of <LATEST_TRADING_DATE>",
            "api": "POST /api/radar/runs",
            "external_call_boundary": (
                "Scan uses local stored data unless the explicit run plan calls providers."
            ),
        }
    if source == "broker_context":
        return {
            "meaning": (
                "Read-only Schwab market context is for sizing, triggers, and "
                "supporting option-chain evidence."
            ),
            "next_action": "Use Schwab market context only after signal evidence is reviewed."
            if ready
            else "Sync read-only Schwab market context before sizing or trigger review.",
            "command": (
                "catalyst-radar schwab-market-sync --ticker <TICKER>"
            ),
            "api": "POST /api/brokers/schwab/market-sync",
            "external_call_boundary": (
                "Schwab market sync is explicit, read-only, rate-limited, and never "
                "submits orders."
            ),
        }
    return {
        "meaning": "Source contributes to priced-in review.",
        "next_action": "Open Ops to inspect source coverage.",
        "command": "catalyst-radar dashboard-tui --once --page ops",
        "api": None,
        "external_call_boundary": "Inspection makes no provider calls.",
    }


def _priced_in_queue_sort_key(row: Mapping[str, object]) -> tuple[int, float, float, str]:
    status = str(row.get("priced_in_status") or "").lower()
    priority = {
        "bullish_not_priced_in": 0,
        "bearish_not_priced_in": 0,
        "overextended_hype": 1,
        "conflicted": 2,
        "fully_priced": 3,
        "stale": 4,
        "blocked": 5,
        "neutral": 6,
    }.get(status, 7)
    return (
        priority,
        -abs(_finite_float(row.get("emotion_reaction_gap"))),
        -_finite_float(row.get("score")),
        str(row.get("ticker") or ""),
    )


def _priced_in_status_matches(row: Mapping[str, object], wanted_status: str) -> bool:
    status = str(row.get("priced_in_status") or "").strip().lower()
    if wanted_status in PRICED_IN_ACTIONABLE_FILTERS:
        return status in PRICED_IN_ACTIONABLE_STATUSES
    return status == wanted_status


def _priced_in_usefulness_filter(
    value: object | None,
) -> tuple[str, frozenset[str]]:
    normalized = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    if normalized in {"", "all", "any"}:
        return "all", frozenset()
    matches = PRICED_IN_USEFULNESS_FILTERS.get(normalized)
    if matches is not None:
        return normalized, matches
    if normalized in PRICED_IN_USEFULNESS_STATUSES:
        return normalized, frozenset({normalized})
    return normalized, frozenset({normalized})


def _priced_in_usefulness_matches(
    row: Mapping[str, object],
    wanted_statuses: frozenset[str],
) -> bool:
    usefulness = _mapping_value(row, "usefulness")
    status = str(usefulness.get("status") or "").strip().lower()
    return status in wanted_statuses


def _priced_in_usefulness_counts(
    rows: Sequence[Mapping[str, object]],
) -> dict[str, int]:
    return dict(
        Counter(
            str(_mapping_value(row, "usefulness").get("status") or "unknown")
            for row in rows
        )
    )


def _priced_in_decision_gap_counts(
    rows: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    actionable_rows = [
        row
        for row in rows
        if str(row.get("priced_in_status") or "").strip().lower()
        in PRICED_IN_ACTIONABLE_STATUSES
    ]
    counts: Counter[str] = Counter()
    sample_tickers: dict[str, list[str]] = defaultdict(list)
    for row in actionable_rows:
        ticker = str(row.get("ticker") or "").strip().upper()
        usefulness = _mapping_value(row, "usefulness")
        for gap in _sequence_value(usefulness.get("missing_for_decision")):
            gap_name = str(gap or "").strip()
            if gap_name:
                counts[gap_name] += 1
                samples = sample_tickers[gap_name]
                if (
                    ticker
                    and ticker not in samples
                    and len(samples) < PRICED_IN_LOCAL_BATCH_MAX_TICKERS
                ):
                    samples.append(ticker)
    return {
        "schema_version": "priced-in-decision-gap-counts-v1",
        "scope": "actionable_mismatch_rows",
        "row_count": len(actionable_rows),
        "counts": dict(sorted(counts.items())),
        "sample_tickers": {
            gap: tickers
            for gap, tickers in sorted(sample_tickers.items())
            if tickers
        },
        "top_gaps": [
            {"gap": gap, "count": count}
            for gap, count in sorted(
                counts.items(),
                key=lambda item: (-item[1], item[0]),
            )
        ],
    }


def _priced_in_source_gap_filter(value: str | Sequence[str] | None) -> tuple[str, ...]:
    raw_values: list[object]
    if value is None:
        raw_values = []
    elif isinstance(value, str):
        raw_values = [value]
    else:
        raw_values = list(value)
    normalized: list[str] = []
    for raw in raw_values:
        for part in str(raw or "").replace(";", ",").split(","):
            source = part.strip().lower().replace("-", "_").replace(" ", "_")
            if source in {"", "all", "none"}:
                continue
            source = PRICED_IN_SOURCE_ALIASES.get(source, source)
            normalized.append(source)
    return tuple(dict.fromkeys(normalized))


def _priced_in_decision_gap_filter(value: str | Sequence[str] | None) -> tuple[str, ...]:
    raw_values: list[object]
    if value is None:
        raw_values = []
    elif isinstance(value, str):
        raw_values = [value]
    else:
        raw_values = list(value)
    aliases = {
        "packet": "candidate_packet",
        "candidate-packet": "candidate_packet",
        "candidate_packets": "candidate_packet",
        "card": "decision_card",
        "decision_cards": "decision_card",
        "decision-card": "decision_card",
        "broker": "broker_context",
        "schwab": "broker_context",
        "portfolio": "broker_context",
        "options_flow": "options",
    }
    normalized: list[str] = []
    for raw in raw_values:
        for part in str(raw or "").replace(";", ",").split(","):
            gap = part.strip().lower().replace("-", "_").replace(" ", "_")
            if gap in {"", "all", "none"}:
                continue
            normalized.append(aliases.get(gap, gap))
    return tuple(dict.fromkeys(normalized))


def _priced_in_source_gap_matches(
    row: Mapping[str, object],
    wanted_sources: Sequence[str],
) -> bool:
    data_sources = _mapping_value(row, "data_sources")
    missing_or_stale = {
        str(item).strip().lower()
        for key in ("missing", "stale")
        for item in _sequence_value(data_sources.get(key))
        if str(item).strip()
    }
    return all(source in missing_or_stale for source in wanted_sources)


def _priced_in_decision_gap_matches(
    row: Mapping[str, object],
    wanted_gaps: Sequence[str],
) -> bool:
    usefulness = _mapping_value(row, "usefulness")
    missing = {
        str(item).strip().lower()
        for item in _sequence_value(usefulness.get("missing_for_decision"))
        if str(item).strip()
    }
    return all(gap in missing for gap in wanted_gaps)


def _priced_in_scan_status(discovery: Mapping[str, object]) -> str:
    run = _mapping_value(discovery, "run")
    scan_yield = _mapping_value(discovery, "yield")
    freshness = _mapping_value(discovery, "freshness")
    active_count = int(_finite_float(freshness.get("active_security_count")))
    requested = int(_finite_float(scan_yield.get("requested_securities")))
    scanned = int(_finite_float(scan_yield.get("scanned_securities")))
    has_named_universe = bool(str(run.get("universe") or "").strip())
    all_active_denominator = active_count or requested or scanned
    if has_named_universe:
        selected_denominator = requested or scanned
        if (
            all_active_denominator >= 500
            and selected_denominator
            and selected_denominator < max(1, int(all_active_denominator * 0.9))
        ):
            return "selected_universe"
        denominator = selected_denominator
    else:
        denominator = all_active_denominator
    if denominator < 500:
        return "universe_too_small"
    if scanned and denominator and scanned < max(1, int(denominator * 0.9)):
        return "partial_scan"
    return "ready"


def _priced_in_queue_headline(
    status: str,
    *,
    total_count: int,
    returned_count: int,
    offset: int,
    status_filter: str,
    filtered: bool,
) -> str:
    range_start = offset + 1 if returned_count else 0
    range_end = offset + returned_count
    showing = f"showing {range_start}-{range_end} of {total_count}"
    if status == "previous_scan":
        if filtered:
            label = (
                "actionable mismatch"
                if status_filter in PRICED_IN_ACTIONABLE_FILTERS
                else "filtered priced-in"
            )
            return (
                "Latest run produced no priced-in rows; showing previous scan "
                f"{label} row(s), {showing}."
            )
        return (
            "Latest run produced no priced-in rows; showing previous full scan, "
            f"{showing} priced-in row(s)."
        )
    if status == "universe_too_small":
        if filtered:
            label = (
                "actionable mismatch"
                if status_filter in PRICED_IN_ACTIONABLE_FILTERS
                else "filtered priced-in"
            )
            return (
                "Local universe is too small for a full-market priced-in read; "
                f"{showing} {label} row(s)."
            )
        return (
            "Local universe is too small for a full-market priced-in read; "
            f"{showing} priced-in row(s)."
        )
    if status == "partial_scan":
        if filtered:
            label = (
                "actionable mismatch"
                if status_filter in PRICED_IN_ACTIONABLE_FILTERS
                else "filtered priced-in"
            )
            return f"Latest scan is partial; {showing} {label} row(s)."
        return f"Latest scan is partial; {showing} priced-in row(s)."
    if status == "selected_universe":
        if filtered:
            label = (
                "actionable mismatch"
                if status_filter in PRICED_IN_ACTIONABLE_FILTERS
                else "filtered priced-in"
            )
            return f"Latest scan used a selected universe; {showing} {label} row(s)."
        return f"Latest scan used a selected universe; {showing} priced-in row(s)."
    if filtered:
        if status_filter in PRICED_IN_ACTIONABLE_FILTERS:
            return (
                f"Latest full scan found {total_count} actionable mismatch row(s); "
                f"{showing}."
            )
        return f"Latest full scan filtered to {total_count} priced-in row(s); {showing}."
    return f"Latest full scan ranked {total_count} priced-in row(s); {showing}."


def _priced_in_queue_next_action(status: str) -> str:
    if status == "previous_scan":
        return (
            "Fix the latest run blocker, then rerun the full scan. Use these rows as "
            "the last useful scan, not fresh market output."
        )
    if status == "universe_too_small":
        return (
            "Ingest Polygon/Massive tickers and fresh bars, then run the radar "
            "without a ticker filter."
        )
    if status == "partial_scan":
        return "Open Ops/Run and fix missing bars before trusting the ranked queue."
    if status == "selected_universe":
        return "Run the radar without --universe to scan all active securities."
    return "Review the largest emotion-versus-reaction gaps first."


def _priced_in_preflight_rows(
    discovery: Mapping[str, object],
    call_plan: Mapping[str, object],
    provider_rows: Sequence[Mapping[str, object]],
    commands: Mapping[str, str],
    config: AppConfig,
    bar_universe: Mapping[str, object],
    source_coverage: Mapping[str, object],
    provider_blocker: Mapping[str, object],
    *,
    stocks_only: bool = False,
    stock_scope: Mapping[str, object] | None = None,
    market_bar_repair: Mapping[str, object] | None = None,
) -> list[dict[str, object]]:
    scan_yield = _mapping_value(discovery, "yield")
    freshness = _mapping_value(discovery, "freshness")
    active = int(_finite_float(freshness.get("active_security_count")))
    requested = int(_finite_float(scan_yield.get("requested_securities")))
    scanned = int(_finite_float(scan_yield.get("scanned_securities")))
    latest_bars = int(_finite_float(freshness.get("active_security_with_as_of_bar_count")))
    missing_bars = int(_finite_float(freshness.get("missing_as_of_daily_bar_count")))
    if stocks_only and stock_scope:
        stock_like_active = int(_finite_float(stock_scope.get("stock_like_active")))
        active = stock_like_active or active
        latest_bars = int(
            _finite_float(stock_scope.get("stock_like_with_as_of_bar")),
        )
        missing_bars = int(
            _finite_float(stock_scope.get("stock_like_missing_as_of_bar")),
        )
    run = _mapping_value(discovery, "run")
    run_universe = str(run.get("universe") or "").strip()
    provider = _provider_name(config.daily_market_provider, default="csv")
    ticker_page_cap = max(1, int(config.polygon_tickers_max_pages))
    ticker_page_delay = config.polygon_ticker_page_delay_seconds
    estimated_pages = _estimated_ticker_seed_pages(bar_universe)
    latest_bar_ticker_count = int(
        _finite_float(bar_universe.get("latest_daily_bar_ticker_count"))
    )
    rows: list[dict[str, object]] = []
    source_actions = _priced_in_preflight_source_actions(source_coverage)
    market_bar_operator_step = _mapping_value(
        market_bar_repair or {},
        "operator_step",
    )

    if (active or requested or scanned) < 500:
        cap_note = ""
        seed_action = "Seed the ticker universe before calling this a full-market scan."
        if provider == "polygon":
            if estimated_pages is not None and estimated_pages > ticker_page_cap:
                cap_note = (
                    f"; latest bars contain {latest_bar_ticker_count} tickers; "
                    f"ticker seed cap is {ticker_page_cap}/{estimated_pages} "
                    "estimated page(s)"
                )
                seed_action = (
                    "Set CATALYST_POLYGON_TICKERS_MAX_PAGES to at least "
                    f"{estimated_pages}, then seed tickers."
                )
                if estimated_pages > 5 and ticker_page_delay <= 0:
                    seed_action = (
                        f"{seed_action} Set CATALYST_POLYGON_TICKER_PAGE_DELAY_SECONDS "
                        "if your plan is rate-limited."
                    )
            else:
                cap_note = f"; Polygon/Massive ticker seed cap is {ticker_page_cap} page(s)"
                if ticker_page_cap <= 1:
                    seed_action = (
                        "Raise CATALYST_POLYGON_TICKERS_MAX_PAGES if needed, "
                        "then seed tickers."
                    )
        rows.append(
            _priced_in_preflight_row(
                "universe",
                "blocked",
                (
                    "Only "
                    f"{active or requested or scanned or 0} "
                    f"active/requested securities are visible{cap_note}."
                ),
                seed_action,
                commands.get("ingest_tickers"),
                "POST /api/radar/universe/seed",
            )
        )
    else:
        rows.append(
            _priced_in_preflight_row(
                "universe",
                "ready",
                (
                    f"{active or requested or scanned} "
                    f"{'stock-like ' if stocks_only else ''}securities are available "
                    "for scan scope."
                ),
                (
                    "Keep scanning stock-like rows without a ticker filter."
                    if stocks_only
                    else "Keep scanning without a ticker filter."
                ),
                commands.get("run_scan"),
                "POST /api/radar/runs",
            )
        )

    selected_scan_count = requested or scanned
    if (
        run_universe
        and active >= 500
        and selected_scan_count
        and selected_scan_count < max(1, int(active * 0.9))
    ):
        rows.append(
            _priced_in_preflight_row(
                "scan_scope",
                "attention",
                (
                    f"Latest run scanned {selected_scan_count}/{active} active "
                    f"securities because it used universe={run_universe}."
                ),
                "Run the radar without --universe for an all-active full scan.",
                commands.get("run_scan"),
                "POST /api/radar/runs",
            )
        )

    bar_coverage_ratio = (latest_bars / active) if active else 0.0
    if latest_bars == 0 or (missing_bars and (stocks_only or bar_coverage_ratio < 0.9)):
        provider_reason = str(provider_blocker.get("reason") or "").strip()
        provider_note = (
            f" Latest {provider} market-data failure: {provider_reason}"
            if provider_reason
            else ""
        )
        next_action = (
            "Wait until the provider releases the target daily bars, use the "
            "DB-backed manual bar template/import path, or intentionally upgrade "
            "the provider plan before rerunning."
            if provider_reason
            else (
                (
                    "Generate the stock-only DB-backed missing-bar template, fill "
                    "the stock-like missing rows, import it, then rerun the "
                    "stocks-only priced-in answer."
                )
                if stocks_only
                else (
                    "Generate a DB-backed missing-bar template, fill only the "
                    "missing ticker rows, import it, then rerun the full-market scan. "
                    "Use the provider run only if its call plan and plan limits "
                    "match your intent."
                )
            )
        )
        repair_action = str(market_bar_operator_step.get("action") or "").strip()
        repair_command = str(
            market_bar_operator_step.get("command")
            or market_bar_operator_step.get("after_manual_command")
            or ""
        ).strip()
        market_bar_row = _priced_in_preflight_row(
            "market_bars",
            "blocked",
            (
                f"Run-as-of {'stock-like ' if stocks_only else ''}bar "
                f"coverage is {latest_bars}/{active or 'n/a'}.{provider_note}"
            ),
            repair_action or next_action,
            repair_command or commands.get("market_bars_template"),
            _priced_in_market_bar_operator_api(
                repair_command,
                market_bar_repair or {},
                fallback="POST /api/radar/market-bars/template",
            ),
        )
        if market_bar_operator_step:
            market_bar_row["operator_step"] = _row_dict(market_bar_operator_step)
        rows.append(market_bar_row)
    elif missing_bars:
        repair_action = str(market_bar_operator_step.get("action") or "").strip()
        repair_command = str(
            market_bar_operator_step.get("command")
            or market_bar_operator_step.get("after_manual_command")
            or ""
        ).strip()
        market_bar_row = _priced_in_preflight_row(
            "market_bars",
            "attention",
            (
                f"Run-as-of bars cover {latest_bars}/{active} "
                f"{'stock-like ' if stocks_only else ''}securities."
            ),
            repair_action
            or (
                "Coverage is broad enough for research; generate the "
                "DB-backed missing-bar template if you want the full active "
                "universe covered before relying on the answer."
            ),
            repair_command or commands.get("market_bars_template"),
            _priced_in_market_bar_operator_api(
                repair_command,
                market_bar_repair or {},
                fallback="POST /api/radar/market-bars/template",
            ),
        )
        if market_bar_operator_step:
            market_bar_row["operator_step"] = _row_dict(market_bar_operator_step)
        rows.append(market_bar_row)
    else:
        rows.append(
            _priced_in_preflight_row(
                "market_bars",
                "ready",
                (
                    f"Run-as-of bars cover {latest_bars}/{active or latest_bars} "
                    f"{'stock-like ' if stocks_only else ''}securities."
                ),
                "Use the latest bars in the next scan.",
                commands.get("run_scan"),
                "POST /api/radar/runs",
            )
        )

    provider_by_layer = {str(row.get("layer") or ""): row for row in provider_rows}
    for layer, area in (
        ("News/events", "catalyst_events"),
        ("Schwab portfolio", "broker_context"),
        ("LLM review", "agent_review"),
    ):
        row = provider_by_layer.get(layer, {})
        source_action = source_actions.get(area)
        if source_action:
            rows.append(_priced_in_source_preflight_row(area, source_action))
            continue
        status = str(row.get("status") or "unknown")
        if status == "blocked":
            mapped_status = "blocked"
        elif status in {"live_call_planned", "attention"}:
            mapped_status = "attention"
        else:
            mapped_status = "ready"
        rows.append(
            _priced_in_preflight_row(
                area,
                mapped_status,
                str(row.get("detail") or row.get("mode") or "not configured"),
                str(row.get("next_action") or "Review provider settings."),
                None,
                None,
            )
        )

    for area in ("local_text", "options"):
        source_action = source_actions.get(area)
        if source_action:
            if area == "local_text" and source_actions.get("catalyst_events"):
                source_action = {
                    **_row_dict(source_action),
                    "next_action": (
                        "Fill catalyst_events first, then run local_text batches "
                        "for rows with event text."
                    ),
                }
            rows.append(_priced_in_source_preflight_row(area, source_action))

    call_status = str(call_plan.get("status") or "unknown")
    rows.append(
        _priced_in_preflight_row(
            "run_call_plan",
            "blocked" if call_status == "blocked" else "ready",
            (
                f"{call_status}; max external calls "
                f"{call_plan.get('max_external_call_count')}"
            ),
            str(call_plan.get("next_action") or "Review the run call plan."),
            "catalyst-radar priced-in-queue --json",
            "GET /api/radar/priced-in",
        )
    )
    return rows


def _priced_in_market_bar_operator_api(
    command: str,
    repair: Mapping[str, object],
    *,
    fallback: str,
) -> str:
    command = command.strip()
    if not command:
        return fallback
    if command == str(repair.get("provider_saved_file_validate_command") or ""):
        return str(repair.get("provider_saved_file_validate_api") or fallback)
    if command == str(repair.get("provider_saved_file_import_command") or ""):
        return str(repair.get("provider_saved_file_import_api") or fallback)
    if "market-bars import" in command:
        return str(
            repair.get("import_api")
            or repair.get("manual_import_api")
            or "POST /api/radar/market-bars/import"
        )
    return fallback


def _priced_in_preflight_source_actions(
    source_coverage: Mapping[str, object],
) -> dict[str, Mapping[str, object]]:
    return {
        str(action.get("source") or ""): action
        for action in _sequence_value(source_coverage.get("actions"))
        if isinstance(action, Mapping)
        and str(action.get("status") or "") not in {"ready", "not_applicable"}
    }


def _priced_in_source_preflight_row(
    area: str,
    action: Mapping[str, object],
) -> dict[str, object]:
    status = str(action.get("status") or "unknown")
    row_count = int(_finite_float(action.get("row_count")))
    available = int(_finite_float(action.get("available")))
    gap_count = int(
        _finite_float(action.get("gap_count"))
        or _finite_float(action.get("missing")) + _finite_float(action.get("stale"))
    )
    coverage = action.get("coverage_pct")
    command = (
        action.get("batch_plan_command")
        or action.get("command")
        or action.get("full_scan_gap_review_command")
    )
    api = action.get("batch_plan_api") or action.get("api")
    return _priced_in_preflight_row(
        area,
        "attention" if status in {"partial", "missing", "stale"} else "blocked",
        (
            f"Priced-in source coverage is {available}/{row_count or 'n/a'} "
            f"({coverage}%); gap rows={gap_count}."
        ),
        str(action.get("next_action") or "Review priced-in source coverage."),
        str(command) if command else None,
        str(api) if api else None,
    )


def _priced_in_preflight_row(
    area: str,
    status: str,
    finding: str,
    next_action: str,
    command: str | None,
    api: str | None,
) -> dict[str, object]:
    return {
        "area": area,
        "status": status,
        "finding": finding,
        "next_action": next_action,
        "command": command,
        "api": api,
    }


def _priced_in_preflight_commands(
    config: AppConfig,
    *,
    target_as_of: date | None = None,
    target_ticker_pages: int | None = None,
    stocks_only: bool = False,
) -> dict[str, str]:
    provider = _provider_name(config.daily_market_provider, default="csv")
    target_value = (
        target_as_of.isoformat()
        if target_as_of is not None
        else "<LATEST_TRADING_DATE>"
    )
    if provider == "polygon":
        page_cap = max(1, int(config.polygon_tickers_max_pages))
        target_pages = max(page_cap, target_ticker_pages or page_cap)
        ingest_tickers = (
            "catalyst-radar ingest-polygon tickers "
            f"--max-pages {target_pages} --confirm-external-call"
        )
        ingest_bars = (
            "catalyst-radar ingest-polygon grouped-daily "
            f"--date {target_value} --confirm-external-call"
        )
    else:
        ingest_tickers = (
            "catalyst-radar ingest-csv --securities <securities.csv> --daily-bars <bars.csv>"
        )
        ingest_bars = ingest_tickers
    return {
        "ingest_tickers": ingest_tickers,
        "ingest_bars": ingest_bars,
        "build_universe": (
            f"catalyst-radar build-universe --as-of {target_value} "
            f"--available-at <UTC-now> --name {config.universe_name} "
            f"--provider {provider}"
        ),
        "market_bars_template": _csv_market_template_command(
            target_as_of,
            missing_only=True,
            stocks_only=stocks_only,
        ),
        "market_bars_import_preview": _csv_market_refresh_command(
            target_as_of,
            execute=False,
            stocks_only=stocks_only,
        ),
        "market_bars_import_execute": _csv_market_refresh_command(
            target_as_of,
            execute=True,
            stocks_only=stocks_only,
        ),
        "review_call_plan": "catalyst-radar dashboard-tui --once --page run",
        "run_scan": (
            f"catalyst-radar run-daily --as-of {target_value} "
            f"--available-at <UTC-now> --provider {provider} --json"
        ),
        "run_selected_universe_scan": (
            f"catalyst-radar run-daily --as-of {target_value} "
            f"--available-at <UTC-now> --provider {provider} "
            f"--universe {config.universe_name} --json"
        ),
        "review_queue": (
            "catalyst-radar priced-in-queue --json"
            + (" --stocks-only" if stocks_only else "")
        ),
    }


def _latest_daily_bar_universe_payload(
    engine: Engine,
    *,
    available_at: datetime,
) -> dict[str, object]:
    with engine.connect() as conn:
        latest_date = conn.scalar(
            select(func.max(daily_bars.c.date)).where(daily_bars.c.available_at <= available_at)
        )
        latest_count = 0
        if latest_date is not None:
            latest_count = int(
                conn.scalar(
                    select(func.count(func.distinct(daily_bars.c.ticker))).where(
                        daily_bars.c.available_at <= available_at,
                        daily_bars.c.date == latest_date,
                    )
                )
                or 0
            )
    estimated_pages = ceil(latest_count / 1000) if latest_count else None
    return {
        "latest_daily_bar_date": latest_date.isoformat() if latest_date is not None else None,
        "latest_daily_bar_ticker_count": latest_count,
        "estimated_ticker_seed_pages": estimated_pages,
    }


def _estimated_ticker_seed_pages(source: Mapping[str, object]) -> int | None:
    value = source.get("estimated_ticker_seed_pages")
    if value is None:
        return None
    pages = int(_finite_float(value))
    return pages if pages > 0 else None


def _latest_market_bar_provider_failure(
    engine: Engine,
    *,
    provider: str,
    target_as_of: str | None,
) -> dict[str, object]:
    provider_name = str(provider or "").strip().lower()
    if provider_name in {"", "csv", "off", "none"}:
        return {}
    with engine.connect() as conn:
        rows = conn.execute(
            select(
                job_runs.c.job_type,
                job_runs.c.provider,
                job_runs.c.status,
                job_runs.c.started_at,
                job_runs.c.error_summary,
                job_runs.c.metadata,
            )
            .where(
                job_runs.c.provider == provider_name,
                job_runs.c.status == "failed",
            )
            .order_by(job_runs.c.started_at.desc(), job_runs.c.id.desc())
            .limit(20)
        ).mappings()
        for row in rows:
            metadata = _mapping_value(row, "metadata")
            job_type = str(row.get("job_type") or "")
            if "daily" not in job_type and "bar" not in job_type:
                continue
            date_text = str(metadata.get("date") or "")
            reason = redact_text(str(row.get("error_summary") or "").strip())
            if target_as_of and target_as_of not in date_text and target_as_of not in reason:
                continue
            if not reason:
                continue
            return {
                "provider": provider_name,
                "job_type": job_type,
                "target_as_of": target_as_of,
                "started_at": _iso_or_none(row.get("started_at")),
                "reason": reason,
            }
    return {}


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
        "priced_in_status": row.get("priced_in_status"),
        "priced_in_score": row.get("priced_in_score"),
        "priced_in_direction": row.get("priced_in_direction"),
        "emotion_score": row.get("emotion_score"),
        "reaction_score": row.get("reaction_score"),
        "emotion_reaction_gap": row.get("emotion_reaction_gap"),
        "why_now": _display_priced_in_reason(row)
        or brief.get("why_now")
        or row.get("top_event_title"),
        "top_catalyst": brief.get("top_catalyst") or support.get("title"),
        "evidence": brief.get("supporting_evidence") or support.get("title"),
        "risk_or_gap": _display_priced_in_reason(row)
        or brief.get("risk_or_gap")
        or risk.get("title"),
        "next_step": row.get("decision_next_step")
        or (_display_priced_in_reason(row) and row.get("priced_in_next_step"))
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


def _first_discovery_blocker_for_codes(
    discovery_snapshot: Mapping[str, object] | None,
    codes: frozenset[str],
) -> dict[str, object]:
    snapshot = discovery_snapshot if isinstance(discovery_snapshot, Mapping) else {}
    for blocker in _sequence_value(snapshot.get("blockers")):
        if not isinstance(blocker, Mapping):
            continue
        row = _row_dict(blocker)
        if str(row.get("code") or "") in codes:
            return row
    return {}


def _research_why_now(candidate: Mapping[str, object], *, top_event: object) -> str:
    priced_reason = _display_priced_in_reason(candidate)
    if priced_reason:
        return priced_reason
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


def _display_priced_in_reason(candidate: Mapping[str, object]) -> str:
    status = str(candidate.get("priced_in_status") or "").strip().lower()
    if status in {"", "neutral"}:
        return ""
    return str(candidate.get("priced_in_reason") or "").strip()


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


def _shallow_row_dict(row: Mapping[str, object] | None) -> dict[str, object]:
    if row is None:
        return {}
    return {str(key): value for key, value in row.items()}


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


def _market_context_value(source: object) -> tuple[object, ...]:
    if not isinstance(source, Mapping):
        return ()
    return _sequence_value(source.get("market_context"))


def _budget_ledger_history_row(entry: BudgetLedgerEntry) -> dict[str, object]:
    return {
        "id": entry.id,
        "ts": _as_utc_datetime(entry.ts),
        "available_at": _as_utc_datetime(entry.available_at),
        "ticker": entry.ticker,
        "task": _json_safe(entry.task),
        "model": entry.model,
        "provider": entry.provider,
        "status": _json_safe(entry.status),
        "skip_reason": _json_safe(entry.skip_reason),
        "input_tokens": entry.token_usage.input_tokens,
        "cached_input_tokens": entry.token_usage.cached_input_tokens,
        "output_tokens": entry.token_usage.output_tokens,
        "estimated_cost_usd": entry.estimated_cost,
        "actual_cost_usd": entry.actual_cost,
        "currency": entry.currency,
        "candidate_state": entry.candidate_state,
        "candidate_state_id": entry.candidate_state_id,
        "candidate_packet_id": entry.candidate_packet_id,
        "decision_card_id": entry.decision_card_id,
        "prompt_version": entry.prompt_version,
        "schema_version": entry.schema_version,
        "outcome_label": entry.outcome_label,
    }


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


def _usefulness_layer(
    layer: str,
    status: str,
    current: str,
    next_action: str,
    evidence: str,
) -> dict[str, object]:
    return {
        "layer": layer,
        "status": status,
        "current": current,
        "next_action": next_action,
        "evidence": evidence,
    }


def _agent_loop_usefulness_status(
    llm_step: Mapping[str, object],
    llm_coverage: Mapping[str, object],
) -> str:
    status = str(llm_step.get("status") or "")
    reason = str(llm_step.get("reason") or "")
    category = str(llm_step.get("category") or "")
    mode = str(llm_coverage.get("mode") or "")
    if status == "success" and reason != "dry_run_only":
        return "ready"
    if status == "success" and reason == "dry_run_only":
        return "research"
    if category == "expected_gate" and reason in {
        "no_llm_review_inputs",
        "no_warning_or_higher_candidates",
    }:
        return "research"
    if mode == "enabled":
        return "research"
    return "blocked"


def _agent_loop_usefulness_current(
    llm_step: Mapping[str, object],
    llm_coverage: Mapping[str, object],
) -> str:
    status = str(llm_step.get("status") or "")
    reason = str(llm_step.get("reason") or "")
    if status == "success" and reason == "dry_run_only":
        return "Agent review ran in dry-run mode; no external model call was made."
    if status == "success":
        return "Agent review completed for the latest run."
    if llm_step:
        return str(
            llm_step.get("meaning")
            or llm_step.get("label")
            or "Agent review did not run."
        )
    mode = str(llm_coverage.get("mode") or "disabled")
    return f"LLM review mode is {mode}."


def _agent_loop_usefulness_action(
    llm_step: Mapping[str, object],
    llm_coverage: Mapping[str, object],
) -> str:
    if llm_step.get("operator_action"):
        return str(llm_step.get("operator_action"))
    mode = str(llm_coverage.get("mode") or "")
    if mode == "enabled":
        return "Wait for Warning or manual-review candidates, then run review."
    return "Keep dry-run review for smoke tests, or configure OpenAI credentials and budgets."


def _activation_blocker_detail(rows: Sequence[Mapping[str, object]]) -> str:
    if not rows:
        return "No blocking readiness rows."
    labels = [str(row.get("area") or "Unknown") for row in rows[:3]]
    suffix = f" plus {len(rows) - 3} more" if len(rows) > 3 else ""
    return f"{', '.join(labels)} need attention{suffix}."


def _activation_next_action(rows: Sequence[Mapping[str, object]]) -> str:
    if not rows:
        return "No operator action required."
    actions: list[str] = []
    seen: set[str] = set()
    for row in rows:
        action = str(row.get("next_action") or "").strip()
        if not action or action in seen:
            continue
        seen.add(action)
        actions.append(action.rstrip("."))
        if len(actions) >= 3:
            break
    if len(rows) > 1 and actions:
        return f"Clear {len(rows)} activation blockers: {'; '.join(actions)}."
    if actions:
        return f"{actions[0]}."
    return "Review the readiness checklist."


def _activation_missing_env(config: AppConfig) -> list[str]:
    return [*_market_activation_missing_env(config), *_event_activation_missing_env(config)]


def _missing_env_next_action(missing_env: Sequence[object]) -> str:
    items = [str(item).strip() for item in missing_env if str(item).strip()]
    if not items:
        return "Run one capped radar cycle and inspect readiness."
    listed = "; ".join(items[:4])
    suffix = f"; plus {len(items) - 4} more" if len(items) > 4 else ""
    return f"Set {listed}{suffix}, then run one capped radar cycle."


def _market_activation_missing_env(config: AppConfig) -> list[str]:
    items: list[str] = []
    market_provider = _provider_name(config.daily_market_provider, default="csv")
    if market_provider == "polygon" and not config.polygon_api_key_configured:
        items.append("CATALYST_POLYGON_API_KEY")
    return items


def _event_activation_missing_env(config: AppConfig) -> list[str]:
    items: list[str] = []
    event_provider = _provider_name(config.daily_event_provider, default="news_fixture")
    if event_provider not in {"sec", "sec_submissions"}:
        items.append("CATALYST_DAILY_EVENT_PROVIDER=sec")
    if not config.sec_enable_live:
        items.append("CATALYST_SEC_ENABLE_LIVE=1")
    if not config.sec_user_agent_configured:
        items.append("CATALYST_SEC_USER_AGENT")
    return items


def _sec_activation_next_action(config: AppConfig) -> str:
    missing = [
        item
        for item in _event_activation_missing_env(config)
        if item != "CATALYST_DAILY_EVENT_PROVIDER=sec"
    ]
    if missing:
        return f"Set {' and '.join(missing)} before using SEC scheduled ingest."
    return "Review SEC provider configuration before using scheduled ingest."


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
            "csv",
            configured=_provider_name(config.daily_market_provider, default="csv")
            in {"csv", "sample", "polygon"},
            current=_provider_name(config.daily_market_provider, default="csv"),
        ),
        _activation_env_row(
            "CATALYST_DAILY_PROVIDER",
            "csv",
            configured=_provider_name(config.daily_provider, default="csv")
            in {"csv", "sample", "polygon"},
            current=_provider_name(config.daily_provider, default="csv"),
        ),
        _activation_env_row(
            "CATALYST_POLYGON_API_KEY",
            "<your Polygon API key>",
            configured=config.polygon_api_key_configured,
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
            configured=config.sec_user_agent_configured,
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


def _dotenv_activation_specs(config: AppConfig) -> list[tuple[str, bool]]:
    market_provider = _provider_name(config.daily_market_provider, default="csv")
    daily_provider = _provider_name(config.daily_provider, default="")
    polygon_required = (
        market_provider == "polygon"
        or daily_provider == "polygon"
        or bool(config.polygon_api_key_configured)
    )
    return [
        ("CATALYST_DAILY_MARKET_PROVIDER", True),
        ("CATALYST_DAILY_PROVIDER", False),
        ("CATALYST_POLYGON_API_KEY", polygon_required),
        ("CATALYST_DAILY_EVENT_PROVIDER", True),
        ("CATALYST_SEC_ENABLE_LIVE", True),
        ("CATALYST_SEC_USER_AGENT", True),
        ("CATALYST_POLYGON_TICKERS_MAX_PAGES", False),
        ("CATALYST_SEC_DAILY_MAX_TICKERS", False),
        ("CATALYST_RADAR_RUN_MIN_INTERVAL_SECONDS", False),
    ]


def _dotenv_activation_row(
    config: AppConfig,
    *,
    values: Mapping[str, object],
    key: str,
    required: bool,
) -> dict[str, object]:
    file_has_value = _dotenv_value_set(values.get(key))
    loaded = _dotenv_key_loaded(config, key)
    if loaded:
        status = "loaded"
        action = "No action required."
    elif file_has_value:
        status = "restart_required"
        action = "Restart services so this .env.local value is loaded."
    elif required:
        status = "missing"
        action = "Add this value to .env.local."
    else:
        status = "optional_default"
        action = "Optional; current default is acceptable for a capped first run."
    return {
        "key": key,
        "required": "yes" if required else "no",
        "file": "set" if file_has_value else "missing",
        "loaded": "yes" if loaded else "no",
        "status": status,
        "action": action,
    }


def _dotenv_value_set(value: object) -> bool:
    return value is not None and str(value).strip() != ""


def _dotenv_key_loaded(config: AppConfig, key: str) -> bool:
    if key == "CATALYST_DAILY_MARKET_PROVIDER":
        return _provider_name(config.daily_market_provider, default="csv") in {
            "csv",
            "sample",
            "polygon",
        }
    if key == "CATALYST_DAILY_PROVIDER":
        return _provider_name(config.daily_provider, default="csv") in {
            "csv",
            "sample",
            "polygon",
        }
    if key == "CATALYST_POLYGON_API_KEY":
        return config.polygon_api_key_configured
    if key == "CATALYST_DAILY_EVENT_PROVIDER":
        return config.daily_event_provider.strip().lower() in {"sec", "sec_submissions"}
    if key == "CATALYST_SEC_ENABLE_LIVE":
        return bool(config.sec_enable_live)
    if key == "CATALYST_SEC_USER_AGENT":
        return config.sec_user_agent_configured
    if key == "CATALYST_POLYGON_TICKERS_MAX_PAGES":
        return config.polygon_tickers_max_pages <= 1
    if key == "CATALYST_SEC_DAILY_MAX_TICKERS":
        return config.sec_daily_max_tickers <= 5
    if key == "CATALYST_RADAR_RUN_MIN_INTERVAL_SECONDS":
        return config.radar_run_min_interval_seconds >= 300
    return False


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
            "external_calls": "0 in plan-only mode; one capped radar cycle with -Execute",
            "command": (
                "powershell -ExecutionPolicy Bypass -File "
                "scripts/run-worker-once.ps1"
            ),
        },
        {
            "mode": "one-shot execute",
            "when": "after the plan-only one-shot smoke matches intent",
            "external_calls": "one capped radar cycle",
            "command": (
                "powershell -ExecutionPolicy Bypass -File "
                "scripts/run-worker-once.ps1 -Execute"
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
    purpose: str | None = None,
) -> dict[str, object]:
    return {
        "name": name,
        "value_template": value_template,
        "purpose": purpose or _activation_env_purpose(name),
        "configured": configured,
        "current": "set" if secret and configured else ("missing" if secret else current),
        "secret": secret,
    }


def _activation_env_purpose(name: str) -> str:
    return {
        "CATALYST_DAILY_MARKET_PROVIDER": (
            "Scheduled daily bar provider used by worker and normal radar ingest."
        ),
        "CATALYST_DAILY_PROVIDER": (
            "Manual/default radar-run provider; keep aligned with the scheduled provider."
        ),
        "CATALYST_POLYGON_API_KEY": (
            "Polygon credential for ticker seeding and live daily bars."
        ),
        "CATALYST_POLYGON_TICKERS_MAX_PAGES": (
            "Hard cap for the first Polygon ticker-reference seed."
        ),
        "CATALYST_DAILY_EVENT_PROVIDER": "Scheduled catalyst/event provider.",
        "CATALYST_SEC_ENABLE_LIVE": "Explicit switch that permits live SEC reads.",
        "CATALYST_SEC_USER_AGENT": "SEC-compliant contact string for live SEC requests.",
        "CATALYST_SEC_DAILY_MAX_TICKERS": "Hard cap for SEC submissions per radar run.",
        "CATALYST_RADAR_RUN_MIN_INTERVAL_SECONDS": (
            "Cooldown that prevents repeated dashboard/API radar runs."
        ),
        "CATALYST_ENABLE_PREMIUM_LLM": "Optional switch for real model-backed review.",
        "CATALYST_LLM_PROVIDER": "Model provider for optional agent review.",
        "CATALYST_LLM_SKEPTIC_MODEL": "Model name for skeptic_review tasks.",
        "OPENAI_API_KEY": "OpenAI credential for optional real agent review.",
        "CATALYST_LLM_INPUT_COST_PER_1M": "Pricing guardrail for LLM input tokens.",
        "CATALYST_LLM_CACHED_INPUT_COST_PER_1M": (
            "Pricing guardrail for cached LLM input tokens."
        ),
        "CATALYST_LLM_OUTPUT_COST_PER_1M": "Pricing guardrail for LLM output tokens.",
        "CATALYST_LLM_PRICING_UPDATED_AT": "Date used to flag stale model pricing.",
        "CATALYST_LLM_DAILY_BUDGET_USD": "Daily dollar cap for optional LLM review.",
        "CATALYST_LLM_MONTHLY_BUDGET_USD": "Monthly dollar cap for optional LLM review.",
        "CATALYST_LLM_TASK_DAILY_CAPS": "Per-task daily call cap for optional LLM review.",
    }.get(name, "Activation setting.")


def _live_data_safe_limits(config: AppConfig) -> list[dict[str, object]]:
    return [
        {
            "guardrail": "Optional Polygon universe seed cap",
            "value": f"{max(1, int(config.polygon_tickers_max_pages))} page(s)",
            "reason": "Only applies after Polygon is configured for broad discovery.",
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
    market_provider = _provider_name(config.daily_market_provider, default="csv")
    polygon_enabled = market_provider == "polygon" and config.polygon_api_key_configured
    seed_pages = max(1, int(config.polygon_tickers_max_pages)) if polygon_enabled else 0
    market_calls = 1 if polygon_enabled else 0
    sec_calls = max(1, int(config.sec_daily_max_tickers))
    return [
        {
            "step": 1,
            "status": "manual",
            "action": "Prepare safe non-secret live defaults in .env.local.",
            "external_calls": 0,
            "command": (
                "powershell -ExecutionPolicy Bypass -File "
                "scripts/prepare-live-env.ps1"
            ),
        },
        {
            "step": 2,
            "status": env_status,
            "action": (
                "Open .env.local, fill the remaining manual values, and do not paste "
                "keys into chat."
            ),
            "external_calls": 0,
            "command": (
                "powershell -ExecutionPolicy Bypass -File "
                "scripts/open-live-env.ps1"
            ),
        },
        {
            "step": 3,
            "status": "manual",
            "action": "Restart the local API and dashboard so the new env is loaded.",
            "external_calls": 0,
            "command": (
                "powershell -ExecutionPolicy Bypass -File "
                "scripts/restart-local.ps1"
            ),
        },
        {
            "step": 4,
            "status": "safe_check",
            "action": (
                "Run the first-live-smoke preflight in plan-only mode before any "
                "provider call."
            ),
            "external_calls": 0,
            "command": (
                "powershell -ExecutionPolicy Bypass -File "
                "scripts/run-first-live-smoke.ps1"
            ),
        },
        {
            "step": 5,
            "status": "manual",
            "action": (
                "Execute one capped live smoke only after the plan-only preflight "
                "matches intent."
            ),
            "external_calls": seed_pages + market_calls + sec_calls,
            "command": (
                "powershell -ExecutionPolicy Bypass -File "
                "scripts/run-first-live-smoke.ps1 -Execute"
            ),
        },
        {
            "step": 6,
            "status": "safe_check",
            "action": "Review readiness and the research shortlist before any investment work.",
            "external_calls": 0,
            "command": _local_api_curl_command("GET", "/api/radar/readiness"),
        },
    ]


def _local_api_curl_command(
    method: str,
    path: str,
    *,
    body: str | None = None,
) -> str:
    command = (
        "curl.exe --insecure --fail --silent --show-error "
        f"--request {method} https://127.0.0.1:8443{path}"
    )
    if body is None:
        return command
    return f'{command} --header "Content-Type: application/json" --data \'{body}\''


def _live_data_call_budget_if_activated(config: AppConfig) -> list[dict[str, object]]:
    market_provider = _provider_name(config.daily_market_provider, default="csv")
    polygon_enabled = market_provider == "polygon" and config.polygon_api_key_configured
    seed_pages = max(1, int(config.polygon_tickers_max_pages)) if polygon_enabled else 0
    market_calls = 1 if polygon_enabled else 0
    sec_calls = max(1, int(config.sec_daily_max_tickers))
    return [
        {
            "operation": "read this activation contract",
            "max_external_calls": 0,
            "provider": "none",
        },
        {
            "operation": "seed Polygon universe once",
            "max_external_calls": seed_pages,
            "provider": "polygon" if polygon_enabled else "polygon (optional)",
        },
        {
            "operation": "run one radar cycle",
            "max_external_calls": market_calls + sec_calls,
            "provider": f"{market_provider if polygon_enabled else 'csv'} + sec",
        },
    ]


def _telemetry_tape_status(rows: Sequence[Mapping[str, object]]) -> dict[str, object]:
    if not rows:
        return {
            "status": "empty",
            "headline": "No recent telemetry events.",
            "next_action": "Run a radar cycle before reviewing telemetry.",
            "attention_count": 0,
            "guarded_count": 0,
            "evidence": "events=0",
        }
    categories = [_telemetry_row_category(row) for row in rows]
    attention_count = sum(1 for category in categories if category == "attention")
    guarded_count = sum(1 for category in categories if category == "guarded")
    latest_category = categories[0]
    latest = rows[0]
    latest_event = str(latest.get("event") or "unknown")
    latest_status = str(latest.get("status") or "unknown")
    latest_reason = str(latest.get("reason") or "n/a") or "n/a"
    if latest_category == "attention":
        status = "attention"
        headline = "Latest telemetry event needs attention."
        next_action = "Inspect the latest event and resolve the failed or rejected operation."
    elif latest_category == "guarded":
        status = "guarded"
        headline = "Latest telemetry event is a safety guard."
        next_action = "Wait for the guard cooldown or adjust the operation cadence deliberately."
    else:
        status = "ready"
        headline = "Latest telemetry event is healthy."
        next_action = (
            "Review older attention events only if they explain current operator behavior."
            if attention_count
            else "No telemetry action required."
        )
    return {
        "status": status,
        "headline": headline,
        "next_action": next_action,
        "attention_count": attention_count,
        "guarded_count": guarded_count,
        "evidence": (
            f"latest={latest_event}; latest_status={latest_status}; "
            f"latest_reason={latest_reason}; attention={attention_count}; "
            f"guarded={guarded_count}"
        ),
    }


def _telemetry_pair_domain(
    *,
    name: str,
    required: bool,
    event_counts: Mapping[str, int],
    last_seen_by_type: Mapping[str, datetime],
    requested: str,
    terminals: Sequence[str],
    ready_action: str,
    missing_action: str,
) -> dict[str, object]:
    has_requested = int(event_counts.get(requested, 0)) > 0
    has_terminal = any(int(event_counts.get(event_type, 0)) > 0 for event_type in terminals)
    event_types = [requested, *terminals]
    if has_requested and has_terminal:
        status = "ready"
        missing_events: list[str] = []
    elif has_requested or has_terminal:
        status = "attention"
        missing_events = []
        if not has_requested:
            missing_events.append(requested)
        if not has_terminal:
            missing_events.append("one_of:" + ",".join(terminals))
    else:
        status = "missing" if required else "waiting"
        missing_events = [requested, "one_of:" + ",".join(terminals)]
    return _telemetry_coverage_domain(
        name=name,
        required=required,
        status=status,
        event_counts=event_counts,
        last_seen_by_type=last_seen_by_type,
        event_types=event_types,
        missing_events=missing_events,
        ready_action=ready_action,
        missing_action=missing_action,
    )


def _telemetry_coverage_domain(
    *,
    name: str,
    required: bool,
    status: str,
    event_counts: Mapping[str, int],
    last_seen_by_type: Mapping[str, datetime],
    event_types: Sequence[str],
    missing_events: Sequence[str],
    ready_action: str,
    missing_action: str,
) -> dict[str, object]:
    visible_event_types = [event_type for event_type in event_types if event_type]
    event_count = sum(int(event_counts.get(event_type, 0)) for event_type in visible_event_types)
    last_seen = _latest_datetime(
        [last_seen_by_type.get(event_type) for event_type in visible_event_types]
    )
    if status == "ready":
        operator_action = ready_action
    elif status == "waiting":
        operator_action = missing_action
    else:
        operator_action = missing_action
    return {
        "domain": name,
        "status": status,
        "required": required,
        "event_count": event_count,
        "last_seen_at": _iso_or_none(last_seen),
        "events_seen": [
            event_type.removeprefix("telemetry.")
            for event_type in visible_event_types
            if int(event_counts.get(event_type, 0)) > 0
        ],
        "missing_events": [_telemetry_missing_event_label(event) for event in missing_events],
        "operator_action": operator_action,
        "evidence": (
            f"events={event_count}; last_seen={_iso_or_none(last_seen) or 'n/a'}; "
            f"required={'yes' if required else 'no'}"
        ),
    }


def _telemetry_missing_event_label(event: str) -> str:
    if event.startswith("one_of:"):
        choices = [
            choice.removeprefix("telemetry.")
            for choice in event.removeprefix("one_of:").split(",")
            if choice
        ]
        return "one_of:" + ",".join(choices)
    return event.removeprefix("telemetry.")


def _latest_datetime(values: object) -> datetime | None:
    parsed: list[datetime] = []
    if isinstance(values, Sequence) and not isinstance(values, str | bytes):
        iterable = values
    else:
        iterable = [values]
    for value in iterable:
        resolved = _parse_utc_datetime(value)
        if resolved is not None:
            parsed.append(resolved)
    return max(parsed) if parsed else None


def _telemetry_rollup_rows(rows: Sequence[Mapping[str, object]]) -> list[dict[str, object]]:
    grouped: dict[str, dict[str, object]] = {}
    for row in rows:
        category = _telemetry_row_category(row)
        group = grouped.setdefault(category, {"count": 0, "latest": row})
        group["count"] = int(group["count"]) + 1

    visible: list[dict[str, object]] = []
    for category in ("attention", "guarded", "ready"):
        group = grouped.get(category)
        if not group:
            continue
        latest = group["latest"]
        latest_row = latest if isinstance(latest, Mapping) else {}
        visible.append(
            {
                "category": _telemetry_category_label(category),
                "count": group["count"],
                "latest_event": latest_row.get("event") or "n/a",
                "latest_status": latest_row.get("outcome")
                or latest_row.get("status")
                or "n/a",
                "latest_reason": latest_row.get("reason") or "n/a",
                "latest_at": latest_row.get("occurred_at") or "n/a",
                "operator_action": _telemetry_category_action(category),
            }
        )
    return visible


def _telemetry_category_label(category: str) -> str:
    return {
        "attention": "Needs attention",
        "guarded": "Safety guard",
        "ready": "Healthy",
    }.get(category, category.replace("_", " ").title())


def _telemetry_category_action(category: str) -> str:
    return {
        "attention": "Inspect the latest failed, rejected, or blocked operation.",
        "guarded": "Wait for the cooldown or deliberately adjust the operation cadence.",
        "ready": "No telemetry action required.",
    }.get(category, "Review the latest telemetry rows.")


def _telemetry_row_category(row: Mapping[str, object]) -> str:
    event = str(row.get("event") or "")
    status = str(row.get("status") or "").strip().lower()
    reason = str(row.get("reason") or "").strip().lower()
    if event.endswith(".rate_limited") or reason == "rate_limited":
        return "guarded"
    if status in {
        "failed",
        "rejected",
        "blocked",
        "blocked_input",
        "needs_review",
    }:
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
            (
                "audit_state=raw record retained"
                if raw_status == "skipped"
                else f"raw_status={raw_status}"
            ),
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


def _database_context_payload(database_url: str) -> dict[str, object]:
    raw = str(database_url or "")
    fingerprint = sha256(raw.encode("utf-8")).hexdigest()[:10] if raw else "unknown"
    if raw.startswith("sqlite:///"):
        path_text = raw.removeprefix("sqlite:///")
        name = Path(path_text).name or "sqlite"
        return {
            "kind": "sqlite",
            "name": name,
            "location": path_text,
            "fingerprint": fingerprint,
        }
    scheme = raw.split("://", maxsplit=1)[0] if "://" in raw else "configured"
    return {
        "kind": scheme,
        "name": f"{scheme} database",
        "location": f"{scheme}://<configured>",
        "fingerprint": fingerprint,
    }


def _radar_step_root_cause_group(
    step: Mapping[str, object],
    config: AppConfig,
) -> dict[str, object]:
    reason = str(step.get("reason") or "")
    category = str(
        step.get("category")
        or classify_step_outcome(str(step.get("status") or ""), reason or None).category
    )
    if reason == "no_scheduled_provider_input":
        return {
            "root_cause": "Scheduled market input disabled",
            "status": "blocked",
            "why": (
                "The run did not schedule a market-data provider, so fresh bars were "
                "unavailable."
            ),
            "current_config": (
                f"CATALYST_DAILY_MARKET_PROVIDER={config.daily_market_provider or 'unset'}"
            ),
            "next_action": (
                "Set CATALYST_DAILY_MARKET_PROVIDER=polygon and CATALYST_POLYGON_API_KEY, "
                "then run one capped radar cycle."
            ),
            "evidence": _step_config_evidence(step),
        }
    if reason == "no_scheduled_event_provider":
        return {
            "root_cause": "Scheduled event input disabled",
            "status": "blocked",
            "why": (
                "The run did not schedule a news/filing provider, so catalyst text "
                "was unavailable."
            ),
            "current_config": (
                f"CATALYST_DAILY_EVENT_PROVIDER={config.daily_event_provider or 'unset'}"
            ),
            "next_action": (
                "Set CATALYST_DAILY_EVENT_PROVIDER=sec, CATALYST_SEC_ENABLE_LIVE=1, "
                "and CATALYST_SEC_USER_AGENT."
            ),
            "evidence": _step_config_evidence(step),
        }
    if reason == "no_text_inputs":
        return {
            "root_cause": "No catalyst text available",
            "status": "attention",
            "why": "No events or local text snippets were available for triage.",
            "current_config": (
                f"event_provider={config.daily_event_provider or 'unset'}; "
                f"SEC live={'yes' if config.sec_enable_live else 'no'}"
            ),
            "next_action": "Configure SEC/news ingestion or add local text snippets.",
            "evidence": _step_config_evidence(step),
        }
    if reason.startswith("degraded_mode_blocks"):
        return {
            "root_cause": "Degraded mode protected high-state work",
            "status": "blocked",
            "why": (
                "The system refused to build high-conviction research artifacts because "
                "current inputs were not trusted."
            ),
            "current_config": "ops degraded mode is enabled for this run",
            "next_action": "Fix the degraded-mode reason, rerun, then rely on packet/card output.",
            "evidence": _degraded_step_evidence(step),
        }
    if reason == "llm_disabled":
        return {
            "root_cause": "Agent review disabled",
            "status": "optional",
            "why": "The run did not request the optional agent-review gate.",
            "current_config": (
                f"CATALYST_LLM_PROVIDER={config.llm_provider or 'none'}; "
                f"OPENAI_API_KEY={'set' if config.openai_api_key else 'unset'}"
            ),
            "next_action": (
                "Use dry-run review for smoke tests, or configure OpenAI credentials, "
                "pricing, budgets, and task caps for real review."
            ),
            "evidence": _step_config_evidence(step),
        }
    if category == "expected_gate":
        return {
            "root_cause": "Expected optional gate did not trigger",
            "status": "optional",
            "why": (
                step.get("meaning")
                or classify_step_outcome(str(step.get("status") or ""), reason or None).meaning
                or "This optional gate had no trigger in the run."
            ),
            "current_config": "required scan path can still be complete",
            "next_action": (
                step.get("operator_action")
                or classify_step_outcome(
                    str(step.get("status") or ""),
                    reason or None,
                ).operator_action
                or "No action required unless this optional gate was expected."
            ),
            "evidence": _step_config_evidence(step),
        }
    if category == "not_ready":
        return {
            "root_cause": "Input not ready",
            "status": "attention",
            "why": (
                step.get("meaning")
                or classify_step_outcome(str(step.get("status") or ""), reason or None).meaning
                or "The step had no usable input."
            ),
            "current_config": "input-dependent step",
            "next_action": (
                step.get("operator_action")
                or classify_step_outcome(
                    str(step.get("status") or ""),
                    reason or None,
                ).operator_action
                or "Add the missing upstream input and rerun."
            ),
            "evidence": _step_config_evidence(step),
        }
    return {
        "root_cause": "Step needs review",
        "status": "review",
        "why": (
            step.get("meaning")
            or classify_step_outcome(str(step.get("status") or ""), reason or None).meaning
            or "The step recorded a non-completed outcome."
        ),
        "current_config": "see raw telemetry",
        "next_action": "Inspect the raw step payload and upstream telemetry.",
        "evidence": _step_config_evidence(step),
    }


def _step_config_evidence(step: Mapping[str, object]) -> str:
    payload = _mapping_value(step, "payload")
    provider = (
        payload.get("provider")
        or payload.get("scheduled_provider")
        or payload.get("scheduled_event_provider")
    )
    parts = [
        f"reason={step.get('reason')}" if step.get("reason") else "",
        f"provider={provider}" if provider else "",
        f"requested={int(_finite_float(step.get('requested_count')))}",
        f"raw={int(_finite_float(step.get('raw_count')))}",
        f"normalized={int(_finite_float(step.get('normalized_count')))}",
    ]
    return "; ".join(part for part in parts if part) or "n/a"


def _degraded_step_evidence(step: Mapping[str, object]) -> str:
    payload = _mapping_value(step, "payload")
    degraded = _mapping_value(payload, "degraded_mode")
    reasons = ", ".join(str(value) for value in _sequence_value(degraded.get("reasons")))
    max_action_state = degraded.get("max_action_state")
    parts = [
        f"reason={step.get('reason')}" if step.get("reason") else "",
        f"degraded_reasons={reasons}" if reasons else "",
        f"max_action_state={max_action_state}" if max_action_state else "",
        f"requested={int(_finite_float(step.get('requested_count')))}",
    ]
    return "; ".join(part for part in parts if part) or _step_config_evidence(step)


def _root_cause_rank(status: str) -> int:
    return {
        "blocked": 0,
        "attention": 1,
        "review": 2,
        "optional": 3,
    }.get(status, 9)


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
        "priced_in_status": row.get("priced_in_status"),
        "priced_in_score": row.get("priced_in_score"),
        "priced_in_direction": row.get("priced_in_direction"),
        "emotion_score": row.get("emotion_score"),
        "reaction_score": row.get("reaction_score"),
        "emotion_reaction_gap": row.get("emotion_reaction_gap"),
        "priced_in_reason": row.get("priced_in_reason"),
        "priced_in_next_step": row.get("priced_in_next_step"),
        "top_catalyst": brief.get("top_catalyst")
        or row.get("top_event_title")
        or support.get("title"),
        "risk_or_gap": _display_priced_in_reason(row)
        or brief.get("risk_or_gap")
        or risk.get("title"),
        "decision_card_id": row.get("decision_card_id"),
        "next_step": (
            row.get("decision_next_step")
            or (_display_priced_in_reason(row) and row.get("priced_in_next_step"))
            or brief.get("next_step")
        ),
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
        rows.append(_shallow_row_dict(row))
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
        rows.append(_shallow_row_dict(row))
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
        (row for row in candidates if isinstance(row, Mapping)),
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
        rows.append(_shallow_row_dict(row))
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
        if "CATALYST_POLYGON_API_KEY" in reason or not config.polygon_api_key_configured:
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
    as_of_bar_coverage: Mapping[str, object],
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
                "Market data is still local CSV/fixture-backed.",
                _csv_market_refresh_discovery_action(as_of_date),
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
    latest_bar_coverage_count = int(
        _finite_float(
            database.get(
                "active_security_with_latest_daily_bar_count",
                database.get("active_security_with_daily_bar_count"),
            )
        )
    )
    missing_latest_bar_tickers = [
        str(ticker)
        for ticker in _sequence_value(database.get("missing_latest_daily_bar_tickers"))
        if str(ticker).strip()
    ]
    if active_count and active_count < 100:
        blockers.append(
            _discovery_blocker(
                "thin_universe",
                f"Only {active_count} active securities are loaded.",
                "Seed or refresh the universe before treating discovery as broad.",
            )
        )
    if as_of_date is not None and latest_bar_date is not None and latest_bar_date < as_of_date:
        stale_finding = (
            f"Latest daily bars are {latest_bar_date.isoformat()}, older than run as-of."
        )
        missing_as_of_count = int(_finite_float(as_of_bar_coverage.get("missing_count")))
        active_as_of_count = int(
            _finite_float(as_of_bar_coverage.get("active_security_count"))
        )
        missing_as_of_tickers = [
            str(ticker)
            for ticker in _sequence_value(
                as_of_bar_coverage.get("missing_tickers")
            )
            if str(ticker).strip()
        ]
        if active_as_of_count:
            stale_finding = (
                f"{stale_finding} As-of coverage: "
                f"{active_as_of_count - missing_as_of_count}/{active_as_of_count} "
                "active securities."
            )
        if missing_as_of_tickers:
            stale_finding = (
                f"{stale_finding} Missing: {', '.join(missing_as_of_tickers[:6])}."
            )
        blockers.append(
            _discovery_blocker(
                "stale_daily_bars",
                stale_finding,
                _csv_market_refresh_next_action(as_of_date),
            )
        )
    if (
        active_count
        and latest_bar_date is not None
        and latest_bar_coverage_count < active_count
    ):
        missing_sample = (
            f" Missing: {', '.join(missing_latest_bar_tickers[:6])}."
            if missing_latest_bar_tickers
            else ""
        )
        blockers.append(
            _discovery_blocker(
                "incomplete_daily_bar_coverage",
                (
                    f"{latest_bar_coverage_count} of {active_count} active "
                    "securities have bars on the latest daily-bar date "
                    f"{latest_bar_date.isoformat()}."
                    f"{missing_sample}"
                ),
                _csv_market_refresh_next_action(as_of_date),
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


def _csv_market_refresh_command(
    as_of_date: date | None,
    *,
    daily_bars_path: str | Path | None = None,
    execute: bool = True,
    stocks_only: bool = False,
    complete_rows_only: bool = False,
) -> str:
    expected_value = (
        as_of_date.isoformat()
        if as_of_date is not None
        else "<LATEST_TRADING_DATE>"
    )
    daily_bars_value = str(daily_bars_path) if daily_bars_path else "<fresh-bars.csv>"
    execute_flag = " --execute" if execute else ""
    stocks_flag = " --stocks-only" if stocks_only else ""
    complete_rows_flag = " --complete-rows-only" if complete_rows_only else ""
    return (
        f"catalyst-radar market-bars import --daily-bars {daily_bars_value}"
        f" --expected-as-of {expected_value}{stocks_flag}"
        f"{complete_rows_flag}{execute_flag}"
    )


def _csv_market_template_command(
    as_of_date: date | None,
    *,
    missing_only: bool = False,
    stocks_only: bool = False,
) -> str:
    expected = (
        as_of_date.isoformat()
        if as_of_date is not None
        else "<LATEST_TRADING_DATE>"
    )
    missing_flag = " --missing-only" if missing_only else ""
    stocks_flag = " --stocks-only" if stocks_only else ""
    return (
        "catalyst-radar market-bars template "
        f"--expected-as-of {expected} "
        f"--out {_csv_market_template_path(as_of_date, stocks_only=stocks_only)}"
        f"{missing_flag}{stocks_flag}"
    )


def _csv_market_template_path(
    as_of_date: date | None,
    *,
    stocks_only: bool = False,
) -> str:
    expected = (
        as_of_date.isoformat()
        if as_of_date is not None
        else "<LATEST_TRADING_DATE>"
    )
    filename_prefix = "manual-stock-bars" if stocks_only else "manual-bars"
    return f"data\\local\\{filename_prefix}-{expected}.csv"


def _csv_market_refresh_next_action(as_of_date: date | None) -> str:
    template_path = _csv_market_template_path(as_of_date)
    template_command = _csv_market_template_command(as_of_date, missing_only=True)
    import_preview_command = _csv_market_refresh_command(
        as_of_date,
        daily_bars_path=template_path,
        execute=False,
        complete_rows_only=True,
    )
    return (
        "Use SEC-only results for research only; fill "
        f"`{template_path}`. If it is missing, generate it with "
        f"`{template_command}`. Then preview complete rows with "
        f"`{import_preview_command}` or configure a live market provider before "
        "acting."
    )


def _csv_market_refresh_discovery_action(as_of_date: date | None) -> str:
    template_path = _csv_market_template_path(as_of_date)
    template_command = _csv_market_template_command(as_of_date, missing_only=True)
    import_preview_command = _csv_market_refresh_command(
        as_of_date,
        daily_bars_path=template_path,
        execute=False,
        complete_rows_only=True,
    )
    return (
        "Use SEC-only results for research only; fill "
        f"`{template_path}`. If it is missing, generate it with "
        f"`{template_command}`. Then preview complete rows with "
        f"`{import_preview_command}` or configure a live market provider before "
        "relying on broad discovery."
    )


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
        "Good enough for SEC-only smoke; add Polygon later for fresh broad-market coverage.",
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
        missing = [
            item
            for item in _event_activation_missing_env(config)
            if item != "CATALYST_DAILY_EVENT_PROVIDER=sec"
        ]
        missing_label = "; ".join(missing) if missing else "required SEC settings"
        return _preflight_row(
            "News/events",
            "blocked",
            provider,
            f"0 live calls until missing SEC settings are set: {missing_label}",
            f"{sec_budget}; SEC live ingest fails closed without required settings.",
            _sec_activation_next_action(config),
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
            "Use for SEC-only validation; configure Polygon later for live market discovery.",
        )
    if provider == "polygon":
        if not config.polygon_api_key_configured:
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


def _scan_provider_alignment_call_plan_row(
    config: AppConfig,
    *,
    provider: str | None,
) -> dict[str, object]:
    scheduled_provider = _provider_name(config.daily_market_provider, default="csv")
    override = str(provider or "").strip().lower()
    if not override:
        return _call_plan_row(
            "Scan provider",
            "aligned",
            scheduled_provider,
            "feature_scan",
            0,
            (
                "Feature scanning derives its provider from "
                f"CATALYST_DAILY_MARKET_PROVIDER={scheduled_provider}."
            ),
            "No action required unless you intentionally override the scan provider.",
        )
    if override != scheduled_provider:
        return _call_plan_row(
            "Scan provider",
            "blocked",
            override,
            "feature_scan",
            0,
            (
                f"Provider override {override} does not match scheduled market "
                f"provider {scheduled_provider}."
            ),
            (
                "Remove the provider override, or align CATALYST_DAILY_MARKET_PROVIDER "
                "and CATALYST_DAILY_PROVIDER before running."
            ),
        )
    return _call_plan_row(
        "Scan provider",
        "aligned",
        override,
        "feature_scan",
        0,
        "Provider override matches the scheduled market-data provider.",
        "No action required.",
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
        missing_sec = [
            item
            for item in _event_activation_missing_env(config)
            if item != "CATALYST_DAILY_EVENT_PROVIDER=sec"
        ]
        if missing_sec:
            return _call_plan_row(
                "News/events",
                "blocked",
                provider,
                "submissions",
                0,
                f"SEC provider is selected, but {', '.join(missing_sec)} is missing.",
                _sec_activation_next_action(config),
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
                "Load securities with CIK metadata through CSV metadata or optional Polygon seed.",
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
    if provider == "polygon" and not config.polygon_api_key_configured:
        return "missing_credentials"
    return _source_mode(provider, fixture_names={"csv", "sample"})


def _event_source_mode(config: AppConfig, provider: str) -> str:
    if provider in {"sec", "sec_submissions"} and (
        not config.sec_enable_live or not config.sec_user_agent_configured
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


def _positive_offset(value: int) -> int:
    return max(0, int(value))


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


def _radar_payload_step_rows(value: object) -> tuple[dict[str, object], ...]:
    if isinstance(value, Mapping):
        rows = value.values()
    elif isinstance(value, Sequence) and not isinstance(value, str | bytes):
        rows = value
    else:
        rows = ()
    return tuple(_row_dict(row) for row in rows if isinstance(row, Mapping))


def _radar_payload_step_blocks_reliance(step: Mapping[str, object]) -> bool:
    status = str(step.get("status") or "")
    reason = step.get("reason")
    reason_text = str(reason) if reason is not None else None
    classification = classify_step_outcome(status, reason_text)
    category = str(step.get("category") or classification.category)
    return bool(step.get("blocks_reliance", classification.blocks_reliance)) or category in {
        "blocked_input",
        "failed",
        "needs_review",
    }


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

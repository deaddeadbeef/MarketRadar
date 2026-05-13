from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import fields, is_dataclass
from datetime import UTC, date, datetime, timedelta
from enum import Enum
from math import isfinite
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
from catalyst_radar.jobs.step_outcomes import (
    StepOutcomeClassification,
    classify_step_outcome,
)
from catalyst_radar.jobs.tasks import DAILY_STEP_ORDER
from catalyst_radar.storage.broker_repositories import BrokerRepository
from catalyst_radar.storage.budget_repositories import BudgetLedgerRepository
from catalyst_radar.storage.schema import (
    alerts,
    candidate_packets,
    candidate_states,
    decision_cards,
    events,
    job_runs,
    paper_trades,
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


def load_candidate_rows(
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

    ranked_packet_stmt = (
        select(
            candidate_packets.c.id,
            candidate_packets.c.candidate_state_id,
            candidate_packets.c.available_at,
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
        rows.append(
            {
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
        )
    return rows


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
    outcome_counts = Counter(
        _radar_run_step_classification(row).category for row in ordered_steps
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
        "status_counts": dict(sorted(status_counts.items())),
        "outcome_category_counts": dict(sorted(outcome_counts.items())),
        "blocking_step_count": outcome_counts.get("blocked_input", 0)
        + outcome_counts.get("failed", 0)
        + outcome_counts.get("needs_review", 0),
        "expected_gate_count": outcome_counts.get("expected_gate", 0),
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
                "meaning": _radar_run_step_classification(row).meaning,
                "operator_action": _radar_run_step_classification(row).operator_action,
                "blocks_reliance": _radar_run_step_classification(row).blocks_reliance,
                "payload": _radar_run_step_payload(row),
            }
            for row in ordered_steps
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
    cutoff = _parse_utc_datetime(summary.get("decision_available_at"))
    health = (
        _row_dict(ops_health)
        if isinstance(ops_health, Mapping)
        else load_ops_health(engine, now=cutoff)
    )
    unscoped_candidates = (
        [_row_dict(row) for row in candidate_rows]
        if candidate_rows is not None
        else load_candidate_rows(engine, available_at=cutoff)
    )
    steps = _radar_steps_by_name(summary)
    candidates = _discovery_run_candidates(unscoped_candidates, summary)
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
    as_of_date = _parse_date(summary.get("as_of"))
    latest_bar_date = _parse_date(database.get("latest_daily_bar_date"))
    latest_candidate_at = _latest_candidate_as_of(candidates)

    candidate_count = len(candidates)
    packet_count = _step_metric(
        steps,
        "candidate_packets",
        "normalized_count",
        default=sum(1 for row in candidates if row.get("candidate_packet_id")),
    )
    card_count = _step_metric(
        steps,
        "decision_cards",
        "normalized_count",
        default=sum(1 for row in candidates if row.get("decision_card_id")),
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
            "latest_candidate_age_days": _age_days(cutoff, latest_candidate_at),
        },
        "yield": {
            "requested_securities": requested_count,
            "scanned_securities": scanned_count,
            "candidate_states": candidate_count,
            "candidate_packets": packet_count,
            "decision_cards": card_count,
        },
        "blockers": blockers,
        "top_discoveries": [
            _discovery_candidate(row)
            for row in (
                candidates[: max(0, int(limit))]
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
    elif llm_mode == "missing_credentials":
        rows.append(
            _readiness_row(
                "LLM review",
                "blocked",
                "OpenAI review is enabled, but OPENAI_API_KEY is missing.",
                "Set OPENAI_API_KEY before running real agentic LLM review.",
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
        rows.append(
            _readiness_row(
                "Alerting",
                "optional",
                "No alerts were generated in the latest run.",
                "Use candidates/research briefs for manual triage or tune alert thresholds.",
                _step_evidence("digest", digest_step),
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
    required_total = max(0, step_count - expected_gate_count)
    return {
        "required_total": required_total,
        "required_complete": min(outcome_counts.get("completed", 0), required_total),
        "blocking_count": int(_finite_float(summary.get("blocking_step_count"))),
        "expected_gate_count": expected_gate_count,
    }


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
    values = [
        parsed
        for row in candidates
        if (parsed := _parse_utc_datetime(row.get("as_of"))) is not None
    ]
    return max(values) if values else None


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


def _iso_or_na(value: datetime | None) -> str:
    return _iso_or_none(value) or "n/a"


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
    if mode == "disabled":
        status = "optional"
        call_budget = "0 LLM calls"
        next_action = "Enable OpenAI review only after model, budget, and key setup."
    elif mode == "missing_credentials":
        status = "blocked"
        call_budget = "0 LLM calls until OPENAI_API_KEY is set"
        next_action = "Set OPENAI_API_KEY, then run one dry-run review before real review."
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
    metadata = row.get("metadata")
    if isinstance(metadata, Mapping):
        category = metadata.get("outcome_category")
        label = metadata.get("outcome_label")
        if isinstance(category, str) and isinstance(label, str):
            inferred = classify_step_outcome(
                str(row.get("status") or ""),
                _radar_run_step_reason(row),
            )
            return StepOutcomeClassification(
                category=category,
                label=label,
                meaning=(
                    str(metadata.get("outcome_meaning"))
                    if metadata.get("outcome_meaning") is not None
                    else inferred.meaning
                ),
                operator_action=(
                    str(metadata.get("operator_action"))
                    if metadata.get("operator_action") is not None
                    else inferred.operator_action
                ),
                blocks_reliance=bool(metadata.get("blocks_reliance")),
            )
    return classify_step_outcome(
        str(row.get("status") or ""),
        _radar_run_step_reason(row),
    )


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

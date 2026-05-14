from __future__ import annotations

import json
import ssl
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from html import escape
from math import isfinite
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

import pandas as pd
import streamlit as st

from apps.dashboard.access import require_viewer
from catalyst_radar.brokers.interactive import (
    create_blocked_order_ticket,
    create_trigger,
    evaluate_triggers,
    opportunity_action_payload,
    record_opportunity_action,
)
from catalyst_radar.core.config import AppConfig
from catalyst_radar.dashboard import data as dashboard_data
from catalyst_radar.dashboard.design import dashboard_style
from catalyst_radar.jobs.step_outcomes import (
    SKIP_EXPLANATIONS as RADAR_SKIP_EXPLANATIONS,
)
from catalyst_radar.jobs.step_outcomes import (
    classify_step_outcome,
)
from catalyst_radar.security.access import Role, role_allows
from catalyst_radar.security.secrets import load_app_dotenv
from catalyst_radar.storage.broker_repositories import BrokerRepository
from catalyst_radar.storage.db import create_schema, engine_from_url

USEFUL_FEEDBACK_LABELS = frozenset({"useful", "acted"})
ALERT_STATUSES = ["planned", "dry_run", "sent", "failed"]
ALERT_ROUTES = [
    "immediate_manual_review",
    "warning_digest",
    "daily_digest",
    "position_watch",
]
def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _sequence(value: object) -> Sequence[object]:
    if isinstance(value, Mapping):
        return (value,)
    if isinstance(value, list | tuple):
        return value
    return ()


def _json_ready(value: object) -> object:
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_json_ready(item) for item in value]
    return value


def _records(value: object) -> list[dict[str, object]]:
    return [
        {str(key): _json_ready(item) for key, item in row.items()}
        for row in _sequence(value)
        if isinstance(row, Mapping)
    ]


def _metric_number(value: object) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    return number if isfinite(number) else 0.0


def _summary_metric_int(
    summary: Mapping[str, Any],
    *keys: str,
    fallback: int = 0,
) -> int:
    for key in keys:
        value = summary.get(key)
        if value is not None:
            return int(_metric_number(value))
    return int(fallback)


def _metric_text(value: object) -> str:
    if value is None or value == "":
        return "n/a"
    if isinstance(value, float):
        return f"{value:.2f}" if isfinite(value) else "n/a"
    if isinstance(value, datetime | date):
        return value.isoformat()
    return str(value)


def _html(value: object) -> str:
    return escape(_metric_text(value), quote=True)


def _currency(value: object) -> str:
    number = _metric_number(value)
    return f"${number:,.2f}"


def _rate(value: object) -> str:
    number = _metric_number(value)
    return f"{number:.1%}"


def _list_text(value: object) -> str:
    items = [str(item) for item in _sequence(value) if str(item).strip()]
    return ", ".join(items) if items else "none"


def _first_present(*values: object) -> object:
    for value in values:
        if value is not None and value != "" and value != [] and value != {}:
            return value
    return None


def _parse_cutoff(value: str) -> datetime | None:
    text = value.strip()
    if not text:
        return None
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("review cutoff must include timezone information")
    return parsed.astimezone(UTC)


def _radar_summary_cutoff(summary: Mapping[str, Any]) -> datetime | None:
    for key in ("finished_at", "decision_available_at"):
        value = summary.get(key)
        if value is None or value == "":
            continue
        try:
            return _parse_cutoff(str(value))
        except ValueError:
            continue
    return None


def _select_value(label: str, options: list[str]) -> str | None:
    value = st.sidebar.selectbox(label, ["All", *options])
    return None if value == "All" else value


def _default_ticker(
    candidate_rows: list[dict[str, object]],
    ipo_rows: list[dict[str, object]],
) -> str:
    for row in (*candidate_rows, *ipo_rows):
        ticker = str(row.get("ticker") or "").strip().upper()
        if ticker:
            return ticker
    return ""


def _table(
    rows: list[dict[str, object]],
    *,
    columns: list[str],
    labels: Mapping[str, str],
    empty: str,
    key: str | None = None,
    selectable: bool = False,
) -> dict[str, object] | None:
    if not rows:
        st.caption(empty)
        return None
    frame = pd.DataFrame(rows)
    for column in columns:
        if column not in frame.columns:
            frame[column] = None
    display_frame = frame[columns].rename(columns=dict(labels))
    if selectable:
        selected_index = _select_table_row(rows, key=key)
        _show_html_table(display_frame, selected_index=selected_index)
        return rows[selected_index]
    _show_html_table(display_frame)
    return None


def _select_table_row(rows: list[dict[str, object]], *, key: str | None) -> int:
    if len(rows) == 1:
        st.caption(f"Selected: {_row_option_label(rows[0], 0)}")
        return 0
    options = list(range(len(rows)))
    return int(
        st.selectbox(
            "Selected row",
            options,
            format_func=lambda index: _row_option_label(rows[int(index)], int(index)),
            key=f"{key}_selected_row" if key else None,
        )
    )


def _row_option_label(row: Mapping[str, object], index: int) -> str:
    ticker = str(row.get("ticker") or "").strip().upper()
    title = str(row.get("title") or row.get("setup_type") or row.get("route") or "").strip()
    status = str(row.get("state") or row.get("status") or row.get("priority") or "").strip()
    score = row.get("final_score") or row.get("score_trigger")
    parts = [part for part in (ticker, title, status) if part]
    if score not in (None, ""):
        parts.append(f"score {_metric_text(score)}")
    return " - ".join(parts) if parts else f"Row {index + 1}"


def _selected_dataframe_index(event: object) -> int | None:
    selection = getattr(event, "selection", None)
    if isinstance(selection, Mapping):
        rows = selection.get("rows")
    else:
        rows = getattr(selection, "rows", None)
    if not rows:
        return None
    try:
        return int(rows[0])
    except (TypeError, ValueError, IndexError):
        return None


def _show_html_table(frame: pd.DataFrame, *, selected_index: int | None = None) -> None:
    if frame.empty:
        return
    headers = "".join(f"<th>{_html(column)}</th>" for column in frame.columns)
    body_rows: list[str] = []
    for index, row in enumerate(frame.to_dict("records")):
        row_class = ' class="mr-table-selected"' if index == selected_index else ""
        cells = "".join(_table_cell_html(value) for value in row.values())
        body_rows.append(f"<tr{row_class}>{cells}</tr>")
    table = (
        '<div class="mr-table-wrap">'
        '<table class="mr-table">'
        f"<thead><tr>{headers}</tr></thead>"
        f"<tbody>{''.join(body_rows)}</tbody>"
        "</table>"
        "</div>"
    )
    st.markdown(table, unsafe_allow_html=True)


def _table_cell_html(value: object) -> str:
    return f"<td>{_value_html(value)}</td>"


def _value_html(value: object) -> str:
    if isinstance(value, list | tuple):
        chips = "".join(
            f'<span class="mr-inline-chip">{_html(item)}</span>'
            for item in value
            if _metric_text(item) != "n/a"
        )
        return chips or "n/a"
    if isinstance(value, Mapping):
        label = value.get("label")
        url = value.get("url")
        if label and isinstance(url, str) and url.startswith(("https://", "http://")):
            return (
                f'<a class="mr-table-link" href="{_html(url)}" target="_blank">{_html(label)}</a>'
            )
        items = [
            f"<strong>{_html(key)}</strong>: {_value_html(item)}"
            for key, item in value.items()
            if item not in (None, "", [], {})
        ]
        return "<br>".join(items) if items else "n/a"
    text = _metric_text(value)
    if text.startswith(("https://", "http://")):
        return f'<a class="mr-table-link" href="{_html(text)}" target="_blank">Open source</a>'
    return _html(text)


def _integer(value: object) -> str:
    number = _metric_number(value)
    return f"{int(number):,}" if number else "n/a"


def _price_range(low: object, high: object) -> str:
    if low in (None, "") and high in (None, ""):
        return "n/a"
    return f"{_currency(low)} - {_currency(high)}"


def _url_link(label: object, url: object) -> Mapping[str, object] | str:
    text = _metric_text(label)
    link = str(url or "").strip()
    if link.startswith(("https://", "http://")):
        return {"label": text, "url": link}
    return text if not link else f"{text} ({link})"


def _tone(value: object) -> str:
    text = str(value or "").lower()
    if text in {"critical", "failed", "blocked", "degraded", "stale", "yes"}:
        return "danger"
    if text in {"high", "warning", "planned", "dry_run", "enabled"}:
        return "warn"
    if text in {"healthy", "success", "sent", "ok", "useful", "acted", "off", "no"}:
        return "good"
    return "neutral"


def _badge_html(label: str, value: object) -> str:
    tone = _tone(value)
    return (
        f'<span class="mr-badge mr-badge-{tone}">'
        f"<strong>{_html(label)}</strong>{_html(value)}</span>"
    )


def _show_status_badges(items: Sequence[tuple[str, object]]) -> None:
    markup = "".join(_badge_html(label, value) for label, value in items)
    if markup:
        st.markdown(f'<div class="mr-badge-row">{markup}</div>', unsafe_allow_html=True)


def _command_cell(label: str, value: object) -> str:
    tone = _tone(value)
    return (
        f'<div class="mr-command-cell mr-command-cell-{tone}">'
        f'<span class="mr-command-label">{_html(label)}</span>'
        f'<span class="mr-command-value">{_html(value)}</span>'
        "</div>"
    )


def _show_command_header(
    *,
    candidate_rows: list[dict[str, object]],
    alert_rows: list[dict[str, object]],
    ipo_rows: list[dict[str, object]],
    ops_health: Mapping[str, Any],
) -> None:
    database = _mapping(ops_health.get("database"))
    degraded_mode = _mapping(ops_health.get("degraded_mode"))
    degraded = "enabled" if bool(degraded_mode.get("enabled")) else "off"
    cells = [
        _command_cell("Database", database.get("status") or "unknown"),
        _command_cell("Candidates", len(candidate_rows)),
        _command_cell("Alerts", len(alert_rows)),
        _command_cell("IPO/S-1", len(ipo_rows)),
        _command_cell("Degraded", degraded),
    ]
    st.markdown(
        '<section class="mr-app-header">'
        '<div class="mr-title-block">'
        "<h1>Market Radar Command Center</h1>"
        "</div>"
        '<div class="mr-status-panel" aria-label="System status">'
        '<span class="mr-status-title">System Status</span>'
        f'<div class="mr-command-strip">{"".join(cells)}</div>'
        "</div>"
        "</section>",
        unsafe_allow_html=True,
    )


def _evidence_label(value: object) -> object:
    item = _mapping(value)
    title = str(item.get("title") or item.get("kind") or "")
    link = item.get("source_url") or item.get("source_id") or item.get("computed_feature_id")
    return title if not link else _url_link(title, link)


def _show_state_mix(rows: list[dict[str, object]]) -> None:
    counts: dict[str, int] = {}
    for row in rows:
        state = _metric_text(row.get("state"))
        if state != "n/a":
            counts[state] = counts.get(state, 0) + 1
    if not counts:
        st.caption("No state mix.")
        return
    total = sum(counts.values())
    bars = []
    for state, count in sorted(counts.items(), key=lambda item: (-item[1], item[0])):
        pct = count / total if total else 0.0
        bars.append(
            '<div class="mr-chart-row">'
            '<div class="mr-chart-row-head">'
            f"<span>{_html(state)}</span>"
            f"<strong>{count}</strong>"
            "</div>"
            '<div class="mr-chart-track">'
            f'<span class="mr-chart-bar" style="width: {pct * 100:.1f}%"></span>'
            "</div>"
            f'<span class="mr-chart-caption">{pct:.0%} of candidates</span>'
            "</div>"
        )
    st.markdown(f'<div class="mr-chart-card">{"".join(bars)}</div>', unsafe_allow_html=True)


def _show_activation_summary(
    config: AppConfig,
    radar_run_summary: Mapping[str, Any],
    broker_summary: Mapping[str, Any],
) -> None:
    summary = _mapping(
        dashboard_data.activation_summary_payload(
            config,
            radar_run_summary=radar_run_summary,
            broker_summary=broker_summary,
        )
    )
    status = str(summary.get("status") or "unknown")
    message = (
        f"{summary.get('headline') or 'Radar activation status'} "
        f"{summary.get('detail') or ''} Next: {summary.get('next_action') or 'n/a'}"
    ).strip()
    if status == "ready":
        st.success(message)
    elif status == "attention":
        st.info(message)
    else:
        st.warning(message)
    st.caption(str(summary.get("evidence") or "No activation evidence."))


def _show_live_activation_plan(
    config: AppConfig,
    radar_run_summary: Mapping[str, Any],
    broker_summary: Mapping[str, Any],
) -> None:
    plan = _mapping(
        dashboard_data.live_activation_plan_payload(
            config,
            radar_run_summary=radar_run_summary,
            broker_summary=broker_summary,
        )
    )
    st.subheader("Live Activation Plan")
    status = str(plan.get("status") or "unknown")
    message = (
        f"{plan.get('headline') or 'Live activation status'} "
        f"Next: {plan.get('next_action') or 'n/a'}"
    ).strip()
    if status == "ready":
        st.success(message)
    elif status == "blocked":
        st.warning(message)
    else:
        st.info(message)
    st.caption(str(plan.get("evidence") or "No live activation evidence."))
    missing_env = [str(item) for item in _records(plan.get("missing_env"))]
    if not missing_env:
        raw_missing = plan.get("missing_env")
        if isinstance(raw_missing, Sequence) and not isinstance(raw_missing, str | bytes):
            missing_env = [str(item) for item in raw_missing]
    if missing_env:
        st.code("\n".join(missing_env), language="text")
    _show_records(
        "Activation Tasks",
        plan.get("tasks"),
        empty="No activation tasks.",
    )
    with st.expander("Live call budgets and safety guards"):
        _show_records(
            "Call Budgets And Guards",
            plan.get("call_budgets"),
            empty="No call-budget rows.",
        )


def _show_telemetry_tape(ops_health: Mapping[str, Any]) -> None:
    tape = _mapping(dashboard_data.telemetry_tape_payload(ops_health))
    status = str(tape.get("status") or "unknown")
    latest = tape.get("latest_event_at") or "n/a"
    event_count = int(_metric_number(tape.get("event_count")))
    if status == "attention":
        st.warning(f"Telemetry tape: {event_count} recent event(s); latest {latest}.")
    elif status == "ready":
        st.info(f"Telemetry tape: {event_count} recent event(s); latest {latest}.")
    else:
        st.caption("Telemetry tape: no recent telemetry events.")
    _show_records(
        "Recent Radar Telemetry",
        tape.get("events"),
        empty="No recent radar telemetry.",
        )


def _show_alert_planning_diagnostics(
    engine: object,
    radar_run_summary: Mapping[str, Any],
) -> None:
    diagnostics = _mapping(
        dashboard_data.alert_planning_diagnostics_payload(
            engine,
            radar_run_summary=radar_run_summary,
        )
    )
    status = str(diagnostics.get("status") or "unknown")
    message = (
        f"{diagnostics.get('headline') or 'Alert planning diagnostics'} "
        f"Next: {diagnostics.get('next_action') or 'n/a'}"
    ).strip()
    if status in {"suppressed", "not_ready", "blocked_input", "failed"}:
        st.warning(message)
    elif status == "ready":
        st.success(message)
    else:
        st.info(message)
    st.caption(str(diagnostics.get("evidence") or "No alert planning evidence."))
    _show_records(
        "Alert Suppression Counts",
        diagnostics.get("counts"),
        empty="No alert suppression counts.",
    )
    _show_records(
        "Alert Suppression Reasons",
        diagnostics.get("rows"),
        empty="No alert suppression rows.",
    )


def _show_live_data_activation_contract(
    config: AppConfig,
    radar_run_summary: Mapping[str, Any],
    broker_summary: Mapping[str, Any],
) -> None:
    contract = _mapping(
        dashboard_data.live_data_activation_contract_payload(
            config,
            radar_run_summary=radar_run_summary,
            broker_summary=broker_summary,
        )
    )
    st.subheader("Live Data Activation")
    status = str(contract.get("status") or "unknown")
    message = (
        f"{contract.get('headline') or 'Live data activation'} "
        f"Next: {contract.get('next_action') or 'n/a'}"
    ).strip()
    if status == "ready":
        st.success(message)
    else:
        st.warning(message)
    _show_status_badges(
        [
            (
                "Contract Calls",
                "yes" if contract.get("makes_external_calls") else "no",
            ),
            ("Read Only", "yes" if contract.get("read_only") else "no"),
            ("Missing Env", len(_sequence(contract.get("missing_env")))),
        ]
    )
    minimum_env_lines = [str(item) for item in _sequence(contract.get("minimum_env_lines"))]
    if minimum_env_lines:
        st.caption(
            "Minimum .env.local block for read-only live market and catalyst data. "
            "The full template below adds optional LLM review settings."
        )
        st.code("\n".join(minimum_env_lines), language="text")
    _show_records(
        "Activation Steps",
        contract.get("operator_steps"),
        empty="No activation steps.",
    )
    with st.expander("Worker automation handoff"):
        worker_env_lines = [
            str(item) for item in _sequence(contract.get("worker_env_lines"))
        ]
        if worker_env_lines:
            st.caption(
                "Use these dry-run-safe worker settings after live inputs are configured."
            )
            st.code("\n".join(worker_env_lines), language="text")
        _show_records(
            "Worker Commands",
            contract.get("worker_commands"),
            empty="No worker commands.",
        )
    with st.expander("Environment template"):
        env_lines = [
            f"{row.get('name')}={row.get('value_template')}"
            for row in _records(contract.get("env_template"))
        ]
        st.code("\n".join(env_lines), language="text")
        _show_records(
            "Environment Status",
            contract.get("env_template"),
            empty="No environment template rows.",
        )
    with st.expander("Activation guardrails"):
        _show_records(
            "Safe Limits",
            contract.get("safe_limits"),
            empty="No safe limits.",
        )
        _show_records(
            "Call Budget If Activated",
            contract.get("call_budget_if_activated"),
            empty="No activation call budget.",
        )


def _show_universe_coverage(
    config: AppConfig,
    ops_health: Mapping[str, Any],
) -> None:
    summary = _mapping(dashboard_data.universe_coverage_payload(config, ops_health))
    status = str(summary.get("status") or "unknown")
    message = (
        f"{summary.get('headline') or 'Universe coverage status'} "
        f"{summary.get('detail') or ''} Next: {summary.get('next_action') or 'n/a'}"
    ).strip()
    if status == "ready":
        st.success(message)
    elif status in {"attention", "partial"}:
        st.info(message)
    else:
        st.warning(message)
    st.caption(str(summary.get("evidence") or "No universe coverage evidence."))
    seed_configured = bool(config.polygon_api_key)
    st.caption(
        f"Manual seed uses Polygon ticker reference data, capped at "
        f"{config.polygon_tickers_max_pages} page(s). "
        + (
            "It may make live Polygon calls because CATALYST_POLYGON_API_KEY is configured."
            if seed_configured
            else "No live Polygon call is available until CATALYST_POLYGON_API_KEY is set."
        )
    )
    if st.button(
        "Seed Universe",
        key="seed_universe_polygon",
        disabled=not seed_configured,
        help="Ingest Polygon ticker reference data using the configured page cap.",
    ):
        try:
            result = _mapping(
                _api_post(
                    config,
                    "/api/radar/universe/seed",
                    {
                        "provider": "polygon",
                        "max_pages": config.polygon_tickers_max_pages,
                    },
                )
            )
        except RuntimeError as exc:
            st.error(str(exc))
            return
        st.success(
            "Universe seed completed: "
            f"securities={int(_metric_number(result.get('security_count')))}; "
            f"rejected={int(_metric_number(result.get('rejected_count')))}; "
            f"cap={int(_metric_number(result.get('max_pages')))} page(s)."
        )


def _show_radar_run_controls(
    engine: object,
    config: AppConfig,
    radar_run_summary: Mapping[str, Any],
    radar_run_cooldown: Mapping[str, Any],
) -> None:
    st.subheader("Radar Run")
    previous_run_result = _mapping(
        st.session_state.pop("manual_radar_run_result", None)
    )
    cooldown = _mapping(radar_run_cooldown)
    cooldown_allowed = bool(cooldown.get("allowed", True))
    control_col, status_col = st.columns([1, 3])
    with control_col:
        run_llm_dry_run = st.checkbox(
            "Agent review dry run",
            value=True,
            key="run_radar_llm_dry_run",
            help=(
                "Runs the agent-review step in dry-run mode only; no model call is made."
            ),
        )
        st.checkbox(
            "Alert dry run",
            value=True,
            key="run_radar_alert_dry_run",
            disabled=True,
            help="Daily alert delivery is locked to dry-run mode from the dashboard.",
        )
        default_run_scope_payload = _mapping(
            dashboard_data.radar_run_default_scope_payload(
                engine,
                config,
                radar_run_summary=radar_run_summary,
            )
        )
        run_scope_payload: dict[str, object] = dict(
            _mapping(default_run_scope_payload.get("scope"))
        )
        if run_scope_payload:
            st.caption(
                str(default_run_scope_payload.get("headline") or "").strip()
                or "Using default radar scope."
            )
        custom_scope = st.checkbox(
            "Custom scope",
            value=False,
            key="run_radar_custom_scope",
            help="Optionally constrain the radar run without changing global settings.",
        )
        if custom_scope:
            with st.expander("Run scope", expanded=True):
                as_of_value = st.date_input(
                    "As of date",
                    value=_radar_default_as_of(
                        default_run_scope_payload,
                        radar_run_summary,
                    ),
                    key="run_radar_as_of",
                )
                tickers_text = st.text_input(
                    "Tickers",
                    value="",
                    placeholder="AAPL, MSFT",
                    key="run_radar_tickers",
                )
                provider_text = st.text_input(
                    "Provider override",
                    value="",
                    placeholder="default",
                    key="run_radar_provider",
                )
                universe_text = st.text_input(
                    "Universe override",
                    value="",
                    placeholder="default",
                    key="run_radar_universe",
                )
                run_scope_payload = _radar_run_scope_payload(
                    as_of_value=as_of_value,
                    tickers_text=tickers_text,
                    provider_text=provider_text,
                    universe_text=universe_text,
                )
        run_requested = st.button(
            "Run Radar",
            key="run_radar_now",
            type="primary",
            disabled=not cooldown_allowed,
            help=(
                "Manual run cooldown is active."
                if not cooldown_allowed
                else "Start one guarded daily radar pass."
            ),
        )
    with status_col:
        _show_radar_run_cooldown(cooldown)
        call_plan = _mapping(
            dashboard_data.radar_run_call_plan_payload(
                engine,
                config,
                as_of=run_scope_payload.get("as_of"),
                provider=run_scope_payload.get("provider"),
                universe=run_scope_payload.get("universe"),
                tickers=_sequence(run_scope_payload.get("tickers")),
                run_llm=run_llm_dry_run,
                llm_dry_run=True,
                dry_run_alerts=True,
            )
        )
        _show_radar_call_plan(call_plan)
        if previous_run_result:
            daily_result = _mapping(previous_run_result.get("daily_result"))
            _show_radar_run_result_notice(
                daily_result,
                fallback_reason=previous_run_result.get("reason"),
            )
            _show_discovery_snapshot(
                _mapping(previous_run_result.get("discovery_snapshot"))
            )
            _show_radar_operator_sections(
                _radar_run_operator_rows(daily_result),
                _radar_run_raw_rows(daily_result),
            )
        if not run_requested:
            _show_radar_run_summary(radar_run_summary)
            return
        try:
            result = _mapping(
                _api_post(
                    config,
                    "/api/radar/runs",
                    {
                        "run_llm": run_llm_dry_run,
                        "llm_dry_run": True,
                        "dry_run_alerts": True,
                        **run_scope_payload,
                    },
                )
            )
        except RuntimeError as exc:
            st.error(str(exc))
            return
        st.session_state["manual_radar_run_result"] = result
        st.rerun()


def _show_radar_call_plan(call_plan: Mapping[str, Any]) -> None:
    if not call_plan:
        st.caption("Run call plan is unavailable.")
        return
    status = str(call_plan.get("status") or "unknown")
    message = (
        f"{call_plan.get('headline') or 'Radar run call plan'} "
        f"Next: {call_plan.get('next_action') or 'n/a'}"
    ).strip()
    if status == "blocked":
        st.warning(message)
    elif bool(call_plan.get("will_call_external_providers")):
        st.info(message)
    else:
        st.caption(message)
    _show_status_badges(
        [
            ("External Calls Max", call_plan.get("max_external_call_count", 0)),
            (
                "Live Providers",
                "yes" if call_plan.get("will_call_external_providers") else "no",
            ),
            (
                "Cooldown",
                f"{_nested(call_plan, 'guardrails', 'manual_run_cooldown_seconds') or 'n/a'}s",
            ),
            (
                "Schwab Calls",
                "yes"
                if _nested(call_plan, "guardrails", "schwab_called_by_radar_run")
                else "no",
            ),
        ]
    )
    with st.expander("Run call plan"):
        _show_records(
            "Planned External Calls",
            call_plan.get("rows"),
            empty="No call-plan rows.",
        )


def _show_radar_run_cooldown(cooldown: Mapping[str, Any]) -> None:
    if not cooldown:
        st.caption("Manual run cooldown status is unavailable.")
        return
    status = str(cooldown.get("status") or "unknown")
    headline = str(cooldown.get("headline") or "Manual radar run cooldown")
    detail = str(cooldown.get("detail") or "")
    next_action = str(cooldown.get("next_action") or "n/a")
    message = f"{headline} {detail} Next: {next_action}".strip()
    if status == "cooldown":
        st.warning(message)
    else:
        st.caption(message)
    _show_status_badges(
        [
            ("Run Gate", "blocked" if status == "cooldown" else "ok"),
            ("Min Interval", f"{int(_metric_number(cooldown.get('min_interval_seconds')))}s"),
            ("Retry After", f"{int(_metric_number(cooldown.get('retry_after_seconds')))}s"),
            ("Reset At", cooldown.get("reset_at") or "n/a"),
        ]
    )
    st.caption(str(cooldown.get("evidence") or "No cooldown evidence."))


def _show_radar_run_summary(summary: Mapping[str, Any]) -> None:
    if not summary:
        st.caption("No prior radar run.")
        return
    _show_status_badges(
        [
            ("Last Run", summary.get("status")),
            ("As Of", summary.get("as_of")),
            ("Provider", summary.get("provider") or "default"),
            ("Universe", summary.get("universe") or "default"),
        ]
    )
    status_counts = _mapping(summary.get("status_counts"))
    outcome_counts = _mapping(summary.get("outcome_category_counts"))
    raw_skipped_count = int(_metric_number(status_counts.get("skipped")))
    blocking_count = _summary_metric_int(
        summary,
        "action_needed_count",
        "blocking_step_count",
    )
    expected_gate_count = _summary_metric_int(
        summary,
        "optional_expected_gate_count",
        "expected_gate_count",
        fallback=int(_metric_number(outcome_counts.get("expected_gate"))),
    )
    required_stage_count = _summary_metric_int(
        summary,
        "required_step_count",
        fallback=max(
            0,
            int(_metric_number(summary.get("step_count"))) - expected_gate_count,
        ),
    )
    completed_required_count = _summary_metric_int(
        summary,
        "required_completed_count",
        fallback=max(0, int(_metric_number(outcome_counts.get("completed")))),
    )
    required_incomplete_count = _summary_metric_int(
        summary,
        "required_incomplete_count",
        fallback=max(0, required_stage_count - completed_required_count),
    )
    metric_cols = st.columns(5)
    st.caption(
        "The required path is the decision signal. Raw skipped rows are retained "
        "for audit, but expected optional gates are not scan failures."
    )
    metric_cols[0].metric(
        "Telemetry Rows",
        int(_metric_number(summary.get("step_count"))),
    )
    metric_cols[1].metric(
        "Required Path",
        f"{min(completed_required_count, required_stage_count)}/{required_stage_count}",
    )
    metric_cols[2].metric("Action Needed", blocking_count)
    metric_cols[3].metric("Optional Gates", expected_gate_count)
    metric_cols[4].metric("Raw Skips Retained", raw_skipped_count)
    volume_cols = st.columns(3)
    volume_cols[0].metric("Requested", int(_metric_number(summary.get("requested_count"))))
    volume_cols[1].metric("Raw", int(_metric_number(summary.get("raw_count"))))
    volume_cols[2].metric("Normalized", int(_metric_number(summary.get("normalized_count"))))
    summary_steps = _records(summary.get("steps"))
    blocking_skips = [
        step
        for step in summary_steps
        if bool(step.get("blocks_reliance"))
        or str(step.get("category") or "") in {"blocked_input", "failed", "needs_review"}
    ]
    expected_skips = [
        step
        for step in summary_steps
        if str(step.get("category") or "") == "expected_gate"
    ]
    if blocking_skips:
        st.warning(
            f"{len(blocking_skips)} run step(s) were blocked by missing or degraded inputs. "
            "Use the readiness checklist before treating the run as current."
        )
    elif required_incomplete_count:
        st.warning(
            f"Required path is incomplete: {min(completed_required_count, required_stage_count)}"
            f"/{required_stage_count} required step(s) completed. Inspect the required "
            "run path for missing inputs before relying on the latest scan."
        )
    elif expected_gate_count:
        st.success(
            f"{len(expected_skips) or expected_gate_count} optional gate(s) did not "
            "trigger by design. Required scan stages completed; raw skip telemetry "
            "is retained only in the audit section below."
        )
    elif raw_skipped_count:
        st.warning(
            f"{raw_skipped_count} raw skipped step(s) were not classified as expected "
            "gates. Inspect diagnostic telemetry before relying on the run."
        )
    _show_radar_operator_sections(
        _radar_summary_operator_rows(summary),
        _radar_summary_raw_rows(summary),
        last_run=True,
    )


def _show_agent_review_summary(
    radar_run_summary: Mapping[str, Any],
    candidate_rows: list[dict[str, object]],
) -> None:
    summary = _mapping(
        dashboard_data.agent_review_summary_payload(
            radar_run_summary,
            candidate_rows,
        )
    )
    if not summary:
        return
    st.subheader("Agent Review")
    status = str(summary.get("status") or "unknown")
    message = (
        f"{summary.get('headline') or 'Agent review status'} "
        f"Next: {summary.get('next_action') or 'n/a'}"
    ).strip()
    if status in {"reviewed", "dry_run_reviewed"}:
        if status == "dry_run_reviewed":
            st.info(message)
        else:
            st.success(message)
    elif status in {"no_review_inputs", "disabled", "expected_gate"}:
        st.caption(message)
    elif status == "attention":
        st.warning(message)
    else:
        st.info(message)
    reviewed_tickers = [str(value) for value in _sequence(summary.get("reviewed_tickers"))]
    _show_status_badges(
        [
            ("Mode", summary.get("mode") or "n/a"),
            ("Requested", int(_metric_number(summary.get("requested_count")))),
            ("Reviewed", int(_metric_number(summary.get("reviewed_packet_count")))),
            ("Tickers", ", ".join(reviewed_tickers) if reviewed_tickers else "none"),
            ("Remaining Gates", len(_records(summary.get("remaining_expected_gates")))),
        ]
    )
    if reviewed_tickers:
        _show_records(
            "Reviewed Candidate Context",
            _records(summary.get("reviewed_candidates")),
            empty="No reviewed candidate context.",
        )
    remaining_gates = _records(summary.get("remaining_expected_gates"))
    if remaining_gates:
        with st.expander("Remaining Agent Gates"):
            _show_records(
                "Gate Conditions",
                remaining_gates,
                empty="No remaining agent gates.",
            )
    st.caption(str(summary.get("evidence") or "No agent review evidence."))


def _show_discovery_snapshot(snapshot: Mapping[str, Any]) -> None:
    if not snapshot:
        return
    st.subheader("Latest Discovery Snapshot")
    status = str(snapshot.get("status") or "unknown")
    message = (
        f"{snapshot.get('headline') or 'Discovery snapshot'} "
        f"{snapshot.get('detail') or ''} Next: {snapshot.get('next_action') or 'n/a'}"
    ).strip()
    if status == "ready":
        st.success(message)
    elif status in {"fixture", "attention"}:
        st.warning(message)
    elif status == "blocked":
        st.error(message)
    else:
        st.info(message)
    st.caption(str(snapshot.get("evidence") or "No discovery evidence."))
    yield_payload = _mapping(snapshot.get("yield"))
    freshness = _mapping(snapshot.get("freshness"))
    cols = st.columns(5)
    cols[0].metric("Requested", int(_metric_number(yield_payload.get("requested_securities"))))
    cols[1].metric("Scanned", int(_metric_number(yield_payload.get("scanned_securities"))))
    cols[2].metric("Candidates", int(_metric_number(yield_payload.get("candidate_states"))))
    cols[3].metric("Packets", int(_metric_number(yield_payload.get("candidate_packets"))))
    cols[4].metric("Cards", int(_metric_number(yield_payload.get("decision_cards"))))
    _show_status_badges(
        [
            ("Latest Bars", freshness.get("latest_daily_bar_date") or "n/a"),
            (
                "Bars Stale",
                "yes" if freshness.get("latest_bars_older_than_as_of") else "no",
            ),
            (
                "Candidate Age",
                freshness.get("latest_candidate_age_days")
                if freshness.get("latest_candidate_age_days") is not None
                else "n/a",
            ),
            (
                "Candidate Session",
                freshness.get("latest_candidate_session_date") or "n/a",
            ),
        ]
    )
    blockers = _records(snapshot.get("blockers"))
    if blockers:
        with st.expander(f"Discovery blockers ({len(blockers)})", expanded=True):
            _show_records(
                "Discovery Blockers",
                blockers,
                empty="No discovery blockers.",
            )
    _show_records(
        "Top Discoveries",
        _visible_discovery_rows(snapshot.get("top_discoveries")),
        empty="No top discoveries for this run.",
    )
    latest_context = _mapping(snapshot.get("latest_candidate_context"))
    latest_rows = _visible_discovery_rows(latest_context.get("top_candidates"))
    if latest_rows:
        stale = bool(latest_context.get("stale_relative_to_run"))
        with st.expander("Latest candidate context", expanded=stale):
            if stale:
                st.caption(
                    "These candidates exist in the database, but their as-of date is older "
                    "than the latest run. Treat them as context, not fresh discoveries."
                )
            else:
                st.caption("Latest persisted candidates available at the run cutoff.")
            _show_status_badges(
                [
                    ("Context Candidates", latest_context.get("candidate_states") or 0),
                    (
                        "Candidate Session",
                        latest_context.get("latest_candidate_session_date")
                        or latest_context.get("latest_candidate_as_of")
                        or "n/a",
                    ),
                    (
                        "Stale vs Run",
                        "yes" if stale else "no",
                    ),
                ]
            )
            _show_records(
                "Latest Candidate Context",
                latest_rows,
                empty="No latest candidate context.",
            )


def _show_actionability_breakdown(candidate_rows: list[dict[str, object]]) -> None:
    breakdown = _mapping(dashboard_data.actionability_breakdown_payload(candidate_rows))
    st.subheader("Actionability Breakdown")
    status = str(breakdown.get("status") or "unknown")
    message = (
        f"{breakdown.get('headline') or 'No actionability summary.'} "
        f"Next: {breakdown.get('next_action') or 'n/a'}"
    ).strip()
    if status == "ready":
        st.success(message)
    elif status in {"research", "watchlist"}:
        st.info(message)
    elif status == "empty":
        st.caption(message)
    else:
        st.warning(message)
    _show_records(
        "Actionability Counts",
        breakdown.get("counts"),
        empty="No actionability counts.",
    )
    _show_records(
        "Top Blockers Or Gaps",
        breakdown.get("top_blockers"),
        empty="No blockers or gaps.",
    )
    _show_records(
        "Candidate Next Actions",
        breakdown.get("next_actions"),
        empty="No candidate next actions.",
    )


def _show_operator_work_queue(
    *,
    config: AppConfig,
    radar_run_summary: Mapping[str, Any],
    broker_summary: Mapping[str, Any],
    discovery_snapshot: Mapping[str, Any],
    candidate_rows: list[dict[str, object]],
) -> None:
    queue = _mapping(
        dashboard_data.operator_work_queue_payload(
            config,
            radar_run_summary=radar_run_summary,
            broker_summary=broker_summary,
            discovery_snapshot=discovery_snapshot,
            candidate_rows=candidate_rows,
        )
    )
    st.subheader("Operator Work Queue")
    status = str(queue.get("status") or "unknown")
    message = (
        f"{queue.get('headline') or 'No operator queue headline.'} "
        f"Next: {queue.get('next_action') or 'n/a'}"
    ).strip()
    if status == "blocked":
        st.warning(message)
    elif status in {"review", "research"}:
        st.info(message)
    elif status == "monitor":
        st.success(message)
    else:
        st.caption(message)
    counts = _mapping(queue.get("counts"))
    _show_status_badges(
        [
            ("Blocking", counts.get("blocking") or 0),
            ("Review", counts.get("review") or 0),
            ("Research", counts.get("research") or 0),
            ("Decision Mode", queue.get("investment_mode") or "unknown"),
            (
                "Investment Ready",
                "yes" if queue.get("safe_to_make_investment_decision") else "no",
            ),
        ]
    )
    _show_records(
        "Priority Queue",
        _visible_operator_queue_rows(queue.get("rows")),
        empty="No operator work queue rows.",
    )


def _visible_operator_queue_rows(value: object) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for rank, row in enumerate(_records(value), start=1):
        rows.append(
            {
                "rank": rank,
                "priority": row.get("priority"),
                "area": row.get("area"),
                "item": row.get("item"),
                "status": row.get("status"),
                "next_action": row.get("next_action"),
                "evidence": row.get("evidence"),
            }
        )
    return rows


def _show_candidate_delta(
    engine: object,
    radar_run_summary: Mapping[str, Any],
) -> None:
    delta = _mapping(
        dashboard_data.candidate_delta_payload(
            engine,
            radar_run_summary=radar_run_summary,
            available_at=_radar_summary_cutoff(radar_run_summary),
        )
    )
    st.subheader("Candidate Delta")
    status = str(delta.get("status") or "unknown")
    message = (
        f"{delta.get('headline') or 'Candidate delta unavailable.'} "
        f"Next: {delta.get('next_action') or 'Review candidate history.'}"
    ).strip()
    if status == "changed":
        st.warning(message)
    elif status == "unchanged":
        st.success(message)
    elif status == "no_current_candidates":
        st.info(message)
    else:
        st.caption(message)
    summary = _mapping(delta.get("summary"))
    _show_status_badges(
        [
            ("Changed", summary.get("changed_candidates") or 0),
            ("New", summary.get("new_candidates") or 0),
            ("State", summary.get("state_changes") or 0),
            ("Score", summary.get("score_moves") or 0),
            ("Blockers", summary.get("blocker_changes") or 0),
            ("Stale Context", summary.get("stale_context_candidates") or 0),
        ]
    )
    st.caption(str(delta.get("evidence") or "No delta evidence."))
    _show_records(
        "Latest Candidate Changes",
        delta.get("rows"),
        empty="No current-run candidate changes.",
    )


def _show_investment_readiness(
    discovery_snapshot: Mapping[str, Any],
    candidate_rows: list[dict[str, object]],
) -> Mapping[str, Any]:
    actionability = _mapping(dashboard_data.actionability_breakdown_payload(candidate_rows))
    readiness = _mapping(
        dashboard_data.investment_readiness_payload(
            discovery_snapshot,
            actionability,
            candidate_rows,
        )
    )
    st.subheader("Investment Decision Readiness")
    status = str(readiness.get("status") or "unknown")
    message = (
        f"{readiness.get('headline') or 'Investment readiness status'} "
        f"{readiness.get('detail') or ''} Next: {readiness.get('next_action') or 'n/a'}"
    ).strip()
    if status == "ready":
        st.success(message)
    elif status == "monitor":
        st.info(message)
    else:
        st.warning(message)
    st.caption(str(readiness.get("evidence") or "No investment readiness evidence."))
    _show_status_badges(
        [
            ("Decision Mode", readiness.get("decision_mode") or "unknown"),
            (
                "Buy Review",
                "ready" if readiness.get("manual_buy_review_ready") else "not ready",
            ),
        ]
    )
    blocker_rows = _records(readiness.get("blocking_reasons"))
    if blocker_rows:
        with st.expander("Why this is not decision-ready", expanded=status != "ready"):
            _show_records(
                "Decision Readiness Blockers",
                blocker_rows,
                empty="No decision-readiness blockers.",
            )
    return readiness


def _show_decision_contract(
    readiness: Mapping[str, Any],
    candidate_rows: list[dict[str, object]],
) -> None:
    st.subheader("Manual Review Gate")
    review_ready = bool(readiness.get("manual_buy_review_ready"))
    mode = str(readiness.get("decision_mode") or "unknown")
    next_action = str(readiness.get("next_action") or "Review readiness inputs.")
    if review_ready:
        st.success(f"Manual buy review can open. Next: {next_action}")
    else:
        st.warning(f"Investment decision blocked for this run. Next: {next_action}")
    cols = st.columns(3)
    cols[0].metric("Manual Review", "OPEN" if review_ready else "BLOCKED")
    cols[1].metric("Mode", mode)
    cols[2].metric(
        "Decision Use",
        "manual review" if review_ready else "research only",
    )
    _show_records(
        "Manual Review Gate Diagnostics",
        _manual_review_gate_rows(candidate_rows),
        empty="No candidate gate diagnostics.",
    )


def _manual_review_gate_rows(
    candidate_rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for candidate in candidate_rows[:8]:
        ticker = _metric_text(candidate.get("ticker"))
        if ticker == "n/a":
            continue
        state = _metric_text(candidate.get("state"))
        card_id = _metric_text(candidate.get("decision_card_id"))
        blockers = _candidate_blocker_values(candidate)
        risk_or_gap = _metric_text(candidate.get("risk_or_gap"))
        if state == "EligibleForManualBuyReview":
            gate_status = "ready" if card_id != "n/a" else "needs decision card"
            why_not_ready = "Decision card is required." if card_id == "n/a" else "n/a"
        elif blockers:
            gate_status = "blocked"
            why_not_ready = "; ".join(blockers)
        else:
            gate_status = "research"
            why_not_ready = (
                risk_or_gap
                if risk_or_gap != "n/a"
                else "Candidate has not reached manual-buy-review state."
            )
        rows.append(
            {
                "Ticker": ticker,
                "State": state,
                "Score": f"{_metric_number(candidate.get('final_score')):.2f}",
                "Gate Status": gate_status,
                "Why Not Ready": why_not_ready,
                "Decision Card": card_id,
            }
        )
    return rows


def _show_research_shortlist(
    candidate_rows: list[dict[str, object]],
    investment_readiness: Mapping[str, Any],
    *,
    market_context: object = (),
) -> None:
    shortlist = _mapping(
        dashboard_data.research_shortlist_payload(
            candidate_rows,
            investment_readiness,
            market_context=market_context,
        )
    )
    st.subheader("Research Shortlist")
    status = str(shortlist.get("status") or "unknown")
    message = (
        f"{shortlist.get('headline') or 'No research shortlist.'} "
        f"Next: {shortlist.get('next_action') or 'n/a'}"
    ).strip()
    if status == "manual_review":
        st.success(message)
    elif status == "research":
        st.info(message)
    elif status == "empty":
        st.caption(message)
    else:
        st.warning(message)
    _show_records(
        "Research Shortlist",
        _visible_shortlist_rows(shortlist.get("rows")),
        empty="No shortlist rows.",
    )


def _visible_shortlist_rows(value: object) -> list[dict[str, object]]:
    return [
        {key: item for key, item in row.items() if key != "audit"}
        for row in _records(value)
    ]


def _visible_discovery_rows(value: object) -> list[dict[str, object]]:
    return [
        {key: item for key, item in row.items() if key != "audit"}
        for row in _records(value)
    ]


def _show_radar_operator_sections(
    operator_rows: list[dict[str, object]],
    raw_rows: list[dict[str, object]],
    *,
    last_run: bool = False,
) -> None:
    action_rows = [
        row
        for row in operator_rows
        if _operator_row_value(row, "Needs Action", "needs_action") == "yes"
    ]
    optional_rows = [
        row
        for row in operator_rows
        if (
            _operator_row_value(row, "Stage", "stage")
            in {"Optional gate", "Expected skipped gate"}
        )
    ]
    required_rows = [
        row
        for row in operator_rows
        if (
            _operator_row_value(row, "Stage", "stage")
            not in {"Optional gate", "Expected skipped gate"}
        )
    ]
    if action_rows:
        _show_records(
            "Run Steps Needing Action",
            _operator_action_rows(action_rows),
            empty="No blocked run steps.",
        )
    else:
        st.caption(
            "Required scan stages are separated from optional gates; raw skipped "
            "telemetry is audit-only."
        )
        _show_records(
            "Required Run Path" if not last_run else "Last Required Run Path",
            _operator_required_rows(required_rows),
            empty="No required run path telemetry.",
        )
    if optional_rows:
        with st.expander(f"Optional gates not triggered ({len(optional_rows)})"):
            st.caption(
                "These gates did not run because their trigger was absent; they are not "
                "scan failures."
            )
            _show_records(
                "Expected Skipped Gates",
                _operator_optional_rows(optional_rows),
                empty="No optional gate telemetry.",
            )
    raw_skip_count = sum(
        1
        for row in raw_rows
        if str(row.get("raw_status") or row.get("status") or "").lower() == "skipped"
    )
    with st.expander(
        f"Audit-only raw telemetry ({raw_skip_count} raw skip record(s) retained)"
    ):
        st.caption(
            "Raw status/reason records are kept here for audit and debugging; "
            "operator decisions should use the required path and optional-gate tables above."
        )
        _show_records(
            "Raw Step Telemetry",
            raw_rows,
            empty="No raw step telemetry.",
        )


def _operator_required_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    return [
        {
            "Outcome": _operator_row_value(row, "Outcome", "outcome"),
            "Stage": _operator_row_value(row, "Stage", "stage"),
            "Step": _operator_row_value(row, "Step", "step"),
            "Requested": _operator_row_object(row, "Requested", "requested"),
            "Raw": _operator_row_object(row, "Raw", "raw"),
            "Normalized": _operator_row_object(row, "Normalized", "normalized"),
        }
        for row in rows
    ]


def _operator_action_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    return [
        {
            "Outcome": _operator_row_value(row, "Outcome", "outcome"),
            "Step": _operator_row_value(row, "Step", "step"),
            "Reason": _operator_row_value(row, "Reason", "reason"),
            "Meaning": _operator_row_value(row, "Meaning", "meaning"),
            "Action": _operator_row_value(row, "Action", "action"),
        }
        for row in rows
    ]


def _operator_optional_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    return [
        {
            "Gate": _operator_row_value(row, "Step", "step"),
            "Outcome": _operator_optional_outcome_label(
                _operator_row_value(row, "Outcome", "outcome")
            ),
            "Reason": _operator_row_value(row, "Reason", "reason"),
            "Runs When": _operator_row_value(row, "Trigger", "trigger"),
            "Meaning": _operator_row_value(row, "Meaning", "meaning"),
            "Operator Note": _operator_row_value(row, "Action", "action"),
        }
        for row in rows
    ]


def _operator_optional_outcome_label(value: object) -> str:
    text = str(value or "")
    if text.strip().lower().replace(" ", "_") == "expected_gate":
        return "Not triggered (expected)"
    return text


def _operator_row_value(row: Mapping[str, object], *keys: str) -> str:
    value = _operator_row_object(row, *keys)
    return "" if value is None else str(value)


def _operator_row_object(row: Mapping[str, object], *keys: str) -> object:
    for key in keys:
        if key in row:
            return row.get(key)
    return None


def _show_radar_run_result_notice(
    daily_result: Mapping[str, Any],
    *,
    fallback_reason: object = None,
) -> None:
    status = _metric_text(daily_result.get("status") or fallback_reason or "unknown")
    blocking_messages = _radar_run_limiting_messages(daily_result, blocking_only=True)
    optional_gate_count = _radar_expected_gate_count(daily_result)
    path_counts = _radar_run_path_counts(daily_result)
    if status == "success" and not blocking_messages:
        if optional_gate_count:
            st.info(
                "Radar run completed with expected gates: required path "
                f"{path_counts['completed']}/{path_counts['required']}; "
                f"{optional_gate_count} optional gate(s) did not trigger."
            )
        else:
            st.success("Radar run status: success. Required path completed.")
    elif status == "failed":
        st.error(f"Radar run status: {status}")
    elif blocking_messages:
        st.warning(f"Radar run status: {status}. Analysis is limited.")
    else:
        st.info(f"Radar run status: {status}. No blocking scan failures detected.")
    for message in blocking_messages[:6]:
        st.caption(message)


def _radar_run_path_counts(daily_result: Mapping[str, Any]) -> dict[str, int]:
    required = 0
    completed = 0
    for step in _mapping(daily_result.get("steps")).values():
        step_mapping = _mapping(step)
        classification = classify_step_outcome(
            str(step_mapping.get("status") or ""),
            str(step_mapping.get("reason") or "") or None,
        )
        category = str(step_mapping.get("category") or classification.category)
        if category == "expected_gate":
            continue
        required += 1
        if category == "completed":
            completed += 1
    return {"required": required, "completed": completed}


def _radar_expected_gate_count(daily_result: Mapping[str, Any]) -> int:
    count = 0
    for step in _mapping(daily_result.get("steps")).values():
        step_mapping = _mapping(step)
        category = str(step_mapping.get("category") or "")
        if category == "expected_gate":
            count += 1
            continue
        classification = classify_step_outcome(
            str(step_mapping.get("status") or ""),
            str(step_mapping.get("reason") or "") or None,
        )
        if classification.category == "expected_gate":
            count += 1
    return count


def _radar_default_as_of(*sources: Mapping[str, Any]) -> date:
    for source in sources:
        scope = _mapping(source.get("scope"))
        for value in (scope.get("as_of"), source.get("as_of")):
            if value is not None:
                try:
                    return date.fromisoformat(str(value)[:10])
                except ValueError:
                    pass
    return datetime.now(UTC).date()


def _radar_run_scope_payload(
    *,
    as_of_value: object,
    tickers_text: str,
    provider_text: str,
    universe_text: str,
) -> dict[str, object]:
    payload: dict[str, object] = {}
    if isinstance(as_of_value, datetime):
        payload["as_of"] = as_of_value.date().isoformat()
    elif isinstance(as_of_value, date):
        payload["as_of"] = as_of_value.isoformat()

    tickers = _parse_ticker_list(tickers_text)
    if tickers:
        payload["tickers"] = tickers
    provider = provider_text.strip().lower()
    if provider:
        payload["provider"] = provider
    universe = universe_text.strip()
    if universe:
        payload["universe"] = universe
    return payload


def _parse_ticker_list(value: str) -> list[str]:
    return [
        ticker
        for ticker in dict.fromkeys(
            item.strip().upper()
            for chunk in value.splitlines()
            for item in chunk.replace(";", ",").split(",")
        )
        if ticker
    ]


def _radar_run_limiting_messages(
    daily_result: Mapping[str, Any],
    *,
    blocking_only: bool,
) -> list[str]:
    messages: list[str] = []
    for name, step in _mapping(daily_result.get("steps")).items():
        step_mapping = _mapping(step)
        reason = str(step_mapping.get("reason") or "")
        if not reason:
            continue
        classification = classify_step_outcome(
            str(step_mapping.get("status") or ""),
            reason,
        )
        blocks_reliance = bool(
            step_mapping.get("blocks_reliance", classification.blocks_reliance)
        )
        if blocking_only and not blocks_reliance:
            continue
        explanation = (
            str(step_mapping.get("meaning"))
            if step_mapping.get("meaning") is not None
            else RADAR_SKIP_EXPLANATIONS.get(reason, reason)
        )
        messages.append(f"{name}: {explanation}")
    return messages


def _radar_run_operator_rows(daily_result: Mapping[str, Any]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for name, step in _mapping(daily_result.get("steps")).items():
        step_mapping = _mapping(step)
        reason = str(step_mapping.get("reason") or "")
        classification = classify_step_outcome(
            str(step_mapping.get("status") or ""),
            reason or None,
        )
        blocks_reliance = bool(
            step_mapping.get("blocks_reliance", classification.blocks_reliance)
        )
        rows.append(
            {
                "Outcome": step_mapping.get("label") or classification.label,
                "Needs Action": "yes" if blocks_reliance else "no",
                "Stage": _radar_operator_stage(classification.category),
                "Step": name,
                "Requested": step_mapping.get("requested_count"),
                "Raw": step_mapping.get("raw_count"),
                "Normalized": step_mapping.get("normalized_count"),
                "Reason": reason or None,
                "Meaning": step_mapping.get("meaning") or RADAR_SKIP_EXPLANATIONS.get(reason),
                "Action": step_mapping.get("operator_action")
                or classification.operator_action,
                "Trigger": step_mapping.get("trigger_condition")
                or classification.trigger_condition,
            }
        )
    return rows


def _radar_run_raw_rows(daily_result: Mapping[str, Any]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for name, step in _mapping(daily_result.get("steps")).items():
        step_mapping = _mapping(step)
        reason = str(step_mapping.get("reason") or "")
        classification = classify_step_outcome(
            str(step_mapping.get("status") or ""),
            reason or None,
        )
        rows.append(
            {
                "step": name,
                "raw_status": step_mapping.get("status"),
                "category": step_mapping.get("category") or classification.category,
                "requested": step_mapping.get("requested_count"),
                "raw": step_mapping.get("raw_count"),
                "normalized": step_mapping.get("normalized_count"),
                "reason": reason or None,
                "payload": step_mapping.get("payload"),
            }
        )
    return rows


def _radar_summary_operator_rows(summary: Mapping[str, Any]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for step in _records(summary.get("steps")):
        name = str(step.get("step") or step.get("name") or "")
        status = str(step.get("status") or "")
        reason = str(step.get("reason") or "")
        category = str(step.get("category") or "")
        classification = classify_step_outcome(status, reason or None)
        blocks_reliance = bool(step.get("blocks_reliance", classification.blocks_reliance))
        rows.append(
            {
                "outcome": step.get("label") or classification.label,
                "needs_action": "yes" if blocks_reliance else "no",
                "stage": _radar_operator_stage(category or classification.category),
                "step": name,
                "requested": step.get("requested_count"),
                "raw": step.get("raw_count"),
                "normalized": step.get("normalized_count"),
                "reason": reason or None,
                "meaning": step.get("meaning") or RADAR_SKIP_EXPLANATIONS.get(reason),
                "action": step.get("operator_action") or classification.operator_action,
                "trigger": step.get("trigger_condition")
                or classification.trigger_condition,
            }
        )
    return rows


def _radar_summary_raw_rows(summary: Mapping[str, Any]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for step in _records(summary.get("steps")):
        name = str(step.get("step") or step.get("name") or "")
        status = str(step.get("status") or "")
        reason = str(step.get("reason") or "")
        category = str(step.get("category") or "")
        rows.append(
            {
                "step": name,
                "raw_status": status,
                "category": category or _radar_step_category(status, reason),
                "requested": step.get("requested_count"),
                "raw": step.get("raw_count"),
                "normalized": step.get("normalized_count"),
                "reason": reason or None,
                "payload": step.get("payload"),
            }
        )
    return rows


def _radar_operator_stage(category: object) -> str:
    value = str(category or "").strip()
    if value == "completed":
        return "Required path"
    if value == "expected_gate":
        return "Expected skipped gate"
    if value in {"blocked_input", "failed", "needs_review"}:
        return "Blocked"
    if value == "not_ready":
        return "Waiting for input"
    return value.replace("_", " ").title() if value else "Unknown"


def _radar_step_category(status: str, reason: str) -> str:
    return classify_step_outcome(status, reason or None).category


def _candidate_rows_with_labels(
    rows: list[dict[str, object]],
    investment_readiness: Mapping[str, Any] | None = None,
) -> list[dict[str, object]]:
    labeled: list[dict[str, object]] = []
    for row in dashboard_data.candidate_decision_labels_payload(
        rows,
        investment_readiness,
    ):
        values = dict(row)
        values["supporting_evidence"] = _evidence_label(values.get("top_supporting_evidence"))
        values["disconfirming_evidence"] = _evidence_label(values.get("top_disconfirming_evidence"))
        values["blocker_summary"] = _candidate_blocker_summary(values)
        labeled.append(values)
    return labeled


def _candidate_blocker_summary(row: Mapping[str, object]) -> object:
    blockers = _candidate_blocker_values(row)
    if blockers:
        visible = blockers[:3]
        suffix = f" +{len(blockers) - len(visible)}" if len(blockers) > len(visible) else ""
        return f"{', '.join(visible)}{suffix}"
    evidence = _mapping(row.get("top_disconfirming_evidence"))
    if evidence:
        return _evidence_label(evidence)
    risk_or_gap = str(_mapping(row.get("research_brief")).get("risk_or_gap") or "").strip()
    if risk_or_gap and not risk_or_gap.lower().startswith("no disconfirming evidence"):
        return risk_or_gap
    return "none captured"


def _candidate_blocker_values(
    row: Mapping[str, object],
    *,
    include_transition_reasons: bool = True,
) -> list[str]:
    values: list[str] = []
    for key in ("hard_blocks", "portfolio_hard_blocks"):
        for item in _sequence(row.get(key)):
            text = str(item or "").strip()
            if text and text not in values:
                values.append(text)
    if include_transition_reasons:
        for item in _sequence(row.get("transition_reasons")):
            text = str(item or "").strip()
            if text and text not in values:
                values.append(text)
    return values


def _candidate_blocker_rows(row: Mapping[str, object]) -> list[dict[str, object]]:
    action = _first_present(
        row.get("decision_next_step"),
        _mapping(row.get("research_brief")).get("next_step"),
        "Review the raw candidate state before escalation.",
    )
    rows: list[dict[str, object]] = []
    seen: set[tuple[str, str]] = set()

    def add(kind: str, detail: object, source: str) -> None:
        text = _metric_text(detail)
        if text == "n/a":
            return
        key = (kind, text)
        if key in seen:
            return
        seen.add(key)
        rows.append(
            {
                "type": kind,
                "detail": detail,
                "source": source,
                "action": action,
            }
        )

    for block in _sequence(row.get("hard_blocks")):
        add("Hard block", block, "scoring policy")
    for block in _sequence(row.get("portfolio_hard_blocks")):
        add("Portfolio block", block, "portfolio context")
    for reason in _sequence(row.get("transition_reasons")):
        add("Transition reason", reason, "state policy")

    evidence = _mapping(row.get("top_disconfirming_evidence"))
    if evidence:
        add(
            "Disconfirming evidence",
            _evidence_label(evidence),
            str(evidence.get("kind") or "candidate packet"),
        )
    return rows


def _candidate_decision_brief_rows(row: Mapping[str, object]) -> list[dict[str, object]]:
    brief = _mapping(row.get("research_brief"))
    support = _mapping(row.get("top_supporting_evidence"))
    risk = _mapping(row.get("top_disconfirming_evidence"))
    source = _first_present(
        brief.get("source"),
        support.get("source_id"),
        support.get("kind"),
    )
    source_url = _first_present(
        brief.get("source_url"),
        support.get("source_url"),
    )
    return [
        {
            "question": "Why now",
            "answer": _first_present(
                brief.get("why_now"),
                row.get("top_event_title"),
                _evidence_label(support),
                "No catalyst summary captured.",
            ),
        },
        {
            "question": "Best evidence",
            "answer": _first_present(
                brief.get("supporting_evidence"),
                row.get("supporting_evidence"),
                _evidence_label(support),
                "No supporting evidence captured.",
            ),
        },
        {
            "question": "Risk or gap",
            "answer": _first_present(
                brief.get("risk_or_gap"),
                row.get("blocker_summary"),
                row.get("disconfirming_evidence"),
                _evidence_label(risk),
                "No risk or gap captured.",
            ),
        },
        {
            "question": "Candidate next step",
            "answer": _first_present(
                row.get("decision_next_step"),
                brief.get("next_step"),
                "Review the candidate before escalation.",
            ),
        },
        {
            "question": "Readiness gate",
            "answer": _first_present(
                row.get("decision_readiness_gate"),
                "No global readiness gate is blocking this row.",
            ),
        },
        {
            "question": "Source",
            "answer": (
                _url_link(source, source_url)
                if source not in (None, "")
                else _first_present(source_url, "No source reference captured.")
            ),
        },
    ]


def _latest_opportunity_action_rows(
    engine: object,
    ticker: object,
    *,
    limit: int = 3,
) -> list[dict[str, object]]:
    symbol = str(ticker or "").strip().upper()
    if not symbol:
        return []
    repo = BrokerRepository(engine)
    return [
        opportunity_action_payload(row)
        for row in repo.list_opportunity_actions(ticker=symbol, limit=max(1, int(limit)))
    ]


def _show_candidate_opportunity_action_form(
    *,
    engine: object,
    selected_candidate: Mapping[str, object],
    dashboard_role: Role,
) -> None:
    ticker = str(selected_candidate.get("ticker") or "").strip().upper()
    if not ticker:
        return
    if not role_allows(dashboard_role, Role.ANALYST):
        st.caption("Analyst role required to save candidate actions.")
        return
    with st.form(f"overview_opportunity_action_form_{ticker}"):
        action = st.selectbox(
            "Candidate action",
            ["watch", "ready", "simulate_entry", "dismiss"],
            help="Save your current operator stance for this candidate.",
        )
        thesis = st.text_area(
            "Thesis",
            value=str(_mapping(selected_candidate.get("research_brief")).get("why_now") or ""),
            height=80,
        )
        notes = st.text_input("Notes")
        submitted = st.form_submit_button("Save Candidate Action")
    if not submitted:
        return
    try:
        record_opportunity_action(
            repo=BrokerRepository(engine),
            ticker=ticker,
            action=action,
            thesis=thesis,
            notes=notes,
            payload={
                "source": "overview_candidate_queue",
                "decision_status": selected_candidate.get("decision_status"),
                "state": selected_candidate.get("state"),
                "final_score": selected_candidate.get("final_score"),
                "blocker_summary": selected_candidate.get("blocker_summary"),
            },
            actor_source="dashboard",
            actor_id="local-dashboard",
            actor_role=dashboard_role.value,
        )
    except ValueError as exc:
        st.error(str(exc))
        return
    st.success(f"Saved {action} action for {ticker}.")


def _visible_research_brief(value: object) -> dict[str, object]:
    brief = dict(_mapping(value))
    brief.pop("audit", None)
    return brief


def _show_mapping(title: str, value: object, *, empty: str) -> None:
    st.subheader(title)
    mapping = _mapping(value)
    if not mapping:
        st.caption(empty)
        return
    rows = [
        {"Field": str(key).replace("_", " "), "Value": _json_ready(item)}
        for key, item in mapping.items()
    ]
    _show_html_table(pd.DataFrame(rows))


def _show_records(title: str, value: object, *, empty: str) -> None:
    st.subheader(title)
    records = _records(value)
    if not records:
        st.caption(empty)
        return
    columns = list(records[0].keys())
    for record in records[1:]:
        for column in record:
            if column not in columns:
                columns.append(column)
    display_rows = [
        {column.replace("_", " ").title(): record.get(column) for column in columns}
        for record in records
    ]
    _show_html_table(pd.DataFrame(display_rows))


def _nested(mapping: Mapping[str, Any], *keys: str) -> object:
    current: object = mapping
    for key in keys:
        current_mapping = _mapping(current)
        if key not in current_mapping:
            return None
        current = current_mapping[key]
    return current


def _show_overview(
    *,
    engine: object,
    config: AppConfig,
    radar_run_summary: Mapping[str, Any],
    radar_run_cooldown: Mapping[str, Any],
    discovery_snapshot: Mapping[str, Any],
    candidate_rows: list[dict[str, object]],
    alert_rows: list[dict[str, object]],
    ipo_rows: list[dict[str, object]],
    theme_rows: list[dict[str, object]],
    validation_summary: Mapping[str, Any],
    cost_summary: Mapping[str, Any],
    ops_health: Mapping[str, Any],
    broker_summary: Mapping[str, Any],
    dashboard_role: Role,
) -> None:
    candidate_frame = pd.DataFrame(candidate_rows)
    alert_frame = pd.DataFrame(alert_rows)
    validation_report = _mapping(validation_summary.get("report"))
    database = _mapping(ops_health.get("database"))
    market_context = broker_summary.get("market_context")
    display_candidate_rows = dashboard_data.candidate_rows_with_market_context(
        candidate_rows,
        market_context,
    )

    metric_cols = st.columns(6)
    metric_cols[0].metric("Candidates", len(candidate_rows))
    metric_cols[1].metric(
        "Avg Score",
        (
            f"{pd.to_numeric(candidate_frame['final_score']).fillna(0).mean():.2f}"
            if "final_score" in candidate_frame
            else "0.00"
        ),
    )
    metric_cols[2].metric("Alerts", len(alert_rows))
    metric_cols[3].metric("IPO S-1", len(ipo_rows))
    metric_cols[4].metric("Themes", len(theme_rows))
    metric_cols[5].metric("LLM Cost", _currency(cost_summary.get("total_actual_cost_usd")))

    _show_operator_work_queue(
        config=config,
        radar_run_summary=radar_run_summary,
        broker_summary=broker_summary,
        discovery_snapshot=discovery_snapshot,
        candidate_rows=candidate_rows,
    )
    _show_activation_summary(config, radar_run_summary, broker_summary)
    _show_live_activation_plan(config, radar_run_summary, broker_summary)
    _show_live_data_activation_contract(config, radar_run_summary, broker_summary)
    _show_telemetry_tape(ops_health)
    _show_universe_coverage(config, ops_health)
    _show_radar_run_controls(engine, config, radar_run_summary, radar_run_cooldown)
    _show_agent_review_summary(radar_run_summary, candidate_rows)
    _show_discovery_snapshot(discovery_snapshot)
    investment_readiness = _show_investment_readiness(discovery_snapshot, candidate_rows)
    _show_decision_contract(investment_readiness, candidate_rows)
    _show_research_shortlist(
        candidate_rows,
        investment_readiness,
        market_context=market_context,
    )
    _show_candidate_delta(engine, radar_run_summary)
    _show_actionability_breakdown(candidate_rows)
    _show_records(
        "Opportunity Focus",
        dashboard_data.opportunity_focus_payload(display_candidate_rows),
        empty="No opportunity focus rows.",
    )
    _show_records(
        "Provider Preflight",
        dashboard_data.provider_preflight_payload(
            config,
            radar_run_summary=radar_run_summary,
            broker_summary=broker_summary,
        ),
        empty="No provider preflight rows.",
    )
    _show_records(
        "Data Source Coverage",
        dashboard_data.data_source_coverage_payload(
            config,
            broker_summary=broker_summary,
        ),
        empty="No data source coverage rows.",
    )
    _show_records(
        "Readiness Checklist",
        dashboard_data.readiness_checklist_payload(
            config,
            radar_run_summary=radar_run_summary,
            broker_summary=broker_summary,
        ),
        empty="No readiness checklist rows.",
    )
    _show_alert_planning_diagnostics(engine, radar_run_summary)

    secondary_cols = st.columns(5)
    secondary_cols[0].metric("Useful Alert Rate", _rate(validation_report.get("useful_alert_rate")))
    secondary_cols[1].metric(
        "False Positives",
        int(_metric_number(validation_report.get("false_positive_count"))),
    )
    secondary_cols[2].metric(
        "High/Critical Alerts",
        (
            int(
                alert_frame.get("priority", pd.Series(dtype=object))
                .isin(["high", "critical"])
                .sum()
            )
            if not alert_frame.empty
            else 0
        ),
    )
    secondary_cols[3].metric(
        "Database",
        str(database.get("status") or "unknown"),
    )
    secondary_cols[4].metric(
        "Degraded",
        "yes" if bool(_mapping(ops_health.get("degraded_mode")).get("enabled")) else "no",
    )

    left, right = st.columns([2, 1])
    with left:
        st.subheader("Candidate Queue")
        selected_candidate = _table(
            _candidate_rows_with_labels(display_candidate_rows, investment_readiness),
            columns=[
                "ticker",
                "decision_status",
                "state",
                "final_score",
                "setup_type",
                "schwab_last_price",
                "schwab_day_change_percent",
                "schwab_relative_volume",
                "schwab_context_status",
                "top_event_type",
                "supporting_evidence",
                "blocker_summary",
                "decision_card_id",
                "decision_next_step",
                "decision_readiness_gate",
                "next_review_at",
            ],
            labels={
                "ticker": "Ticker",
                "decision_status": "Decision",
                "state": "State",
                "final_score": "Score",
                "setup_type": "Setup",
                "schwab_last_price": "Schwab Price",
                "schwab_day_change_percent": "Schwab %",
                "schwab_relative_volume": "Schwab RVOL",
                "schwab_context_status": "Schwab Context",
                "top_event_type": "Top Event",
                "supporting_evidence": "Evidence",
                "blocker_summary": "Risk / Blocker",
                "decision_card_id": "Card",
                "decision_next_step": "Next Step",
                "decision_readiness_gate": "Readiness Gate",
                "next_review_at": "Next Review",
            },
            empty="No candidate rows.",
            key="overview_candidate_queue",
            selectable=True,
        )
    with right:
        st.subheader("State Mix")
        _show_state_mix(candidate_rows)

    if selected_candidate:
        _show_status_badges(
            [
                ("Ticker", selected_candidate.get("ticker")),
                ("State", selected_candidate.get("state")),
                ("Decision", selected_candidate.get("decision_status")),
                ("Priority Score", selected_candidate.get("final_score")),
                ("Setup", selected_candidate.get("setup_type")),
                ("Schwab", selected_candidate.get("schwab_context_status")),
            ]
        )
        _show_records(
            "Decision Brief",
            _candidate_decision_brief_rows(selected_candidate),
            empty="No decision brief.",
        )
        _show_mapping(
            "Selected Candidate",
            {
                "ticker": selected_candidate.get("ticker"),
                "decision_status": selected_candidate.get("decision_status"),
                "decision_next_step": selected_candidate.get("decision_next_step"),
                "decision_readiness_gate": selected_candidate.get(
                    "decision_readiness_gate"
                ),
                "state": selected_candidate.get("state"),
                "score": selected_candidate.get("final_score"),
                "top_event": selected_candidate.get("top_event_title"),
                "schwab_last_price": selected_candidate.get("schwab_last_price"),
                "schwab_day_change_percent": selected_candidate.get(
                    "schwab_day_change_percent"
                ),
                "schwab_relative_volume": selected_candidate.get(
                    "schwab_relative_volume"
                ),
                "schwab_price_trend_5d_percent": selected_candidate.get(
                    "schwab_price_trend_5d_percent"
                ),
                "schwab_option_call_put_ratio": selected_candidate.get(
                    "schwab_option_call_put_ratio"
                ),
                "schwab_market_as_of": selected_candidate.get("schwab_market_as_of"),
                "supporting_evidence": selected_candidate.get("supporting_evidence"),
                "disconfirming_evidence": selected_candidate.get("disconfirming_evidence"),
                "blocker_summary": selected_candidate.get("blocker_summary"),
                "hard_blocks": selected_candidate.get("hard_blocks"),
                "transition_reasons": selected_candidate.get("transition_reasons"),
                "decision_card_id": selected_candidate.get("decision_card_id"),
                "next_review_at": selected_candidate.get("next_review_at"),
            },
            empty="No selected candidate.",
        )
        _show_records(
            "Blocker Diagnostics",
            _candidate_blocker_rows(selected_candidate),
            empty="No blocker diagnostics.",
        )
        _show_candidate_opportunity_action_form(
            engine=engine,
            selected_candidate=selected_candidate,
            dashboard_role=dashboard_role,
        )
        _show_candidate_schwab_context_refresh(
            config=config,
            selected_candidate=selected_candidate,
            dashboard_role=dashboard_role,
        )
        _show_records(
            "Saved Candidate Actions",
            _latest_opportunity_action_rows(
                engine,
                selected_candidate.get("ticker"),
            ),
            empty="No saved candidate actions.",
        )
        _show_mapping(
            "Research Brief",
            _visible_research_brief(selected_candidate.get("research_brief")),
            empty="No research brief.",
        )

    st.subheader("Recent Alerts")
    _table(
        alert_rows[:10],
        columns=["ticker", "priority", "status", "route", "title", "available_at", "feedback"],
        labels={
            "ticker": "Ticker",
            "priority": "Priority",
            "status": "Status",
            "route": "Route",
            "title": "Title",
            "available_at": "Available",
            "feedback": "Feedback",
        },
        empty="No alert rows.",
    )


def _show_candidate_schwab_context_refresh(
    *,
    config: AppConfig,
    selected_candidate: Mapping[str, object],
    dashboard_role: Role,
) -> None:
    st.subheader("Schwab Market Context")
    refresh_message = st.session_state.pop("candidate_schwab_refresh_message", None)
    if refresh_message:
        st.success(str(refresh_message))
    ticker = str(selected_candidate.get("ticker") or "").strip().upper()
    context_status = str(selected_candidate.get("schwab_context_status") or "missing")
    if context_status == "available":
        st.caption("Stored Schwab quote, volume, history, and options context is available.")
    else:
        st.caption(
            "No stored Schwab market context for this candidate yet. Refreshing is an "
            "explicit Schwab API call guarded by the market-sync cooldown and ticker cap."
        )
    if not role_allows(dashboard_role, Role.ANALYST):
        st.caption("Analyst role required to refresh Schwab market context.")
        return
    disabled = not ticker
    if st.button(
        "Refresh Schwab Context",
        key=f"refresh_schwab_context_{ticker or 'none'}",
        disabled=disabled,
        help="Calls the existing rate-limited Schwab market-sync endpoint for this ticker.",
    ):
        try:
            result = _api_post(
                config,
                "/api/brokers/schwab/market-sync",
                {"tickers": [ticker], "include_history": True, "include_options": True},
            )
            st.session_state["candidate_schwab_refresh_message"] = (
                f"Schwab market context rows: {len(_records(_mapping(result).get('items')))}"
            )
            st.rerun()
        except RuntimeError as exc:
            st.error(str(exc))


def _show_ticker_layer(engine, ticker: str, cutoff: datetime | None) -> None:
    if not ticker:
        st.info("Select a ticker in the sidebar.")
        return
    detail = dashboard_data.load_ticker_detail(engine, ticker, available_at=cutoff)
    if detail is None:
        st.warning("Ticker not found in current radar data.")
        return

    latest_candidate = _mapping(detail.get("latest_candidate"))
    candidate_packet = _mapping(detail.get("candidate_packet"))
    decision_card = _mapping(detail.get("decision_card"))
    packet_payload = _mapping(candidate_packet.get("payload")) or candidate_packet
    card_payload = _mapping(decision_card.get("payload")) or decision_card

    metric_cols = st.columns(6)
    metric_cols[0].metric("Ticker", _metric_text(detail.get("ticker") or ticker))
    metric_cols[1].metric("State", _metric_text(latest_candidate.get("state")))
    metric_cols[2].metric("Score", _metric_text(latest_candidate.get("final_score")))
    metric_cols[3].metric("Setup", _metric_text(latest_candidate.get("setup_type")))
    metric_cols[4].metric("As Of", _metric_text(latest_candidate.get("as_of")))
    metric_cols[5].metric(
        "Review Only",
        _metric_text(
            _first_present(
                detail.get("manual_review_only"),
                card_payload.get("manual_review_only"),
            )
        ),
    )
    _show_status_badges(
        [
            ("State", latest_candidate.get("state")),
            ("Setup", latest_candidate.get("setup_type")),
            ("Review Only", detail.get("manual_review_only")),
        ]
    )

    setup_plan = _first_present(
        detail.get("setup_plan"),
        card_payload.get("trade_plan"),
        packet_payload.get("trade_plan"),
    )
    portfolio_impact = _first_present(
        detail.get("portfolio_impact"),
        card_payload.get("portfolio_impact"),
        packet_payload.get("portfolio_impact"),
    )

    left, right = st.columns([1, 1])
    with left:
        _show_mapping("Setup Plan", setup_plan, empty="No setup plan fields.")
    with right:
        hard_blocks = _first_present(
            latest_candidate.get("hard_blocks"),
            latest_candidate.get("portfolio_hard_blocks"),
            _nested(card_payload, "controls", "hard_blocks"),
        )
        _show_records("Hard Blocks", hard_blocks, empty="No hard blocks.")

    evidence_left, evidence_right = st.columns([1, 1])
    with evidence_left:
        _show_records(
            "Supporting Evidence",
            _first_present(
                packet_payload.get("supporting_evidence"),
                card_payload.get("supporting_evidence"),
                latest_candidate.get("top_supporting_evidence"),
            ),
            empty="No supporting evidence.",
        )
    with evidence_right:
        _show_records(
            "Disconfirming Evidence",
            _first_present(
                packet_payload.get("disconfirming_evidence"),
                card_payload.get("disconfirming_evidence"),
                latest_candidate.get("top_disconfirming_evidence"),
            ),
            empty="No disconfirming evidence.",
        )

    _show_records("Events", detail.get("events"), empty="No event rows.")
    _show_records("Snippets", detail.get("snippets"), empty="No snippet rows.")
    _show_mapping("Portfolio Impact", portfolio_impact, empty="No portfolio impact fields.")
    _show_records("Validation Rows", detail.get("validation_results"), empty="No validation rows.")
    _show_records("Paper Trades", detail.get("paper_trades"), empty="No paper rows.")


def _show_ipo_layer(ipo_rows: list[dict[str, object]]) -> None:
    metric_cols = st.columns(4)
    metric_cols[0].metric("Filings", len(ipo_rows))
    metric_cols[1].metric(
        "With Price Range",
        sum(1 for row in ipo_rows if row.get("price_range_low") is not None),
    )
    metric_cols[2].metric(
        "With Risk Flags",
        sum(1 for row in ipo_rows if _sequence(row.get("risk_flags"))),
    )
    metric_cols[3].metric(
        "Median Gross Proceeds",
        _currency(
            pd.Series(
                [
                    _metric_number(row.get("estimated_gross_proceeds"))
                    for row in ipo_rows
                    if row.get("estimated_gross_proceeds") is not None
                ]
            ).median()
            if ipo_rows
            else 0.0
        ),
    )

    _table(
        [_ipo_summary_row(row) for row in ipo_rows],
        columns=[
            "ticker",
            "form_type",
            "filing_date",
            "proposed_ticker",
            "exchange",
            "shares_offered",
            "price_range_low",
            "price_range_high",
            "estimated_gross_proceeds",
            "risk_flags",
        ],
        labels={
            "ticker": "Ticker",
            "form_type": "Form",
            "filing_date": "Filing Date",
            "proposed_ticker": "Proposed Symbol",
            "exchange": "Exchange",
            "shares_offered": "Shares",
            "price_range_low": "Low",
            "price_range_high": "High",
            "estimated_gross_proceeds": "Gross Proceeds",
            "risk_flags": "Risk Flags",
        },
        empty="No IPO S-1 analysis rows.",
    )

    if ipo_rows:
        selected_index = int(
            st.selectbox(
                "Selected S-1 filing",
                list(range(len(ipo_rows))),
                format_func=lambda index: _ipo_detail_label(ipo_rows[int(index)], int(index)),
                key="ipo_s1_selected_filing",
            )
        )
        row = ipo_rows[selected_index]
        left, right = st.columns([1, 1])
        with left:
            _show_ipo_terms(row)
        with right:
            _show_ipo_notes(row)


def _ipo_summary_row(row: Mapping[str, object]) -> dict[str, object]:
    values = dict(row)
    gross_proceeds = row.get("estimated_gross_proceeds")
    if gross_proceeds not in (None, ""):
        values["estimated_gross_proceeds"] = _currency(gross_proceeds)
    if row.get("shares_offered") not in (None, ""):
        values["shares_offered"] = _integer(row.get("shares_offered"))
    for key in ("price_range_low", "price_range_high"):
        value = row.get(key)
        if value not in (None, ""):
            values[key] = _currency(value)
    return values


def _ipo_detail_label(row: Mapping[str, object], index: int) -> str:
    ticker = str(_first_present(row.get("ticker"), row.get("proposed_ticker"), "") or "").upper()
    form_type = _metric_text(row.get("form_type"))
    filing_date = _metric_text(row.get("filing_date"))
    gross = (
        _currency(row.get("estimated_gross_proceeds"))
        if row.get("estimated_gross_proceeds") not in (None, "")
        else "gross n/a"
    )
    prefix = f"{ticker} {form_type}".strip() or f"Filing {index + 1}"
    return f"{prefix} filed {filing_date} - {gross}"


def _show_ipo_notes(row: Mapping[str, object]) -> None:
    notes = [
        ("Summary", row.get("summary")),
        ("Underwriters", _list_text(row.get("underwriters"))),
        ("Sections", _list_text(row.get("sections_found"))),
        ("Use of proceeds", row.get("use_of_proceeds_summary")),
    ]
    body = "".join(
        '<div class="mr-note-row">'
        f'<span class="mr-note-label">{_html(label)}</span>'
        f"<p>{_value_html(value)}</p>"
        "</div>"
        for label, value in notes
        if _metric_text(value) != "n/a"
    )
    st.subheader("Offering Notes")
    st.markdown(f'<div class="mr-note-card">{body or "No notes."}</div>', unsafe_allow_html=True)


def _show_ipo_terms(row: Mapping[str, object]) -> None:
    analysis = _mapping(row.get("analysis"))
    term_rows = [
        ("Company", _first_present(analysis.get("company_name"), row.get("company_name"))),
        ("Form", _first_present(row.get("form_type"), analysis.get("form_type"))),
        (
            "Proposed symbol",
            _first_present(row.get("proposed_ticker"), analysis.get("proposed_ticker")),
        ),
        ("Exchange", _first_present(row.get("exchange"), analysis.get("exchange"))),
        (
            "Shares offered",
            _integer(_first_present(row.get("shares_offered"), analysis.get("shares_offered"))),
        ),
        (
            "Price range",
            _price_range(
                _first_present(row.get("price_range_low"), analysis.get("price_range_low")),
                _first_present(row.get("price_range_high"), analysis.get("price_range_high")),
            ),
        ),
        (
            "Estimated gross proceeds",
            _currency(
                _first_present(
                    row.get("estimated_gross_proceeds"),
                    analysis.get("estimated_gross_proceeds"),
                )
            ),
        ),
        (
            "Source",
            _first_present(
                row.get("document_url"),
                row.get("source_url"),
                analysis.get("source_url"),
            ),
        ),
        (
            "Underwriters",
            _sequence(_first_present(row.get("underwriters"), analysis.get("underwriters"))),
        ),
        (
            "Risk flags",
            _sequence(_first_present(row.get("risk_flags"), analysis.get("risk_flags"))),
        ),
        (
            "Sections found",
            _sequence(_first_present(row.get("sections_found"), analysis.get("sections_found"))),
        ),
    ]
    rows = [
        {"Field": label, "Value": value}
        for label, value in term_rows
        if _metric_text(value) != "n/a" and value != ()
    ]
    st.subheader("Offering Terms")
    _show_html_table(pd.DataFrame(rows))


def _show_alerts_layer(
    alert_rows: list[dict[str, object]],
    *,
    engine,
    cutoff: datetime | None,
) -> None:
    frame = pd.DataFrame(alert_rows)
    metric_cols = st.columns(5)
    metric_cols[0].metric("Total", len(alert_rows))
    metric_cols[1].metric(
        "Planned",
        int((frame.get("status") == "planned").sum()) if not frame.empty else 0,
    )
    metric_cols[2].metric(
        "Dry Run",
        int((frame.get("status") == "dry_run").sum()) if not frame.empty else 0,
    )
    metric_cols[3].metric(
        "High/Critical",
        (
            int(frame.get("priority", pd.Series(dtype=object)).isin(["high", "critical"]).sum())
            if not frame.empty
            else 0
        ),
    )
    metric_cols[4].metric(
        "Useful Feedback",
        (
            int(
                frame.get("feedback_label", pd.Series(dtype=object))
                .isin(USEFUL_FEEDBACK_LABELS)
                .sum()
            )
            if not frame.empty
            else 0
        ),
    )

    selected_alert = _table(
        alert_rows,
        columns=[
            "id",
            "ticker",
            "route",
            "channel",
            "priority",
            "status",
            "state",
            "score_trigger",
            "title",
            "available_at",
            "feedback",
        ],
        labels={
            "id": "ID",
            "ticker": "Ticker",
            "route": "Route",
            "channel": "Channel",
            "priority": "Priority",
            "status": "Status",
            "state": "State",
            "score_trigger": "Score",
            "title": "Title",
            "available_at": "Available",
            "feedback": "Feedback",
        },
        empty="No alert rows.",
        key="alerts_table",
        selectable=True,
    )

    st.subheader("Alert Detail")
    alert_ids = [str(row.get("id")) for row in alert_rows if row.get("id")]
    if selected_alert:
        selected_alert_id = str(selected_alert.get("id") or "").strip()
    elif alert_ids:
        selected_alert_id = st.selectbox("Alert", alert_ids)
    else:
        selected_alert_id = ""
    if not selected_alert_id:
        return
    detail = dashboard_data.load_alert_detail(engine, selected_alert_id, available_at=cutoff)
    if detail is None:
        st.warning("Alert ID was not found.")
        return
    metric_cols = st.columns(5)
    metric_cols[0].metric("Ticker", _metric_text(detail.get("ticker")))
    metric_cols[1].metric("Route", _metric_text(detail.get("route")))
    metric_cols[2].metric("Priority", _metric_text(detail.get("priority")))
    metric_cols[3].metric("Status", _metric_text(detail.get("status")))
    metric_cols[4].metric("Feedback", _metric_text(detail.get("feedback_label")))
    _show_status_badges(
        [
            ("Priority", detail.get("priority")),
            ("Status", detail.get("status")),
            ("Feedback", detail.get("feedback_label")),
        ]
    )
    left, right = st.columns([1, 1])
    with left:
        _show_mapping(
            "Review Context",
            {
                "id": detail.get("id"),
                "candidate_state_id": detail.get("candidate_state_id"),
                "candidate_packet_id": detail.get("candidate_packet_id"),
                "decision_card_id": detail.get("decision_card_id"),
                "trigger_kind": detail.get("trigger_kind"),
                "trigger_fingerprint": detail.get("trigger_fingerprint"),
                "summary": detail.get("summary"),
            },
            empty="No review context.",
        )
    with right:
        _show_mapping(
            "Feedback Reference",
            {
                "feedback_url": detail.get("feedback_url"),
                "feedback_id": detail.get("feedback_id"),
                "feedback_label": detail.get("feedback_label"),
                "feedback_notes": detail.get("feedback_notes"),
                "feedback_created_at": detail.get("feedback_created_at"),
            },
            empty="No feedback reference.",
        )
    _show_mapping("Evidence Payload", detail.get("payload"), empty="No evidence payload.")


def _show_themes_layer(theme_rows: list[dict[str, object]]) -> None:
    frame = pd.DataFrame(theme_rows)
    metric_cols = st.columns(4)
    metric_cols[0].metric("Themes", len(theme_rows))
    metric_cols[1].metric(
        "Candidates",
        int(pd.to_numeric(frame.get("candidate_count", pd.Series(dtype=float))).fillna(0).sum()),
    )
    metric_cols[2].metric(
        "Average Score",
        f"{pd.to_numeric(frame.get('avg_score', pd.Series(dtype=float))).fillna(0).mean():.2f}"
        if not frame.empty
        else "0.00",
    )
    metric_cols[3].metric(
        "Latest As Of",
        str(frame["latest_as_of"].dropna().max() if "latest_as_of" in frame else "n/a"),
    )
    _table(
        theme_rows,
        columns=["theme", "candidate_count", "avg_score", "top_tickers", "states", "latest_as_of"],
        labels={
            "theme": "Theme",
            "candidate_count": "Candidates",
            "avg_score": "Average Score",
            "top_tickers": "Top Tickers",
            "states": "State Mix",
            "latest_as_of": "Latest As Of",
        },
        empty="No theme rows.",
    )


def _show_validation_layer(summary: Mapping[str, Any]) -> None:
    if not summary:
        st.info("No validation summary.")
        return
    latest_run = _mapping(summary.get("latest_run"))
    report = _mapping(summary.get("report"))
    precision = _mapping(report.get("precision"))
    primary_precision = precision.get("target_20d_25")
    if primary_precision is None and precision:
        primary_precision = next(iter(precision.values()))

    metric_cols = st.columns(6)
    metric_cols[0].metric("Run", str(latest_run.get("id") or report.get("run_id") or "n/a"))
    metric_cols[1].metric("Precision", _rate(primary_precision))
    metric_cols[2].metric("Useful Rate", _rate(report.get("useful_alert_rate")))
    metric_cols[3].metric(
        "False Positives",
        int(_metric_number(report.get("false_positive_count"))),
    )
    metric_cols[4].metric("Missed", int(_metric_number(report.get("missed_opportunity_count"))))
    metric_cols[5].metric("Leakage Flags", int(_metric_number(report.get("leakage_failure_count"))))

    left, right = st.columns([1, 1])
    with left:
        _show_mapping("Latest Run", latest_run, empty="No latest run.")
    with right:
        _show_mapping("Report", report, empty="No validation report.")
    _show_records("Paper Trades", summary.get("paper_trades"), empty="No paper rows.")
    _show_records("Useful Labels", summary.get("useful_labels"), empty="No useful labels.")


def _show_costs_layer(summary: Mapping[str, Any]) -> None:
    actual_cost = _metric_number(summary.get("total_actual_cost_usd"))
    estimated_cost = _metric_number(summary.get("total_estimated_cost_usd"))
    status_counts = _mapping(summary.get("status_counts"))
    useful_count = int(_metric_number(summary.get("useful_alert_count")))
    cost_per_useful = summary.get("cost_per_useful_alert")

    metric_cols = st.columns(4)
    metric_cols[0].metric("Actual LLM Cost", _currency(actual_cost))
    metric_cols[1].metric("Estimated LLM Cost", _currency(estimated_cost))
    metric_cols[2].metric("Attempts", int(_metric_number(summary.get("attempt_count"))))
    metric_cols[3].metric("Skipped", int(_metric_number(status_counts.get("skipped"))))
    secondary_cols = st.columns(3)
    secondary_cols[0].metric("Completed", int(_metric_number(status_counts.get("completed"))))
    secondary_cols[1].metric("Useful Alerts", useful_count)
    secondary_cols[2].metric(
        "Cost / Useful",
        "n/a" if cost_per_useful is None else _currency(cost_per_useful),
    )
    _show_records("Ledger Rows", summary.get("rows"), empty="No budget ledger rows.")
    _show_records("Spend By Task", summary.get("by_task"), empty="No task rows.")
    _show_records("Spend By Model", summary.get("by_model"), empty="No model rows.")


def _api_base_url(config: AppConfig) -> str:
    redirect = str(config.schwab_redirect_uri or "").strip()
    if redirect:
        parsed = urlparse(redirect)
        if parsed.scheme and parsed.netloc:
            return f"{parsed.scheme}://{parsed.netloc}".rstrip("/")
    return "http://127.0.0.1:8000"


def _api_post(config: AppConfig, path: str, payload: Mapping[str, Any] | None = None) -> object:
    base_url = _api_base_url(config)
    url = f"{base_url}{path}"
    data = json.dumps(dict(payload or {})).encode("utf-8")
    request = Request(
        url,
        data=data,
        method="POST",
        headers={"content-type": "application/json"},
    )
    parsed = urlparse(url)
    local_https = parsed.scheme == "https" and parsed.hostname in {"127.0.0.1", "localhost"}
    context = ssl._create_unverified_context() if local_https else None
    try:
        with urlopen(request, timeout=config.http_timeout_seconds, context=context) as response:
            body = response.read().decode("utf-8")
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"API {exc.code}: {detail}") from exc
    except URLError as exc:
        raise RuntimeError(f"API unavailable: {exc.reason}") from exc
    return json.loads(body) if body else {}


def _broker_ticker_options(
    summary: Mapping[str, Any],
    candidate_rows: list[dict[str, object]],
) -> list[str]:
    tickers: set[str] = set()
    for collection in (
        candidate_rows,
        _records(summary.get("positions")),
        _records(summary.get("market_context")),
        _records(summary.get("triggers")),
        _records(summary.get("order_tickets")),
    ):
        for row in collection:
            ticker = str(row.get("ticker") or "").strip().upper()
            if ticker:
                tickers.add(ticker)
    return sorted(tickers)


def _broker_latest_price(summary: Mapping[str, Any], ticker: str) -> float:
    symbol = ticker.upper()
    for row in _records(summary.get("market_context")):
        if str(row.get("ticker") or "").upper() == symbol:
            return _metric_number(row.get("last_price") or row.get("mark_price"))
    return 0.0


def _show_broker_rate_limits(summary: Mapping[str, Any]) -> None:
    rows = []
    for row in _records(summary.get("rate_limits")):
        retry_after = int(_metric_number(row.get("retry_after_seconds")))
        rows.append(
            {
                "Operation": _metric_text(row.get("operation")),
                "Allowed": "yes" if bool(row.get("allowed")) else "no",
                "Min Interval": f"{int(_metric_number(row.get('min_interval_seconds')))}s",
                "Retry After": f"{retry_after}s" if retry_after else "ready",
                "Reset At": _metric_text(row.get("reset_at")),
            }
        )
    _show_records("Schwab API Guard", rows, empty="No Schwab API guard state.")


def _show_broker_controls(
    *,
    config: AppConfig,
    selected_ticker: str,
    dashboard_role: Role,
) -> None:
    st.subheader("Broker Controls")
    if not role_allows(dashboard_role, Role.ANALYST):
        st.caption("Analyst role required for broker control actions.")
        return
    api_base = _api_base_url(config)
    control_cols = st.columns(4)
    control_cols[0].link_button(
        "Connect Schwab",
        f"{api_base}/api/brokers/schwab/connect",
        use_container_width=True,
    )
    if control_cols[1].button("Sync Portfolio", use_container_width=True):
        try:
            result = _api_post(config, "/api/brokers/schwab/sync")
            st.success(f"Portfolio sync queued: {_metric_text(_mapping(result).get('status'))}")
        except RuntimeError as exc:
            st.error(str(exc))
    if control_cols[2].button("Refresh Market", use_container_width=True):
        try:
            result = _api_post(
                config,
                "/api/brokers/schwab/market-sync",
                {"tickers": [selected_ticker], "include_history": True, "include_options": True},
            )
            st.success(f"Market context rows: {len(_records(_mapping(result).get('items')))}")
        except RuntimeError as exc:
            st.error(str(exc))
    if control_cols[3].button("Disconnect", use_container_width=True):
        try:
            result = _api_post(config, "/api/brokers/schwab/disconnect")
            st.warning(f"Broker status: {_metric_text(_mapping(result).get('status'))}")
        except RuntimeError as exc:
            st.error(str(exc))


def _show_opportunity_action_form(
    *,
    engine: Any,
    selected_ticker: str,
    dashboard_role: Role,
) -> None:
    if not role_allows(dashboard_role, Role.ANALYST):
        st.caption("Analyst role required to save opportunity actions.")
        return
    with st.form("broker_opportunity_action_form"):
        action = st.selectbox(
            "Opportunity action",
            ["watch", "ready", "simulate_entry", "dismiss"],
        )
        thesis = st.text_area("Thesis", height=90)
        notes = st.text_input("Notes")
        submitted = st.form_submit_button("Save Action")
    if submitted:
        try:
            record_opportunity_action(
                repo=BrokerRepository(engine),
                ticker=selected_ticker,
                action=action,
                thesis=thesis,
                notes=notes,
                payload={"source": "dashboard"},
                actor_source="dashboard",
                actor_id="local-dashboard",
                actor_role=dashboard_role.value,
            )
            st.success(f"Saved {action} for {selected_ticker}.")
        except ValueError as exc:
            st.error(str(exc))


def _show_trigger_form(
    *,
    engine: Any,
    selected_ticker: str,
    dashboard_role: Role,
) -> None:
    if not role_allows(dashboard_role, Role.ANALYST):
        st.caption("Analyst role required to manage triggers.")
        return
    left, right = st.columns([1, 1])
    with left.form("broker_trigger_form"):
        trigger_type = st.selectbox(
            "Trigger",
            [
                "price_above",
                "price_below",
                "volume_above",
                "relative_volume_above",
                "call_put_ratio_above",
            ],
        )
        operator = st.selectbox(
            "Operator",
            ["gte", "lte", "gt", "lt"],
            index=1 if trigger_type.endswith("below") else 0,
        )
        threshold = st.number_input("Threshold", min_value=0.0, value=0.0)
        notes = st.text_input("Trigger notes")
        submitted = st.form_submit_button("Add Trigger")
    if submitted:
        try:
            create_trigger(
                repo=BrokerRepository(engine),
                ticker=selected_ticker,
                trigger_type=trigger_type,
                operator=operator,
                threshold=threshold,
                notes=notes,
                payload={"source": "dashboard"},
                actor_source="dashboard",
                actor_id="local-dashboard",
                actor_role=dashboard_role.value,
            )
            st.success(f"Added trigger for {selected_ticker}.")
        except ValueError as exc:
            st.error(str(exc))
    with right:
        if st.button("Evaluate Triggers", use_container_width=True):
            try:
                rows = evaluate_triggers(
                    repo=BrokerRepository(engine),
                    tickers=[selected_ticker],
                    actor_source="dashboard",
                    actor_id="local-dashboard",
                    actor_role=dashboard_role.value,
                )
                fired = [row for row in rows if row.status.value == "fired"]
                st.success(f"Evaluated {len(rows)} trigger(s); fired {len(fired)}.")
            except ValueError as exc:
                st.error(str(exc))


def _show_order_ticket_form(
    *,
    engine: Any,
    config: AppConfig,
    summary: Mapping[str, Any],
    selected_ticker: str,
    dashboard_role: Role,
) -> None:
    if not role_allows(dashboard_role, Role.ANALYST):
        st.caption("Analyst role required to save order previews.")
        return
    latest_price = _broker_latest_price(summary, selected_ticker)
    with st.form("broker_order_ticket_form"):
        side = st.selectbox("Side", ["buy", "sell"])
        entry_price = st.number_input("Entry Price", min_value=0.0, value=latest_price)
        invalidation_price = st.number_input("Invalidation Price", min_value=0.0, value=0.0)
        risk_pct = st.number_input(
            "Risk Per Trade",
            min_value=0.0,
            max_value=1.0,
            value=float(config.risk_per_trade_pct),
            format="%.4f",
        )
        notes = st.text_input("Ticket notes")
        submitted = st.form_submit_button("Preview Ticket")
    if submitted:
        try:
            ticket = create_blocked_order_ticket(
                repo=BrokerRepository(engine),
                ticker=selected_ticker,
                side=side,
                entry_price=entry_price,
                invalidation_price=invalidation_price,
                risk_per_trade_pct=risk_pct,
                notes=notes,
                config=config,
                actor_source="dashboard",
                actor_id="local-dashboard",
                actor_role=dashboard_role.value,
            )
            st.warning(
                f"Ticket saved as blocked preview; submission_allowed={ticket.submission_allowed}."
            )
        except ValueError as exc:
            st.error(str(exc))


def _show_broker_layer(
    summary: Mapping[str, Any],
    *,
    engine: Any,
    config: AppConfig,
    candidate_rows: list[dict[str, object]],
    dashboard_role: Role,
) -> None:
    snapshot = _mapping(summary.get("snapshot"))
    exposure = _mapping(summary.get("exposure"))
    metric_cols = st.columns(5)
    metric_cols[0].metric("Connection", _metric_text(snapshot.get("connection_status")))
    metric_cols[1].metric("Accounts", _metric_text(snapshot.get("account_count")))
    metric_cols[2].metric("Positions", _metric_text(snapshot.get("position_count")))
    metric_cols[3].metric("Open Orders", _metric_text(snapshot.get("open_order_count")))
    metric_cols[4].metric(
        "Stale",
        "yes" if bool(exposure.get("broker_data_stale")) else "no",
    )
    _show_status_badges(
        [
            ("Broker", snapshot.get("broker")),
            ("Connection", snapshot.get("connection_status")),
            ("Read Only", exposure.get("read_only")),
            ("Action Routing", "disabled"),
        ]
    )
    ticker_options = _broker_ticker_options(summary, candidate_rows)
    selected_ticker = (
        st.selectbox("Active Ticker", ticker_options)
        if ticker_options
        else st.text_input("Active Ticker", value="").strip().upper()
    )
    if selected_ticker:
        _show_broker_controls(
            config=config,
            selected_ticker=selected_ticker,
            dashboard_role=dashboard_role,
        )
        action_col, trigger_col, ticket_col = st.columns([1, 1, 1])
        with action_col:
            _show_opportunity_action_form(
                engine=engine,
                selected_ticker=selected_ticker,
                dashboard_role=dashboard_role,
            )
        with trigger_col:
            _show_trigger_form(
                engine=engine,
                selected_ticker=selected_ticker,
                dashboard_role=dashboard_role,
            )
        with ticket_col:
            _show_order_ticket_form(
                engine=engine,
                config=config,
                summary=summary,
                selected_ticker=selected_ticker,
                dashboard_role=dashboard_role,
            )
    left, right = st.columns([1, 1])
    with left:
        _show_mapping(
            "Portfolio Snapshot",
            {
                "last_successful_sync_at": snapshot.get("last_successful_sync_at"),
                "portfolio_equity": exposure.get("portfolio_equity"),
                "cash": exposure.get("cash"),
                "buying_power": exposure.get("buying_power"),
                "snapshot_as_of": exposure.get("snapshot_as_of"),
                "hard_blocks": exposure.get("hard_blocks"),
            },
            empty="No broker snapshot.",
        )
    with right:
        _show_mapping("Exposure", exposure.get("exposure_before"), empty="No exposure rows.")
    _show_records("Positions", summary.get("positions"), empty="No broker positions.")
    _show_records("Balances", summary.get("balances"), empty="No broker balances.")
    _show_records("Open Orders", summary.get("open_orders"), empty="No broker open orders.")
    _show_broker_rate_limits(summary)
    _show_records("Market Context", summary.get("market_context"), empty="No market context.")
    _show_records(
        "Opportunity Actions",
        summary.get("opportunity_actions"),
        empty="No opportunity actions.",
    )
    _show_records("Triggers", summary.get("triggers"), empty="No broker triggers.")
    _show_records(
        "Order Tickets",
        summary.get("order_tickets"),
        empty="No blocked order tickets.",
    )


def _show_ops_layer(health: Mapping[str, Any]) -> None:
    if not health:
        st.info("No ops health rows.")
        return
    provider_banners = _records(health.get("provider_banners"))
    for banner in provider_banners:
        provider = banner.get("provider") or "unknown provider"
        status = banner.get("status") or "unknown"
        reason = banner.get("reason") or "no reason reported"
        st.warning(f"{provider}: {status} - {reason}")

    stale_data = health.get("stale_data")
    stale_detected = (
        bool(stale_data.get("detected")) if isinstance(stale_data, Mapping) else bool(stale_data)
    )
    degraded_mode = _mapping(health.get("degraded_mode"))
    ops_metrics = _mapping(health.get("metrics"))
    cost_metrics = _mapping(ops_metrics.get("cost"))

    metric_cols = st.columns(5)
    metric_cols[0].metric(
        "Degraded Mode",
        "enabled" if bool(degraded_mode.get("enabled")) else "off",
    )
    metric_cols[1].metric("Stale Data", "yes" if stale_detected else "no")
    metric_cols[2].metric("LLM Actual Cost", _currency(cost_metrics.get("total_actual_cost_usd")))
    metric_cols[3].metric(
        "Unsupported Claims",
        _rate(ops_metrics.get("unsupported_claim_rate")),
    )
    metric_cols[4].metric("False Positives", _rate(ops_metrics.get("false_positive_rate")))

    database = _mapping(health.get("database"))
    left, right = st.columns([1, 1])
    with left:
        _show_mapping("Database", database, empty="No database status.")
    with right:
        _show_mapping("Degraded Mode", degraded_mode, empty="No degraded-mode status.")
    _show_records("Provider Health", health.get("providers"), empty="No provider health rows.")
    _show_records("Recent Incidents", health.get("incidents"), empty="No incidents.")
    _show_records(
        "Telemetry Events",
        _mapping(health.get("telemetry")).get("events"),
        empty="No telemetry events.",
    )
    _show_records("Job Rows", health.get("jobs"), empty="No job rows.")


load_app_dotenv()
dashboard_role = require_viewer()

st.set_page_config(page_title="Market Radar Command Center", layout="wide")
st.markdown(dashboard_style(), unsafe_allow_html=True)

config = AppConfig.from_env()
engine = engine_from_url(config.database_url)
create_schema(engine)

radar_run_summary = _mapping(dashboard_data.load_radar_run_summary(engine))
radar_run_cooldown = _mapping(
    dashboard_data.radar_run_cooldown_payload(engine, config)
)
latest_run_cutoff = _radar_summary_cutoff(radar_run_summary)
default_candidate_rows = dashboard_data.load_candidate_rows(
    engine,
    available_at=latest_run_cutoff,
)
default_ticker = _default_ticker(
    default_candidate_rows,
    dashboard_data.load_ipo_s1_rows(engine, available_at=latest_run_cutoff),
)
if default_ticker and "ticker_filter" not in st.session_state:
    st.session_state["ticker_filter"] = default_ticker

st.sidebar.header("Review Controls")
ticker_filter = (
    st.sidebar.text_input("Ticker", key="ticker_filter", placeholder=default_ticker or "MSFT")
    .strip()
    .upper()
)
cutoff_text = st.sidebar.text_input("Available at", value="", placeholder="2026-05-10T21:05:00Z")
try:
    available_at = _parse_cutoff(cutoff_text)
except ValueError as exc:
    st.sidebar.warning(str(exc))
    available_at = None
data_available_at = available_at or latest_run_cutoff
if latest_run_cutoff is not None and available_at is None:
    st.sidebar.caption(
        f"Using latest radar run cutoff: {latest_run_cutoff.isoformat()}"
    )
alert_status = _select_value("Alert status", ALERT_STATUSES)
alert_route = _select_value("Alert route", ALERT_ROUTES)

candidate_rows = (
    dashboard_data.load_radar_run_candidate_rows(engine, radar_run_summary)
    if available_at is None and radar_run_summary
    else dashboard_data.load_candidate_rows(engine, available_at=data_available_at)
)
theme_rows = dashboard_data.load_theme_rows(engine, available_at=data_available_at)
alert_rows = dashboard_data.load_alert_rows(
    engine,
    ticker=ticker_filter or None,
    status=alert_status,
    route=alert_route,
    available_at=data_available_at,
)
ipo_rows = dashboard_data.load_ipo_s1_rows(
    engine,
    ticker=ticker_filter or None,
    available_at=data_available_at,
)
validation_summary = _mapping(dashboard_data.load_validation_summary(engine))
cost_summary = _mapping(dashboard_data.load_cost_summary(engine, available_at=data_available_at))
ops_health = _mapping(dashboard_data.load_ops_health(engine))
broker_summary = _mapping(dashboard_data.load_broker_summary(engine))
discovery_snapshot = _mapping(
    dashboard_data.radar_discovery_snapshot_payload(
        engine,
        config,
        radar_run_summary=radar_run_summary,
    )
)

_show_command_header(
    candidate_rows=candidate_rows,
    alert_rows=alert_rows,
    ipo_rows=ipo_rows,
    ops_health=ops_health,
)

tabs = st.tabs(
    [
        "Overview",
        "Ticker",
        "IPO/S-1",
        "Alerts",
        "Themes",
        "Validation",
        "Costs",
        "Broker",
        "Ops",
    ]
)

with tabs[0]:
    _show_overview(
        engine=engine,
        config=config,
        radar_run_summary=radar_run_summary,
        radar_run_cooldown=radar_run_cooldown,
        discovery_snapshot=discovery_snapshot,
        candidate_rows=candidate_rows,
        alert_rows=alert_rows,
        ipo_rows=ipo_rows,
        theme_rows=theme_rows,
        validation_summary=validation_summary,
        cost_summary=cost_summary,
        ops_health=ops_health,
        broker_summary=broker_summary,
        dashboard_role=dashboard_role,
    )

with tabs[1]:
    _show_ticker_layer(engine, ticker_filter, data_available_at)

with tabs[2]:
    _show_ipo_layer(ipo_rows)

with tabs[3]:
    _show_alerts_layer(alert_rows, engine=engine, cutoff=data_available_at)

with tabs[4]:
    _show_themes_layer(theme_rows)

with tabs[5]:
    _show_validation_layer(validation_summary)

with tabs[6]:
    _show_costs_layer(cost_summary)

with tabs[7]:
    _show_broker_layer(
        broker_summary,
        engine=engine,
        config=config,
        candidate_rows=candidate_rows,
        dashboard_role=dashboard_role,
    )

with tabs[8]:
    _show_ops_layer(ops_health)

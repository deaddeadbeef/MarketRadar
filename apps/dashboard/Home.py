from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from html import escape
from math import isfinite
from typing import Any

import pandas as pd
import streamlit as st

from apps.dashboard.access import require_viewer
from catalyst_radar.core.config import AppConfig
from catalyst_radar.dashboard import data as dashboard_data
from catalyst_radar.dashboard.design import dashboard_style
from catalyst_radar.security.secrets import load_app_dotenv
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
                f'<a class="mr-table-link" href="{_html(url)}" target="_blank">'
                f"{_html(label)}</a>"
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
    return (
        '<div class="mr-command-cell">'
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
        f'<div class="mr-command-strip">{"".join(cells)}</div>',
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
            f'<span>{_html(state)}</span>'
            f'<strong>{count}</strong>'
            "</div>"
            '<div class="mr-chart-track">'
            f'<span class="mr-chart-bar" style="width: {pct * 100:.1f}%"></span>'
            "</div>"
            f'<span class="mr-chart-caption">{pct:.0%} of candidates</span>'
            "</div>"
        )
    st.markdown(f'<div class="mr-chart-card">{"".join(bars)}</div>', unsafe_allow_html=True)


def _candidate_rows_with_labels(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    labeled: list[dict[str, object]] = []
    for row in rows:
        values = dict(row)
        values["supporting_evidence"] = _evidence_label(values.get("top_supporting_evidence"))
        values["disconfirming_evidence"] = _evidence_label(
            values.get("top_disconfirming_evidence")
        )
        labeled.append(values)
    return labeled


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
    candidate_rows: list[dict[str, object]],
    alert_rows: list[dict[str, object]],
    ipo_rows: list[dict[str, object]],
    theme_rows: list[dict[str, object]],
    validation_summary: Mapping[str, Any],
    cost_summary: Mapping[str, Any],
    ops_health: Mapping[str, Any],
) -> None:
    candidate_frame = pd.DataFrame(candidate_rows)
    alert_frame = pd.DataFrame(alert_rows)
    validation_report = _mapping(validation_summary.get("report"))
    database = _mapping(ops_health.get("database"))

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
            _candidate_rows_with_labels(candidate_rows),
            columns=[
                "ticker",
                "state",
                "final_score",
                "setup_type",
                "top_event_type",
                "supporting_evidence",
                "decision_card_id",
                "next_review_at",
            ],
            labels={
                "ticker": "Ticker",
                "state": "State",
                "final_score": "Score",
                "setup_type": "Setup",
                "top_event_type": "Top Event",
                "supporting_evidence": "Evidence",
                "decision_card_id": "Card",
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
                ("Priority Score", selected_candidate.get("final_score")),
                ("Setup", selected_candidate.get("setup_type")),
            ]
        )
        _show_mapping(
            "Selected Candidate",
            {
                "ticker": selected_candidate.get("ticker"),
                "state": selected_candidate.get("state"),
                "score": selected_candidate.get("final_score"),
                "top_event": selected_candidate.get("top_event_title"),
                "supporting_evidence": selected_candidate.get("supporting_evidence"),
                "disconfirming_evidence": selected_candidate.get("disconfirming_evidence"),
                "decision_card_id": selected_candidate.get("decision_card_id"),
                "next_review_at": selected_candidate.get("next_review_at"),
            },
            empty="No selected candidate.",
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
        f'<p>{_value_html(value)}</p>'
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


def _show_broker_layer(summary: Mapping[str, Any]) -> None:
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
    _show_records("Job Rows", health.get("jobs"), empty="No job rows.")


load_app_dotenv()
require_viewer()

st.set_page_config(page_title="Market Radar Command Center", layout="wide")
st.markdown(dashboard_style(), unsafe_allow_html=True)

st.title("Market Radar Command Center")

config = AppConfig.from_env()
engine = engine_from_url(config.database_url)
create_schema(engine)

candidate_rows = dashboard_data.load_candidate_rows(engine)
default_ticker = _default_ticker(
    candidate_rows,
    dashboard_data.load_ipo_s1_rows(engine),
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
alert_status = _select_value("Alert status", ALERT_STATUSES)
alert_route = _select_value("Alert route", ALERT_ROUTES)

theme_rows = dashboard_data.load_theme_rows(engine)
alert_rows = dashboard_data.load_alert_rows(
    engine,
    ticker=ticker_filter or None,
    status=alert_status,
    route=alert_route,
    available_at=available_at,
)
ipo_rows = dashboard_data.load_ipo_s1_rows(
    engine,
    ticker=ticker_filter or None,
    available_at=available_at,
)
validation_summary = _mapping(dashboard_data.load_validation_summary(engine))
cost_summary = _mapping(dashboard_data.load_cost_summary(engine, available_at=available_at))
ops_health = _mapping(dashboard_data.load_ops_health(engine))
broker_summary = _mapping(dashboard_data.load_broker_summary(engine))

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
        candidate_rows=candidate_rows,
        alert_rows=alert_rows,
        ipo_rows=ipo_rows,
        theme_rows=theme_rows,
        validation_summary=validation_summary,
        cost_summary=cost_summary,
        ops_health=ops_health,
    )

with tabs[1]:
    _show_ticker_layer(engine, ticker_filter, available_at)

with tabs[2]:
    _show_ipo_layer(ipo_rows)

with tabs[3]:
    _show_alerts_layer(alert_rows, engine=engine, cutoff=available_at)

with tabs[4]:
    _show_themes_layer(theme_rows)

with tabs[5]:
    _show_validation_layer(validation_summary)

with tabs[6]:
    _show_costs_layer(cost_summary)

with tabs[7]:
    _show_broker_layer(broker_summary)

with tabs[8]:
    _show_ops_layer(ops_health)

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime
from typing import Any

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from apps.dashboard.access import require_viewer
from catalyst_radar.core.config import AppConfig
from catalyst_radar.dashboard import data as dashboard_data
from catalyst_radar.storage.db import engine_from_url

USEFUL_FEEDBACK_LABELS = frozenset({"useful", "acted"})


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _json_ready(value: object) -> object:
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_json_ready(item) for item in value]
    return value


def _records(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list | tuple):
        return []
    return [
        {str(key): _json_ready(row_value) for key, row_value in item.items()}
        for item in value
        if isinstance(item, Mapping)
    ]


def _select_value(label: str, options: list[str]) -> str | None:
    value = st.sidebar.selectbox(label, ["All", *options])
    return None if value == "All" else value


def _load_rows(
    *,
    ticker: str | None,
    status: str | None,
    route: str | None,
) -> tuple[list[dict[str, object]], str | None]:
    loader = getattr(dashboard_data, "load_alert_rows", None)
    if loader is None:
        return [], "Alert row helper is not available in shared dashboard data."
    config = AppConfig.from_env()
    rows = loader(
        engine_from_url(config.database_url),
        ticker=ticker,
        status=status,
        route=route,
    )
    return _records(rows), None


def _load_detail(alert_id: str) -> tuple[Mapping[str, Any] | None, str | None]:
    loader = getattr(dashboard_data, "load_alert_detail", None)
    if loader is None:
        return None, "Alert detail helper is not available in shared dashboard data."
    config = AppConfig.from_env()
    detail = loader(engine_from_url(config.database_url), alert_id)
    if detail is None:
        return None, None
    return _mapping(detail), None


def _display_value(value: object) -> str:
    if value is None or value == "":
        return "n/a"
    if isinstance(value, datetime | date):
        return value.isoformat()
    return str(value)


load_dotenv(".env.local")
require_viewer()

st.set_page_config(page_title="Alerts", layout="wide")
st.title("Alerts")
st.caption("Candidate alert review, evidence context, and feedback history.")

ticker_filter = st.sidebar.text_input("Ticker", value="", placeholder="MSFT").strip().upper()
status_filter = _select_value("Status", ["planned", "dry_run", "sent", "failed"])
route_filter = _select_value(
    "Route",
    ["immediate_manual_review", "warning_digest", "daily_digest", "position_watch"],
)

rows, error = _load_rows(
    ticker=ticker_filter or None,
    status=status_filter,
    route=route_filter,
)
if error is not None:
    st.warning(error)

frame = pd.DataFrame(rows)
total_count = len(rows)
planned_count = int((frame.get("status") == "planned").sum()) if not frame.empty else 0
dry_run_count = int((frame.get("status") == "dry_run").sum()) if not frame.empty else 0
priority_count = (
    int(frame.get("priority", pd.Series(dtype=object)).isin(["high", "critical"]).sum())
    if not frame.empty
    else 0
)
useful_count = (
    int(frame.get("feedback_label", pd.Series(dtype=object)).isin(USEFUL_FEEDBACK_LABELS).sum())
    if not frame.empty
    else 0
)

metrics = st.columns(5)
metrics[0].metric("Total", total_count)
metrics[1].metric("Planned", planned_count)
metrics[2].metric("Dry Run", dry_run_count)
metrics[3].metric("High/Critical", priority_count)
metrics[4].metric("Useful Feedback", useful_count)

display_columns = [
    "ticker",
    "route",
    "channel",
    "priority",
    "status",
    "state",
    "score_trigger",
    "dedupe_key",
    "title",
    "available_at",
    "feedback",
]
if frame.empty:
    st.info("No alert rows match the current filters.")
else:
    for column in display_columns:
        if column not in frame.columns:
            frame[column] = None
    st.subheader("Alert Queue")
    st.dataframe(
        frame[display_columns].rename(
            columns={
                "ticker": "Ticker",
                "route": "Route",
                "channel": "Channel",
                "priority": "Priority",
                "status": "Status",
                "state": "State",
                "score_trigger": "Score Trigger",
                "dedupe_key": "Dedupe Key",
                "title": "Title",
                "available_at": "Available Time",
                "feedback": "Feedback",
            }
        ),
        width="stretch",
        hide_index=True,
    )

st.subheader("Alert Detail")
selected_alert_id = st.text_input("Alert ID", value="", placeholder="alert id").strip()
if selected_alert_id:
    detail, detail_error = _load_detail(selected_alert_id)
    if detail_error is not None:
        st.warning(detail_error)
    elif detail is None:
        st.warning("Alert ID was not found.")
    else:
        metric_columns = st.columns(5)
        metric_columns[0].metric("Ticker", _display_value(detail.get("ticker")))
        metric_columns[1].metric("Route", _display_value(detail.get("route")))
        metric_columns[2].metric("Priority", _display_value(detail.get("priority")))
        metric_columns[3].metric("Status", _display_value(detail.get("status")))
        metric_columns[4].metric("Feedback", _display_value(detail.get("feedback_label")))

        left, right = st.columns([1, 1])
        with left:
            st.markdown("**Review Context**")
            st.json(
                _json_ready(
                    {
                        "id": detail.get("id"),
                        "candidate_state_id": detail.get("candidate_state_id"),
                        "candidate_packet_id": detail.get("candidate_packet_id"),
                        "decision_card_id": detail.get("decision_card_id"),
                        "trigger_kind": detail.get("trigger_kind"),
                        "trigger_fingerprint": detail.get("trigger_fingerprint"),
                        "summary": detail.get("summary"),
                    }
                )
            )
        with right:
            st.markdown("**Feedback Reference**")
            st.json(
                _json_ready(
                    {
                        "feedback_url": detail.get("feedback_url"),
                        "feedback_id": detail.get("feedback_id"),
                        "feedback_label": detail.get("feedback_label"),
                        "feedback_notes": detail.get("feedback_notes"),
                        "feedback_created_at": detail.get("feedback_created_at"),
                    }
                )
            )

        st.markdown("**Evidence Payload**")
        st.json(_json_ready(detail.get("payload")))
else:
    st.caption("Enter an alert ID to inspect candidate evidence and feedback reference.")

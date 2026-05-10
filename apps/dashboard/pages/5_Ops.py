from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime
from math import isfinite
from typing import Any

import pandas as pd
import streamlit as st

from apps.dashboard.access import require_viewer
from catalyst_radar.core.config import AppConfig
from catalyst_radar.dashboard import data as dashboard_data
from catalyst_radar.security.secrets import load_app_dotenv
from catalyst_radar.storage.db import engine_from_url


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


def _show_records(title: str, value: object, *, empty: str) -> None:
    st.subheader(title)
    records = _records(value)
    if records:
        st.dataframe(pd.DataFrame(records), width="stretch", hide_index=True)
    else:
        st.caption(empty)


def _float_or_none(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if isfinite(number) else None


def _format_currency(value: object) -> str:
    number = _float_or_none(value)
    return "n/a" if number is None else f"${number:,.2f}"


def _format_count(value: object) -> str:
    number = _float_or_none(value)
    return "n/a" if number is None else f"{int(number):,}"


def _format_rate(value: object) -> str:
    number = _float_or_none(value)
    if number is None:
        return "n/a"
    percentage = number * 100 if -1 <= number <= 1 else number
    return f"{percentage:.1f}%"


def _format_score(value: object) -> str:
    number = _float_or_none(value)
    return "n/a" if number is None else f"{number:.1f}"


def _list_items(value: object) -> list[str]:
    if not isinstance(value, list | tuple):
        return []
    return [str(item) for item in value if str(item).strip()]


def _list_text(value: object) -> str:
    items = _list_items(value)
    return ", ".join(items) if items else "None"


def _load_health() -> tuple[Mapping[str, Any], str | None]:
    loader = getattr(dashboard_data, "load_ops_health", None)
    if loader is None:
        return {}, "Ops helper is not available in shared dashboard data."
    config = AppConfig.from_env()
    return _mapping(loader(engine_from_url(config.database_url))), None


load_app_dotenv()
require_viewer()

st.set_page_config(page_title="Ops", layout="wide")
st.title("Ops")
st.caption("Provider, database, and freshness status for radar review readiness.")

health, error = _load_health()
if error is not None:
    st.warning(error)
elif not health:
    st.info("No ops health rows found.")
else:
    provider_banners = _records(health.get("provider_banners"))
    if provider_banners:
        st.subheader("Provider Banners")
        for banner in provider_banners:
            provider = banner.get("provider") or "unknown provider"
            status = banner.get("status") or "unknown"
            reason = banner.get("reason") or "no reason reported"
            runbook = banner.get("runbook")
            message = f"{provider}: {status} - {reason}"
            if runbook:
                message = f"{message} | Runbook: {runbook}"
            st.warning(message)

    stale_data = health.get("stale_data")
    stale_detected = (
        bool(stale_data.get("detected")) if isinstance(stale_data, Mapping) else bool(stale_data)
    )
    if stale_detected:
        st.warning("Stale data detected; review provider and database timestamps.")

    degraded_mode = _mapping(health.get("degraded_mode"))
    disabled_states = degraded_mode.get("disabled_states")
    degraded_cols = st.columns(3)
    degraded_cols[0].metric(
        "Degraded Mode",
        "Enabled" if bool(degraded_mode.get("enabled")) else "Off",
    )
    degraded_cols[1].metric(
        "Max Action State",
        str(degraded_mode.get("max_action_state") or "n/a"),
    )
    degraded_cols[2].metric(
        "Disabled States",
        _format_count(len(_list_items(disabled_states))),
    )
    if bool(degraded_mode.get("enabled")):
        st.warning(f"Disabled states: {_list_text(disabled_states)}")
    degraded_reasons = degraded_mode.get("reasons")
    if degraded_reasons:
        st.caption(f"Degraded reasons: {_list_text(degraded_reasons)}")

    ops_metrics = _mapping(health.get("metrics"))
    cost_metrics = _mapping(ops_metrics.get("cost"))
    metric_cols = st.columns(5)
    metric_cols[0].metric(
        "Total LLM Actual Cost",
        _format_currency(cost_metrics.get("total_actual_cost_usd")),
    )
    metric_cols[1].metric(
        "Cost / Useful Alert",
        _format_currency(cost_metrics.get("cost_per_useful_alert")),
    )
    metric_cols[2].metric(
        "Stale Incident Count",
        _format_count(ops_metrics.get("stale_incident_count")),
    )
    metric_cols[3].metric(
        "Unsupported-Claim Rate",
        _format_rate(ops_metrics.get("unsupported_claim_rate")),
    )
    metric_cols[4].metric(
        "False-Positive Rate",
        _format_rate(ops_metrics.get("false_positive_rate")),
    )

    score_drift = _mapping(health.get("score_drift"))
    latest_drift = _mapping(score_drift.get("latest"))
    previous_drift = _mapping(score_drift.get("previous"))
    drift_cols = st.columns(4)
    drift_cols[0].metric(
        "Score Drift",
        "Detected" if bool(score_drift.get("detected")) else "Normal",
    )
    drift_cols[1].metric(
        "Latest Avg Score",
        _format_score(latest_drift.get("mean_score")),
    )
    drift_cols[2].metric(
        "Previous Avg Score",
        _format_score(previous_drift.get("mean_score")),
    )
    drift_cols[3].metric("Mean Shift", _format_score(score_drift.get("mean_shift")))
    if bool(score_drift.get("detected")):
        st.warning(f"Score drift reason: {score_drift.get('reason') or 'unspecified'}")
    latest_as_of = latest_drift.get("as_of")
    previous_as_of = previous_drift.get("as_of")
    if latest_as_of or previous_as_of:
        st.caption(
            f"Score windows: latest {latest_as_of or 'n/a'}; "
            f"previous {previous_as_of or 'n/a'}"
        )

    _show_records(
        "Recent Data-Quality Incidents",
        health.get("incidents"),
        empty="No data-quality incidents available.",
    )

    database = _mapping(health.get("database"))
    database_status = str(database.get("status") or "unknown")
    database_checked = database.get("checked_at") or database.get("last_checked_at") or "n/a"

    metrics = st.columns(3)
    metrics[0].metric("Database", database_status)
    metrics[1].metric("Database Checked", str(_json_ready(database_checked)))
    metrics[2].metric("Providers", len(_records(health.get("providers"))))

    if database:
        st.subheader("Database")
        st.json(_json_ready(database))

    _show_records(
        "Provider Health",
        health.get("providers"),
        empty="No provider health rows available.",
    )

    if stale_detected:
        st.subheader("Stale Data")
        if isinstance(stale_data, Mapping):
            st.json(_json_ready(stale_data))
        else:
            _show_records("Stale Data Rows", stale_data, empty="No stale data rows available.")

    job_rows = _records(health.get("jobs"))
    if job_rows:
        st.subheader("Job Rows")
        st.dataframe(pd.DataFrame(job_rows), width="stretch", hide_index=True)

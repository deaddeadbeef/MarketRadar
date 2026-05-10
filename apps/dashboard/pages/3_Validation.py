from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime
from typing import Any

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from catalyst_radar.core.config import AppConfig
from catalyst_radar.dashboard import data as dashboard_data
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
    records: list[dict[str, object]] = []
    for item in value:
        if isinstance(item, Mapping):
            records.append({str(key): _json_ready(row_value) for key, row_value in item.items()})
    return records


def _show_records(title: str, value: object, *, empty: str) -> None:
    st.subheader(title)
    records = _records(value)
    if records:
        st.dataframe(pd.DataFrame(records), width="stretch", hide_index=True)
    else:
        st.caption(empty)


def _rate(value: object) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "0.0%"
    return f"{number:.1%}"


def _number(value: object) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _load_summary() -> tuple[Mapping[str, Any], str | None]:
    loader = getattr(dashboard_data, "load_validation_summary", None)
    if loader is None:
        return {}, "Validation helper is not available in shared dashboard data."
    config = AppConfig.from_env()
    return _mapping(loader(engine_from_url(config.database_url))), None


load_dotenv(".env.local")

st.set_page_config(page_title="Validation", layout="wide")
st.title("Validation")
st.caption("Replay, label, and simulated-paper outcomes for review quality control.")

summary, error = _load_summary()
if error is not None:
    st.warning(error)
elif not summary:
    st.info("No validation summary found.")
else:
    latest_run = _mapping(summary.get("latest_run"))
    report = _mapping(summary.get("report"))
    precision = _mapping(report.get("precision"))
    primary_precision = precision.get("target_20d_25")
    if primary_precision is None and precision:
        primary_precision = next(iter(precision.values()))

    metrics = st.columns(6)
    metrics[0].metric("Run", str(latest_run.get("id") or report.get("run_id") or "n/a"))
    metrics[1].metric("Precision", _rate(primary_precision))
    metrics[2].metric("Useful Rate", _rate(report.get("useful_alert_rate")))
    metrics[3].metric("False Positives", _number(report.get("false_positive_count")))
    metrics[4].metric("Missed", _number(report.get("missed_opportunity_count")))
    metrics[5].metric("Leakage Flags", _number(report.get("leakage_failure_count")))

    left, right = st.columns([1, 1])
    with left:
        st.subheader("Latest Run")
        st.json(_json_ready(latest_run))
    with right:
        st.subheader("Report")
        st.json(_json_ready(report))

    _show_records(
        "Paper Trades",
        summary.get("paper_trades"),
        empty="No simulated-paper rows available.",
    )
    _show_records(
        "Useful Labels",
        summary.get("useful_labels"),
        empty="No useful-label rows available.",
    )

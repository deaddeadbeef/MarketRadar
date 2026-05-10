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
    return [
        {str(key): _json_ready(row_value) for key, row_value in item.items()}
        for item in value
        if isinstance(item, Mapping)
    ]


def _number(value: object, *, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _load_summary() -> tuple[Mapping[str, Any], str | None]:
    loader = getattr(dashboard_data, "load_cost_summary", None)
    if loader is None:
        return {}, "Cost helper is not available in shared dashboard data."
    config = AppConfig.from_env()
    return _mapping(loader(engine_from_url(config.database_url))), None


load_dotenv(".env.local")

st.set_page_config(page_title="Costs", layout="wide")
st.title("Costs")
st.caption("LLM budget-ledger summary from persisted rows only.")

summary, error = _load_summary()
if error is not None:
    st.warning(error)

actual_cost = _number(summary.get("total_actual_cost_usd"))
estimated_cost = _number(summary.get("total_estimated_cost_usd"))
attempt_count = int(_number(summary.get("attempt_count")))
status_counts = _mapping(summary.get("status_counts"))
skipped_count = int(_number(status_counts.get("skipped")))
completed_count = int(_number(status_counts.get("completed")))
useful_count = int(_number(summary.get("useful_alert_count", summary.get("useful_count", 0))))
cost_per_useful = summary.get("cost_per_useful_alert")
if cost_per_useful is None:
    if actual_cost <= 0:
        cost_per_useful = 0.0
    elif useful_count:
        cost_per_useful = actual_cost / useful_count

metrics = st.columns(4)
metrics[0].metric("Actual LLM Cost", f"${actual_cost:.2f}")
metrics[1].metric("Estimated LLM Cost", f"${estimated_cost:.2f}")
metrics[2].metric("Attempts", attempt_count)
metrics[3].metric("Skipped", skipped_count)

secondary_metrics = st.columns(3)
secondary_metrics[0].metric("Completed", completed_count)
secondary_metrics[1].metric("Useful Alerts", useful_count)
secondary_metrics[2].metric(
    "Cost Per Useful Alert",
    "n/a" if cost_per_useful is None else f"${_number(cost_per_useful):.2f}",
)

st.caption("Missing spend rows remain zero; no paid model spend is inferred.")
st.caption(f"Useful alert feedback counted in the current validation context: {useful_count}.")

detail_rows = _records(summary.get("rows"))
if detail_rows:
    st.subheader("Ledger Rows")
    st.dataframe(pd.DataFrame(detail_rows), width="stretch", hide_index=True)

by_task_rows = _records(summary.get("by_task"))
if by_task_rows:
    st.subheader("Spend By Task")
    st.dataframe(pd.DataFrame(by_task_rows), width="stretch", hide_index=True)

by_model_rows = _records(summary.get("by_model"))
if by_model_rows:
    st.subheader("Spend By Model")
    st.dataframe(pd.DataFrame(by_model_rows), width="stretch", hide_index=True)

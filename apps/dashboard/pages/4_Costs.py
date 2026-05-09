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
st.caption("Deterministic review-cost summary from persisted rows only.")

summary, error = _load_summary()
if error is not None:
    st.warning(error)

total_cost = _number(summary.get("total_cost_usd", summary.get("total_cost", 0.0)))
useful_count = int(_number(summary.get("useful_alert_count", summary.get("useful_count", 0))))
cost_per_useful = summary.get("cost_per_useful_alert")
if cost_per_useful is None:
    if total_cost <= 0:
        cost_per_useful = 0.0
    elif useful_count:
        cost_per_useful = total_cost / useful_count

metrics = st.columns(3)
metrics[0].metric("Total Cost", f"${total_cost:.2f}")
metrics[1].metric("Useful Alerts", useful_count)
metrics[2].metric(
    "Cost Per Useful Alert",
    "n/a" if cost_per_useful is None else f"${_number(cost_per_useful):.2f}",
)

st.caption("Missing spend rows remain zero; no paid model spend is inferred.")

detail_rows = _records(
    summary.get("rows")
    or summary.get("provider_costs")
    or summary.get("cost_rows")
    or summary.get("items")
)
if detail_rows:
    st.subheader("Cost Rows")
    st.dataframe(pd.DataFrame(detail_rows), use_container_width=True, hide_index=True)

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


def _show_records(title: str, value: object, *, empty: str) -> None:
    st.subheader(title)
    records = _records(value)
    if records:
        st.dataframe(pd.DataFrame(records), use_container_width=True, hide_index=True)
    else:
        st.caption(empty)


def _load_health() -> tuple[Mapping[str, Any], str | None]:
    loader = getattr(dashboard_data, "load_ops_health", None)
    if loader is None:
        return {}, "Ops helper is not available in shared dashboard data."
    config = AppConfig.from_env()
    return _mapping(loader(engine_from_url(config.database_url))), None


load_dotenv(".env.local")

st.set_page_config(page_title="Ops", layout="wide")
st.title("Ops")
st.caption("Provider, database, and freshness status for radar review readiness.")

health, error = _load_health()
if error is not None:
    st.warning(error)
elif not health:
    st.info("No ops health rows found.")
else:
    stale_data = health.get("stale_data")
    stale_detected = (
        bool(stale_data.get("detected")) if isinstance(stale_data, Mapping) else bool(stale_data)
    )
    if stale_detected:
        st.warning("Stale data detected; review provider and database timestamps.")

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
        st.dataframe(pd.DataFrame(job_rows), use_container_width=True, hide_index=True)

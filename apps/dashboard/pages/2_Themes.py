from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from catalyst_radar.core.config import AppConfig
from catalyst_radar.dashboard import data as dashboard_data
from catalyst_radar.storage.db import engine_from_url


def _json_ready(value: object) -> object:
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return ", ".join(f"{key}: {item}" for key, item in value.items())
    if isinstance(value, list | tuple):
        return ", ".join(str(item) for item in value)
    return value


def _load_rows() -> tuple[list[dict[str, object]], str | None]:
    loader = getattr(dashboard_data, "load_theme_rows", None)
    if loader is None:
        return [], "Theme helper is not available in shared dashboard data."
    config = AppConfig.from_env()
    rows = loader(engine_from_url(config.database_url))
    return [
        {str(key): _json_ready(value) for key, value in row.items()}
        for row in rows
        if isinstance(row, Mapping)
    ], None


load_dotenv(".env.local")

st.set_page_config(page_title="Themes", layout="wide")
st.title("Themes")
st.caption("Candidate theme aggregation for review prioritization.")

rows, error = _load_rows()
if error is not None:
    st.warning(error)
elif not rows:
    st.info("No theme summary rows found.")
else:
    frame = pd.DataFrame(rows)
    for column in (
        "theme",
        "candidate_count",
        "avg_score",
        "top_tickers",
        "states",
        "latest_as_of",
    ):
        if column not in frame.columns:
            frame[column] = None

    metrics = st.columns(4)
    metrics[0].metric("Themes", len(frame))
    metrics[1].metric("Candidates", int(pd.to_numeric(frame["candidate_count"]).fillna(0).sum()))
    metrics[2].metric("Average Score", f"{pd.to_numeric(frame['avg_score']).fillna(0).mean():.2f}")
    metrics[3].metric("Latest As Of", str(frame["latest_as_of"].dropna().max() or "n/a"))

    st.dataframe(
        frame[
            [
                "theme",
                "candidate_count",
                "avg_score",
                "top_tickers",
                "states",
                "latest_as_of",
            ]
        ].rename(
            columns={
                "theme": "Theme",
                "candidate_count": "Candidate Count",
                "avg_score": "Average Score",
                "top_tickers": "Top Tickers",
                "states": "State Mix",
                "latest_as_of": "Latest As Of",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )

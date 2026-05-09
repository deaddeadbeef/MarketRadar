from __future__ import annotations

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from catalyst_radar.core.config import AppConfig
from catalyst_radar.dashboard.data import load_candidate_rows
from catalyst_radar.storage.db import engine_from_url


def _evidence_label(value: object) -> str:
    if not isinstance(value, dict):
        return ""
    title = str(value.get("title") or value.get("kind") or "")
    link = (
        value.get("source_url")
        or value.get("source_id")
        or value.get("computed_feature_id")
        or ""
    )
    if not link:
        return title
    return f"{title} [{link}]"


load_dotenv(".env.local")

st.set_page_config(page_title="Catalyst Radar", layout="wide")
st.title("Catalyst Radar")
st.caption("Deterministic Phase 1 radar. No LLM calls are required for this view.")

config = AppConfig.from_env()
engine = engine_from_url(config.database_url)
rows = load_candidate_rows(engine)

if not rows:
    st.info("No candidate states found. Run ingest and scan first.")
    st.code(
        "catalyst-radar ingest-csv --securities data/sample/securities.csv "
        "--daily-bars data/sample/daily_bars.csv --holdings data/sample/holdings.csv\n"
        "catalyst-radar scan --as-of 2026-05-08",
        language="powershell",
    )
else:
    frame = pd.DataFrame(rows)
    frame["supporting_evidence"] = frame["top_supporting_evidence"].map(_evidence_label)
    frame["disconfirming_evidence"] = frame["top_disconfirming_evidence"].map(
        _evidence_label
    )
    left, right = st.columns([2, 1])
    with left:
        st.subheader("Candidates")
        st.dataframe(
            frame[
                [
                    "ticker",
                    "state",
                    "final_score",
                    "hard_blocks",
                    "supporting_evidence",
                    "disconfirming_evidence",
                    "candidate_packet_id",
                    "decision_card_id",
                    "as_of",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )
    with right:
        st.subheader("State Mix")
        st.bar_chart(frame["state"].value_counts())

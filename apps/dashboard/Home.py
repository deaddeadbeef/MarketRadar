from __future__ import annotations

import pandas as pd
import streamlit as st

from apps.dashboard.access import require_viewer
from catalyst_radar.core.config import AppConfig
from catalyst_radar.dashboard.data import load_alert_rows, load_candidate_rows
from catalyst_radar.security.secrets import load_app_dotenv
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


load_app_dotenv()
require_viewer()

st.set_page_config(page_title="Catalyst Radar", layout="wide")
st.title("Catalyst Radar")
st.caption(
    "Deterministic decision-support review for current candidates, evidence, "
    "packets, cards, setup context, and scheduled next review."
)

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
    display_columns = [
        "ticker",
        "state",
        "final_score",
        "hard_blocks",
        "supporting_evidence",
        "disconfirming_evidence",
        "candidate_packet_id",
        "decision_card_id",
        "setup_type",
        "next_review_at",
        "as_of",
    ]
    for column in display_columns:
        if column not in frame.columns:
            frame[column] = None

    display_frame = frame[display_columns].rename(
        columns={
            "ticker": "Ticker",
            "state": "State",
            "final_score": "Score",
            "hard_blocks": "Hard Blocks",
            "supporting_evidence": "Supporting Evidence",
            "disconfirming_evidence": "Disconfirming Evidence",
            "candidate_packet_id": "Packet ID",
            "decision_card_id": "Card ID",
            "setup_type": "Setup Type",
            "next_review_at": "Next Review",
            "as_of": "As Of",
        }
    )
    left, right = st.columns([2, 1])
    with left:
        st.subheader("Candidate Review Queue")
        st.dataframe(
            display_frame,
            width="stretch",
            hide_index=True,
        )
    with right:
        st.subheader("State Mix")
        st.bar_chart(frame["state"].value_counts())
        st.metric("Candidates", len(frame))
        st.metric("Average Score", f"{frame['final_score'].mean():.2f}")

    alert_rows = load_alert_rows(engine, limit=10)
    st.subheader("Recent Alerts")
    if alert_rows:
        alert_frame = pd.DataFrame(alert_rows)
        alert_columns = [
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
        for column in alert_columns:
            if column not in alert_frame.columns:
                alert_frame[column] = None
        st.dataframe(
            alert_frame[alert_columns].rename(
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
                    "available_at": "Available",
                    "feedback": "Feedback",
                }
            ),
            width="stretch",
            hide_index=True,
        )
    else:
        st.caption("No recent alert rows available.")

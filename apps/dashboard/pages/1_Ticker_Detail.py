from __future__ import annotations

from collections.abc import Mapping, Sequence
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
    records: list[dict[str, object]] = []
    for item in _sequence(value):
        if isinstance(item, Mapping):
            records.append({str(key): _json_ready(row_value) for key, row_value in item.items()})
        else:
            records.append({"value": _json_ready(item)})
    return records


def _show_records(title: str, value: object, *, empty: str) -> None:
    st.subheader(title)
    records = _records(value)
    if records:
        st.dataframe(pd.DataFrame(records), use_container_width=True, hide_index=True)
    else:
        st.caption(empty)


def _show_mapping(title: str, value: object, *, empty: str) -> None:
    st.subheader(title)
    mapping = _mapping(value)
    if mapping:
        rows = [{"Field": str(key), "Value": _json_ready(item)} for key, item in mapping.items()]
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    else:
        st.caption(empty)


def _nested(mapping: Mapping[str, Any], *keys: str) -> object:
    current: object = mapping
    for key in keys:
        current_mapping = _mapping(current)
        if key not in current_mapping:
            return None
        current = current_mapping[key]
    return current


def _first_present(*values: object) -> object:
    for value in values:
        if value is not None and value != "" and value != [] and value != {}:
            return value
    return None


def _metric_value(value: object) -> str:
    if value is None or value == "":
        return "n/a"
    if isinstance(value, float):
        return f"{value:.2f}"
    if isinstance(value, datetime | date):
        return value.isoformat()
    return str(value)


def _load_detail(ticker: str) -> tuple[Mapping[str, Any] | None, str | None]:
    loader = getattr(dashboard_data, "load_ticker_detail", None)
    if loader is None:
        return None, "Ticker detail helper is not available in shared dashboard data."
    config = AppConfig.from_env()
    detail = loader(engine_from_url(config.database_url), ticker)
    if detail is None:
        return None, None
    return _mapping(detail), None


load_dotenv(".env.local")

st.set_page_config(page_title="Ticker Detail", layout="wide")
st.title("Ticker Detail")
st.caption("Point-in-time candidate, evidence, validation, and simulated-paper review.")

ticker = st.sidebar.text_input("Ticker", value="", placeholder="MSFT").strip().upper()

if not ticker:
    st.info("Enter a ticker to review current radar detail.")
else:
    detail, error = _load_detail(ticker)
    if error is not None:
        st.warning(error)
    elif detail is None:
        st.warning("Ticker not found in current radar data.")
    else:
        latest_candidate = _mapping(detail.get("latest_candidate"))
        candidate_packet = _mapping(detail.get("candidate_packet"))
        decision_card = _mapping(detail.get("decision_card"))
        packet_payload = _mapping(candidate_packet.get("payload")) or candidate_packet
        card_payload = _mapping(decision_card.get("payload")) or decision_card

        metric_columns = st.columns(6)
        metric_columns[0].metric("Ticker", _metric_value(detail.get("ticker") or ticker))
        metric_columns[1].metric("State", _metric_value(latest_candidate.get("state")))
        metric_columns[2].metric("Score", _metric_value(latest_candidate.get("final_score")))
        metric_columns[3].metric("Setup", _metric_value(latest_candidate.get("setup_type")))
        metric_columns[4].metric("As Of", _metric_value(latest_candidate.get("as_of")))
        metric_columns[5].metric(
            "Review Only",
            _metric_value(
                _first_present(
                    detail.get("manual_review_only"),
                    card_payload.get("manual_review_only"),
                )
            ),
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
            _show_mapping("Setup Plan", setup_plan, empty="No setup plan fields available.")
        with right:
            hard_blocks = _first_present(
                latest_candidate.get("hard_blocks"),
                latest_candidate.get("portfolio_hard_blocks"),
                _nested(card_payload, "controls", "hard_blocks"),
            )
            _show_records("Hard Blocks", hard_blocks, empty="No hard blocks reported.")

        evidence_left, evidence_right = st.columns([1, 1])
        with evidence_left:
            _show_records(
                "Supporting Evidence",
                _first_present(
                    packet_payload.get("supporting_evidence"),
                    card_payload.get("supporting_evidence"),
                    latest_candidate.get("top_supporting_evidence"),
                ),
                empty="No supporting evidence rows available.",
            )
        with evidence_right:
            _show_records(
                "Disconfirming Evidence",
                _first_present(
                    packet_payload.get("disconfirming_evidence"),
                    card_payload.get("disconfirming_evidence"),
                    latest_candidate.get("top_disconfirming_evidence"),
                ),
                empty="No disconfirming evidence rows available.",
            )

        _show_records("Events", detail.get("events"), empty="No event rows available.")
        _show_records("Snippets", detail.get("snippets"), empty="No snippet rows available.")
        _show_mapping("Portfolio Impact", portfolio_impact, empty="No portfolio impact fields.")
        _show_records(
            "Validation Rows",
            detail.get("validation_results"),
            empty="No validation rows available.",
        )
        _show_records(
            "Paper Trades",
            detail.get("paper_trades"),
            empty="No simulated-paper rows available.",
        )

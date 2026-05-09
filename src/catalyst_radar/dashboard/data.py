from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import Engine, and_, func, select

from catalyst_radar.storage.schema import (
    candidate_packets,
    candidate_states,
    decision_cards,
    signal_features,
)


def load_candidate_rows(engine: Engine) -> list[dict[str, object]]:
    ranked_packets = (
        select(
            candidate_packets.c.id,
            candidate_packets.c.candidate_state_id,
            candidate_packets.c.available_at,
            candidate_packets.c.payload,
            func.row_number()
            .over(
                partition_by=candidate_packets.c.candidate_state_id,
                order_by=(
                    candidate_packets.c.available_at.desc(),
                    candidate_packets.c.created_at.desc(),
                    candidate_packets.c.id.desc(),
                ),
            )
            .label("packet_rank"),
        )
        .where(candidate_packets.c.candidate_state_id.is_not(None))
        .subquery()
    )
    ranked_cards = (
        select(
            decision_cards.c.id,
            decision_cards.c.candidate_packet_id,
            decision_cards.c.available_at,
            decision_cards.c.next_review_at,
            decision_cards.c.payload,
            func.row_number()
            .over(
                partition_by=decision_cards.c.candidate_packet_id,
                order_by=(
                    decision_cards.c.available_at.desc(),
                    decision_cards.c.created_at.desc(),
                    decision_cards.c.id.desc(),
                ),
            )
            .label("card_rank"),
        )
        .subquery()
    )
    stmt = (
        select(
            candidate_states,
            signal_features.c.payload.label("signal_payload"),
            ranked_packets.c.id.label("candidate_packet_id"),
            ranked_packets.c.available_at.label("candidate_packet_available_at"),
            ranked_packets.c.payload.label("candidate_packet_payload"),
            ranked_cards.c.id.label("decision_card_id"),
            ranked_cards.c.available_at.label("decision_card_available_at"),
            ranked_cards.c.next_review_at.label("next_review_at"),
            ranked_cards.c.payload.label("decision_card_payload"),
        )
        .join(
            signal_features,
            and_(
                signal_features.c.ticker == candidate_states.c.ticker,
                signal_features.c.as_of == candidate_states.c.as_of,
                signal_features.c.feature_version == candidate_states.c.feature_version,
            ),
            isouter=True,
        )
        .join(
            ranked_packets,
            and_(
                ranked_packets.c.candidate_state_id == candidate_states.c.id,
                ranked_packets.c.packet_rank == 1,
            ),
            isouter=True,
        )
        .join(
            ranked_cards,
            and_(
                ranked_cards.c.candidate_packet_id == ranked_packets.c.id,
                ranked_cards.c.card_rank == 1,
            ),
            isouter=True,
        )
        .order_by(candidate_states.c.final_score.desc(), candidate_states.c.as_of.desc())
        .limit(200)
    )
    with engine.connect() as conn:
        return [_candidate_row(row._mapping) for row in conn.execute(stmt)]


def _candidate_row(row: Any) -> dict[str, object]:
    values = dict(row)
    for key in (
        "as_of",
        "created_at",
        "candidate_packet_available_at",
        "decision_card_available_at",
        "next_review_at",
    ):
        if key in values and values[key] is not None:
            values[key] = _as_utc_datetime(values[key])
    signal_payload = values.pop("signal_payload", None)
    candidate_packet_payload = values.pop("candidate_packet_payload", None)
    decision_card_payload = values.pop("decision_card_payload", None)
    candidate_payload = (
        signal_payload.get("candidate", {}) if isinstance(signal_payload, dict) else {}
    )
    candidate_metadata = candidate_payload.get("metadata", {})
    if not isinstance(candidate_metadata, dict):
        candidate_metadata = {}
    portfolio_impact = candidate_metadata.get("portfolio_impact", {})
    if not isinstance(portfolio_impact, dict):
        portfolio_impact = {}

    values["setup_type"] = candidate_metadata.get("setup_type")
    values["portfolio_hard_blocks"] = portfolio_impact.get("hard_blocks", [])
    values["entry_zone"] = candidate_payload.get("entry_zone")
    values["invalidation_price"] = candidate_payload.get("invalidation_price")
    values["material_event_count"] = candidate_metadata.get("material_event_count", 0)
    values["top_event_type"] = candidate_metadata.get("top_event_type")
    values["top_event_title"] = candidate_metadata.get("top_event_title")
    values["top_event_source"] = candidate_metadata.get("top_event_source")
    values["top_event_source_url"] = candidate_metadata.get("top_event_source_url")
    values["top_event_source_quality"] = candidate_metadata.get("top_event_source_quality")
    values["top_event_materiality"] = candidate_metadata.get("top_event_materiality")
    values["has_event_conflict"] = candidate_metadata.get("has_event_conflict", False)
    values["event_conflicts"] = candidate_metadata.get("event_conflicts", [])
    values["local_narrative_score"] = candidate_metadata.get("local_narrative_score", 0.0)
    values["local_narrative_bonus"] = candidate_metadata.get("local_narrative_bonus", 0.0)
    values["novelty_score"] = candidate_metadata.get("novelty_score", 0.0)
    values["sentiment_score"] = candidate_metadata.get("sentiment_score", 0.0)
    values["source_quality_score"] = candidate_metadata.get("source_quality_score", 0.0)
    values["theme_match_score"] = candidate_metadata.get("theme_match_score", 0.0)
    values["theme_hits"] = candidate_metadata.get("theme_hits", [])
    values["selected_snippet_ids"] = candidate_metadata.get("selected_snippet_ids", [])
    values["selected_snippet_count"] = candidate_metadata.get("selected_snippet_count", 0)
    values["text_feature_version"] = candidate_metadata.get("text_feature_version")
    values["options_flow_score"] = candidate_metadata.get("options_flow_score", 0.0)
    values["options_risk_score"] = candidate_metadata.get("options_risk_score", 0.0)
    values["call_put_ratio"] = candidate_metadata.get("call_put_ratio", 0.0)
    values["iv_percentile"] = candidate_metadata.get("iv_percentile", 0.0)
    values["sector_rotation_score"] = candidate_metadata.get("sector_rotation_score", 0.0)
    values["theme_velocity_score"] = candidate_metadata.get("theme_velocity_score", 0.0)
    values["peer_readthrough_score"] = candidate_metadata.get("peer_readthrough_score", 0.0)
    values["candidate_theme"] = candidate_metadata.get("candidate_theme")
    values["theme_feature_version"] = candidate_metadata.get("theme_feature_version")
    values["options_feature_version"] = candidate_metadata.get("options_feature_version")
    packet_payload = (
        candidate_packet_payload if isinstance(candidate_packet_payload, dict) else {}
    )
    card_payload = decision_card_payload if isinstance(decision_card_payload, dict) else {}
    values["supporting_evidence_count"] = len(
        packet_payload.get("supporting_evidence", [])
    )
    values["disconfirming_evidence_count"] = len(
        packet_payload.get("disconfirming_evidence", [])
    )
    values["top_supporting_evidence"] = _top_evidence_summary(
        packet_payload.get("supporting_evidence", [])
    )
    values["top_disconfirming_evidence"] = _top_evidence_summary(
        packet_payload.get("disconfirming_evidence", [])
    )
    values["manual_review_disclaimer"] = card_payload.get("disclaimer")
    return values


def _top_evidence_summary(value: object) -> dict[str, object] | None:
    if not isinstance(value, list) or not value or not isinstance(value[0], dict):
        return None
    item = value[0]
    return {
        "kind": item.get("kind"),
        "title": item.get("title"),
        "source_id": item.get("source_id"),
        "source_url": item.get("source_url"),
        "computed_feature_id": item.get("computed_feature_id"),
        "strength": item.get("strength"),
    }


def _as_utc_datetime(value: object) -> object:
    if not isinstance(value, datetime):
        return value
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)

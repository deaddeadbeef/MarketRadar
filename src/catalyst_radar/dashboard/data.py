from __future__ import annotations

from typing import Any

from sqlalchemy import Engine, and_, select

from catalyst_radar.storage.schema import candidate_states, signal_features


def load_candidate_rows(engine: Engine) -> list[dict[str, object]]:
    stmt = (
        select(candidate_states, signal_features.c.payload.label("signal_payload"))
        .join(
            signal_features,
            and_(
                signal_features.c.ticker == candidate_states.c.ticker,
                signal_features.c.as_of == candidate_states.c.as_of,
                signal_features.c.feature_version == candidate_states.c.feature_version,
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
    signal_payload = values.pop("signal_payload", None)
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
    return values

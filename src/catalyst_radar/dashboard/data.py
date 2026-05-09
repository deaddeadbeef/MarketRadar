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
    return values

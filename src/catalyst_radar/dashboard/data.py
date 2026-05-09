from __future__ import annotations

from sqlalchemy import Engine, select

from catalyst_radar.storage.schema import candidate_states


def load_candidate_rows(engine: Engine) -> list[dict[str, object]]:
    stmt = (
        select(candidate_states)
        .order_by(candidate_states.c.final_score.desc(), candidate_states.c.as_of.desc())
        .limit(200)
    )
    with engine.connect() as conn:
        return [dict(row._mapping) for row in conn.execute(stmt)]

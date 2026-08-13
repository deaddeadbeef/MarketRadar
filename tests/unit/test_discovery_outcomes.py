from __future__ import annotations

import pytest

pytestmark = pytest.mark.discovery

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from sqlalchemy import insert

from catalyst_radar.discovery.label import build_discovery_label_payload
from catalyst_radar.discovery.outcomes import build_discovery_outcomes_update
from catalyst_radar.discovery.proof import build_discovery_proof
from catalyst_radar.storage.db import create_schema, engine_from_url
from catalyst_radar.storage.schema import daily_bars


def _seed_bars(engine, ticker: str, start: date, days: int = 12) -> None:
    rows = []
    price = 100.0
    for i in range(days):
        d = start + timedelta(days=i)
        if d.weekday() >= 5:
            continue
        price *= 1.01
        rows.append(
            {
                "ticker": ticker,
                "date": d,
                "open": price * 0.99,
                "high": price * 1.02,
                "low": price * 0.98,
                "close": price,
                "volume": 1_000_000,
                "vwap": price,
                "adjusted": True,
                "provider": "test",
                "source_ts": datetime.combine(d, datetime.min.time(), tzinfo=UTC),
                "available_at": datetime.now(tz=UTC),
            }
        )
    with engine.begin() as conn:
        conn.execute(insert(daily_bars), rows)


def test_discovery_outcomes_preview_and_proof_attach(tmp_path: Path) -> None:
    db = tmp_path / "out.db"
    engine = engine_from_url(f"sqlite:///{db}")
    create_schema(engine)
    start = date(2026, 7, 1)
    _seed_bars(engine, "MU", start, days=20)
    _seed_bars(engine, "SPY", start, days=20)

    written = build_discovery_label_payload(
        engine=engine,
        ticker="MU",
        label="good-research",
        events_path=Path("data/sample/world_events.json"),
        execute=True,
    )
    assert written["label_status"] == "written"

    preview = build_discovery_outcomes_update(engine=engine, execute=False, limit=20)
    assert preview["mode"] == "preview"
    assert preview["discovery_ledger_count"] >= 1
    assert preview["db_writes_made"] == 0

    executed = build_discovery_outcomes_update(engine=engine, execute=True, limit=20)
    assert executed["mode"] == "executed"
    assert int(executed["db_writes_made"] or 0) >= 1

    proof = build_discovery_proof(engine=engine, limit=20)
    assert proof["count"] >= 1
    assert "outcomes" in proof

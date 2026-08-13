from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
from sqlalchemy import create_engine

from catalyst_radar.discovery.bars import import_discovery_bars, write_session_bars_csv
from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.repositories import MarketRepository

pytestmark = pytest.mark.discovery


def test_import_discovery_bars_preview_and_execute(tmp_path: Path) -> None:
    csv_path = write_session_bars_csv(
        tmp_path / "bars.csv",
        tickers=["MU", "SPY"],
        end=date(2026, 8, 13),
        sessions=8,
    )
    engine = create_engine("sqlite:///:memory:")
    preview = import_discovery_bars(engine=engine, csv_path=csv_path, execute=False)
    assert preview["status"] == "preview"
    assert preview["db_writes_made"] == 0
    assert preview["ticker_count"] == 2
    executed = import_discovery_bars(engine=engine, csv_path=csv_path, execute=True)
    assert executed["status"] == "executed"
    assert executed["db_writes_made"] == preview["row_count"]
    create_schema(engine)
    stored = MarketRepository(engine).daily_bars("MU", date(2026, 8, 13), 20)
    assert len(stored) == 8

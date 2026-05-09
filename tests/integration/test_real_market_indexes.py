from __future__ import annotations

from sqlalchemy import inspect

from catalyst_radar.storage.db import create_schema, engine_from_url


def test_real_market_indexes_exist(tmp_path) -> None:
    engine = engine_from_url(f"sqlite:///{tmp_path / 'market.db'}")
    create_schema(engine)

    inspector = inspect(engine)
    daily_bar_indexes = {index["name"] for index in inspector.get_indexes("daily_bars")}
    security_indexes = {index["name"] for index in inspector.get_indexes("securities")}
    universe_snapshot_indexes = {
        index["name"] for index in inspector.get_indexes("universe_snapshots")
    }
    universe_member_indexes = {
        index["name"] for index in inspector.get_indexes("universe_members")
    }
    raw_provider_indexes = {
        index["name"] for index in inspector.get_indexes("raw_provider_records")
    }

    assert "ix_daily_bars_ticker_date_available_at" in daily_bar_indexes
    assert "ix_securities_active_ticker" in security_indexes
    assert "ix_universe_snapshots_name_asof_available_at" in universe_snapshot_indexes
    assert "ix_universe_members_snapshot_rank_ticker" in universe_member_indexes
    assert "ix_raw_provider_provider_kind_source" in raw_provider_indexes

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

from sqlalchemy import create_engine

from catalyst_radar.core.models import DailyBar, Security
from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.provider_repositories import ProviderRepository
from catalyst_radar.storage.repositories import MarketRepository
from catalyst_radar.universe.builder import UniverseBuilder
from catalyst_radar.universe.filters import UniverseFilterConfig


def test_universe_builder_persists_ranked_snapshot_members() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    create_schema(engine)
    market_repo = MarketRepository(engine)
    provider_repo = ProviderRepository(engine)
    as_of = date(2026, 5, 8)
    as_of_dt = datetime(2026, 5, 8, 21, tzinfo=UTC)
    market_repo.upsert_securities(
        [
            _security("AAPL", sector="Technology"),
            _security("MSFT", sector="Technology"),
            _security("THIN", sector="Technology"),
        ]
    )
    market_repo.upsert_daily_bars(
        [
            *_bars("AAPL", close=214, volume=65_000_000, as_of=as_of),
            *_bars("MSFT", close=455, volume=28_000_000, as_of=as_of),
            *_bars("THIN", close=2.1, volume=10_000, as_of=as_of),
        ]
    )
    builder = UniverseBuilder(
        market_repo=market_repo,
        provider_repo=provider_repo,
        config=UniverseFilterConfig(min_price=5, min_avg_dollar_volume=10_000_000),
        name="liquid-us",
        provider="polygon",
    )

    snapshot = builder.build(as_of=as_of, available_at=as_of_dt)
    stored_snapshot = provider_repo.latest_universe_snapshot(
        name="liquid-us",
        as_of=as_of_dt,
        available_at=as_of_dt,
    )
    member_rows = provider_repo.list_universe_member_rows(snapshot.id)

    assert snapshot.member_count == 2
    assert snapshot.excluded_count == 1
    assert provider_repo.list_universe_members(snapshot.id) == ["AAPL", "MSFT"]
    assert [row.rank for row in member_rows] == [1, 2]
    assert stored_snapshot is not None
    assert stored_snapshot.member_count == 2
    assert stored_snapshot.metadata["excluded_count"] == 1
    assert stored_snapshot.metadata["exclusion_reason_counts"]["low_price"] == 1


def _security(ticker: str, *, sector: str) -> Security:
    return Security(
        ticker=ticker,
        name=f"{ticker} Corp.",
        exchange="XNAS",
        sector=sector,
        industry="Software",
        market_cap=1_000_000_000,
        avg_dollar_volume_20d=0,
        has_options=False,
        is_active=True,
        updated_at=datetime(2026, 5, 8, 20, tzinfo=UTC),
    )


def _bars(ticker: str, *, close: float, volume: int, as_of: date) -> list[DailyBar]:
    start = as_of - timedelta(days=19)
    return [
        DailyBar(
            ticker=ticker,
            date=start + timedelta(days=index),
            open=close - 1,
            high=close + 1,
            low=close - 2,
            close=close,
            volume=volume,
            vwap=close,
            adjusted=True,
            provider="polygon",
            source_ts=datetime(2026, 5, 8, 20, tzinfo=UTC),
            available_at=datetime(2026, 5, 8, 21, tzinfo=UTC),
        )
        for index in range(20)
    ]

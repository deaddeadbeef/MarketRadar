from datetime import UTC, datetime

from sqlalchemy import create_engine

from catalyst_radar.core.models import DailyBar, Security
from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.repositories import MarketRepository


def test_repository_round_trips_security_and_bars() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    create_schema(engine)
    repo = MarketRepository(engine)

    updated_at = datetime(2026, 5, 8, 20, tzinfo=UTC)
    repo.upsert_securities(
        [
            Security(
                ticker="AAA",
                name="Alpha Analytics",
                exchange="NASDAQ",
                sector="Technology",
                industry="Software",
                market_cap=5_000_000_000,
                avg_dollar_volume_20d=50_000_000,
                has_options=True,
                is_active=True,
                updated_at=updated_at,
            )
        ]
    )

    repo.upsert_daily_bars(
        [
            DailyBar(
                ticker="AAA",
                date=updated_at.date(),
                open=100,
                high=110,
                low=99,
                close=109,
                volume=1_500_000,
                vwap=108,
                adjusted=True,
                provider="sample",
                source_ts=updated_at,
                available_at=datetime(2026, 5, 8, 21, tzinfo=UTC),
            )
        ]
    )

    securities = repo.list_active_securities()
    bars = repo.daily_bars("AAA", end=updated_at.date(), lookback=10)

    assert [security.ticker for security in securities] == ["AAA"]
    assert len(bars) == 1
    assert bars[0].close == 109

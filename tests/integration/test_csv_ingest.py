from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from catalyst_radar.connectors.csv_market import load_daily_bars_csv, load_securities_csv
from catalyst_radar.core.models import DailyBar, Security
from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.repositories import MarketRepository
from catalyst_radar.storage.schema import candidate_states, daily_bars, signal_features


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
    assert securities[0].updated_at.tzinfo is not None
    assert len(bars) == 1
    assert bars[0].close == 109
    assert bars[0].source_ts.tzinfo is not None
    assert bars[0].available_at.tzinfo is not None


def test_schema_compiles_postgres_volume_and_json_types() -> None:
    dialect = postgresql.dialect()

    daily_bars_ddl = str(CreateTable(daily_bars).compile(dialect=dialect))
    signal_features_ddl = str(CreateTable(signal_features).compile(dialect=dialect))
    candidate_states_ddl = str(CreateTable(candidate_states).compile(dialect=dialect))

    assert "volume BIGINT NOT NULL" in daily_bars_ddl
    assert "payload JSONB NOT NULL" in signal_features_ddl
    assert "hard_blocks JSONB NOT NULL" in candidate_states_ddl
    assert "transition_reasons JSONB NOT NULL" in candidate_states_ddl


def test_csv_connector_loads_fixture_rows() -> None:
    fixture_dir = Path("tests/fixtures")

    securities_rows = load_securities_csv(fixture_dir / "securities.csv")
    daily_bar_rows = load_daily_bars_csv(fixture_dir / "daily_bars.csv")

    assert securities_rows[0].ticker == "AAA"
    assert daily_bar_rows[0].provider == "sample"
    assert daily_bar_rows[0].available_at.isoformat().startswith("2026-05-01T21:00:00")

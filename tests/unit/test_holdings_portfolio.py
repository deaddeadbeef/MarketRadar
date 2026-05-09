from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import create_engine

from catalyst_radar.connectors.base import ConnectorRecordKind
from catalyst_radar.connectors.csv_market import load_holdings_csv
from catalyst_radar.connectors.market_data import CsvMarketDataConnector
from catalyst_radar.connectors.provider_ingest import _holding_from_payload
from catalyst_radar.core.models import HoldingSnapshot
from catalyst_radar.portfolio.holdings import latest_portfolio_state, positions_by_ticker
from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.repositories import MarketRepository


def test_holdings_csv_missing_portfolio_columns_default_to_zero(tmp_path: Path) -> None:
    holdings_csv = tmp_path / "holdings.csv"
    holdings_csv.write_text(
        "\n".join(
            [
                "ticker,shares,market_value,sector,theme,as_of",
                "AAA,20,2000,Technology,ai_infrastructure,2026-05-08T20:00:00Z",
            ]
        ),
        encoding="utf-8",
    )

    rows = load_holdings_csv(holdings_csv)

    assert rows == [
        HoldingSnapshot(
            ticker="AAA",
            shares=20.0,
            market_value=2000.0,
            sector="Technology",
            theme="ai_infrastructure",
            as_of=datetime(2026, 5, 8, 20, tzinfo=UTC),
        )
    ]


def test_holdings_csv_preserves_portfolio_columns(tmp_path: Path) -> None:
    holdings_csv = tmp_path / "holdings.csv"
    holdings_csv.write_text(
        "\n".join(
            [
                "ticker,shares,market_value,sector,theme,as_of,portfolio_value,cash",
                "AAA,20,2000,Technology,ai_infrastructure,2026-05-08T20:00:00Z,"
                "100000,25000",
            ]
        ),
        encoding="utf-8",
    )

    rows = load_holdings_csv(holdings_csv)

    assert rows[0].portfolio_value == 100000.0
    assert rows[0].cash == 25000.0


def test_provider_holding_normalization_preserves_optional_portfolio_fields() -> None:
    connector = CsvMarketDataConnector(
        securities_path="data/sample/securities.csv",
        daily_bars_path="data/sample/daily_bars.csv",
        holdings_path="data/sample/holdings.csv",
    )

    raw_holdings = [
        record
        for record in connector.fetch(_request())
        if record.kind == ConnectorRecordKind.HOLDING
    ]
    normalized = connector.normalize(raw_holdings)
    holding = _holding_from_payload(normalized[0].payload)

    assert holding.portfolio_value == 100000.0
    assert holding.cash == 25000.0


def test_repository_round_trips_portfolio_fields() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    create_schema(engine)
    repo = MarketRepository(engine)
    as_of = datetime(2026, 5, 8, 20, tzinfo=UTC)

    repo.upsert_holdings(
        [
            HoldingSnapshot(
                ticker="AAA",
                shares=20,
                market_value=2000,
                sector="Technology",
                theme="ai_infrastructure",
                as_of=as_of,
                portfolio_value=100000,
                cash=25000,
            )
        ]
    )

    rows = repo.list_holdings()

    assert rows[0].portfolio_value == 100000.0
    assert rows[0].cash == 25000.0


def test_create_schema_upgrades_existing_sqlite_holdings_table() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as conn:
        conn.exec_driver_sql(
            """
            CREATE TABLE holdings_snapshots (
              ticker TEXT NOT NULL,
              as_of TIMESTAMP NOT NULL,
              shares FLOAT NOT NULL,
              market_value FLOAT NOT NULL,
              sector VARCHAR NOT NULL,
              theme VARCHAR NOT NULL,
              PRIMARY KEY (ticker, as_of)
            )
            """
        )
    create_schema(engine)
    create_schema(engine)
    repo = MarketRepository(engine)
    as_of = datetime(2026, 5, 8, 20, tzinfo=UTC)

    repo.upsert_holdings(
        [
            HoldingSnapshot(
                ticker="AAA",
                shares=20,
                market_value=2000,
                sector="Technology",
                theme="ai_infrastructure",
                as_of=as_of,
                portfolio_value=100000,
                cash=25000,
            )
        ]
    )

    rows = repo.list_holdings()

    assert rows[0].portfolio_value == 100000.0
    assert rows[0].cash == 25000.0


def test_latest_portfolio_state_selects_latest_position_rows_as_of_scan_time() -> None:
    older = datetime(2026, 5, 8, 20, tzinfo=UTC)
    latest = datetime(2026, 5, 9, 20, tzinfo=UTC)
    future = datetime(2026, 5, 10, 20, tzinfo=UTC)
    state = latest_portfolio_state(
        [
            HoldingSnapshot("OLD", 1, 100, "Technology", "AI", older, 50_000, 5_000),
            HoldingSnapshot("AAA", 10, 1_000, "Technology", "AI", latest, 100_000, 25_000),
            HoldingSnapshot("BBB", 20, 2_000, "Healthcare", "Defensive", latest, 100_000, 25_000),
            HoldingSnapshot("FUT", 30, 3_000, "Energy", "Power", future, 200_000, 50_000),
        ],
        as_of=datetime(2026, 5, 10, 1, tzinfo=UTC),
        fallback_value=75_000,
        fallback_cash=10_000,
    )

    assert state.as_of == latest
    assert state.portfolio_value == 100_000
    assert state.cash == 25_000
    assert state.source == "holdings_latest_by_ticker"
    assert [holding.ticker for holding in state.holdings] == ["AAA", "BBB", "OLD"]


def test_latest_portfolio_state_keeps_older_positions_during_partial_refresh() -> None:
    older = datetime(2026, 5, 8, 20, tzinfo=UTC)
    partial = datetime(2026, 5, 9, 20, tzinfo=UTC)

    state = latest_portfolio_state(
        [
            HoldingSnapshot("AAA", 10, 1_000, "Technology", "AI", older, 100_000, 25_000),
            HoldingSnapshot("BBB", 200, 20_000, "Technology", "Cloud", older, 100_000, 25_000),
            HoldingSnapshot("AAA", 12, 1_200, "Technology", "AI", partial, 100_000, 25_000),
        ],
        as_of=datetime(2026, 5, 10, 1, tzinfo=UTC),
        fallback_value=0,
        fallback_cash=0,
    )

    positions = positions_by_ticker(state)

    assert state.as_of == partial
    assert positions["AAA"]["notional"] == 1_200
    assert positions["BBB"]["notional"] == 20_000
    assert set(positions) == {"AAA", "BBB"}


def test_latest_portfolio_state_uses_config_fallback_when_no_rows_exist_as_of() -> None:
    state = latest_portfolio_state(
        [
            HoldingSnapshot(
                "FUT",
                30,
                3_000,
                "Energy",
                "Power",
                datetime(2026, 5, 10, 20, tzinfo=UTC),
                200_000,
                50_000,
            )
        ],
        as_of=datetime(2026, 5, 10, 1, tzinfo=UTC),
        fallback_value=75_000,
        fallback_cash=10_000,
    )

    assert state.portfolio_value == 75_000
    assert state.cash == 10_000
    assert state.holdings == ()
    assert state.source == "config_fallback"


def test_latest_portfolio_state_preserves_zero_when_snapshot_and_fallback_are_zero() -> None:
    as_of = datetime(2026, 5, 9, 20, tzinfo=UTC)
    state = latest_portfolio_state(
        [HoldingSnapshot("AAA", 10, 1_000, "Technology", "AI", as_of)],
        as_of=datetime(2026, 5, 10, 1, tzinfo=UTC),
        fallback_value=0,
        fallback_cash=0,
    )

    assert state.portfolio_value == 0.0
    assert state.cash == 0.0
    assert state.source == "holdings_latest_by_ticker"


def test_latest_portfolio_state_uses_conservative_value_for_inconsistent_account_rows() -> None:
    as_of = datetime(2026, 5, 9, 20, tzinfo=UTC)

    state = latest_portfolio_state(
        [
            HoldingSnapshot("AAA", 10, 1_000, "Technology", "AI", as_of, 120_000, 30_000),
            HoldingSnapshot("BBB", 20, 2_000, "Healthcare", "Defensive", as_of, 100_000, 25_000),
        ],
        as_of=datetime(2026, 5, 10, 1, tzinfo=UTC),
        fallback_value=150_000,
        fallback_cash=50_000,
    )

    assert state.portfolio_value == 100_000
    assert state.cash == 25_000
    assert state.input_warnings == ("inconsistent_portfolio_value", "inconsistent_cash")


def test_positions_by_ticker_maps_holdings_to_risk_positions() -> None:
    as_of = datetime(2026, 5, 9, 20, tzinfo=UTC)
    state = latest_portfolio_state(
        [
            HoldingSnapshot("AAA", 10, 1_000, "Technology", "AI", as_of, 100_000, 25_000),
            HoldingSnapshot("BBB", 20, 2_000, "Healthcare", "Defensive", as_of, 100_000, 25_000),
        ],
        as_of=datetime(2026, 5, 10, 1, tzinfo=UTC),
        fallback_value=0,
        fallback_cash=0,
    )

    assert positions_by_ticker(state) == {
        "AAA": {"notional": 1_000, "sector": "Technology", "theme": "AI", "shares": 10},
        "BBB": {
            "notional": 2_000,
            "sector": "Healthcare",
            "theme": "Defensive",
            "shares": 20,
        },
    }


def _request():
    from catalyst_radar.connectors.base import ConnectorRequest

    return ConnectorRequest(
        provider="csv",
        endpoint="fixture-csv",
        params={"scope": "unit"},
        requested_at=datetime(2026, 5, 10, 1, tzinfo=UTC),
    )

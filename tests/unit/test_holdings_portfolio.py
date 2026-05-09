from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import create_engine

from catalyst_radar.connectors.base import ConnectorRecordKind
from catalyst_radar.connectors.csv_market import load_holdings_csv
from catalyst_radar.connectors.market_data import CsvMarketDataConnector
from catalyst_radar.connectors.provider_ingest import _holding_from_payload
from catalyst_radar.core.models import HoldingSnapshot
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


def _request():
    from catalyst_radar.connectors.base import ConnectorRequest

    return ConnectorRequest(
        provider="csv",
        endpoint="fixture-csv",
        params={"scope": "unit"},
        requested_at=datetime(2026, 5, 10, 1, tzinfo=UTC),
    )

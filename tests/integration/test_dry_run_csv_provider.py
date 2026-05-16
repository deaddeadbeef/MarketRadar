from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from catalyst_radar.connectors.base import (
    ConnectorHealthStatus,
    ConnectorRecordKind,
    ConnectorRequest,
)
from catalyst_radar.connectors.market_data import CsvMarketDataConnector
from catalyst_radar.connectors.provider_registry import ConnectorRegistry, default_csv_connector
from catalyst_radar.core.models import DailyBar, HoldingSnapshot, Security
from catalyst_radar.storage.provider_repositories import replay_normalized_records


def test_dry_run_connector_fetches_raw_securities_and_daily_bars() -> None:
    connector = _connector()

    records = connector.fetch(_request())
    second_records = connector.fetch(_request())

    assert len(records) == 45
    assert sum(record.kind == ConnectorRecordKind.SECURITY for record in records) == 8
    assert sum(record.kind == ConnectorRecordKind.DAILY_BAR for record in records) == 36
    assert sum(record.kind == ConnectorRecordKind.HOLDING for record in records) == 1
    assert all(record.provider == "csv" for record in records)
    assert all(record.payload_hash for record in records)
    assert [record.payload_hash for record in records] == [
        record.payload_hash for record in second_records
    ]
    assert all("record" in record.payload for record in records)
    assert connector.rejected_payloads == ()


def test_dry_run_connector_normalizes_payloads_for_current_domain_models() -> None:
    connector = _connector()

    raw_records = connector.fetch(_request())
    normalized = replay_normalized_records(raw_records, connector)
    securities = [
        _security_from_payload(record.payload)
        for record in normalized
        if record.kind == ConnectorRecordKind.SECURITY
    ]
    daily_bars = [
        _daily_bar_from_payload(record.payload)
        for record in normalized
        if record.kind == ConnectorRecordKind.DAILY_BAR
    ]
    holdings = [
        _holding_from_payload(record.payload)
        for record in normalized
        if record.kind == ConnectorRecordKind.HOLDING
    ]

    assert len(securities) == 8
    assert securities[0].ticker == "AAA"
    assert securities[0].updated_at == datetime(2026, 5, 8, 20, tzinfo=UTC)
    by_ticker = {security.ticker: security for security in securities}
    assert by_ticker["AAPL"].metadata["cik"] == "0000320193"
    assert by_ticker["MSFT"].metadata["cik"] == "0000789019"
    assert len(daily_bars) == 36
    assert daily_bars[0].ticker == "AAA"
    assert daily_bars[0].available_at == datetime(2026, 5, 1, 21, tzinfo=UTC)
    assert len(holdings) == 1
    assert holdings[0].ticker == "AAA"


def test_missing_daily_bars_path_produces_down_health(tmp_path: Path) -> None:
    connector = CsvMarketDataConnector(
        securities_path="data/sample/securities.csv",
        daily_bars_path=tmp_path / "missing_daily_bars.csv",
    )

    health = connector.healthcheck()

    assert health.status == ConnectorHealthStatus.DOWN
    assert "missing required csv path" in health.reason


def test_missing_optional_holdings_path_produces_degraded_health(tmp_path: Path) -> None:
    connector = CsvMarketDataConnector(
        securities_path="data/sample/securities.csv",
        daily_bars_path="data/sample/daily_bars.csv",
        holdings_path=tmp_path / "missing_holdings.csv",
    )

    health = connector.healthcheck()

    assert health.status == ConnectorHealthStatus.DEGRADED
    assert "missing optional holdings csv path" in health.reason


def test_cost_estimate_is_zero_for_configured_csv_files() -> None:
    connector = _connector()

    estimate = connector.estimate_cost(_request())

    assert estimate.provider == "csv"
    assert estimate.request_count == 3
    assert estimate.estimated_cost_usd == 0.0


def test_missing_timestamp_row_is_exposed_as_rejected_payload(tmp_path: Path) -> None:
    daily_bars = tmp_path / "daily_bars.csv"
    daily_bars.write_text(
        "\n".join(
            [
                "ticker,date,open,high,low,close,volume,vwap,adjusted,provider,source_ts,"
                "available_at",
                "BAD,2026-05-01,1,1,1,1,100,1,true,sample,,2026-05-01T21:00:00Z",
            ]
        ),
        encoding="utf-8",
    )
    connector = CsvMarketDataConnector(
        securities_path="data/sample/securities.csv",
        daily_bars_path=daily_bars,
    )

    records = connector.fetch(_request())

    assert all(
        record.payload["record"]["ticker"] != "BAD"
        for record in records
        if record.kind == ConnectorRecordKind.DAILY_BAR
    )
    assert len(connector.rejected_payloads) == 1
    rejected = connector.rejected_payloads[0]
    assert rejected.kind == ConnectorRecordKind.DAILY_BAR
    assert rejected.affected_tickers == ("BAD",)
    assert rejected.payload["record"]["ticker"] == "BAD"
    assert "missing mandatory timestamp field" in rejected.reason


def test_future_source_timestamp_row_is_rejected(tmp_path: Path) -> None:
    daily_bars = tmp_path / "daily_bars.csv"
    daily_bars.write_text(
        "\n".join(
            [
                "ticker,date,open,high,low,close,volume,vwap,adjusted,provider,source_ts,"
                "available_at",
                "FUT,2999-01-01,1,1,1,1,100,1,true,sample,2999-01-01T20:00:00Z,"
                "2999-01-01T21:00:00Z",
            ]
        ),
        encoding="utf-8",
    )
    connector = CsvMarketDataConnector(
        securities_path="data/sample/securities.csv",
        daily_bars_path=daily_bars,
    )

    records = connector.fetch(_request())

    assert all(
        record.payload["record"]["ticker"] != "FUT"
        for record in records
        if record.kind == ConnectorRecordKind.DAILY_BAR
    )
    assert len(connector.rejected_payloads) == 1
    assert "source_ts is later than actual fetch time" in connector.rejected_payloads[0].reason


def test_missing_identity_field_is_rejected_before_normalization(tmp_path: Path) -> None:
    daily_bars = tmp_path / "daily_bars.csv"
    daily_bars.write_text(
        "\n".join(
            [
                "ticker,date,open,high,low,close,volume,vwap,adjusted,provider,source_ts,"
                "available_at",
                ",2026-05-01,1,1,1,1,100,1,true,sample,2026-05-01T20:00:00Z,"
                "2026-05-01T21:00:00Z",
            ]
        ),
        encoding="utf-8",
    )
    connector = CsvMarketDataConnector(
        securities_path="data/sample/securities.csv",
        daily_bars_path=daily_bars,
    )

    records = connector.fetch(_request())

    assert all(record.kind != ConnectorRecordKind.DAILY_BAR for record in records)
    assert len(connector.rejected_payloads) == 1
    assert "missing mandatory field(s): ticker" in connector.rejected_payloads[0].reason


def test_invalid_numeric_daily_bar_field_is_rejected_before_normalization(tmp_path: Path) -> None:
    daily_bars = tmp_path / "daily_bars.csv"
    daily_bars.write_text(
        "\n".join(
            [
                "ticker,date,open,high,low,close,volume,vwap,adjusted,provider,source_ts,"
                "available_at",
                "BAD,2026-05-01,not-a-number,1,1,1,100,1,true,sample,"
                "2026-05-01T20:00:00Z,2026-05-01T21:00:00Z",
            ]
        ),
        encoding="utf-8",
    )
    connector = CsvMarketDataConnector(
        securities_path="data/sample/securities.csv",
        daily_bars_path=daily_bars,
    )

    records = connector.fetch(_request())

    assert all(
        record.payload["record"]["ticker"] != "BAD"
        for record in records
        if record.kind == ConnectorRecordKind.DAILY_BAR
    )
    assert len(connector.rejected_payloads) == 1
    assert "could not convert string to float" in connector.rejected_payloads[0].reason


def test_registry_instances_are_explicit_and_resettable() -> None:
    registry = ConnectorRegistry()
    connector = default_csv_connector(
        "data/sample/securities.csv",
        "data/sample/daily_bars.csv",
    )

    registry.register_connector("CSV", connector)
    assert registry.get_connector("csv") is connector

    registry.reset()
    try:
        registry.get_connector("csv")
    except KeyError as exc:
        assert "not registered" in str(exc)
    else:
        raise AssertionError("registry should not retain connectors after reset")


def _connector() -> CsvMarketDataConnector:
    return CsvMarketDataConnector(
        securities_path="data/sample/securities.csv",
        daily_bars_path="data/sample/daily_bars.csv",
        holdings_path="data/sample/holdings.csv",
    )


def _request() -> ConnectorRequest:
    return ConnectorRequest(
        provider="csv",
        endpoint="fixture-csv",
        params={"scope": "integration"},
        requested_at=datetime(2026, 5, 10, 1, tzinfo=UTC),
    )


def _security_from_payload(payload: object) -> Security:
    row = dict(payload)  # type: ignore[arg-type]
    return Security(
        ticker=str(row["ticker"]),
        name=str(row["name"]),
        exchange=str(row["exchange"]),
        sector=str(row["sector"]),
        industry=str(row["industry"]),
        market_cap=float(row["market_cap"]),
        avg_dollar_volume_20d=float(row["avg_dollar_volume_20d"]),
        has_options=bool(row["has_options"]),
        is_active=bool(row["is_active"]),
        updated_at=datetime.fromisoformat(str(row["updated_at"])),
        metadata=dict(row.get("metadata") or {}),
    )


def _daily_bar_from_payload(payload: object) -> DailyBar:
    row = dict(payload)  # type: ignore[arg-type]
    return DailyBar(
        ticker=str(row["ticker"]),
        date=datetime.fromisoformat(str(row["date"])).date(),
        open=float(row["open"]),
        high=float(row["high"]),
        low=float(row["low"]),
        close=float(row["close"]),
        volume=int(row["volume"]),
        vwap=float(row["vwap"]),
        adjusted=bool(row["adjusted"]),
        provider=str(row["provider"]),
        source_ts=datetime.fromisoformat(str(row["source_ts"])),
        available_at=datetime.fromisoformat(str(row["available_at"])),
    )


def _holding_from_payload(payload: object) -> HoldingSnapshot:
    row = dict(payload)  # type: ignore[arg-type]
    return HoldingSnapshot(
        ticker=str(row["ticker"]),
        shares=float(row["shares"]),
        market_value=float(row["market_value"]),
        sector=str(row["sector"]),
        theme=str(row["theme"]),
        as_of=datetime.fromisoformat(str(row["as_of"])),
    )

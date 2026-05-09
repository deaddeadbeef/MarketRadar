from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from catalyst_radar.connectors.base import (
    ConnectorHealthStatus,
    ConnectorRecordKind,
    ConnectorRequest,
)
from catalyst_radar.connectors.http import FakeHttpTransport, HttpResponse, JsonHttpClient
from catalyst_radar.connectors.polygon import (
    PolygonEndpoint,
    PolygonMarketDataConnector,
)
from catalyst_radar.core.models import DataQualitySeverity


def test_healthcheck_fails_closed_without_key() -> None:
    connector = PolygonMarketDataConnector(
        api_key=None,
        client=JsonHttpClient(transport=FakeHttpTransport({}), timeout_seconds=3),
    )

    health = connector.healthcheck()

    assert health.status == ConnectorHealthStatus.DOWN
    assert health.reason == "missing CATALYST_POLYGON_API_KEY"


def test_grouped_daily_normalizes_adjusted_daily_bars() -> None:
    connector = PolygonMarketDataConnector(
        api_key="fixture-key",
        client=_client_for_fixture(_grouped_daily_url(), _fixture("grouped_daily_2026-05-08.json")),
    )
    request = _grouped_daily_request()

    raw = connector.fetch(request)
    normalized = connector.normalize(raw)
    aapl = next(record for record in normalized if record.identity == "AAPL:2026-05-08")

    assert aapl.kind == ConnectorRecordKind.DAILY_BAR
    assert aapl.payload["adjusted"] is True
    assert aapl.payload["provider"] == "polygon"
    assert aapl.source_ts.tzinfo is not None
    assert aapl.available_at.tzinfo is not None
    assert not any("fixture-key" in record.request_hash for record in raw)


def test_grouped_daily_rejects_missing_timestamp() -> None:
    connector = PolygonMarketDataConnector(
        api_key="fixture-key",
        client=_client_for_fixture(_grouped_daily_url(), _fixture("grouped_daily_2026-05-08.json")),
    )

    connector.fetch(_grouped_daily_request())

    assert connector.rejected_payloads[0].affected_tickers == ("BADTS",)
    assert connector.rejected_payloads[0].fail_closed_action == "reject-payload"


def test_next_session_availability_policy_uses_next_day_11_utc() -> None:
    connector = PolygonMarketDataConnector(
        api_key="fixture-key",
        client=_client_for_fixture(_grouped_daily_url(), _fixture("grouped_daily_2026-05-08.json")),
        availability_policy="next_session_11_utc",
    )

    raw = connector.fetch(_grouped_daily_request())

    assert raw[0].source_ts == datetime(2026, 5, 8, tzinfo=UTC)
    assert raw[0].available_at == datetime(2026, 5, 9, 11, tzinfo=UTC)


def test_ticker_pages_follow_next_url_without_leaking_key() -> None:
    first_url = (
        "https://api.polygon.io/v3/reference/tickers?"
        "market=stocks&active=true&limit=1000&apiKey=fixture-key"
    )
    fixture_next_url = "https://api.polygon.io/v3/reference/tickers?cursor=page-2"
    second_url = f"{fixture_next_url}&apiKey=fixture-key"
    transport = FakeHttpTransport(
        {
            first_url: _response(first_url, _fixture("tickers_page_1.json")),
            second_url: _response(second_url, _fixture("tickers_page_2.json")),
        }
    )
    connector = PolygonMarketDataConnector(
        api_key="fixture-key",
        client=JsonHttpClient(transport=transport, timeout_seconds=3),
    )
    raw_records = connector.fetch(
        ConnectorRequest(
            provider="polygon",
            endpoint=PolygonEndpoint.TICKERS.value,
            params={"market": "stocks", "active": True, "limit": 1000},
            requested_at=datetime(2026, 5, 9, 12, tzinfo=UTC),
        )
    )
    normalized = connector.normalize(raw_records)

    assert transport.requests == [first_url, second_url]
    assert len(raw_records) == 4
    assert "apiKey=" not in raw_records[0].request_hash
    assert "fixture-key" not in raw_records[0].request_hash
    assert {record.identity for record in normalized} == {"AAPL", "SPY", "MSFT", "OLD"}
    aapl = next(record for record in normalized if record.identity == "AAPL")
    assert aapl.payload["sector"] == "Unknown"
    assert aapl.payload["industry"] == "Technology"
    assert aapl.payload["metadata"]["type"] == "CS"


def test_grouped_daily_contract_failure_is_abort_rejection() -> None:
    url = _grouped_daily_url()
    transport = FakeHttpTransport(
        {
            url: HttpResponse(
                status_code=200,
                url=url,
                headers={"content-type": "application/json"},
                body=b'{"status":"OK","adjusted":false,"results":[]}',
            )
        }
    )
    connector = PolygonMarketDataConnector(
        api_key="fixture-key",
        client=JsonHttpClient(transport=transport, timeout_seconds=3),
    )

    raw = connector.fetch(_grouped_daily_request())

    assert raw == []
    assert connector.rejected_payloads[0].severity == DataQualitySeverity.CRITICAL
    assert connector.rejected_payloads[0].fail_closed_action == "abort-ingest"


def test_cost_estimate_counts_one_grouped_daily_request() -> None:
    connector = PolygonMarketDataConnector(
        api_key="fixture-key",
        client=JsonHttpClient(transport=FakeHttpTransport({}), timeout_seconds=3),
    )

    estimate = connector.estimate_cost(_grouped_daily_request())

    assert estimate.provider == "polygon"
    assert estimate.request_count == 1
    assert estimate.estimated_cost_usd == 0.0


def _grouped_daily_request() -> ConnectorRequest:
    return ConnectorRequest(
        provider="polygon",
        endpoint=PolygonEndpoint.GROUPED_DAILY.value,
        params={"date": "2026-05-08", "adjusted": True, "include_otc": False},
        requested_at=datetime(2026, 5, 9, 12, tzinfo=UTC),
    )


def _grouped_daily_url() -> str:
    return (
        "https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/"
        "2026-05-08?adjusted=true&include_otc=false&apiKey=fixture-key"
    )


def _client_for_fixture(url: str, fixture_path: Path) -> JsonHttpClient:
    return JsonHttpClient(
        transport=FakeHttpTransport({url: _response(url, fixture_path)}),
        timeout_seconds=3,
    )


def _response(url: str, fixture_path: Path) -> HttpResponse:
    return HttpResponse(
        status_code=200,
        url=url,
        headers={"content-type": "application/json"},
        body=fixture_path.read_bytes(),
    )


def _fixture(name: str) -> Path:
    return Path("tests/fixtures/polygon") / name

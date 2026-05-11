from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import urlencode

import pytest

from catalyst_radar.brokers.models import (
    BrokerConnection,
    BrokerConnectionStatus,
    broker_connection_id,
)
from catalyst_radar.brokers.portfolio_context import latest_broker_portfolio_context
from catalyst_radar.brokers.schwab import SchwabClient
from catalyst_radar.brokers.sync import sync_schwab_read_only
from catalyst_radar.connectors.http import FakeHttpTransport, HttpResponse, JsonHttpClient
from catalyst_radar.storage.broker_repositories import BrokerRepository
from catalyst_radar.storage.db import create_schema, engine_from_url
from catalyst_radar.storage.repositories import MarketRepository


def test_schwab_fake_sync_persists_accounts_positions_balances_and_orders(tmp_path: Path) -> None:
    engine = engine_from_url(f"sqlite:///{(tmp_path / 'schwab.db').as_posix()}")
    create_schema(engine)
    now = datetime(2026, 5, 12, 14, tzinfo=UTC)
    base_url = "https://api.schwabapi.com"
    transport = FakeHttpTransport(
        {
            f"{base_url}/trader/v1/accounts/accountNumbers": _fixture_response(
                "account_numbers.json"
            ),
            f"{base_url}/trader/v1/accounts?fields=positions": _fixture_response(
                "accounts_positions.json"
            ),
            _orders_url(base_url, now): _fixture_response("open_orders.json"),
        }
    )
    client = SchwabClient(
        client=JsonHttpClient(transport, timeout_seconds=5.0),
        access_token="fake-access-token",
        base_url=base_url,
    )

    result = sync_schwab_read_only(
        client=client,
        broker_repo=BrokerRepository(engine),
        market_repo=MarketRepository(engine),
        now=now,
    )

    repo = BrokerRepository(engine)
    assert result.account_count == 1
    assert result.balance_count == 1
    assert result.position_count == 1
    assert result.open_order_count == 1
    assert repo.latest_connection().status.value == "connected"
    assert repo.latest_balance().equity == 250000.75
    assert repo.latest_positions()[0].ticker == "GLW"
    assert repo.latest_positions()[0].sector == "technology"
    assert repo.list_open_orders()[0].ticker == "GLW"
    assert MarketRepository(engine).list_holdings()[0].ticker == "GLW"

    context = latest_broker_portfolio_context(engine, ticker="GLW", available_at=now)
    assert context["broker_connected"] is True
    assert context["broker_data_stale"] is False
    assert context["existing_position"]["market_value"] == 9500

    later = now + timedelta(minutes=15)
    empty_order_transport = FakeHttpTransport(
        {
            f"{base_url}/trader/v1/accounts/accountNumbers": _fixture_response(
                "account_numbers.json"
            ),
            f"{base_url}/trader/v1/accounts?fields=positions": _fixture_response(
                "accounts_positions.json"
            ),
            _orders_url(base_url, later): HttpResponse(
                status_code=200,
                url="fixture://empty-open-orders",
                headers={"content-type": "application/json"},
                body=b"[]",
            ),
        }
    )
    empty_order_client = SchwabClient(
        client=JsonHttpClient(empty_order_transport, timeout_seconds=5.0),
        access_token="fake-access-token",
        base_url=base_url,
    )

    second_result = sync_schwab_read_only(
        client=empty_order_client,
        broker_repo=repo,
        market_repo=MarketRepository(engine),
        now=later,
    )

    assert second_result.open_order_count == 0
    assert repo.list_open_orders() == []

    no_position_time = later + timedelta(minutes=15)
    no_position_transport = FakeHttpTransport(
        {
            f"{base_url}/trader/v1/accounts/accountNumbers": _fixture_response(
                "account_numbers.json"
            ),
            f"{base_url}/trader/v1/accounts?fields=positions": _no_positions_response(),
            _orders_url(base_url, no_position_time): HttpResponse(
                status_code=200,
                url="fixture://empty-open-orders",
                headers={"content-type": "application/json"},
                body=b"[]",
            ),
        }
    )
    no_position_client = SchwabClient(
        client=JsonHttpClient(no_position_transport, timeout_seconds=5.0),
        access_token="fake-access-token",
        base_url=base_url,
    )

    third_result = sync_schwab_read_only(
        client=no_position_client,
        broker_repo=repo,
        market_repo=MarketRepository(engine),
        now=no_position_time,
    )
    empty_context = latest_broker_portfolio_context(
        engine,
        ticker="GLW",
        available_at=no_position_time,
    )

    assert third_result.position_count == 0
    assert repo.latest_positions() == []
    assert empty_context["existing_position"] is None
    latest_holdings = {
        holding.ticker: holding
        for holding in MarketRepository(engine).list_holdings()
        if holding.as_of == no_position_time
    }
    assert latest_holdings["GLW"].shares == 0.0
    assert latest_holdings["GLW"].market_value == 0.0


def test_failed_schwab_sync_does_not_mark_success(tmp_path: Path) -> None:
    engine = engine_from_url(f"sqlite:///{(tmp_path / 'schwab-fail.db').as_posix()}")
    create_schema(engine)
    repo = BrokerRepository(engine)
    previous_success = datetime(2026, 5, 12, 13, tzinfo=UTC)
    attempted_at = datetime(2026, 5, 12, 14, tzinfo=UTC)
    connection_id = broker_connection_id()
    repo.upsert_connection(
        BrokerConnection(
            id=connection_id,
            broker="schwab",
            user_id="local",
            status=BrokerConnectionStatus.CONNECTED,
            created_at=previous_success,
            updated_at=previous_success,
            last_successful_sync_at=previous_success,
            metadata={"mode": "read_only"},
        )
    )
    client = SchwabClient(
        client=JsonHttpClient(FakeHttpTransport({}), timeout_seconds=5.0),
        access_token="fake-access-token",
        base_url="https://api.schwabapi.com",
    )

    with pytest.raises(RuntimeError, match="missing fake HTTP response"):
        sync_schwab_read_only(client=client, broker_repo=repo, now=attempted_at)

    connection = repo.latest_connection()
    assert connection is not None
    assert connection.status == BrokerConnectionStatus.ERROR
    assert connection.last_successful_sync_at == previous_success
    assert connection.updated_at == attempted_at
    assert connection.metadata["error_type"] == "RuntimeError"


def _fixture_response(name: str) -> HttpResponse:
    path = Path("tests/fixtures/schwab") / name
    return HttpResponse(
        status_code=200,
        url=f"fixture://{name}",
        headers={"content-type": "application/json"},
        body=path.read_bytes(),
    )


def _no_positions_response() -> HttpResponse:
    return HttpResponse(
        status_code=200,
        url="fixture://no-positions",
        headers={"content-type": "application/json"},
        body=(
            b'[{"securitiesAccount":{"accountHash":"account-hash-123",'
            b'"type":"MARGIN","currentBalances":{"cashBalance":250000.75,'
            b'"buyingPower":500000.0,"liquidationValue":250000.75},'
            b'"positions":[]}}]'
        ),
    )


def _orders_url(base_url: str, now: datetime) -> str:
    query = urlencode(
        {
            "fromEnteredTime": (now - timedelta(days=30))
            .isoformat()
            .replace("+00:00", "Z"),
            "toEnteredTime": now.isoformat().replace("+00:00", "Z"),
            "status": "WORKING",
        }
    )
    return f"{base_url}/trader/v1/accounts/account-hash-123/orders?{query}"

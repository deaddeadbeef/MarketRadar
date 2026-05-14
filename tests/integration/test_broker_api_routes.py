from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from fastapi.testclient import TestClient

from apps.api.main import create_app
from catalyst_radar.brokers.models import (
    BrokerAccount,
    BrokerBalanceSnapshot,
    BrokerConnection,
    BrokerConnectionStatus,
    BrokerMarketSnapshot,
    BrokerPosition,
    BrokerSyncResult,
    BrokerToken,
    broker_account_id,
    broker_balance_snapshot_id,
    broker_connection_id,
    broker_market_snapshot_id,
    broker_position_id,
    broker_token_id,
)
from catalyst_radar.brokers.tokens import TokenCipher
from catalyst_radar.security.audit import AuditLogRepository
from catalyst_radar.storage.broker_repositories import BrokerRepository
from catalyst_radar.storage.db import create_schema, engine_from_url


def test_schwab_status_and_connect_fail_cleanly_without_app_credentials(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'api.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    _blank_schwab_app_env(monkeypatch)
    create_schema(engine_from_url(database_url))
    client = TestClient(create_app())

    status_response = client.get("/api/brokers/schwab/status")
    connect_response = client.get("/api/brokers/schwab/connect", follow_redirects=False)

    assert status_response.status_code == 200
    assert status_response.json()["configured"] is False
    assert status_response.json()["connected"] is False
    assert connect_response.status_code == 503
    assert "SCHWAB_CLIENT_ID" in connect_response.json()["detail"]


def test_schwab_connect_records_state_and_callback_rejects_mismatch(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'oauth.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("SCHWAB_CLIENT_ID", "client-id")
    monkeypatch.setenv("SCHWAB_CLIENT_SECRET", "client-secret")
    monkeypatch.setenv("SCHWAB_REDIRECT_URI", "https://localhost/callback")
    monkeypatch.setenv("BROKER_TOKEN_ENCRYPTION_KEY", "local-dev-key")
    engine = engine_from_url(database_url)
    create_schema(engine)
    client = TestClient(create_app())

    connect_response = client.get("/api/brokers/schwab/connect", follow_redirects=False)
    redirect = connect_response.headers["location"]
    query = parse_qs(urlparse(redirect).query)
    connection = BrokerRepository(engine).latest_connection()
    mismatch_response = client.get("/api/brokers/schwab/callback?code=fake-code&state=wrong-state")

    assert connect_response.status_code == 307
    assert query["state"]
    assert connection is not None
    assert connection.status == BrokerConnectionStatus.NEEDS_AUTH
    assert connection.metadata["oauth_state"] == query["state"][0]
    assert mismatch_response.status_code == 400
    assert "state" in mismatch_response.json()["detail"]


def test_portfolio_routes_return_synced_broker_snapshot(tmp_path: Path, monkeypatch) -> None:
    database_url = f"sqlite:///{(tmp_path / 'portfolio.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = engine_from_url(database_url)
    create_schema(engine)
    _seed_broker_rows(engine)
    client = TestClient(create_app())

    snapshot = client.get("/api/portfolio/snapshot")
    positions = client.get("/api/portfolio/positions")
    exposure = client.get("/api/portfolio/exposure")

    assert snapshot.status_code == 200
    assert snapshot.json()["connection_status"] == "connected"
    assert positions.status_code == 200
    assert positions.json()["items"][0]["ticker"] == "GLW"
    assert exposure.status_code == 200
    assert exposure.json()["portfolio_equity"] == 250000.0


def test_disconnect_makes_portfolio_context_not_connected(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'disconnect.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    engine = engine_from_url(database_url)
    create_schema(engine)
    _seed_broker_rows(engine)
    client = TestClient(create_app())

    disconnect = client.post("/api/brokers/schwab/disconnect")
    status = client.get("/api/brokers/schwab/status")
    exposure = client.get("/api/portfolio/exposure")

    assert disconnect.status_code == 200
    assert status.json()["connected"] is False
    assert status.json()["status"] == "disconnected"
    assert exposure.json()["broker_connected"] is False
    assert "broker_disconnected" in exposure.json()["hard_blocks"]


def test_schwab_sync_rejects_expired_access_token_without_refresh(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'expired-token.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("BROKER_TOKEN_ENCRYPTION_KEY", "local-dev-key")
    engine = engine_from_url(database_url)
    create_schema(engine)
    _seed_broker_rows(engine)
    repo = BrokerRepository(engine)
    connection = repo.latest_connection()
    assert connection is not None
    now = datetime.now(UTC)
    repo.upsert_token(
        BrokerToken(
            id=broker_token_id(connection.id),
            connection_id=connection.id,
            access_token_encrypted=TokenCipher("local-dev-key").encrypt("expired-access"),
            access_token_expires_at=now - timedelta(minutes=5),
            created_at=now - timedelta(hours=1),
            updated_at=now - timedelta(hours=1),
        )
    )
    client = TestClient(create_app())

    response = client.post("/api/brokers/schwab/sync")

    assert response.status_code == 409
    assert "expired" in response.json()["detail"]
    assert BrokerRepository(engine).latest_connection().status == (
        BrokerConnectionStatus.NEEDS_AUTH
    )


def test_schwab_sync_marks_needs_auth_when_refresh_fails(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'refresh-fail.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("SCHWAB_CLIENT_ID", "client-id")
    monkeypatch.setenv("SCHWAB_CLIENT_SECRET", "client-secret")
    monkeypatch.setenv("SCHWAB_REDIRECT_URI", "https://localhost/callback")
    monkeypatch.setenv("BROKER_TOKEN_ENCRYPTION_KEY", "local-dev-key")
    engine = engine_from_url(database_url)
    create_schema(engine)
    _seed_broker_rows(engine)
    repo = BrokerRepository(engine)
    connection = repo.latest_connection()
    assert connection is not None
    now = datetime.now(UTC)
    cipher = TokenCipher("local-dev-key")
    repo.upsert_token(
        BrokerToken(
            id=broker_token_id(connection.id),
            connection_id=connection.id,
            access_token_encrypted=cipher.encrypt("expired-access"),
            refresh_token_encrypted=cipher.encrypt("revoked-refresh"),
            access_token_expires_at=now - timedelta(minutes=5),
            refresh_token_expires_at=now + timedelta(days=1),
            created_at=now - timedelta(hours=1),
            updated_at=now - timedelta(hours=1),
        )
    )

    class FailingRefreshClient:
        def refresh_access_token(self, refresh_token: str):
            assert refresh_token == "revoked-refresh"
            raise RuntimeError("HTTP 400 from token endpoint")

    monkeypatch.setattr(
        "catalyst_radar.api.routes.brokers.SchwabOAuthService",
        lambda *args, **kwargs: FailingRefreshClient(),
    )
    client = TestClient(create_app())

    response = client.post("/api/brokers/schwab/sync")

    assert response.status_code == 409
    assert "refresh failed" in response.json()["detail"]
    connection = BrokerRepository(engine).latest_connection()
    assert connection.status == BrokerConnectionStatus.NEEDS_AUTH
    assert connection.metadata["reason"] == "refresh_failed"


def test_schwab_sync_returns_429_on_repeated_attempt_without_second_schwab_call(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'sync-rate-limit.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("BROKER_TOKEN_ENCRYPTION_KEY", "local-dev-key")
    monkeypatch.setenv("SCHWAB_SYNC_MIN_INTERVAL_SECONDS", "120")
    engine = engine_from_url(database_url)
    create_schema(engine)
    _seed_broker_rows(engine)
    _seed_valid_token(engine)
    calls: list[str] = []

    def fake_sync(**kwargs):
        calls.append("sync")
        return BrokerSyncResult(
            connection_id=broker_connection_id(),
            account_count=1,
            balance_count=1,
            position_count=0,
            open_order_count=0,
            synced_at=datetime.now(UTC),
        )

    monkeypatch.setattr("catalyst_radar.api.routes.brokers.sync_schwab_read_only", fake_sync)
    client = TestClient(create_app())

    first = client.post("/api/brokers/schwab/sync")
    second = client.post("/api/brokers/schwab/sync")

    assert first.status_code == 200
    assert second.status_code == 429
    assert second.headers["retry-after"]
    assert second.json()["detail"]["operation"] == "portfolio_sync"
    assert calls == ["sync"]


def test_schwab_market_sync_limits_ticker_batch_size(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'market-max-tickers.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("SCHWAB_MARKET_SYNC_MAX_TICKERS", "1")
    engine = engine_from_url(database_url)
    create_schema(engine)
    _seed_broker_rows(engine)
    client = TestClient(create_app())

    response = client.post(
        "/api/brokers/schwab/market-sync",
        json={"tickers": ["GLW", "MSFT"]},
    )

    assert response.status_code == 400
    assert "maximum is 1" in response.json()["detail"]


def test_schwab_market_sync_returns_429_on_repeated_attempt_without_second_schwab_call(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'market-rate-limit.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("BROKER_TOKEN_ENCRYPTION_KEY", "local-dev-key")
    monkeypatch.setenv("SCHWAB_MARKET_SYNC_MIN_INTERVAL_SECONDS", "120")
    engine = engine_from_url(database_url)
    create_schema(engine)
    _seed_broker_rows(engine)
    _seed_valid_token(engine)
    calls: list[str] = []

    def fake_market_sync(**kwargs):
        calls.append("market")
        now = datetime.now(UTC)
        return [
            BrokerMarketSnapshot(
                id=broker_market_snapshot_id("GLW", now),
                ticker="GLW",
                as_of=now,
                last_price=95.0,
                raw_payload={},
                created_at=now,
            )
        ]

    monkeypatch.setattr("catalyst_radar.api.routes.brokers.sync_market_context", fake_market_sync)
    client = TestClient(create_app())

    first = client.post("/api/brokers/schwab/market-sync", json={"tickers": ["GLW"]})
    second = client.post("/api/brokers/schwab/market-sync", json={"tickers": ["GLW"]})

    assert first.status_code == 200
    assert second.status_code == 429
    assert second.headers["retry-after"]
    assert second.json()["detail"]["operation"] == "market_context_sync"
    assert calls == ["market"]


def test_order_preview_is_never_submittable_even_when_flag_enabled(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'preview.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("SCHWAB_ORDER_SUBMISSION_ENABLED", "true")
    engine = engine_from_url(database_url)
    create_schema(engine)
    _seed_broker_rows(engine)
    client = TestClient(create_app())

    response = client.post(
        "/api/orders/preview",
        json={
            "ticker": "GLW",
            "side": "buy",
            "entry_price": 95.0,
            "invalidation_price": 90.0,
            "risk_per_trade_pct": 0.005,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["action"] == "preview_only"
    assert payload["submission_enabled"] is True
    assert payload["submission_allowed"] is False
    assert "broker_read_only_integration" in payload["hard_blocks"]


def test_interactive_routes_record_actions_triggers_and_blocked_tickets(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'interactive-api.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("SCHWAB_ORDER_SUBMISSION_ENABLED", "true")
    engine = engine_from_url(database_url)
    create_schema(engine)
    _seed_broker_rows(engine)
    repo = BrokerRepository(engine)
    now = datetime(2026, 5, 12, 14, tzinfo=UTC)
    repo.upsert_market_snapshots(
        [
            BrokerMarketSnapshot(
                id=broker_market_snapshot_id("GLW", now),
                ticker="GLW",
                as_of=now,
                last_price=95.0,
                total_volume=2_000_000,
                raw_payload={},
                created_at=now,
            )
        ]
    )
    client = TestClient(create_app())

    action = client.post(
        "/api/opportunities/actions",
        headers={
            "X-Catalyst-Actor": "analyst-1 apikey=hidden-actor-secret",
            "X-Catalyst-Role": "analyst apikey=hidden-role-secret",
        },
        json={
            "ticker": "GLW",
            "action": "watch",
            "thesis": "early signal",
            "source": "apikey=hidden-action-source",
        },
    )
    trigger = client.post(
        "/api/market/triggers",
        headers={
            "X-Catalyst-Actor": "analyst-1 apikey=hidden-actor-secret",
            "X-Catalyst-Role": "analyst apikey=hidden-role-secret",
        },
        json={
            "ticker": "GLW",
            "trigger_type": "price_above",
            "operator": "gte",
            "threshold": 90.0,
            "source": "apikey=hidden-trigger-source",
        },
    )
    evaluated = client.post(
        "/api/market/triggers/evaluate",
        headers={
            "X-Catalyst-Actor": "analyst-1 apikey=hidden-actor-secret",
            "X-Catalyst-Role": "analyst apikey=hidden-role-secret",
        },
        json={"tickers": ["GLW"]},
    )
    ticket = client.post(
        "/api/orders/tickets",
        headers={
            "X-Catalyst-Actor": "analyst-1 apikey=hidden-actor-secret",
            "X-Catalyst-Role": "analyst apikey=hidden-role-secret",
        },
        json={
            "ticker": "GLW",
            "side": "buy",
            "entry_price": 95.0,
            "invalidation_price": 90.0,
        },
    )
    actions = client.get("/api/opportunities/actions?ticker=GLW")
    triggers = client.get("/api/market/triggers?ticker=GLW")
    tickets = client.get("/api/orders/tickets?ticker=GLW")

    assert action.status_code == 200
    assert action.json()["status"] == "active"
    assert trigger.status_code == 200
    assert trigger.json()["status"] == "active"
    assert evaluated.status_code == 200
    assert evaluated.json()["items"][0]["status"] == "fired"
    assert ticket.status_code == 200
    assert ticket.json()["submission_allowed"] is False
    assert ticket.json()["status"] == "blocked"
    assert "broker_read_only_integration" in ticket.json()["preview"]["hard_blocks"]
    assert actions.json()["items"][0]["thesis"] == "early signal"
    assert triggers.json()["items"][0]["status"] == "fired"
    assert tickets.json()["items"][0]["submission_allowed"] is False
    telemetry = AuditLogRepository(engine).list_events(ticker="GLW")
    assert [event.event_type for event in telemetry] == [
        "telemetry.operator.opportunity_action.saved",
        "telemetry.operator.trigger.saved",
        "telemetry.operator.triggers.evaluated",
        "telemetry.operator.order_ticket.preview_saved",
    ]
    assert {event.actor_source for event in telemetry} == {"api"}
    assert {event.actor_id for event in telemetry} == {"analyst-1 apikey=<redacted>"}
    assert {event.actor_role for event in telemetry} == {"analyst apikey=<redacted>"}
    assert "hidden-actor-secret" not in str(telemetry)
    assert "hidden-role-secret" not in str(telemetry)
    assert "hidden-action-source" not in str(telemetry[0].metadata)
    assert "hidden-trigger-source" not in str(telemetry[1].metadata)
    assert telemetry[3].metadata["submission_allowed"] is False
    assert not [
        route.path
        for route in create_app().routes
        if "submit" in getattr(route, "path", "") or "place" in getattr(route, "path", "")
    ]


def _blank_schwab_app_env(monkeypatch) -> None:
    for key in (
        "SCHWAB_CLIENT_ID",
        "SCHWAB_CLIENT_SECRET",
        "SCHWAB_REDIRECT_URI",
    ):
        monkeypatch.setenv(key, "")


def _seed_broker_rows(engine) -> None:
    repo = BrokerRepository(engine)
    now = datetime(2026, 5, 12, 14, tzinfo=UTC)
    connection_id = broker_connection_id()
    account_id = broker_account_id("schwab", "account-hash-123")
    repo.upsert_connection(
        BrokerConnection(
            id=connection_id,
            broker="schwab",
            user_id="local",
            status=BrokerConnectionStatus.CONNECTED,
            created_at=now,
            updated_at=now,
            last_successful_sync_at=now,
            metadata={},
        )
    )
    repo.upsert_accounts(
        [
            BrokerAccount(
                id=account_id,
                connection_id=connection_id,
                broker="schwab",
                broker_account_id="12345678",
                account_hash="account-hash-123",
                account_type="MARGIN",
                display_name="MARGIN ending 5678",
                is_active=True,
                created_at=now,
                updated_at=now,
            )
        ]
    )
    repo.upsert_balance_snapshots(
        [
            BrokerBalanceSnapshot(
                id=broker_balance_snapshot_id(account_id, now),
                account_id=account_id,
                as_of=now,
                cash=50000.0,
                buying_power=100000.0,
                liquidation_value=250000.0,
                equity=250000.0,
                raw_payload={},
                created_at=now,
            )
        ]
    )
    repo.upsert_positions(
        [
            BrokerPosition(
                id=broker_position_id(account_id, "GLW", now),
                account_id=account_id,
                as_of=now,
                ticker="GLW",
                quantity=100,
                average_price=80.0,
                market_value=9500.0,
                unrealized_pnl=1500.0,
                sector="technology",
                theme="automation",
                raw_payload={},
                created_at=now,
            )
        ]
    )


def _seed_valid_token(engine) -> None:
    repo = BrokerRepository(engine)
    connection = repo.latest_connection()
    assert connection is not None
    now = datetime.now(UTC)
    repo.upsert_token(
        BrokerToken(
            id=broker_token_id(connection.id),
            connection_id=connection.id,
            access_token_encrypted=TokenCipher("local-dev-key").encrypt("access-token"),
            refresh_token_encrypted=TokenCipher("local-dev-key").encrypt("refresh-token"),
            access_token_expires_at=now + timedelta(hours=1),
            refresh_token_expires_at=now + timedelta(days=1),
            created_at=now,
            updated_at=now,
        )
    )

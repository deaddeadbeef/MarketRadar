from __future__ import annotations

import secrets
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import RedirectResponse

from catalyst_radar.brokers.interactive import (
    create_blocked_order_ticket,
    create_trigger,
    evaluate_triggers,
    market_snapshot_payload,
    normalize_tickers,
    opportunity_action_payload,
    order_ticket_payload,
    record_opportunity_action,
    sync_market_context,
    trigger_payload,
)
from catalyst_radar.brokers.models import (
    BrokerConnection,
    BrokerConnectionStatus,
    BrokerToken,
    broker_connection_id,
    broker_token_id,
)
from catalyst_radar.brokers.order_preview import (
    OrderPreviewRequest,
    build_disabled_order_preview,
)
from catalyst_radar.brokers.portfolio_context import (
    balances_payload,
    exposure_payload,
    latest_broker_portfolio_context,
    open_orders_payload,
    portfolio_snapshot_payload,
    positions_payload,
)
from catalyst_radar.brokers.rate_limit import (
    SCHWAB_MARKET_SYNC_OPERATION,
    SCHWAB_PORTFOLIO_SYNC_OPERATION,
    SchwabRateLimitExceeded,
    acquire_schwab_api_slot,
    schwab_rate_limit_config_payload,
    schwab_rate_limit_status,
)
from catalyst_radar.brokers.schwab import (
    SchwabClient,
    SchwabConfigurationError,
    SchwabOAuthService,
    SchwabOAuthSettings,
)
from catalyst_radar.brokers.sync import sync_schwab_read_only
from catalyst_radar.brokers.tokens import TokenCipher
from catalyst_radar.connectors.http import JsonHttpClient, UrlLibHttpTransport
from catalyst_radar.core.config import AppConfig
from catalyst_radar.security.access import Role, require_role
from catalyst_radar.storage.broker_repositories import BrokerRepository
from catalyst_radar.storage.db import create_schema, engine_from_url
from catalyst_radar.storage.repositories import MarketRepository

router = APIRouter(tags=["brokers"])


def _engine():
    config = AppConfig.from_env()
    engine = engine_from_url(config.database_url)
    create_schema(engine)
    return engine


@router.get("/api/brokers/schwab/connect", dependencies=[Depends(require_role(Role.ANALYST))])
def schwab_connect() -> RedirectResponse:
    config = AppConfig.from_env()
    try:
        settings = SchwabOAuthSettings.from_config(config)
    except SchwabConfigurationError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    now = datetime.now(UTC)
    state = secrets.token_urlsafe(16)
    repo = BrokerRepository(_engine())
    existing = repo.latest_connection()
    repo.upsert_connection(
        BrokerConnection(
            id=broker_connection_id(),
            broker="schwab",
            user_id="local",
            status=BrokerConnectionStatus.NEEDS_AUTH,
            created_at=existing.created_at if existing is not None else now,
            updated_at=now,
            metadata={
                "mode": "read_only",
                "oauth_state": state,
                "oauth_started_at": now.isoformat(),
            },
        )
    )
    url = SchwabOAuthService(settings).authorization_url(state=state)
    return RedirectResponse(url)


@router.get("/api/brokers/schwab/callback", dependencies=[Depends(require_role(Role.ANALYST))])
def schwab_callback(
    code: str | None = None,
    state: str | None = None,
    error: str | None = None,
) -> dict[str, object]:
    if error:
        raise HTTPException(status_code=400, detail=f"Schwab OAuth error: {error}")
    if not code:
        raise HTTPException(status_code=400, detail="Schwab OAuth code is required")
    config = AppConfig.from_env()
    try:
        settings = SchwabOAuthSettings.from_config(config)
        cipher = _token_cipher(config)
    except (SchwabConfigurationError, ValueError) as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    repo = BrokerRepository(_engine())
    existing = repo.latest_connection()
    expected_state = existing.metadata.get("oauth_state") if existing is not None else None
    if not state or not expected_state or state != expected_state:
        raise HTTPException(status_code=400, detail="Schwab OAuth state is invalid or expired")
    token_payload = SchwabOAuthService(
        settings,
        JsonHttpClient(UrlLibHttpTransport(), config.http_timeout_seconds),
    ).exchange_code(code)
    now = datetime.now(UTC)
    access_token = str(token_payload.get("access_token") or "")
    refresh_token = token_payload.get("refresh_token")
    if not access_token:
        raise HTTPException(status_code=502, detail="Schwab token response missing access_token")
    connection_id = broker_connection_id()
    repo.upsert_connection(
        BrokerConnection(
            id=connection_id,
            broker="schwab",
            user_id="local",
            status=BrokerConnectionStatus.CONNECTED,
            created_at=existing.created_at if existing is not None else now,
            updated_at=now,
            metadata={"oauth_state_validated": True, "mode": "read_only"},
        )
    )
    repo.upsert_token(
        BrokerToken(
            id=broker_token_id(connection_id),
            connection_id=connection_id,
            access_token_encrypted=cipher.encrypt(access_token),
            refresh_token_encrypted=(cipher.encrypt(str(refresh_token)) if refresh_token else None),
            access_token_expires_at=now
            + timedelta(seconds=int(token_payload.get("expires_in") or 1800)),
            refresh_token_expires_at=now + timedelta(days=7) if refresh_token else None,
            created_at=now,
            updated_at=now,
        )
    )
    return {"status": "connected", "broker": "schwab", "read_only": True}


@router.get("/api/brokers/schwab/status", dependencies=[Depends(require_role(Role.VIEWER))])
def schwab_status() -> dict[str, object]:
    config = AppConfig.from_env()
    engine = _engine()
    repo = BrokerRepository(engine)
    connection = repo.latest_connection()
    token = repo.latest_token(connection.id) if connection is not None else None
    connected = bool(
        connection
        and token
        and connection.status == BrokerConnectionStatus.CONNECTED
        and token.access_token_expires_at > datetime.now(UTC)
    )
    return {
        "broker": "schwab",
        "configured": all(
            (
                config.schwab_client_id,
                config.schwab_client_secret,
                config.schwab_redirect_uri,
            )
        ),
        "connected": connected,
        "status": connection.status.value if connection is not None else "missing",
        "last_successful_sync_at": (
            connection.last_successful_sync_at.isoformat()
            if connection is not None and connection.last_successful_sync_at is not None
            else None
        ),
        "order_submission_enabled": bool(config.schwab_order_submission_enabled),
        "order_submission_available": False,
        "rate_limits": schwab_rate_limit_status(engine, config=config),
        "rate_limit_config": schwab_rate_limit_config_payload(config),
    }


@router.post(
    "/api/brokers/schwab/disconnect",
    dependencies=[Depends(require_role(Role.ANALYST))],
)
def schwab_disconnect() -> dict[str, object]:
    repo = BrokerRepository(_engine())
    connection = repo.latest_connection()
    if connection is None:
        return {"status": "already_disconnected", "broker": "schwab"}
    repo.mark_connection_disconnected(connection.id, now=datetime.now(UTC))
    return {"status": "disconnected", "broker": "schwab"}


@router.post("/api/brokers/schwab/sync", dependencies=[Depends(require_role(Role.ANALYST))])
def schwab_sync() -> dict[str, object]:
    config = AppConfig.from_env()
    engine = _engine()
    repo = BrokerRepository(engine)
    connection = repo.latest_connection()
    token = repo.latest_token(connection.id) if connection is not None else None
    if connection is None or token is None:
        raise HTTPException(status_code=409, detail="Schwab connection token is missing")
    if connection.status != BrokerConnectionStatus.CONNECTED:
        raise HTTPException(
            status_code=409,
            detail=f"Schwab connection is {connection.status.value}",
        )
    _acquire_schwab_rate_limit_slot(
        engine,
        operation=SCHWAB_PORTFOLIO_SYNC_OPERATION,
        min_interval_seconds=config.schwab_sync_min_interval_seconds,
        metadata={"endpoint": "/api/brokers/schwab/sync"},
    )
    try:
        access_token = _active_access_token(config, repo, connection, token)
    except ValueError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    client = SchwabClient(
        client=JsonHttpClient(UrlLibHttpTransport(), config.http_timeout_seconds),
        access_token=access_token,
        base_url=config.schwab_base_url,
    )
    result = sync_schwab_read_only(
        client=client,
        broker_repo=BrokerRepository(engine),
        market_repo=MarketRepository(engine),
    )
    return {
        "status": result.status.value,
        "account_count": result.account_count,
        "balance_count": result.balance_count,
        "position_count": result.position_count,
        "open_order_count": result.open_order_count,
        "synced_at": result.synced_at.isoformat(),
    }


@router.post(
    "/api/brokers/schwab/market-sync",
    dependencies=[Depends(require_role(Role.ANALYST))],
)
def schwab_market_sync(payload: dict[str, Any] | None = None) -> dict[str, object]:
    body = payload or {}
    tickers = normalize_tickers(body.get("tickers") or body.get("ticker") or [])
    if not tickers:
        raise HTTPException(status_code=400, detail="At least one ticker is required")
    config = AppConfig.from_env()
    if len(tickers) > config.schwab_market_sync_max_tickers:
        raise HTTPException(
            status_code=400,
            detail=(
                "Too many Schwab market-sync tickers; maximum is "
                f"{config.schwab_market_sync_max_tickers}"
            ),
        )
    engine = _engine()
    repo = BrokerRepository(engine)
    connection = repo.latest_connection()
    token = repo.latest_token(connection.id) if connection is not None else None
    if connection is None or token is None:
        raise HTTPException(status_code=409, detail="Schwab connection token is missing")
    if connection.status != BrokerConnectionStatus.CONNECTED:
        raise HTTPException(
            status_code=409,
            detail=f"Schwab connection is {connection.status.value}",
        )
    _acquire_schwab_rate_limit_slot(
        engine,
        operation=SCHWAB_MARKET_SYNC_OPERATION,
        min_interval_seconds=config.schwab_market_sync_min_interval_seconds,
        metadata={"endpoint": "/api/brokers/schwab/market-sync", "tickers": tickers},
    )
    access_token = _active_access_token(config, repo, connection, token)
    client = SchwabClient(
        client=JsonHttpClient(UrlLibHttpTransport(), config.http_timeout_seconds),
        access_token=access_token,
        base_url=config.schwab_base_url,
    )
    snapshots = sync_market_context(
        client=client,
        repo=repo,
        tickers=tickers,
        include_history=bool(body.get("include_history", True)),
        include_options=bool(body.get("include_options", True)),
    )
    return {"items": [market_snapshot_payload(row) for row in snapshots]}


@router.get("/api/market/context", dependencies=[Depends(require_role(Role.VIEWER))])
def market_context(ticker: str | None = None) -> dict[str, object]:
    repo = BrokerRepository(_engine())
    tickers = normalize_tickers(ticker or [])
    return {
        "items": [
            market_snapshot_payload(row)
            for row in repo.latest_market_snapshots(tickers=tickers or None)
        ]
    }


@router.post("/api/opportunities/actions", dependencies=[Depends(require_role(Role.ANALYST))])
def opportunity_action(payload: dict[str, Any]) -> dict[str, object]:
    ticker = str(payload.get("ticker") or "").strip().upper()
    action = str(payload.get("action") or "").strip()
    if not ticker or not action:
        raise HTTPException(status_code=400, detail="ticker and action are required")
    row = record_opportunity_action(
        repo=BrokerRepository(_engine()),
        ticker=ticker,
        action=action,
        thesis=payload.get("thesis"),
        notes=payload.get("notes"),
        payload={key: value for key, value in payload.items() if key not in {"thesis", "notes"}},
    )
    return opportunity_action_payload(row)


@router.get("/api/opportunities/actions", dependencies=[Depends(require_role(Role.VIEWER))])
def opportunity_actions(ticker: str | None = None) -> dict[str, object]:
    repo = BrokerRepository(_engine())
    return {
        "items": [
            opportunity_action_payload(row) for row in repo.list_opportunity_actions(ticker=ticker)
        ]
    }


@router.post("/api/market/triggers", dependencies=[Depends(require_role(Role.ANALYST))])
def market_trigger(payload: dict[str, Any]) -> dict[str, object]:
    required = ("ticker", "trigger_type", "operator", "threshold")
    missing = [name for name in required if payload.get(name) in (None, "")]
    if missing:
        raise HTTPException(status_code=400, detail=f"missing fields: {', '.join(missing)}")
    row = create_trigger(
        repo=BrokerRepository(_engine()),
        ticker=str(payload["ticker"]),
        trigger_type=str(payload["trigger_type"]),
        operator=str(payload["operator"]),
        threshold=float(payload["threshold"]),
        notes=payload.get("notes"),
        payload={key: value for key, value in payload.items() if key != "notes"},
    )
    return trigger_payload(row)


@router.post(
    "/api/market/triggers/evaluate",
    dependencies=[Depends(require_role(Role.ANALYST))],
)
def market_triggers_evaluate(payload: dict[str, Any] | None = None) -> dict[str, object]:
    body = payload or {}
    rows = evaluate_triggers(
        repo=BrokerRepository(_engine()),
        tickers=body.get("tickers") or body.get("ticker") or [],
    )
    return {"items": [trigger_payload(row) for row in rows]}


@router.get("/api/market/triggers", dependencies=[Depends(require_role(Role.VIEWER))])
def market_triggers(ticker: str | None = None) -> dict[str, object]:
    repo = BrokerRepository(_engine())
    return {"items": [trigger_payload(row) for row in repo.list_triggers(ticker=ticker)]}


@router.get("/api/portfolio/snapshot", dependencies=[Depends(require_role(Role.VIEWER))])
def portfolio_snapshot() -> dict[str, object]:
    return portfolio_snapshot_payload(_engine())


@router.get("/api/portfolio/positions", dependencies=[Depends(require_role(Role.VIEWER))])
def portfolio_positions() -> dict[str, object]:
    return {"items": positions_payload(_engine())}


@router.get("/api/portfolio/balances", dependencies=[Depends(require_role(Role.VIEWER))])
def portfolio_balances() -> dict[str, object]:
    return {"items": balances_payload(_engine())}


@router.get("/api/portfolio/open-orders", dependencies=[Depends(require_role(Role.VIEWER))])
def portfolio_open_orders() -> dict[str, object]:
    return {"items": open_orders_payload(_engine())}


@router.get("/api/portfolio/exposure", dependencies=[Depends(require_role(Role.VIEWER))])
def portfolio_exposure() -> dict[str, object]:
    return exposure_payload(_engine())


@router.post("/api/orders/preview", dependencies=[Depends(require_role(Role.ANALYST))])
def order_preview(payload: dict[str, Any]) -> dict[str, object]:
    config = AppConfig.from_env()
    request = OrderPreviewRequest(
        ticker=str(payload.get("ticker") or ""),
        side=str(payload.get("side") or "buy"),
        entry_price=float(payload.get("entry_price") or 0.0),
        invalidation_price=float(payload.get("invalidation_price") or 0.0),
        risk_per_trade_pct=float(payload.get("risk_per_trade_pct") or config.risk_per_trade_pct),
        account_id=payload.get("account_id"),
    )
    context = latest_broker_portfolio_context(_engine(), ticker=request.ticker, config=config)
    return build_disabled_order_preview(request, portfolio_context=context, config=config)


@router.post("/api/orders/tickets", dependencies=[Depends(require_role(Role.ANALYST))])
def order_ticket(payload: dict[str, Any]) -> dict[str, object]:
    required = ("ticker", "side", "entry_price", "invalidation_price")
    missing = [name for name in required if payload.get(name) in (None, "")]
    if missing:
        raise HTTPException(status_code=400, detail=f"missing fields: {', '.join(missing)}")
    row = create_blocked_order_ticket(
        repo=BrokerRepository(_engine()),
        ticker=str(payload["ticker"]),
        side=str(payload["side"]),
        entry_price=float(payload["entry_price"]),
        invalidation_price=float(payload["invalidation_price"]),
        risk_per_trade_pct=(
            float(payload["risk_per_trade_pct"])
            if payload.get("risk_per_trade_pct") not in (None, "")
            else None
        ),
        account_id=payload.get("account_id"),
        notes=payload.get("notes"),
        config=AppConfig.from_env(),
    )
    return order_ticket_payload(row)


@router.get("/api/orders/tickets", dependencies=[Depends(require_role(Role.VIEWER))])
def order_tickets(ticker: str | None = None) -> dict[str, object]:
    repo = BrokerRepository(_engine())
    return {"items": [order_ticket_payload(row) for row in repo.list_order_tickets(ticker=ticker)]}


def _token_cipher(config: AppConfig) -> TokenCipher:
    if not config.broker_token_encryption_key:
        msg = "BROKER_TOKEN_ENCRYPTION_KEY is required"
        raise ValueError(msg)
    return TokenCipher(config.broker_token_encryption_key)


def _acquire_schwab_rate_limit_slot(
    engine,
    *,
    operation: str,
    min_interval_seconds: int,
    metadata: dict[str, Any],
) -> None:
    try:
        acquire_schwab_api_slot(
            engine,
            operation=operation,
            min_interval_seconds=min_interval_seconds,
            metadata=metadata,
        )
    except SchwabRateLimitExceeded as exc:
        raise HTTPException(
            status_code=429,
            detail=exc.state.as_payload(),
            headers={"Retry-After": str(exc.state.retry_after_seconds)},
        ) from exc


def _active_access_token(
    config: AppConfig,
    repo: BrokerRepository,
    connection: BrokerConnection,
    token: BrokerToken,
) -> str:
    now = datetime.now(UTC)
    cipher = _token_cipher(config)
    if token.access_token_expires_at > now + timedelta(seconds=60):
        return cipher.decrypt(token.access_token_encrypted)
    if not token.refresh_token_encrypted:
        _mark_needs_auth(repo, connection, now, "access_token_expired")
        raise HTTPException(
            status_code=409,
            detail="Schwab access token expired and refresh token is missing",
        )
    if token.refresh_token_expires_at is not None and token.refresh_token_expires_at <= now:
        _mark_needs_auth(repo, connection, now, "refresh_token_expired")
        raise HTTPException(status_code=409, detail="Schwab refresh token expired")
    try:
        settings = SchwabOAuthSettings.from_config(config)
    except SchwabConfigurationError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    refresh_token = cipher.decrypt(token.refresh_token_encrypted)
    try:
        token_payload = SchwabOAuthService(
            settings,
            JsonHttpClient(UrlLibHttpTransport(), config.http_timeout_seconds),
        ).refresh_access_token(refresh_token)
    except RuntimeError as exc:
        _mark_needs_auth(repo, connection, now, "refresh_failed")
        raise HTTPException(
            status_code=409,
            detail=f"Schwab token refresh failed: {exc}",
        ) from exc
    access_token = str(token_payload.get("access_token") or "")
    if not access_token:
        raise HTTPException(status_code=502, detail="Schwab refresh response missing access_token")
    new_refresh_token = str(token_payload.get("refresh_token") or refresh_token)
    repo.upsert_token(
        BrokerToken(
            id=broker_token_id(connection.id),
            connection_id=connection.id,
            access_token_encrypted=cipher.encrypt(access_token),
            refresh_token_encrypted=cipher.encrypt(new_refresh_token),
            access_token_expires_at=now
            + timedelta(seconds=int(token_payload.get("expires_in") or 1800)),
            refresh_token_expires_at=token.refresh_token_expires_at,
            created_at=token.created_at,
            updated_at=now,
        )
    )
    return access_token


def _mark_needs_auth(
    repo: BrokerRepository,
    connection: BrokerConnection,
    now: datetime,
    reason: str,
) -> None:
    repo.upsert_connection(
        BrokerConnection(
            id=connection.id,
            broker=connection.broker,
            user_id=connection.user_id,
            status=BrokerConnectionStatus.NEEDS_AUTH,
            created_at=connection.created_at,
            updated_at=now,
            last_successful_sync_at=connection.last_successful_sync_at,
            metadata={"mode": "read_only", "reason": reason},
        )
    )

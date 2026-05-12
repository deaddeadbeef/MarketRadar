from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from math import isfinite
from typing import Any

from catalyst_radar.brokers.models import (
    BrokerMarketSnapshot,
    BrokerOpportunityAction,
    BrokerOpportunityActionType,
    BrokerOrderTicket,
    BrokerOrderTicketStatus,
    BrokerTrigger,
    BrokerTriggerStatus,
    broker_market_snapshot_id,
    broker_opportunity_action_id,
    broker_order_ticket_id,
    broker_trigger_id,
)
from catalyst_radar.brokers.order_preview import (
    OrderPreviewRequest,
    build_disabled_order_preview,
)
from catalyst_radar.brokers.portfolio_context import latest_broker_portfolio_context
from catalyst_radar.brokers.schwab import SchwabClient
from catalyst_radar.core.config import AppConfig
from catalyst_radar.storage.broker_repositories import BrokerRepository


def normalize_tickers(values: Sequence[str] | str) -> list[str]:
    raw_values = values.split(",") if isinstance(values, str) else list(values)
    return sorted({str(value).strip().upper() for value in raw_values if str(value).strip()})


def record_opportunity_action(
    *,
    repo: BrokerRepository,
    ticker: str,
    action: str,
    thesis: str | None = None,
    notes: str | None = None,
    payload: Mapping[str, Any] | None = None,
    now: datetime | None = None,
) -> BrokerOpportunityAction:
    timestamp = _now(now)
    action_type = BrokerOpportunityActionType(action)
    status = "dismissed" if action_type == BrokerOpportunityActionType.DISMISS else "active"
    row = BrokerOpportunityAction(
        id=broker_opportunity_action_id(ticker, action_type.value, timestamp),
        ticker=ticker,
        action=action_type,
        status=status,
        thesis=thesis,
        notes=notes,
        payload=dict(payload or {}),
        created_at=timestamp,
        updated_at=timestamp,
    )
    return repo.upsert_opportunity_action(row)


def sync_market_context(
    *,
    client: SchwabClient,
    repo: BrokerRepository,
    tickers: Sequence[str] | str,
    now: datetime | None = None,
    include_history: bool = True,
    include_options: bool = True,
) -> list[BrokerMarketSnapshot]:
    timestamp = _now(now)
    symbols = normalize_tickers(tickers)
    if not symbols:
        return []
    quotes_payload = client.get_quotes(symbols)
    snapshots = [
        _snapshot_from_schwab(
            ticker=symbol,
            quote_payload=_mapping(quotes_payload.get(symbol)),
            history_payload=_optional_call(
                client.get_price_history,
                symbol,
                enabled=include_history,
            ),
            options_payload=_optional_call(
                client.get_option_chain,
                symbol,
                enabled=include_options,
            ),
            synced_at=timestamp,
        )
        for symbol in symbols
    ]
    repo.upsert_market_snapshots(snapshots)
    return snapshots


def create_trigger(
    *,
    repo: BrokerRepository,
    ticker: str,
    trigger_type: str,
    operator: str,
    threshold: float,
    notes: str | None = None,
    payload: Mapping[str, Any] | None = None,
    now: datetime | None = None,
) -> BrokerTrigger:
    timestamp = _now(now)
    trigger = BrokerTrigger(
        id=broker_trigger_id(ticker, trigger_type, float(threshold), timestamp),
        ticker=ticker,
        trigger_type=trigger_type,
        operator=operator,
        threshold=float(threshold),
        latest_value=None,
        status=BrokerTriggerStatus.ACTIVE,
        notes=notes,
        payload=dict(payload or {}),
        created_at=timestamp,
        updated_at=timestamp,
    )
    return repo.upsert_trigger(trigger)


def evaluate_triggers(
    *,
    repo: BrokerRepository,
    tickers: Sequence[str] | str | None = None,
    now: datetime | None = None,
) -> list[BrokerTrigger]:
    timestamp = _now(now)
    requested = set(normalize_tickers(tickers or []))
    rows = repo.list_triggers(active_only=False)
    evaluated: list[BrokerTrigger] = []
    for trigger in rows:
        if requested and trigger.ticker not in requested:
            continue
        snapshot = repo.latest_market_snapshot(trigger.ticker)
        latest_value = _trigger_value(trigger.trigger_type, snapshot)
        fired = (
            latest_value is not None
            and trigger.status == BrokerTriggerStatus.ACTIVE
            and _compare(latest_value, trigger.operator, trigger.threshold)
        )
        updated = BrokerTrigger(
            id=trigger.id,
            ticker=trigger.ticker,
            trigger_type=trigger.trigger_type,
            operator=trigger.operator,
            threshold=trigger.threshold,
            latest_value=latest_value,
            status=BrokerTriggerStatus.FIRED if fired else trigger.status,
            notes=trigger.notes,
            payload=trigger.payload,
            created_at=trigger.created_at,
            updated_at=timestamp,
            fired_at=timestamp if fired else trigger.fired_at,
        )
        evaluated.append(repo.upsert_trigger(updated))
    return evaluated


def create_blocked_order_ticket(
    *,
    repo: BrokerRepository,
    ticker: str,
    side: str,
    entry_price: float,
    invalidation_price: float,
    config: AppConfig,
    account_id: str | None = None,
    risk_per_trade_pct: float | None = None,
    notes: str | None = None,
    now: datetime | None = None,
) -> BrokerOrderTicket:
    timestamp = _now(now)
    request = OrderPreviewRequest(
        ticker=ticker,
        side=side,
        entry_price=float(entry_price),
        invalidation_price=float(invalidation_price),
        risk_per_trade_pct=float(risk_per_trade_pct or config.risk_per_trade_pct),
        account_id=account_id,
    )
    preview = build_disabled_order_preview(
        request,
        portfolio_context=latest_broker_portfolio_context(
            repo.engine,
            ticker=request.ticker,
            config=config,
        ),
        config=config,
    )
    ticket = BrokerOrderTicket(
        id=broker_order_ticket_id(request.ticker, timestamp),
        ticker=request.ticker,
        side=request.side,
        quantity=float(preview.get("proposed_shares") or 0.0),
        limit_price=float(preview.get("entry_price") or 0.0),
        invalidation_price=float(preview.get("invalidation_price") or 0.0),
        risk_budget=float(preview.get("risk_budget_dollars") or 0.0),
        status=BrokerOrderTicketStatus.BLOCKED,
        submission_allowed=False,
        notes=notes,
        preview_payload=preview,
        created_at=timestamp,
        updated_at=timestamp,
    )
    return repo.upsert_order_ticket(ticket)


def market_snapshot_payload(row: BrokerMarketSnapshot) -> dict[str, object]:
    return {
        "id": row.id,
        "ticker": row.ticker,
        "as_of": row.as_of.isoformat(),
        "last_price": row.last_price,
        "bid_price": row.bid_price,
        "ask_price": row.ask_price,
        "mark_price": row.mark_price,
        "day_change_percent": row.day_change_percent,
        "total_volume": row.total_volume,
        "relative_volume": row.relative_volume,
        "high_52_week": row.high_52_week,
        "low_52_week": row.low_52_week,
        "price_trend_5d_percent": row.price_trend_5d_percent,
        "option_call_put_ratio": row.option_call_put_ratio,
        "option_iv_percentile": row.option_iv_percentile,
        "created_at": row.created_at.isoformat(),
    }


def opportunity_action_payload(row: BrokerOpportunityAction) -> dict[str, object]:
    return {
        "id": row.id,
        "ticker": row.ticker,
        "action": row.action.value,
        "status": row.status,
        "thesis": row.thesis,
        "notes": row.notes,
        "created_at": row.created_at.isoformat(),
        "updated_at": row.updated_at.isoformat(),
    }


def trigger_payload(row: BrokerTrigger) -> dict[str, object]:
    return {
        "id": row.id,
        "ticker": row.ticker,
        "trigger_type": row.trigger_type,
        "operator": row.operator,
        "threshold": row.threshold,
        "latest_value": row.latest_value,
        "status": row.status.value,
        "notes": row.notes,
        "created_at": row.created_at.isoformat(),
        "updated_at": row.updated_at.isoformat(),
        "fired_at": row.fired_at.isoformat() if row.fired_at is not None else None,
    }


def order_ticket_payload(row: BrokerOrderTicket) -> dict[str, object]:
    return {
        "id": row.id,
        "ticker": row.ticker,
        "side": row.side,
        "quantity": row.quantity,
        "limit_price": row.limit_price,
        "stop_price": row.stop_price,
        "invalidation_price": row.invalidation_price,
        "risk_budget": row.risk_budget,
        "status": row.status.value,
        "submission_allowed": row.submission_allowed,
        "notes": row.notes,
        "created_at": row.created_at.isoformat(),
        "updated_at": row.updated_at.isoformat(),
        "preview": dict(row.preview_payload),
    }


def _snapshot_from_schwab(
    *,
    ticker: str,
    quote_payload: Mapping[str, Any],
    history_payload: Mapping[str, Any],
    options_payload: Mapping[str, Any],
    synced_at: datetime,
) -> BrokerMarketSnapshot:
    quote = _merged_quote(quote_payload)
    trend, relative_volume = _history_metrics(history_payload, quote.get("totalVolume"))
    call_put_ratio, iv_percentile = _option_metrics(options_payload)
    return BrokerMarketSnapshot(
        id=broker_market_snapshot_id(ticker, synced_at),
        ticker=ticker,
        as_of=synced_at,
        last_price=_first_float(
            quote.get("lastPrice"),
            quote.get("regularMarketLastPrice"),
            quote.get("mark"),
            quote.get("closePrice"),
        ),
        bid_price=_first_float(quote.get("bidPrice"), quote.get("bid")),
        ask_price=_first_float(quote.get("askPrice"), quote.get("ask")),
        mark_price=_first_float(quote.get("mark"), quote.get("markPrice")),
        day_change_percent=_first_float(
            quote.get("netPercentChange"),
            quote.get("regularMarketPercentChange"),
        ),
        total_volume=_first_float(quote.get("totalVolume"), quote.get("volume")),
        relative_volume=relative_volume,
        high_52_week=_first_float(quote.get("52WeekHigh"), quote.get("high52")),
        low_52_week=_first_float(quote.get("52WeekLow"), quote.get("low52")),
        price_trend_5d_percent=trend,
        option_call_put_ratio=call_put_ratio,
        option_iv_percentile=iv_percentile,
        raw_payload={
            "quote": dict(quote_payload),
            "history": dict(history_payload),
            "options": dict(options_payload),
        },
        created_at=synced_at,
    )


def _merged_quote(payload: Mapping[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = dict(payload)
    for key in ("quote", "regular", "fundamental"):
        value = payload.get(key)
        if isinstance(value, Mapping):
            merged.update(value)
    return merged


def _history_metrics(
    payload: Mapping[str, Any],
    quote_volume: object,
) -> tuple[float | None, float | None]:
    candles = [row for row in payload.get("candles", []) if isinstance(row, Mapping)]
    closes = [_first_float(row.get("close")) for row in candles]
    closes = [value for value in closes if value is not None and value > 0]
    trend = None
    if len(closes) >= 2 and closes[0] > 0:
        trend = ((closes[-1] - closes[0]) / closes[0]) * 100
    volumes = [_first_float(row.get("volume")) for row in candles[:-1] or candles]
    volumes = [value for value in volumes if value is not None and value > 0]
    total_volume = _first_float(quote_volume)
    relative_volume = None
    if total_volume is not None and volumes:
        average_volume = sum(volumes) / len(volumes)
        if average_volume > 0:
            relative_volume = total_volume / average_volume
    return trend, relative_volume


def _option_metrics(payload: Mapping[str, Any]) -> tuple[float | None, float | None]:
    call_values = _option_side_values(_mapping(payload.get("callExpDateMap")))
    put_values = _option_side_values(_mapping(payload.get("putExpDateMap")))
    call_interest = sum(call_values["activity"])
    put_interest = sum(put_values["activity"])
    ratio = None
    if put_interest > 0:
        ratio = call_interest / put_interest
    elif call_interest > 0:
        ratio = call_interest
    vol_values = [*call_values["volatility"], *put_values["volatility"]]
    iv = (sum(vol_values) / len(vol_values)) if vol_values else None
    return ratio, iv


def _option_side_values(payload: Mapping[str, Any]) -> dict[str, list[float]]:
    values = {"activity": [], "volatility": []}
    for expirations in payload.values():
        if not isinstance(expirations, Mapping):
            continue
        for contracts in expirations.values():
            if not isinstance(contracts, list):
                continue
            for contract in contracts:
                if not isinstance(contract, Mapping):
                    continue
                activity = _first_float(
                    contract.get("totalVolume"),
                    contract.get("volume"),
                    contract.get("openInterest"),
                )
                volatility = _first_float(
                    contract.get("volatility"),
                    contract.get("theoreticalVolatility"),
                    contract.get("iv"),
                )
                if activity is not None:
                    values["activity"].append(activity)
                if volatility is not None:
                    values["volatility"].append(volatility)
    return values


def _trigger_value(
    trigger_type: str,
    snapshot: BrokerMarketSnapshot | None,
) -> float | None:
    if snapshot is None:
        return None
    field_by_type = {
        "price": snapshot.last_price or snapshot.mark_price,
        "price_above": snapshot.last_price or snapshot.mark_price,
        "price_below": snapshot.last_price or snapshot.mark_price,
        "volume": snapshot.total_volume,
        "volume_above": snapshot.total_volume,
        "relative_volume": snapshot.relative_volume,
        "relative_volume_above": snapshot.relative_volume,
        "call_put_ratio": snapshot.option_call_put_ratio,
        "call_put_ratio_above": snapshot.option_call_put_ratio,
    }
    return field_by_type.get(trigger_type)


def _compare(value: float, operator: str, threshold: float) -> bool:
    normalized = operator.strip().lower()
    if normalized in {"gte", ">=", "above_or_equal"}:
        return value >= threshold
    if normalized in {"gt", ">", "above"}:
        return value > threshold
    if normalized in {"lte", "<=", "below_or_equal"}:
        return value <= threshold
    if normalized in {"lt", "<", "below"}:
        return value < threshold
    msg = f"unsupported trigger operator: {operator}"
    raise ValueError(msg)


def _optional_call(method, symbol: str, *, enabled: bool) -> Mapping[str, Any]:
    if not enabled:
        return {}
    try:
        return _mapping(method(symbol))
    except RuntimeError as exc:
        return {"error": str(exc)}


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _first_float(*values: object) -> float | None:
    for value in values:
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if isfinite(number):
            return number
    return None


def _now(value: datetime | None) -> datetime:
    timestamp = value or datetime.now(UTC)
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        return timestamp.replace(tzinfo=UTC)
    return timestamp.astimezone(UTC)

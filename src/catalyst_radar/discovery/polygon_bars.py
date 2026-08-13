"""Mapped-ticker Polygon daily bars. Explicit confirm; never grouped-daily."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta
from time import sleep
from typing import Any

from catalyst_radar.connectors.http import JsonHttpClient, UrlLibHttpTransport
from catalyst_radar.core.models import DailyBar
from catalyst_radar.discovery.brief import build_discovery_brief
from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.repositories import MarketRepository

POLYGON_BARS_SCHEMA = "discovery-polygon-bars-v1"
DEFAULT_LOOKBACK_DAYS = 21


def mapped_tickers_from_events(
    events_path: str,
    *,
    theme_peers_path: str | None = None,
    include_theme_expansion: bool = False,
) -> list[str]:
    if include_theme_expansion:
        brief = build_discovery_brief(
            events_path=events_path,
            theme_peers_path=theme_peers_path,
            engine=None,
            limit=200,
        )
        tickers: list[str] = []
        for row in brief.get("discoveries") or []:
            if not isinstance(row, dict):
                continue
            ticker = str(row.get("ticker") or "").strip().upper()
            if ticker and ticker not in tickers:
                tickers.append(ticker)
    else:
        from catalyst_radar.discovery.brief import load_world_events

        bundle = load_world_events(events_path)
        tickers = []
        for event in bundle.events:
            for ticker in [*event.tickers, *event.secondary_tickers]:
                if ticker not in tickers:
                    tickers.append(ticker)
    if "SPY" not in tickers:
        tickers.append("SPY")
    return tickers


def fetch_polygon_daily_bars(
    *,
    api_key: str,
    tickers: Sequence[str],
    start: date,
    end: date,
    client: JsonHttpClient | None = None,
    confirm_external_call: bool = False,
    base_url: str = "https://api.polygon.io",
) -> dict[str, object]:
    if not confirm_external_call:
        return {
            "schema_version": POLYGON_BARS_SCHEMA,
            "status": "blocked_missing_confirm_external_call",
            "tickers": list(tickers),
            "start": start.isoformat(),
            "end": end.isoformat(),
            "external_calls_made": 0,
            "db_writes_made": 0,
            "next_action": "Re-run with --confirm-external-call to fetch mapped ticker bars.",
        }
    key = str(api_key or "").strip()
    if not key:
        raise ValueError("missing CATALYST_POLYGON_API_KEY")
    http = client or JsonHttpClient(UrlLibHttpTransport(), timeout_seconds=20.0)
    bars: list[DailyBar] = []
    errors: list[str] = []
    calls = 0
    for ticker in tickers:
        symbol = str(ticker).strip().upper()
        if not symbol:
            continue
        url = (
            f"{base_url.rstrip('/')}/v2/aggs/ticker/{symbol}/range/1/day/"
            f"{start.isoformat()}/{end.isoformat()}"
            f"?adjusted=true&sort=asc&limit=120&apiKey={key}"
        )
        calls += 1
        try:
            payload = http.get_json(url)
        except Exception as exc:  # noqa: BLE001
            message = str(exc)
            if "429" in message:
                sleep(12.0)
                try:
                    payload = http.get_json(url)
                    calls += 1
                except Exception as retry_exc:  # noqa: BLE001
                    errors.append(f"{symbol}: {retry_exc}")
                    continue
            else:
                errors.append(f"{symbol}: {exc}")
                continue
        sleep(1.1)
        status = str(payload.get("status") or "")
        if status not in {"OK", "DELAYED"}:
            errors.append(f"{symbol}: status={status}")
            continue
        for item in payload.get("results") or []:
            if not isinstance(item, dict):
                continue
            bars.append(_agg_to_bar(symbol, item))
    return {
        "schema_version": POLYGON_BARS_SCHEMA,
        "status": "fetched",
        "tickers": [str(t).upper() for t in tickers if str(t).strip()],
        "start": start.isoformat(),
        "end": end.isoformat(),
        "bar_count": len(bars),
        "external_calls_made": calls,
        "errors": errors[:20],
        "bars": bars,
    }


def write_polygon_bars(
    *,
    engine,
    api_key: str,
    tickers: Sequence[str],
    start: date,
    end: date,
    client: JsonHttpClient | None = None,
    confirm_external_call: bool = False,
    execute: bool = False,
) -> dict[str, object]:
    fetched = fetch_polygon_daily_bars(
        api_key=api_key,
        tickers=tickers,
        start=start,
        end=end,
        client=client,
        confirm_external_call=confirm_external_call,
    )
    if fetched.get("status") != "fetched":
        return fetched
    bars = list(fetched.pop("bars") or [])
    fetched["db_writes_required"] = len(bars)
    fetched["db_writes_made"] = 0
    fetched["investment_advice"] = False
    if not execute:
        fetched["status"] = "preview"
        fetched["next_action"] = (
            f"Fetched {len(bars)} bars in memory. Re-run with --execute to write them."
        )
        return fetched
    create_schema(engine)
    if bars:
        MarketRepository(engine).upsert_daily_bars(bars)
    fetched["status"] = "executed"
    fetched["db_writes_made"] = len(bars)
    fetched["next_action"] = "Bars written. Run discovery-brief / assert-discovery-ready."
    return fetched


def default_bar_window(
    *,
    end: date | None = None,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
) -> tuple[date, date]:
    last = end or datetime.now(tz=UTC).date()
    start = last - timedelta(days=max(5, lookback_days))
    return start, last


def _agg_to_bar(ticker: str, item: dict[str, Any]) -> DailyBar:
    millis = int(item.get("t") or 0)
    day = datetime.fromtimestamp(millis / 1000, tz=UTC).date()
    close = float(item.get("c") or 0.0)
    open_px = float(item.get("o") or close)
    high = float(item.get("h") or max(open_px, close))
    low = float(item.get("l") or min(open_px, close))
    volume = int(item.get("v") or 0)
    vwap = float(item.get("vw") or close)
    stamp = datetime(day.year, day.month, day.day, 21, tzinfo=UTC)
    return DailyBar(
        ticker=ticker,
        date=day,
        open=open_px,
        high=high,
        low=low,
        close=close,
        volume=volume,
        vwap=vwap,
        adjusted=True,
        provider="polygon",
        source_ts=stamp,
        available_at=stamp,
    )

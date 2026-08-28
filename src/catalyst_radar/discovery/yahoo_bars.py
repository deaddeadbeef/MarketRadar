"""Mapped-ticker Yahoo daily bars. No API key; explicit confirm like Polygon."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from time import sleep
from typing import Any
from urllib.parse import quote

from catalyst_radar.connectors.http import JsonHttpClient, UrlLibHttpTransport
from catalyst_radar.core.models import DailyBar
from catalyst_radar.discovery.polygon_bars import default_bar_window, mapped_tickers_from_events
from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.repositories import MarketRepository

YAHOO_BARS_SCHEMA = "discovery-yahoo-bars-v1"
YAHOO_BASE_URL = "https://query1.finance.yahoo.com"
YAHOO_PROVIDER = "yahoo"
DEFAULT_PAUSE_SECONDS = 0.25
DEFAULT_TIMEOUT_SECONDS = 20.0
YAHOO_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; MarketRadar/0.1; research-only daily bars)",
    "Accept": "application/json,text/plain,*/*",
}

__all__ = [
    "DEFAULT_PAUSE_SECONDS",
    "YAHOO_BARS_SCHEMA",
    "YAHOO_BASE_URL",
    "YAHOO_PROVIDER",
    "default_bar_window",
    "fetch_yahoo_daily_bars",
    "mapped_tickers_from_events",
    "parse_yahoo_chart",
    "write_yahoo_bars",
    "yahoo_chart_url",
    "yahoo_symbol",
]


def yahoo_symbol(ticker: str) -> str:
    raw = str(ticker or "").strip().upper()
    if not raw:
        return ""
    return raw.replace(".", "-")


def yahoo_chart_url(
    ticker: str,
    start: date,
    end: date,
    *,
    base_url: str = YAHOO_BASE_URL,
) -> str:
    symbol = quote(yahoo_symbol(ticker))
    period1 = int(datetime(start.year, start.month, start.day, tzinfo=UTC).timestamp())
    period2 = int(
        (datetime(end.year, end.month, end.day, tzinfo=UTC) + timedelta(days=1)).timestamp()
    )
    return (
        f"{base_url.rstrip('/')}/v8/finance/chart/{symbol}"
        f"?interval=1d&period1={period1}&period2={period2}"
        "&events=div%2Csplits&includeAdjustedClose=true"
    )


def parse_yahoo_chart(ticker: str, payload: Mapping[str, Any], *, start: date, end: date) -> list[DailyBar]:
    symbol = str(ticker or "").strip().upper()
    chart = payload.get("chart") if isinstance(payload, dict) else None
    if not isinstance(chart, dict):
        raise RuntimeError("Yahoo payload missing chart object")
    error = chart.get("error")
    if error:
        raise RuntimeError(f"Yahoo chart error: {error}")
    results = chart.get("result") or []
    if not results or not isinstance(results[0], dict):
        return []
    result = results[0]
    stamps = result.get("timestamp") or []
    indicators = result.get("indicators") if isinstance(result.get("indicators"), dict) else {}
    quotes = (indicators.get("quote") or [{}])[0] if indicators else {}
    adj_rows = (indicators.get("adjclose") or [{}])[0] if indicators else {}
    opens = list(quotes.get("open") or [])
    highs = list(quotes.get("high") or [])
    lows = list(quotes.get("low") or [])
    closes = list(quotes.get("close") or [])
    volumes = list(quotes.get("volume") or [])
    adj_closes = list(adj_rows.get("adjclose") or [])
    bars: list[DailyBar] = []
    for index, stamp in enumerate(stamps):
        try:
            day = datetime.fromtimestamp(int(stamp), tz=UTC).date()
        except (TypeError, ValueError, OSError):
            continue
        if day < start or day > end:
            continue
        close = _nth_float(adj_closes, index)
        if close is None:
            close = _nth_float(closes, index)
        if close is None:
            continue
        open_px = _nth_float(opens, index)
        high = _nth_float(highs, index)
        low = _nth_float(lows, index)
        if open_px is None:
            open_px = close
        if high is None:
            high = max(open_px, close)
        if low is None:
            low = min(open_px, close)
        volume = _nth_int(volumes, index)
        available = datetime(day.year, day.month, day.day, 21, tzinfo=UTC)
        bars.append(
            DailyBar(
                ticker=symbol,
                date=day,
                open=open_px,
                high=high,
                low=low,
                close=close,
                volume=volume,
                vwap=close,
                adjusted=True,
                provider=YAHOO_PROVIDER,
                source_ts=available,
                available_at=available,
            )
        )
    bars.sort(key=lambda row: row.date)
    return bars


def fetch_yahoo_daily_bars(
    *,
    tickers: Sequence[str],
    start: date,
    end: date,
    client: JsonHttpClient | None = None,
    confirm_external_call: bool = False,
    base_url: str = YAHOO_BASE_URL,
    pause_seconds: float = DEFAULT_PAUSE_SECONDS,
) -> dict[str, Any]:
    symbols = [str(item).strip().upper() for item in tickers if str(item).strip()]
    if not confirm_external_call:
        return {
            "schema_version": YAHOO_BARS_SCHEMA,
            "status": "blocked_missing_confirm_external_call",
            "provider": YAHOO_PROVIDER,
            "tickers": symbols,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "external_calls_made": 0,
            "db_writes_made": 0,
            "next_action": "Re-run with --confirm-external-call to fetch mapped ticker bars.",
        }
    http = client or JsonHttpClient(UrlLibHttpTransport(), timeout_seconds=DEFAULT_TIMEOUT_SECONDS)
    bars: list[DailyBar] = []
    errors: list[str] = []
    calls = 0
    blocked = False
    for index, symbol in enumerate(symbols):
        url = yahoo_chart_url(symbol, start, end, base_url=base_url)
        calls += 1
        try:
            payload = http.get_json(url, headers=YAHOO_HEADERS)
            bars.extend(parse_yahoo_chart(symbol, payload, start=start, end=end))
        except Exception as exc:  # noqa: BLE001
            message = str(exc)
            if "429" in message:
                sleep(8.0)
                try:
                    payload = http.get_json(url, headers=YAHOO_HEADERS)
                    calls += 1
                    bars.extend(parse_yahoo_chart(symbol, payload, start=start, end=end))
                except Exception as retry_exc:  # noqa: BLE001
                    errors.append(f"{symbol}: {retry_exc}")
                    blocked = True
            else:
                if _looks_blocked(message):
                    blocked = True
                errors.append(f"{symbol}: {message}")
        if pause_seconds > 0 and index < len(symbols) - 1:
            sleep(pause_seconds)
    status = "fetched"
    if not bars and blocked:
        status = "error"
    elif not bars and errors:
        status = "error"
    return {
        "schema_version": YAHOO_BARS_SCHEMA,
        "status": status,
        "provider": YAHOO_PROVIDER,
        "tickers": symbols,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "bar_count": len(bars),
        "external_calls_made": calls,
        "errors": errors[:20],
        "bars": bars,
        "next_action": (
            "Public source blocked or returned no bars. Do not invent a Polygon key."
            if status == "error"
            else None
        ),
    }


def write_yahoo_bars(
    *,
    engine,
    tickers: Sequence[str],
    start: date,
    end: date,
    client: JsonHttpClient | None = None,
    confirm_external_call: bool = False,
    execute: bool = False,
    base_url: str = YAHOO_BASE_URL,
    pause_seconds: float = DEFAULT_PAUSE_SECONDS,
) -> dict[str, Any]:
    fetched = fetch_yahoo_daily_bars(
        tickers=tickers,
        start=start,
        end=end,
        client=client,
        confirm_external_call=confirm_external_call,
        base_url=base_url,
        pause_seconds=pause_seconds,
    )
    if fetched.get("status") != "fetched":
        fetched.setdefault("db_writes_made", 0)
        fetched.setdefault("investment_advice", False)
        return fetched
    bars = list(fetched.pop("bars") or [])
    fetched["db_writes_required"] = len(bars)
    fetched["db_writes_made"] = 0
    fetched["investment_advice"] = False
    fetched["ticker_count"] = len({bar.ticker for bar in bars})
    fetched["row_count"] = len(bars)
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


def _nth_float(values: Sequence[object], index: int) -> float | None:
    if index >= len(values):
        return None
    value = values[index]
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _nth_int(values: Sequence[object], index: int) -> int:
    number = _nth_float(values, index)
    if number is None:
        return 0
    return int(number)


def _looks_blocked(message: str) -> bool:
    text = message.casefold()
    return any(token in text for token in ("http 403", "http 429", "http 401", "blocked", "html response"))


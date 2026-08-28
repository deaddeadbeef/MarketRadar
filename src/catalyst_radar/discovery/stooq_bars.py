"""Mapped-ticker Stooq daily bars. No API key; explicit confirm like Polygon."""

from __future__ import annotations

import csv
import io
from collections.abc import Sequence
from datetime import UTC, date, datetime
from time import sleep
from typing import Any

from catalyst_radar.connectors.http import HttpTransport, UrlLibHttpTransport, redact_url
from catalyst_radar.core.models import DailyBar
from catalyst_radar.discovery.polygon_bars import default_bar_window, mapped_tickers_from_events
from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.repositories import MarketRepository

STOOQ_BARS_SCHEMA = "discovery-stooq-bars-v1"
STOOQ_BASE_URL = "https://stooq.com"
STOOQ_PROVIDER = "stooq"
DEFAULT_PAUSE_SECONDS = 0.35
DEFAULT_TIMEOUT_SECONDS = 20.0
STOOQ_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; MarketRadar/0.1; research-only daily bars)",
    "Accept": "text/csv,text/plain,*/*",
}

__all__ = [
    "DEFAULT_PAUSE_SECONDS",
    "STOOQ_BARS_SCHEMA",
    "STOOQ_BASE_URL",
    "STOOQ_PROVIDER",
    "default_bar_window",
    "fetch_stooq_daily_bars",
    "mapped_tickers_from_events",
    "parse_stooq_csv",
    "stooq_daily_url",
    "stooq_symbol",
    "write_stooq_bars",
]


def stooq_symbol(ticker: str) -> str:
    """Map a US equity ticker to Stooq's `symbol.us` form."""
    raw = str(ticker or "").strip()
    if not raw:
        return ""
    lowered = raw.lower()
    if lowered.endswith((".us", ".uk", ".de", ".jp", ".hk", ".pl")):
        return lowered.replace(".", "-", 1) if lowered.count(".") > 1 else lowered
    symbol = raw.replace(".", "-").strip("-").lower()
    return f"{symbol}.us" if symbol else ""


def stooq_daily_url(
    ticker: str,
    start: date,
    end: date,
    *,
    base_url: str = STOOQ_BASE_URL,
) -> str:
    symbol = stooq_symbol(ticker)
    return (
        f"{base_url.rstrip('/')}/q/d/l/"
        f"?s={symbol}&i=d&d1={start.strftime('%Y%m%d')}&d2={end.strftime('%Y%m%d')}"
    )


def parse_stooq_csv(
    ticker: str,
    text: str,
    *,
    start: date,
    end: date,
) -> list[DailyBar]:
    symbol = str(ticker or "").strip().upper()
    cleaned = (text or "").lstrip("\ufeff").strip()
    if not cleaned:
        return []
    preview = cleaned[:240].casefold()
    if "<html" in preview or "<!doctype" in preview:
        raise RuntimeError("HTML response instead of CSV (source blocked or not daily bars)")
    if preview.startswith("no data"):
        return []
    reader = csv.DictReader(io.StringIO(cleaned))
    fieldnames = [str(name or "").strip() for name in (reader.fieldnames or [])]
    lowered = {name.casefold(): name for name in fieldnames if name}
    date_key = lowered.get("date")
    close_key = lowered.get("close")
    if date_key is None or close_key is None:
        raise RuntimeError("Stooq CSV missing Date/Close columns")
    open_key = lowered.get("open")
    high_key = lowered.get("high")
    low_key = lowered.get("low")
    volume_key = lowered.get("volume")
    bars: list[DailyBar] = []
    for raw in reader:
        day_text = str(raw.get(date_key) or "").strip()
        if not day_text:
            continue
        try:
            day = date.fromisoformat(day_text[:10])
        except ValueError:
            continue
        if day < start or day > end:
            continue
        close = _optional_float(raw.get(close_key))
        if close is None:
            continue
        open_px = _optional_float(raw.get(open_key) if open_key else None)
        high = _optional_float(raw.get(high_key) if high_key else None)
        low = _optional_float(raw.get(low_key) if low_key else None)
        if open_px is None:
            open_px = close
        if high is None:
            high = max(open_px, close)
        if low is None:
            low = min(open_px, close)
        volume = _optional_int(raw.get(volume_key) if volume_key else None)
        stamp = datetime(day.year, day.month, day.day, 21, tzinfo=UTC)
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
                provider=STOOQ_PROVIDER,
                source_ts=stamp,
                available_at=stamp,
            )
        )
    bars.sort(key=lambda row: row.date)
    return bars


def fetch_stooq_daily_bars(
    *,
    tickers: Sequence[str],
    start: date,
    end: date,
    transport: HttpTransport | None = None,
    confirm_external_call: bool = False,
    base_url: str = STOOQ_BASE_URL,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    pause_seconds: float = DEFAULT_PAUSE_SECONDS,
) -> dict[str, Any]:
    symbols = [str(item).strip().upper() for item in tickers if str(item).strip()]
    if not confirm_external_call:
        return {
            "schema_version": STOOQ_BARS_SCHEMA,
            "status": "blocked_missing_confirm_external_call",
            "provider": STOOQ_PROVIDER,
            "tickers": symbols,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "external_calls_made": 0,
            "db_writes_made": 0,
            "next_action": "Re-run with --confirm-external-call to fetch mapped ticker bars.",
        }
    http = transport or UrlLibHttpTransport()
    bars: list[DailyBar] = []
    errors: list[str] = []
    calls = 0
    blocked = False
    for index, symbol in enumerate(symbols):
        url = stooq_daily_url(symbol, start, end, base_url=base_url)
        calls += 1
        try:
            text = _get_text(http, url, timeout_seconds=timeout_seconds)
            bars.extend(parse_stooq_csv(symbol, text, start=start, end=end))
        except Exception as exc:  # noqa: BLE001
            message = str(exc)
            if _looks_blocked(message):
                blocked = True
            errors.append(f"{symbol}: {message}")
        if pause_seconds > 0 and index < len(symbols) - 1:
            sleep(pause_seconds)
    status = "fetched"
    if not bars and blocked:
        status = "error"
    return {
        "schema_version": STOOQ_BARS_SCHEMA,
        "status": status,
        "provider": STOOQ_PROVIDER,
        "tickers": symbols,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "bar_count": len(bars),
        "external_calls_made": calls,
        "errors": errors[:20],
        "bars": bars,
        "next_action": (
            "Public source blocked or returned no CSV. Do not invent a Polygon key."
            if status == "error"
            else None
        ),
    }


def write_stooq_bars(
    *,
    engine,
    tickers: Sequence[str],
    start: date,
    end: date,
    transport: HttpTransport | None = None,
    confirm_external_call: bool = False,
    execute: bool = False,
    base_url: str = STOOQ_BASE_URL,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    pause_seconds: float = DEFAULT_PAUSE_SECONDS,
) -> dict[str, Any]:
    fetched = fetch_stooq_daily_bars(
        tickers=tickers,
        start=start,
        end=end,
        transport=transport,
        confirm_external_call=confirm_external_call,
        base_url=base_url,
        timeout_seconds=timeout_seconds,
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


def _get_text(transport: HttpTransport, url: str, *, timeout_seconds: float) -> str:
    response = transport.get(url, headers=STOOQ_HEADERS, timeout_seconds=timeout_seconds)
    if response.status_code < 200 or response.status_code >= 300:
        raise RuntimeError(f"HTTP {response.status_code} from {redact_url(url)}")
    text = response.body.decode("utf-8", errors="replace")
    preview = text[:240].casefold()
    if "<html" in preview or "<!doctype" in preview:
        raise RuntimeError(f"HTML response from {redact_url(url)} (blocked or not CSV)")
    return text


def _looks_blocked(message: str) -> bool:
    text = message.casefold()
    return any(
        token in text
        for token in ("html response", "http 403", "http 429", "http 451", "blocked")
    )


def _optional_float(value: object) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _optional_int(value: object) -> int:
    number = _optional_float(value)
    if number is None:
        return 0
    return int(number)

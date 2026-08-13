"""Mapped-ticker daily bar import for event-time discovery joins."""

from __future__ import annotations

import csv
from collections.abc import Sequence
from datetime import UTC, date, datetime
from pathlib import Path

from sqlalchemy.engine import Engine

from catalyst_radar.core.models import DailyBar
from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.repositories import MarketRepository

BARS_SCHEMA = "discovery-bars-v1"
REQUIRED_COLUMNS = ("ticker", "date", "open", "high", "low", "close", "volume")


def load_discovery_bars_csv(path: str | Path) -> list[DailyBar]:
    file_path = Path(path)
    with file_path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError("bars CSV is missing a header row")
        missing = [name for name in REQUIRED_COLUMNS if name not in reader.fieldnames]
        if missing:
            raise ValueError(f"bars CSV missing columns: {', '.join(missing)}")
        rows: list[DailyBar] = []
        for index, raw in enumerate(reader, start=2):
            rows.append(_row_to_bar(raw, index))
    if not rows:
        raise ValueError("bars CSV has no data rows")
    return rows


def import_discovery_bars(
    *,
    engine: Engine,
    csv_path: str | Path,
    execute: bool = False,
) -> dict[str, object]:
    bars = load_discovery_bars_csv(csv_path)
    tickers = sorted({bar.ticker for bar in bars})
    dates = sorted({bar.date for bar in bars})
    preview = {
        "schema_version": BARS_SCHEMA,
        "status": "executed" if execute else "preview",
        "csv_path": str(csv_path),
        "row_count": len(bars),
        "ticker_count": len(tickers),
        "tickers": tickers[:40],
        "date_min": dates[0].isoformat(),
        "date_max": dates[-1].isoformat(),
        "external_calls_made": 0,
        "db_writes_made": 0,
        "db_writes_required": len(bars),
        "investment_advice": False,
    }
    if not execute:
        preview["next_action"] = (
            f"Preview only. Re-run with --execute to write {len(bars)} mapped bars."
        )
        return preview
    create_schema(engine)
    MarketRepository(engine).upsert_daily_bars(bars)
    preview["db_writes_made"] = len(bars)
    preview["next_action"] = (
        "Bars written. Re-run assert-discovery-ready to measure event-time join."
    )
    return preview


def _row_to_bar(raw: dict[str, str], line_no: int) -> DailyBar:
    ticker = str(raw.get("ticker") or "").strip().upper()
    if not ticker:
        raise ValueError(f"line {line_no}: ticker is required")
    try:
        day = date.fromisoformat(str(raw.get("date") or "").strip())
    except ValueError as exc:
        raise ValueError(f"line {line_no}: date must be YYYY-MM-DD") from exc
    close = _num(raw.get("close"), "close", line_no)
    open_px = _num(raw.get("open"), "open", line_no, default=close)
    high = _num(raw.get("high"), "high", line_no, default=max(open_px, close))
    low = _num(raw.get("low"), "low", line_no, default=min(open_px, close))
    volume = int(_num(raw.get("volume"), "volume", line_no, default=1_000_000))
    vwap = _num(raw.get("vwap"), "vwap", line_no, default=close)
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
        provider=str(raw.get("provider") or "discovery-csv").strip() or "discovery-csv",
        source_ts=stamp,
        available_at=stamp,
    )


def weekday_sessions(end: date, count: int) -> list[date]:
    days: list[date] = []
    cursor = end
    while len(days) < max(1, count):
        if cursor.weekday() < 5:
            days.append(cursor)
        cursor = date.fromordinal(cursor.toordinal() - 1)
    return list(reversed(days))


def write_session_bars_csv(
    path: str | Path,
    *,
    tickers: Sequence[str],
    end: date,
    sessions: int = 12,
) -> Path:
    """Helper for tests and local smoke data. Deterministic flat-then-step prices."""
    file_path = Path(path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    days = weekday_sessions(end, sessions)
    with file_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(REQUIRED_COLUMNS))
        writer.writeheader()
        for ticker_index, ticker in enumerate(tickers):
            base = 40.0 + ticker_index * 15.0
            for day_index, day in enumerate(days):
                close = base + day_index * 0.25
                writer.writerow(
                    {
                        "ticker": ticker,
                        "date": day.isoformat(),
                        "open": f"{close:.2f}",
                        "high": f"{close:.2f}",
                        "low": f"{close:.2f}",
                        "close": f"{close:.2f}",
                        "volume": "1000000",
                    }
                )
    return file_path


def _num(value: object, field: str, line_no: int, default: float | None = None) -> float:
    text = str(value or "").strip()
    if not text:
        if default is None:
            raise ValueError(f"line {line_no}: {field} is required")
        return float(default)
    try:
        return float(text)
    except ValueError as exc:
        raise ValueError(f"line {line_no}: {field} must be numeric") from exc

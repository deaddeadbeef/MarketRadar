from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
from sqlalchemy import create_engine

from catalyst_radar.connectors.http import FakeHttpTransport, HttpResponse
from catalyst_radar.discovery.stooq_bars import (
    STOOQ_PROVIDER,
    fetch_stooq_daily_bars,
    parse_stooq_csv,
    stooq_daily_url,
    stooq_symbol,
    write_stooq_bars,
)
from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.repositories import MarketRepository

pytestmark = pytest.mark.discovery

CSV_BODY = (
    "Date,Open,High,Low,Close,Volume\n"
    "2026-08-10,100.0,102.0,99.0,101.0,1500000\n"
    "2026-08-11,101.0,103.0,100.5,102.5,1600000\n"
    "2026-08-12,102.5,104.0,101.0,103.0,1700000\n"
)


def test_stooq_symbol_maps_us_tickers() -> None:
    assert stooq_symbol("MSFT") == "msft.us"
    assert stooq_symbol("BRK.B") == "brk-b.us"
    assert stooq_symbol("spy.us") == "spy.us"
    assert stooq_symbol("  gs  ") == "gs.us"


def test_parse_stooq_csv_filters_window_and_sets_provider() -> None:
    bars = parse_stooq_csv(
        "GS",
        CSV_BODY,
        start=date(2026, 8, 11),
        end=date(2026, 8, 12),
    )
    assert [bar.date.isoformat() for bar in bars] == ["2026-08-11", "2026-08-12"]
    assert bars[0].ticker == "GS"
    assert bars[0].provider == STOOQ_PROVIDER
    assert bars[0].close == 102.5
    assert bars[0].volume == 1_600_000


def test_fetch_stooq_requires_confirm_and_parses_csv() -> None:
    start = date(2026, 8, 10)
    end = date(2026, 8, 12)
    url = stooq_daily_url("GS", start, end)
    transport = FakeHttpTransport({url: HttpResponse(200, url, {}, CSV_BODY.encode())})
    blocked = fetch_stooq_daily_bars(
        tickers=["GS"],
        start=start,
        end=end,
        transport=transport,
        confirm_external_call=False,
        pause_seconds=0,
    )
    assert blocked["status"] == "blocked_missing_confirm_external_call"
    assert blocked["external_calls_made"] == 0
    assert transport.requests == []

    fetched = fetch_stooq_daily_bars(
        tickers=["GS"],
        start=start,
        end=end,
        transport=transport,
        confirm_external_call=True,
        pause_seconds=0,
    )
    assert fetched["status"] == "fetched"
    assert fetched["bar_count"] == 3
    assert fetched["external_calls_made"] == 1
    assert fetched["provider"] == STOOQ_PROVIDER
    assert transport.requests == [url]


def test_write_stooq_bars_preview_and_execute(tmp_path: Path) -> None:
    start = date(2026, 8, 10)
    end = date(2026, 8, 12)
    url = stooq_daily_url("CAT", start, end)
    transport = FakeHttpTransport({url: HttpResponse(200, url, {}, CSV_BODY.encode())})
    engine = create_engine(f"sqlite:///{tmp_path / 'bars.db'}")
    preview = write_stooq_bars(
        engine=engine,
        tickers=["CAT"],
        start=start,
        end=end,
        transport=transport,
        confirm_external_call=True,
        execute=False,
        pause_seconds=0,
    )
    assert preview["status"] == "preview"
    assert preview["db_writes_made"] == 0
    executed = write_stooq_bars(
        engine=engine,
        tickers=["CAT"],
        start=start,
        end=end,
        transport=transport,
        confirm_external_call=True,
        execute=True,
        pause_seconds=0,
    )
    assert executed["status"] == "executed"
    assert executed["db_writes_made"] == 3
    create_schema(engine)
    stored = MarketRepository(engine).daily_bars("CAT", date(2026, 8, 12), 20)
    assert len(stored) == 3
    assert stored[-1].provider == STOOQ_PROVIDER
    assert stored[-1].close == 103.0


def test_fetch_stooq_html_block_is_error() -> None:
    start = date(2026, 8, 10)
    end = date(2026, 8, 12)
    url = stooq_daily_url("XOM", start, end)
    html = b"<!DOCTYPE html><html><body>blocked</body></html>"
    transport = FakeHttpTransport({url: HttpResponse(200, url, {}, html)})
    fetched = fetch_stooq_daily_bars(
        tickers=["XOM"],
        start=start,
        end=end,
        transport=transport,
        confirm_external_call=True,
        pause_seconds=0,
    )
    assert fetched["status"] == "error"
    assert fetched["bar_count"] == 0
    assert fetched["errors"]

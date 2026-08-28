from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest
from sqlalchemy import create_engine

from catalyst_radar.connectors.http import FakeHttpTransport, HttpResponse, JsonHttpClient
from catalyst_radar.discovery.yahoo_bars import (
    YAHOO_PROVIDER,
    fetch_yahoo_daily_bars,
    parse_yahoo_chart,
    write_yahoo_bars,
    yahoo_chart_url,
    yahoo_symbol,
)
from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.repositories import MarketRepository

pytestmark = pytest.mark.discovery


def _chart_payload() -> dict:
    return {
        "chart": {
            "result": [
                {
                    "timestamp": [1786455000, 1786541400, 1786627800],
                    "indicators": {
                        "quote": [
                            {
                                "open": [100.0, 101.0, 102.0],
                                "high": [102.0, 103.0, 104.0],
                                "low": [99.0, 100.5, 101.0],
                                "close": [101.0, 102.5, 103.0],
                                "volume": [1_500_000, 1_600_000, 1_700_000],
                            }
                        ],
                        "adjclose": [{"adjclose": [101.0, 102.5, 103.0]}],
                    },
                }
            ],
            "error": None,
        }
    }


def test_yahoo_symbol_maps_class_shares() -> None:
    assert yahoo_symbol("MSFT") == "MSFT"
    assert yahoo_symbol("BRK.B") == "BRK-B"


def test_parse_yahoo_chart_uses_adjusted_close() -> None:
    bars = parse_yahoo_chart(
        "GS",
        _chart_payload(),
        start=date(2026, 8, 11),
        end=date(2026, 8, 13),
    )
    assert [bar.date.isoformat() for bar in bars] == ["2026-08-11", "2026-08-12", "2026-08-13"]
    assert bars[0].ticker == "GS"
    assert bars[0].provider == YAHOO_PROVIDER
    assert bars[-1].close == 103.0
    assert bars[-1].volume == 1_700_000


def test_fetch_yahoo_requires_confirm_and_parses_json() -> None:
    start = date(2026, 8, 11)
    end = date(2026, 8, 13)
    url = yahoo_chart_url("GS", start, end)
    transport = FakeHttpTransport(
        {url: HttpResponse(200, url, {}, json.dumps(_chart_payload()).encode())}
    )
    client = JsonHttpClient(transport, timeout_seconds=5.0)
    blocked = fetch_yahoo_daily_bars(
        tickers=["GS"],
        start=start,
        end=end,
        client=client,
        confirm_external_call=False,
        pause_seconds=0,
    )
    assert blocked["status"] == "blocked_missing_confirm_external_call"
    assert blocked["external_calls_made"] == 0
    fetched = fetch_yahoo_daily_bars(
        tickers=["GS"],
        start=start,
        end=end,
        client=client,
        confirm_external_call=True,
        pause_seconds=0,
    )
    assert fetched["status"] == "fetched"
    assert fetched["bar_count"] == 3
    assert fetched["provider"] == YAHOO_PROVIDER
    assert transport.requests == [url]


def test_write_yahoo_bars_preview_and_execute(tmp_path: Path) -> None:
    start = date(2026, 8, 11)
    end = date(2026, 8, 13)
    url = yahoo_chart_url("CAT", start, end)
    transport = FakeHttpTransport(
        {url: HttpResponse(200, url, {}, json.dumps(_chart_payload()).encode())}
    )
    client = JsonHttpClient(transport, timeout_seconds=5.0)
    engine = create_engine(f"sqlite:///{tmp_path / 'bars.db'}")
    preview = write_yahoo_bars(
        engine=engine,
        tickers=["CAT"],
        start=start,
        end=end,
        client=client,
        confirm_external_call=True,
        execute=False,
        pause_seconds=0,
    )
    assert preview["status"] == "preview"
    assert preview["db_writes_made"] == 0
    executed = write_yahoo_bars(
        engine=engine,
        tickers=["CAT"],
        start=start,
        end=end,
        client=client,
        confirm_external_call=True,
        execute=True,
        pause_seconds=0,
    )
    assert executed["status"] == "executed"
    assert executed["db_writes_made"] == 3
    create_schema(engine)
    stored = MarketRepository(engine).daily_bars("CAT", date(2026, 8, 13), 20)
    assert len(stored) == 3
    assert stored[-1].provider == YAHOO_PROVIDER

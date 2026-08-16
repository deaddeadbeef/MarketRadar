from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, date, datetime

import pytest

from catalyst_radar.connectors.http import FakeHttpTransport, HttpResponse, JsonHttpClient
from catalyst_radar.discovery.insights import build_discovery_insights, format_discovery_insights
from catalyst_radar.discovery.polygon_bars import (
    DEFAULT_LOOKBACK_DAYS,
    default_bar_window,
    fetch_polygon_daily_bars,
)

pytestmark = pytest.mark.discovery


def test_insights_from_sample_events_are_research_only() -> None:
    payload = build_discovery_insights(
        events_path="data/sample/world_events.json",
        engine=None,
        include_cases=False,
        now=datetime(2026, 7, 19, 12, tzinfo=UTC),
        theme_peers_path=None,
    )
    assert payload["investment_advice"] is False
    assert payload["decision_support_only"] is True
    assert payload["leads"]
    text = format_discovery_insights(payload)
    assert "not investment advice" in text.casefold() or "investment_advice=false" in text


def test_polygon_daily_bars_parse_fixture_payload() -> None:
    url = (
        "https://api.polygon.io/v2/aggs/ticker/MU/range/1/day/"
        "2026-08-01/2026-08-13?adjusted=true&sort=asc&limit=120"
    )
    body = (
        b'{"status":"OK","results":[{"t":1754524800000,"o":100,"h":102,"l":99,'
        b'"c":101,"v":1000000,"vw":100.5}]}'
    )
    transport = FakeHttpTransport({url: HttpResponse(200, url, {}, body)})
    client = JsonHttpClient(transport, timeout_seconds=5.0)
    blocked = fetch_polygon_daily_bars(
        api_key="test-key",
        tickers=["MU"],
        start=date(2026, 8, 1),
        end=date(2026, 8, 13),
        client=client,
        confirm_external_call=False,
    )
    assert blocked["status"] == "blocked_missing_confirm_external_call"
    fetched = fetch_polygon_daily_bars(
        api_key="test-key",
        tickers=["MU"],
        start=date(2026, 8, 1),
        end=date(2026, 8, 13),
        client=client,
        confirm_external_call=True,
    )
    assert fetched["status"] == "fetched"
    assert fetched["bar_count"] == 1
    assert fetched["external_calls_made"] == 1
    assert transport.requests == [url]
    assert "apiKey=" not in url
    _assert_no_polygon_secret(fetched, "test-key")


def test_polygon_default_lookback_is_40_calendar_days() -> None:
    assert DEFAULT_LOOKBACK_DAYS == 40
    start, end = default_bar_window(end=date(2026, 8, 13))
    assert end == date(2026, 8, 13)
    assert (end - start).days == 40


def test_polygon_payload_and_errors_redact_api_key() -> None:
    api_key = "poly-secret-key-xyz"
    leak_url = (
        "https://api.polygon.io/v2/aggs/ticker/MU/range/1/day/"
        f"2026-08-01/2026-08-13?adjusted=true&sort=asc&limit=120&apiKey={api_key}"
    )

    class LeakTransport:
        def get(
            self,
            url: str,
            *,
            headers: Mapping[str, str],
            timeout_seconds: float,
        ) -> HttpResponse:
            raise RuntimeError(
                f"HTTP 401 from {leak_url}; detail=unauthorized {api_key}"
            )

        def post(
            self,
            url: str,
            *,
            headers: Mapping[str, str],
            body: bytes,
            timeout_seconds: float,
        ) -> HttpResponse:
            raise RuntimeError("unexpected POST")

    fetched = fetch_polygon_daily_bars(
        api_key=api_key,
        tickers=["MU"],
        start=date(2026, 8, 1),
        end=date(2026, 8, 13),
        client=JsonHttpClient(LeakTransport(), timeout_seconds=5.0),
        confirm_external_call=True,
    )
    assert fetched["status"] == "fetched"
    assert fetched["errors"]
    _assert_no_polygon_secret(fetched, api_key)


def _assert_no_polygon_secret(payload: object, api_key: str) -> None:
    blob = json.dumps(payload, default=str)
    assert "apiKey=" not in blob
    assert api_key not in blob

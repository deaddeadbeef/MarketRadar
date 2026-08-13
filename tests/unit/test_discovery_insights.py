from __future__ import annotations

from datetime import UTC, datetime

import pytest

from catalyst_radar.connectors.http import FakeHttpTransport, HttpResponse, JsonHttpClient
from catalyst_radar.discovery.insights import build_discovery_insights, format_discovery_insights
from catalyst_radar.discovery.polygon_bars import fetch_polygon_daily_bars

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
        "2026-08-01/2026-08-13?adjusted=true&sort=asc&limit=120&apiKey=test-key"
    )
    body = (
        b'{"status":"OK","results":[{"t":1754524800000,"o":100,"h":102,"l":99,'
        b'"c":101,"v":1000000,"vw":100.5}]}'
    )
    client = JsonHttpClient(
        FakeHttpTransport({url: HttpResponse(200, url, {}, body)}),
        timeout_seconds=5.0,
    )
    blocked = fetch_polygon_daily_bars(
        api_key="test-key",
        tickers=["MU"],
        start=__import__("datetime").date(2026, 8, 1),
        end=__import__("datetime").date(2026, 8, 13),
        client=client,
        confirm_external_call=False,
    )
    assert blocked["status"] == "blocked_missing_confirm_external_call"
    fetched = fetch_polygon_daily_bars(
        api_key="test-key",
        tickers=["MU"],
        start=__import__("datetime").date(2026, 8, 1),
        end=__import__("datetime").date(2026, 8, 13),
        client=client,
        confirm_external_call=True,
    )
    assert fetched["status"] == "fetched"
    assert fetched["bar_count"] == 1
    assert fetched["external_calls_made"] == 1

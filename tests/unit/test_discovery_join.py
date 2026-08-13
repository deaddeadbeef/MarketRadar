from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import create_engine

from catalyst_radar.core.models import DailyBar
from catalyst_radar.discovery.brief import build_discovery_brief
from catalyst_radar.discovery.join import join_event_ticker, load_bars_by_ticker
from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.repositories import MarketRepository

pytestmark = pytest.mark.discovery


def _ts(day: date) -> datetime:
    return datetime(day.year, day.month, day.day, 21, tzinfo=UTC)


def _bar(ticker: str, day: date, close: float) -> DailyBar:
    stamp = _ts(day)
    return DailyBar(
        ticker=ticker,
        date=day,
        open=close,
        high=close,
        low=close,
        close=close,
        volume=1_000_000,
        vwap=close,
        adjusted=True,
        provider="test",
        source_ts=stamp,
        available_at=stamp,
    )


def _weekdays(end: date, count: int) -> list[date]:
    days: list[date] = []
    cursor = end
    while len(days) < count:
        if cursor.weekday() < 5:
            days.append(cursor)
        cursor -= timedelta(days=1)
    return list(reversed(days))


def _engine_with_bars() -> object:
    engine = create_engine("sqlite:///:memory:")
    create_schema(engine)
    repo = MarketRepository(engine)
    event_day = date(2026, 8, 10)
    end = date(2026, 8, 12)
    sessions = _weekdays(end, 12)
    rows: list[DailyBar] = []
    for index, day in enumerate(sessions):
        # AAA ramps after the event; BBB stays flat; CCC stops in July.
        aaa_close = 100.0 + (8.0 if day >= event_day else 0.0) + index * 0.1
        rows.append(_bar("AAA", day, aaa_close))
        rows.append(_bar("BBB", day, 50.0))
        rows.append(_bar("SPY", day, 500.0 + index))
    for day in _weekdays(date(2026, 7, 1), 8):
        rows.append(_bar("CCC", day, 20.0))
    repo.upsert_daily_bars(rows)
    return engine


def test_event_join_uses_bars_not_missing_pre_event_tape() -> None:
    engine = _engine_with_bars()
    now = datetime(2026, 8, 12, 18, tzinfo=UTC)
    event_at = datetime(2026, 8, 10, 12, tzinfo=UTC)
    bars = load_bars_by_ticker(engine, ["AAA", "BBB", "CCC"], end=now.date())
    moved = join_event_ticker(
        ticker="AAA",
        event_available_at=event_at,
        event_direction="bullish",
        emotion_score=70.0,
        bars_by_ticker=bars,
        now=now,
    )
    flat = join_event_ticker(
        ticker="BBB",
        event_available_at=event_at,
        event_direction="bullish",
        emotion_score=70.0,
        bars_by_ticker=bars,
        now=now,
    )
    old = join_event_ticker(
        ticker="CCC",
        event_available_at=event_at,
        event_direction="bullish",
        emotion_score=70.0,
        bars_by_ticker=bars,
        now=now,
    )
    assert moved.join_status == "joined"
    assert moved.ret_5d_pct is not None
    assert flat.join_status == "joined"
    assert flat.reaction_score < moved.reaction_score
    assert round(flat.emotion_reaction_gap, 2) == round(70.0 - flat.reaction_score, 2)
    assert old.join_status == "missing_scan"
    assert old.priced_in_status in {"unknown", "stale"}


def test_stale_bars_are_missing_scan_not_quiet_tape() -> None:
    engine = create_engine("sqlite:///:memory:")
    create_schema(engine)
    repo = MarketRepository(engine)
    repo.upsert_daily_bars(
        [_bar("MU", day, 100.0 + i) for i, day in enumerate(_weekdays(date(2026, 5, 13), 8))]
    )
    now = datetime(2026, 8, 12, tzinfo=UTC)
    bars = load_bars_by_ticker(engine, ["MU"], end=now.date())
    joined = join_event_ticker(
        ticker="MU",
        event_available_at=datetime(2026, 7, 28, tzinfo=UTC),
        event_direction="bullish",
        emotion_score=60.0,
        bars_by_ticker=bars,
        now=now,
    )
    assert joined.join_status == "missing_scan"
    assert joined.priced_in_status in {"unknown", "stale"}


def test_brief_dedupes_tickers_and_event_joins(tmp_path: Path) -> None:
    engine = _engine_with_bars()
    events = {
        "schema_version": "world-events-v1",
        "generated_at": "2026-08-12T12:00:00+00:00",
        "source": "test",
        "events": [
            {
                "id": "evt_a",
                "title": "Test event A",
                "summary": "A",
                "themes": [],
                "tickers": ["AAA", "BBB"],
                "secondary_tickers": ["CCC"],
                "direction": "bullish",
                "materiality": 0.8,
                "source_quality": 0.6,
                "source_category": "social",
                "sources": [],
                "available_at": "2026-08-10T12:00:00+00:00",
            },
            {
                "id": "evt_b",
                "title": "Test event B",
                "summary": "B",
                "themes": [],
                "tickers": ["AAA"],
                "secondary_tickers": [],
                "direction": "bullish",
                "materiality": 0.7,
                "source_quality": 0.5,
                "source_category": "social",
                "sources": [],
                "available_at": "2026-08-10T12:00:00+00:00",
            },
        ],
    }
    path = tmp_path / "events.json"
    path.write_text(__import__("json").dumps(events), encoding="utf-8")
    brief = build_discovery_brief(
        events_path=path,
        engine=engine,
        limit=10,
        now=datetime(2026, 8, 12, 18, tzinfo=UTC),
        theme_peers_path=None,
    )
    tickers = [row["ticker"] for row in brief["discoveries"]]
    assert tickers.count("AAA") == 1
    by_ticker = {row["ticker"]: row for row in brief["discoveries"]}
    assert by_ticker["AAA"]["join_status"] == "joined"
    assert by_ticker["CCC"]["join_status"] == "missing_scan"
    assert by_ticker["CCC"]["quiet_tape"] is False
    assert all(row["usefulness"] == "research_only" for row in brief["discoveries"])
    assert brief["join_coverage"]["coverage_pct"] >= 50.0

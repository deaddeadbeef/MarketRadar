from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from catalyst_radar.discovery.from_posts import build_world_events_from_posts
from catalyst_radar.discovery.mapper import DEFAULT_THEME_TICKERS
from catalyst_radar.discovery.ux import COMPANY_NAMES, apply_novice_ux, company_name

pytestmark = pytest.mark.discovery

# 8 posts / 3 event_ids. Do not use data/sample/x_posts.json as the story-count law.
LAW_POSTS = Path("data/sample/x_posts_2026-08-13.json")
LAW_WORLD_EVENT_IDS = {
    "evt_hbm4_memory_asp",
    "evt_china_nand_share",
    "evt_cpi_hormuz_macro",
}


def test_novice_ux_hides_theme_missing_scan_and_speaks_plain_english() -> None:
    brief = {
        "freshness_status": "fresh",
        "events": [
            {
                "id": "e1",
                "title": "Memory prices jump",
                "direction": "bullish",
                "tickers": ["SNDK"],
            }
        ],
        "discoveries": [
            {
                "ticker": "EURN",
                "origin": "theme",
                "join_status": "missing_scan",
                "discovery_score": 90,
                "event_title": "Hormuz",
            },
            {
                "ticker": "SNDK",
                "origin": "event",
                "join_status": "joined",
                "discovery_score": 70,
                "event_title": "Memory prices jump",
                "ret_5d_pct": -0.4,
                "quiet_tape": True,
            },
            {
                "ticker": "SKHY",
                "origin": "event",
                "join_status": "missing_scan",
                "discovery_score": 80,
                "event_title": "Memory prices jump",
            },
        ],
    }
    payload = apply_novice_ux(brief)
    tickers = [row["ticker"] for row in payload["discoveries"]]
    assert tickers[0] == "SNDK"
    assert "EURN" not in tickers
    assert "powershell" not in str(payload["next_action"]).casefold()
    assert "Sandisk" in payload["headline"]
    assert "barely moved" in payload["headline"]
    assert payload["novice"]["focus_ticker"] == "SNDK"
    assert company_name("SNDK") == "Sandisk"


def test_every_default_theme_ticker_has_display_name() -> None:
    tickers = {ticker for values in DEFAULT_THEME_TICKERS.values() for ticker in values}
    assert tickers
    for ticker in sorted(tickers):
        name = company_name(ticker)
        assert ticker in COMPANY_NAMES
        assert name == COMPANY_NAMES[ticker]
        assert name != ticker or COMPANY_NAMES[ticker] == ticker


def test_novice_stories_follow_world_events_not_post_count() -> None:
    raw = json.loads(LAW_POSTS.read_text(encoding="utf-8"))
    posts = raw["posts"]
    assert len(posts) == 8

    world = build_world_events_from_posts(
        posts_path=LAW_POSTS,
        now=datetime(2026, 8, 13, 12, tzinfo=UTC),
    )
    brief = {"freshness_status": "fresh", "events": world["events"], "discoveries": []}
    payload = apply_novice_ux(brief)
    stories = payload["novice"]["events"]
    # 3 stories on the 8/3 fixture. Do not freeze uncapped copy as a forever law;
    # the inequalities survive PR 6's cap of 8.
    assert len(stories) == 3
    assert len(stories) <= 8
    assert len(stories) <= len(brief["events"])
    assert len(stories) != len(posts)
    assert {row["id"] for row in stories} == LAW_WORLD_EVENT_IDS
    assert "3 stories" in payload["headline"]
    assert "8 stor" not in payload["headline"]

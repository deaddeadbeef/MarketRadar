from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from catalyst_radar.discovery.case_file import build_discovery_case_file
from catalyst_radar.discovery.from_posts import build_world_events_from_posts
from catalyst_radar.discovery.source_hosts import (
    best_source_category,
    classify_url,
    is_primary_host,
)

pytestmark = pytest.mark.discovery

NOW = datetime(2026, 8, 28, 1, tzinfo=UTC)


def test_classify_official_hosts() -> None:
    assert classify_url(
        "https://www.kansascityfed.org/newsroom/2026-news-releases/example"
    ) == "regulatory"
    assert classify_url("https://www.bls.gov/schedule/2026/09_sched_list.htm") == "regulatory"
    assert classify_url(
        "https://investors.gilead.com/news/news-details/2026/example/default.aspx"
    ) == "primary_source"
    assert (
        classify_url("https://www.businesswire.com/news/home/20260817093045/en/")
        == "company_press_release"
    )
    assert (
        classify_url(
            "https://www.reuters.com/business/energy/opec-has-agreement-principle-september-quota-increase-pause-thereafter-source-2026-08-02/"
        )
        == "reputable_news"
    )
    assert classify_url("https://x.com/mktboxapp/status/2092995268639633486") == "social"
    assert classify_url("https://nltimes.nl/2026/08/20/example") == "unknown"


def test_best_category_upgrades_social_when_official_url_present() -> None:
    assert (
        best_source_category(
            [
                "https://x.com/mktboxapp/status/1",
                "https://www.kansascityfed.org/newsroom/example",
            ],
            declared="social",
        )
        == "regulatory"
    )
    assert (
        best_source_category(
            ["https://x.com/foo/status/1"],
            declared="social",
        )
        == "social"
    )
    assert is_primary_host("https://www.bea.gov/news/2026/personal-income-and-outlays-july-2026")


def test_from_posts_does_not_keep_official_url_as_social(tmp_path: Path) -> None:
    posts = {
        "schema_version": "x-posts-v1",
        "generated_at": "2026-08-27T15:26:00+00:00",
        "source": "unit",
        "posts": [
            {
                "id": "kc",
                "event_id": "jackson_hole",
                "title": "Jackson Hole opens",
                "text": "Warsh speaks Friday. $JPM",
                "url": "https://www.kansascityfed.org/newsroom/2026-news-releases/example",
                "author": "@KansasCityFed",
                "published_at": "2026-08-25T16:00:00+00:00",
                "themes": ["policy"],
                "tickers": ["JPM"],
                "direction": "mixed",
                "provider": "x",
            },
            {
                "id": "tweet",
                "event_id": "jackson_hole",
                "title": "chatter",
                "text": "speech tomorrow $JPM",
                "url": "https://x.com/mktboxapp/status/1",
                "author": "@mktboxapp",
                "published_at": "2026-08-27T15:15:00+00:00",
                "themes": ["policy"],
                "tickers": ["JPM"],
                "direction": "mixed",
                "provider": "x",
            },
        ],
    }
    path = tmp_path / "posts.json"
    path.write_text(json.dumps(posts), encoding="utf-8")
    event = build_world_events_from_posts(posts_path=path, now=NOW)["events"][0]
    assert event["source_category"] == "regulatory"


def test_case_file_primary_confirmed_from_feed_url(tmp_path: Path) -> None:
    events = {
        "schema_version": "world-events-v1",
        "generated_at": "2026-08-27T15:27:17+00:00",
        "source": "unit",
        "events": [
            {
                "id": "evt_jackson_hole_fomc_window",
                "title": "Jackson Hole opens; Warsh keynote Friday",
                "summary": "KC Fed calendar. $JPM",
                "themes": ["policy"],
                "tickers": ["JPM"],
                "secondary_tickers": [],
                "direction": "mixed",
                "materiality": 0.83,
                "source_quality": 0.62,
                "source_category": "social",
                "sources": [
                    {
                        "provider": "x",
                        "url": "https://www.kansascityfed.org/newsroom/2026-news-releases/example",
                        "author": "@KansasCityFed",
                        "published_at": "2026-08-25T16:00:00+00:00",
                        "engagement": {"likes": 0, "views": 0},
                    }
                ],
                "available_at": "2026-08-25T16:00:00+00:00",
            }
        ],
    }
    path = tmp_path / "world_events.json"
    path.write_text(json.dumps(events), encoding="utf-8")
    case = build_discovery_case_file(ticker="JPM", events_path=path, engine=None)
    assert case["confirmation"]["status"] == "primary_confirmed"
    assert case["confirmation"]["feed_primary_source_count"] == 1
    assert case["investment_advice"] is False
    assert case["can_make_investment_decision"] is False

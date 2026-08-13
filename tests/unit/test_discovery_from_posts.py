from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from catalyst_radar.discovery.from_posts import (
    build_world_events_from_posts,
    convert_posts_file,
)

pytestmark = pytest.mark.discovery


def test_from_posts_groups_and_extracts_cashtags(tmp_path: Path) -> None:
    posts = {
        "schema_version": "x-posts-v1",
        "generated_at": "2026-08-13T12:00:00+00:00",
        "source": "unit",
        "posts": [
            {
                "id": "a",
                "event_id": "hbm",
                "text": "$MU sold out through 2027",
                "published_at": "2026-08-13T10:00:00+00:00",
                "themes": ["memory"],
                "direction": "bullish",
            },
            {
                "id": "b",
                "event_id": "hbm",
                "text": "Same tape, $WDC still quiet",
                "published_at": "2026-08-13T11:00:00+00:00",
                "themes": ["memory"],
                "direction": "bullish",
            },
        ],
    }
    path = tmp_path / "posts.json"
    path.write_text(json.dumps(posts), encoding="utf-8")
    payload = build_world_events_from_posts(
        posts_path=path,
        now=datetime(2026, 8, 13, 12, tzinfo=UTC),
    )
    assert payload["schema_version"] == "world-events-v1"
    assert len(payload["events"]) == 1
    event = payload["events"][0]
    assert "MU" in event["tickers"]
    assert "WDC" in event["tickers"]
    assert event["source_category"] == "social"
    assert event["direction"] == "bullish"


def test_convert_posts_preview_does_not_write(tmp_path: Path) -> None:
    src = Path("data/sample/x_posts.json")
    dest = tmp_path / "world_events.json"
    preview = convert_posts_file(posts_path=src, destination=dest, execute=False)
    assert preview["status"] == "preview"
    assert preview["file_writes_made"] == 0
    assert not dest.exists()
    executed = convert_posts_file(posts_path=src, destination=dest, execute=True)
    assert executed["status"] == "executed"
    assert dest.is_file()
    assert executed["event_count"] >= 1

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from sqlalchemy import create_engine

from catalyst_radar.discovery.bars import import_discovery_bars, write_session_bars_csv
from catalyst_radar.discovery.from_posts import convert_posts_file
from catalyst_radar.discovery.ready import build_discovery_readiness

pytestmark = pytest.mark.discovery


def test_posts_plus_mapped_bars_can_pass_discovery_ready(tmp_path: Path) -> None:
    now = datetime(2026, 8, 13, 18, tzinfo=UTC)
    posts = {
        "schema_version": "x-posts-v1",
        "generated_at": now.isoformat(),
        "source": "loop_test",
        "posts": [
            {
                "id": "p1",
                "event_id": "loop",
                "text": "$AAA and $BBB lag a world event",
                "published_at": "2026-08-12T15:00:00+00:00",
                "direction": "bullish",
                "materiality": 0.7,
                "source_quality": 0.4,
            }
        ],
    }
    posts_path = tmp_path / "posts.json"
    posts_path.write_text(json.dumps(posts), encoding="utf-8")
    events_path = tmp_path / "world_events.json"
    converted = convert_posts_file(
        posts_path=posts_path,
        destination=events_path,
        execute=True,
        now=now,
    )
    assert converted["event_count"] == 1

    bars_path = write_session_bars_csv(
        tmp_path / "bars.csv",
        tickers=["AAA", "BBB", "SPY"],
        end=date(2026, 8, 13),
        sessions=12,
    )
    engine = create_engine("sqlite:///:memory:")
    imported = import_discovery_bars(engine=engine, csv_path=bars_path, execute=True)
    assert imported["db_writes_made"] > 0

    ready = build_discovery_readiness(
        events_path=events_path,
        engine=engine,
        now=now,
        theme_peers_path=None,
    )
    assert ready["freshness_ok"] is True
    assert ready["join_target_met"] is True
    assert ready["ready"] is True
    assert ready["first_blocker"] is None

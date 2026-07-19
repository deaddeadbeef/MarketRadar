from __future__ import annotations

import json
from pathlib import Path

from catalyst_radar.discovery.brief import build_discovery_brief, load_world_events
from catalyst_radar.discovery.mapper import map_event_tickers
from catalyst_radar.discovery.models import WORLD_EVENTS_SCHEMA


def test_load_sample_world_events() -> None:
    path = Path("data/sample/world_events.json")
    bundle = load_world_events(path)
    assert bundle.schema_version == WORLD_EVENTS_SCHEMA
    assert len(bundle.events) >= 3
    assert bundle.events[0].tickers


def test_map_event_tickers_includes_theme_defaults() -> None:
    path = Path("data/sample/world_events.json")
    event = load_world_events(path).events[0]
    mapped = map_event_tickers(event)
    assert "XOM" in mapped["primary_tickers"]
    assert mapped["all_tickers"]
    assert "energy_security" in mapped["theme_hits"] or mapped["primary_tickers"]


def test_build_discovery_brief_zero_calls_research_only() -> None:
    path = Path("data/sample/world_events.json")
    brief = build_discovery_brief(events_path=path, limit=20)
    assert brief["schema_version"] == "discovery-brief-v1"
    assert brief["investment_advice"] is False
    assert brief["can_make_investment_decision"] is False
    assert brief["external_calls_made"] == 0
    assert brief["db_writes_made"] == 0
    assert brief["event_count"] >= 3
    assert brief["discovery_count"] >= 1
    assert all(row["usefulness"] in {"research_only", "watch", "blocked"} for row in brief["discoveries"])
    # Social pilot sources should not produce investment-ready rows.
    assert all(row["usefulness"] != "decision_ready" for row in brief["discoveries"])
    top = brief["discoveries"][0]
    assert top["ticker"]
    assert top["event_id"]
    assert "why_now" in top


def test_build_discovery_brief_rejects_bad_schema(tmp_path: Path) -> None:
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"schema_version": "nope", "events": []}), encoding="utf-8")
    try:
        load_world_events(bad)
        raise AssertionError("expected ValueError")
    except ValueError as exc:
        assert "schema_version" in str(exc)

from __future__ import annotations

import json
from pathlib import Path

from catalyst_radar.discovery.brief import build_discovery_brief
from catalyst_radar.discovery.case_file import build_discovery_case_file
from catalyst_radar.discovery.models import WORLD_EVENTS_SCHEMA
from catalyst_radar.discovery.x_events import (
    X_POSTS_SCHEMA,
    load_x_posts,
    posts_to_world_events,
    write_world_events_from_x_posts,
)

FIXTURE = Path("tests/fixtures/x/sample_posts.json")


def test_load_sample_x_posts() -> None:
    payload = load_x_posts(FIXTURE)
    assert payload["schema_version"] == X_POSTS_SCHEMA
    assert len(payload["posts"]) >= 4


def test_posts_to_world_events_social_x_provider() -> None:
    loaded = load_x_posts(FIXTURE)
    world = posts_to_world_events(loaded, query="semiconductors")
    assert world["schema_version"] == WORLD_EVENTS_SCHEMA
    events = world["events"]
    assert isinstance(events, list)
    assert len(events) >= 1
    for event in events:
        assert event["source_category"] == "social"
        assert event["sources"]
        assert all(src.get("provider") == "x" for src in event["sources"])
        assert 0.0 <= float(event["materiality"]) <= 1.0
        assert 0.2 <= float(event["source_quality"]) <= 0.45
    # Expect semiconductor/memory and energy buckets from fixture.
    theme_blob = " ".join(
        " ".join(event.get("themes") or []) for event in events  # type: ignore[arg-type]
    )
    assert "semiconductor" in theme_blob or "memory" in theme_blob or "energy" in theme_blob
    tickers = {
        ticker
        for event in events
        for ticker in list(event.get("tickers") or []) + list(event.get("secondary_tickers") or [])
    }
    assert "MU" in tickers or "NVDA" in tickers or "XOM" in tickers


def test_write_and_brief_research_only(tmp_path: Path) -> None:
    out = tmp_path / "world_events.json"
    result = write_world_events_from_x_posts(
        FIXTURE,
        out,
        execute=True,
        query="fixture",
    )
    assert result["external_calls_made"] == 0
    assert result["investment_advice"] is False
    assert result["written"] is True
    assert out.is_file()

    brief = build_discovery_brief(events_path=out, engine=None, limit=50)
    assert brief["schema_version"] == "discovery-brief-v1"
    assert brief["investment_advice"] is False
    assert brief["external_calls_made"] == 0
    assert brief["discovery_count"] >= 1
    discoveries = brief["discoveries"]
    assert discoveries
    for row in discoveries:
        assert row["source_category"] == "social"
        assert row["usefulness"] == "research_only"

    # Case file for a ticker from the brief stays research-only / not advice.
    ticker = str(discoveries[0]["ticker"])
    case = build_discovery_case_file(ticker=ticker, events_path=out, engine=None)
    assert case["investment_advice"] is False
    assert case["can_make_investment_decision"] is False
    assert case["external_calls_made"] == 0


def test_cli_discovery_from_x_offline_json(monkeypatch, tmp_path: Path, capsys) -> None:
    # Product path: legacy off still allows active discovery-from-x.
    from catalyst_radar.deprecation import LEGACY_WORKBENCH_ENV
    from catalyst_radar.cli import main

    monkeypatch.delenv(LEGACY_WORKBENCH_ENV, raising=False)
    code = main(
        [
            "discovery-from-x",
            "--posts",
            str(FIXTURE),
            "--json",
        ]
    )
    assert code == 0
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["schema_version"] == "discovery-from-x-result-v1"
    assert payload["external_calls_made"] == 0
    assert payload["investment_advice"] is False
    assert payload["event_count"] >= 1
    assert payload["mode"] == "preview"
    world = payload["world_events"]
    assert world["schema_version"] == WORLD_EVENTS_SCHEMA

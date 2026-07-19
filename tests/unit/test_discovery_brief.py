from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from catalyst_radar.discovery.brief import build_discovery_brief, load_world_events
from catalyst_radar.discovery.ingest import (
    import_world_events_local,
    validate_world_events_file,
)
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
    brief = build_discovery_brief(events_path=path, limit=20, now=datetime(2026, 7, 19, tzinfo=UTC))
    assert brief["schema_version"] == "discovery-brief-v1"
    assert brief["investment_advice"] is False
    assert brief["can_make_investment_decision"] is False
    assert brief["external_calls_made"] == 0
    assert brief["db_writes_made"] == 0
    assert brief["event_count"] >= 3
    assert brief["discovery_count"] >= 1
    assert brief["freshness_status"] == "fresh"
    assert brief["join_coverage"]["no_db"] == brief["discovery_count"]
    assert all(row["usefulness"] in {"research_only", "watch", "blocked"} for row in brief["discoveries"])
    assert all(row["join_status"] == "no_db" for row in brief["discoveries"])
    # Social pilot sources should not produce investment-ready rows.
    assert all(row["usefulness"] != "decision_ready" for row in brief["discoveries"])
    top = brief["discoveries"][0]
    assert top["ticker"]
    assert top["event_id"]
    assert "why_now" in top


def test_build_discovery_brief_marks_stale_events() -> None:
    path = Path("data/sample/world_events.json")
    future = datetime(2026, 7, 19, tzinfo=UTC) + timedelta(hours=48)
    brief = build_discovery_brief(events_path=path, limit=5, now=future)
    assert brief["freshness_status"] == "stale"
    assert float(brief["events_age_hours"]) > 36
    assert "stale" in str(brief["next_action"]).casefold() or "STALE" in str(brief["headline"])


def test_validate_and_import_world_events(tmp_path: Path) -> None:
    src = Path("data/sample/world_events.json")
    validation = validate_world_events_file(src)
    assert validation["valid"] is True
    assert validation["event_count"] >= 3
    dest = tmp_path / "world_events.json"
    preview = import_world_events_local(events_path=src, destination=dest, execute=False)
    assert preview["mode"] == "preview"
    assert preview["db_writes_made"] == 0
    assert not dest.exists()
    executed = import_world_events_local(events_path=src, destination=dest, execute=True)
    assert executed["mode"] == "execute"
    assert executed["db_writes_made"] == 1
    assert dest.is_file()


def test_build_discovery_brief_rejects_bad_schema(tmp_path: Path) -> None:
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"schema_version": "nope", "events": []}), encoding="utf-8")
    try:
        load_world_events(bad)
        raise AssertionError("expected ValueError")
    except ValueError as exc:
        assert "schema_version" in str(exc)


def test_discovery_case_file_research_only() -> None:
    from catalyst_radar.discovery.case_file import build_discovery_case_file

    case = build_discovery_case_file(
        ticker="MU",
        events_path=Path("data/sample/world_events.json"),
        engine=None,
    )
    assert case["schema_version"] == "discovery-case-file-v1"
    assert case["status"] == "ready"
    assert case["ticker"] == "MU"
    assert case["investment_advice"] is False
    assert case["can_make_investment_decision"] is False
    assert case["confirmation"]["status"] in {
        "unconfirmed_social",
        "corroborated_social",
        "unconfirmed",
        "primary_confirmed",
    }
    assert case["invalidation"]
    assert "price_reaction" in case


def test_discovery_row_allowed_in_value_ledger() -> None:
    from catalyst_radar.validation.value_ledger import (
        ALLOWED_ARTIFACT_TYPES,
        build_value_ledger_entry,
    )

    assert "discovery_row" in ALLOWED_ARTIFACT_TYPES
    entry = build_value_ledger_entry(
        artifact_type="discovery_row",
        artifact_id="evt_semi_vol_bear_2026_07:MU",
        label="good-research",
        estimated_value_usd=5.0,
        confidence=0.5,
        source="test",
        ticker="MU",
        supported_action="research",
        user_decision="wait",
    )
    assert entry.artifact_type == "discovery_row"
    assert entry.ticker == "MU"

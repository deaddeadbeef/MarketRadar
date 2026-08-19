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

# 8 posts / 3 event_ids. Do not use data/sample/x_posts.json as the story-count law.
LAW_POSTS = Path("data/sample/x_posts_2026-08-13.json")
LAW_EVENT_IDS = ("hbm4_memory_asp", "china_nand_share", "cpi_hormuz_macro")
LAW_WORLD_EVENT_IDS = tuple(f"evt_{event_id}" for event_id in LAW_EVENT_IDS)
NOW = datetime(2026, 8, 13, 12, tzinfo=UTC)


def _write_posts(tmp_path: Path, posts: list[dict[str, object]]) -> Path:
    path = tmp_path / "posts.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": "x-posts-v1",
                "generated_at": "2026-08-13T12:00:00+00:00",
                "source": "unit",
                "posts": posts,
            }
        ),
        encoding="utf-8",
    )
    return path


def _payload(path: Path) -> dict[str, object]:
    return build_world_events_from_posts(posts_path=path, now=NOW)


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
        now=NOW,
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
    assert executed["external_calls_made"] == 0
    assert preview["external_calls_made"] == 0


def test_convert_posts_require_event_id_rejects_missing_ids(tmp_path: Path) -> None:
    path = _write_posts(
        tmp_path,
        [
            {
                "id": "p1",
                "text": "$MU sold out through 2027",
                "published_at": "2026-08-13T10:00:00+00:00",
                "themes": ["memory"],
            }
        ],
    )
    dest = tmp_path / "world_events.json"
    payload = convert_posts_file(
        posts_path=path,
        destination=dest,
        execute=True,
        require_event_id=True,
    )
    assert payload["status"] == "error"
    assert payload["missing_event_id_count"] == 1
    assert payload["file_writes_made"] == 0
    assert not dest.exists()


def test_convert_posts_counts_missing_event_id_without_failing(tmp_path: Path) -> None:
    path = _write_posts(
        tmp_path,
        [
            {
                "id": "p1",
                "event_id": "hbm4_memory_asp",
                "text": "$MU sold out through 2027",
                "published_at": "2026-08-13T10:00:00+00:00",
                "themes": ["memory"],
            },
            {
                "id": "p2",
                "text": "$SNDK still tight",
                "published_at": "2026-08-13T11:00:00+00:00",
                "themes": ["memory"],
            },
        ],
    )
    payload = convert_posts_file(
        posts_path=path,
        destination=tmp_path / "out.json",
        execute=False,
    )
    assert payload["status"] == "preview"
    assert payload["missing_event_id_count"] == 1


def test_law_fixture_clusters_eight_posts_to_three_world_events() -> None:
    raw = json.loads(LAW_POSTS.read_text(encoding="utf-8"))
    posts = raw["posts"]
    assert raw["source"] == "x_live_curated_2026-08-13"
    assert len(posts) == 8
    assert {str(post["event_id"]) for post in posts} == set(LAW_EVENT_IDS)

    payload = build_world_events_from_posts(
        posts_path=LAW_POSTS,
        now=NOW,
    )
    ids = {event["id"] for event in payload["events"]}
    assert payload["schema_version"] == "world-events-v1"
    assert len(payload["events"]) == 3
    assert len(payload["events"]) != len(posts)
    assert ids == set(LAW_WORLD_EVENT_IDS)


def test_missing_event_id_groups_by_first_theme(tmp_path: Path) -> None:
    path = _write_posts(
        tmp_path,
        [
            {
                "id": "p1",
                "text": "$MU sold out through 2027",
                "published_at": "2026-08-13T10:00:00+00:00",
                "themes": ["memory", "hbm"],
            },
            {
                "id": "p2",
                "text": "$SNDK still described as tight",
                "published_at": "2026-08-13T11:00:00+00:00",
                "themes": ["memory"],
            },
        ],
    )
    payload = build_world_events_from_posts(
        posts_path=path,
        now=NOW,
    )
    assert [event["id"] for event in payload["events"]] == ["evt_memory"]


def test_missing_event_id_and_theme_groups_by_post_id(tmp_path: Path) -> None:
    path = _write_posts(
        tmp_path,
        [
            {
                "id": "solo_a",
                "text": "$MU sold out through 2027",
                "published_at": "2026-08-13T10:00:00+00:00",
                "tickers": ["MU"],
            },
            {
                "id": "solo_b",
                "text": "$XOM slips on inventory",
                "published_at": "2026-08-13T11:00:00+00:00",
                "tickers": ["XOM"],
            },
        ],
    )
    payload = build_world_events_from_posts(
        posts_path=path,
        now=NOW,
    )
    assert {event["id"] for event in payload["events"]} == {"evt_solo_a", "evt_solo_b"}


def test_missing_event_id_theme_and_tickers_drops_group(tmp_path: Path) -> None:
    path = _write_posts(
        tmp_path,
        [
            {
                "id": "empty",
                "text": "No mapped names in this note.",
                "published_at": "2026-08-13T10:00:00+00:00",
            }
        ],
    )
    payload = build_world_events_from_posts(
        posts_path=path,
        now=NOW,
    )
    assert payload["events"] == []


def test_from_posts_picks_title_from_highest_materiality_post(tmp_path: Path) -> None:
    path = _write_posts(
        tmp_path,
        [
            {
                "id": "first",
                "event_id": "hbm",
                "title": "First post is not the story",
                "text": "Low-signal recap of $MU.",
                "published_at": "2026-08-13T10:00:00+00:00",
                "themes": ["memory"],
                "materiality": 0.2,
            },
            {
                "id": "lead",
                "event_id": "hbm",
                "title": "Micron sold out through 2027",
                "text": "$MU HBM is sold out through 2027.",
                "published_at": "2026-08-13T11:00:00+00:00",
                "themes": ["memory"],
                "materiality": 0.9,
            },
        ],
    )
    event = build_world_events_from_posts(posts_path=path, now=NOW)["events"][0]
    assert event["title"] == "Micron sold out through 2027"
    assert event["summary"] == "$MU HBM is sold out through 2027."
    assert "Low-signal recap" not in event["summary"]
    assert "First post" not in event["title"]


def test_from_posts_title_tiebreak_prefers_longer_title(tmp_path: Path) -> None:
    path = _write_posts(
        tmp_path,
        [
            {
                "id": "short",
                "event_id": "hbm",
                "title": "Short MU note",
                "text": "Short text about $MU",
                "published_at": "2026-08-13T10:00:00+00:00",
                "themes": ["memory"],
                "materiality": 0.5,
            },
            {
                "id": "long",
                "event_id": "hbm",
                "title": "Much longer Micron sold-out briefing",
                "text": "The longer title should win the tie.",
                "published_at": "2026-08-13T11:00:00+00:00",
                "themes": ["memory"],
                "materiality": 0.5,
            },
        ],
    )
    event = build_world_events_from_posts(posts_path=path, now=NOW)["events"][0]
    assert event["title"] == "Much longer Micron sold-out briefing"
    assert event["summary"] == "The longer title should win the tie."
    assert "Short text" not in event["summary"]


def test_from_posts_summary_falls_back_to_title_when_text_empty(tmp_path: Path) -> None:
    path = _write_posts(
        tmp_path,
        [
            {
                "id": "noise",
                "event_id": "hbm",
                "title": "Keep this noise",
                "text": "noise tweet about $MU",
                "published_at": "2026-08-13T10:00:00+00:00",
                "themes": ["memory"],
                "materiality": 0.1,
            },
            {
                "id": "lead",
                "event_id": "hbm",
                "title": "Lead story title only",
                "text": "",
                "published_at": "2026-08-13T11:00:00+00:00",
                "themes": ["memory"],
                "materiality": 0.8,
            },
        ],
    )
    event = build_world_events_from_posts(posts_path=path, now=NOW)["events"][0]
    assert event["title"] == "Lead story title only"
    assert event["summary"] == "Lead story title only"


def test_from_posts_truncates_chosen_title_and_summary(tmp_path: Path) -> None:
    path = _write_posts(
        tmp_path,
        [
            {
                "id": "short",
                "event_id": "hbm",
                "title": "Short first title",
                "text": "First tweet body that must not be concatenated.",
                "published_at": "2026-08-13T10:00:00+00:00",
                "themes": ["memory"],
                "materiality": 0.2,
            },
            {
                "id": "lead",
                "event_id": "hbm",
                "title": "T" * 200,
                "text": "S" * 900,
                "published_at": "2026-08-13T11:00:00+00:00",
                "themes": ["memory"],
                "materiality": 0.9,
            },
        ],
    )
    event = build_world_events_from_posts(posts_path=path, now=NOW)["events"][0]
    assert event["title"] == "T" * 180
    assert event["summary"] == "S" * 800
    assert "First tweet body" not in event["summary"]


def test_empty_event_id_falls_back_to_first_theme(tmp_path: Path) -> None:
    path = _write_posts(
        tmp_path,
        [
            {
                "id": "p1",
                "event_id": "",
                "text": "$MU sold out through 2027",
                "published_at": "2026-08-13T10:00:00+00:00",
                "themes": ["memory"],
            },
            {
                "id": "p2",
                "event_id": "",
                "text": "$SNDK still described as tight",
                "published_at": "2026-08-13T11:00:00+00:00",
                "themes": ["memory"],
            },
        ],
    )
    assert [event["id"] for event in _payload(path)["events"]] == ["evt_memory"]


def test_empty_event_id_without_theme_groups_by_post_id(tmp_path: Path) -> None:
    path = _write_posts(
        tmp_path,
        [
            {
                "id": "solo_a",
                "event_id": "",
                "text": "$MU sold out through 2027",
                "published_at": "2026-08-13T10:00:00+00:00",
                "tickers": ["MU"],
            }
        ],
    )
    assert [event["id"] for event in _payload(path)["events"]] == ["evt_solo_a"]


def test_different_event_ids_do_not_merge_on_shared_theme(tmp_path: Path) -> None:
    path = _write_posts(
        tmp_path,
        [
            {
                "id": "p1",
                "event_id": "hbm4_memory_asp",
                "text": "$MU sold out through 2027",
                "published_at": "2026-08-13T10:00:00+00:00",
                "themes": ["memory"],
            },
            {
                "id": "p2",
                "event_id": "china_nand_share",
                "text": "$SNDK NAND share commentary",
                "published_at": "2026-08-13T11:00:00+00:00",
                "themes": ["memory"],
            },
        ],
    )
    assert {event["id"] for event in _payload(path)["events"]} == {
        "evt_hbm4_memory_asp",
        "evt_china_nand_share",
    }


def test_different_first_themes_stay_split_despite_shared_later_theme(
    tmp_path: Path,
) -> None:
    path = _write_posts(
        tmp_path,
        [
            {
                "id": "p1",
                "text": "$MU sold out through 2027",
                "published_at": "2026-08-13T10:00:00+00:00",
                "themes": ["memory", "china_export"],
                "tickers": ["MU", "SNDK"],
            },
            {
                "id": "p2",
                "text": "$WDC NAND share commentary",
                "published_at": "2026-08-13T11:00:00+00:00",
                "themes": ["china_export", "memory"],
                "tickers": ["MU", "WDC"],
            },
        ],
    )
    assert {event["id"] for event in _payload(path)["events"]} == {
        "evt_memory",
        "evt_china_export",
    }


def test_no_stripped_live_dump_fixture_with_false_count_three() -> None:
    assert not Path("data/sample/x_posts_no_event_id.json").exists()


def test_asco_intismeran_teaching_fixture_is_one_pending_binary() -> None:
    path = Path("data/sample/x_posts_2026-06-02_asco_intismeran.json")
    payload = build_world_events_from_posts(
        posts_path=path,
        now=datetime(2026, 6, 2, 18, tzinfo=UTC),
    )
    events = payload["events"]
    assert len(events) == 1
    event = events[0]
    assert event["id"] == "evt_mrna_intismeran_p3_window"
    assert "MRNA" in event["tickers"]
    assert "MRK" in event["tickers"]
    assert "Phase 3" in event["title"] or "Phase 3" in event["summary"]
    assert "+130" not in event["title"]
    assert "+130" not in event["summary"]

from __future__ import annotations

import importlib.util
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from catalyst_radar.discovery.brief import (
    LOCAL_EVENTS_PATH,
    SAMPLE_EVENTS_PATH,
    build_discovery_brief,
    classify_events_path,
    default_events_path,
)
from catalyst_radar.discovery.from_posts import build_world_events_from_posts
from catalyst_radar.discovery.mapper import DEFAULT_THEME_TICKERS
from catalyst_radar.discovery.ux import (
    COMPANY_NAMES,
    NOVICE_LIMIT,
    apply_novice_case_file,
    apply_novice_ux,
    company_name,
    is_novice_eligible,
)

pytestmark = pytest.mark.discovery

# 8 posts / 3 event_ids. Do not use data/sample/x_posts.json as the story-count law.
LAW_POSTS = Path("data/sample/x_posts_2026-08-13.json")
LAW_WORLD_EVENT_IDS = {
    "evt_hbm4_memory_asp",
    "evt_china_nand_share",
    "evt_cpi_hormuz_macro",
}
VISIBLE_JARGON = ("missing_scan", "powershell", "paper path", "preview good-research")
FRONTEND = Path("apps/radar-desktop/frontend/app.js")


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


def test_every_default_theme_ticker_has_display_name() -> None:
    tickers = {ticker for values in DEFAULT_THEME_TICKERS.values() for ticker in values}
    assert tickers
    for ticker in sorted(tickers):
        name = company_name(ticker)
        assert ticker in COMPANY_NAMES
        assert name == COMPANY_NAMES[ticker]
        assert name != ticker or COMPANY_NAMES[ticker] == ticker


def test_unlisted_cashtags_stay_off_the_novice_eight() -> None:
    assert is_novice_eligible("SKHY") is False
    assert is_novice_eligible("CXMT") is False
    assert is_novice_eligible("ZZZZ") is False
    assert is_novice_eligible("DRAM") is True
    assert company_name("DRAM") == "Roundhill Memory ETF"

    discoveries = [
        {
            "ticker": "SKHY",
            "origin": "event",
            "join_status": "missing_scan",
            "discovery_score": 99,
            "event_title": "HBM",
        },
        {
            "ticker": "CXMT",
            "origin": "event",
            "join_status": "missing_scan",
            "discovery_score": 98,
            "event_title": "HBM",
        },
        {
            "ticker": "ZZZZ",
            "origin": "event",
            "join_status": "missing_scan",
            "discovery_score": 97,
            "event_title": "HBM",
        },
        {
            "ticker": "DRAM",
            "origin": "event",
            "join_status": "missing_scan",
            "discovery_score": 50,
            "event_title": "HBM",
        },
        {
            "ticker": "MU",
            "origin": "event",
            "join_status": "missing_scan",
            "discovery_score": 40,
            "event_title": "HBM",
        },
        {
            "ticker": "SNDK",
            "origin": "event",
            "join_status": "joined",
            "discovery_score": 30,
            "event_title": "HBM",
            "ret_5d_pct": -0.2,
            "quiet_tape": True,
        },
    ]
    payload = apply_novice_ux(
        {
            "freshness_status": "fresh",
            "events": [{"id": "e1", "title": "HBM", "materiality": 0.7, "sources": [{}]}],
            "discoveries": discoveries,
        }
    )
    tickers = [row["ticker"] for row in payload["discoveries"]]
    assert "SKHY" not in tickers
    assert "CXMT" not in tickers
    assert "ZZZZ" not in tickers
    assert "DRAM" in tickers
    assert "MU" in tickers
    assert "SNDK" in tickers
    assert len(tickers) <= NOVICE_LIMIT


def test_novice_events_rank_and_cap_eight_never_pad() -> None:
    events = [
        {
            "id": f"e{i}",
            "title": f"Story {i}",
            "materiality": 0.1 + (i % 10) * 0.05,
            "sources": [{}] * (1 + (i % 3)),
            "tickers": ["MU"],
        }
        for i in range(40)
    ]
    payload = apply_novice_ux(
        {"freshness_status": "fresh", "events": events, "discoveries": []}
    )
    stories = payload["novice"]["events"]
    assert len(stories) == 8
    assert len(stories) <= min(8, len(events))
    assert "8 stories" in payload["headline"]
    assert "40 stor" not in payload["headline"]

    def rank(event: dict[str, object]) -> float:
        return float(event["materiality"]) * max(1, len(event["sources"]))  # type: ignore[arg-type]

    expected_ids = [row["id"] for row in sorted(events, key=rank, reverse=True)[:8]]
    assert [row["id"] for row in stories] == expected_ids

    three = apply_novice_ux(
        {"freshness_status": "fresh", "events": events[:3], "discoveries": []}
    )
    assert len(three["novice"]["events"]) == 3
    assert len(three["novice"]["events"]) <= min(8, 3)


def test_novice_picker_can_keep_ranks_thirteen_to_twenty() -> None:
    symbols = [
        "MU",
        "SNDK",
        "WDC",
        "STX",
        "NVDA",
        "TSM",
        "ASML",
        "AMAT",
        "LRCX",
        "KLAC",
        "AVGO",
        "INTC",
        "AMD",
        "QCOM",
        "DELL",
        "HPQ",
        "SONY",
        "AAPL",
        "CAT",
        "DE",
    ]
    discoveries = [
        {
            "ticker": symbol,
            "origin": "event",
            "join_status": "joined" if index >= 13 else "missing_scan",
            "discovery_score": float(index),
            "event_title": "Memory",
            "ret_5d_pct": 0.1,
        }
        for index, symbol in enumerate(symbols, start=1)
    ]
    payload = apply_novice_ux(
        {
            "freshness_status": "fresh",
            "events": [{"id": "e", "title": "Memory"}],
            "discoveries": discoveries,
        }
    )
    tickers = [row["ticker"] for row in payload["discoveries"]]
    assert len(tickers) == 8
    assert set(tickers) == {"AMD", "QCOM", "DELL", "HPQ", "SONY", "AAPL", "CAT", "DE"}


def test_novice_visible_copy_has_no_operator_jargon() -> None:
    payload = apply_novice_ux(
        {
            "freshness_status": "fresh",
            "events": [
                {
                    "id": "e1",
                    "title": "Memory prices jump",
                    "summary": "Micron is in the story.",
                    "tickers": ["MU"],
                }
            ],
            "discoveries": [
                {
                    "ticker": "MU",
                    "origin": "event",
                    "join_status": "joined",
                    "discovery_score": 70,
                    "event_title": "Memory prices jump",
                    "ret_5d_pct": -0.4,
                    "quiet_tape": True,
                }
            ],
            "case_file": {
                "status": "ready",
                "ticker": "MU",
                "company_name": "Micron",
                "next_action": (
                    "Import recent bars and rescan; optional paper path only after "
                    "policy allows; then preview good-research."
                ),
            },
        }
    )
    case = apply_novice_case_file(
        {
            "status": "ready",
            "ticker": "MU",
            "company_name": "Micron",
            "next_action": "Local scan join missing_scan. Use powershell.",
        }
    )
    blobs = _visible_novice_copy(payload) + [str(case["next_action"])]
    _assert_no_visible_jargon(blobs)
    assert "Open Micron" in str(payload["case_file"]["next_action"])
    assert "news site" in str(case["next_action"])


def test_cli_default_events_path_falls_back_to_sample_when_local_missing() -> None:
    assert not LOCAL_EVENTS_PATH.is_file()
    assert SAMPLE_EVENTS_PATH.is_file()
    path = default_events_path()
    assert path == SAMPLE_EVENTS_PATH
    assert classify_events_path(path) == "fixture"
    brief = build_discovery_brief(events_path=path, limit=5)
    assert brief["event_count"] >= 1
    assert brief["events"]
    desktop_path = default_events_path(allow_sample=False)
    assert desktop_path == LOCAL_EVENTS_PATH
    assert classify_events_path(desktop_path) == "missing"
    assert classify_events_path(SAMPLE_EVENTS_PATH) == "fixture"
    assert classify_events_path(Path("data/local/does-not-exist.json")) == "missing"


def test_desktop_snapshot_default_path_is_empty_when_local_missing(capsys) -> None:
    assert not LOCAL_EVENTS_PATH.is_file()
    snapshot = _load_discovery_snapshot()
    code = snapshot.main(["--json", "--scan-limit", "20"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    brief = payload["event_discovery"]
    assert payload["events_path_kind"] == "missing"
    assert brief["events_path_kind"] == "missing"
    assert brief["events"] == []
    assert brief.get("event_count", 0) == 0
    assert "Hormuz" not in str(brief.get("headline") or "")
    assert "No stories loaded yet" in str(brief["headline"])


def test_desktop_snapshot_missing_local_is_empty(tmp_path: Path, capsys) -> None:
    missing = tmp_path / "no-world-events.json"
    snapshot = _load_discovery_snapshot()
    code = snapshot.main(["--events", str(missing), "--json", "--scan-limit", "20"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    brief = payload["event_discovery"]
    assert payload["events_path_kind"] == "missing"
    assert brief["events_path_kind"] == "missing"
    assert brief["events"] == []
    assert brief["novice"]["events"] == []
    assert "No stories loaded yet" in str(brief["headline"])
    _assert_no_visible_jargon(_visible_novice_copy(brief) + [str(payload.get("next_action") or "")])


def test_desktop_snapshot_refuses_sample_fixture(capsys) -> None:
    snapshot = _load_discovery_snapshot()
    code = snapshot.main(
        ["--events", str(SAMPLE_EVENTS_PATH), "--json", "--scan-limit", "20"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    brief = payload["event_discovery"]
    assert payload["events_path_kind"] == "fixture"
    assert brief["events"] == []
    assert brief.get("event_count", 0) == 0
    assert "Hormuz" not in str(brief.get("headline") or "")
    assert "No stories loaded yet" in str(brief["headline"])


def test_desktop_snapshot_overwrites_case_next_action(tmp_path: Path, capsys) -> None:
    events_path = tmp_path / "world_events.json"
    events_path.write_text(
        json.dumps(
            {
                "schema_version": "world-events-v1",
                "generated_at": "2026-08-13T12:00:00+00:00",
                "source": "unit",
                "events": [
                    {
                        "id": "evt_memory",
                        "title": "Memory prices jump",
                        "summary": "Micron is in the story.",
                        "themes": ["memory"],
                        "tickers": ["MU"],
                        "secondary_tickers": [],
                        "direction": "bullish",
                        "materiality": 0.8,
                        "source_quality": 0.4,
                        "source_category": "social",
                        "sources": [{"provider": "x", "url": None, "author": None}],
                        "available_at": "2026-08-13T12:00:00+00:00",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    snapshot = _load_discovery_snapshot()
    code = snapshot.main(
        [
            "--events",
            str(events_path),
            "--ticker",
            "MU",
            "--json",
            "--scan-limit",
            "20",
        ]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    brief = payload["event_discovery"]
    assert payload["events_path_kind"] == "local"
    case = brief["case_file"]
    assert case["status"] == "ready"
    assert "paper path" not in str(case["next_action"]).casefold()
    assert "missing_scan" not in str(case["next_action"]).casefold()
    assert "preview good-research" not in str(case["next_action"]).casefold()
    assert "news site" in str(case["next_action"]).casefold()
    _assert_no_visible_jargon(
        _visible_novice_copy(brief) + [str(case["next_action"]), str(payload.get("next_action") or "")]
    )


def test_world_events_frontend_hides_preview_and_uses_scan_limit_20() -> None:
    source = FRONTEND.read_text(encoding="utf-8")
    assert "scan_limit: isDiscoveryHome() ? 20" in source
    assert "scan_limit: isDiscoveryHome() ? 12" not in source
    assert "preview ${escapeHtml(suggested)}" not in source
    assert "preview good-research" not in source.casefold()
    assert "This helped" in source
    assert "This was noise" in source
    assert 'data-label="${escapeHtml(value)}"' in source
    buttons = source[source.index("function renderCaseLabelButtons") :]
    buttons = buttons[: buttons.index("function fallbackCaseChips")]
    _assert_no_visible_jargon([buttons])


def _visible_novice_copy(payload: dict[str, object]) -> list[str]:
    novice = payload.get("novice") if isinstance(payload.get("novice"), dict) else {}
    case = payload.get("case_file") if isinstance(payload.get("case_file"), dict) else {}
    blobs = [
        str(payload.get("headline") or ""),
        str(payload.get("next_action") or ""),
        str(novice.get("tagline") or ""),
        str(novice.get("disclaimer") or ""),
        str(case.get("next_action") or ""),
        str(case.get("price_detail") or ""),
    ]
    for event in novice.get("events") or []:
        if isinstance(event, dict):
            blobs.extend([str(event.get("title") or ""), str(event.get("summary") or "")])
    for lead in novice.get("leads") or []:
        if isinstance(lead, dict):
            blobs.extend(
                [
                    str(lead.get("name") or ""),
                    str(lead.get("why") or ""),
                    str(lead.get("price_line") or ""),
                    str(lead.get("status_line") or ""),
                ]
            )
    return blobs


def _assert_no_visible_jargon(blobs: list[str]) -> None:
    haystack = "\n".join(blobs).casefold()
    for token in VISIBLE_JARGON:
        assert token not in haystack


def _load_discovery_snapshot():
    path = Path("scripts/discovery-snapshot.py")
    spec = importlib.util.spec_from_file_location("discovery_snapshot_pr6", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

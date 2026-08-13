from __future__ import annotations

import pytest

from catalyst_radar.discovery.ux import apply_novice_ux, company_name

pytestmark = pytest.mark.discovery


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

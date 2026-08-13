"""Plain-English discovery copy for first-time market users."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

COMPANY_NAMES: dict[str, str] = {
    "AAPL": "Apple",
    "AMD": "AMD",
    "AMAT": "Applied Materials",
    "ASML": "ASML",
    "AVGO": "Broadcom",
    "BKR": "Baker Hughes",
    "CAT": "Caterpillar",
    "COP": "ConocoPhillips",
    "CVX": "Chevron",
    "CXMT": "ChangXin Memory",
    "DELL": "Dell",
    "DRAM": "Roundhill Memory ETF",
    "EURN": "Euronav",
    "F": "Ford",
    "FRO": "Frontline",
    "GLD": "Gold ETF",
    "GM": "General Motors",
    "GOGL": "Golden Ocean",
    "INTC": "Intel",
    "KLAC": "KLA",
    "LMT": "Lockheed Martin",
    "LRCX": "Lam Research",
    "MU": "Micron",
    "NOC": "Northrop Grumman",
    "NVDA": "Nvidia",
    "OXY": "Occidental",
    "QCOM": "Qualcomm",
    "RTX": "RTX",
    "SKHY": "SK hynix",
    "SLB": "Schlumberger",
    "SNDK": "Sandisk",
    "SONY": "Sony",
    "SPY": "S&P 500 ETF",
    "STX": "Seagate",
    "TSLA": "Tesla",
    "TSM": "TSMC",
    "WDC": "Western Digital",
    "XOM": "Exxon Mobil",
}

NOVICE_LIMIT = 8


def company_name(ticker: str) -> str:
    symbol = str(ticker or "").strip().upper()
    return COMPANY_NAMES.get(symbol, symbol)


def apply_novice_ux(brief: Mapping[str, Any]) -> dict[str, Any]:
    """Return a brief copy shaped for a first-time user."""
    payload = dict(brief)
    events = [row for row in (brief.get("events") or []) if isinstance(row, Mapping)]
    raw_leads = [row for row in (brief.get("discoveries") or []) if isinstance(row, Mapping)]
    leads = _pick_novice_leads(raw_leads)
    top = leads[0] if leads else None
    freshness = str(brief.get("freshness_status") or "unknown")
    payload["discoveries"] = leads
    payload["discovery_count"] = len(leads)
    payload["headline"] = _headline(events, top, freshness)
    payload["next_action"] = _next_action(freshness, top)
    payload["next_command"] = "Press R to refresh this briefing."
    payload["canonical_next_action"] = payload["next_action"]
    payload["canonical_next_command"] = payload["next_command"]
    payload["novice"] = {
        "schema_version": "discovery-novice-v1",
        "tagline": "Stories from X, then which stocks have not moved much yet.",
        "disclaimer": (
            "This is a research briefing, not a shopping list and not investment advice. "
            "Check a real news site before you do anything with money."
        ),
        "events": [_event_card(event) for event in events],
        "leads": [_lead_card(row) for row in leads],
        "focus_ticker": str((top or {}).get("ticker") or ""),
    }
    return payload


def _pick_novice_leads(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    scored: list[dict[str, Any]] = []
    for row in rows:
        ticker = str(row.get("ticker") or "").strip().upper()
        if not ticker or ticker in {"SPY", "QQQ", "IWM"}:
            continue
        origin = str(row.get("origin") or "event")
        join = str(row.get("join_status") or "")
        if origin == "theme" and join != "joined":
            continue
        if join == "missing_scan" and origin != "event":
            continue
        item = dict(row)
        card = _lead_card(item)
        item.update(card)
        item["_rank"] = (
            2 if join == "joined" and origin == "event" else
            1 if join == "joined" else
            0
        )
        scored.append(item)
    scored.sort(
        key=lambda row: (
            int(row.get("_rank") or 0),
            float(row.get("discovery_score") or 0.0),
        ),
        reverse=True,
    )
    cleaned: list[dict[str, Any]] = []
    for row in scored[:NOVICE_LIMIT]:
        row.pop("_rank", None)
        cleaned.append(row)
    return cleaned


def _headline(
    events: Sequence[Mapping[str, Any]],
    top: Mapping[str, Any] | None,
    freshness: str,
) -> str:
    if freshness == "stale":
        return "This briefing is out of date. Refresh the news feed, then come back."
    if not events:
        return "No stories loaded yet. Add today's world events to get started."
    count = len(events)
    if top is None:
        return f"{count} story from X today." if count == 1 else f"{count} stories from X today."
    name = company_name(str(top.get("ticker") or ""))
    move = _price_phrase(top.get("ret_5d_pct"), joined=str(top.get("join_status")) == "joined")
    return f"{count} stories from X. {name} {move}."


def _next_action(freshness: str, top: Mapping[str, Any] | None) -> str:
    if freshness == "stale":
        return "Refresh today's stories, then open the first name in the list."
    if top is None:
        return "When stories appear, start with the first name and read why it showed up."
    name = company_name(str(top.get("ticker") or ""))
    return (
        f"Open {name}, read the short explanation, then check a regular news site "
        "before you make any money decision."
    )


def _event_card(event: Mapping[str, Any]) -> dict[str, object]:
    direction = str(event.get("direction") or "mixed")
    mood = {"bullish": "Upbeat", "bearish": "Cautious", "mixed": "Mixed"}.get(direction, "Mixed")
    tickers = [
        str(item).upper()
        for item in (event.get("tickers") or [])
        if str(item).strip()
    ]
    names = [company_name(ticker) for ticker in tickers[:5]]
    return {
        "id": event.get("id"),
        "title": event.get("title"),
        "summary": event.get("summary"),
        "mood": mood,
        "direction": direction,
        "names": names,
        "tickers": tickers,
    }


def _lead_card(row: Mapping[str, Any]) -> dict[str, object]:
    ticker = str(row.get("ticker") or "").upper()
    joined = str(row.get("join_status")) == "joined"
    ret = row.get("ret_5d_pct")
    return {
        "ticker": ticker,
        "name": company_name(ticker),
        "event_title": row.get("event_title"),
        "why": _why_plain(row),
        "price_line": _price_phrase(ret, joined=joined),
        "price_detail": _price_detail(ret, joined=joined),
        "status_line": _status_line(row),
        "joined": joined,
        "origin": row.get("origin") or "event",
    }


def _why_plain(row: Mapping[str, Any]) -> str:
    title = str(row.get("event_title") or "a current world story").strip()
    name = company_name(str(row.get("ticker") or ""))
    return f"{name} is tied to: {title}"


def _price_phrase(ret_5d: object, *, joined: bool) -> str:
    if not joined or ret_5d is None:
        return "does not have a clear recent price read yet"
    try:
        value = float(ret_5d)
    except (TypeError, ValueError):
        return "does not have a clear recent price read yet"
    if abs(value) < 1.5:
        return "has barely moved this week"
    direction = "up" if value > 0 else "down"
    return f"is {direction} about {abs(value):.1f}% this week"


def _price_detail(ret_5d: object, *, joined: bool) -> str:
    if not joined or ret_5d is None:
        return "We do not have enough recent prices to say what the stock did."
    try:
        value = float(ret_5d)
    except (TypeError, ValueError):
        return "We do not have enough recent prices to say what the stock did."
    sign = "+" if value >= 0 else "−"
    return f"About {sign}{abs(value):.1f}% over the last five trading days."


def _status_line(row: Mapping[str, Any]) -> str:
    if str(row.get("join_status")) != "joined":
        return "Need prices before we can say whether this looks late or early."
    if row.get("quiet_tape"):
        return "The stock has not moved much compared with how loud the story is."
    return "The stock already moved some. Read before you treat this as 'late' or 'early'."

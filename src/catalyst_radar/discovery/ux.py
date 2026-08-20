"""Plain-English discovery copy for first-time market users."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

COMPANY_NAMES: dict[str, str] = {
    "AAPL": "Apple",
    "AEM": "Agnico Eagle",
    "ALB": "Albemarle",
    "AMAT": "Applied Materials",
    "AMD": "AMD",
    "AMGN": "Amgen",
    "ANET": "Arista",
    "ASML": "ASML",
    "AVGO": "Broadcom",
    "BKR": "Baker Hughes",
    "BNTX": "BioNTech",
    "CAT": "Caterpillar",
    "CEG": "Constellation Energy",
    "CIEN": "Ciena",
    "COHR": "Coherent",
    "COP": "ConocoPhillips",
    "CRDO": "Credo",
    "CVX": "Chevron",
    "CXMT": "ChangXin Memory",
    "DAC": "Danaos",
    "DE": "Deere",
    "DELL": "Dell",
    "DRAM": "Roundhill Memory ETF",
    "EMR": "Emerson",
    "ETN": "Eaton",
    "EURN": "Euronav",
    "F": "Ford",
    "FDX": "FedEx",
    "FRO": "Frontline",
    "GD": "General Dynamics",
    "GILD": "Gilead",
    "GLD": "Gold ETF",
    "GLW": "Corning",
    "GM": "General Motors",
    "GOGL": "Golden Ocean",
    "HAL": "Halliburton",
    "HII": "Huntington Ingalls",
    "HPQ": "HP",
    "HUBB": "Hubbell",
    "INTC": "Intel",
    "KLAC": "KLA",
    "LHX": "L3Harris",
    "LITE": "Lumentum",
    "LMT": "Lockheed Martin",
    "LRCX": "Lam Research",
    "MPC": "Marathon Petroleum",
    "MRK": "Merck",
    "MRNA": "Moderna",
    "MRVL": "Marvell",
    "MU": "Micron",
    "NEM": "Newmont",
    "NOC": "Northrop Grumman",
    "NVDA": "Nvidia",
    "OXY": "Occidental",
    "PFE": "Pfizer",
    "PSX": "Phillips 66",
    "PWR": "Quanta",
    "QCOM": "Qualcomm",
    "REGN": "Regeneron",
    "RTX": "RTX",
    "SBLK": "Star Bulk",
    "SKHY": "SK hynix",
    "SLB": "Schlumberger",
    "SMCI": "Super Micro",
    "SNDK": "Sandisk",
    "SONY": "Sony",
    "SPY": "S&P 500 ETF",
    "STNG": "Scorpio Tankers",
    "STX": "Seagate",
    "TSLA": "Tesla",
    "TSM": "TSMC",
    "UPS": "UPS",
    "VLO": "Valero",
    "VRT": "Vertiv",
    "WDC": "Western Digital",
    "XOM": "Exxon Mobil",
    "ZIM": "ZIM",
}

NOVICE_LIMIT = 8
# Unlisted / non-US-common cashtags stay on operator JSON; they do not consume the eight.
NOVICE_UNLISTED: frozenset[str] = frozenset({"SKHY", "CXMT"})


def company_name(ticker: str) -> str:
    symbol = str(ticker or "").strip().upper()
    return COMPANY_NAMES.get(symbol, symbol)


def is_novice_eligible(ticker: str) -> bool:
    """Keep names the newbie can recognize; drop unlisted cashtags."""
    symbol = str(ticker or "").strip().upper()
    if not symbol or symbol in {"SPY", "QQQ", "IWM"}:
        return False
    if symbol in NOVICE_UNLISTED:
        return False
    return symbol in COMPANY_NAMES


def apply_novice_ux(brief: Mapping[str, Any]) -> dict[str, Any]:
    """Return a brief copy shaped for a first-time user."""
    payload = dict(brief)
    raw_events = [row for row in (brief.get("events") or []) if isinstance(row, Mapping)]
    events = _pick_novice_events(raw_events)
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
    if isinstance(payload.get("case_file"), Mapping):
        payload["case_file"] = apply_novice_case_file(payload["case_file"])
    payload["novice"] = {
        "schema_version": "discovery-novice-v1",
        "tagline": "Grok mines X; this screen is the briefing.",
        "disclaimer": (
            "This is a research briefing, not a shopping list and not investment advice. "
            "Check a real news site before you do anything with money."
        ),
        "events": [_event_card(event) for event in events],
        "leads": [_lead_card(row) for row in leads],
        "focus_ticker": str((top or {}).get("ticker") or ""),
    }
    return payload


def apply_novice_case_file(case: Mapping[str, Any]) -> dict[str, Any]:
    """Overwrite mounted case_file.next_action with newbie English."""
    payload = dict(case)
    ticker = str(payload.get("ticker") or "").strip().upper()
    name = str(payload.get("company_name") or "").strip() or company_name(ticker)
    if str(payload.get("status") or "") != "ready" or not name:
        payload["next_action"] = (
            "When stories appear, start with the first name and read why it showed up."
        )
        return payload
    payload["next_action"] = (
        f"Open {name}, read the short explanation, then check a regular news site "
        "before you make any money decision."
    )
    return payload


def _pick_novice_events(events: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    scored = [dict(event) for event in events]
    scored.sort(key=_event_rank_score, reverse=True)
    return scored[:NOVICE_LIMIT]


def _event_rank_score(event: Mapping[str, Any]) -> float:
    try:
        materiality = float(event.get("materiality") or 0.0)
    except (TypeError, ValueError):
        materiality = 0.0
    sources = event.get("sources")
    if isinstance(sources, Sequence) and not isinstance(sources, (str, bytes)):
        source_count = len(sources)
    else:
        source_count = 0
    return materiality * max(1, source_count)


def _pick_novice_leads(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    scored: list[dict[str, Any]] = []
    for row in rows:
        ticker = str(row.get("ticker") or "").strip().upper()
        if not ticker or not is_novice_eligible(ticker):
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
    story_word = "story" if count == 1 else "stories"
    if top is None:
        return f"{count} {story_word} from X today."
    name = company_name(str(top.get("ticker") or ""))
    move = _price_phrase(top.get("ret_5d_pct"), joined=str(top.get("join_status")) == "joined")
    return f"{count} {story_word} from X. {name} {move}."


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

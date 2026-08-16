from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy.engine import Engine

from catalyst_radar.discovery.join import (
    join_event_ticker,
    load_bars_by_ticker,
    missing_join,
    no_db_join,
)
from catalyst_radar.discovery.mapper import (
    MEGA_CAP_TICKERS,
    load_theme_ticker_map,
    map_event_tickers,
)
from catalyst_radar.discovery.models import (
    DISCOVERY_BRIEF_SCHEMA,
    WORLD_EVENTS_SCHEMA,
    EventSource,
    WorldEvent,
    WorldEventBundle,
    clamp_score,
    normalize_themes,
    normalize_tickers,
    parse_datetime,
)

DEFAULT_EVENTS_PATH = Path("data/sample/world_events.json")
LOCAL_EVENTS_PATH = Path("data/local/world_events.json")
FRESHNESS_STALE_HOURS = 24.0
DISCOVERY_BARS_NEXT_COMMAND = (
    "catalyst-radar discovery-bars --polygon --confirm-external-call"
)


def load_world_events(path: str | Path) -> WorldEventBundle:
    file_path = Path(path)
    raw = json.loads(file_path.read_text(encoding="utf-8"))
    if not isinstance(raw, Mapping):
        msg = "world events file must be a JSON object"
        raise ValueError(msg)
    schema = str(raw.get("schema_version") or WORLD_EVENTS_SCHEMA)
    if schema != WORLD_EVENTS_SCHEMA:
        msg = f"unsupported world events schema_version: {schema}"
        raise ValueError(msg)
    events_raw = raw.get("events")
    if not isinstance(events_raw, list):
        msg = "world events file must include an events list"
        raise ValueError(msg)
    events = tuple(_parse_event(item) for item in events_raw)
    return WorldEventBundle(
        schema_version=schema,
        generated_at=parse_datetime(raw.get("generated_at"), "generated_at"),
        source=str(raw.get("source") or "unknown"),
        events=events,
    )


def build_discovery_brief(
    *,
    events_path: str | Path = DEFAULT_EVENTS_PATH,
    theme_peers_path: str | Path | None = Path("config/theme_peers.yaml"),
    engine: Engine | None = None,
    limit: int = 25,
    now: datetime | None = None,
) -> dict[str, object]:
    bundle = load_world_events(events_path)
    theme_map = load_theme_ticker_map(theme_peers_path)
    db_enabled = engine is not None
    clock = now if now is not None else datetime.now(tz=UTC)
    if clock.tzinfo is None:
        clock = clock.replace(tzinfo=UTC)
    else:
        clock = clock.astimezone(UTC)

    event_rows: list[dict[str, object]] = []
    discoveries: list[dict[str, object]] = []
    mapped_tickers: list[str] = []
    mapped_by_event: list[tuple[object, Mapping[str, object], float]] = []

    for event in bundle.events:
        mapped = map_event_tickers(event, theme_ticker_map=theme_map)
        event_payload = event.as_payload()
        event_payload["mapped_tickers"] = mapped
        event_rows.append(event_payload)

        emotion = _event_emotion_score(event)
        all_tickers = list(mapped["all_tickers"])  # type: ignore[arg-type]
        for ticker in all_tickers:
            if ticker not in mapped_tickers:
                mapped_tickers.append(ticker)
        mapped_by_event.append((event, mapped, emotion))

    bars_by_ticker: dict[str, list] = {}
    if db_enabled:
        try:
            bars_by_ticker = load_bars_by_ticker(
                engine,
                mapped_tickers,
                end=clock.date(),
            )
        except Exception:
            bars_by_ticker = {}

    for event, mapped, emotion in mapped_by_event:
        all_tickers = list(mapped["all_tickers"])  # type: ignore[arg-type]
        for ticker in all_tickers:
            if ticker in event.tickers:
                origin = "event"
            elif ticker in event.secondary_tickers:
                origin = "event_secondary"
            else:
                origin = "theme"
            role = "primary" if origin == "event" else "secondary"
            if not db_enabled:
                event_join = no_db_join(emotion_score=emotion)
            elif not bars_by_ticker:
                event_join = missing_join(
                    reason="Local bar load failed or returned no mapped rows.",
                    emotion_score=emotion,
                )
            else:
                event_join = join_event_ticker(
                    ticker=str(ticker),
                    event_available_at=event.available_at,
                    event_direction=event.direction,
                    emotion_score=emotion,
                    bars_by_ticker=bars_by_ticker,
                    now=clock,
                )
            discovery = _discovery_row(
                event=event,
                ticker=ticker,
                role=role,
                origin=origin,
                emotion_score=emotion,
                event_join=event_join,
            )
            discoveries.append(discovery)

    discoveries = _dedupe_best_per_ticker(discoveries)
    discoveries.sort(
        key=lambda row: (
            1 if row.get("join_status") == "joined" else 0,
            1 if row.get("quiet_tape") else 0,
            float(row.get("discovery_score") or 0.0),
            float(row.get("emotion_reaction_gap") or 0.0),
            float(row.get("materiality") or 0.0),
        ),
        reverse=True,
    )
    discoveries = discoveries[: max(1, int(limit))]

    research_count = sum(1 for row in discoveries if row.get("usefulness") == "research_only")
    watch_count = sum(1 for row in discoveries if row.get("usefulness") == "watch")
    blocked_count = sum(1 for row in discoveries if row.get("usefulness") == "blocked")
    joined_count = sum(1 for row in discoveries if row.get("join_status") == "joined")
    missing_scan_count = sum(
        1 for row in discoveries if row.get("join_status") == "missing_scan"
    )
    quiet_tape_count = sum(1 for row in discoveries if row.get("quiet_tape") is True)

    age_hours = max(
        0.0,
        (clock - bundle.generated_at.astimezone(UTC)).total_seconds() / 3600.0,
    )
    freshness_status = (
        "stale" if age_hours > FRESHNESS_STALE_HOURS else "fresh"
    )
    missing_sample = [
        str(row.get("ticker"))
        for row in discoveries
        if row.get("join_status") == "missing_scan"
    ][:12]
    join_coverage_pct = round(
        (100.0 * joined_count / len(discoveries)) if discoveries else 0.0,
        1,
    )
    no_db_count = sum(1 for row in discoveries if row.get("join_status") == "no_db")
    next_action, next_command = _next_operator_step(
        events_path=Path(events_path),
        freshness_status=freshness_status,
        missing_scan_count=missing_scan_count,
        missing_sample=missing_sample,
        discovery_count=len(discoveries),
        join_coverage_pct=join_coverage_pct,
        no_db_count=no_db_count,
    )
    join_target_pct = 50.0
    goal_status = {
        "schema_version": "discovery-goal-status-v1",
        "freshness_ok": freshness_status == "fresh",
        "freshness_status": freshness_status,
        "join_coverage_pct": join_coverage_pct,
        "join_target_pct": join_target_pct,
        "join_target_met": bool(discoveries) and join_coverage_pct >= join_target_pct,
        "discovery_count": len(discoveries),
        "quiet_tape_count": quiet_tape_count,
        "investment_advice": False,
    }

    return {
        "schema_version": DISCOVERY_BRIEF_SCHEMA,
        "generated_at": clock.isoformat(),
        "events_path": str(Path(events_path)),
        "events_source": bundle.source,
        "events_generated_at": bundle.generated_at.isoformat(),
        "events_age_hours": round(age_hours, 2),
        "freshness_status": freshness_status,
        "event_count": len(event_rows),
        "discovery_count": len(discoveries),
        "mapped_ticker_count": len(mapped_tickers),
        "counts": {
            "events": len(event_rows),
            "discoveries": len(discoveries),
            "research_only": research_count,
            "watch": watch_count,
            "blocked": blocked_count,
            "joined": joined_count,
            "missing_scan": missing_scan_count,
            "quiet_tape": quiet_tape_count,
            "mapped_tickers": len(mapped_tickers),
        },
        "join_coverage": {
            "joined": joined_count,
            "missing_scan": missing_scan_count,
            "no_db": no_db_count,
            "sample_missing_tickers": missing_sample,
            "coverage_pct": join_coverage_pct,
            "target_pct": join_target_pct,
            "target_met": goal_status["join_target_met"],
        },
        "goal_status": goal_status,
        "events": event_rows,
        "discoveries": discoveries,
        "headline": _headline(event_rows, discoveries, freshness_status=freshness_status),
        "next_action": next_action,
        "next_command": next_command,
        "canonical_next_action": next_action,
        "canonical_next_command": next_command,
        "investment_advice": False,
        "can_make_investment_decision": False,
        "decision_support_only": True,
        "external_calls_made": 0,
        "external_calls_required": 0,
        "db_writes_made": 0,
        "db_writes_required": 0,
        "limitations": [
            "Social/X sources are research signals, not trade triggers.",
            "Discovery ranks attention, not expected return.",
            "Price reaction join is best-effort from local scan rows when available.",
            f"Events older than {FRESHNESS_STALE_HOURS:.0f}h are marked stale.",
        ],
    }


def default_events_path() -> Path:
    candidates = [
        LOCAL_EVENTS_PATH,
        Path("data/sample/world_events.json"),
        DEFAULT_EVENTS_PATH,
    ]
    for path in candidates:
        if path.is_file():
            return path
    return DEFAULT_EVENTS_PATH


def _parse_event(raw: object) -> WorldEvent:
    if not isinstance(raw, Mapping):
        msg = "each event must be an object"
        raise ValueError(msg)
    event_id = str(raw.get("id") or "").strip()
    title = str(raw.get("title") or "").strip()
    if not event_id or not title:
        msg = "each event requires id and title"
        raise ValueError(msg)
    sources_raw = raw.get("sources") or []
    sources: list[EventSource] = []
    if isinstance(sources_raw, list):
        for item in sources_raw:
            if not isinstance(item, Mapping):
                continue
            published = item.get("published_at")
            sources.append(
                EventSource(
                    provider=str(item.get("provider") or "unknown"),
                    url=str(item["url"]) if item.get("url") else None,
                    author=str(item["author"]) if item.get("author") else None,
                    published_at=(
                        parse_datetime(published, "published_at") if published else None
                    ),
                    engagement=(
                        dict(item["engagement"])
                        if isinstance(item.get("engagement"), Mapping)
                        else {}
                    ),
                )
            )
    direction = str(raw.get("direction") or "mixed").casefold()
    if direction not in {"bullish", "bearish", "mixed"}:
        direction = "mixed"
    source_category = str(raw.get("source_category") or "social").casefold()
    return WorldEvent(
        id=event_id,
        title=title,
        summary=str(raw.get("summary") or "").strip(),
        themes=normalize_themes(raw.get("themes")),
        tickers=normalize_tickers(raw.get("tickers")),
        secondary_tickers=normalize_tickers(raw.get("secondary_tickers")),
        direction=direction,
        materiality=clamp_score(raw.get("materiality"), 0.5),
        source_quality=clamp_score(raw.get("source_quality"), 0.25),
        source_category=source_category,
        sources=tuple(sources),
        available_at=parse_datetime(raw.get("available_at"), "available_at"),
    )


def _event_emotion_score(event: WorldEvent) -> float:
    # Map 0-1 materiality/source quality into the 0-100 emotion-like scale used by priced-in.
    base = (event.materiality * 55.0) + (event.source_quality * 35.0)
    if event.source_category in {"social", "promotional"}:
        base = min(base, 70.0)
    return round(max(0.0, min(100.0, base)), 2)


def _dedupe_best_per_ticker(
    rows: Sequence[Mapping[str, object]],
) -> list[dict[str, object]]:
    best: dict[str, dict[str, object]] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        ticker = str(row.get("ticker") or "").strip().upper()
        if not ticker:
            continue
        current = dict(row)
        previous = best.get(ticker)
        if previous is None:
            best[ticker] = current
            continue
        if float(current.get("discovery_score") or 0.0) > float(
            previous.get("discovery_score") or 0.0
        ):
            best[ticker] = current
    return list(best.values())


def _discovery_row(
    *,
    event: WorldEvent,
    ticker: str,
    role: str,
    emotion_score: float,
    event_join: Any,
    origin: str = "event",
) -> dict[str, object]:
    reaction = float(event_join.reaction_score)
    priced_status = str(event_join.priced_in_status or "unknown")
    gap = float(event_join.emotion_reaction_gap)
    ret_5d_pct = event_join.ret_5d_pct
    join_status = str(event_join.join_status)

    usefulness = _usefulness(
        source_category=event.source_category,
        source_quality=event.source_quality,
        gap=gap,
        priced_status=priced_status,
    )
    quiet_tape = _is_quiet_tape(
        join_status=join_status,
        reaction=reaction,
        ret_5d_pct=ret_5d_pct,
        priced_status=priced_status,
        bar_count=int(event_join.bar_count or 0),
    )
    discovery_score = round(
        (gap * 0.55)
        + (emotion_score * 0.25)
        + (event.materiality * 100.0 * 0.12)
        + (event.source_quality * 100.0 * 0.08),
        2,
    )
    if origin == "theme":
        discovery_score = round(discovery_score * 0.62, 2)
    elif role == "secondary":
        discovery_score = round(discovery_score * 0.88, 2)
    # Prefer under-reacted names when reaction data is present.
    if quiet_tape and join_status == "joined" and gap >= 10:
        discovery_score = round(discovery_score + 12.0, 2)
    elif quiet_tape:
        discovery_score = round(discovery_score + 6.0, 2)
    elif join_status == "joined" and reaction >= 55:
        discovery_score = round(discovery_score * 0.70, 2)
    # Mega-caps often already price world narratives; demote lag ranking.
    if ticker in MEGA_CAP_TICKERS:
        if join_status == "joined" and (reaction >= 35 or (ret_5d_pct is not None and abs(ret_5d_pct) >= 4)):
            discovery_score = round(discovery_score * 0.62, 2)
        else:
            discovery_score = round(discovery_score * 0.85, 2)

    return {
        "ticker": ticker,
        "event_id": event.id,
        "event_title": event.title,
        "role": role,
        "origin": origin,
        "direction": event.direction,
        "themes": list(event.themes),
        "materiality": event.materiality,
        "source_quality": event.source_quality,
        "source_category": event.source_category,
        "emotion_score": emotion_score,
        "reaction_score": round(reaction, 2),
        "emotion_reaction_gap": round(gap, 2),
        "ret_5d_pct": ret_5d_pct,
        "join_status": join_status,
        "last_bar_date": event_join.last_bar_date,
        "bar_count": event_join.bar_count,
        "join_reason": event_join.reason,
        "quiet_tape": quiet_tape,
        "priced_in_status": priced_status,
        "discovery_score": discovery_score,
        "usefulness": usefulness,
        "why_now": (
            f"{event.direction.capitalize()} world event '{event.title}' maps to {ticker}; "
            f"emotion {emotion_score:.0f} vs reaction {reaction:.0f} "
            f"(gap {gap:.0f}); join={join_status}."
        ),
        "next_step": (
            "Research only: verify with primary sources and local priced-in case file."
            if usefulness == "research_only"
            else "Watch for confirmation and quiet-tape persistence."
            if usefulness == "watch"
            else "Blocked for action; keep as narrative context only."
        ),
        "sources": [source.as_payload() for source in event.sources[:3]],
    }


def _usefulness(
    *,
    source_category: str,
    source_quality: float,
    gap: float,
    priced_status: str,
) -> str:
    if priced_status in {"blocked", "conflicted"}:
        return "blocked"
    if source_category in {"social", "promotional"} or source_quality < 0.45:
        return "research_only"
    if gap >= 20:
        return "watch"
    return "research_only"


def _finite(value: object, default: float = 0.0) -> float:
    try:
        number = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default
    if number != number:
        return default
    return number


def _is_quiet_tape(
    *,
    join_status: str,
    reaction: float,
    ret_5d_pct: float | None,
    priced_status: str,
    bar_count: int = 0,
) -> bool:
    if join_status != "joined":
        return False
    if bar_count < 3 or ret_5d_pct is None:
        return False
    if priced_status in {"fully_priced", "overextended_hype", "blocked", "conflicted", "stale"}:
        return False
    if reaction > 35:
        return False
    if abs(ret_5d_pct) >= 8.0:
        return False
    return True


def _next_operator_step(
    *,
    events_path: Path,
    freshness_status: str,
    missing_scan_count: int,
    missing_sample: Sequence[str],
    discovery_count: int,
    join_coverage_pct: float = 0.0,
    no_db_count: int = 0,
) -> tuple[str, str]:
    if freshness_status == "stale":
        # validate-only never refreshes the feed; point operators at the install path.
        return (
            "World-events file is stale. Install a fresh world-events-v1 JSON into "
            "data/local/world_events.json (Grok daily task output or a new events file), "
            "then reopen World Events so the main app reloads the discovery queue.",
            "powershell -ExecutionPolicy Bypass -File scripts/refresh-world-events.ps1 "
            "-EventsPath <fresh-world-events.json> -Execute",
        )
    if no_db_count > 0 and missing_scan_count == 0:
        return (
            "Discovery has no local DB join. Load .env.local database URL and rescan "
            "mapped tickers before trusting reaction gaps.",
            DISCOVERY_BARS_NEXT_COMMAND,
        )
    if missing_scan_count > 0:
        sample = ",".join(missing_sample[:8]) if missing_sample else "TICKER"
        return (
            f"{missing_scan_count} discovery row(s) lack local scan/priced-in joins "
            f"(join coverage {join_coverage_pct:.0f}%, target 50%). "
            "Run mapped bar fill + scan, then reopen World Events. "
            f"Sample missing: {sample}.",
            DISCOVERY_BARS_NEXT_COMMAND,
        )
    if discovery_count == 0:
        return (
            "No discoveries mapped. Expand themes/tickers in world-events JSON "
            "or refresh the Grok daily events feed.",
            f"catalyst-radar discovery-brief --events {events_path} --json",
        )
    if join_coverage_pct < 50.0:
        return (
            f"Join coverage {join_coverage_pct:.0f}% is below the 50% goal for top leads. "
            "Fill bars for mapped tickers and rescan, then label useful/noisy leads.",
            DISCOVERY_BARS_NEXT_COMMAND,
        )
    return (
        "Review top discovery rows as research-only leads, open a case, and label "
        "from the Proof loop. Confirm with primary sources before any capital decision.",
        f"catalyst-radar discovery-brief --events {events_path} --json",
    )


def _headline(
    events: Sequence[Mapping[str, object]],
    discoveries: Sequence[Mapping[str, object]],
    *,
    freshness_status: str = "fresh",
) -> str:
    if not events:
        return "No world events loaded."
    prefix = "STALE events · " if freshness_status == "stale" else ""
    top = discoveries[0] if discoveries else None
    if top is None:
        return f"{prefix}{len(events)} world event(s) loaded; no mapped tickers yet."
    join = top.get("join_status")
    return (
        f"{prefix}{len(events)} world event(s) → top discovery {top.get('ticker')} "
        f"(gap {top.get('emotion_reaction_gap')}, {top.get('usefulness')}, join={join})."
    )

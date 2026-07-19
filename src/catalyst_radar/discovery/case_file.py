from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy.engine import Engine

from catalyst_radar.discovery.brief import (
    build_discovery_brief,
    default_events_path,
)
from catalyst_radar.events.models import SourceCategory

CASE_FILE_SCHEMA = "discovery-case-file-v1"


def build_discovery_case_file(
    *,
    ticker: str,
    events_path: str | Path | None = None,
    theme_peers_path: str | Path | None = Path("config/theme_peers.yaml"),
    engine: Engine | None = None,
    event_id: str | None = None,
) -> dict[str, object]:
    """Build a research case file for one discovery ticker.

    Zero provider calls. Optional local DB for priced-in join and SEC events.
    """
    symbol = str(ticker or "").strip().upper()
    if not symbol:
        msg = "ticker is required"
        raise ValueError(msg)

    path = Path(events_path) if events_path else default_events_path()
    brief = build_discovery_brief(
        events_path=path,
        theme_peers_path=theme_peers_path,
        engine=engine,
        limit=200,
    )
    discoveries = [
        row
        for row in _rows(brief.get("discoveries"))
        if str(row.get("ticker") or "").upper() == symbol
    ]
    if event_id:
        discoveries = [
            row for row in discoveries if str(row.get("event_id") or "") == event_id
        ]
    if not discoveries:
        return {
            "schema_version": CASE_FILE_SCHEMA,
            "status": "not_found",
            "ticker": symbol,
            "events_path": str(path),
            "headline": f"No discovery row for {symbol} in current world-events brief.",
            "next_action": (
                "Re-run discovery-brief or add this ticker to a world event mapping."
            ),
            "next_command": f"catalyst-radar discovery-brief --events {path} --json",
            "investment_advice": False,
            "can_make_investment_decision": False,
            "external_calls_made": 0,
            "db_writes_made": 0,
        }

    primary = discoveries[0]
    related_events = _events_for_discoveries(brief, discoveries)
    confirmation = _confirmation_status(
        engine=engine,
        ticker=symbol,
        world_events=related_events,
    )
    invalidation = _invalidation_checklist(primary, confirmation)
    local_sec = confirmation.get("primary_events") or []

    usefulness = str(primary.get("usefulness") or "research_only")
    if confirmation.get("status") == "primary_confirmed" and usefulness == "research_only":
        # Still not investment advice; only raise attention band.
        usefulness = "watch" if float(primary.get("emotion_reaction_gap") or 0) >= 15 else usefulness

    return {
        "schema_version": CASE_FILE_SCHEMA,
        "status": "ready",
        "ticker": symbol,
        "events_path": str(path),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "headline": (
            f"{symbol}: {primary.get('event_title')} — "
            f"gap {primary.get('emotion_reaction_gap')}, "
            f"join={primary.get('join_status')}, "
            f"confirm={confirmation.get('status')}"
        ),
        "discovery": primary,
        "related_discoveries": discoveries,
        "world_events": related_events,
        "price_reaction": {
            "join_status": primary.get("join_status"),
            "emotion_score": primary.get("emotion_score"),
            "reaction_score": primary.get("reaction_score"),
            "emotion_reaction_gap": primary.get("emotion_reaction_gap"),
            "ret_5d_pct": primary.get("ret_5d_pct"),
            "quiet_tape": primary.get("quiet_tape"),
            "priced_in_status": primary.get("priced_in_status"),
            "price_not_fully_discovered": _price_not_fully_discovered(primary),
        },
        "confirmation": confirmation,
        "usefulness": usefulness,
        "trust_ladder": {
            "current": usefulness,
            "social_only_cap": "research_only",
            "note": (
                "Social/X alone never authorizes capital decisions. "
                "Primary confirmation can raise attention to watch, not auto-buy."
            ),
        },
        "invalidation": invalidation,
        "why_this_ticker": primary.get("why_now"),
        "next_action": _case_next_action(primary, confirmation),
        "next_command": (
            f"catalyst-radar value-ledger record --artifact-type discovery_row "
            f"--artifact-id {primary.get('event_id')}:{symbol} "
            f"--ticker {symbol} --label good-research --estimated-value-usd 5 "
            f"--confidence 0.5 --source discovery-case --preview --json"
        ),
        "label_command_preview": (
            f"catalyst-radar discovery-label --ticker {symbol} "
            f"--event-id {primary.get('event_id')} --label good-research --preview --json"
        ),
        "local_primary_events": local_sec,
        "investment_advice": False,
        "can_make_investment_decision": False,
        "decision_support_only": True,
        "external_calls_made": 0,
        "external_calls_required": 0,
        "db_writes_made": 0,
        "db_writes_required": 0,
        "limitations": [
            "Case file is research support only.",
            "Missing scan join means reaction is unknown, not proven lag.",
            "Primary events listed are local DB only; empty list means not confirmed here.",
        ],
    }


def _price_not_fully_discovered(row: Mapping[str, Any]) -> bool | None:
    join = str(row.get("join_status") or "")
    if join != "joined":
        return None
    status = str(row.get("priced_in_status") or "")
    if status in {"bullish_not_priced_in", "bearish_not_priced_in"}:
        return True
    if status in {"fully_priced", "overextended_hype"}:
        return False
    gap = float(row.get("emotion_reaction_gap") or 0.0)
    reaction = float(row.get("reaction_score") or 0.0)
    if gap >= 20 and reaction < 40:
        return True
    if reaction >= 55:
        return False
    return None


def _events_for_discoveries(
    brief: Mapping[str, object],
    discoveries: Sequence[Mapping[str, object]],
) -> list[dict[str, object]]:
    event_ids = {str(row.get("event_id") or "") for row in discoveries}
    events = []
    for event in _rows(brief.get("events")):
        if str(event.get("id") or "") in event_ids:
            events.append(dict(event))
    return events


def _confirmation_status(
    *,
    engine: Engine | None,
    ticker: str,
    world_events: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    social_sources = 0
    for event in world_events:
        for source in _rows(event.get("sources")):
            provider = str(source.get("provider") or "").casefold()
            if provider in {"x", "twitter", "social"} or str(event.get("source_category")) == "social":
                social_sources += 1

    primary_events: list[dict[str, object]] = []
    if engine is not None:
        primary_events = _load_primary_local_events(engine, ticker)

    if primary_events:
        status = "primary_confirmed"
        detail = f"{len(primary_events)} local primary/regulatory event(s) found for {ticker}."
    elif social_sources >= 2:
        status = "corroborated_social"
        detail = "Multiple social sources; still needs primary confirmation."
    elif social_sources == 1:
        status = "unconfirmed_social"
        detail = "Single social/world-event lineage only."
    else:
        status = "unconfirmed"
        detail = "No sources attached."

    return {
        "status": status,
        "detail": detail,
        "social_source_count": social_sources,
        "primary_event_count": len(primary_events),
        "primary_events": primary_events[:5],
        "allows_above_research_only": status == "primary_confirmed",
    }


def _load_primary_local_events(engine: Engine, ticker: str) -> list[dict[str, object]]:
    try:
        from catalyst_radar.storage.event_repositories import EventRepository
    except Exception:
        return []
    try:
        now = datetime.now(tz=UTC)
        rows = EventRepository(engine).list_events_for_ticker(
            ticker,
            as_of=now,
            available_at=now,
            min_materiality=0.0,
            limit=20,
        )
    except Exception:
        return []

    primary_categories = {
        SourceCategory.PRIMARY_SOURCE,
        SourceCategory.REGULATORY,
        SourceCategory.REPUTABLE_NEWS,
        SourceCategory.COMPANY_PRESS_RELEASE,
    }
    out: list[dict[str, object]] = []
    for row in rows:
        if row.source_category not in primary_categories and row.provider not in {"sec", "edgar"}:
            # Skip pure social fan-out rows for confirmation.
            if row.provider == "world_events" or row.source_category == SourceCategory.SOCIAL:
                continue
        out.append(
            {
                "id": row.id,
                "title": row.title,
                "provider": row.provider,
                "source": row.source,
                "source_category": str(row.source_category),
                "materiality": row.materiality,
                "source_quality": row.source_quality,
                "source_url": row.source_url,
                "source_ts": row.source_ts.isoformat(),
            }
        )
    return out


def _invalidation_checklist(
    discovery: Mapping[str, object],
    confirmation: Mapping[str, object],
) -> list[dict[str, str]]:
    return [
        {
            "id": "event_false",
            "check": "World event narrative is wrong, outdated, or not material to this ticker.",
            "action": "Label false-positive and drop.",
        },
        {
            "id": "already_priced",
            "check": (
                f"Price already reacted (reaction={discovery.get('reaction_score')}, "
                f"status={discovery.get('priced_in_status')})."
            ),
            "action": "Label too-late; demote discovery score.",
        },
        {
            "id": "mapping_wrong",
            "check": "Theme/ticker map is weak second-order association only.",
            "action": "Label noisy; tighten theme map.",
        },
        {
            "id": "no_primary",
            "check": (
                f"Confirmation still {confirmation.get('status')}; no primary source."
            ),
            "action": "Keep research_only; seek SEC/filing/reputable news.",
        },
        {
            "id": "liquidity_risk",
            "check": "Name is too illiquid or binary for the thesis.",
            "action": "Do not escalate; policy hard blocks apply if scored.",
        },
    ]


def _case_next_action(
    discovery: Mapping[str, object],
    confirmation: Mapping[str, object],
) -> str:
    if discovery.get("join_status") == "missing_scan":
        return (
            "Local scan join missing. Import bars / scan this ticker before treating "
            "the reaction gap as evidence of under-discovery."
        )
    if confirmation.get("status") != "primary_confirmed":
        return (
            "Research only: confirm the world event with primary/regulatory sources, "
            "then re-open this case file."
        )
    if discovery.get("quiet_tape") or discovery.get("price_not_fully_discovered"):
        return (
            "Primary-backed and tape still relatively quiet. Deepen research; "
            "do not treat as automated buy. Optional: label good-research."
        )
    return (
        "Review invalidation checklist and label the discovery "
        "(useful / noisy / too-late / false-positive)."
    )


def _rows(value: object) -> list[dict[str, object]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]

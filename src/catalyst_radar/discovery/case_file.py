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
    lag_flag = _price_not_fully_discovered(primary)

    usefulness = str(primary.get("usefulness") or "research_only")
    if confirmation.get("status") == "primary_confirmed" and usefulness == "research_only":
        # Still not investment advice; only raise attention band.
        usefulness = "watch" if float(primary.get("emotion_reaction_gap") or 0) >= 15 else usefulness

    price_reaction = {
        "join_status": primary.get("join_status"),
        "emotion_score": primary.get("emotion_score"),
        "reaction_score": primary.get("reaction_score"),
        "emotion_reaction_gap": primary.get("emotion_reaction_gap"),
        "ret_5d_pct": primary.get("ret_5d_pct"),
        "quiet_tape": primary.get("quiet_tape"),
        "priced_in_status": primary.get("priced_in_status"),
        "price_not_fully_discovered": lag_flag,
    }
    operator_analysis = build_operator_analysis(
        ticker=symbol,
        discovery=primary,
        confirmation=confirmation,
        related_discoveries=discoveries,
        world_events=related_events,
        all_event_discoveries=_sibling_discoveries_for_event(brief, primary),
        usefulness=usefulness,
        lag_flag=lag_flag,
    )
    disposition = str(operator_analysis.get("disposition_label") or "good-research")
    next_action = str(
        operator_analysis.get("next_action") or _case_next_action(primary, confirmation)
    )

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
        "price_reaction": price_reaction,
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
        "operator_analysis": operator_analysis,
        "why_this_ticker": primary.get("why_now"),
        "next_action": next_action,
        "next_command": (
            f"catalyst-radar value-ledger record --artifact-type discovery_row "
            f"--artifact-id {primary.get('event_id')}:{symbol} "
            f"--ticker {symbol} --label {disposition} --estimated-value-usd 5 "
            f"--confidence 0.5 --source discovery-case --preview --json"
        ),
        "label_command_preview": (
            f"catalyst-radar discovery-label --ticker {symbol} "
            f"--event-id {primary.get('event_id')} --label {disposition} --preview --json"
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
            "Operator analysis is deterministic decision-support text, not investment advice.",
        ],
    }


def build_operator_analysis(
    *,
    ticker: str,
    discovery: Mapping[str, Any],
    confirmation: Mapping[str, Any],
    related_discoveries: Sequence[Mapping[str, Any]],
    world_events: Sequence[Mapping[str, Any]],
    all_event_discoveries: Sequence[Mapping[str, Any]],
    usefulness: str,
    lag_flag: bool | None,
) -> dict[str, object]:
    """Deterministic operator readout for the World Events case panel."""
    symbol = str(ticker or "").strip().upper()
    gap = _finite(discovery.get("emotion_reaction_gap"))
    emotion = _finite(discovery.get("emotion_score"))
    reaction = _finite(discovery.get("reaction_score"))
    ret_5d = discovery.get("ret_5d_pct")
    ret_5d_f = None if ret_5d is None else _finite(ret_5d)
    join = str(discovery.get("join_status") or "no_db")
    role = str(discovery.get("role") or "secondary")
    priced_status = str(discovery.get("priced_in_status") or "unknown")
    confirm_status = str(confirmation.get("status") or "unconfirmed")
    event_title = str(discovery.get("event_title") or "world event")
    quiet = bool(discovery.get("quiet_tape"))

    signal = _signal_quality(
        join=join,
        gap=gap,
        emotion=emotion,
        reaction=reaction,
        lag_flag=lag_flag,
        priced_status=priced_status,
        quiet=quiet,
        ret_5d_pct=ret_5d_f,
    )
    mapping = _map_quality(role=role, discovery=discovery, world_events=world_events)
    trust = _trust_readout(confirm_status=confirm_status, usefulness=usefulness)
    disposition = _disposition(
        signal_id=str(signal["id"]),
        confirm_status=confirm_status,
        lag_flag=lag_flag,
        reaction=reaction,
        gap=gap,
    )
    checklist = _ten_minute_checklist(
        confirm_status=confirm_status,
        join=join,
        disposition=str(disposition["label"]),
    )
    queue_context = _queue_context(
        symbol=symbol,
        gap=gap,
        event_title=event_title,
        peers=all_event_discoveries,
    )
    chips = _analysis_chips(
        join=join,
        gap=gap,
        lag_flag=lag_flag,
        signal_id=str(signal["id"]),
        confirm_status=confirm_status,
        ret_5d_pct=ret_5d_f,
    )
    summary_lines = [
        str(signal["summary"]),
        str(mapping["summary"]),
        str(trust["summary"]),
        str(queue_context["summary"]),
    ]
    next_action = str(disposition["next_action"])

    return {
        "schema_version": "discovery-operator-analysis-v1",
        "ticker": symbol,
        "headline": str(signal["headline"]),
        "summary_lines": summary_lines,
        "signal_quality": signal,
        "map_quality": mapping,
        "trust": trust,
        "disposition": disposition,
        "disposition_label": disposition["label"],
        "checklist": checklist,
        "queue_context": queue_context,
        "chips": chips,
        "metrics": {
            "emotion_score": round(emotion, 2),
            "reaction_score": round(reaction, 2),
            "emotion_reaction_gap": round(gap, 2),
            "ret_5d_pct": None if ret_5d_f is None else round(ret_5d_f, 2),
            "join_status": join,
            "priced_in_status": priced_status,
            "role": role,
            "quiet_tape": quiet,
        },
        "next_action": next_action,
        "investment_advice": False,
    }


def _sibling_discoveries_for_event(
    brief: Mapping[str, object],
    primary: Mapping[str, Any],
) -> list[dict[str, object]]:
    event_id = str(primary.get("event_id") or "")
    out: list[dict[str, object]] = []
    for row in _rows(brief.get("discoveries")):
        if event_id and str(row.get("event_id") or "") == event_id:
            out.append(dict(row))
    return out


def _signal_quality(
    *,
    join: str,
    gap: float,
    emotion: float,
    reaction: float,
    lag_flag: bool | None,
    priced_status: str,
    quiet: bool,
    ret_5d_pct: float | None,
) -> dict[str, object]:
    ret_bit = (
        f" 5d return {ret_5d_pct:+.1f}%."
        if ret_5d_pct is not None
        else " 5d return unavailable."
    )
    if join in {"missing_scan", "no_db"}:
        return {
            "id": "need_data",
            "label": "Need market data",
            "strength": "blocked",
            "headline": "Cannot judge lag until local bars/scan join this ticker.",
            "summary": (
                f"Join status is {join}. Emotion {emotion:.0f} is narrative-only until "
                f"price reaction is measured.{ret_bit}"
            ),
        }
    if priced_status in {"fully_priced", "overextended_hype"} or (
        lag_flag is False and reaction >= 45
    ):
        return {
            "id": "already_moved",
            "label": "Already moved / chase risk",
            "strength": "weak",
            "headline": "Price reaction already looks strong relative to the event emotion.",
            "summary": (
                f"Reaction {reaction:.0f} vs emotion {emotion:.0f} (gap {gap:.1f}); "
                f"priced-in status {priced_status}.{ret_bit} Prefer other leads unless "
                "primary news is brand-new."
            ),
        }
    if lag_flag is True or (gap >= 20 and reaction < 40 and quiet):
        return {
            "id": "lag_candidate",
            "label": "Possible lag",
            "strength": "strong",
            "headline": "Emotion ahead of measured reaction — classic under-discovery shape.",
            "summary": (
                f"Emotion {emotion:.0f} vs reaction {reaction:.0f} (gap {gap:.1f}); "
                f"quiet_tape={quiet}.{ret_bit} Still research-only until primary confirms."
            ),
        }
    if abs(gap) < 8:
        return {
            "id": "flat_gap",
            "label": "Weak lag signal",
            "strength": "weak",
            "headline": "Gap is near zero — not a strong under-discovery setup.",
            "summary": (
                f"Emotion {emotion:.0f} vs reaction {reaction:.0f} (gap {gap:.1f}). "
                f"Theme may still be worth a primary check, but ranking is weak.{ret_bit}"
            ),
        }
    if gap < -15:
        return {
            "id": "reaction_led",
            "label": "Reaction-led",
            "strength": "weak",
            "headline": "Reaction exceeds event emotion in the model — lag thesis is inverted.",
            "summary": (
                f"Gap {gap:.1f} with reaction {reaction:.0f}.{ret_bit} "
                "Demote unless the causal map is uniquely strong."
            ),
        }
    return {
        "id": "mixed",
        "label": "Mixed signal",
        "strength": "medium",
        "headline": "Some separation between emotion and reaction; not a clean lag print.",
        "summary": (
            f"Emotion {emotion:.0f}, reaction {reaction:.0f}, gap {gap:.1f}, "
            f"status {priced_status}.{ret_bit}"
        ),
    }


def _map_quality(
    *,
    role: str,
    discovery: Mapping[str, Any],
    world_events: Sequence[Mapping[str, Any]],
) -> dict[str, object]:
    themes = [
        str(item)
        for item in (discovery.get("themes") or [])
        if str(item or "").strip()
    ]
    theme_text = ", ".join(themes[:4]) if themes else "unspecified themes"
    event_tickers: set[str] = set()
    for event in world_events:
        for key in ("tickers", "secondary_tickers"):
            for item in event.get(key) or []:
                text = str(item or "").strip().upper()
                if text:
                    event_tickers.add(text)
    symbol = str(discovery.get("ticker") or "").upper()
    listed = symbol in event_tickers
    if role == "primary" and listed:
        return {
            "id": "primary_map",
            "label": "Direct map",
            "strength": "strong",
            "summary": (
                f"{symbol} is a primary mapped name on this event ({theme_text}). "
                "Causal fit is at least first-order in the mapper."
            ),
        }
    if role == "primary":
        return {
            "id": "primary_theme",
            "label": "Primary theme map",
            "strength": "medium",
            "summary": (
                f"{symbol} ranks as primary via themes ({theme_text}). "
                "Confirm the business link before treating as second-order alpha."
            ),
        }
    if listed:
        return {
            "id": "secondary_listed",
            "label": "Second-order listed",
            "strength": "medium",
            "summary": (
                f"{symbol} is an explicit secondary ticker ({theme_text}). "
                "Plausible beneficiary/casualty — still verify the chain."
            ),
        }
    return {
        "id": "weak_second_order",
        "label": "Weak / second-order map",
        "strength": "weak",
        "summary": (
            f"{symbol} is theme-expanded secondary coverage ({theme_text}). "
            "High risk of mapping noise — kill thesis if the chain is thin."
        ),
    }


def _trust_readout(*, confirm_status: str, usefulness: str) -> dict[str, object]:
    if confirm_status == "primary_confirmed":
        return {
            "id": "primary_confirmed",
            "label": "Primary-backed (still not a buy)",
            "summary": (
                f"Local primary/regulatory evidence found. Trust band is {usefulness}; "
                "never auto-escalates to capital decisions."
            ),
        }
    if confirm_status == "corroborated_social":
        return {
            "id": "corroborated_social",
            "label": "Social only (multi-source)",
            "summary": (
                "Multiple social sources corroborate the narrative, but there is still "
                "no primary/regulatory confirmation. Cap remains research_only."
            ),
        }
    if confirm_status == "unconfirmed_social":
        return {
            "id": "unconfirmed_social",
            "label": "Single social lineage",
            "summary": "Thin social lineage only. Treat as a rumor until primary sources exist.",
        }
    return {
        "id": "unconfirmed",
        "label": "Unconfirmed",
        "summary": "No usable confirmation sources yet. Do not escalate attention.",
    }


def _disposition(
    *,
    signal_id: str,
    confirm_status: str,
    lag_flag: bool | None,
    reaction: float,
    gap: float,
) -> dict[str, object]:
    if signal_id == "need_data":
        return {
            "label": "noisy",
            "title": "Fix data before labeling useful",
            "reason": "Without a price join, a value label is mostly noise.",
            "next_action": (
                "Import recent bars and rescan this ticker, then reopen the case file."
            ),
        }
    if signal_id in {"already_moved", "reaction_led"} or (
        lag_flag is False and reaction >= 50
    ):
        return {
            "label": "too-late",
            "title": "Likely too late / already priced",
            "reason": "Measured reaction is already material versus the event emotion.",
            "next_action": (
                "Confirm the event in primary sources only if needed for learning; "
                "otherwise label too-late and move on."
            ),
        }
    if confirm_status != "primary_confirmed" and (
        signal_id == "flat_gap" or abs(gap) < 8
    ):
        return {
            "label": "noisy",
            "title": "Default: skip unless primary is fresh",
            "reason": "Weak lag math plus social-only trust — low expected attention value.",
            "next_action": (
                "Spend at most 5–10 minutes on a primary source. If the event is real "
                "and the map is tight, watch; otherwise label noisy and drop."
            ),
        }
    if signal_id == "lag_candidate" and confirm_status != "primary_confirmed":
        return {
            "label": "good-research",
            "title": "Watch / research (not a buy)",
            "reason": "Lag shape is interesting but still social-capped.",
            "next_action": (
                "Confirm the world event with primary/regulatory sources, re-check the "
                "price reaction, then label good-research or too-late."
            ),
        }
    if confirm_status == "primary_confirmed" and (
        signal_id == "lag_candidate" or (gap >= 15 and reaction < 45)
    ):
        return {
            "label": "good-research",
            "title": "Primary-backed watch",
            "reason": "Better trust plus lag shape — still decision support only.",
            "next_action": (
                "Deepen research and optional paper path only after policy allows; "
                "label good-research if the case was useful."
            ),
        }
    return {
        "label": "noisy",
        "title": "Low priority",
        "reason": "No strong lag + primary combination.",
        "next_action": (
            "Quick primary check or skip. Label noisy if the lead wasted attention."
        ),
    }


def _ten_minute_checklist(
    *,
    confirm_status: str,
    join: str,
    disposition: str,
) -> list[dict[str, str]]:
    steps = [
        {
            "id": "confirm_event",
            "step": "Confirm the world event in a primary outlet (wire, filing, company PR).",
            "done_when": "You can cite a non-social source or prove the narrative is false.",
        },
        {
            "id": "confirm_map",
            "step": "Confirm the causal chain from that event to this ticker.",
            "done_when": "You can state why revenue/costs/risk change for this name.",
        },
        {
            "id": "confirm_price",
            "step": "Check whether price already moved on that news in your window.",
            "done_when": "You accept joined reaction/ret_5d as enough or mark too-late.",
        },
        {
            "id": "label",
            "step": f"Label the lead ({disposition} suggested) so the value ledger learns.",
            "done_when": "Preview/execute discovery-label or value-ledger record.",
        },
    ]
    if join in {"missing_scan", "no_db"}:
        steps.insert(
            0,
            {
                "id": "join_data",
                "step": "Load bars and scan so reaction is measured, not assumed.",
                "done_when": "join_status becomes joined on this ticker.",
            },
        )
    if confirm_status == "primary_confirmed":
        steps[0] = {
            "id": "confirm_event",
            "step": "Re-read local primary evidence and check it still matches the thesis.",
            "done_when": "Primary event still supports the mapped ticker story.",
        }
    return steps


def _queue_context(
    *,
    symbol: str,
    gap: float,
    event_title: str,
    peers: Sequence[Mapping[str, Any]],
) -> dict[str, object]:
    scored: list[tuple[str, float]] = []
    for row in peers:
        ticker = str(row.get("ticker") or "").upper()
        if not ticker:
            continue
        scored.append((ticker, _finite(row.get("emotion_reaction_gap"))))
    if not scored:
        return {
            "summary": f"No peer queue context for this event yet.",
            "peer_count": 0,
            "rank": None,
            "stronger_peers": [],
            "weaker_peers": [],
        }
    scored.sort(key=lambda item: item[1], reverse=True)
    rank = next((i + 1 for i, (t, _) in enumerate(scored) if t == symbol), None)
    stronger = [t for t, g in scored if g > gap + 0.5][:4]
    weaker = [t for t, g in scored if g < gap - 0.5][:4]
    if rank is None:
        summary = f"On event '{event_title[:60]}', peers available but rank unknown."
    elif rank == 1:
        summary = (
            f"Highest gap on this event among {len(scored)} mapped names "
            f"(gap {gap:.1f})."
        )
    else:
        better = ", ".join(stronger[:3]) if stronger else "other names"
        summary = (
            f"Rank {rank}/{len(scored)} by gap on this event. "
            f"Stronger lag shape: {better}."
        )
    return {
        "summary": summary,
        "peer_count": len(scored),
        "rank": rank,
        "stronger_peers": stronger,
        "weaker_peers": weaker,
        "event_title": event_title,
    }


def _analysis_chips(
    *,
    join: str,
    gap: float,
    lag_flag: bool | None,
    signal_id: str,
    confirm_status: str,
    ret_5d_pct: float | None,
) -> list[dict[str, str]]:
    if join == "joined":
        if lag_flag is True:
            lag_chip = "Tape may lag"
        elif lag_flag is False:
            lag_chip = "Likely already priced"
        elif signal_id == "flat_gap":
            lag_chip = "Weak lag signal"
        else:
            lag_chip = "Joined · lag unclear"
    elif join == "missing_scan":
        lag_chip = "Need price join"
    else:
        lag_chip = "No DB join"

    chips = [
        {"id": "lag", "label": lag_chip},
        {"id": "gap", "label": f"Gap {gap:.2f}"},
        {"id": "join", "label": join},
        {"id": "trust", "label": confirm_status},
    ]
    if ret_5d_pct is not None:
        chips.insert(2, {"id": "ret5", "label": f"5d {ret_5d_pct:+.1f}%"})
    return chips


def _finite(value: object, default: float = 0.0) -> float:
    try:
        number = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default
    if number != number:
        return default
    return number


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

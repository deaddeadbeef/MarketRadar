"""Human research digest from a discovery brief. Decision support only."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path

from sqlalchemy.engine import Engine

from catalyst_radar.discovery.brief import build_discovery_brief, default_events_path
from catalyst_radar.discovery.case_file import build_discovery_case_file
from catalyst_radar.discovery.persist import persist_discovery_brief

INSIGHTS_SCHEMA = "discovery-insights-v1"


def build_discovery_insights(
    *,
    events_path: str | Path | None = None,
    engine: Engine | None = None,
    limit: int = 8,
    include_cases: bool = True,
    now: datetime | None = None,
    theme_peers_path: str | Path | None = Path("config/theme_peers.yaml"),
    persist: bool = False,
) -> dict[str, object]:
    path = Path(events_path) if events_path else default_events_path()
    brief = build_discovery_brief(
        events_path=path,
        theme_peers_path=theme_peers_path,
        engine=engine,
        limit=max(25, limit),
        now=now,
    )
    discoveries = [
        row for row in brief.get("discoveries") or [] if isinstance(row, Mapping)
    ][:limit]
    cases: list[dict[str, object]] = []
    if include_cases:
        for row in discoveries[: min(3, len(discoveries))]:
            ticker = str(row.get("ticker") or "")
            if not ticker:
                continue
            case = build_discovery_case_file(
                ticker=ticker,
                events_path=path,
                theme_peers_path=theme_peers_path,
                engine=engine,
                event_id=str(row.get("event_id") or "") or None,
            )
            cases.append(
                {
                    "ticker": ticker,
                    "headline": case.get("headline"),
                    "confirmation": (case.get("confirmation") or {}).get("status")
                    if isinstance(case.get("confirmation"), Mapping)
                    else None,
                    "operator_analysis": (
                        (case.get("operator_analysis") or {}).get("disposition")
                        if isinstance(case.get("operator_analysis"), Mapping)
                        else None
                    ),
                    "invalidation": [
                        item.get("action")
                        for item in (case.get("invalidation") or [])
                        if isinstance(item, Mapping)
                    ][:4],
                    "next_action": case.get("next_action"),
                }
            )
    persist_info = persist_discovery_brief(brief) if persist else None
    return {
        "schema_version": INSIGHTS_SCHEMA,
        "headline": brief.get("headline"),
        "freshness_status": brief.get("freshness_status"),
        "events_age_hours": brief.get("events_age_hours"),
        "event_count": brief.get("event_count"),
        "discovery_count": brief.get("discovery_count"),
        "join_coverage": brief.get("join_coverage"),
        "goal_status": brief.get("goal_status"),
        "events": _event_summaries(brief.get("events") or []),
        "leads": [_lead(row) for row in discoveries],
        "cases": cases,
        "next_action": brief.get("next_action"),
        "next_command": brief.get("next_command"),
        "investment_advice": False,
        "decision_support_only": True,
        "can_make_investment_decision": False,
        "external_calls_made": 0,
        "persist": persist_info,
        "limitations": [
            "Social/X sources stay research_only until primary confirmation.",
            "This ranks attention, not expected return.",
            "Use case invalidation before any capital decision.",
        ],
    }


def format_discovery_insights(payload: Mapping[str, object]) -> str:
    lines = [
        f"MarketRadar insights · {payload.get('headline')}",
        f"freshness={payload.get('freshness_status')} age_h={payload.get('events_age_hours')} "
        f"events={payload.get('event_count')} discoveries={payload.get('discovery_count')}",
        f"join={payload.get('join_coverage')}",
        "",
        "Events:",
    ]
    for event in payload.get("events") or []:
        if not isinstance(event, Mapping):
            continue
        lines.append(
            f"- {event.get('id')}: {event.get('title')} "
            f"[{event.get('direction')}] tickers={event.get('tickers')}"
        )
    lines.append("")
    lines.append("Ranked research leads (not investment advice):")
    for index, lead in enumerate(payload.get("leads") or [], start=1):
        if not isinstance(lead, Mapping):
            continue
        lines.append(
            f"{index}. {lead.get('ticker')}  score={lead.get('discovery_score')}  "
            f"join={lead.get('join_status')}  {lead.get('usefulness')}  "
            f"origin={lead.get('origin')}  "
            f"gap={lead.get('emotion_reaction_gap')}  ret5d={lead.get('ret_5d_pct')}%  "
            f"quiet={lead.get('quiet_tape')}"
        )
        lines.append(f"   event: {lead.get('event_title')}")
        lines.append(f"   why: {lead.get('why_now')}")
        lines.append(f"   next: {lead.get('next_step')}")
    if payload.get("cases"):
        lines.append("")
        lines.append("Top case files:")
        for case in payload.get("cases") or []:
            if not isinstance(case, Mapping):
                continue
            lines.append(
                f"- {case.get('ticker')}: {case.get('headline')} "
                f"confirm={case.get('confirmation')}"
            )
            for action in case.get("invalidation") or []:
                lines.append(f"    invalidate: {action}")
    lines.append("")
    lines.append(f"next_action: {payload.get('next_action')}")
    lines.append("decision_support_only=true investment_advice=false")
    return "\n".join(lines)


def _lead(row: Mapping[str, object]) -> dict[str, object]:
    return {
        "ticker": row.get("ticker"),
        "event_id": row.get("event_id"),
        "event_title": row.get("event_title"),
        "discovery_score": row.get("discovery_score"),
        "join_status": row.get("join_status"),
        "usefulness": row.get("usefulness"),
        "emotion_score": row.get("emotion_score"),
        "reaction_score": row.get("reaction_score"),
        "emotion_reaction_gap": row.get("emotion_reaction_gap"),
        "ret_5d_pct": row.get("ret_5d_pct"),
        "quiet_tape": row.get("quiet_tape"),
        "origin": row.get("origin"),
        "priced_in_status": row.get("priced_in_status"),
        "why_now": row.get("why_now"),
        "next_step": row.get("next_step"),
        "last_bar_date": row.get("last_bar_date"),
    }


def _event_summaries(events: Sequence[object]) -> list[dict[str, object]]:
    out: list[dict[str, object]] = []
    for event in events:
        if not isinstance(event, Mapping):
            continue
        out.append(
            {
                "id": event.get("id"),
                "title": event.get("title"),
                "direction": event.get("direction"),
                "themes": event.get("themes"),
                "tickers": event.get("tickers") or event.get("mapped_tickers"),
                "source_category": event.get("source_category"),
            }
        )
    return out

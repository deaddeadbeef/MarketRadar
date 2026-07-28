from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy.engine import Engine

from catalyst_radar.discovery.brief import default_events_path
from catalyst_radar.discovery.case_file import build_discovery_case_file
from catalyst_radar.storage.validation_repositories import ValidationRepository
from catalyst_radar.validation.value_ledger import (
    build_value_ledger_entry,
    value_ledger_write_payload,
)


def build_discovery_label_payload(
    *,
    engine: Engine,
    ticker: str,
    label: str,
    event_id: str | None = None,
    events_path: str | Path | None = None,
    estimated_value_usd: float = 5.0,
    confidence: float = 0.55,
    notes: str | None = None,
    execute: bool = False,
    supported_action: str = "research",
    user_decision: str = "wait",
) -> dict[str, object]:
    """Preview or write a value-ledger entry for a discovery row."""
    case = build_discovery_case_file(
        ticker=ticker,
        events_path=events_path or default_events_path(),
        engine=engine,
        event_id=event_id,
    )
    if case.get("status") != "ready":
        return {
            **case,
            "schema_version": "discovery-label-v1",
            "label_status": "blocked",
            "db_writes_made": 0,
            "db_writes_required": 0,
        }

    discovery = case.get("discovery") if isinstance(case.get("discovery"), Mapping) else {}
    artifact_id = f"{discovery.get('event_id')}:{str(ticker).upper()}"
    # Anchor outcomes to event/discovery date when present so forward returns work.
    as_of = _date_from_discovery(discovery, case)
    entry = build_value_ledger_entry(
        artifact_type="discovery_row",
        artifact_id=artifact_id,
        label=label,
        estimated_value_usd=estimated_value_usd,
        confidence=confidence,
        source="discovery-label",
        ticker=str(ticker).upper(),
        as_of=as_of,
        priced_in_status=str(discovery.get("priced_in_status") or "") or None,
        emotion_score=_optional_float(discovery.get("emotion_score")),
        reaction_score=_optional_float(discovery.get("reaction_score")),
        emotion_reaction_gap=_optional_float(discovery.get("emotion_reaction_gap")),
        supported_action=supported_action,
        user_decision=user_decision,
        notes=notes,
        payload={
            "event_id": discovery.get("event_id"),
            "event_title": discovery.get("event_title"),
            "join_status": discovery.get("join_status"),
            "quiet_tape": discovery.get("quiet_tape"),
            "usefulness": discovery.get("usefulness"),
            "confirmation": case.get("confirmation"),
            "case_schema": case.get("schema_version"),
            "as_of": as_of.isoformat() if as_of else None,
        },
        available_at=datetime.now(tz=UTC),
    )
    if execute:
        ValidationRepository(engine).upsert_value_ledger_entry(entry)
    write = value_ledger_write_payload(entry, execute=execute, command_name="record")
    return {
        "schema_version": "discovery-label-v1",
        "label_status": "written" if execute else "preview",
        "ticker": str(ticker).upper(),
        "event_id": discovery.get("event_id"),
        "artifact_type": "discovery_row",
        "artifact_id": artifact_id,
        "label": label,
        "investment_advice": False,
        "can_make_investment_decision": False,
        "case_headline": case.get("headline"),
        "value_ledger": write,
        "external_calls_made": 0,
        "db_writes_made": write.get("db_writes_made", 0) if isinstance(write, Mapping) else 0,
        "db_writes_required": write.get("db_writes_required", 1) if isinstance(write, Mapping) else 1,
        "next_action": (
            "Label written. Later update value outcomes when forward bars exist."
            if execute
            else "Preview only. Re-run with --execute to write the value-ledger row."
        ),
    }


def _optional_float(value: object) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _date_from_discovery(
    discovery: Mapping[str, Any],
    case: Mapping[str, Any],
):
    from datetime import date

    for key in ("as_of", "available_at", "events_generated_at"):
        raw = discovery.get(key)
        if raw is None and key == "events_generated_at":
            raw = case.get("generated_at")
        if raw is None:
            continue
        text = str(raw).strip()
        if not text:
            continue
        try:
            if "T" in text:
                return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
            return date.fromisoformat(text[:10])
        except ValueError:
            continue
    # Fall back to today so outcome machinery has an anchor.
    return datetime.now(tz=UTC).date()

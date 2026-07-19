from __future__ import annotations

import json
import shutil
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy.engine import Engine

from catalyst_radar.discovery.brief import LOCAL_EVENTS_PATH, load_world_events
from catalyst_radar.discovery.mapper import load_theme_ticker_map, map_event_tickers
from catalyst_radar.discovery.models import WorldEvent, WorldEventBundle
from catalyst_radar.events.dedupe import body_hash, canonicalize_url, dedupe_key
from catalyst_radar.events.models import CanonicalEvent, EventType, SourceCategory

INGEST_SCHEMA = "discovery-ingest-v1"


def validate_world_events_file(path: str | Path) -> dict[str, object]:
    file_path = Path(path)
    errors: list[str] = []
    if not file_path.is_file():
        return {
            "schema_version": INGEST_SCHEMA,
            "status": "missing_file",
            "events_path": str(file_path),
            "valid": False,
            "errors": [f"file not found: {file_path}"],
            "event_count": 0,
            "external_calls_made": 0,
            "db_writes_made": 0,
            "db_writes_required": 0,
        }
    try:
        bundle = load_world_events(file_path)
    except Exception as exc:
        return {
            "schema_version": INGEST_SCHEMA,
            "status": "invalid",
            "events_path": str(file_path),
            "valid": False,
            "errors": [str(exc)],
            "event_count": 0,
            "external_calls_made": 0,
            "db_writes_made": 0,
            "db_writes_required": 0,
        }
    if not bundle.events:
        errors.append("events list is empty")
    for event in bundle.events:
        if not event.tickers and not event.themes:
            errors.append(f"{event.id}: needs tickers or themes")
    return {
        "schema_version": INGEST_SCHEMA,
        "status": "valid" if not errors else "invalid",
        "events_path": str(file_path),
        "valid": not errors,
        "errors": errors,
        "event_count": len(bundle.events),
        "events_source": bundle.source,
        "events_generated_at": bundle.generated_at.isoformat(),
        "external_calls_made": 0,
        "db_writes_made": 0,
        "db_writes_required": 0,
        "investment_advice": False,
    }


def import_world_events_local(
    *,
    events_path: str | Path,
    destination: str | Path = LOCAL_EVENTS_PATH,
    execute: bool = False,
) -> dict[str, object]:
    validation = validate_world_events_file(events_path)
    if not validation.get("valid"):
        validation["mode"] = "preview" if not execute else "execute"
        validation["destination"] = str(destination)
        return validation

    src = Path(events_path)
    dest = Path(destination)
    planned_writes = 1
    if not execute:
        return {
            **validation,
            "mode": "preview",
            "destination": str(dest),
            "db_writes_required": planned_writes,
            "db_writes_made": 0,
            "next_command": (
                f"catalyst-radar discovery-ingest --events {src} "
                f"--destination {dest} --execute --json"
            ),
            "next_action": "Review the file, then re-run with --execute to install locally.",
        }

    dest.parent.mkdir(parents=True, exist_ok=True)
    if src.resolve() != dest.resolve():
        shutil.copy2(src, dest)
    else:
        # Touch-normalize by rewriting pretty JSON for stability.
        payload = json.loads(src.read_text(encoding="utf-8"))
        dest.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    return {
        **validation,
        "mode": "execute",
        "status": "imported",
        "destination": str(dest),
        "db_writes_required": planned_writes,
        "db_writes_made": 1,
        "next_command": f"catalyst-radar discovery-brief --events {dest} --json",
        "next_action": "Run discovery-brief against the local world-events file.",
    }


def fanout_world_events_to_store(
    *,
    events_path: str | Path,
    engine: Engine,
    theme_peers_path: str | Path | None = Path("config/theme_peers.yaml"),
    execute: bool = False,
    max_tickers_per_event: int = 12,
) -> dict[str, object]:
    validation = validate_world_events_file(events_path)
    if not validation.get("valid"):
        return {
            **validation,
            "mode": "preview" if not execute else "execute",
            "fanout_rows_planned": 0,
            "fanout_rows_written": 0,
        }

    bundle = load_world_events(events_path)
    theme_map = load_theme_ticker_map(theme_peers_path)
    canonical_rows = _fanout_rows(
        bundle,
        theme_map=theme_map,
        max_tickers_per_event=max_tickers_per_event,
    )
    planned = len(canonical_rows)
    if not execute:
        return {
            **validation,
            "mode": "preview",
            "fanout_rows_planned": planned,
            "fanout_rows_written": 0,
            "sample_tickers": sorted({row.ticker for row in canonical_rows})[:20],
            "db_writes_required": planned,
            "db_writes_made": 0,
            "next_command": (
                f"catalyst-radar discovery-ingest --events {events_path} "
                "--fanout-events --execute --json"
            ),
            "next_action": (
                "Preview only. Re-run with --fanout-events --execute to write "
                "SOCIAL CanonicalEvents (research-only quality)."
            ),
            "investment_advice": False,
        }

    from catalyst_radar.storage.event_repositories import EventRepository

    written = EventRepository(engine).upsert_events(canonical_rows)
    return {
        **validation,
        "mode": "execute",
        "status": "fanout_written",
        "fanout_rows_planned": planned,
        "fanout_rows_written": written,
        "sample_tickers": sorted({row.ticker for row in canonical_rows})[:20],
        "db_writes_required": planned,
        "db_writes_made": written,
        "next_command": f"catalyst-radar discovery-brief --events {events_path} --json",
        "next_action": "Fan-out complete. Social events remain research-only in policy.",
        "investment_advice": False,
    }


def _fanout_rows(
    bundle: WorldEventBundle,
    *,
    theme_map: Mapping[str, Sequence[str]],
    max_tickers_per_event: int,
) -> list[CanonicalEvent]:
    rows: list[CanonicalEvent] = []
    for event in bundle.events:
        mapped = map_event_tickers(event, theme_ticker_map=theme_map)
        tickers = list(mapped["all_tickers"])[: max(1, int(max_tickers_per_event))]  # type: ignore[index]
        for ticker in tickers:
            rows.append(_canonical_from_world_event(event, ticker=str(ticker)))
    return rows


def _canonical_from_world_event(event: WorldEvent, *, ticker: str) -> CanonicalEvent:
    body = event.summary or event.title
    content_hash = body_hash(f"{event.id}:{ticker}:{body}")
    first_url = event.sources[0].url if event.sources else None
    canonical_url = canonicalize_url(first_url)
    key = dedupe_key(
        ticker=ticker,
        provider="world_events",
        canonical_url=canonical_url or f"world://{event.id}/{ticker}",
        content_hash=content_hash,
    )
    source_category = (
        SourceCategory.SOCIAL
        if event.source_category in {"social", "promotional", "unknown"}
        else SourceCategory.AGGREGATOR
    )
    # Cap social materiality so fan-out cannot alone drive buy-review emotion.
    materiality = min(float(event.materiality), 0.40)
    source_quality = min(float(event.source_quality), 0.35)
    payload: dict[str, Any] = {
        "world_event_id": event.id,
        "themes": list(event.themes),
        "direction": event.direction,
        "sources": [source.as_payload() for source in event.sources],
        "origin": "discovery_fanout",
    }
    return CanonicalEvent(
        id=f"world:{event.id}:{ticker.upper()}",
        ticker=ticker.upper(),
        event_type=EventType.NEWS,
        provider="world_events",
        source="grok_x_world_events",
        source_category=source_category,
        source_url=canonical_url,
        title=event.title[:240],
        body_hash=content_hash,
        dedupe_key=key if key.startswith(ticker.upper()) else f"{ticker.upper()}:world_events:{event.id}",
        source_quality=source_quality,
        materiality=materiality,
        source_ts=event.available_at,
        available_at=event.available_at,
        payload=payload,
    )

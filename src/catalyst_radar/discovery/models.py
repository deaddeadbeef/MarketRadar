from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any


WORLD_EVENTS_SCHEMA = "world-events-v1"
DISCOVERY_BRIEF_SCHEMA = "discovery-brief-v1"


@dataclass(frozen=True)
class EventSource:
    provider: str
    url: str | None
    author: str | None
    published_at: datetime | None
    engagement: Mapping[str, Any]

    def as_payload(self) -> dict[str, object]:
        return {
            "provider": self.provider,
            "url": self.url,
            "author": self.author,
            "published_at": self.published_at.isoformat() if self.published_at else None,
            "engagement": dict(self.engagement),
        }


@dataclass(frozen=True)
class WorldEvent:
    id: str
    title: str
    summary: str
    themes: tuple[str, ...]
    tickers: tuple[str, ...]
    secondary_tickers: tuple[str, ...]
    direction: str
    materiality: float
    source_quality: float
    source_category: str
    sources: tuple[EventSource, ...]
    available_at: datetime

    def as_payload(self) -> dict[str, object]:
        return {
            "id": self.id,
            "title": self.title,
            "summary": self.summary,
            "themes": list(self.themes),
            "tickers": list(self.tickers),
            "secondary_tickers": list(self.secondary_tickers),
            "direction": self.direction,
            "materiality": self.materiality,
            "source_quality": self.source_quality,
            "source_category": self.source_category,
            "sources": [source.as_payload() for source in self.sources],
            "available_at": self.available_at.isoformat(),
        }


@dataclass(frozen=True)
class WorldEventBundle:
    schema_version: str
    generated_at: datetime
    source: str
    events: tuple[WorldEvent, ...]

    def as_payload(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "generated_at": self.generated_at.isoformat(),
            "source": self.source,
            "events": [event.as_payload() for event in self.events],
        }


def parse_datetime(value: object, field: str) -> datetime:
    if value is None or value == "":
        return datetime.now(tz=UTC)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        msg = f"{field} must be an ISO-8601 datetime"
        raise ValueError(msg) from exc
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def clamp_score(value: object, default: float = 0.0) -> float:
    try:
        number = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        number = default
    if number != number:  # NaN
        number = default
    return max(0.0, min(1.0, number))


def normalize_tickers(values: object) -> tuple[str, ...]:
    if values is None:
        return ()
    if isinstance(values, str):
        text = values.strip()
        if text.startswith("[") and text.endswith("]"):
            text = text[1:-1]
        raw = [part.strip().strip("'\"") for part in text.replace(";", ",").split(",")]
    elif isinstance(values, Sequence):
        raw = [str(item).strip().strip("'\"[]") for item in values]
    else:
        return ()
    seen: set[str] = set()
    ordered: list[str] = []
    for item in raw:
        ticker = item.upper().strip()
        # Reject YAML-bracket debris and non-symbols (e.g. "[MU", "SNDK]").
        if not ticker or ticker in seen:
            continue
        if not all(ch.isalnum() or ch in {".", "-", "^"} for ch in ticker):
            continue
        if not any(ch.isalpha() for ch in ticker):
            continue
        seen.add(ticker)
        ordered.append(ticker)
    return tuple(ordered)


def normalize_themes(values: object) -> tuple[str, ...]:
    if values is None:
        return ()
    if isinstance(values, str):
        raw = [part.strip() for part in values.replace(";", ",").split(",")]
    elif isinstance(values, Sequence):
        raw = [str(item).strip() for item in values]
    else:
        return ()
    return tuple(theme for theme in (item.casefold().replace(" ", "_") for item in raw) if theme)

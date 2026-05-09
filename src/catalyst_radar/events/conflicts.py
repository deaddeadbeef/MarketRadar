from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from typing import Any

from catalyst_radar.events.models import CanonicalEvent, EventType

_RAISE_PHRASES = (
    "raises guidance",
    "raised guidance",
    "raises full-year guidance",
    "outlook raised",
)
_CUT_PHRASES = (
    "cuts guidance",
    "cut guidance",
    "cuts full-year guidance",
    "outlook cut",
)


def detect_event_conflicts(events: Iterable[CanonicalEvent]) -> tuple[dict[str, Any], ...]:
    guidance_by_ticker: dict[str, list[CanonicalEvent]] = defaultdict(list)
    for event in events:
        if (
            event.event_type == EventType.GUIDANCE
            and event.source_quality >= 0.5
            and event.materiality >= 0.5
        ):
            guidance_by_ticker[event.ticker.upper()].append(event)

    conflicts: list[dict[str, Any]] = []
    for ticker in sorted(guidance_by_ticker):
        raise_ids: list[str] = []
        cut_ids: list[str] = []
        for event in guidance_by_ticker[ticker]:
            text = _event_text(event)
            if _contains_any(text, _RAISE_PHRASES):
                raise_ids.append(event.id)
            if _contains_any(text, _CUT_PHRASES):
                cut_ids.append(event.id)
        if raise_ids and cut_ids:
            conflicts.append(
                {
                    "ticker": ticker,
                    "conflict_type": "guidance_direction_conflict",
                    "source_event_ids": [raise_ids[0], cut_ids[0]],
                }
            )

    return tuple(sorted(conflicts, key=lambda item: (item["ticker"], item["conflict_type"])))


def _event_text(event: CanonicalEvent) -> str:
    payload_body = event.payload.get("body", "")
    return f"{event.title} {payload_body}".casefold()


def _contains_any(value: str, phrases: tuple[str, ...]) -> bool:
    return any(phrase.casefold() in value for phrase in phrases)

from __future__ import annotations

from dataclasses import fields, is_dataclass
from typing import Any

from catalyst_radar.events.models import EventClassification, EventType, RawEvent
from catalyst_radar.events.source_quality import score_source_quality

_BASE_MATERIALITY = {
    EventType.EARNINGS: 0.65,
    EventType.GUIDANCE: 0.75,
    EventType.SEC_FILING: 0.60,
    EventType.INSIDER: 0.45,
    EventType.ANALYST_REVISION: 0.45,
    EventType.SECTOR_READ_THROUGH: 0.40,
    EventType.PRODUCT_CUSTOMER: 0.45,
    EventType.LEGAL_REGULATORY: 0.55,
    EventType.FINANCING: 0.55,
    EventType.CORPORATE_ACTION: 0.60,
    EventType.NEWS: 0.35,
}

_GUIDANCE_PHRASES = ("raises guidance", "cuts guidance", "revises guidance", "outlook")
_GUIDANCE_RESULTS_PHRASES = ("guidance", "results", "results of operations", "financial condition")


def classify_event(raw_event: RawEvent) -> EventClassification:
    source_quality = score_source_quality(
        source=raw_event.source,
        category=raw_event.source_category,
        url=raw_event.url,
    )
    text = f"{raw_event.title} {raw_event.body}".casefold()
    form_type = str(raw_event.payload.get("form_type", "")).upper()

    event_type = EventType.NEWS
    reasons: list[str] = []
    minimum_materiality: float | None = None

    if form_type == "8-K" and _contains_any(text, _GUIDANCE_RESULTS_PHRASES):
        event_type = EventType.GUIDANCE
        reasons.append("sec_form_8k_guidance_results")
        minimum_materiality = 0.80
    elif form_type in {"10-Q", "10-K"}:
        event_type = EventType.SEC_FILING
        reasons.append(f"sec_form_{form_type.casefold()}")
        minimum_materiality = 0.65
    elif _contains_any(text, ("earnings", "results", "quarter")):
        event_type = EventType.EARNINGS
        reasons.append("earnings_language")
        minimum_materiality = 0.65
    elif _contains_any(text, _GUIDANCE_PHRASES):
        event_type = EventType.GUIDANCE
        reasons.append("guidance_language")
        minimum_materiality = 0.75
    elif _contains_any(text, ("insider", "form 4", "director purchased", "officer purchased")):
        event_type = EventType.INSIDER
        reasons.append("insider_language")
    elif _contains_any(text, ("upgrade", "downgrade", "price target", "revision")):
        event_type = EventType.ANALYST_REVISION
        reasons.append("analyst_revision_language")
    elif _contains_any(text, ("lawsuit", "investigation", "regulatory", "fda")):
        event_type = EventType.LEGAL_REGULATORY
        reasons.append("legal_regulatory_language")
    elif _contains_any(text, ("offering", "convertible", "debt", "financing")):
        event_type = EventType.FINANCING
        reasons.append("financing_language")
    elif _contains_any(text, ("split", "dividend", "merger", "spinoff")):
        event_type = EventType.CORPORATE_ACTION
        reasons.append("corporate_action_language")
    else:
        reasons.append("fallback_news")

    materiality = _materiality(event_type, source_quality.score)
    if minimum_materiality is not None:
        materiality = max(materiality, minimum_materiality)
    if source_quality.score < 0.35:
        materiality = min(materiality, 0.35)

    requires_confirmation = source_quality.score < 0.35
    requires_text_triage = event_type in {
        EventType.GUIDANCE,
        EventType.SEC_FILING,
        EventType.LEGAL_REGULATORY,
        EventType.FINANCING,
    }

    return _build_classification(
        event_type=event_type,
        source_quality=source_quality.score,
        materiality=_clamp(materiality, 0.0, 1.0),
        reasons=tuple((*source_quality.reasons, *reasons)),
        requires_confirmation=requires_confirmation,
        requires_text_triage=requires_text_triage,
    )


def _build_classification(**kwargs: Any) -> EventClassification:
    if is_dataclass(EventClassification):
        field_names = {field.name for field in fields(EventClassification)}
        filtered_kwargs = {key: value for key, value in kwargs.items() if key in field_names}
        return EventClassification(**filtered_kwargs)
    return EventClassification(**kwargs)


def _materiality(event_type: EventType, source_quality: float) -> float:
    score = _BASE_MATERIALITY[event_type] + ((source_quality - 0.5) * 0.30)
    return _clamp(score, 0.0, 1.0)


def _contains_any(value: str, phrases: tuple[str, ...]) -> bool:
    return any(phrase.casefold() in value for phrase in phrases)


def _clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, float(value)))

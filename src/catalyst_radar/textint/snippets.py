from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from catalyst_radar.events.models import CanonicalEvent, EventType
from catalyst_radar.textint.embeddings import embed_text
from catalyst_radar.textint.ontology import OntologyHit, ThemeDefinition, match_ontology
from catalyst_radar.textint.sentiment import score_sentiment


@dataclass(frozen=True)
class TextSnippet:
    id: str
    snippet_hash: str
    section: str
    event_id: str
    ticker: str
    event_type: str
    provider: str
    source: str
    source_category: str
    source_url: str | None
    source_ts: datetime
    available_at: datetime
    title: str
    text: str
    source_quality: float
    materiality: float
    ontology_hits: tuple[OntologyHit, ...]
    sentiment: float
    embedding: tuple[float, ...]
    payload: Mapping[str, Any]
    rank_score: tuple[float, float, float, int]

    @property
    def ontology_theme_ids(self) -> tuple[str, ...]:
        return tuple(hit.theme_id for hit in self.ontology_hits)


def extract_snippets(
    events: Iterable[CanonicalEvent],
    *,
    ontology: Mapping[str, ThemeDefinition] | None = None,
    limit: int | None = None,
) -> tuple[TextSnippet, ...]:
    snippets = tuple(_snippet_from_event(event, ontology=ontology) for event in events)
    ranked = rank_snippets(snippets)
    if limit is None:
        return ranked
    if limit < 0:
        msg = "limit must be non-negative"
        raise ValueError(msg)
    return ranked[:limit]


def select_snippets(
    events: Iterable[CanonicalEvent],
    *,
    ontology: Mapping[str, ThemeDefinition] | None = None,
    limit: int | None = None,
) -> tuple[TextSnippet, ...]:
    return extract_snippets(events, ontology=ontology, limit=limit)


def rank_snippets(snippets: Iterable[TextSnippet]) -> tuple[TextSnippet, ...]:
    return tuple(
        sorted(
            snippets,
            key=lambda snippet: (
                -snippet.source_quality,
                -snippet.materiality,
                -sum(hit.score for hit in snippet.ontology_hits),
                -_event_type_priority(snippet.event_type),
                snippet.source_ts,
                snippet.event_id,
                snippet.snippet_hash,
            ),
        )
    )


def stable_snippet_hash(*, event_id: str, ticker: str, text: str) -> str:
    payload = {
        "event_id": str(event_id),
        "ticker": str(ticker).upper(),
        "text": _normalize_text(text),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _snippet_from_event(
    event: CanonicalEvent,
    *,
    ontology: Mapping[str, ThemeDefinition] | None,
) -> TextSnippet:
    text = _event_text(event)
    ontology_hits = match_ontology(text, ontology)
    ontology_score = sum(hit.score for hit in ontology_hits)
    snippet_id = stable_snippet_hash(event_id=event.id, ticker=event.ticker, text=text)
    rank_score = (
        float(event.source_quality),
        float(event.materiality),
        float(ontology_score),
        _event_type_priority(event.event_type),
    )
    return TextSnippet(
        id=snippet_id,
        snippet_hash=snippet_id,
        section="event",
        event_id=event.id,
        ticker=event.ticker,
        event_type=event.event_type.value,
        provider=event.provider,
        source=event.source,
        source_category=event.source_category.value,
        source_url=event.source_url,
        source_ts=event.source_ts,
        available_at=event.available_at,
        title=event.title,
        text=text,
        source_quality=event.source_quality,
        materiality=event.materiality,
        ontology_hits=ontology_hits,
        sentiment=score_sentiment(text),
        embedding=embed_text(text),
        payload={
            "body_hash": event.body_hash,
            "dedupe_key": event.dedupe_key,
            "ontology_hits": [
                {
                    "theme_id": hit.theme_id,
                    "terms": list(hit.matched_terms),
                    "score": hit.score,
                }
                for hit in ontology_hits
            ],
        },
        rank_score=rank_score,
    )


def _event_text(event: CanonicalEvent) -> str:
    parts = [event.title, *_payload_text_parts(event.payload)]
    return _normalize_text(" ".join(part for part in parts if part))


def _payload_text_parts(payload: Mapping[str, Any]) -> tuple[str, ...]:
    body_parts: list[str] = []
    other_parts: list[str] = []
    for key, value in sorted(payload.items(), key=lambda item: str(item[0])):
        if key in {"body", "summary", "description", "items"}:
            body_parts.extend(_flatten_text(value))
        else:
            other_parts.extend(_flatten_text(value))
    return tuple((*body_parts, *other_parts))


def _flatten_text(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        text = value.strip()
        return (text,) if text else ()
    if isinstance(value, Mapping):
        parts: list[str] = []
        for key in sorted(value):
            parts.extend(_flatten_text(value[key]))
        return tuple(parts)
    if isinstance(value, Sequence) and not isinstance(value, bytes | bytearray):
        parts = []
        for item in value:
            parts.extend(_flatten_text(item))
        return tuple(parts)
    return ()


def _normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _event_type_priority(event_type: EventType | str) -> int:
    value = event_type.value if isinstance(event_type, EventType) else str(event_type)
    return {
        EventType.GUIDANCE.value: 100,
        EventType.SEC_FILING.value: 90,
        EventType.EARNINGS.value: 80,
        EventType.LEGAL_REGULATORY.value: 70,
        EventType.SECTOR_READ_THROUGH.value: 60,
        EventType.PRODUCT_CUSTOMER.value: 55,
        EventType.FINANCING.value: 50,
        EventType.CORPORATE_ACTION.value: 45,
        EventType.ANALYST_REVISION.value: 40,
        EventType.INSIDER.value: 35,
        EventType.NEWS.value: 10,
    }.get(value, 0)

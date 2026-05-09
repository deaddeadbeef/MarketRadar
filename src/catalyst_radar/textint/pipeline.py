from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import select

from catalyst_radar.events.models import CanonicalEvent, EventType, SourceCategory
from catalyst_radar.storage.event_repositories import EventRepository
from catalyst_radar.storage.schema import events
from catalyst_radar.storage.text_repositories import TextRepository
from catalyst_radar.textint.models import TextFeature, TextSnippet
from catalyst_radar.textint.novelty import score_novelty
from catalyst_radar.textint.ontology import load_ontology
from catalyst_radar.textint.snippets import TextSnippet as ExtractedSnippet
from catalyst_radar.textint.snippets import extract_snippets

TEXT_FEATURE_VERSION = "textint-v1"
DEFAULT_SNIPPET_LIMIT_PER_TICKER = 5


@dataclass(frozen=True)
class TextPipelineResult:
    feature_count: int
    snippet_count: int
    features: tuple[TextFeature, ...]
    snippets: tuple[TextSnippet, ...]


def run_text_pipeline(
    event_repo: EventRepository,
    text_repo: TextRepository,
    *,
    as_of: datetime,
    available_at: datetime,
    ontology_path: Path | str = Path("config/themes.yaml"),
    tickers: Iterable[str] | None = None,
    snippet_limit_per_ticker: int = DEFAULT_SNIPPET_LIMIT_PER_TICKER,
) -> TextPipelineResult:
    as_of_dt = _to_utc_datetime(as_of, "as_of")
    available_at_dt = _to_utc_datetime(available_at, "available_at")
    ontology = load_ontology(ontology_path)
    canonical_events = _events_for_pipeline(
        event_repo=event_repo,
        as_of=as_of_dt,
        available_at=available_at_dt,
        tickers=tickers,
    )
    if not canonical_events:
        return TextPipelineResult(feature_count=0, snippet_count=0, features=(), snippets=())

    snippets_by_ticker: dict[str, list[ExtractedSnippet]] = defaultdict(list)
    for snippet in extract_snippets(canonical_events, ontology=ontology):
        snippets_by_ticker[snippet.ticker].append(snippet)

    persisted_snippets: list[TextSnippet] = []
    features: list[TextFeature] = []
    for ticker in sorted(snippets_by_ticker):
        selected = _dedupe_snippets(snippets_by_ticker[ticker])[:snippet_limit_per_ticker]
        if not selected:
            continue
        stored_snippets = [_stored_snippet(snippet) for snippet in selected]
        persisted_snippets.extend(stored_snippets)
        features.append(
            _text_feature(
                ticker=ticker,
                as_of=as_of_dt,
                selected=selected,
                stored_snippets=stored_snippets,
                prior_snippets=text_repo.list_snippets_for_ticker(
                    ticker,
                    as_of=as_of_dt,
                    available_at=available_at_dt,
                    limit=50,
                ),
            )
        )

    snippet_count = text_repo.upsert_snippets(persisted_snippets)
    feature_count = text_repo.upsert_text_features(features)
    return TextPipelineResult(
        feature_count=feature_count,
        snippet_count=snippet_count,
        features=tuple(features),
        snippets=tuple(persisted_snippets),
    )


def _events_for_pipeline(
    *,
    event_repo: EventRepository,
    as_of: datetime,
    available_at: datetime,
    tickers: Iterable[str] | None,
) -> list[CanonicalEvent]:
    filters = [
        events.c.source_ts <= as_of,
        events.c.available_at <= available_at,
    ]
    normalized_tickers = _normalized_tickers(tickers)
    if normalized_tickers:
        filters.append(events.c.ticker.in_(normalized_tickers))
    stmt = (
        select(events)
        .where(*filters)
        .order_by(
            events.c.ticker,
            events.c.source_ts.desc(),
            events.c.available_at.desc(),
            events.c.materiality.desc(),
            events.c.id.desc(),
        )
    )
    with event_repo.engine.connect() as conn:
        return [_event_from_row(row._mapping) for row in conn.execute(stmt)]


def _dedupe_snippets(snippets: Iterable[ExtractedSnippet]) -> list[ExtractedSnippet]:
    by_hash: dict[str, ExtractedSnippet] = {}
    for snippet in snippets:
        by_hash.setdefault(snippet.snippet_hash, snippet)
    return sorted(
        by_hash.values(),
        key=lambda snippet: (
            -snippet.source_quality,
            -snippet.materiality,
            -sum(hit.score for hit in snippet.ontology_hits),
            snippet.source_ts,
            snippet.id,
        ),
    )


def _stored_snippet(snippet: ExtractedSnippet) -> TextSnippet:
    return TextSnippet(
        id=snippet.id,
        ticker=snippet.ticker,
        event_id=snippet.event_id,
        snippet_hash=snippet.snippet_hash,
        section=snippet.section,
        text=snippet.text,
        source=snippet.source,
        source_url=snippet.source_url,
        source_quality=snippet.source_quality,
        event_type=snippet.event_type,
        materiality=snippet.materiality,
        ontology_hits=snippet.ontology_hit_payloads,
        sentiment=snippet.sentiment,
        embedding=snippet.embedding,
        source_ts=snippet.source_ts,
        available_at=snippet.available_at,
        payload={
            **dict(snippet.payload),
            "provider": snippet.provider,
            "source_category": snippet.source_category,
            "title": snippet.title,
        },
    )


def _text_feature(
    *,
    ticker: str,
    as_of: datetime,
    selected: list[ExtractedSnippet],
    stored_snippets: list[TextSnippet],
    prior_snippets: list[TextSnippet],
) -> TextFeature:
    novelty_scores = [
        score_novelty(snippet.text, prior_snippets)
        for snippet in selected
    ]
    source_quality_score = _average(snippet.source_quality * 100 for snippet in selected)
    materiality_score = _average(snippet.materiality * 100 for snippet in selected)
    sentiment_score = _average(snippet.sentiment * 100 for snippet in selected)
    novelty_score = _average(novelty_scores)
    theme_hits = _theme_hits(selected)
    theme_match_score = min(
        100.0,
        sum(float(hit["count"]) for hit in theme_hits) * 25.0,
    )
    conflict_penalty = 0.0
    positive_sentiment_component = max(0.0, sentiment_score + 100.0) / 2.0
    local_narrative_score = _clamp(
        (source_quality_score * 0.30)
        + (theme_match_score * 0.25)
        + (novelty_score * 0.20)
        + (materiality_score * 0.15)
        + (positive_sentiment_component * 0.10)
        - conflict_penalty,
        0.0,
        100.0,
    )
    source_ts = max(snippet.source_ts for snippet in selected)
    available_at = max(snippet.available_at for snippet in selected)
    feature_id = _feature_id(ticker=ticker, as_of=as_of, version=TEXT_FEATURE_VERSION)
    return TextFeature(
        id=feature_id,
        ticker=ticker,
        as_of=as_of,
        feature_version=TEXT_FEATURE_VERSION,
        local_narrative_score=round(local_narrative_score, 2),
        novelty_score=round(novelty_score, 2),
        sentiment_score=round(sentiment_score, 2),
        source_quality_score=round(source_quality_score, 2),
        theme_match_score=round(theme_match_score, 2),
        conflict_penalty=conflict_penalty,
        selected_snippet_ids=[snippet.id for snippet in stored_snippets],
        theme_hits=theme_hits,
        source_ts=source_ts,
        available_at=available_at,
        payload={
            "snippet_count": len(stored_snippets),
            "event_ids": [snippet.event_id for snippet in selected],
            "snippet_hashes": [snippet.snippet_hash for snippet in selected],
        },
    )


def _theme_hits(snippets: Iterable[ExtractedSnippet]) -> list[dict[str, object]]:
    buckets: dict[str, dict[str, object]] = {}
    for snippet in snippets:
        for hit in snippet.ontology_hits:
            bucket = buckets.setdefault(
                hit.theme_id,
                {"theme_id": hit.theme_id, "count": 0, "terms": set()},
            )
            bucket["count"] = int(bucket["count"]) + 1
            terms = bucket["terms"]
            if isinstance(terms, set):
                terms.update(hit.matched_terms)
    return [
        {
            "theme_id": theme_id,
            "count": bucket["count"],
            "terms": sorted(bucket["terms"]) if isinstance(bucket["terms"], set) else [],
        }
        for theme_id, bucket in sorted(buckets.items())
    ]


def _event_from_row(row: Mapping[str, Any]) -> CanonicalEvent:
    return CanonicalEvent(
        id=row["id"],
        ticker=row["ticker"],
        event_type=EventType(row["event_type"]),
        provider=row["provider"],
        source=row["source"],
        source_category=SourceCategory(row["source_category"]),
        source_url=row["source_url"],
        title=row["title"],
        body_hash=row["body_hash"],
        dedupe_key=row["dedupe_key"],
        source_quality=row["source_quality"],
        materiality=row["materiality"],
        source_ts=_as_datetime(row["source_ts"]),
        available_at=_as_datetime(row["available_at"]),
        payload=row["payload"],
    )


def _feature_id(*, ticker: str, as_of: datetime, version: str) -> str:
    payload = {
        "ticker": ticker.upper(),
        "as_of": as_of.isoformat(),
        "version": version,
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _normalized_tickers(tickers: Iterable[str] | None) -> tuple[str, ...]:
    if tickers is None:
        return ()
    return tuple(sorted({ticker.upper() for ticker in tickers if ticker.strip()}))


def _average(values: Iterable[float]) -> float:
    numbers = tuple(float(value) for value in values)
    if not numbers:
        return 0.0
    return sum(numbers) / len(numbers)


def _clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, float(value)))


def _to_utc_datetime(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        msg = f"{field_name} must be a datetime"
        raise TypeError(msg)
    if value.tzinfo is None or value.utcoffset() is None:
        msg = f"{field_name} must be timezone-aware"
        raise ValueError(msg)
    return value.astimezone(UTC)


def _as_datetime(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


__all__ = ["TEXT_FEATURE_VERSION", "TextPipelineResult", "run_text_pipeline"]

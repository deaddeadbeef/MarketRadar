from datetime import UTC, datetime

from sqlalchemy import create_engine, func, select

from catalyst_radar.events.models import CanonicalEvent, EventType, SourceCategory
from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.schema import text_snippets
from catalyst_radar.storage.text_repositories import TextRepository
from catalyst_radar.textint.models import TextSnippet as StoredTextSnippet
from catalyst_radar.textint.snippets import extract_snippets


def test_high_quality_ontology_event_ranks_above_promotional_event() -> None:
    ranked = extract_snippets(
        [
            canonical_event(
                event_id="promo",
                title="MSFT could double",
                body="Sponsored recap with no clear read-through.",
                source="Sponsored Stocks Daily",
                source_category=SourceCategory.PROMOTIONAL,
                event_type=EventType.NEWS,
                source_quality=0.1,
                materiality=0.25,
            ),
            canonical_event(
                event_id="sec",
                title="MSFT 8-K",
                body="NAND and SSD demand are creating an inference storage bottleneck.",
                source="SEC EDGAR",
                source_category=SourceCategory.PRIMARY_SOURCE,
                event_type=EventType.GUIDANCE,
                source_quality=1.0,
                materiality=0.85,
            ),
        ]
    )

    assert [snippet.event_id for snippet in ranked] == ["sec", "promo"]
    assert ranked[0].ontology_theme_ids == ("ai_infrastructure_storage",)
    assert ranked[0].snippet_hash == extract_snippets(
        [canonical_event(event_id="sec")]
    )[0].snippet_hash


def test_extracted_snippet_can_be_persisted_with_json_ready_ontology_hits() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    create_schema(engine)
    repo = TextRepository(engine)
    extracted = extract_snippets([canonical_event(event_id="sec")])[0]

    repo.upsert_snippets([stored_snippet_from_extracted(extracted)])

    with engine.connect() as conn:
        assert conn.scalar(select(func.count()).select_from(text_snippets)) == 1


def test_snippet_hash_ignores_non_text_payload_metadata() -> None:
    first = extract_snippets(
        [canonical_event(event_id="sec", extra_payload={"published_at": "2026-05-10"})]
    )[0]
    second = extract_snippets(
        [canonical_event(event_id="sec", extra_payload={"published_at": "2026-05-11"})]
    )[0]

    assert first.text == second.text
    assert first.snippet_hash == second.snippet_hash


def canonical_event(
    *,
    event_id: str,
    title: str = "MSFT 8-K",
    body: str = "NAND and SSD demand are creating an inference storage bottleneck.",
    source: str = "SEC EDGAR",
    source_category: SourceCategory = SourceCategory.PRIMARY_SOURCE,
    event_type: EventType = EventType.GUIDANCE,
    source_quality: float = 1.0,
    materiality: float = 0.85,
    extra_payload: dict[str, object] | None = None,
) -> CanonicalEvent:
    payload = {"body": body}
    if extra_payload:
        payload.update(extra_payload)
    return CanonicalEvent(
        id=event_id,
        ticker="MSFT",
        event_type=event_type,
        provider="fixture",
        source=source,
        source_category=source_category,
        source_url=f"https://example.com/{event_id}",
        title=title,
        body_hash=event_id,
        dedupe_key=f"MSFT:{event_id}",
        source_quality=source_quality,
        materiality=materiality,
        source_ts=datetime(2026, 5, 10, 12, tzinfo=UTC),
        available_at=datetime(2026, 5, 10, 13, tzinfo=UTC),
        payload=payload,
    )


def stored_snippet_from_extracted(extracted) -> StoredTextSnippet:
    return StoredTextSnippet(
        id=extracted.id,
        ticker=extracted.ticker,
        event_id=extracted.event_id,
        snippet_hash=extracted.snippet_hash,
        section=extracted.section,
        text=extracted.text,
        source=extracted.source,
        source_url=extracted.source_url,
        source_quality=extracted.source_quality,
        event_type=extracted.event_type,
        materiality=extracted.materiality,
        ontology_hits=extracted.ontology_hit_payloads,
        sentiment=extracted.sentiment,
        embedding=extracted.embedding,
        source_ts=extracted.source_ts,
        available_at=extracted.available_at,
        payload=extracted.payload,
    )

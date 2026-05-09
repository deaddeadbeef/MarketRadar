from datetime import UTC, datetime

from catalyst_radar.events.models import CanonicalEvent, EventType, SourceCategory
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
) -> CanonicalEvent:
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
        payload={"body": body},
    )

from datetime import UTC, datetime

from catalyst_radar.events.conflicts import detect_event_conflicts
from catalyst_radar.events.models import CanonicalEvent, EventType, SourceCategory


def test_detects_conflicting_guidance_direction() -> None:
    conflicts = detect_event_conflicts(
        [
            event("raise", "MSFT raises full-year guidance", "Raises FY guidance."),
            event("cut", "MSFT cuts full-year guidance", "Cuts FY guidance."),
        ]
    )

    assert conflicts == (
        {
            "ticker": "MSFT",
            "conflict_type": "guidance_direction_conflict",
            "source_event_ids": ["raise", "cut"],
        },
    )


def test_ignores_low_quality_promotional_conflict() -> None:
    conflicts = detect_event_conflicts(
        [
            event("raise", "MSFT raises full-year guidance", "Raises FY guidance."),
            event(
                "promo-cut",
                "MSFT cuts full-year guidance",
                "Cuts FY guidance.",
                source_quality=0.1,
                source_category=SourceCategory.PROMOTIONAL,
            ),
        ]
    )

    assert conflicts == ()


def event(
    event_id: str,
    title: str,
    body: str,
    *,
    source_quality: float = 0.85,
    source_category: SourceCategory = SourceCategory.REPUTABLE_NEWS,
) -> CanonicalEvent:
    return CanonicalEvent(
        id=event_id,
        ticker="MSFT",
        event_type=EventType.GUIDANCE,
        provider="news_fixture",
        source="Reuters",
        source_category=source_category,
        source_url=f"https://reuters.example.com/{event_id}",
        title=title,
        body_hash=event_id,
        dedupe_key=f"MSFT:{event_id}",
        source_quality=source_quality,
        materiality=0.8,
        source_ts=datetime(2026, 5, 10, 12, tzinfo=UTC),
        available_at=datetime(2026, 5, 10, 12, tzinfo=UTC),
        payload={"body": body},
    )

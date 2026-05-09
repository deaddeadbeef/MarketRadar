from datetime import UTC, datetime

from catalyst_radar.events.classifier import classify_event
from catalyst_radar.events.models import EventType, RawEvent, SourceCategory


def test_sec_8k_guidance_is_high_materiality() -> None:
    result = classify_event(
        RawEvent(
            ticker="MSFT",
            provider="sec",
            source="SEC EDGAR",
            source_category=SourceCategory.PRIMARY_SOURCE,
            title="MSFT 8-K guidance update",
            body="Item 2.02 results of operations and financial condition. Raises guidance.",
            url="https://www.sec.gov/example",
            source_ts=datetime(2026, 5, 10, 12, tzinfo=UTC),
            available_at=datetime(2026, 5, 10, 13, tzinfo=UTC),
            payload={"form_type": "8-K"},
        )
    )

    assert result.event_type == EventType.GUIDANCE
    assert result.materiality >= 0.8
    assert result.requires_text_triage is True


def test_low_quality_promotional_news_is_not_material_alone() -> None:
    result = classify_event(
        RawEvent(
            ticker="MSFT",
            provider="news",
            source="Sponsored Stocks Daily",
            source_category=SourceCategory.PROMOTIONAL,
            title="MSFT could double soon",
            body="Sponsored promotional recap.",
            url="https://promo.example.com/msft",
            source_ts=datetime(2026, 5, 10, 12, tzinfo=UTC),
            available_at=datetime(2026, 5, 10, 12, tzinfo=UTC),
            payload={},
        )
    )

    assert result.event_type == EventType.NEWS
    assert result.source_quality <= 0.2
    assert result.materiality <= 0.35
    assert result.requires_confirmation is True

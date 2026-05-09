from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import create_engine, func, select

from catalyst_radar.events.models import CanonicalEvent, EventType, SourceCategory
from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.event_repositories import EventRepository
from catalyst_radar.storage.schema import events


def test_upsert_event_dedupes_by_dedupe_key() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    create_schema(engine)
    repo = EventRepository(engine)
    event = canonical_event()

    assert repo.upsert_events([event]) == 1
    assert repo.upsert_events([event]) == 1

    with engine.connect() as conn:
        assert conn.scalar(select(func.count()).select_from(events)) == 1


def test_upsert_event_replaces_existing_dedupe_key_row() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    create_schema(engine)
    repo = EventRepository(engine)

    repo.upsert_events([canonical_event(event_id="older", title="Old title")])
    repo.upsert_events([canonical_event(event_id="newer", title="New title")])

    rows = repo.list_events_for_ticker(
        "MSFT",
        as_of=datetime(2026, 5, 10, 21, tzinfo=UTC),
        available_at=datetime(2026, 5, 10, 21, tzinfo=UTC),
    )

    assert [row.id for row in rows] == ["newer"]
    assert rows[0].title == "New title"


def test_list_events_for_ticker_respects_available_at() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    create_schema(engine)
    repo = EventRepository(engine)
    repo.upsert_events(
        [
            canonical_event(
                event_id="past",
                dedupe_key="MSFT:past",
                available_at=datetime(2026, 5, 10, 13, tzinfo=UTC),
            ),
            canonical_event(
                event_id="future",
                dedupe_key="MSFT:future",
                available_at=datetime(2026, 5, 10, 15, tzinfo=UTC),
            ),
        ]
    )

    rows = repo.list_events_for_ticker(
        "MSFT",
        as_of=datetime(2026, 5, 10, 21, tzinfo=UTC),
        available_at=datetime(2026, 5, 10, 14, tzinfo=UTC),
    )

    assert [row.id for row in rows] == ["past"]


def test_list_events_for_ticker_respects_source_as_of_and_materiality() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    create_schema(engine)
    repo = EventRepository(engine)
    repo.upsert_events(
        [
            canonical_event(
                event_id="material",
                dedupe_key="MSFT:material",
                materiality=0.75,
                source_ts=datetime(2026, 5, 10, 12, tzinfo=UTC),
            ),
            canonical_event(
                event_id="low",
                dedupe_key="MSFT:low",
                materiality=0.2,
                source_ts=datetime(2026, 5, 10, 12, tzinfo=UTC),
            ),
            canonical_event(
                event_id="future-source",
                dedupe_key="MSFT:future-source",
                materiality=0.9,
                source_ts=datetime(2026, 5, 11, 12, tzinfo=UTC),
                available_at=datetime(2026, 5, 11, 13, tzinfo=UTC),
            ),
        ]
    )

    rows = repo.list_events_for_ticker(
        "msft",
        as_of=datetime(2026, 5, 10, 21, tzinfo=UTC),
        available_at=datetime(2026, 5, 12, 14, tzinfo=UTC),
        min_materiality=0.5,
    )

    assert [row.id for row in rows] == ["material"]


def test_latest_material_events_by_ticker_limits_each_ticker() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    create_schema(engine)
    repo = EventRepository(engine)
    repo.upsert_events(
        [
            canonical_event(
                event_id="msft-old",
                dedupe_key="MSFT:old",
                source_ts=datetime(2026, 5, 10, 10, tzinfo=UTC),
            ),
            canonical_event(
                event_id="msft-new",
                dedupe_key="MSFT:new",
                source_ts=datetime(2026, 5, 10, 12, tzinfo=UTC),
            ),
            canonical_event(
                event_id="aapl-new",
                ticker="AAPL",
                dedupe_key="AAPL:new",
                source_ts=datetime(2026, 5, 10, 12, tzinfo=UTC),
            ),
        ]
    )

    rows = repo.latest_material_events_by_ticker(
        ["msft", "AAPL", "MSFT"],
        as_of=datetime(2026, 5, 10, 21, tzinfo=UTC),
        available_at=datetime(2026, 5, 10, 21, tzinfo=UTC),
        min_materiality=0.5,
        limit_per_ticker=1,
    )

    assert list(rows) == ["AAPL", "MSFT"]
    assert [row.id for row in rows["MSFT"]] == ["msft-new"]
    assert [row.id for row in rows["AAPL"]] == ["aapl-new"]


def test_canonical_event_coerces_ticker_clamps_scores_and_freezes_payload() -> None:
    event = canonical_event(
        ticker="msft",
        source_quality=1.5,
        materiality=-0.25,
        payload={"nested": {"value": 1}},
    )

    assert event.ticker == "MSFT"
    assert event.source_quality == 1.0
    assert event.materiality == 0.0
    with pytest.raises(TypeError):
        event.payload["nested"] = {"value": 2}  # type: ignore[index]


def test_canonical_event_requires_aware_event_timestamps() -> None:
    with pytest.raises(ValueError, match="source_ts"):
        canonical_event(source_ts=datetime(2026, 5, 10, 12))

    with pytest.raises(ValueError, match="available_at"):
        canonical_event(available_at=datetime(2026, 5, 10, 13))


def test_canonical_event_rejects_available_before_source_timestamp() -> None:
    with pytest.raises(ValueError, match="available_at"):
        canonical_event(
            source_ts=datetime(2026, 5, 10, 13, tzinfo=UTC),
            available_at=datetime(2026, 5, 10, 12, tzinfo=UTC),
        )


def canonical_event(**overrides: object) -> CanonicalEvent:
    event_id = str(overrides.pop("event_id", "event-1"))
    values = {
        "id": event_id,
        "ticker": "MSFT",
        "event_type": EventType.SEC_FILING,
        "provider": "sec",
        "source": "SEC EDGAR",
        "source_category": SourceCategory.PRIMARY_SOURCE,
        "source_url": "https://www.sec.gov/Archives/example",
        "title": "MSFT 8-K",
        "body_hash": "body-hash",
        "dedupe_key": "MSFT:sec:8-k:2026-05-10",
        "source_quality": 1.0,
        "materiality": 0.85,
        "source_ts": datetime(2026, 5, 10, 12, tzinfo=UTC),
        "available_at": datetime(2026, 5, 10, 13, tzinfo=UTC),
        "payload": {"form_type": "8-K", "classification_reasons": ["sec_form_8k"]},
    }
    values.update(overrides)
    return CanonicalEvent(**values)  # type: ignore[arg-type]

from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import Engine, delete, insert, select

from catalyst_radar.core.immutability import thaw_json_value
from catalyst_radar.events.models import CanonicalEvent, EventType, SourceCategory
from catalyst_radar.storage.schema import events


class EventRepository:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def upsert_events(self, rows: Iterable[CanonicalEvent]) -> int:
        count = 0
        with self.engine.begin() as conn:
            for row in rows:
                conn.execute(delete(events).where(events.c.dedupe_key == row.dedupe_key))
                conn.execute(insert(events).values(**_event_row(row)))
                count += 1
        return count

    def list_events_for_ticker(
        self,
        ticker: str,
        *,
        as_of: datetime,
        available_at: datetime,
        min_materiality: float = 0.0,
        limit: int = 20,
    ) -> list[CanonicalEvent]:
        normalized_ticker = ticker.upper()
        stmt = (
            select(events)
            .where(
                events.c.ticker == normalized_ticker,
                events.c.source_ts <= _to_utc_datetime(as_of, "as_of"),
                events.c.available_at <= _to_utc_datetime(available_at, "available_at"),
                events.c.materiality >= min(1.0, max(0.0, float(min_materiality))),
            )
            .order_by(
                events.c.source_ts.desc(),
                events.c.available_at.desc(),
                events.c.materiality.desc(),
                events.c.id.desc(),
            )
            .limit(max(0, int(limit)))
        )
        with self.engine.connect() as conn:
            return [_event_from_row(row._mapping) for row in conn.execute(stmt)]

    def latest_material_events_by_ticker(
        self,
        tickers: Iterable[str],
        *,
        as_of: datetime,
        available_at: datetime,
        min_materiality: float,
        limit_per_ticker: int,
    ) -> dict[str, list[CanonicalEvent]]:
        normalized = sorted({ticker.upper() for ticker in tickers if ticker.strip()})
        return {
            ticker: self.list_events_for_ticker(
                ticker,
                as_of=as_of,
                available_at=available_at,
                min_materiality=min_materiality,
                limit=limit_per_ticker,
            )
            for ticker in normalized
        }


def _event_row(event: CanonicalEvent) -> dict[str, Any]:
    return {
        "id": event.id,
        "ticker": event.ticker,
        "event_type": event.event_type.value,
        "provider": event.provider,
        "source": event.source,
        "source_category": event.source_category.value,
        "source_url": event.source_url,
        "title": event.title,
        "body_hash": event.body_hash,
        "dedupe_key": event.dedupe_key,
        "source_quality": event.source_quality,
        "materiality": event.materiality,
        "source_ts": event.source_ts,
        "available_at": event.available_at,
        "payload": thaw_json_value(event.payload),
        "created_at": datetime.now(UTC),
    }


def _event_from_row(row: Any) -> CanonicalEvent:
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


__all__ = ["EventRepository"]

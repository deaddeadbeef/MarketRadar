from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import Engine, delete, insert, or_, select

from catalyst_radar.core.immutability import thaw_json_value
from catalyst_radar.storage.schema import text_features, text_snippets
from catalyst_radar.textint.models import TextFeature, TextSnippet


class TextRepository:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def upsert_snippets(self, rows: Iterable[TextSnippet]) -> int:
        count = 0
        with self.engine.begin() as conn:
            for row in rows:
                conn.execute(
                    delete(text_snippets).where(
                        text_snippets.c.event_id == row.event_id,
                        or_(
                            text_snippets.c.snippet_hash == row.snippet_hash,
                            text_snippets.c.section == row.section,
                        ),
                    )
                )
                conn.execute(insert(text_snippets).values(**_snippet_row(row)))
                count += 1
        return count

    def list_snippets_for_ticker(
        self,
        ticker: str,
        as_of: datetime,
        available_at: datetime,
        limit: int = 20,
    ) -> list[TextSnippet]:
        stmt = (
            select(text_snippets)
            .where(
                text_snippets.c.ticker == ticker.upper(),
                text_snippets.c.source_ts <= _to_utc_datetime(as_of, "as_of"),
                text_snippets.c.available_at <= _to_utc_datetime(
                    available_at,
                    "available_at",
                ),
            )
            .order_by(
                text_snippets.c.source_ts.desc(),
                text_snippets.c.available_at.desc(),
                text_snippets.c.materiality.desc(),
                text_snippets.c.source_quality.desc(),
                text_snippets.c.id.desc(),
            )
            .limit(max(0, int(limit)))
        )
        with self.engine.connect() as conn:
            return [_snippet_from_row(row._mapping) for row in conn.execute(stmt)]

    def upsert_text_features(self, rows: Iterable[TextFeature]) -> int:
        count = 0
        with self.engine.begin() as conn:
            for row in rows:
                conn.execute(
                    delete(text_features).where(
                        text_features.c.ticker == row.ticker,
                        text_features.c.as_of == row.as_of,
                        text_features.c.feature_version == row.feature_version,
                    )
                )
                conn.execute(insert(text_features).values(**_feature_row(row)))
                count += 1
        return count

    def latest_text_features_by_ticker(
        self,
        tickers: Iterable[str],
        as_of: datetime,
        available_at: datetime,
    ) -> dict[str, TextFeature]:
        normalized = sorted({ticker.upper() for ticker in tickers if ticker.strip()})
        if not normalized:
            return {}
        stmt = (
            select(text_features)
            .where(
                text_features.c.ticker.in_(normalized),
                text_features.c.as_of <= _to_utc_datetime(as_of, "as_of"),
                text_features.c.available_at <= _to_utc_datetime(
                    available_at,
                    "available_at",
                ),
            )
            .order_by(
                text_features.c.ticker,
                text_features.c.as_of.desc(),
                text_features.c.available_at.desc(),
                text_features.c.source_ts.desc(),
                text_features.c.id.desc(),
            )
        )
        result: dict[str, TextFeature] = {}
        with self.engine.connect() as conn:
            for row in conn.execute(stmt):
                feature = _feature_from_row(row._mapping)
                result.setdefault(feature.ticker, feature)
        return result


def _snippet_row(snippet: TextSnippet) -> dict[str, Any]:
    return {
        "id": snippet.id,
        "ticker": snippet.ticker,
        "event_id": snippet.event_id,
        "snippet_hash": snippet.snippet_hash,
        "section": snippet.section,
        "text": snippet.text,
        "source": snippet.source,
        "source_url": snippet.source_url,
        "source_quality": snippet.source_quality,
        "event_type": snippet.event_type,
        "materiality": snippet.materiality,
        "ontology_hits": thaw_json_value(snippet.ontology_hits),
        "sentiment": snippet.sentiment,
        "embedding": thaw_json_value(snippet.embedding),
        "source_ts": snippet.source_ts,
        "available_at": snippet.available_at,
        "payload": thaw_json_value(snippet.payload),
        "created_at": datetime.now(UTC),
    }


def _feature_row(feature: TextFeature) -> dict[str, Any]:
    return {
        "id": feature.id,
        "ticker": feature.ticker,
        "as_of": feature.as_of,
        "feature_version": feature.feature_version,
        "local_narrative_score": feature.local_narrative_score,
        "novelty_score": feature.novelty_score,
        "sentiment_score": feature.sentiment_score,
        "source_quality_score": feature.source_quality_score,
        "theme_match_score": feature.theme_match_score,
        "conflict_penalty": feature.conflict_penalty,
        "selected_snippet_ids": thaw_json_value(feature.selected_snippet_ids),
        "theme_hits": thaw_json_value(feature.theme_hits),
        "source_ts": feature.source_ts,
        "available_at": feature.available_at,
        "payload": thaw_json_value(feature.payload),
        "created_at": datetime.now(UTC),
    }


def _snippet_from_row(row: Any) -> TextSnippet:
    return TextSnippet(
        id=row["id"],
        ticker=row["ticker"],
        event_id=row["event_id"],
        snippet_hash=row["snippet_hash"],
        section=row["section"],
        text=row["text"],
        source=row["source"],
        source_url=row["source_url"],
        source_quality=row["source_quality"],
        event_type=row["event_type"],
        materiality=row["materiality"],
        ontology_hits=row["ontology_hits"],
        sentiment=row["sentiment"],
        embedding=row["embedding"],
        source_ts=_as_datetime(row["source_ts"]),
        available_at=_as_datetime(row["available_at"]),
        payload=row["payload"],
    )


def _feature_from_row(row: Any) -> TextFeature:
    return TextFeature(
        id=row["id"],
        ticker=row["ticker"],
        as_of=_as_datetime(row["as_of"]),
        feature_version=row["feature_version"],
        local_narrative_score=row["local_narrative_score"],
        novelty_score=row["novelty_score"],
        sentiment_score=row["sentiment_score"],
        source_quality_score=row["source_quality_score"],
        theme_match_score=row["theme_match_score"],
        conflict_penalty=row["conflict_penalty"],
        selected_snippet_ids=row["selected_snippet_ids"],
        theme_hits=row["theme_hits"],
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


__all__ = ["TextRepository"]

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import create_engine, func, select

from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.schema import text_features, text_snippets
from catalyst_radar.storage.text_repositories import TextRepository
from catalyst_radar.textint.models import TextFeature, TextSnippet


def test_upsert_snippets_dedupes_by_snippet_hash_and_event() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    create_schema(engine)
    repo = TextRepository(engine)

    assert repo.upsert_snippets([snippet()]) == 1
    assert repo.upsert_snippets([snippet()]) == 1

    with engine.connect() as conn:
        assert conn.scalar(select(func.count()).select_from(text_snippets)) == 1


def test_latest_text_feature_respects_available_at() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    create_schema(engine)
    repo = TextRepository(engine)
    repo.upsert_text_features(
        [
            feature(id="past", available_at=datetime(2026, 5, 10, 13, tzinfo=UTC)),
            feature(
                id="future",
                feature_version="textint-v2",
                available_at=datetime(2026, 5, 10, 15, tzinfo=UTC),
            ),
        ]
    )

    result = repo.latest_text_features_by_ticker(
        ["MSFT"],
        as_of=datetime(2026, 5, 10, 21, tzinfo=UTC),
        available_at=datetime(2026, 5, 10, 14, tzinfo=UTC),
    )

    assert result["MSFT"].id == "past"
    with engine.connect() as conn:
        assert conn.scalar(select(func.count()).select_from(text_features)) == 2


def snippet(**overrides: object) -> TextSnippet:
    values = {
        "id": "snippet-1",
        "ticker": "MSFT",
        "event_id": "event-1",
        "snippet_hash": "snippet-hash-1",
        "section": "body",
        "text": "NAND demand and datacenter SSD storage bottlenecks are improving.",
        "source": "SEC EDGAR",
        "source_url": "https://www.sec.gov/Archives/example",
        "source_quality": 1.0,
        "event_type": "sec_filing",
        "materiality": 0.85,
        "ontology_hits": [{"theme_id": "ai_infrastructure_storage", "terms": ["NAND"]}],
        "sentiment": 0.25,
        "embedding": [0.1, 0.2, 0.3],
        "source_ts": datetime(2026, 5, 10, 12, tzinfo=UTC),
        "available_at": datetime(2026, 5, 10, 13, tzinfo=UTC),
        "payload": {"rank": 1},
    }
    values.update(overrides)
    return TextSnippet(**values)  # type: ignore[arg-type]


def feature(**overrides: object) -> TextFeature:
    values = {
        "id": "feature-1",
        "ticker": "MSFT",
        "as_of": datetime(2026, 5, 10, 21, tzinfo=UTC),
        "feature_version": "textint-v1",
        "local_narrative_score": 70.0,
        "novelty_score": 100.0,
        "sentiment_score": 25.0,
        "source_quality_score": 100.0,
        "theme_match_score": 80.0,
        "conflict_penalty": 0.0,
        "selected_snippet_ids": ["snippet-1"],
        "theme_hits": [{"theme_id": "ai_infrastructure_storage", "count": 1}],
        "source_ts": datetime(2026, 5, 10, 12, tzinfo=UTC),
        "available_at": datetime(2026, 5, 10, 13, tzinfo=UTC),
        "payload": {"snippet_count": 1},
    }
    values.update(overrides)
    return TextFeature(**values)  # type: ignore[arg-type]

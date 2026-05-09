from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy import create_engine, func, select

from catalyst_radar.cli import main
from catalyst_radar.events.models import CanonicalEvent, EventType, SourceCategory
from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.event_repositories import EventRepository
from catalyst_radar.storage.schema import text_features, text_snippets
from catalyst_radar.storage.text_repositories import TextRepository
from catalyst_radar.textint.models import TextFeature, TextSnippet
from catalyst_radar.textint.pipeline import run_text_pipeline


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


def test_run_text_pipeline_persists_snippets_and_feature() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    create_schema(engine)
    event_repo = EventRepository(engine)
    text_repo = TextRepository(engine)
    event_repo.upsert_events(
        [
            canonical_event(
                event_id="visible",
                title="MSFT raises guidance",
                body="NAND and SSD demand are creating an inference storage bottleneck.",
            ),
            canonical_event(
                event_id="future",
                source_ts=datetime(2026, 5, 10, 15, tzinfo=UTC),
                available_at=datetime(2026, 5, 10, 15, tzinfo=UTC),
            ),
        ]
    )

    result = run_text_pipeline(
        event_repo,
        text_repo,
        as_of=datetime(2026, 5, 10, 21, tzinfo=UTC),
        available_at=datetime(2026, 5, 10, 14, tzinfo=UTC),
        ontology_path=Path("config/themes.yaml"),
    )

    assert result.feature_count == 1
    assert result.snippet_count == 1
    feature_row = text_repo.latest_text_features_by_ticker(
        ["MSFT"],
        as_of=datetime(2026, 5, 10, 21, tzinfo=UTC),
        available_at=datetime(2026, 5, 10, 14, tzinfo=UTC),
    )["MSFT"]
    assert feature_row.local_narrative_score > 0
    assert feature_row.novelty_score == 100.0
    assert feature_row.source_quality_score > 0
    assert feature_row.theme_match_score > 0
    assert feature_row.selected_snippet_ids
    snippets = text_repo.list_snippets_for_ticker(
        "MSFT",
        as_of=datetime(2026, 5, 10, 21, tzinfo=UTC),
        available_at=datetime(2026, 5, 10, 14, tzinfo=UTC),
    )
    assert [row.event_id for row in snippets] == ["visible"]


def test_run_text_pipeline_dedupes_duplicate_snippet_hashes() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    create_schema(engine)
    event_repo = EventRepository(engine)
    text_repo = TextRepository(engine)
    duplicate_body = "NAND and SSD demand are creating an inference storage bottleneck."
    event_repo.upsert_events(
        [
            canonical_event(event_id="first", body=duplicate_body),
            canonical_event(event_id="second", body=duplicate_body),
        ]
    )

    result = run_text_pipeline(
        event_repo,
        text_repo,
        as_of=datetime(2026, 5, 10, 21, tzinfo=UTC),
        available_at=datetime(2026, 5, 10, 14, tzinfo=UTC),
        ontology_path=Path("config/themes.yaml"),
    )

    assert result.snippet_count == 1
    assert len(result.features[0].selected_snippet_ids) == 1


def test_textint_cli_processes_events_and_prints_features(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'textint.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    assert main(["init-db"]) == 0
    capsys.readouterr()
    engine = create_engine(database_url, future=True)
    EventRepository(engine).upsert_events(
        [
            canonical_event(
                event_id="cli-event",
                title="MSFT raises guidance",
                body="NAND and SSD demand are creating an inference storage bottleneck.",
            )
        ]
    )

    assert main(
        [
            "run-textint",
            "--as-of",
            "2026-05-10",
            "--available-at",
            "2026-05-10T14:00:00Z",
            "--ontology",
            "config/themes.yaml",
        ]
    ) == 0
    assert capsys.readouterr().out == "processed text_features=1 snippets=1\n"

    assert main(
        [
            "text-features",
            "--ticker",
            "MSFT",
            "--as-of",
            "2026-05-10",
            "--available-at",
            "2026-05-10T14:00:00Z",
        ]
    ) == 0
    output = capsys.readouterr().out
    assert output.startswith("MSFT local_narrative=")
    assert " novelty=100.00 snippets=1\n" in output


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


def canonical_event(**overrides: object) -> CanonicalEvent:
    event_id = str(overrides.pop("event_id", "event-1"))
    body = str(
        overrides.pop(
            "body",
            "NAND and SSD demand are creating an inference storage bottleneck.",
        )
    )
    values = {
        "id": event_id,
        "ticker": "MSFT",
        "event_type": EventType.GUIDANCE,
        "provider": "news_fixture",
        "source": "Reuters",
        "source_category": SourceCategory.REPUTABLE_NEWS,
        "source_url": f"https://reuters.example.com/{event_id}",
        "title": "MSFT raises guidance",
        "body_hash": event_id,
        "dedupe_key": f"MSFT:{event_id}",
        "source_quality": 0.85,
        "materiality": 0.9,
        "source_ts": datetime(2026, 5, 10, 12, tzinfo=UTC),
        "available_at": datetime(2026, 5, 10, 13, tzinfo=UTC),
        "payload": {"body": body},
    }
    values.update(overrides)
    return CanonicalEvent(**values)  # type: ignore[arg-type]

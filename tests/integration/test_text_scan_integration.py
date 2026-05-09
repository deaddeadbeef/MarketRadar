from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

from sqlalchemy import create_engine

from catalyst_radar.connectors.csv_market import load_daily_bars_csv, load_securities_csv
from catalyst_radar.core.config import AppConfig
from catalyst_radar.dashboard.data import load_candidate_rows
from catalyst_radar.pipeline.scan import run_scan
from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.repositories import MarketRepository
from catalyst_radar.storage.text_repositories import TextRepository
from catalyst_radar.textint.models import TextFeature


def test_scan_attaches_point_in_time_text_feature_metadata() -> None:
    engine = _engine()
    market_repo = MarketRepository(engine)
    text_repo = TextRepository(engine)
    _load_market_fixtures(market_repo)
    text_repo.upsert_text_features(
        [
            text_feature(
                id="visible",
                available_at=datetime(2026, 5, 8, 20, 30, tzinfo=UTC),
                local_narrative_score=80.0,
                novelty_score=90.0,
            ),
            text_feature(
                id="future",
                feature_version="textint-v2",
                available_at=datetime(2026, 5, 8, 22, tzinfo=UTC),
                local_narrative_score=100.0,
                novelty_score=100.0,
            ),
        ]
    )

    result = _scan_result(market_repo, text_repo)

    assert result.candidate.metadata["local_narrative_score"] == 80.0
    assert result.candidate.metadata["local_narrative_bonus"] == 4.8
    assert result.candidate.metadata["novelty_score"] == 90.0
    assert result.candidate.metadata["text_feature_version"] == "textint-v1"
    assert result.candidate.metadata["selected_snippet_count"] == 2


def test_text_scan_fields_are_persisted_for_dashboard_rows() -> None:
    engine = _engine()
    market_repo = MarketRepository(engine)
    text_repo = TextRepository(engine)
    _load_market_fixtures(market_repo)
    text_repo.upsert_text_features([text_feature()])

    result = _scan_result(market_repo, text_repo)
    market_repo.save_scan_result(result.candidate, result.policy)

    row = next(row for row in load_candidate_rows(engine) if row["ticker"] == "AAA")

    assert row["local_narrative_score"] == 70.0
    assert row["novelty_score"] == 100.0
    assert row["theme_hits"] == [
        {"theme_id": "ai_infrastructure_storage", "count": 1, "terms": ["NAND"]}
    ]
    assert row["sentiment_score"] == 25.0
    assert row["selected_snippet_count"] == 2
    assert row["text_feature_version"] == "textint-v1"


def _engine():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    create_schema(engine)
    return engine


def _load_market_fixtures(market_repo: MarketRepository) -> None:
    fixture_dir = Path("tests/fixtures")
    market_repo.upsert_securities(load_securities_csv(fixture_dir / "securities.csv"))
    market_repo.upsert_daily_bars(load_daily_bars_csv(fixture_dir / "daily_bars.csv"))


def _scan_result(market_repo: MarketRepository, text_repo: TextRepository):
    return next(
        row
        for row in run_scan(
            market_repo,
            as_of=date(2026, 5, 8),
            available_at=datetime(2026, 5, 8, 21, tzinfo=UTC),
            text_repo=text_repo,
            config=AppConfig(portfolio_value=100_000, portfolio_cash=25_000),
        )
        if row.ticker == "AAA"
    )


def text_feature(**overrides: object) -> TextFeature:
    values = {
        "id": "feature-1",
        "ticker": "AAA",
        "as_of": datetime(2026, 5, 8, 21, tzinfo=UTC),
        "feature_version": "textint-v1",
        "local_narrative_score": 70.0,
        "novelty_score": 100.0,
        "sentiment_score": 25.0,
        "source_quality_score": 85.0,
        "theme_match_score": 75.0,
        "conflict_penalty": 0.0,
        "selected_snippet_ids": ["snippet-1", "snippet-2"],
        "theme_hits": [
            {"theme_id": "ai_infrastructure_storage", "count": 1, "terms": ["NAND"]}
        ],
        "source_ts": datetime(2026, 5, 8, 20, tzinfo=UTC),
        "available_at": datetime(2026, 5, 8, 20, 30, tzinfo=UTC),
        "payload": {"snippet_count": 2},
    }
    values.update(overrides)
    return TextFeature(**values)  # type: ignore[arg-type]

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from sqlalchemy import create_engine

from catalyst_radar.connectors.csv_market import load_daily_bars_csv, load_securities_csv
from catalyst_radar.core.config import AppConfig
from catalyst_radar.dashboard.data import load_candidate_rows
from catalyst_radar.features.options import OptionFeatureInput
from catalyst_radar.pipeline.scan import run_scan
from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.feature_repositories import FeatureRepository
from catalyst_radar.storage.repositories import MarketRepository
from catalyst_radar.storage.text_repositories import TextRepository
from catalyst_radar.textint.models import TextFeature


def test_scan_attaches_point_in_time_options_theme_sector_and_peer_metadata() -> None:
    engine = _engine()
    market_repo = MarketRepository(engine)
    feature_repo = FeatureRepository(engine)
    text_repo = TextRepository(engine)
    _load_market_fixtures(market_repo)
    feature_repo.upsert_option_features(
        [
            option_feature(call_volume=12_000.0),
            option_feature(
                as_of=datetime(2026, 5, 8, 22, tzinfo=UTC),
                available_at=datetime(2026, 5, 8, 22, tzinfo=UTC),
                call_volume=100_000.0,
            ),
        ]
    )
    text_repo.upsert_text_features([text_feature()])

    result = _scan_result(
        market_repo=market_repo,
        feature_repo=feature_repo,
        text_repo=text_repo,
    )

    metadata = result.candidate.metadata
    assert metadata["options_flow_score"] > 50.0
    assert metadata["options_risk_score"] >= 0.0
    assert metadata["call_put_ratio"] == 3.0
    assert metadata["iv_percentile"] == 0.72
    assert metadata["candidate_theme"] == "ai_infrastructure_storage"
    assert metadata["sector_rotation_score"] > 50.0
    assert metadata["theme_velocity_score"] > 60.0
    assert metadata["peer_readthrough_score"] > 0.0
    assert metadata["options_feature_version"] == "options-v1"
    assert metadata["theme_feature_version"] == "theme-v1"


def test_future_available_option_feature_is_ignored_by_scan() -> None:
    engine = _engine()
    market_repo = MarketRepository(engine)
    feature_repo = FeatureRepository(engine)
    _load_market_fixtures(market_repo)
    feature_repo.upsert_option_features(
        [
            option_feature(
                source_ts=datetime(2026, 5, 8, 20, 45, tzinfo=UTC),
                available_at=datetime(2026, 5, 9, 21, tzinfo=UTC),
            )
        ]
    )

    result = _scan_result(market_repo=market_repo, feature_repo=feature_repo)

    assert result.candidate.metadata["options_flow_score"] == 0.0
    assert result.candidate.metadata["options_feature_version"] is None


def test_options_theme_fields_are_persisted_for_dashboard_rows() -> None:
    engine = _engine()
    market_repo = MarketRepository(engine)
    feature_repo = FeatureRepository(engine)
    text_repo = TextRepository(engine)
    _load_market_fixtures(market_repo)
    feature_repo.upsert_option_features([option_feature()])
    text_repo.upsert_text_features([text_feature()])

    result = _scan_result(
        market_repo=market_repo,
        feature_repo=feature_repo,
        text_repo=text_repo,
    )
    market_repo.save_scan_result(result.candidate, result.policy)

    row = next(row for row in load_candidate_rows(engine) if row["ticker"] == "AAA")

    assert row["options_flow_score"] > 50.0
    assert row["options_risk_score"] >= 0.0
    assert row["sector_rotation_score"] > 50.0
    assert row["theme_velocity_score"] > 60.0
    assert row["peer_readthrough_score"] > 0.0
    assert row["candidate_theme"] == "ai_infrastructure_storage"
    assert row["options_feature_version"] == "options-v1"


def test_max_optional_support_cannot_override_stale_data_policy() -> None:
    engine = _engine()
    market_repo = MarketRepository(engine)
    feature_repo = FeatureRepository(engine)
    text_repo = TextRepository(engine)
    _load_market_fixtures(market_repo)
    stale_as_of = datetime(2026, 5, 9, 21, tzinfo=UTC)
    feature_repo.upsert_option_features(
        [
            option_feature(
                as_of=stale_as_of,
                source_ts=stale_as_of - timedelta(minutes=15),
                available_at=stale_as_of,
                call_volume=100_000.0,
                put_volume=1.0,
                call_open_interest=100_000.0,
                put_open_interest=1.0,
                iv_percentile=1.0,
            )
        ]
    )
    text_repo.upsert_text_features(
        [
            text_feature(
                as_of=stale_as_of,
                source_ts=stale_as_of - timedelta(hours=1),
                available_at=stale_as_of,
                local_narrative_score=100.0,
                novelty_score=100.0,
                theme_match_score=100.0,
            )
        ]
    )

    result = next(
        row
        for row in run_scan(
            market_repo,
            as_of=date(2026, 5, 9),
            available_at=stale_as_of,
            feature_repo=feature_repo,
            text_repo=text_repo,
            config=AppConfig(portfolio_value=100_000, portfolio_cash=25_000),
        )
        if row.ticker == "AAA"
    )

    assert result.candidate.metadata["options_bonus"] == 4.0
    assert result.candidate.metadata["sector_theme_bonus"] >= 4.0
    assert result.candidate.data_stale is True
    assert "data_stale" in result.policy.hard_blocks


def _engine():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    create_schema(engine)
    return engine


def _load_market_fixtures(market_repo: MarketRepository) -> None:
    fixture_dir = Path("tests/fixtures")
    market_repo.upsert_securities(load_securities_csv(fixture_dir / "securities.csv"))
    market_repo.upsert_daily_bars(load_daily_bars_csv(fixture_dir / "daily_bars.csv"))


def _scan_result(
    *,
    market_repo: MarketRepository,
    feature_repo: FeatureRepository | None = None,
    text_repo: TextRepository | None = None,
):
    return next(
        row
        for row in run_scan(
            market_repo,
            as_of=date(2026, 5, 8),
            available_at=datetime(2026, 5, 8, 21, tzinfo=UTC),
            feature_repo=feature_repo,
            text_repo=text_repo,
            config=AppConfig(portfolio_value=100_000, portfolio_cash=25_000),
        )
        if row.ticker == "AAA"
    )


def option_feature(**overrides: object) -> OptionFeatureInput:
    values = {
        "ticker": "AAA",
        "as_of": datetime(2026, 5, 8, 21, tzinfo=UTC),
        "provider": "options_fixture",
        "call_volume": 12_000.0,
        "put_volume": 4_000.0,
        "call_open_interest": 50_000.0,
        "put_open_interest": 30_000.0,
        "iv_percentile": 0.72,
        "skew": 0.18,
        "source_ts": datetime(2026, 5, 8, 20, 45, tzinfo=UTC),
        "available_at": datetime(2026, 5, 8, 21, tzinfo=UTC),
        "payload": {"fixture": True},
    }
    values.update(overrides)
    return OptionFeatureInput(**values)  # type: ignore[arg-type]


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
            {"theme_id": "ai_infrastructure_storage", "count": 2, "terms": ["NAND", "SSD"]}
        ],
        "source_ts": datetime(2026, 5, 8, 20, tzinfo=UTC),
        "available_at": datetime(2026, 5, 8, 20, 30, tzinfo=UTC),
        "payload": {"snippet_count": 2},
    }
    values.update(overrides)
    return TextFeature(**values)  # type: ignore[arg-type]

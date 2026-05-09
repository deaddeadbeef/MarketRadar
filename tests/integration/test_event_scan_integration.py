from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

from sqlalchemy import create_engine

from catalyst_radar.connectors.csv_market import load_daily_bars_csv, load_securities_csv
from catalyst_radar.core.config import AppConfig
from catalyst_radar.dashboard.data import load_candidate_rows
from catalyst_radar.events.models import CanonicalEvent, EventType, SourceCategory
from catalyst_radar.pipeline.scan import run_scan
from catalyst_radar.scoring.setups import SetupType
from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.event_repositories import EventRepository
from catalyst_radar.storage.repositories import MarketRepository


def test_scan_attaches_point_in_time_material_events() -> None:
    engine = _engine()
    market_repo = MarketRepository(engine)
    event_repo = EventRepository(engine)
    _load_market_fixtures(market_repo)
    event_repo.upsert_events(
        [
            canonical_event(
                id="visible",
                dedupe_key="AAA:visible",
                ticker="AAA",
                available_at=datetime(2026, 5, 8, 20, 30, tzinfo=UTC),
            ),
            canonical_event(
                id="future",
                dedupe_key="AAA:future",
                ticker="AAA",
                available_at=datetime(2026, 5, 8, 22, tzinfo=UTC),
            ),
        ]
    )

    result = _scan_result(market_repo, event_repo)

    assert result.candidate.metadata["material_event_count"] == 1
    assert result.candidate.metadata["events"][0]["id"] == "visible"
    assert result.candidate.metadata["events"][0]["source_id"] == "visible"
    assert result.candidate.metadata["event_conflicts"] == ()
    assert result.candidate.metadata["has_event_conflict"] is False


def test_guidance_event_selects_filings_catalyst_setup() -> None:
    engine = _engine()
    market_repo = MarketRepository(engine)
    event_repo = EventRepository(engine)
    _load_market_fixtures(market_repo)
    event_repo.upsert_events([canonical_event(ticker="AAA")])

    result = _scan_result(market_repo, event_repo)

    assert result.candidate.metadata["setup_type"] == SetupType.FILINGS_CATALYST.value
    assert result.candidate.metadata["setup_metadata"]["event_confirmed"] is True
    assert result.candidate.metadata["setup_metadata"]["source_event_id"] == "event-1"


def test_event_scan_fields_are_persisted_for_dashboard_rows() -> None:
    engine = _engine()
    market_repo = MarketRepository(engine)
    event_repo = EventRepository(engine)
    _load_market_fixtures(market_repo)
    event_repo.upsert_events([canonical_event(ticker="AAA")])

    result = _scan_result(market_repo, event_repo)
    market_repo.save_scan_result(result.candidate, result.policy)

    row = next(row for row in load_candidate_rows(engine) if row["ticker"] == "AAA")

    assert row["material_event_count"] == 1
    assert row["top_event_type"] == EventType.GUIDANCE.value
    assert row["top_event_source"] == "Reuters"
    assert row["top_event_source_url"] == "https://reuters.example.com/aaa"
    assert row["top_event_source_quality"] == 0.85
    assert row["top_event_materiality"] == 0.9
    assert row["has_event_conflict"] is False


def _engine():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    create_schema(engine)
    return engine


def _load_market_fixtures(market_repo: MarketRepository) -> None:
    fixture_dir = Path("tests/fixtures")
    market_repo.upsert_securities(load_securities_csv(fixture_dir / "securities.csv"))
    market_repo.upsert_daily_bars(load_daily_bars_csv(fixture_dir / "daily_bars.csv"))


def _scan_result(market_repo: MarketRepository, event_repo: EventRepository):
    return next(
        row
        for row in run_scan(
            market_repo,
            as_of=date(2026, 5, 8),
            available_at=datetime(2026, 5, 8, 21, tzinfo=UTC),
            event_repo=event_repo,
            config=AppConfig(portfolio_value=100_000, portfolio_cash=25_000),
        )
        if row.ticker == "AAA"
    )


def canonical_event(**overrides: object) -> CanonicalEvent:
    values = {
        "id": "event-1",
        "ticker": "AAA",
        "event_type": EventType.GUIDANCE,
        "provider": "news_fixture",
        "source": "Reuters",
        "source_category": SourceCategory.REPUTABLE_NEWS,
        "source_url": "https://reuters.example.com/aaa",
        "title": "AAA raises guidance",
        "body_hash": "hash",
        "dedupe_key": "AAA:event-1",
        "source_quality": 0.85,
        "materiality": 0.9,
        "source_ts": datetime(2026, 5, 8, 20, tzinfo=UTC),
        "available_at": datetime(2026, 5, 8, 20, 30, tzinfo=UTC),
        "payload": {"body": "AAA raises guidance.", "classification_reasons": ["guidance"]},
    }
    values.update(overrides)
    return CanonicalEvent(**values)  # type: ignore[arg-type]

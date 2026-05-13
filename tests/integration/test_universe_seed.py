from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import create_engine, func, select

from catalyst_radar.connectors.http import FakeHttpTransport, HttpResponse
from catalyst_radar.connectors.provider_ingest import ProviderIngestError
from catalyst_radar.core.config import AppConfig
from catalyst_radar.core.models import JobStatus
from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.provider_repositories import ProviderRepository
from catalyst_radar.storage.repositories import MarketRepository
from catalyst_radar.storage.schema import data_quality_incidents, job_runs
from catalyst_radar.universe.seed import seed_polygon_tickers


def test_seed_polygon_tickers_ingests_reference_securities(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    first_url = (
        "https://api.polygon.io/v3/reference/tickers?"
        "market=stocks&active=true&limit=1000&apiKey=fixture-key"
    )
    transport = FakeHttpTransport(
        {
            first_url: HttpResponse(
                status_code=200,
                url=first_url,
                headers={"content-type": "application/json"},
                body=_fixture("tickers_page_1.json").read_bytes(),
            )
        }
    )

    result = seed_polygon_tickers(
        engine,
        config=AppConfig(
            polygon_api_key="fixture-key",
            polygon_tickers_max_pages=1,
        ),
        max_pages=1,
        transport=transport,
    )

    assert transport.requests == [first_url]
    assert result.provider == "polygon"
    assert result.max_pages == 1
    assert result.security_count == 2
    assert result.rejected_count == 0
    assert {row.ticker for row in MarketRepository(engine).list_active_securities()} == {
        "AAPL",
        "SPY",
    }


def test_seed_polygon_tickers_missing_api_key_fails_closed_without_http(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path)
    transport = FakeHttpTransport({})

    with pytest.raises(ProviderIngestError, match="missing CATALYST_POLYGON_API_KEY"):
        seed_polygon_tickers(
            engine,
            config=AppConfig(polygon_api_key=None),
            transport=transport,
        )

    assert transport.requests == []
    assert ProviderRepository(engine).latest_health("polygon").reason == (
        "missing CATALYST_POLYGON_API_KEY"
    )
    with engine.connect() as conn:
        job = conn.execute(
            select(job_runs).where(job_runs.c.job_type == "polygon_tickers")
        ).one()
        incident_count = conn.execute(
            select(func.count()).select_from(data_quality_incidents)
        ).scalar_one()
    assert job.status == JobStatus.FAILED.value
    assert incident_count == 1
    assert MarketRepository(engine).list_active_securities() == []


def test_seed_polygon_tickers_rejects_page_cap_above_configured_limit(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path)

    with pytest.raises(ValueError, match="max_pages exceeds configured cap"):
        seed_polygon_tickers(
            engine,
            config=AppConfig(
                polygon_api_key="fixture-key",
                polygon_tickers_max_pages=1,
            ),
            max_pages=2,
            transport=FakeHttpTransport({}),
        )


def _engine(tmp_path: Path):
    engine = create_engine(
        f"sqlite:///{(tmp_path / 'universe-seed.db').as_posix()}",
        future=True,
    )
    create_schema(engine)
    return engine


def _fixture(name: str) -> Path:
    return Path("tests/fixtures/polygon") / name

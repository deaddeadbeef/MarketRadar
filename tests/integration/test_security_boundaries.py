from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy import create_engine, select

from catalyst_radar.connectors.base import (
    ConnectorHealth,
    ConnectorHealthStatus,
    ConnectorRequest,
    NormalizedRecord,
    ProviderCostEstimate,
    RawRecord,
)
from catalyst_radar.connectors.provider_ingest import (
    ProviderIngestError,
    ingest_provider_records,
)
from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.provider_repositories import ProviderRepository
from catalyst_radar.storage.repositories import MarketRepository
from catalyst_radar.storage.schema import data_quality_incidents, job_runs


def test_provider_ingest_redacts_secret_from_health_job_and_incident(
    tmp_path: Path,
) -> None:
    engine = create_engine(f"sqlite:///{(tmp_path / 'security.db').as_posix()}", future=True)
    create_schema(engine)
    provider_repo = ProviderRepository(engine)

    with pytest.raises(ProviderIngestError) as excinfo:
        ingest_provider_records(
            connector=_LeakyConnector(),
            request=ConnectorRequest(
                provider="leaky",
                endpoint="test",
                params={},
                requested_at=datetime(2026, 5, 10, tzinfo=UTC),
            ),
            market_repo=MarketRepository(engine),
            provider_repo=provider_repo,
            job_type="leaky_ingest",
            metadata={"api_key": "metadata-secret"},
        )

    with engine.connect() as conn:
        job = conn.execute(select(job_runs)).one()
        incident = conn.execute(select(data_quality_incidents)).one()

    health = provider_repo.latest_health("leaky")
    assert health is not None
    persisted = " ".join(
        [
            str(excinfo.value),
            str(health.reason),
            str(job.error_summary),
            str(job.metadata),
            str(incident.reason),
            str(incident.payload),
        ]
    )
    assert "secret-token" not in persisted
    assert "metadata-secret" not in persisted
    assert "<redacted>" in persisted


class _LeakyConnector:
    def healthcheck(self) -> ConnectorHealth:
        return ConnectorHealth(
            provider="leaky",
            status=ConnectorHealthStatus.HEALTHY,
            checked_at=datetime(2026, 5, 10, tzinfo=UTC),
            reason="ok",
        )

    def fetch(self, request: ConnectorRequest) -> list[RawRecord]:
        del request
        raise RuntimeError("provider failed with apikey=secret-token")

    def normalize(self, records: list[RawRecord]) -> list[NormalizedRecord]:
        del records
        return []

    def estimate_cost(self, request: ConnectorRequest) -> ProviderCostEstimate:
        del request
        return ProviderCostEstimate(
            provider="leaky",
            request_count=1,
            estimated_cost_usd=0.0,
        )

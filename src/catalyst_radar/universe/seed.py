from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime

from sqlalchemy import Engine

from catalyst_radar.connectors.base import ConnectorRequest
from catalyst_radar.connectors.http import (
    HttpTransport,
    JsonHttpClient,
    UrlLibHttpTransport,
)
from catalyst_radar.connectors.polygon import PolygonEndpoint, PolygonMarketDataConnector
from catalyst_radar.connectors.provider_ingest import ingest_provider_records
from catalyst_radar.core.config import AppConfig
from catalyst_radar.storage.provider_repositories import ProviderRepository
from catalyst_radar.storage.repositories import MarketRepository


@dataclass(frozen=True)
class UniverseSeedResult:
    provider: str
    job_id: str
    max_pages: int
    date: date | None
    requested_count: int
    raw_count: int
    normalized_count: int
    security_count: int
    daily_bar_count: int
    holding_count: int
    rejected_count: int

    def as_payload(self) -> dict[str, object]:
        return {
            "status": "success",
            "provider": self.provider,
            "job_id": self.job_id,
            "max_pages": self.max_pages,
            "date": self.date.isoformat() if self.date is not None else None,
            "requested_count": self.requested_count,
            "raw_count": self.raw_count,
            "normalized_count": self.normalized_count,
            "security_count": self.security_count,
            "daily_bar_count": self.daily_bar_count,
            "holding_count": self.holding_count,
            "rejected_count": self.rejected_count,
        }


def seed_polygon_tickers(
    engine: Engine,
    *,
    config: AppConfig,
    max_pages: int | None = None,
    date_value: date | None = None,
    requested_at: datetime | None = None,
    transport: HttpTransport | None = None,
) -> UniverseSeedResult:
    page_cap = _bounded_page_cap(
        requested=max_pages,
        configured=config.polygon_tickers_max_pages,
    )
    endpoint = PolygonEndpoint.TICKERS
    params: dict[str, object] = {
        "market": "stocks",
        "active": True,
        "limit": 1000,
        "max_pages": page_cap,
    }
    if date_value is not None:
        params["date"] = date_value.isoformat()
    connector = PolygonMarketDataConnector(
        api_key=config.polygon_api_key if config.polygon_api_key_configured else None,
        client=JsonHttpClient(
            transport=transport or UrlLibHttpTransport(),
            timeout_seconds=config.http_timeout_seconds,
        ),
        base_url=config.polygon_base_url,
        availability_policy=config.provider_availability_policy,
    )
    request = ConnectorRequest(
        provider="polygon",
        endpoint=endpoint.value,
        params=params,
        requested_at=requested_at or datetime.now(UTC),
        idempotency_key=f"polygon-ticker-seed:{date_value or 'latest'}:{page_cap}",
    )
    metadata = {
        "provider": "polygon",
        "endpoint": endpoint.value,
        "date": date_value.isoformat() if date_value is not None else None,
        "max_pages": page_cap,
        "availability_policy": config.provider_availability_policy,
        "source": "manual_universe_seed",
    }
    result = ingest_provider_records(
        connector=connector,
        request=request,
        market_repo=MarketRepository(engine),
        provider_repo=ProviderRepository(engine),
        job_type=endpoint.value,
        metadata=metadata,
    )
    return UniverseSeedResult(
        provider=result.provider,
        job_id=result.job_id,
        max_pages=page_cap,
        date=date_value,
        requested_count=result.requested_count,
        raw_count=result.raw_count,
        normalized_count=result.normalized_count,
        security_count=result.security_count,
        daily_bar_count=result.daily_bar_count,
        holding_count=result.holding_count,
        rejected_count=result.rejected_count,
    )


def _bounded_page_cap(*, requested: int | None, configured: int) -> int:
    if configured <= 0:
        msg = "configured Polygon ticker page cap must be greater than zero"
        raise ValueError(msg)
    if requested is None:
        return configured
    if requested <= 0:
        msg = "max_pages must be greater than zero"
        raise ValueError(msg)
    if requested > configured:
        msg = (
            "max_pages exceeds configured cap "
            f"CATALYST_POLYGON_TICKERS_MAX_PAGES={configured}"
        )
        raise ValueError(msg)
    return requested


__all__ = ["UniverseSeedResult", "seed_polygon_tickers"]

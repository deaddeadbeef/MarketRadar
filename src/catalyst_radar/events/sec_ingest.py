from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from catalyst_radar.connectors.base import ConnectorRequest
from catalyst_radar.connectors.http import (
    HeaderInjectingTransport,
    JsonHttpClient,
    UrlLibHttpTransport,
)
from catalyst_radar.connectors.provider_ingest import (
    ProviderIngestResult,
    ingest_provider_records,
)
from catalyst_radar.connectors.sec import SecSubmissionsConnector
from catalyst_radar.core.config import AppConfig
from catalyst_radar.storage.event_repositories import EventRepository
from catalyst_radar.storage.provider_repositories import ProviderRepository
from catalyst_radar.storage.repositories import MarketRepository


@dataclass(frozen=True)
class SecSubmissionTarget:
    ticker: str
    cik: str

    def as_payload(self) -> dict[str, str]:
        return {"ticker": self.ticker, "cik": self.cik}


@dataclass(frozen=True)
class SecSubmissionsBatchResult:
    targets: tuple[SecSubmissionTarget, ...]
    results: tuple[ProviderIngestResult, ...]
    live: bool

    def as_payload(self) -> dict[str, object]:
        return {
            "schema_version": "sec-submissions-batch-result-v1",
            "provider": "sec",
            "endpoint": "submissions-batch",
            "live": self.live,
            "target_count": len(self.targets),
            "targets": [target.as_payload() for target in self.targets],
            "external_calls_made": len(self.targets) if self.live else 0,
            "raw_count": sum(result.raw_count for result in self.results),
            "normalized_count": sum(result.normalized_count for result in self.results),
            "security_count": sum(result.security_count for result in self.results),
            "daily_bar_count": sum(result.daily_bar_count for result in self.results),
            "holding_count": sum(result.holding_count for result in self.results),
            "event_count": sum(result.event_count for result in self.results),
            "rejected_count": sum(result.rejected_count for result in self.results),
            "job_ids": [result.job_id for result in self.results],
        }


def parse_sec_submission_target(value: str) -> SecSubmissionTarget:
    raw = str(value or "").strip()
    if ":" not in raw:
        msg = f"--target must use TICKER:CIK form, got {value!r}"
        raise ValueError(msg)
    ticker, cik = (part.strip() for part in raw.split(":", 1))
    if not ticker or not cik:
        msg = f"--target must include both ticker and CIK, got {value!r}"
        raise ValueError(msg)
    return SecSubmissionTarget(ticker=ticker.upper(), cik=cik.zfill(10))


def ingest_sec_submissions_batch(
    *,
    config: AppConfig,
    market_repo: MarketRepository,
    provider_repo: ProviderRepository,
    event_repo: EventRepository,
    targets: Sequence[SecSubmissionTarget],
    fixture_path: Path | None = None,
) -> SecSubmissionsBatchResult:
    parsed_targets = tuple(targets)
    if not parsed_targets:
        msg = "at least one SEC submissions target is required"
        raise ValueError(msg)
    results = tuple(
        ingest_sec_record(
            config=config,
            market_repo=market_repo,
            provider_repo=provider_repo,
            event_repo=event_repo,
            sec_command="submissions",
            ticker=target.ticker,
            cik=target.cik,
            fixture_path=fixture_path,
            document_fixture_path=None,
        )
        for target in parsed_targets
    )
    return SecSubmissionsBatchResult(
        targets=parsed_targets,
        results=results,
        live=fixture_path is None,
    )


def ingest_sec_record(
    *,
    config: AppConfig,
    market_repo: MarketRepository,
    provider_repo: ProviderRepository,
    event_repo: EventRepository,
    sec_command: str,
    ticker: str,
    cik: str,
    fixture_path: Path | None,
    document_fixture_path: Path | None,
) -> ProviderIngestResult:
    if sec_command not in {"submissions", "ipo-s1"}:
        msg = f"unsupported sec command: {sec_command}"
        raise ValueError(msg)
    if fixture_path is None and not config.sec_enable_live:
        msg = "live SEC ingest requires CATALYST_SEC_ENABLE_LIVE=1"
        raise ValueError(msg)
    if fixture_path is None and not config.sec_user_agent_configured:
        msg = "CATALYST_SEC_USER_AGENT is required for live SEC ingest"
        raise ValueError(msg)

    transport = (
        HeaderInjectingTransport(
            UrlLibHttpTransport(),
            {"User-Agent": config.sec_user_agent or ""},
        )
        if fixture_path is None
        else None
    )
    connector = SecSubmissionsConnector(
        fixture_path=fixture_path,
        document_fixture_path=document_fixture_path,
        client=(
            JsonHttpClient(
                transport=transport,
                timeout_seconds=config.http_timeout_seconds,
            )
            if transport is not None
            else None
        ),
        document_transport=transport if sec_command == "ipo-s1" else None,
        document_headers={"User-Agent": config.sec_user_agent or ""}
        if transport is not None and sec_command == "ipo-s1"
        else None,
        document_timeout_seconds=config.http_timeout_seconds,
        base_url=config.sec_base_url,
    )
    metadata = {
        "provider": "sec",
        "endpoint": sec_command,
        "ticker": ticker.upper(),
        "cik": cik,
        "fixture": str(fixture_path) if fixture_path is not None else None,
        "document_fixture": (
            str(document_fixture_path) if document_fixture_path is not None else None
        ),
        "live": fixture_path is None,
    }
    request = ConnectorRequest(
        provider="sec",
        endpoint=sec_command,
        params={"ticker": ticker.upper(), "cik": cik},
        requested_at=datetime.now(UTC),
    )
    return ingest_provider_records(
        connector=connector,
        request=request,
        market_repo=market_repo,
        provider_repo=provider_repo,
        job_type="sec_ipo_s1" if sec_command == "ipo-s1" else "sec_submissions",
        metadata=metadata,
        event_repo=event_repo,
    )

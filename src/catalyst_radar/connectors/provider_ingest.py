from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pandas as pd

from catalyst_radar.connectors.base import (
    ConnectorHealth,
    ConnectorHealthStatus,
    ConnectorRecordKind,
    ConnectorRequest,
    MarketDataConnector,
    NormalizedRecord,
)
from catalyst_radar.core.models import (
    DailyBar,
    DataQualitySeverity,
    HoldingSnapshot,
    JobStatus,
    Security,
)
from catalyst_radar.storage.provider_repositories import ProviderRepository
from catalyst_radar.storage.repositories import MarketRepository


@dataclass(frozen=True)
class ProviderIngestResult:
    provider: str
    job_id: str
    requested_count: int
    raw_count: int
    normalized_count: int
    security_count: int
    daily_bar_count: int
    holding_count: int
    rejected_count: int


class ProviderIngestError(RuntimeError):
    pass


def ingest_provider_records(
    *,
    connector: MarketDataConnector,
    request: ConnectorRequest,
    market_repo: MarketRepository,
    provider_repo: ProviderRepository,
    job_type: str,
    metadata: Mapping[str, Any],
) -> ProviderIngestResult:
    health = connector.healthcheck()
    provider_repo.save_health(health)
    job_id = provider_repo.start_job(job_type, health.provider, metadata=metadata)

    requested_count = 0
    raw_count = 0
    normalized_count = 0
    try:
        if health.status == ConnectorHealthStatus.DOWN:
            _finish_failed_job(
                provider_repo=provider_repo,
                job_id=job_id,
                requested_count=requested_count,
                raw_count=raw_count,
                normalized_count=normalized_count,
                reason=health.reason,
            )
            _record_critical_incident(
                provider_repo=provider_repo,
                provider=health.provider,
                kind=job_type,
                reason=health.reason,
                metadata=metadata,
            )
            raise ProviderIngestError(health.reason)

        raw_records = connector.fetch(request)
        rejections = _rejected_payloads(connector)
        requested_count = len(raw_records) + len(rejections)
        raw_count = provider_repo.save_raw_records(raw_records)
        _record_rejected_payloads(provider_repo, rejections)

        abort_rejections = _abort_rejections(rejections)
        if abort_rejections:
            reason = "; ".join(str(rejected.reason) for rejected in abort_rejections)
            provider_repo.save_health(
                ConnectorHealth(
                    provider=health.provider,
                    status=ConnectorHealthStatus.DOWN,
                    checked_at=datetime.now(UTC),
                    reason=reason,
                )
            )
            _finish_failed_job(
                provider_repo=provider_repo,
                job_id=job_id,
                requested_count=requested_count,
                raw_count=raw_count,
                normalized_count=normalized_count,
                reason=reason,
            )
            raise ProviderIngestError(reason)

        normalized_records = connector.normalize(raw_records)
        normalized_count = provider_repo.save_normalized_records(normalized_records)

        if rejections:
            provider_repo.save_health(
                ConnectorHealth(
                    provider=health.provider,
                    status=ConnectorHealthStatus.DEGRADED,
                    checked_at=datetime.now(UTC),
                    reason=f"rejected payloads={len(rejections)}",
                )
            )

        securities = _securities_from_normalized(normalized_records)
        daily_bars = _daily_bars_from_normalized(normalized_records)
        holdings = _holdings_from_normalized(normalized_records)
        market_repo.upsert_market_snapshot(
            securities_rows=securities,
            daily_bar_rows=daily_bars,
            holding_rows=holdings,
        )

        provider_repo.finish_job(
            job_id,
            JobStatus.PARTIAL_SUCCESS.value if rejections else JobStatus.SUCCESS.value,
            requested_count=requested_count,
            raw_count=raw_count,
            normalized_count=normalized_count,
            error_summary=f"rejected payloads={len(rejections)}" if rejections else None,
        )
        return ProviderIngestResult(
            provider=health.provider,
            job_id=job_id,
            requested_count=requested_count,
            raw_count=raw_count,
            normalized_count=normalized_count,
            security_count=len(securities),
            daily_bar_count=len(daily_bars),
            holding_count=len(holdings),
            rejected_count=len(rejections),
        )
    except ProviderIngestError:
        raise
    except Exception as exc:
        reason = str(exc)
        provider_repo.save_health(
            ConnectorHealth(
                provider=health.provider,
                status=ConnectorHealthStatus.DOWN,
                checked_at=datetime.now(UTC),
                reason=reason,
            )
        )
        _finish_failed_job(
            provider_repo=provider_repo,
            job_id=job_id,
            requested_count=requested_count,
            raw_count=raw_count,
            normalized_count=normalized_count,
            reason=reason,
        )
        _record_critical_incident(
            provider_repo=provider_repo,
            provider=health.provider,
            kind=job_type,
            reason=reason,
            metadata=metadata,
        )
        raise ProviderIngestError(reason) from exc


def _finish_failed_job(
    *,
    provider_repo: ProviderRepository,
    job_id: str,
    requested_count: int,
    raw_count: int,
    normalized_count: int,
    reason: str,
) -> None:
    provider_repo.finish_job(
        job_id,
        JobStatus.FAILED.value,
        requested_count=requested_count,
        raw_count=raw_count,
        normalized_count=normalized_count,
        error_summary=reason,
    )


def _record_critical_incident(
    *,
    provider_repo: ProviderRepository,
    provider: str,
    kind: str,
    reason: str,
    metadata: Mapping[str, Any],
) -> None:
    provider_repo.record_incident(
        provider=provider,
        severity=DataQualitySeverity.CRITICAL,
        kind=kind,
        affected_tickers=(),
        reason=reason,
        fail_closed_action="abort-ingest",
        payload=dict(metadata),
    )


def _rejected_payloads(connector: MarketDataConnector) -> tuple[Any, ...]:
    rejected = getattr(connector, "rejected_payloads", ())
    return tuple(rejected)


def _record_rejected_payloads(
    provider_repo: ProviderRepository,
    rejected_payloads: Sequence[Any],
) -> None:
    for rejected in rejected_payloads:
        provider_repo.record_incident(
            provider=str(rejected.provider),
            severity=rejected.severity,
            kind=rejected.kind.value,
            affected_tickers=rejected.affected_tickers,
            reason=str(rejected.reason),
            fail_closed_action=str(rejected.fail_closed_action),
            payload=rejected.payload,
        )


def _abort_rejections(rejected_payloads: Sequence[Any]) -> list[Any]:
    return [
        rejected
        for rejected in rejected_payloads
        if rejected.severity == DataQualitySeverity.CRITICAL
        or rejected.fail_closed_action == "abort-ingest"
    ]


def _securities_from_normalized(records: Sequence[NormalizedRecord]) -> list[Security]:
    return [
        _security_from_payload(record.payload)
        for record in records
        if record.kind == ConnectorRecordKind.SECURITY
    ]


def _daily_bars_from_normalized(records: Sequence[NormalizedRecord]) -> list[DailyBar]:
    return [
        _daily_bar_from_normalized(record)
        for record in records
        if record.kind == ConnectorRecordKind.DAILY_BAR
    ]


def _holdings_from_normalized(records: Sequence[NormalizedRecord]) -> list[HoldingSnapshot]:
    return [
        _holding_from_payload(record.payload)
        for record in records
        if record.kind == ConnectorRecordKind.HOLDING
    ]


def _security_from_payload(payload: Mapping[str, Any]) -> Security:
    return Security(
        ticker=str(payload["ticker"]).upper(),
        name=str(payload["name"]),
        exchange=str(payload["exchange"]),
        sector=str(payload["sector"]),
        industry=str(payload["industry"]),
        market_cap=float(payload["market_cap"]),
        avg_dollar_volume_20d=float(payload["avg_dollar_volume_20d"]),
        has_options=bool(payload["has_options"]),
        is_active=bool(payload["is_active"]),
        updated_at=_parse_datetime(payload["updated_at"]),
        metadata=payload.get("metadata", {}),
    )


def _daily_bar_from_normalized(record: NormalizedRecord) -> DailyBar:
    payload = record.payload
    return DailyBar(
        ticker=str(payload["ticker"]).upper(),
        date=pd.Timestamp(payload["date"]).date(),
        open=float(payload["open"]),
        high=float(payload["high"]),
        low=float(payload["low"]),
        close=float(payload["close"]),
        volume=int(payload["volume"]),
        vwap=float(payload["vwap"]),
        adjusted=bool(payload["adjusted"]),
        provider=str(payload["provider"]),
        source_ts=record.source_ts,
        available_at=record.available_at,
    )


def _holding_from_payload(payload: Mapping[str, Any]) -> HoldingSnapshot:
    return HoldingSnapshot(
        ticker=str(payload["ticker"]).upper(),
        shares=float(payload["shares"]),
        market_value=float(payload["market_value"]),
        sector=str(payload["sector"]),
        theme=str(payload["theme"]),
        as_of=_parse_datetime(payload["as_of"]),
        portfolio_value=float(payload.get("portfolio_value", 0.0) or 0.0),
        cash=float(payload.get("cash", 0.0) or 0.0),
    )


def _parse_datetime(value: Any) -> datetime:
    parsed = pd.Timestamp(value).to_pydatetime()
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


__all__ = [
    "ProviderIngestError",
    "ProviderIngestResult",
    "ingest_provider_records",
]

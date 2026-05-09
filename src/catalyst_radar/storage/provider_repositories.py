from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import Engine, insert, select, update

from catalyst_radar.connectors.base import (
    ConnectorHealth,
    ConnectorHealthStatus,
    ConnectorRecordKind,
    MarketDataConnector,
    NormalizedRecord,
    RawRecord,
)
from catalyst_radar.core.immutability import thaw_json_value
from catalyst_radar.core.models import DataQualitySeverity, JobStatus
from catalyst_radar.storage.schema import (
    data_quality_incidents,
    job_runs,
    normalized_provider_records,
    provider_health,
    raw_provider_records,
    universe_members,
    universe_snapshots,
)


class ProviderRepository:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def save_raw_records(self, records: Iterable[RawRecord]) -> int:
        count = 0
        with self.engine.begin() as conn:
            for record in records:
                conn.execute(
                    insert(raw_provider_records).values(
                        id=str(uuid4()),
                        provider=record.provider,
                        kind=record.kind.value,
                        request_hash=record.request_hash,
                        payload_hash=record.payload_hash,
                        payload=thaw_json_value(record.payload),
                        source_ts=_to_utc_datetime(record.source_ts),
                        fetched_at=_to_utc_datetime(record.fetched_at),
                        available_at=_to_utc_datetime(record.available_at),
                        license_tag=record.license_tag,
                        retention_policy=record.retention_policy,
                        created_at=datetime.now(UTC),
                    )
                )
                count += 1
        return count

    def save_normalized_records(self, records: Iterable[NormalizedRecord]) -> int:
        count = 0
        with self.engine.begin() as conn:
            for record in records:
                conn.execute(
                    insert(normalized_provider_records).values(
                        id=str(uuid4()),
                        provider=record.provider,
                        kind=record.kind.value,
                        identity=record.identity,
                        payload=thaw_json_value(record.payload),
                        source_ts=_to_utc_datetime(record.source_ts),
                        available_at=_to_utc_datetime(record.available_at),
                        raw_payload_hash=record.raw_payload_hash,
                        created_at=datetime.now(UTC),
                    )
                )
                count += 1
        return count

    def list_raw_records(
        self,
        provider: str | None = None,
        kind: ConnectorRecordKind | None = None,
    ) -> list[RawRecord]:
        filters = []
        if provider is not None:
            filters.append(raw_provider_records.c.provider == provider)
        if kind is not None:
            filters.append(raw_provider_records.c.kind == kind.value)
        stmt = (
            select(raw_provider_records)
            .where(*filters)
            .order_by(
                raw_provider_records.c.source_ts,
                raw_provider_records.c.fetched_at,
                raw_provider_records.c.available_at,
                raw_provider_records.c.provider,
                raw_provider_records.c.kind,
                raw_provider_records.c.payload_hash,
                raw_provider_records.c.id,
            )
        )
        with self.engine.connect() as conn:
            return [
                RawRecord(
                    provider=row.provider,
                    kind=ConnectorRecordKind(row.kind),
                    request_hash=row.request_hash,
                    payload_hash=row.payload_hash,
                    payload=row.payload,
                    source_ts=_as_datetime(row.source_ts),
                    fetched_at=_as_datetime(row.fetched_at),
                    available_at=_as_datetime(row.available_at),
                    license_tag=row.license_tag,
                    retention_policy=row.retention_policy,
                )
                for row in conn.execute(stmt)
            ]

    def list_normalized_records(
        self,
        provider: str | None = None,
        kind: ConnectorRecordKind | None = None,
    ) -> list[NormalizedRecord]:
        filters = []
        if provider is not None:
            filters.append(normalized_provider_records.c.provider == provider)
        if kind is not None:
            filters.append(normalized_provider_records.c.kind == kind.value)
        stmt = (
            select(normalized_provider_records)
            .where(*filters)
            .order_by(
                normalized_provider_records.c.source_ts,
                normalized_provider_records.c.available_at,
                normalized_provider_records.c.provider,
                normalized_provider_records.c.kind,
                normalized_provider_records.c.identity,
                normalized_provider_records.c.raw_payload_hash,
                normalized_provider_records.c.id,
            )
        )
        with self.engine.connect() as conn:
            return [
                NormalizedRecord(
                    provider=row.provider,
                    kind=ConnectorRecordKind(row.kind),
                    identity=row.identity,
                    payload=row.payload,
                    source_ts=_as_datetime(row.source_ts),
                    available_at=_as_datetime(row.available_at),
                    raw_payload_hash=row.raw_payload_hash,
                )
                for row in conn.execute(stmt)
            ]

    def save_health(self, health: ConnectorHealth) -> str:
        health_id = str(uuid4())
        with self.engine.begin() as conn:
            conn.execute(
                insert(provider_health).values(
                    id=health_id,
                    provider=health.provider,
                    status=health.status.value,
                    checked_at=_to_utc_datetime(health.checked_at),
                    reason=health.reason,
                    latency_ms=health.latency_ms,
                )
            )
        return health_id

    def latest_health(self, provider: str) -> ConnectorHealth | None:
        stmt = (
            select(provider_health)
            .where(provider_health.c.provider == provider)
            .order_by(provider_health.c.checked_at.desc(), provider_health.c.id.desc())
            .limit(1)
        )
        with self.engine.connect() as conn:
            row = conn.execute(stmt).first()
        if row is None:
            return None
        return ConnectorHealth(
            provider=row.provider,
            status=ConnectorHealthStatus(row.status),
            checked_at=_as_datetime(row.checked_at),
            reason=row.reason,
            latency_ms=row.latency_ms,
        )

    def start_job(
        self,
        job_type: str,
        provider: str | None,
        metadata: Mapping[str, Any] | None = None,
    ) -> str:
        job_id = str(uuid4())
        with self.engine.begin() as conn:
            conn.execute(
                insert(job_runs).values(
                    id=job_id,
                    job_type=job_type,
                    provider=provider,
                    status=JobStatus.RUNNING.value,
                    started_at=datetime.now(UTC),
                    finished_at=None,
                    requested_count=0,
                    raw_count=0,
                    normalized_count=0,
                    error_summary=None,
                    metadata=thaw_json_value(metadata or {}),
                )
            )
        return job_id

    def finish_job(
        self,
        job_id: str,
        status: str,
        requested_count: int,
        raw_count: int,
        normalized_count: int,
        error_summary: str | None = None,
    ) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                update(job_runs)
                .where(job_runs.c.id == job_id)
                .values(
                    status=status,
                    finished_at=datetime.now(UTC),
                    requested_count=requested_count,
                    raw_count=raw_count,
                    normalized_count=normalized_count,
                    error_summary=error_summary,
                )
            )

    def record_incident(
        self,
        provider: str,
        severity: DataQualitySeverity,
        kind: str,
        affected_tickers: Sequence[str],
        reason: str,
        fail_closed_action: str,
        payload: Mapping[str, Any],
        source_ts: datetime | None = None,
        available_at: datetime | None = None,
    ) -> str:
        incident_id = str(uuid4())
        with self.engine.begin() as conn:
            conn.execute(
                insert(data_quality_incidents).values(
                    id=incident_id,
                    provider=provider,
                    severity=severity.value,
                    kind=kind,
                    affected_tickers=list(affected_tickers),
                    reason=reason,
                    fail_closed_action=fail_closed_action,
                    payload=thaw_json_value(payload),
                    detected_at=datetime.now(UTC),
                    source_ts=_to_utc_datetime(source_ts) if source_ts is not None else None,
                    available_at=(
                        _to_utc_datetime(available_at) if available_at is not None else None
                    ),
                )
            )
        return incident_id

    def record_rejected_payload(
        self,
        provider: str,
        kind: str,
        payload: Mapping[str, Any],
        reason: str,
        severity: DataQualitySeverity,
        fail_closed_action: str,
    ) -> str:
        return self.record_incident(
            provider=provider,
            severity=severity,
            kind=kind,
            affected_tickers=(),
            reason=reason,
            fail_closed_action=fail_closed_action,
            payload=payload,
        )

    def save_universe_snapshot(
        self,
        name: str,
        as_of: datetime,
        provider: str,
        source_ts: datetime,
        available_at: datetime,
        members: Iterable[str | Mapping[str, Any]],
        metadata: Mapping[str, Any] | None = None,
    ) -> str:
        snapshot_id = str(uuid4())
        member_rows = [_universe_member_row(snapshot_id, member) for member in members]
        with self.engine.begin() as conn:
            conn.execute(
                insert(universe_snapshots).values(
                    id=snapshot_id,
                    name=name,
                    as_of=_to_utc_datetime(as_of),
                    provider=provider,
                    source_ts=_to_utc_datetime(source_ts),
                    available_at=_to_utc_datetime(available_at),
                    member_count=len(member_rows),
                    metadata=thaw_json_value(metadata or {}),
                )
            )
            for row in member_rows:
                conn.execute(insert(universe_members).values(**row))
        return snapshot_id

    def list_universe_members(self, snapshot_id: str) -> list[str]:
        stmt = (
            select(universe_members.c.ticker)
            .where(universe_members.c.snapshot_id == snapshot_id)
            .order_by(universe_members.c.rank, universe_members.c.ticker)
        )
        with self.engine.connect() as conn:
            return [row.ticker for row in conn.execute(stmt)]

    def latest_universe_snapshot(
        self,
        *,
        name: str,
        as_of: datetime,
        available_at: datetime,
    ) -> UniverseSnapshotRecord | None:
        stmt = (
            select(universe_snapshots)
            .where(
                universe_snapshots.c.name == name,
                universe_snapshots.c.as_of <= _to_utc_datetime(as_of),
                universe_snapshots.c.available_at <= _to_utc_datetime(available_at),
            )
            .order_by(
                universe_snapshots.c.as_of.desc(),
                universe_snapshots.c.available_at.desc(),
                universe_snapshots.c.id.desc(),
            )
            .limit(1)
        )
        with self.engine.connect() as conn:
            row = conn.execute(stmt).first()
        if row is None:
            return None
        return UniverseSnapshotRecord(
            id=row.id,
            name=row.name,
            as_of=_as_datetime(row.as_of),
            provider=row.provider,
            source_ts=_as_datetime(row.source_ts),
            available_at=_as_datetime(row.available_at),
            member_count=row.member_count,
            metadata=row._mapping["metadata"],
        )

    def list_universe_member_rows(self, snapshot_id: str) -> list[UniverseMemberRecord]:
        stmt = (
            select(universe_members)
            .where(universe_members.c.snapshot_id == snapshot_id)
            .order_by(universe_members.c.rank, universe_members.c.ticker)
        )
        with self.engine.connect() as conn:
            return [
                UniverseMemberRecord(
                    snapshot_id=row.snapshot_id,
                    ticker=row.ticker,
                    reason=row.reason,
                    rank=row.rank,
                    metadata=row._mapping["metadata"],
                )
                for row in conn.execute(stmt)
            ]


def replay_normalized_records(
    raw_records: Sequence[RawRecord],
    connector: MarketDataConnector,
) -> list[NormalizedRecord]:
    raw_payload_hashes = {record.payload_hash for record in raw_records}
    normalized_records = connector.normalize(raw_records)
    for record in normalized_records:
        if record.raw_payload_hash not in raw_payload_hashes:
            msg = (
                "normalized record references unknown raw payload hash: "
                f"{record.raw_payload_hash}"
            )
            raise ValueError(msg)
        if record.available_at is None:
            msg = "normalized records must include available_at"
            raise ValueError(msg)
    return normalized_records


@dataclass(frozen=True)
class UniverseSnapshotRecord:
    id: str
    name: str
    as_of: datetime
    provider: str
    source_ts: datetime
    available_at: datetime
    member_count: int
    metadata: Mapping[str, Any]


@dataclass(frozen=True)
class UniverseMemberRecord:
    snapshot_id: str
    ticker: str
    reason: str
    rank: int | None
    metadata: Mapping[str, Any]


def _as_datetime(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _to_utc_datetime(value: datetime) -> datetime:
    if not isinstance(value, datetime):
        msg = "datetime values must be timezone-aware before persistence"
        raise ValueError(msg)
    if value.tzinfo is None or value.utcoffset() is None:
        msg = "datetime values must be timezone-aware before persistence"
        raise ValueError(msg)
    return value.astimezone(UTC)


def _universe_member_row(snapshot_id: str, member: str | Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(member, str):
        return {
            "snapshot_id": snapshot_id,
            "ticker": member,
            "reason": "",
            "rank": None,
            "metadata": {},
        }
    return {
        "snapshot_id": snapshot_id,
        "ticker": str(member["ticker"]),
        "reason": str(member.get("reason", "")),
        "rank": member.get("rank"),
        "metadata": thaw_json_value(member.get("metadata", {})),
    }


__all__ = [
    "ProviderRepository",
    "UniverseMemberRecord",
    "UniverseSnapshotRecord",
    "replay_normalized_records",
]

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, select

from catalyst_radar.connectors.base import (
    ConnectorHealth,
    ConnectorHealthStatus,
    ConnectorRecordKind,
    ConnectorRequest,
    NormalizedRecord,
    ProviderCostEstimate,
    RawRecord,
)
from catalyst_radar.core.models import DataQualitySeverity, JobStatus
from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.provider_repositories import (
    ProviderRepository,
    replay_normalized_records,
)
from catalyst_radar.storage.schema import data_quality_incidents, job_runs


def test_raw_records_round_trip_payload_hash_and_timestamps() -> None:
    repo = _repo()
    source_ts = datetime(2026, 5, 10, 1, 0, tzinfo=UTC)
    fetched_at = source_ts + timedelta(seconds=2)
    available_at = source_ts + timedelta(seconds=5)
    record = RawRecord(
        provider="dry-run",
        kind=ConnectorRecordKind.DAILY_BAR,
        request_hash="request-1",
        payload_hash="payload-1",
        payload={"ticker": "MSFT", "nested": {"prices": [1, 2, 3]}},
        source_ts=source_ts,
        fetched_at=fetched_at,
        available_at=available_at,
        license_tag="local-csv-fixture",
        retention_policy="retain-local-fixture",
    )

    assert repo.save_raw_records([record]) == 1

    rows = repo.list_raw_records(provider="dry-run", kind=ConnectorRecordKind.DAILY_BAR)
    assert rows == [record]
    assert rows[0].source_ts.tzinfo is UTC
    assert rows[0].fetched_at.tzinfo is UTC
    assert rows[0].available_at.tzinfo is UTC


def test_raw_records_list_in_source_and_fetch_order() -> None:
    repo = _repo()
    later = _raw_record("payload-later")
    earlier = RawRecord(
        provider="dry-run",
        kind=ConnectorRecordKind.SECURITY,
        request_hash="request-1",
        payload_hash="payload-earlier",
        payload={"ticker": "AAPL"},
        source_ts=later.source_ts - timedelta(minutes=1),
        fetched_at=later.fetched_at - timedelta(minutes=1),
        available_at=later.available_at - timedelta(minutes=1),
        license_tag="local-csv-fixture",
        retention_policy="retain-local-fixture",
    )

    repo.save_raw_records([later, earlier])

    assert [record.payload_hash for record in repo.list_raw_records()] == [
        "payload-earlier",
        "payload-later",
    ]


def test_repository_normalizes_non_utc_datetimes_before_sqlite_round_trip() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    create_schema(engine)
    repo = ProviderRepository(engine)
    plus_eight = timezone(timedelta(hours=8))
    source_ts = datetime(2026, 5, 10, 9, 0, tzinfo=plus_eight)
    fetched_at = source_ts + timedelta(seconds=2)
    available_at = source_ts + timedelta(seconds=5)
    raw = RawRecord(
        provider="dry-run",
        kind=ConnectorRecordKind.DAILY_BAR,
        request_hash="request-1",
        payload_hash="payload-1",
        payload={"ticker": "MSFT"},
        source_ts=source_ts,
        fetched_at=fetched_at,
        available_at=available_at,
        license_tag="local-csv-fixture",
        retention_policy="retain-local-fixture",
    )

    repo.save_raw_records([raw])
    repo.save_health(
        ConnectorHealth(
            provider="dry-run",
            status=ConnectorHealthStatus.HEALTHY,
            checked_at=source_ts,
            reason="ok",
        )
    )
    incident_id = repo.record_incident(
        provider="dry-run",
        severity=DataQualitySeverity.WARNING,
        kind=ConnectorRecordKind.DAILY_BAR.value,
        affected_tickers=("MSFT",),
        reason="late",
        fail_closed_action="skip-record",
        payload={"ticker": "MSFT"},
        source_ts=source_ts,
        available_at=available_at,
    )

    listed_raw = repo.list_raw_records()[0]
    latest_health = repo.latest_health("dry-run")
    with engine.connect() as conn:
        incident = conn.execute(
            select(data_quality_incidents).where(data_quality_incidents.c.id == incident_id)
        ).first()

    assert listed_raw.source_ts == datetime(2026, 5, 10, 1, 0, tzinfo=UTC)
    assert listed_raw.fetched_at == datetime(2026, 5, 10, 1, 0, 2, tzinfo=UTC)
    assert listed_raw.available_at == datetime(2026, 5, 10, 1, 0, 5, tzinfo=UTC)
    assert latest_health is not None
    assert latest_health.checked_at == datetime(2026, 5, 10, 1, 0, tzinfo=UTC)
    assert incident is not None
    assert incident.source_ts == datetime(2026, 5, 10, 1, 0)
    assert incident.available_at == datetime(2026, 5, 10, 1, 0, 5)


def test_normalized_records_round_trip_identity_and_raw_payload_hash() -> None:
    repo = _repo()
    source_ts = datetime(2026, 5, 10, 1, 0, tzinfo=UTC)
    record = NormalizedRecord(
        provider="dry-run",
        kind=ConnectorRecordKind.SECURITY,
        identity="MSFT",
        payload={"ticker": "MSFT", "name": "Microsoft"},
        source_ts=source_ts,
        available_at=source_ts + timedelta(seconds=1),
        raw_payload_hash="payload-1",
    )

    assert repo.save_normalized_records([record]) == 1

    rows = repo.list_normalized_records(provider="dry-run", kind=ConnectorRecordKind.SECURITY)
    assert rows == [record]
    assert rows[0].source_ts.tzinfo is UTC
    assert rows[0].available_at.tzinfo is UTC


def test_normalized_records_list_in_source_and_identity_order() -> None:
    repo = _repo()
    source_ts = datetime(2026, 5, 10, 1, 0, tzinfo=UTC)
    records = [
        NormalizedRecord(
            provider="dry-run",
            kind=ConnectorRecordKind.SECURITY,
            identity="MSFT",
            payload={"ticker": "MSFT"},
            source_ts=source_ts + timedelta(minutes=1),
            available_at=source_ts + timedelta(minutes=2),
            raw_payload_hash="payload-msft",
        ),
        NormalizedRecord(
            provider="dry-run",
            kind=ConnectorRecordKind.SECURITY,
            identity="AAPL",
            payload={"ticker": "AAPL"},
            source_ts=source_ts,
            available_at=source_ts + timedelta(minutes=1),
            raw_payload_hash="payload-aapl",
        ),
    ]

    repo.save_normalized_records(records)

    assert [record.identity for record in repo.list_normalized_records()] == ["AAPL", "MSFT"]


def test_latest_health_returns_most_recent_checked_at() -> None:
    repo = _repo()
    older = datetime(2026, 5, 10, 1, 0, tzinfo=UTC)
    newer = older + timedelta(minutes=5)

    repo.save_health(
        ConnectorHealth(
            provider="dry-run",
            status=ConnectorHealthStatus.DEGRADED,
            checked_at=older,
            reason="slow",
            latency_ms=100.0,
        )
    )
    repo.save_health(
        ConnectorHealth(
            provider="dry-run",
            status=ConnectorHealthStatus.HEALTHY,
            checked_at=newer,
            reason="ok",
            latency_ms=12.5,
        )
    )

    health = repo.latest_health("dry-run")

    assert health == ConnectorHealth(
        provider="dry-run",
        status=ConnectorHealthStatus.HEALTHY,
        checked_at=newer,
        reason="ok",
        latency_ms=12.5,
    )
    assert health.checked_at.tzinfo is UTC


def test_job_run_transitions_from_running_to_terminal_status() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    create_schema(engine)
    repo = ProviderRepository(engine)

    job_id = repo.start_job("daily-ingest", "dry-run", metadata={"symbols": ["MSFT"]})

    with engine.connect() as conn:
        started = conn.execute(select(job_runs).where(job_runs.c.id == job_id)).first()
    assert started is not None
    assert started.status == JobStatus.RUNNING.value
    assert started.finished_at is None
    assert started._mapping["metadata"] == {"symbols": ["MSFT"]}

    repo.finish_job(
        job_id,
        JobStatus.SUCCESS.value,
        requested_count=1,
        raw_count=1,
        normalized_count=1,
    )

    with engine.connect() as conn:
        finished = conn.execute(select(job_runs).where(job_runs.c.id == job_id)).first()
    assert finished is not None
    assert finished.status == JobStatus.SUCCESS.value
    assert finished.finished_at is not None
    assert finished.requested_count == 1
    assert finished.raw_count == 1
    assert finished.normalized_count == 1


def test_job_run_records_failure_status_and_error_summary() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    create_schema(engine)
    repo = ProviderRepository(engine)

    job_id = repo.start_job("daily-ingest", "dry-run")
    repo.finish_job(
        job_id,
        JobStatus.FAILED.value,
        requested_count=2,
        raw_count=1,
        normalized_count=0,
        error_summary="provider unavailable",
    )

    with engine.connect() as conn:
        failed = conn.execute(select(job_runs).where(job_runs.c.id == job_id)).first()
    assert failed is not None
    assert failed.status == JobStatus.FAILED.value
    assert failed.finished_at is not None
    assert failed.requested_count == 2
    assert failed.raw_count == 1
    assert failed.normalized_count == 0
    assert failed.error_summary == "provider unavailable"


def test_incident_persistence_round_trips_severity_tickers_and_action() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    create_schema(engine)
    repo = ProviderRepository(engine)
    source_ts = datetime(2026, 5, 10, 1, 0, tzinfo=UTC)
    incident_id = repo.record_incident(
        provider="dry-run",
        severity=DataQualitySeverity.ERROR,
        kind=ConnectorRecordKind.DAILY_BAR.value,
        affected_tickers=("MSFT", "AAPL"),
        reason="missing close",
        fail_closed_action="skip-record",
        payload={"ticker": "MSFT", "close": None},
        source_ts=source_ts,
        available_at=source_ts + timedelta(seconds=1),
    )

    with engine.connect() as conn:
        incident = conn.execute(
            select(data_quality_incidents).where(data_quality_incidents.c.id == incident_id)
        ).first()

    assert incident is not None
    assert incident.severity == DataQualitySeverity.ERROR.value
    assert incident.affected_tickers == ["MSFT", "AAPL"]
    assert incident.fail_closed_action == "skip-record"
    assert incident.payload == {"ticker": "MSFT", "close": None}


def test_universe_snapshot_round_trips_member_list() -> None:
    repo = _repo()
    as_of = datetime(2026, 5, 10, 1, 0, tzinfo=UTC)

    snapshot_id = repo.save_universe_snapshot(
        name="liquid-large-cap",
        as_of=as_of,
        provider="dry-run",
        source_ts=as_of,
        available_at=as_of + timedelta(seconds=1),
        members=[
            {"ticker": "AAPL", "reason": "liquid", "rank": 2, "metadata": {"score": 90}},
            {"ticker": "MSFT", "reason": "liquid", "rank": 1, "metadata": {"score": 95}},
        ],
        metadata={"source": "fixture"},
    )

    assert repo.list_universe_members(snapshot_id) == ["MSFT", "AAPL"]


def test_replay_rejects_unknown_raw_payload_hash() -> None:
    raw = _raw_record("payload-1")
    connector = _StaticConnector(
        [
            NormalizedRecord(
                provider="dry-run",
                kind=ConnectorRecordKind.SECURITY,
                identity="MSFT",
                payload={"ticker": "MSFT"},
                source_ts=raw.source_ts,
                available_at=raw.available_at,
                raw_payload_hash="missing",
            )
        ]
    )

    with pytest.raises(ValueError, match="unknown raw payload hash"):
        replay_normalized_records([raw], connector)


def test_replay_returns_valid_normalized_records_from_connector() -> None:
    raw = _raw_record("payload-1")
    normalized = NormalizedRecord(
        provider="dry-run",
        kind=ConnectorRecordKind.SECURITY,
        identity="MSFT",
        payload={"ticker": "MSFT"},
        source_ts=raw.source_ts,
        available_at=raw.available_at,
        raw_payload_hash=raw.payload_hash,
    )
    connector = _StaticConnector([normalized])

    assert replay_normalized_records([raw], connector) == [normalized]
    assert connector.normalized_input == [raw]


def test_replay_rejects_normalized_records_without_availability() -> None:
    raw = _raw_record("payload-1")
    missing_available_at = object.__new__(NormalizedRecord)
    object.__setattr__(missing_available_at, "provider", "dry-run")
    object.__setattr__(missing_available_at, "kind", ConnectorRecordKind.SECURITY)
    object.__setattr__(missing_available_at, "identity", "MSFT")
    object.__setattr__(missing_available_at, "payload", {"ticker": "MSFT"})
    object.__setattr__(missing_available_at, "source_ts", raw.source_ts)
    object.__setattr__(missing_available_at, "available_at", None)
    object.__setattr__(missing_available_at, "raw_payload_hash", raw.payload_hash)
    connector = _StaticConnector([missing_available_at])

    with pytest.raises(ValueError, match="available_at"):
        replay_normalized_records([raw], connector)


def test_rejected_invalid_payload_is_stored_in_incident_payload() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    create_schema(engine)
    repo = ProviderRepository(engine)
    invalid_payload = {"provider": "", "source_ts": None, "ticker": "MSFT"}

    incident_id = repo.record_rejected_payload(
        provider="dry-run",
        kind=ConnectorRecordKind.DAILY_BAR.value,
        payload=invalid_payload,
        reason="provider must be non-empty",
        severity=DataQualitySeverity.CRITICAL,
        fail_closed_action="drop-payload",
    )

    with engine.connect() as conn:
        incident = conn.execute(
            select(data_quality_incidents).where(data_quality_incidents.c.id == incident_id)
        ).first()
    assert incident is not None
    assert incident.payload == invalid_payload
    assert incident.source_ts is None
    assert incident.available_at is None
    assert incident.severity == DataQualitySeverity.CRITICAL.value


def _repo() -> ProviderRepository:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    create_schema(engine)
    return ProviderRepository(engine)


def _raw_record(payload_hash: str) -> RawRecord:
    source_ts = datetime(2026, 5, 10, 1, 0, tzinfo=UTC)
    return RawRecord(
        provider="dry-run",
        kind=ConnectorRecordKind.SECURITY,
        request_hash="request-1",
        payload_hash=payload_hash,
        payload={"ticker": "MSFT"},
        source_ts=source_ts,
        fetched_at=source_ts + timedelta(seconds=1),
        available_at=source_ts + timedelta(seconds=2),
        license_tag="local-csv-fixture",
        retention_policy="retain-local-fixture",
    )


class _StaticConnector:
    def __init__(self, normalized_records: list[NormalizedRecord]) -> None:
        self.normalized_records = normalized_records
        self.normalized_input: Sequence[RawRecord] | None = None

    def fetch(self, request: ConnectorRequest) -> list[RawRecord]:
        raise NotImplementedError

    def normalize(self, records: Sequence[RawRecord]) -> list[NormalizedRecord]:
        self.normalized_input = records
        return self.normalized_records

    def healthcheck(self) -> ConnectorHealth:
        raise NotImplementedError

    def estimate_cost(self, request: ConnectorRequest) -> ProviderCostEstimate:
        raise NotImplementedError

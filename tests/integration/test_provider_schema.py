from pathlib import Path

from sqlalchemy import create_engine, inspect
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.schema import (
    data_quality_incidents,
    job_runs,
    normalized_provider_records,
    provider_health,
    raw_provider_records,
    universe_members,
    universe_snapshots,
)

PROVIDER_TABLES = {
    "raw_provider_records",
    "normalized_provider_records",
    "provider_health",
    "job_runs",
    "data_quality_incidents",
    "universe_snapshots",
    "universe_members",
}


def test_create_schema_adds_provider_foundation_tables() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

    create_schema(engine)

    table_names = set(inspect(engine).get_table_names())
    assert PROVIDER_TABLES.issubset(table_names)


def test_provider_migration_declares_required_tables_and_availability_columns() -> None:
    migration_sql = Path("sql/migrations/002_provider_foundation.sql").read_text(encoding="utf-8")

    for table_name in PROVIDER_TABLES:
        assert f"CREATE TABLE IF NOT EXISTS {table_name}" in migration_sql

    for expected_column in [
        "source_ts TIMESTAMPTZ NOT NULL",
        "available_at TIMESTAMPTZ NOT NULL",
        "payload JSONB NOT NULL",
    ]:
        assert expected_column in migration_sql


def test_provider_tables_compile_postgres_jsonb_and_timestamps() -> None:
    dialect = postgresql.dialect()

    raw_ddl = str(CreateTable(raw_provider_records).compile(dialect=dialect))
    normalized_ddl = str(CreateTable(normalized_provider_records).compile(dialect=dialect))
    health_ddl = str(CreateTable(provider_health).compile(dialect=dialect))
    job_ddl = str(CreateTable(job_runs).compile(dialect=dialect))
    incident_ddl = str(CreateTable(data_quality_incidents).compile(dialect=dialect))
    universe_ddl = str(CreateTable(universe_snapshots).compile(dialect=dialect))
    member_ddl = str(CreateTable(universe_members).compile(dialect=dialect))

    assert "payload JSONB NOT NULL" in raw_ddl
    assert "available_at TIMESTAMP WITH TIME ZONE NOT NULL" in raw_ddl
    assert "payload JSONB NOT NULL" in normalized_ddl
    assert "available_at TIMESTAMP WITH TIME ZONE NOT NULL" in normalized_ddl
    assert "checked_at TIMESTAMP WITH TIME ZONE NOT NULL" in health_ddl
    assert "metadata JSONB NOT NULL" in job_ddl
    assert "requested_count INTEGER DEFAULT 0 NOT NULL" in job_ddl
    assert "raw_count INTEGER DEFAULT 0 NOT NULL" in job_ddl
    assert "normalized_count INTEGER DEFAULT 0 NOT NULL" in job_ddl
    assert "payload JSONB NOT NULL" in incident_ddl
    assert "affected_tickers JSONB NOT NULL" in incident_ddl
    assert "available_at TIMESTAMP WITH TIME ZONE NOT NULL" in universe_ddl
    assert "metadata JSONB NOT NULL" in member_ddl


def test_provider_schema_exports_expected_tables() -> None:
    assert raw_provider_records.name == "raw_provider_records"
    assert normalized_provider_records.name == "normalized_provider_records"
    assert provider_health.name == "provider_health"
    assert job_runs.name == "job_runs"
    assert data_quality_incidents.name == "data_quality_incidents"
    assert universe_snapshots.name == "universe_snapshots"
    assert universe_members.name == "universe_members"


def test_incidents_allow_missing_source_and_availability_timestamps() -> None:
    assert data_quality_incidents.c.source_ts.nullable is True
    assert data_quality_incidents.c.available_at.nullable is True

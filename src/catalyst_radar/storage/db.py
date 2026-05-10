from __future__ import annotations

from pathlib import Path

from sqlalchemy import Engine, create_engine, inspect

from catalyst_radar.storage.schema import audit_events, job_locks, metadata


def engine_from_url(database_url: str) -> Engine:
    if database_url.startswith("sqlite:///"):
        db_path = Path(database_url.removeprefix("sqlite:///"))
        if str(db_path) != ":memory:":
            db_path.parent.mkdir(parents=True, exist_ok=True)
    return create_engine(database_url, future=True)


def create_schema(engine: Engine) -> None:
    metadata.create_all(engine)
    if engine.dialect.name == "sqlite":
        _upgrade_sqlite_audit_events(engine)
        _upgrade_sqlite_job_locks(engine)
        _upgrade_sqlite_holdings_snapshots(engine)


def _upgrade_sqlite_audit_events(engine: Engine) -> None:
    if engine.dialect.name != "sqlite":
        return
    inspector = inspect(engine)
    if "audit_events" in inspector.get_table_names():
        return
    audit_events.create(engine)


def _upgrade_sqlite_job_locks(engine: Engine) -> None:
    if engine.dialect.name != "sqlite":
        return
    inspector = inspect(engine)
    if "job_locks" in inspector.get_table_names():
        return
    job_locks.create(engine)


def _upgrade_sqlite_holdings_snapshots(engine: Engine) -> None:
    with engine.begin() as conn:
        existing_columns = {
            str(row[1]) for row in conn.exec_driver_sql("PRAGMA table_info(holdings_snapshots)")
        }
        if "portfolio_value" not in existing_columns:
            conn.exec_driver_sql(
                "ALTER TABLE holdings_snapshots "
                "ADD COLUMN portfolio_value FLOAT DEFAULT 0"
            )
        if "cash" not in existing_columns:
            conn.exec_driver_sql(
                "ALTER TABLE holdings_snapshots ADD COLUMN cash FLOAT DEFAULT 0"
            )

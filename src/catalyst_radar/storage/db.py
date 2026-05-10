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
    elif engine.dialect.name == "postgresql":
        _ensure_postgres_audit_events_contract(engine)


def _upgrade_sqlite_audit_events(engine: Engine) -> None:
    if engine.dialect.name != "sqlite":
        return
    inspector = inspect(engine)
    if "audit_events" in inspector.get_table_names():
        _ensure_sqlite_audit_events_contract(engine)
        return
    audit_events.create(engine)
    _ensure_sqlite_audit_events_contract(engine)


def _ensure_sqlite_audit_events_contract(engine: Engine) -> None:
    with engine.begin() as conn:
        existing_columns = {
            str(row[1]) for row in conn.exec_driver_sql("PRAGMA table_info(audit_events)")
        }
        if "paper_trade_id" not in existing_columns:
            conn.exec_driver_sql("ALTER TABLE audit_events ADD COLUMN paper_trade_id VARCHAR")
        if "alert_id" not in existing_columns:
            conn.exec_driver_sql("ALTER TABLE audit_events ADD COLUMN alert_id VARCHAR")
        if "decision" not in existing_columns:
            conn.exec_driver_sql("ALTER TABLE audit_events ADD COLUMN decision VARCHAR")
        if "reason" not in existing_columns:
            conn.exec_driver_sql("ALTER TABLE audit_events ADD COLUMN reason TEXT")
        if "hard_blocks" not in existing_columns:
            conn.exec_driver_sql(
                "ALTER TABLE audit_events ADD COLUMN hard_blocks JSON NOT NULL DEFAULT '[]'"
            )
        conn.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_audit_events_artifact "
            "ON audit_events (artifact_type, artifact_id)"
        )
        conn.exec_driver_sql(
            """
            CREATE TRIGGER IF NOT EXISTS trg_audit_events_no_update
            BEFORE UPDATE ON audit_events
            BEGIN
              SELECT RAISE(ABORT, 'audit_events is append-only');
            END
            """
        )
        conn.exec_driver_sql(
            """
            CREATE TRIGGER IF NOT EXISTS trg_audit_events_no_delete
            BEFORE DELETE ON audit_events
            BEGIN
              SELECT RAISE(ABORT, 'audit_events is append-only');
            END
            """
        )


def _ensure_postgres_audit_events_contract(engine: Engine) -> None:
    with engine.begin() as conn:
        for statement in _postgres_audit_events_contract_statements():
            conn.exec_driver_sql(statement)


def _postgres_audit_events_contract_statements() -> tuple[str, ...]:
    return (
        "ALTER TABLE audit_events ADD COLUMN IF NOT EXISTS paper_trade_id VARCHAR",
        "ALTER TABLE audit_events ADD COLUMN IF NOT EXISTS alert_id VARCHAR",
        "ALTER TABLE audit_events ADD COLUMN IF NOT EXISTS decision VARCHAR",
        "ALTER TABLE audit_events ADD COLUMN IF NOT EXISTS reason TEXT",
        (
            "ALTER TABLE audit_events ADD COLUMN IF NOT EXISTS "
            "hard_blocks JSONB NOT NULL DEFAULT '[]'::jsonb"
        ),
        (
            "CREATE INDEX IF NOT EXISTS ix_audit_events_artifact "
            "ON audit_events (artifact_type, artifact_id)"
        ),
        """
        CREATE OR REPLACE FUNCTION reject_audit_events_mutation()
        RETURNS trigger AS $$
        BEGIN
          RAISE EXCEPTION 'audit_events is append-only';
        END;
        $$ LANGUAGE plpgsql
        """,
        "DROP TRIGGER IF EXISTS trg_audit_events_no_update ON audit_events",
        """
        CREATE TRIGGER trg_audit_events_no_update
        BEFORE UPDATE ON audit_events
        FOR EACH ROW
        EXECUTE FUNCTION reject_audit_events_mutation()
        """,
        "DROP TRIGGER IF EXISTS trg_audit_events_no_delete ON audit_events",
        """
        CREATE TRIGGER trg_audit_events_no_delete
        BEFORE DELETE ON audit_events
        FOR EACH ROW
        EXECUTE FUNCTION reject_audit_events_mutation()
        """,
    )


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

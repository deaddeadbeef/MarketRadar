from __future__ import annotations

from pathlib import Path

from sqlalchemy import Engine, create_engine

from catalyst_radar.storage.schema import metadata


def engine_from_url(database_url: str) -> Engine:
    if database_url.startswith("sqlite:///"):
        db_path = Path(database_url.removeprefix("sqlite:///"))
        if str(db_path) != ":memory:":
            db_path.parent.mkdir(parents=True, exist_ok=True)
    return create_engine(database_url, future=True)


def create_schema(engine: Engine) -> None:
    metadata.create_all(engine)

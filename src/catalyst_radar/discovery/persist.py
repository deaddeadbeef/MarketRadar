"""Persist daily discovery briefs so freshness/join can be measured over time."""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

DEFAULT_BRIEF_DIR = Path("data/local/discovery-briefs")


def persist_discovery_brief(
    payload: Mapping[str, Any],
    *,
    dest_dir: str | Path = DEFAULT_BRIEF_DIR,
    clock: datetime | None = None,
) -> dict[str, object]:
    now = clock if clock is not None else datetime.now(tz=UTC)
    if now.tzinfo is None:
        now = now.replace(tzinfo=UTC)
    else:
        now = now.astimezone(UTC)
    directory = Path(dest_dir)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{now.date().isoformat()}.json"
    serializable = dict(payload)
    path.write_text(
        json.dumps(serializable, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    return {
        "schema_version": "discovery-brief-persist-v1",
        "path": str(path),
        "db_writes_made": 0,
        "file_writes_made": 1,
        "generated_at": now.isoformat(),
    }

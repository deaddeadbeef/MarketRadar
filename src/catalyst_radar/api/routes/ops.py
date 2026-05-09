from __future__ import annotations

from collections.abc import Callable
from typing import Any

from fastapi import APIRouter

from catalyst_radar.core.config import AppConfig
from catalyst_radar.dashboard import data as dashboard_data
from catalyst_radar.storage.db import engine_from_url

router = APIRouter(prefix="/api/ops", tags=["ops"])


def _engine():
    return engine_from_url(AppConfig.from_env().database_url)


def _dashboard_helper(name: str) -> Callable[..., Any]:
    try:
        return getattr(dashboard_data, name)
    except AttributeError as exc:
        msg = f"dashboard data helper is unavailable: {name}"
        raise RuntimeError(msg) from exc


@router.get("/health")
def health() -> dict[str, object]:
    load_ops_health = _dashboard_helper("load_ops_health")
    return load_ops_health(_engine())

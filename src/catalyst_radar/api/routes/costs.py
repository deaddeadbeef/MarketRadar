from __future__ import annotations

from collections.abc import Callable
from typing import Any

from fastapi import APIRouter

from catalyst_radar.core.config import AppConfig
from catalyst_radar.dashboard import data as dashboard_data
from catalyst_radar.storage.db import engine_from_url

router = APIRouter(prefix="/api/costs", tags=["costs"])


def _engine():
    return engine_from_url(AppConfig.from_env().database_url)


def _dashboard_helper(name: str) -> Callable[..., Any]:
    try:
        return getattr(dashboard_data, name)
    except AttributeError as exc:
        msg = f"dashboard data helper is unavailable: {name}"
        raise RuntimeError(msg) from exc


@router.get("/summary")
def summary() -> dict[str, object]:
    load_cost_summary = _dashboard_helper("load_cost_summary")
    return load_cost_summary(_engine())

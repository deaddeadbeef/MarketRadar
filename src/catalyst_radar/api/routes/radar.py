from __future__ import annotations

from collections.abc import Callable
from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from catalyst_radar.core.config import AppConfig
from catalyst_radar.dashboard import data as dashboard_data
from catalyst_radar.security.access import Role, require_role
from catalyst_radar.storage.db import engine_from_url

router = APIRouter(prefix="/api/radar", tags=["radar"])


def _engine():
    return engine_from_url(AppConfig.from_env().database_url)


def _dashboard_helper(name: str) -> Callable[..., Any]:
    try:
        return getattr(dashboard_data, name)
    except AttributeError as exc:
        msg = f"dashboard data helper is unavailable: {name}"
        raise RuntimeError(msg) from exc


@router.get("/candidates", dependencies=[Depends(require_role(Role.VIEWER))])
def candidates() -> dict[str, object]:
    load_candidate_rows = _dashboard_helper("load_candidate_rows")
    return {"items": load_candidate_rows(_engine())}


@router.get("/candidates/{ticker}", dependencies=[Depends(require_role(Role.VIEWER))])
def candidate_detail(ticker: str) -> dict[str, object]:
    load_ticker_detail = _dashboard_helper("load_ticker_detail")
    detail = load_ticker_detail(_engine(), ticker.upper())
    if detail is None:
        raise HTTPException(status_code=404, detail="candidate not found")
    return detail

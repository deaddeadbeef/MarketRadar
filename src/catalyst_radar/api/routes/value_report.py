from __future__ import annotations

from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query

from catalyst_radar.core.config import AppConfig
from catalyst_radar.security.access import Role, require_role
from catalyst_radar.storage.db import engine_from_url
from catalyst_radar.validation.value_report import monthly_value_report_payload

router = APIRouter(prefix="/api/value-report", tags=["value-report"])


def _engine():
    return engine_from_url(AppConfig.from_env().database_url)


@router.get("/monthly", dependencies=[Depends(require_role(Role.VIEWER))])
def monthly_value_report(
    month: Annotated[str, Query(pattern=r"^\d{4}-\d{2}$")],
    available_at: Annotated[datetime | None, Query()] = None,
    target_monthly_value_usd: Annotated[float, Query(gt=0)] = 40.0,
    min_useful_evidence_count: Annotated[int, Query(ge=1)] = 2,
) -> dict[str, object]:
    try:
        return monthly_value_report_payload(
            _engine(),
            month=month,
            available_at=available_at,
            target_monthly_value_usd=target_monthly_value_usd,
            min_useful_evidence_count=min_useful_evidence_count,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

from __future__ import annotations

from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, ConfigDict

from catalyst_radar.core.config import AppConfig
from catalyst_radar.security.access import Role, require_role
from catalyst_radar.storage.db import engine_from_url
from catalyst_radar.validation.value_outcomes import (
    load_value_outcomes_payload,
    value_outcome_update_payload,
)

router = APIRouter(prefix="/api/value-outcomes", tags=["value-outcomes"])


class ValueOutcomeUpdateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    value_ledger_entry_id: str
    outcome_available_at: datetime
    sector_etf_ticker: str | None = None
    invalidation_price: float | None = None
    execute: bool = False


def _engine():
    return engine_from_url(AppConfig.from_env().database_url)


@router.get("", dependencies=[Depends(require_role(Role.VIEWER))])
def value_outcomes(
    value_ledger_entry_id: Annotated[str | None, Query()] = None,
    available_at: Annotated[datetime | None, Query()] = None,
    ticker: Annotated[str | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=1000)] = 200,
) -> dict[str, object]:
    return load_value_outcomes_payload(
        _engine(),
        value_ledger_entry_id=value_ledger_entry_id,
        available_at=available_at,
        ticker=ticker,
        limit=limit,
    )


@router.post("/update", dependencies=[Depends(require_role(Role.ANALYST))])
def value_outcome_update(request: ValueOutcomeUpdateRequest) -> dict[str, object]:
    try:
        return value_outcome_update_payload(
            _engine(),
            value_ledger_entry_id=request.value_ledger_entry_id,
            outcome_available_at=request.outcome_available_at,
            sector_etf_ticker=request.sector_etf_ticker,
            invalidation_price=request.invalidation_price,
            execute=request.execute,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


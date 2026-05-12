from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import create_engine

from catalyst_radar.brokers.rate_limit import (
    SCHWAB_PORTFOLIO_SYNC_OPERATION,
    SchwabRateLimitExceeded,
    acquire_schwab_api_slot,
    schwab_rate_limit_status,
)
from catalyst_radar.core.config import AppConfig
from catalyst_radar.storage.db import create_schema


def test_schwab_rate_limit_blocks_repeated_attempts_until_reset() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    create_schema(engine)
    now = datetime(2026, 5, 13, 12, 0, tzinfo=UTC)

    first = acquire_schwab_api_slot(
        engine,
        operation=SCHWAB_PORTFOLIO_SYNC_OPERATION,
        min_interval_seconds=60,
        now=now,
    )

    assert first.allowed is True
    assert first.retry_after_seconds == 0
    with pytest.raises(SchwabRateLimitExceeded) as exc:
        acquire_schwab_api_slot(
            engine,
            operation=SCHWAB_PORTFOLIO_SYNC_OPERATION,
            min_interval_seconds=60,
            now=now,
        )
    assert exc.value.state.retry_after_seconds == 60


def test_schwab_rate_limit_status_reports_ready_and_blocked_operations() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    create_schema(engine)
    now = datetime(2026, 5, 13, 12, 0, tzinfo=UTC)
    config = AppConfig.from_env(
        {
            "SCHWAB_SYNC_MIN_INTERVAL_SECONDS": "60",
            "SCHWAB_MARKET_SYNC_MIN_INTERVAL_SECONDS": "30",
        }
    )
    acquire_schwab_api_slot(
        engine,
        operation=SCHWAB_PORTFOLIO_SYNC_OPERATION,
        min_interval_seconds=60,
        now=now,
    )

    rows = schwab_rate_limit_status(engine, config=config, now=now)

    by_operation = {str(row["operation"]): row for row in rows}
    assert by_operation["portfolio_sync"]["allowed"] is False
    assert by_operation["portfolio_sync"]["retry_after_seconds"] == 60
    assert by_operation["market_context_sync"]["allowed"] is True

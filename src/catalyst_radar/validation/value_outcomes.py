from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any

from sqlalchemy import Engine, select

from catalyst_radar.core.immutability import thaw_json_value
from catalyst_radar.storage.schema import daily_bars
from catalyst_radar.storage.validation_repositories import ValidationRepository
from catalyst_radar.validation.models import (
    ValueLedgerEntry,
    ValueOutcome,
    value_outcome_id,
)

HORIZONS = (5, 10, 20, 60)


@dataclass(frozen=True)
class _BarPoint:
    date: date
    open: float
    close: float
    high: float
    low: float


def value_outcome_update_payload(
    engine: Engine,
    *,
    value_ledger_entry_id: str,
    outcome_available_at: datetime,
    execute: bool = False,
    sector_etf_ticker: str | None = None,
    invalidation_price: float | None = None,
) -> dict[str, object]:
    repo = ValidationRepository(engine)
    entry = repo.value_ledger_entry(value_ledger_entry_id)
    if entry is None:
        msg = "value ledger entry not found"
        raise ValueError(msg)
    outcome = compute_value_outcome(
        engine,
        entry,
        outcome_available_at=outcome_available_at,
        sector_etf_ticker=sector_etf_ticker,
        invalidation_price=invalidation_price,
    )
    if execute:
        repo.upsert_value_outcome(outcome)
    preview_command = _value_outcome_update_command(
        value_ledger_entry_id=entry.id,
        outcome_available_at=outcome.outcome_available_at,
        sector_etf_ticker=sector_etf_ticker,
        invalidation_price=invalidation_price,
        execute=False,
    )
    execute_command = _value_outcome_update_command(
        value_ledger_entry_id=entry.id,
        outcome_available_at=outcome.outcome_available_at,
        sector_etf_ticker=sector_etf_ticker,
        invalidation_price=invalidation_price,
        execute=True,
    )
    return {
        "schema_version": "value-outcome-update-v1",
        "mode": "executed" if execute else "preview",
        "external_calls_required": 0,
        "external_calls_made": 0,
        "db_writes_required": 1,
        "db_writes_made": 1 if execute else 0,
        "preview_command": preview_command,
        "execute_command": execute_command if not execute else None,
        "api": "POST /api/value-outcomes/update",
        "api_preview_request_body": _value_outcome_update_request_body(
            value_ledger_entry_id=entry.id,
            outcome_available_at=outcome.outcome_available_at,
            sector_etf_ticker=sector_etf_ticker,
            invalidation_price=invalidation_price,
            execute=False,
        ),
        "api_execute_request_body": (
            _value_outcome_update_request_body(
                value_ledger_entry_id=entry.id,
                outcome_available_at=outcome.outcome_available_at,
                sector_etf_ticker=sector_etf_ticker,
                invalidation_price=invalidation_price,
                execute=True,
            )
            if not execute
            else None
        ),
        "outcome": value_outcome_payload(outcome),
        "next_action": (
            "Value outcome saved."
            if execute
            else "Preview only. Re-run with --execute to write this outcome row."
        ),
    }


def _value_outcome_update_command(
    *,
    value_ledger_entry_id: str,
    outcome_available_at: datetime,
    sector_etf_ticker: str | None,
    invalidation_price: float | None,
    execute: bool,
) -> str:
    parts = ["catalyst-radar", "value-outcome", "update"]
    _append_command_option(parts, "--ledger-id", value_ledger_entry_id)
    _append_command_option(
        parts,
        "--outcome-available-at",
        outcome_available_at.isoformat(),
    )
    _append_command_option(parts, "--sector-etf", sector_etf_ticker)
    _append_command_option(parts, "--invalidation-price", invalidation_price)
    parts.append("--execute" if execute else "--preview")
    parts.append("--json")
    return " ".join(parts)


def _value_outcome_update_request_body(
    *,
    value_ledger_entry_id: str,
    outcome_available_at: datetime,
    sector_etf_ticker: str | None,
    invalidation_price: float | None,
    execute: bool,
) -> dict[str, object]:
    return {
        "value_ledger_entry_id": value_ledger_entry_id,
        "outcome_available_at": outcome_available_at.isoformat(),
        "sector_etf_ticker": sector_etf_ticker,
        "invalidation_price": invalidation_price,
        "execute": execute,
    }


def compute_value_outcome(
    engine: Engine,
    entry: ValueLedgerEntry,
    *,
    outcome_available_at: datetime,
    sector_etf_ticker: str | None = None,
    invalidation_price: float | None = None,
) -> ValueOutcome:
    cutoff = _to_utc_datetime(outcome_available_at, "outcome_available_at")
    now = datetime.now(UTC)
    ticker = _required_text(entry.ticker, "ticker").upper() if entry.ticker else None
    as_of = entry.as_of
    if ticker is None or as_of is None:
        return _empty_outcome(
            entry,
            outcome_available_at=cutoff,
            status="missing_context",
            payload={"reason": "ticker_or_as_of_missing"},
            created_at=now,
        )

    entry_bar = _entry_bar(
        engine,
        ticker=ticker,
        as_of=as_of,
        available_at=entry.available_at,
    )
    if entry_bar is None:
        return _empty_outcome(
            entry,
            outcome_available_at=cutoff,
            status="missing_entry_price",
            payload={"reason": "entry_bar_missing"},
            created_at=now,
        )

    future = _future_bars(
        engine,
        ticker=ticker,
        after=as_of,
        available_at=cutoff,
        limit=max(HORIZONS),
    )
    spy_entry = _entry_bar(engine, ticker="SPY", as_of=as_of, available_at=entry.available_at)
    spy_future = _future_bars(
        engine,
        ticker="SPY",
        after=as_of,
        available_at=cutoff,
        limit=max(HORIZONS),
    )
    sector_ticker = (
        sector_etf_ticker.strip().upper()
        if sector_etf_ticker is not None and sector_etf_ticker.strip()
        else None
    )
    sector_entry = (
        _entry_bar(
            engine,
            ticker=sector_ticker,
            as_of=as_of,
            available_at=entry.available_at,
        )
        if sector_ticker
        else None
    )
    sector_future = (
        _future_bars(
            engine,
            ticker=sector_ticker,
            after=as_of,
            available_at=cutoff,
            limit=max(HORIZONS),
        )
        if sector_ticker
        else []
    )

    returns = _horizon_returns(entry_bar.close, future)
    spy_returns = (
        _horizon_returns(spy_entry.close, spy_future) if spy_entry is not None else {}
    )
    sector_returns = (
        _horizon_returns(sector_entry.close, sector_future)
        if sector_entry is not None
        else {}
    )
    resolved_invalidation = _resolved_invalidation_price(entry, invalidation_price)
    expected_review_horizon_days = max(HORIZONS)
    expected_review_horizon_expired = len(future) >= expected_review_horizon_days
    setup_follow_through = _setup_follow_through_status(entry, returns)
    expected_direction = _expected_direction(entry)
    gap_outcome, gap_return = _gap_outcome(entry_bar.close, future)
    status = "computed" if expected_review_horizon_expired else "insufficient_data"
    return ValueOutcome(
        id=value_outcome_id(
            value_ledger_entry_id=entry.id,
            outcome_available_at=cutoff,
        ),
        value_ledger_entry_id=entry.id,
        ticker=ticker,
        as_of=as_of,
        outcome_available_at=cutoff,
        status=status,
        entry_price=entry_bar.close,
        trading_days_observed=len(future),
        return_5d=returns.get(5),
        return_10d=returns.get(10),
        return_20d=returns.get(20),
        return_60d=returns.get(60),
        spy_return_5d=spy_returns.get(5),
        spy_return_10d=spy_returns.get(10),
        spy_return_20d=spy_returns.get(20),
        spy_return_60d=spy_returns.get(60),
        spy_relative_return_5d=_relative(returns.get(5), spy_returns.get(5)),
        spy_relative_return_10d=_relative(returns.get(10), spy_returns.get(10)),
        spy_relative_return_20d=_relative(returns.get(20), spy_returns.get(20)),
        spy_relative_return_60d=_relative(returns.get(60), spy_returns.get(60)),
        sector_etf_ticker=sector_ticker,
        sector_return_5d=sector_returns.get(5),
        sector_return_10d=sector_returns.get(10),
        sector_return_20d=sector_returns.get(20),
        sector_return_60d=sector_returns.get(60),
        sector_relative_return_5d=_relative(returns.get(5), sector_returns.get(5)),
        sector_relative_return_10d=_relative(returns.get(10), sector_returns.get(10)),
        sector_relative_return_20d=_relative(returns.get(20), sector_returns.get(20)),
        sector_relative_return_60d=_relative(returns.get(60), sector_returns.get(60)),
        max_adverse_excursion=_min_return(entry_bar.close, future),
        max_favorable_excursion=_max_return(entry_bar.close, future),
        invalidation_price=resolved_invalidation,
        invalidation_touched=_invalidation_touched(
            future,
            resolved_invalidation,
            direction=expected_direction,
        ),
        payload={
            "horizons": list(HORIZONS),
            "missing_horizons": [horizon for horizon in HORIZONS if len(future) < horizon],
            "expected_review_horizon_days": expected_review_horizon_days,
            "expected_review_horizon_expired": expected_review_horizon_expired,
            "setup_follow_through": setup_follow_through,
            "setup_follow_through_horizon_days": 20,
            "setup_follow_through_direction": expected_direction,
            "invalidation_touch_direction": expected_direction or "long_stop",
            "gap_outcome": gap_outcome,
            "gap_return": gap_return,
            "spy_available": spy_entry is not None and bool(spy_future),
            "sector_available": sector_entry is not None and bool(sector_future),
            "no_future_leakage": True,
        },
        created_at=now,
        updated_at=now,
    )


def load_value_outcomes_payload(
    engine: Engine,
    *,
    value_ledger_entry_id: str | None = None,
    available_at: datetime | None = None,
    ticker: str | None = None,
    limit: int = 200,
) -> dict[str, object]:
    cutoff = _to_utc_datetime(available_at, "available_at") if available_at else None
    rows = ValidationRepository(engine).list_value_outcomes(
        value_ledger_entry_id=value_ledger_entry_id,
        available_at=cutoff,
        ticker=ticker,
        limit=limit,
    )
    return {
        "schema_version": "value-outcomes-v1",
        "external_calls_made": 0,
        "db_writes_made": 0,
        "available_at": cutoff.isoformat() if cutoff is not None else None,
        "count": len(rows),
        "status_counts": _status_counts(rows),
        "outcomes": [value_outcome_payload(row) for row in rows],
    }


def load_value_outcome_coverage_payload(
    engine: Engine,
    *,
    available_at: datetime | None = None,
    period_start: date | None = None,
    period_end: date | None = None,
    limit: int = 200,
) -> dict[str, object]:
    cutoff = _to_utc_datetime(available_at or datetime.now(UTC), "available_at")
    resolved_start, resolved_end = _resolved_period(
        cutoff.date(),
        period_start=period_start,
        period_end=period_end,
    )
    repo = ValidationRepository(engine)
    entries = repo.list_value_ledger_entries(
        available_at=cutoff,
        period_start=resolved_start,
        period_end=resolved_end,
        limit=10_000,
    )
    outcomes = repo.list_value_outcomes(available_at=cutoff, limit=10_000)
    latest_outcome_by_ledger = _latest_outcomes_by_ledger(outcomes)
    rows = [
        _value_outcome_coverage_row(
            entry,
            latest_outcome_by_ledger.get(entry.id),
            cutoff,
        )
        for entry in entries
    ]
    missing = [row for row in rows if row["outcome_status"] == "missing"]
    first_missing = missing[0] if missing else None
    linked = len(rows) - len(missing)
    status_counts = Counter(str(row["outcome_status"]) for row in rows)
    coverage_pct = round((linked / len(rows)) * 100, 2) if rows else None
    status = _value_outcome_coverage_status(
        ledger_entry_count=len(rows),
        missing_count=len(missing),
        status_counts=status_counts,
    )
    return {
        "schema_version": "value-outcome-coverage-v1",
        "status": status,
        "external_calls_made": 0,
        "db_writes_made": 0,
        "available_at": cutoff.isoformat(),
        "period_start": resolved_start.isoformat(),
        "period_end": resolved_end.isoformat(),
        "ledger_entry_count": len(rows),
        "linked_outcome_count": linked,
        "missing_outcome_count": len(missing),
        "first_missing_value_ledger_entry_id": _first_missing_outcome_field(
            first_missing,
            "value_ledger_entry_id",
        ),
        "first_missing_ticker": _first_missing_outcome_field(first_missing, "ticker"),
        "canonical_next_command": _first_missing_outcome_field(
            first_missing,
            "preview_update_command",
        ),
        "computed_outcome_count": int(status_counts.get("computed") or 0),
        "insufficient_data_count": int(status_counts.get("insufficient_data") or 0),
        "outcome_status_counts": dict(sorted(status_counts.items())),
        "coverage_pct": coverage_pct,
        "rows": rows[: max(1, int(limit))],
        "next_action": _value_outcome_coverage_next_action(status),
    }


def load_value_outcome_payload(
    engine: Engine,
    *,
    outcome_id: str,
) -> dict[str, object]:
    resolved_id = _required_text(outcome_id, "outcome_id")
    outcome = ValidationRepository(engine).value_outcome(resolved_id)
    if outcome is None:
        msg = f"value outcome not found: {resolved_id}"
        raise ValueError(msg)
    return {
        "schema_version": "value-outcome-v1",
        "external_calls_made": 0,
        "db_writes_made": 0,
        "outcome": value_outcome_payload(outcome),
    }


def value_outcome_summary_payload(engine: Engine) -> dict[str, object]:
    rows = ValidationRepository(engine).list_value_outcomes(limit=1000)
    return {
        "schema_version": "value-outcome-summary-v1",
        "external_calls_made": 0,
        "db_writes_made": 0,
        "outcome_count": len(rows),
        "status_counts": _status_counts(rows),
    }


def value_outcome_payload(outcome: ValueOutcome) -> dict[str, object]:
    payload = thaw_json_value(outcome.payload)
    if not isinstance(payload, Mapping):
        payload = {}
    return {
        "id": outcome.id,
        "value_ledger_entry_id": outcome.value_ledger_entry_id,
        "ticker": outcome.ticker,
        "as_of": outcome.as_of.isoformat(),
        "outcome_available_at": outcome.outcome_available_at.isoformat(),
        "status": outcome.status,
        "entry_price": outcome.entry_price,
        "trading_days_observed": outcome.trading_days_observed,
        "return_5d": outcome.return_5d,
        "return_10d": outcome.return_10d,
        "return_20d": outcome.return_20d,
        "return_60d": outcome.return_60d,
        "spy_return_5d": outcome.spy_return_5d,
        "spy_return_10d": outcome.spy_return_10d,
        "spy_return_20d": outcome.spy_return_20d,
        "spy_return_60d": outcome.spy_return_60d,
        "spy_relative_return_5d": outcome.spy_relative_return_5d,
        "spy_relative_return_10d": outcome.spy_relative_return_10d,
        "spy_relative_return_20d": outcome.spy_relative_return_20d,
        "spy_relative_return_60d": outcome.spy_relative_return_60d,
        "sector_etf_ticker": outcome.sector_etf_ticker,
        "sector_return_5d": outcome.sector_return_5d,
        "sector_return_10d": outcome.sector_return_10d,
        "sector_return_20d": outcome.sector_return_20d,
        "sector_return_60d": outcome.sector_return_60d,
        "sector_relative_return_5d": outcome.sector_relative_return_5d,
        "sector_relative_return_10d": outcome.sector_relative_return_10d,
        "sector_relative_return_20d": outcome.sector_relative_return_20d,
        "sector_relative_return_60d": outcome.sector_relative_return_60d,
        "max_adverse_excursion": outcome.max_adverse_excursion,
        "max_favorable_excursion": outcome.max_favorable_excursion,
        "invalidation_price": outcome.invalidation_price,
        "invalidation_touched": outcome.invalidation_touched,
        "setup_follow_through": payload.get("setup_follow_through"),
        "setup_follow_through_horizon_days": payload.get(
            "setup_follow_through_horizon_days"
        ),
        "setup_follow_through_direction": payload.get(
            "setup_follow_through_direction"
        ),
        "gap_outcome": payload.get("gap_outcome"),
        "gap_return": payload.get("gap_return"),
        "payload": payload,
        "created_at": outcome.created_at.isoformat(),
        "updated_at": outcome.updated_at.isoformat(),
    }


def _empty_outcome(
    entry: ValueLedgerEntry,
    *,
    outcome_available_at: datetime,
    status: str,
    payload: Mapping[str, object],
    created_at: datetime,
) -> ValueOutcome:
    return ValueOutcome(
        id=value_outcome_id(
            value_ledger_entry_id=entry.id,
            outcome_available_at=outcome_available_at,
        ),
        value_ledger_entry_id=entry.id,
        ticker=(entry.ticker or "UNKNOWN"),
        as_of=entry.as_of or entry.entry_date,
        outcome_available_at=outcome_available_at,
        status=status,
        trading_days_observed=0,
        payload=payload,
        created_at=created_at,
        updated_at=created_at,
    )


def _entry_bar(
    engine: Engine,
    *,
    ticker: str,
    as_of: date,
    available_at: datetime,
) -> _BarPoint | None:
    stmt = (
        select(daily_bars)
        .where(
            daily_bars.c.ticker == ticker.upper(),
            daily_bars.c.date <= as_of,
            daily_bars.c.available_at <= available_at,
        )
        .order_by(daily_bars.c.date.desc())
        .limit(1)
    )
    with engine.connect() as conn:
        row = conn.execute(stmt).first()
    return _bar_point(row._mapping) if row is not None else None


def _future_bars(
    engine: Engine,
    *,
    ticker: str,
    after: date,
    available_at: datetime,
    limit: int,
) -> list[_BarPoint]:
    stmt = (
        select(daily_bars)
        .where(
            daily_bars.c.ticker == ticker.upper(),
            daily_bars.c.date > after,
            daily_bars.c.available_at <= available_at,
        )
        .order_by(daily_bars.c.date)
        .limit(limit)
    )
    with engine.connect() as conn:
        return [_bar_point(row._mapping) for row in conn.execute(stmt)]


def _bar_point(row: Mapping[str, Any]) -> _BarPoint:
    return _BarPoint(
        date=row["date"],
        open=float(row["open"]),
        close=float(row["close"]),
        high=float(row["high"]),
        low=float(row["low"]),
    )


def _horizon_returns(entry_price: float, rows: list[_BarPoint]) -> dict[int, float]:
    return {
        horizon: (rows[horizon - 1].close / entry_price) - 1
        for horizon in HORIZONS
        if len(rows) >= horizon
    }


def _min_return(entry_price: float, rows: list[_BarPoint]) -> float | None:
    if not rows:
        return None
    return (min(row.low for row in rows) / entry_price) - 1


def _max_return(entry_price: float, rows: list[_BarPoint]) -> float | None:
    if not rows:
        return None
    return (max(row.high for row in rows) / entry_price) - 1


def _relative(primary: float | None, benchmark: float | None) -> float | None:
    if primary is None or benchmark is None:
        return None
    return primary - benchmark


def _expected_direction(entry: ValueLedgerEntry) -> str | None:
    direction = str(entry.priced_in_direction or "").strip().lower()
    if direction in {"bullish", "bearish"}:
        return direction
    status = str(entry.priced_in_status or "").strip().lower()
    if status.startswith("bullish"):
        return "bullish"
    if status.startswith("bearish"):
        return "bearish"
    return None


def _setup_follow_through_status(
    entry: ValueLedgerEntry,
    returns: Mapping[int, float],
) -> str:
    direction = _expected_direction(entry)
    if direction is None:
        return "not_applicable"
    return_20d = returns.get(20)
    if return_20d is None:
        return "insufficient_data"
    if direction == "bullish":
        return "followed_through" if return_20d > 0 else "failed"
    return "followed_through" if return_20d < 0 else "failed"


def _gap_outcome(
    entry_close: float,
    future: list[_BarPoint],
) -> tuple[str, float | None]:
    if not future:
        return "unavailable", None
    gap_return = (future[0].open / entry_close) - 1
    if gap_return > 0:
        return "gap_up", gap_return
    if gap_return < 0:
        return "gap_down", gap_return
    return "gap_flat", gap_return


def _resolved_invalidation_price(
    entry: ValueLedgerEntry,
    invalidation_price: float | None,
) -> float | None:
    if invalidation_price is not None:
        return float(invalidation_price)
    payload = thaw_json_value(entry.payload)
    if isinstance(payload, Mapping) and payload.get("invalidation_price") is not None:
        return float(payload["invalidation_price"])
    return None


def _invalidation_touched(
    rows: list[_BarPoint],
    invalidation_price: float | None,
    *,
    direction: str | None,
) -> bool:
    if invalidation_price is None:
        return False
    if direction == "bearish":
        return any(row.high >= invalidation_price for row in rows)
    return any(row.low <= invalidation_price for row in rows)


def _status_counts(rows: list[ValueOutcome]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        counts[row.status] = counts.get(row.status, 0) + 1
    return dict(sorted(counts.items()))


def _latest_outcomes_by_ledger(
    outcomes: list[ValueOutcome],
) -> dict[str, ValueOutcome]:
    latest: dict[str, ValueOutcome] = {}
    for outcome in outcomes:
        latest.setdefault(outcome.value_ledger_entry_id, outcome)
    return latest


def _value_outcome_coverage_row(
    entry: ValueLedgerEntry,
    outcome: ValueOutcome | None,
    outcome_available_at: datetime,
) -> dict[str, object]:
    outcome_payload = value_outcome_payload(outcome) if outcome is not None else {}
    row = {
        "value_ledger_entry_id": entry.id,
        "ticker": entry.ticker,
        "entry_date": entry.entry_date.isoformat(),
        "as_of": entry.as_of.isoformat() if entry.as_of is not None else None,
        "label": entry.label,
        "supported_action": entry.supported_action,
        "user_decision": entry.user_decision,
        "outcome_status": outcome.status if outcome is not None else "missing",
        "value_outcome_id": outcome.id if outcome is not None else None,
        "outcome_available_at": (
            outcome.outcome_available_at.isoformat() if outcome is not None else None
        ),
        "trading_days_observed": (
            outcome.trading_days_observed if outcome is not None else None
        ),
        "return_5d": outcome.return_5d if outcome is not None else None,
        "return_20d": outcome.return_20d if outcome is not None else None,
        "spy_relative_return_20d": (
            outcome.spy_relative_return_20d if outcome is not None else None
        ),
        "setup_follow_through": outcome_payload.get("setup_follow_through"),
        "gap_outcome": outcome_payload.get("gap_outcome"),
        "preview_update_command": None,
    }
    if outcome is None:
        row["preview_update_command"] = (
            "catalyst-radar value-outcome update "
            f"--ledger-id {entry.id} "
            f"--outcome-available-at {outcome_available_at.isoformat()} "
            "--preview --json"
        )
    return row


def _first_missing_outcome_field(
    row: Mapping[str, object] | None,
    field: str,
) -> str | None:
    if row is None:
        return None
    value = row.get(field)
    if isinstance(value, str) and value.strip():
        return value
    return None


def _value_outcome_coverage_status(
    *,
    ledger_entry_count: int,
    missing_count: int,
    status_counts: Counter[str],
) -> str:
    if ledger_entry_count <= 0:
        return "no_ledger_entries"
    if missing_count:
        return "gaps"
    if any(status != "computed" for status in status_counts):
        return "incomplete"
    return "ready"


def _value_outcome_coverage_next_action(status: str) -> str:
    if status == "no_ledger_entries":
        return (
            "Record value-ledger entries for surfaced candidates before outcome "
            "coverage can be measured."
        )
    if status == "gaps":
        return (
            "Preview or record missing value outcomes before claiming monthly "
            "value evidence."
        )
    if status == "incomplete":
        return (
            "Review insufficient or missing-context outcomes before treating the "
            "month as measured."
        )
    return "All value-ledger entries in this period have computed value outcomes."


def _resolved_period(
    reference_date: date,
    *,
    period_start: date | None,
    period_end: date | None,
) -> tuple[date, date]:
    start = period_start or reference_date.replace(day=1)
    end = period_end or _month_end(start)
    if end < start:
        msg = "period_end must be greater than or equal to period_start"
        raise ValueError(msg)
    return start, end


def _month_end(start: date) -> date:
    if start.month == 12:
        return start.replace(year=start.year + 1, month=1, day=1) - timedelta(days=1)
    return start.replace(month=start.month + 1, day=1) - timedelta(days=1)


def _append_command_option(
    parts: list[str],
    flag: str,
    value: object | None,
) -> None:
    if value is None:
        return
    text = str(value).strip()
    if not text:
        return
    parts.extend([flag, _command_arg(text)])


def _command_arg(value: str) -> str:
    safe_chars = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_./:@+-")
    if value and all(char in safe_chars for char in value):
        return value
    return "'" + value.replace("'", "''") + "'"


def _required_text(value: object, field_name: str) -> str:
    text = str(value).strip()
    if not text:
        msg = f"{field_name} must not be blank"
        raise ValueError(msg)
    return text


def _to_utc_datetime(value: datetime, field_name: str) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if not isinstance(value, datetime):
        msg = f"{field_name} must be a datetime"
        raise TypeError(msg)
    if value.tzinfo is None or value.utcoffset() is None:
        msg = f"{field_name} must be timezone-aware"
        raise ValueError(msg)
    return value.astimezone(UTC)

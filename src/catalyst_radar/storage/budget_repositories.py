from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import Engine, delete, func, insert, select

from catalyst_radar.agents.models import (
    BudgetLedgerEntry,
    LLMCallStatus,
    LLMTaskName,
    TokenUsage,
)
from catalyst_radar.core.immutability import thaw_json_value
from catalyst_radar.storage.schema import budget_ledger


class BudgetLedgerRepository:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def upsert_entry(self, entry: BudgetLedgerEntry) -> None:
        with self.engine.begin() as conn:
            conn.execute(delete(budget_ledger).where(budget_ledger.c.id == entry.id))
            conn.execute(insert(budget_ledger).values(**_entry_row(entry)))

    def list_entries(
        self,
        *,
        available_at: datetime | None = None,
        ticker: str | None = None,
        task: str | None = None,
        status: str | None = None,
        limit: int = 200,
    ) -> list[BudgetLedgerEntry]:
        filters = _entry_filters(
            available_at=available_at,
            ticker=ticker,
            task=task,
            status=status,
        )
        stmt = (
            select(budget_ledger)
            .where(*filters)
            .order_by(
                budget_ledger.c.available_at.desc(),
                budget_ledger.c.created_at.desc(),
                budget_ledger.c.id.desc(),
            )
            .limit(_positive_limit(limit))
        )
        with self.engine.connect() as conn:
            return [_entry_from_row(row._mapping) for row in conn.execute(stmt)]

    def spend_between(
        self,
        *,
        start: datetime,
        end: datetime,
        statuses: Sequence[LLMCallStatus] = (LLMCallStatus.COMPLETED,),
    ) -> float:
        stmt = select(func.coalesce(func.sum(budget_ledger.c.actual_cost), 0.0)).where(
            budget_ledger.c.ts >= _to_utc_datetime(start, "start"),
            budget_ledger.c.ts < _to_utc_datetime(end, "end"),
            budget_ledger.c.status.in_([LLMCallStatus(status).value for status in statuses]),
        )
        with self.engine.connect() as conn:
            return float(conn.execute(stmt).scalar_one())

    def task_count_between(
        self,
        *,
        task: str,
        start: datetime,
        end: datetime,
        statuses: Sequence[LLMCallStatus] = (
            LLMCallStatus.COMPLETED,
            LLMCallStatus.DRY_RUN,
            LLMCallStatus.PLANNED,
        ),
    ) -> int:
        stmt = select(func.count()).select_from(budget_ledger).where(
            budget_ledger.c.task == LLMTaskName(task).value,
            budget_ledger.c.ts >= _to_utc_datetime(start, "start"),
            budget_ledger.c.ts < _to_utc_datetime(end, "end"),
            budget_ledger.c.status.in_([LLMCallStatus(status).value for status in statuses]),
        )
        with self.engine.connect() as conn:
            return int(conn.execute(stmt).scalar_one())

    def summary(
        self,
        *,
        available_at: datetime | None = None,
        limit: int = 200,
    ) -> dict[str, object]:
        entries = self.list_entries(available_at=available_at, limit=limit)
        status_counts: dict[str, int] = {}
        by_task: dict[str, dict[str, object]] = {}
        by_model: dict[str, dict[str, object]] = {}

        for entry in entries:
            status_counts[entry.status.value] = status_counts.get(entry.status.value, 0) + 1
            _add_group_totals(by_task, entry.task.value, "task", entry)
            _add_group_totals(by_model, entry.model or "none", "model", entry)

        return {
            "currency": "USD",
            "total_estimated_cost_usd": round(
                sum(entry.estimated_cost for entry in entries),
                10,
            ),
            "total_actual_cost_usd": round(
                sum(entry.actual_cost for entry in entries),
                10,
            ),
            "attempt_count": len(entries),
            "status_counts": status_counts,
            "by_task": sorted(
                by_task.values(),
                key=lambda row: (-float(row["actual_cost_usd"]), str(row["task"])),
            ),
            "by_model": sorted(
                by_model.values(),
                key=lambda row: (-float(row["actual_cost_usd"]), str(row["model"])),
            ),
            "rows": [_entry_summary_row(entry) for entry in entries],
        }


def _entry_row(entry: BudgetLedgerEntry) -> dict[str, Any]:
    return {
        "id": entry.id,
        "ts": entry.ts,
        "available_at": entry.available_at,
        "ticker": entry.ticker,
        "candidate_state_id": entry.candidate_state_id,
        "candidate_packet_id": entry.candidate_packet_id,
        "decision_card_id": entry.decision_card_id,
        "task": entry.task.value,
        "model": entry.model,
        "provider": entry.provider,
        "status": entry.status.value,
        "skip_reason": entry.skip_reason.value if entry.skip_reason else None,
        "input_tokens": entry.token_usage.input_tokens,
        "cached_input_tokens": entry.token_usage.cached_input_tokens,
        "output_tokens": entry.token_usage.output_tokens,
        "tool_calls": thaw_json_value(entry.tool_calls),
        "estimated_cost": entry.estimated_cost,
        "actual_cost": entry.actual_cost,
        "currency": entry.currency,
        "candidate_state": entry.candidate_state,
        "prompt_version": entry.prompt_version,
        "schema_version": entry.schema_version,
        "outcome_label": entry.outcome_label,
        "payload": thaw_json_value(entry.payload),
        "created_at": entry.created_at,
    }


def _entry_from_row(row: Any) -> BudgetLedgerEntry:
    return BudgetLedgerEntry(
        id=row["id"],
        ts=_as_datetime(row["ts"]),
        available_at=_as_datetime(row["available_at"]),
        ticker=row["ticker"],
        candidate_state_id=row["candidate_state_id"],
        candidate_packet_id=row["candidate_packet_id"],
        decision_card_id=row["decision_card_id"],
        task=row["task"],
        model=row["model"],
        provider=row["provider"],
        status=row["status"],
        skip_reason=row["skip_reason"],
        token_usage=TokenUsage(
            input_tokens=row["input_tokens"],
            cached_input_tokens=row["cached_input_tokens"],
            output_tokens=row["output_tokens"],
        ),
        tool_calls=row["tool_calls"],
        estimated_cost=row["estimated_cost"],
        actual_cost=row["actual_cost"],
        currency=row["currency"],
        candidate_state=row["candidate_state"],
        prompt_version=row["prompt_version"],
        schema_version=row["schema_version"],
        outcome_label=row["outcome_label"],
        payload=row["payload"],
        created_at=_as_datetime(row["created_at"]),
    )


def _entry_filters(
    *,
    available_at: datetime | None,
    ticker: str | None,
    task: str | None,
    status: str | None,
) -> list[Any]:
    filters = []
    if available_at is not None:
        filters.append(
            budget_ledger.c.available_at <= _to_utc_datetime(available_at, "available_at")
        )
    if ticker is not None and ticker.strip():
        filters.append(budget_ledger.c.ticker == ticker.upper())
    if task is not None and task.strip():
        filters.append(budget_ledger.c.task == LLMTaskName(task).value)
    if status is not None and status.strip():
        filters.append(budget_ledger.c.status == LLMCallStatus(status).value)
    return filters


def _add_group_totals(
    groups: dict[str, dict[str, object]],
    key: str,
    label: str,
    entry: BudgetLedgerEntry,
) -> None:
    row = groups.setdefault(
        key,
        {
            label: key,
            "estimated_cost_usd": 0.0,
            "actual_cost_usd": 0.0,
            "attempt_count": 0,
        },
    )
    row["estimated_cost_usd"] = round(
        float(row["estimated_cost_usd"]) + entry.estimated_cost,
        10,
    )
    row["actual_cost_usd"] = round(
        float(row["actual_cost_usd"]) + entry.actual_cost,
        10,
    )
    row["attempt_count"] = int(row["attempt_count"]) + 1


def _entry_summary_row(entry: BudgetLedgerEntry) -> dict[str, object]:
    return {
        "id": entry.id,
        "ts": entry.ts.isoformat(),
        "available_at": entry.available_at.isoformat(),
        "ticker": entry.ticker,
        "task": entry.task.value,
        "model": entry.model,
        "provider": entry.provider,
        "status": entry.status.value,
        "skip_reason": entry.skip_reason.value if entry.skip_reason else None,
        "input_tokens": entry.token_usage.input_tokens,
        "cached_input_tokens": entry.token_usage.cached_input_tokens,
        "output_tokens": entry.token_usage.output_tokens,
        "estimated_cost_usd": entry.estimated_cost,
        "actual_cost_usd": entry.actual_cost,
        "currency": entry.currency,
        "candidate_state": entry.candidate_state,
        "prompt_version": entry.prompt_version,
        "schema_version": entry.schema_version,
        "outcome_label": entry.outcome_label,
    }


def _positive_limit(value: int) -> int:
    return max(1, int(value))


def _to_utc_datetime(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        msg = f"{field_name} must be a datetime"
        raise TypeError(msg)
    if value.tzinfo is None or value.utcoffset() is None:
        msg = f"{field_name} must be timezone-aware"
        raise ValueError(msg)
    return value.astimezone(UTC)


def _as_datetime(value: datetime | str) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


__all__ = ["BudgetLedgerRepository"]

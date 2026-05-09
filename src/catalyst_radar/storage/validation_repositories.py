from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import Engine, delete, insert, select, update

from catalyst_radar.core.immutability import thaw_json_value
from catalyst_radar.core.models import ActionState
from catalyst_radar.storage.schema import (
    decision_cards,
    paper_trades,
    useful_alert_labels,
    validation_results,
    validation_runs,
)
from catalyst_radar.validation.models import (
    PaperDecision,
    PaperTrade,
    PaperTradeState,
    UsefulAlertLabel,
    ValidationResult,
    ValidationRun,
    ValidationRunStatus,
)


class ValidationRepository:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def upsert_validation_run(self, run: ValidationRun) -> None:
        with self.engine.begin() as conn:
            conn.execute(delete(validation_runs).where(validation_runs.c.id == run.id))
            conn.execute(delete(validation_results).where(validation_results.c.run_id == run.id))
            conn.execute(insert(validation_runs).values(**_validation_run_row(run)))

    def finish_validation_run(
        self,
        run_id: str,
        status: ValidationRunStatus,
        metrics: Mapping[str, Any],
        *,
        finished_at: datetime | None = None,
    ) -> None:
        resolved_finished_at = _to_utc_datetime(
            finished_at or datetime.now(UTC),
            "finished_at",
        )
        with self.engine.begin() as conn:
            conn.execute(
                update(validation_runs)
                .where(validation_runs.c.id == run_id)
                .values(
                    status=ValidationRunStatus(status).value,
                    metrics=thaw_json_value(metrics),
                    finished_at=resolved_finished_at,
                )
            )

    def latest_validation_run(self, run_id: str) -> ValidationRun | None:
        stmt = select(validation_runs).where(validation_runs.c.id == run_id).limit(1)
        with self.engine.connect() as conn:
            row = conn.execute(stmt).first()
        return _validation_run_from_row(row._mapping) if row is not None else None

    def upsert_validation_results(self, rows: Iterable[ValidationResult]) -> int:
        count = 0
        with self.engine.begin() as conn:
            for row in rows:
                conn.execute(delete(validation_results).where(validation_results.c.id == row.id))
                conn.execute(insert(validation_results).values(**_validation_result_row(row)))
                count += 1
        return count

    def list_validation_results(
        self,
        run_id: str,
        *,
        available_at: datetime | None = None,
    ) -> list[ValidationResult]:
        filters = [validation_results.c.run_id == run_id]
        if available_at is not None:
            filters.append(
                validation_results.c.available_at
                <= _to_utc_datetime(available_at, "available_at")
            )
        stmt = select(validation_results).where(*filters).order_by(
            validation_results.c.as_of,
            validation_results.c.ticker,
            validation_results.c.baseline,
        )
        with self.engine.connect() as conn:
            return [_validation_result_from_row(row._mapping) for row in conn.execute(stmt)]

    def upsert_paper_trade(self, trade: PaperTrade) -> None:
        with self.engine.begin() as conn:
            conn.execute(delete(paper_trades).where(paper_trades.c.id == trade.id))
            conn.execute(insert(paper_trades).values(**_paper_trade_row(trade)))

    def latest_paper_trade_for_card(
        self,
        decision_card_id: str,
        available_at: datetime,
    ) -> PaperTrade | None:
        stmt = (
            select(paper_trades)
            .where(
                paper_trades.c.decision_card_id == decision_card_id,
                paper_trades.c.available_at
                <= _to_utc_datetime(available_at, "available_at"),
            )
            .order_by(
                paper_trades.c.available_at.desc(),
                paper_trades.c.updated_at.desc(),
                paper_trades.c.id.desc(),
            )
            .limit(1)
        )
        with self.engine.connect() as conn:
            row = conn.execute(stmt).first()
        return _paper_trade_from_row(row._mapping) if row is not None else None

    def list_paper_trades(self, *, available_at: datetime | None = None) -> list[PaperTrade]:
        filters = []
        if available_at is not None:
            filters.append(
                paper_trades.c.available_at <= _to_utc_datetime(available_at, "available_at")
            )
        stmt = select(paper_trades).where(*filters).order_by(
            paper_trades.c.available_at.desc(),
            paper_trades.c.ticker,
        )
        with self.engine.connect() as conn:
            return [_paper_trade_from_row(row._mapping) for row in conn.execute(stmt)]

    def insert_useful_alert_label(self, label: UsefulAlertLabel) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                delete(useful_alert_labels).where(useful_alert_labels.c.id == label.id)
            )
            conn.execute(insert(useful_alert_labels).values(**_useful_alert_label_row(label)))

    def latest_useful_alert_label(
        self,
        *,
        artifact_type: str,
        artifact_id: str,
    ) -> UsefulAlertLabel | None:
        stmt = (
            select(useful_alert_labels)
            .where(
                useful_alert_labels.c.artifact_type == artifact_type,
                useful_alert_labels.c.artifact_id == artifact_id,
            )
            .order_by(
                useful_alert_labels.c.created_at.desc(),
                useful_alert_labels.c.id.desc(),
            )
            .limit(1)
        )
        with self.engine.connect() as conn:
            row = conn.execute(stmt).first()
        return _useful_alert_label_from_row(row._mapping) if row is not None else None

    def list_useful_alert_labels(
        self,
        *,
        available_at: datetime | None = None,
    ) -> list[UsefulAlertLabel]:
        filters = []
        if available_at is not None:
            filters.append(
                useful_alert_labels.c.created_at
                <= _to_utc_datetime(available_at, "available_at")
            )
        stmt = select(useful_alert_labels).where(*filters).order_by(
            useful_alert_labels.c.artifact_type,
            useful_alert_labels.c.artifact_id,
            useful_alert_labels.c.created_at.desc(),
            useful_alert_labels.c.id.desc(),
        )
        with self.engine.connect() as conn:
            latest: dict[tuple[str, str], UsefulAlertLabel] = {}
            for row in conn.execute(stmt):
                label = _useful_alert_label_from_row(row._mapping)
                latest.setdefault((label.artifact_type, label.artifact_id), label)
            return sorted(latest.values(), key=lambda label: label.created_at)

    def decision_card_payload(
        self,
        decision_card_id: str,
        *,
        available_at: datetime | None = None,
    ) -> dict[str, Any] | None:
        filters = [decision_cards.c.id == decision_card_id]
        if available_at is not None:
            filters.append(
                decision_cards.c.available_at <= _to_utc_datetime(available_at, "available_at")
            )
        stmt = select(decision_cards).where(*filters).limit(1)
        with self.engine.connect() as conn:
            row = conn.execute(stmt).first()
        if row is None:
            return None
        values = row._mapping
        return {
            "id": values["id"],
            "ticker": values["ticker"],
            "as_of": _as_datetime(values["as_of"]),
            "action_state": values["action_state"],
            "setup_type": values["setup_type"],
            "final_score": values["final_score"],
            "next_review_at": _as_datetime(values["next_review_at"]),
            "payload": values["payload"],
            "source_ts": _as_datetime(values["source_ts"]),
            "available_at": _as_datetime(values["available_at"]),
        }


def _validation_run_row(run: ValidationRun) -> dict[str, Any]:
    return {
        "id": run.id,
        "run_type": run.run_type,
        "as_of_start": run.as_of_start,
        "as_of_end": run.as_of_end,
        "decision_available_at": run.decision_available_at,
        "status": run.status.value,
        "config": thaw_json_value(run.config),
        "metrics": thaw_json_value(run.metrics),
        "started_at": run.started_at,
        "finished_at": run.finished_at,
        "created_at": datetime.now(UTC),
    }


def _validation_result_row(result: ValidationResult) -> dict[str, Any]:
    return {
        "id": result.id,
        "run_id": result.run_id,
        "ticker": result.ticker,
        "as_of": result.as_of,
        "available_at": result.available_at,
        "state": result.state.value,
        "final_score": result.final_score,
        "candidate_state_id": result.candidate_state_id,
        "candidate_packet_id": result.candidate_packet_id,
        "decision_card_id": result.decision_card_id,
        "baseline": result.baseline,
        "labels": thaw_json_value(result.labels),
        "leakage_flags": list(result.leakage_flags),
        "payload": thaw_json_value(result.payload),
        "created_at": datetime.now(UTC),
    }


def _paper_trade_row(trade: PaperTrade) -> dict[str, Any]:
    return {
        "id": trade.id,
        "decision_card_id": trade.decision_card_id,
        "ticker": trade.ticker,
        "as_of": trade.as_of,
        "decision": trade.decision.value,
        "state": trade.state.value,
        "entry_price": trade.entry_price,
        "entry_at": trade.entry_at,
        "invalidation_price": trade.invalidation_price,
        "shares": trade.shares,
        "notional": trade.notional,
        "max_loss": trade.max_loss,
        "outcome_labels": thaw_json_value(trade.outcome_labels),
        "source_ts": trade.source_ts,
        "available_at": trade.available_at,
        "payload": thaw_json_value(trade.payload),
        "created_at": trade.created_at,
        "updated_at": trade.updated_at,
    }


def _useful_alert_label_row(label: UsefulAlertLabel) -> dict[str, Any]:
    return {
        "id": label.id,
        "artifact_type": label.artifact_type,
        "artifact_id": label.artifact_id,
        "ticker": label.ticker,
        "label": label.label,
        "notes": label.notes,
        "created_at": label.created_at,
    }


def _validation_run_from_row(row: Any) -> ValidationRun:
    return ValidationRun(
        id=row["id"],
        run_type=row["run_type"],
        as_of_start=_as_datetime(row["as_of_start"]),
        as_of_end=_as_datetime(row["as_of_end"]),
        decision_available_at=_as_datetime(row["decision_available_at"]),
        status=ValidationRunStatus(row["status"]),
        config=row["config"],
        metrics=row["metrics"],
        started_at=_as_datetime(row["started_at"]),
        finished_at=_as_datetime(row["finished_at"]) if row["finished_at"] else None,
    )


def _validation_result_from_row(row: Any) -> ValidationResult:
    return ValidationResult(
        id=row["id"],
        run_id=row["run_id"],
        ticker=row["ticker"],
        as_of=_as_datetime(row["as_of"]),
        available_at=_as_datetime(row["available_at"]),
        state=ActionState(row["state"]),
        final_score=row["final_score"],
        candidate_state_id=row["candidate_state_id"],
        candidate_packet_id=row["candidate_packet_id"],
        decision_card_id=row["decision_card_id"],
        baseline=row["baseline"],
        labels=row["labels"],
        leakage_flags=tuple(row["leakage_flags"]),
        payload=row["payload"],
    )


def _paper_trade_from_row(row: Any) -> PaperTrade:
    return PaperTrade(
        id=row["id"],
        decision_card_id=row["decision_card_id"],
        ticker=row["ticker"],
        as_of=_as_datetime(row["as_of"]),
        decision=PaperDecision(row["decision"]),
        state=PaperTradeState(row["state"]),
        entry_price=row["entry_price"],
        entry_at=_as_datetime(row["entry_at"]) if row["entry_at"] else None,
        invalidation_price=row["invalidation_price"],
        shares=row["shares"],
        notional=row["notional"],
        max_loss=row["max_loss"],
        outcome_labels=row["outcome_labels"],
        source_ts=_as_datetime(row["source_ts"]),
        available_at=_as_datetime(row["available_at"]),
        payload=row["payload"],
        created_at=_as_datetime(row["created_at"]),
        updated_at=_as_datetime(row["updated_at"]),
    )


def _useful_alert_label_from_row(row: Any) -> UsefulAlertLabel:
    return UsefulAlertLabel(
        id=row["id"],
        artifact_type=row["artifact_type"],
        artifact_id=row["artifact_id"],
        ticker=row["ticker"],
        label=row["label"],
        notes=row["notes"],
        created_at=_as_datetime(row["created_at"]),
    )


def _to_utc_datetime(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        msg = f"{field_name} must be a datetime"
        raise TypeError(msg)
    if value.tzinfo is None or value.utcoffset() is None:
        msg = f"{field_name} must be timezone-aware"
        raise ValueError(msg)
    return value.astimezone(UTC)


def _as_datetime(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


__all__ = ["ValidationRepository"]

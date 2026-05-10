from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import Engine, delete, insert, select

from catalyst_radar.alerts.models import UserFeedback, user_feedback_id
from catalyst_radar.core.immutability import thaw_json_value
from catalyst_radar.security.audit import AuditLogRepository
from catalyst_radar.security.redaction import redact_text
from catalyst_radar.storage.schema import (
    alerts,
    candidate_packets,
    decision_cards,
    paper_trades,
    useful_alert_labels,
)
from catalyst_radar.storage.schema import (
    user_feedback as user_feedback_table,
)
from catalyst_radar.validation.models import UsefulAlertLabel, useful_alert_label_id

ALLOWED_FEEDBACK_LABELS = frozenset(
    {"useful", "noisy", "too_late", "too_early", "ignored", "acted"}
)
ALLOWED_ARTIFACT_TYPES = frozenset(
    {"candidate_packet", "decision_card", "paper_trade", "alert"}
)


@dataclass(frozen=True)
class FeedbackRecordResult:
    user_feedback: UserFeedback
    useful_label: UsefulAlertLabel


class FeedbackError(ValueError):
    pass


class InvalidFeedbackError(FeedbackError):
    pass


class MissingArtifactError(FeedbackError):
    pass


class TickerMismatchError(InvalidFeedbackError):
    pass


def record_feedback(
    engine: Engine,
    *,
    artifact_type: str,
    artifact_id: str,
    ticker: str,
    label: str,
    notes: str | None = None,
    source: str = "api",
    created_at: datetime | None = None,
    actor_id: str | None = None,
    actor_role: str | None = None,
) -> FeedbackRecordResult:
    resolved_artifact_type = _allowed_value(
        artifact_type,
        allowed=ALLOWED_ARTIFACT_TYPES,
        field_name="artifact_type",
    )
    resolved_label = _allowed_value(
        label,
        allowed=ALLOWED_FEEDBACK_LABELS,
        field_name="label",
    )
    resolved_artifact_id = _required_text(artifact_id, "artifact_id")
    resolved_ticker = _required_text(ticker, "ticker").upper()
    resolved_notes = _optional_text(notes)
    resolved_source = _required_text(source, "source")
    resolved_created_at = _to_utc_datetime(created_at or datetime.now(UTC), "created_at")

    artifact_ticker = _artifact_ticker(
        engine,
        artifact_type=resolved_artifact_type,
        artifact_id=resolved_artifact_id,
        available_at=resolved_created_at,
    )
    if artifact_ticker != resolved_ticker:
        raise TickerMismatchError("ticker must match the referenced artifact")

    feedback = UserFeedback(
        id=user_feedback_id(
            artifact_type=resolved_artifact_type,
            artifact_id=resolved_artifact_id,
            label=resolved_label,
            created_at=resolved_created_at,
        ),
        artifact_type=resolved_artifact_type,
        artifact_id=resolved_artifact_id,
        ticker=resolved_ticker,
        label=resolved_label,
        notes=resolved_notes,
        source=resolved_source,
        payload={},
        created_at=resolved_created_at,
    )
    useful_label = UsefulAlertLabel(
        id=useful_alert_label_id(
            artifact_type=resolved_artifact_type,
            artifact_id=resolved_artifact_id,
            label=resolved_label,
        ),
        artifact_type=resolved_artifact_type,
        artifact_id=resolved_artifact_id,
        ticker=resolved_ticker,
        label=resolved_label,
        notes=resolved_notes,
        created_at=resolved_created_at,
    )

    with engine.begin() as conn:
        conn.execute(delete(user_feedback_table).where(user_feedback_table.c.id == feedback.id))
        conn.execute(insert(user_feedback_table).values(**_user_feedback_row(feedback)))
        conn.execute(
            delete(useful_alert_labels).where(useful_alert_labels.c.id == useful_label.id)
        )
        conn.execute(insert(useful_alert_labels).values(**_useful_alert_label_row(useful_label)))
    AuditLogRepository(engine).append_event(
        event_type="feedback_recorded",
        actor_source=resolved_source,
        actor_id=_optional_text(actor_id),
        actor_role=_optional_text(actor_role),
        artifact_type=resolved_artifact_type,
        artifact_id=resolved_artifact_id,
        ticker=resolved_ticker,
        candidate_packet_id=(
            resolved_artifact_id if resolved_artifact_type == "candidate_packet" else None
        ),
        decision_card_id=(
            resolved_artifact_id if resolved_artifact_type == "decision_card" else None
        ),
        paper_trade_id=(
            resolved_artifact_id if resolved_artifact_type == "paper_trade" else None
        ),
        alert_id=resolved_artifact_id if resolved_artifact_type == "alert" else None,
        status="success",
        metadata={"label": resolved_label},
        after_payload={
            "notes": redact_text(resolved_notes) if resolved_notes is not None else None
        },
        occurred_at=resolved_created_at,
        available_at=resolved_created_at,
    )
    return FeedbackRecordResult(user_feedback=feedback, useful_label=useful_label)


def _allowed_value(value: object, *, allowed: frozenset[str], field_name: str) -> str:
    text = _required_text(value, field_name)
    if text not in allowed:
        allowed_values = ", ".join(sorted(allowed))
        raise InvalidFeedbackError(f"{field_name} must be one of: {allowed_values}")
    return text


def _required_text(value: object, field_name: str) -> str:
    text = str(value).strip()
    if not text:
        raise InvalidFeedbackError(f"{field_name} must not be blank")
    return text


def _optional_text(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _artifact_ticker(
    engine: Engine,
    *,
    artifact_type: str,
    artifact_id: str,
    available_at: datetime,
) -> str:
    table_by_artifact = {
        "candidate_packet": candidate_packets,
        "decision_card": decision_cards,
        "paper_trade": paper_trades,
        "alert": alerts,
    }
    table = table_by_artifact[artifact_type]
    filters = [table.c.id == artifact_id]
    if "available_at" in table.c:
        filters.append(table.c.available_at <= available_at)
    with engine.connect() as conn:
        row = conn.execute(select(table.c.ticker).where(*filters).limit(1)).first()
    if row is None:
        raise MissingArtifactError("referenced artifact not found")
    return str(row[0]).upper()


def _user_feedback_row(feedback: UserFeedback) -> dict[str, object]:
    return {
        "id": feedback.id,
        "artifact_type": feedback.artifact_type,
        "artifact_id": feedback.artifact_id,
        "ticker": feedback.ticker,
        "label": feedback.label,
        "notes": feedback.notes,
        "source": feedback.source,
        "payload": thaw_json_value(feedback.payload),
        "created_at": feedback.created_at,
    }


def _useful_alert_label_row(label: UsefulAlertLabel) -> dict[str, object]:
    return {
        "id": label.id,
        "artifact_type": label.artifact_type,
        "artifact_id": label.artifact_id,
        "ticker": label.ticker,
        "label": label.label,
        "notes": label.notes,
        "created_at": label.created_at,
    }


def _to_utc_datetime(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise InvalidFeedbackError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise InvalidFeedbackError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)

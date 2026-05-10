from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import Engine, delete, insert, select

from catalyst_radar.alerts.models import (
    Alert,
    AlertChannel,
    AlertPriority,
    AlertRoute,
    AlertStatus,
    AlertSuppression,
    UserFeedback,
)
from catalyst_radar.core.immutability import thaw_json_value
from catalyst_radar.storage.schema import alert_suppressions, alerts, user_feedback


class AlertRepository:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def upsert_alert(self, alert: Alert) -> None:
        with self.engine.begin() as conn:
            conn.execute(delete(alerts).where(alerts.c.id == alert.id))
            conn.execute(insert(alerts).values(**_alert_row(alert)))

    def insert_suppression(self, suppression: AlertSuppression) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                delete(alert_suppressions).where(
                    alert_suppressions.c.id == suppression.id
                )
            )
            conn.execute(insert(alert_suppressions).values(**_suppression_row(suppression)))

    def latest_alert_by_dedupe_key(
        self,
        dedupe_key: str,
        available_at: datetime,
    ) -> Alert | None:
        stmt = (
            select(alerts)
            .where(
                alerts.c.dedupe_key == dedupe_key,
                alerts.c.available_at
                <= _to_utc_datetime(available_at, "available_at"),
            )
            .order_by(
                alerts.c.available_at.desc(),
                alerts.c.created_at.desc(),
                alerts.c.id.desc(),
            )
            .limit(1)
        )
        with self.engine.connect() as conn:
            row = conn.execute(stmt).first()
        return _alert_from_row(row._mapping) if row is not None else None

    def alert_by_id(
        self,
        alert_id: str,
        available_at: datetime | None = None,
    ) -> Alert | None:
        filters = [alerts.c.id == alert_id]
        if available_at is not None:
            filters.append(
                alerts.c.available_at <= _to_utc_datetime(available_at, "available_at")
            )
        stmt = select(alerts).where(*filters).limit(1)
        with self.engine.connect() as conn:
            row = conn.execute(stmt).first()
        return _alert_from_row(row._mapping) if row is not None else None

    def list_alerts(
        self,
        *,
        available_at: datetime | None = None,
        ticker: str | None = None,
        status: str | None = None,
        route: str | None = None,
        limit: int = 200,
    ) -> list[Alert]:
        filters = []
        if available_at is not None:
            filters.append(
                alerts.c.available_at <= _to_utc_datetime(available_at, "available_at")
            )
        if ticker is not None and ticker.strip():
            filters.append(alerts.c.ticker == ticker.upper())
        if status is not None and status.strip():
            filters.append(alerts.c.status == AlertStatus(status).value)
        if route is not None and route.strip():
            filters.append(alerts.c.route == AlertRoute(route).value)
        stmt = (
            select(alerts)
            .where(*filters)
            .order_by(
                alerts.c.available_at.desc(),
                alerts.c.created_at.desc(),
                alerts.c.id.desc(),
            )
            .limit(_positive_limit(limit))
        )
        with self.engine.connect() as conn:
            return [_alert_from_row(row._mapping) for row in conn.execute(stmt)]

    def list_suppressions(
        self,
        *,
        available_at: datetime | None = None,
        limit: int = 200,
    ) -> list[AlertSuppression]:
        filters = []
        if available_at is not None:
            filters.append(
                alert_suppressions.c.available_at
                <= _to_utc_datetime(available_at, "available_at")
            )
        stmt = (
            select(alert_suppressions)
            .where(*filters)
            .order_by(
                alert_suppressions.c.available_at.desc(),
                alert_suppressions.c.created_at.desc(),
                alert_suppressions.c.id.desc(),
            )
            .limit(_positive_limit(limit))
        )
        with self.engine.connect() as conn:
            return [_suppression_from_row(row._mapping) for row in conn.execute(stmt)]

    def insert_user_feedback(self, feedback: UserFeedback) -> None:
        with self.engine.begin() as conn:
            conn.execute(delete(user_feedback).where(user_feedback.c.id == feedback.id))
            conn.execute(insert(user_feedback).values(**_user_feedback_row(feedback)))

    def latest_feedback(
        self,
        *,
        artifact_type: str,
        artifact_id: str,
    ) -> UserFeedback | None:
        stmt = (
            select(user_feedback)
            .where(
                user_feedback.c.artifact_type == artifact_type,
                user_feedback.c.artifact_id == artifact_id,
            )
            .order_by(
                user_feedback.c.created_at.desc(),
                user_feedback.c.id.desc(),
            )
            .limit(1)
        )
        with self.engine.connect() as conn:
            row = conn.execute(stmt).first()
        return _user_feedback_from_row(row._mapping) if row is not None else None


def _alert_row(alert: Alert) -> dict[str, Any]:
    return {
        "id": alert.id,
        "ticker": alert.ticker,
        "as_of": alert.as_of,
        "source_ts": alert.source_ts,
        "available_at": alert.available_at,
        "candidate_state_id": alert.candidate_state_id,
        "candidate_packet_id": alert.candidate_packet_id,
        "decision_card_id": alert.decision_card_id,
        "action_state": alert.action_state,
        "route": alert.route.value,
        "channel": alert.channel.value,
        "priority": alert.priority.value,
        "status": alert.status.value,
        "dedupe_key": alert.dedupe_key,
        "trigger_kind": alert.trigger_kind,
        "trigger_fingerprint": alert.trigger_fingerprint,
        "title": alert.title,
        "summary": alert.summary,
        "feedback_url": alert.feedback_url,
        "payload": thaw_json_value(alert.payload),
        "created_at": alert.created_at,
        "sent_at": alert.sent_at,
    }


def _suppression_row(suppression: AlertSuppression) -> dict[str, Any]:
    return {
        "id": suppression.id,
        "ticker": suppression.ticker,
        "as_of": suppression.as_of,
        "available_at": suppression.available_at,
        "candidate_state_id": suppression.candidate_state_id,
        "decision_card_id": suppression.decision_card_id,
        "route": suppression.route.value,
        "dedupe_key": suppression.dedupe_key,
        "trigger_kind": suppression.trigger_kind,
        "trigger_fingerprint": suppression.trigger_fingerprint,
        "reason": suppression.reason,
        "payload": thaw_json_value(suppression.payload),
        "created_at": suppression.created_at,
    }


def _user_feedback_row(feedback: UserFeedback) -> dict[str, Any]:
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


def _alert_from_row(row: Any) -> Alert:
    return Alert(
        id=row["id"],
        ticker=row["ticker"],
        as_of=_as_datetime(row["as_of"]),
        source_ts=_as_datetime(row["source_ts"]),
        available_at=_as_datetime(row["available_at"]),
        candidate_state_id=row["candidate_state_id"],
        candidate_packet_id=row["candidate_packet_id"],
        decision_card_id=row["decision_card_id"],
        action_state=row["action_state"],
        route=AlertRoute(row["route"]),
        channel=AlertChannel(row["channel"]),
        priority=AlertPriority(row["priority"]),
        status=AlertStatus(row["status"]),
        dedupe_key=row["dedupe_key"],
        trigger_kind=row["trigger_kind"],
        trigger_fingerprint=row["trigger_fingerprint"],
        title=row["title"],
        summary=row["summary"],
        feedback_url=row["feedback_url"],
        payload=row["payload"],
        created_at=_as_datetime(row["created_at"]),
        sent_at=_as_datetime(row["sent_at"]) if row["sent_at"] else None,
    )


def _suppression_from_row(row: Any) -> AlertSuppression:
    return AlertSuppression(
        id=row["id"],
        ticker=row["ticker"],
        as_of=_as_datetime(row["as_of"]),
        available_at=_as_datetime(row["available_at"]),
        candidate_state_id=row["candidate_state_id"],
        decision_card_id=row["decision_card_id"],
        route=AlertRoute(row["route"]),
        dedupe_key=row["dedupe_key"],
        trigger_kind=row["trigger_kind"],
        trigger_fingerprint=row["trigger_fingerprint"],
        reason=row["reason"],
        payload=row["payload"],
        created_at=_as_datetime(row["created_at"]),
    )


def _user_feedback_from_row(row: Any) -> UserFeedback:
    return UserFeedback(
        id=row["id"],
        artifact_type=row["artifact_type"],
        artifact_id=row["artifact_id"],
        ticker=row["ticker"],
        label=row["label"],
        notes=row["notes"],
        source=row["source"],
        payload=row["payload"],
        created_at=_as_datetime(row["created_at"]),
    )


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


__all__ = ["AlertRepository"]

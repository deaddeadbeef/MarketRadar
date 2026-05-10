from __future__ import annotations

from datetime import UTC, datetime

import pytest

from catalyst_radar.alerts.channels import (
    DryRunAlertChannel,
    EmailAlertChannel,
    WebhookAlertChannel,
)
from catalyst_radar.alerts.digest import build_alert_digest, digest_payload
from catalyst_radar.alerts.models import (
    Alert,
    AlertChannel,
    AlertPriority,
    AlertRoute,
    AlertStatus,
    AlertSuppression,
)
from catalyst_radar.core.models import ActionState

GENERATED_AT = datetime(2026, 5, 10, 15, 0, tzinfo=UTC)


def test_digest_groups_alerts_by_route_and_priority() -> None:
    alerts = (
        _alert(id="alert-2", route=AlertRoute.DAILY_DIGEST, priority=AlertPriority.NORMAL),
        _alert(
            id="alert-1",
            route=AlertRoute.IMMEDIATE_MANUAL_REVIEW,
            channel=AlertChannel.DASHBOARD,
            priority=AlertPriority.HIGH,
        ),
        _alert(id="alert-3", route=AlertRoute.WARNING_DIGEST, priority=AlertPriority.HIGH),
    )

    digest = build_alert_digest(alerts, (), GENERATED_AT)

    assert tuple(digest.groups) == (
        "immediate_manual_review:high",
        "warning_digest:high",
        "daily_digest:normal",
    )
    assert [alert.id for alert in digest.groups["warning_digest:high"]] == ["alert-3"]
    payload = digest_payload(digest)
    assert payload["group_count"] == 3
    assert payload["groups"][0]["route"] == "immediate_manual_review"
    first_alert = payload["groups"][0]["alerts"][0]
    assert first_alert["feedback_url"] == "/api/alerts/alert-1/feedback"
    assert first_alert["candidate_state_id"] == "state-alert-1"
    assert first_alert["candidate_packet_id"] == "packet-alert-1"
    assert first_alert["decision_card_id"] == "card-alert-1"


def test_digest_includes_suppressed_count() -> None:
    digest = build_alert_digest(
        (_alert(),),
        (
            _suppression(id="suppression-1"),
            _suppression(id="suppression-2"),
        ),
        GENERATED_AT,
    )

    assert digest.suppressed_count == 2
    assert digest_payload(digest)["suppressed_count"] == 2


def test_dry_run_channel_marks_payload_without_network_send() -> None:
    result = DryRunAlertChannel(
        payload_defaults={"network_io": True, "alert_id": "wrong"}
    ).deliver(_alert(), dry_run=True)

    assert result.status == "dry_run"
    assert result.dry_run is True
    assert result.payload["network_io"] is False
    assert result.payload["alert_id"] == "alert-1"
    assert result.payload["adapter"] == "dry_run"


def test_email_and_webhook_channels_are_disabled_without_explicit_adapter() -> None:
    alert = _alert()

    email_result = EmailAlertChannel().deliver(alert, dry_run=True)
    webhook_result = WebhookAlertChannel().deliver(alert, dry_run=True)

    assert email_result.payload["network_io"] is False
    assert webhook_result.payload["network_io"] is False
    with pytest.raises(RuntimeError, match="external alert delivery is not enabled"):
        EmailAlertChannel().deliver(alert, dry_run=False)
    with pytest.raises(RuntimeError, match="external alert delivery is not enabled"):
        WebhookAlertChannel().deliver(alert, dry_run=False)


def _alert(
    *,
    id: str = "alert-1",
    route: AlertRoute = AlertRoute.WARNING_DIGEST,
    channel: AlertChannel = AlertChannel.DIGEST,
    priority: AlertPriority = AlertPriority.HIGH,
) -> Alert:
    return Alert(
        id=id,
        ticker="MSFT",
        as_of=GENERATED_AT,
        source_ts=GENERATED_AT,
        available_at=GENERATED_AT,
        candidate_state_id=f"state-{id}",
        candidate_packet_id=f"packet-{id}",
        decision_card_id=f"card-{id}",
        action_state=ActionState.WARNING.value,
        route=route,
        channel=channel,
        priority=priority,
        status=AlertStatus.PLANNED,
        dedupe_key=f"dedupe-{id}",
        trigger_kind="score_delta",
        trigger_fingerprint="score_delta:10",
        title="MSFT warning evidence digest",
        summary="MSFT candidate review prompt",
        feedback_url=f"/api/alerts/{id}/feedback",
        created_at=GENERATED_AT,
    )


def _suppression(*, id: str) -> AlertSuppression:
    return AlertSuppression(
        id=id,
        ticker="MSFT",
        as_of=GENERATED_AT,
        available_at=GENERATED_AT,
        route=AlertRoute.WARNING_DIGEST,
        dedupe_key=f"dedupe-{id}",
        trigger_kind="score_delta",
        trigger_fingerprint="score_delta:10",
        reason="duplicate_trigger",
        created_at=GENERATED_AT,
    )

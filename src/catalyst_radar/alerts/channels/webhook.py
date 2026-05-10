from __future__ import annotations

from dataclasses import dataclass

from catalyst_radar.alerts.channels.base import DeliveryResult, dry_run_delivery_result
from catalyst_radar.alerts.models import Alert


@dataclass(frozen=True)
class WebhookAlertChannel:
    channel: str = "webhook"

    def deliver(self, alert: Alert, *, dry_run: bool = True) -> DeliveryResult:
        if not dry_run:
            raise RuntimeError("external alert delivery is not enabled")
        return dry_run_delivery_result(
            alert,
            channel=self.channel,
            payload={"adapter": "webhook"},
        )

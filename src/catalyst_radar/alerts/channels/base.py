from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Protocol

from catalyst_radar.alerts.models import Alert
from catalyst_radar.core.immutability import freeze_mapping


@dataclass(frozen=True)
class DeliveryResult:
    alert_id: str
    channel: str
    status: str
    dry_run: bool
    payload: Mapping[str, object]

    def __post_init__(self) -> None:
        object.__setattr__(self, "payload", freeze_mapping(self.payload, "payload"))


class AlertChannelAdapter(Protocol):
    def deliver(self, alert: Alert, *, dry_run: bool = True) -> DeliveryResult: ...


@dataclass(frozen=True)
class DryRunAlertChannel:
    channel: str = "dry_run"
    payload_defaults: Mapping[str, object] = field(default_factory=dict)

    def deliver(self, alert: Alert, *, dry_run: bool = True) -> DeliveryResult:
        if not dry_run:
            raise RuntimeError("external alert delivery is not enabled")
        return dry_run_delivery_result(
            alert,
            channel=self.channel,
            payload={"adapter": "dry_run", **dict(self.payload_defaults)},
        )


def dry_run_delivery_result(
    alert: Alert,
    *,
    channel: str | None = None,
    payload: Mapping[str, object] | None = None,
) -> DeliveryResult:
    delivery_payload = {
        **dict(payload or {}),
        "network_io": False,
        "alert_id": str(alert.id),
        "ticker": str(alert.ticker),
        "route": _enum_value(alert.route),
        "title": str(alert.title),
        "summary": str(alert.summary),
    }
    return DeliveryResult(
        alert_id=str(alert.id),
        channel=str(channel or _enum_value(alert.channel)),
        status="dry_run",
        dry_run=True,
        payload=delivery_payload,
    )


def _enum_value(value: object) -> str:
    enum_value = getattr(value, "value", None)
    return str(enum_value if enum_value is not None else value)

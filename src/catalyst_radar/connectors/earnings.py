from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from catalyst_radar.connectors.base import (
    ConnectorHealth,
    ConnectorHealthStatus,
    ConnectorRecordKind,
    ConnectorRequest,
    NormalizedRecord,
    ProviderCostEstimate,
    RawRecord,
)
from catalyst_radar.connectors.news import _source_quality
from catalyst_radar.connectors.sec import (
    FIXTURE_RETENTION_POLICY,
    _canonical_event_payload,
    _event_id,
    _hash_payload,
    _mapping,
    _parse_datetime,
    _raw_payload,
    body_hash,
    dedupe_key,
)
from catalyst_radar.core.immutability import thaw_json_value

EARNINGS_PROVIDER_NAME = "earnings_fixture"
EARNINGS_LICENSE_TAG = "earnings-fixture"


class EarningsCalendarConnector:
    def __init__(
        self,
        *,
        fixture_path: str | Path,
        provider: str = EARNINGS_PROVIDER_NAME,
    ) -> None:
        self.fixture_path = Path(fixture_path)
        self.provider = provider

    def fetch(self, request: ConnectorRequest) -> list[RawRecord]:
        payload = self._load_payload()
        fetched_at = request.requested_at
        request_hash = _hash_payload(
            {
                "provider": request.provider,
                "endpoint": request.endpoint,
                "params": thaw_json_value(request.params),
                "fixture_path": str(self.fixture_path),
            }
        )
        ticker = str(request.params.get("ticker") or payload.get("ticker") or "").upper()
        events = payload.get("events")
        if not isinstance(events, list):
            msg = "earnings fixture events must be a list"
            raise ValueError(msg)
        records: list[RawRecord] = []
        for event in events:
            event_payload = dict(_mapping(event, "event"))
            available_at = _parse_datetime(event_payload.get("available_at"), "available_at")
            raw_payload = _raw_payload(
                ConnectorRecordKind.EARNINGS_EVENT,
                {"ticker": ticker, "record": event_payload},
            )
            records.append(
                RawRecord(
                    provider=self.provider,
                    kind=ConnectorRecordKind.EARNINGS_EVENT,
                    request_hash=request_hash,
                    payload_hash=_hash_payload(raw_payload),
                    payload=raw_payload,
                    source_ts=available_at,
                    fetched_at=max(fetched_at, available_at),
                    available_at=available_at,
                    license_tag=EARNINGS_LICENSE_TAG,
                    retention_policy=FIXTURE_RETENTION_POLICY,
                )
            )
        return records

    def normalize(self, records: Sequence[RawRecord]) -> list[NormalizedRecord]:
        normalized: list[NormalizedRecord] = []
        for record in records:
            if record.kind != ConnectorRecordKind.EARNINGS_EVENT:
                continue
            payload = _mapping(record.payload.get("record"), "record")
            event = _mapping(payload.get("record"), "event")
            ticker = str(payload["ticker"]).upper()
            title = str(event.get("title") or f"{ticker} earnings date").strip()
            category = str(event.get("source_category") or "aggregator")
            source = str(event.get("source") or record.provider)
            event_date = str(event.get("event_date") or "")
            event_time = str(event.get("time") or "")
            content_hash = body_hash(f"{ticker} {event_date} {event_time} {title}")
            dedupe = dedupe_key(
                ticker=ticker,
                provider=record.provider,
                canonical_url=None,
                content_hash=content_hash,
            )
            event_payload = _canonical_event_payload(
                event_id=_event_id(dedupe),
                ticker=ticker,
                event_type="earnings",
                provider=record.provider,
                source=source,
                source_category=category,
                source_url=None,
                title=title,
                body_hash_value=content_hash,
                dedupe=dedupe,
                source_quality=_source_quality(category),
                materiality=0.55,
                source_ts=record.source_ts,
                available_at=record.available_at,
                payload={
                    "event_date": event_date,
                    "time": event_time,
                    "event_risk": "upcoming_earnings",
                    "classification_reasons": ["earnings_calendar"],
                },
            )
            normalized.append(
                NormalizedRecord(
                    provider=record.provider,
                    kind=ConnectorRecordKind.EVENT,
                    identity=dedupe,
                    payload=event_payload,
                    source_ts=record.source_ts,
                    available_at=record.available_at,
                    raw_payload_hash=record.payload_hash,
                )
            )
        return normalized

    def healthcheck(self) -> ConnectorHealth:
        if self.fixture_path.exists():
            return ConnectorHealth(
                provider=self.provider,
                status=ConnectorHealthStatus.HEALTHY,
                checked_at=datetime.now(UTC),
                reason="earnings fixture path is readable",
            )
        return ConnectorHealth(
            provider=self.provider,
            status=ConnectorHealthStatus.DOWN,
            checked_at=datetime.now(UTC),
            reason=f"missing earnings fixture path: {self.fixture_path}",
        )

    def estimate_cost(self, request: ConnectorRequest) -> ProviderCostEstimate:
        return ProviderCostEstimate(
            provider=request.provider,
            request_count=1,
            estimated_cost_usd=0.0,
        )

    def _load_payload(self) -> Mapping[str, Any]:
        with self.fixture_path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        return _mapping(payload, "fixture")


__all__ = [
    "EARNINGS_LICENSE_TAG",
    "EARNINGS_PROVIDER_NAME",
    "EarningsCalendarConnector",
]

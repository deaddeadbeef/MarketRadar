from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from hashlib import sha256
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
from catalyst_radar.core.immutability import thaw_json_value

OPTIONS_PROVIDER_NAME = "options_fixture"
OPTIONS_LICENSE_TAG = "options-fixture"
OPTIONS_RETENTION_POLICY = "local-fixture-retain"


class OptionsAggregateConnector:
    def __init__(
        self,
        *,
        fixture_path: str | Path,
        provider: str = OPTIONS_PROVIDER_NAME,
    ) -> None:
        self.fixture_path = Path(fixture_path)
        self.provider = provider

    def fetch(self, request: ConnectorRequest) -> list[RawRecord]:
        payload = self._load_payload()
        header = _fixture_header(payload)
        provider = str(header.get("provider") or self.provider)
        source_ts = _parse_datetime(header["source_ts"], "source_ts")
        available_at = _parse_datetime(header["available_at"], "available_at")
        as_of = _parse_datetime(header["as_of"], "as_of")
        request_hash = _hash_payload(
            {
                "provider": request.provider,
                "endpoint": request.endpoint,
                "params": thaw_json_value(request.params),
                "fixture_path": str(self.fixture_path),
            }
        )
        results = payload.get("results")
        if not isinstance(results, list):
            msg = "options fixture results must be a list"
            raise ValueError(msg)

        records: list[RawRecord] = []
        for index, result in enumerate(results, start=1):
            row = dict(_mapping(result, "result"))
            row_source_ts = _parse_datetime(row.get("source_ts", source_ts), "source_ts")
            row_available_at = _parse_datetime(
                row.get("available_at", available_at),
                "available_at",
            )
            row_as_of = _parse_datetime(row.get("as_of", as_of), "as_of")
            raw_payload = {
                "source": provider,
                "kind": ConnectorRecordKind.OPTION_FEATURE.value,
                "path": str(self.fixture_path),
                "row_number": index,
                "header": {
                    "as_of": as_of.isoformat(),
                    "source_ts": source_ts.isoformat(),
                    "available_at": available_at.isoformat(),
                    "provider": provider,
                },
                "record": {
                    **row,
                    "as_of": row_as_of.isoformat(),
                    "source_ts": row_source_ts.isoformat(),
                    "available_at": row_available_at.isoformat(),
                    "provider": provider,
                },
            }
            records.append(
                RawRecord(
                    provider=provider,
                    kind=ConnectorRecordKind.OPTION_FEATURE,
                    request_hash=request_hash,
                    payload_hash=_hash_payload(raw_payload),
                    payload=raw_payload,
                    source_ts=row_source_ts,
                    fetched_at=max(request.requested_at, row_source_ts),
                    available_at=row_available_at,
                    license_tag=OPTIONS_LICENSE_TAG,
                    retention_policy=OPTIONS_RETENTION_POLICY,
                )
            )
        return records

    def normalize(self, records: Sequence[RawRecord]) -> list[NormalizedRecord]:
        normalized: list[NormalizedRecord] = []
        for record in records:
            if record.kind != ConnectorRecordKind.OPTION_FEATURE:
                continue
            payload = _mapping(record.payload.get("record"), "record")
            ticker = str(payload["ticker"]).upper()
            as_of = _parse_datetime(payload["as_of"], "as_of")
            option_payload = {
                "ticker": ticker,
                "as_of": as_of.isoformat(),
                "provider": str(payload.get("provider") or record.provider),
                "call_volume": float(payload["call_volume"]),
                "put_volume": float(payload["put_volume"]),
                "call_open_interest": float(payload["call_open_interest"]),
                "put_open_interest": float(payload["put_open_interest"]),
                "iv_percentile": float(payload["iv_percentile"]),
                "skew": float(payload["skew"]),
                "source_ts": record.source_ts.isoformat(),
                "available_at": record.available_at.isoformat(),
                "payload": {
                    "fixture_path": str(self.fixture_path),
                    "raw_record": thaw_json_value(payload),
                },
            }
            normalized.append(
                NormalizedRecord(
                    provider=record.provider,
                    kind=ConnectorRecordKind.OPTION_FEATURE,
                    identity=f"{ticker}:{as_of.isoformat()}",
                    payload=option_payload,
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
                reason="options fixture path is readable",
            )
        return ConnectorHealth(
            provider=self.provider,
            status=ConnectorHealthStatus.DOWN,
            checked_at=datetime.now(UTC),
            reason=f"missing options fixture path: {self.fixture_path}",
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


def _fixture_header(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    for field in ("as_of", "source_ts", "available_at"):
        if field not in payload:
            msg = f"options fixture missing header field: {field}"
            raise ValueError(msg)
    return payload


def _mapping(value: object, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        msg = f"{field_name} must be a mapping"
        raise ValueError(msg)
    return value


def _parse_datetime(value: object, field_name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        msg = f"{field_name} must include timezone information"
        raise ValueError(msg)
    return parsed.astimezone(UTC)


def _hash_payload(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return sha256(encoded).hexdigest()


__all__ = [
    "OPTIONS_LICENSE_TAG",
    "OPTIONS_PROVIDER_NAME",
    "OPTIONS_RETENTION_POLICY",
    "OptionsAggregateConnector",
]

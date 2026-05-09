from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, time, timedelta
from enum import StrEnum
from hashlib import sha256
from typing import Any
from urllib.parse import urlencode

from catalyst_radar.connectors.base import (
    ConnectorHealth,
    ConnectorHealthStatus,
    ConnectorRecordKind,
    ConnectorRequest,
    NormalizedRecord,
    ProviderCostEstimate,
    RawRecord,
)
from catalyst_radar.connectors.http import JsonHttpClient, redact_url
from catalyst_radar.connectors.market_data import RejectedPayload
from catalyst_radar.core.immutability import thaw_json_value
from catalyst_radar.core.models import DataQualitySeverity

POLYGON_PROVIDER_NAME = "polygon"
POLYGON_LICENSE_TAG = "polygon-market-data"
POLYGON_RETENTION_POLICY = "retain-per-provider-license"


class PolygonEndpoint(StrEnum):
    GROUPED_DAILY = "polygon_grouped_daily"
    TICKERS = "polygon_tickers"


class PolygonMarketDataConnector:
    def __init__(
        self,
        *,
        api_key: str | None,
        client: JsonHttpClient,
        base_url: str = "https://api.polygon.io",
        provider: str = POLYGON_PROVIDER_NAME,
        availability_policy: str = "live_fetch",
        license_tag: str = POLYGON_LICENSE_TAG,
        retention_policy: str = POLYGON_RETENTION_POLICY,
    ) -> None:
        self.api_key = api_key.strip() if api_key is not None else None
        self.client = client
        self.base_url = base_url.rstrip("/")
        self.provider = provider
        self.availability_policy = availability_policy
        self.license_tag = license_tag
        self.retention_policy = retention_policy
        self._rejected_payloads: list[RejectedPayload] = []

    @property
    def rejected_payloads(self) -> tuple[RejectedPayload, ...]:
        return tuple(self._rejected_payloads)

    def fetch(self, request: ConnectorRequest) -> list[RawRecord]:
        self._rejected_payloads = []
        if not self.api_key:
            raise ValueError("missing CATALYST_POLYGON_API_KEY")
        endpoint = PolygonEndpoint(request.endpoint)
        if endpoint == PolygonEndpoint.GROUPED_DAILY:
            return self._fetch_grouped_daily(request)
        if endpoint == PolygonEndpoint.TICKERS:
            return self._fetch_tickers(request)
        msg = f"unsupported polygon endpoint: {request.endpoint}"
        raise ValueError(msg)

    def normalize(self, records: Sequence[RawRecord]) -> list[NormalizedRecord]:
        normalized: list[NormalizedRecord] = []
        for record in records:
            endpoint = str(record.payload.get("endpoint", ""))
            provider_record = _record_payload(record)
            if endpoint == PolygonEndpoint.GROUPED_DAILY.value:
                payload = _normalize_grouped_daily_payload(provider_record)
                identity = f"{payload['ticker']}:{payload['date']}"
                kind = ConnectorRecordKind.DAILY_BAR
            elif endpoint == PolygonEndpoint.TICKERS.value:
                payload = _normalize_security_payload(provider_record)
                identity = str(payload["ticker"])
                kind = ConnectorRecordKind.SECURITY
            else:
                continue
            normalized.append(
                NormalizedRecord(
                    provider=record.provider,
                    kind=kind,
                    identity=identity,
                    payload=payload,
                    source_ts=record.source_ts,
                    available_at=record.available_at,
                    raw_payload_hash=record.payload_hash,
                )
            )
        return normalized

    def healthcheck(self) -> ConnectorHealth:
        if not self.api_key:
            return ConnectorHealth(
                provider=self.provider,
                status=ConnectorHealthStatus.DOWN,
                checked_at=datetime.now(UTC),
                reason="missing CATALYST_POLYGON_API_KEY",
            )
        return ConnectorHealth(
            provider=self.provider,
            status=ConnectorHealthStatus.HEALTHY,
            checked_at=datetime.now(UTC),
            reason="polygon api key configured",
        )

    def estimate_cost(self, request: ConnectorRequest) -> ProviderCostEstimate:
        endpoint = PolygonEndpoint(request.endpoint)
        request_count = 1
        if endpoint == PolygonEndpoint.TICKERS:
            request_count = int(request.params.get("expected_pages", 1))
        return ProviderCostEstimate(
            provider=self.provider,
            request_count=request_count,
            estimated_cost_usd=0.0,
        )

    def _fetch_grouped_daily(self, request: ConnectorRequest) -> list[RawRecord]:
        fetched_at = datetime.now(UTC)
        date_value = str(request.params["date"])
        adjusted = _bool_param(request.params.get("adjusted", True))
        include_otc = _bool_param(request.params.get("include_otc", False))
        url = self._url(
            f"/v2/aggs/grouped/locale/us/market/stocks/{date_value}",
            {
                "adjusted": _url_bool(adjusted),
                "include_otc": _url_bool(include_otc),
            },
        )
        payload = self.client.get_json(url)
        request_hash = _hash_payload(
            {
                "provider": self.provider,
                "endpoint": request.endpoint,
                "params": thaw_json_value(request.params),
                "url": redact_url(url),
            }
        )
        if not _bool_param(payload.get("adjusted", False)):
            self._reject(
                ConnectorRecordKind.DAILY_BAR,
                _raw_payload(
                    PolygonEndpoint.GROUPED_DAILY,
                    {"ticker": date_value, "payload": payload},
                ),
                "grouped daily payload is not adjusted",
            )
            return []
        records = []
        for item in payload.get("results", []):
            record = _clean_mapping(item)
            ticker = str(record.get("T", "")).upper()
            raw_payload = _raw_payload(
                PolygonEndpoint.GROUPED_DAILY,
                {"ticker": ticker, "provider_payload": record},
            )
            try:
                _require_fields(record, ("T", "t", "o", "h", "l", "c", "v", "vw"))
                source_ts = _timestamp_ms(record["t"], "t")
                records.append(
                    RawRecord(
                        provider=self.provider,
                        kind=ConnectorRecordKind.DAILY_BAR,
                        request_hash=request_hash,
                        payload_hash=_hash_payload(raw_payload),
                        payload=raw_payload,
                        source_ts=source_ts,
                        fetched_at=fetched_at,
                        available_at=self._available_at(source_ts, fetched_at),
                        license_tag=self.license_tag,
                        retention_policy=self.retention_policy,
                    )
                )
            except (TypeError, ValueError) as exc:
                self._reject(ConnectorRecordKind.DAILY_BAR, raw_payload, str(exc))
        return records

    def _fetch_tickers(self, request: ConnectorRequest) -> list[RawRecord]:
        fetched_at = datetime.now(UTC)
        params = {
            "market": str(request.params.get("market", "stocks")),
            "active": _url_bool(_bool_param(request.params.get("active", True))),
            "limit": str(request.params.get("limit", 1000)),
        }
        url = self._url("/v3/reference/tickers", params)
        records = []
        while url:
            page_payload = self.client.get_json(url)
            request_hash = _hash_payload(
                {
                    "provider": self.provider,
                    "endpoint": request.endpoint,
                    "params": thaw_json_value(request.params),
                    "url": redact_url(url),
                }
            )
            for item in page_payload.get("results", []):
                record = _clean_mapping(item)
                ticker = str(record.get("ticker", "")).upper()
                raw_payload = _raw_payload(
                    PolygonEndpoint.TICKERS,
                    {"ticker": ticker, "provider_payload": record},
                )
                try:
                    _require_fields(record, ("ticker", "name", "primary_exchange", "active"))
                    records.append(
                        RawRecord(
                            provider=self.provider,
                            kind=ConnectorRecordKind.SECURITY,
                            request_hash=request_hash,
                            payload_hash=_hash_payload(raw_payload),
                            payload=raw_payload,
                            source_ts=fetched_at,
                            fetched_at=fetched_at,
                            available_at=fetched_at,
                            license_tag=self.license_tag,
                            retention_policy=self.retention_policy,
                        )
                    )
                except (TypeError, ValueError) as exc:
                    self._reject(ConnectorRecordKind.SECURITY, raw_payload, str(exc))
            next_url = page_payload.get("next_url")
            url = str(next_url) if next_url else ""
        return records

    def _url(self, path: str, params: Mapping[str, Any]) -> str:
        all_params = {**params, "apiKey": self.api_key}
        return f"{self.base_url}{path}?{urlencode(all_params)}"

    def _available_at(self, source_ts: datetime, fetched_at: datetime) -> datetime:
        if self.availability_policy == "live_fetch":
            return fetched_at
        if self.availability_policy == "next_session_11_utc":
            return datetime.combine(source_ts.date() + timedelta(days=1), time(11), tzinfo=UTC)
        msg = f"unsupported provider availability policy: {self.availability_policy}"
        raise ValueError(msg)

    def _reject(self, kind: ConnectorRecordKind, payload: Mapping[str, Any], reason: str) -> None:
        self._rejected_payloads.append(
            RejectedPayload(
                provider=self.provider,
                kind=kind,
                payload=payload,
                reason=reason,
                severity=DataQualitySeverity.ERROR,
                fail_closed_action="reject-payload",
            )
        )


def _record_payload(record: RawRecord) -> Mapping[str, Any]:
    payload = record.payload.get("record")
    if not isinstance(payload, Mapping):
        msg = "raw polygon payload must contain a record mapping"
        raise ValueError(msg)
    return payload


def _normalize_grouped_daily_payload(record: Mapping[str, Any]) -> dict[str, Any]:
    source = _provider_payload(record)
    source_ts = _timestamp_ms(source["t"], "t")
    return {
        "ticker": str(record["ticker"]).upper(),
        "date": source_ts.date().isoformat(),
        "open": float(source["o"]),
        "high": float(source["h"]),
        "low": float(source["l"]),
        "close": float(source["c"]),
        "volume": int(source["v"]),
        "vwap": float(source["vw"]),
        "adjusted": True,
        "provider": POLYGON_PROVIDER_NAME,
        "source_ts": source_ts.isoformat(),
        "available_at": source_ts.isoformat(),
        "metadata": {"provider_record": "grouped_daily"},
    }


def _normalize_security_payload(record: Mapping[str, Any]) -> dict[str, Any]:
    source = _provider_payload(record)
    sector = str(source.get("sic_description") or source.get("sector") or "Unknown")
    industry = str(source.get("industry") or source.get("sic_description") or "Unknown")
    return {
        "ticker": str(record["ticker"]).upper(),
        "name": str(source["name"]),
        "exchange": str(source["primary_exchange"]),
        "sector": sector,
        "industry": industry,
        "market_cap": float(source.get("market_cap") or 0.0),
        "avg_dollar_volume_20d": float(source.get("avg_dollar_volume_20d") or 0.0),
        "has_options": bool(source.get("has_options") or False),
        "is_active": bool(source["active"]),
        "updated_at": datetime.now(UTC).isoformat(),
        "metadata_source": "polygon_reference",
        "metadata": {
            "type": source.get("type"),
            "market": source.get("market"),
            "locale": source.get("locale"),
            "currency_name": source.get("currency_name"),
            "cik": source.get("cik"),
            "composite_figi": source.get("composite_figi"),
        },
    }


def _provider_payload(record: Mapping[str, Any]) -> Mapping[str, Any]:
    payload = record.get("provider_payload")
    if not isinstance(payload, Mapping):
        msg = "polygon record missing provider_payload"
        raise ValueError(msg)
    return payload


def _raw_payload(endpoint: PolygonEndpoint, record: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "source": POLYGON_PROVIDER_NAME,
        "endpoint": endpoint.value,
        "record": dict(record),
    }


def _require_fields(record: Mapping[str, Any], fields: tuple[str, ...]) -> None:
    missing = [field for field in fields if _is_missing(record.get(field))]
    if missing:
        msg = f"missing mandatory field(s): {', '.join(missing)}"
        raise ValueError(msg)


def _is_missing(value: Any) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())


def _timestamp_ms(value: Any, field: str) -> datetime:
    try:
        timestamp = int(value)
    except (TypeError, ValueError) as exc:
        msg = f"{field} must be a millisecond timestamp"
        raise ValueError(msg) from exc
    return datetime.fromtimestamp(timestamp / 1000, UTC)


def _bool_param(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    return bool(value)


def _url_bool(value: bool) -> str:
    return "true" if value else "false"


def _clean_mapping(value: Mapping[str, Any]) -> dict[str, Any]:
    return {str(key): item for key, item in value.items()}


def _hash_payload(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return sha256(encoded).hexdigest()


__all__ = [
    "POLYGON_LICENSE_TAG",
    "POLYGON_PROVIDER_NAME",
    "POLYGON_RETENTION_POLICY",
    "PolygonEndpoint",
    "PolygonMarketDataConnector",
]

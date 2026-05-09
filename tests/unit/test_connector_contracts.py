from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta

import pytest

from catalyst_radar.connectors.base import (
    ConnectorHealth,
    ConnectorHealthStatus,
    ConnectorRecordKind,
    ConnectorRequest,
    MarketDataConnector,
    NormalizedRecord,
    ProviderCostEstimate,
    RawRecord,
)
from catalyst_radar.core.models import DataQualitySeverity, JobStatus

SOURCE_TS = datetime(2026, 5, 10, 9, 30, tzinfo=UTC)
FETCHED_AT = SOURCE_TS + timedelta(seconds=1)
AVAILABLE_AT = SOURCE_TS + timedelta(minutes=15)


def test_core_provider_enums_are_available() -> None:
    assert DataQualitySeverity.INFO.value == "info"
    assert DataQualitySeverity.CRITICAL.value == "critical"
    assert JobStatus.RUNNING.value == "running"
    assert JobStatus.PARTIAL_SUCCESS.value == "partial_success"


def test_event_connector_record_kinds_are_available() -> None:
    assert ConnectorRecordKind.EVENT.value == "event"
    assert ConnectorRecordKind.SEC_FILING.value == "sec_filing"
    assert ConnectorRecordKind.NEWS_ARTICLE.value == "news_article"
    assert ConnectorRecordKind.EARNINGS_EVENT.value == "earnings_event"
    assert ConnectorRecordKind.OPTION_FEATURE.value == "option_feature"


def test_connector_request_rejects_blank_provider_and_missing_or_naive_timestamp() -> None:
    with pytest.raises(ValueError, match="provider"):
        ConnectorRequest(
            provider=" ",
            endpoint="/securities",
            params={},
            requested_at=SOURCE_TS,
        )

    with pytest.raises(ValueError, match="endpoint"):
        ConnectorRequest(
            provider="csv",
            endpoint=" ",
            params={},
            requested_at=SOURCE_TS,
        )

    with pytest.raises(ValueError, match="requested_at"):
        ConnectorRequest(
            provider="csv",
            endpoint="/securities",
            params={},
            requested_at=datetime(2026, 5, 10, 9, 30),
        )

    with pytest.raises(ValueError, match="requested_at"):
        ConnectorRequest(
            provider="csv",
            endpoint="/securities",
            params={},
            requested_at=None,  # type: ignore[arg-type]
        )


def test_raw_record_rejects_blank_hashes_and_invalid_timestamps() -> None:
    with pytest.raises(ValueError, match="request_hash"):
        raw_record(request_hash=" ")

    with pytest.raises(ValueError, match="payload_hash"):
        raw_record(payload_hash="")

    with pytest.raises(ValueError, match="license_tag"):
        raw_record(license_tag=" ")

    with pytest.raises(ValueError, match="retention_policy"):
        raw_record(retention_policy="")

    with pytest.raises(ValueError, match="source_ts"):
        raw_record(source_ts=datetime(2026, 5, 10, 9, 30))

    with pytest.raises(ValueError, match="fetched_at"):
        raw_record(fetched_at=SOURCE_TS - timedelta(seconds=1))

    with pytest.raises(ValueError, match="available_at"):
        raw_record(available_at=SOURCE_TS - timedelta(seconds=1))


def test_normalized_record_rejects_blank_provider_hashes_and_invalid_timestamps() -> None:
    with pytest.raises(ValueError, match="provider"):
        normalized_record(provider="")

    with pytest.raises(ValueError, match="identity"):
        normalized_record(identity=" ")

    with pytest.raises(ValueError, match="raw_payload_hash"):
        normalized_record(raw_payload_hash=" ")

    with pytest.raises(ValueError, match="source_ts"):
        normalized_record(source_ts=None)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="available_at"):
        normalized_record(available_at=SOURCE_TS - timedelta(seconds=1))


def test_health_rejects_blank_provider_and_naive_checked_at() -> None:
    with pytest.raises(ValueError, match="provider"):
        ConnectorHealth(
            provider="",
            status=ConnectorHealthStatus.HEALTHY,
            checked_at=SOURCE_TS,
            reason="ok",
        )

    with pytest.raises(ValueError, match="checked_at"):
        ConnectorHealth(
            provider="csv",
            status=ConnectorHealthStatus.HEALTHY,
            checked_at=datetime(2026, 5, 10, 9, 30),
            reason="ok",
        )


def test_payload_and_params_are_defensively_copied_and_immutable() -> None:
    params = {"ticker": "MSFT", "filters": {"tickers": ["MSFT"]}}
    request = ConnectorRequest(
        provider="csv",
        endpoint="/securities",
        params=params,
        requested_at=SOURCE_TS,
    )
    params["ticker"] = "AAPL"
    params["filters"]["tickers"].append("AAPL")

    assert request.params["ticker"] == "MSFT"
    assert request.params["filters"]["tickers"] == ("MSFT",)
    with pytest.raises(TypeError):
        request.params["ticker"] = "NVDA"  # type: ignore[index]
    with pytest.raises(TypeError):
        request.params["filters"]["tickers"][0] = "NVDA"  # type: ignore[index]

    payload = {"ticker": "MSFT", "source": {"fields": ["ticker"]}}
    raw = raw_record(payload=payload)
    normalized = normalized_record(payload=payload)
    payload["ticker"] = "AAPL"
    payload["source"]["fields"].append("close")

    assert raw.payload["ticker"] == "MSFT"
    assert raw.payload["source"]["fields"] == ("ticker",)
    assert normalized.payload["ticker"] == "MSFT"
    assert normalized.payload["source"]["fields"] == ("ticker",)
    with pytest.raises(TypeError):
        raw.payload["ticker"] = "NVDA"  # type: ignore[index]
    with pytest.raises(TypeError):
        normalized.payload["ticker"] = "NVDA"  # type: ignore[index]


def test_market_data_connector_protocol_allows_zero_cost_dry_run_csv() -> None:
    connector: MarketDataConnector = DryRunConnector()
    request = ConnectorRequest(
        provider="csv",
        endpoint="fixture",
        params={"path": "data/sample/securities.csv"},
        requested_at=SOURCE_TS,
    )

    estimate = connector.estimate_cost(request)

    assert estimate.provider == "csv"
    assert estimate.request_count == 1
    assert estimate.estimated_cost_usd == 0.0
    assert estimate.currency == "USD"


def test_provider_cost_estimate_rejects_negative_counts_costs_and_blank_currency() -> None:
    with pytest.raises(ValueError, match="request_count"):
        ProviderCostEstimate(
            provider="csv",
            request_count=-1,
            estimated_cost_usd=0,
        )

    with pytest.raises(ValueError, match="estimated_cost_usd"):
        ProviderCostEstimate(
            provider="csv",
            request_count=1,
            estimated_cost_usd=-0.01,
        )

    with pytest.raises(ValueError, match="currency"):
        ProviderCostEstimate(
            provider="csv",
            request_count=1,
            estimated_cost_usd=0,
            currency=" ",
        )


def raw_record(**overrides: object) -> RawRecord:
    values = {
        "provider": "csv",
        "kind": ConnectorRecordKind.SECURITY,
        "request_hash": "request-hash",
        "payload_hash": "payload-hash",
        "payload": {"ticker": "MSFT"},
        "source_ts": SOURCE_TS,
        "fetched_at": FETCHED_AT,
        "available_at": AVAILABLE_AT,
        "license_tag": "internal-test",
        "retention_policy": "fixture",
    }
    values.update(overrides)
    return RawRecord(**values)  # type: ignore[arg-type]


def normalized_record(**overrides: object) -> NormalizedRecord:
    values = {
        "provider": "csv",
        "kind": ConnectorRecordKind.SECURITY,
        "identity": "MSFT",
        "payload": {"ticker": "MSFT"},
        "source_ts": SOURCE_TS,
        "available_at": AVAILABLE_AT,
        "raw_payload_hash": "payload-hash",
    }
    values.update(overrides)
    return NormalizedRecord(**values)  # type: ignore[arg-type]


class DryRunConnector:
    def fetch(self, request: ConnectorRequest) -> list[RawRecord]:
        return [raw_record(provider=request.provider)]

    def normalize(self, records: Sequence[RawRecord]) -> list[NormalizedRecord]:
        return [
            normalized_record(
                provider=record.provider,
                kind=record.kind,
                raw_payload_hash=record.payload_hash,
            )
            for record in records
        ]

    def healthcheck(self) -> ConnectorHealth:
        return ConnectorHealth(
            provider="csv",
            status=ConnectorHealthStatus.HEALTHY,
            checked_at=SOURCE_TS,
            reason="fixture available",
        )

    def estimate_cost(self, request: ConnectorRequest) -> ProviderCostEstimate:
        return ProviderCostEstimate(
            provider=request.provider,
            request_count=1,
            estimated_cost_usd=0.0,
        )

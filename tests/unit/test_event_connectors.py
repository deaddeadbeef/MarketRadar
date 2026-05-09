from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from catalyst_radar.connectors.base import (
    ConnectorHealthStatus,
    ConnectorRecordKind,
    ConnectorRequest,
)
from catalyst_radar.connectors.earnings import EarningsCalendarConnector
from catalyst_radar.connectors.news import NewsJsonConnector
from catalyst_radar.connectors.sec import SecSubmissionsConnector


def test_sec_submissions_fixture_normalizes_recent_filings() -> None:
    connector = SecSubmissionsConnector(
        fixture_path=Path("tests/fixtures/sec/submissions_msft.json")
    )
    request = ConnectorRequest(
        provider="sec",
        endpoint="submissions",
        params={"ticker": "MSFT", "cik": "0000789019"},
        requested_at=datetime(2026, 5, 10, 14, tzinfo=UTC),
    )

    raw = connector.fetch(request)
    normalized = connector.normalize(raw)

    assert len(raw) == 2
    assert len(normalized) == 2
    assert raw[0].kind == ConnectorRecordKind.SEC_FILING
    assert raw[0].license_tag == "sec-public"
    assert raw[0].retention_policy == "retain-fixture"
    assert normalized[0].kind == ConnectorRecordKind.EVENT
    assert normalized[0].identity == normalized[0].payload["dedupe_key"]
    assert normalized[0].payload["ticker"] == "MSFT"
    assert normalized[0].payload["event_type"] in {"guidance", "sec_filing"}
    assert normalized[0].payload["source_category"] == "primary_source"
    assert normalized[0].payload["payload"]["form_type"] == "8-K"


def test_news_fixture_dedupes_tracking_url_payloads() -> None:
    connector = NewsJsonConnector(fixture_path=Path("tests/fixtures/news/ticker_news_msft.json"))
    request = ConnectorRequest(
        provider="news_fixture",
        endpoint="ticker-news",
        params={"ticker": "MSFT"},
        requested_at=datetime(2026, 5, 10, 14, tzinfo=UTC),
    )

    raw = connector.fetch(request)
    normalized = connector.normalize(raw)

    dedupe_keys = {record.payload["dedupe_key"] for record in normalized}
    assert len(raw) == 2
    assert len(dedupe_keys) == len(normalized)
    assert all(record.kind == ConnectorRecordKind.NEWS_ARTICLE for record in raw)
    assert all(record.kind == ConnectorRecordKind.EVENT for record in normalized)
    assert all("utm_source" not in str(record.payload["source_url"]) for record in normalized)
    assert normalized[0].payload["event_type"] == "guidance"
    assert normalized[1].payload["payload"]["requires_confirmation"] is True


def test_earnings_fixture_marks_upcoming_event_risk() -> None:
    connector = EarningsCalendarConnector(
        fixture_path=Path("tests/fixtures/earnings/calendar_msft.json")
    )
    request = ConnectorRequest(
        provider="earnings_fixture",
        endpoint="earnings-calendar",
        params={"ticker": "MSFT"},
        requested_at=datetime(2026, 5, 10, 14, tzinfo=UTC),
    )

    raw = connector.fetch(request)
    normalized = connector.normalize(raw)

    assert raw[0].kind == ConnectorRecordKind.EARNINGS_EVENT
    assert raw[0].license_tag == "earnings-fixture"
    assert raw[0].retention_policy == "retain-fixture"
    assert normalized[0].kind == ConnectorRecordKind.EVENT
    assert normalized[0].payload["event_type"] == "earnings"
    assert normalized[0].payload["payload"]["event_risk"] == "upcoming_earnings"
    assert normalized[0].payload["payload"]["event_date"] == "2026-05-15"


def test_fixture_healthchecks_report_missing_paths() -> None:
    missing = Path("tests/fixtures/missing-events.json")

    assert (
        SecSubmissionsConnector(fixture_path=missing).healthcheck().status
        == ConnectorHealthStatus.DOWN
    )
    assert (
        NewsJsonConnector(fixture_path=missing).healthcheck().status
        == ConnectorHealthStatus.DOWN
    )
    assert (
        EarningsCalendarConnector(fixture_path=missing).healthcheck().status
        == ConnectorHealthStatus.DOWN
    )

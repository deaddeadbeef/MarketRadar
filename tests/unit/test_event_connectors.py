from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from catalyst_radar.connectors.base import (
    ConnectorHealthStatus,
    ConnectorRecordKind,
    ConnectorRequest,
)
from catalyst_radar.connectors.earnings import EarningsCalendarConnector
from catalyst_radar.connectors.http import FakeHttpTransport, HttpResponse
from catalyst_radar.connectors.news import NewsJsonConnector
from catalyst_radar.connectors.provider_ingest import _event_from_payload
from catalyst_radar.connectors.sec import SecSubmissionsConnector
from catalyst_radar.events.conflicts import detect_event_conflicts


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
    assert raw[0].retention_policy == "public-sec-retain"
    assert normalized[0].kind == ConnectorRecordKind.EVENT
    assert normalized[0].identity == normalized[0].payload["dedupe_key"]
    assert normalized[0].payload["ticker"] == "MSFT"
    assert normalized[0].payload["event_type"] in {"guidance", "sec_filing"}
    assert normalized[0].payload["source_category"] == "primary_source"
    assert normalized[0].payload["payload"]["form_type"] == "8-K"


def test_sec_ipo_s1_downloads_document_and_normalizes_offer_analysis() -> None:
    connector = SecSubmissionsConnector(
        fixture_path=Path("tests/fixtures/sec/submissions_acme_s1.json"),
        document_fixture_path=Path("tests/fixtures/sec/acme_s1.htm"),
    )
    request = ConnectorRequest(
        provider="sec",
        endpoint="ipo-s1",
        params={"ticker": "ACME", "cik": "0002000001"},
        requested_at=datetime(2026, 5, 10, 14, tzinfo=UTC),
    )

    raw = connector.fetch(request)
    normalized = connector.normalize(raw)

    assert len(raw) == 1
    assert raw[0].kind == ConnectorRecordKind.SEC_FILING
    assert raw[0].payload["record"]["document_downloaded"] is True
    assert "We are offering 12,500,000 shares" in raw[0].payload["record"]["document_text"]
    assert raw[0].payload["record"]["document_text_hash"]
    assert len(normalized) == 1
    payload = normalized[0].payload
    event_payload = payload["payload"]
    analysis = event_payload["ipo_analysis"]
    assert payload["event_type"] == "financing"
    assert payload["source_category"] == "primary_source"
    assert payload["source_url"].endswith("/acme-20260510xs1.htm")
    assert event_payload["form_type"] == "S-1"
    assert list(event_payload["classification_reasons"]) == [
        "sec_form_s-1",
        "ipo_registration_statement",
    ]
    assert event_payload["requires_text_triage"] is True
    assert event_payload["document_text_hash"] == raw[0].payload["record"]["document_text_hash"]
    assert "estimated gross proceeds" in event_payload["summary"]
    assert "Acme Robotics" in event_payload["body"]
    assert analysis["proposed_ticker"] == "ACME"
    assert analysis["exchange"] == "Nasdaq Global Select Market"
    assert analysis["shares_offered"] == 12_500_000
    assert analysis["price_range_low"] == 17.0
    assert analysis["price_range_high"] == 19.0
    assert analysis["estimated_gross_proceeds"] == 225_000_000.0
    assert list(analysis["underwriters"]) == [
        "Morgan Stanley & Co. LLC",
        "Goldman Sachs & Co. LLC",
    ]
    assert "history_of_losses" in analysis["risk_flags"]


def test_sec_ipo_s1_downloads_public_document_with_http_transport() -> None:
    document_url = (
        "https://www.sec.gov/Archives/edgar/data/"
        "2000001/000200000126000001/acme-20260510xs1.htm"
    )
    document_text = Path("tests/fixtures/sec/acme_s1.htm").read_bytes()
    transport = FakeHttpTransport(
        {
            document_url: HttpResponse(
                status_code=200,
                url=document_url,
                headers={"content-type": "text/html"},
                body=document_text,
            )
        }
    )
    connector = SecSubmissionsConnector(
        fixture_path=Path("tests/fixtures/sec/submissions_acme_s1.json"),
        document_transport=transport,
        document_headers={"User-Agent": "MarketRadar test contact@example.com"},
    )
    request = ConnectorRequest(
        provider="sec",
        endpoint="ipo-s1",
        params={"ticker": "ACME", "cik": "0002000001"},
        requested_at=datetime(2026, 5, 10, 14, tzinfo=UTC),
    )

    raw = connector.fetch(request)
    normalized = connector.normalize(raw)

    assert transport.requests == [document_url]
    assert raw[0].payload["record"]["document_source"] == "sec_archive"
    assert raw[0].payload["record"]["document_downloaded"] is True
    assert normalized[0].payload["payload"]["ipo_analysis"]["proposed_ticker"] == "ACME"


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
    assert normalized[0].payload["event_type"] == "earnings"
    assert normalized[1].payload["payload"]["requires_confirmation"] is True


def test_news_promotional_source_name_overrides_reputable_category(tmp_path: Path) -> None:
    fixture_path = tmp_path / "promo_news.json"
    fixture_path.write_text(
        json.dumps(
            {
                "ticker": "MSFT",
                "articles": [
                    {
                        "source": "Sponsored Stocks Daily",
                        "source_category": "reputable_news",
                        "title": "MSFT could double soon",
                        "body": "Sponsored promotional recap.",
                        "url": "https://promo.example.com/msft",
                        "published_at": "2026-05-10T12:31:00Z",
                        "available_at": "2026-05-10T12:31:00Z",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    connector = NewsJsonConnector(fixture_path=fixture_path)
    request = ConnectorRequest(
        provider="news_fixture",
        endpoint="ticker-news",
        params={"ticker": "MSFT"},
        requested_at=datetime(2026, 5, 10, 14, tzinfo=UTC),
    )

    normalized = connector.normalize(connector.fetch(request))

    payload = normalized[0].payload
    assert payload["source_quality"] <= 0.2
    assert payload["materiality"] <= 0.35
    assert payload["payload"]["requires_confirmation"] is True


def test_news_mailchimp_tracking_params_share_dedupe_key(tmp_path: Path) -> None:
    fixture_path = tmp_path / "mailchimp_news.json"
    article = {
        "source": "Reuters",
        "source_category": "reputable_news",
        "title": "Microsoft raises cloud guidance",
        "body": "Microsoft raises cloud guidance after stronger demand.",
        "published_at": "2026-05-10T12:30:00Z",
        "available_at": "2026-05-10T12:35:00Z",
    }
    fixture_path.write_text(
        json.dumps(
            {
                "ticker": "MSFT",
                "articles": [
                    {
                        **article,
                        "url": "https://reuters.example.com/markets/msft-cloud?mc_cid=aaa&id=123",
                    },
                    {
                        **article,
                        "url": "https://reuters.example.com/markets/msft-cloud?id=123&mc_eid=bbb",
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    connector = NewsJsonConnector(fixture_path=fixture_path)
    request = ConnectorRequest(
        provider="news_fixture",
        endpoint="ticker-news",
        params={"ticker": "MSFT"},
        requested_at=datetime(2026, 5, 10, 14, tzinfo=UTC),
    )

    normalized = connector.normalize(connector.fetch(request))

    assert normalized[0].payload["dedupe_key"] == normalized[1].payload["dedupe_key"]
    assert normalized[0].payload["source_url"] == normalized[1].payload["source_url"]
    assert normalized[0].payload["source_url"].endswith("?id=123")


def test_news_body_is_preserved_for_guidance_conflict_detection(tmp_path: Path) -> None:
    fixture_path = tmp_path / "conflicting_news.json"
    fixture_path.write_text(
        json.dumps(
            {
                "ticker": "MSFT",
                "articles": [
                    {
                        "source": "Reuters",
                        "source_category": "reputable_news",
                        "title": "Microsoft updates annual outlook",
                        "body": "Microsoft raises guidance for fiscal 2026.",
                        "url": "https://reuters.example.com/a",
                        "published_at": "2026-05-10T12:30:00Z",
                        "available_at": "2026-05-10T12:31:00Z",
                    },
                    {
                        "source": "Reuters",
                        "source_category": "reputable_news",
                        "title": "Microsoft updates annual outlook again",
                        "body": "Microsoft cuts guidance for fiscal 2026.",
                        "url": "https://reuters.example.com/b",
                        "published_at": "2026-05-10T12:32:00Z",
                        "available_at": "2026-05-10T12:33:00Z",
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    connector = NewsJsonConnector(fixture_path=fixture_path)
    request = ConnectorRequest(
        provider="news_fixture",
        endpoint="ticker-news",
        params={"ticker": "MSFT"},
        requested_at=datetime(2026, 5, 10, 14, tzinfo=UTC),
    )

    events = [
        _event_from_payload(record.payload)
        for record in connector.normalize(connector.fetch(request))
    ]

    assert [event.event_type.value for event in events] == ["guidance", "guidance"]
    assert detect_event_conflicts(events) == (
        {
            "ticker": "MSFT",
            "conflict_type": "guidance_direction_conflict",
            "source_event_ids": [events[0].id, events[1].id],
        },
    )


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
    assert raw[0].retention_policy == "fixture-retain"
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

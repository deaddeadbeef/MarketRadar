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
from catalyst_radar.connectors.sec import (
    FIXTURE_RETENTION_POLICY,
    _canonical_event_payload,
    _event_id,
    _hash_payload,
    _mapping,
    _parse_datetime,
    _raw_payload,
    body_hash,
    canonicalize_url,
    dedupe_key,
)
from catalyst_radar.core.immutability import thaw_json_value

NEWS_PROVIDER_NAME = "news_fixture"
NEWS_LICENSE_TAG = "news-fixture"


class NewsJsonConnector:
    def __init__(
        self,
        *,
        fixture_path: str | Path,
        provider: str = NEWS_PROVIDER_NAME,
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
        articles = payload.get("articles")
        if not isinstance(articles, list):
            msg = "news fixture articles must be a list"
            raise ValueError(msg)
        records: list[RawRecord] = []
        for article in articles:
            article_payload = dict(_mapping(article, "article"))
            source_ts = _parse_datetime(article_payload.get("published_at"), "published_at")
            available_at = _parse_datetime(article_payload.get("available_at"), "available_at")
            raw_payload = _raw_payload(
                ConnectorRecordKind.NEWS_ARTICLE,
                {"ticker": ticker, "record": article_payload},
            )
            records.append(
                RawRecord(
                    provider=self.provider,
                    kind=ConnectorRecordKind.NEWS_ARTICLE,
                    request_hash=request_hash,
                    payload_hash=_hash_payload(raw_payload),
                    payload=raw_payload,
                    source_ts=source_ts,
                    fetched_at=max(fetched_at, source_ts),
                    available_at=available_at,
                    license_tag=NEWS_LICENSE_TAG,
                    retention_policy=FIXTURE_RETENTION_POLICY,
                )
            )
        return records

    def normalize(self, records: Sequence[RawRecord]) -> list[NormalizedRecord]:
        normalized: list[NormalizedRecord] = []
        for record in records:
            if record.kind != ConnectorRecordKind.NEWS_ARTICLE:
                continue
            payload = _mapping(record.payload.get("record"), "record")
            article = _mapping(payload.get("record"), "article")
            ticker = str(payload["ticker"]).upper()
            title = str(article.get("title") or "").strip()
            body = str(article.get("body") or "")
            category = str(article.get("source_category") or "unknown")
            source = str(article.get("source") or "Unknown")
            canonical_url = canonicalize_url(str(article.get("url") or ""))
            content_hash = body_hash(f"{title} {body}")
            dedupe = dedupe_key(
                ticker=ticker,
                provider=record.provider,
                canonical_url=canonical_url,
                content_hash=content_hash,
            )
            event_type, materiality, reasons, requires_confirmation = _classify_news(
                title=title,
                body=body,
                source_category=category,
            )
            quality = _source_quality(category)
            event_payload = _canonical_event_payload(
                event_id=_event_id(dedupe),
                ticker=ticker,
                event_type=event_type,
                provider=record.provider,
                source=source,
                source_category=category,
                source_url=canonical_url,
                title=title,
                body_hash_value=content_hash,
                dedupe=dedupe,
                source_quality=quality,
                materiality=materiality,
                source_ts=record.source_ts,
                available_at=record.available_at,
                payload={
                    "classification_reasons": reasons,
                    "requires_confirmation": requires_confirmation,
                    "published_at": article.get("published_at"),
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
                reason="news fixture path is readable",
            )
        return ConnectorHealth(
            provider=self.provider,
            status=ConnectorHealthStatus.DOWN,
            checked_at=datetime.now(UTC),
            reason=f"missing news fixture path: {self.fixture_path}",
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


def _classify_news(
    *,
    title: str,
    body: str,
    source_category: str,
) -> tuple[str, float, list[str], bool]:
    combined = f"{title} {body}".lower()
    if source_category == "promotional":
        return "news", 0.25, ["promotional_source"], True
    if "guidance" in combined or "raises" in combined or "cuts" in combined:
        return "guidance", 0.82, ["guidance_language"], False
    if "earnings" in combined:
        return "earnings", 0.7, ["earnings_language"], False
    return "news", 0.45, ["news_article"], False


def _source_quality(source_category: str) -> float:
    return {
        "primary_source": 1.0,
        "regulatory": 0.95,
        "reputable_news": 0.85,
        "company_press_release": 0.8,
        "analyst_provider": 0.7,
        "aggregator": 0.55,
        "social": 0.25,
        "promotional": 0.15,
        "unknown": 0.35,
    }.get(source_category, 0.35)


__all__ = [
    "NEWS_LICENSE_TAG",
    "NEWS_PROVIDER_NAME",
    "NewsJsonConnector",
]

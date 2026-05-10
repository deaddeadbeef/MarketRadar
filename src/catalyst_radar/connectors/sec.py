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
from catalyst_radar.connectors.http import JsonHttpClient
from catalyst_radar.core.immutability import thaw_json_value
from catalyst_radar.events.dedupe import body_hash, canonicalize_url, dedupe_key

SEC_PROVIDER_NAME = "sec"
SEC_LICENSE_TAG = "sec-public"
SEC_RETENTION_POLICY = "public-sec-retain"
FIXTURE_RETENTION_POLICY = "fixture-retain"


class SecSubmissionsConnector:
    def __init__(
        self,
        *,
        fixture_path: str | Path | None = None,
        client: JsonHttpClient | None = None,
        base_url: str = "https://data.sec.gov",
        provider: str = SEC_PROVIDER_NAME,
    ) -> None:
        self.fixture_path = Path(fixture_path) if fixture_path is not None else None
        self.client = client
        self.base_url = base_url.rstrip("/")
        self.provider = provider

    def fetch(self, request: ConnectorRequest) -> list[RawRecord]:
        payload = self._load_payload(request)
        fetched_at = request.requested_at
        request_hash = _hash_payload(
            {
                "provider": request.provider,
                "endpoint": request.endpoint,
                "params": thaw_json_value(request.params),
                "fixture_path": str(self.fixture_path) if self.fixture_path else None,
            }
        )
        records: list[RawRecord] = []
        ticker = str(request.params.get("ticker") or payload.get("ticker") or "").upper()
        cik = str(request.params.get("cik") or payload.get("cik") or "")
        recent = _mapping(_mapping(payload.get("filings"), "filings").get("recent"), "recent")
        count = max((len(value) for value in recent.values() if isinstance(value, list)), default=0)
        for index in range(count):
            filing = _recent_filing(recent, index)
            source_ts = _parse_datetime(
                filing.get("acceptanceDateTime") or filing.get("filingDate"),
                "acceptanceDateTime",
            )
            raw_payload = _raw_payload(
                ConnectorRecordKind.SEC_FILING,
                {
                    "ticker": ticker,
                    "cik": cik,
                    "company_name": payload.get("name"),
                    "record": filing,
                },
            )
            records.append(
                RawRecord(
                    provider=self.provider,
                    kind=ConnectorRecordKind.SEC_FILING,
                    request_hash=request_hash,
                    payload_hash=_hash_payload(raw_payload),
                    payload=raw_payload,
                    source_ts=source_ts,
                    fetched_at=max(fetched_at, source_ts),
                    available_at=(
                        source_ts
                        if self.fixture_path is not None
                        else max(fetched_at, source_ts)
                    ),
                    license_tag=SEC_LICENSE_TAG,
                    retention_policy=SEC_RETENTION_POLICY,
                )
            )
        return records

    def normalize(self, records: Sequence[RawRecord]) -> list[NormalizedRecord]:
        normalized: list[NormalizedRecord] = []
        for record in records:
            if record.kind != ConnectorRecordKind.SEC_FILING:
                continue
            payload = _mapping(record.payload.get("record"), "record")
            filing = _mapping(payload.get("record"), "filing")
            ticker = str(payload["ticker"]).upper()
            cik = str(payload.get("cik") or "")
            form_type = str(filing.get("form") or "").upper()
            accession = str(filing.get("accessionNumber") or "")
            document = str(filing.get("primaryDocument") or "")
            items = str(filing.get("items") or "")
            source_url = _sec_filing_url(cik, accession, document)
            title = f"{ticker} {form_type}".strip()
            body = " ".join(part for part in (title, items) if part)
            content_hash = body_hash(body)
            canonical_url = canonicalize_url(source_url)
            dedupe = dedupe_key(
                ticker=ticker,
                provider=record.provider,
                canonical_url=canonical_url,
                content_hash=content_hash,
            )
            event_type, materiality, reasons, requires_text_triage = _classify_sec(
                form_type=form_type,
                title=title,
                body=body,
            )
            event_payload = _canonical_event_payload(
                event_id=_event_id(dedupe),
                ticker=ticker,
                event_type=event_type,
                provider=record.provider,
                source="SEC EDGAR",
                source_category="primary_source",
                source_url=canonical_url,
                title=title,
                body_hash_value=content_hash,
                dedupe=dedupe,
                source_quality=1.0,
                materiality=materiality,
                source_ts=record.source_ts,
                available_at=record.available_at,
                payload={
                    "accession_number": accession,
                    "cik": cik,
                    "form_type": form_type,
                    "filing_date": filing.get("filingDate"),
                    "primary_document": document,
                    "items": items,
                    "classification_reasons": reasons,
                    "requires_text_triage": requires_text_triage,
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
        if self.fixture_path is not None:
            if self.fixture_path.exists():
                return ConnectorHealth(
                    provider=self.provider,
                    status=ConnectorHealthStatus.HEALTHY,
                    checked_at=datetime.now(UTC),
                    reason="sec fixture path is readable",
                )
            return ConnectorHealth(
                provider=self.provider,
                status=ConnectorHealthStatus.DOWN,
                checked_at=datetime.now(UTC),
                reason=f"missing sec fixture path: {self.fixture_path}",
            )
        if self.client is None:
            return ConnectorHealth(
                provider=self.provider,
                status=ConnectorHealthStatus.DOWN,
                checked_at=datetime.now(UTC),
                reason="sec client is not configured",
            )
        return ConnectorHealth(
            provider=self.provider,
            status=ConnectorHealthStatus.HEALTHY,
            checked_at=datetime.now(UTC),
            reason="sec client configured",
        )

    def estimate_cost(self, request: ConnectorRequest) -> ProviderCostEstimate:
        return ProviderCostEstimate(
            provider=request.provider,
            request_count=1,
            estimated_cost_usd=0.0,
        )

    def _load_payload(self, request: ConnectorRequest) -> Mapping[str, Any]:
        if self.fixture_path is not None:
            with self.fixture_path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
            return _mapping(payload, "fixture")
        if self.client is None:
            msg = "sec connector requires fixture_path or client"
            raise ValueError(msg)
        cik = str(request.params["cik"]).zfill(10)
        return self.client.get_json(f"{self.base_url}/submissions/CIK{cik}.json")


def _classify_sec(
    *,
    form_type: str,
    title: str,
    body: str,
) -> tuple[str, float, list[str], bool]:
    combined = f"{title} {body}".lower()
    if form_type == "8-K" and ("guidance" in combined or "item 2.02" in combined):
        return "guidance", 0.85, ["sec_form_8k", "guidance_language"], True
    if form_type == "8-K":
        return "sec_filing", 0.75, ["sec_form_8k"], True
    if form_type in {"10-Q", "10-K"}:
        return "sec_filing", 0.65, [f"sec_form_{form_type.lower()}"], False
    return "sec_filing", 0.5, ["sec_filing"], False


def _recent_filing(recent: Mapping[str, Any], index: int) -> dict[str, Any]:
    return {
        key: value[index] if isinstance(value, list) and index < len(value) else None
        for key, value in recent.items()
    }


def _sec_filing_url(cik: str, accession: str, document: str) -> str:
    compact_cik = str(cik).lstrip("0") or str(cik)
    compact_accession = accession.replace("-", "")
    return f"https://www.sec.gov/Archives/edgar/data/{compact_cik}/{compact_accession}/{document}"


def _raw_payload(kind: ConnectorRecordKind, record: Mapping[str, Any]) -> dict[str, Any]:
    return {"source": kind.value, "kind": kind.value, "record": dict(record)}


def _canonical_event_payload(
    *,
    event_id: str,
    ticker: str,
    event_type: str,
    provider: str,
    source: str,
    source_category: str,
    source_url: str | None,
    title: str,
    body_hash_value: str,
    dedupe: str,
    source_quality: float,
    materiality: float,
    source_ts: datetime,
    available_at: datetime,
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "id": event_id,
        "ticker": ticker.upper(),
        "event_type": event_type,
        "provider": provider,
        "source": source,
        "source_category": source_category,
        "source_url": source_url,
        "title": title,
        "body_hash": body_hash_value,
        "dedupe_key": dedupe,
        "source_quality": max(0.0, min(1.0, float(source_quality))),
        "materiality": max(0.0, min(1.0, float(materiality))),
        "source_ts": source_ts.isoformat(),
        "available_at": available_at.isoformat(),
        "payload": dict(payload),
    }


def _event_id(dedupe: str) -> str:
    return sha256(dedupe.encode("utf-8")).hexdigest()


def _hash_payload(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return sha256(encoded).hexdigest()


def _parse_datetime(value: Any, field: str) -> datetime:
    if value is None:
        msg = f"{field} is required"
        raise ValueError(msg)
    text = str(value).strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    if "T" not in text:
        text = f"{text}T00:00:00+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        msg = f"{field} must include timezone information"
        raise ValueError(msg)
    return parsed.astimezone(UTC)


def _mapping(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        msg = f"{field} must be a mapping"
        raise ValueError(msg)
    return value


__all__ = [
    "FIXTURE_RETENTION_POLICY",
    "SEC_LICENSE_TAG",
    "SEC_PROVIDER_NAME",
    "SecSubmissionsConnector",
]

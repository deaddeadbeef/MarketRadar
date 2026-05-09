# Phase 5 Event Connectors Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add deterministic SEC, news, and earnings event ingestion so Catalyst Radar can store source-linked, point-in-time events and use them as auditable scan evidence without LLM calls.

**Architecture:** Reuse the existing provider abstraction and raw/normalized provider storage from Phases 2 and 3. Add a canonical `events` table plus an `EventRepository`, deterministic classification/materiality/source-quality helpers, fixture-first connectors, CLI ingestion commands, and scan metadata wiring. Event data can improve deterministic candidate evidence and setup selection, but this phase does not add local embeddings, LLM synthesis, Decision Cards, options features, alerting, or broker/order functionality.

**Tech Stack:** Python 3.11, SQLAlchemy Core, SQLite-compatible local storage with PostgreSQL migration SQL, standard-library URL/hash parsing, existing `JsonHttpClient`/fixture transport, pytest, ruff.

---

## Current Baseline

Build from:

```text
main @ e141e0f
```

Current verified baseline:

- `python -m pytest` passes with 159 tests.
- `python -m ruff check src tests apps` passes.
- Provider abstraction exists in `src/catalyst_radar/connectors/base.py`.
- Raw/normalized provider persistence exists in `src/catalyst_radar/storage/provider_repositories.py`.
- Market scan persists candidate states and signal payloads through `MarketRepository.save_scan_result()`.
- Setup policies contain event-dependent placeholders for post-earnings and filings catalyst setups.
- Portfolio-aware policy is enforced before manual buy-review eligibility.

Important current limit:

- The scanner has no canonical event store, no event source-quality model, no event materiality model, and no way to attach source-linked event evidence to candidates.

## Scope

In this phase, implement:

- Canonical event model and database table.
- Event repository with point-in-time reads.
- Source-quality scoring.
- Event classification and materiality scoring.
- URL/hash dedupe.
- Fixture-first SEC submissions connector.
- Fixture-first news connector.
- Fixture-first earnings connector.
- CLI ingestion and event listing commands.
- Provider ingest support for normalized event records.
- Scan metadata wiring for recent material events.
- Setup selection that can pick `PostEarnings` or `FilingsCatalyst` only when matching point-in-time event evidence exists.
- Dashboard row enrichment with event count, top source, source URL, source quality, materiality, and conflict flags.
- Tests and fixture smokes.

Out of scope for this phase:

- Local embeddings, sentiment models, ontology/theme text intelligence, pgvector.
- Paid news provider selection.
- Options aggregate features.
- Sparse LLM router, Evidence Packets, Skeptic Agent, Decision Cards.
- Alert routing.
- Broker connectivity or automated trading.

## File Structure

Create:

- `src/catalyst_radar/events/__init__.py`  
  Package exports for event models and helpers.
- `src/catalyst_radar/events/models.py`  
  Event enums and immutable dataclasses: `EventType`, `SourceCategory`, `RawEvent`, `EventClassification`, `CanonicalEvent`, `EventEvidenceSummary`.
- `src/catalyst_radar/events/source_quality.py`  
  Deterministic source scoring by category, source name, domain, and provider metadata.
- `src/catalyst_radar/events/dedupe.py`  
  Canonical URL normalization and stable body/title hash helpers.
- `src/catalyst_radar/events/classifier.py`  
  Rule-based event taxonomy and materiality scoring.
- `src/catalyst_radar/events/conflicts.py`  
  Deterministic same-ticker conflict detection for contradictory high-quality event evidence.
- `src/catalyst_radar/connectors/sec.py`  
  SEC submissions connector. Fixture mode is required; live mode uses the existing HTTP client and official data.sec.gov URL shape.
- `src/catalyst_radar/connectors/news.py`  
  JSON fixture news connector using explicit source metadata.
- `src/catalyst_radar/connectors/earnings.py`  
  JSON fixture earnings calendar connector.
- `sql/migrations/005_events.sql`  
  PostgreSQL-compatible event schema migration.
- `tests/fixtures/sec/submissions_msft.json`
- `tests/fixtures/news/ticker_news_msft.json`
- `tests/fixtures/earnings/calendar_msft.json`
- `tests/unit/test_event_source_quality.py`
- `tests/unit/test_event_dedupe.py`
- `tests/unit/test_event_classifier.py`
- `tests/unit/test_event_conflicts.py`
- `tests/unit/test_event_connectors.py`
- `tests/integration/test_event_repository.py`
- `tests/integration/test_event_ingest_cli.py`
- `tests/integration/test_event_scan_integration.py`

Modify:

- `src/catalyst_radar/connectors/base.py`  
  Add event record kinds.
- `src/catalyst_radar/connectors/provider_ingest.py`  
  Persist normalized event records through `EventRepository` and include `event_count` in ingest results.
- `src/catalyst_radar/storage/schema.py`  
  Add `events` table and indexes.
- `src/catalyst_radar/storage/repositories.py`  
  Add `EventRepository` or a focused `src/catalyst_radar/storage/event_repositories.py`. Prefer `event_repositories.py` if `repositories.py` grows too much during implementation.
- `src/catalyst_radar/storage/db.py`  
  Keep `create_schema()` compatible with existing SQLite DBs.
- `src/catalyst_radar/cli.py`  
  Add `ingest-sec`, `ingest-news`, `ingest-earnings`, and `events` commands.
- `src/catalyst_radar/pipeline/scan.py`  
  Read recent events as of scan `available_at` and attach event metadata to candidate snapshots.
- `src/catalyst_radar/scoring/score.py`  
  Add a small deterministic event pillar input without letting events override risk or portfolio hard blocks.
- `src/catalyst_radar/scoring/setup_policies.py`  
  Select event-dependent setup policies only when event evidence exists.
- `src/catalyst_radar/dashboard/data.py`  
  Surface event count, top event type/source/title/URL, source quality, materiality, and conflict flags in candidate rows.
- `tests/unit/test_connector_contracts.py`  
  Cover the new record kind enum values.
- `tests/integration/test_provider_ingest_cli.py`  
  Regression that existing CSV/Polygon ingest output still works.

## Data Contract

Canonical event rows must include:

```text
id                    deterministic string id
ticker                uppercase ticker
event_type            EventType value
provider              provider that supplied the payload
source                source or publisher name
source_category       SourceCategory value
source_url            canonical URL when available
title                 short event title
body_hash             stable content hash
dedupe_key            stable key used for upsert/dedupe
source_quality        0.0 to 1.0
materiality           0.0 to 1.0
source_ts             source/provider event timestamp
available_at          timestamp Catalyst Radar could first use the event
payload               JSON payload with source ids and classifier reasons
created_at            persistence timestamp
```

Point-in-time invariant:

```text
An event can affect a scan only when event.available_at <= scan.available_at.
```

Policy invariant:

```text
Events may add evidence, setup reasons, and deterministic score support.
Events may not bypass stale-data, liquidity, risk, chase, portfolio, or cash hard blocks.
```

## Task 1: Create Event Models And Storage

**Files:**

- Create: `src/catalyst_radar/events/__init__.py`
- Create: `src/catalyst_radar/events/models.py`
- Modify: `src/catalyst_radar/storage/schema.py`
- Create: `src/catalyst_radar/storage/event_repositories.py`
- Create: `sql/migrations/005_events.sql`
- Test: `tests/integration/test_event_repository.py`

- [ ] **Step 1: Write the failing repository tests**

Add tests that prove event upsert, dedupe, latest reads, point-in-time reads, and timezone coercion behavior.

```python
from datetime import UTC, datetime

from sqlalchemy import create_engine, func, select

from catalyst_radar.events.models import CanonicalEvent, EventType, SourceCategory
from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.event_repositories import EventRepository
from catalyst_radar.storage.schema import events


def test_upsert_event_dedupes_by_dedupe_key() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    create_schema(engine)
    repo = EventRepository(engine)
    event = canonical_event()

    repo.upsert_events([event])
    repo.upsert_events([event])

    with engine.connect() as conn:
        assert conn.scalar(select(func.count()).select_from(events)) == 1


def test_list_events_for_ticker_respects_available_at() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    create_schema(engine)
    repo = EventRepository(engine)
    repo.upsert_events(
        [
            canonical_event(
                event_id="past",
                dedupe_key="MSFT:past",
                available_at=datetime(2026, 5, 10, 13, tzinfo=UTC),
            ),
            canonical_event(
                event_id="future",
                dedupe_key="MSFT:future",
                available_at=datetime(2026, 5, 10, 15, tzinfo=UTC),
            ),
        ]
    )

    rows = repo.list_events_for_ticker(
        "MSFT",
        as_of=datetime(2026, 5, 10, 21, tzinfo=UTC),
        available_at=datetime(2026, 5, 10, 14, tzinfo=UTC),
    )

    assert [row.id for row in rows] == ["past"]


def canonical_event(**overrides: object) -> CanonicalEvent:
    values = {
        "id": "event-1",
        "ticker": "MSFT",
        "event_type": EventType.SEC_FILING,
        "provider": "sec",
        "source": "SEC EDGAR",
        "source_category": SourceCategory.PRIMARY_SOURCE,
        "source_url": "https://www.sec.gov/Archives/example",
        "title": "MSFT 8-K",
        "body_hash": "body-hash",
        "dedupe_key": "MSFT:sec:8-k:2026-05-10",
        "source_quality": 1.0,
        "materiality": 0.85,
        "source_ts": datetime(2026, 5, 10, 12, tzinfo=UTC),
        "available_at": datetime(2026, 5, 10, 13, tzinfo=UTC),
        "payload": {"form_type": "8-K", "classification_reasons": ["sec_form_8k"]},
    }
    values.update(overrides)
    return CanonicalEvent(**values)  # type: ignore[arg-type]
```

- [ ] **Step 2: Run the failing tests**

Run:

```powershell
python -m pytest tests/integration/test_event_repository.py -q
```

Expected:

```text
ModuleNotFoundError: No module named 'catalyst_radar.events'
```

- [ ] **Step 3: Add event dataclasses and validation**

Implement immutable dataclasses with timezone validation:

```python
class EventType(StrEnum):
    EARNINGS = "earnings"
    GUIDANCE = "guidance"
    SEC_FILING = "sec_filing"
    INSIDER = "insider"
    ANALYST_REVISION = "analyst_revision"
    SECTOR_READ_THROUGH = "sector_read_through"
    PRODUCT_CUSTOMER = "product_customer"
    LEGAL_REGULATORY = "legal_regulatory"
    FINANCING = "financing"
    CORPORATE_ACTION = "corporate_action"
    NEWS = "news"


class SourceCategory(StrEnum):
    PRIMARY_SOURCE = "primary_source"
    REGULATORY = "regulatory"
    REPUTABLE_NEWS = "reputable_news"
    COMPANY_PRESS_RELEASE = "company_press_release"
    ANALYST_PROVIDER = "analyst_provider"
    AGGREGATOR = "aggregator"
    SOCIAL = "social"
    PROMOTIONAL = "promotional"
    UNKNOWN = "unknown"
```

`CanonicalEvent.__post_init__()` must:

- uppercase `ticker`
- reject blank `id`, `ticker`, `provider`, `source`, `title`, `body_hash`, `dedupe_key`
- require aware `source_ts` and `available_at`
- reject `available_at < source_ts`
- clamp `source_quality` and `materiality` into `[0.0, 1.0]`
- freeze `payload`

- [ ] **Step 4: Add schema and migration**

Add `events` to `src/catalyst_radar/storage/schema.py`:

```python
events = Table(
    "events",
    metadata,
    Column("id", String, primary_key=True),
    Column("ticker", String, nullable=False),
    Column("event_type", String, nullable=False),
    Column("provider", String, nullable=False),
    Column("source", Text, nullable=False),
    Column("source_category", String, nullable=False),
    Column("source_url", Text),
    Column("title", Text, nullable=False),
    Column("body_hash", String, nullable=False),
    Column("dedupe_key", String, nullable=False),
    Column("source_quality", Float, nullable=False),
    Column("materiality", Float, nullable=False),
    Column("source_ts", DateTime(timezone=True), nullable=False),
    Column("available_at", DateTime(timezone=True), nullable=False),
    Column("payload", json_type, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)
Index("ux_events_dedupe_key", events.c.dedupe_key, unique=True)
Index("ix_events_ticker_available_at", events.c.ticker, events.c.available_at)
Index("ix_events_type_materiality", events.c.event_type, events.c.materiality)
```

Add equivalent PostgreSQL DDL in `sql/migrations/005_events.sql` using `CREATE TABLE IF NOT EXISTS` and `CREATE UNIQUE INDEX IF NOT EXISTS`.

- [ ] **Step 5: Add `EventRepository`**

Implement:

```python
class EventRepository:
    def __init__(self, engine: Engine) -> None: ...
    def upsert_events(self, rows: Iterable[CanonicalEvent]) -> int: ...
    def list_events_for_ticker(
        self,
        ticker: str,
        *,
        as_of: datetime,
        available_at: datetime,
        min_materiality: float = 0.0,
        limit: int = 20,
    ) -> list[CanonicalEvent]: ...
    def latest_material_events_by_ticker(
        self,
        tickers: Iterable[str],
        *,
        as_of: datetime,
        available_at: datetime,
        min_materiality: float,
        limit_per_ticker: int,
    ) -> dict[str, list[CanonicalEvent]]: ...
```

For SQLite portability, perform upsert by deleting the existing `dedupe_key` row before insert.

- [ ] **Step 6: Run repository tests**

Run:

```powershell
python -m pytest tests/integration/test_event_repository.py -q
```

Expected:

```text
2 passed
```

- [ ] **Step 7: Commit**

```powershell
git add src/catalyst_radar/events src/catalyst_radar/storage/schema.py src/catalyst_radar/storage/event_repositories.py sql/migrations/005_events.sql tests/integration/test_event_repository.py
git commit -m "feat: add canonical event storage"
```

## Task 2: Add Source Quality, Dedupe, And Classification

**Files:**

- Create: `src/catalyst_radar/events/source_quality.py`
- Create: `src/catalyst_radar/events/dedupe.py`
- Create: `src/catalyst_radar/events/classifier.py`
- Create: `src/catalyst_radar/events/conflicts.py`
- Test: `tests/unit/test_event_source_quality.py`
- Test: `tests/unit/test_event_dedupe.py`
- Test: `tests/unit/test_event_classifier.py`
- Test: `tests/unit/test_event_conflicts.py`

- [ ] **Step 1: Write source-quality tests**

```python
from catalyst_radar.events.models import SourceCategory
from catalyst_radar.events.source_quality import score_source_quality


def test_primary_sources_score_highest() -> None:
    result = score_source_quality(
        source="SEC EDGAR",
        category=SourceCategory.PRIMARY_SOURCE,
        url="https://www.sec.gov/Archives/example",
    )

    assert result.score == 1.0
    assert "primary_source" in result.reasons


def test_promotional_source_scores_low() -> None:
    result = score_source_quality(
        source="Sponsored Stocks Daily",
        category=SourceCategory.PROMOTIONAL,
        url="https://promo.example.com/msft",
    )

    assert result.score <= 0.2
    assert "promotional_source" in result.reasons
```

- [ ] **Step 2: Write dedupe tests**

```python
from catalyst_radar.events.dedupe import body_hash, canonicalize_url, dedupe_key


def test_canonicalize_url_removes_tracking_params() -> None:
    assert (
        canonicalize_url("https://Example.com/path?utm_source=x&id=123#section")
        == "https://example.com/path?id=123"
    )


def test_body_hash_is_stable_across_whitespace() -> None:
    assert body_hash("Guidance raised\n\nfor FY 2026") == body_hash(
        "Guidance raised for FY 2026"
    )


def test_dedupe_key_prefers_canonical_url() -> None:
    assert dedupe_key(
        ticker="msft",
        provider="news",
        canonical_url="https://example.com/article",
        content_hash="abc",
    ) == "MSFT:news:https://example.com/article"
```

- [ ] **Step 3: Write classifier tests**

```python
from datetime import UTC, datetime

from catalyst_radar.events.classifier import classify_event
from catalyst_radar.events.models import EventType, RawEvent, SourceCategory


def test_sec_8k_guidance_is_high_materiality() -> None:
    result = classify_event(
        RawEvent(
            ticker="MSFT",
            provider="sec",
            source="SEC EDGAR",
            source_category=SourceCategory.PRIMARY_SOURCE,
            title="MSFT 8-K guidance update",
            body="Item 2.02 results of operations and financial condition. Raises guidance.",
            url="https://www.sec.gov/example",
            source_ts=datetime(2026, 5, 10, 12, tzinfo=UTC),
            available_at=datetime(2026, 5, 10, 13, tzinfo=UTC),
            payload={"form_type": "8-K"},
        )
    )

    assert result.event_type == EventType.GUIDANCE
    assert result.materiality >= 0.8
    assert result.requires_text_triage is True


def test_low_quality_promotional_news_is_not_material_alone() -> None:
    result = classify_event(
        RawEvent(
            ticker="MSFT",
            provider="news",
            source="Sponsored Stocks Daily",
            source_category=SourceCategory.PROMOTIONAL,
            title="MSFT could double soon",
            body="Sponsored promotional recap.",
            url="https://promo.example.com/msft",
            source_ts=datetime(2026, 5, 10, 12, tzinfo=UTC),
            available_at=datetime(2026, 5, 10, 12, tzinfo=UTC),
            payload={},
        )
    )

    assert result.event_type == EventType.NEWS
    assert result.source_quality <= 0.2
    assert result.materiality <= 0.35
    assert result.requires_confirmation is True
```

- [ ] **Step 4: Write conflict tests**

```python
from datetime import UTC, datetime

from catalyst_radar.events.models import CanonicalEvent, EventType, SourceCategory


def test_detects_conflicting_guidance_direction() -> None:
    conflicts = detect_event_conflicts(
        [
            event("raise", "MSFT raises full-year guidance", "Raises FY guidance."),
            event("cut", "MSFT cuts full-year guidance", "Cuts FY guidance."),
        ]
    )

    assert conflicts == (
        {
            "ticker": "MSFT",
            "conflict_type": "guidance_direction_conflict",
            "source_event_ids": ["raise", "cut"],
        },
    )


def test_ignores_low_quality_promotional_conflict() -> None:
    conflicts = detect_event_conflicts(
        [
            event("raise", "MSFT raises full-year guidance", "Raises FY guidance."),
            event(
                "promo-cut",
                "MSFT cuts full-year guidance",
                "Cuts FY guidance.",
                source_quality=0.1,
                source_category=SourceCategory.PROMOTIONAL,
            ),
        ]
    )

    assert conflicts == ()


def event(
    event_id: str,
    title: str,
    body: str,
    *,
    source_quality: float = 0.85,
    source_category: SourceCategory = SourceCategory.REPUTABLE_NEWS,
) -> CanonicalEvent:
    return CanonicalEvent(
        id=event_id,
        ticker="MSFT",
        event_type=EventType.GUIDANCE,
        provider="news_fixture",
        source="Reuters",
        source_category=source_category,
        source_url=f"https://reuters.example.com/{event_id}",
        title=title,
        body_hash=event_id,
        dedupe_key=f"MSFT:{event_id}",
        source_quality=source_quality,
        materiality=0.8,
        source_ts=datetime(2026, 5, 10, 12, tzinfo=UTC),
        available_at=datetime(2026, 5, 10, 12, tzinfo=UTC),
        payload={"body": body},
    )
```

- [ ] **Step 5: Run failing tests**

```powershell
python -m pytest tests/unit/test_event_source_quality.py tests/unit/test_event_dedupe.py tests/unit/test_event_classifier.py tests/unit/test_event_conflicts.py -q
```

Expected:

```text
ModuleNotFoundError
```

- [ ] **Step 6: Implement helpers**

Rules to implement exactly:

- Primary source: base `1.00`
- Regulatory: base `0.95`
- Reputable news: base `0.85`
- Company press release: base `0.75`
- Analyst provider: base `0.70`
- Aggregator: base `0.55`
- Social: base `0.25`
- Promotional: base `0.10`
- Unknown: base `0.40`
- `sec.gov` URL adds `primary_source_domain` reason and clamps score to at least `0.95`
- URL host containing `promo`, `sponsored`, or `stockpick` clamps score to at most `0.20`

Classifier rules:

- SEC `8-K` with guidance/results language -> `EventType.GUIDANCE`, materiality at least `0.80`
- SEC `10-Q` or `10-K` -> `EventType.SEC_FILING`, materiality at least `0.65`
- title/body containing `earnings`, `results`, or `quarter` -> `EventType.EARNINGS`, materiality at least `0.65`
- title/body containing `raises guidance`, `cuts guidance`, `revises guidance`, or `outlook` -> `EventType.GUIDANCE`, materiality at least `0.75`
- title/body containing `insider`, `Form 4`, `director purchased`, or `officer purchased` -> `EventType.INSIDER`
- title/body containing `upgrade`, `downgrade`, `price target`, or `revision` -> `EventType.ANALYST_REVISION`
- title/body containing `lawsuit`, `investigation`, `regulatory`, or `FDA` -> `EventType.LEGAL_REGULATORY`
- title/body containing `offering`, `convertible`, `debt`, or `financing` -> `EventType.FINANCING`
- title/body containing `split`, `dividend`, `merger`, or `spinoff` -> `EventType.CORPORATE_ACTION`
- fallback -> `EventType.NEWS`

Materiality formula:

```python
score = base_by_event_type + (source_quality - 0.5) * 0.30
score = clamp(score, 0.0, 1.0)
if source_quality < 0.35:
    score = min(score, 0.35)
```

Conflict rules:

- Only consider events with `source_quality >= 0.5` and `materiality >= 0.5`.
- For same ticker `GUIDANCE` events, detect a conflict when one title/body contains a raise phrase and another contains a cut phrase.
- Raise phrases: `raises guidance`, `raised guidance`, `raises full-year guidance`, `outlook raised`.
- Cut phrases: `cuts guidance`, `cut guidance`, `cuts full-year guidance`, `outlook cut`.
- Return stable tuples of dictionaries sorted by ticker and conflict type.
- Conflict payload dictionaries must include `ticker`, `conflict_type`, and `source_event_ids`.

- [ ] **Step 7: Run helper tests**

```powershell
python -m pytest tests/unit/test_event_source_quality.py tests/unit/test_event_dedupe.py tests/unit/test_event_classifier.py tests/unit/test_event_conflicts.py -q
```

Expected:

```text
all tests passed
```

- [ ] **Step 8: Commit**

```powershell
git add src/catalyst_radar/events tests/unit/test_event_source_quality.py tests/unit/test_event_dedupe.py tests/unit/test_event_classifier.py tests/unit/test_event_conflicts.py
git commit -m "feat: classify event quality and materiality"
```

## Task 3: Implement Fixture-First SEC, News, And Earnings Connectors

**Files:**

- Modify: `src/catalyst_radar/connectors/base.py`
- Create: `src/catalyst_radar/connectors/sec.py`
- Create: `src/catalyst_radar/connectors/news.py`
- Create: `src/catalyst_radar/connectors/earnings.py`
- Create: `tests/fixtures/sec/submissions_msft.json`
- Create: `tests/fixtures/news/ticker_news_msft.json`
- Create: `tests/fixtures/earnings/calendar_msft.json`
- Test: `tests/unit/test_event_connectors.py`
- Test: `tests/unit/test_connector_contracts.py`

- [ ] **Step 1: Write connector tests**

```python
from datetime import UTC, datetime
from pathlib import Path

from catalyst_radar.connectors.base import ConnectorRecordKind, ConnectorRequest
from catalyst_radar.connectors.earnings import EarningsCalendarConnector
from catalyst_radar.connectors.news import NewsJsonConnector
from catalyst_radar.connectors.sec import SecSubmissionsConnector
from catalyst_radar.events.models import EventType


def test_sec_submissions_fixture_normalizes_recent_filings() -> None:
    connector = SecSubmissionsConnector(fixture_path=Path("tests/fixtures/sec/submissions_msft.json"))
    request = ConnectorRequest(
        provider="sec",
        endpoint="submissions",
        params={"ticker": "MSFT", "cik": "0000789019"},
        requested_at=datetime(2026, 5, 10, 14, tzinfo=UTC),
    )

    raw = connector.fetch(request)
    normalized = connector.normalize(raw)

    assert raw[0].kind == ConnectorRecordKind.SEC_FILING
    assert normalized[0].kind == ConnectorRecordKind.EVENT
    assert normalized[0].payload["ticker"] == "MSFT"
    assert normalized[0].payload["event_type"] in {
        EventType.GUIDANCE.value,
        EventType.SEC_FILING.value,
    }


def test_news_fixture_dedupes_tracking_url_payloads() -> None:
    connector = NewsJsonConnector(fixture_path=Path("tests/fixtures/news/ticker_news_msft.json"))
    request = ConnectorRequest(
        provider="news_fixture",
        endpoint="ticker-news",
        params={"ticker": "MSFT"},
        requested_at=datetime(2026, 5, 10, 14, tzinfo=UTC),
    )

    normalized = connector.normalize(connector.fetch(request))

    dedupe_keys = {record.payload["dedupe_key"] for record in normalized}
    assert len(dedupe_keys) == len(normalized)
    assert all(record.kind == ConnectorRecordKind.EVENT for record in normalized)


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

    normalized = connector.normalize(connector.fetch(request))

    assert normalized[0].payload["event_type"] == EventType.EARNINGS.value
    assert normalized[0].payload["payload"]["event_risk"] == "upcoming_earnings"
```

- [ ] **Step 2: Add fixture payloads**

SEC fixture shape:

```json
{
  "cik": "0000789019",
  "ticker": "MSFT",
  "name": "MICROSOFT CORP",
  "filings": {
    "recent": {
      "accessionNumber": ["0000789019-26-000100", "0000789019-26-000101"],
      "filingDate": ["2026-05-10", "2026-05-09"],
      "acceptanceDateTime": ["2026-05-10T12:01:00Z", "2026-05-09T12:01:00Z"],
      "form": ["8-K", "10-Q"],
      "primaryDocument": ["msft-20260510x8k.htm", "msft-20260509x10q.htm"],
      "items": ["Item 2.02 Results of Operations and Financial Condition", ""]
    }
  }
}
```

News fixture shape:

```json
{
  "ticker": "MSFT",
  "articles": [
    {
      "source": "Reuters",
      "source_category": "reputable_news",
      "title": "Microsoft raises cloud guidance after earnings",
      "body": "Microsoft raises guidance after stronger cloud demand.",
      "url": "https://reuters.example.com/markets/msft-cloud?utm_source=feed",
      "published_at": "2026-05-10T12:30:00Z",
      "available_at": "2026-05-10T12:35:00Z"
    },
    {
      "source": "Sponsored Stocks Daily",
      "source_category": "promotional",
      "title": "MSFT could double soon",
      "body": "Sponsored promotional recap.",
      "url": "https://promo.example.com/msft",
      "published_at": "2026-05-10T12:31:00Z",
      "available_at": "2026-05-10T12:31:00Z"
    }
  ]
}
```

Earnings fixture shape:

```json
{
  "ticker": "MSFT",
  "events": [
    {
      "event_date": "2026-05-15",
      "time": "amc",
      "title": "Microsoft earnings date",
      "source": "earnings_fixture",
      "source_category": "aggregator",
      "available_at": "2026-05-10T12:00:00Z"
    }
  ]
}
```

- [ ] **Step 3: Add event record kinds**

Extend `ConnectorRecordKind`:

```python
class ConnectorRecordKind(StrEnum):
    SECURITY = "security"
    DAILY_BAR = "daily_bar"
    HOLDING = "holding"
    UNIVERSE_MEMBER = "universe_member"
    EVENT = "event"
    SEC_FILING = "sec_filing"
    NEWS_ARTICLE = "news_article"
    EARNINGS_EVENT = "earnings_event"
```

- [ ] **Step 4: Implement connectors**

All event connectors must:

- return `ConnectorHealth(HEALTHY)` when the fixture path exists
- return `ConnectorHealth(DOWN)` when the required fixture path is missing
- create `RawRecord` rows with the source payload kind (`SEC_FILING`, `NEWS_ARTICLE`, `EARNINGS_EVENT`)
- create `NormalizedRecord(kind=ConnectorRecordKind.EVENT, identity=<event dedupe key>)`
- include the canonical event payload fields required by `EventRepository`
- set `license_tag` to `sec-public`, `news-fixture`, or `earnings-fixture`
- set `retention_policy` to `retain-fixture`
- preserve source timestamps and availability timestamps

- [ ] **Step 5: Run connector tests**

```powershell
python -m pytest tests/unit/test_event_connectors.py tests/unit/test_connector_contracts.py -q
```

Expected:

```text
all tests passed
```

- [ ] **Step 6: Commit**

```powershell
git add src/catalyst_radar/connectors/base.py src/catalyst_radar/connectors/sec.py src/catalyst_radar/connectors/news.py src/catalyst_radar/connectors/earnings.py tests/fixtures/sec tests/fixtures/news tests/fixtures/earnings tests/unit/test_event_connectors.py tests/unit/test_connector_contracts.py
git commit -m "feat: add fixture event connectors"
```

## Task 4: Wire Provider Ingest And CLI Commands

**Files:**

- Modify: `src/catalyst_radar/connectors/provider_ingest.py`
- Modify: `src/catalyst_radar/cli.py`
- Test: `tests/integration/test_event_ingest_cli.py`
- Test: `tests/integration/test_provider_ingest_cli.py`

- [ ] **Step 1: Write CLI ingest tests**

```python
from pathlib import Path

import pytest
from sqlalchemy import create_engine, func, select

from catalyst_radar.cli import main
from catalyst_radar.storage.event_repositories import EventRepository
from catalyst_radar.storage.schema import events, normalized_provider_records, raw_provider_records


def test_ingest_sec_submissions_persists_events(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'events.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)

    exit_code = main(
        [
            "ingest-sec",
            "submissions",
            "--ticker",
            "MSFT",
            "--cik",
            "0000789019",
            "--fixture",
            "tests/fixtures/sec/submissions_msft.json",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert captured.out == (
        "ingested provider=sec raw=2 normalized=2 securities=0 "
        "daily_bars=0 holdings=0 events=2 rejected=0\n"
    )

    engine = create_engine(database_url, future=True)
    with engine.connect() as conn:
        assert conn.scalar(select(func.count()).select_from(raw_provider_records)) == 2
        assert conn.scalar(select(func.count()).select_from(normalized_provider_records)) == 2
        assert conn.scalar(select(func.count()).select_from(events)) == 2

    rows = EventRepository(engine).list_events_for_ticker(
        "MSFT",
        as_of=_dt("2026-05-10T21:00:00Z"),
        available_at=_dt("2026-05-10T14:00:00Z"),
    )
    assert [row.ticker for row in rows] == ["MSFT", "MSFT"]


def test_events_command_filters_future_available_events(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'events.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    assert main(["ingest-news", "--fixture", "tests/fixtures/news/ticker_news_msft.json"]) == 0
    capsys.readouterr()

    exit_code = main(
        [
            "events",
            "--ticker",
            "MSFT",
            "--as-of",
            "2026-05-10",
            "--available-at",
            "2026-05-10T12:32:00Z",
        ]
    )

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "MSFT" in output
    assert "Sponsored Stocks Daily" in output
    assert "Reuters" not in output
```

The test should import a shared `_dt()` helper or define one locally:

```python
from datetime import datetime, UTC


def _dt(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
```

- [ ] **Step 2: Confirm CLI output contract**

The final expected SEC output is:

```text
ingested provider=sec raw=2 normalized=2 securities=0 daily_bars=0 holdings=0 events=2 rejected=0
```

- [ ] **Step 3: Update provider ingest**

Change `ProviderIngestResult` to include:

```python
event_count: int
```

Change `ingest_provider_records()` signature:

```python
def ingest_provider_records(
    *,
    connector: MarketDataConnector,
    request: ConnectorRequest,
    market_repo: MarketRepository,
    provider_repo: ProviderRepository,
    job_type: str,
    metadata: Mapping[str, Any],
    event_repo: EventRepository | None = None,
) -> ProviderIngestResult:
```

Implementation rule:

- Existing market upserts remain unchanged.
- If normalized records contain `ConnectorRecordKind.EVENT`, require `event_repo`.
- Convert payloads to `CanonicalEvent` and call `event_repo.upsert_events()`.
- If event records exist and `event_repo` is `None`, fail closed with `ProviderIngestError("event repository required for event records")`.
- Existing CSV and Polygon outputs continue to include `events=0` only for `_print_provider_result()` provider-style commands. Preserve the exact `ingest-csv` output.

- [ ] **Step 4: Add CLI commands**

Add parser commands:

```text
ingest-sec submissions --ticker TICKER --cik CIK [--fixture PATH]
ingest-news --fixture PATH
ingest-earnings --fixture PATH
events --ticker TICKER --as-of YYYY-MM-DD [--available-at ISO8601] [--limit N]
```

Implementation details:

- `ingest-sec` requires `--fixture` in this phase unless `CATALYST_SEC_ENABLE_LIVE=1`.
- Live SEC mode uses `JsonHttpClient(UrlLibHttpTransport())`, `config.http_timeout_seconds`, and the connector base URL.
- SEC live command must set a SEC-compliant user agent from config before any network request. If the user agent is missing, return exit code `1` and print a clear stderr error.
- `ingest-news` and `ingest-earnings` are fixture-only in this phase.
- The `events` command prints one line per event:

```text
MSFT 2026-05-10T12:35:00+00:00 guidance materiality=0.92 quality=0.85 source=Reuters title=Microsoft raises cloud guidance after earnings
```

- [ ] **Step 5: Run CLI tests**

```powershell
python -m pytest tests/integration/test_event_ingest_cli.py tests/integration/test_provider_ingest_cli.py -q
```

Expected:

```text
all tests passed
```

- [ ] **Step 6: Commit**

```powershell
git add src/catalyst_radar/connectors/provider_ingest.py src/catalyst_radar/cli.py tests/integration/test_event_ingest_cli.py tests/integration/test_provider_ingest_cli.py
git commit -m "feat: ingest canonical events from providers"
```

## Task 5: Attach Event Evidence To Scan And Setup Selection

**Files:**

- Modify: `src/catalyst_radar/pipeline/scan.py`
- Modify: `src/catalyst_radar/scoring/score.py`
- Modify: `src/catalyst_radar/scoring/policy.py`
- Modify: `src/catalyst_radar/scoring/setup_policies.py`
- Modify: `src/catalyst_radar/dashboard/data.py`
- Test: `tests/integration/test_event_scan_integration.py`
- Test: `tests/unit/test_setup_policies.py`
- Test: `tests/unit/test_score.py`
- Test: `tests/unit/test_policy.py`

- [ ] **Step 1: Write event scan integration tests**

```python
from datetime import UTC, date, datetime
from pathlib import Path

from sqlalchemy import create_engine

from catalyst_radar.connectors.csv_market import load_daily_bars_csv, load_securities_csv
from catalyst_radar.core.config import AppConfig
from catalyst_radar.events.models import CanonicalEvent, EventType, SourceCategory
from catalyst_radar.pipeline.scan import run_scan
from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.event_repositories import EventRepository
from catalyst_radar.storage.repositories import MarketRepository


def test_scan_attaches_point_in_time_material_events() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    create_schema(engine)
    market_repo = MarketRepository(engine)
    event_repo = EventRepository(engine)
    fixture_dir = Path("tests/fixtures")
    market_repo.upsert_securities(load_securities_csv(fixture_dir / "securities.csv"))
    market_repo.upsert_daily_bars(load_daily_bars_csv(fixture_dir / "daily_bars.csv"))
    event_repo.upsert_events(
        [
            canonical_event(
                id="visible",
                dedupe_key="AAA:visible",
                ticker="AAA",
                available_at=datetime(2026, 5, 8, 20, 30, tzinfo=UTC),
            ),
            canonical_event(
                id="future",
                dedupe_key="AAA:future",
                ticker="AAA",
                available_at=datetime(2026, 5, 8, 22, 0, tzinfo=UTC),
            ),
        ]
    )

    result = next(
        row
        for row in run_scan(
            market_repo,
            as_of=date(2026, 5, 8),
            available_at=datetime(2026, 5, 8, 21, tzinfo=UTC),
            event_repo=event_repo,
            config=AppConfig(portfolio_value=100_000, portfolio_cash=25_000),
        )
        if row.ticker == "AAA"
    )

    assert result.candidate.metadata["material_event_count"] == 1
    assert result.candidate.metadata["events"][0]["id"] == "visible"
    assert result.candidate.metadata["events"][0]["source_id"] == "visible"
    assert result.candidate.metadata["event_conflicts"] == []
```

Helper:

```python
def canonical_event(**overrides: object) -> CanonicalEvent:
    values = {
        "id": "event-1",
        "ticker": "AAA",
        "event_type": EventType.GUIDANCE,
        "provider": "news_fixture",
        "source": "Reuters",
        "source_category": SourceCategory.REPUTABLE_NEWS,
        "source_url": "https://reuters.example.com/aaa",
        "title": "AAA raises guidance",
        "body_hash": "hash",
        "dedupe_key": "AAA:event-1",
        "source_quality": 0.85,
        "materiality": 0.9,
        "source_ts": datetime(2026, 5, 8, 20, tzinfo=UTC),
        "available_at": datetime(2026, 5, 8, 20, 30, tzinfo=UTC),
        "payload": {"classification_reasons": ["guidance_language"]},
    }
    values.update(overrides)
    return CanonicalEvent(**values)  # type: ignore[arg-type]
```

- [ ] **Step 2: Add score regression test**

Add a test proving event support is bounded:

```python
def test_event_support_is_bounded_and_cannot_override_stale_data_policy() -> None:
    candidate = candidate_from_features(
        _strong_features(),
        portfolio_penalty=0.0,
        data_stale=True,
        entry_zone=(100.0, 104.0),
        invalidation_price=94.0,
        reward_risk=2.4,
        event_support_score=100.0,
    )

    result = evaluate_policy(candidate)

    assert candidate.final_score <= 100.0
    assert candidate.metadata["event_support_score"] == 100.0
    assert candidate.metadata["event_bonus"] == 8.0
    assert result.state == ActionState.BLOCKED
    assert "data_stale" in result.hard_blocks
```

- [ ] **Step 3: Add policy conflict regression test**

Add this test to `tests/unit/test_policy.py`:

```python
def test_event_conflict_downgrades_buy_review_candidate_to_research_only() -> None:
    candidate = candidate_from_features(
        _features(),
        portfolio_penalty=0.0,
        data_stale=False,
        entry_zone=(100.0, 103.0),
        invalidation_price=94.0,
        reward_risk=2.5,
        metadata={
            "portfolio_impact": {"hard_blocks": []},
            "has_event_conflict": True,
            "event_conflicts": [
                {
                    "ticker": "AAA",
                    "conflict_type": "guidance_direction_conflict",
                    "source_event_ids": ["raise", "cut"],
                }
            ],
        },
    )

    result = evaluate_policy(candidate)

    assert candidate.final_score >= 85
    assert result.state == ActionState.RESEARCH_ONLY
    assert result.reasons == ("event_conflict_requires_manual_resolution",)
```

- [ ] **Step 4: Update scan signature**

Change:

```python
def run_scan(..., event_repo: EventRepository | None = None) -> list[ScanResult]:
```

Implementation:

- Before scanning candidates, build `events_by_ticker = event_repo.latest_material_events_by_ticker(...)` when `event_repo` is provided.
- Use `min_materiality=0.50` and `limit_per_ticker=5`.
- For each candidate, compute `event_conflicts = detect_event_conflicts(material_events)`.
- Add metadata:

```python
"events": [_event_payload(event) for event in material_events],
"material_event_count": len(material_events),
"top_event_type": material_events[0].event_type.value if material_events else None,
"top_event_title": material_events[0].title if material_events else None,
"top_event_source": material_events[0].source if material_events else None,
"top_event_source_url": material_events[0].source_url if material_events else None,
"top_event_source_quality": material_events[0].source_quality if material_events else None,
"top_event_materiality": material_events[0].materiality if material_events else None,
"event_support_score": _event_support_score(material_events),
"event_source_ids": [event.id for event in material_events],
"event_conflicts": list(event_conflicts),
"has_event_conflict": bool(event_conflicts),
```

Event payload must include:

```text
id, source_id, event_type, title, source, source_category, source_quality,
materiality, source_ts, available_at, source_url
```

- [ ] **Step 5: Update setup selection**

Change `select_setup_plan()` to accept:

```python
def select_setup_plan(
    bars: Sequence[DailyBar],
    features: MarketFeatures,
    *,
    material_events: Sequence[CanonicalEvent] = (),
) -> SetupPlan:
```

Rules:

- If the latest material event type is `EARNINGS` and the event has `payload.event_risk != "upcoming_earnings"`, use `post_earnings_plan()`.
- If the latest material event type is `GUIDANCE` or `SEC_FILING`, use `filings_catalyst_plan()`.
- Otherwise preserve the current market-only selection.
- Event-driven setup metadata must set `event_confirmed=True`, `source_event_id=<id>`, and `source_quality=<score>`.
- Upcoming earnings events are risk context only; they must not promote `PostEarnings`.

- [ ] **Step 6: Update score support**

Keep score version explicit:

```python
SCORE_VERSION = "score-v2-events"
```

Update existing `tests/unit/test_score.py` assertions that expect `score-v1` so they expect `score-v2-events`.

Add optional event support to `candidate_from_features()`:

```python
event_support_score: float = 0.0
```

Final event support rule:

```text
event_bonus = min(8.0, max(0.0, event_support_score) * 0.08)
final_score = clamp(market_score + event_bonus - risk_penalty - portfolio_penalty, 0, 100)
```

This keeps event data helpful but unable to dominate market/portfolio policy.

- [ ] **Step 7: Update policy conflict downgrade**

In `src/catalyst_radar/scoring/policy.py`, update the policy version:

```python
POLICY_VERSION = "policy-v2-events"
```

After hard-block handling and before buy-review/watchlist state checks, add:

```python
if candidate.metadata.get("has_event_conflict") is True:
    return PolicyResult(
        state=ActionState.RESEARCH_ONLY,
        reasons=("event_conflict_requires_manual_resolution",),
    )
```

This is a downgrade, not a hard block: the user can still inspect the candidate, but it cannot reach Warning or buy-review states while source conflicts are unresolved.
Update existing `tests/unit/test_policy.py` assertions that expect `policy-v1` so they expect `policy-v2-events`.

- [ ] **Step 8: Update dashboard rows**

Add fields in `_candidate_row()`:

```python
values["material_event_count"] = candidate_metadata.get("material_event_count", 0)
values["top_event_type"] = candidate_metadata.get("top_event_type")
values["top_event_title"] = candidate_metadata.get("top_event_title")
values["top_event_source"] = candidate_metadata.get("top_event_source")
values["top_event_source_url"] = candidate_metadata.get("top_event_source_url")
values["top_event_source_quality"] = candidate_metadata.get("top_event_source_quality")
values["top_event_materiality"] = candidate_metadata.get("top_event_materiality")
values["has_event_conflict"] = candidate_metadata.get("has_event_conflict", False)
values["event_conflicts"] = candidate_metadata.get("event_conflicts", [])
```

- [ ] **Step 9: Run scan tests**

```powershell
python -m pytest tests/integration/test_event_scan_integration.py tests/unit/test_setup_policies.py tests/unit/test_score.py tests/unit/test_policy.py -q
```

Expected:

```text
all tests passed
```

- [ ] **Step 10: Commit**

```powershell
git add src/catalyst_radar/pipeline/scan.py src/catalyst_radar/scoring/score.py src/catalyst_radar/scoring/policy.py src/catalyst_radar/scoring/setup_policies.py src/catalyst_radar/dashboard/data.py tests/integration/test_event_scan_integration.py tests/unit/test_setup_policies.py tests/unit/test_score.py tests/unit/test_policy.py
git commit -m "feat: attach event evidence to scans"
```

## Task 6: Full Verification, Review, And Phase Notes

**Files:**

- Create: `docs/phase-5-review.md`
- Modify only if needed: tests or docs surfaced by review.

- [ ] **Step 1: Run focused suite**

```powershell
python -m pytest tests/unit/test_event_source_quality.py tests/unit/test_event_dedupe.py tests/unit/test_event_classifier.py tests/unit/test_event_conflicts.py tests/unit/test_event_connectors.py tests/unit/test_policy.py tests/integration/test_event_repository.py tests/integration/test_event_ingest_cli.py tests/integration/test_event_scan_integration.py -q
```

Expected:

```text
all tests passed
```

- [ ] **Step 2: Run full suite**

```powershell
python -m pytest
```

Expected:

```text
all tests passed
```

- [ ] **Step 3: Run lint**

```powershell
python -m ruff check src tests apps
```

Expected:

```text
All checks passed!
```

- [ ] **Step 4: Run event CLI smoke**

```powershell
Remove-Item data/local/catalyst_radar.db -ErrorAction SilentlyContinue
$env:CATALYST_DATABASE_URL="sqlite:///data/local/catalyst_radar.db"
.\.venv\Scripts\catalyst-radar.exe init-db
.\.venv\Scripts\catalyst-radar.exe ingest-csv --securities tests/fixtures/securities.csv --daily-bars tests/fixtures/daily_bars.csv --holdings tests/fixtures/holdings.csv
.\.venv\Scripts\catalyst-radar.exe ingest-sec submissions --ticker MSFT --cik 0000789019 --fixture tests/fixtures/sec/submissions_msft.json
.\.venv\Scripts\catalyst-radar.exe ingest-news --fixture tests/fixtures/news/ticker_news_msft.json
.\.venv\Scripts\catalyst-radar.exe ingest-earnings --fixture tests/fixtures/earnings/calendar_msft.json
.\.venv\Scripts\catalyst-radar.exe events --ticker MSFT --as-of 2026-05-10 --available-at 2026-05-10T14:00:00Z
.\.venv\Scripts\catalyst-radar.exe scan --as-of 2026-05-08
```

Expected outputs include:

```text
initialized database
ingested securities=6 daily_bars=36 holdings=1
ingested provider=sec raw=2 normalized=2 securities=0 daily_bars=0 holdings=0 events=2 rejected=0
ingested provider=news_fixture raw=2 normalized=2 securities=0 daily_bars=0 holdings=0 events=2 rejected=0
ingested provider=earnings_fixture raw=1 normalized=1 securities=0 daily_bars=0 holdings=0 events=1 rejected=0
scanned candidates=3
```

- [ ] **Step 5: Run Polygon regression smoke**

```powershell
Remove-Item data/local/catalyst_radar.db -ErrorAction SilentlyContinue
$env:CATALYST_DATABASE_URL="sqlite:///data/local/catalyst_radar.db"
.\.venv\Scripts\catalyst-radar.exe init-db
.\.venv\Scripts\catalyst-radar.exe ingest-polygon tickers --fixture tests/fixtures/polygon/tickers_page_1.json --date 2026-05-08
.\.venv\Scripts\catalyst-radar.exe ingest-polygon grouped-daily --fixture tests/fixtures/polygon/grouped_daily_2026-05-07.json --date 2026-05-07
.\.venv\Scripts\catalyst-radar.exe ingest-polygon grouped-daily --fixture tests/fixtures/polygon/grouped_daily_2026-05-08.json --date 2026-05-08
.\.venv\Scripts\catalyst-radar.exe provider-health --provider polygon
.\.venv\Scripts\catalyst-radar.exe build-universe --name liquid-us --provider polygon --as-of 2026-05-08 --available-at 2026-05-08T21:30:00Z
.\.venv\Scripts\catalyst-radar.exe scan --as-of 2026-05-08 --available-at 2026-05-08T21:30:00Z --universe liquid-us
```

Expected outputs include:

```text
provider=polygon status=degraded
built universe=liquid-us members=2 excluded=1
scanned candidates=2
```

- [ ] **Step 6: Subagent review**

Dispatch a review subagent with:

```text
Review Phase 5 event connector implementation. Focus on point-in-time correctness, event dedupe/upsert behavior, provider ingest regressions, event score overpowering risk policy, and CLI fixture smoke coverage. Do not edit files. Return high/medium findings with file/line references and exact failing tests or reproduction steps.
```

Fix every high or medium finding and rerun the affected tests plus the full suite.

- [ ] **Step 7: Write `docs/phase-5-review.md`**

Include:

- outcome
- verification command outputs
- event CLI smoke output
- Polygon regression smoke output
- review findings fixed
- residual risks

Residual risks to carry unless implementation removes them:

- SEC live mode depends on current SEC behavior and must be verified with a compliant user agent before real use.
- News and earnings connectors are fixture/provider skeletons until a real licensed provider is selected.
- Source quality is deterministic and conservative, not a substitute for local text intelligence.
- Event score support is intentionally capped.
- No LLM evidence packets or Decision Cards exist yet.

- [ ] **Step 8: Commit review notes**

```powershell
git add docs/phase-5-review.md
git commit -m "docs: record phase 5 verification"
```

## Subagent Work Split

Use non-overlapping write sets:

- Worker A: Task 1 only. Owns `events/models.py`, `storage/schema.py`, `storage/event_repositories.py`, migration, repository tests.
- Worker B: Task 2 only. Owns `events/source_quality.py`, `events/dedupe.py`, `events/classifier.py`, `events/conflicts.py`, unit tests.
- Worker C: Task 3 only. Owns event connectors, connector fixtures, connector unit tests, connector enum update.
- Worker D: Task 4 only after Workers A and C finish. Owns provider ingest and CLI event ingestion tests.
- Main agent: Task 5 scan/scoring/dashboard integration and final verification.

Workers are not alone in the codebase. Each worker must preserve edits from other workers, avoid reverting unrelated changes, and list changed files in the final response.

## Phase Acceptance Criteria

- Event records are stored separately from raw/normalized provider payloads.
- Every event has `source_ts` and `available_at`.
- Event reads and scan integration honor `available_at`.
- Duplicate event payloads collapse by `dedupe_key`.
- Low-quality promotional events cannot materially boost candidates by themselves.
- Event-dependent setups require event evidence.
- Existing CSV and Polygon ingest/smoke flows remain working.
- Full test suite and ruff pass.
- Phase review note exists with residual risks.

## Execution Start

After this plan is committed:

```powershell
git checkout main
git pull --ff-only
git worktree add .worktrees/phase-5-event-connectors -b feature/phase-5-event-connectors
cd .worktrees/phase-5-event-connectors
python -m pip install -e ".[dev]"
python -m pytest
python -m ruff check src tests apps
```

If the worktree already exists, verify it with:

```powershell
git -C .worktrees/phase-5-event-connectors status --short --branch
```

Do not start implementation unless the worktree is clean and baseline verification passes.

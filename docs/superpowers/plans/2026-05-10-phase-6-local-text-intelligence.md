# Phase 6 Local Text Intelligence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add deterministic, provider-free local text intelligence on top of Phase 5 events: source-linked snippets, ontology/theme matching, sentiment direction, lightweight local embeddings, novelty, and text-feature metadata that can improve candidate evidence without LLM calls.

**Architecture:** Build a `textint` package that reads canonical events, extracts ranked snippets, matches them against a versioned ontology, computes deterministic sentiment and novelty, stores `text_snippets` and `text_features`, and exposes those features to scan metadata and scoring as a bounded local narrative signal. The implementation is intentionally local and dependency-light: embeddings use a deterministic hashing-vector fallback, not paid APIs or external model downloads.

**Tech Stack:** Python 3.11, SQLAlchemy Core, SQLite-compatible storage with PostgreSQL migration SQL, standard-library hashing/math, YAML-like config parsed through a small repo-local parser, pytest, ruff.

---

## Current Baseline

Build from:

```text
main @ 65f4fb7
```

Current verified baseline:

- `python -m pytest` passes with 200 tests.
- `python -m ruff check src tests apps` passes.
- Canonical `events` table and `EventRepository` exist.
- SEC/news/earnings fixture connectors persist canonical events.
- Scan reads recent material events and persists event metadata.
- Event support is capped and cannot override hard policy blocks.
- Event conflicts downgrade candidates to `ResearchOnly`.

Important current limit:

- Event body/title text is only stored as event payload. There is no snippet store, ontology, local narrative score, novelty score, or selected evidence list for future candidate packets and LLM escalation.

## Scope

In this phase, implement:

- `text_snippets` and `text_features` tables.
- Text repository with point-in-time reads.
- Initial ontology config for `ai_infrastructure_storage` and `datacenter_power`.
- Deterministic snippet extraction from event title/body/payload.
- Snippet ranking by source quality, materiality, ontology hits, and event type.
- Lightweight sentiment scoring from finance-oriented phrase lists.
- Deterministic hashing-vector embeddings for local similarity/novelty.
- Novelty scoring against prior ticker/theme snippets.
- Text pipeline command that processes events as of a timestamp.
- Scan metadata wiring for local narrative, novelty, theme hits, sentiment, and selected snippets.
- A bounded local narrative score contribution that cannot bypass risk, portfolio, stale-data, or conflict policy gates.

Out of scope for this phase:

- Downloading transformer models or adding heavyweight ML dependencies.
- pgvector/Postgres vector indexes.
- Sparse LLM router, Evidence Packets, Skeptic Agent, Decision Cards.
- Paid transcript/news provider selection.
- Options aggregate features.
- Alert routing.
- Broker connectivity or automated trading.

## File Structure

Create:

- `config/themes.yaml`  
  Initial ontology terms and read-through mappings from the specs.
- `src/catalyst_radar/textint/__init__.py`  
  Package exports.
- `src/catalyst_radar/textint/models.py`  
  `TextSnippet`, `TextFeature`, `OntologyTheme`, `OntologyMatch`, `SentimentResult`, `EmbeddingVector`, and `NoveltyResult` dataclasses.
- `src/catalyst_radar/textint/ontology.py`  
  Small parser for the initial ontology config and deterministic term matching.
- `src/catalyst_radar/textint/snippets.py`  
  Snippet extraction, hashing, and ranking.
- `src/catalyst_radar/textint/sentiment.py`  
  Finance-oriented deterministic phrase scoring.
- `src/catalyst_radar/textint/embeddings.py`  
  Deterministic hashing-vector embeddings and cosine similarity.
- `src/catalyst_radar/textint/novelty.py`  
  Novelty scoring against prior snippets for ticker/theme.
- `src/catalyst_radar/textint/pipeline.py`  
  Event-to-snippet/text-feature orchestration.
- `src/catalyst_radar/storage/text_repositories.py`  
  Persistence for snippets and text features.
- `sql/migrations/006_textint.sql`  
  PostgreSQL-compatible text intelligence migration.
- `tests/unit/test_ontology.py`
- `tests/unit/test_snippet_selection.py`
- `tests/unit/test_text_sentiment.py`
- `tests/unit/test_text_embeddings.py`
- `tests/unit/test_text_novelty.py`
- `tests/integration/test_text_pipeline.py`
- `tests/integration/test_text_scan_integration.py`

Modify:

- `src/catalyst_radar/storage/schema.py`  
  Add `text_snippets` and `text_features`.
- `src/catalyst_radar/cli.py`  
  Add `run-textint` and `text-features` commands.
- `src/catalyst_radar/pipeline/scan.py`  
  Load latest point-in-time text features and selected snippets for candidates.
- `src/catalyst_radar/scoring/score.py`  
  Add bounded local narrative bonus.
- `src/catalyst_radar/dashboard/data.py`  
  Expose local narrative, novelty, theme hits, sentiment, and selected snippet count.
- `tests/unit/test_score.py`
- `tests/integration/test_event_scan_integration.py`

## Data Contract

`text_snippets` rows:

```text
id                    deterministic snippet id
ticker                uppercase ticker
event_id              source event id
snippet_hash          stable hash of normalized text
section               title/body/summary
text                  snippet text
source                source name
source_url            source URL when available
source_quality        0.0 to 1.0
event_type            canonical event type
materiality           0.0 to 1.0
ontology_hits         JSON list of theme ids and terms
sentiment             -1.0 to 1.0
embedding             JSON list of floats
source_ts             source event timestamp
available_at          event/snippet availability timestamp
payload               JSON audit payload
created_at            persistence timestamp
```

`text_features` rows:

```text
id                    deterministic ticker/as_of/version id
ticker                uppercase ticker
as_of                 feature timestamp
feature_version       text-feature version
local_narrative_score 0.0 to 100.0
novelty_score         0.0 to 100.0
sentiment_score       -100.0 to 100.0
source_quality_score  0.0 to 100.0
theme_match_score     0.0 to 100.0
conflict_penalty      0.0 to 100.0
selected_snippet_ids  JSON list
theme_hits            JSON object/list
source_ts             max selected source timestamp
available_at          max selected availability timestamp
payload               JSON audit payload
created_at            persistence timestamp
```

Point-in-time invariant:

```text
Text snippets/features can affect a scan only when available_at <= scan.available_at.
```

Policy invariant:

```text
Local narrative can add bounded support and evidence. It cannot override stale data, liquidity, risk, portfolio, cash, chase, or unresolved-conflict gates.
```

## Task 1: Add Text Models, Schema, And Repository

**Files:**

- Create: `src/catalyst_radar/textint/__init__.py`
- Create: `src/catalyst_radar/textint/models.py`
- Modify: `src/catalyst_radar/storage/schema.py`
- Create: `src/catalyst_radar/storage/text_repositories.py`
- Create: `sql/migrations/006_textint.sql`
- Test: `tests/integration/test_text_pipeline.py`

- [ ] **Step 1: Write repository tests**

```python
from datetime import UTC, datetime

from sqlalchemy import create_engine, func, select

from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.schema import text_features, text_snippets
from catalyst_radar.storage.text_repositories import TextRepository
from catalyst_radar.textint.models import TextFeature, TextSnippet


def test_upsert_snippets_dedupes_by_snippet_hash_and_event() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    create_schema(engine)
    repo = TextRepository(engine)

    repo.upsert_snippets([snippet()])
    repo.upsert_snippets([snippet()])

    with engine.connect() as conn:
        assert conn.scalar(select(func.count()).select_from(text_snippets)) == 1


def test_latest_text_feature_respects_available_at() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    create_schema(engine)
    repo = TextRepository(engine)
    repo.upsert_text_features(
        [
            feature(id="past", available_at=datetime(2026, 5, 10, 13, tzinfo=UTC)),
            feature(id="future", available_at=datetime(2026, 5, 10, 15, tzinfo=UTC)),
        ]
    )

    result = repo.latest_text_features_by_ticker(
        ["MSFT"],
        as_of=datetime(2026, 5, 10, 21, tzinfo=UTC),
        available_at=datetime(2026, 5, 10, 14, tzinfo=UTC),
    )

    assert result["MSFT"].id == "past"
```

Also define concrete `snippet()` and `feature()` helpers in the test using all required fields.

- [ ] **Step 2: Run failing test**

```powershell
python -m pytest tests/integration/test_text_pipeline.py -q
```

Expected:

```text
ModuleNotFoundError: No module named 'catalyst_radar.textint'
```

- [ ] **Step 3: Implement models**

Validation rules:

- Uppercase tickers.
- Require nonblank IDs, event IDs, hashes, section, text, source, feature version.
- Require timezone-aware `source_ts` and `available_at`.
- Reject `available_at < source_ts`.
- Clamp score fields to documented ranges.
- Freeze JSON-like mappings/sequences.

- [ ] **Step 4: Add schema and migration**

Add SQLAlchemy tables and indexes:

```text
text_snippets:
  primary key id
  unique index event_id + snippet_hash
  index ticker + available_at
  index snippet_hash

text_features:
  primary key id
  unique index ticker + as_of + feature_version
  index ticker + available_at
```

Add equivalent `sql/migrations/006_textint.sql`.

- [ ] **Step 5: Implement `TextRepository`**

Methods:

```python
upsert_snippets(rows: Iterable[TextSnippet]) -> int
list_snippets_for_ticker(ticker, as_of, available_at, limit=20) -> list[TextSnippet]
upsert_text_features(rows: Iterable[TextFeature]) -> int
latest_text_features_by_ticker(tickers, as_of, available_at) -> dict[str, TextFeature]
```

Use SQLite-portable delete-before-insert upserts.

- [ ] **Step 6: Run repository tests and lint**

```powershell
python -m pytest tests/integration/test_text_pipeline.py -q
python -m ruff check src/catalyst_radar/textint src/catalyst_radar/storage/text_repositories.py tests/integration/test_text_pipeline.py
```

- [ ] **Step 7: Commit**

```powershell
git add src/catalyst_radar/textint src/catalyst_radar/storage/schema.py src/catalyst_radar/storage/text_repositories.py sql/migrations/006_textint.sql tests/integration/test_text_pipeline.py
git commit -m "feat: add text intelligence storage"
```

## Task 2: Add Ontology, Snippets, Sentiment, Embeddings, And Novelty

**Files:**

- Create: `config/themes.yaml`
- Create: `src/catalyst_radar/textint/ontology.py`
- Create: `src/catalyst_radar/textint/snippets.py`
- Create: `src/catalyst_radar/textint/sentiment.py`
- Create: `src/catalyst_radar/textint/embeddings.py`
- Create: `src/catalyst_radar/textint/novelty.py`
- Test: `tests/unit/test_ontology.py`
- Test: `tests/unit/test_snippet_selection.py`
- Test: `tests/unit/test_text_sentiment.py`
- Test: `tests/unit/test_text_embeddings.py`
- Test: `tests/unit/test_text_novelty.py`

- [ ] **Step 1: Write ontology tests**

```python
from pathlib import Path

from catalyst_radar.textint.ontology import load_ontology, match_ontology


def test_initial_ontology_matches_ai_storage_terms() -> None:
    ontology = load_ontology(Path("config/themes.yaml"))

    matches = match_ontology(
        "NAND demand and datacenter SSD storage bottlenecks are improving.",
        ontology,
    )

    assert matches[0].theme_id == "ai_infrastructure_storage"
    assert {"NAND", "SSD", "storage bottleneck"} <= set(matches[0].terms)
```

- [ ] **Step 2: Write snippet ranking tests**

```python
def test_snippet_ranking_prefers_high_quality_ontology_hits() -> None:
    snippets = extract_snippets([high_quality_event(), low_quality_event()], ontology)

    ranked = rank_snippets(snippets, limit=1)

    assert ranked[0].source_quality == 0.9
    assert ranked[0].ontology_hits[0]["theme_id"] == "ai_infrastructure_storage"
```

Use concrete event helpers from existing event tests.

- [ ] **Step 3: Write sentiment, embedding, and novelty tests**

Required expectations:

- `score_sentiment("raises guidance and stronger demand") > 0`
- `score_sentiment("cuts guidance and regulatory investigation") < 0`
- `embed_text("same text") == embed_text("same text")`
- `cosine_similarity(embed_text("NAND SSD demand"), embed_text("NAND SSD demand")) == 1.0`
- novelty is lower when a new snippet is similar to prior snippets and higher when theme/text differs.

- [ ] **Step 4: Implement helpers**

Ontology config:

```yaml
themes:
  ai_infrastructure_storage:
    terms:
      - NAND
      - SSD
      - datacenter storage
      - inference storage
      - storage bottleneck
    sectors:
      - Semiconductors
      - Technology Hardware
    read_through:
      - memory
      - storage controllers
      - equipment
  datacenter_power:
    terms:
      - power density
      - grid constraint
      - UPS
      - switchgear
      - cooling load
    sectors:
      - Electrical Equipment
      - Industrials
    read_through:
      - cooling
      - grid equipment
      - data center infrastructure
```

Parser rule:

- Support this exact simple YAML subset: nested mappings with two-space indentation and list items.
- Raise `ValueError` on missing `themes`.

Embedding rule:

- Use deterministic hashing vector with 64 dimensions.
- Tokenize with lowercase alphanumeric tokens.
- Normalize vector to unit length.
- Store as JSON list of floats rounded to 6 decimals.

Novelty rule:

```text
novelty = 100 * (1 - max cosine similarity to prior snippets)
```

If there are no prior snippets, novelty is `100.0`.

- [ ] **Step 5: Run helper tests and lint**

```powershell
python -m pytest tests/unit/test_ontology.py tests/unit/test_snippet_selection.py tests/unit/test_text_sentiment.py tests/unit/test_text_embeddings.py tests/unit/test_text_novelty.py -q
python -m ruff check src/catalyst_radar/textint tests/unit/test_ontology.py tests/unit/test_snippet_selection.py tests/unit/test_text_sentiment.py tests/unit/test_text_embeddings.py tests/unit/test_text_novelty.py
```

- [ ] **Step 6: Commit**

```powershell
git add config/themes.yaml src/catalyst_radar/textint tests/unit/test_ontology.py tests/unit/test_snippet_selection.py tests/unit/test_text_sentiment.py tests/unit/test_text_embeddings.py tests/unit/test_text_novelty.py
git commit -m "feat: add local text intelligence primitives"
```

## Task 3: Implement Text Pipeline And CLI

**Files:**

- Create/modify: `src/catalyst_radar/textint/pipeline.py`
- Modify: `src/catalyst_radar/cli.py`
- Test: `tests/integration/test_text_pipeline.py`

- [ ] **Step 1: Write pipeline integration tests**

Add tests that:

- Seed canonical events for `MSFT`.
- Run `run_text_pipeline(event_repo, text_repo, as_of, available_at, ontology_path)`.
- Assert snippets and one `TextFeature` are persisted.
- Assert future-available events are ignored.
- Assert duplicate snippet hashes are deduped.

Expected feature fields:

```text
local_narrative_score > 0
novelty_score >= 0
source_quality_score > 0
theme_match_score > 0 when ontology terms match
selected_snippet_ids not empty
```

- [ ] **Step 2: Add CLI tests**

Add CLI coverage:

```text
run-textint --as-of 2026-05-10 --available-at 2026-05-10T14:00:00Z --ontology config/themes.yaml
text-features --ticker MSFT --as-of 2026-05-10 --available-at 2026-05-10T14:00:00Z
```

Expected output:

```text
processed text_features=1 snippets=2
MSFT local_narrative=70.00 novelty=100.00 snippets=2
```

- [ ] **Step 3: Implement pipeline**

`run_text_pipeline()` must:

- Read events with `available_at <= available_at`.
- Extract snippets from event title/body.
- Match ontology.
- Score sentiment.
- Embed snippets.
- Look up prior snippets for novelty.
- Rank top snippets.
- Persist snippets.
- Persist one text feature per ticker/as_of/version.

Version constants:

```python
TEXT_FEATURE_VERSION = "textint-v1"
```

- [ ] **Step 4: Implement CLI**

Add commands:

```text
run-textint --as-of YYYY-MM-DD [--available-at ISO8601] [--ontology PATH]
text-features --ticker TICKER --as-of YYYY-MM-DD [--available-at ISO8601]
```

Default ontology path: `config/themes.yaml`.

- [ ] **Step 5: Run tests and lint**

```powershell
python -m pytest tests/integration/test_text_pipeline.py -q
python -m ruff check src/catalyst_radar/textint src/catalyst_radar/cli.py tests/integration/test_text_pipeline.py
```

- [ ] **Step 6: Commit**

```powershell
git add src/catalyst_radar/textint/pipeline.py src/catalyst_radar/cli.py tests/integration/test_text_pipeline.py
git commit -m "feat: run local text intelligence pipeline"
```

## Task 4: Integrate Text Features Into Scan And Dashboard

**Files:**

- Modify: `src/catalyst_radar/pipeline/scan.py`
- Modify: `src/catalyst_radar/scoring/score.py`
- Modify: `src/catalyst_radar/dashboard/data.py`
- Test: `tests/integration/test_text_scan_integration.py`
- Test: `tests/unit/test_score.py`

- [ ] **Step 1: Write scan integration tests**

Test cases:

- Scan attaches point-in-time text feature metadata when `text_repo` is passed.
- Future-available text features are ignored.
- Dashboard rows expose `local_narrative_score`, `novelty_score`, `theme_hits`, `sentiment_score`, and `selected_snippet_count`.
- A max local narrative score cannot override stale-data policy.

- [ ] **Step 2: Update scoring**

Add optional `local_narrative_score` to `candidate_from_features()`.

Bounded bonus:

```text
local_narrative_bonus = min(6.0, max(0.0, local_narrative_score) * 0.06)
```

This stacks with the existing capped event bonus, but policy hard blocks still dominate.

Update:

```python
SCORE_VERSION = "score-v3-textint"
```

- [ ] **Step 3: Update scan**

Change signature:

```python
def run_scan(..., text_repo: TextRepository | None = None)
```

When present, read latest text features by ticker with point-in-time filters and add metadata:

```text
local_narrative_score
local_narrative_bonus
novelty_score
sentiment_score
source_quality_score
theme_match_score
theme_hits
selected_snippet_ids
selected_snippet_count
text_feature_version
```

- [ ] **Step 4: Update CLI scan**

Instantiate `TextRepository` in `scan` command and pass it to `run_scan()`.

- [ ] **Step 5: Update dashboard data**

Extract the metadata fields above into dashboard rows.

- [ ] **Step 6: Run tests and lint**

```powershell
python -m pytest tests/integration/test_text_scan_integration.py tests/unit/test_score.py tests/integration/test_event_scan_integration.py -q
python -m ruff check src tests apps
```

- [ ] **Step 7: Commit**

```powershell
git add src/catalyst_radar/pipeline/scan.py src/catalyst_radar/scoring/score.py src/catalyst_radar/dashboard/data.py src/catalyst_radar/cli.py tests/integration/test_text_scan_integration.py tests/unit/test_score.py tests/integration/test_event_scan_integration.py
git commit -m "feat: attach local text intelligence to scans"
```

## Task 5: Full Verification, Review, And Phase Notes

**Files:**

- Create: `docs/phase-6-review.md`

- [ ] **Step 1: Run full suite**

```powershell
python -m pytest
```

Expected:

```text
all tests passed
```

- [ ] **Step 2: Run lint**

```powershell
python -m ruff check src tests apps
```

Expected:

```text
All checks passed!
```

- [ ] **Step 3: Run text intelligence smoke**

```powershell
Remove-Item data/local/catalyst_radar.db -ErrorAction SilentlyContinue
$env:CATALYST_DATABASE_URL="sqlite:///data/local/catalyst_radar.db"
python -m catalyst_radar.cli init-db
python -m catalyst_radar.cli ingest-csv --securities tests/fixtures/securities.csv --daily-bars tests/fixtures/daily_bars.csv --holdings tests/fixtures/holdings.csv
python -m catalyst_radar.cli ingest-news --fixture tests/fixtures/news/ticker_news_msft.json
python -m catalyst_radar.cli run-textint --as-of 2026-05-10 --available-at 2026-05-10T14:00:00Z --ontology config/themes.yaml
python -m catalyst_radar.cli text-features --ticker MSFT --as-of 2026-05-10 --available-at 2026-05-10T14:00:00Z
python -m catalyst_radar.cli scan --as-of 2026-05-08
```

- [ ] **Step 4: Run existing event and Polygon smokes**

Use the Phase 5 smoke commands from `docs/phase-5-review.md`.

- [ ] **Step 5: Final code review**

Dispatch a review subagent:

```text
Review Phase 6 local text intelligence. Focus on point-in-time correctness, text-feature dedupe, ontology parser limits, local narrative score bounds, snippet source traceability, and regressions to Phase 5 event scanning. Do not edit files.
```

Fix every high or medium finding.

- [ ] **Step 6: Write review note**

`docs/phase-6-review.md` must include:

- outcome
- verification command outputs
- text intelligence smoke output
- event/Polygon regression smoke output
- review findings fixed
- residual risks

Residual risks to carry unless removed:

- Hashing-vector embeddings are deterministic fallback embeddings, not semantic transformer embeddings.
- Ontology parser supports only the repo’s simple config subset.
- Sentiment is phrase-based and conservative.
- No LLM evidence packets or Decision Cards exist yet.
- No paid transcripts/news provider is integrated.

- [ ] **Step 7: Commit review notes**

```powershell
git add docs/phase-6-review.md
git commit -m "docs: record phase 6 verification"
```

## Subagent Work Split

Use non-overlapping write sets:

- Worker A: Task 1 only. Owns text models, schema, text repository, migration, repository tests.
- Worker B: Task 2 only. Owns ontology/snippet/sentiment/embedding/novelty primitives and unit tests.
- Worker C: Task 3 only after Workers A and B finish. Owns text pipeline, CLI text commands, text pipeline tests.
- Main agent: Task 4 scan/scoring/dashboard integration and final verification.

Workers are not alone in the codebase. Each worker must preserve edits from other workers, avoid reverting unrelated changes, and list changed files in the final response.

## Phase Acceptance Criteria

- Text snippets and features are persisted separately from events.
- Every snippet and feature has `source_ts` and `available_at`.
- Text feature reads and scan integration honor `available_at`.
- Ontology hits are source-linked to snippets and themes.
- Novelty is deterministic and point-in-time.
- Local narrative support is bounded and cannot bypass hard policy gates.
- Existing event, CSV, and Polygon smokes remain working.
- Full test suite and ruff pass.
- Phase review note exists with residual risks.

## Execution Start

After this plan is committed:

```powershell
git checkout main
git worktree add .worktrees/phase-6-local-text-intelligence -b feature/phase-6-local-text-intelligence
cd .worktrees/phase-6-local-text-intelligence
python -m pip install -e ".[dev]"
python -m pytest
python -m ruff check src tests apps
```

Do not start implementation unless the worktree is clean and baseline verification passes.

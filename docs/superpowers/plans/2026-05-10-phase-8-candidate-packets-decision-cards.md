# Phase 8 Candidate Packets And Decision Cards Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convert deterministic scan results into durable, point-in-time candidate packets and complete human review decision cards so every Warning-or-higher candidate has source-linked support, disconfirming evidence, conflicts, hard blocks, portfolio context, and next review guidance.

**Architecture:** Candidate packets are deterministic evidence assemblies built from already persisted scan payloads, events, snippets, text features, option features, portfolio impacts, and policy outcomes. Decision cards are deterministic manual-review artifacts built from candidate packets. This phase creates LLM-ready schemas and validation rules, but does not make live LLM calls and does not produce automated trade instructions.

**Tech Stack:** Python 3.11, SQLAlchemy Core, SQLite-compatible local storage with PostgreSQL migration SQL, existing scan/event/text/feature repositories, pytest, ruff.

---

## Current Baseline

Build from:

```text
main @ a104b1c
```

Current verified baseline:

- `python -m pytest` passes with 250 tests.
- `python -m ruff check src tests apps` passes.
- Deterministic scoring includes market, event, local text, options, sector, theme, peer, setup, and portfolio impact metadata.
- Candidate states and signal feature payloads are persisted.
- Portfolio impact rows are persisted with point-in-time metadata.
- Options, theme, sector, and peer features are evidence-only and cannot override hard policy gates.

Important current limit:

- Candidate state rows store policy outcomes, but do not persist a unified candidate packet.
- Warning-or-higher candidates do not yet have a guaranteed supporting and disconfirming evidence section.
- Decision cards do not exist yet.
- Dashboard rows expose scan metadata, but not packet/card availability or review artifacts.
- LLM review and budget controls remain future phases; this phase must stay deterministic.

## Scope

In this phase, implement:

- `candidate_packets` table with deterministic packet JSON, source timestamps, availability timestamps, and schema version.
- `decision_cards` table with deterministic card JSON, source timestamps, availability timestamps, schema version, and user decision placeholder.
- Candidate packet models and validation helpers.
- Candidate packet builder from latest persisted candidate state and signal feature payloads.
- Supporting evidence extraction from events, snippets, computed features, options, sector/theme/peer metadata, and setup/portfolio context.
- Disconfirming evidence extraction from hard blocks, missing trade plan, data stale flags, event conflicts, option risk, chase risk, portfolio blocks, and weak evidence.
- Decision card builder for Warning and EligibleForManualBuyReview candidates, with full required fields and explicit manual-review language.
- CLI commands to build and inspect packets/cards.
- Dashboard data exposure for packet/card presence, top evidence, hard blocks, conflicts, and next review time.
- Fixture-backed integration tests proving point-in-time behavior and deterministic replay.

Out of scope:

- Live LLM calls, OpenAI client wiring, or prompt execution.
- Skeptic Agent generation beyond deterministic bear-case/disconfirming-evidence rules.
- Alerts, email/webhook delivery, or feedback links.
- Paper trading and backtest outcome analytics.
- Automated order placement or any statement that the system makes a buy decision.

## File Structure

Create:

- `src/catalyst_radar/pipeline/candidate_packet.py`  
  Candidate packet models, evidence item models, validation, and builder.
- `src/catalyst_radar/decision_cards/__init__.py`
- `src/catalyst_radar/decision_cards/models.py`  
  Decision card models and required-field validation.
- `src/catalyst_radar/decision_cards/builder.py`  
  Deterministic decision card creation from packets.
- `src/catalyst_radar/storage/candidate_packet_repositories.py`  
  Persistence and point-in-time reads for packets and cards.
- `sql/migrations/008_candidate_packets_decision_cards.sql`
- `tests/unit/test_candidate_packet_builder.py`
- `tests/unit/test_decision_card_builder.py`
- `tests/integration/test_candidate_packet_repository.py`
- `tests/integration/test_candidate_packets_cli.py`
- `tests/golden/test_candidate_packets_replay.py`
- `docs/phase-8-review.md`

Modify:

- `src/catalyst_radar/storage/schema.py`  
  Add `candidate_packets` and `decision_cards` SQLAlchemy tables and indexes.
- `src/catalyst_radar/cli.py`  
  Add packet/card build and inspect commands.
- `src/catalyst_radar/dashboard/data.py`  
  Expose latest packet/card metadata for candidate rows.
- `src/catalyst_radar/storage/repositories.py`  
  Add helper to retrieve candidate state IDs or latest candidate states if needed.
- Existing tests that assert table inventory or dashboard row shape.

## Data Contracts

`candidate_packets` rows:

```text
id                    deterministic ticker/as_of/state/schema id
ticker                uppercase ticker
as_of                 candidate timestamp
candidate_state_id    candidate_states.id when available
state                 action state at packet creation
final_score           candidate final score
schema_version        candidate-packet-v1
source_ts             max source timestamp across selected packet inputs
available_at          max availability timestamp across selected packet inputs
payload               JSON packet containing evidence, conflicts, blocks, features, policy, and audit metadata
created_at            persistence timestamp
```

`decision_cards` rows:

```text
id                    deterministic ticker/as_of/action_state/schema id
ticker                uppercase ticker
as_of                 candidate timestamp
candidate_packet_id   candidate_packets.id
action_state          Warning or EligibleForManualBuyReview
setup_type            setup selected by deterministic policy
final_score           candidate final score
schema_version        decision-card-v1
source_ts             candidate packet source timestamp
available_at          card creation availability timestamp
next_review_at        deterministic next review time
user_decision         nullable manual feedback placeholder
payload               JSON decision card
created_at            persistence timestamp
```

Candidate packet payload:

```text
identity              ticker, as_of, state, versions
scores                final, pillars, risk penalty, portfolio penalty, score deltas when available
trade_plan            entry zone, invalidation, reward/risk, missing fields
portfolio_impact     exposure before/after, max loss, hard blocks
supporting_evidence   source-linked and computed-feature evidence items
disconfirming_evidence source-linked and computed-feature evidence items
conflicts             unresolved event/data/policy conflicts
hard_blocks           hard policy blocks
escalation            deterministic review/card eligibility reasons
audit                 source_ts, available_at, feature/policy/schema versions
```

Decision card payload:

```text
identity              ticker, company when known, version, as_of, action_state, setup_type
scores                final score, pillar scores, risk penalty, portfolio penalty, score delta
trade_plan            entry zone, invalidation price, max loss if wrong, reward/risk
position_sizing       risk_per_trade_pct, shares, notional, cash check, sizing notes
portfolio_impact      single-name, sector, theme, correlated-basket exposure before/after
evidence              top supporting evidence
disconfirming_evidence top disconfirming evidence and bear-case bullets
controls              hard blocks, missing trade plan, upcoming events, next review time, user decision
disclaimer            manual review only; no automated trade placement
audit                 packet id, schema version, source_ts, available_at
```

## Invariants

Point-in-time invariant:

```text
Packet and card builders may use only records whose available_at <= requested available_at.
```

Source-link invariant:

```text
Every evidence item must contain at least one of source_id, source_url, or computed_feature_id.
```

Manual-review invariant:

```text
Decision cards are decision-support artifacts only. They must not say the system is buying, selling, recommending, executing, or placing an order.
```

Eligibility invariant:

```text
EligibleForManualBuyReview cards require entry zone, invalidation, reward/risk, position sizing, portfolio impact, supporting evidence, disconfirming evidence, hard-block summary, and next review time.
```

Evidence sufficiency invariant:

```text
Warning-or-higher candidates must have at least one supporting evidence item and at least one disconfirming evidence item. If deterministic inputs cannot produce a disconfirming item, add an explicit evidence_gap item tied to the computed candidate state.
```

## Task 1: Schema, Models, And Repository

**Files:**

- Create: `src/catalyst_radar/storage/candidate_packet_repositories.py`
- Create: `src/catalyst_radar/pipeline/candidate_packet.py`
- Modify: `src/catalyst_radar/storage/schema.py`
- Create: `sql/migrations/008_candidate_packets_decision_cards.sql`
- Test: `tests/integration/test_candidate_packet_repository.py`

- [x] **Step 1: Write repository tests first**

Cover:

- Upserting the same packet ID replaces the prior payload.
- Latest packet lookup respects ticker, as_of, and available_at.
- Future-available packets are excluded.
- Decision card lookup can return the latest card for a ticker/as_of.
- Packet/card JSON round trips without losing evidence arrays.

- [x] **Step 2: Add SQLAlchemy schema and migration**

Add tables:

- `candidate_packets`
- `decision_cards`

Indexes:

- `ix_candidate_packets_ticker_as_of_available_at`
- `ix_candidate_packets_state_available_at`
- `ix_decision_cards_ticker_as_of_available_at`
- `ix_decision_cards_action_state_available_at`

Use JSON-compatible columns for SQLite and JSONB variants for PostgreSQL.

- [x] **Step 3: Add packet model dataclasses**

Include:

```python
CANDIDATE_PACKET_SCHEMA_VERSION = "candidate-packet-v1"

@dataclass(frozen=True)
class EvidenceItem:
    kind: str
    title: str
    summary: str
    polarity: Literal["supporting", "disconfirming", "neutral"]
    strength: float
    source_id: str | None = None
    source_url: str | None = None
    computed_feature_id: str | None = None
    source_quality: float | None = None
    source_ts: datetime | None = None
    available_at: datetime | None = None
    payload: Mapping[str, Any] = field(default_factory=dict)
```

Add `CandidatePacket` with:

- `id`
- `ticker`
- `as_of`
- `candidate_state_id`
- `state`
- `final_score`
- `supporting_evidence`
- `disconfirming_evidence`
- `conflicts`
- `hard_blocks`
- `payload`
- `schema_version`
- `source_ts`
- `available_at`

Validation:

- Reject naive timestamps.
- Reject evidence without source/computed-feature linkage.
- Reject Warning-or-higher packets missing supporting or disconfirming evidence.

- [x] **Step 4: Add repository methods**

Methods:

```python
upsert_candidate_packet(packet: CandidatePacket) -> None
upsert_decision_card(card: DecisionCard) -> None
latest_candidate_packet(ticker: str, as_of: datetime, available_at: datetime) -> CandidatePacket | None
latest_decision_card(ticker: str, as_of: datetime, available_at: datetime) -> DecisionCard | None
list_latest_cards(as_of: datetime, available_at: datetime, limit: int = 200) -> list[DecisionCard]
```

Use deterministic IDs so rebuilding is idempotent.

## Task 2: Candidate Packet Builder

**Files:**

- Modify: `src/catalyst_radar/pipeline/candidate_packet.py`
- Modify: `src/catalyst_radar/storage/repositories.py`
- Test: `tests/unit/test_candidate_packet_builder.py`
- Test: `tests/golden/test_candidate_packets_replay.py`

- [x] **Step 1: Write builder unit tests**

Cover:

- Warning candidate gets supporting evidence from event/text/computed features.
- Warning candidate gets disconfirming evidence from missing trade plan, conflict, risk, or evidence gap.
- Blocked candidate carries hard blocks and block reasons.
- EligibleForManualBuyReview packet contains trade plan and portfolio impact.
- Evidence item validation rejects unsupported claims.
- Rebuilding the same inputs yields the same packet ID and payload.

- [x] **Step 2: Load packet inputs from persisted scan payloads**

Source:

- `candidate_states`
- matching `signal_features.payload`
- optional matching `portfolio_impacts`
- optional selected `events`
- optional selected `text_snippets`
- optional selected `text_features`
- optional selected `option_features`

Do not recompute scores in the builder. It must explain the stored state.

- [x] **Step 3: Build supporting evidence deterministically**

Evidence sources:

- Top high-quality material event.
- Selected text snippets and ontology/theme hits.
- Strong price/relative-strength/volume pillar scores.
- Positive local narrative score.
- Positive option flow score, with aggregate-only wording.
- Positive sector/theme/peer support.
- Complete setup plan.
- Portfolio impact within limits.

Computed feature IDs:

```text
signal_features:<ticker>:<as_of>:<feature_version>:pillar_scores
signal_features:<ticker>:<as_of>:<feature_version>:local_narrative_score
signal_features:<ticker>:<as_of>:<feature_version>:options_flow_score
signal_features:<ticker>:<as_of>:<feature_version>:sector_theme_bonus
portfolio_impacts:<ticker>:<as_of>:<setup_type>
```

- [x] **Step 4: Build disconfirming evidence deterministically**

Evidence sources:

- Hard blocks.
- Missing trade plan fields.
- Stale data.
- Event conflicts.
- Weak or missing local narrative.
- Elevated risk penalty.
- Elevated options risk score.
- Chase block.
- Portfolio concentration/cash blocks.
- Low reward/risk or no invalidation.
- Explicit evidence gap when no stronger disconfirming evidence exists.

Computed feature IDs:

```text
candidate_states:<id>:hard_blocks
candidate_states:<id>:transition_reasons
signal_features:<ticker>:<as_of>:<feature_version>:risk_penalty
signal_features:<ticker>:<as_of>:<feature_version>:portfolio_penalty
signal_features:<ticker>:<as_of>:<feature_version>:missing_trade_plan
```

- [x] **Step 5: Build escalation metadata**

Include:

- `packet_required` when state is `Warning` or `EligibleForManualBuyReview`.
- `decision_card_required` when state is `EligibleForManualBuyReview`.
- `llm_review_candidate` when state is `Warning` or higher, but record `llm_review_status="not_configured_phase_8"`.
- `no_trade_execution=true`.

## Task 3: Deterministic Decision Cards

**Files:**

- Create: `src/catalyst_radar/decision_cards/models.py`
- Create: `src/catalyst_radar/decision_cards/builder.py`
- Test: `tests/unit/test_decision_card_builder.py`

- [x] **Step 1: Write card builder tests**

Cover:

- EligibleForManualBuyReview card includes every spec-required field.
- Warning card can be created as a research card but is not labeled buy-review eligible.
- Missing required trade plan blocks EligibleForManualBuyReview card generation.
- Hard-blocked candidates produce a blocked/research card only, not a buy-review card.
- Position sizing is copied from deterministic scan metadata.
- Card language contains manual review wording and no execution wording.

- [x] **Step 2: Add decision card dataclasses**

Include:

```python
DECISION_CARD_SCHEMA_VERSION = "decision-card-v1"

@dataclass(frozen=True)
class DecisionCard:
    id: str
    ticker: str
    as_of: datetime
    candidate_packet_id: str
    action_state: ActionState
    setup_type: str | None
    final_score: float
    next_review_at: datetime
    payload: Mapping[str, Any]
    schema_version: str
    source_ts: datetime
    available_at: datetime
    user_decision: str | None = None
```

Validation:

- Reject naive timestamps.
- Require card payload identity/scores/trade_plan/portfolio_impact/evidence/disconfirming_evidence/controls/audit.
- Require manual-review disclaimer.
- Reject phrases such as `buy now`, `sell now`, `execute`, `place order`, or `automatic trade`.

- [x] **Step 3: Build deterministic card payload**

Rules:

- Identity: ticker, company/name when present in metadata, version, as_of, action_state, setup_type.
- Scores: final, pillar scores, risk penalty, portfolio penalty, score delta when available.
- Trade plan: entry zone, invalidation, max loss, reward/risk.
- Position sizing: copy computed shares/notional/risk from metadata; do not recalculate if unavailable.
- Portfolio impact: copy deterministic impact payload.
- Evidence: top supporting items by strength/source quality.
- Disconfirming evidence: top disconfirming items by strength/source quality.
- Controls: hard blocks, conflicts, missing trade plan, next review time, user decision placeholder.
- Audit: candidate packet id, schema version, source_ts, available_at.

Next review time:

- EligibleForManualBuyReview: next market session open placeholder at `as_of + 1 day 13:30 UTC`.
- Warning: `as_of + 2 days`.
- Blocked/Research card if built explicitly: `as_of + 7 days`.

## Task 4: CLI And Dashboard Integration

**Files:**

- Modify: `src/catalyst_radar/cli.py`
- Modify: `src/catalyst_radar/dashboard/data.py`
- Test: `tests/integration/test_candidate_packets_cli.py`
- Test: existing dashboard data tests or add focused coverage if none exist.

- [x] **Step 1: Add CLI parser commands**

Commands:

```text
build-packets --as-of YYYY-MM-DD [--available-at ISO] [--ticker TICKER] [--min-state Warning]
build-decision-cards --as-of YYYY-MM-DD [--available-at ISO] [--ticker TICKER]
candidate-packet --ticker TICKER --as-of YYYY-MM-DD [--available-at ISO] [--json]
decision-card --ticker TICKER --as-of YYYY-MM-DD [--available-at ISO] [--json]
```

Output examples:

```text
built candidate_packets=2
built decision_cards=1
MSFT packet state=Warning supporting=4 disconfirming=2 conflicts=0
MSFT decision_card state=EligibleForManualBuyReview next_review_at=2026-05-11T13:30:00+00:00
```

- [x] **Step 2: Add build command behavior**

Rules:

- `build-packets` reads persisted candidate states from the repository and builds packets for Warning, EligibleForManualBuyReview, Blocked, ThesisWeakening, and ExitInvalidateReview by default.
- `--min-state Warning` excludes NoAction, ResearchOnly, and AddToWatchlist unless explicitly requested.
- `build-decision-cards` builds cards only when a packet exists or builds the packet first within the same command.
- Commands return nonzero only for structural failures, not for zero eligible candidates.

- [x] **Step 3: Add inspect command behavior**

Rules:

- Human output is concise and source-linked.
- `--json` prints full JSON payload.
- Missing packet/card returns exit code 1 with a clear message.

- [x] **Step 4: Dashboard data additions**

Add fields:

- `candidate_packet_id`
- `candidate_packet_available_at`
- `supporting_evidence_count`
- `disconfirming_evidence_count`
- `decision_card_id`
- `decision_card_available_at`
- `next_review_at`
- `manual_review_disclaimer`

Keep dashboard data read-only.

## Task 5: Verification, Smoke, Review, And Documentation

**Files:**

- Create: `docs/phase-8-review.md`
- Modify: phase plan checklist while executing.

- [x] **Step 1: Run focused tests as tasks land**

Commands:

```text
python -m pytest tests/unit/test_candidate_packet_builder.py
python -m pytest tests/unit/test_decision_card_builder.py
python -m pytest tests/integration/test_candidate_packet_repository.py
python -m pytest tests/integration/test_candidate_packets_cli.py
python -m pytest tests/golden/test_candidate_packets_replay.py
```

- [x] **Step 2: Run full verification**

Commands:

```text
python -m pytest
python -m ruff check src tests apps
```

- [x] **Step 3: Run fixture smoke**

Use an isolated SQLite database:

```text
$env:CATALYST_DATABASE_URL="sqlite:///tmp/phase8-smoke.db"
python -m catalyst_radar.cli init-db
python -m catalyst_radar.cli ingest-csv --securities tests/fixtures/market/securities.csv --daily-bars tests/fixtures/market/daily_bars.csv --holdings tests/fixtures/portfolio/holdings.csv
python -m catalyst_radar.cli ingest-news --fixture tests/fixtures/news/news_events.json
python -m catalyst_radar.cli ingest-sec submissions --ticker MSFT --cik 0000789019 --fixture tests/fixtures/sec/msft_submissions.json
python -m catalyst_radar.cli ingest-earnings --fixture tests/fixtures/earnings/earnings_calendar.json
python -m catalyst_radar.cli ingest-options --fixture tests/fixtures/options/options_summary_2026-05-08.json
python -m catalyst_radar.cli run-textint --as-of 2026-05-10 --available-at 2026-05-10T13:00:00+00:00
python -m catalyst_radar.cli scan --as-of 2026-05-10 --available-at 2026-05-10T13:00:00+00:00
python -m catalyst_radar.cli build-packets --as-of 2026-05-10 --available-at 2026-05-10T13:00:00+00:00
python -m catalyst_radar.cli build-decision-cards --as-of 2026-05-10 --available-at 2026-05-10T13:00:00+00:00
python -m catalyst_radar.cli candidate-packet --ticker MSFT --as-of 2026-05-10 --available-at 2026-05-10T13:00:00+00:00
python -m catalyst_radar.cli decision-card --ticker MSFT --as-of 2026-05-10 --available-at 2026-05-10T13:00:00+00:00
```

If no fixture candidate reaches buy-review state, inspect the top available Warning candidate instead and record that in the review doc.

- [x] **Step 4: Review pass**

Ask subagents to review:

```text
Review Phase 8 candidate packet and decision card implementation. Focus on point-in-time correctness, source-link enforcement, deterministic replay, unsupported claim prevention, manual-review wording, and whether Warning-or-higher candidates always receive supporting and disconfirming evidence. Do not edit files.
```

- [x] **Step 5: Document phase outcome**

`docs/phase-8-review.md` must include:

- Outcome.
- Verification commands and exact pass/fail outputs.
- Fixture smoke output.
- Review findings and fixes.
- Residual risks.

## Exit Criteria

- Every Warning-or-higher persisted candidate can produce a candidate packet.
- Every candidate packet contains source-linked supporting and disconfirming evidence.
- Every EligibleForManualBuyReview candidate can produce a complete deterministic decision card.
- Decision cards contain entry zone, invalidation, sizing, reward/risk, portfolio impact, evidence, conflicts, hard blocks, and next review time when available.
- Decision cards state manual review only and do not imply automated trading.
- Packet/card builders are point-in-time and idempotent.
- Dashboard data exposes packet/card availability.
- CLI supports build and inspect workflows.
- Full pytest and ruff pass.
- Phase review document exists with smoke evidence.


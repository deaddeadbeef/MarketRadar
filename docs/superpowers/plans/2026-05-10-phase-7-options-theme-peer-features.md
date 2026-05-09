# Phase 7 Options, Theme, And Peer Features Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add deterministic options aggregate features, sector/theme features, and peer read-through support so scoring covers the next non-LLM evidence pillars without producing options trade recommendations.

**Architecture:** Build fixture-first feature modules and storage around aggregate data only. Options features, theme velocity, sector rotation, and peer read-through are point-in-time evidence signals that can add bounded support to candidate metadata and scoring; missing optional data remains neutral unless a later setup explicitly requires it.

**Tech Stack:** Python 3.11, SQLAlchemy Core, SQLite-compatible local storage with PostgreSQL migration SQL, existing provider ingest contracts, pandas/numpy for market math, pytest, ruff.

---

## Current Baseline

Build from:

```text
main @ f86fd88
```

Current verified baseline:

- `python -m pytest` passes with 222 tests.
- `python -m ruff check src tests apps` passes.
- Canonical events and local text intelligence are merged.
- Scan metadata includes market, setup, portfolio, event, and local text evidence.
- Event and local text bonuses are bounded and cannot override hard policy gates.

Important current limit:

- There is no options aggregate storage or connector.
- Sector rotation remains implicit in existing market RS fields only.
- Theme velocity and peer read-through do not exist yet.
- Scoring does not yet include explicit options, sector/theme, or peer support metadata.

## Scope

In this phase, implement:

- `option_features` table with point-in-time aggregate options signals.
- Fixture-first options aggregate connector.
- Provider ingest support for normalized option feature records.
- Options feature scoring from call/put ratio, call volume/open-interest pressure, IV percentile, skew, and abnormal activity.
- Theme config and deterministic theme membership.
- Theme velocity from candidate/text/theme evidence available in time.
- Peer read-through from event/text themes into related tickers.
- Sector rotation score from existing sector ETF bars and ticker/sector relative movement.
- Scan metadata and bounded score support for options, sector rotation, theme velocity, and peer confirmation.

Out of scope:

- Per-contract option chains.
- Option trade recommendations, strikes, expiries, Greeks, or trade execution.
- Paid options provider integration.
- Statistical correlation model beyond existing sector/theme placeholders.
- Candidate packets and LLM evidence packets.

## File Structure

Create:

- `config/theme_peers.yaml`  
  Theme membership, peers, and read-through mappings for fixture-tested themes.
- `src/catalyst_radar/features/options.py`  
  `OptionFeatureInput`, `OptionFeatureScore`, aggregate options feature scoring.
- `src/catalyst_radar/features/theme.py`  
  Theme config parser, theme membership, and theme velocity scoring.
- `src/catalyst_radar/features/peers.py`  
  Peer read-through scoring from theme/event/text evidence.
- `src/catalyst_radar/features/sector.py`  
  Sector rotation scoring from ticker, SPY, and sector ETF bars/features.
- `src/catalyst_radar/connectors/options.py`  
  Fixture-first aggregate options connector producing normalized records.
- `src/catalyst_radar/storage/feature_repositories.py`  
  Option feature persistence and point-in-time reads.
- `sql/migrations/007_options_theme.sql`
- `tests/fixtures/options/options_summary_2026-05-08.json`
- `tests/unit/test_options_features.py`
- `tests/unit/test_theme_features.py`
- `tests/unit/test_peer_readthrough.py`
- `tests/unit/test_sector_features.py`
- `tests/integration/test_options_ingest.py`
- `tests/integration/test_options_theme_scan.py`

Modify:

- `src/catalyst_radar/connectors/base.py`  
  Add `ConnectorRecordKind.OPTION_FEATURE`.
- `src/catalyst_radar/connectors/provider_ingest.py`  
  Persist normalized option feature records when `feature_repo` is supplied.
- `src/catalyst_radar/cli.py`  
  Add `ingest-options --fixture`, wire `FeatureRepository` into scan.
- `src/catalyst_radar/storage/schema.py`  
  Add `option_features`.
- `src/catalyst_radar/pipeline/scan.py`  
  Attach options/theme/peer/sector metadata and pass bounded support into scoring.
- `src/catalyst_radar/scoring/score.py`  
  Add bounded support components and score version.
- `src/catalyst_radar/dashboard/data.py`  
  Expose options/theme/peer/sector fields.
- Existing tests that assert score version.

## Data Contracts

`option_features` rows:

```text
id                    deterministic provider/ticker/as_of id
ticker                uppercase ticker
as_of                 feature timestamp
provider              options data provider or fixture name
call_volume           aggregate call volume
put_volume            aggregate put volume
call_open_interest    aggregate call open interest
put_open_interest     aggregate put open interest
iv_percentile         0.0 to 1.0
skew                  put/call or provider-normalized skew, finite float
abnormality_score     0.0 to 100.0
source_ts             provider source timestamp
available_at          point-in-time availability timestamp
payload               JSON audit payload
created_at            persistence timestamp
```

Point-in-time invariant:

```text
Option, theme, sector, and peer features can affect scans only when their inputs have available_at <= scan.available_at.
```

Policy invariant:

```text
Options, sector, theme, and peer features can add bounded support and evidence. They cannot override stale data, liquidity, risk, portfolio, cash, chase, or unresolved-conflict gates.
```

## Task 1: Option Feature Models, Storage, And Scoring

**Files:**

- Create: `src/catalyst_radar/features/options.py`
- Modify: `src/catalyst_radar/storage/schema.py`
- Create: `src/catalyst_radar/storage/feature_repositories.py`
- Create: `sql/migrations/007_options_theme.sql`
- Test: `tests/unit/test_options_features.py`
- Test: `tests/integration/test_options_ingest.py`

- [ ] **Step 1: Write option scoring unit tests**

Cover:

- Call/put ratio above 1.5 scores positively when volume is meaningful.
- High IV percentile and high skew create risk penalty but still finite score.
- Missing or zero volume returns neutral `options_flow_score=0.0`, not an error.
- NaN/Inf inputs are finite-safe and return neutral/degraded fields.

- [ ] **Step 2: Implement `features/options.py`**

Add:

```python
OPTION_FEATURE_VERSION = "options-v1"

@dataclass(frozen=True)
class OptionFeatureInput:
    ticker: str
    as_of: datetime
    provider: str
    call_volume: float
    put_volume: float
    call_open_interest: float
    put_open_interest: float
    iv_percentile: float
    skew: float
    source_ts: datetime
    available_at: datetime
    payload: Mapping[str, Any] = field(default_factory=dict)

@dataclass(frozen=True)
class OptionFeatureScore:
    ticker: str
    as_of: datetime
    provider: str
    call_put_ratio: float
    call_oi_ratio: float
    iv_percentile: float
    skew: float
    abnormality_score: float
    options_flow_score: float
    options_risk_score: float
    source_ts: datetime
    available_at: datetime
    payload: Mapping[str, Any]
```

Function:

```python
compute_option_feature_score(input: OptionFeatureInput) -> OptionFeatureScore
```

Rules:

- Clamp `iv_percentile` to 0.0-1.0.
- `call_put_ratio = call_volume / max(put_volume, 1.0)`.
- `call_oi_ratio = call_open_interest / max(put_open_interest, 1.0)`.
- `abnormality_score` combines capped call/put ratio, call OI ratio, and IV percentile, 0-100.
- `options_flow_score` is 0-100 and finite-safe.
- `options_risk_score` rises with IV percentile above 0.8 and skew above 1.0.
- Reject naive timestamps and `available_at < source_ts`.

- [ ] **Step 3: Write repository tests**

Add integration tests proving:

- `upsert_option_features()` replaces by deterministic `id`.
- `latest_option_features_by_ticker()` respects `as_of` and `available_at`.
- Future-available option rows are ignored.

- [ ] **Step 4: Add schema, migration, and repository**

Add `option_features` table and `FeatureRepository` methods:

```python
upsert_option_features(rows: Iterable[OptionFeatureInput]) -> int
latest_option_features_by_ticker(tickers, as_of, available_at) -> dict[str, OptionFeatureInput]
```

- [ ] **Step 5: Run tests and commit**

```powershell
python -m pytest tests/unit/test_options_features.py tests/integration/test_options_ingest.py -q
python -m ruff check src/catalyst_radar/features/options.py src/catalyst_radar/storage/feature_repositories.py tests/unit/test_options_features.py tests/integration/test_options_ingest.py
git add src/catalyst_radar/features/options.py src/catalyst_radar/storage/schema.py src/catalyst_radar/storage/feature_repositories.py sql/migrations/007_options_theme.sql tests/unit/test_options_features.py tests/integration/test_options_ingest.py
git commit -m "feat: add option feature storage"
```

## Task 2: Fixture Options Connector And CLI Ingest

**Files:**

- Create: `src/catalyst_radar/connectors/options.py`
- Modify: `src/catalyst_radar/connectors/base.py`
- Modify: `src/catalyst_radar/connectors/provider_ingest.py`
- Modify: `src/catalyst_radar/cli.py`
- Create: `tests/fixtures/options/options_summary_2026-05-08.json`
- Test: `tests/integration/test_options_ingest.py`

- [ ] **Step 1: Write connector/CLI tests**

Fixture shape:

```json
{
  "as_of": "2026-05-08T21:00:00Z",
  "source_ts": "2026-05-08T20:45:00Z",
  "available_at": "2026-05-08T21:00:00Z",
  "provider": "options_fixture",
  "results": [
    {
      "ticker": "AAA",
      "call_volume": 12000,
      "put_volume": 4000,
      "call_open_interest": 50000,
      "put_open_interest": 30000,
      "iv_percentile": 0.72,
      "skew": 0.18
    }
  ]
}
```

Tests:

- Connector emits raw and normalized `OPTION_FEATURE` records.
- `ingest-options --fixture ...` persists one option feature.
- Output:

```text
ingested provider=options_fixture raw=1 normalized=1 option_features=1 rejected=0
```

- [ ] **Step 2: Implement connector and provider ingest support**

Add `ConnectorRecordKind.OPTION_FEATURE`.

`OptionsAggregateConnector`:

- reads fixture JSON
- normalizes each result into payload fields matching `OptionFeatureInput`
- uses deterministic identity `TICKER:as_of`
- source and availability timestamps come from fixture header unless row overrides them

`ingest_provider_records()`:

- accepts optional `feature_repo`
- persists option feature records if present
- fails closed if option records exist and `feature_repo` is missing

- [ ] **Step 3: Implement CLI**

Add:

```text
ingest-options --fixture PATH
```

Print provider-style output including a concrete count, for example `option_features=1`.

- [ ] **Step 4: Run tests and commit**

```powershell
python -m pytest tests/integration/test_options_ingest.py tests/unit/test_connector_contracts.py -q
python -m ruff check src/catalyst_radar/connectors src/catalyst_radar/cli.py tests/integration/test_options_ingest.py
git add src/catalyst_radar/connectors/base.py src/catalyst_radar/connectors/options.py src/catalyst_radar/connectors/provider_ingest.py src/catalyst_radar/cli.py tests/fixtures/options tests/integration/test_options_ingest.py
git commit -m "feat: ingest aggregate option features"
```

## Task 3: Theme, Sector, And Peer Feature Primitives

**Files:**

- Create: `config/theme_peers.yaml`
- Create: `src/catalyst_radar/features/theme.py`
- Create: `src/catalyst_radar/features/peers.py`
- Create: `src/catalyst_radar/features/sector.py`
- Test: `tests/unit/test_theme_features.py`
- Test: `tests/unit/test_peer_readthrough.py`
- Test: `tests/unit/test_sector_features.py`

- [ ] **Step 1: Write primitive tests**

Cover:

- `AAA` maps to `ai_infrastructure_storage` through Technology/Software fixture config.
- Theme velocity rises when local text feature has matching `theme_hits`.
- Peer read-through gives a bounded positive score to configured peers only when source theme evidence exists.
- Sector rotation score is finite and positive when ticker and sector ETF outperform SPY.
- Empty/missing inputs return neutral scores.

- [ ] **Step 2: Add theme config**

`config/theme_peers.yaml`:

```yaml
themes:
  ai_infrastructure_storage:
    sectors:
      - Technology
    industries:
      - Software
      - Semiconductors
    tickers:
      - AAA
      - CCC
    peers:
      - MSFT
      - NVDA
      - MU
  datacenter_power:
    sectors:
      - Industrials
    industries:
      - Construction
    tickers:
      - BBB
    peers:
      - ETN
      - PWR
      - VRT
```

Use the same simple YAML subset parser style as Phase 6. Do not add dependencies.

- [ ] **Step 3: Implement primitives**

Required functions:

```python
load_theme_peer_config(path: Path | str = Path("config/theme_peers.yaml")) -> ThemePeerConfig
theme_for_security(ticker, sector, industry, metadata, config) -> str
theme_velocity_score(text_feature: TextFeature | None, theme_id: str) -> float
peer_readthrough_score(ticker, source_theme_hits, config) -> PeerReadthroughScore
sector_rotation_score(ticker_bars, spy_bars, sector_bars) -> SectorRotationScore
```

Rules:

- Scores are 0-100, finite-safe.
- Peer confirmation is capped and requires matching theme evidence.
- Sector score should not require more history than existing fixtures provide.

- [ ] **Step 4: Run tests and commit**

```powershell
python -m pytest tests/unit/test_theme_features.py tests/unit/test_peer_readthrough.py tests/unit/test_sector_features.py -q
python -m ruff check src/catalyst_radar/features/theme.py src/catalyst_radar/features/peers.py src/catalyst_radar/features/sector.py tests/unit/test_theme_features.py tests/unit/test_peer_readthrough.py tests/unit/test_sector_features.py
git add config/theme_peers.yaml src/catalyst_radar/features/theme.py src/catalyst_radar/features/peers.py src/catalyst_radar/features/sector.py tests/unit/test_theme_features.py tests/unit/test_peer_readthrough.py tests/unit/test_sector_features.py
git commit -m "feat: add theme sector peer features"
```

## Task 4: Scan, Scoring, And Dashboard Integration

**Files:**

- Modify: `src/catalyst_radar/pipeline/scan.py`
- Modify: `src/catalyst_radar/scoring/score.py`
- Modify: `src/catalyst_radar/dashboard/data.py`
- Modify: `src/catalyst_radar/cli.py`
- Test: `tests/integration/test_options_theme_scan.py`
- Test: `tests/unit/test_score.py`

- [ ] **Step 1: Write scan integration tests**

Cover:

- Scan attaches point-in-time option feature metadata when `feature_repo` is passed.
- Future-available option features are ignored.
- Theme, sector rotation, and peer read-through fields appear in candidate metadata.
- Dashboard rows expose `options_flow_score`, `options_risk_score`, `sector_rotation_score`, `theme_velocity_score`, `peer_readthrough_score`, and `candidate_theme`.
- Max optional support cannot override stale-data policy.

- [ ] **Step 2: Update scoring**

Change:

```python
SCORE_VERSION = "score-v4-options-theme"
```

Add bounded optional bonuses:

```text
options_bonus = min(4.0, max(0.0, options_flow_score) * 0.04)
sector_theme_bonus = min(6.0, (sector_rotation_score * 0.02) + (theme_velocity_score * 0.02) + (peer_readthrough_score * 0.02))
options_risk_penalty = min(4.0, max(0.0, options_risk_score) * 0.04)
```

Final score adds optional bonuses and subtracts options risk, while hard policy gates remain unchanged.

- [ ] **Step 3: Update scan**

Add optional `feature_repo`.

For each candidate:

- Load latest option feature by ticker/as_of/available_at.
- Compute option score when option feature exists; otherwise neutral values.
- Load theme config and compute candidate theme.
- Compute theme velocity from text feature and theme.
- Compute peer read-through from text/theme evidence.
- Compute sector rotation from market bars.
- Add metadata fields:

```text
options_flow_score
options_risk_score
call_put_ratio
iv_percentile
sector_rotation_score
theme_velocity_score
peer_readthrough_score
candidate_theme
theme_feature_version
options_feature_version
```

- [ ] **Step 4: Update CLI scan and dashboard**

- CLI scan instantiates `FeatureRepository`.
- Dashboard rows expose the metadata above.

- [ ] **Step 5: Run tests and commit**

```powershell
python -m pytest tests/integration/test_options_theme_scan.py tests/unit/test_score.py tests/integration/test_text_scan_integration.py tests/integration/test_event_scan_integration.py -q
python -m ruff check src tests apps
git add src/catalyst_radar/pipeline/scan.py src/catalyst_radar/scoring/score.py src/catalyst_radar/dashboard/data.py src/catalyst_radar/cli.py tests/integration/test_options_theme_scan.py tests/unit/test_score.py
git commit -m "feat: attach options theme peer evidence to scans"
```

## Task 5: Full Verification, Review, And Phase Notes

**Files:**

- Create: `docs/phase-7-review.md`

- [ ] **Step 1: Run full suite**

```powershell
python -m pytest
```

- [ ] **Step 2: Run lint**

```powershell
python -m ruff check src tests apps
```

- [ ] **Step 3: Run options/theme smoke**

```powershell
Remove-Item data/local/catalyst_radar.db -ErrorAction SilentlyContinue
$env:CATALYST_DATABASE_URL="sqlite:///data/local/catalyst_radar.db"
python -m catalyst_radar.cli init-db
python -m catalyst_radar.cli ingest-csv --securities tests/fixtures/securities.csv --daily-bars tests/fixtures/daily_bars.csv --holdings tests/fixtures/holdings.csv
python -m catalyst_radar.cli ingest-options --fixture tests/fixtures/options/options_summary_2026-05-08.json
python -m catalyst_radar.cli scan --as-of 2026-05-08
```

- [ ] **Step 4: Run text, event, and Polygon regression smokes**

Use Phase 6 smoke commands from `docs/phase-6-review.md`.

- [ ] **Step 5: Final code review**

Dispatch final review:

```text
Review Phase 7 options/theme/peer features. Focus on point-in-time correctness, optional-data neutrality, score bounds, no options trade recommendation leakage, provider ingest regressions, and dashboard metadata shape. Do not edit files.
```

Fix every high or medium finding.

- [ ] **Step 6: Write review note**

`docs/phase-7-review.md` must include:

- outcome
- verification command outputs
- options/theme smoke output
- text/event/Polygon regression smoke output
- review findings fixed
- residual risks

Residual risks to carry unless removed:

- Options connector is fixture-only aggregate data.
- Options scores are evidence signals, not options trade recommendations.
- Theme/peer mappings are static config, not learned relationships.
- Sector rotation is deterministic fixture-scale math, not a full cross-sectional model.
- Candidate packets and validation remain future phases.

- [ ] **Step 7: Commit review notes**

```powershell
git add docs/phase-7-review.md
git commit -m "docs: record phase 7 verification"
```

## Subagent Work Split

Use non-overlapping write sets:

- Worker A: Task 1 only. Owns option feature model, storage, migration, repository, option feature tests.
- Worker B: Task 2 only after Worker A. Owns options connector, provider ingest support, fixture, CLI ingest tests.
- Worker C: Task 3 only. Owns theme/peer/sector primitives and unit tests.
- Main agent: Task 4 scan/scoring/dashboard integration and final verification.

Workers are not alone in the codebase. Each worker must preserve edits from other workers, avoid reverting unrelated changes, and list changed files in the final response.

## Phase Acceptance Criteria

- Aggregate options features persist with `source_ts` and `available_at`.
- Options fixture connector ingests without live network calls.
- Option, sector, theme, and peer support is finite-safe and point-in-time.
- Missing optional options data is neutral.
- Optional support is bounded and cannot bypass hard policy gates.
- No options trade recommendation is generated.
- Existing CSV, text, event, and Polygon smokes remain working.
- Full tests and ruff pass.
- Phase review note exists with residual risks.

## Execution Start

After this plan is committed:

```powershell
git checkout main
git worktree add .worktrees/phase-7-options-theme-peer-features -b feature/phase-7-options-theme-peer-features
cd .worktrees/phase-7-options-theme-peer-features
python -m pip install -e ".[dev]"
python -m pytest
python -m ruff check src tests apps
```

Do not start implementation unless the worktree is clean and baseline verification passes.

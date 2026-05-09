# Catalyst Radar v1.1.1 - Engineering Specification

Detailed implementation plan, schemas, APIs, pipelines, tests, and runbooks for the cost-optimized market radar.

Prepared for: Captain  
Date: May 9, 2026  
Version: v1.1.1  
Document type: Engineering Spec  
Architecture companion: Catalyst Radar v1.1.1 - Architecture Specification  
Status: Sharpened implementation baseline, preserving v1.1.0 detail and adding MVP decisions, expanded schemas, explicit portfolio controls, and acceptance gates

## Engineering Thesis

The implementation should maximize deterministic computation and minimize LLM spend. Build the signal engine first, prove it with backtests and shadow mode, then add sparse LLM escalation as a controlled synthesis layer. Every service must expose reproducible inputs, outputs, timestamps, costs, and testable contracts.

This tool supports investment review. It must not imply guaranteed returns, autonomous trading, or model authority over the user's judgment.

## 1. Implementation Overview

The first build should be a low-cost, deterministic-first MVP. The system should run nightly, scan the full universe without frontier LLMs, identify candidates, and produce dashboard rows. Sparse LLM calls are added only after feature and policy gates are stable.

```text
Data connectors pull bars, options, filings, news, calendar, transcripts
  -> Normalize and store raw/normalized data with availability timestamps
  -> Compute deterministic market features and local text features
  -> Score candidates and apply hard blocks
  -> Escalate to LLM review only when gates pass
  -> Generate Decision Cards, alerts, feedback, and validation outcomes
```

### Engineering Non-Negotiables

- All core features are point-in-time reproducible.
- LLM calls are optional for the scanner and never required to compute base scores.
- Every model call logs prompt version, schema version, token usage, estimated cost, actual cost, and candidate state.
- Any missing required Decision Card field downgrades action state to Blocked or Research Only.
- No real-money workflow until backtest, shadow mode, and paper-trading gates pass.
- Every action-state transition stores the rule, inputs, timestamp, and source records that caused it.
- Model pricing, provider credentials, budget caps, universe filters, and policy thresholds must be config-driven.

## 2. Repository and Service Layout

```text
catalyst-radar/
  apps/
    api/                     # FastAPI service
    dashboard/               # Next.js or Streamlit UI
    worker/                  # batch/queue workers
  packages/
    connectors/              # market, SEC, news, options, transcripts
    core/                    # config, logging, schemas, common utilities
    features/                # deterministic feature functions
    textint/                 # embeddings, ontology, local NLP
    scoring/                 # scoring model and policy gates
    portfolio/               # exposure, concentration, sizing, holdings adapters
    agents/                  # LLM router and prompt templates
    alerts/                  # email/telegram/slack/webhook
    validation/              # backtest, paper trade, evals
  infra/
    docker-compose.yml
    terraform/
    k8s/
  sql/
    migrations/
    seed/
  tests/
    unit/
    integration/
    golden/
    evals/
  notebooks/
    research/
    diagnostics/
  docs/
    architecture/
    engineering/
    runbooks/
```

### Service Boundaries

| Service | Input | Output |
| --- | --- | --- |
| connector-worker | Provider APIs/files | raw_* and normalized tables |
| feature-worker | normalized market data | signal_features |
| text-worker | events and documents | text_snippets, text_features |
| scoring-worker | features and events | candidate_states |
| portfolio-worker | holdings, candidate state, risk settings | portfolio_impact, sizing fields, exposure blocks |
| llm-worker | candidate packets | evidence_packets, decision_cards |
| api | DB plus services | dashboard and alert endpoints |
| validation-worker | historical data | backtest and paper-trade reports |

### MVP Build Choice

Unless overridden before implementation, the recommended MVP is:

- Python monorepo with package boundaries matching the layout above.
- FastAPI for API contracts.
- Streamlit for the first dashboard if speed matters most; Next.js if product-grade UI is required immediately.
- Postgres in Docker Compose.
- No premium LLM calls enabled by default in local/dev.

## 3. Data Connectors

### Connector Interface

```python
class Connector(Protocol):
    name: str
    version: str

    def fetch(self, start: datetime, end: datetime, **kwargs) -> list[RawRecord]: ...
    def normalize(self, records: list[RawRecord]) -> list[NormalizedRecord]: ...
    def healthcheck(self) -> ConnectorHealth: ...
    def estimate_cost(self, request: ConnectorRequest) -> Decimal: ...
```

### Connectors to Implement

| Connector | MVP requirement | Notes |
| --- | --- | --- |
| Securities master | Required | Universe, sector, industry, corporate actions, active flags. |
| Daily bars | Required | Adjusted OHLCV for full universe. |
| Intraday bars | Optional MVP | Active watchlist only. |
| Options aggregate | Recommended | Aggregated OI/IV/volume features, not full chains initially. |
| SEC filings | Required | data.sec.gov submissions, company facts, filing documents. |
| News | Required | Ticker news API/RSS with source-quality scoring. |
| Earnings calendar | Required | Event-risk windows and post-earnings setups. |
| Transcripts | Optional early | Only if affordable/licensed; local snippeting required. |
| Analyst revisions | Optional | High value but often paid. |
| Holdings/portfolio | Recommended by Sprint 3 | Can start as manual CSV upload, then broker/read-only adapter later. |

### Provider Abstraction Rules

- Store raw payloads for replay if licensing permits.
- Never assume provider timestamp is system availability timestamp.
- Normalize tickers through securities master to handle symbol changes.
- Separate connector failures from data-quality failures.
- Every provider response must include provider name, request ID if available, fetch timestamp, and license/retention tag.
- A connector can be healthy while the data is not actionable; data quality is evaluated downstream.

## 4. Database and Schemas

Use Postgres for MVP, with TimescaleDB optional for time-series partitioning and pgvector for local embeddings. Keep schemas explicit and migration-controlled.

### Core SQL Schema Excerpt

```sql
CREATE TABLE securities (
  ticker TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  exchange TEXT,
  sector TEXT,
  industry TEXT,
  market_cap NUMERIC,
  avg_dollar_volume_20d NUMERIC,
  has_options BOOLEAN DEFAULT FALSE,
  is_active BOOLEAN DEFAULT TRUE,
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE daily_bars (
  ticker TEXT REFERENCES securities(ticker),
  date DATE NOT NULL,
  open NUMERIC,
  high NUMERIC,
  low NUMERIC,
  close NUMERIC,
  volume BIGINT,
  vwap NUMERIC,
  adjusted BOOLEAN DEFAULT TRUE,
  provider TEXT,
  source_ts TIMESTAMPTZ,
  available_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (ticker, date, provider)
);

CREATE TABLE signal_features (
  ticker TEXT REFERENCES securities(ticker),
  as_of TIMESTAMPTZ NOT NULL,
  price_strength NUMERIC,
  volume_score NUMERIC,
  options_flow NUMERIC,
  local_narrative NUMERIC,
  fundamental_event NUMERIC,
  sector_rotation NUMERIC,
  novelty NUMERIC,
  risk_penalty NUMERIC,
  portfolio_impact NUMERIC,
  final_score NUMERIC,
  feature_version TEXT NOT NULL,
  payload JSONB NOT NULL,
  PRIMARY KEY (ticker, as_of, feature_version)
);
```

### Expanded MVP Tables

```sql
CREATE TABLE events (
  id UUID PRIMARY KEY,
  ticker TEXT REFERENCES securities(ticker),
  event_type TEXT NOT NULL,
  source TEXT NOT NULL,
  source_url TEXT,
  title TEXT,
  body_hash TEXT,
  source_quality NUMERIC,
  materiality NUMERIC,
  source_ts TIMESTAMPTZ,
  available_at TIMESTAMPTZ NOT NULL,
  payload JSONB NOT NULL
);

CREATE TABLE candidate_states (
  id UUID PRIMARY KEY,
  ticker TEXT REFERENCES securities(ticker),
  as_of TIMESTAMPTZ NOT NULL,
  state TEXT NOT NULL,
  previous_state TEXT,
  final_score NUMERIC,
  score_delta_5d NUMERIC,
  hard_blocks TEXT[] DEFAULT '{}',
  transition_reasons JSONB NOT NULL,
  feature_version TEXT NOT NULL,
  policy_version TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE evidence_packets (
  id UUID PRIMARY KEY,
  ticker TEXT REFERENCES securities(ticker),
  as_of TIMESTAMPTZ NOT NULL,
  candidate_state_id UUID REFERENCES candidate_states(id),
  claims JSONB NOT NULL,
  bear_case JSONB NOT NULL,
  unresolved_conflicts JSONB NOT NULL,
  recommended_policy_downgrade BOOLEAN DEFAULT FALSE,
  model TEXT,
  prompt_version TEXT,
  schema_version TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE decision_cards (
  id UUID PRIMARY KEY,
  ticker TEXT REFERENCES securities(ticker),
  as_of TIMESTAMPTZ NOT NULL,
  action_state TEXT NOT NULL,
  setup_type TEXT,
  final_score NUMERIC,
  entry_zone JSONB,
  invalidation_price NUMERIC,
  reward_risk NUMERIC,
  position_sizing JSONB,
  portfolio_impact JSONB,
  evidence_packet_id UUID REFERENCES evidence_packets(id),
  hard_blocks TEXT[] DEFAULT '{}',
  next_review_at TIMESTAMPTZ,
  user_decision TEXT,
  created_at TIMESTAMPTZ NOT NULL
);
```

### Additional Tables

| Table | Purpose |
| --- | --- |
| raw_provider_records | Immutable raw payloads and provider metadata. |
| option_features | Aggregated options signals by ticker/date. |
| text_snippets | Extracted snippets with embeddings, hashes, source quality. |
| holdings_snapshots | User holdings or manually imported exposure snapshots. |
| portfolio_impact | Single-name, sector, theme, and correlated-basket exposure before/after. |
| budget_ledger | LLM/tool/data costs by task. |
| paper_trades | Simulated decisions and outcomes. |
| model_evals | Agent/source-faithfulness/calibration evaluation results. |

## 5. Feature Engineering

### Market Feature Module

```python
def compute_market_features(ticker: str, as_of: date, ctx: FeatureContext) -> MarketFeatures:
    bars = ctx.daily_bars(ticker, end=as_of, lookback=300)
    sector = ctx.sector_etf(ticker)
    return MarketFeatures(
        ret_5d=log_return(bars.close, 5),
        ret_20d=log_return(bars.close, 20),
        rs_20_sector=percentile_rank(log_return(ticker, 20) - log_return(sector, 20)),
        rs_60_spy=percentile_rank(log_return(ticker, 60) - log_return("SPY", 60)),
        near_52w_high=bars.close[-1] / max(bars.high[-252:]),
        ma_regime=ma_regime_score(bars),
        rel_volume_5d=mean(bars.volume[-5:]) / median(bars.volume[-60:]),
        dollar_volume_z=zscore(dollar_volume(bars)[-1], dollar_volume(bars)[-60:]),
        atr_pct=atr(bars, 14) / bars.close[-1],
        extension_20d=(bars.close[-1] / sma(bars.close, 20)) - 1,
    )
```

### Feature Families and Formulas

| Family | Formula/approach | Output |
| --- | --- | --- |
| Relative strength | Percentile rank of returns vs SPY, sector ETF, and peer group. | 0-100 score |
| Breakout | Close vs prior base/high, moving average regime, volume confirmation. | setup flags |
| Volume | Relative volume 1d/5d, dollar-volume z-score, accumulation days. | 0-100 score |
| Volatility | ATR, realized-vol percentiles, compression/expansion ratio. | risk and setup fields |
| Options | Call/put ratio, call volume/OI percentile, IV percentile, skew shift. | 0-100 score |
| Liquidity | Avg dollar volume, spread proxy, slippage estimate. | pass/warn/block |
| Portfolio | Existing exposure, sector/theme concentration, correlation cluster. | pass/warn/block plus impact score |

### Feature Versioning

- Feature functions must include a `feature_version` string.
- Backtests should pin `feature_version` and `model_version`.
- Breaking feature logic changes require migration or parallel computation.
- Golden test fixtures must cover at least one strong candidate, weak candidate, blocked candidate, and stale-data candidate.

## 6. Event and Text Pipeline

### Event Detection

```python
def classify_event(event: RawEvent) -> EventClassification:
    source_quality = source_quality_score(event.source)
    event_type = rule_based_taxonomy(event.title, event.body, event.form_type)
    materiality = materiality_rules(event_type, source_quality, event.ticker)
    return EventClassification(
        event_type=event_type,
        source_quality=source_quality,
        materiality=materiality,
        requires_text_triage=materiality >= 0.5,
    )
```

### Local NLP Pipeline

```text
Normalize and dedupe text
  -> Extract ticker/entity/source metadata
  -> Split into passages and sections
  -> Apply keyword ontology and finance sentiment classifier
  -> Create local embeddings
  -> Compute novelty vs prior ticker/theme memory
  -> Select top snippets for LLM only if escalation gates pass
```

### Ontology File Format

```yaml
themes:
  ai_infrastructure_storage:
    terms: [NAND, SSD, datacenter storage, inference storage, storage bottleneck]
    sectors: [Semiconductors, Technology Hardware]
    read_through: [memory, storage controllers, equipment]
  datacenter_power:
    terms: [power density, grid constraint, UPS, switchgear, cooling load]
    sectors: [Electrical Equipment, Industrials]
    read_through: [cooling, grid equipment, data center infrastructure]
```

### Snippet Selection

- Never send entire filings by default.
- Select passages by event type, ontology match, source quality, novelty, and section importance.
- Include previous-quarter or previous-filing baseline passages for comparison when available.
- Tag each snippet with `source_id`, `source_ts`, `available_at`, section name, extraction method, and hash.
- Penalize promotional or social-media-only claims unless independently confirmed by higher-quality sources.

## 7. Scoring and Policy Engine

### Candidate Score

```python
final_candidate_score = (
    0.22 * price_strength
  + 0.12 * volume
  + 0.12 * options_flow
  + 0.18 * local_narrative
  + 0.16 * fundamental_event
  + 0.12 * sector_rotation
  + 0.08 * novelty
  - risk_penalty
  - portfolio_penalty
)
```

The first implementation should treat these weights as configurable defaults, not tuned truth. Tuning requires out-of-sample validation and comparison against simple baselines.

### Policy Evaluation

```python
def evaluate_policy(candidate: Candidate) -> PolicyResult:
    blocks = []

    if candidate.data_stale:
        blocks.append("core_data_stale")
    if candidate.liquidity.avg_dollar_volume < 10_000_000:
        blocks.append("liquidity_hard_block")
    if candidate.spread_pct > 1.0:
        blocks.append("spread_hard_block")
    if candidate.risk_penalty >= 20:
        blocks.append("risk_penalty_hard_block")
    if candidate.event_risk.within_no_new_position_window:
        blocks.append("event_risk_hard_block")
    if candidate.portfolio_impact.max_single_name_after > candidate.policy.max_single_name_pct:
        blocks.append("single_name_exposure_hard_block")
    if candidate.portfolio_impact.sector_after > candidate.policy.max_sector_pct:
        blocks.append("sector_exposure_hard_block")

    missing_trade_plan = []
    if not candidate.entry_zone:
        missing_trade_plan.append("missing_entry_zone")
    if not candidate.invalidation_price:
        missing_trade_plan.append("missing_invalidation")
    if candidate.reward_risk < 2.0:
        missing_trade_plan.append("reward_risk_too_low")

    if blocks:
        return PolicyResult(state="Blocked", hard_blocks=blocks)

    if candidate.final_score >= 85 and candidate.strong_pillars >= 3 and not missing_trade_plan:
        return PolicyResult(state="EligibleForManualBuyReview")
    if candidate.final_score >= 72:
        return PolicyResult(state="Warning", missing_trade_plan=missing_trade_plan)
    if candidate.final_score >= 60:
        return PolicyResult(state="AddToWatchlist", missing_trade_plan=missing_trade_plan)
    if candidate.final_score >= 50:
        return PolicyResult(state="ResearchOnly", missing_trade_plan=missing_trade_plan)
    return PolicyResult(state="NoAction")
```

### Default Portfolio Rules

| Rule | Default | Effect |
| --- | --- | --- |
| Per-trade risk | 0.50% of portfolio value | Position sizing starts from max loss if wrong. |
| Max single-name exposure | 8% of portfolio value | Blocks oversized position. |
| Max sector exposure | 30% of portfolio value | Blocks excessive sector concentration. |
| Max theme exposure | 35% of portfolio value | Warns or blocks crowded thematic basket. |
| Reward/risk minimum | 2.0 | Blocks buy-review state if unmet. |
| Liquidity floor | $10M avg dollar volume | Blocks illiquid names by default. |

Defaults should be configurable because user risk tolerance and account size matter.

### Setup-Specific Policy Plugins

| Plugin | Required outputs |
| --- | --- |
| BreakoutPolicy | breakout_level, entry_zone, invalidation, chase_block, reward/risk. |
| PullbackPolicy | pullback_zone, thesis_intact flag, invalidation, bounce confirmation. |
| PostEarningsPolicy | gap level, consolidation zone, gap-fill invalidation, revision follow-up. |
| SectorRotationPolicy | leader confirmation, laggard RS acceleration, sector invalidation. |
| FilingsCatalystPolicy | official catalyst, market confirmation, contradiction checks. |

## 8. LLM Router and Agent Implementation

### Router Contract

```python
class LLMRouter:
    def route(self, task: LLMTask, candidate: CandidatePacket) -> LLMDecision:
        cost = self.estimate_cost(task, candidate)
        allowed, reason = self.budget_controller.allow_llm_call(
            ticker=candidate.ticker,
            task=task.name,
            estimated_cost=cost,
            candidate_state=candidate.state,
        )
        if not allowed:
            return LLMDecision(skip=True, reason=reason)
        model = self.select_model(task, candidate)
        return LLMDecision(skip=False, model=model, max_tokens=task.max_output_tokens)
```

### Prompt and Version Discipline

- Prompts live in versioned files.
- Every prompt output uses JSON schema validation.
- Invalid JSON or failed schema validation triggers retry once, then downgrade.
- No agent may create unsupported facts; all claims require `source_id` or `computed_feature_id`.
- Pricing and model selection are config-driven.
- Prompt inputs must include only selected snippets, computed features, candidate state, and policy context needed for the task.

### Agent Schemas

```python
EvidencePacket = {
  "ticker": str,
  "as_of": datetime,
  "claims": [
    {
      "claim": str,
      "source_id": str,
      "source_quality": float,
      "evidence_type": str,
      "sentiment": float,
      "confidence": float,
      "uncertainty_notes": str,
    }
  ],
  "bear_case": [str],
  "unresolved_conflicts": [str],
  "recommended_policy_downgrade": bool,
}
```

### LLM Failure Behavior

- If LLM service fails, deterministic scanning continues.
- If schema validation fails twice, downgrade to Research Only or Warning without Decision Card.
- If unsupported claims are detected, reject the output and record an eval failure.
- If budget is exceeded, queue or skip LLM review and surface deterministic state.

## 9. Decision Card Implementation

### Decision Card Fields

| Field group | Required fields |
| --- | --- |
| Identity | ticker, company, version, as_of, action_state, setup_type. |
| Scores | final score, pillar scores, risk penalty, portfolio penalty, score delta. |
| Trade plan | entry zone, invalidation price, max loss if wrong, position size, reward/risk. |
| Portfolio impact | single-name, sector, theme, correlated-basket exposure before/after. |
| Evidence | top supporting evidence, disconfirming evidence, source quality, conflicts. |
| Controls | hard blocks, upcoming events, next review time, user decision. |

### Position Sizing

```python
risk_budget_dollars = portfolio_value * risk_per_trade_pct
stop_distance = abs(entry_price - invalidation_price)
shares = floor(risk_budget_dollars / stop_distance)
position_value = shares * entry_price

if position_value > portfolio_value * max_single_name_pct:
    shares = floor((portfolio_value * max_single_name_pct) / entry_price)
```

### Decision Card JSON Skeleton

```json
{
  "ticker": "SNDK",
  "version": "1.1.1",
  "action_state": "EligibleForManualBuyReview",
  "setup_type": "SectorRotationLaggard",
  "scores": {
    "final": 87.2,
    "price_strength": 91,
    "risk_penalty": 8,
    "portfolio_penalty": 0
  },
  "entry_zone": {"low": 100.00, "high": 103.00},
  "invalidation_price": 94.50,
  "reward_risk": 2.7,
  "position_sizing": {"risk_per_trade_pct": 0.5, "shares": 0},
  "portfolio_impact": {
    "single_name_after_pct": 0,
    "sector_after_pct": 0,
    "theme_after_pct": 0
  },
  "evidence": [],
  "disconfirming_evidence": [],
  "hard_blocks": [],
  "next_review_at": "2026-05-10T13:30:00Z"
}
```

## 10. Dashboard and Alerts

### Dashboard Views

| View | Contents |
| --- | --- |
| Radar Home | Top candidates, states, score deltas, hard blocks, review queue. |
| Ticker Detail | Chart, pillar history, evidence timeline, Decision Cards, invalidation. |
| Theme View | Theme velocity, leaders, laggards, read-through candidates. |
| Cost View | LLM/tool spend, call caps, cost per useful alert. |
| Validation View | Backtest, paper-trade, precision, false-positive review. |
| Ops View | Provider health, job status, stale data, schema failures. |

### Initial API Contracts

| Endpoint | Purpose |
| --- | --- |
| `GET /api/radar/candidates` | Current candidates with state, score, delta, and block summary. |
| `GET /api/radar/candidates/{ticker}` | Ticker detail with features, events, snippets, state history. |
| `GET /api/radar/decision-cards/{id}` | Full Decision Card payload. |
| `POST /api/radar/decision-cards/{id}/feedback` | User decision and useful-alert label. |
| `GET /api/ops/health` | Provider and job health. |
| `GET /api/costs/summary` | Spend by task, model, ticker, and period. |

### Alert Routing

```python
if state == "EligibleForManualBuyReview":
    send_immediate_alert(decision_card)
elif state == "Warning" and score_delta_5d >= 10:
    send_digest_plus_optional_push(candidate)
elif state in ["ResearchOnly", "AddToWatchlist"]:
    include_in_daily_digest(candidate)
elif state in ["ThesisWeakening", "ExitInvalidateReview"]:
    send_position_watch_alert(candidate)
```

### Alert Dedupe

- Do not alert again unless state changes, score delta exceeds threshold, a new high-quality event arrives, or invalidation triggers.
- Suppress repeated article duplicates with canonical URL/hash clustering.
- Weekly summary includes suppressed alerts count for audit.
- Every alert should include a one-click feedback path: useful, noisy, too late, too early, ignored, acted.

## 11. Backtesting and Paper Trading

### Point-in-Time Backtest Requirements

- Use only features/events with `available_at <= decision timestamp`.
- Replay candidate states and policy blocks, not just scores.
- Apply universe filters point-in-time where possible.
- Separate training/tuning periods from out-of-sample evaluation.
- Compare against simple baselines before tuning complex scores.

### Baselines

| Baseline | Purpose |
| --- | --- |
| SPY-relative momentum top decile | Proves the radar beats simple momentum sorting. |
| Sector-relative momentum top decile | Tests whether sector normalization adds value. |
| Earnings/news event-only watchlist | Tests event pipeline value. |
| Random eligible universe sample | Sanity check for false discovery. |
| User manual watchlist if available | Measures whether tool improves existing workflow. |

### Backtest Labels

| Label | Definition |
| --- | --- |
| `target_10d_15` | Forward 10-trading-day return >= 15%. |
| `target_20d_25` | Forward 20-trading-day return >= 25%. |
| `target_60d_40` | Forward 60-trading-day return >= 40%. |
| `sector_outperformance` | Forward return beats sector ETF by >= 20%. |
| `useful_alert` | User-defined: materially improved attention, timing, or risk control. |

### Paper Trading Workflow

```python
for decision_card in eligible_cards:
    simulated_decision = policy_or_user_decision(decision_card)
    record_entry_if_approved(simulated_decision)
    monitor_invalidation_daily()
    record_outcome_after_10_20_60d()
    update_precision_and_cost_metrics()
```

### Release Gates Before Real Capital

- No known point-in-time leakage.
- Shadow mode has run through at least one earnings/event cycle.
- Paper trades record entry, invalidation, max adverse excursion, and outcome.
- Useful-alert rate is tracked and reviewed weekly.
- False positives and missed opportunities are reviewed.
- Cost per useful alert is within budget.

## 12. Testing and Evaluation

### Test Pyramid

| Test type | Examples |
| --- | --- |
| Unit tests | Feature formulas, policy gates, sizing formula, ontology matching. |
| Integration tests | Connector -> normalized DB -> feature -> score pipeline. |
| Golden tests | Known ticker/date inputs produce stable scores and states. |
| Schema tests | Decision Card and EvidencePacket JSON validation. |
| Data quality tests | Stale data, missing bars, corporate-action mismatch. |
| LLM evals | Source faithfulness, unsupported claims, sentiment direction, bear-case quality. |
| Backtest tests | No leakage, availability timestamps enforced. |

### LLM Evaluation Checklist

- Does every claim have a source ID?
- Are unsupported or hallucinated claims rejected?
- Does the Skeptic Agent find the major disconfirming evidence?
- Does the Decision Card avoid buy/guarantee language?
- Does the agent correctly downgrade when evidence conflicts?
- Is the generated summary consistent with deterministic score and policy state?

## 13. Cost and Budget Controls

### Budget Ledger Schema Excerpt

```sql
CREATE TABLE budget_ledger (
  id UUID PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL,
  ticker TEXT,
  task TEXT NOT NULL,
  model TEXT,
  input_tokens BIGINT,
  cached_input_tokens BIGINT,
  output_tokens BIGINT,
  tool_calls JSONB,
  estimated_cost NUMERIC,
  actual_cost NUMERIC,
  candidate_state TEXT,
  prompt_version TEXT,
  outcome_label TEXT
);
```

### Daily Caps

| Task | Default daily cap |
| --- | --- |
| `mini_extraction` | 200 |
| `mid_review` | 50 |
| `skeptic_review` | 20 |
| `gpt55_decision_card` | 8 |
| `full_transcript_deep_dive` | Manual only |

### Cost Degrade Rules

- If monthly budget is >80% used, GPT-5.5 Decision Cards are allowed only for `final_score >= 90`.
- If monthly budget is exceeded, all new LLM tasks downgrade to local summary only.
- If a ticker already has a fresh evidence packet and no material new event, reuse cached output.
- If source snippets exceed prompt budget, use extractive ranking and truncate by source quality and novelty.
- If model pricing config is missing or stale, require explicit operator confirmation before premium calls.

## 14. Deployment and Operations

### Job Schedule

| Job | Schedule | SLA |
| --- | --- | --- |
| `daily_bar_ingest` | After U.S. close | Complete before nightly scan. |
| `feature_scan` | After ingest | Full universe in under 30 minutes MVP target. |
| `event_ingest` | Hourly or provider webhook | Material events available within minutes where possible. |
| `local_text_triage` | After event ingest | Candidates updated same cycle. |
| `scoring_policy` | After features/events | State transition logged. |
| `llm_review` | Budgeted async queue | Only escalated candidates. |
| `digest` | Before U.S. open / after close | User timezone aware. |
| `validation_update` | Daily/weekly | Outcome metrics updated. |

### Runbook Snippets

If market data provider fails:

```text
mark provider_health = degraded
disable action states above AddToWatchlist
show banner in dashboard
retry with backoff or backup provider
```

If LLM service fails:

```text
continue deterministic scanner
queue eligible candidates for later review
do not block dashboard scoring
```

If score distribution shifts abnormally:

```text
freeze new buy-review states
run drift diagnostics
require manual override to resume
```

## 15. Security, Secrets, and Compliance

| Control | Implementation |
| --- | --- |
| Secrets | Use Vault, AWS Secrets Manager, or environment-specific encrypted secrets. |
| API keys | Least privilege, rotate keys, never log full tokens. |
| Data licenses | Tag source/license and enforce retention rules. |
| Prompt data | Send only required snippets; avoid account data in prompts. |
| Audit logs | Record user decisions, overrides, hard-block bypasses, and model calls. |
| Access control | Admin, analyst, viewer roles for dashboard. |
| Backups | Daily DB snapshots, object storage lifecycle policies. |
| Disaster recovery | Restore test monthly for production pilot. |

### Local Configuration Convention

- Use `.env.local` for local secrets and `.env.example` for documented variables.
- Never commit real API keys, broker tokens, or paid provider credentials.
- Keep policy thresholds in versioned YAML or TOML config.
- Keep user holdings import optional and local-first until an explicit broker integration phase.

## 16. Implementation Roadmap

### Phases

| Phase | Build outputs | Exit criteria |
| --- | --- | --- |
| Phase 1 - Deterministic MVP | Universe, bars, feature scanner, dashboard, action states. | Full nightly scan works; no GPT-5.5 needed. |
| Phase 2 - Event/local NLP | SEC/news connectors, ontology, embeddings, novelty, source quality. | Candidate state improves from text without high cost. |
| Phase 3 - Sparse LLM | Router, budgets, evidence packets, Decision Cards. | LLM calls capped; schema validation passes. |
| Phase 4 - Validation | Backtest, shadow mode, paper trading, eval dashboards. | Beats simple baselines and passes release gates. |
| Phase 5 - Pilot | Limited real-capital workflow if user chooses. | Kill switch, audit logs, monthly review. |

### First Four Implementation Sprints

| Sprint | Goal | Deliverables |
| --- | --- | --- |
| 1 | Data and universe foundation | securities, daily_bars, connector health, basic dashboard. |
| 2 | Feature engine | RS, volume, liquidity, volatility, sector maps, score history. |
| 3 | Policy and portfolio engine | action states, hard blocks, sizing, invalidation requirements, portfolio impact. |
| 4 | Event/text MVP | SEC/news ingest, ontology, local embeddings, candidate packet generation. |

### Recommended First Build Slice

Build the deterministic MVP first:

1. Securities master.
2. Daily bar ingest.
3. Feature computation.
4. Candidate scoring.
5. Policy/action states.
6. Minimal dashboard.
7. Golden tests and point-in-time backtest skeleton.

No LLM integration should be required to prove this slice.

## 17. Appendices

### Operational Definitions

| Term | Definition |
| --- | --- |
| Candidate packet | All features, events, snippets, risk blocks, and state data needed for review. |
| Evidence packet | Structured claims with source IDs and uncertainty notes. |
| Decision Card | Final human-facing card that contains action state, trade plan fields, evidence, risks, and review cadence. |
| Hard block | Policy rule that prevents buy-review state. |
| Useful alert | Alert that improved attention, timing, or risk control, even if no trade was taken. |
| Availability timestamp | Time the system could first use a source record or computed feature. |
| Portfolio impact | Exposure and concentration effect if the candidate is approved at the suggested size. |

### Engineering Acceptance Criteria

- Nightly scan completes under configured SLA.
- Premium model calls remain within daily and monthly budget caps.
- Every feature and event has availability timestamp.
- Every Decision Card passes JSON schema and required-field validation.
- Blocked candidates explain which rule blocked them.
- Backtest and paper-trade modules can compute cost per useful alert.
- System can run in deterministic-only degraded mode.
- Dashboard can show current state, state history, escalation reason, block reason, and evidence links.
- User feedback can be recorded for every alert and Decision Card.
- Portfolio exposure gates are enforced before Eligible for Manual Buy Review.


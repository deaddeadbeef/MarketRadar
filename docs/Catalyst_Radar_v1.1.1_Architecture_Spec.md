# Catalyst Radar v1.1.1 - Architecture Specification

Cost-optimized sparse-LLM market radar architecture for public-equity opportunity detection and auditable decision support.

Prepared for: Captain  
Date: May 9, 2026  
Version: v1.1.1  
Document type: Architecture Spec  
Status: Sharpened implementation baseline, preserving v1.1.0 detail and correcting governance, validation, and action-state gaps

## Core Architecture Thesis

Catalyst Radar should be built as a deterministic-first market intelligence system with sparse LLM escalation. The core platform uses code, statistics, local NLP, feature stores, rules, and calibrated models for breadth and repeatability. Frontier LLMs are reserved for high-value synthesis, contradiction review, and final Decision Cards only after gating logic proves the candidate deserves the spend.

This system is decision support. It is not an autonomous trader, a registered investment adviser, or a guarantee engine. The highest automated action state is **Eligible for Manual Buy Review**.

## 1. Executive Summary - What, Why, How

### What

Catalyst Radar is a cost-optimized market radar for detecting public-equity candidates that may be entering an asymmetric re-rating phase. It monitors liquid U.S.-listed equities and turns raw signals into auditable action states:

- No Action
- Research Only
- Add to Watchlist
- Warning
- Eligible for Manual Buy Review
- Blocked
- Thesis Weakening
- Exit/Invalidate Review

### Why

Important market moves often have weak early signals spread across price action, relative strength, volume, options, filings, news, earnings language, and sector read-throughs. The value is not a single prediction. The value is faster filtering, better prioritization, stronger evidence discipline, explicit invalidation, and better post-decision learning.

The v1.1.x split keeps strategic architecture separate from concrete engineering implementation. This v1.1.1 pass sharpens the architecture around:

- action-state consistency
- portfolio and risk boundaries
- current model-risk framing
- measurable usefulness
- MVP decision points

### How

The platform uses a cascade: deterministic market scanner first, event detection second, local NLP third, scoring and policy gates fourth, cheap LLM review fifth, and GPT-5.5 Decision Cards only for candidates that may become action-relevant. Most tickers exit before any premium LLM call.

| Layer | Primary method | Purpose |
| --- | --- | --- |
| Market breadth | Python, polars, vectorized statistics | Scan thousands of tickers cheaply. |
| Event intelligence | Rules, taxonomy, source-quality scoring | Detect filings, news, earnings, and analyst changes. |
| Text triage | Local embeddings, finance sentiment classifiers, ontology matching | Classify sentiment, theme, novelty, and evidence type cheaply. |
| Decision policy | Rules engine plus calibrated score | Convert signals into allowed actions and hard blocks. |
| LLM synthesis | Sparse model escalation | Explain only top candidates and produce Decision Cards. |

## 2. Architecture Principles

| Principle | Implication |
| --- | --- |
| Cost is an architecture constraint | Every stage has call budgets and escalation gates. |
| Do not use LLMs for math | Relative strength, z-scores, options abnormality, risk limits, and portfolio exposure are deterministic. |
| LLMs extract and synthesize; they do not own the score | The scoring engine is deterministic or calibrated ML. LLMs explain and challenge evidence. |
| No action without invalidation | Any actionable state must define entry, invalidation, max loss, and next review. |
| Source-linked evidence only | Narrative claims must trace to source IDs and timestamps. |
| Fail closed | Missing data, stale feeds, unresolved conflicts, or hard blocks downgrade action states. |
| Human-in-the-loop | The highest automated state is Eligible for Manual Buy Review, not Buy. |
| Point-in-time or it does not count | Backtests and replay use availability timestamps, not only source timestamps. |
| Measure usefulness, not just accuracy | Alerts must be evaluated by whether they improved attention, timing, risk control, or avoided bad trades. |
| Separate signal from permission | A high score can create interest; only policy gates can permit manual buy review. |

## 3. System Context and Boundaries

External data providers:

- market bars
- options aggregates
- SEC filings
- news
- earnings calendar
- transcripts
- analyst and revision feeds

Flow:

```text
External data providers
  -> Catalyst Radar ingestion and normalization boundary
  -> feature store, text store, event store, scoring service, policy engine
  -> sparse LLM review and Decision Card generation
  -> dashboard, alerts, feedback, validation, paper-trading logs
```

### Primary Users

- Single operator or small research desk focused on medium-term thematic and momentum opportunities.
- User needs prioritized candidates, not every raw signal.
- System provides decision support, not fully automated trading.

### In-scope Instruments

- U.S.-listed common stocks and ADRs.
- Initially filtered by liquidity, price, market cap, options availability where relevant, and data coverage.

### Out of Scope for v1.1.1

- Autonomous order execution.
- Options trade recommendations as first-class output.
- Material nonpublic information or prohibited scraping.
- Low-liquidity microcaps, binary biotech setups, and data-poor instruments unless explicitly enabled later.
- Fully automated financial advice or guaranteed-return language.

## 4. Reference Architecture

```text
S0 Universe Builder
  -> S1 Deterministic Market Scanner
  -> S2 Event Detector
  -> S3 Local NLP and Evidence Triage
  -> S4 Scoring Engine and Policy Gates
  -> S5 Sparse LLM Evidence Review
  -> S6 GPT-5.5 Decision Card
  -> Alert Delivery, Dashboard, Feedback, and Validation
```

### Core Services

| Service | Responsibility | Cost profile |
| --- | --- | --- |
| Universe Service | Maintain eligible securities and exclusions. | Low |
| Market Feature Service | Compute price, volume, RS, liquidity, volatility, options signals. | Low |
| Event Service | Normalize filings, news, earnings, revisions into events. | Low-medium |
| Text Intelligence Service | Embeddings, source quality, local sentiment, theme, novelty. | Low |
| Scoring Service | Blend pillars and compute candidate score. | Low |
| Policy Service | Apply hard blocks, action-state transitions, risk controls. | Low |
| Portfolio Risk Service | Compute exposure, concentration, correlation, current holdings impact, and max-loss budget. | Low |
| LLM Review Service | Structured extraction, skeptic review, Decision Cards. | Controlled |
| Alert Service | Deduped notifications, digests, dashboard state. | Low |
| Validation Service | Backtests, paper trading, evals, score calibration. | Medium |

## 5. Sparse-LLM Cascade

The cascade is the central cost-control mechanism. Each stage reduces the candidate pool before higher-cost processing.

| Stage | Typical input | Typical output | LLM use |
| --- | --- | --- | --- |
| S0 Universe | 1,000-2,000 tickers | Eligible universe | No |
| S1 Market scan | Full universe | Top 10-15 percent | No |
| S2 Event triage | Market-interesting names plus event stream | 30-100 candidates | No |
| S3 Local NLP | Relevant snippets | Text scores and snippets | No or local only |
| S4 Scoring and policy | All feature scores | Watch, Warning, Blocked | No |
| S5 LLM review | Warning-level candidates | Evidence packet and bear case | Small or mid model only |
| S6 Decision Card | Buy-review candidates | Human-facing Decision Card | GPT-5.5 only |

### Escalation Rules

Escalate to local NLP if:

- `market_score >= 60`, or
- `high_quality_event == true`

Escalate to small or mid LLM if:

- `combined_score >= 72`, or
- `score_delta_5d >= 10`, or
- `source_quality_event == high`

Escalate to GPT-5.5 Decision Card if:

- `final_score >= 85`
- `price_strength >= 70`
- at least 3 pillars are `>= 70`
- `risk_penalty < 12`
- no hard block is active
- entry zone and invalidation can be defined
- portfolio impact is within configured limits
- evidence packet contains both supporting and disconfirming evidence

## 6. Signal and Decision Architecture

### Signal Pillars

| Pillar | Example features | Owner |
| --- | --- | --- |
| Price strength | 20d sector-relative RS, 60d SPY-relative RS, 52-week high proximity, moving-average regime. | Market Feature Service |
| Volume | Relative volume, dollar-volume z-score, accumulation ratio. | Market Feature Service |
| Options flow | Call/put ratios, OI shifts, IV percentile, skew changes. | Options Feature Service |
| Local narrative | Source quality, theme match, sentiment, novelty, peer confirmation. | Text Intelligence Service |
| Fundamental/event | Guidance, filings flags, earnings tone, revision clusters. | Event and Text Services |
| Sector rotation | Theme leader strength, ETF trend, laggard catch-up, peer read-through. | Sector Service |
| Risk penalty | Liquidity, price extension, stale data, event risk, concentration, evidence weakness. | Policy Service |
| Portfolio impact | Existing exposure, correlated positions, sector/theme concentration, max loss if wrong. | Portfolio Risk Service |

### Action-State Architecture

| State | Meaning | Can user act? |
| --- | --- | --- |
| No Action | Signal below threshold or irrelevant. | No |
| Research Only | Interesting but insufficient evidence. | Read only |
| Add to Watchlist | Worth monitoring; needs more evidence or better entry. | No trade review |
| Warning | Multiple pillars improving. | Run skeptic/research |
| Eligible for Manual Buy Review | All gates pass and Decision Card complete. | Manual review only |
| Blocked | A hard block prevents action. | No unless explicit override |
| Thesis Weakening | Score/evidence deteriorating. | Review held positions |
| Exit/Invalidate Review | Price, thesis, or risk invalidation triggered. | Review exit or de-risk |

### Action-State Invariants

- No state above Add to Watchlist is allowed when core market data is stale.
- Eligible for Manual Buy Review requires entry, invalidation, sizing, reward/risk, portfolio impact, evidence, disconfirming evidence, and next review time.
- A candidate may have a high score and still be Blocked.
- The UI must show both escalation reasons and block reasons.

## 7. Data Architecture

### Logical Data Zones

| Zone | Contents | Retention |
| --- | --- | --- |
| Raw data lake | Provider payloads, filings, transcripts, news, bars, options. | Long-term, immutable where licensed. |
| Normalized warehouse | Canonical tickers, OHLCV, event records, adjusted symbols. | Long-term |
| Feature store | Point-in-time features and candidate scores. | Long-term for backtests |
| Text store | Snippets, embeddings, source metadata, dedupe hashes. | Configurable by license |
| Decision store | Decision Cards, action states, hard blocks, user feedback. | Long-term audit |
| Validation store | Backtests, paper trades, outcomes, eval results. | Long-term |

### Point-in-Time Rule

Every feature, event, and text claim must store both:

- `source_ts`: the timestamp on the original source or provider payload
- `available_at`: the timestamp the system could first use the data

Backtests must use `available_at` to avoid future leakage.

### Data Quality Gates

- Core bars stale -> disable all action states above Add to Watchlist.
- Corporate action mismatch -> block scoring until adjusted.
- Unresolved source conflict -> downgrade to Research Only or Blocked.
- Provider outage -> enter degraded mode and display coverage status.
- Missing availability timestamp -> exclude record from backtest and action-state promotion.

## 8. Agent and Model Architecture

Agents are organized by responsibility, not by model size. Many agents should be deterministic services first and LLM prompts only when needed.

| Agent/service | Non-LLM implementation | LLM role |
| --- | --- | --- |
| Market Structure | Feature computation and anomaly rules. | Summarize why a pattern matters only for high-level alerts. |
| Event Detector | Form types, item numbers, keyword taxonomy, source quality. | Resolve ambiguous event meaning. |
| Narrative Triage | Embeddings, ontology, source scoring, novelty math. | Synthesize thesis from selected snippets. |
| Filings Analyst | Section diff, regex flags, XBRL deltas. | Read suspicious sections and classify materiality. |
| Skeptic | Risk rules and evidence sufficiency checks. | Produce human-readable bear case for Warning or BuyReview. |
| Decision Card Writer | Policy fields and computed sizing. | Write final evidence-backed card, no independent buy call. |

### Model Tiering

| Model tier | Used for | Do not use for |
| --- | --- | --- |
| No LLM | Market math, risk rules, backtests, portfolio limits. | Narrative synthesis. |
| Local models | Sentiment, embeddings, taxonomy classification. | Final Decision Cards. |
| Small model | JSON extraction from selected snippets. | Complex contradictory evidence. |
| Mid model | Evidence review and Skeptic Agent. | Full-market screening. |
| GPT-5.5 | Final Decision Cards and rare complex thesis review. | Routine scans or repetitive summaries. |

### Model-Risk Posture

The system should follow the practical spirit of SR 11-7 model-risk management even though this is a personal or small-desk tool, not a bank model-governance program:

- document model purpose and limitations
- validate outputs against baselines
- monitor drift
- record issues and overrides
- separate signal generation from final human decision

## 9. Cost Architecture

As of the pricing sources checked on May 9, 2026, OpenAI lists GPT-5.5 standard text pricing at:

- $5.00 per 1M input tokens
- $0.50 per 1M cached input tokens
- $30.00 per 1M output tokens

Built-in tools and tokens used with those tools are billed according to OpenAI pricing rules, so implementation should treat model and tool cost as runtime budget constraints rather than fixed constants.

| Mode | Target user | Monthly AI spend target | Total spend target incl. data/hosting |
| --- | --- | --- | --- |
| Barebones MVP | Build and paper test | $50-$250 | $250-$800 |
| Lean Solo | Daily research assistant | $150-$600 | $400-$1,500 |
| Focused Pro | High-quality solo workflow | $500-$1,500 | $1,000-$3,500 |
| Research Desk | Small team / multi-watchlist | $2,000-$6,000+ | $5,000-$15,000+ |

### Budget-Control Architecture

- BudgetController approves or rejects LLM calls by task, candidate state, estimated cost, daily cap, and monthly cap.
- CostTracker records actual tokens, cached tokens, model, tool calls, ticker, state, prompt version, downstream usefulness, and user decision.
- If budget exceeds configured thresholds, the system downgrades outputs to Research Only or routes to batch/deferred processing.
- Model pricing must be config-driven and updateable without code changes.

## 10. Security, Compliance, and Model Risk

| Risk area | Architecture control |
| --- | --- |
| Investment-risk framing | Outputs are decision support, not guaranteed return claims. |
| Data licensing | Connector-specific retention and redistribution rules. |
| Secrets | Encrypted secret store, key rotation, least privilege. |
| Prompt/data leakage | No sensitive account data in prompts unless required; redact user notes before model calls. |
| Model drift | Calibration dashboards, regime monitoring, paper-only fallback. |
| AI hallucination | Source-linked claims, JSON schemas, no unsupported claims. |
| Operational failure | Fail closed on stale data or missing required fields. |
| Fraud/misinformation | Prefer primary sources, penalize promotional claims, flag unverifiable AI/social-media claims. |

### Human Approval Boundary

The system may produce Eligible for Manual Buy Review but must not automatically place trades in v1.1.1. If broker integration is later added, it should be read-only until a separate major version defines order routing, explicit approvals, broker reconciliation, account permissions, and kill switches.

## 11. Observability and Governance

### Required Telemetry

- Stage-level candidate counts: universe -> market scan -> event triage -> local NLP -> LLM review -> Decision Card.
- Cost per stage, cost per useful alert, cost per buy-review candidate.
- Feature freshness, provider health, stale-data incidents.
- LLM source-faithfulness evals and unsupported-claim rates.
- Alert precision, max adverse excursion, outcome by setup type.
- Action-state transition reasons.
- Block frequency by rule.
- User feedback: approved, rejected, deferred, useful, noisy, too late, too early.

### Governance Dashboards

| Dashboard | Purpose |
| --- | --- |
| Ops health | Data freshness, job success, provider status. |
| Signal quality | Pillar distributions, score drift, candidate counts. |
| Cost control | Monthly spend, LLM calls by task, cache hit rate. |
| Validation | Backtest/paper-trade results, false positives, baselines. |
| Decision audit | Cards generated, approvals/rejections, overrides, user feedback. |

## 12. Deployment Architecture

| Environment | Purpose | Notes |
| --- | --- | --- |
| Local/dev | Feature development and tests. | Synthetic/small data, no premium LLM by default. |
| Staging | Replay tests and integration validation. | Uses sandbox connectors where possible. |
| Shadow production | Live alerts, no real-capital action. | Required before pilot. |
| Paper trading | Simulated decisions and outcomes. | Required release gate. |
| Limited pilot | Small real-capital exposure if user chooses. | Kill switch active. |

### Suggested Technology Stack

| Layer | Recommended |
| --- | --- |
| Backend | Python + FastAPI |
| Batch orchestration | Prefect or Dagster |
| Feature compute | polars/pandas + vectorized Python |
| Database | Postgres + TimescaleDB extension |
| Vector store | pgvector for MVP |
| Object storage | S3-compatible bucket |
| Queue | Redis Queue/Celery for MVP; Redpanda/Kafka later |
| Dashboard | Next.js or Streamlit |
| Observability | OpenTelemetry + Prometheus/Grafana |
| LLM orchestration | OpenAI Agents SDK or lightweight custom router |

### MVP Decisions Needed Before Build

- Dashboard: Streamlit for speed or Next.js for long-term product polish.
- First data providers: free/cheap bootstrap versus paid quality feeds.
- Hosting: local Windows dev, Docker Compose, or cloud VM.
- Alert targets: dashboard only, email, Telegram, Slack, or webhook.
- Initial watch universe: full liquid U.S. universe or a smaller curated universe for faster validation.

## 13. Architecture Decisions and Tradeoffs

| Decision | Reason | Tradeoff |
| --- | --- | --- |
| Sparse LLM cascade | Cuts cost and preserves frontier reasoning for high-value tasks. | More engineering complexity. |
| Separate policy engine from scoring | Prevents high score from becoming ungoverned action. | Requires more explicit rules. |
| Local NLP before LLM | Cheap high-volume text triage. | May miss subtle nuance until escalated. |
| Decision Card as final artifact | Forces actionability and auditability. | More fields, stricter gates. |
| Fail closed on data issues | Prevents bad actions from stale data. | May miss opportunities during provider outages. |
| Point-in-time feature store | Supports serious validation. | More storage and timestamp complexity. |
| Useful-alert metric | Measures decision quality beyond hit rate. | Requires explicit user feedback. |
| Portfolio Risk Service | Prevents repeated correlated bets from looking independent. | Requires holdings/exposure data. |

## 14. Architecture Acceptance Criteria

- The full universe can be scanned nightly without premium LLM calls.
- At least 90 percent of tickers exit before any LLM call in normal operation.
- Every Warning or higher candidate has source-linked evidence and disconfirming evidence.
- Every buy-review candidate has entry zone, invalidation, sizing, reward/risk, and portfolio impact.
- System can show exactly why a candidate was escalated or blocked.
- Backtests can replay candidate state using availability timestamps.
- Monthly AI spend can be capped and enforced automatically.
- If required data is stale, action states above Add to Watchlist are disabled.
- Every useful-alert label can be traced to a user action or explicit review outcome.
- LLM-generated claims with no source ID are rejected or quarantined.

## 15. Source Appendix

| Source | Use |
| --- | --- |
| OpenAI API Pricing - https://openai.com/api/pricing/ | Model pricing and budget assumptions. |
| OpenAI GPT-5.5 model docs - https://developers.openai.com/api/docs/models/gpt-5.5 | Current model documentation reference. |
| OpenAI Agents SDK Docs - https://developers.openai.com/api/docs/guides/agents | Agent orchestration framing. |
| SEC/Investor.gov AI Investment Fraud Alert - https://www.investor.gov/introduction-investing/general-resources/news-alerts/alerts-bulletins/investor-alerts/artificial-intelligence-fraud | Investor-risk framing for AI claims. |
| FINRA AI Investment Fraud guidance - https://www.finra.org/investors/insights/artificial-intelligence-and-investment-fraud | Risk framing for guaranteed-return and AI-trading claims. |
| Federal Reserve Supervisory Guidance on Model Risk Management, SR 11-7 - https://www.federalreserve.gov/frrs/guidance/supervisory-guidance-on-model-risk-management.htm | Model-risk validation, governance, monitoring, and effective challenge framing. |
| Catalyst Radar v1.0.0 Cost-Optimized Decision System Spec | Baseline document consolidated into v1.1.x split architecture and engineering specs. |


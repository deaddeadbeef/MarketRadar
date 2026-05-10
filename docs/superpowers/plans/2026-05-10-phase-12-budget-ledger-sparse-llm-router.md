# Phase 12 Budget Ledger And Sparse LLM Router Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add controlled, auditable LLM review plumbing without allowing LLMs into deterministic scanning, scoring, policy gates, risk math, or portfolio decisions.

**Architecture:** Keep the current deterministic pipeline as the system of record. Add an `agents` package that can decide whether an LLM task is allowed, estimate and record cost, run only fake deterministic clients in this phase, and write every skip or call attempt to a budget ledger. Feed the existing cost API and Streamlit Costs page from the ledger so future paid model work has a visible audit trail before real spend is enabled.

**Tech Stack:** Python 3.11, SQLAlchemy Core, SQLite/PostgreSQL-compatible schema, dataclasses, stdlib JSON schema-style validation, FastAPI, Streamlit, pytest, ruff.

**Implementation status:** Planned from `main @ d8af7f6 docs: mark phase 11 complete`.

---

## Current Baseline

Verified after Phase 11:

```text
python -m pytest
368 passed in 130.96s (0:02:10)

python -m ruff check src tests apps
All checks passed!
```

Existing capabilities to build on:

- Full deterministic scan, candidate states, candidate packets, Decision Cards, validation, paper trading, API, dashboard, alerts, alert feedback, and cost view are on `main`.
- `AppConfig.enable_premium_llm` already defaults to `False`.
- Candidate packets contain source-linked supporting and disconfirming evidence, `schema_version`, `source_ts`, and `available_at`.
- Dashboard/API cost summary currently uses validation metrics and useful alert labels, but there is no actual model-cost ledger.
- There is no `src/catalyst_radar/agents` package, no LLM router, no budget controller, and no `budget_ledger` table.

## Spec Requirements For This Phase

- Full-universe scans must still run without LLM configuration or API keys.
- LLMs must not compute market math, risk rules, portfolio limits, backtests, base scores, or action-state permission.
- LLM tasks are sparse: only Warning-or-higher candidates, strong score deltas, high-quality events, or later manual-review candidates can be considered.
- Budget approval must account for task, candidate state, estimated cost, daily cap, monthly cap, per-task cap, model config, and pricing freshness.
- Every attempted call or skip must be auditable by ticker, task, model, state, prompt version, schema version, token counts, estimated cost, actual cost, status, outcome, and reason.
- Local/dev defaults must fail closed: premium LLM disabled unless explicitly configured.
- Phase 12 should define stable interfaces for Phase 13 evidence packets, skeptic review, and LLM-assisted Decision Cards, but it should not add paid model integration yet.

## Scope

Implement in this phase:

- `budget_ledger` schema, SQLAlchemy table, migration, repository, and summary queries.
- Config-driven LLM model names, pricing, daily/monthly budgets, per-task caps, and pricing staleness checks.
- Agent models and task definitions for `mini_extraction`, `mid_review`, `skeptic_review`, `gpt55_decision_card`, and `full_transcript_deep_dive`.
- `BudgetController` for deterministic allow/skip decisions.
- `LLMRouter` with a fake deterministic client and no real provider dependency.
- Prompt and schema foundation for `evidence_review_v1`.
- CLI commands for budget status and fake/dry-run LLM review.
- Cost API/dashboard summary backed by ledger rows.
- Focused unit/integration tests and a phase review document.

Out of scope:

- Real OpenAI client calls.
- Secret management beyond reading config from `.env.local` and environment variables.
- Real evidence packet generation, skeptic narratives, or LLM-written Decision Cards.
- Scheduler/worker automation.
- Broker, order execution, or autonomous investment decisions.

## File Structure

Create:

- `src/catalyst_radar/agents/__init__.py`
- `src/catalyst_radar/agents/models.py`
- `src/catalyst_radar/agents/tasks.py`
- `src/catalyst_radar/agents/budget.py`
- `src/catalyst_radar/agents/router.py`
- `src/catalyst_radar/agents/schemas.py`
- `src/catalyst_radar/agents/prompts/evidence_review_v1.md`
- `src/catalyst_radar/storage/budget_repositories.py`
- `sql/migrations/011_budget_llm.sql`
- `tests/unit/test_budget_controller.py`
- `tests/unit/test_llm_router.py`
- `tests/unit/test_agent_schemas.py`
- `tests/integration/test_budget_repository.py`
- `tests/integration/test_llm_cli.py`
- `docs/phase-12-review.md`

Modify:

- `src/catalyst_radar/core/config.py`
- `src/catalyst_radar/storage/schema.py`
- `src/catalyst_radar/cli.py`
- `src/catalyst_radar/dashboard/data.py`
- `apps/dashboard/pages/4_Costs.py`
- `src/catalyst_radar/api/routes/costs.py`
- `tests/unit/test_config.py`
- `tests/integration/test_dashboard_data.py`
- `tests/integration/test_api_routes.py`
- `docs/superpowers/plans/2026-05-09-full-product-implementation.md`

## Data Contract

`budget_ledger` row:

```text
id
ts
available_at
ticker
candidate_state_id
candidate_packet_id
decision_card_id
task
model
provider
status
skip_reason
input_tokens
cached_input_tokens
output_tokens
tool_calls
estimated_cost
actual_cost
currency
candidate_state
prompt_version
schema_version
outcome_label
payload
created_at
```

Status values:

```text
planned
skipped
dry_run
completed
failed
schema_rejected
```

Skip reasons:

```text
premium_llm_disabled
candidate_state_not_eligible
task_daily_cap_exceeded
daily_budget_exceeded
monthly_budget_exceeded
monthly_soft_cap_requires_high_score
model_not_configured
pricing_missing
pricing_stale
manual_task_requires_operator
candidate_packet_missing
schema_validation_failed
client_error
```

## Subagent Execution Plan

Use fresh workers with disjoint write scopes:

- Worker A owns schema, migration, `agents/models.py`, and `storage/budget_repositories.py`.
- Worker B owns config, `agents/tasks.py`, `agents/budget.py`, and budget-controller tests.
- Worker C owns `agents/router.py`, `agents/schemas.py`, prompt file, fake client, router tests, and CLI review commands.
- Worker D owns cost summary/API/dashboard updates and related tests.

After each worker returns, run a review pass for spec compliance and quality before merging the worker's changes into the phase branch. Do not let any worker edit files outside its assigned scope unless the parent agent explicitly reassigns ownership.

## Task 0: Create Phase Branch And Baseline

**Files:**
- No source files.

- [ ] **Step 1: Confirm clean main**

Run:

```powershell
git status --short --branch
git rev-parse --short HEAD
```

Expected:

```text
## main
d8af7f6
```

- [ ] **Step 2: Create implementation branch**

Run:

```powershell
git switch -c feature/phase-12-budget-ledger-sparse-llm-router
```

Expected:

```text
Switched to a new branch 'feature/phase-12-budget-ledger-sparse-llm-router'
```

- [ ] **Step 3: Run focused baseline checks**

Run:

```powershell
python -m pytest tests/unit/test_config.py tests/integration/test_dashboard_data.py tests/integration/test_api_routes.py
python -m ruff check src tests apps
```

Expected:

```text
All selected tests pass
All checks passed!
```

## Task 1: Add Budget Ledger Schema And Repository

**Files:**
- Create: `src/catalyst_radar/agents/__init__.py`
- Create: `src/catalyst_radar/agents/models.py`
- Create: `src/catalyst_radar/storage/budget_repositories.py`
- Create: `sql/migrations/011_budget_llm.sql`
- Modify: `src/catalyst_radar/storage/schema.py`
- Test: `tests/integration/test_budget_repository.py`

- [ ] **Step 1: Write the failing repository tests**

Add `tests/integration/test_budget_repository.py` with tests for insert/list, cutoff filtering, and summary by task/model/status.

Required assertions:

```python
assert repo.list_entries(available_at=VISIBLE_AT) == [visible_entry]
assert repo.list_entries(available_at=VISIBLE_AT, ticker="aapl") == []
assert summary["total_actual_cost_usd"] == 0.19
assert summary["total_estimated_cost_usd"] == 0.22
assert summary["status_counts"] == {"completed": 1, "skipped": 1}
assert summary["by_task"][0]["task"] == "mid_review"
```

Run:

```powershell
python -m pytest tests/integration/test_budget_repository.py -q
```

Expected:

```text
FAIL with ModuleNotFoundError: No module named 'catalyst_radar.storage.budget_repositories'
```

- [ ] **Step 2: Add ledger models**

Create `src/catalyst_radar/agents/models.py`.

Required public objects:

```python
class LLMTaskName(StrEnum):
    MINI_EXTRACTION = "mini_extraction"
    MID_REVIEW = "mid_review"
    SKEPTIC_REVIEW = "skeptic_review"
    GPT55_DECISION_CARD = "gpt55_decision_card"
    FULL_TRANSCRIPT_DEEP_DIVE = "full_transcript_deep_dive"


class LLMCallStatus(StrEnum):
    PLANNED = "planned"
    SKIPPED = "skipped"
    DRY_RUN = "dry_run"
    COMPLETED = "completed"
    FAILED = "failed"
    SCHEMA_REJECTED = "schema_rejected"


class LLMSkipReason(StrEnum):
    PREMIUM_LLM_DISABLED = "premium_llm_disabled"
    CANDIDATE_STATE_NOT_ELIGIBLE = "candidate_state_not_eligible"
    TASK_DAILY_CAP_EXCEEDED = "task_daily_cap_exceeded"
    DAILY_BUDGET_EXCEEDED = "daily_budget_exceeded"
    MONTHLY_BUDGET_EXCEEDED = "monthly_budget_exceeded"
    MONTHLY_SOFT_CAP_REQUIRES_HIGH_SCORE = "monthly_soft_cap_requires_high_score"
    MODEL_NOT_CONFIGURED = "model_not_configured"
    PRICING_MISSING = "pricing_missing"
    PRICING_STALE = "pricing_stale"
    MANUAL_TASK_REQUIRES_OPERATOR = "manual_task_requires_operator"
    CANDIDATE_PACKET_MISSING = "candidate_packet_missing"
    SCHEMA_VALIDATION_FAILED = "schema_validation_failed"
    CLIENT_ERROR = "client_error"

@dataclass(frozen=True)
class TokenUsage:
    input_tokens: int = 0
    cached_input_tokens: int = 0
    output_tokens: int = 0

@dataclass(frozen=True)
class BudgetLedgerEntry:
    id: str
    ts: datetime
    available_at: datetime
    task: LLMTaskName
    status: LLMCallStatus
    estimated_cost: float
    actual_cost: float
    currency: str = "USD"
    ticker: str | None = None
    candidate_state_id: str | None = None
    candidate_packet_id: str | None = None
    decision_card_id: str | None = None
    model: str | None = None
    provider: str = "none"
    skip_reason: LLMSkipReason | None = None
    token_usage: TokenUsage = TokenUsage()
    tool_calls: Sequence[Mapping[str, Any]] = ()
    candidate_state: str | None = None
    prompt_version: str | None = None
    schema_version: str | None = None
    outcome_label: str | None = None
    payload: Mapping[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
```

Validation rules:

- `id` is nonblank and stable.
- `ts`, `available_at`, and `created_at` are timezone-aware UTC.
- token counts are nonnegative integers.
- estimated and actual costs are finite and nonnegative.
- `ticker` is uppercased when present.
- `payload` and `tool_calls` are JSON-safe.

Add helper:

```python
def budget_ledger_id(
    *,
    task: str,
    ticker: str | None,
    candidate_packet_id: str | None,
    status: str,
    available_at: datetime,
    prompt_version: str | None = None,
) -> str:
    normalized = [
        "budget-ledger-v1",
        task,
        ticker.upper() if ticker else None,
        candidate_packet_id,
        status,
        available_at.astimezone(UTC).isoformat(),
        prompt_version,
    ]
    canonical = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:32]
    return f"budget-ledger-v1:{digest}"
```

The ID should be a URL-safe deterministic digest, following the Phase 11 `alert_id()` pattern.

- [ ] **Step 3: Add SQLAlchemy table and migration**

Modify `src/catalyst_radar/storage/schema.py` and create `sql/migrations/011_budget_llm.sql`.

SQL migration shape:

```sql
CREATE TABLE IF NOT EXISTS budget_ledger (
  id TEXT PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL,
  available_at TIMESTAMPTZ NOT NULL,
  ticker TEXT,
  candidate_state_id TEXT,
  candidate_packet_id TEXT,
  decision_card_id TEXT,
  task TEXT NOT NULL,
  model TEXT,
  provider TEXT NOT NULL,
  status TEXT NOT NULL,
  skip_reason TEXT,
  input_tokens BIGINT NOT NULL,
  cached_input_tokens BIGINT NOT NULL,
  output_tokens BIGINT NOT NULL,
  tool_calls JSONB NOT NULL,
  estimated_cost NUMERIC NOT NULL,
  actual_cost NUMERIC NOT NULL,
  currency TEXT NOT NULL,
  candidate_state TEXT,
  prompt_version TEXT,
  schema_version TEXT,
  outcome_label TEXT,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_budget_ledger_available_at
  ON budget_ledger (available_at);

CREATE INDEX IF NOT EXISTS ix_budget_ledger_task_status_ts
  ON budget_ledger (task, status, ts);

CREATE INDEX IF NOT EXISTS ix_budget_ledger_ticker_ts
  ON budget_ledger (ticker, ts);
```

SQLAlchemy table should use the existing `json_type` variant and indexes matching the migration.

- [ ] **Step 4: Implement repository**

Create `src/catalyst_radar/storage/budget_repositories.py`.

Required methods:

```python
class BudgetLedgerRepository:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def upsert_entry(self, entry: BudgetLedgerEntry) -> None:
        raise NotImplementedError("write row to budget_ledger")

    def list_entries(
        self,
        *,
        available_at: datetime | None = None,
        ticker: str | None = None,
        task: str | None = None,
        status: str | None = None,
        limit: int = 200,
    ) -> list[BudgetLedgerEntry]:
        raise NotImplementedError("read visible budget_ledger rows")

    def spend_between(
        self,
        *,
        start: datetime,
        end: datetime,
        statuses: Sequence[LLMCallStatus] = (LLMCallStatus.COMPLETED,),
    ) -> float:
        raise NotImplementedError("sum actual_cost by UTC time window")

    def task_count_between(
        self,
        *,
        task: str,
        start: datetime,
        end: datetime,
        statuses: Sequence[LLMCallStatus] = (
            LLMCallStatus.COMPLETED,
            LLMCallStatus.DRY_RUN,
            LLMCallStatus.PLANNED,
        ),
    ) -> int:
        raise NotImplementedError("count task rows by UTC time window")

    def summary(
        self,
        *,
        available_at: datetime | None = None,
        limit: int = 200,
    ) -> dict[str, object]:
        raise NotImplementedError("return cost summary for dashboard/API")
```

Summary must include:

```python
{
    "currency": "USD",
    "total_estimated_cost_usd": 0.0,
    "total_actual_cost_usd": 0.0,
    "attempt_count": 0,
    "status_counts": {},
    "by_task": [],
    "by_model": [],
    "rows": [],
}
```

- [ ] **Step 5: Run repository tests**

Run:

```powershell
python -m pytest tests/integration/test_budget_repository.py -q
python -m ruff check src/catalyst_radar/agents src/catalyst_radar/storage tests/integration/test_budget_repository.py
```

Expected:

```text
All selected tests pass
All checks passed!
```

- [ ] **Step 6: Commit**

Run:

```powershell
git add src/catalyst_radar/agents/__init__.py src/catalyst_radar/agents/models.py src/catalyst_radar/storage/budget_repositories.py src/catalyst_radar/storage/schema.py sql/migrations/011_budget_llm.sql tests/integration/test_budget_repository.py
git commit -m "feat: add budget ledger storage"
```

## Task 2: Add Config, Task Definitions, And Budget Controller

**Files:**
- Create: `src/catalyst_radar/agents/tasks.py`
- Create: `src/catalyst_radar/agents/budget.py`
- Modify: `src/catalyst_radar/core/config.py`
- Test: `tests/unit/test_config.py`
- Test: `tests/unit/test_budget_controller.py`

- [ ] **Step 1: Write config tests**

Extend `tests/unit/test_config.py`.

Required cases:

```python
def test_llm_config_defaults_fail_closed() -> None:
    config = AppConfig.from_env({})
    assert config.enable_premium_llm is False
    assert config.llm_evidence_model is None
    assert config.llm_input_cost_per_1m is None
    assert config.llm_daily_budget_usd == 0.0
    assert config.llm_monthly_budget_usd == 0.0


def test_llm_config_reads_pricing_and_caps() -> None:
    config = AppConfig.from_env(
        {
            "CATALYST_ENABLE_PREMIUM_LLM": "true",
            "CATALYST_LLM_EVIDENCE_MODEL": "model-review",
            "CATALYST_LLM_PROVIDER": "openai",
            "CATALYST_LLM_INPUT_COST_PER_1M": "5.00",
            "CATALYST_LLM_CACHED_INPUT_COST_PER_1M": "0.50",
            "CATALYST_LLM_OUTPUT_COST_PER_1M": "30.00",
            "CATALYST_LLM_DAILY_BUDGET_USD": "2.50",
            "CATALYST_LLM_MONTHLY_BUDGET_USD": "50.00",
            "CATALYST_LLM_TASK_DAILY_CAPS": "mid_review=3,gpt55_decision_card=1",
            "CATALYST_LLM_PRICING_UPDATED_AT": "2026-05-10",
        }
    )
    assert config.llm_provider == "openai"
    assert config.llm_evidence_model == "model-review"
    assert config.llm_task_daily_caps["mid_review"] == 3
```

Run:

```powershell
python -m pytest tests/unit/test_config.py -q
```

Expected:

```text
FAIL with AttributeError for llm_* config fields
```

- [ ] **Step 2: Add config fields**

Modify `src/catalyst_radar/core/config.py`.

Add helpers:

```python
def _optional_float(env: Mapping[str, str], key: str) -> float | None:
    raw = env.get(key)
    return None if raw is None or raw == "" else float(raw)


def _nonnegative_float(env: Mapping[str, str], key: str, default: float) -> float:
    value = _float(env, key, default)
    if value < 0:
        raise ValueError(f"{key} must be greater than or equal to zero")
    return value


def _task_caps(env: Mapping[str, str], key: str) -> Mapping[str, int]:
    raw = env.get(key)
    if raw is None or raw.strip() == "":
        return {}
    caps: dict[str, int] = {}
    for item in raw.split(","):
        name, value = item.split("=", maxsplit=1)
        cap = int(value)
        if cap < 0:
            raise ValueError(f"{key} cap must be greater than or equal to zero")
        caps[name.strip()] = cap
    return caps
```

Add `AppConfig` fields:

```python
llm_provider: str = "none"
llm_evidence_model: str | None = None
llm_skeptic_model: str | None = None
llm_decision_card_model: str | None = None
llm_input_cost_per_1m: float | None = None
llm_cached_input_cost_per_1m: float | None = None
llm_output_cost_per_1m: float | None = None
llm_pricing_updated_at: str | None = None
llm_pricing_stale_after_days: int = 30
llm_daily_budget_usd: float = 0.0
llm_monthly_budget_usd: float = 0.0
llm_monthly_soft_cap_pct: float = 0.80
llm_task_daily_caps: Mapping[str, int] = field(default_factory=dict)
```

Keep defaults fail-closed. `CATALYST_ENABLE_PREMIUM_LLM=true` alone must not be enough to allow a premium call without model, pricing, and positive budget values.

- [ ] **Step 3: Write budget controller tests**

Create `tests/unit/test_budget_controller.py`.

Required cases:

The file must contain tests named:

```text
test_estimates_cost_with_cached_tokens
test_blocks_when_premium_llm_disabled
test_blocks_ineligible_candidate_state
test_blocks_missing_model_or_pricing
test_blocks_stale_pricing
test_blocks_per_task_daily_cap
test_blocks_daily_and_monthly_budget_caps
test_blocks_gpt55_below_score_after_soft_monthly_cap
test_allows_when_all_gates_pass
```

Run:

```powershell
python -m pytest tests/unit/test_budget_controller.py -q
```

Expected:

```text
FAIL with ModuleNotFoundError for catalyst_radar.agents.budget
```

- [ ] **Step 4: Add task definitions**

Create `src/catalyst_radar/agents/tasks.py`.

Task defaults:

```python
DEFAULT_TASKS = {
    "mini_extraction": LLMTask(
        name=LLMTaskName.MINI_EXTRACTION,
        eligible_states=(ActionState.RESEARCH_ONLY, ActionState.ADD_TO_WATCHLIST),
        default_daily_cap=200,
        max_input_tokens=4000,
        max_output_tokens=700,
        prompt_version="mini_extraction_v1",
        schema_version="evidence-review-v1",
        model_config_key="llm_evidence_model",
    ),
    "mid_review": LLMTask(
        name=LLMTaskName.MID_REVIEW,
        eligible_states=(ActionState.WARNING, ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW),
        default_daily_cap=50,
        max_input_tokens=8000,
        max_output_tokens=1200,
        prompt_version="evidence_review_v1",
        schema_version="evidence-review-v1",
        model_config_key="llm_evidence_model",
    ),
    "skeptic_review": LLMTask(
        name=LLMTaskName.SKEPTIC_REVIEW,
        eligible_states=(ActionState.WARNING, ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW),
        default_daily_cap=20,
        max_input_tokens=9000,
        max_output_tokens=1400,
        prompt_version="skeptic_review_v1",
        schema_version="evidence-review-v1",
        model_config_key="llm_skeptic_model",
    ),
    "gpt55_decision_card": LLMTask(
        name=LLMTaskName.GPT55_DECISION_CARD,
        eligible_states=(ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW,),
        default_daily_cap=8,
        max_input_tokens=12000,
        max_output_tokens=2200,
        prompt_version="decision_card_v1",
        schema_version="decision-card-v1",
        model_config_key="llm_decision_card_model",
    ),
    "full_transcript_deep_dive": LLMTask(
        name=LLMTaskName.FULL_TRANSCRIPT_DEEP_DIVE,
        eligible_states=(ActionState.WARNING, ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW),
        default_daily_cap=0,
        max_input_tokens=40000,
        max_output_tokens=4000,
        prompt_version="full_transcript_deep_dive_v1",
        schema_version="evidence-review-v1",
        model_config_key="llm_skeptic_model",
        manual_only=True,
    ),
}
```

Use existing `ActionState` values and no real model names in task definitions. Models come from config.

- [ ] **Step 5: Implement budget controller**

Create `src/catalyst_radar/agents/budget.py`.

Required public contract:

```python
@dataclass(frozen=True)
class BudgetDecision:
    allowed: bool
    reason: LLMSkipReason | None
    estimated_cost: float
    daily_spend: float
    monthly_spend: float
    task_daily_count: int


class BudgetController:
    def __init__(
        self,
        *,
        config: AppConfig,
        ledger_repo: BudgetLedgerRepository,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.config = config
        self.ledger_repo = ledger_repo
        self.now = now or (lambda: datetime.now(UTC))

    def estimate_cost(self, usage: TokenUsage) -> float:
        raise NotImplementedError("calculate token cost from configured per-1M rates")

    def allow_llm_call(
        self,
        *,
        task: LLMTask,
        ticker: str | None,
        candidate_state: ActionState | str,
        final_score: float,
        estimated_usage: TokenUsage,
        available_at: datetime,
    ) -> BudgetDecision:
        raise NotImplementedError("evaluate config, state, budget, and cap gates")
```

Decision order:

1. Premium disabled.
2. Manual-only task.
3. Candidate state not eligible.
4. Model not configured.
5. Pricing missing.
6. Pricing stale.
7. Per-task daily cap exceeded.
8. Daily budget exceeded.
9. Monthly budget exceeded.
10. Monthly soft cap blocks GPT-5.5 task unless `final_score >= 90`.
11. Allow.

- [ ] **Step 6: Run config and budget tests**

Run:

```powershell
python -m pytest tests/unit/test_config.py tests/unit/test_budget_controller.py -q
python -m ruff check src/catalyst_radar/core/config.py src/catalyst_radar/agents tests/unit/test_config.py tests/unit/test_budget_controller.py
```

Expected:

```text
All selected tests pass
All checks passed!
```

- [ ] **Step 7: Commit**

Run:

```powershell
git add src/catalyst_radar/core/config.py src/catalyst_radar/agents/tasks.py src/catalyst_radar/agents/budget.py tests/unit/test_config.py tests/unit/test_budget_controller.py
git commit -m "feat: add llm budget controller"
```

## Task 3: Add Router, Fake Client, Prompt, And Schema Validation

**Files:**
- Create: `src/catalyst_radar/agents/router.py`
- Create: `src/catalyst_radar/agents/schemas.py`
- Create: `src/catalyst_radar/agents/prompts/evidence_review_v1.md`
- Test: `tests/unit/test_llm_router.py`
- Test: `tests/unit/test_agent_schemas.py`

- [ ] **Step 1: Write schema tests**

Create `tests/unit/test_agent_schemas.py`.

Required cases:

The file must contain tests named:

```text
test_validates_source_linked_evidence_review_output
test_rejects_claim_without_source_or_computed_feature
test_rejects_wrong_ticker
test_rejects_non_json_object
```

Run:

```powershell
python -m pytest tests/unit/test_agent_schemas.py -q
```

Expected:

```text
FAIL with ModuleNotFoundError for catalyst_radar.agents.schemas
```

- [ ] **Step 2: Implement schema validation**

Create `src/catalyst_radar/agents/schemas.py`.

Required functions:

```python
class AgentSchemaError(ValueError):
    pass

def validate_evidence_review_output(
    payload: Mapping[str, Any],
    *,
    ticker: str,
    as_of: datetime,
) -> Mapping[str, Any]:
    raise NotImplementedError("validate and return JSON-safe evidence review payload")
```

Minimum accepted payload:

```json
{
  "ticker": "MSFT",
  "as_of": "2026-05-08T21:00:00+00:00",
  "claims": [
    {
      "claim": "Revenue guide was raised.",
      "source_id": "event-msft",
      "source_quality": 0.9,
      "evidence_type": "filing",
      "sentiment": 0.6,
      "confidence": 0.8,
      "uncertainty_notes": "Needs follow-up on margin pressure."
    }
  ],
  "bear_case": ["Valuation is extended."],
  "unresolved_conflicts": [],
  "recommended_policy_downgrade": false
}
```

Every claim must have either `source_id` or `computed_feature_id`. Do not import third-party schema libraries in this phase.

- [ ] **Step 3: Write router tests**

Create `tests/unit/test_llm_router.py`.

Required cases:

The file must contain tests named:

```text
test_router_returns_skip_when_budget_blocks
test_router_dry_run_logs_estimate_without_client_call
test_router_fake_client_logs_completed_entry
test_router_rejects_schema_failure_and_logs_schema_rejected
test_router_does_not_mutate_candidate_packet_payload
```

Run:

```powershell
python -m pytest tests/unit/test_llm_router.py -q
```

Expected:

```text
FAIL with ModuleNotFoundError for catalyst_radar.agents.router
```

- [ ] **Step 4: Add prompt file**

Create `src/catalyst_radar/agents/prompts/evidence_review_v1.md`.

Prompt requirements:

```markdown
# Evidence Review v1

You are reviewing a source-linked equity candidate packet for investment decision support.

Rules:
- Do not compute scores, risk limits, sizing, or portfolio exposure.
- Do not recommend an autonomous buy or sell action.
- Use only the provided candidate packet, computed features, and evidence snippets.
- Every factual claim must include `source_id` or `computed_feature_id`.
- Return only JSON matching schema `evidence-review-v1`.
```

- [ ] **Step 5: Implement router and fake client**

Create `src/catalyst_radar/agents/router.py`.

Required public objects:

```python
class LLMClient(Protocol):
    def complete(self, request: LLMClientRequest) -> LLMClientResult:
        raise NotImplementedError("client implementations return schema candidate and token usage")


@dataclass(frozen=True)
class LLMRouteDecision:
    skip: bool
    reason: LLMSkipReason | None
    task: LLMTask
    model: str | None
    estimated_cost: float
    max_tokens: int


class FakeLLMClient:
    def complete(self, request: LLMClientRequest) -> LLMClientResult:
        raise NotImplementedError("return deterministic source-linked evidence review JSON")


class LLMRouter:
    def route(
        self,
        *,
        task: LLMTask,
        candidate: CandidatePacket,
        available_at: datetime,
    ) -> LLMRouteDecision:
        raise NotImplementedError("estimate cost and ask BudgetController")

    def review_candidate(
        self,
        *,
        task: LLMTask,
        candidate: CandidatePacket,
        available_at: datetime,
        dry_run: bool = False,
    ) -> LLMReviewResult:
        raise NotImplementedError("route, optionally call client, validate schema, and ledger result")
```

Behavior:

- `route()` estimates token usage from canonical packet JSON and task max output tokens.
- If budget blocks, write a `BudgetLedgerEntry(status="skipped")` and return a skip decision.
- `review_candidate(dry_run=True)` writes `status="dry_run"` and does not call the client.
- `review_candidate(dry_run=False)` calls only the injected client.
- Fake client returns source-linked JSON derived from the first supporting evidence item.
- Schema failures write `status="schema_rejected"` with `skip_reason="schema_validation_failed"`.
- Client exceptions write `status="failed"` with `skip_reason="client_error"`.
- Router must not persist or mutate candidate states, scores, packets, Decision Cards, alerts, or portfolio rows.

- [ ] **Step 6: Run router and schema tests**

Run:

```powershell
python -m pytest tests/unit/test_agent_schemas.py tests/unit/test_llm_router.py -q
python -m ruff check src/catalyst_radar/agents tests/unit/test_agent_schemas.py tests/unit/test_llm_router.py
```

Expected:

```text
All selected tests pass
All checks passed!
```

- [ ] **Step 7: Commit**

Run:

```powershell
git add src/catalyst_radar/agents/router.py src/catalyst_radar/agents/schemas.py src/catalyst_radar/agents/prompts/evidence_review_v1.md tests/unit/test_agent_schemas.py tests/unit/test_llm_router.py
git commit -m "feat: add sparse llm router foundation"
```

## Task 4: Add LLM Budget And Review CLI Commands

**Files:**
- Modify: `src/catalyst_radar/cli.py`
- Test: `tests/integration/test_llm_cli.py`

- [ ] **Step 1: Write CLI tests**

Create `tests/integration/test_llm_cli.py`.

Required cases:

The file must contain tests named:

```text
test_llm_budget_status_reports_zero_without_ledger_rows
test_run_llm_review_requires_candidate_packet
test_run_llm_review_dry_run_logs_dry_run_entry
test_run_llm_review_fake_client_logs_completed_entry
test_run_llm_review_default_premium_disabled_logs_skip
test_llm_budget_status_json_includes_caps_and_rows
```

Run:

```powershell
python -m pytest tests/integration/test_llm_cli.py -q
```

Expected:

```text
FAIL because CLI command is unknown
```

- [ ] **Step 2: Add parser commands**

Modify `build_parser()` in `src/catalyst_radar/cli.py`.

Add:

```python
budget_status = subparsers.add_parser("llm-budget-status")
budget_status.add_argument("--available-at", type=_parse_aware_datetime)
budget_status.add_argument("--json", action="store_true")

llm_review = subparsers.add_parser("run-llm-review")
llm_review.add_argument("--ticker", required=True)
llm_review.add_argument("--as-of", type=date.fromisoformat, required=True)
llm_review.add_argument("--available-at", type=_parse_aware_datetime)
llm_review.add_argument(
    "--task",
    choices=["mini_extraction", "mid_review", "skeptic_review", "gpt55_decision_card"],
    default="mid_review",
)
llm_review.add_argument("--fake", action="store_true")
llm_review.add_argument("--dry-run", action="store_true")
llm_review.add_argument("--json", action="store_true")
```

- [ ] **Step 3: Implement command handlers**

Add handlers in `main()`.

`llm-budget-status`:

- Create schema.
- Build `BudgetLedgerRepository`.
- Return ledger summary.
- Include caps from config without implying spend when no ledger rows exist.

Text output example:

```text
llm_budget_status actual_cost=0.000000 estimated_cost=0.000000 attempts=0 skipped=0 completed=0 source=budget_ledger
```

`run-llm-review`:

- Create schema.
- Find latest candidate packet by ticker/as-of/available-at.
- Build `BudgetController`, `LLMRouter`, and task definition.
- Use `FakeLLMClient` only when `--fake` is passed.
- Without `--fake` and without `--dry-run`, return a skip/failure because real clients are out of scope.
- Print route status and ledger ID.

Text output examples:

```text
llm_review ticker=MSFT task=mid_review status=skipped reason=premium_llm_disabled ledger_id=budget-ledger-v1:<digest>
llm_review ticker=MSFT task=mid_review status=dry_run model=fake estimated_cost=0.000000 ledger_id=budget-ledger-v1:<digest>
llm_review ticker=MSFT task=mid_review status=completed model=fake actual_cost=0.000000 ledger_id=budget-ledger-v1:<digest>
```

- [ ] **Step 4: Run CLI tests**

Run:

```powershell
python -m pytest tests/integration/test_llm_cli.py -q
python -m ruff check src/catalyst_radar/cli.py tests/integration/test_llm_cli.py
```

Expected:

```text
All selected tests pass
All checks passed!
```

- [ ] **Step 5: Commit**

Run:

```powershell
git add src/catalyst_radar/cli.py tests/integration/test_llm_cli.py
git commit -m "feat: add llm budget cli"
```

## Task 5: Wire Ledger Into Cost API And Dashboard

**Files:**
- Modify: `src/catalyst_radar/dashboard/data.py`
- Modify: `apps/dashboard/pages/4_Costs.py`
- Modify: `src/catalyst_radar/api/routes/costs.py`
- Test: `tests/integration/test_dashboard_data.py`
- Test: `tests/integration/test_api_routes.py`

- [ ] **Step 1: Write dashboard data tests**

Extend `tests/integration/test_dashboard_data.py`.

Required cases:

The file must contain tests named:

```text
test_load_cost_summary_uses_budget_ledger_rows
test_load_cost_summary_keeps_validation_cost_separate
test_load_cost_summary_hides_future_ledger_rows_by_default
```

Run:

```powershell
python -m pytest tests/integration/test_dashboard_data.py -q
```

Expected:

```text
FAIL because load_cost_summary does not read budget_ledger
```

- [ ] **Step 2: Update shared cost summary**

Modify `load_cost_summary()` in `src/catalyst_radar/dashboard/data.py`.

Rules:

- Query `BudgetLedgerRepository.summary(available_at=datetime.now(UTC))` by default.
- Do not double-count validation-run metrics as ledger cost.
- Keep useful alert count and cost-per-useful-alert calculation.
- If ledger actual cost is zero and validation metrics include historical cost, return it as `validation_total_cost_usd`, not `total_actual_cost_usd`.

Required output fields:

```python
{
    "currency": "USD",
    "total_actual_cost_usd": 0.0,
    "total_estimated_cost_usd": 0.0,
    "validation_total_cost_usd": 0.0,
    "useful_alert_count": 0,
    "cost_per_useful_alert": 0.0,
    "attempt_count": 0,
    "status_counts": {},
    "by_task": [],
    "by_model": [],
    "rows": [],
    "caps": {
        "premium_llm_enabled": False,
        "daily_budget_usd": 0.0,
        "monthly_budget_usd": 0.0,
        "task_daily_caps": {},
    },
    "source": "budget_ledger",
}
```

- [ ] **Step 3: Update API route test**

Extend `tests/integration/test_api_routes.py`.

Required assertion:

```python
response = client.get("/api/costs/summary")
assert response.status_code == 200
payload = response.json()
assert payload["source"] == "budget_ledger"
assert "total_actual_cost_usd" in payload
assert "status_counts" in payload
```

- [ ] **Step 4: Update Costs page**

Modify `apps/dashboard/pages/4_Costs.py`.

Add metrics:

- Actual LLM Cost
- Estimated LLM Cost
- Attempts
- Skipped
- Completed
- Useful Alerts
- Cost Per Useful Alert

Add tables:

- Ledger rows when present.
- Spend by task when present.
- Spend by model when present.

Keep the existing caption that missing spend rows remain zero.

- [ ] **Step 5: Run API/dashboard tests**

Run:

```powershell
python -m pytest tests/integration/test_dashboard_data.py tests/integration/test_api_routes.py -q
python -m ruff check src/catalyst_radar/dashboard/data.py apps/dashboard/pages/4_Costs.py src/catalyst_radar/api/routes/costs.py tests/integration/test_dashboard_data.py tests/integration/test_api_routes.py
```

Expected:

```text
All selected tests pass
All checks passed!
```

- [ ] **Step 6: Commit**

Run:

```powershell
git add src/catalyst_radar/dashboard/data.py apps/dashboard/pages/4_Costs.py src/catalyst_radar/api/routes/costs.py tests/integration/test_dashboard_data.py tests/integration/test_api_routes.py
git commit -m "feat: expose llm budget costs"
```

## Task 6: Prove Deterministic Pipeline Still Works Without LLM Config

**Files:**
- Test: existing fixture pipeline through CLI.
- Modify only if a regression is found.

- [ ] **Step 1: Run full unit and integration suite**

Run:

```powershell
python -m pytest
```

Expected:

```text
All tests pass
```

- [ ] **Step 2: Run ruff**

Run:

```powershell
python -m ruff check src tests apps
```

Expected:

```text
All checks passed!
```

- [ ] **Step 3: Run deterministic CLI smoke without LLM env**

Run:

```powershell
Remove-Item data/local/phase12-smoke.db -ErrorAction SilentlyContinue
$env:CATALYST_DATABASE_URL="sqlite:///data/local/phase12-smoke.db"
$env:PYTHONPATH="src"
Remove-Item Env:CATALYST_ENABLE_PREMIUM_LLM -ErrorAction SilentlyContinue
python -m catalyst_radar.cli init-db
python -m catalyst_radar.cli ingest-csv --securities data/sample/securities.csv --daily-bars data/sample/daily_bars.csv --holdings data/sample/holdings.csv
python -m catalyst_radar.cli scan --as-of 2026-05-08 --available-at 2026-05-10T14:00:00Z
python -m catalyst_radar.cli build-packets --as-of 2026-05-08 --available-at 2026-05-10T14:00:00Z
python -m catalyst_radar.cli build-decision-cards --as-of 2026-05-08 --available-at 2026-05-10T14:00:00Z
python -m catalyst_radar.cli build-alerts --as-of 2026-05-08 --available-at 2026-05-10T14:00:00Z
python -m catalyst_radar.cli llm-budget-status --available-at 2026-05-10T14:00:00Z
```

Expected:

```text
initialized database
ingested securities=6 daily_bars=36 holdings=1
scanned candidates=3
built candidate_packets=2
built_decision_cards cards=2
built_alerts alerts=1 suppressions=2 available_at=2026-05-10T14:00:00+00:00
llm_budget_status actual_cost=0.000000 estimated_cost=0.000000 attempts=0 skipped=0 completed=0 source=budget_ledger
```

- [ ] **Step 4: Run fake LLM CLI smoke**

Run with explicit fake-safe config:

```powershell
$env:CATALYST_ENABLE_PREMIUM_LLM="true"
$env:CATALYST_LLM_PROVIDER="fake"
$env:CATALYST_LLM_EVIDENCE_MODEL="fake-evidence-review-v1"
$env:CATALYST_LLM_INPUT_COST_PER_1M="0"
$env:CATALYST_LLM_CACHED_INPUT_COST_PER_1M="0"
$env:CATALYST_LLM_OUTPUT_COST_PER_1M="0"
$env:CATALYST_LLM_DAILY_BUDGET_USD="1"
$env:CATALYST_LLM_MONTHLY_BUDGET_USD="10"
$env:CATALYST_LLM_PRICING_UPDATED_AT="2026-05-10"
python -m catalyst_radar.cli run-llm-review --ticker BBB --as-of 2026-05-08 --available-at 2026-05-10T14:00:00Z --task mid_review --fake
python -m catalyst_radar.cli llm-budget-status --available-at 2026-05-10T14:00:00Z --json
```

Expected:

```text
llm_review ticker=BBB task=mid_review status=completed model=fake-evidence-review-v1 actual_cost=0.000000 ledger_id=budget-ledger-v1:<digest>
```

JSON should include one completed ledger row.

- [ ] **Step 5: Commit any regression fixes**

If files changed during verification, commit them:

```powershell
git add <changed-files>
git commit -m "fix: keep deterministic pipeline llm-free"
```

## Task 7: Review, Documentation, And Master Plan Update

**Files:**
- Create: `docs/phase-12-review.md`
- Modify: `docs/superpowers/plans/2026-05-09-full-product-implementation.md`

- [ ] **Step 1: Write phase review**

Create `docs/phase-12-review.md` with:

```markdown
# Phase 12 Review

## What Shipped

- Budget ledger schema, repository, and cost summaries.
- Config-driven model pricing and budget caps.
- Budget controller with fail-closed skip reasons.
- Sparse LLM router foundation with fake deterministic client only.
- CLI budget status and fake/dry-run review commands.
- Cost API/dashboard backed by ledger rows.

## Verification

- `python -m pytest` -> record the final pass count and duration from this phase.
- `python -m ruff check src tests apps` -> record `All checks passed!`.
- Deterministic CLI smoke without LLM config -> record the command transcript summary.
- Fake LLM review smoke -> record the completed fake-client ledger row summary.

## Safety Boundaries

- No real LLM provider dependency was added.
- Premium LLM remains disabled by default.
- Router does not mutate candidate scores, policy states, packets, cards, alerts, or portfolio data.
- Skipped and completed attempts are ledgered.

## Known Limits

- Evidence review is fake-client only.
- Phase 13 must add real evidence packet generation, skeptic review, and source-faithfulness evals.
- Pricing values are operator-configured and must be refreshed before real paid calls.
```

- [ ] **Step 2: Update master full-product plan**

Modify `docs/superpowers/plans/2026-05-09-full-product-implementation.md`:

- Update current status to include Phase 12.
- Add the Phase 12 plan path.
- Mark Phase 12 exit criteria complete after verification.
- Leave Phase 13 as the next phase.

- [ ] **Step 3: Final verification**

Run:

```powershell
python -m pytest
python -m ruff check src tests apps
git status --short
```

Expected:

```text
All tests pass
All checks passed!
Only intended docs/source/test files are modified before final commit, or the working tree is clean after commit.
```

- [ ] **Step 4: Commit docs**

Run:

```powershell
git add docs/phase-12-review.md docs/superpowers/plans/2026-05-09-full-product-implementation.md
git commit -m "docs: review phase 12 llm budget router"
```

## Acceptance Criteria

- `python -m pytest` passes.
- `python -m ruff check src tests apps` passes.
- Full deterministic fixture pipeline still runs with no LLM environment variables and no API key.
- Premium LLM is disabled by default in local/dev.
- Budget controller blocks missing config, missing pricing, stale pricing, disabled premium, ineligible states, per-task cap, daily cap, monthly cap, and monthly soft-cap cases.
- Router returns explicit skip reasons and logs skipped attempts.
- Fake-client review logs completed ledger entries with prompt/schema version, model, task, ticker, candidate state, token counts, estimated cost, actual cost, and outcome.
- No real LLM dependency or provider call is introduced.
- Cost summary API and dashboard show ledger-backed actual/estimated spend, attempts, skips, completions, task/model breakdowns, and useful-alert cost.
- Phase 13 can reuse the router, budget controller, schema validation, prompt versioning, and ledger interfaces without redesign.

## Review Checklist

Run this checklist before merging:

- Point-in-time: all ledger list/detail/summary views honor `available_at` and do not show future rows.
- Fail-closed: no premium call can be allowed by a single env var.
- Deterministic boundary: router output never changes scan scores, policy state, position sizing, portfolio exposure, alerts, or Decision Cards.
- Auditability: every skip, dry run, schema rejection, failure, and completed fake call writes one ledger row.
- Cost math: cached input, normal input, and output token rates are calculated separately.
- No double counting: validation-run costs and budget-ledger costs are surfaced separately.
- No credentials: no API keys or paid-provider SDK dependencies are added.
- UI copy: Costs page remains decision-support oriented and does not imply autonomous trading.

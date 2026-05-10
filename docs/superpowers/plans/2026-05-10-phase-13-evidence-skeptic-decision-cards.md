# Phase 13 Evidence, Skeptic Review, And Decision Cards Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn Phase 12's gated LLM plumbing into source-faithful evidence review, skeptic review, and LLM-assisted Decision Card drafting while preserving deterministic scores, policy gates, risk math, and human approval boundaries.

**Architecture:** Reuse the existing `candidate_packets` and `decision_cards` tables instead of adding duplicate evidence storage. Add agent-facing evidence packet views, schema validators, prompts, router dispatch, optional OpenAI Responses API client support, and evals. LLM output can annotate or draft review text, but it must never recompute market math, action state, position sizing, portfolio exposure, or order intent.

**Tech Stack:** Python 3.11, dataclasses, SQLAlchemy Core, SQLite/PostgreSQL-compatible schema, pytest, ruff, OpenAI Responses API behind a fail-closed optional client.

**Implementation Status:** Complete through local verification. Real OpenAI provider smoke is pending a user-provided `OPENAI_API_KEY`; the no-key provider path was verified fail-closed and ledgered.

---

## Baseline

Created worktree:

```text
C:\Users\fpan1\MarketRadar\.worktrees\phase-13-evidence-skeptic-decision-cards
branch: feature/phase-13-evidence-skeptic-decision-cards
base: main @ 908eaff docs: update phase 12 review fixes
```

Focused baseline:

```text
python -m pytest tests/unit/test_candidate_packet_builder.py tests/unit/test_decision_card_builder.py tests/unit/test_agent_schemas.py tests/unit/test_llm_router.py tests/integration/test_candidate_packet_repository.py -q
................................................... [100%]

python -m ruff check src/catalyst_radar/agents src/catalyst_radar/decision_cards src/catalyst_radar/pipeline/candidate_packet.py src/catalyst_radar/storage/candidate_packet_repositories.py tests/unit/test_candidate_packet_builder.py tests/unit/test_decision_card_builder.py tests/unit/test_agent_schemas.py tests/unit/test_llm_router.py tests/integration/test_candidate_packet_repository.py
All checks passed!
```

Current repo facts:

- `candidate_packets` and `decision_cards` tables already exist in `src/catalyst_radar/storage/schema.py`.
- `CandidatePacket`, `EvidenceItem`, `build_candidate_packet()`, and `canonical_packet_json()` already exist in `src/catalyst_radar/pipeline/candidate_packet.py`.
- `DecisionCard`, `build_decision_card()`, and deterministic manual-review safety checks already exist in `src/catalyst_radar/decision_cards/`.
- Phase 12 added `BudgetController`, `LLMRouter`, fake client, budget ledger, `run-llm-review`, and cost surfaces.
- Phase 12 intentionally left real OpenAI calls disabled. Phase 13 may add an optional client but must keep local/dev fail-closed.

Official OpenAI references for the optional provider path:

- API docs home: `https://developers.openai.com/api/docs`
- Responses API is recommended for new projects: `https://developers.openai.com/api/docs/guides/migrate-to-responses`
- Structured output uses `text.format` with `type: "json_schema"` and `strict: true`: `https://developers.openai.com/api/docs/guides/structured-outputs`
- Evals guidance: `https://developers.openai.com/api/docs/guides/evaluation-best-practices`

No API key is needed for Tasks 1-5. Ask the user for an OpenAI API key only before Task 6's real-provider smoke.

## Scope

Implement:

- Agent evidence packet view over existing `CandidatePacket`.
- Source-faithfulness checks for every LLM claim.
- Skeptic-review and decision-card-draft schemas and prompts.
- Router schema dispatch for `evidence-review-v1`, `skeptic-review-v1`, and `decision-card-v1`.
- Fake client outputs for skeptic and Decision Card tasks.
- Optional OpenAI Responses API client behind existing budget/config gates.
- CLI support for fake/dry-run/real gated skeptic and Decision Card review paths.
- Evals and tests proving unsupported claims are rejected and deterministic boundaries hold.
- Phase review docs and master-plan update.

Do not implement:

- Broker/order placement.
- LLM score calculation.
- LLM risk/position sizing/portfolio exposure math.
- Autonomous buy/sell decisions.
- Secret manager or role-based access. Those belong to Phase 15.

## File Structure

Create:

- `src/catalyst_radar/agents/evidence.py`
- `src/catalyst_radar/agents/openai_client.py`
- `src/catalyst_radar/agents/skeptic.py`
- `src/catalyst_radar/agents/prompts/skeptic_v1.md`
- `src/catalyst_radar/agents/prompts/decision_card_v1.md`
- `tests/unit/test_evidence_packet_schema.py`
- `tests/unit/test_skeptic_review.py`
- `tests/evals/test_llm_source_faithfulness.py`
- `docs/phase-13-review.md`

Modify:

- `pyproject.toml`
- `src/catalyst_radar/agents/router.py`
- `src/catalyst_radar/agents/schemas.py`
- `src/catalyst_radar/agents/tasks.py`
- `src/catalyst_radar/cli.py`
- `src/catalyst_radar/decision_cards/builder.py`
- `src/catalyst_radar/decision_cards/models.py`
- `src/catalyst_radar/decision_cards/__init__.py`
- `tests/unit/test_agent_schemas.py`
- `tests/unit/test_llm_router.py`
- `tests/unit/test_decision_card_builder.py`
- `tests/integration/test_llm_cli.py`
- `docs/superpowers/plans/2026-05-09-full-product-implementation.md`

## Data Contracts

### Agent Evidence Packet View

`build_agent_evidence_packet(packet)` returns a JSON-safe mapping:

```python
{
    "schema_version": "agent-evidence-packet-v1",
    "candidate_packet_id": packet.id,
    "ticker": packet.ticker,
    "as_of": packet.as_of.isoformat(),
    "available_at": packet.available_at.isoformat(),
    "state": packet.state.value,
    "final_score": packet.final_score,
    "supporting_evidence": [
        {
            "ref": "supporting_evidence[0]",
            "kind": "computed_feature",
            "title": "Relative strength confirmed",
            "summary": "Relative strength and price pillars are above deterministic thresholds.",
            "source_id": None,
            "source_url": None,
            "computed_feature_id": "signal_features:MSFT:2026-05-08:pillar_scores",
            "source_quality": 0.8,
            "strength": 0.91,
        }
    ],
    "disconfirming_evidence": [],
    "conflicts": [],
    "hard_blocks": [],
    "allowed_reference_ids": ["event-msft"],
    "allowed_computed_feature_ids": ["signal_features:MSFT:2026-05-08:pillar_scores"],
    "no_trade_execution": True,
}
```

### Skeptic Review v1

Minimum accepted payload:

```python
{
    "ticker": "MSFT",
    "as_of": "2026-05-08T21:00:00+00:00",
    "schema_version": "skeptic-review-v1",
    "bear_case": [
        {
            "claim": "Valuation risk remains elevated.",
            "source_id": None,
            "computed_feature_id": "signal_features:MSFT:2026-05-08:risk_penalty",
            "severity": "medium",
            "confidence": 0.74,
            "why_it_matters": "A high risk penalty can reduce margin of safety."
        }
    ],
    "missing_evidence": ["No updated margin guidance was present."],
    "contradictions": [],
    "recommended_policy_downgrade": False,
    "manual_review_notes": "Human reviewer should inspect valuation and event durability."
}
```

Rules:

- Every `bear_case` item must include `source_id` or `computed_feature_id`.
- IDs must exist in the agent evidence packet allow-list.
- `severity` is one of `low`, `medium`, `high`.
- `confidence` is `0.0..1.0`.
- No text may contain forbidden execution wording from `decision_cards.models.FORBIDDEN_EXECUTION_PHRASES`.

### Decision Card Draft v1

Minimum accepted payload:

```python
{
    "ticker": "MSFT",
    "as_of": "2026-05-08T21:00:00+00:00",
    "schema_version": "decision-card-v1",
    "summary": "Manual-review setup with evidence-backed catalyst and defined invalidation.",
    "supporting_points": [
        {
            "text": "Relative strength is above the deterministic threshold.",
            "computed_feature_id": "signal_features:MSFT:2026-05-08:pillar_scores"
        }
    ],
    "risks": [
        {
            "text": "Volatility penalty remains non-zero.",
            "computed_feature_id": "signal_features:MSFT:2026-05-08:risk_penalty"
        }
    ],
    "questions_for_human": ["Is catalyst durability confirmed by primary source?"],
    "manual_review_only": True
}
```

Rules:

- Draft text may be stored under the Decision Card payload key `llm_review`, but it must not overwrite deterministic `scores`, `trade_plan`, `position_sizing`, `portfolio_impact`, `controls`, or `identity.action_state`.
- Every supporting point and risk must include `source_id` or `computed_feature_id`.
- `manual_review_only` must be `True`.
- Forbidden execution wording is rejected.

## Task 0: Confirm Branch Baseline

**Files:**
- No source edits.

- [ ] **Step 1: Confirm branch and clean status**

Run:

```powershell
git status --short --branch
git rev-parse --short HEAD
```

Expected:

```text
## feature/phase-13-evidence-skeptic-decision-cards
908eaff
```

- [ ] **Step 2: Run focused baseline**

Run:

```powershell
python -m pytest tests/unit/test_candidate_packet_builder.py tests/unit/test_decision_card_builder.py tests/unit/test_agent_schemas.py tests/unit/test_llm_router.py tests/integration/test_candidate_packet_repository.py -q
python -m ruff check src/catalyst_radar/agents src/catalyst_radar/decision_cards src/catalyst_radar/pipeline/candidate_packet.py src/catalyst_radar/storage/candidate_packet_repositories.py tests/unit/test_candidate_packet_builder.py tests/unit/test_decision_card_builder.py tests/unit/test_agent_schemas.py tests/unit/test_llm_router.py tests/integration/test_candidate_packet_repository.py
```

Expected:

```text
All selected tests pass
All checks passed!
```

## Task 1: Add Agent Evidence Packet View And Source-Faithfulness Checks

**Files:**
- Create: `src/catalyst_radar/agents/evidence.py`
- Test: `tests/unit/test_evidence_packet_schema.py`
- Test: `tests/evals/test_llm_source_faithfulness.py`

- [ ] **Step 1: Write failing evidence packet tests**

Create `tests/unit/test_evidence_packet_schema.py` with tests named:

Required test names:

- `test_build_agent_evidence_packet_collects_allowed_references`
- `test_source_faithfulness_accepts_known_source_or_feature_ids`
- `test_source_faithfulness_rejects_unknown_references`
- `test_source_faithfulness_rejects_unlinked_claims`

Required assertions:

```python
view = build_agent_evidence_packet(_candidate())
assert view["schema_version"] == "agent-evidence-packet-v1"
assert view["candidate_packet_id"] == "packet-msft"
assert "event-msft" in view["allowed_reference_ids"]
assert "feature-risk-msft" in view["allowed_computed_feature_ids"]
assert view["no_trade_execution"] is True

violations = source_faithfulness_violations(
    {"claims": [{"claim": "Unsupported claim."}]},
    view,
)
assert violations == ["claims[0] must include source_id or computed_feature_id"]
```

Run:

```powershell
python -m pytest tests/unit/test_evidence_packet_schema.py -q
```

Expected:

```text
FAIL with ModuleNotFoundError: No module named 'catalyst_radar.agents.evidence'
```

- [ ] **Step 2: Implement evidence view**

Create `src/catalyst_radar/agents/evidence.py` with public functions:

Required public API:

- `AGENT_EVIDENCE_PACKET_SCHEMA_VERSION = "agent-evidence-packet-v1"`
- `build_agent_evidence_packet(packet: CandidatePacket) -> Mapping[str, Any]`
- `source_faithfulness_violations(payload: Mapping[str, Any], evidence_packet: Mapping[str, Any]) -> list[str]`

Implementation details:

- Use `packet.supporting_evidence` and `packet.disconfirming_evidence`.
- Preserve source fields; do not invent IDs.
- Add `source_id` values to `allowed_reference_ids`.
- Add `source_url` values to `allowed_reference_ids`.
- Add `computed_feature_id` values to `allowed_computed_feature_ids`.
- Scan nested payload fields named `claims`, `bear_case`, `supporting_points`, and `risks`.
- For every item in those lists, require either an allowed `source_id` or an allowed `computed_feature_id`.
- Return human-readable violation strings with stable index paths.

- [ ] **Step 3: Add source-faithfulness evals**

Create `tests/evals/test_llm_source_faithfulness.py` with cases:

Required test names:

- `test_eval_accepts_source_linked_skeptic_review`
- `test_eval_rejects_hallucinated_source_id`
- `test_eval_rejects_decision_card_draft_without_references`

Run:

```powershell
python -m pytest tests/unit/test_evidence_packet_schema.py tests/evals/test_llm_source_faithfulness.py -q
python -m ruff check src/catalyst_radar/agents/evidence.py tests/unit/test_evidence_packet_schema.py tests/evals/test_llm_source_faithfulness.py
```

Expected:

```text
All selected tests pass
All checks passed!
```

- [ ] **Step 4: Commit**

Run:

```powershell
git add src/catalyst_radar/agents/evidence.py tests/unit/test_evidence_packet_schema.py tests/evals/test_llm_source_faithfulness.py
git commit -m "feat: add agent evidence packet checks"
```

## Task 2: Add Skeptic And Decision Card Output Schemas

**Files:**
- Modify: `src/catalyst_radar/agents/schemas.py`
- Test: `tests/unit/test_agent_schemas.py`

- [ ] **Step 1: Write failing schema tests**

Extend `tests/unit/test_agent_schemas.py` with tests named:

Required test names:

- `test_validates_source_linked_skeptic_review_output`
- `test_rejects_skeptic_review_unknown_source_reference`
- `test_rejects_skeptic_review_forbidden_execution_language`
- `test_validates_source_linked_decision_card_draft_output`
- `test_rejects_decision_card_draft_that_is_not_manual_review_only`
- `test_rejects_decision_card_draft_unknown_feature_reference`

Run:

```powershell
python -m pytest tests/unit/test_agent_schemas.py -q
```

Expected:

```text
FAIL with ImportError for validate_skeptic_review_output or validate_decision_card_draft_output
```

- [ ] **Step 2: Implement schema validators**

Modify `src/catalyst_radar/agents/schemas.py`.

Add:

Required public API:

- `validate_skeptic_review_output(payload: Mapping[str, Any], *, ticker: str, as_of: datetime, evidence_packet: Mapping[str, Any]) -> Mapping[str, Any]`
- `validate_decision_card_draft_output(payload: Mapping[str, Any], *, ticker: str, as_of: datetime, evidence_packet: Mapping[str, Any]) -> Mapping[str, Any]`

Validation must:

- Require matching ticker and as_of.
- Require matching schema version.
- Call `source_faithfulness_violations()` and raise `AgentSchemaError` on any violation.
- Reject forbidden execution phrases using `FORBIDDEN_EXECUTION_PHRASES`.
- Reject non-string narrative fields.
- Bound confidence numbers to `0.0..1.0`.
- Return JSON-safe normalized payloads.

- [ ] **Step 3: Run schema tests**

Run:

```powershell
python -m pytest tests/unit/test_agent_schemas.py tests/unit/test_evidence_packet_schema.py tests/evals/test_llm_source_faithfulness.py -q
python -m ruff check src/catalyst_radar/agents/schemas.py tests/unit/test_agent_schemas.py
```

Expected:

```text
All selected tests pass
All checks passed!
```

- [ ] **Step 4: Commit**

Run:

```powershell
git add src/catalyst_radar/agents/schemas.py tests/unit/test_agent_schemas.py
git commit -m "feat: add skeptic and decision draft schemas"
```

## Task 3: Add Prompt Files And Router Dispatch

**Files:**
- Create: `src/catalyst_radar/agents/prompts/skeptic_v1.md`
- Create: `src/catalyst_radar/agents/prompts/decision_card_v1.md`
- Modify: `src/catalyst_radar/agents/tasks.py`
- Modify: `src/catalyst_radar/agents/router.py`
- Test: `tests/unit/test_llm_router.py`

- [ ] **Step 1: Add prompt files**

Create `src/catalyst_radar/agents/prompts/skeptic_v1.md`:

```markdown
# Skeptic Review v1

You are producing a bear-case review for a human investment reviewer.

Rules:
- Use only the supplied agent evidence packet.
- Every factual claim must include `source_id` or `computed_feature_id`.
- Do not compute scores, risk limits, sizing, portfolio exposure, or price targets.
- Do not recommend autonomous buying, selling, or order placement.
- Return only JSON matching schema `skeptic-review-v1`.
```

Create `src/catalyst_radar/agents/prompts/decision_card_v1.md`:

```markdown
# Decision Card Draft v1

You are drafting narrative sections for a human-review-only Decision Card.

Rules:
- Use only the supplied agent evidence packet and deterministic Decision Card payload.
- Do not alter action state, scores, trade plan, position sizing, portfolio impact, hard blocks, or next review time.
- Every factual point must include `source_id` or `computed_feature_id`.
- Do not say the system will buy, sell, execute, or place orders.
- Return only JSON matching schema `decision-card-v1`.
```

- [ ] **Step 2: Write failing router tests**

Extend `tests/unit/test_llm_router.py` with tests named:

Required test names:

- `test_router_fake_client_logs_skeptic_review_entry`
- `test_router_fake_client_logs_decision_card_draft_entry`
- `test_router_rejects_skeptic_review_with_unknown_source`
- `test_router_does_not_mutate_packet_or_decision_card_payloads`

Run:

```powershell
python -m pytest tests/unit/test_llm_router.py -q
```

Expected:

```text
FAIL because router still rejects decision-card-v1 and has no skeptic-review-v1 dispatch
```

- [ ] **Step 3: Update task definitions**

Modify `src/catalyst_radar/agents/tasks.py`:

- Set `skeptic_review.schema_version = "skeptic-review-v1"`.
- Keep `gpt55_decision_card.schema_version = "decision-card-v1"`.
- Keep eligible states sparse:
  - `skeptic_review`: Warning, ThesisWeakening, EligibleForManualBuyReview.
  - `gpt55_decision_card`: EligibleForManualBuyReview only.

- [ ] **Step 4: Update router and fake client**

Modify `src/catalyst_radar/agents/router.py`:

- Build an agent evidence packet from the candidate packet before sending to the client.
- Include that evidence packet in `LLMClientRequest` as `evidence_packet`.
- `FakeLLMClient.complete()` returns:
  - evidence review payload for `evidence-review-v1`;
  - skeptic review payload for `skeptic-review-v1`;
  - decision card draft payload for `decision-card-v1`.
- `_validate_output()` dispatches to:
  - `validate_evidence_review_output()`;
  - `validate_skeptic_review_output()`;
  - `validate_decision_card_draft_output()`.
- `outcome_label` is task-specific:
  - `evidence_review`;
  - `skeptic_review`;
  - `decision_card_draft`.
- Schema failures remain ledgered as `schema_rejected/schema_validation_failed`.

- [ ] **Step 5: Run router tests**

Run:

```powershell
python -m pytest tests/unit/test_llm_router.py tests/unit/test_agent_schemas.py tests/unit/test_evidence_packet_schema.py tests/evals/test_llm_source_faithfulness.py -q
python -m ruff check src/catalyst_radar/agents tests/unit/test_llm_router.py
```

Expected:

```text
All selected tests pass
All checks passed!
```

- [ ] **Step 6: Commit**

Run:

```powershell
git add src/catalyst_radar/agents/tasks.py src/catalyst_radar/agents/router.py src/catalyst_radar/agents/prompts/skeptic_v1.md src/catalyst_radar/agents/prompts/decision_card_v1.md tests/unit/test_llm_router.py
git commit -m "feat: dispatch skeptic and decision card llm schemas"
```

## Task 4: Add Skeptic Service And Decision Card Draft Attachment

**Files:**
- Create: `src/catalyst_radar/agents/skeptic.py`
- Modify: `src/catalyst_radar/decision_cards/builder.py`
- Modify: `src/catalyst_radar/decision_cards/models.py`
- Modify: `src/catalyst_radar/decision_cards/__init__.py`
- Test: `tests/unit/test_skeptic_review.py`
- Test: `tests/unit/test_decision_card_builder.py`

- [ ] **Step 1: Write failing skeptic service tests**

Create `tests/unit/test_skeptic_review.py` with tests named:

Required test names:

- `test_skeptic_review_runs_only_for_warning_or_higher_candidates`
- `test_skeptic_review_returns_schema_rejected_for_unfaithful_output`
- `test_skeptic_review_result_never_changes_candidate_state`

Run:

```powershell
python -m pytest tests/unit/test_skeptic_review.py -q
```

Expected:

```text
FAIL with ModuleNotFoundError: No module named 'catalyst_radar.agents.skeptic'
```

- [ ] **Step 2: Implement skeptic service**

Create `src/catalyst_radar/agents/skeptic.py`:

```python
def run_skeptic_review(
    *,
    router: LLMRouter,
    candidate: CandidatePacket,
    available_at: datetime,
    dry_run: bool = False,
) -> LLMReviewResult:
    return router.review_candidate(
        task=DEFAULT_TASKS["skeptic_review"],
        candidate=candidate,
        available_at=available_at,
        dry_run=dry_run,
    )
```

Keep the service intentionally thin; budget/state gating belongs to `BudgetController` and `LLMRouter`.

- [ ] **Step 3: Write failing Decision Card draft attachment tests**

Extend `tests/unit/test_decision_card_builder.py` with tests named:

Required test names:

- `test_attach_llm_review_adds_narrative_without_changing_deterministic_fields`
- `test_attach_llm_review_rejects_execution_language`
- `test_attach_llm_review_requires_manual_review_only`

Run:

```powershell
python -m pytest tests/unit/test_decision_card_builder.py -q
```

Expected:

```text
FAIL with ImportError for attach_llm_review_to_decision_card
```

- [ ] **Step 4: Implement Decision Card draft attachment**

Modify `src/catalyst_radar/decision_cards/builder.py` to add public API
`attach_llm_review_to_decision_card(card: DecisionCard, draft: Mapping[str, Any]) -> DecisionCard`.

Rules:

- Validate the draft with existing forbidden phrase checks and manual-review-only requirement.
- Add a new payload key `llm_review`.
- Do not alter existing deterministic payload keys.
- Return a new `DecisionCard` with the same `id`, `ticker`, `as_of`, `candidate_packet_id`, `action_state`, `setup_type`, `final_score`, `next_review_at`, `schema_version`, `source_ts`, `available_at`, and `user_decision`.

Modify `src/catalyst_radar/decision_cards/__init__.py` to export the helper.

- [ ] **Step 5: Run service and Decision Card tests**

Run:

```powershell
python -m pytest tests/unit/test_skeptic_review.py tests/unit/test_decision_card_builder.py tests/unit/test_llm_router.py -q
python -m ruff check src/catalyst_radar/agents/skeptic.py src/catalyst_radar/decision_cards tests/unit/test_skeptic_review.py tests/unit/test_decision_card_builder.py
```

Expected:

```text
All selected tests pass
All checks passed!
```

- [ ] **Step 6: Commit**

Run:

```powershell
git add src/catalyst_radar/agents/skeptic.py src/catalyst_radar/decision_cards/builder.py src/catalyst_radar/decision_cards/models.py src/catalyst_radar/decision_cards/__init__.py tests/unit/test_skeptic_review.py tests/unit/test_decision_card_builder.py
git commit -m "feat: attach llm review to decision cards"
```

## Task 5: Add Optional OpenAI Responses Client

**Files:**
- Modify: `pyproject.toml`
- Create: `src/catalyst_radar/agents/openai_client.py`
- Modify: `src/catalyst_radar/cli.py`
- Test: `tests/unit/test_llm_router.py`
- Test: `tests/integration/test_llm_cli.py`

- [ ] **Step 1: Write failing OpenAI client unit tests**

Add tests named:

Required test names:

- `test_openai_client_builds_responses_request_with_strict_json_schema`
- `test_openai_client_converts_usage_to_token_usage`
- `test_openai_client_requires_api_key_for_real_call`

Use a fake injected SDK object; do not make network calls.

Run:

```powershell
python -m pytest tests/unit/test_llm_router.py -q
```

Expected:

```text
FAIL with ModuleNotFoundError: No module named 'catalyst_radar.agents.openai_client'
```

- [ ] **Step 2: Add dependency metadata**

Modify `pyproject.toml`:

```toml
dependencies = [
  "openai>=2.0",
  "openai>=2.0",
]
```

The implementation must lazy-import the SDK inside `OpenAIResponsesClient` so normal tests and fake-client runs do not require an API key or network.

- [ ] **Step 3: Implement OpenAI Responses client**

Create `src/catalyst_radar/agents/openai_client.py` with:

Required public API:

- `class OpenAIResponsesClient`
- `OpenAIResponsesClient.__init__(*, api_key: str | None = None, sdk_client: Any | None = None) -> None`
- `OpenAIResponsesClient.complete(request: LLMClientRequest) -> LLMClientResult`

Request rules:

- Use Responses API, not Chat Completions.
- Set `store=False`.
- Pass prompt rules as `instructions`.
- Pass canonical evidence packet/request JSON as `input`.
- Use strict structured output:
  - `text={"format": {"type": "json_schema", "name": request.schema_version.replace("-", "_"), "schema": schema_for_task(request.task), "strict": True}}`
- Parse `response.output_text` as JSON.
- Map usage fields into `TokenUsage`.
- Raise clear `RuntimeError("openai_api_key_missing")` when a real SDK client is used without `OPENAI_API_KEY`.

- [ ] **Step 4: Wire CLI provider selection**

Modify `src/catalyst_radar/cli.py`:

- If `--fake`, use `FakeLLMClient`.
- If `--dry-run`, no client call happens.
- If provider is `openai`, premium is enabled, and no `--fake`, use `OpenAIResponsesClient`.
- If provider is not fake/openai, keep fail-closed `_SafeDisabledLLMClient`.
- Do not print API keys.
- Real-provider failures remain ledgered as `failed/client_error` and return nonzero.

- [ ] **Step 5: Run focused tests**

Run:

```powershell
python -m pytest tests/unit/test_llm_router.py tests/integration/test_llm_cli.py -q
python -m ruff check pyproject.toml src/catalyst_radar/agents/openai_client.py src/catalyst_radar/cli.py tests/unit/test_llm_router.py tests/integration/test_llm_cli.py
```

Expected:

```text
All selected tests pass
All checks passed!
```

- [ ] **Step 6: Commit**

Run:

```powershell
git add pyproject.toml src/catalyst_radar/agents/openai_client.py src/catalyst_radar/cli.py tests/unit/test_llm_router.py tests/integration/test_llm_cli.py
git commit -m "feat: add gated openai responses client"
```

## Task 6: CLI Smokes And API-Key-Gated Real Provider Smoke

**Files:**
- Modify only if regressions are found.

- [ ] **Step 1: Run fake skeptic and Decision Card CLI smokes**

Run:

```powershell
Remove-Item data/local/phase13-smoke.db -ErrorAction SilentlyContinue
$env:CATALYST_DATABASE_URL="sqlite:///data/local/phase13-smoke.db"
$env:PYTHONPATH="src"
python -m catalyst_radar.cli init-db
python -m catalyst_radar.cli ingest-csv --securities data/sample/securities.csv --daily-bars data/sample/daily_bars.csv --holdings data/sample/holdings.csv
python -m catalyst_radar.cli scan --as-of 2026-05-08 --available-at 2026-05-10T14:00:00Z
python -m catalyst_radar.cli build-packets --as-of 2026-05-08 --available-at 2026-05-10T14:00:00Z
python -m catalyst_radar.cli build-decision-cards --as-of 2026-05-08 --available-at 2026-05-10T14:00:00Z
$env:CATALYST_ENABLE_PREMIUM_LLM="true"
$env:CATALYST_LLM_PROVIDER="fake"
$env:CATALYST_LLM_EVIDENCE_MODEL="fake-evidence-review-v1"
$env:CATALYST_LLM_SKEPTIC_MODEL="fake-skeptic-v1"
$env:CATALYST_LLM_DECISION_CARD_MODEL="fake-decision-card-v1"
$env:CATALYST_LLM_INPUT_COST_PER_1M="0"
$env:CATALYST_LLM_CACHED_INPUT_COST_PER_1M="0"
$env:CATALYST_LLM_OUTPUT_COST_PER_1M="0"
$env:CATALYST_LLM_DAILY_BUDGET_USD="1"
$env:CATALYST_LLM_MONTHLY_BUDGET_USD="10"
$env:CATALYST_LLM_PRICING_UPDATED_AT="2026-05-10"
python -m catalyst_radar.cli run-llm-review --ticker AAA --as-of 2026-05-08 --available-at 2026-05-10T14:00:00Z --task skeptic_review --fake
python -m catalyst_radar.cli run-llm-review --ticker AAA --as-of 2026-05-08 --available-at 2026-05-10T14:00:00Z --task gpt55_decision_card --fake
python -m catalyst_radar.cli llm-budget-status --available-at 2026-05-10T14:00:00Z --json
```

Expected:

```text
skeptic_review status=completed
gpt55_decision_card for sample AAA status=skipped reason=candidate_state_not_eligible
gpt55_decision_card for seeded eligible smoke packet ZZZ status=completed
budget summary includes completed skeptic and Decision Card rows
```

- [ ] **Step 2: Stop and ask for API key before real-provider smoke**

Before running a real provider call, ask:

```text
I need an OpenAI API key now for the real-provider smoke. Please provide a temporary key or set OPENAI_API_KEY in this shell/session.
```

Do not proceed with a real call until the user provides the key or confirms it is set.

- [ ] **Step 3: Run real-provider smoke only after key is available**

Run:

```powershell
$env:CATALYST_ENABLE_PREMIUM_LLM="true"
$env:CATALYST_LLM_PROVIDER="openai"
python -m catalyst_radar.cli run-llm-review --ticker AAA --as-of 2026-05-08 --available-at 2026-05-10T14:00:00Z --task skeptic_review --json
```

Expected:

```text
Run this command only after the user has set OPENAI_API_KEY and CATALYST_LLM_* model, pricing, and cap variables in the shell.
Either status=completed with schema-valid source-linked output, or status=failed/client_error with a ledger row.
No deterministic scores, cards, alerts, or portfolio rows are mutated by this smoke.
```

- [ ] **Step 4: Commit any smoke regression fixes**

If source files changed:

```powershell
git add <changed-files>
git commit -m "fix: stabilize phase 13 llm smokes"
```

## Task 7: Full Verification And Documentation

**Files:**
- Create: `docs/phase-13-review.md`
- Modify: `docs/superpowers/plans/2026-05-09-full-product-implementation.md`

- [ ] **Step 1: Run full verification**

Run:

```powershell
python -m pytest
python -m ruff check src tests apps
git diff --check
```

Expected:

```text
All tests pass
All checks passed!
No whitespace errors
```

- [ ] **Step 2: Write phase review**

Create `docs/phase-13-review.md` with:

```markdown
# Phase 13 Review

## What Shipped

- Agent evidence packet view over existing CandidatePacket.
- Source-faithfulness checks and evals.
- Skeptic-review and Decision Card draft schemas/prompts.
- Router dispatch for evidence, skeptic, and Decision Card schemas.
- Fake-client outputs for all Phase 13 task schemas.
- Optional gated OpenAI Responses API client.
- CLI fake smokes for skeptic and Decision Card review.

## Verification

- `python -m pytest` -> record final pass count and duration.
- `python -m ruff check src tests apps` -> record result.
- Fake Phase 13 CLI smoke -> record ledger row count and statuses.
- Real-provider smoke -> record skipped/not run, failed with ledger, or completed with ledger.

## Safety Boundaries

- LLM output never changes deterministic scores, action state, trade plan, position sizing, portfolio exposure, alerts, or orders.
- Every accepted LLM claim is source-linked to the agent evidence packet.
- Unsupported claims are schema-rejected and ledgered.
- Premium LLM remains disabled by default.

## Known Limits

- Real provider smoke requires a user-provided OpenAI API key.
- Phase 14 still needs scheduling, health checks, metrics, and runbooks.
- Phase 15 still needs secret management and audit controls.
```

- [ ] **Step 3: Update master plan**

Modify `docs/superpowers/plans/2026-05-09-full-product-implementation.md`:

- Mark Phase 13 implementation tasks complete.
- Add the Phase 13 plan path.
- Add delivered details and final verification summary.
- Leave Phase 14 as next phase.

- [ ] **Step 4: Commit docs**

Run:

```powershell
git add docs/phase-13-review.md docs/superpowers/plans/2026-05-09-full-product-implementation.md
git commit -m "docs: review phase 13 evidence skeptic cards"
```

## Acceptance Criteria

- `python -m pytest` passes.
- `python -m ruff check src tests apps` passes.
- Every Warning-or-higher candidate has source-linked supporting and disconfirming evidence in its candidate packet.
- Skeptic review is limited to Warning, ThesisWeakening, and EligibleForManualBuyReview candidates.
- Decision Card draft review is limited to EligibleForManualBuyReview candidates.
- Evidence, skeptic, and Decision Card draft outputs are schema-validated.
- Unsupported source IDs, feature IDs, and unlinked claims are rejected.
- Rejected outputs are ledgered as `schema_rejected/schema_validation_failed`.
- Fake-client skeptic and Decision Card review paths complete deterministically.
- Optional OpenAI provider path uses Responses API with strict JSON schema and remains fail-closed without a key.
- No API key is committed, logged, or printed.
- LLM output does not mutate deterministic scores, policy state, risk, sizing, portfolio exposure, alerts, or order behavior.
- Decision Cards continue to say manual review only and never imply autonomous trading.

## Review Checklist

- Source-faithfulness: every accepted claim has a known source or computed feature ID.
- Point-in-time: evidence packet view excludes future packet inputs.
- Budget: all task attempts write ledger rows and enforce Phase 12 caps.
- Safety: forbidden execution wording is rejected in all LLM narrative paths.
- OpenAI: real provider path uses Responses API, `store=False`, strict JSON schema, and no hardcoded credentials.
- UI/API: existing dashboard and cost surfaces still load with new ledger rows.
- Docs: Phase 13 review records whether real-provider smoke was skipped, failed with ledger, or completed.

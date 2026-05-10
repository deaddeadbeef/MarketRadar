# Phase 13 Review

## What Shipped

- Agent evidence packet view over existing `CandidatePacket` objects and the existing `candidate_packets` table.
- Source-faithfulness validation for accepted LLM claims, including source IDs, source URLs, computed feature IDs, and stable evidence refs.
- Skeptic-review and Decision Card draft schemas with forbidden execution-language checks.
- Prompt files and router dispatch for `evidence-review-v1`, `skeptic-review-v1`, and `decision-card-v1`.
- Fake-client outputs for all Phase 13 task schemas.
- A thin skeptic review service that delegates state, budget, and schema gates to `LLMRouter`.
- Decision Card LLM narrative attachment under `payload["llm_review"]` without mutating deterministic fields.
- Optional OpenAI Responses API client behind existing premium LLM, model, pricing, cap, and API-key gates.
- CLI fake smokes and fail-closed no-key provider behavior.

## Verification

- `python -m pytest` -> `480 passed in 134.38s (0:02:14)`.
- `python -m ruff check src tests apps` -> `All checks passed!`.
- Fake Phase 13 CLI smoke:
  - `skeptic_review` for sample `AAA` completed with `schema_version=skeptic-review-v1`.
  - `gpt55_decision_card` for sample `AAA` skipped with `candidate_state_not_eligible`, as expected because the sample candidate is `Warning`.
  - Seeded eligible smoke packet `ZZZ` completed `gpt55_decision_card` with `schema_version=decision-card-v1`.
- Smoke budget ledger at `2026-05-10T14:00:00Z` recorded `5` attempts: `2 completed`, `2 skipped`, and `1 failed`.
- No-key OpenAI provider smoke failed closed before any real provider call: `status=failed`, `skip_reason=client_error`, `actual_cost_usd=0.0`, model `gpt-5.1`.
- Real-provider smoke was not run because no OpenAI API key was available in the session.

## Safety Boundaries

- LLM output never changes deterministic scores, action state, trade plan, position sizing, portfolio exposure, alerts, or orders.
- Every accepted skeptic or Decision Card claim must link to the agent evidence packet by `source_id`, `source_url`, or `computed_feature_id`.
- Unsupported, unknown, or unlinked claims are schema-rejected and ledgered.
- Forbidden autonomous execution language is rejected in LLM narrative paths.
- Premium LLM remains disabled by default.
- The OpenAI client uses Responses API request construction with `store=False` and strict JSON schema output formatting.
- No API key is committed, logged, or printed.

## Known Limits

- A real OpenAI provider smoke still requires a user-provided `OPENAI_API_KEY`.
- Current sample data does not contain an `EligibleForManualBuyReview` candidate, so the Decision Card fake completion smoke uses a synthetic eligible packet.
- Phase 14 still needs scheduling, health checks, metrics, and runbooks.
- Phase 15 still needs secret management and audit controls.

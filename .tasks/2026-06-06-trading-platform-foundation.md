# Agentic Trading Platform Foundation

## Goal

Build the first full-platform foundation for MarketRadar: a deterministic,
agentic trading plan surface that turns a stored Decision Card into a supervised
paper-trading plan while keeping broker execution disabled.

## Scope

- Add a read-only trading platform planner that composes Decision Card evidence,
  agentic paper-trade intent, risk approval state, order intent, supervision
  controls, and live-execution kill switches.
- Add a CLI command that returns the plan as JSON or concise human text.
- Prove with tests that the command makes zero external calls, performs zero
  database writes, submits no broker orders, and does not create paper trades.
- Document the command beside the existing validation and paper-trading workflow.

## Non-Goals

- No autonomous live trading.
- No Schwab order placement.
- No broker order-ticket writes.
- No new provider, OpenAI, web, shell, or market-data calls.

## Acceptance Criteria

- `catalyst-radar trading-platform-plan --decision-card-id <CARD_ID> --available-at <UTC> --json`
  emits `schema_version=agentic-trading-platform-plan-v1`.
- Eligible manual-buy-review cards with complete trade-plan and sizing data emit
  `status=ready_for_paper_trade`, `approved_for_paper_trade=true`, and
  `approved_for_live_submission=false`.
- Blocked or incomplete cards emit `status=blocked` with deterministic
  `paper_trade_blocks`.
- Every payload includes `external_calls_required=0`, `external_calls_made=0`,
  `db_writes_required=0`, `db_writes_made=0`, `broker_order_submitted=false`,
  `order_submission_allowed=false`, and `no_execution=true`.
- Focused unit and integration tests pass, along with ruff and diff checks.

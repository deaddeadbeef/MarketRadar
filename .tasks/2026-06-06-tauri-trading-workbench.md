# Tauri Trading Workbench Platform

## Goal

Turn the Tauri desktop app into a comprehensive trading platform shell where
MarketRadar is one tool in a larger supervised trading workflow.

## Product Direction

The desktop app should become a Trading Workbench centered on trade ideas,
portfolio/risk context, agent review, paper trading, validation, broker preview,
and human journal/review loops. MarketRadar remains the scouting and evidence
engine, but it is not the whole product frame.

## First Slice Scope

- Reframe the native app as `MarketRadar Trading Workbench`.
- Add a platform module map to the desktop configuration and API configuration
  surfaces.
- Expose the platform modules in the Tauri shell: Command Center, Portfolio,
  Market Radar, Trade Planner, Risk Desk, Paper Trading, Broker Desk,
  Backtest/Replay, Alerts, Journal, and Agent Cockpit.
- Add platform pages/routes for the new trading modules while preserving the
  existing dashboard pages and zero-call browsing contract.
- Keep live trading disabled and visible through the shell.

## Non-Goals For This Slice

- No live broker submission.
- No autonomous trading.
- No new market data, OpenAI, Schwab, broker, shell, or web calls.
- No paper-trade execution from the new shell. Existing preview-first commands
  remain the execution boundary.

## Acceptance Criteria

- The Tauri window and static shell identify as a Trading Workbench.
- The navigation exposes MarketRadar as a platform tool, not the product root.
- The platform overview includes all expected modules and explicit live-trading
  disabled state.
- New platform pages are routeable by command/page alias and preserve
  `provider_calls=0` browsing.
- Desktop/API config manifests expose the platform module map for automation.
- Existing desktop frontend and Tauri tests pass.

## Current Slice: Lifecycle Actions

- Add local ledger/outcome command metadata to trade lifecycle rows.
- Render guarded lifecycle row actions in the desktop workbench.
- Route lifecycle actions through the existing dashboard backend command path.
- Preserve the no-live-trading boundary: no broker order submission and no
  provider calls from browsing.

## Current Slice: Agent Preview Actions

- Add safe agent preview command metadata to Agent Cockpit action rows.
- Render row-level preview buttons for proposed human actions.
- Route preview buttons through the existing dashboard backend command path.
- Keep `agent execute` outside clickable browsing controls.

## Current Slice: Portfolio And Risk Actions

- Add safe portfolio/risk review command metadata to Portfolio and Risk Desk rows.
- Render row-level review/preview buttons for read-only portfolio context and
  paper-risk approval gates.
- Route safe buttons through existing page/backend command paths.
- Keep live submission and broker execution as disabled boundary text.

## Current Slice: Shared Action Bus

- Normalize paper, ticket, lifecycle, portfolio, risk, broker, and agent actions
  into a single workbench action-bus snapshot contract.
- Render a Command Center action panel and module-filtered action panels in the
  Tauri dashboard.
- Route action-bus buttons through one frontend dispatcher with explicit
  backend-command, page-route, local-write, and boundary handling.
- Preserve zero provider calls, no live broker submission, and disabled
  autonomous execution.

## Current Slice: Workflow Map

- Add a supervised workflow-map snapshot contract that connects MarketRadar
  scouting to candidate review, decision review, trade planning, risk approval,
  paper trading, broker boundary, journal/validation, and agent review.
- Render the workflow map in the Tauri Command Center and as a module-filtered
  stage view on relevant platform pages.
- Reuse the shared action dispatcher for workflow stage controls.
- Preserve the same decision-support boundary: zero provider calls from
  browsing, no live broker submission, and no autonomous execution.

## Current Slice: Priority Queue

- Add a supervised priority-queue snapshot contract that ranks the current
  workflow blocker, safe module handoffs, local preview commands, guarded local
  writes, and disabled execution boundaries.
- Render the priority queue in the Tauri Command Center and module-filtered
  platform pages, reusing the shared action dispatcher.
- Expose queue status, primary item, and item count in automation JSON for
  agentic desktop control.
- Preserve the same decision-support boundary: zero provider calls from
  browsing, no live broker submission, and no autonomous execution.

## Current Slice: Supervision Gates

- Add a supervision-gates snapshot contract that explains zero-call browsing,
  local previews, guarded local writes, broker submission, agent execution, and
  autonomous execution boundaries.
- Render supervision gates in the Tauri Command Center and module-filtered
  pages so operators can see which actions require manual approval.
- Require an explicit arm step before any Tauri workbench local-write command
  can run through the shared dispatcher.
- Expose supervision status, primary gate, approval-required count, and armed
  local-write state in automation JSON.

## Current Slice: Decision Brief

- Add a top-level decision-brief snapshot contract that turns MarketRadar's
  current scouted opportunity plus the active trading plan into one
  agent-readable workbench dossier.
- Include ticker, source tool, scouted evidence, setup, risk posture, workflow
  blocker, priority item, supervision gate, safe next action, and execution
  boundary metadata.
- Render the decision brief in the Tauri Command Center before workflow and
  action controls so operators see the current case before interacting with
  tools.
- Expose decision-brief status, ticker, next command, and source tool in
  automation JSON without adding provider, broker, shell, or DB calls.

## Current Slice: Scenario Matrix

- Add a read-only scenario-matrix snapshot contract derived from the active
  trading plan's entry, invalidation, reward/risk, target, sizing, and blocker
  data.
- Render the scenario matrix in the Tauri Command Center and relevant planning,
  risk, paper, and broker pages so operators can inspect downside/upside before
  interacting with local-write controls.
- Expose scenario status, ticker, scenario count, and reward/risk in automation
  JSON for agentic desktop control.
- Preserve the same boundary: no provider calls, no broker order submission,
  no shell execution, and no database writes from browsing the matrix.

## Current Slice: Risk Envelope

- Add a read-only risk-envelope snapshot contract that combines the active
  MarketRadar plan with broker portfolio context, sizing state, paper/live
  blockers, and execution-boundary checks.
- Render the risk envelope in the Tauri Command Center and relevant portfolio,
  planning, risk, paper, and broker pages before local-write controls.
- Expose risk-envelope status, ticker, sizing state, risk block count, and
  max-loss context in automation JSON for agentic desktop review.
- Preserve the same boundary: no provider calls, no broker order submission,
  no shell execution, and no database writes from browsing the envelope.

## Current Slice: Trade Runbook

- Add a read-only trade-runbook snapshot contract that sequences the active
  MarketRadar idea through decision review, scenario review, risk envelope,
  paper preview, guarded paper record, broker-ticket preview, live boundary,
  and journal/validation review.
- Render the runbook in the Tauri Command Center and relevant trading pages so
  agents and operators can follow the supervised workflow from one surface.
- Expose runbook status, active step, step count, and blocked-step count in
  automation JSON for agentic desktop control.
- Preserve the same boundary: no provider calls, no broker order submission,
  no shell execution, and no database writes from browsing the runbook.

## Current Slice: Operator State

- Add a top-level operator-state snapshot contract that condenses the current
  workbench case into active blocker, active module, safe next control,
  readiness, risk, and execution-boundary summaries.
- Render operator state at the top of the Tauri Command Center and relevant
  module pages so an agent or human can orient without reconciling every panel.
- Expose operator status, active module, active blocker, and next command in
  automation JSON for agentic desktop control.
- Preserve the same boundary: no provider calls, no broker order submission,
  no shell execution, and no database writes from browsing operator state.

## Current Slice: Execution Sandbox

- Add a top-level execution-sandbox snapshot contract that separates local
  previews, guarded local writes, live broker submission, and agent execution
  boundaries.
- Render execution lanes in the Tauri Command Center and relevant module pages
  before local write controls.
- Expose sandbox status, active lane, preview count, and disabled-boundary
  count in automation JSON.
- Preserve the same boundary: no provider calls, no broker order submission,
  no autonomous execution, and no database writes from browsing the sandbox.

## Current Slice: Position Sizing

- Add a read-only position-sizing snapshot contract that turns portfolio
  equity, risk-per-trade, entry/stop, buying power, and current blockers into
  a risk-budget sizing worksheet.
- Render the sizing worksheet in the Tauri Command Center and relevant
  portfolio, planning, risk, paper, and broker pages before risk and execution
  controls.
- Expose sizing status, ticker, suggested shares, and risk budget in
  automation JSON for agentic desktop review.
- Preserve the same boundary: no provider calls, no broker order submission,
  no shell execution, and no database writes from browsing the worksheet.

## Current Slice: Order Ticket Draft

- Add a read-only order-ticket-draft snapshot contract that composes the active
  idea, sizing recommendation, ticket prices, risk estimates, and local ticket
  commands into one broker-safe draft.
- Render the draft in the Tauri Command Center and relevant planning, risk,
  paper, and broker pages between sizing and risk controls.
- Expose draft status, ticker, suggested shares, and preview command in
  automation JSON for agentic desktop review.
- Preserve the same boundary: no provider calls, no broker order submission,
  no shell execution, and no database writes from browsing the draft.

## Current Slice: Portfolio Impact Preview

- Add a read-only portfolio-impact-preview snapshot contract that preserves the
  decision card's proposed notional, max loss, hard blocks, and exposure-scope
  availability alongside current broker portfolio context.
- Render the preview in the Tauri Command Center and relevant portfolio,
  planning, risk, paper, and broker pages before sizing and ticket controls.
- Expose impact status, ticker, proposed notional, and blocker count in
  automation JSON for agentic risk review.
- Preserve the same boundary: no provider calls, no broker order submission,
  no shell execution, and no database writes from browsing the preview.

## Current Slice: Paper Trade Preview

- Add a read-only paper-trade-preview snapshot contract that turns the active
  agentic paper intent, risk gate, confirmed/suggested size, and paper commands
  into one supervised preview dossier.
- Render the preview in the Tauri Command Center and relevant planning, risk,
  paper, and broker pages between ticket drafting and risk/runbook controls.
- Expose preview status, ticker, decision, suggested quantity, and blocker count
  in automation JSON for agentic desktop review.
- Preserve the same boundary: preview makes no provider calls, writes no rows,
  submits no broker order, and keeps the record command behind manual approval.

## Current Slice: Learning Loop

- Add a read-only learning-loop snapshot contract that joins the active
  MarketRadar plan, paper preview, local paper evidence, validation replay,
  value ledger, and outcome state into one agent-review dossier.
- Render the loop in the Tauri Command Center and relevant paper, backtest,
  validation, journal, and agent pages so operators can see what the platform
  has learned without reconciling three evidence tables.
- Expose loop status, ticker, stage, validation result, outcome, and blocked
  card count in automation JSON for agentic desktop review.
- Preserve the same boundary: the loop makes no provider calls, submits no
  broker order, performs no autonomous strategy update, and writes no database
  rows from browsing the dossier.

## Current Slice: Strategy Review

- Add a read-only strategy-review snapshot contract that turns learning-loop,
  validation, outcome, scenario, and risk-envelope evidence into supervised
  strategy hypotheses.
- Render the review in the Tauri Command Center and relevant trade planner,
  backtest, validation, journal, and agent pages so agents and operators can
  inspect rule-change evidence without mutating strategy.
- Expose review status, ticker, stage, hypothesis counts, blocked hypothesis
  count, and update permission in automation JSON.
- Preserve the same boundary: the review makes no provider calls, submits no
  broker order, performs no autonomous strategy update, and writes no database
  rows from browsing the dossier.

## Current Slice: Trade Monitor

- Add a read-only trade-monitor snapshot contract that joins active MarketRadar
  plan, open paper trade, lifecycle/outcome evidence, risk blockers, alerts,
  trigger rules, and open-order context into one position-watch dossier.
- Render the monitor in the Tauri Command Center and relevant portfolio, risk,
  paper-trading, broker, alerts, journal, and agent pages.
- Expose monitor status, ticker, stage, active paper trade count, blocker count,
  open order count, trigger id, and exit-update permission in automation JSON.
- Preserve the same boundary: the monitor makes no provider calls, submits no
  broker order, performs no autonomous exit/state update, and writes no
  database rows from browsing the dossier.

## Current Slice: Capital Allocation

- Add a read-only capital-allocation snapshot contract that joins portfolio
  equity, cash, buying power, current exposure, portfolio impact, position
  sizing, active paper exposure, open orders, and risk blockers into one
  allocation-review dossier.
- Render the allocation dossier in the Tauri Command Center and relevant
  portfolio, trade-planner, risk, paper-trading, broker, and agent pages.
- Expose allocation status, ticker, suggested notional, buying-power usage,
  blocked-check count, and allocation permission in automation JSON.
- Preserve the same boundary: the allocation review makes no provider calls,
  submits no broker order, performs no autonomous allocation change, and writes
  no database rows from browsing the dossier.

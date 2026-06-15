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

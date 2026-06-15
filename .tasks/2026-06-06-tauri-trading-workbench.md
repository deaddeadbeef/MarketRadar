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

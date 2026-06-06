## Agentic Paper-Trading Manager

Objective: expand MarketRadar toward an intelligent agentic trading tool by adding a zero-execution paper-trade intent manager that reasons over existing decision cards and routes humans to the guarded paper-decision workflow.

Acceptance criteria:
- Agentic paper-trade intents consume stored decision-card evidence only.
- The manager returns specialist rationale, deterministic hard blocks, and an explicit paper-decision command.
- Preview mode makes 0 provider, broker, OpenAI, web, shell, or order-submission calls.
- Any local write remains behind the existing `paper-decision --execute` path.
- Blocked or non-manual-review cards cannot be framed as ready paper entries without surfacing the block.
- Tests prove the no-execution contract and CLI preview payload.

Status:
- Goal created in Codex.
- Worktree created at `C:\Users\fpan1\MarketRadar\.worktrees\agentic-paper-manager` on `codex/agentic-paper-manager`.
- Baseline passed: `python -m pytest tests\unit\test_agent_schemas.py tests\unit\test_portfolio.py tests\unit\test_backtest_replay.py tests\integration\test_paper_trading.py -q`.
- Completed: implementation plan, pure agentic paper-trade intent builder, `agentic-paper-intent` CLI preview, README docs, and tests.
- Validation passed:
  - `python -m pytest tests\unit\test_agentic_paper_trading.py tests\integration\test_validation_cli.py tests\integration\test_paper_trading.py -q`
  - `python -m pytest tests\unit\test_agent_schemas.py tests\unit\test_portfolio.py tests\unit\test_backtest_replay.py tests\integration\test_paper_trading.py -q`
  - `python -m ruff check src tests`
  - `git diff --check`

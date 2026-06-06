# Agentic Paper Trading Manager Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an agentic paper-trading manager that turns existing MarketRadar decision cards into auditable paper-trade intents without broker execution.

**Architecture:** MarketRadar's scanner, candidate packets, decision cards, and policy gates remain the source of truth. The new manager consumes a decision card, emits specialist rationale and deterministic hard blocks, then points to the existing guarded `paper-decision` CLI path for optional local paper-trade writes. Broker execution remains out of scope.

**Tech Stack:** Python 3.11, dataclasses, SQLAlchemy-backed decision-card lookup through existing repositories, pytest, ruff.

---

## Acceptance Criteria

- Agentic paper-trade intents are built only from stored decision-card payloads and explicit CLI inputs.
- Preview payloads expose `external_calls_required=0`, `external_calls_made=0`, `broker_order_submitted=false`, `order_submission_allowed=false`, and `no_execution=true`.
- Specialist rationale includes catalyst, skeptic, market-structure, portfolio, execution-plan, and risk-governor views.
- Cards not in `EligibleForManualBuyReview`, cards with hard blocks, and cards with missing trade-plan fields remain blocked or need override; the manager must not describe them as ready.
- The manager returns both preview and execute commands for the existing `paper-decision` workflow, but does not write paper-trade rows itself.
- CLI tests prove a preview is zero-call and does not insert `paper_trades`.

## File Plan

- Create `src/catalyst_radar/agents/paper_trading.py`
  - `AgenticPaperTradeIntent` dataclass.
  - `build_agentic_paper_trade_intent(...)` pure function.
  - Deterministic specialist summaries and hard-block checks.
- Modify `src/catalyst_radar/cli.py`
  - Add `agentic-paper-intent` preview command.
  - Reuse existing decision-card lookup and paper-decision command semantics.
- Add `tests/unit/test_agentic_paper_trading.py`
  - Unit tests for ready, blocked, and missing-plan decision cards.
- Modify `tests/integration/test_validation_cli.py`
  - Integration test for `agentic-paper-intent --json` preview and no paper-trade writes.
- Modify `README.md`
  - Document the new preview command under the paper-trading workflow.

## Task 1: Pure Intent Builder

**Files:**
- Create: `src/catalyst_radar/agents/paper_trading.py`
- Test: `tests/unit/test_agentic_paper_trading.py`

- [x] Write tests for an eligible manual-review card producing a ready paper intent.
- [x] Write tests for a blocked card producing hard blocks and `requires_override=true`.
- [x] Implement the intent dataclass and pure builder.
- [x] Verify: `python -m pytest tests\unit\test_agentic_paper_trading.py -q`.

## Task 2: CLI Preview Surface

**Files:**
- Modify: `src/catalyst_radar/cli.py`
- Modify: `tests/integration/test_validation_cli.py`

- [x] Add parser arguments for `agentic-paper-intent`.
- [x] Load the decision card by id and `available_at`.
- [x] Return a JSON preview with zero calls and generated `paper-decision` commands.
- [x] Prove preview does not insert a `paper_trades` row.
- [x] Verify: `python -m pytest tests\integration\test_validation_cli.py -k agentic_paper -q`.

## Task 3: Documentation And Validation

**Files:**
- Modify: `README.md`

- [x] Add the command to the paper decision section.
- [x] Run focused tests and ruff.
- [ ] Commit, push, and open a PR.

## Validation Commands

```powershell
C:\Users\fpan1\MarketRadar\.venv\Scripts\python.exe -m pytest tests\unit\test_agentic_paper_trading.py tests\integration\test_validation_cli.py -k "agentic_paper or paper" -q
C:\Users\fpan1\MarketRadar\.venv\Scripts\python.exe -m pytest tests\unit\test_agent_schemas.py tests\unit\test_portfolio.py tests\unit\test_backtest_replay.py tests\integration\test_paper_trading.py -q
C:\Users\fpan1\MarketRadar\.venv\Scripts\python.exe -m ruff check src tests
```

# Agentic Trading Platform Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a read-only agentic trading platform planner that turns stored MarketRadar Decision Cards into supervised paper-trade plans with live broker execution disabled.

**Architecture:** The planner composes the existing agentic paper-trade intent with a deterministic risk and execution-control envelope. The CLI reads only the local validation repository, never writes rows, and returns explicit paper-readiness and live-submission block state.

**Tech Stack:** Python 3.11, SQLAlchemy validation repositories, existing `catalyst-radar` argparse CLI, pytest, ruff.

---

## File Structure

- Create `src/catalyst_radar/trading/__init__.py` to expose the new trading package.
- Create `src/catalyst_radar/trading/platform.py` for the pure planner and JSON payload contract.
- Modify `src/catalyst_radar/cli.py` to add `trading-platform-plan`.
- Create `tests/unit/test_trading_platform.py` for planner behavior.
- Create `tests/integration/test_trading_platform_cli.py` for zero-write CLI behavior.
- Modify `README.md` to document the command in the paper-trading workflow.

## Task 1: Planner Contract

**Files:**
- Create: `src/catalyst_radar/trading/__init__.py`
- Create: `src/catalyst_radar/trading/platform.py`
- Test: `tests/unit/test_trading_platform.py`

- [ ] **Step 1: Write the ready planner test**

```python
def test_builds_ready_supervised_paper_trading_plan() -> None:
    plan = build_trading_platform_plan(
        _decision_card(),
        available_at=AVAILABLE_AT,
        entry_price=100.0,
        config=AppConfig(portfolio_value=25_000.0),
    ).to_payload()

    assert plan["schema_version"] == "agentic-trading-platform-plan-v1"
    assert plan["status"] == "ready_for_paper_trade"
    assert plan["risk_approval"]["approved_for_paper_trade"] is True
    assert plan["risk_approval"]["approved_for_live_submission"] is False
    assert plan["execution_controls"]["broker_order_submitted"] is False
    assert plan["execution_controls"]["order_submission_allowed"] is False
    assert plan["execution_controls"]["external_calls_made"] == 0
    assert plan["execution_controls"]["db_writes_made"] == 0
```

- [ ] **Step 2: Write the blocked planner test**

```python
def test_blocks_trading_plan_when_decision_card_is_not_manual_review_ready() -> None:
    plan = build_trading_platform_plan(
        _decision_card(action_state=ActionState.WARNING.value),
        available_at=AVAILABLE_AT,
        entry_price=100.0,
        config=AppConfig(portfolio_value=25_000.0),
    ).to_payload()

    assert plan["status"] == "blocked"
    assert plan["risk_approval"]["approved_for_paper_trade"] is False
    assert "action_state_not_manual_review_eligible" in plan["risk_approval"]["paper_trade_blocks"]
```

- [ ] **Step 3: Run the focused test to verify failure**

Run: `C:\Users\fpan1\MarketRadar\.venv\Scripts\python.exe -m pytest tests\unit\test_trading_platform.py -q`

Expected: fail because `catalyst_radar.trading.platform` does not exist yet.

- [ ] **Step 4: Implement the planner**

Implement `build_trading_platform_plan(card, available_at, entry_price=None, entry_at=None, override_reason=None, config=None, broker_data_stale=False)`.

The returned payload must include `schema_version`, `status`, `autonomy_level`, `strategy_proposal`, `risk_approval`, `order_intent`, `execution_controls`, `supervision`, `agentic_paper_intent`, `capability_map`, and `next_action`.

- [ ] **Step 5: Run the focused test to verify pass**

Run: `C:\Users\fpan1\MarketRadar\.venv\Scripts\python.exe -m pytest tests\unit\test_trading_platform.py -q`

Expected: pass.

## Task 2: CLI Surface

**Files:**
- Modify: `src/catalyst_radar/cli.py`
- Test: `tests/integration/test_trading_platform_cli.py`

- [ ] **Step 1: Write CLI integration test**

```python
def test_trading_platform_plan_cli_is_zero_call_zero_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'trading-platform.db').as_posix()}"
    monkeypatch.setenv("CATALYST_DATABASE_URL", database_url)
    monkeypatch.setenv("CATALYST_PORTFOLIO_VALUE", "25000")
    assert main(["init-db"]) == 0
    _insert_manual_review_decision_card(database_url)

    assert main([
        "trading-platform-plan",
        "--decision-card-id",
        "card-MSFT",
        "--available-at",
        "2026-05-10T21:05:00+00:00",
        "--entry-price",
        "100",
        "--json",
    ]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "ready_for_paper_trade"
    assert payload["execution_controls"]["external_calls_made"] == 0
    assert payload["execution_controls"]["db_writes_made"] == 0
```

- [ ] **Step 2: Run integration test to verify failure**

Run: `C:\Users\fpan1\MarketRadar\.venv\Scripts\python.exe -m pytest tests\integration\test_trading_platform_cli.py -q`

Expected: fail because the parser command is absent.

- [ ] **Step 3: Add parser and handler**

Add `trading-platform-plan` near `agentic-paper-intent`, load the Decision Card through `ValidationRepository.decision_card_payload`, call `build_trading_platform_plan`, and print JSON using `dashboard_json_default`.

- [ ] **Step 4: Run integration test to verify pass**

Run: `C:\Users\fpan1\MarketRadar\.venv\Scripts\python.exe -m pytest tests\integration\test_trading_platform_cli.py -q`

Expected: pass.

## Task 3: Docs And Verification

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Document the command**

Add this command to the preview-first paper workflow:

```powershell
catalyst-radar trading-platform-plan --decision-card-id <CARD_ID> --available-at <UTC-cutoff> --entry-price <price> --json
```

Explain that it is an agentic platform plan, not a broker route.

- [ ] **Step 2: Run validation**

Run:

```powershell
C:\Users\fpan1\MarketRadar\.venv\Scripts\python.exe -m pytest tests\unit\test_trading_platform.py tests\unit\test_agentic_paper_trading.py tests\integration\test_trading_platform_cli.py tests\integration\test_validation_cli.py tests\integration\test_paper_trading.py -q
C:\Users\fpan1\MarketRadar\.venv\Scripts\python.exe -m ruff check src tests
git diff --check
```

Expected: all pass.

- [ ] **Step 3: Commit**

```powershell
git add .tasks\2026-06-06-trading-platform-foundation.md docs\superpowers\plans\2026-06-06-agentic-trading-platform-foundation.md src\catalyst_radar\trading\__init__.py src\catalyst_radar\trading\platform.py src\catalyst_radar\cli.py tests\unit\test_trading_platform.py tests\integration\test_trading_platform_cli.py README.md
git commit -m "feat: add agentic trading platform plan"
```

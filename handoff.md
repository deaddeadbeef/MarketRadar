# MarketRadar Handoff

Last updated: 2026-05-16 11:29:15 +08:00

## Current Objective

Keep polishing MarketRadar until it becomes genuinely useful to the user, not just technically complete. The immediate operational goal is to get the product out of local/demo-only mode without forcing the user to buy or obtain a Polygon API key.

The user confirmed:

- They do not have a Polygon API key.
- They were confused by `CATALYST_SEC_USER_AGENT`.
- They filled `CATALYST_SEC_USER_AGENT` in `.env.local`.
- Polygon should be treated as optional for now.

## Key Decision

The correct near-term live path is **SEC-only catalyst ingestion with local CSV market data**:

```powershell
CATALYST_DAILY_MARKET_PROVIDER=csv
CATALYST_DAILY_PROVIDER=csv
CATALYST_DAILY_EVENT_PROVIDER=sec
CATALYST_SEC_ENABLE_LIVE=1
CATALYST_SEC_USER_AGENT=<redacted local contact string>
CATALYST_POLYGON_API_KEY=
```

`CATALYST_SEC_USER_AGENT` is not a secret. It is a SEC-required identifying contact string for EDGAR requests, such as `MarketRadar user@example.com`. Do not paste the user's actual value into chat or checked-in docs.

`CATALYST_POLYGON_API_KEY` must remain optional unless the operator explicitly switches `CATALYST_DAILY_MARKET_PROVIDER=polygon`.

## Current Repository State

This work was merged to `main` through PR #176 using rebase merge. This
handoff may also have later docs-only refresh commits, so use `git log -1` for
the exact current SHA instead of relying on a hard-coded commit in this file.

Current expected branch:

```text
main
```

Files changed by PR #176:

- `scripts/prepare-live-env.ps1`
- `scripts/open-live-env.ps1`
- `scripts/check-live-activation.ps1`
- `scripts/run-first-live-smoke.ps1`
- `scripts/run-worker-once.ps1`
- `src/catalyst_radar/dashboard/data.py`
- `README.md`
- `docs/runbooks/radar-run.md`
- `tests/integration/test_local_scripts.py`
- `tests/integration/test_dashboard_data.py`
- `handoff.md`

## What Changed In PR #176

Activation and helper behavior was changed from "Polygon plus SEC is required" to "CSV plus SEC is the safe first useful live mode":

- `scripts/prepare-live-env.ps1`
  - Now writes `CATALYST_DAILY_MARKET_PROVIDER=csv`.
  - Now writes `CATALYST_DAILY_PROVIDER=csv`.
  - Still enables live SEC with low caps.
  - No longer asks for a Polygon key as a manual required value.

- `scripts/open-live-env.ps1`
  - Now tells the operator to fill only `CATALYST_SEC_USER_AGENT`.
  - States Polygon is optional and only needed if the market provider is switched to Polygon.

- `scripts/check-live-activation.ps1`, `scripts/run-first-live-smoke.ps1`, and `scripts/run-worker-once.ps1`
  - Polygon guidance now says it is only needed when `CATALYST_DAILY_MARKET_PROVIDER=polygon`.

- `scripts/run-first-live-smoke.ps1`
  - Plan-only mode remains zero-call.
  - `-Execute` now skips Polygon universe seeding unless the call plan says the market provider is Polygon.
  - The SEC-only smoke path can run a capped radar cycle without a Polygon key.

- `src/catalyst_radar/dashboard/data.py`
  - Market activation missing-env logic no longer forces Polygon.
  - The live data minimum env block now uses CSV for market data.
  - `.env.local` activation status treats `CATALYST_POLYGON_API_KEY` as required only if Polygon is configured.
  - Operator steps and call-budget rows account for optional Polygon seeding.
  - Market preflight wording no longer tells the user to switch to Polygon for the first useful SEC-only smoke.

- `README.md` and `docs/runbooks/radar-run.md`
  - Updated to document SEC-only first live smoke.
  - Polygon is described as a later optional broad-market upgrade.

## Verification Already Run

Focused tests passed:

```powershell
py -m pytest tests\integration\test_local_scripts.py tests\integration\test_dashboard_data.py::test_activation_summary_payload_calls_out_fixture_mode tests\integration\test_dashboard_data.py::test_live_activation_plan_payload_separates_optional_gates_from_blockers tests\integration\test_dashboard_data.py::test_live_data_activation_contract_gives_exact_safe_next_steps tests\integration\test_dashboard_data.py::test_live_data_activation_contract_never_leaks_configured_secrets tests\integration\test_dashboard_data.py::test_dotenv_activation_status_reports_missing_file tests\integration\test_dashboard_data.py::test_dotenv_activation_status_reports_restart_required_without_leaking_values tests\integration\test_dashboard_data.py::test_dotenv_activation_status_names_missing_required_values tests\integration\test_dashboard_data.py::test_dotenv_activation_status_reports_loaded_values tests\integration\test_runbook_docs.py -q
```

Result:

```text
24 passed
```

Broader dashboard/local-script/docs validation also passed:

```powershell
py -m pytest tests\integration\test_local_scripts.py tests\integration\test_dashboard_data.py tests\integration\test_runbook_docs.py -q
```

Result:

```text
passed; no failures
```

This broader slice was rerun after the handoff was added. The first rerun attempt hit the 120-second tool timeout while Python was flushing output, so it was discarded. The standalone rerun with a longer timeout completed successfully.

The local runtime was restarted after the `.env.local` update, after code changes, and again after PR #176 was merged to `main`:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\restart-local.ps1
```

Current expected local endpoints:

```text
Dashboard: http://127.0.0.1:8514
API:       https://127.0.0.1:8443
```

## Latest Local Runtime Validation

After the user filled `CATALYST_SEC_USER_AGENT`, `scripts\prepare-live-env.ps1 -Quiet` was run. A redacted env-state check showed:

```text
CATALYST_DAILY_MARKET_PROVIDER=csv
CATALYST_DAILY_PROVIDER=csv
CATALYST_POLYGON_API_KEY=empty_or_placeholder
CATALYST_DAILY_EVENT_PROVIDER=sec
CATALYST_SEC_ENABLE_LIVE=1
CATALYST_SEC_USER_AGENT=set_redacted_length_21
CATALYST_SEC_DAILY_MAX_TICKERS=5
```

This is now valid for the SEC-only path.

`scripts\check-live-activation.ps1` reported:

```text
Live activation: ready
Headline: Live data activation inputs are configured.
Next: Inspect the call plan, skip Polygon seeding unless configured, then run one capped cycle.
.env.local: loaded; loaded=8; missing=0; restart_required=0
External calls made: 0
```

`scripts\run-first-live-smoke.ps1` in plan-only mode reported:

```text
Live activation: ready
Radar call plan: local_or_dry_run_only; max_external_calls=0
Plan only: no provider calls were made.
No live provider calls are currently planned; fix call-plan expected gates before expecting SEC data.
Polygon universe seeding will be skipped unless the market provider is polygon.
Execute budget: polygon_universe_seed_pages=0; radar_external_calls_max=0
External calls made: 0
```

`scripts\run-first-live-smoke.ps1 -Execute` was run once. It skipped Polygon universe seeding because the market provider is CSV, made zero external calls, and created a local radar run.

The latest status after the pre-merge execute was:

```text
API: ok; build=564dc3f7dc72; version=0.1.0
Readiness: research_only; investable=False; next=Clear 2 setup blockers: Configure a live daily market provider and keep batch/rate limits enabled; Fix the first skipped/failed upstream step before treating candidates as complete.
Latest run: success; required=6/7; action_needed=0; optional_gates=4; audit_rows=5
Live activation: ready; missing=0
Call plan: local_or_dry_run_only; will_call_external=False; max_external_calls=0
Telemetry: ready; events=25; attention=0; guarded=0
Telemetry coverage: ready; required_ready=3/3; missing_required=0
External calls made: 0
```

The latest run was technically successful but incomplete on the required path:

```text
status=success
required_step_count=7
required_completed_count=6
run_path_status=incomplete
skipped required step=event_ingest
reason=no_sec_cik_targets
meaning=No active securities had CIK metadata for SEC submission checks.
operator_action=Add CIK metadata before SEC submission checks can run.
```

That means the old Polygon-key blocker is gone, but the next usefulness blocker is real product data shape: the active local securities do not expose CIK metadata, so the SEC live adapter has no submission targets.

After PR #176 was merged and services were restarted from `main`, `scripts\market-radar-status.ps1` reported activation-ready state. The exact build SHA will change if this handoff receives docs-only cleanup commits; the important stable fields are:

```text
API: ok; version=0.1.0
Readiness: research_only; investable=False; next=Clear 2 setup blockers: Configure a live daily market provider and keep batch/rate limits enabled; Fix the first skipped/failed upstream step before treating candidates as complete.
Latest run: success; required=6/7; action_needed=0; optional_gates=4; audit_rows=5
Live activation: ready; missing=0
Call plan: local_or_dry_run_only; will_call_external=False; max_external_calls=0
Telemetry: ready; events=25; attention=0; guarded=0
Telemetry coverage: ready; required_ready=3/3; missing_required=0
External calls made: 0
```

Post-merge plan-only smoke from `main` reported:

```text
Live activation: ready
Radar call plan: local_or_dry_run_only; max_external_calls=0
Plan only: no provider calls were made.
No live provider calls are currently planned; fix call-plan expected gates before expecting SEC data.
Polygon universe seeding will be skipped unless the market provider is polygon.
Execute budget: polygon_universe_seed_pages=0; radar_external_calls_max=0
External calls made: 0
```

## Local Secret State

Do not print or commit `.env.local`.

Last redacted inspection after the user said `Done` showed:

```text
CATALYST_POLYGON_API_KEY=empty_or_placeholder
CATALYST_SEC_USER_AGENT=set_redacted_length_21
```

This is acceptable for the SEC-only path.

## Refresh Commands

Run these in order if resuming from a fresh shell or after changing `.env.local`:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\prepare-live-env.ps1 -Quiet
powershell -ExecutionPolicy Bypass -File scripts\restart-local.ps1
powershell -ExecutionPolicy Bypass -File scripts\check-live-activation.ps1
powershell -ExecutionPolicy Bypass -File scripts\run-first-live-smoke.ps1
```

Only if the plan-only smoke matches intent and the max external call count is acceptable:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\run-first-live-smoke.ps1 -Execute
```

Then inspect:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\market-radar-status.ps1
curl.exe --insecure --fail --silent --show-error --request GET https://127.0.0.1:8443/api/radar/readiness
curl.exe --insecure --fail --silent --show-error --request GET https://127.0.0.1:8443/api/radar/runs/latest
```

## Provider-Call Safety Rules

Do not make provider calls until:

1. Local services have restarted after `.env.local` changes.
2. `scripts/check-live-activation.ps1` is clean or only reports non-blocking optional Polygon state.
3. `scripts/run-first-live-smoke.ps1` plan-only mode reports the intended call budget.

Expected first useful call budget without Polygon:

- Polygon universe seed: `0`
- Polygon market data: `0`
- SEC submissions: up to `CATALYST_SEC_DAILY_MAX_TICKERS`, currently intended as `5`
- Schwab: `0`
- OpenAI: `0`

If the call plan is blocked, do not use `-Execute`. Inspect `/api/radar/runs/call-plan` first.

## Known Product Limitations After This Change

This change makes live SEC catalyst ingestion usable without Polygon, but it does not make the product fully investable.

Remaining limitations:

- Market data stays local CSV until another live market source is configured.
- Investment readiness should remain `research_only` if market data is fixture/stale.
- SEC polling requires active securities with CIK metadata. If the local CSV securities file lacks CIKs for useful tickers, SEC live calls may be capped but find no targets.
- Polygon remains the existing broad-market live data adapter, but the user does not currently have a key.
- OpenAI/LLM review remains disabled/dry-run by design.
- Schwab is read-only context only; order submission remains disabled.

## Next Useful Product Slice

The next change should focus on CIK target coverage, not Polygon:

- Add support for optional CIK metadata in the local securities source, likely by accepting columns such as `cik`, `cik_str`, or `central_index_key` from `data\sample\securities.csv`.
- Ensure `src\catalyst_radar\connectors\csv_market.py` carries those optional columns into `Security.metadata`.
- Add a small watchlist of active real tickers with known CIKs so SEC submissions can actually be called in capped live mode.
- Update call-plan/readiness tests so SEC-only activation can distinguish "configured but no CIK targets" from "ready to call SEC".
- Re-run plan-only first, then execute only when the SEC call budget is low and explicit.

Relevant code paths:

```text
src\catalyst_radar\jobs\tasks.py::_security_cik
src\catalyst_radar\dashboard\data.py
src\catalyst_radar\connectors\csv_market.py
data\sample\securities.csv
```

## How To Resume If Interrupted

1. Check branch and worktree:

   ```powershell
   git status --short --branch
   ```

2. Re-run focused tests:

   ```powershell
   py -m pytest tests\integration\test_local_scripts.py tests\integration\test_dashboard_data.py::test_live_data_activation_contract_gives_exact_safe_next_steps tests\integration\test_runbook_docs.py -q
   ```

3. Run broader validation before a future PR:

   ```powershell
   py -m pytest tests\integration\test_local_scripts.py tests\integration\test_dashboard_data.py tests\integration\test_runbook_docs.py -q
   ```

4. Restart services and run the zero-call/live smoke sequence from the "Refresh Commands" section.

5. For future changes, create a new feature/docs branch and use PR plus rebase merge. PR #176 is already merged.

## PR And Merge Expectations

The repo has been using protected `main` with PRs and rebase merges. Do not push directly to `main`.

PR #176, `Make first live activation SEC-only`, has already been merged. Later docs-only handoff cleanup PRs may exist. The next product PR should be the CIK target coverage slice described above, unless the user redirects.

## Do Not Do

- Do not ask the user for a Polygon key again unless they explicitly choose Polygon.
- Do not paste `.env.local` contents into chat.
- Do not run `scripts/run-first-live-smoke.ps1 -Execute` if plan-only mode is blocked or unexpectedly high.
- Do not mark the active goal complete; the product is improving but not fully useful/investable yet.
- Do not update the checked-in PR ledger just to include a just-merged ledger PR, because that creates a self-referential loop.

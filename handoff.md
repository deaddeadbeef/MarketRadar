# MarketRadar Handoff

Last updated: 2026-05-20 10:22:09 +08:00

## Latest Manual CSV Fill Progress

Goal alignment:

- The remaining full-market blocker is filling
  `data\local\manual-bars-2026-05-15.csv` with real scan-date OHLCV/VWAP
  values.
- This slice makes that manual work measurable. The import preview, repair-plan
  preview, API payload, and quick status now show how many manual rows are
  complete, partially filled, or empty.
- This makes 0 Polygon/Massive, SEC, Schwab, OpenAI, broker, order, or provider
  calls.

Fix in this slice:

- Manual CSV validation now computes `fill_progress`:
  - `complete_rows`: all required OHLCV/VWAP fields filled;
  - `partial_rows`: at least one required OHLCV/VWAP field filled, but not all;
  - `empty_rows`: no required OHLCV/VWAP fields filled;
  - `filled_rows`: complete plus partial.
- `market-bars import` text output prints `fill_progress=...`.
- `market-bars repair-plan` text output prints local template fill progress.
- `scripts\market-radar-status.ps1 -Quick` prints
  `- local template fill progress: ...`.
- API JSON responses expose the same `fill_progress` object.

Live zero-call observation:

```text
manual_market_bars_import status=invalid rows=523 tickers=523 active=12613 latest_bar=2026-05-15 expected_as_of=2026-05-15 executed=false external_calls=0
coverage=bars_at_expected=523 existing=12090 after_import=12613 missing=0 scope=active_universe
fill_progress=complete=0 partial=0 empty=523 filled=0
invalid=rows=523 blank_required=3138 invalid_numeric=0
```

Quick status observation:

```text
- local template preview: status=invalid; rows=523; invalid_rows=523; blank_required=3138; missing_after_import=0; external_calls=0
- local template fill progress: complete=0; partial=0; empty=523; filled=0
- local template blank fields: close=523, high=523, low=523, open=523, volume=523, vwap=523
External calls made: 0
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_provider_ingest_cli.py::test_market_bars_repair_plan_previews_existing_local_template tests\integration\test_provider_ingest_cli.py::test_market_bars_import_rejects_blank_numeric_fields tests\integration\test_api_routes.py::test_post_radar_market_bars_template_and_import_use_database_universe tests\integration\test_local_scripts.py::test_market_radar_status_script_is_zero_external_call_sitrep -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\market\manual_bars.py src\catalyst_radar\cli.py tests\integration\test_provider_ingest_cli.py tests\integration\test_api_routes.py tests\integration\test_local_scripts.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli market-bars import --daily-bars data\local\manual-bars-2026-05-15.csv --expected-as-of 2026-05-15
powershell -ExecutionPolicy Bypass -File .\scripts\market-radar-status.ps1 -Quick
```

Results:

- Focused pytest passed.
- Ruff passed.
- `git diff --check` passed.
- Live import preview and quick status both reported `external_calls=0` and
  fill progress `complete=0 partial=0 empty=523 filled=0`.

Next useful product action:

- Fill OHLCV/VWAP values in the manual CSV. Re-run import preview; the progress
  line should move rows from `empty` to `partial` or `complete`.
- The goal remains blocked until `complete=523`, the preview is `ready`, and the
  local import is executed, or until the user explicitly approves the guarded
  Polygon/Massive grouped-daily fill.

## Latest Manual Template Overwrite Guard

Goal alignment:

- The active blocker is still `data\local\manual-bars-2026-05-15.csv`. Since
  that file is the manual path to unblock the full-market priced-in scan, the
  system must not accidentally destroy operator-entered OHLCV/VWAP values.
- This slice adds an overwrite guard to manual market-bar template generation.
  It directly protects the artifact that must be filled before the full-market
  priced-in answer can become trustworthy.
- This makes 0 Polygon/Massive, SEC, Schwab, OpenAI, broker, order, or provider
  calls.

Fix in this slice:

- `write_manual_market_bars_template(..., overwrite=False)` now scans an
  existing output CSV before writing.
- If the existing file has any filled required OHLCV/VWAP values, template
  generation refuses to overwrite it.
- The CLI exposes `market-bars template --overwrite` for deliberate replacement
  after backup/confirmation.
- The API request body supports `"overwrite": true` for the same explicit
  replacement path.
- Blank generated templates can still be regenerated without `--overwrite`,
  which preserves the current local workflow.

Live zero-call guard check:

```text
manual market bars failed: refusing to overwrite manual market-bar template with 1 row(s) containing filled OHLCV/VWAP values: C:\Users\fpan1\AppData\Local\Temp\market-radar-filled-template-guard.csv; rerun with --overwrite only after backing up or confirming the filled values are no longer needed
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_provider_ingest_cli.py::test_market_bars_template_refuses_to_overwrite_filled_manual_rows tests\integration\test_provider_ingest_cli.py::test_market_bars_missing_only_template_import_counts_existing_bars tests\integration\test_api_routes.py::test_post_radar_market_bars_template_and_import_use_database_universe tests\integration\test_api_routes.py::test_post_radar_market_bars_template_and_import_can_scope_to_stocks -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\market\manual_bars.py src\catalyst_radar\cli.py src\catalyst_radar\api\routes\radar.py tests\integration\test_provider_ingest_cli.py tests\integration\test_api_routes.py
git diff --check
powershell -ExecutionPolicy Bypass -File .\scripts\market-radar-status.ps1 -Quick
```

Results:

- Focused pytest passed.
- Ruff passed.
- `git diff --check` passed.
- Live temp-file guard check refused to overwrite one filled row.
- Quick status still reports `bars=12090/12613`, `missing=523`, and
  `External calls made: 0`.

Next useful product action:

- Fill OHLCV/VWAP in `data\local\manual-bars-2026-05-15.csv`, then run the
  import preview again.
- If the template must be regenerated after any rows are filled, back it up
  first and use `--overwrite` only deliberately.
- Do not run the guarded Polygon/Massive grouped-daily fill without explicit
  user approval.

## Latest Local Manual Template Regeneration

Goal alignment:

- The active blocker is still the 523 missing scan-date market bars.
- Provider fill still requires explicit approval, so the useful zero-call work
  is making the local manual repair path as easy and safe as possible.
- The existing `data\local\manual-bars-2026-05-15.csv` was inspected before
  regeneration. It had 523 rows, no `name` column, and zero rows with any
  required OHLCV/VWAP field filled, so regenerating it did not overwrite
  operator-entered bar values.
- `data\local\` is ignored by git, so the regenerated CSV is a local workspace
  artifact and not part of the PR.
- This made 0 Polygon/Massive, SEC, Schwab, OpenAI, broker, order, or provider
  calls.

Local artifact state after regeneration:

```text
file: data\local\manual-bars-2026-05-15.csv
rows: 523
has_name_column: true
filled_required_rows: 0
first_row: AACO | CS | Abony Acquisition Corp. I Class A Ordinary Share
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m catalyst_radar.cli market-bars template --expected-as-of 2026-05-15 --out data\local\manual-bars-2026-05-15.csv --missing-only
.\.venv\Scripts\python.exe -m catalyst_radar.cli market-bars import --daily-bars data\local\manual-bars-2026-05-15.csv --expected-as-of 2026-05-15
powershell -ExecutionPolicy Bypass -File .\scripts\market-radar-status.ps1 -Quick
```

Results:

- Template generation reported `rows=523`, `external_calls=0`, and the new
  `Rows include security names` next action.
- Import preview still exits non-zero with `status=invalid`, which is expected
  until OHLCV/VWAP values are filled. It reported `blank_required=3138` and
  `external_calls=0`.
- Quick status still reports `bars=12090/12613`, `missing=523`, and
  `External calls made: 0`.

Next useful product action:

- Fill OHLCV/VWAP in `data\local\manual-bars-2026-05-15.csv`, then run the
  import preview again.
- Do not regenerate this file again without rechecking for filled required
  values first.
- Do not run the guarded Polygon/Massive grouped-daily fill without explicit
  user approval.

## Latest Manual Bar Template Name Column

Goal alignment:

- The active completion blocker is still the missing scan-date market bars.
  Provider fill requires explicit approval, so the useful zero-call path is to
  make the manual repair artifact easier to fill and audit.
- This slice adds security names to generated manual market-bar templates.
  The name column helps the operator identify ambiguous tickers, units,
  warrants, ADRs, preferred shares, funds, and common stocks while filling real
  OHLCV/VWAP values.
- This does not change import semantics. Imports still read the same required
  daily-bar fields; the new `name` column is descriptive.
- This makes 0 Polygon/Massive, SEC, Schwab, OpenAI, broker, order, or provider
  calls.

Fix in this slice:

- `MANUAL_BAR_COLUMNS` now includes `name` after `security_type`.
- `write_manual_market_bars_template(...)` writes the active security name into
  each generated row.
- The template payload now exposes `template_columns`.
- Template `next_action` now tells the operator that rows include names.
- Tests cover template row names, template payload columns, and unchanged import
  behavior.

Live zero-call observation:

```text
manual_market_bars_template status=ready rows=523 scope=missing_as_of_bars expected_as_of=2026-05-15 external_calls=0
next_action=Rows include security names and are sorted stock-like first. Fill open, high, low, close, volume, and vwap for every row, then preview the import before executing.

ticker: AACO
security_type: CS
name: Abony Acquisition Corp. I Class A Ordinary Share

ticker: ADAC
security_type: CS
name: American Drive Acquisition Company Class A Ordinary Shares

ticker: ADXN
security_type: ADRC
name: Addex Therapeutics Ltd American Depositary Shares
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_provider_ingest_cli.py::test_market_bars_template_sorts_stock_like_rows_first tests\integration\test_provider_ingest_cli.py::test_market_bars_stocks_only_template_and_import_scope tests\integration\test_provider_ingest_cli.py::test_market_bars_repair_plan_previews_existing_local_template tests\integration\test_api_routes.py::test_post_radar_market_bars_template_and_import_can_scope_to_stocks -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\market\manual_bars.py tests\integration\test_provider_ingest_cli.py tests\integration\test_api_routes.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli market-bars template --expected-as-of 2026-05-15 --out $env:TEMP\market-radar-manual-bars-name-check.csv --missing-only
```

Results:

- Focused pytest passed.
- Ruff passed.
- `git diff --check` passed.
- The live generated template had 523 rows, `external_calls=0`, and populated
  `name` values for the first missing rows.

Next useful product action after merge:

- Regenerate `data\local\manual-bars-2026-05-15.csv` if the operator wants the
  checked-in local repair template to include the new `name` column, then fill
  OHLCV/VWAP values and preview import.
- Do not overwrite a partially filled operator CSV without checking first.
- The actual completion blocker remains unchanged: real scan-date bars are
  still required, manually or through explicit approval for the guarded
  Polygon/Massive grouped-daily fill.

## Latest Goal Completion Audit

Objective restated as concrete deliverables:

- MarketRadar must scan the full active market, not only a handpicked watchlist.
- It must combine price reaction with market-emotion evidence and answer:
  "has price fully matched market expectations?"
- CLI/API surfaces must expose that answer, the scan scope, evidence gaps,
  safety boundaries, and next commands.
- Dashboard/TUI surfaces must make the same answer understandable for a human
  operator.
- The system must not make Polygon/Massive, SEC, Schwab, OpenAI, broker, or
  order calls while browsing, planning, auditing, or rendering. Provider calls
  require an explicit command and operator approval.

Prompt-to-artifact checklist:

- Full active market scan:
  - Evidence: `priced-in-answer --json` reports
    `full_scan.active_securities=12613`, `ranked_rows=12087`,
    `unscanned_rows=526`, and `scan_scope.mode=full_scan`.
  - Status: incomplete. The scan is broad, but 523 active securities still lack
    scan-date price reaction.
- Price/emotion answer:
  - Evidence: `priced-in-answer --json` answers
    `Full-market priced-in answer is not ready: 523 row(s) still lack scan-date
    price reaction.`
  - Status: blocked, not complete.
- CLI/API usefulness:
  - Evidence: `priced-in-answer --json`, `market-bars repair-plan`, and
    `scripts\market-radar-status.ps1 -Quick` expose the blocker, next command,
    sample tickers, security-type breakdown, and zero-call boundary.
  - Status: useful for diagnosis and repair planning, but the final priced-in
    answer is blocked by missing bars.
- Dashboard/TUI usefulness:
  - Evidence: `dashboard-tui --once --scan-mode all --page overview` and
    `dashboard-tui --once --page ops` render the full-scan audit, decision
    readiness, and missing-bar type summary with `External calls made: 0`.
  - Status: useful for human review, but it correctly shows a blocked answer.
- Provider/broker/agent safety:
  - Evidence: quick status reports `External calls made: 0`; repair plan says
    the provider fill command is one call and requires explicit approval.
  - Status: aligned. Do not run provider fill without explicit approval.
- Completion gate:
  - Evidence: quick status reports `Full-market next: bars=12090/12613;
    missing=523`; local template preview is `invalid` with
    `blank_required=3138`.
  - Status: not achieved. The blocking artifact is
    `data\local\manual-bars-2026-05-15.csv` or the guarded provider fill.

Current audit conclusion:

- Do not mark the active goal complete.
- More dashboard polish, source enrichment, Schwab work, or agent work would be
  drift unless it directly helps clear or explain the missing scan-date market
  bars.
- The next real completion action is data acquisition:
  - fill/import `data\local\manual-bars-2026-05-15.csv`; or
  - get explicit user approval to run the one-call Polygon/Massive grouped
    daily fill.
- Do not run this command without explicit approval:

```powershell
.\.venv\Scripts\python.exe -m catalyst_radar.cli ingest-polygon grouped-daily --date 2026-05-15 --confirm-external-call
```

## Latest Dashboard Missing-Bar Type Visibility

Goal alignment:

- The CLI/API now show the full-market missing-bar type breakdown. The
  dashboard is a human operator surface, so it also needs to explain why the
  full-market priced-in answer is still blocked.
- This slice keeps the dashboard work narrow: no layout redesign, no provider
  execution, and no new source workflow. It only renders the same market-bar
  blocker shape already present in the priced-in audit diagnostic.
- This makes 0 Polygon/Massive, SEC, Schwab, OpenAI, broker, order, or provider
  calls while rendering.

Fix in this slice:

- Overview now renders a `Missing bar types:` line when the priced-in audit has
  missing market-bar diagnostics.
- Ops now renders the same `Missing bar types:` line above the source-gap
  table, so the operational blocker and the source workflow sit together.
- A small TUI helper formats the diagnostic as:
  `523 missing scan-date bars; types ADRC:8, CS:123, ...; company-like 131; ...`.
- Tests cover the helper output and keep the full-scan TUI smoke green.

Live zero-call observations:

```text
Page: overview | View: Full scan | Answer: attention ready=false | Trade status: research only | Trade safe: False | External calls made: 0
Full scan audit: attention; ranked 12087/12613; bars 12090/12613; sources 1/6; stock bars 5521/5652 missing 131
Missing bar types: 523 missing scan-date bars; types ADRC:8, CS:123, ETF:1, ETV:1, FUND:2, PFD:14, RIGHT:47, SP:6, UN...
Decision readiness: 10 row(s) look decision-ready inside the scanned subset, but 523 market-bar row(s) are missing from the full scan.
```

```text
Page: ops | View: Full scan | Answer: attention ready=false | Trade status: research only | Trade safe: False | External calls made: 0
Operations
Missing bar types: 523 missing scan-date bars; types ADRC:8, CS:123, ETF:1, ETV:1, FUND:2, PFD:14, RIGHT:47, SP:6, UN...
Priced-in Source Gaps
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_tui_once_can_show_full_scan_mode tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_market_bar_missing_type_summary_is_human_readable -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\tui.py tests\integration\test_dashboard_demo_seed_cli.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli dashboard-tui --once --scan-mode all --page overview
.\.venv\Scripts\python.exe -m catalyst_radar.cli dashboard-tui --once --page ops
```

Results:

- Focused TUI tests passed.
- Ruff passed.
- `git diff --check` passed.
- Live Overview and Ops TUI renders showed `External calls made: 0` and the new
  `Missing bar types:` line.

Next useful product action after merge:

- The UI now explains the full-market bar blocker in both CLI/API/status and
  dashboard surfaces.
- The blocker itself is unchanged: fill/import
  `data\local\manual-bars-2026-05-15.csv`, or explicitly approve the one-call
  Polygon/Massive grouped-daily fill.

## Latest Full-Market Missing-Bar Type Breakdown

Goal alignment:

- The active goal is still the full-market priced-in scan. The current blocker
  is not dashboard layout or new source enrichment; it is incomplete scan-date
  market bars for the active universe.
- The prior slice proved that all 523 missing full-market bars have no exact
  local daily-bar history. This slice makes the blocker easier to understand
  by showing which instrument types those 523 rows belong to.
- This is useful because "full scan" now has an operational shape: the missing
  rows are mostly units, warrants, common stocks, and rights. The system should
  not silently narrow the scan, but the operator can now see what the full scan
  still needs.
- This makes 0 Polygon/Massive, SEC, Schwab, OpenAI, broker, order, or provider
  calls.

Fix in this slice:

- `manual_market_bars_repair_plan(...)` now includes
  `missing_security_type_counts` in the repair-plan payload.
- The text CLI prints `missing_security_types=...`.
- `scripts\market-radar-status.ps1 -Quick` prints
  `- missing security types: ...`.
- The API route inherits the same payload through
  `POST /api/radar/market-bars/repair-plan`.
- Tests cover CLI payload/output, API payload, and quick-status script text.

Live zero-call observation:

```text
manual_market_bars_repair_plan status=attention scope=active_universe expected_as_of=2026-05-15 active=12613 existing=12090 missing=523 external_calls=0
missing_security_types=ADRC:8,CS:123,ETF:1,ETV:1,FUND:2,PFD:14,RIGHT:47,SP:6,UNIT:176,WARRANT:145
local_bar_history=missing_with_history=0 missing_without_history=523
provider_option=status=ready_for_approval external_calls=1 key_configured=true command=catalyst-radar ingest-polygon grouped-daily --date 2026-05-15 --confirm-external-call
```

Quick status observation:

```text
Full-market next: bars=12090/12613; missing=523; command=catalyst-radar market-bars template --expected-as-of 2026-05-15 --out data\local\manual-bars-2026-05-15.csv --missing-only
- local bar history: missing_with_history=0; missing_without_history=523
- missing security types: ADRC=8, CS=123, ETF=1, ETV=1, FUND=2, PFD=14, RIGHT=47, SP=6, UNIT=176, WARRANT=145
External calls made: 0
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_provider_ingest_cli.py::test_market_bars_repair_plan_previews_existing_local_template tests\integration\test_provider_ingest_cli.py::test_market_bars_repair_plan_reports_manual_and_guarded_provider_paths tests\integration\test_local_scripts.py::test_market_radar_status_script_is_zero_external_call_sitrep tests\integration\test_api_routes.py::test_post_radar_market_bars_template_and_import_can_scope_to_stocks -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\market\manual_bars.py src\catalyst_radar\cli.py tests\integration\test_provider_ingest_cli.py tests\integration\test_local_scripts.py tests\integration\test_api_routes.py
powershell -NoProfile -Command '& { $null = [scriptblock]::Create((Get-Content -Raw .\scripts\market-radar-status.ps1)); "powershell syntax ok" }'
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli market-bars repair-plan --expected-as-of 2026-05-15
.\.venv\Scripts\python.exe -m catalyst_radar.cli market-bars repair-plan --expected-as-of 2026-05-15 --json
powershell -ExecutionPolicy Bypass -File .\scripts\market-radar-status.ps1 -Quick
```

Results:

- Focused pytest passed.
- Ruff passed.
- PowerShell syntax parse passed.
- `git diff --check` passed.
- Repair-plan text, repair-plan JSON, and quick status all reported
  `external_calls=0` and the same missing security-type breakdown.

Next useful product action after merge:

- The blocker is now transparent but unchanged: to complete the full-market
  priced-in scan, fill/import `data\local\manual-bars-2026-05-15.csv` with real
  OHLCV/VWAP values, or explicitly approve the one-call Polygon/Massive grouped
  daily fill.
- Do not run the provider command without explicit user approval.

## Latest Drift Check And Local Bar History Diagnostic

Goal alignment:

- The active goal is still the full-market priced-in scan: scan the active
  market, compare price reaction with market-emotion evidence, and identify
  stocks where price has not yet matched expectations.
- The last several PRs stayed aligned by blocking answer/source/provider work
  while scan-date market bars are incomplete. That is the right product
  boundary: without full scan-date price reaction, the system can produce a
  browseable queue but not a trustworthy full-market answer.
- The current drift risk is adding dashboard polish, SEC/Schwab enrichment, or
  agent/provider features before the full-market bar blocker is cleared.
- This slice remains useful and narrow: it tells the operator whether the 523
  missing full-market bars can be recovered from existing local bar history.
  The answer is no: all 523 missing tickers have no exact local daily-bar
  history in the database.
- This makes 0 Polygon/Massive, SEC, Schwab, OpenAI, broker, order, or provider
  calls.

Fix in this slice:

- `manual_market_bars_repair_plan(...)` now splits missing scan-date tickers
  into:
  - `missing_with_local_history_*`;
  - `missing_without_local_history_*`.
- The text CLI now prints:
  - `local_bar_history=missing_with_history=... missing_without_history=...`;
  - a sample of missing tickers with no local bar history.
- `scripts\market-radar-status.ps1 -Quick` now includes the same local-history
  diagnostic in the fast full-market status block.
- Tests cover the new payload fields, text CLI output, and quick-status script
  strings.

Live zero-call observation:

```text
Market Radar quick status
API: ok; build=fabe2da7200d; version=0.1.0
Global readiness: research_only; investable=False
Full-market next: bars=12090/12613; missing=523; command=catalyst-radar market-bars template --expected-as-of 2026-05-15 --out data\local\manual-bars-2026-05-15.csv --missing-only
Fast market-bar repair: status=attention; scope=active_universe; active=12613; existing=12090; missing=523; external_calls=0
- local bar history: missing_with_history=0; missing_without_history=523
- missing without local history: AACBR, AACBU, AACIW, AACO, AACOU, AACOW, ACAAU, ACAAW, ADAC, ADXN, AEAQ, AEAQU
- provider option: status=ready_for_approval; external_calls=1; command=catalyst-radar ingest-polygon grouped-daily --date 2026-05-15 --confirm-external-call
- provider boundary: This repair plan makes 0 provider calls. The provider command makes one Polygon/Massive grouped-daily request and must only be run after explicit operator approval.
- local template preview: status=invalid; rows=523; invalid_rows=523; blank_required=3138; missing_after_import=0; external_calls=0
External calls made: 0
```

Repair-plan observation:

```text
manual_market_bars_repair_plan status=attention scope=active_universe expected_as_of=2026-05-15 active=12613 existing=12090 missing=523 external_calls=0
local_bar_history=missing_with_history=0 missing_without_history=523
missing_without_local_history=AACBR,AACBU,AACIW,AACO,AACOU,AACOW,ACAAU,ACAAW,ADAC,ADXN,AEAQ,AEAQU plus 511 more
local_template_preview=status=invalid rows=523 invalid_rows=523 blank_required=3138 missing_after_import=0 external_calls=0
provider_option=status=ready_for_approval external_calls=1 key_configured=true command=catalyst-radar ingest-polygon grouped-daily --date 2026-05-15 --confirm-external-call
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_provider_ingest_cli.py::test_market_bars_repair_plan_previews_existing_local_template tests\integration\test_provider_ingest_cli.py::test_market_bars_repair_plan_reports_manual_and_guarded_provider_paths tests\integration\test_local_scripts.py::test_market_radar_status_script_is_zero_external_call_sitrep -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\market\manual_bars.py src\catalyst_radar\cli.py tests\integration\test_provider_ingest_cli.py tests\integration\test_local_scripts.py
powershell -NoProfile -Command '& { $null = [scriptblock]::Create((Get-Content -Raw .\scripts\market-radar-status.ps1)); "powershell syntax ok" }'
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli market-bars repair-plan --expected-as-of 2026-05-15
.\.venv\Scripts\python.exe -m catalyst_radar.cli market-bars repair-plan --expected-as-of 2026-05-15 --json
```

Results:

- Focused pytest passed.
- Ruff passed.
- PowerShell syntax parse passed.
- `git diff --check` passed.
- Repair-plan text and JSON both reported `external_calls=0` and
  `missing_with_history=0 missing_without_history=523`.

Next useful product action after merge:

- Do not add more source enrichment or dashboard polish until it directly helps
  clear or explain this blocker.
- The full-market scan cannot become trustworthy from existing local history.
  The operator must either fill/import `data\local\manual-bars-2026-05-15.csv`
  with real OHLCV/VWAP values, or explicitly approve the one-call
  Polygon/Massive grouped-daily fill.
- Do not run the provider command without explicit approval:

```powershell
.\.venv\Scripts\python.exe -m catalyst_radar.cli ingest-polygon grouped-daily --date 2026-05-15 --confirm-external-call
```

## Latest Quick Status Full-Market Repair Alignment

Goal alignment:

- After the repair-plan preview slice, `market-bars repair-plan` correctly
  showed the full active-universe blocker.
- `scripts\market-radar-status.ps1 -Quick` still used the stocks-only repair
  plan for its fast detailed block. That was a goal-drift risk because the
  default product goal is the full-market priced-in scan.
- This slice changes quick status so the first detailed repair block is the
  full active-universe market-bar repair path. Stocks-only details still remain
  available in the fuller status/audit path.
- This makes 0 Polygon/Massive, SEC, Schwab, OpenAI, broker, order, or provider
  calls.

Fix in this slice:

- The quick-status repair-plan command no longer passes `--stocks-only`.
- Quick status now prefers `data\local\manual-bars-<date>.csv` for local
  preview evidence, and falls back to the stock-only template only if the
  full-market template is absent.
- The quick heading changed from `Stock scan next:` to `Full-market next:`.

Live zero-call observation:

```text
Market Radar quick status
API: ok; build=c3aa11878577; version=0.1.0
Full-market next: bars=12090/12613; missing=523; command=catalyst-radar market-bars template --expected-as-of 2026-05-15 --out data\local\manual-bars-2026-05-15.csv --missing-only
Fast market-bar repair: status=attention; scope=active_universe; active=12613; existing=12090; missing=523; external_calls=0
- local template preview: status=invalid; rows=523; invalid_rows=523; blank_required=3138; missing_after_import=0; external_calls=0
- local template blank fields: close=523, high=523, low=523, open=523, volume=523, vwap=523
External calls made: 0
```

Validation run in this slice:

```powershell
powershell -NoProfile -Command '& { $null = [scriptblock]::Create((Get-Content -Raw .\scripts\market-radar-status.ps1)); "powershell syntax ok" }'
powershell -ExecutionPolicy Bypass -File .\scripts\market-radar-status.ps1 -Quick
.\.venv\Scripts\python.exe -m pytest tests\integration\test_local_scripts.py::test_market_radar_status_script_is_zero_external_call_sitrep -q
```

Next useful product action after merge:

- The CLI status path now points to the same full-market blocker as
  `priced-in-answer` and `market-bars repair-plan`.
- The real blocker remains unchanged: fill the 523-row full-market template or
  explicitly approve the one-call Polygon/Massive grouped-daily repair.

## Latest Manual Template Preview In Repair Plan

Goal alignment:

- The active full-market blocker is still missing scan-date bars. The live
  default repair target is `data\local\manual-bars-2026-05-15.csv`.
- The template file already exists, but the repair-plan surface only said to
  fill and preview it. That forced the operator to run a second command before
  learning whether the existing file was ready, invalid, or still blank.
- This slice keeps the scope narrow and useful: the repair plan now inspects the
  existing local template when it is present and reports the same zero-call
  preview evidence inline.
- This makes 0 Polygon/Massive, SEC, Schwab, OpenAI, broker, order, or provider
  calls.

Fix in this slice:

- `manual_market_bars_repair_plan(...)` now computes the default local template
  path for the requested scope:
  - full market: `data\local\manual-bars-<date>.csv`;
  - stocks-only: `data\local\manual-stock-bars-<date>.csv`.
- The repair-plan payload now includes:
  - `local_template_path`;
  - `local_template_exists`;
  - `local_template_preview` when the file exists.
- `local_template_preview` reuses the existing import-preview validation and
  keeps `external_calls_made=0`. It does not write to the database.
- The text CLI now prints:
  - local template path/existence;
  - preview status, row count, invalid-row count, blank-required count,
    missing-after-import count, and external-call count;
  - blank required field counts;
  - up to three invalid examples.

Live zero-call observation:

```text
manual_market_bars_repair_plan status=attention scope=active_universe expected_as_of=2026-05-15 active=12613 existing=12090 missing=523 external_calls=0
local_template=path=data\local\manual-bars-2026-05-15.csv exists=true
local_template_preview=status=invalid rows=523 invalid_rows=523 blank_required=3138 missing_after_import=0 external_calls=0
local_template_blank_required_fields=open=523,high=523,low=523,close=523,volume=523,vwap=523
local_template_invalid_examples=row 2 AACO 2026-05-15: blank close,high,low,open | row 3 ADAC 2026-05-15: blank close,high,low,open | row 4 ADXN 2026-05-15: blank close,high,low,open
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_api_routes.py::test_post_radar_market_bars_template_and_import_can_scope_to_stocks tests\integration\test_provider_ingest_cli.py::test_market_bars_repair_plan_previews_existing_local_template tests\integration\test_provider_ingest_cli.py::test_market_bars_repair_plan_reports_manual_and_guarded_provider_paths -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\market\manual_bars.py src\catalyst_radar\cli.py tests\integration\test_provider_ingest_cli.py tests\integration\test_api_routes.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli market-bars repair-plan --expected-as-of 2026-05-15
.\.venv\Scripts\python.exe -m catalyst_radar.cli market-bars repair-plan --expected-as-of 2026-05-15 --json
```

Next useful product action after merge:

- The system now tells the operator exactly why the existing full-market
  template cannot be imported: all 523 rows still have blank required OHLCV/VWAP
  fields.
- The real next step is still data completion: fill those fields or explicitly
  approve the one-call Polygon/Massive grouped-daily repair path.

## Latest Full-Market Source Guard Drift Check

Goal alignment:

- The active product goal is still: scan the full active market, combine price
  reaction with market-emotion evidence, and tell the operator where price has
  not yet matched expectations.
- The dashboard and CLI/API must support that goal. They should not become a
  separate dashboard-polish project, and they should not steer the operator into
  SEC, Schwab, OpenAI, or broker side quests while the scan universe itself is
  incomplete.
- The latest drift check found a real gap: stocks-only source-batch surfaces
  already blocked provider side quests while stock-like market bars were
  incomplete, but the default full-market source overview could still promote
  catalyst or broker source work even though 523 active securities had no
  scan-date market bar.
- This slice fixes that full-market path. The first useful action for the
  default full scan is now the active-universe market-bar repair plan, not SEC
  or Schwab enrichment.
- This slice makes 0 Polygon/Massive, SEC, Schwab, OpenAI, broker, order, or
  provider calls.

Fix in this slice:

- `priced_in_source_gap_batches_payload(... source="market_bars")` now plans
  against the full active universe by default instead of only the already-ranked
  queue subset.
- The same market-bar planner handles both scopes:
  - `--stocks-only`: `coverage_basis=stock_like_active_as_of_bars`, blocker
    reason `missing_stock_like_as_of_bars`, and repair commands include
    `--stocks-only`.
  - default full market: `coverage_basis=active_universe_as_of_bars`, blocker
    reason `missing_active_as_of_bars`, and repair commands target
    `data\local\manual-bars-<date>.csv` without `--stocks-only`.
- The all-source overview carries `coverage_basis` through source rows and
  recommendations, so full-market wording says `active` / `full-market` instead
  of falling back to `stock-like` / `full-stock`.
- `execute_priced_in_source_batch(...)` now blocks every non-`market_bars`
  source execution when the relevant market-bar scope is incomplete, including
  the default full-market scan. The blocked execution payload returns
  `external_calls_made=0` and an `execution_blocker`.
- The dashboard/TUI source workflow now promotes market bars for both stocks-only
  and full-market scans when market bars are the coverage blocker.
- The separate priced-in evidence plan now orders partial market-bar coverage
  before catalyst, local text, options, broker context, and agent review, so the
  agent brief no longer lists SEC source work as the first evidence step while
  full-market bars are missing.

Live zero-call observations:

```text
priced_in_source_batch_overview status=attention sources=6 ready_sources=2 blocked_sources=3 gap_rows=48842 external_calls=0
goal_alignment=status=aligned stocks_only=false ranked=12087 source_gap_rows=48842
  goal=Find stocks where market emotion has not yet been matched by price reaction.
  blocker=market_bars evidence has 523 gap row(s); 0 eligible row(s), 523 blocked row(s); blocked_reason=missing_active_as_of_bars; examples=AACBR, AACBU, AACIW, AACO, AACOU.
  next_useful_step=Fill missing as-of bars for the active universe; then rerun the full priced-in scan.
coverage_first=source=market_bars gaps=523 calls=0 command=catalyst-radar market-bars template --expected-as-of 2026-05-15 --out data\local\manual-bars-2026-05-15.csv --missing-only
  why=Fresh price reaction defines the scan universe; clear active market-bar gaps before claiming full-market coverage.
decision_shortcut_blocked=blocked_by=market_bars gaps=523 calls=0 command=catalyst-radar market-bars template --expected-as-of 2026-05-15 --out data\local\manual-bars-2026-05-15.csv --missing-only
  action=Clear market_bars first; decision shortcuts are hidden until every active row has scan-date price reaction.
```

```text
priced_in_source_batch_execution source=broker_context status=blocked external_calls=0
reason=market_bars must be complete before executing broker_context source batches for a full scan; 523 active row(s) still lack scan-date price reaction.
execution_blocker=blocked_by=market_bars gaps=523 calls=0 command=catalyst-radar market-bars template --expected-as-of 2026-05-15 --out data\local\manual-bars-2026-05-15.csv --missing-only
```

Agent brief dry-run observation:

```text
Priced-in answer is blocked; decision_ready=false; Full-market priced-in answer is not ready: 523 row(s) still lack scan-date price reaction.
Priced-in evidence plan is attention; next=Coverage is broad enough for research; generate the DB-backed missing-bar template if you want the full active universe covered before relying on the answer.
Priced-in source workflow is attention; coverage-first=Fill missing as-of bars for the active universe; then rerun the full priced-in scan.
external_calls_made: broker=0, market_data=0, openai=0
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_source_batches_prioritize_full_market_bar_coverage tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_answer_uses_stock_scope_for_market_bar_coverage tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_source_execution_blocks_until_stock_bars_complete tests\integration\test_dashboard_data.py::test_priced_in_preflight_uses_manual_bar_template_for_partial_full_scan_bars -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\dashboard\source_batches.py src\catalyst_radar\dashboard\tui.py tests\integration\test_dashboard_demo_seed_cli.py tests\integration\test_dashboard_data.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-source-batches --source all --limit 1
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-source-batches --source broker_context --execute-next
.\.venv\Scripts\python.exe -m catalyst_radar.cli agent-brief --json
```

Expected note:

- `priced-in-source-batches --source broker_context --execute-next` exits
  non-zero because it is blocked. That is correct; the output is the evidence,
  and `external_calls=0` is the safety proof.
- The all-source overview still shows per-source planning details for SEC and
  Schwab rows, but direct execution is blocked until market bars are complete.
  Do not follow those provider commands until the market-bar blocker is cleared
  or the user explicitly overrides the call plan.

Next useful product action after merge:

- Stop adding dashboard polish unless it directly reduces this blocker or makes
  the full-market scan answer clearer.
- The only useful default next step is to fill/import
  `data\local\manual-bars-2026-05-15.csv`, or get explicit user approval for the
  one Polygon/Massive grouped-daily repair path.
- After the 523 missing active bars are cleared, rerun the full priced-in scan
  and only then proceed to source evidence gaps such as SEC catalyst events,
  local text, options, broker context, and agent review.

## Latest Priced-In Answer Partial-Scan Guard

Goal alignment:

- The stocks-only source-batch and execution surfaces already blocked side
  quests while `market_bars` remained incomplete, but the direct
  `priced-in-answer --stocks-only` surface still printed
  `status=decision_ready` when 9 rows inside the scanned subset looked
  reviewable.
- That was misleading for the product goal. A full-stock priced-in answer
  cannot be ready while 131 stock-like rows still lack scan-date price
  reaction.
- This slice keeps the candidate evidence visible, but changes answer-level
  readiness to blocked until stock-like market-bar coverage is complete. It
  makes 0 Polygon/Massive, SEC, Schwab, OpenAI, broker, order, or DB-write
  calls.

Fix in this slice:

- `priced_in_answer_payload(...)` now inspects the market-bar action after the
  stock-scope coverage override.
- If `market_bars` still has a gap, answer status becomes `blocked` even when
  `decision_ready_rows` is nonzero.
- The answer text now says the scanned-subset rows still look reviewable, but
  the full scan must be repaired first.
- `decision_readiness.recommended_gap` becomes `market_bars` with the zero-call
  missing-bar template command.
- `trust_blockers` now puts the `market_bars` blocker first when it is the
  answer-level blocker.
- The dry-run agent brief now calls the ranked result list the
  `priced-in queue` instead of saying the priced-in scan itself is ready. This
  keeps the browseable queue distinct from the blocked answer.

Live zero-call observation:

```text
priced_in_answer status=blocked decision_ready=false investment_decision_ready=false total=5521 mismatches=9 research=0 blocked=2674 external_calls=0
answer=Stocks-only priced-in answer is not ready: 131 row(s) still lack scan-date price reaction. 9 scanned-subset row(s) still look reviewable, but the full scan must be repaired first.
headline=Full scan blocked by 131 missing stock-like market-bar row(s); 5521 scanned row(s) are only a subset.
decision_readiness=status=blocked actionable=9 decision_ready=9 summary=9 row(s) look decision-ready inside the scanned subset, but 131 market-bar row(s) are missing from the full scan.
recommended_gap=market_bars count=131 command=catalyst-radar market-bars template --expected-as-of 2026-05-15 --out data\local\manual-stock-bars-2026-05-15.csv --missing-only --stocks-only
trust_blockers:
- market_bars status=attention next=Fill stock-like missing as-of bars first; then rerun the stocks-only priced-in scan. command=catalyst-radar market-bars template --expected-as-of 2026-05-15 --out data\local\manual-stock-bars-2026-05-15.csv --missing-only --stocks-only
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_answer_blocks_incomplete_stock_bars_even_with_ready_rows tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_answer_uses_stock_scope_for_market_bar_coverage tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_answer_cli_outputs_current_scan_answer tests\unit\test_agent_sdk_orchestrator.py::test_agent_sdk_dry_run_brief_is_multi_agent_and_zero_call -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\agents\sdk_orchestrator.py tests\integration\test_dashboard_data.py tests\unit\test_agent_sdk_orchestrator.py
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-answer --stocks-only
```

Next useful product action after merge:

- This closes the last observed readiness overclaim on the direct answer
  surface.
- The actual blocker remains data completion: fill/import
  `data\local\manual-stock-bars-2026-05-15.csv`, or explicitly approve the one
  Polygon/Massive grouped-daily call.
- Avoid additional dashboard/status polish unless it directly clears data
  coverage or the user explicitly redirects.

## Latest Source Execution Market-Bar Guard

Goal alignment:

- The overview and dashboard no longer promote decision shortcuts while
  `market_bars` is the first blocker, but a direct
  `priced-in-source-batches --source <source> --stocks-only --execute-next`
  could still enter provider-specific execution planning.
- That was still a drift risk: the system should not spend SEC/Schwab/provider
  calls to enrich partial stock rows before every stock-like row has scan-date
  price reaction.
- This slice blocks non-`market_bars` source execution for stocks-only scans
  until the stock-like market-bar scope is complete. It makes 0
  Polygon/Massive, SEC, Schwab, OpenAI, broker, order, or DB-write calls.

Fix in this slice:

- `execute_priced_in_source_batch(...)` checks the full stocks-only
  `market_bars` plan before executing any other source.
- If market bars are incomplete, execution returns `status=blocked`,
  `external_calls_made=0`, and an `execution_blocker` with:
  - `blocked_by=market_bars`;
  - the missing stock-like bar count;
  - the zero-call repair command.
- The text CLI prints the execution blocker.

Live zero-call observations:

```text
priced_in_source_batch_execution source=broker_context status=blocked external_calls=0
reason=market_bars must be complete before executing broker_context source batches for a stocks-only scan; 131 stock-like row(s) still lack scan-date price reaction.
execution_blocker=blocked_by=market_bars gaps=131 calls=0 command=catalyst-radar market-bars template --expected-as-of 2026-05-15 --out data\local\manual-stock-bars-2026-05-15.csv --missing-only --stocks-only
```

```text
priced_in_source_batch_execution source=catalyst_events status=blocked external_calls=0
reason=market_bars must be complete before executing catalyst_events source batches for a stocks-only scan; 131 stock-like row(s) still lack scan-date price reaction.
execution_blocker=blocked_by=market_bars gaps=131 calls=0 command=catalyst-radar market-bars template --expected-as-of 2026-05-15 --out data\local\manual-stock-bars-2026-05-15.csv --missing-only --stocks-only
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_source_execution_blocks_until_stock_bars_complete -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\source_batches.py src\catalyst_radar\cli.py tests\integration\test_dashboard_demo_seed_cli.py
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-source-batches --source broker_context --stocks-only --execute-next
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-source-batches --source catalyst_events --stocks-only --execute-next
```

Expected note:

- The live execute commands return a non-zero exit code while status is
  `blocked`; that is correct. Their output is the evidence source, and
  `external_calls=0`.

Next useful product action after merge:

- The system now blocks accidental source-fill side quests until price-reaction
  coverage is complete.
- The only meaningful next product move remains data completion: fill/import
  the 131 rows or explicitly approve the one Polygon/Massive grouped-daily call.

## Latest Decision Shortcut Suppression

Goal alignment:

- The stocks-only source-batch overview still showed a Schwab
  `decision_shortcut` while the scan universe itself had an unresolved
  `market_bars` blocker.
- That could pull the operator into spending provider calls on broker context
  before every stock-like row has scan-date price reaction.
- This slice keeps the source table visible, but suppresses the promoted
  decision shortcut until the market-bar coverage blocker is cleared. It makes
  0 Polygon/Massive, SEC, Schwab, OpenAI, broker, order, or DB-write calls.

Fix in this slice:

- `priced-in-source-batches --source all --stocks-only` now emits
  `decision_shortcut_recommendation=null` when `market_bars` is the
  coverage-first blocker.
- The payload includes `decision_shortcut_blocker` explaining that
  `market_bars` must be cleared first.
- The dashboard source workflow now sets `decision_shortcut_action=None` and
  carries the same blocker when `market_bars` is coverage-first.
- The text CLI prints:

  ```text
  decision_shortcut_blocked=blocked_by=market_bars gaps=131 calls=0 command=catalyst-radar market-bars template --expected-as-of 2026-05-15 --out data\local\manual-stock-bars-2026-05-15.csv --missing-only --stocks-only
  ```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_answer_uses_stock_scope_for_market_bar_coverage -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\dashboard\tui.py src\catalyst_radar\cli.py tests\integration\test_dashboard_demo_seed_cli.py
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-source-batches --source all --stocks-only --limit 1
.\.venv\Scripts\python.exe -m catalyst_radar.cli dashboard-snapshot --stocks-only --json
```

Next useful product action after merge:

- The recommendation surface no longer encourages side quests while market bars
  are incomplete.
- The only meaningful next product move remains data completion: fill/import
  the 131 rows or explicitly approve the one Polygon/Massive grouped-daily call.

## Latest Repair-Plan Field Checklist

Goal alignment:

- The active blocker is still the same: 131 stock-like tickers lack as-of bars
  for the 2026-05-15 stock scan.
- Quick status can now inspect the local template and show actual blank fields,
  but the API/dashboard source-batch repair plan still only said to fill the
  template.
- This slice keeps the field checklist in the CLI/API/dashboard repair payloads
  themselves, so the dashboard/source workflow can say exactly what a newly
  generated missing-bar template requires. It makes 0 Polygon/Massive, SEC,
  Schwab, OpenAI, broker, order, or DB-write calls.

Fix in this slice:

- `manual-market-bars-repair-plan-v1` now includes:
  - `required_fill_fields`;
  - `blank_required_field_counts_if_new_template`;
  - `template_row_count`.
- The stocks-only `market_bars` source-batch diagnostic now carries the same
  field checklist.
- The text source-batch CLI prints
  `blank_required_fields_if_new_template=...` for market-bar blockers.

Live zero-call observation:

```text
blank_required_fields_if_new_template=close:131,high:131,low:131,open:131,volume:131,vwap:131
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_provider_ingest_cli.py::test_market_bars_repair_plan_reports_manual_and_guarded_provider_paths tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_answer_uses_stock_scope_for_market_bar_coverage -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\market\manual_bars.py src\catalyst_radar\dashboard\data.py src\catalyst_radar\cli.py tests\integration\test_provider_ingest_cli.py tests\integration\test_dashboard_demo_seed_cli.py
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-source-batches --source market_bars --stocks-only
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-source-batches --source all --stocks-only --limit 1
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-source-batches --source market_bars --stocks-only --json
```

Next useful product action after merge:

- The toolchain is now honest and specific about the missing-bar blocker.
- Further zero-call status/dashboard work is likely drift unless it directly
  clears data coverage. The next meaningful move is to fill/import the 131 rows
  or explicitly approve the one Polygon/Massive grouped-daily call.

## Latest Manual Bar Blank-Field Summary

Goal alignment:

- The active full-stock scan remains blocked by 131 stock-like tickers missing
  as-of market bars for 2026-05-15.
- The local manual template preview said `blank_required=786`, but the human
  still had to infer which fields were blank.
- This slice keeps the scope narrow and useful: expose the blank-field counts
  needed to fill the template correctly. It makes 0 Polygon/Massive, SEC,
  Schwab, OpenAI, broker, order, or DB-write calls.

Fix in this slice:

- `market-bars import --json` now includes `blank_required_field_counts`.
- The text CLI prints `blank_required_fields=...` when an import preview is
  invalid due to blank required fields.
- `scripts\market-radar-status.ps1 -Quick` now prints the local template blank
  field summary.

Live zero-call observation:

```text
- local template blank fields: close=131, high=131, low=131, open=131, volume=131, vwap=131
```

Live JSON preview observation:

```json
"blank_required_field_counts": {
  "close": 131,
  "high": 131,
  "low": 131,
  "open": 131,
  "volume": 131,
  "vwap": 131
}
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_provider_ingest_cli.py::test_market_bars_import_rejects_blank_numeric_fields tests\integration\test_local_scripts.py::test_market_radar_status_script_is_zero_external_call_sitrep -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\market\manual_bars.py src\catalyst_radar\cli.py tests\integration\test_provider_ingest_cli.py tests\integration\test_local_scripts.py
powershell -NoProfile -Command '& { $null = [scriptblock]::Create((Get-Content -Raw .\scripts\market-radar-status.ps1)); "powershell syntax ok" }'
powershell -ExecutionPolicy Bypass -File .\scripts\market-radar-status.ps1 -Quick
.\.venv\Scripts\python.exe -m catalyst_radar.cli market-bars import --daily-bars data\local\manual-stock-bars-2026-05-15.csv --expected-as-of 2026-05-15 --stocks-only --json
```

Expected note:

- The live import preview command returns a non-zero exit code while status is
  `invalid`; that is correct. Its JSON output is still the evidence source.

Next useful product action after merge:

- Clear the blocker by filling/importing
  `data\local\manual-stock-bars-2026-05-15.csv`, or explicitly approve the one
  Polygon/Massive grouped-daily call.
- Do not spend more slices on dashboard/status polish until the bar coverage
  blocker is cleared or the user explicitly redirects.

## Latest Quick Status Invalid Row Examples

Goal alignment:

- Quick status identified that the local stock-bar template was invalid, but it
  did not show the user which rows or fields were invalid.
- That slowed the only zero-call path to clearing the 131 missing stock-like
  bars.
- This slice changes only `scripts\market-radar-status.ps1` quick output and
  makes 0 Polygon/Massive, SEC, Schwab, OpenAI, broker, order, or DB-write
  calls.

Fix in this slice:

- `scripts\market-radar-status.ps1 -Quick` now prints up to three
  `invalid_examples` from the local manual bar preview.
- Live quick status now shows:

  ```text
  - local template invalid examples: row 2 AACO 2026-05-15: blank close,high,low,open | row 3 ADAC 2026-05-15: blank close,high,low,open | row 4 ADXN 2026-05-15: blank close,high,low,open
  ```

Validation run in this slice:

```powershell
powershell -NoProfile -Command '& { $null = [scriptblock]::Create((Get-Content -Raw .\scripts\market-radar-status.ps1)); "powershell syntax ok" }'
powershell -ExecutionPolicy Bypass -File .\scripts\market-radar-status.ps1 -Quick
.\.venv\Scripts\python.exe -m pytest tests\integration\test_local_scripts.py::test_market_radar_status_script_is_zero_external_call_sitrep -q
```

Next useful product action after merge:

- The zero-call manual path is now clearer: fill each missing stock-bar row's
  required OHLC fields, preview the import again, then only run `--execute`
  when the preview is valid.

## Latest Quick Status Stock-Scan Next Action

Goal alignment:

- Quick status still printed a first-line `Readiness` next action that pointed
  at the global active-universe `manual-bars` template.
- The detailed repair block below it was already correct for the actual
  stocks-only scan blocker, but the first instruction was easy for a human to
  follow incorrectly.
- This slice changes only `scripts\market-radar-status.ps1` output text and
  makes 0 Polygon/Massive, SEC, Schwab, OpenAI, broker, order, or DB-write
  calls.

Fix in this slice:

- The old quick-status `Readiness:` line is now labeled `Global readiness:`.
- Quick status prints a separate `Stock scan next:` line before the detailed
  repair block:

  ```text
  Stock scan next: bars=5521/5652; missing=131; command=catalyst-radar market-bars template --expected-as-of 2026-05-15 --out data\local\manual-stock-bars-2026-05-15.csv --missing-only --stocks-only
  ```

Live zero-call observation:

```text
Market Radar quick status
API: ok; build=61f6feb9c91e; version=0.1.0
Global readiness: research_only; investable=False; next=...
Stock scan next: bars=5521/5652; missing=131; command=catalyst-radar market-bars template --expected-as-of 2026-05-15 --out data\local\manual-stock-bars-2026-05-15.csv --missing-only --stocks-only
External calls made: 0
```

Validation run in this slice:

```powershell
powershell -NoProfile -Command '& { $null = [scriptblock]::Create((Get-Content -Raw .\scripts\market-radar-status.ps1)); "powershell syntax ok" }'
powershell -ExecutionPolicy Bypass -File .\scripts\market-radar-status.ps1 -Quick
.\.venv\Scripts\python.exe -m pytest tests\integration\test_local_scripts.py::test_market_radar_status_script_is_zero_external_call_sitrep -q
.\.venv\Scripts\python.exe -m ruff check tests\integration\test_local_scripts.py
```

Next useful product action after merge:

- The quick operator status, CLI/API source-batch plan, and dashboard workflow
  should all point humans to the same stocks-only market-bar blocker.
- Actual scan completion still requires filled local bars or explicit approval
  for the one Polygon/Massive grouped-daily call.

## Latest Dashboard Source Workflow Stock-Bar Fix

Goal alignment:

- After the source-batch CLI/API fix, the stocks-only dashboard snapshot still
  told the human to start with `catalyst_events`.
- That was dashboard drift: the human-facing workflow must start with the same
  blocker as the corrected CLI/API surface, because a stock without an as-of
  bar cannot be judged for price reaction.
- This slice changes only dashboard snapshot/TUI planning metadata and makes 0
  Polygon/Massive, SEC, Schwab, OpenAI, broker, order, or DB-write calls.

Fix in this slice:

- `dashboard_snapshot_payload(... priced_in_stocks_only=True)` now builds the
  priced-in answer before the source workflow, then lets the workflow use
  answer `trust_blockers`.
- When the answer has a stocks-only `market_bars` blocker, the dashboard source
  workflow promotes `market_bars` to the first coverage step.
- The dashboard goal panel now uses the actual zero-call manual template command
  instead of telling the user to run `batch market_bars execute`.

Live zero-call dashboard observation:

```text
external=0
workflow_next=Fill stock-like missing as-of bars first; then rerun the stocks-only priced-in scan.
coverage_cmd=catalyst-radar market-bars template --expected-as-of 2026-05-15 --out data\local\manual-stock-bars-2026-05-15.csv --missing-only --stocks-only
goal_next=Fill stock-like missing as-of bars first; then rerun the stocks-only priced-in scan.
goal_cmd=catalyst-radar market-bars template --expected-as-of 2026-05-15 --out data\local\manual-stock-bars-2026-05-15.csv --missing-only --stocks-only
boundary=Template generation and import preview are zero-call. Provider fills or source-batch execution require explicit approval.
first_step=market_bars:131
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_answer_uses_stock_scope_for_market_bar_coverage tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_snapshot_cli_outputs_dashboard_command_center_json -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\tui.py tests\integration\test_dashboard_demo_seed_cli.py
.\.venv\Scripts\python.exe -m catalyst_radar.cli dashboard-snapshot --stocks-only --json
git diff --check
```

Next useful product action after merge:

- The CLI/API and dashboard now agree on the next blocker.
- The actual blocker remains: fill/import
  `data\local\manual-stock-bars-2026-05-15.csv`, or explicitly approve the one
  Polygon/Massive grouped-daily call.

## Latest Source-Batch Stock-Bar Scope Fix

Goal alignment:

- After the stocks-only answer fix, another surface still drifted:
  `priced-in-source-batches --source all --stocks-only` reported
  `market_bars no_gaps` because source batches looked only at already ranked
  rows.
- That was wrong for the product goal. A full stock scan cannot claim complete
  price-reaction coverage while 131 active stock-like rows have no as-of bar.
- This slice keeps the source-batch operator view aligned with the answer
  surface and still makes 0 Polygon/Massive, SEC, Schwab, OpenAI, broker, or
  order calls.

Fix in this slice:

- `priced_in_source_gap_batches_payload(..., source="market_bars",
  stocks_only=True)` now uses stock-like active market-bar scope instead of
  ranked-row source gaps.
- `priced-in-source-batches --source all --stocks-only` now reports:
  - overview `status=attention`;
  - `market_bars attention 131`;
  - stock scope `active=5652 scanned=5521 unscanned=131`;
  - coverage-first recommendation `source=market_bars`;
  - next command:

    ```text
    catalyst-radar market-bars template --expected-as-of 2026-05-15 --out data\local\manual-stock-bars-2026-05-15.csv --missing-only --stocks-only
    ```

- The source-batch overview still shows runnable SEC/Schwab source chunks, but
  the first coverage recommendation is now the market-bar blocker because fresh
  price reaction defines the scan universe.

Live zero-call observation:

```text
priced_in_source_batch_overview status=attention sources=6 ready_sources=2 blocked_sources=3 gap_rows=22193 external_calls=0
full_scan=mode=full_scan active=5652 scanned=5521 ranked=5521 stocks_only=true source_gap_rows=22193 examples_are_samples=true
scope_note=The stocks-only full scan has as-of price reaction for 5521/5652 stock-like active row(s).
coverage_first=source=market_bars gaps=131 calls=0 command=catalyst-radar market-bars template --expected-as-of 2026-05-15 --out data\local\manual-stock-bars-2026-05-15.csv --missing-only --stocks-only
market_bars attention 131 ... next_command=n/a
```

Direct market-bar source-batch observation:

```text
status=attention
gaps=131
blocked_reason=missing_stock_like_as_of_bars
template=catalyst-radar market-bars template --expected-as-of 2026-05-15 --out data\local\manual-stock-bars-2026-05-15.csv --missing-only --stocks-only
external=0
```

Validation run so far:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_answer_uses_stock_scope_for_market_bar_coverage -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py tests\integration\test_dashboard_demo_seed_cli.py
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-source-batches --source all --stocks-only --limit 1
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-source-batches --source market_bars --stocks-only --json
git diff --check
```

Next useful product action after merge:

- The blocker is unchanged but now consistently surfaced:
  fill/import `data\local\manual-stock-bars-2026-05-15.csv`, or explicitly
  approve the one Polygon/Massive grouped-daily call.
- Do not execute provider/source-fill commands without explicit operator
  approval.

## Latest Goal Drift Check

User checkpoint:

- The user explicitly paused the PR stream and asked whether the work has
  drifted from the goal.

Current active goal:

- Build MarketRadar so it can scan the market and help answer whether the
  market's expectations for a stock are already priced in.
- Keep two usable surfaces aligned:
  - CLI/API for repeatable, scriptable checks and functional testing;
  - dashboard/TUI for human review and action.

Drift assessment:

- Recent work is still goal-aligned because it is correcting the scan's
  truthfulness and operator path:
  - market-bar coverage must use the stock-like active universe, not only the
    currently ranked rows;
  - `priced-in-answer --stocks-only` must not mix all-instrument counts into a
    stocks-only answer;
  - quick status and repair-plan surfaces make the next blocker visible without
    making surprise provider calls.
- The risk area is real: too much iteration can become UI/status polishing if
  it does not move the scan toward more complete market evidence.
- The next useful product step after this narrow answer-scope PR is not another
  cosmetic dashboard pass. It is to clear the 131 missing stock-like as-of bars
  by manual import or by explicit approval for one Polygon/Massive grouped-daily
  call, then rerun the stocks-only full scan.

Do not do next without explicit approval:

```powershell
catalyst-radar ingest-polygon grouped-daily --date 2026-05-15 --confirm-external-call
```

Immediate branch decision:

- Finish and merge the current `feat/answer-stock-scan-summary` branch because
  it fixes answer-surface scope drift.
- Stop before any data-provider execution.

## Latest Stocks-Only Answer Summary Fix

Goal alignment check:

- After the source-coverage fix, `priced-in-answer --stocks-only` correctly
  reported `market_bars 5521/5652`, but the full-scan summary still showed
  all-instrument active/scanned counts (`active=12613`, `scanned=12087`) and
  the next command omitted `--stocks-only`.
- That was still confusing for the actual product goal: a human asking "which
  stocks are not priced in?" needs the answer scope, counts, and command hints
  to stay in the stocks-only lane.
- This slice changes only zero-call answer metadata and command text. It does
  not run Polygon/Massive, SEC, Schwab, OpenAI, broker, order, or DB-write
  actions.

Fix in this slice:

- `priced-in-answer --stocks-only` now uses the stock-like market-bar scope in
  `full_scan`:
  - `active_securities=5652`;
  - `scanned_rows=5521`;
  - `unscanned_rows=131`;
  - `scan_scope_basis=stock_like_active_as_of_bars`.
- The decision-ready next command now keeps `--stocks-only`:

  ```text
  catalyst-radar priced-in-queue --stocks-only --full-scan --limit 50
  ```

- The legacy `full_scan_export` command now also keeps `--stocks-only` for
  stocks-only answers:

  ```text
  catalyst-radar priced-in-queue --stocks-only --full-scan --all --json
  ```

- The sample explanation now says visible rows are a page from the
  stocks-only active universe, not the entire instrument universe.
- The regression test now asserts the stock scope active/scanned/unscanned
  counts and stocks-only export command when an active common stock is missing
  its as-of bar.

Live zero-call answer observation:

```text
full_scan=mode=full_scan active=5652 scanned=5521 ranked=5521 visible=1-5 sample=true
sample_explanation=The tickers below are rows 1-5 from the current ranked page, not the stocks-only active universe of 5652 row(s).
next_action=Review the stocks-only full scan; decision-ready tickers are a filtered subset, not the scan universe.
next_command=catalyst-radar priced-in-queue --stocks-only --full-scan --limit 50
full_scan_export=catalyst-radar priced-in-queue --stocks-only --full-scan --all --json
source_coverage=market_bars 5521/5652 (131 missing); ...
```

JSON observation:

```text
full_scan.active_securities=5652
full_scan.scanned_rows=5521
full_scan.unscanned_rows=131
full_scan.scan_scope_basis=stock_like_active_as_of_bars
external_calls_made=0
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_answer_uses_stock_scope_for_market_bar_coverage tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_answer_cli_outputs_current_scan_answer -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py tests\integration\test_dashboard_demo_seed_cli.py
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-answer --stocks-only
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-answer --stocks-only --json
git diff --check
```

Observed:

- Focused tests passed (`2 passed`).
- Ruff passed.
- Live text and JSON answer surfaces show stock-scope counts and stock-only
  next command.
- `git diff --check` passed.

Next useful product action:

- Merge and restart so the CLI/API/dashboard answer surface has consistent
  stocks-only scope metadata.
- The data blocker remains: fill/import
  `data\local\manual-stock-bars-2026-05-15.csv`, or explicitly approve the one
  Polygon/Massive grouped-daily call.

## Latest Priced-In Answer Stock-Bar Scope Fix

Goal alignment check:

- The live `priced-in-answer --stocks-only` command could still report
  `market_bars 5521/5521` because it used ranked-row source coverage, even
  though the stock-like active scope was still `5521/5652` with `131` missing
  as-of bars.
- That is directly goal-relevant drift: the main answer surface must not imply
  the stock scan's market-bar layer is complete when active stock-like rows are
  missing bars.
- This slice keeps the answer useful but honest: there are still 9
  decision-ready not-priced-in rows, but market-bar coverage now shows the full
  stocks-only scope and the correct repair command.
- No Polygon/Massive, SEC, Schwab, OpenAI, broker, order, or DB-write execution
  was run.

Fix in this slice:

- `priced_in_answer_payload(...)` now applies the same market-bar scope
  correction used by the full audit.
- `trust_blockers` now overlays source-coverage action details onto preflight
  steps, so stocks-only answer blockers inherit the stocks-only command.
- Added a regression test where a second active common stock has no as-of bar;
  the answer must report `market_bars 1/2 (1 missing)` rather than the ranked
  row's `1/1`.

Live zero-call answer observation:

```text
priced_in_answer status=decision_ready decision_ready=true investment_decision_ready=false total=5521 mismatches=9 research=0 blocked=2674 external_calls=0
source_coverage=market_bars 5521/5652 (131 missing); catalyst_events 9/5521 (5512 missing); local_text 9/5521 (5512 missing); options 0/5521 (5521 missing); theme_peer_sector 5521/5521; broker_context 4/5521 (5517 missing)
trust_blockers:
- market_bars status=attention next=Fill stock-like missing as-of bars first; then rerun the stocks-only priced-in scan. command=catalyst-radar market-bars template --expected-as-of 2026-05-15 --out data\local\manual-stock-bars-2026-05-15.csv --missing-only --stocks-only
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_answer_uses_stock_scope_for_market_bar_coverage tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_answer_cli_outputs_current_scan_answer -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py tests\integration\test_dashboard_demo_seed_cli.py
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-answer --stocks-only
git diff --check
```

Observed:

- Focused tests passed (`2 passed`).
- Ruff passed.
- Live answer reports `market_bars 5521/5652 (131 missing)`.
- `git diff --check` passed.

Next useful product action:

- Merge and restart so the normal CLI/API/dashboard answer surface uses the
  corrected stock-bar scope.
- The real data blocker remains the same: fill/import
  `data\local\manual-stock-bars-2026-05-15.csv`, or explicitly approve the one
  Polygon/Massive grouped-daily call.

## Latest Quick Operator Status

Goal alignment check:

- The fast repair-plan CLI/API exists, but the full status script still waited
  on the broad priced-in audit before showing the first blocker. That is too
  slow for a human trying to answer "what blocks the stock scan right now?"
- This slice keeps full status unchanged for deep audits and adds a quick
  replacement view for the immediate stock-bar blocker.
- It remains zero-call: no Polygon/Massive, SEC, Schwab, OpenAI, broker, or
  order execution.

Fix in this slice:

- Added `-Quick` to `scripts\market-radar-status.ps1`.
- Quick mode prints:
  - API health/build;
  - readiness headline;
  - stock-like market-bar repair plan from the local CLI JSON path;
  - manual template/preview commands;
  - guarded provider option and approval boundary;
  - local manual template preview if the CSV exists.
- Quick mode returns before the broad priced-in audit, latest run, broker,
  ops, and telemetry calls.
- README now documents:

  ```powershell
  powershell -ExecutionPolicy Bypass -File scripts/market-radar-status.ps1 -Quick
  ```

Live zero-call quick status observation:

```text
Market Radar quick status
API: ok; build=5d968376094a; version=0.1.0
Readiness: research_only; investable=False; next=...
Fast market-bar repair: status=attention; scope=stock_like; active=5652; existing=5521; missing=131; external_calls=0
- manual template: catalyst-radar market-bars template --expected-as-of 2026-05-15 --out data\local\manual-stock-bars-2026-05-15.csv --missing-only --stocks-only
- preview import: catalyst-radar market-bars import --daily-bars data\local\manual-stock-bars-2026-05-15.csv --expected-as-of 2026-05-15 --stocks-only
- provider option: status=ready_for_approval; external_calls=1; command=catalyst-radar ingest-polygon grouped-daily --date 2026-05-15 --confirm-external-call
- provider boundary: This repair plan makes 0 provider calls. The provider command makes one Polygon/Massive grouped-daily request and must only be run after explicit operator approval.
- local template preview: status=invalid; rows=131; invalid_rows=131; blank_required=786; missing_after_import=0; external_calls=0
External calls made: 0
```

Validation run in this slice:

```powershell
powershell -NoProfile -Command '& { $null = [scriptblock]::Create((Get-Content -Raw .\scripts\market-radar-status.ps1)); "powershell syntax ok" }'
powershell -ExecutionPolicy Bypass -File .\scripts\market-radar-status.ps1 -Quick
.\.venv\Scripts\python.exe -m pytest tests\integration\test_local_scripts.py::test_readme_mentions_restart_script_for_local_dashboard tests\integration\test_local_scripts.py::test_market_radar_status_script_is_zero_external_call_sitrep -q
.\.venv\Scripts\python.exe -m ruff check tests\integration\test_local_scripts.py
git diff --check
```

Observed:

- PowerShell syntax parsed.
- Quick status returned the stock-like repair plan and `External calls made: 0`.
- Focused tests passed (`2 passed`).
- Ruff passed.
- `git diff --check` passed.

Next useful product action:

- Merge and restart so `-Quick` is available from the normal local checkout.
- The data blocker still remains: either fill/import
  `data\local\manual-stock-bars-2026-05-15.csv`, or explicitly approve the
  one Polygon/Massive grouped-daily call.

## Latest Fast Market-Bar Repair Plan

Goal alignment check:

- The active objective is still not "make the dashboard prettier"; it is to
  scan the stock universe and answer whether price has caught up to market
  expectations.
- The current first blocker remains stock-like as-of market bars:
  `5521/5652` covered for `2026-05-15`, with `131` missing.
- The broad priced-in audit is useful but slow for this one blocker. The
  useful small slice is a fast zero-call repair-plan surface that gives the
  operator the exact manual CSV path and the guarded one-call provider option.
- This slice does not execute Polygon/Massive, SEC, Schwab, OpenAI, broker, or
  order calls. It only reads local DB state and reports the plan.

Fix in this slice:

- Added reusable `manual_market_bars_repair_plan(...)` in
  `src\catalyst_radar\market\manual_bars.py`.
- Added CLI:

  ```powershell
  catalyst-radar market-bars repair-plan --expected-as-of 2026-05-15 --stocks-only
  ```

- Added API:

  ```text
  POST /api/radar/market-bars/repair-plan
  ```

  request body:

  ```json
  {"expected_as_of":"2026-05-15","stocks_only":true}
  ```

- Added the new route to the security boundary allowlist.
- Added focused CLI/API tests that prove:
  - the payload is zero-call;
  - stock-like scope counts active/existing/missing bars correctly;
  - manual template, preview, and execute commands are returned;
  - the Polygon/Massive command is only presented as a guarded approval option.

Live zero-call CLI observation:

```text
manual_market_bars_repair_plan status=attention scope=stock_like expected_as_of=2026-05-15 active=5652 existing=5521 missing=131 external_calls=0
missing_as_of_tickers=AACO,ADAC,ADXN,AEAQ,AGM.A,AIRT,ALOV,ARCI,AXIN,BEBE,BLIV,BPAC plus 119 more
manual_template=catalyst-radar market-bars template --expected-as-of 2026-05-15 --out data\local\manual-stock-bars-2026-05-15.csv --missing-only --stocks-only
preview_import=catalyst-radar market-bars import --daily-bars data\local\manual-stock-bars-2026-05-15.csv --expected-as-of 2026-05-15 --stocks-only
execute_import=catalyst-radar market-bars import --daily-bars data\local\manual-stock-bars-2026-05-15.csv --expected-as-of 2026-05-15 --stocks-only --execute
provider_option=status=ready_for_approval external_calls=1 key_configured=true command=catalyst-radar ingest-polygon grouped-daily --date 2026-05-15 --confirm-external-call
approval_boundary=This repair plan makes 0 provider calls. The provider command makes one Polygon/Massive grouped-daily request and must only be run after explicit operator approval.
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_provider_ingest_cli.py::test_market_bars_repair_plan_reports_manual_and_guarded_provider_paths tests\integration\test_api_routes.py::test_post_radar_market_bars_template_and_import_can_scope_to_stocks tests\integration\test_security_boundaries.py -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\market\manual_bars.py src\catalyst_radar\cli.py src\catalyst_radar\api\routes\radar.py tests\integration\test_provider_ingest_cli.py tests\integration\test_api_routes.py tests\integration\test_security_boundaries.py
.\.venv\Scripts\python.exe -m catalyst_radar.cli market-bars repair-plan --expected-as-of 2026-05-15 --stocks-only
git diff --check
```

Observed:

- Focused tests passed (`15 passed`).
- Ruff passed.
- Live CLI repair-plan returned `active=5652`, `existing=5521`,
  `missing=131`, and `external_calls=0`.
- `git diff --check` passed.

Next useful product action:

- Merge and restart the local API/dashboard so the API route is available.
- After restart, smoke:

  ```powershell
  curl.exe --insecure --fail --silent --show-error --request POST https://127.0.0.1:8443/api/radar/market-bars/repair-plan --header "Content-Type: application/json" --data "{\"expected_as_of\":\"2026-05-15\",\"stocks_only\":true}"
  ```

- Do not run the provider command unless the user explicitly approves the one
  Polygon/Massive grouped-daily call.

## Latest Guarded Market-Bar Provider Option

Goal alignment check:

- The current blocker remains data, not another scan algorithm: the stocks-only
  full scan is missing 131 stock-like as-of bars.
- The dashboard/status surfaces already showed the zero-call manual CSV path,
  but the alternative provider path was easy to miss. The useful next step is
  to make the choice explicit: fill the CSV with zero calls, or intentionally
  approve one Polygon/Massive grouped-daily call.
- This does not execute providers. It only exposes the existing guarded command
  in CLI/API/dashboard payloads.

Fix in this slice:

- The market-bars source row now carries the provider fill plan:
  - `provider_fill_status`;
  - `provider_fill_external_call_count`;
  - `provider_fill_command`;
  - `provider_fill_plan`.
- In stocks-only mode, the provider fill plan now uses the effective
  stock-like missing count. This prevents a false `not_needed` plan when ranked
  rows have bars but an active stock-like row is still unranked due to missing
  bars.
- The TUI run page now shows a `Provider fill option` line for market-bar
  blockers.
- `scripts\market-radar-status.ps1` now prints the stock-like provider option
  and an explicit approval boundary.
- No provider, broker, OpenAI, order, or DB-write execution was run.

Live zero-call JSON audit observation:

```text
partial stock_like_active_as_of_bars 5521 5652 131
ready_for_approval 1
catalyst-radar ingest-polygon grouped-daily --date 2026-05-15 --confirm-external-call
external_calls 0
```

Live zero-call TUI observation:

```text
Answer: attention ready=false
Source coverage: market_bars 5521/5652 (131 missing); ...
Provider fill option: ready_for_approval; 1 external call(s) only after explicit approval: `catalyst-radar ingest-polygon grouped-daily --date 2026-05-15 --confirm-external-call`
```

Live zero-call status observation:

```text
- stock-like provider option: status=ready_for_approval; external_calls=1; command=catalyst-radar ingest-polygon grouped-daily --date 2026-05-15 --confirm-external-call
- stock-like provider boundary: run only after explicit approval; grouped daily writes local bars, then rerun audit.
External calls made: 0
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_full_scan_audit_reports_stock_only_bar_coverage tests\integration\test_dashboard_data.py::test_priced_in_full_scan_audit_payload_consolidates_current_state tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_run_page_shows_priced_in_evidence_plan tests\integration\test_local_scripts.py::test_market_radar_status_script_is_zero_external_call_sitrep -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\dashboard\tui.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py tests\integration\test_local_scripts.py
powershell -NoProfile -Command '$null = [scriptblock]::Create((Get-Content -Raw .\scripts\market-radar-status.ps1)); "powershell syntax ok"'
git diff --check
```

Observed:

- Focused tests passed (`4 passed`).
- Ruff passed.
- PowerShell status script parsed successfully.
- `git diff --check` passed.

Next useful product action:

- If the user explicitly approves provider execution, the next market-bar
  command is:

  ```powershell
  catalyst-radar ingest-polygon grouped-daily --date 2026-05-15 --confirm-external-call
  ```

- Otherwise keep the zero-call path: fill and preview
  `data\local\manual-stock-bars-2026-05-15.csv`, then import only after
  preview returns `status=ready`.

## Latest TUI Full-Scan Blocker Correction

Goal alignment check:

- After the API/CLI audit was corrected, the TUI run page still used the older
  preflight evidence plan. It could show `Answer: decision ready ready=true`
  while the full-stock audit still had missing market bars.
- That is a dashboard-side drift from the user's actual goal. The dashboard is
  for human eyes, so it must make the first full-scan blocker obvious before
  encouraging a run or provider batch.

Fix in this slice:

- The TUI header now uses the full-scan audit status when present. If the audit
  is `attention` or `blocked`, the header shows `ready=false` even when a
  short-list row has enough local evidence to be decision-useful.
- The run page now prefers audit-backed source rows over the older preflight
  evidence-plan rows.
- The first visible blocker is selected from the source coverage order, so
  market bars appear before catalyst/options/broker gaps when market bars are
  incomplete.
- The run page now shows the corrected market-bar coverage and the exact
  missing-bar template command.
- No provider, broker, OpenAI, order, or DB-write execution was run.

Live zero-call TUI observations:

```text
dashboard-tui --once --page run
Answer: attention ready=false
Source coverage: market_bars 12090/12613 (523 missing); ...
market_bars | partial | 12090/12613 | 523 | Fill missing as-of bars for the active universe; then rerun the full priced-in scan.
```

```text
dashboard-tui --once --page run --stocks-only
Answer: attention ready=false
Source coverage: market_bars 5521/5652 (131 missing); ...
market_bars | partial | 5521/5652 | 131 | Fill stock-like missing as-of bars first; then rerun the stocks-only priced-in scan.
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_run_page_shows_priced_in_evidence_plan tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_tui_once_can_show_full_scan_mode tests\integration\test_dashboard_demo_seed_cli.py::test_modern_dashboard_tui_supports_mouse_navigation -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\tui.py tests\integration\test_dashboard_demo_seed_cli.py
git diff --check
```

Observed:

- Focused TUI tests passed (`3 passed`).
- Ruff passed.
- `git diff --check` passed.

Next useful product action:

- The dashboard now points the user at the real first blocker. To progress the
  scan itself, fill/import the missing-bar template or explicitly approve the
  one-call Polygon/Massive grouped-daily provider fill.

## Latest Full-Scan Market-Bar Scope Correction

Goal alignment check:

- The user explicitly pushed back on "only these tickers" and wants a full
  stock scan. The previous audit output still had a confusing semantic gap:
  source coverage could say `market_bars 5521/5521` ready because it only
  counted ranked rows, even though 131 stock-like active securities were
  unranked because their as-of bars were missing.
- That was drift-prone because the dashboard could look healthier than the
  actual stock universe. A full-stock priced-in answer must account for
  stock-like active rows that failed to enter the ranked output due to missing
  market bars.

Fix in this slice:

- `priced_in_full_scan_audit_payload` now computes market-bar repair/stock
  scope before building audit source rows.
- The `market_bars` source coverage row is now rebased to the intended market
  bar universe:
  - stocks-only mode uses stock-like active as-of bar coverage;
  - all-instrument mode uses active-universe as-of bar coverage.
- The audit preserves existing options/catalyst/broker diagnostics while only
  recomputing the market-bars source action.
- The market-bars source row now carries:
  - `coverage_basis`;
  - `as_of_bar_scope`;
  - `stale`;
  - `missing`;
  - a useful missing-bar template command.
- No provider, broker, OpenAI, order, or DB-write execution was run.

Live zero-call audit observation:

```text
source_coverage=ready=1/6 weak=options,broker_context,catalyst_events
- market_bars status=partial coverage=5521/5652 gap_rows=131 decision=0 research=0 actionable=0 next=Fill stock-like missing as-of bars first; then rerun the stocks-only priced-in scan.
```

Live JSON audit observation:

```text
market_bars 5521/5652 (131 missing); catalyst_events 9/5521 (5512 missing); local_text 9/5521 (5512 missing); options 0/5521 (5521 missing); theme_peer_sector 5521/5521; broker_context 4/5521 (5517 missing)
partial stock_like_active_as_of_bars 5521 5652 131
catalyst-radar market-bars template --expected-as-of 2026-05-15 --out data\local\manual-stock-bars-2026-05-15.csv --missing-only --stocks-only
external_calls 0
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_full_scan_audit_reports_stock_only_bar_coverage tests\integration\test_dashboard_data.py::test_priced_in_full_scan_audit_payload_consolidates_current_state tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_audit_cli_outputs_full_scan_audit -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py
git diff --check
```

Observed:

- Focused tests passed (`3 passed`).
- Ruff passed.
- `git diff --check` passed.

Next useful product action:

- The current first blocker is now shown honestly: complete the 131 stock-like
  missing bars, either by filling
  `data\local\manual-stock-bars-2026-05-15.csv` and importing it after preview,
  or by explicitly approving the one-call Polygon/Massive grouped-daily provider
  fill plan.
- Do not claim the stocks-only priced-in scan is complete until market-bar
  source coverage reads `5652/5652`.

## Latest Drift Check And Stocks-Only Market Bar Scope

Goal alignment check:

- The active goal remains: scan the market broadly enough to decide whether a
  stock's price has already matched market expectations.
- The current branch is still aligned with that goal. It is not dashboard
  decoration. It removes a concrete full-scan blocker by letting the operator
  repair missing daily bars for stock-like rows first.
- The prior active-universe blocker was 523 missing as-of market bars. The
  useful stock-first split is now visible:
  - `5652` stock-like active securities;
  - `5521` already have the 2026-05-15 as-of bar;
  - `131` stock-like rows are missing and need manual bar values;
  - `392` additional missing rows are non-stock instruments and should not
    block the first stock-priced-in scan.
- This keeps "useful" well defined: complete stock-like market-bar coverage
  first, then continue catalyst/options/agent evidence coverage. Do not make
  the user fill 523 mixed rows before the stock scan can progress.

Fix in this slice:

- `market-bars template` now accepts `--stocks-only`.
- `market-bars import` now accepts `--stocks-only`.
- The manual bar template/import payloads now report:
  - `stocks_only`;
  - `template_scope` such as `stock_like_missing_as_of_bars`;
  - `coverage_scope` such as `stock_like`.
- API parity was added through:
  - `POST /api/radar/market-bars/template` with `stocks_only: true`;
  - `POST /api/radar/market-bars/import` with `stocks_only: true`.
- `scripts\market-radar-status.ps1` now prefers
  `data\local\manual-stock-bars-<run-as-of>.csv` when present and previews it
  with `--stocks-only`.
- Status output now shows the stock-like coverage split and the exact
  stock-like template/preview commands.
- No Polygon/Massive, SEC, Schwab, OpenAI, broker/order, or database-write
  execution was run while validating this branch.

Live zero-write/zero-provider-call observations:

```text
manual_market_bars_template status=ready rows=131 scope=stock_like_missing_as_of_bars expected_as_of=2026-05-15 path=data\local\manual-stock-bars-2026-05-15.csv external_calls=0
coverage=active=5652 existing=5521 missing=131 missing_only=true stocks_only=true
import_command=catalyst-radar market-bars import --daily-bars data\local\manual-stock-bars-2026-05-15.csv --expected-as-of 2026-05-15 --stocks-only
execute_command=catalyst-radar market-bars import --daily-bars data\local\manual-stock-bars-2026-05-15.csv --expected-as-of 2026-05-15 --stocks-only --execute
```

The generated stock-like template is intentionally blank, so preview exits
nonzero with `status=invalid`, which is correct:

```text
manual_market_bars_import status=invalid rows=131 tickers=131 active=5652 latest_bar=2026-05-15 expected_as_of=2026-05-15 executed=false external_calls=0
coverage=bars_at_expected=131 existing=5521 after_import=5652 missing=0 scope=stock_like
invalid=rows=131 blank_required=786 invalid_numeric=0
next_action=Fix blank or invalid required fields, then preview again before running --execute.
```

Status script live observation:

```text
Stock-like market bars: active=5652; with_as_of_bar=5521; missing=131; non_stock_missing=392
- stock-like template command: catalyst-radar market-bars template --expected-as-of 2026-05-15 --out data\local\manual-stock-bars-2026-05-15.csv --missing-only --stocks-only
- stock-like preview command: catalyst-radar market-bars import --daily-bars data\local\manual-stock-bars-2026-05-15.csv --expected-as-of 2026-05-15 --stocks-only
- local template preview: status=invalid; rows=131; invalid_rows=131; blank_required=786; missing_after_import=0; external_calls=0
External calls made: 0
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_provider_ingest_cli.py::test_market_bars_stocks_only_template_and_import_scope tests\integration\test_provider_ingest_cli.py::test_market_bars_missing_only_template_import_counts_existing_bars tests\integration\test_api_routes.py::test_post_radar_market_bars_template_and_import_can_scope_to_stocks tests\integration\test_dashboard_data.py::test_priced_in_full_scan_audit_payload_consolidates_current_state tests\integration\test_local_scripts.py::test_market_radar_status_script_is_zero_external_call_sitrep -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\market\manual_bars.py src\catalyst_radar\cli.py src\catalyst_radar\api\routes\radar.py src\catalyst_radar\dashboard\data.py tests\integration\test_provider_ingest_cli.py tests\integration\test_api_routes.py tests\integration\test_local_scripts.py tests\integration\test_dashboard_data.py
powershell -NoProfile -Command '$null = [scriptblock]::Create((Get-Content -Raw .\scripts\market-radar-status.ps1)); "powershell syntax ok"'
git diff --check
```

Observed:

- Focused tests passed (`5 passed`).
- Ruff passed.
- PowerShell status script parsed successfully.
- `git diff --check` passed.

Next useful product action:

- Fill `data\local\manual-stock-bars-2026-05-15.csv` for the 131 stock-like
  rows.
- Preview without writing:

  ```powershell
  catalyst-radar market-bars import --daily-bars data\local\manual-stock-bars-2026-05-15.csv --expected-as-of 2026-05-15 --stocks-only
  ```

- Only after preview returns `status=ready`, run the same command with
  `--execute` to write local stock-like daily bars.
- Then rerun zero-call status and the priced-in audit before any SEC/Schwab
  execution.

## Latest Status Preview For Local Manual Bars

Goal alignment check:

- The immediate full-scan blocker is still market-bar coverage for the current
  scan date. The local missing-bar template exists, but the operator should not
  have to remember a separate preview command just to see whether it is ready.
- This is not dashboard polish for its own sake. It keeps the replacement
  CLI/status surface centered on the next useful action: fill the generated
  523-row CSV until preview returns `status=ready`, then execute the local DB
  import.

Fix in this slice:

- `scripts\market-radar-status.ps1` now checks for
  `data\local\manual-bars-<run-as-of>.csv`.
- If the file exists, status runs the zero-write CLI preview:

  ```powershell
  catalyst-radar market-bars import --daily-bars data\local\manual-bars-<date>.csv --expected-as-of <date> --json
  ```

- The status output now includes:
  - local template status;
  - row count;
  - invalid row count;
  - blank required field count;
  - missing rows after import;
  - invalid examples;
  - next action.
- The JSON status payload now includes `manual_market_bar_preview`.
- No provider calls, broker calls, OpenAI calls, order actions, or database
  writes were made.

Live zero-write/zero-call status observation:

```text
- local template preview: status=invalid; rows=523; invalid_rows=523; blank_required=3138; missing_after_import=0; external_calls=0
- local template invalid examples: row 2 AACO 2026-05-15: blank close,high,low,open | row 3 ADAC 2026-05-15: blank close,high,low,open | row 4 ADXN 2026-05-15: blank close,high,low,open
- local template next: Fix blank or invalid required fields, then preview again before running --execute.
External calls made: 0
```

Validation run in this slice:

```powershell
powershell -NoProfile -Command '$null = [scriptblock]::Create((Get-Content -Raw .\scripts\market-radar-status.ps1)); "powershell syntax ok"'
.\.venv\Scripts\python.exe -m pytest tests\integration\test_local_scripts.py::test_market_radar_status_script_is_zero_external_call_sitrep -q
.\.venv\Scripts\python.exe -m ruff check tests\integration\test_local_scripts.py
git diff --check
```

Observed:

- Focused status-script test passed (`1 passed`).
- Ruff passed for the touched test.
- PowerShell syntax parsed successfully.
- `git diff --check` passed.

## Latest Manual Bar Import Invalid Preview

Goal alignment check:

- Full-market priced-in analysis is still blocked by incomplete market-bar
  coverage: the current scan date has 12,090 of 12,613 active symbols covered,
  leaving 523 missing rows.
- The missing-only template exists locally at
  `data\local\manual-bars-2026-05-15.csv`, but a blank or partially filled
  template previously failed preview with a parser error such as
  `cannot convert float NaN to integer`.
- That was not a useful CLI/API replacement UI: the operator needs to know how
  many rows/fields are still blank before any local DB write.

Fix in this slice:

- `market-bars import` now inspects manual CSV rows before converting them into
  `DailyBar` objects.
- Blank or invalid required fields now return a structured
  `manual-market-bars-import-v1` payload with `status=invalid`.
- The invalid preview includes:
  - row count and ticker count;
  - expected-as-of coverage if the rows were filled;
  - invalid row count;
  - blank required field count;
  - invalid numeric field count;
  - example row diagnostics;
  - the same execute command, but only as the next command after fixing and
    previewing again.
- API parity is covered by `POST /api/radar/market-bars/import`, which now
  returns a 200 payload with `status=invalid` for a blank template instead of
  requiring the client to parse a conversion exception.
- No provider calls, broker calls, OpenAI calls, order actions, or database
  writes were made.

Live zero-write/zero-call observation on the generated local template:

```text
manual_market_bars_import status=invalid rows=523 tickers=523 active=12613 latest_bar=2026-05-15 expected_as_of=2026-05-15 executed=false external_calls=0
coverage=bars_at_expected=523 existing=12090 after_import=12613 missing=0
invalid=rows=523 blank_required=3138 invalid_numeric=0
next_action=Fix blank or invalid required fields, then preview again before running --execute.
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_provider_ingest_cli.py::test_market_bars_import_rejects_blank_numeric_fields tests\integration\test_provider_ingest_cli.py::test_market_bars_missing_only_template_import_counts_existing_bars tests\integration\test_provider_ingest_cli.py::test_market_bars_import_requires_expected_full_active_coverage tests\integration\test_api_routes.py::test_post_radar_market_bars_template_and_import_use_database_universe -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\market\manual_bars.py src\catalyst_radar\cli.py tests\integration\test_provider_ingest_cli.py tests\integration\test_api_routes.py
git diff --check
```

Observed:

- Focused tests passed (`4 passed`).
- Ruff passed.
- `git diff --check` passed.

Next useful product action:

- Fill `data\local\manual-bars-2026-05-15.csv`.
- Preview it with:

  ```powershell
  catalyst-radar market-bars import --daily-bars data\local\manual-bars-2026-05-15.csv --expected-as-of 2026-05-15
  ```

- Only after preview returns `status=ready`, run the same command with
  `--execute` to write local daily bars.
- Then rerun the zero-call status and plan-only smoke before any provider
  source execution.

## Latest Missing-Only Market Bar Guidance

Goal alignment check:

- The active product goal is still full-market priced-in mismatch detection:
  every ranked stock row needs enough price-reaction and market-emotion evidence
  before the dashboard can answer whether expectations are already reflected in
  price.
- The current full-scan blocker is not that MarketRadar lacks a manual bar
  repair path. The path already exists as
  `catalyst-radar market-bars template --missing-only`.
- The drift was in the operator guidance: local status and several
  readiness/preflight strings still said to generate an active-universe
  template and fill every active ticker. On the live database that sounds like
  a 12,613-row manual job even though only 523 as-of rows are missing.
- This slice keeps the work useful and small by wiring the existing
  missing-only repair mode into the human-facing CLI/API/dashboard guidance.

Fix in this slice:

- `priced_in_preflight_payload` now surfaces the market-bar repair command with
  `--missing-only`.
- Market-bar preflight/readiness text now says "missing-bar template" and "fill
  only missing ticker rows" instead of implying a full active-universe refill.
- `priced_in_full_scan_audit_payload` market-bar repair guidance now points at
  the missing-bar template workflow.
- `scripts\market-radar-status.ps1` now prints:

  ```text
  catalyst-radar market-bars template --expected-as-of <date> --out data\local\manual-bars-<date>.csv --missing-only
  ```

  and says to fill only missing ticker rows.
- `scripts\refresh-csv-market-data.ps1` now says to fill the generated ticker
  rows when an import preview is incomplete.
- No provider calls, broker calls, OpenAI calls, order actions, or database
  writes were made.

Live zero-call observation after the patch:

```text
Market as-of coverage: active=12613; with_as_of_bar=12090; missing=523
- template command: catalyst-radar market-bars template --expected-as-of 2026-05-15 --out data\local\manual-bars-2026-05-15.csv --missing-only
- market coverage: Generate the missing-bar template and fill only missing ticker rows before import.
- latest-bar coverage: Fill only missing ticker rows for the latest/as-of date before treating bars as fresh.
External calls made: 0
```

Live missing-only template smoke:

```text
manual_market_bars_template status=ready rows=523 scope=missing_as_of_bars expected_as_of=2026-05-15 path=data\local\_codex-missing-bars-smoke.csv external_calls=0
coverage=active=12613 existing=12090 missing=523 missing_only=true
```

The temporary smoke file was removed after verification.

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_provider_ingest_cli.py::test_market_bars_missing_only_template_import_counts_existing_bars tests\integration\test_dashboard_data.py::test_priced_in_preflight_uses_manual_bar_template_for_partial_full_scan_bars tests\integration\test_dashboard_data.py::test_priced_in_full_scan_audit_payload_consolidates_current_state tests\integration\test_dashboard_data.py::test_operator_work_queue_prioritizes_full_scan_market_bar_root_cause tests\integration\test_local_scripts.py::test_market_radar_status_script_is_zero_external_call_sitrep tests\integration\test_local_scripts.py::test_refresh_csv_market_data_script_wraps_local_ingest_without_provider_calls -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py tests\integration\test_dashboard_data.py tests\integration\test_local_scripts.py
powershell -NoProfile -Command '$null = [scriptblock]::Create((Get-Content -Raw .\scripts\market-radar-status.ps1)); $null = [scriptblock]::Create((Get-Content -Raw .\scripts\refresh-csv-market-data.ps1)); "powershell syntax ok"'
git diff --check
```

Observed:

- Focused tests passed (`6 passed`).
- Ruff passed.
- PowerShell syntax parsed successfully.
- `git diff --check` passed.

Next useful product action:

- The immediate market-bar repair surface is now practical: generate the
  523-row missing-bar template, fill those rows, preview import, execute import,
  then rerun the zero-call status and plan-only smoke.
- Provider/source execution remains approval-gated. Do not run the import
  execute path unless the filled CSV exists and the user intends the local DB
  write.

## Latest CIK Template Validate-First Output

Goal alignment check:

- Manual CIK repair is the current local path to unblock the two
  catalyst-events rows (`FRBA`, `SSBI`) that cannot receive SEC catalyst
  evidence.
- The validation gate existed, but the template writer still told the operator
  to fill and import directly. That could skip the no-write safety check.

Fix in this slice:

- `write_sec_cik_override_template_csv` now returns a path-specific
  `validate_command` alongside `import_command`.
- `sec_cik_override_template_payload` now includes the default validate command.
- `catalyst-radar ingest-sec cik-overrides-template ...` now prints:

  ```text
  validate_command=catalyst-radar ingest-sec cik-overrides --csv <template.csv> --validate-only
  import_command=catalyst-radar ingest-sec cik-overrides --csv <template.csv>
  next_action=Fill cik and optional sec_company_name for each row, validate the completed CSV, then import it before replanning catalyst_events.
  ```

- No SEC/Massive/Polygon, Schwab, OpenAI, broker/order execution, or database
  write was run. A temporary smoke template file was generated and removed.

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_sec_cik_metadata.py::test_write_sec_cik_override_template_csv_writes_blank_cik_rows tests\integration\test_sec_cik_metadata.py::test_ingest_sec_cik_overrides_template_cli_writes_current_blockers tests\integration\test_dashboard_data.py::test_sec_cik_override_template_payload_exports_missing_company_like_rows -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\events\sec_cik.py src\catalyst_radar\cli.py src\catalyst_radar\dashboard\data.py tests\integration\test_sec_cik_metadata.py tests\integration\test_dashboard_data.py
```

Observed:

- Focused tests passed (`3 passed`).
- Ruff passed.
- Live CLI smoke printed `validate_command`, `import_command`,
  `external_calls=0`, and validate-first next action.

## Latest CIK Override Validation Gate

Goal alignment check:

- The full-market priced-in goal is currently blocked first by
  `catalyst_events` coverage. The current stock scan has `5510` SEC-eligible
  rows, `2` missing-CIK blockers (`FRBA`, `SSBI`), and a next SEC chunk that
  would cost `5` calls if explicitly approved.
- The prior slices exposed the blocker and repair commands. This slice adds a
  zero-call, no-write validation gate before manual CIK import, so a filled
  override CSV can be checked before it changes local security metadata.
- This is useful because bad or blank CIK rows would leave the full-scan SEC
  catalyst blocker unresolved while appearing operationally "handled."

Fix in this slice:

- Added `validate_sec_cik_overrides` and `validate_sec_cik_overrides_csv`.
- Added CLI validation:

  ```powershell
  catalyst-radar ingest-sec cik-overrides --csv <cik-overrides.csv> --validate-only
  ```

- Added API validation:

  ```text
  POST /api/radar/sec/cik-overrides/validate
  {"overrides":[{"ticker":"FRBA","cik":"...","sec_company_name":"..."}]}
  ```

- Validation reports status, requested rows, syntactically valid rows, update
  candidates, already-current rows, unmatched active securities, invalid rows,
  duplicate tickers, examples, import command, next action, and
  `external_calls_made=0`.
- Source-batch diagnostics, CLI all/source views, TUI batch text, and
  `market-radar-status.ps1` now include the CIK validate command before the CIK
  import command.
- No SEC/Massive/Polygon, Schwab, OpenAI, broker/order execution, or database
  write was run.

Live zero-call source-batch observation before PR:

```text
priced_in_source_batches ... external_calls=0
diagnostic_manual_template_command=catalyst-radar ingest-sec cik-overrides-template --out data\local\cik-overrides-template.csv --stocks-only
diagnostic_manual_validate_command=catalyst-radar ingest-sec cik-overrides --csv <cik-overrides.csv> --validate-only
diagnostic_manual_validate_api=POST /api/radar/sec/cik-overrides/validate
diagnostic_manual_command=catalyst-radar ingest-sec cik-overrides --csv <cik-overrides.csv>
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_sec_cik_metadata.py::test_validate_sec_cik_overrides_reports_import_readiness_without_writes tests\integration\test_sec_cik_metadata.py::test_ingest_sec_cik_overrides_validate_only_cli_reports_without_writes tests\integration\test_api_routes.py::test_post_radar_sec_cik_overrides_validate_returns_zero_call_plan tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_exposes_missing_cik_blockers tests\integration\test_local_scripts.py::test_market_radar_status_script_is_zero_external_call_sitrep tests\integration\test_security_boundaries.py::test_openapi_routes_are_allowlisted_and_broker_routes_are_explicit -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\events\sec_cik.py src\catalyst_radar\cli.py src\catalyst_radar\api\routes\radar.py src\catalyst_radar\dashboard\data.py src\catalyst_radar\dashboard\tui.py tests\integration\test_sec_cik_metadata.py tests\integration\test_api_routes.py tests\integration\test_dashboard_data.py tests\integration\test_local_scripts.py tests\integration\test_security_boundaries.py
git diff --check
powershell -NoProfile -Command '$null = [scriptblock]::Create((Get-Content -Raw .\scripts\market-radar-status.ps1)); "powershell syntax ok"'
```

Observed:

- Focused tests passed (`6 passed`).
- Ruff passed for touched Python files.
- PowerShell status script parsed successfully.
- `git diff --check` passed.

## Latest Dashboard Source Blocker Diagnostics

Goal alignment check:

- The active goal is still full-market priced-in mismatch detection: scan stock
  rows, gather enough market-emotion evidence, and compare that evidence with
  price reaction.
- The prior slice made `market-radar-status.ps1` show the CIK blocker. This
  slice keeps CLI/API/dashboard aligned by pushing the same diagnostic into the
  zero-call source-batch overview and TUI guidance.
- This is dashboard/operator clarity, not a new provider path.

Fix in this slice:

- `priced-in-source-batches --source all --stocks-only` now carries CIK blocker
  details through the coverage-first recommendation and goal-alignment blocker:
  eligible row count, blocked row count, blocked reason, sample blocked tickers,
  and repair commands.
- The CLI all-source overview now prints source diagnostics when a source has
  blocked rows or blocker examples.
- The TUI run page now tells the operator to use `batch <source>` for blockers,
  first provider chunk, and exact call budget instead of leaving the evidence
  plan as a generic table.
- TUI batch messages now include a concise source blocker summary and CIK import
  command when present.
- No SEC/Massive/Polygon, Schwab, OpenAI, broker/order execution, or database
  write was run.

Live zero-call observation before PR:

```text
priced_in_source_batch_overview ... external_calls=0
blocker=catalyst_events evidence has 5512 gap row(s); 5510 eligible row(s), 2 blocked row(s); blocked_reason=missing_cik; examples=FRBA, SSBI.
coverage_first=source=catalyst_events ... calls=5 ...
diagnostic=eligible=5510 blocked=2 reason=missing_cik
blocked_examples=FRBA,SSBI
template=catalyst-radar ingest-sec cik-overrides-template --out data\local\cik-overrides-template.csv --stocks-only
import=catalyst-radar ingest-sec cik-overrides --csv <cik-overrides.csv>
```

TUI run-page smoke:

```text
Priced-in Evidence Plan
Inspect source blocker : Type `batch catalyst_events` for blockers, first provider chunk, and exact call budget; type `batch all` for the source map.
External calls made: 0
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_all_source_gap_batches_prioritizes_decision_useful_gaps tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_run_page_shows_priced_in_evidence_plan -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\dashboard\tui.py src\catalyst_radar\cli.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_batch_command_opens_full_scan_source_batch_plan tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_batch_command_explains_non_company_cik_gaps tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_source_batches_cli_prints_non_company_route tests\integration\test_dashboard_data.py::test_priced_in_all_source_gap_batches_payload_summarizes_next_chunks tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_plans_safe_sync_batches tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_exposes_missing_cik_blockers -q
git diff --check
```

Observed:

- Focused tests passed (`2 passed`).
- Existing source-batch/dashboard focused tests passed (`6 passed`).
- Ruff passed.
- `git diff --check` passed.

## Latest Status CIK Blocker Surface

Goal alignment check:

- Full-scan coverage-first work is still `catalyst_events`.
- The source-batch planner already knew that the current stock scan has
  thousands of SEC-eligible rows and a small number of missing-CIK blockers,
  but the local status output did not show that split.
- This matters because the full-market goal is not just "run the next batch";
  it is to make the whole stock scan explainable. Rows without CIK metadata
  cannot receive SEC company catalyst evidence.

Fix in this slice:

- `scripts\market-radar-status.ps1` now calls the existing zero-call
  source-batch planning endpoint for the current stock coverage source:

  ```text
  /api/radar/priced-in/source-batches?source=<coverage-source>&stocks_only=true&batch_limit=1
  ```

- The status output now prints:
  - SEC-eligible rows;
  - blocked rows;
  - external calls required by the next reviewed chunk;
  - sample missing-CIK tickers;
  - CIK override template/import commands;
  - CIK metadata refresh command.
- The API call is planning-only and reports `external_calls_made=0`.
- No SEC/Massive/Polygon, Schwab, OpenAI, broker/order execution, or database
  write was run.

Alignment pause from 2026-05-20:

- Goal is still full-market priced-in mismatch detection: scan all stock rows,
  collect enough market-emotion evidence, then compare that evidence against
  price reaction.
- This slice is aligned because it exposes the current full-scan SEC catalyst
  blocker instead of adding another dashboard polish layer.
- Live zero-call status now reports:
  - `stock coverage SEC plan: eligible=5510; blocked=2; next_calls=5`;
  - `stock coverage missing CIK: FRBA, SSBI`;
  - CIK template/import/refresh commands;
  - `External calls made: 0`.
- A drift-risk bug was caught before PR: the status script first read
  `first_batch`, but the source-batch API returns `batches`. The script now
  falls back to the first returned batch and reports the real planned call
  count.

## Latest TUI Full-Scan Next-Step Copy

Goal alignment check:

- The CLI/status surface now separates full-scan coverage work from the current
  decision-shortlist repair path.
- The terminal dashboard overview still compressed that into
  `Coverage-first` / `Decision shortcut`, which was technically true but less
  clear for a human operator trying to keep the full-market goal in mind.

Fix in this slice:

- The TUI overview/source hint now says:
  - `Full-scan coverage: <source> (<gap count> full-scan gap row(s))`;
  - `Shortlist context: <source> (<decision-ready row count> decision-ready row(s))`.
- This keeps the dashboard aligned with the goal:
  - full-scan coverage is the market-wide evidence-fill path;
  - shortlist context is only for the currently visible decision-ready mismatch
    rows.
- No provider, broker, OpenAI, order, or database-write action was run.

## Latest Full-Scan Status Wording

Goal alignment check:

- The active goal is full-stock priced-in mismatch scanning: find stocks where
  market emotion has not yet been matched by price reaction.
- The local status output already had both the full-scan evidence plan and the
  decision-shortlist gap, but the labels could still over-focus the operator on
  the current 9-row decision shortlist.
- This slice keeps the behavior zero-call and renames the status lines so the
  full-scan coverage path is visibly separate from decision-context repair.

Fix in this slice:

- `scripts\market-radar-status.ps1` now prints:
  - `stock coverage-first gap`, sourced from the priced-in evidence plan;
  - `stock coverage command`, usually the zero-call source-batch plan for
    `catalyst_events`;
  - `stock decision-context gap`, sourced from the current decision-shortlist
    repair recommendation;
  - `stock point-in-time template/validate/import` for the options repair path.
- No Polygon/Massive, Schwab, SEC, OpenAI, broker/order execution, or database
  import was run.

Expected live interpretation:

- Full-scan coverage-first work is still `catalyst_events`: the stock scan has
  thousands of catalyst/text gaps and needs SEC/event evidence before the whole
  market-emotion side is trustworthy.
- Decision-context repair is still `options`: the current 9 decision-useful
  mismatch rows need point-in-time options context if those rows are being
  reviewed first.

## Latest Options Fixture Validation

Goal alignment check:

- The stock scan still cannot trust options context until point-in-time options
  rows are filled for the scan date.
- The prior slices made the template/export path visible, but importing a
  half-filled template would fail late or write bad evidence if validation were
  too loose.
- This slice adds a zero-call, no-write validation step before import. It keeps
  the dashboard/CLI/API repair path useful without adding a new provider.

Fix in this slice:

- Added options fixture validation:

  ```powershell
  catalyst-radar ingest-options --fixture data\local\point-in-time-options-2026-05-15.json --validate-only --expected-as-of 2026-05-15
  ```

- Added API parity:

  ```text
  POST /api/radar/options/fixture-validate
  {"fixture_path":"data\\local\\point-in-time-options-2026-05-15.json","expected_as_of":"2026-05-15"}
  ```

- The validator checks:
  - header datetime fields are present and timezone-aware;
  - optional expected `as_of` date matches;
  - `results` is a list;
  - ticker is present and not duplicated;
  - aggregate numeric fields are present, nonblank, finite, and nonnegative;
  - `iv_percentile` is between `0` and `1`.
- Source-batch diagnostics and TUI `batch options` now show commands in this
  order:
  1. template export;
  2. validate-only;
  3. import.
- `market-radar-status.ps1` also prints `stock gap validate` when present.
- No Polygon/Massive, Schwab, SEC, OpenAI, broker/order execution, or options
  DB import was run.

Current live zero-call observation from local CLI:

```text
options_fixture_validation status=ready rows=1 valid=1 invalid=0 blank_required=0 invalid_numeric=0 missing_fields=0 duplicates=0 as_of=2026-05-08T21:00:00+00:00 external_calls=0
import_command=catalyst-radar ingest-options --fixture tests\fixtures\options\options_summary_2026-05-08.json
next_action=Import the validated point-in-time options fixture.
```

`priced-in-source-batches --source options --stocks-only --batch-limit 1` now
prints:

```text
diagnostic_point_in_time_template=catalyst-radar ingest-options --fixture-template --out data\local\point-in-time-options-2026-05-15.json --stocks-only
diagnostic_point_in_time_validate=catalyst-radar ingest-options --fixture data\local\point-in-time-options-2026-05-15.json --validate-only --expected-as-of 2026-05-15
diagnostic_point_in_time_import=catalyst-radar ingest-options --fixture <point-in-time-options-2026-05-15.json>
external_calls=0
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_options_ingest.py::test_validate_options_fixture_json_rejects_blank_template_rows tests\integration\test_options_ingest.py::test_validate_options_fixture_json_accepts_filled_rows tests\integration\test_options_ingest.py::test_ingest_options_validate_only_cli_reports_invalid_fixture tests\integration\test_dashboard_data.py::test_options_fixture_template_payload_exports_point_in_time_skeleton tests\integration\test_dashboard_data.py::test_priced_in_all_source_gap_batches_blocks_options_shortcut_when_not_point_in_time tests\integration\test_dashboard_data.py::test_priced_in_full_scan_audit_payload_consolidates_current_state tests\integration\test_dashboard_data.py::test_priced_in_queue_payload_reports_full_scan_instrument_scope tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_source_batches_cli_prints_options_point_in_time_import tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_batch_message_prints_options_point_in_time_import tests\integration\test_api_routes.py::test_post_radar_options_fixture_validate_returns_zero_call_result tests\integration\test_security_boundaries.py::test_openapi_routes_are_allowlisted_and_broker_routes_are_explicit tests\integration\test_local_scripts.py::test_market_radar_status_script_is_zero_external_call_sitrep -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\connectors\options.py src\catalyst_radar\cli.py src\catalyst_radar\dashboard\data.py src\catalyst_radar\dashboard\tui.py src\catalyst_radar\api\routes\radar.py tests\integration\test_options_ingest.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py tests\integration\test_api_routes.py tests\integration\test_local_scripts.py tests\integration\test_security_boundaries.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli ingest-options --fixture tests\fixtures\options\options_summary_2026-05-08.json --validate-only --expected-as-of 2026-05-08
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-source-batches --source options --stocks-only --batch-limit 1
```

Observed:

- Focused test set passed (`12 passed`).
- Ruff passed for Python files.
- `git diff --check` passed.
- Known fixture validation stayed zero-call and returned `status=ready`.
- Source-batch planning stayed zero-call and now prints the validation command.

Next useful product action:

- The template/validate/import path is now safe for local point-in-time options
  data, but no actual 2026-05-15 options values have been filled yet.
- Without user-provided point-in-time options or approval to rerun current
  Schwab chains against a current scan, the `options` source gap remains real.
- Provider/source execution remains approval-gated.

## Latest Status Surface For Options Template

Goal alignment check:

- After the options fixture-template PR merged, the audit/status surface still
  needed one human-facing cleanup: `market-radar-status.ps1` showed that
  options were the current stock decision gap, but did not print the new
  template/import repair commands.
- The audit repair command also lost the `--stocks-only` flag when the audit
  was run in stock-only mode, even though the source-batch command preserved
  it.
- This is a narrow dashboard/CLI status fix. It does not add provider calls or
  new source behavior.

Fix in this slice:

- Stock-only priced-in audit repair now keeps `--stocks-only` on:

  ```text
  point_in_time_template_command=catalyst-radar ingest-options --fixture-template --out data\local\point-in-time-options-2026-05-15.json --stocks-only
  ```

- `scripts\market-radar-status.ps1` now resolves the recommended stock source
  row and prints repair commands when present:

  ```text
  - stock gap template: catalyst-radar ingest-options --fixture-template --out data\local\point-in-time-options-2026-05-15.json --stocks-only
  - stock gap import: catalyst-radar ingest-options --fixture <point-in-time-options-2026-05-15.json>
  ```

- No Polygon/Massive, Schwab, SEC, OpenAI, or broker/order execution was run.

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_full_scan_audit_payload_consolidates_current_state tests\integration\test_dashboard_data.py::test_priced_in_queue_payload_reports_full_scan_instrument_scope tests\integration\test_local_scripts.py::test_market_radar_status_script_is_zero_external_call_sitrep -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py tests\integration\test_dashboard_data.py tests\integration\test_local_scripts.py
git diff --check
powershell -ExecutionPolicy Bypass -File scripts\market-radar-status.ps1
```

Observed:

- Focused test set passed (`3 passed`).
- Ruff passed.
- `git diff --check` passed.
- Live local status stayed `External calls made: 0` and printed the stock gap
  template/import lines. Because the API service was still running the previous
  merged build at that moment, the live status template line did not yet include
  `--stocks-only`; after this slice is merged and services restart, it should.

## Latest Options Fixture Template Export

Goal alignment check:

- The active product goal is still the full stock priced-in scan: identify
  stocks where market emotion has not yet been matched by price reaction.
- The largest decision-useful evidence gap is still `options`: `5521` current
  stock rows have missing point-in-time options context.
- The prior slice surfaced the import command, but the operator still had to
  invent the fixture file shape. This slice keeps the scope small and makes the
  repair path directly usable without running Schwab or any provider.

Fix in this slice:

- Added a zero-call options fixture template writer:

  ```powershell
  catalyst-radar ingest-options --fixture-template --out data\local\point-in-time-options-2026-05-15.json --stocks-only
  ```

- The template is the exact JSON shape accepted by the existing
  `ingest-options --fixture` command:

  ```json
  {
    "as_of": "2026-05-15T21:00:00+00:00",
    "source_ts": "2026-05-15T21:00:00+00:00",
    "available_at": "2026-05-15T21:00:00+00:00",
    "provider": "options_fixture",
    "results": [
      {
        "ticker": "A",
        "call_volume": "",
        "put_volume": "",
        "call_open_interest": "",
        "put_open_interest": "",
        "iv_percentile": "",
        "skew": ""
      }
    ]
  }
  ```

- Added API parity:

  ```text
  GET /api/radar/options/fixture-template?stocks_only=true
  ```

- Blocked `options` source-batch diagnostics now print both:
  - `diagnostic_point_in_time_template=... --fixture-template ...`
  - `diagnostic_point_in_time_import=... --fixture ...`
- TUI `batch options` messages now show the template command before the import
  command, so the terminal dashboard does not leave the user guessing the file
  format.
- No Polygon/Massive, Schwab, SEC, OpenAI, or broker/order execution was run.

Current live zero-call observation from local CLI:

```text
options_fixture_template status=ready source=options stocks_only=true target_as_of=2026-05-15T21:00:00+00:00 source_gap_rows=5521 rows=5521 output=C:\Users\fpan1\AppData\Local\Temp\market-radar-point-in-time-options-template.json external_calls=0
template_examples=A,AAMI,AAOI,MSFT,AAPL
columns=ticker,call_volume,put_volume,call_open_interest,put_open_interest,iv_percentile,skew
import_command=catalyst-radar ingest-options --fixture C:\Users\fpan1\AppData\Local\Temp\market-radar-point-in-time-options-template.json
api=GET /api/radar/options/fixture-template?stocks_only=true
boundary=Template/export is zero-call. Values must describe option context available at the scan date; do not backfill current chains into an older scan.
```

`priced-in-source-batches --source options --stocks-only --batch-limit 1` now
prints:

```text
diagnostic_point_in_time_template=catalyst-radar ingest-options --fixture-template --out data\local\point-in-time-options-2026-05-15.json --stocks-only
diagnostic_point_in_time_import=catalyst-radar ingest-options --fixture <point-in-time-options-2026-05-15.json>
external_calls=0
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_options_ingest.py::test_write_options_fixture_template_json_writes_importable_shape tests\integration\test_options_ingest.py::test_ingest_options_fixture_template_cli_writes_gap_template tests\integration\test_dashboard_data.py::test_options_fixture_template_payload_exports_point_in_time_skeleton tests\integration\test_dashboard_data.py::test_priced_in_all_source_gap_batches_blocks_options_shortcut_when_not_point_in_time tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_source_batches_cli_prints_options_point_in_time_import tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_batch_message_prints_options_point_in_time_import tests\integration\test_api_routes.py::test_get_radar_options_fixture_template_returns_zero_call_fixture tests\integration\test_security_boundaries.py::test_openapi_routes_are_allowlisted_and_broker_routes_are_explicit -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\connectors\options.py src\catalyst_radar\dashboard\data.py src\catalyst_radar\cli.py src\catalyst_radar\dashboard\tui.py src\catalyst_radar\api\routes\radar.py tests\integration\test_options_ingest.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py tests\integration\test_api_routes.py tests\integration\test_security_boundaries.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli ingest-options --fixture-template --out $env:TEMP\market-radar-point-in-time-options-template.json --stocks-only
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-source-batches --source options --stocks-only --batch-limit 1
Get-Content -Path $env:TEMP\market-radar-point-in-time-options-template.json -TotalCount 22
```

Observed:

- Focused test set passed (`8 passed`).
- Ruff passed.
- `git diff --check` passed.
- Template export produced `5521` stock rows for scan date `2026-05-15` and
  stayed `external_calls=0`.
- The generated JSON has the exact header and aggregate fields accepted by
  `ingest-options --fixture`.

Next useful product action:

- If point-in-time options aggregates are available for `2026-05-15`, fill the
  template and import it:

  ```powershell
  catalyst-radar ingest-options --fixture data\local\point-in-time-options-2026-05-15.json
  ```

- If only current Schwab chains are available, use them only for a current
  rerun with a current scan date and current bars. Do not backfill current
  chains into the older `2026-05-15` scan.
- Provider/source execution remains approval-gated.

## Latest Options Point-In-Time Repair Command

Goal alignment check:

- The stock-only priced-in audit currently shows `9` decision-ready mismatch
  rows, but every stock row is missing usable point-in-time options context for
  the stored scan date.
- `options` is the highest decision-useful gap, but live Schwab option batches
  are blocked because the stored options are newer than the scan date. Backfill
  must be point-in-time, not current-live data pretending it was available on
  `2026-05-15`.
- This slice does not add a new options provider or run Schwab. It makes the
  existing zero-call source-batch diagnostic tell the operator the exact local
  fixture import shape to use.

Fix in this slice:

- Blocked `options` source-batch diagnostics now include:

  ```text
  point_in_time_import_command=catalyst-radar ingest-options --fixture <point-in-time-options-YYYY-MM-DD.json>
  ```

- `priced-in-source-batches --source options --stocks-only` now prints:

  ```text
  diagnostic_point_in_time_import=catalyst-radar ingest-options --fixture <point-in-time-options-2026-05-15.json>
  ```

- TUI `batch options` messages now include the same **Point-in-time import**
  instruction when options are blocked by non-point-in-time stored data.
- The existing audit repair payload continues to use the same helper, so audit
  and source-batch diagnostics stay consistent.
- No Polygon/Massive, Schwab, SEC, OpenAI, or broker/order execution was run.

Current live zero-call observation from local CLI:

```text
priced_in_source_batches source=options status=blocked gap_rows=5521 plannable=0 ... external_calls=0
blocked_examples=A,MSFT,AAPL,AA reason=newer_than_scan
diagnostic_point_in_time_import=catalyst-radar ingest-options --fixture <point-in-time-options-2026-05-15.json>
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_all_source_gap_batches_blocks_options_shortcut_when_not_point_in_time tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_source_batches_cli_prints_options_point_in_time_import tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_batch_message_prints_options_point_in_time_import -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\cli.py src\catalyst_radar\dashboard\tui.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-source-batches --source options --stocks-only --batch-limit 1
```

Observed:

- Focused test set passed (`3 passed`).
- Ruff passed.
- `git diff --check` passed.
- Live source-batch planning for options stayed zero-call and now prints the
  point-in-time fixture import command.

Next useful product action:

- If a point-in-time options fixture for the original scan date exists, import
  it with:

  ```powershell
  catalyst-radar ingest-options --fixture <point-in-time-options-2026-05-15.json>
  ```

- If using current Schwab option data instead, rerun the scan with a current
  scan date and current bars; do not backfill current options into the older
  scan.
- Provider/source execution is still approval-gated.

## Latest Stock-Scan Status Sitrep

Goal alignment check:

- After the CIK-template PR merged, the generic local status script still led
  with full active-universe market-bar coverage. That is valid for the broad
  active universe, but it can distract from the user's stated stock-scan goal:
  identify stocks where market expectations have not yet matched price.
- The stock-only priced-in audit already has a clearer goal-specific summary:
  `5521` ranked stock rows, `9` decision-ready mismatch rows, and source
  evidence blockers. This slice surfaces that summary directly in the local
  status output before the broader active-universe bar warnings.

Fix in this slice:

- `scripts\market-radar-status.ps1` now reads:

  ```text
  /api/radar/priced-in/audit?stocks_only=true&limit=1
  ```

- The stock-only audit call uses a `90` second curl timeout because this local
  payload can take longer than the default status-call timeout on the current
  database. Other status calls keep the default timeout.
- The JSON payload now includes `priced_in_stock_audit`.
- Human-readable status now prints a **Stock priced-in scan** block:
  - stock audit status;
  - ranked and scanned stock rows;
  - decision-ready mismatch count;
  - zero-call boundary;
  - answer-lens boundary;
  - top decision gap;
  - evidence-plan next action and command.
- This does not change core readiness semantics; full active-universe bars can
  still be reported as a broader blocker. It just makes the stock-scan goal
  visible before that generic warning.
- No Polygon/Massive, Schwab, SEC, OpenAI, or broker/order execution was run.

Current live zero-call observation from local status:

```text
Stock priced-in scan: status=attention; ranked=5521; scanned=12087; decision_ready=9; external_calls=0
- stock answer lens: decision_ready; actionable=9; boundary=This shortlist ranks market-expectation mismatch evidence only; it is not trade approval.
- stock decision gap: options; gaps=5521; Inspect options first. Stored options exist after this scan date. Example tickers: A, MSFT, AAPL, AA. Rerun only with a current scan date and current bars, or ingest point-in-time options for the original scan date.
- stock evidence plan: Review the run call plan and refresh event ingestion before trusting emotion.
- stock evidence command: catalyst-radar priced-in-source-batches --source catalyst_events --stocks-only --all --json
External calls made: 0
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_local_scripts.py::test_market_radar_status_script_is_zero_external_call_sitrep -q
.\.venv\Scripts\python.exe -m ruff check tests\integration\test_local_scripts.py
git diff --check
powershell -ExecutionPolicy Bypass -File scripts\market-radar-status.ps1
```

Observed:

- Focused script contract test passed.
- Ruff passed for the touched test file.
- Live status stayed zero-call and now shows the stock scan before the generic
  market-bar coverage warning.

Next useful product action:

- The stock-specific status still says `attention`; that is correct.
- Do not treat the `9` decision-ready mismatch rows as investment-ready. The
  answer lens is a ranked evidence shortlist, not trade approval.
- Continue with source evidence coverage:
  - either fill the two local missing CIKs through the zero-call template path;
  - or, with explicit approval only, run one capped SEC source batch for
    `catalyst_events`.

## Latest Missing-CIK Template Export

Goal alignment check:

- The active product goal is still the stock full-scan question: find stocks
  where market emotion has not yet been matched by price reaction.
- The current stock scan already has a zero-call source plan, but
  `catalyst_events` still has `5512` stock-row gaps. `5510` rows are plannable
  SEC targets; `2` company-like rows (`FRBA`, `SSBI`) are blocked only because
  their local security metadata has no SEC CIK.
- Running the first SEC source chunk remains approval-gated, so this slice did
  not make a provider call. It made the current local CIK blocker exportable as
  a human-fillable CSV template instead.

Fix in this slice:

- Added a zero-provider template payload for current `catalyst_events`
  missing-CIK blockers:
  - schema: `sec-cik-override-template-v1`
  - source: `catalyst_events`
  - rows: current source-gap tickers with missing CIK metadata, excluding
    rows that already have CIKs and routing non-company instruments away from
    SEC company filing batches.
- Added CLI:

  ```powershell
  catalyst-radar ingest-sec cik-overrides-template --out <cik-overrides-template.csv> --stocks-only
  ```

- The generated CSV columns are:

  ```csv
  ticker,cik,sec_company_name,security_type,template_reason
  ```

- Added read-only API parity:

  ```text
  GET /api/radar/sec/cik-overrides-template?stocks_only=true
  ```

- Existing `priced-in-source-batches --source catalyst_events --stocks-only`
  diagnostics now print the template command/API alongside the existing manual
  import path:

  ```text
  diagnostic_manual_template_command=catalyst-radar ingest-sec cik-overrides-template --out data\local\cik-overrides-template.csv --stocks-only
  diagnostic_manual_template_api=GET /api/radar/sec/cik-overrides-template?stocks_only=true
  ```

- The TUI missing-CIK diagnostic suffix also includes the template command.
- No Polygon/Massive, Schwab, SEC, OpenAI, or broker/order execution was run.

Current live zero-call observation from local CLI:

```text
sec_cik_override_template status=ready source=catalyst_events stocks_only=true source_gap_rows=5512 rows=2 output=C:\Users\fpan1\AppData\Local\Temp\market-radar-cik-overrides-template.csv external_calls=0
missing_cik_examples=FRBA,SSBI
columns=ticker,cik,sec_company_name,security_type,template_reason
import_command=catalyst-radar ingest-sec cik-overrides --csv C:\Users\fpan1\AppData\Local\Temp\market-radar-cik-overrides-template.csv
api=GET /api/radar/sec/cik-overrides-template?stocks_only=true
```

Generated temp CSV preview:

```csv
ticker,cik,sec_company_name,security_type,template_reason
FRBA,,,CS,missing_sec_cik_for_catalyst_events_source_gap
SSBI,,,CS,missing_sec_cik_for_catalyst_events_source_gap
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_sec_cik_metadata.py tests\integration\test_dashboard_data.py::test_sec_cik_override_template_payload_exports_missing_company_like_rows tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_exposes_missing_cik_blockers tests\integration\test_api_routes.py::test_get_radar_sec_cik_overrides_template_returns_zero_call_rows tests\integration\test_api_routes.py::test_post_radar_sec_cik_overrides_imports_manual_metadata tests\integration\test_security_boundaries.py::test_openapi_routes_are_allowlisted_and_broker_routes_are_explicit -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\events\sec_cik.py src\catalyst_radar\dashboard\data.py src\catalyst_radar\cli.py src\catalyst_radar\api\routes\radar.py src\catalyst_radar\dashboard\tui.py tests\integration\test_sec_cik_metadata.py tests\integration\test_dashboard_data.py tests\integration\test_api_routes.py tests\integration\test_security_boundaries.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli ingest-sec cik-overrides-template --out $env:TEMP\market-radar-cik-overrides-template.csv --stocks-only
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-source-batches --source catalyst_events --stocks-only --batch-limit 1
```

Observed:

- Focused test set passed (`12 passed`).
- Ruff passed.
- `git diff --check` passed.
- Template generation made `external_calls=0` and produced two blank-CIK rows:
  `FRBA` and `SSBI`.
- Source-batch planning remained `external_calls=0` and still shows the first
  approval-gated SEC chunk as `AAT`, `AAUC`, `AB`, `ABAT`, `ABBV`.

Next useful product action:

- If exact SEC CIKs for `FRBA` and `SSBI` are known, fill the generated CSV and
  import it:

  ```powershell
  catalyst-radar ingest-sec cik-overrides --csv <completed-cik-overrides-template.csv>
  ```

- This import makes `0` external calls and should reduce the
  `catalyst_events` unplannable stock rows from `2` to `0`.
- Do not guess CIK values. If using SEC company-tickers instead, treat that as
  a SEC provider call and get explicit approval first.
- After the two CIK blockers are cleared, the next meaningful source-coverage
  step is still the approval-gated SEC batch:

  ```powershell
  catalyst-radar priced-in-source-batches --source catalyst_events --stocks-only --execute-next
  ```

  That is a `5` SEC-call read-only source fill; do not run it without explicit
  approval.

## Latest Manual SEC CIK Override Path

Goal alignment check:

- The current stock-scan source coverage plan is still correctly centered on
  `catalyst_events`.
- Running the first SEC source chunk is approval-gated, so the next safe slice
  reduced a non-provider blocker instead of making a surprise SEC call.
- Live zero-call diagnostics show `5512` stock `catalyst_events` gaps, `5510`
  plannable SEC targets, and only `2` company-like missing-CIK blockers:
  `FRBA` and `SSBI`.

Fix in this slice:

- Added a zero-provider manual CIK metadata import:
  `catalyst-radar ingest-sec cik-overrides --csv <cik-overrides.csv>`
- The CSV must include `ticker,cik` and can include `sec_company_name`.
- The importer updates only metadata for active local securities:
  - `cik`
  - `cik_source=manual_cik_override`
  - `cik_updated_at`
  - optional `sec_company_name`
- Added API parity:
  `POST /api/radar/sec/cik-overrides`
  with payload:
  `{"overrides":[{"ticker":"FRBA","cik":"...","sec_company_name":"..."}]}`
- Existing missing-CIK diagnostics now show both paths:
  - live SEC company-tickers refresh:
    `catalyst-radar ingest-sec company-tickers`
  - no-provider manual override:
    `catalyst-radar ingest-sec cik-overrides --csv <cik-overrides.csv>`
- API route inventory was updated for current actual routes, including earlier
  market-bar and priced-in audit endpoints that were already live.
- No Polygon/Massive, Schwab, SEC, OpenAI, or broker/order execution was run.

Current live zero-call observation from local CLI:

```text
priced_in_source_batches source=catalyst_events status=ready gap_rows=5512 plannable=5510 total_batches=1102 external_calls=0
blocked_examples=FRBA,SSBI reason=missing_cik
missing_cik_types=CS:2 company_like=2 non_company=0 unknown=0
diagnostic_command=catalyst-radar ingest-sec company-tickers
diagnostic_manual_command=catalyst-radar ingest-sec cik-overrides --csv <cik-overrides.csv>
diagnostic_manual_api=POST /api/radar/sec/cik-overrides
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_sec_cik_metadata.py tests\integration\test_api_routes.py::test_post_radar_sec_cik_overrides_imports_manual_metadata tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_exposes_missing_cik_blockers tests\integration\test_security_boundaries.py::test_openapi_routes_are_allowlisted_and_broker_routes_are_explicit -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\events\sec_cik.py src\catalyst_radar\cli.py src\catalyst_radar\api\routes\radar.py src\catalyst_radar\dashboard\data.py tests\integration\test_sec_cik_metadata.py tests\integration\test_api_routes.py tests\integration\test_dashboard_data.py tests\integration\test_security_boundaries.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-source-batches --source catalyst_events --stocks-only --batch-limit 1
```

Observed:

- Focused test set passed (`8 passed`).
- Ruff passed.
- `git diff --check` passed.
- Live source-batch diagnostic remained zero-call and now prints the manual CIK
  override command/API.

Next useful product action:

- If the exact CIKs for `FRBA` and `SSBI` are known, create a local CSV:

  ```csv
  ticker,cik,sec_company_name
  FRBA,<cik>,<name>
  SSBI,<cik>,<name>
  ```

- Then run:
  `catalyst-radar ingest-sec cik-overrides --csv <path-to-csv>`
- This makes `0` external calls and should reduce the `catalyst_events`
  unplannable stock rows from `2` to `0`.
- Do not guess CIK values. If using SEC company-tickers instead, treat that as a
  SEC provider call and get explicit approval first.

## Latest Stock-Scan Goal Alignment Surface

Goal alignment check:

- The user stopped the work to check for drift after many PRs.
- The active goal is still: scan the market to find stocks whose prices have
  not yet matched market expectations, with both CLI/API and dashboard surfaces.
- The current useful blocker is still source evidence coverage for the stock
  scan, not additional market-bar repair or decorative dashboard work.
- The local stock scan currently has `5521` ranked stock rows. Market bars are
  usable for this stock lens. The first broad evidence gap is
  `catalyst_events`, with `5512` stock-row gaps and `5510` plannable SEC
  targets.

Fix in this slice:

- Added `--stocks-only` startup controls to:
  - `catalyst-radar dashboard-snapshot`
  - `catalyst-radar dashboard-tui`
  - `catalyst-radar agent-brief`
- The dashboard/TUI/agent snapshot controls now preserve
  `priced_in_stocks_only=True` when requested, instead of requiring the user to
  type `stocks` after opening the TUI.
- `priced_in_all_source_gap_batches_payload` now includes a
  `goal_alignment` block:
  - goal: find stocks where market emotion has not yet been matched by price;
  - useful definition: ranked stock rows need fresh price reaction plus enough
    catalyst/context evidence to judge the emotion-price gap;
  - current state: ranked rows and source-gap rows;
  - current blocker: first source gap;
  - next useful step and command;
  - explicit zero-call/provider-boundary language.
- CLI `priced-in-source-batches --source all` prints that goal-alignment block.
- TUI source workflow now shows the same human-readable goal/useful/now/blocker
  summary and points the operator to `batch <source>` before any execute
  command.
- No Polygon/Massive, Schwab, SEC, OpenAI, or broker/order execution was run.

Current live zero-call observation from local CLI:

```text
priced_in_source_batch_overview status=ready sources=6 ready_sources=2 blocked_sources=2 gap_rows=22062 external_calls=0
full_scan=mode=full_scan active=12613 scanned=12087 ranked=5521 stocks_only=true source_gap_rows=22062 examples_are_samples=true
goal_alignment=status=aligned stocks_only=true ranked=5521 source_gap_rows=22062 useful=Useful means a ranked stock row has fresh price reaction plus enough catalyst/context evidence to judge the emotion-price gap.
current=The current stock scan covers 5521 ranked row(s) and has 22062 source evidence gap row(s).
blocker=catalyst_events evidence has 5512 gap row(s).
next_useful_command=catalyst-radar priced-in-source-batches --source catalyst_events --stocks-only --execute-next calls=5
boundary=This is a zero-call plan. Execute only one reviewed source chunk when the provider and call budget are intentional.
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_all_source_gap_batches_payload_summarizes_next_chunks tests\integration\test_dashboard_data.py::test_priced_in_all_source_gap_batches_prioritizes_decision_useful_gaps tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_snapshot_cli_outputs_dashboard_command_center_json tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_snapshot_ops_page_shows_priced_in_source_actions tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_tui_once_can_show_full_scan_mode tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_queue_cli_outputs_same_zero_call_signal -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\cli.py src\catalyst_radar\dashboard\data.py src\catalyst_radar\dashboard\tui.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-source-batches --source all --stocks-only --limit 1
.\.venv\Scripts\python.exe -m catalyst_radar.cli dashboard-tui --once --stocks-only --page ops
```

Observed:

- Focused test set passed (`6 passed`).
- Ruff passed.
- `git diff --check` passed.
- Live stock-only source overview and TUI Ops smoke both made
  `external_calls=0`.

Next useful product action:

- If the operator explicitly approves the first provider chunk, run only:
  `catalyst-radar priced-in-source-batches --source catalyst_events --stocks-only --execute-next`
- That chunk is `5` SEC calls for `AAT`, `AAUC`, `AB`, `ABAT`, and `ABBV`.
- Do not run that command without explicit approval.
- After the first SEC chunk, rerun the stock-only source overview and check
  whether `local_text` becomes runnable for the stored event text.

## Latest Stocks-Only Source-Batch Scope

Goal alignment check:

- The user's actual goal is for MarketRadar to scan the market and identify
  stocks where market emotion has not been fully matched by price reaction.
- After the stock-only priced-in filter, the next highest-value blocker is not
  more manual market-bar plumbing. The zero-call audit shows stock market bars
  are usable for the ranked stock scan; source evidence coverage is the trust
  gap.
- This slice keeps follow-up source-batch planning in the same stock-only
  universe as the stock-only audit, so a user reviewing `5521` stock rows does
  not accidentally jump back to the broader all-instrument universe when
  planning catalyst, local-text, options, or broker-context fills.

Fix in this slice:

- `priced_in_source_gap_batches_payload` and
  `priced_in_all_source_gap_batches_payload` now accept `stocks_only=True`.
- CLI:
  - `catalyst-radar priced-in-source-batches --source <source> --stocks-only`
  - `catalyst-radar priced-in-source-batches --source all --stocks-only`
  - The same flag is preserved for `--all`, `--execute-next`, and
    `--execute-batches`.
- API:
  - `GET /api/radar/priced-in/source-batches?...&stocks_only=true`
  - `POST /api/radar/priced-in/source-batches/execute-next` accepts
    `stocks_only`.
- TUI batch commands now pass the current stock-only filter into plan and
  execute helpers.
- Stock-only source plans now emit stock-only review/export/execute commands:
  - `catalyst-radar priced-in-queue --stocks-only --full-scan --source-gap ...`
  - `catalyst-radar priced-in-source-batches --source ... --stocks-only --all --json`
  - `catalyst-radar priced-in-source-batches --source ... --stocks-only --execute-next`
- No Polygon/Massive, Schwab, SEC, OpenAI, or broker/order execution was run.

Current live zero-call observation from local CLI:

```text
priced_in_source_batches source=catalyst_events status=ready gap_rows=5512 plannable=5510 batch_size=5 total_batches=1102 external_calls=0
scan_scope=mode=full_scan stocks_only=true gap_rows=5512 plannable=5510 returned_tickers=5 batch_sample=true
approval_checklist=required=true provider=sec calls=5 trade_orders=false command=catalyst-radar priced-in-source-batches --source catalyst_events --stocks-only --execute-next
blocked_examples=FRBA,SSBI reason=missing_cik
all_batches=catalyst-radar priced-in-source-batches --source catalyst_events --stocks-only --all --json
```

```text
priced_in_source_batch_overview status=ready sources=6 ready_sources=2 blocked_sources=2 gap_rows=22062 external_calls=0
full_scan=mode=full_scan active=12613 scanned=12087 ranked=5521 stocks_only=true source_gap_rows=22062 examples_are_samples=true
coverage_first=source=catalyst_events gaps=5512 calls=5 command=catalyst-radar priced-in-source-batches --source catalyst_events --stocks-only --execute-next
decision_shortcut=source=broker_context decision=5 actionable=5 calls=1 command=catalyst-radar priced-in-source-batches --source broker_context --stocks-only --execute-next
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_plans_safe_sync_batches tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_queue_cli_outputs_same_zero_call_signal tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_audit_cli_outputs_full_scan_audit tests\integration\test_api_routes.py::test_get_radar_priced_in_source_batches_returns_zero_call_plan tests\integration\test_api_routes.py::test_get_radar_priced_in_source_batches_can_return_all_source_overview tests\integration\test_api_routes.py::test_post_radar_priced_in_source_batch_execute_next_runs_one_chunk tests\integration\test_api_routes.py::test_post_radar_priced_in_source_batch_execute_next_can_run_capped_batches -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\dashboard\source_batches.py src\catalyst_radar\cli.py src\catalyst_radar\api\routes\radar.py src\catalyst_radar\dashboard\tui.py tests\integration\test_dashboard_demo_seed_cli.py tests\integration\test_api_routes.py tests\integration\test_dashboard_data.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-source-batches --source catalyst_events --stocks-only --batch-limit 1
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-source-batches --source all --stocks-only --limit 1
.\.venv\Scripts\python.exe -m catalyst_radar.cli dashboard-tui --once --page ops
```

Observed:

- Focused test set passed (`7 passed`).
- Ruff passed.
- `git diff --check` passed.
- CLI source-batch planning and all-source overview made `external_calls=0`.
- TUI Ops one-shot rendered and made `External calls made: 0`.

Next useful product action:

- Stay focused on evidence coverage for the stock scan. The first coverage-first
  source is `catalyst_events`, with `5512` stock-row gaps and `5510` plannable
  SEC targets. The first chunk is intentionally capped to `5` SEC calls.
- Do not execute the SEC chunk unless the operator explicitly approves that
  provider budget:
  `catalyst-radar priced-in-source-batches --source catalyst_events --stocks-only --execute-next`
- Before running the SEC chunk, inspect:
  `catalyst-radar priced-in-source-batches --source catalyst_events --stocks-only --all --json`
  and decide whether the `5`-call first chunk is intentional.

## Latest Stocks-Only Priced-In Filter

Goal alignment check:

- The user asked for MarketRadar to identify whether any stock has not fully
  matched market expectations.
- The full active-universe scan intentionally includes non-stock instruments,
  but the operator needs a direct stock-only lens over the same local ranked
  scan.
- This slice adds that lens without changing the underlying scan, without
  shrinking the stored universe, and without making provider calls.

Fix in this slice:

- `priced_in_queue_payload` now accepts `stocks_only=True` and filters ranked
  local rows to company-like instruments (`CS`, `ADRC`).
- `priced_in_answer_payload` and `priced_in_full_scan_audit_payload` accept the
  same flag and preserve it in commands, scope metadata, and export commands.
- CLI:
  - `catalyst-radar priced-in-queue --stocks-only`
  - `catalyst-radar priced-in-answer --stocks-only`
  - `catalyst-radar priced-in-audit --stocks-only`
- API:
  - `GET /api/radar/priced-in?stocks_only=true`
  - `GET /api/radar/priced-in/answer?stocks_only=true`
  - `GET /api/radar/priced-in/audit?stocks_only=true`
- Streamlit **Priced-in Full Scan** now has a **Stocks only** checkbox.
- TUI command mode now accepts `stocks`, `stock`, `stocks-only`, or
  `stocks_only`; `full` returns to all instruments.
- No Polygon/Massive, Schwab, SEC, OpenAI, or broker/order execution was run.

Current live zero-call observation from local CLI:

```text
cli_stocks_only count=3 total=5521 calls=0 first=A,MSFT,AAMI
audit_stocks_only scope=stocks_only ranked=5521 preview=1 command=catalyst-radar priced-in-audit --stocks-only --all --json calls=0
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_queue_payload_reports_full_scan_instrument_scope tests\integration\test_api_routes.py::test_get_radar_priced_in_queue_returns_cli_ready_rows tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_queue_cli_outputs_same_zero_call_signal tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_audit_cli_outputs_full_scan_audit tests\integration\test_dashboard_entrypoint.py::test_dashboard_wires_priced_in_full_scan_panel_after_usefulness -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\cli.py src\catalyst_radar\api\routes\radar.py src\catalyst_radar\dashboard\tui.py apps\dashboard\Home.py tests\integration\test_dashboard_data.py tests\integration\test_api_routes.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-queue --stocks-only --limit 3 --json
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-audit --stocks-only --limit 1 --json
```

Observed:

- Focused test set passed (`5 passed`).
- Ruff passed.
- `git diff --check` passed.
- Local CLI stock-only queue and audit both reported `external_calls_made=0`.

Next useful product action:

- Use `priced-in-queue --stocks-only --all --json` or the dashboard **Stocks
  only** checkbox to inspect the current stock-like ranked rows.
- This does not remove the market-bar blocker: `131` stock-like bars for
  `2026-05-15` are still missing before the stocks-only answer can be called
  complete.

## Latest Stock-First Manual Market-Bar Template

Goal alignment check:

- The previous slice showed the real local stock-only blocker:
  `131` missing stock-like bars out of `5652` stock-like active rows.
- The missing-only manual CSV still needed to be easier for a human to use:
  its first rows should be actual stocks, not wrappers, if the operator chooses
  the manual repair path instead of a live Polygon/Massive fill.

Fix in this slice:

- `catalyst-radar market-bars template --missing-only` now sorts rows as:
  stock-like (`ADRC`, `CS`) first, unknown type next, and non-stock/fund/wrapper
  rows last.
- The manual template payload and CLI output now include
  `row_order=stock_like_then_unknown_then_non_stock`.
- The CSV schema is unchanged; existing `security_type` and `template_reason`
  columns remain the human hints.
- No Polygon/Massive, Schwab, SEC, OpenAI, or broker/order execution was run.

Current live zero-call observation from local DB:

```text
manual_market_bars_template status=ready rows=523 scope=missing_as_of_bars expected_as_of=2026-05-15 external_calls=0
coverage=active=12613 existing=12090 missing=523 missing_only=true
row_order=stock_like_then_unknown_then_non_stock
first_rows=AACO:CS, ADAC:CS, ADXN:ADRC, AEAQ:CS, AGM.A:CS, AIRT:CS, ALOV:CS, ARCI:CS
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_provider_ingest_cli.py::test_market_bars_template_uses_database_active_universe tests\integration\test_provider_ingest_cli.py::test_market_bars_missing_only_template_import_counts_existing_bars tests\integration\test_provider_ingest_cli.py::test_market_bars_template_sorts_stock_like_rows_first tests\integration\test_api_routes.py::test_post_radar_market_bars_template_and_import_use_database_universe -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\market\manual_bars.py src\catalyst_radar\cli.py tests\integration\test_provider_ingest_cli.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli market-bars template --expected-as-of 2026-05-15 --out <temp-csv> --missing-only
```

Observed:

- Focused test set passed (`4 passed`).
- Ruff passed.
- `git diff --check` passed.
- Live temp CSV first rows are stock-like (`CS`/`ADRC`).

Next useful product action:

- If live provider execution is not approved, generate the missing-only CSV and
  fill rows from the top. The first 131 missing rows are the stock-like blockers
  for a stocks-only priced-in answer.
- If the operator explicitly approves one provider call, the guarded
  Polygon/Massive grouped-daily command remains the faster full-date fill.

## Latest Stocks-Only Market-Bar Scope

Goal alignment check:

- The user's goal is specifically to find stocks whose price has not yet
  matched market expectations.
- The full active-universe scan also contains ETFs/funds/wrappers/rights/
  warrants/preferreds. Blocking a stock answer on every non-stock wrapper row is
  too vague for the operator.
- This slice keeps the full active-universe answer honest while adding a
  separate stocks-only market-bar coverage boundary.

Fix in this slice:

- `priced_in_full_scan_audit_payload.market_bars.repair` now includes
  `stock_scope` with schema `priced-in-market-bar-stock-scope-v1`.
- The stock scope reports:
  - company-like security types used for the stocks-only boundary (`ADRC`, `CS`);
  - stock-like active count;
  - stock-like bars present for the scan date;
  - stock-like missing as-of bars;
  - stock-like coverage percentage;
  - non-stock and unknown missing bar counts;
  - missing stock-like samples;
  - `external_calls_made=0`;
  - an explicit answer boundary explaining that this does not complete the full
    active-universe answer.
- CLI `priced-in-audit` now prints `stock_bar_scope`.
- Streamlit **Priced-in Full Scan** now shows **Stocks-only bar coverage** inside
  the market-bar repair panel.
- TUI full-scan audit summary now includes stock-bar coverage when the audit
  payload provides it.
- The prior `priced-in-preflight` test expectation was updated for the guarded
  Polygon/Massive `--confirm-external-call` command.
- No Polygon/Massive, Schwab, SEC, OpenAI, or broker/order execution was run.

Current live zero-call observation from local DB:

```text
missing_bar_diagnostic=status=attention company_like=131 fund_like=4 wrappers=388 unknown=0 external_calls=0
stock_bar_scope=status=attention coverage=5521/5652 missing=131 non_stock_missing=392 unknown_missing=0 external_calls=0
provider_command=catalyst-radar ingest-polygon grouped-daily --date 2026-05-15 --confirm-external-call
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_full_scan_audit_payload_consolidates_current_state tests\integration\test_dashboard_data.py::test_priced_in_full_scan_audit_reports_stock_only_bar_coverage tests\integration\test_dashboard_data.py::test_priced_in_preflight_payload_reports_exact_next_steps tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_audit_cli_outputs_full_scan_audit tests\integration\test_dashboard_entrypoint.py::test_dashboard_wires_priced_in_full_scan_panel_after_usefulness -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\cli.py src\catalyst_radar\dashboard\tui.py apps\dashboard\Home.py tests\integration\test_dashboard_data.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-audit --limit 1
```

Observed:

- Focused test set passed (`5 passed`).
- Ruff passed.
- `git diff --check` passed.
- Live zero-call CLI audit printed the stock-bar scope above.

Next useful product action:

- For a stocks-only answer, the first concrete data blocker is now the `131`
  missing stock-like bars for `2026-05-15`.
- If the operator explicitly approves one provider call, the guarded
  Polygon/Massive command should fill both stock and non-stock missing bars for
  the scan date:
  `catalyst-radar ingest-polygon grouped-daily --date 2026-05-15 --confirm-external-call`.
- Without live provider approval, use the missing-only manual CSV template and
  prioritize the `sample_missing_stock_like_tickers` stock-like rows first.

## Latest Polygon/Massive Confirmation Guard

Goal alignment check:

- The product goal is not a pretty dashboard by itself. The goal is a
  full-market priced-in scan: find stocks where price has not yet matched
  market expectations.
- The current hard blocker remains daily market-bar coverage for the full active
  universe. The latest zero-call audit still plans a one-call Polygon/Massive
  grouped-daily fill for `2026-05-15`.
- The dashboard/CLI work in this slice is useful only because it makes the
  market-data fill path explicit and prevents accidental live provider calls.

Problem fixed in this slice:

- The provider fill plan previously surfaced a copy/paste command that could
  call Polygon/Massive immediately:
  `catalyst-radar ingest-polygon grouped-daily --date 2026-05-15`.
- That violated the no-surprise-provider-call boundary. A live operator command
  should fail unless the operator adds an explicit confirmation flag.

Fix in this slice:

- `catalyst-radar ingest-polygon grouped-daily` and
  `catalyst-radar ingest-polygon tickers` now accept
  `--confirm-external-call`.
- If a real Polygon/Massive key is configured and no `--fixture` is provided,
  the CLI exits with code `2` unless `--confirm-external-call` is present.
- Fixture ingests remain unchanged and do not require the confirmation flag.
- Missing or placeholder Polygon/Massive keys still use the existing fail-closed
  provider job path before any network request is possible.
- `priced_in_full_scan_audit_payload.market_bars.repair.provider_fill_plan`
  now surfaces the guarded command:
  `catalyst-radar ingest-polygon grouped-daily --date 2026-05-15 --confirm-external-call`.
- `priced-in-preflight`, universe coverage guidance, README examples,
  `scripts/run-full-market-scan.ps1`, and the radar runbook now show guarded
  Polygon/Massive commands.
- No Polygon/Massive, Schwab, SEC, OpenAI, or broker/order execution was run.

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_polygon_ingest_cli.py::test_polygon_ingest_requires_api_key tests\integration\test_polygon_ingest_cli.py::test_polygon_ingest_rejects_placeholder_api_key tests\integration\test_polygon_ingest_cli.py::test_polygon_live_ingest_requires_explicit_confirmation tests\integration\test_polygon_ingest_cli.py::test_polygon_fixture_ingest_persists_raw_normalized_and_daily_bars tests\integration\test_polygon_ingest_cli.py::test_polygon_fixture_ingest_does_not_require_real_api_key tests\integration\test_dashboard_data.py::test_priced_in_full_scan_audit_payload_consolidates_current_state tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_audit_cli_outputs_full_scan_audit tests\integration\test_local_scripts.py -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\cli.py src\catalyst_radar\dashboard\data.py tests\integration\test_polygon_ingest_cli.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli ingest-polygon grouped-daily --date 2026-05-15
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-audit --limit 1 --json
```

Observed:

- Focused test set passed (`22 passed`).
- Ruff passed.
- `git diff --check` passed.
- Live unconfirmed Polygon/Massive command exited `2` and printed:
  `polygon ingest requires --confirm-external-call for live provider requests`.
- The zero-call audit still reports `external_calls=0` and now returns:
  `provider_command=catalyst-radar ingest-polygon grouped-daily --date 2026-05-15 --confirm-external-call`.

Next useful product action:

- If the operator explicitly approves one market-data provider call, run:
  `catalyst-radar ingest-polygon grouped-daily --date 2026-05-15 --confirm-external-call`.
- After the provider fill, rerun the priced-in full-scan audit and ranking from
  updated local bars. The old audit is not automatically revalidated.
- If live provider execution is not approved, use the missing-only manual CSV
  repair path from the previous section.

## Latest Market-Bar Provider Fill Plan

Current problem:

- The full scan still has `523` missing bars for `2026-05-15`.
- The missing-only CSV path is now practical, but the dashboard/CLI did not
  tell the operator whether the configured Polygon/Massive key could fill the
  same date with a single grouped-daily request.
- The project also needs to avoid surprise provider calls. A command that can
  call Polygon/Massive must be shown as a plan with an explicit approval
  boundary, not executed implicitly.

Fix in this slice:

- `priced_in_full_scan_audit_payload.market_bars.repair` now includes a
  `provider_fill_plan` object with schema
  `priced-in-market-bar-provider-fill-plan-v1`.
- The provider plan reports:
  - provider label `Polygon/Massive grouped daily`;
  - target as-of date;
  - missing bar count;
  - whether a real Polygon/Massive key is configured;
  - provider command;
  - manual missing-only template command;
  - `execute_external_call_count`;
  - `external_calls_made=0`;
  - explicit approval and point-in-time boundaries.
- CLI `priced-in-audit` now prints the provider fill plan under
  `market_bar_repair`.
- Streamlit **Priced-in Full Scan** now surfaces a **Polygon/Massive Fill Plan**
  with **Execute Calls**, key state, approval boundary, provider command, and
  missing-only manual fallback.
- No Polygon/Massive, Schwab, SEC, OpenAI, or broker/order execution was run.

Current live zero-call observations from the branch:

```text
provider_fill_plan=provider=Polygon/Massive grouped daily status=ready_for_approval execute_calls=1 key_configured=true external_calls=0
  provider_command=catalyst-radar ingest-polygon grouped-daily --date 2026-05-15
  manual_template=catalyst-radar market-bars template --expected-as-of 2026-05-15 --out data\local\manual-bars-2026-05-15.csv --missing-only
  approval_boundary=This plan makes 0 provider calls. The provider command makes one Polygon/Massive grouped-daily request and must only be run after explicit operator approval.
```

```text
api_provider_fill_plan status=ready_for_approval execute_calls=1 key_configured=True external_calls=0 command=catalyst-radar ingest-polygon grouped-daily --date 2026-05-15
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_full_scan_audit_payload_consolidates_current_state tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_audit_cli_outputs_full_scan_audit tests\integration\test_dashboard_entrypoint.py::test_dashboard_wires_priced_in_full_scan_panel_after_usefulness -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\cli.py apps\dashboard\Home.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py tests\integration\test_dashboard_entrypoint.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-audit --limit 1
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-audit --limit 1 --json
```

Observed:

- Focused three-test set passed (`3 passed`).
- Ruff passed.
- `git diff --check` passed.
- CLI text and JSON checks reported:
  - `status=ready_for_approval`;
  - `execute_calls=1`;
  - key configured `true`;
  - `external_calls_made=0`;
  - provider command `catalyst-radar ingest-polygon grouped-daily --date 2026-05-15`.
- Local services were restarted from the branch for API/dashboard verification.
- API health returned commit `adb1a4434b4c`.
- Streamlit health returned `ok`.
- API `GET /api/radar/priced-in/audit?limit=1` reported the same provider fill
  plan with `external_calls_made=0`.
- Browser verification on `http://127.0.0.1:8514` confirmed the dashboard text
  included:
  - `Polygon/Massive Fill Plan`;
  - `Execute Calls`;
  - `This plan makes 0 provider calls`;
  - `catalyst-radar ingest-polygon grouped-daily --date 2026-05-15`;
  - `--missing-only`.
- Browser console check reported 0 current errors.

Next useful product action:

- If the operator explicitly approves one market-data provider call, run:
  `catalyst-radar ingest-polygon grouped-daily --date 2026-05-15`.
- After provider fill, rerun the scan/audit from updated local bars; do not
  treat the older audit as automatically revalidated.
- If provider execution is not approved, use the missing-only manual CSV path.

## Latest Missing-Only Market-Bar Repair Path

Current problem:

- The previous slice made the missing market-bar blocker visible and classified:
  `523` missing as-of bars for `2026-05-15`.
- The existing manual repair command generated a full active-universe template.
  On the live DB that means `12613` rows, even though only `523` bars are
  missing.
- The import preview also expected the CSV itself to cover the full active
  universe, so a practical missing-only CSV would still preview as incomplete
  even when the local DB already held the other `12090` bars.
- That was safe but not useful enough for the operator: resolving a 523-row gap
  should not require editing or importing a 12613-row file.

Fix in this slice:

- `catalyst-radar market-bars template` now supports `--missing-only`.
- The API request for `POST /api/radar/market-bars/template` now supports
  `missing_only`.
- Template payloads now report:
  - `template_scope`;
  - `row_count`;
  - `active_security_count`;
  - `existing_as_of_bar_count`;
  - `missing_as_of_bar_count`;
  - `missing_only`;
  - `external_calls_made=0`.
- Manual template CSVs now include helper columns:
  - `security_type`;
  - `template_reason`.
- Manual import preview now validates expected coverage as:
  - existing DB bars at `expected_as_of`;
  - plus tickers present in the CSV for `expected_as_of`.
- This allows a missing-only filled CSV to preview as ready when it completes
  the local active-universe coverage.
- No Polygon/Massive, Schwab, SEC, OpenAI, or broker/order execution was run.

Current live zero-call observation from the branch:

```text
live_missing_template status=ready scope=missing_as_of_bars rows=523 active=12613 existing=12090 missing=523 calls=0 first=AACBR,AACBU,AACIW,AACO,AACOU
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_provider_ingest_cli.py::test_market_bars_template_uses_database_active_universe tests\integration\test_provider_ingest_cli.py::test_market_bars_import_requires_expected_full_active_coverage tests\integration\test_provider_ingest_cli.py::test_market_bars_missing_only_template_import_counts_existing_bars tests\integration\test_provider_ingest_cli.py::test_market_bars_import_executes_without_securities_csv tests\integration\test_api_routes.py::test_post_radar_market_bars_template_and_import_use_database_universe -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\market\manual_bars.py src\catalyst_radar\cli.py src\catalyst_radar\api\routes\radar.py tests\integration\test_provider_ingest_cli.py tests\integration\test_api_routes.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli market-bars template --expected-as-of 2026-05-15 --out <temp-csv> --missing-only --json
```

Observed:

- Focused five-test set passed (`5 passed`).
- Ruff passed.
- `git diff --check` passed.
- Live missing-only template generation reported:
  - `row_count=523`;
  - `active_security_count=12613`;
  - `existing_as_of_bar_count=12090`;
  - `missing_as_of_bar_count=523`;
  - `external_calls_made=0`.

Next useful product action:

- The missing-bar blocker now has a practical local repair file:
  `catalyst-radar market-bars template --expected-as-of 2026-05-15 --out <file> --missing-only`.
- Filling that CSV still requires a trusted market data source. Do not call
  Polygon/Massive or another provider unless the operator explicitly approves.
- After the CSV is filled, preview it with:
  `catalyst-radar market-bars import --daily-bars <file> --expected-as-of 2026-05-15`.
- Execute only after preview reports `missing=0`:
  `catalyst-radar market-bars import --daily-bars <file> --expected-as-of 2026-05-15 --execute`.

## Latest Market-Bar Missing Diagnostics

Current problem:

- MarketRadar's stated goal is a full-market priced-in scan: determine whether
  a stock's price has caught up with market expectations.
- Price-reaction scoring depends on point-in-time daily bars for the scan date.
- The full active-universe audit currently sees broad but incomplete market-bar
  coverage:
  - active securities: `12613`;
  - scanned/ranked rows: `12087`;
  - target as-of date: `2026-05-15`;
  - active tickers missing as-of bars: `523`;
  - external calls while inspecting: `0`.
- Before this slice, the repair path showed a missing ticker sample and manual
  template/import commands, but it did not explain which missing rows are actual
  stock/company rows versus funds, preferreds, rights, units, or warrants.
- That made the next action too vague: a human operator could not tell whether
  the missing bars should be filled as companies, handled as non-company
  instruments, or excluded from a stocks-only answer.

Fix in this slice:

- `priced_in_full_scan_audit_payload.market_bars.repair` now includes a
  `diagnostic` object with schema
  `priced-in-market-bar-missing-diagnostic-v1`.
- The diagnostic is zero-call and classifies missing as-of bars from local
  `securities.metadata.type` plus local `daily_bars` coverage.
- The diagnostic schema is stable even when the scan/as-of date is unavailable
  or no missing rows exist; this keeps CLI/API/dashboard consumers from special
  casing a `null` diagnostic.
- CLI `priced-in-audit` now prints:
  - diagnostic status;
  - company-like/fund-like/wrapper/unknown missing counts;
  - security-type counts;
  - sample tickers by category;
  - the route boundary that explains full active-universe versus stocks-only
    behavior.
- Streamlit **Priced-in Full Scan** now surfaces the same diagnostic in the
  market-bar repair area with human labels such as **Company-like Missing** and
  **Wrapper Missing**.
- No Polygon/Massive, Schwab, SEC, OpenAI, or broker/order execution was run.

Current live zero-call observations from the branch:

```text
missing_bar_diagnostic=status=attention company_like=131 fund_like=4 wrappers=388 unknown=0 external_calls=0
  missing_bar_types=ADRC:8,CS:123,ETF:1,ETV:1,FUND:2,PFD:14,RIGHT:47,SP:6,UNIT:176,WARRANT:145
  sample_company_like_tickers=AACO,ADAC,ADXN,AEAQ,AGM.A sample_fund_like_tickers=CTWO,GRF,MXE,ZTAX sample_wrapper_tickers=AACBR,AACBU,AACIW,AACOU,AACOW
  route_boundary=Market bars are required for price-reaction scoring. Non-company instruments can stay in a full active-universe scan only if their own bars are present; otherwise route or exclude them from a stocks-only answer.
```

```text
cli_json_market_bar_diagnostic status=attention missing=523 company_like=131 fund_like=4 wrappers=388 unknown=0 calls=0 schema=priced-in-market-bar-missing-diagnostic-v1
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_full_scan_audit_payload_consolidates_current_state tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_audit_cli_outputs_full_scan_audit tests\integration\test_dashboard_entrypoint.py::test_dashboard_wires_priced_in_full_scan_panel_after_usefulness -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\cli.py apps\dashboard\Home.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py tests\integration\test_dashboard_entrypoint.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-audit --limit 1
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-audit --limit 1 --json
```

Observed:

- Focused three-test set passed (`3 passed`).
- Ruff passed.
- `git diff --check` passed.
- CLI text and JSON checks reported the market-bar diagnostic with
  `external_calls_made=0`.
- Local services were restarted from the branch for API/dashboard verification.
- API health returned commit `d99502a378c5`.
- Streamlit health returned `ok`.
- API `GET /api/radar/priced-in/audit?limit=1` reported:
  - `status=attention`;
  - `missing=523`;
  - `company_like=131`;
  - `fund_like=4`;
  - `wrappers=388`;
  - `unknown=0`;
  - `calls=0`;
  - schema `priced-in-market-bar-missing-diagnostic-v1`.
- Browser verification on `http://127.0.0.1:8514` confirmed the dashboard text
  included:
  - `Company-like Missing`;
  - `Wrapper Missing`;
  - `Market bars are required for price-reaction scoring`;
  - `523`;
  - `131`;
  - `388`.
- Browser console check reported 0 current errors.

Next useful product action:

- This slice makes the market-bar blocker actionable; it does not fill bars.
- The next non-drift action is to resolve market-bar coverage before trusting
  the full scan as a price-reaction answer:
  - generate a local missing-bar template for `2026-05-15`;
  - fill/import all 523 missing bars from a user-approved source; or
  - explicitly route/exclude non-company instruments for a stocks-only answer.
- Provider calls remain a separate operator decision. Do not run Polygon/Massive
  or other live market-data calls without explicit approval.

## Latest Local Text Repair Surface

Current problem:

- Local text intelligence remains sparse and is not independently runnable for
  most full-scan rows.
- The source-batch plan already knew local text was blocked by missing
  catalyst event text, but the main priced-in audit/dashboard only showed a
  generic "run text intelligence" action.
- That was misleading because local text cannot score rows whose
  `catalyst_events` evidence has not been filled or routed.
- Current local zero-call state observed before this slice:
  - `local_text` source coverage: `12/12087`;
  - local text gap rows: `12075`;
  - local text plannable rows: `0`;
  - diagnostic: `missing_catalyst_events`;
  - prerequisite source: `catalyst_events`;
  - external calls while inspecting: `0`.

Fix in this slice:

- `priced_in_full_scan_audit_payload.sources[]` now carries a local-text
  `repair` object when local text rows have missing/stale evidence.
- The repair schema reuses `priced-in-source-gap-repair-v1` and includes:
  - source;
  - blocked status;
  - diagnostic status `missing_catalyst_events`;
  - full-scan gap count;
  - sample tickers;
  - `provider_batch_allowed=false`;
  - prerequisite source `catalyst_events`;
  - prerequisite batch-plan command;
  - zero-call review/export commands;
  - local-text batch-plan command;
  - current-context boundary;
  - write/provider-call boundary;
  - `external_calls_made=0`;
  - usefulness impact.
- CLI `priced-in-audit` now prints local-text source repair details under the
  `local_text` source row, including:
  - `diagnostic=missing_catalyst_events`;
  - `provider_batch_allowed=false`;
  - prerequisite catalyst-events batch-plan command;
  - zero-call planning/local-write boundary.
- Streamlit **Priced-in Source Gaps** rows now include the local-text
  prerequisite source and blocked repair status.
- No SEC, Schwab, Polygon/Massive, OpenAI, or broker/order execution was run.

Current live zero-call observations from the branch:

```text
local_text_repair source=local_text status=blocked diagnostic=missing_catalyst_events prereq=catalyst_events allowed=False calls=0
```

```text
- local_text status=partial coverage=12/12087 gap_rows=12075 decision=0 research=0 actionable=0 next=Run text intelligence for the scan date before relying on narrative strength.
  repair=status=blocked diagnostic=missing_catalyst_events provider_batch_allowed=false next=Fill catalyst_events first; local text can only process rows with stored event text.
    prerequisite=catalyst-radar priced-in-source-batches --source catalyst_events --all --json
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_full_scan_audit_payload_consolidates_current_state tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_audit_cli_outputs_full_scan_audit tests\integration\test_dashboard_entrypoint.py::test_dashboard_wires_priced_in_full_scan_panel_after_usefulness tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_plans_local_text_batches -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\cli.py apps\dashboard\Home.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py tests\integration\test_dashboard_entrypoint.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-audit --limit 1
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-audit --limit 1 --json
```

Observed:

- Focused four-test set passed (`4 passed`).
- Ruff passed.
- `git diff --check` passed.
- CLI and JSON checks reported local-text repair with `external_calls_made=0`.
- Local services were restarted from the branch for API/dashboard verification.
- API `GET /api/radar/priced-in/audit?limit=1` reported:
  - `source=local_text`;
  - `status=blocked`;
  - `diagnostic=missing_catalyst_events`;
  - `prereq=catalyst_events`;
  - `allowed=False`;
  - `calls=0`.
- Browser verification on `http://127.0.0.1:8514` confirmed the dashboard text
  included:
  - `missing_catalyst_events`;
  - `local_text`;
  - `catalyst_events`;
  - **Fill catalyst_events first**.
- Browser console check reported 0 current errors.

Next useful product action:

- This slice explains why local text is blocked; it does not execute catalyst or
  local-text batches.
- The first hard trust blocker remains market-bar coverage:
  - `523` active tickers are missing as-of bars for `2026-05-15`.
- The scan can rank price reaction, but the emotion evidence path still needs:
  - complete market bars;
  - catalyst event fill/routing;
  - then local text intelligence over stored event text.

## Latest Catalyst Evidence Repair Surface

Current problem:

- Full-market ranked rows still have a broad catalyst-event evidence gap.
- Catalyst events matter because they explain the market-emotion side of the
  price-vs-expectation question; local text intelligence is also blocked for
  rows without stored event text.
- The detailed source-batch command already separated company-like SEC catalyst
  rows from ETF/fund/wrapper rows, but the main priced-in audit/dashboard did
  not put that routing beside the answer.
- Current local zero-call state observed before this slice:
  - `catalyst_events` source coverage: `9/5521`;
  - company-like catalyst gap rows: `5512`;
  - routed non-company gap rows: `6563`;
  - next SEC batch plan: `1102` batches of up to `5` ticker(s);
  - next batch preview calls if executed: `5` SEC calls;
  - external calls while inspecting: `0`.

Fix in this slice:

- `priced_in_full_scan_audit_payload.sources[]` now carries a catalyst-events
  `repair` object when catalyst evidence rows have missing/stale evidence.
- The repair schema reuses `priced-in-source-gap-repair-v1` and includes:
  - source;
  - status;
  - diagnostic status;
  - full-scan/company-like catalyst gap counts;
  - routed non-company gap counts;
  - company-like/non-company row counts;
  - sample company-like gap tickers;
  - sample routed non-company tickers;
  - whether a provider batch is allowed;
  - zero-call review/export commands;
  - batch plan command and API;
  - non-company evidence route;
  - current-context boundary;
  - provider/write boundary;
  - `external_calls_made=0`;
  - usefulness impact that local text remains blocked until event text exists.
- CLI `priced-in-audit` now prints catalyst source repair details under the
  `catalyst_events` source row, including:
  - `diagnostic=company_like_sec_and_non_company_routes`;
  - `provider_batch_allowed=true`;
  - `non_company_route=...`;
  - catalyst batch-plan command;
  - zero-call planning/provider execution boundary.
- Streamlit **Priced-in Source Gaps** rows now include repair columns for
  company-like catalyst gaps and routed non-company rows.
- No SEC, Schwab, Polygon/Massive, OpenAI, or broker/order execution was run.

Current live zero-call observations from the branch:

```text
catalyst_repair source=catalyst_events status=attention diagnostic=company_like_sec_and_non_company_routes company_like=5512 routed=6563 allowed=True calls=0 batch=catalyst-radar priced-in-source-batches --source catalyst_events --all --json
```

```text
- catalyst_events status=partial coverage=9/5521 gap_rows=5512 decision=0 research=0 actionable=0 next=Fill SEC catalyst events for company-like rows; route ETF/fund/wrapper rows to underlying, theme, fund-flow, or similar non-company evidence.
  repair=status=attention diagnostic=company_like_sec_and_non_company_routes provider_batch_allowed=true next=Fill SEC catalyst events for company-like rows; route ETF/fund/wrapper rows to underlying, theme, fund-flow, or similar non-company evidence.
    non_company_route=Use fund, underlying, theme, sector, flow, or constituent evidence instead of SEC company filing batches.
    batch_plan=catalyst-radar priced-in-source-batches --source catalyst_events --all --json
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_full_scan_audit_payload_consolidates_current_state tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_audit_cli_outputs_full_scan_audit tests\integration\test_dashboard_entrypoint.py::test_dashboard_wires_priced_in_full_scan_panel_after_usefulness -q
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_exposes_missing_cik_blockers tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_classifies_non_company_cik_gaps tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_plans_local_text_batches tests\integration\test_api_routes.py::test_get_radar_priced_in_source_batches_returns_zero_call_plan -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\cli.py apps\dashboard\Home.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py tests\integration\test_dashboard_entrypoint.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-audit --limit 1
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-audit --limit 1 --json
```

Observed:

- Focused three-test set passed (`3 passed`).
- Source-batch/API four-test set passed (`4 passed`).
- Ruff passed.
- `git diff --check` passed.
- CLI and JSON checks reported catalyst repair with `external_calls_made=0`.
- Local services were restarted from the branch for API/dashboard verification.
- API `GET /api/radar/priced-in/audit?limit=1` reported:
  - `source=catalyst_events`;
  - `status=attention`;
  - `diagnostic=company_like_sec_and_non_company_routes`;
  - `company_like=5512`;
  - `routed=6563`;
  - `allowed=True`;
  - `calls=0`.
- Browser verification on `http://127.0.0.1:8514` confirmed the dashboard text
  included:
  - `company_like_sec_and_non_company_routes`;
  - `5512`;
  - `6563`;
  - the catalyst-events source row.
- Browser console check reported 0 current errors.
- PR #356, `Surface catalyst evidence repair`, was merged by rebase as
  `d98256e39e6d`.
- Local services were restarted after merge from `main`:
  - API health returned commit `d98256e39e6d`;
  - Streamlit health returned `ok`.
- Post-merge API verification showed:
  - `post_merge_catalyst_repair source=catalyst_events`;
  - `status=attention`;
  - `diagnostic=company_like_sec_and_non_company_routes`;
  - `company_like=5512`;
  - `routed=6563`;
  - `allowed=True`;
  - `calls=0`.
- Post-merge browser verification showed the dashboard text included:
  - `company_like_sec_and_non_company_routes`;
  - `5512.00`;
  - `6563.00`;
  - `catalyst_events`.
- Post-merge browser console check reported 0 current errors.

Next useful product action:

- This slice explains catalyst-event routing and why local text remains blocked;
  it does not execute SEC batches.
- The first hard trust blocker remains market-bar coverage:
  - `523` active tickers are missing as-of bars for `2026-05-15`.
- Source-fill execution is still a separate operator decision. For
  catalyst_events, `priced-in-source-batches --source catalyst_events --all
  --json` shows the full zero-call plan, and `--execute-next` would make a capped
  SEC call batch only after explicit approval.

## Latest Options Evidence Repair Surface

Current problem:

- The full scan correctly ranks the active universe first, but the highest
  payoff evidence gap is currently options coverage.
- The detailed source-batch command already knew when options were blocked
  because stored option features were newer than the scan date, but the main
  priced-in audit/dashboard did not put that diagnosis directly beside the
  answer.
- That made the main answer less useful: the operator could see
  `options 0/12087`, but had to know which secondary command explained whether
  to run a source batch, rerun a current scan, or ingest point-in-time options.
- Current local state observed before this slice:
  - full scan ranked rows: `12087`;
  - active securities: `12613`;
  - options coverage: `0/12087`;
  - current options diagnostic: `newer_than_scan`;
  - provider batch allowed for this scan date: `false`;
  - external calls while inspecting: `0`.

Fix in this slice:

- `priced_in_full_scan_audit_payload.sources[]` now carries an options
  `repair` object when options rows have missing/stale evidence.
- The repair schema is `priced-in-source-gap-repair-v1` and includes:
  - source;
  - status;
  - diagnostic status;
  - full-scan gap count;
  - scan as-of date(s);
  - sample tickers;
  - whether a provider batch is currently allowed;
  - zero-call review/export commands;
  - batch plan command;
  - point-in-time fixture import command;
  - current-context boundary;
  - write/provider-call boundary;
  - `external_calls_made=0`;
  - usefulness impact.
- `recommended_source_gap.repair` is included when the recommended source gap is
  options. It is omitted for non-options recommendations instead of returning an
  empty object.
- CLI `priced-in-audit` now prints:
  - `source_gap_repair=...` under the recommended source gap when options is the
    recommendation;
  - `repair=...` under the options source row in all cases where options has a
    repair path;
  - `point_in_time_import=...`;
  - the explicit no-hidden-provider-call/write boundary.
- Streamlit **Priced-in Full Scan** now shows an **Evidence repair** block when
  the recommended source gap has a repair object.
- Streamlit **Priced-in Source Gaps** rows now include options repair status,
  diagnostic, and next action.
- No provider/source execution was run.

Current live zero-call observations from the branch:

```text
recommended_source_gap=source=options decision=10 actionable=12 research=0 gap_rows=12087 review=catalyst-radar priced-in-audit --source-gap options --limit 25
  source_gap_repair=source=options status=blocked diagnostic=newer_than_scan provider_batch_allowed=false external_calls=0
    point_in_time_import=catalyst-radar ingest-options --fixture <point-in-time-options-2026-05-15.json>
```

```text
options repair:
status=blocked
diagnostic_status=newer_than_scan
gap_count=12087
scan_as_of_dates=2026-05-15
provider_batch_allowed=false
external_calls_made=0
next_action=Stored options exist after this scan date. Example tickers: A, MSFT, AAAU, AAPL, AA. Rerun only with a current scan date and current bars, or ingest point-in-time options for the original scan date.
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_full_scan_audit_payload_consolidates_current_state tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_audit_cli_outputs_full_scan_audit tests\integration\test_dashboard_entrypoint.py::test_dashboard_wires_priced_in_full_scan_panel_after_usefulness tests\integration\test_dashboard_data.py::test_priced_in_queue_payload_diagnoses_options_after_scan_date tests\integration\test_dashboard_data.py::test_priced_in_all_source_gap_batches_blocks_options_shortcut_when_not_point_in_time tests\integration\test_api_routes.py::test_get_radar_priced_in_audit_returns_zero_call_audit -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\cli.py apps\dashboard\Home.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py tests\integration\test_dashboard_entrypoint.py tests\integration\test_api_routes.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-audit --limit 3
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-audit --limit 3 --json
```

Observed:

- Focused six-test set passed (`6 passed`).
- Ruff passed.
- `git diff --check` passed.
- CLI and JSON checks reported options repair with `external_calls_made=0`.
- Local services were restarted from the branch for API/dashboard verification.
- API `GET /api/radar/priced-in/audit?limit=3` reported:
  - `source=options`;
  - `status=blocked`;
  - `diagnostic=newer_than_scan`;
  - `provider_batch_allowed=false`;
  - `external_calls_made=0`;
  - point-in-time import command for `2026-05-15`.
- Browser verification on `http://127.0.0.1:8514` confirmed the dashboard text
  included:
  - **Priced-in Full Scan**;
  - **Full-Market Scan**;
  - **First source gap**;
  - **Evidence repair**;
  - `newer_than_scan`;
  - `point-in-time-options-2026-05-15.json`.
- Browser console check reported 0 current errors.
- PR #354, `Surface options evidence repair`, was merged by rebase as
  `d0b274cc5847`.
- Local services were restarted after merge from `main`:
  - API health returned commit `d0b274cc5847`;
  - Streamlit health returned `ok`.
- Post-merge API verification showed:
  - `post_merge_api_repair source=options`;
  - `status=blocked`;
  - `diagnostic=newer_than_scan`;
  - `allowed=False`;
  - `calls=0`;
  - point-in-time import command for `2026-05-15`.
- Post-merge browser verification showed the dashboard text included:
  - **Evidence repair**;
  - `point-in-time-options-2026-05-15.json`;
  - `newer_than_scan`;
  - **Current Schwab option chains**;
  - **Review/export/plan commands make 0 provider calls**.
- Post-merge browser console check reported 0 current errors.

Next useful product action:

- This slice explains why the options gap is not safely runnable for the older
  scan date. It does not fill options coverage.
- The first hard trust blocker is still market-bar coverage:
  - `523` active tickers are missing as-of bars for `2026-05-15`.
- After market bars are complete, decide one explicit options path:
  - ingest point-in-time option features for the original scan date; or
  - rerun a current scan with current bars and current read-only Schwab options.
- Continue avoiding UI-only polish unless it directly helps answer:
  "Which full-market rows show emotion not matched by price, and what evidence
  is missing before I can trust that?"

## Latest Full-Scan Market-Bar Repair Surface

Current problem:

- The full scan is now clearly primary, but the first trust blocker is still
  incomplete as-of market bars.
- `market-radar-status.ps1` showed the correct repair path, but the priced-in
  audit/dashboard did not put that path beside the full-scan answer.
- Current local state:
  - active securities: `12613`;
  - as-of bars available: `12090`;
  - missing as-of bars: `523`;
  - expected scan date: `2026-05-15`;
  - external calls while inspecting: `0`.

Fix in this slice:

- `priced_in_full_scan_audit_payload.market_bars` now includes a `repair`
  object.
- `market_bars.repair` schema is `priced-in-market-bar-repair-v1` and includes:
  - status;
  - target as-of date;
  - active/covered/missing counts;
  - missing ticker sample;
  - DB-backed template command;
  - import preview command;
  - import execute command;
  - matching API endpoints;
  - explicit write boundary;
  - `external_calls_made=0`.
- CLI `priced-in-audit` now prints `market_bar_repair=...` immediately after
  `market_bars=...`, including the sample missing tickers and template/import
  commands.
- Streamlit **Priced-in Full Scan** now shows a **Market bar coverage is
  incomplete** warning directly under **Full-Market Scan** when as-of coverage
  is incomplete.
- The dashboard warning shows:
  - missing count;
  - expected as-of date;
  - sample missing tickers;
  - template/import commands;
  - boundary that template/import are local and call no market providers.
- No provider/source execution was run.

Current live zero-call observations from the branch:

```text
market_bars=status=attention coverage=12090/12613 missing=523 coverage_pct=95.9
market_bar_repair=status=attention expected_as_of=2026-05-15 missing=523 sample=AACBR,AACBU,AACIW,AACO,AACOU,AACOW,ACAAU,ACAAW,ADAC,ADXN,AEAQ,AEAQU external_calls=0
template=catalyst-radar market-bars template --expected-as-of 2026-05-15 --out data\local\manual-bars-2026-05-15.csv
preview_import=catalyst-radar market-bars import --daily-bars <fresh-bars.csv> --expected-as-of 2026-05-15
execute_import=catalyst-radar market-bars import --daily-bars <fresh-bars.csv> --expected-as-of 2026-05-15 --execute
```

```text
GET /api/radar/priced-in/audit?limit=5
api_repair=attention expected=2026-05-15 missing=523 calls=0 sample=AACBR,AACBU,AACIW,AACO,AACOU,AACOW,ACAAU,ACAAW,ADAC,ADXN,AEAQ,AEAQU
```

Browser verification on `http://127.0.0.1:8514`:

- **Market bar coverage is incomplete** rendered.
- The warning showed `523 active ticker(s)`.
- **Missing as-of bar examples** rendered.
- The dated template command rendered.
- The dated import command rendered.
- The no-market-provider-call boundary rendered.
- Browser console check reported 0 current errors.

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_full_scan_audit_payload_consolidates_current_state tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_audit_cli_outputs_full_scan_audit tests\integration\test_dashboard_entrypoint.py::test_dashboard_wires_priced_in_full_scan_panel_after_usefulness -q
.\.venv\Scripts\python.exe -m pytest tests\integration\test_api_routes.py::test_get_radar_priced_in_audit_returns_zero_call_audit tests\integration\test_provider_ingest_cli.py::test_market_bars_template_uses_database_active_universe tests\integration\test_provider_ingest_cli.py::test_market_bars_import_requires_expected_full_active_coverage -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\cli.py apps\dashboard\Home.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py tests\integration\test_dashboard_entrypoint.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-audit --limit 5
curl.exe --insecure --silent --show-error --fail "https://127.0.0.1:8443/api/radar/priced-in/audit?limit=5"
```

Observed:

- Focused three-test set passed (`3 passed`).
- API/provider-ingest three-test set passed (`3 passed`).
- Ruff passed.
- `git diff --check` passed.
- CLI and API checks both reported `external_calls=0`.
- Local services were restarted from the branch for API/dashboard verification.
- PR #352, `Surface full scan market bar repair`, was merged by rebase as
  `5fd8216b9e8f`.
- Local services were restarted after merge from `main`:
  - API health returned commit `5fd8216b9e8f`;
  - Streamlit health returned `ok`.
- Post-merge API verification showed:
  - `api_repair=attention`;
  - `expected=2026-05-15`;
  - `missing=523`;
  - `calls=0`.
- Post-merge CLI verification showed:
  - `market_bar_repair=status=attention`;
  - `expected_as_of=2026-05-15`;
  - `missing=523`;
  - `external_calls=0`.

Next useful product action:

- The product now says exactly how to repair the first full-scan trust gap
  without provider calls. The remaining large data-kind gaps are:
  - options: `0/12087`;
  - local text: `12/12087`;
  - broker context: `5/12087` and currently stale;
  - catalyst events: `9/5521` company-like rows.
- Next useful slice should make one of those evidence source families similarly
  explicit in the full-scan answer path, without adding hidden provider calls.

## Latest Goal-Drift Check And Full-Scan Primary UX

Current goal:

- MarketRadar should scan the full market and rank whether market emotion for
  any stock has not yet been matched by price reaction.
- Dashboard/CLI/API are tools to expose that answer and the missing evidence,
  not separate goals.

Drift check:

- The backend already had a full-market priced-in scan, but the UI and CLI made
  the 10-row answer shortlist and next provider-batch previews too prominent.
- That made it look like MarketRadar was only looking at a few tickers.
- The corrected product rule is:
  - **full scan first**;
  - shortlist = priority lens over the scan;
  - provider batches = evidence-fill logistics, not scan scope.

Fix in this slice:

- `priced_in_full_scan_audit_payload` now includes `primary_scan`.
- `primary_scan` schema is `priced-in-primary-full-scan-v1` and explicitly
  reports:
  - full active-universe scope;
  - active securities;
  - scanned/ranked rows;
  - visible row range and display mode;
  - whether the visible rows are the complete full scan or a page;
  - `shortlist_role=priority_lens_not_scan_scope`;
  - `source_batch_role=provider_fill_logistics_not_scan_scope`;
  - zero external calls;
  - full-scan review/export commands.
- `answer_shortlist` remains backward-compatible but now labels itself as a
  priority lens, not the scan universe:
  - `lens=market_expectation_priority_lens`;
  - `selection_scope=priority_lens_not_scan_universe`;
  - `selection_note=... not the scan universe`;
  - full-scan review/export commands.
- Each shortlist row now includes a zero-call `drilldown` block:
  - `detail_command`, e.g. `catalyst-radar candidate-detail AAPL`;
  - `detail_api`;
  - evidence gap summary;
  - per-source review/plan commands;
  - explicit boundary that review/planning are zero-call and provider execution
    still requires separate approval.
- CLI `priced-in-audit` now prints `primary_full_scan=...` before the
  recommendation/shortlist output.
- Streamlit **Priced-in Full Scan** now shows **Full-Market Scan** and
  **Full-scan Ranked Rows** before the priority lens. The old
  **Market Expectation Shortlist** label was replaced by
  **Priority Lens: Market Emotion Mismatches**.
- No provider/source execution was run.

Current live zero-call observations from the branch:

```text
primary_full_scan=scope=full_active_universe active=12613 scanned=12087 ranked=12087 display=page_preview visible=1-10 external_calls=0
boundary=The full scan is the ranked universe. Shortlists are priority lenses; provider batches are evidence-fill chunks.
answer_shortlist=... selection=priority_lens_not_scan_universe full_scan_rows=12087 external_calls=0
selection_note=These 10 visible row(s) are a priority lens over 12087 ranked full-scan row(s), not the scan universe.
```

```text
GET /api/radar/priced-in/audit?limit=10
api_primary=full_active_universe ranked=12087 display=page_preview shortlist=priority_lens_not_scan_universe calls=0
```

Browser verification on `http://127.0.0.1:8514`:

- **Full-Market Scan** rendered.
- **Full-scan Ranked Rows** rendered.
- **Priority Lens: Market Emotion Mismatches** rendered after the full scan.
- **Top Priority Rows** rendered.
- **Download Full Scan Rows JSON** rendered.
- The page text included `12087` and `priority lens over`.
- Browser console check reported 0 current errors.

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_full_scan_audit_payload_consolidates_current_state tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_audit_cli_outputs_full_scan_audit tests\integration\test_dashboard_entrypoint.py::test_dashboard_wires_priced_in_full_scan_panel_after_usefulness -q
.\.venv\Scripts\python.exe -m pytest tests\integration\test_api_routes.py::test_get_radar_priced_in_audit_returns_zero_call_audit tests\integration\test_dashboard_data.py::test_priced_in_full_scan_audit_payload_reuses_cached_zero_call_audit -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\cli.py apps\dashboard\Home.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py tests\integration\test_dashboard_entrypoint.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-audit --limit 10
curl.exe --insecure --silent --show-error --fail "https://127.0.0.1:8443/api/radar/priced-in/audit?limit=10"
```

Observed:

- Focused three-test set passed (`3 passed`).
- API/cache two-test set passed (`2 passed`).
- Ruff passed.
- `git diff --check` passed.
- CLI and API checks both reported `external_calls=0`.
- Local services were restarted from the branch for API/dashboard verification.
- PR #350, `Make priced-in full scan primary`, was merged by rebase as
  `64ca6d777608`.
- Local services were restarted after merge from `main`:
  - API health returned commit `64ca6d777608`;
  - Streamlit health returned `ok`.
- Post-merge CLI verification showed:
  - `primary_full_scan=scope=full_active_universe`;
  - `ranked=12087`;
  - `selection=priority_lens_not_scan_universe`;
  - `external_calls=0`.
- Post-merge API verification showed:
  - `api_primary=full_active_universe`;
  - `ranked=12087`;
  - `display=page_preview`;
  - `shortlist=priority_lens_not_scan_universe`;
  - `calls=0`.

Next useful product action:

- Keep the main answer path full-scan-first. Any future shortlist, table page,
  source-gap sample, or provider batch must be labeled as a lens/chunk over the
  full scan.
- Do not spend another slice on UI polish unless it helps answer:
  "Which full-market rows show emotion not matched by price, and what evidence
  is missing before I can trust that?"

## Latest Market Expectation Shortlist

Current problem:

- The full-scan dashboard had the data needed to answer the user's real
  question, but the first useful rows were buried inside the large full-scan
  table and source-gap mechanics.
- The operator needed the dashboard and CLI/API to first answer:
  "Which stocks currently look like market emotion has not been fully matched by
  price reaction?"

Fix in this slice:

- `priced_in_full_scan_audit_payload` now includes `answer_shortlist`.
- The shortlist is derived from the same ranked full-scan rows already loaded by
  the audit path, so it adds no provider calls.
- The shortlist schema is `priced-in-answer-shortlist-v1` and includes:
  - status;
  - focus (`full_scan` or a selected source-gap focus);
  - decision-ready row count;
  - actionable mismatch row count;
  - rows needing evidence;
  - visible row count and sample flag;
  - full-scan row count;
  - investment-decision boundary;
  - top rows with ticker, rank, status, decision-ready flag, gap, emotion,
    reaction, priced-in score, missing/stale sources, why-now, and next step.
- CLI `priced-in-audit` now prints `answer_shortlist=...` immediately after the
  source-gap recommendation, before the larger full-scan preview table.
- Streamlit **Priced-in Full Scan** now shows **Market Expectation Shortlist**
  above source-gap mechanics and the large table.
- Streamlit renders **Top Market Emotion Mismatches** with the ranked top rows
  and repeats the "not trade approval" boundary.
- This remains zero-call local analysis only.

Current live zero-call observations from the branch:

```text
answer_shortlist=status=decision_ready focus=full_scan decision_ready=10 actionable=12 visible=10 sample=false external_calls=0
  summary=Showing 10 of 10 decision-ready not-priced-in row(s).
  boundary=This shortlist ranks market-expectation mismatch evidence only; it is not trade approval.
  A 1 bullish_not_priced_in true 65.9 65.9 0.0 options Review the priced-in evidence and optional source gaps.
```

```text
GET /api/radar/priced-in/audit?limit=10
api_shortlist_status=decision_ready focus=full_scan decision=10 visible=10 first=A calls=0
```

Browser verification on `http://127.0.0.1:8514`:

- **Market Expectation Shortlist** rendered.
- **Top Market Emotion Mismatches** rendered.
- The section showed `External Calls = 0`.
- The section included the "not trade approval" boundary.
- The visible dashboard rows included current top mismatch tickers such as
  `MSFT` and `AAPL` under the latest-run cutoff.
- Browser console check reported 0 current errors.

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_full_scan_audit_payload_consolidates_current_state tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_audit_cli_outputs_full_scan_audit tests\integration\test_api_routes.py::test_get_radar_priced_in_audit_returns_zero_call_audit tests\integration\test_dashboard_entrypoint.py::test_dashboard_wires_priced_in_full_scan_panel_after_usefulness -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\cli.py apps\dashboard\Home.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py tests\integration\test_dashboard_entrypoint.py tests\integration\test_api_routes.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-audit --limit 10
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-audit --limit 10 --json
curl.exe --insecure --silent --show-error --fail "https://127.0.0.1:8443/api/radar/priced-in/audit?limit=10"
```

Observed:

- Focused four-test set passed (`4 passed`).
- Ruff passed.
- `git diff --check` passed.
- CLI and API checks both reported `external_calls=0`.
- Local services were restarted from the branch for API/dashboard verification.
- PR #348, `Add priced-in answer shortlist`, was merged by rebase as
  `878f21438444`.
- Local services were restarted after merge from `main`:
  - API health returned commit `878f21438444`;
  - Streamlit health returned `ok`.
- Post-merge API verification:
  - `/api/radar/priced-in/audit?limit=10` returned
    `shortlist_status=decision_ready`, `focus=full_scan`,
    `decision=10`, `visible=10`, `first=A`, and `calls=0`.

Next useful product action:

- Continue improving the answer path, not just source-gap plumbing. A good next
  slice is to add row-level drill-down links/actions from each shortlist ticker
  into candidate detail and local evidence gaps.

## Latest Full-Scan Versus Provider-Batch Boundary

Current problem:

- The user asked, "Why only these tickers? I want full scan."
- The product was doing the local full-scan analysis, but the source-fill/action
  surfaces showed the next capped provider batch first. That made a list like
  `AAMI, AAOI, AAL, AAON, AAP` look like the scan scope, when it was only the
  next Schwab read-only provider-fill chunk.
- This confusion is severe because MarketRadar's actual goal is full-market
  scanning for emotion-versus-price mismatch, while provider fills must remain
  deliberately rate-limited and manually approved.

Fix in this slice:

- `priced_in_source_gap_batches_payload` now exposes the ticker list scope
  explicitly in `scan_scope`:
  - `returned_ticker_scope=next_provider_batch_preview` when the visible
    tickers are only the next provider chunk;
  - `returned_ticker_scope=returned_provider_batches` when all returned provider
    batches are shown;
  - `batch_preview_note`, which states that the visible tickers are not the scan
    universe.
- The API still reports full-scan counts separately:
  - `full_scan_gap_rows`;
  - `plannable_rows`;
  - `planned_batches`;
  - `returned_tickers`;
  - `tickers_are_batch_sample`.
- Selected source-gap audit actions now include:
  - `review_rows_command`;
  - `export_rows_command`;
  - `all_batches_command`;
  - `all_batches_api`;
  - `batch_preview_note`.
- CLI `priced-in-source-batches` now prints `ticker_scope=...` and
  `ticker_scope_note=...` directly under the full-scan scope line.
- CLI `priced-in-audit --source-gap ...` now prints the full source-gap export
  command and the all-provider-batches command before the first provider batch.
- Streamlit **Selected Source Gap Action** now labels the visible ticker list as
  `NEXT PROVIDER BATCH PREVIEW NOT FULL SCAN`, adds a `FULL SCAN SCOPE` column,
  and surfaces the full source-gap export plus all-provider-batches commands.
- Streamlit **Priced-in Source Gaps** now adds a plain caption that example
  tickers are priority examples only; the gap count and full-scan table are the
  scan scope.
- This remains zero-call browsing/planning only. No SEC, Schwab, Polygon/Massive,
  OpenAI, or order-submission call is made by these views.

Current live zero-call observations from the branch:

```text
priced_in_source_batches source=broker_context status=ready gap_rows=12082 plannable=12082 ... batch_size=5 batches=1 total_batches=2417 ... external_calls=0
scan_scope=mode=full_scan gap_rows=12082 plannable=12082 returned_batches=1 planned_batches=2417 returned_tickers=5 batch_sample=true ticker_scope=next_provider_batch_preview
ticker_scope_note=Returned tickers are the next source-fill batch preview: 5 of 12082 plannable row(s), from 12082 full-scan broker_context gap row(s). This is not the scan universe.
```

```text
GET /api/radar/priced-in/source-batches?source=broker_context&batch_limit=1
api_source=broker_context status=ready scope=next_provider_batch_preview note=Returned tickers are the next source-fill batch preview: 5 of 12082 plannable row(s), from 12082 full-scan broker_context gap row(s). This is not the scan universe. calls=0
```

Browser verification on `http://127.0.0.1:8514`:

- **Display complete full scan** rendered.
- The **Source gap** filter was changed to `broker_context`.
- **Selected Source Gap Action** rendered after the dashboard rebuilt.
- The selected action section rendered:
  - "Selected source-gap actions are full-scan decisions...";
  - `FULL SCAN SCOPE = all 12082 gap row(s)`;
  - `NEXT PROVIDER BATCH PREVIEW NOT FULL SCAN = AAMI, AAOI, AAL, AAON, AAP`;
  - `TICKER SCOPE NOTE = ... This is not the scan universe`;
  - `SOURCE GAP FULL SCAN EXPORT`;
  - `ALL PROVIDER BATCHES COMMAND`;
  - approval checklist text including "No trading permission".
- Browser console check after reload/selection reported 0 current errors. Older
  connection-refused entries were from the service restart and were not current
  page errors.

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_plans_safe_sync_batches tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_can_return_full_scan_plan tests\integration\test_dashboard_data.py::test_priced_in_all_source_gap_batches_payload_summarizes_next_chunks tests\integration\test_dashboard_data.py::test_priced_in_full_scan_audit_payload_consolidates_current_state tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_queue_cli_outputs_same_zero_call_signal tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_audit_cli_outputs_full_scan_audit tests\integration\test_api_routes.py::test_get_radar_priced_in_source_batches_returns_zero_call_plan tests\integration\test_dashboard_entrypoint.py::test_dashboard_wires_priced_in_full_scan_panel_after_usefulness -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\cli.py apps\dashboard\Home.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py tests\integration\test_dashboard_entrypoint.py tests\integration\test_api_routes.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-source-batches --source broker_context --batch-limit 1
curl.exe --insecure --silent --show-error --fail "https://127.0.0.1:8443/api/radar/priced-in/source-batches?source=broker_context&batch_limit=1"
```

Observed:

- Focused eight-test set passed (`8 passed`).
- Ruff passed.
- `git diff --check` passed.
- CLI and API checks both reported `external_calls=0`.
- Local services were restarted from the branch for dashboard verification.
- PR #346, `Clarify full scan batch boundaries`, was merged by rebase as
  `38ba587c9e5a`.
- Local services were restarted after merge from `main`:
  - API health returned commit `38ba587c9e5a`;
  - Streamlit health returned `ok`.
- Post-merge API verification:
  - `/api/radar/priced-in/source-batches?source=broker_context&batch_limit=1`
    returned `scope=next_provider_batch_preview`,
    `full_gap_rows=12082`, `returned_tickers=5`,
    `calls=0`, and the explicit "This is not the scan universe" note.

Next useful product action:

- Keep the main operator path centered on the full-scan answer:
  "which stocks have market emotion not yet matched by price?"
- Source-fill execution still requires explicit user approval because it can
  call SEC/Schwab/market providers.
- Do not run any `--execute-next`, `--execute-batches`, SEC, Schwab, Polygon, or
  order-submission command unless the user explicitly approves the provider
  calls after reviewing the checklist.

## Latest Audit Cache Status UX

Current problem:

- The audit cache made repeated full-scan views much faster, but the operator
  could not tell whether a dashboard/API response was a cold rebuild or a cache
  hit.

Fix in this slice:

- `priced_in_full_scan_audit_payload` now returns a `performance` object for
  cached zero-call audit paths.
- On cold builds, `performance` includes:
  - `cache_status=miss`;
  - `build_elapsed_ms`;
  - cache TTL and key-scope description.
- On cache hits, `performance` includes:
  - `cache_status=hit`;
  - original `build_elapsed_ms`;
  - `cache_age_ms`;
  - cache TTL and key-scope description.
- CLI `priced-in-audit` now prints `performance=cache=...`.
- Streamlit **Priced-in Full Scan** now shows a **Cache** status badge and an
  `Audit performance: cache=..., build_ms=..., age_ms=..., ttl_s=...` caption.
- This remains zero-call observability only.

Current live zero-call observations from the branch:

```text
API /api/radar/priced-in/audit?all_rows=true call=1 elapsed_s=45.181 cache=miss build_ms=44407 rows=12087/12087 external_calls=0
API /api/radar/priced-in/audit?all_rows=true call=2 elapsed_s=0.746 cache=hit build_ms=44407 age_ms=1343 rows=12087/12087 external_calls=0
```

Browser verification on `http://127.0.0.1:8514`:

- Cache badge rendered as hit or miss.
- `Audit performance: cache=...` rendered.
- `Full scan rows: showing all 1-12087 of 12087` still rendered.
- Browser console check reported 0 errors.

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_full_scan_audit_payload_reuses_cached_zero_call_audit tests\integration\test_dashboard_data.py::test_priced_in_full_scan_audit_payload_consolidates_current_state tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_audit_cli_outputs_full_scan_audit tests\integration\test_api_routes.py::test_get_radar_priced_in_audit_returns_zero_call_audit tests\integration\test_dashboard_entrypoint.py::test_dashboard_wires_priced_in_full_scan_panel_after_usefulness -q
.\.venv\Scripts\python.exe -m ruff check apps\dashboard\Home.py src\catalyst_radar\dashboard\data.py src\catalyst_radar\cli.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py tests\integration\test_dashboard_entrypoint.py
git diff --check
```

Observed:

- Focused five-test set passed (`5 passed`).
- Ruff passed.
- `git diff --check` passed.
- PR #344, `Show priced-in audit cache status`, merged by rebase as `ff35c4e`.
- Local services were restarted after merge:
  - API health returned commit `ff35c4e73888`;
  - Streamlit health returned `ok`.
- Post-merge API verification:
  - first `/api/radar/priced-in/audit?all_rows=true` call returned
    `cache=miss`, `rows=12087/12087`, `external_calls=0`, and took `44.973s`;
  - immediate second same API call returned `cache=hit`,
    `rows=12087/12087`, `external_calls=0`, and took `0.673s`.

Next useful product action:

- Actual source-fill execution still requires explicit user approval because it
  can call SEC/Schwab/market providers.
- Next useful zero-call slice: make source-fill approval screens show a single
  operator checklist before any SEC/Schwab/market provider execution command.

## Latest Full-Audit Cache

Current problem:

- Full-scan display is now the default dashboard path, but a cold full-audit
  payload build currently takes roughly 43-45 seconds on the local database.
- Repeating the same dashboard/API view immediately rebuilt the same zero-call
  audit, making the human dashboard feel sluggish.

Fix in this slice:

- `priced_in_full_scan_audit_payload` now has a small in-process cache.
- The cache is used only when `queue` and `preflight` are not supplied by the
  caller, so tests and internal composed flows that pass explicit inputs keep
  their exact behavior.
- The cache key includes:
  - database URL;
  - `available_at`;
  - source-gap filter;
  - preview limit/offset;
  - all-rows flag;
  - a database state token.
- The database state token is built from cheap count/max checks across the
  priced-in data dependencies: securities, bars, features, candidate states,
  packets, decision cards, events, text snippets/features, options, broker
  market snapshots, and job runs.
- Cache entries are copied on read/write so callers cannot mutate shared cached
  payloads.
- Cache TTL is 180 seconds with a small max size of 12 entries.
- This does not cache or execute provider calls; it only caches local zero-call
  audit payloads.

Current live zero-call observations from the branch:

```text
direct helper call=1 elapsed_s=42.939 rows=12087/12087 external_calls=0
direct helper call=2 elapsed_s=0.312 rows=12087/12087 external_calls=0
```

```text
API /api/radar/priced-in/audit?all_rows=true call=1 elapsed_s=44.975 rows=12087/12087 external_calls=0
API /api/radar/priced-in/audit?all_rows=true call=2 elapsed_s=0.719 rows=12087/12087 external_calls=0
```

Browser verification on `http://127.0.0.1:8514`:

- **Table display: complete full scan** rendered.
- `Full scan rows: showing all 1-12087 of 12087` rendered.
- **Download Full Scan Rows JSON** rendered.
- The priority-preview boundary rendered.
- Browser console check reported 0 errors.

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_full_scan_audit_payload_reuses_cached_zero_call_audit tests\integration\test_dashboard_data.py::test_priced_in_full_scan_audit_payload_consolidates_current_state tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_audit_cli_outputs_full_scan_audit tests\integration\test_api_routes.py::test_get_radar_priced_in_audit_returns_zero_call_audit tests\integration\test_dashboard_entrypoint.py::test_dashboard_wires_priced_in_full_scan_panel_after_usefulness -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py tests\integration\test_dashboard_data.py
git diff --check
```

Observed:

- Focused five-test set passed (`5 passed`).
- Ruff passed.
- `git diff --check` passed.
- The cache regression test proved the second same-filter audit does not call
  `priced_in_queue_payload` again and that cached payloads are copied before
  returning.
- PR #342, `Cache priced-in audit payloads`, merged by rebase as `f32d576`.
- Local services were restarted after merge:
  - API health returned commit `f32d576c32f0`;
  - Streamlit health returned `ok`.
- Post-merge API verification:
  - first `/api/radar/priced-in/audit?all_rows=true` call returned
    `12087/12087`, `external_calls=0`, and took `45.0s`;
  - immediate second same API call returned `12087/12087`,
    `external_calls=0`, and took `0.64s`.

Next useful product action:

- Actual source-fill execution still requires explicit user approval because it
  can call SEC/Schwab/market providers.
- Next useful zero-call slice: make the dashboard/API show the cache status and
  last build duration so the operator can tell cold rebuilds from cached views.

## Latest Full-Scan Default UX

Current problem:

- The user saw a handful of tickers and reasonably asked why MarketRadar was
  not doing a full scan.
- The backend was already scanning the full active universe, but the default
  dashboard display still opened as a page preview.
- Recommended source gaps and source tables showed short ticker lists without
  making it visually impossible to confuse those examples with the whole scan.

Fix in this slice:

- Streamlit **Priced-in Full Scan** now defaults to
  **Display complete full scan**.
- The old row controls were renamed to **Rows per page** and **Page offset** so
  they read as page-preview controls, not scan-size controls.
- The panel now states the full scan scope in plain text:
  scanned rows, ranked rows, active securities, and whether the current table is
  a complete full scan or a page preview.
- The status badge now says **Display: complete full scan** or
  **Display: page preview** instead of vague visible-ticker wording.
- The first source-gap callout now includes a `sample_boundary`:
  example tickers are only a priority preview, while the gap itself covers the
  full-scan row count.
- `recommended_source_gap` now includes `full_scan_command`, for example:
  `catalyst-radar priced-in-audit --source-gap options --all --json`.
- The dashboard shows that full source-gap command next to the smaller preview
  command and source-batch plan command.
- Dashboard source-gap tables now label short lists as
  `priority_examples_preview`, `coverage_examples_preview`, and
  `first_batch_preview`.
- CLI `priced-in-audit` now prints `display=page_preview|complete`,
  `full_source_gap_export=...`, `sample_boundary=...`, and
  `priority_examples_preview=...`.
- This remains zero-call browsing/export only.

Current live zero-call observations from the branch:

```text
priced-in-audit --limit 5
active=12613 scanned=12087 ranked=12087 decision=10 external_calls=0
full_scan_rows=1-5/12087 display=page_preview sample=true all_rows=false
sample_boundary=Example tickers are only a priority preview; the source gap itself covers 12087 full-scan row(s).
full_source_gap_export=catalyst-radar priced-in-audit --source-gap options --all --json
```

```text
GET /api/radar/priced-in/audit?all_rows=true
api_all_rows=12087/12087 all=True external_calls=0
```

Browser verification on `http://127.0.0.1:8514`:

- **Display complete full scan** rendered as the default full-scan control.
- The panel rendered:
  `Full scan scope: scanned 12087 row(s), ranked 12087 row(s), from 12613 active securities. Table display: complete full scan.`
- The table caption rendered:
  `Full scan rows: showing all 1-12087 of 12087.`
- The first source-gap callout rendered the priority-preview boundary.
- The dashboard exposed `Download Full Scan Rows JSON`.
- The full source-gap export command was present with the latest-run
  `--available-at` cutoff.

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_full_scan_audit_payload_consolidates_current_state tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_audit_cli_outputs_full_scan_audit tests\integration\test_api_routes.py::test_get_radar_priced_in_audit_returns_zero_call_audit tests\integration\test_dashboard_entrypoint.py::test_dashboard_wires_priced_in_full_scan_panel_after_usefulness -q
.\.venv\Scripts\python.exe -m ruff check apps\dashboard\Home.py src\catalyst_radar\dashboard\data.py src\catalyst_radar\cli.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py tests\integration\test_dashboard_entrypoint.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-audit --limit 5
curl.exe --insecure --silent --show-error --fail "https://127.0.0.1:8443/api/radar/priced-in/audit?limit=5"
curl.exe --insecure --silent --show-error --fail "https://127.0.0.1:8443/api/radar/priced-in/audit?all_rows=true"
```

Observed:

- Focused four-test set passed (`4 passed`).
- Ruff passed.
- `git diff --check` passed.
- Live CLI/API checks showed the full universe is 12,087 ranked rows from
  12,613 active securities and all browsing/export checks made
  `external_calls=0`.
- PR #340, `Default dashboard to full scan display`, merged by rebase as
  `c7beb6e`.
- Local services were restarted after merge:
  - API health returned commit `c7beb6eed016`;
  - Streamlit health returned `ok`.
- Post-merge API verification:
  - `/api/radar/priced-in/audit?all_rows=true` returned
    `12087/12087`, `all=True`, `external_calls=0`, and
    `full_cmd=catalyst-radar priced-in-audit --source-gap options --all --json`.
- Post-merge browser verification on `http://127.0.0.1:8514` confirmed:
  - **Display complete full scan** is present;
  - `Table display: complete full scan` is present;
  - `Full scan rows: showing all 1-12087 of 12087` is present;
  - the priority-preview boundary is present;
  - the full source-gap export command is present;
  - **Download Full Scan Rows JSON** is present;
  - browser console check reported 0 errors.

Next useful product action:

- Actual source-fill execution still requires explicit user approval because it
  can call SEC/Schwab/market providers.
- Next useful zero-call slice: reduce the 45-second full-audit build latency or
  cache the audit payload after a scan, because full-scan display is now the
  default human path.

## Latest Payoff-Ranked Next Action

Current problem:

- Source-gap payoff columns were visible in the table, but the first visible
  next-action area still did not explicitly say which source gap to inspect
  first and why.
- Users still had to infer that the first table row was the most valuable
  coverage action.

Fix in this slice:

- `priced_in_full_scan_audit_payload` now includes
  `recommended_source_gap`.
- The recommendation includes:
  - source;
  - status;
  - decision-useful gap rows;
  - actionable gap rows;
  - research-useful gap rows;
  - total gap rows;
  - priority example tickers;
  - rationale;
  - next action;
  - source-gap review command;
  - source-batch plan command;
  - provider-call boundary.
- CLI `priced-in-audit` now prints `recommended_source_gap=...`,
  `why=...`, and the zero-call boundary before the row preview.
- Streamlit **Priced-in Full Scan** now shows a **First source gap** callout
  above the command/table area, with payoff badges and the review/plan commands.
- This remains zero-call guidance only.

Current live zero-call observations from the branch:

```text
priced-in-audit --limit 5 --json
rec_source=options decision=10 actionable=12 research=0 gaps=12087 review=catalyst-radar priced-in-audit --source-gap options --limit 25 external_calls=0
```

```text
priced-in-audit --limit 5
recommended_source_gap=source=options decision=10 actionable=12 research=0 gap_rows=12087 review=catalyst-radar priced-in-audit --source-gap options --limit 25
  why=options has the highest current payoff: 10 decision-useful gap row(s), 12 actionable gap row(s), 0 research-useful gap row(s), and 12087 total gap row(s).
  boundary=Reviewing this recommendation makes 0 provider calls. Execute source batches only after explicitly approving provider calls.
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_full_scan_audit_payload_consolidates_current_state tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_audit_cli_outputs_full_scan_audit tests\integration\test_api_routes.py::test_get_radar_priced_in_audit_returns_zero_call_audit tests\integration\test_dashboard_entrypoint.py::test_dashboard_wires_priced_in_full_scan_panel_after_usefulness -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\cli.py apps\dashboard\Home.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py tests\integration\test_dashboard_entrypoint.py tests\integration\test_api_routes.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-audit --limit 5 --json
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-audit --limit 5
```

Observed so far:

- Focused four-test set passed (`4 passed`).
- Ruff passed.
- `git diff --check` passed.
- Live branch CLI recommended options first with `decision=10`,
  `actionable=12`, `gaps=12087`, and `external_calls=0`.

Next useful product action:

- Commit, open a PR, merge by rebase, restart local services, verify API and
  Streamlit health, then run live API/dashboard checks for the first-source-gap
  callout.

## Latest Source-Gap Payoff Ranking

Current problem:

- The dashboard listed source gaps by source name and raw gap count, but did
  not make the "what should I fix first?" payoff obvious.
- The source overview payload already had decision/research/actionable gap
  counts, but the main full-scan audit/dashboard source table did not.

Fix in this slice:

- `priced_in_full_scan_audit_payload` now keeps internal ranked
  `planning_rows` for the audit, with no extra provider calls.
- Audit source rows now include:
  - `decision_useful_gap_rows`;
  - `research_useful_gap_rows`;
  - `actionable_gap_rows`;
  - `priority_sample_tickers`.
- CLI `priced-in-audit` now prints those payoff counts under `sources:`.
- Streamlit **Priced-in Source Gaps** now shows decision-useful,
  research-useful, actionable counts, priority examples, and sorts sources by
  decision-useful impact first, then actionable, research-useful, and raw gap
  count.
- This remains zero-call analysis only.

Current live zero-call observation from the branch:

```text
priced-in-audit --limit 5 --json
source=options decision=10 research=0 actionable=12 gaps=12087 examples=A,MSFT,AAMI,AAOI,AAAU
source=broker_context decision=5 research=0 actionable=7 gaps=12082 examples=AAMI,AAOI,AAL,AAON,AAP
source=market_bars decision=0 research=0 actionable=0 gaps=0 examples=
external_calls=0
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_full_scan_audit_payload_consolidates_current_state tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_audit_cli_outputs_full_scan_audit tests\integration\test_api_routes.py::test_get_radar_priced_in_audit_returns_zero_call_audit tests\integration\test_dashboard_entrypoint.py::test_dashboard_wires_priced_in_full_scan_panel_after_usefulness -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\cli.py apps\dashboard\Home.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py tests\integration\test_dashboard_entrypoint.py tests\integration\test_api_routes.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-audit --limit 5 --json
```

Observed:

- Focused four-test set passed (`4 passed`).
- Ruff passed.
- `git diff --check` passed.
- Live branch CLI audit source rows showed options first by payoff
  (`decision=10`, `actionable=12`), broker context second
  (`decision=5`, `actionable=7`), and `external_calls=0`.
- PR #337 merged as `c7482b5`.
- Post-merge local services were restarted:
  - API health returned commit `c7482b56c514`;
  - Streamlit health returned `ok`.
- Live API verification:
  - `/api/radar/priced-in/audit?limit=5`
  - ranked source payoff as options first (`decision=10`,
    `actionable=12`, `gaps=12087`) and broker context second
    (`decision=5`, `actionable=7`, `gaps=12082`), with
    `external_calls=0`.
- Browser verification on `http://127.0.0.1:8514`:
  - **Priced-in Source Gaps** rendered `DECISION USEFUL`,
    `RESEARCH USEFUL`, `ACTIONABLE`, and `PRIORITY EXAMPLES`.
  - Options appeared before broker context in the table.
  - The dashboard uses its latest-run cutoff, so the displayed decision/research
    split can differ from the no-cutoff API check, but the payoff columns and
    ordering are present.

Next useful product action:

- Actual source-fill execution still requires explicit user approval because it
  can call SEC/Schwab/market providers.
- Next useful zero-call product slice: make the dashboard's top-level
  "next action" panel pull from this payoff ranking so the first screen says
  exactly which source gap to inspect first and why.

## Latest Source-Gap First-Batch Actions

Current problem:

- Selected source-gap actions told the user which source was missing and which
  plan command to inspect, but still did not show the first executable chunk.
- Users could confuse a small ticker list with "the scan" instead of "the first
  provider-safe batch."
- A naive implementation that called the source-batch planner separately
  reloaded the full scan and made selected source-gap audit views too slow.

Fix in this slice:

- `priced_in_full_scan_audit_payload(..., source_gap=...)` now includes
  first-batch source-fill details in `preview.source_gap_actions`.
- Added fields include:
  - `batch_status`;
  - `full_scan_gap_rows`;
  - `plannable_gap_rows`;
  - `unplannable_gap_rows`;
  - `provider_batch_count`;
  - `batch_size`;
  - `first_batch_scope`;
  - `first_batch_tickers`;
  - `first_batch_external_calls`;
  - `first_batch_command`;
  - `execute_next_command`;
  - `execute_batches_command`;
  - `diagnostic_status`;
  - `blocked_reason`;
  - `diagnostic_next_action`;
  - `batch_scope`.
- CLI `priced-in-audit --source-gap <source>` now prints:
  - `provider_batch_plan=...`;
  - `first_provider_batch=...`;
  - `execute_next=...` when a provider batch is executable;
  - `blocked=...` when the source is blocked;
  - `batch_scope=First provider batch only...`.
- Streamlit **Selected Source Gap Action** now shows a curated row with first
  provider batch tickers, call count, execute command, blocker reason, and
  batch scope.
- Performance guard: the audit path now asks `priced_in_queue_payload` to keep
  internal `planning_rows`, filters the selected source-gap preview in memory,
  and passes the same ranked rows into `priced_in_source_gap_batches_payload`.
  This avoids a second full-scan queue build for selected source-gap views.
- This remains zero-call planning only; no provider execution command is run.

Current live zero-call observations from the branch:

```text
priced-in-audit --source-gap broker_context --limit 2 --json
broker_status=ready gap=12082 batches=2417 first=AAMI,AAOI,AAL,AAON,AAP calls=1 execute=catalyst-radar priced-in-source-batches --source broker_context --execute-next external_calls=0 elapsed_s=47.3
```

```text
priced-in-audit --source-gap options --limit 2 --json
options_status=blocked gap=12087 batches=0 blocked=newer_than_scan first_count=0 execute= external_calls=0 elapsed_s=47.7
```

```text
priced-in-audit --source-gap broker_context --limit 2
selected_source_gap_actions:
  provider_batch_plan=status=ready gap_rows=12082 plannable=12082 batches=2417
  first_provider_batch=tickers=AAMI,AAOI,AAL,AAON,AAP calls=1 command=catalyst-radar schwab-market-sync --ticker AAMI --ticker AAOI --ticker AAL --ticker AAON --ticker AAP
  execute_next=catalyst-radar priced-in-source-batches --source broker_context --execute-next
  batch_scope=First provider batch only; full scan has 12082 gap row(s) and 2417 planned batch(es).
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_full_scan_audit_payload_consolidates_current_state tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_audit_cli_outputs_full_scan_audit tests\integration\test_api_routes.py::test_get_radar_priced_in_audit_returns_zero_call_audit tests\integration\test_dashboard_entrypoint.py::test_dashboard_wires_priced_in_full_scan_panel_after_usefulness -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\cli.py apps\dashboard\Home.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py tests\integration\test_dashboard_entrypoint.py tests\integration\test_api_routes.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-audit --source-gap broker_context --limit 2 --json
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-audit --source-gap options --limit 2 --json
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-audit --source-gap broker_context --limit 2
```

Observed:

- Focused four-test set passed (`4 passed`).
- Ruff passed.
- `git diff --check` passed.
- Live branch broker-context selected action showed the first provider batch,
  one required Schwab call, the execute-next command, and `external_calls=0`.
- Live branch options selected action showed `blocked_reason=newer_than_scan`,
  no executable batch, and `external_calls=0`.
- PR #335 merged as `0e57669`.
- Post-merge local services were restarted:
  - API health returned commit `0e57669fdddf`;
  - Streamlit health returned `ok`.
- Live API verification:
  - `/api/radar/priced-in/audit?source_gap=broker_context&limit=2`
  - returned `api_batch_status=ready`, `gap=12082`, `batches=2417`,
    first batch `AAMI,AAOI,AAL,AAON,AAP`, `calls=1`,
    execute command
    `catalyst-radar priced-in-source-batches --source broker_context --execute-next`,
    and `external_calls=0`.
- Browser verification on `http://127.0.0.1:8514`:
  - selecting **Source gap** = `broker_context` rendered
    **Selected Source Gap Action**;
  - the table showed `BATCH STATUS=ready`, `FULL SCAN GAP ROWS=12082`,
    `PROVIDER BATCH COUNT=2417`, first provider batch
    `AAMI, AAOI, AAL, AAON, AAP`, `FIRST BATCH CALLS=1`, the execute-next
    command, and `First provider batch only; full scan has 12082 gap row(s)
    and 2417 planned batch(es).`

Next useful product action:

- Actual source-fill execution still requires explicit user approval because it
  can call SEC/Schwab/market providers.
- Next useful zero-call product slice: make the dashboard's "what should I do
  now?" path prioritize coverage actions by payoff, for example showing
  source-fill candidates that unblock the most decision-useful rows first.

## Latest Full-Scan All-Rows UX

Current problem:

- Users could see only a small ticker page in the audit/dashboard and reasonably
  ask "why only these tickers?"
- The engine already scanned the full active universe, but the human-facing
  surfaces did not make the distinction between "visible page" and "full scan"
  strong enough.
- The CLI already had a separate full-scan queue export, but the audit command,
  API route, and dashboard did not expose a first-class all-rows audit mode.

Fix in this slice:

- `priced_in_full_scan_audit_payload` now accepts `all_rows=True`.
- CLI `priced-in-audit` now accepts `--all`.
- API `GET /api/radar/priced-in/audit` now accepts `all_rows=true`.
- Dashboard **Priced-in Full Scan** now has a **Show all rows** checkbox.
- When **Show all rows** is on, the dashboard requests the complete ranked
  full-scan row set from the local database, resets offset to 0, renders it in
  a Streamlit dataframe instead of a giant custom HTML table, and exposes a
  **Download Full Scan Rows JSON** button.
- The audit payload now reports:
  - `scope.all_rows_requested`;
  - `scope.audit_full_export_command`;
  - `preview.all_rows`;
  - `preview.audit_full_export_command`;
  - `commands.audit_full_scan`.
- CLI text output now prints `all_rows=true|false` beside the row range, so a
  small ticker list is clearly a page, not the scan universe.
- This remains zero-call browsing/export only.

Current live zero-call observations from the branch:

```text
priced-in-audit --all --json
audit_all_rows=12087/12087 all=True external_calls=0 command=catalyst-radar priced-in-audit --all
```

```text
priced-in-queue --full-scan --all --json
queue_all_rows=12087/12087 external_calls=0
```

```text
priced-in-audit --limit 5
full_scan_rows=1-5/12087 sample=true all_rows=false export=catalyst-radar priced-in-queue --full-scan --all --json
full_scan_row_note=The tickers below are rows 1-5 from the current ranked page, not the full scan universe of 12087 row(s).
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_full_scan_audit_payload_consolidates_current_state tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_audit_cli_outputs_full_scan_audit tests\integration\test_api_routes.py::test_get_radar_priced_in_audit_returns_zero_call_audit tests\integration\test_dashboard_entrypoint.py::test_dashboard_wires_priced_in_full_scan_panel_after_usefulness -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\cli.py src\catalyst_radar\api\routes\radar.py apps\dashboard\Home.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py tests\integration\test_api_routes.py tests\integration\test_dashboard_entrypoint.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-audit --all --json
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-queue --full-scan --all --json
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-audit --limit 5
```

Observed:

- Focused four-test set passed (`4 passed`).
- Ruff passed.
- `git diff --check` passed.
- Live branch CLI audit all-rows export returned all `12087/12087` rows with
  `external_calls=0`.
- Live branch full-scan queue export returned all `12087/12087` rows with
  `external_calls=0`.
- Live branch audit preview still shows only the selected row page, but now
  prints `all_rows=false` and the row note explicitly says the visible tickers
  are not the full scan universe.
- PR #333 merged as `c478540`.
- Post-merge local services were restarted:
  - API health returned commit `c47854007ba2`;
  - Streamlit health returned `ok`.
- Live API verification:
  - `/api/radar/priced-in/audit?all_rows=true`
  - returned `api_all_rows=12087/12087`, `all=True`,
    `external_calls=0`, and command
    `catalyst-radar priced-in-audit --all`.
- Browser verification on `http://127.0.0.1:8514`:
  - **Priced-in Full Scan** rendered **Show all rows**.
  - Checking **Show all rows** loaded the complete local row set and showed:
    `Full scan rows: showing all 1-12087 of 12087`.
  - The dashboard exposed **Download Full Scan Rows JSON**.

Next useful product action:

- Actual source-fill execution still requires explicit user approval because it
  can call SEC/Schwab/market providers.
- The next zero-call UX improvement would be to make selected source-gap action
  rows show the first executable provider chunk and clearly label that chunk as
  a provider batch, not the full scan universe.

## Latest Selected Source-Gap Action

Current problem:

- Full-scan audit row filtering could show rows missing a selected source, but
  the source's plan command and provider-call boundary were buried in the
  general source coverage table.
- A user inspecting `--source-gap options` still had to connect the filtered
  rows to the separate source-batch plan command manually.

Fix in this slice:

- `preview.source_gap_actions` is now included when the audit preview has a
  selected source-gap filter.
- Each selected source action includes:
  - source;
  - status;
  - gap count;
  - coverage percent;
  - next action;
  - plan command;
  - execution boundary explaining browsing/planning makes 0 provider calls and
    execution is separate.
- CLI `priced-in-audit --source-gap <source>` now prints
  `selected_source_gap_actions:` before the row preview.
- Streamlit **Priced-in Full Scan** now shows **Selected Source Gap Action**
  above **Full-scan Ranked Rows** when a source-gap filter is selected.
- No provider calls are added.

Current live zero-call observations from the branch:

```text
priced-in-audit --source-gap options --limit 3
selected_source_gap_actions:
- options status=missing gap_rows=12087 ... plan=catalyst-radar priced-in-source-batches --source options --all --json
  boundary=Planning and browsing make 0 provider calls; execute source batches only after approving provider calls.
external_calls=0
```

JSON observation:

```text
action=broker_context plan=catalyst-radar priced-in-source-batches --source broker_context --all --json boundary=Planning and browsing make 0 provider calls; execute source batches only after approving provider calls. external_calls=0
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_full_scan_audit_payload_consolidates_current_state tests\integration\test_api_routes.py::test_get_radar_priced_in_audit_returns_zero_call_audit tests\integration\test_dashboard_entrypoint.py::test_dashboard_wires_priced_in_full_scan_panel_after_usefulness tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_audit_cli_outputs_full_scan_audit -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py apps\dashboard\Home.py src\catalyst_radar\cli.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_entrypoint.py tests\integration\test_dashboard_demo_seed_cli.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-audit --source-gap options --limit 3
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-audit --source-gap broker_context --limit 2 --json
```

Observed:

- Focused four-test set passed (`4 passed`).
- Ruff passed.
- `git diff --check` passed.
- Live branch CLI source-gap action output reported `external_calls=0`.
- PR #331 merged as `c1867f3`.
- Post-merge local services were restarted:
  - API health returned commit `c1867f3f170c`.
  - Streamlit health returned `ok`.
- Live API verification:
  - `/api/radar/priced-in/audit?source_gap=options&limit=2`
  - returned `api_action=options`,
    `plan=catalyst-radar priced-in-source-batches --source options --all --json`,
    the zero-call execution boundary, and `external_calls=0`.
- Browser verification on `http://127.0.0.1:8514`:
  - select **Source gap** = `options`;
  - Overview rendered **Selected Source Gap Action** above
    **Full-scan Ranked Rows**;
  - the selected action table included the options plan command and
    `Planning and browsing make 0 provider calls; execute source batches only
    after approving provider calls.`

Next useful product action:

- Actual source-fill execution still requires explicit user approval because it
  can call SEC/Schwab/market providers.

## Latest Full-Scan Source-Gap Row Filter

Current problem:

- Full-scan audit paging let users browse the ranked universe, but the audit
  row page did not directly answer "show me the full-scan rows missing this
  data layer."
- Users still had to jump from the audit panel to separate source-gap commands
  to inspect source-specific row examples.

Fix in this slice:

- `priced_in_full_scan_audit_payload` now accepts `source_gap` for the preview
  rows only. Global full-scan counts, source coverage, trust gaps, and answer
  remain anchored to the full ranked scan.
- CLI `priced-in-audit` now accepts `--source-gap <source>`.
- API `GET /api/radar/priced-in/audit` now accepts `source_gap=<source>`.
- Streamlit **Priced-in Full Scan** now has a **Source gap** control next to
  the row-count and offset controls.
- Audit page commands preserve the source-gap filter, for example:
  `catalyst-radar priced-in-audit --source-gap options --limit 5 --offset 5`.
- This remains zero-call browsing only. Provider source-fill chunks remain
  separate explicit actions.

Current live zero-call observations from the branch:

```text
priced-in-audit --source-gap options --limit 5
full_scan_rows=1-5/12087 sample=true export=catalyst-radar priced-in-queue --full-scan --all --json
full_scan_row_note=This audit row page is filtered to rows missing or stale for options. The tickers below are rows 1-5 from the current ranked page, not the full scan universe of 12087 row(s).
more=catalyst-radar priced-in-audit --source-gap options --limit 5 --offset 5
...
```

JSON observation:

```text
filter=broker_context range=6-8/12082 rows=3 more=catalyst-radar priced-in-audit --source-gap broker_context --limit 3 --offset 8 external_calls=0 first=AAP
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_full_scan_audit_payload_consolidates_current_state tests\integration\test_api_routes.py::test_get_radar_priced_in_audit_returns_zero_call_audit tests\integration\test_dashboard_entrypoint.py::test_dashboard_wires_priced_in_full_scan_panel_after_usefulness tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_audit_cli_outputs_full_scan_audit -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py apps\dashboard\Home.py src\catalyst_radar\cli.py src\catalyst_radar\api\routes\radar.py tests\integration\test_dashboard_data.py tests\integration\test_api_routes.py tests\integration\test_dashboard_entrypoint.py tests\integration\test_dashboard_demo_seed_cli.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-audit --source-gap options --limit 5
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-audit --source-gap broker_context --limit 3 --offset 5 --json
```

Observed:

- Focused four-test set passed (`4 passed`).
- Ruff passed.
- `git diff --check` passed.
- Live branch CLI source-gap audit browsing reported `external_calls=0`.
- PR #329 merged as `66e21b8`.
- Post-merge local services were restarted:
  - API health returned commit `66e21b8cceb9`.
  - Streamlit health returned `ok`.
- Live API verification:
  - `/api/radar/priced-in/audit?source_gap=options&limit=4`
  - returned `api_filter=options`, `range=1-4/12087`, `rows=4`,
    `external_calls=0`, and first row `missing_sources=options`.
- Browser verification on `http://127.0.0.1:8514` showed the Overview tab
  rendering **Full-scan rows**, **Row offset**, and **Source gap** controls
  before **Full-scan Ranked Rows** and **Full-scan Trust Gaps**.

Next useful product action:

- Actual source-fill execution still requires explicit user approval because it
  can call SEC/Schwab/market providers.

## Latest Full-Scan Audit Paging

Current problem:

- The dashboard and CLI now show full-scan rows, but only the first preview
  page was reachable from the audit surface.
- Users could still ask "why only these tickers?" after seeing row examples,
  because the row page was not directly pageable in the audit/dashboard view.

Fix in this slice:

- `priced_in_full_scan_audit_payload` now carries audit-specific page commands:
  - `preview.audit_page_command`
  - `preview.audit_next_page_command`
- CLI `priced-in-audit` now accepts:
  - `--limit <N>`
  - `--offset <N>`
- CLI audit output prints `more=<next priced-in-audit command>` when another
  preview page exists.
- API `GET /api/radar/priced-in/audit` now accepts `limit` and `offset` query
  params and forwards them as `preview_limit` / `preview_offset`.
- Streamlit **Priced-in Full Scan** now has row-count and offset controls above
  the full-scan ranked rows table.
- This remains zero-call browsing only; provider source-fill chunks remain
  separate explicit actions.

Current live zero-call observation from the branch:

```text
priced-in-audit --limit 10 --offset 25
full_scan_rows=26-35/12087 sample=true export=catalyst-radar priced-in-queue --full-scan --all --json
full_scan_row_note=The tickers below are rows 26-35 from the current ranked page, not the full scan universe of 12087 row(s).
more=catalyst-radar priced-in-audit --limit 10 --offset 35
...
```

JSON observation:

```text
range=26-35/12087 rows=10 more=catalyst-radar priced-in-audit --limit 10 --offset 35 external_calls=0
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_full_scan_audit_payload_consolidates_current_state tests\integration\test_api_routes.py::test_get_radar_priced_in_audit_returns_zero_call_audit tests\integration\test_dashboard_entrypoint.py::test_dashboard_wires_priced_in_full_scan_panel_after_usefulness tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_audit_cli_outputs_full_scan_audit -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py apps\dashboard\Home.py src\catalyst_radar\cli.py src\catalyst_radar\api\routes\radar.py tests\integration\test_dashboard_data.py tests\integration\test_api_routes.py tests\integration\test_dashboard_entrypoint.py tests\integration\test_dashboard_demo_seed_cli.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-audit --limit 10 --offset 25
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-audit --limit 10 --offset 25 --json
```

Observed:

- Focused four-test set passed (`4 passed`).
- Ruff passed.
- `git diff --check` passed.
- Live branch CLI paging reported rows 26-35 of 12,087 with
  `external_calls=0` and a `more=` audit command.
- PR #327 merged as `3dd4105`.
- Post-merge local services were restarted:
  - API health returned commit `3dd410515f29`.
  - Streamlit health returned `ok`.
- Live API verification:
  - `/api/radar/priced-in/audit?limit=3&offset=30`
  - returned `api_range=31-33/12087`, `rows=3`,
    `more=catalyst-radar priced-in-audit --limit 3 --offset 33`, and
    `external_calls=0`.
- Browser verification on `http://127.0.0.1:8514` showed the Overview tab
  rendering **Full-scan rows** and **Row offset** controls before
  **Full-scan Ranked Rows**, then **Full-scan Trust Gaps**, then
  **Priced-in Source Gaps**.

Next useful product action:

- Actual source-fill execution still requires explicit user approval because it
  can call SEC/Schwab/market providers.

## Latest Full-Scan Row Visibility

Current problem:

- The user asked again: "Why only these tickers? I want full scan."
- The stored priced-in result is already a full-scan ranked universe, but the
  web full-scan panel still showed counts and source-gap examples without the
  actual row page.
- That made first provider chunks or example tickers look like the scan itself.

Fix in this slice:

- `priced_in_full_scan_audit_payload` now returns a zero-call
  `preview` object and `preview_rows` page for the full ranked scan.
- The preview defaults to the first 25 ranked rows and carries:
  row range, total ranked rows, next-page/export commands, sample explanation,
  ticker, priced-in status, usefulness, decision-ready flag, gap scores,
  missing/stale sources, and next step.
- The Streamlit **Priced-in Full Scan** panel now shows
  **Full-scan Ranked Rows** before trust/source gaps.
- The CLI `priced-in-audit` now prints the same full-scan row page before
  source rows.
- This does not run providers and does not add a bulk source-fill button.

Current live zero-call observation:

```text
priced-in-audit
priced_in_audit status=attention active=12613 scanned=12087 ranked=12087 ... external_calls=0
full_scan_rows=1-25/12087 sample=true export=catalyst-radar priced-in-queue --full-scan --all --json
full_scan_row_note=The tickers below are rows 1-25 from the current ranked page, not the full scan universe of 12087 row(s).
full_scan_preview:
A bullish_not_priced_in decision_useful true ...
MSFT bullish_not_priced_in decision_useful true ...
...
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_full_scan_audit_payload_consolidates_current_state tests\integration\test_dashboard_entrypoint.py::test_dashboard_wires_priced_in_full_scan_panel_after_usefulness tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_audit_cli_outputs_full_scan_audit -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py apps\dashboard\Home.py src\catalyst_radar\cli.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_entrypoint.py tests\integration\test_dashboard_demo_seed_cli.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-audit
$json = .\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-audit --json | ConvertFrom-Json; "preview_rows=$($json.preview_rows.Count) range=$($json.preview.row_start)-$($json.preview.row_end)/$($json.preview.total_rows) external_calls=$($json.external_calls_made) first=$($json.preview_rows[0].ticker)"
```

Observed:

- Focused three-test set passed (`3 passed`).
- Ruff passed.
- `git diff --check` passed.
- Live audit reported `preview_rows=25`, `range=1-25/12087`,
  `external_calls=0`, and first preview ticker `A`.
- PR #325 merged as `254923e`.
- Post-merge local services were restarted:
  - API health returned commit `254923ee6765`.
  - Streamlit health returned `ok`.
- Browser verification on `http://127.0.0.1:8514` showed the Overview tab
  rendering **Full-scan Ranked Rows** with rows 1-25 of 12,087 before
  **Full-scan Trust Gaps** and **Priced-in Source Gaps**.

Next useful product action:

- Actual source-fill execution still requires explicit user approval because it
  can call SEC/Schwab/market providers.

## Latest Web Full-Scan Trust Gaps

Current problem:

- The CLI/API `priced-in-answer` now keeps trust gaps visible when decision-ready
  rows exist, but the Streamlit **Priced-in Full Scan** panel did not show the
  same trust-gap distinction.
- Browser users could see decision-ready counts and source gaps, but not the
  explicit "full-scan trust gaps remain" table.

Fix in this slice:

- `priced_in_full_scan_audit_payload` now includes:
  - `trust_blockers`;
  - `source_coverage.trust_gap_count`.
- The Streamlit **Priced-in Full Scan** panel now shows:
  - a `Trust Gaps` badge;
  - a **Full-scan Trust Gaps** table before **Priced-in Source Gaps**.
- This reuses the existing zero-call trust-blocker logic. It does not add a
  provider call or a new planner.

Current live zero-call observation:

```text
priced-in-audit --json
status=attention external_calls=0 trust_gap_count=5 trust_blockers=5
first=catalyst_events
command=catalyst-radar priced-in-source-batches --source catalyst_events --all --json
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_full_scan_audit_payload_consolidates_current_state tests\integration\test_dashboard_entrypoint.py::test_dashboard_wires_priced_in_full_scan_panel_after_usefulness -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py apps\dashboard\Home.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_entrypoint.py
git diff --check
$json = .\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-audit --json | ConvertFrom-Json; ...
```

Observed:

- Focused two-test set passed (`2 passed`).
- Ruff passed.
- `git diff --check` passed.
- Live audit JSON reported `external_calls=0`, `trust_gap_count=5`, and
  `trust_blockers=5`.
- PR #324 merged as `42484b2`.
- Post-merge local services were restarted:
  - API health returned commit `42484b283034`.
  - Streamlit health returned `ok`.
- Browser verification on `http://127.0.0.1:8514` showed
  **Full-scan Trust Gaps** before **Priced-in Source Gaps** in the Overview tab.

Next useful product action:

- Actual source-fill execution still requires explicit user approval because
  it can call SEC/Schwab/market providers.

## Latest Priced-in Answer Trust Gaps

Current problem:

- `priced-in-answer` could report `decision_ready=true` and show the
  not-priced-in rows, but the payload suppressed `trust_blockers` when the
  answer status was `decision_ready`.
- That made the CLI/API answer easier to misread as "the whole full scan is
  fully trusted" even when source coverage still had catalyst/text/options/
  broker/bar gaps.

Fix in this slice:

- `priced_in_answer_payload` no longer hides trust gaps for decision-ready
  answers.
- The same answer can now say:
  - `decision_ready=true` for the filtered not-priced-in answer; and
  - `trust_blockers` for full-scan reliability gaps that still need attention.
- No provider calls are added. This only changes the zero-call answer payload
  and CLI rendering of fields that already existed.

Current live zero-call observation:

```text
priced-in-answer
priced_in_answer status=decision_ready decision_ready=true investment_decision_ready=false total=12087 ...
answer=Not fully priced for 10 decision-ready row(s); review the top evidence before any action.
source_coverage=market_bars 12087/12087; catalyst_events 9/5521 ...; options 0/12087 ...
trust_blockers:
- catalyst_events status=attention ... command=catalyst-radar priced-in-source-batches --source catalyst_events --all --json
- local_text status=attention ...
- options status=attention ...
- broker_context status=attention ...
- market_bars status=attention ...
external_calls=0
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_answer_keeps_trust_gaps_when_decision_ready tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_answer_cli_outputs_current_scan_answer -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py tests\integration\test_dashboard_data.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-answer
```

Observed:

- Focused two-test set passed (`2 passed`).
- Ruff passed.
- `git diff --check` passed.
- Live `priced-in-answer` reported `external_calls=0` and printed
  `trust_blockers` while still showing the 10 decision-ready not-priced-in rows.

Next useful product action:

- Consider surfacing the same trust-gap distinction in the Streamlit
  **Priced-in Full Scan** panel if users still confuse decision-ready rows with
  full-scan trust completion.

## Latest Web Dashboard Full-Scan Panel

Current problem:

- CLI/TUI now explain that the five-ticker lists are first provider chunks,
  but the Streamlit dashboard still led with older usefulness/operator sections.
- A human opening the browser dashboard could still miss the actual product
  question: "Has price fully matched market expectations across the full scan?"

Fix in this slice:

- Added a zero-call Streamlit section named **Priced-in Full Scan**.
- It uses the existing `priced_in_full_scan_audit_payload`, not a new planner.
- The panel appears immediately after **Market Radar Usefulness** and before
  **Operator Work Queue**.
- It shows:
  - full scan rows;
  - active securities;
  - research leads;
  - decision-ready rows;
  - ready source count;
  - full-scan review/export commands;
  - priced-in source gap rows sorted with gaps before no-gap sources.
- The panel includes `external_calls_made` and does not execute providers.

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_entrypoint.py::test_dashboard_wires_priced_in_full_scan_panel_after_usefulness -q
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_entrypoint.py::test_dashboard_wires_priced_in_full_scan_panel_after_usefulness tests\integration\test_dashboard_entrypoint.py::test_dashboard_wires_operator_work_queue_before_activation_sections tests\integration\test_dashboard_data.py::test_priced_in_full_scan_audit_payload_consolidates_current_state -q
.\.venv\Scripts\python.exe -m ruff check apps\dashboard\Home.py tests\integration\test_dashboard_entrypoint.py
git diff --check
```

Observed:

- Focused Streamlit wiring test passed.
- Focused three-test set passed (`3 passed`).
- Ruff passed.
- `git diff --check` passed.
- A broader `tests\integration\test_dashboard_entrypoint.py` run exceeded a
  4-minute timeout in this environment, so do not record it as passing.
- PR #321 merged as `054d2d3`.
- Post-merge local services restarted successfully:
  - API health returned `commit=054d2d3d72ac`.
  - Streamlit health returned `ok`.
- Browser verification on `http://127.0.0.1:8514` showed **Priced-in Full Scan**
  in the Overview tab, after **Market Radar Usefulness** and before
  **Operator Work Queue**, with the **Priced-in Source Gaps** table visible.

Next useful product action:

- Actual source-fill execution still requires explicit user approval because
  it can call SEC/Schwab/market providers.

## Latest Full-Scan Ticker Scope Clarification

Current problem:

- The user asked: "Why only these tickers? I want full scan."
- The stored priced-in queue already is a full-market scan, but the source-fill
  UX showed first safe ticker chunks in places where they could be mistaken for
  the whole scan universe.
- The Ops workbench also put no-op sources like `market_bars` ahead of source
  gaps, so pressing Enter could show a dead-end status instead of the first
  useful source-fill plan.

Fix in this slice:

- CLI all-source source-batch output now prints the full-scan row commands:
  - `full_scan_review=catalyst-radar priced-in-queue --full-scan --limit 50`
  - `full_scan_export=catalyst-radar priced-in-queue --full-scan --all --json`
- CLI source-specific source-batch output now prints source-gap row commands:
  - `review_full_scan_source_gap=... --source-gap <source> --limit 50`
  - `export_full_scan_source_gap=... --source-gap <source> --all --json`
- CLI recommendation batch lines now include
  `scope=first_provider_chunk`, making the five-ticker lists explicitly
  provider chunks, not the scan universe.
- TUI `batch <source>` responses now lead with
  `first provider chunk only` and include review/export commands for every
  matching full-scan row.
- TUI `batch all` responses now say the full scan universe size and distinguish
  full-scan rows from first safe provider chunks.
- Ops source workbench rows are sorted so sources with useful/gap work appear
  before no-gap/no-op sources.

Current live zero-call observation:

```text
priced-in-source-batches --source all --all
full_scan=mode=full_scan active=12613 scanned=12087 ranked=12087 source_gap_rows=48319 examples_are_samples=true
full_scan_review=catalyst-radar priced-in-queue --full-scan --limit 50
full_scan_export=catalyst-radar priced-in-queue --full-scan --all --json
coverage_first_batch=scope=first_provider_chunk rows=1-5 tickers=AAT,AAUC,AB,ABAT,ABBV calls=5 ...
decision_shortcut_batch=scope=first_provider_chunk rows=1-5 tickers=AAMI,AAOI,AAL,AAON,AAP calls=1 ...
external_calls=0

priced-in-source-batches --source catalyst_events --batch-limit 1
scan_scope=mode=full_scan gap_rows=12075 plannable=5510 returned_batches=1 planned_batches=1102 returned_tickers=5 batch_sample=true
review_full_scan_source_gap=catalyst-radar priced-in-queue --full-scan --source-gap catalyst_events --limit 50
export_full_scan_source_gap=catalyst-radar priced-in-queue --full-scan --source-gap catalyst_events --all --json
external_calls=0
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_demo_seed_cli.py -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\cli.py src\catalyst_radar\dashboard\tui.py tests\integration\test_dashboard_demo_seed_cli.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-source-batches --source all --all
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-source-batches --source catalyst_events --batch-limit 1
```

Observed:

- Dashboard integration file passed (`30 passed`).
- Ruff passed.
- `git diff --check` passed.
- Live all-source and catalyst source-batch commands reported
  `external_calls=0` and printed the new full-scan review/export commands.

Next useful product action:

- The full scan is present as a paged/exportable ranked universe. The remaining
  real work is filling source gaps across that universe through explicit,
  rate-limited provider chunks.
- Do not run external source-fill chunks automatically. If the user explicitly
  approves provider calls, the next coverage-first command remains:
  `catalyst-radar priced-in-source-batches --source catalyst_events --execute-next`.

## Latest Agent Brief Source Workflow Alignment

Current problem:

- The dry-run multi-agent brief used the older priced-in evidence-plan fields,
  but it ignored the newer `priced_in_source_workflow` dashboard payload.
- That meant the agent brief could drift from the dashboard's current
  coverage-first and decision-shortcut guidance.

Fix in this slice:

- The redacted agent snapshot now includes an allowlisted
  `priced_in.source_workflow` object:
  - status/headline/next action;
  - coverage-first action and command;
  - decision-shortcut action and command;
  - priority scope metadata;
  - sanitized source steps and sample tickers.
- The deterministic multi-agent brief now:
  - adds a `Priced-in source workflow...` insight;
  - includes coverage-first and decision-shortcut actions/commands in
    `next_actions`;
  - still makes zero OpenAI, market-data, and broker calls.
- Raw payload-like fields are still excluded by the redaction allowlist.

Current live zero-call observation:

```text
agent-brief --json
mode=dry_run status=dry_run calls={'broker': 0, 'market_data': 0, 'openai': 0}
Priced-in source workflow is attention; coverage-first=Fill SEC catalyst events...
NEXT:
catalyst-radar priced-in-source-batches --source catalyst_events --all --json
Start with broker_context...
catalyst-radar priced-in-source-batches --source broker_context --all --json
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\unit\test_agent_sdk_orchestrator.py -q
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_demo_seed_cli.py::test_agent_brief_cli_outputs_zero_call_dry_run tests\integration\test_dashboard_demo_seed_cli.py::test_agent_brief_cli_real_mode_blocks_without_explicit_gates -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\agents\sdk_orchestrator.py tests\unit\test_agent_sdk_orchestrator.py tests\integration\test_dashboard_demo_seed_cli.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli agent-brief --json
```

Observed:

- Agent SDK unit tests passed (`3 passed`).
- Focused agent CLI integration tests passed (`2 passed`).
- Ruff passed.
- `git diff --check` passed.
- Live dry-run brief reported `openai=0`, `market_data=0`, `broker=0`.

Next useful product action:

- The useful agent path is now aligned with the dashboard source workflow.
- Do not enable real OpenAI Agents SDK mode until the user explicitly chooses
  it and the gates are set:
  `CATALYST_ENABLE_AGENT_SDK=true`, `CATALYST_ENABLE_PREMIUM_LLM=true`,
  `CATALYST_LLM_PROVIDER=openai`, `CATALYST_AGENT_SDK_MODEL`, `OPENAI_API_KEY`.

## Latest TUI `batch all` First Chunks

Current problem:

- The CLI all-source plan now printed first safe chunks, but the TUI dashboard
  command `batch all` still summarized source statuses without the concrete
  first chunk details.
- Computing the all-source plan on every dashboard render would make normal
  browsing slower, so this was added only to the explicit on-demand command.

Fix in this slice:

- `batch all` in the TUI response now includes:
  - `Coverage-first chunk: <source> rows <start>-<end>; tickers ...; calls ...; command ...`
  - `Decision shortcut chunk: <source> rows <start>-<end>; tickers ...; calls ...; command ...`
- This is plan-only and still makes no provider calls.
- Actual execution still requires `batch <source> execute` or
  `batch <source> execute <N>`.

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_demo_seed_cli.py -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\tui.py tests\integration\test_dashboard_demo_seed_cli.py
git diff --check
```

Observed:

- Dashboard integration file passed (`30 passed`).
- Ruff passed.
- `git diff --check` passed.

Next useful product action:

- The remaining practical gap is not another plan display. It is the actual
  external source fill, which should only run after explicit user approval:
  - `catalyst-radar priced-in-source-batches --source catalyst_events --execute-next`
  - `catalyst-radar priced-in-source-batches --source broker_context --execute-next`

## Latest CLI Source Plan First Chunk

Current problem:

- `priced-in-source-batches --source all --all` already reported the
  coverage-first and decision-shortcut recommendations, but the human still had
  to run another plan command to see the first safe chunk tickers and command.
- That was unnecessary friction for the full-scan source-fill loop.

Fix in this slice:

- The all-source CLI printer now includes first-batch details for executable
  recommendations:
  - `coverage_first_batch=rows=... tickers=... calls=... command=...`
  - `decision_shortcut_batch=rows=... tickers=... calls=... command=...`
- This reuses fields already present in the existing API payload; no new route
  or duplicate planner was added.
- It remains plan-only and reports `external_calls=0`.

Current live zero-call observation:

```text
priced-in-source-batches --source all --all
coverage_first=source=catalyst_events gaps=12075 calls=5 command=catalyst-radar priced-in-source-batches --source catalyst_events --execute-next
  coverage_first_batch=rows=1-5 tickers=AAT,AAUC,AB,ABAT,ABBV calls=5 command=catalyst-radar ingest-sec submissions-batch --target ...
decision_shortcut=source=broker_context decision=5 actionable=7 calls=1 command=catalyst-radar priced-in-source-batches --source broker_context --execute-next
  decision_shortcut_batch=rows=1-5 tickers=AAMI,AAOI,AAL,AAON,AAP calls=1 command=catalyst-radar schwab-market-sync --ticker ...
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_demo_seed_cli.py -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\cli.py tests\integration\test_dashboard_demo_seed_cli.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-source-batches --source all --all
```

Observed:

- Dashboard integration file passed (`30 passed`).
- Ruff passed.
- `git diff --check` passed.
- Live command reported `external_calls=0` and printed both first-chunk lines.

Next useful product action:

- Do not run those chunks automatically.
- If the user approves external calls, the next guarded source-fill commands
  are:
  - coverage-first: `catalyst-radar priced-in-source-batches --source catalyst_events --execute-next`
  - decision shortcut: `catalyst-radar priced-in-source-batches --source broker_context --execute-next`
  - capped coverage: `catalyst-radar priced-in-source-batches --source catalyst_events --execute-batches 3`
  - capped broker context: `catalyst-radar priced-in-source-batches --source broker_context --execute-batches 3`

## Latest TUI Source Coverage Workbench

Current problem:

- The static Ops page already printed the full-scan source-gap workflow, but
  the interactive TUI table still led with provider health rows.
- That made the dashboard less useful for the actual product goal:
  improve the full-market answer by filling the most important missing data
  layers, without confusing a source-fill chunk with the full scan universe.

Fix in this slice:

- The interactive Ops page now uses a **Source coverage workbench** table:
  - one row per source workflow item;
  - columns: priority, source, status, gap rows, useful rows, examples,
    plan command, next action;
  - the detail panel states the coverage-first action and the decision
    shortcut.
- Pressing Enter or clicking a source row is plan-only:
  - it calls the existing `batch <source>` planning path;
  - it does **not** execute provider calls;
  - execution still requires an explicit `batch <source> execute` or
    `batch <source> execute <N>` command.
- The Ops guide text now says exactly that:
  - click/Enter source rows to inspect a plan;
  - execute only when the provider and call budget are intentional.

Current live zero-call observations:

```text
priced-in-source-batches --source all --all
status=ready sources=6 ready_sources=2 blocked_sources=2 gap_rows=48319 external_calls=0
full_scan active=12613 scanned=12087 ranked=12087
coverage_first=catalyst_events gaps=12075 calls=5
decision_shortcut=broker_context decision=5 actionable=7 calls=1
```

Live Ops static output still shows:

```text
Priced-in Source Gaps
catalyst_events partial ...
local_text partial ...
options missing ...
broker_context partial ...
Source Fill Workflow
Coverage-first: Fill SEC catalyst events...
Decision shortcut: Start with broker_context...
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_demo_seed_cli.py -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\tui.py tests\integration\test_dashboard_demo_seed_cli.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli dashboard-tui --once --page ops
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-source-batches --source all --all
```

Observed:

- Dashboard integration file passed (`30 passed`).
- Ruff passed.
- `git diff --check` passed.
- Live planning commands made `0` provider calls.

Next useful product action:

- The next CLI/API/dashboard gap is to make the **source plan itself**
  easier to act on:
  - expose the coverage-first recommendation and decision shortcut as compact
    API fields that the web dashboard/TUI can both display;
  - provide a one-command dry-run plan for the next safe chunk;
  - do not auto-run `catalyst_events` or `broker_context` because those are
    external-call workflows and must stay explicitly guarded.

## Latest TUI Full Scan vs Decision Review Clarification

User question that triggered this slice:

- "Why only these tickers? I want full scan."

Important product interpretation:

- MarketRadar already has a full stored priced-in scan in the local DB:
  - `12087` ranked rows from the latest scan page;
  - `12613` active securities in the current active-universe audit;
  - dashboard browsing and rendering still make `0` provider calls.
- The small ticker list is not the scan universe. It is the filtered
  decision-ready review subset:
  - `10` rows are not-priced-in and have enough local artifacts for human
    priced-in review;
  - those rows are still not trade approval;
  - optional context remains missing, especially `options` for 10 rows and
    `broker_context` for 5 rows.

Fix in this slice:

- Added a dedicated `review` / `decision-ready` TUI page:
  - page aliases: `11`, `d`, `decision`, `decision-ready`, `review`;
  - sidebar entry: `11 Decision Review`;
  - command `ready` now opens the review page instead of silently narrowing
    the Insights page.
- Added `dashboard_filters_for_page(...)`:
  - `review` forces `priced_in_status=actionable`,
    `priced_in_usefulness=decision_useful`, `priced_in_offset=0`;
  - normal Insights/overview keeps full scan defaults unless the user chooses
    a scan filter.
- Fixed the in-progress review-row logic:
  - `_priced_in_overview_rows(...)` now preserves the structured
    `usefulness` object instead of replacing it with display text;
  - `_priced_in_review_rows(...)` can now reliably identify
    `decision_useful` / `decision_ready` rows.
- Added open-row support for the review page:
  - `open 1`, Enter, or clicking a review row opens `candidate:<ticker>`;
  - the status message states that decision-ready is still not trade approval.
- `dashboard-tui --once --page review` now applies the same review filters as
  the interactive TUI, so CLI testing and human dashboard behavior match.

Current live zero-call observations after the fix:

```text
dashboard-tui --once --page overview --scan-mode all
Page: overview | View: Full scan | External calls made: 0
Full-market priced-in queue - showing rows 1-50 of 12087
Decision readiness: 10 not-priced-in row(s) are decision-ready.
```

```text
dashboard-tui --once --page review
Page: review | View: Decision-ready filter | External calls made: 0
Decision Review - priced-in answer, not trade approval
Remaining optional context: broker_context missing on 5; options missing on 10
```

```text
priced-in-answer
status=decision_ready decision_ready=true investment_decision_ready=false
total=12087 mismatches=12 blocked=7920 external_calls=0
scan_scope=Showing ranked rows 1-5 of 12087; the visible tickers are one page
from the full scan, not the scan universe.
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_demo_seed_cli.py -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\tui.py src\catalyst_radar\cli.py tests\integration\test_dashboard_demo_seed_cli.py
.\.venv\Scripts\python.exe -m catalyst_radar.cli dashboard-tui --once --page overview --scan-mode all
.\.venv\Scripts\python.exe -m catalyst_radar.cli dashboard-tui --once --page review
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-answer
```

Observed:

- Dashboard integration file passed (`30 passed`).
- Ruff passed.
- Full scan overview shows the paged full universe (`1-50 of 12087`), not a
  hardcoded watchlist.
- Decision Review shows the smaller human-review subset (`10` rows).
- All read/render commands reported `External calls made: 0`.

Next useful product action:

- Keep the dashboard centered on the full-market question:
  "Has price fully matched market expectations?"
- Improve full-scan navigation and source-gap workflows next:
  - make paging/export controls even more obvious in the TUI;
  - give the user one clear action to fill missing broad coverage;
  - avoid expanding Decision Cards for all 12k rows unless a later design
    proves that useful. The useful pattern is broad cheap scan, then deep
    review only for ranked rows that survive the evidence gates.

## Latest Local Candidate Packet / Decision Card Refresh

Current local DB action:

- After the manual-bar guidance fix was merged and the local services were
  restarted, the priced-in answer still recommended local artifacts:

  ```text
  catalyst-radar build-packets --as-of 2026-05-15 --ticker A --ticker MSFT --ticker AAMI --ticker AAOI --ticker AAA --ticker AAAU --ticker AAPL --ticker AA --ticker AAL --ticker AAON --ticker AAP --ticker AAAC --min-state ResearchOnly
  ```

- That command was run from `main` and built:

  ```text
  built candidate_packets=12
  ```

- The follow-up decision-card command was then run for the remaining local
  decision-card gaps:

  ```text
  catalyst-radar build-decision-cards --as-of 2026-05-15 --ticker A --ticker AAMI --ticker AAOI --ticker AAA --ticker AAAU --ticker AA --ticker AAL --ticker AAON --ticker AAP --ticker AAAC --min-state ResearchOnly
  built decision_cards=10
  ```

Current live zero-call observation after local artifact generation:

```text
priced_in_answer status=decision_ready decision_ready=true investment_decision_ready=false total=12087 mismatches=12 research=0 blocked=7920 external_calls=0
answer=Not fully priced for 10 decision-ready row(s); review the top evidence before any action.
decision_readiness=status=ready actionable=12 decision_ready=10
```

`priced-in-queue --decision-ready --limit 20` now reports:

```text
count=10 total=10 external_calls=0
source_coverage=market_bars 10/10; catalyst_events 9/9; local_text 10/10; options 0/10 (10 missing); theme_peer_sector 10/10; broker_context 5/10 (5 missing)
```

Dashboard snapshot:

```text
Page: overview | View: Full scan | Answer: decision ready ready=true | Trade status: research only | Trade safe: False | External calls made: 0
Decision readiness: 10 not-priced-in row(s) are decision-ready.
```

Important interpretation:

- MarketRadar can now answer the full-scan priced-in question for the current
  stored scan at a research/decision-support level:
  - 10 rows look not fully priced in and have local Decision Cards.
  - The answer is still **not trade approval**.
- Overall readiness remains `research_only` because:
  - active-universe market-bar coverage is still partial (`12090/12613`);
  - options are still missing for the 10 decision-ready rows;
  - broker context is missing for 5 of those 10 rows;
  - full source coverage remains incomplete outside the decision-ready subset.
- Do not rerun packet/card generation unless a new scan, new as-of date, or
  new evidence changes the candidate set.

Next useful product action:

- Improve the dashboard/CLI review path for the 10 decision-ready rows:
  - make the Decision Card view easy to open from the TUI;
  - separate "priced-in decision-ready" from "safe to trade";
  - surface the remaining optional gaps (`options`, `broker_context`) in a
    concise review checklist.
- To move from research-ready to fuller coverage, fill the missing active
  market bars through the manual template/import path.

## Latest Manual-Bar Guidance Alignment

Current problem:

- `market-radar-status` correctly told the operator to use the DB-backed
  manual market-bar template/import path when the full active universe had
  partial bar coverage.
- `priced-in-answer` and `priced-in-preflight` still surfaced a provider
  `run-daily` command from the market-bars attention row. In the current local
  environment this was confusing because the user is intentionally avoiding
  Polygon/Massive unless explicitly configured.

Fix in this slice:

- The priced-in preflight `market_bars` attention row now points directly at:

  ```text
  catalyst-radar market-bars template --expected-as-of <LATEST_TRADING_DATE> --out data\local\manual-bars-<LATEST_TRADING_DATE>.csv
  ```

- The row keeps status `attention`, because `12090/12613` active securities
  have run-as-of bars and that remains broad enough for research, but it now
  clearly says to generate the active-universe template if full active coverage
  is required before relying on the answer.
- The API field changed with the command:

  ```text
  POST /api/radar/market-bars/template
  ```

- `priced-in-answer` trust blockers inherit the same manual-template command,
  so the answer view no longer tells the user to fix this partial coverage by
  making a Polygon-style provider run.

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_preflight_uses_manual_bar_template_for_partial_full_scan_bars tests\integration\test_dashboard_data.py::test_priced_in_preflight_recommends_manual_bar_template_for_missing_bars tests\integration\test_dashboard_data.py::test_priced_in_answer_payload_summarizes_current_scan tests\integration\test_dashboard_data.py::test_priced_in_full_scan_audit_payload_consolidates_current_state -q
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-preflight
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-answer
```

Observed:

- Focused regression passed (`4 passed`).
- Live CLI checks made `0` provider calls.
- Live `priced-in-answer` trust blocker now prints
  `command=catalyst-radar market-bars template ...` for partial market-bar
  coverage.

Next useful product action from this older slice has been completed locally:
Candidate Packets and Decision Cards were built for the current actionable
mismatch rows. Keep provider calls guarded: reviewing preflight/answer/queue
remains zero-call.

## Latest Non-Company Evidence Surface

Current problem:

- Non-company rows were correctly routed away from SEC company filing
  requirements, but the UI still mostly said "routed" rather than showing what
  evidence the route actually had.
- For ETF/fund/warrant/wrapper rows, the user needs to see the concrete local
  evidence path:
  - what instrument this is;
  - how emotion compares with price reaction;
  - what theme/sector context exists;
  - whether flow/volume features are present;
  - whether the underlying/fund objective is stored or only inferred from name.

Fix in this slice:

- Queue rows and ticker detail evidence briefs now include
  `non_company_evidence` for non-company instruments.
- The payload is local-only and makes no provider calls:

  ```text
  schema_version=priced-in-non-company-evidence-v1
  route=market_theme_fund_or_flow
  external_calls_made=0
  ```

- Evidence checkpoints currently include:
  - `instrument_identity`
  - `market_reaction`
  - `theme_sector_context`
  - `flow_volume_context`
  - `fund_objective` or `underlying_hint` when available/inferable
- CLI `priced-in-queue` and `candidate-detail` print the non-company evidence
  summary.
- TUI overview rows include the non-company evidence summary in the "Why now"
  text, so the dashboard has a human-visible route instead of only a hidden JSON
  field.
- Security metadata lookup now returns name, exchange, sector, industry,
  market-cap, average dollar volume, options availability, and metadata so the
  local evidence payload can be built without new storage or providers.

Current live zero-call observation after the change:

```text
priced_in_queue status=ready count=8 total=12087 offset=0 external_calls=0
scan_scope=scanned=12087 requested=n/a filter=all ranked_after_filter=12087 visible_page=8
instrument_scope=rows=12087 company_like=5521 non_company=6566 unknown=0

AAA non_company_evidence=status=available route=market_theme_fund_or_flow
AAAU non_company_evidence=status=available route=market_theme_fund_or_flow
```

Example live row:

```text
AAAU: Goldman Sachs Physical Gold ETF Shares
emotion 63.72 vs reaction 0
theme/peer/sector scores=0/0/50
```

Important interpretation:

- This does not make the full scan decision-ready by itself.
- It makes the non-company route inspectable from the API/CLI/TUI using stored
  local data.
- Full-scan audit remains authoritative for full-market decision readiness:

  ```text
  research=10
  decision=0
  external_calls=0 while viewing
  ```

- `candidate-detail <ticker>` may see newer post-run artifacts for one ticker;
  do not confuse that with the full-scan audit count.

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_answer_payload_summarizes_current_scan tests\integration\test_dashboard_data.py::test_priced_in_full_scan_audit_payload_consolidates_current_state tests\integration\test_dashboard_data.py::test_priced_in_queue_payload_reports_full_scan_instrument_scope tests\integration\test_dashboard_data.py::test_priced_in_queue_payload_routes_non_company_usefulness_through_theme_context tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_classifies_non_company_cik_gaps tests\integration\test_dashboard_data.py::test_priced_in_queue_payload_filters_source_gaps tests\integration\test_dashboard_data.py::test_priced_in_queue_payload_filters_decision_gaps tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_queue_cli_outputs_same_zero_call_signal tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_queue_cli_prints_non_company_evidence_route tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_overview_rows_include_non_company_evidence_summary tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_audit_cli_outputs_full_scan_audit tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_tui_once_can_show_full_scan_mode tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_source_batches_cli_prints_non_company_route -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\dashboard\tui.py src\catalyst_radar\cli.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-queue --full-scan --limit 8
.\.venv\Scripts\python.exe -m catalyst_radar.cli candidate-detail AAAU
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-audit
.\.venv\Scripts\python.exe -m catalyst_radar.cli dashboard-tui --once --page overview
```

Observed:

- Focused integration pass passed (`13 passed`).
- Ruff passed.
- `git diff --check` passed.
- Live CLI/TUI smokes made `0` provider calls.

Next useful product action:

- The next bottleneck is still decision usefulness:
  - build candidate packets/decision cards for the actionable mismatch rows; and
  - fill or explicitly waive options/broker context for rows where that context
    is only optional.
- Improve the dashboard answer panel so it clearly separates:
  - full-scan audit state;
  - per-ticker detail state;
  - post-run artifacts that are useful but not yet counted in the latest full
    scan.

## Latest Non-Company Usefulness Route Correction

User-facing problem:

- The full scan was already broad, but the queue could still feel like "only
  these tickers" because the visible table is a page through the ranked scan.
- More importantly, non-company rows were still scored as not decision-useful
  when they lacked company-style evidence:
  - `catalyst_events`
  - `local_text`
- That was wrong for ETF/fund/warrant/wrapper rows. Those rows should remain in
  the full scan, but their first useful evidence route is market reaction plus
  theme/sector/fund/flow/underlying context, not SEC company filings.

Fix in this slice:

- Priced-in queue rows now carry an explicit row-level `instrument` payload:
  - `security_type`
  - `category` (`company_like`, `non_company`, `unknown`)
  - `evidence_route`
  - `sec_catalyst_applicable`
- Usefulness scoring is now instrument-aware:
  - company-like/unknown rows still require `market_bars`, `catalyst_events`,
    and `local_text` as core sources;
  - non-company rows require `market_bars` and `theme_peer_sector` as core
    sources;
  - missing `catalyst_events` and `local_text` on non-company rows are reported
    as routed optional context, not decision blockers.
- Candidate detail evidence briefs now receive instrument metadata when loading
  a ticker detail view.
- CLI and TUI row summaries hide routed company-style sources from the primary
  "missing" display and show them as routed instead.
- Source-gap planning still sees routed rows. That is intentional: the planner
  must be able to say "these non-company rows are routed", not incorrectly
  return "no gaps".

Important interpretation:

- Full scan remains the broad ranked universe:

  ```text
  active=12613
  scanned=12087
  ranked=12087
  visible_page=5 in the CLI smoke, 50 in the TUI overview
  ```

- "Only these tickers" means the UI is showing the current page or provider-safe
  batch sample, not the scan universe.
- Current live instrument scope:

  ```text
  rows=12087
  company_like=5521
  non_company=6566
  unknown=0
  ```

- Current live decision state:

  ```text
  research_useful=10
  decision_useful=0
  external_calls=0 while viewing
  ```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_answer_payload_summarizes_current_scan tests\integration\test_dashboard_data.py::test_priced_in_full_scan_audit_payload_consolidates_current_state tests\integration\test_dashboard_data.py::test_priced_in_queue_payload_reports_full_scan_instrument_scope tests\integration\test_dashboard_data.py::test_priced_in_queue_payload_routes_non_company_usefulness_through_theme_context tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_classifies_non_company_cik_gaps tests\integration\test_dashboard_data.py::test_priced_in_queue_payload_filters_source_gaps tests\integration\test_dashboard_data.py::test_priced_in_queue_payload_filters_decision_gaps tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_queue_cli_outputs_same_zero_call_signal tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_audit_cli_outputs_full_scan_audit tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_tui_once_can_show_full_scan_mode tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_source_batches_cli_prints_non_company_route -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\dashboard\tui.py src\catalyst_radar\cli.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-queue --full-scan --limit 5
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-audit
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-source-batches --source catalyst_events --limit 1
.\.venv\Scripts\python.exe -m catalyst_radar.cli dashboard-tui --once --page overview
```

Observed:

- Focused integration pass passed (`11 passed`).
- Ruff passed.
- `git diff --check` passed.
- Live CLI/TUI smokes made `0` provider calls.
- Live queue smoke printed:

  ```text
  priced_in_queue status=ready count=5 total=12087 offset=0 external_calls=0
  scan_scope=scanned=12087 requested=n/a filter=all ranked_after_filter=12087 visible_page=5
  headline=Latest full scan ranked 12087 priced-in row(s); showing 1-5 of 12087.
  ```

Next useful product action:

- Build the actual non-company evidence source instead of only routing around
  company-style evidence:
  - ETF/fund description and objective;
  - underlying or constituent exposure when available;
  - sector/theme exposure;
  - flow/volume confirmation;
  - local notes/text where available.
- Then improve the dashboard so the main "Insights" view leads with the actual
  answer to the user's question: which stocks look emotionally underpriced or
  overpriced relative to market expectations, and why.

## Latest Non-Company Catalyst Route Correction

Current problem:

- After instrument scope was added, MarketRadar could explain that the full scan
  contained both operating companies and non-company instruments.
- But `catalyst_events` planning still treated ETF/fund/wrapper rows as if they
  belonged in SEC company filing batches whenever those rows had a catalyst gap.
- That was still misleading for the user goal. Full scan must remain broad, but
  SEC company filing calls must not be planned for non-company instruments.

Fix in this slice:

- `catalyst_events` source coverage is now instrument-aware:
  - company-like/unknown rows remain SEC-catalyst applicable;
  - ETF/fund/ETN/right/warrant/wrapper rows are routed to non-company evidence;
  - source coverage summary prints routed non-company rows separately.
- `priced-in-source-batches --source catalyst_events` now plans SEC submission
  batches only for company-like/unknown rows with CIKs.
- Non-company source-gap rows are reported as `routed`, not as missing-CIK
  blockers.
- CLI/TUI batch summaries now show `routed` counts and route examples.

Current live zero-call observation after the change:

```text
priced_in_queue source_coverage=market_bars 12087/12087; catalyst_events 9/5521 (5512 missing, 6563 non-company routed); local_text 12/12087 (12075 missing); options 0/12087 (12087 missing); theme_peer_sector 12087/12087; broker_context 5/12087 (12082 missing)

priced_in_source_batches source=catalyst_events status=ready gap_rows=12075 plannable=5510 routed=6563 external_calls=0
diagnostic=status=eligible eligible=5510 blocked=2 reason=SEC event batches require CIK metadata for each ticker.
blocked_examples=FRBA,SSBI reason=missing_cik
non_company_route=routed=6563 examples=ABLVW,DAICW,DFSCW,FGIWW,GIPRW route=Use fund, underlying, theme, sector, flow, or constituent evidence instead of SEC company filing batches.
```

Important interpretation:

- This is still a full-market scan.
- SEC catalyst batches now target only the applicable company-like side of the
  universe.
- The remaining SEC-specific blocker is small and concrete: `FRBA` and `SSBI`
  are company-like rows still missing CIK metadata.
- The non-company route is now explicit, but the actual fund/underlying/flow
  evidence ingestion path is still the next missing product slice.

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_answer_payload_summarizes_current_scan tests\integration\test_dashboard_data.py::test_priced_in_full_scan_audit_payload_consolidates_current_state tests\integration\test_dashboard_data.py::test_priced_in_queue_payload_reports_full_scan_instrument_scope tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_classifies_non_company_cik_gaps tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_batch_command_explains_non_company_cik_gaps tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_source_batches_cli_prints_non_company_route tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_audit_cli_outputs_full_scan_audit tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_tui_once_can_show_full_scan_mode -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\dashboard\tui.py src\catalyst_radar\cli.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-audit
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-source-batches --source catalyst_events --limit 1
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-queue --full-scan --limit 1
```

Observed:

- Focused integration pass passed (`8 passed`).
- Ruff passed.
- `git diff --check` passed.
- Live CLI smokes made `0` provider calls.

Next useful product action:

- Build the actual non-company evidence source for ETF/fund/wrapper rows:
  underlying or constituent context, fund/theme description, sector exposure,
  flow/volume confirmation, and local text where available.
- Then update usefulness scoring so non-company rows are judged against that
  evidence route rather than the company catalyst/local-text path.

## Latest Full-Scan Instrument Scope Correction

User-facing problem:

- The dashboard/CLI showed only the current candidate page or one provider batch,
  which made it look like MarketRadar was scanning only a handful of tickers.
- The full scan was actually still broad:

  ```text
  active=12613
  scanned=12087
  ranked=12087
  visible_page=1..50 by default
  ```

- The confusing part was evidence routing. SEC company filings apply to
  operating-company rows, not every active market instrument. ETF/fund/wrapper
  rows must stay in the full scan, but they need fund, underlying, theme, or
  flow evidence instead of operating-company SEC filings.

Fix in this slice:

- Added `instrument_scope` to the priced-in queue and full-scan audit payloads.
- The scope classifies every ranked full-scan row by security metadata type and
  exposes:
  - `company_like_rows`
  - `non_company_rows`
  - `unknown_type_rows`
  - `type_counts`
  - `sec_catalyst_applicability`
- CLI `priced-in-audit` and `priced-in-queue` now print the instrument scope,
  including the SEC applicability boundary.
- TUI overview now includes a human-readable `Instrument scope:` line so the
  operator can see that visible tickers are only a page, not the scan universe.

Current live zero-call observation after the change:

```text
priced_in_audit status=attention active=12613 scanned=12087 ranked=12087 research=10 decision=0 external_calls=0
instrument_scope=rows=12087 company_like=5521 non_company=6566 unknown=0 types=ADRC:370,CS:5151,ETF:4987,ETN:49,ETS:106,ETV:85,FUND:337,PFD:412,RIGHT:52,SP:150,UNIT:100,WARRANT:288
sec_catalyst_applicability=applicable=5521 non_applicable=6566 unknown=0
```

Important interpretation:

- This is still a full-market scan, not a watchlist scan.
- The visible table is just a page through the ranked scan.
- SEC catalyst coverage should be filled for the `5521` company-like rows.
- The `6566` ETF/fund/wrapper rows need a separate non-company evidence route.
- Current status remains `attention` / research-only: the system is not yet
  decision-useful.

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_answer_payload_summarizes_current_scan tests\integration\test_dashboard_data.py::test_priced_in_full_scan_audit_payload_consolidates_current_state tests\integration\test_dashboard_data.py::test_priced_in_queue_payload_reports_full_scan_instrument_scope tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_classifies_non_company_cik_gaps tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_audit_cli_outputs_full_scan_audit tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_snapshot_cli_outputs_dashboard_command_center_json tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_tui_once_can_show_full_scan_mode tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_batch_command_explains_non_company_cik_gaps -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\dashboard\tui.py src\catalyst_radar\cli.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-audit
.\.venv\Scripts\python.exe -m catalyst_radar.cli dashboard-tui --once --page overview
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-queue --full-scan --limit 1
```

Observed:

- Focused integration pass passed (`8 passed`).
- Ruff passed.
- `git diff --check` passed.
- Live CLI/TUI smokes made `0` provider calls.

Next useful product action:

- Implement the non-company evidence route so ETF/fund/wrapper rows can be
  evaluated through underlying/theme/fund-flow evidence instead of being blocked
  behind SEC company filings.
- Continue capped SEC catalyst batches only for company-like rows when the SEC
  call budget is intentional.

## Latest SEC CIK Diagnostic Correction

Current problem:

- After PR #307, the next full-scan catalyst blocker was clear:

  ```text
  catalyst_events gap_rows=12075
  plannable=10474
  blocked_missing_cik=1601
  ```

- The planner recommended `catalyst-radar ingest-sec company-tickers` for the
  blocked rows.
- Running that live SEC refresh made exactly one external SEC call and updated
  zero active securities:

  ```text
  refreshed_sec_cik_metadata provider=sec live=True active=12613 missing_before=1602 matched=0 updated=0 missing_after=1602 external_calls=1
  unmatched_examples=AAAA,AAEQ,AAOG,AAOX,AAPD,AAPU,AAPW,AAUA,AAUS,AAVM
  ```

- Inspection showed the remaining missing-CIK active rows are mostly not
  operating-company stocks:

  ```text
  ETF=1534, ETS=63, CS=2, RIGHT=1, WARRANT=1, ETN=1
  ```

Fix in this slice:

- `priced_in_source_gap_batches_payload(... source="catalyst_events")` now
  classifies missing-CIK blockers by instrument type.
- The diagnostic now distinguishes:
  - company-like rows (`CS`, `ADRC`) that may be fixed by SEC company tickers;
  - non-company instruments (`ETF`, `ETS`, `ETN`, `RIGHT`, `WARRANT`, etc.) that
    should not be expected to clear through SEC company tickers;
  - unknown-type rows.
- CLI output now prints:

  ```text
  missing_cik_types=CS:2,ETF:1533,ETN:1,ETS:63,RIGHT:1,WARRANT:1 company_like=2 non_company=1599 unknown=0
  non_company_cik_examples=AMDD,BOXX,CAFX,CLIP,IQMM
  company_like_cik_examples=FRBA,SSBI
  diagnostic_next=Refresh SEC company tickers only for the small company-like/unknown subset, then handle ETF/ETN/fund-like rows through fund, underlying, or theme evidence instead of SEC company filings.
  ```

- TUI `batch catalyst_events` messages now include the missing-CIK type
  breakdown instead of hiding this as a generic metadata problem.

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_exposes_missing_cik_blockers tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_classifies_non_company_cik_gaps tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_source_batches_cli_prints_blocked_source_samples tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_batch_command_explains_non_company_cik_gaps -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\dashboard\tui.py src\catalyst_radar\cli.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-source-batches --source catalyst_events --limit 1
```

Observed:

- Focused tests passed (`4 passed`).
- Ruff passed.
- Live source-batch planning made `0` provider calls and now shows the
  missing-CIK instrument breakdown.

Important interpretation:

- The remaining missing-CIK rows are not a normal metadata-refresh backlog.
- For full-market priced-in work, SEC filings are an operating-company catalyst
  source. ETF/ETN/fund-like rows need a different evidence route, likely
  underlying/theme/fund flow evidence, or they should be scoped separately from
  SEC catalyst coverage.

Next useful product action:

- Add an explicit non-company/ETF evidence route or filter so the full-scan
  audit stops treating ETF-style instruments as if they require operating-company
  SEC filings.
- Keep using capped `catalyst_events` batches for the `10474` CIK-backed rows
  when the SEC call budget is intentional.

## Latest Capped Source-Batch Runner

Current problem:

- The full scan is now correctly exposed as the whole ranked universe, but
  filling source evidence still required manually repeating one provider chunk
  at a time.
- For the current live full scan, `catalyst_events` is the broadest emotion-side
  blocker:

  ```text
  gap_rows=12075
  plannable=10474
  total_batches=2095
  batch_size=5
  blocked_missing_cik=1601
  ```

Fix in this slice:

- Added a capped source-batch runner that loops the existing guarded one-chunk
  executor:

  ```powershell
  catalyst-radar priced-in-source-batches --source catalyst_events --execute-batches 3
  ```

- The runner:
  - executes up to `N` chunks only after the explicit `--execute-batches N`
    flag is provided;
  - stops early on blocked, failed, or no-action results;
  - keeps every existing provider guardrail from `--execute-next`;
  - caps API requests at `max_batches <= 50`;
  - reports before/after gap rows, plannable rows, executed chunks, and external
    calls.
- API support was added without breaking the existing endpoint:

  ```text
  POST /api/radar/priced-in/source-batches/execute-next
  body: {"source":"catalyst_events","max_batches":3}
  ```

  `max_batches` defaults to `1`, so existing one-chunk callers keep the old
  payload.
- TUI command support was added:

  ```text
  batch catalyst_events execute
  batch catalyst_events execute 3
  ```

  The Ops page now explains that `execute` runs one guarded chunk and
  `execute 3` runs a capped batch set.
- Source-batch planning now prints the capped runner command:

  ```text
  execute_batches=catalyst-radar priced-in-source-batches --source catalyst_events --execute-batches 3
  ```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_batch_execute_can_run_capped_chunks tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_snapshot_ops_page_shows_priced_in_source_actions tests\integration\test_dashboard_demo_seed_cli.py::test_source_batch_run_executes_capped_chunks_and_reports_delta tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_source_batches_execute_batches_cli_runs_capped_batch_run tests\integration\test_api_routes.py::test_post_radar_priced_in_source_batch_execute_next_can_run_capped_batches -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\source_batches.py src\catalyst_radar\dashboard\data.py src\catalyst_radar\cli.py src\catalyst_radar\api\routes\radar.py src\catalyst_radar\dashboard\tui.py tests\integration\test_dashboard_demo_seed_cli.py tests\integration\test_api_routes.py
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-source-batches --source catalyst_events --limit 1
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-source-batches --source all --execute-batches 3
.\.venv\Scripts\python.exe -m catalyst_radar.cli dashboard-tui --once --page ops
```

Observed:

- Focused tests passed (`5 passed` in the final focused run; `9 passed` in the
  wider touched source/API/CLI group before the stale Ops assertion was
  corrected).
- Ruff passed.
- Live `catalyst_events` planning made `0` provider calls and now shows
  `execute_batches=... --execute-batches 3`.
- Live guard check correctly rejects `--source all --execute-batches 3` because
  all-source remains plan-only.
- Live TUI Ops made `0` provider calls and explains the capped runner.

Important:

- This does not execute live SEC calls by itself.
- This does not make MarketRadar decision-useful yet.
- It makes the broad evidence fill operational: the user can now intentionally
  advance full-market `catalyst_events` coverage by capped chunks instead of
  repeating a one-chunk command manually.

Next useful product action:

- Use the capped runner to fill `catalyst_events` only when the SEC call budget
  is intentional.
- Refresh CIK metadata with `catalyst-radar ingest-sec company-tickers` to clear
  the current `1601` missing-CIK catalyst rows.
- After enough catalyst event text exists, run capped `local_text` batches to
  score the market emotion narrative over the same full scan.

## Latest Full-Scan Audit Surface

Current problem:

- The dashboard had separate surfaces for priced-in answer, preflight,
  source batches, and source workflow.
- Those pieces were accurate, but they still required the operator to mentally
  assemble the answer to:

  ```text
  Can MarketRadar currently answer whether price matches market expectations?
  If not, what exact full-scan coverage is missing?
  ```

Fix in this slice:

- Added `priced_in_full_scan_audit_payload()` as a zero-call consolidated audit
  over existing local queue/preflight data.
- Added CLI:

  ```powershell
  catalyst-radar priced-in-audit
  catalyst-radar priced-in-audit --json
  ```

- Added API route:

  ```text
  GET /api/radar/priced-in/audit
  ```

- Added the audit payload to the TUI/dashboard snapshot and displayed a compact
  Insights line:

  ```text
  Full scan audit: attention; ranked 12087/12613; bars 12090/12613; sources 2/6; next Review the run call plan and refresh event ingestion before trusting emotion.
  ```

What the live audit says before this slice is merged/restarted:

```text
priced_in_audit status=attention active=12613 scanned=12087 ranked=12087 research=10 decision=0 external_calls=0
answer=Partially. MarketRadar has research output, but source or coverage gaps still need attention before trusting the answer.
market_bars=status=attention coverage=12090/12613 missing=523 coverage_pct=95.9
source_coverage=ready=2/6 weak=options,broker_context,catalyst_events
next_action=Review the run call plan and refresh event ingestion before trusting emotion.
next_command=catalyst-radar priced-in-source-batches --source catalyst_events --all --json
```

Important interpretation:

- This does not make any provider calls.
- This does not claim the system is investable.
- The current state is still research-only/attention:
  - `12087/12613` active securities are ranked.
  - `523` active securities still lack as-of bars.
  - Only `2/6` priced-in source classes are fully covered.
  - There are `10` research leads and `0` decision-ready rows.
- The next broad evidence action remains catalyst-event coverage, not current
  options sync.

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_full_scan_audit_payload_consolidates_current_state tests\integration\test_dashboard_data.py::test_priced_in_answer_payload_summarizes_current_scan tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_audit_cli_outputs_full_scan_audit tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_snapshot_cli_outputs_dashboard_command_center_json tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_tui_once_can_show_full_scan_mode tests\integration\test_api_routes.py::test_get_radar_priced_in_audit_returns_zero_call_audit tests\integration\test_api_routes.py::test_get_radar_priced_in_answer_returns_current_scan_answer -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\dashboard\tui.py src\catalyst_radar\api\routes\radar.py src\catalyst_radar\cli.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py tests\integration\test_api_routes.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-audit
.\.venv\Scripts\python.exe -m catalyst_radar.cli dashboard-tui --once --page overview
```

Observed:

- Focused tests passed (`7 passed`).
- Ruff passed.
- `git diff --check` passed.
- Live CLI/TUI branch smokes made 0 provider calls.
- Live API curl returned 404 before merge because the running service was still
  the prior `main` build. Re-run API curl after this PR is merged and local
  services are restarted.

Next useful product slice:

- Use the audit output as the human/operator entry point.
- Then either:
  - fill the 523 as-of daily-bar gaps through the manual bar import path, or
  - proceed with explicit `catalyst_events` source batches if accepting the
    current `95.9%` market-bar coverage as research-only.

## Latest Full-Scan Source Scope And Options Shortcut Fix

User asked again:

```text
Why only these tickers? I want full scan
```

Current live answer:

- MarketRadar is not scanning only the visible tickers.
- The current latest useful scan is full-active scoped:

  ```text
  active securities=12613
  scanned/ranked rows=12087
  visible CLI answer page=1-5
  visible TUI overview page=1-50
  ```

- The visible tickers are a human-review page or a provider-safe source-fill
  chunk, not the product universe.
- The source-fill overview now prints this explicitly:

  ```text
  full_scan=mode=full_scan active=12613 scanned=12087 ranked=12087 source_gap_rows=48319 examples_are_samples=true
  scope_note=The full scan covers 12087 ranked row(s). Source rows, first batches, and example tickers are coverage summaries or provider-safe chunks, not the scan universe.
  ```

Root cause fixed in this slice:

- `priced-in-source-batches --source all` had no top-level full-scan scope
  block, so the first visible example tickers still felt like the universe.
- The all-source planner treated `options` as a runnable Schwab chunk even when
  the options diagnostic said stored option features were newer than the scan
  date. That was wrong for point-in-time evidence:

  ```text
  Stored options exist after this scan date.
  Rerun only with a current scan date and current bars, or ingest point-in-time options.
  ```

- The TUI Ops page used a separate source workflow payload and still showed
  `Decision shortcut: options`, even after the CLI all-source planner correctly
  diagnosed options as non-point-in-time.

Fix in this slice:

- `priced_in_all_source_gap_batches_payload()` now includes:

  ```text
  scan_scope.schema_version=priced-in-source-overview-scan-scope-v1
  scan_scope.mode=full_scan
  scan_scope.active_securities
  scan_scope.scanned_rows
  scan_scope.ranked_rows
  scan_scope.source_gap_rows
  scan_scope.examples_are_samples=true
  scan_scope.review_full_scan_command
  scan_scope.export_full_scan_command
  ```

- The CLI all-source source-batch overview prints the full-scan scope before
  any source recommendations.
- `_priced_in_source_plannable_rows()` now blocks `options` source batches when
  the options diagnostic is non-point-in-time (`newer_than_scan`,
  `after_decision_cutoff`, or `eligible_but_not_scored`).
- Blocked options plans expose the nested option diagnostic and no longer emit
  an `execute_next_command`.
- The TUI source workflow now skips options as a decision shortcut when its
  action is explicitly non-point-in-time. In the current live data it now
  recommends read-only `broker_context` as the smaller shortcut while keeping
  broad coverage-first work on `catalyst_events`.

Live zero-call observations after the fix:

```text
priced_in_source_batch_overview status=ready sources=6 ready_sources=2 blocked_sources=2 gap_rows=48319 external_calls=0
full_scan=mode=full_scan active=12613 scanned=12087 ranked=12087 source_gap_rows=48319 examples_are_samples=true
coverage_first=source=catalyst_events gaps=12075 calls=5
decision_shortcut=source=broker_context decision=0 actionable=7 calls=1
options blocked 12087 ... plannable 0 ... next=Stored options exist after this scan date...
```

The live TUI Ops page now shows:

```text
Decision shortcut: Start with broker_context; it clears evidence for 5 research-useful row(s) in the visible ranked page.
Full scan = the whole ranked universe. Source-fill tickers = the next rate-limited provider chunk, not the ticker universe.
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_all_source_gap_batches_payload_summarizes_next_chunks tests\integration\test_dashboard_data.py::test_priced_in_all_source_gap_batches_prioritizes_decision_useful_gaps tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_plans_safe_sync_batches tests\integration\test_dashboard_data.py::test_priced_in_queue_payload_diagnoses_options_after_scan_date tests\integration\test_dashboard_data.py::test_priced_in_all_source_gap_batches_blocks_options_shortcut_when_not_point_in_time tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_snapshot_cli_outputs_dashboard_command_center_json tests\integration\test_dashboard_demo_seed_cli.py::test_source_workflow_skips_non_point_in_time_options_shortcut tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_queue_cli_outputs_same_zero_call_signal tests\integration\test_api_routes.py::test_get_radar_priced_in_source_batches_can_return_all_source_overview -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\dashboard\tui.py src\catalyst_radar\cli.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py tests\integration\test_api_routes.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-source-batches --source all --limit 1
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-source-batches --source options --limit 1
.\.venv\Scripts\python.exe -m catalyst_radar.cli dashboard-tui --once --page ops
```

Observed:

- Focused dashboard/API/TUI tests passed (`9 passed`).
- Ruff passed.
- `git diff --check` passed.
- Live source-batch and TUI smokes made 0 provider calls.
- A broader `pytest tests\integration\test_dashboard_data.py
  tests\integration\test_dashboard_demo_seed_cli.py ...` run was attempted but
  exceeded the 300-second tool timeout, so do not count that full broad run as
  passed.

Next useful product action:

- For broad evidence coverage, start with `catalyst_events` because it has
  12,075 full-scan gaps and is source-of-emotion evidence.
- For a smaller non-options shortcut, use `broker_context` only as read-only
  supporting context.
- Do not sync current options for the old 2026-05-15 scan as if they were
  point-in-time evidence. Either rerun with current bars or ingest historical
  option features for the original scan date.

## Latest Provider Availability Blocker Surfacing Fix

After the provider-error detail fix, the live DB contained a failed
`polygon_grouped_daily` job for the target date:

```text
2026-05-18
```

Root cause fixed in this slice:

- The provider failure existed in `job_runs`, but `priced-in-preflight` and the
  TUI Run page still made the operator infer the real blocker from logs.
- The dashboard could say only "missing bars" even though the current actionable
  reason was:

  ```text
  NOT_AUTHORIZED: Attempted to request today's data before end of day.
  ```

Fix in this slice:

- `priced_in_preflight_payload()` now checks recent failed market-data provider
  jobs for the target as-of date.
- The payload exposes:

  ```text
  provider_blocker
  ```

- The market-bars evidence row now includes the latest provider failure when it
  matches the target date.
- `priced-in-preflight` CLI prints a `provider_blocker` line.
- The TUI Run page now surfaces the provider-date blocker as the first evidence
  action:

  ```text
  Wait until the provider releases the target daily bars, use the DB-backed
  manual bar template/import path, or intentionally upgrade the provider plan
  before rerunning.
  ```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_preflight_recommends_manual_bar_template_for_missing_bars -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\cli.py tests\integration\test_dashboard_data.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-preflight
.\.venv\Scripts\python.exe -m catalyst_radar.cli dashboard-tui --once --page run
```

Observed:

- Focused pytest passed.
- Ruff passed.
- `git diff --check` passed.
- `priced-in-preflight` made 0 external calls and printed:

  ```text
  provider_blocker provider=polygon target_as_of=2026-05-18 reason=HTTP 403 ...
  detail=NOT_AUTHORIZED: Attempted to request today's data before end of day.
  ```

- TUI one-shot needed a longer timeout than 30 seconds against the live DB but
  completed successfully and showed the provider-date blocker on the Run page.

Current data-state conclusion:

- This is no longer a "wrong ticker list" problem.
- The active universe is already full enough to attempt a full scan:

  ```text
  active=12613
  ```

- The current blocker is that same-day grouped daily bars for `2026-05-18` are
  not available to the current Polygon/Massive plan yet.
- Next useful action is to wait for provider end-of-day availability, import a
  manual full-bar CSV, or upgrade plan; then rerun the default one-call helper
  and inspect coverage.

## Latest Provider Error Detail Fix

Live execution attempt after the minimal-call full-scan fix:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\run-full-market-scan.ps1 -Execute
```

Observed:

- The helper correctly planned only one provider call:

  ```text
  Execute provider calls: ticker_pages=0; grouped_daily=1; total=1; call_plan_max=6
  ```

- Polygon/Massive key validity check succeeded separately against market status:

  ```text
  market_status status=200
  ```

- The grouped daily endpoint failed with HTTP 403. The provider response body
  explained the real blocker:

  ```text
  NOT_AUTHORIZED: Attempted to request today's data before end of day.
  ```

Fix in this slice:

- `JsonHttpClient` now preserves a short redacted provider response detail for
  non-2xx JSON responses.
- Query secrets remain redacted in URLs and response detail.
- This makes future CLI/API failures actionable instead of only showing:

  ```text
  HTTP 403 from <redacted URL>
  ```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\unit\test_http_client.py -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\connectors\http.py tests\unit\test_http_client.py
git diff --check
powershell -ExecutionPolicy Bypass -File scripts\run-full-market-scan.ps1 -Execute
```

Observed:

- HTTP client unit tests passed.
- Ruff passed.
- `git diff --check` passed.
- Live full-scan execution still failed before scan because the provider blocks
  same-day grouped daily data for the current plan/date, but the error now
  includes the provider message and redacts the key.

Current data-state conclusion:

- The full active universe exists: `12,613` active securities.
- The default full-scan helper is now appropriately minimal: one grouped daily
  call, then local scan.
- The current blocker is external availability for `2026-05-18` daily bars:
  wait until the provider releases end-of-day data for the plan, use a manual
  CSV import, or upgrade the Polygon/Massive plan.

## Latest Full-Scan Minimal Provider Call Fix

User clarification:

- Full scan means every active security for the target run date.
- A small visible ticker list is only a review page or enrichment chunk.
- The next useful step is to unblock the `2026-05-18` market bars, not to keep
  reseeding tickers or reviewing stale candidate rows.

Root cause fixed in this slice:

- `scripts\run-full-market-scan.ps1` still planned Polygon ticker reseeding by
  default:

  ```text
  catalyst-radar ingest-polygon tickers --max-pages 13
  catalyst-radar ingest-polygon grouped-daily --date 2026-05-18
  ```

- The active universe is already present:

  ```text
  active=12613
  ```

- That made the default helper look like a 14-call operation even though the
  next blocker only needs the grouped daily bars for the target date.

Fix in this slice:

- `priced_in_preflight_payload()` now includes a structured `scan_scope`:

  ```text
  active_security_count
  requested_securities
  scanned_securities
  universe
  ```

- `priced-in-preflight` CLI prints the scan scope line so the operator can see
  the real universe size.
- `scripts\run-full-market-scan.ps1` now skips Polygon ticker reseeding when
  the active universe is already seeded. It only seeds tickers when:

  ```text
  active_security_count < 500
  ```

  or when the operator explicitly passes:

  ```powershell
  -RefreshTickers
  ```

- The script now prints the execute-time provider-call budget:

  ```text
  Execute provider calls: ticker_pages=0; grouped_daily=1; total=1; call_plan_max=6
  ```

- If `-Execute -RefreshTickers` would exceed the call-plan max, the script
  fails before any provider calls and tells the operator to drop
  `-RefreshTickers` or intentionally raise the guarded cap.

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_preflight_payload_reports_exact_next_steps tests\integration\test_dashboard_data.py::test_priced_in_preflight_recommends_manual_bar_template_for_missing_bars tests\integration\test_local_scripts.py::test_run_full_market_scan_script_is_plan_first_and_execute_gated -q
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py tests\integration\test_local_scripts.py -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\cli.py tests\integration\test_dashboard_data.py tests\integration\test_local_scripts.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-preflight
powershell -ExecutionPolicy Bypass -File scripts\run-full-market-scan.ps1
powershell -ExecutionPolicy Bypass -File scripts\run-full-market-scan.ps1 -RefreshTickers
powershell -ExecutionPolicy Bypass -File scripts\run-full-market-scan.ps1 -RefreshTickers -Execute
```

Observed:

- Focused pytest passed.
- Broader touched-file pytest passed after rerunning with a longer timeout.
- Ruff passed.
- `git diff --check` passed.
- `priced-in-preflight` made 0 external calls and reported:

  ```text
  scan_scope active=12613 requested=0 scanned=0 universe=all
  ```

- Default full-scan plan made 0 external calls and now reports:

  ```text
  Ticker seed: skipped
  Execute provider calls: ticker_pages=0; grouped_daily=1; total=1; call_plan_max=6
  catalyst-radar ingest-polygon grouped-daily --date 2026-05-18
  catalyst-radar run-daily --as-of 2026-05-18 ...
  ```

- `-RefreshTickers` plan remains available for an intentional reseed, but shows:

  ```text
  Execute provider calls: ticker_pages=13; grouped_daily=1; total=14; call_plan_max=6
  ```

- `-RefreshTickers -Execute` failed before provider calls with:

  ```text
  External calls made: 0
  Planned provider calls=14 exceeds call_plan_max=6.
  ```

Next useful product slice:

- Run the default helper with `-Execute` to make one Polygon/Massive grouped
  daily call for `2026-05-18`, then inspect market-bar coverage.
- If coverage remains below full active universe, decide whether the remaining
  symbols are legitimately missing/no-trade instruments or whether the active
  universe needs a targeted cleanup/reseed.

## Latest Full-Scan Target Date Fix

User clarification:

- They asked why the dashboard only shows a few tickers and said they want a
  full scan.
- Product meaning is now explicitly:

  ```text
  Full scan target:
    Every active security in the local database for the current run-as-of date.

  Review rows:
    A paged, human-reviewable result set from the last useful scan. A small
    visible page is not the scan universe.

  Provider/source chunks:
    Rate-safe fetch or enrichment batches. These chunks are execution units,
    not the product scope.
  ```

Root cause fixed in this slice:

- `scripts\run-full-market-scan.ps1` said it would scan all active securities,
  but when `-AsOf` was omitted it defaulted to:

  ```powershell
  $provider.latest_daily_bar_date
  ```

- In the current live database that was `2026-05-15`, while the dashboard
  blocker is the latest run-as-of date, `2026-05-18`, with:

  ```text
  active=12613; with_as_of_bar=0; missing=12613
  ```

- That meant the helper could refresh and run the wrong date without clearing
  the fresh full-scan blocker.

Fix in this slice:

- `priced_in_preflight_payload()` now exposes:

  ```text
  target_as_of
  target_as_of_source
  latest_run_as_of
  ```

- Target selection is deliberately simple:

  ```text
  latest run as_of first, otherwise latest stored daily-bar date.
  ```

- The `priced-in-preflight` CLI line now prints the target date and source.
- `scripts\run-full-market-scan.ps1` now resolves scan date in this order:

  ```text
  explicit -AsOf argument
  preflight target_as_of
  provider latest_daily_bar_date fallback
  ```

- Plan output now shows:

  ```text
  Scan as-of: 2026-05-18; source=run_as_of
  ```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_preflight_payload_reports_exact_next_steps tests\integration\test_dashboard_data.py::test_priced_in_preflight_recommends_manual_bar_template_for_missing_bars tests\integration\test_dashboard_data.py::test_priced_in_preflight_warns_when_latest_run_is_selected_universe tests\integration\test_local_scripts.py::test_run_full_market_scan_script_is_plan_first_and_execute_gated -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\cli.py tests\integration\test_dashboard_data.py tests\integration\test_local_scripts.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-preflight
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-preflight --json
powershell -ExecutionPolicy Bypass -File scripts\run-full-market-scan.ps1
```

Observed:

- Focused pytest passed.
- Ruff passed.
- `git diff --check` passed.
- `priced-in-preflight` made 0 external calls and reported:

  ```text
  target_as_of=2026-05-18 target_source=run_as_of
  latest_bar_date=2026-05-15
  ```

- `scripts\run-full-market-scan.ps1` plan mode made 0 external calls and now
  plans:

  ```text
  catalyst-radar ingest-polygon grouped-daily --date 2026-05-18
  catalyst-radar run-daily --as-of 2026-05-18 ...
  ```

Current answer to "why only these tickers?":

- The visible ticker rows are only the last useful review page.
- The live full-scan universe is 12,613 active securities.
- The current blocker is not the ticker universe; it is missing 2026-05-18
  daily bars for all 12,613 active securities.
- Do not run source-provider chunks or Schwab/LLM work before the run-as-of
  market bars are filled, unless intentionally doing a separate enrichment
  slice.

Next useful product slice:

- Either import a complete `2026-05-18` daily-bar file for all 12,613 active
  securities, or explicitly execute the Polygon/Massive full-market helper after
  reviewing the plan and external-call budget.
- After bars are complete, rerun `priced-in-preflight`; only then run the full
  priced-in scan and review whether market emotion has outrun price reaction.

## Latest Local Restart Python Fix

Post-merge local service restart exposed a launcher issue:

- `scripts\restart-local.ps1` used plain `python`.
- On this workstation that resolved to the Microsoft Store/system Python, not
  the repo venv.
- The API failed to start because that interpreter did not have the TUI
  dependency loaded by the API route imports:

  ```text
  ModuleNotFoundError: No module named 'textual'
  ```

Fix in this slice:

- `scripts\restart-local.ps1` now prefers:

  ```powershell
  .\.venv\Scripts\python.exe
  ```

  and falls back to `python` only if the repo venv executable is missing.
- The script output now includes the Python executable path it used.

Validation to run for this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_local_scripts.py::test_restart_local_script_restarts_only_market_radar_processes -q
.\.venv\Scripts\python.exe -m ruff check scripts tests\integration\test_local_scripts.py
git diff --check
powershell -ExecutionPolicy Bypass -File scripts\restart-local.ps1
powershell -ExecutionPolicy Bypass -File scripts\market-radar-status.ps1
```

Expected:

- Restart should bring both API and Streamlit dashboard back up using the repo
  venv interpreter.
- Status should report the new `Full scan market bars` operator next action and
  make 0 external calls.

## Latest Full-Scan Readiness Priority Fix

User clarification:

- They asked why the dashboard only showed a small ticker set and reiterated
  that they want a full scan.
- The answer is:

  ```text
  Visible dashboard rows:
    A human-review page from the ranked scan. The current live smoke shows
    rows 1-50 from 12,087 previous useful scan rows.

  Fresh full-scan blocker:
    The latest run as-of date still has 0/12,613 active-security daily bars,
    so the fresh full-market scan cannot be trusted yet.

  Provider chunks:
    Small SEC/Schwab/source batches are execution chunks, not the scan universe.
  ```

Root cause fixed in this slice:

- `priced-in-preflight` already pointed at the DB-backed full active-universe
  market-bar template, but the top-level readiness/operator summaries still
  selected downstream `Research loop` and `Decision Cards` blockers first.
- That made the dashboard feel like the current page or source chunk was the
  product's scope, instead of showing the root full-scan blocker.

Fix in this slice:

- `operator_work_queue_payload()` now promotes `stale_daily_bars` and
  `incomplete_daily_bar_coverage` discovery blockers into a first-class
  `Full scan market bars` operator row.
- When that root blocker exists, downstream `Research loop` and
  `Decision Cards` readiness rows are suppressed in the operator queue so the
  first human action is the full-universe market-bar refresh.
- `market_radar_usefulness_payload()` no longer marks the automatic market scan
  as ready just because live providers are configured; it remains blocked until
  full run-as-of market bars are present.
- The overview caption now says `Next data step:` and, when applicable,
  `Fresh full scan blocked by market bars...` with the DB-backed template
  command.
- The TUI hero and navigation lines were shortened so the price answer,
  trade-safety state, keyboard help, and mouse help remain visible in
  Windows-terminal-sized layouts.
- `candidate_delta_payload()` now counts stale candidate context from the
  database when the latest run produced no current-run candidate rows.

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_operator_work_queue_prioritizes_full_scan_market_bar_root_cause tests\integration\test_dashboard_data.py::test_market_radar_usefulness_payload_blocks_live_scan_until_full_bars tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_tui_once_can_show_full_scan_mode -q
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_demo_seed_cli.py::test_modern_dashboard_tui_supports_mouse_navigation tests\integration\test_dashboard_data.py::test_radar_readiness_candidate_delta_keeps_stale_context_without_current_rows -q
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\dashboard\tui.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli dashboard-snapshot --page overview --json
.\.venv\Scripts\python.exe -m catalyst_radar.cli dashboard-tui --once --page overview
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-preflight
```

Observed:

- Focused tests passed.
- The broader dashboard data and demo TUI integration files passed.
- Ruff passed.
- `git diff --check` passed.
- Live dashboard snapshot made 0 external calls and reported:

  ```text
  operator_area=Full scan market bars
  full_scan_rows=12087
  visible_page=50
  external_calls=0
  ```

- Live TUI overview made 0 external calls and now says the current tickers are
  only rows 1-50 from the previous 12,087-row full-market scan, while the next
  data step is the fresh full-scan market-bar template for the 0/12,613
  run-as-of coverage blocker.

Next useful product slice:

- Import complete 2026-05-18 daily bars for all 12,613 active securities, then
  rerun plan-only preflight before any capped provider execution.
- Keep source/broker chunks visibly labeled as chunks, not scan universe.

## Latest Priced-In Preflight Manual-Bar Guidance Fix

Follow-up after the DB-backed manual bar work:

- `priced-in-preflight` still told the operator to run the Polygon scheduled
  scan as the first fix for missing run-as-of market bars.
- That was technically available, but it was the wrong default for the user's
  current "full scan, no surprise provider calls" workflow.

Fix in this slice:

- The market-bar preflight blocker now points first at the zero-call
  DB-backed active-universe template:

  ```powershell
  catalyst-radar market-bars template --expected-as-of <LATEST_TRADING_DATE> --out data\local\manual-bars-<LATEST_TRADING_DATE>.csv
  ```

- The preflight command map now also exposes:

  ```text
  market_bars_import_preview=catalyst-radar market-bars import --daily-bars <fresh-bars.csv> --expected-as-of <LATEST_TRADING_DATE>
  market_bars_import_execute=catalyst-radar market-bars import --daily-bars <fresh-bars.csv> --expected-as-of <LATEST_TRADING_DATE> --execute
  ```

- The Polygon/Massive `run_scan` command remains available in the same command
  map, but the first blocked evidence step says to use the provider run only if
  the call plan and plan limits match intent.
- The TUI Run page now shows the same manual-bar evidence step instead of
  making Polygon look like the only way to unblock a fresh full scan.

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_preflight_payload_reports_exact_next_steps tests\integration\test_dashboard_data.py::test_priced_in_preflight_recommends_manual_bar_template_for_missing_bars tests\integration\test_dashboard_data.py::test_priced_in_preflight_warns_when_latest_run_is_selected_universe tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_preflight_cli_outputs_zero_call_plan -q
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-preflight
.\.venv\Scripts\python.exe -m catalyst_radar.cli dashboard-tui --once --page run
```

Observed:

- Ruff passed.
- Focused pytest passed.
- `git diff --check` passed.
- `priced-in-preflight` made 0 external calls and showed
  `market_bars blocked Run-as-of bar coverage is 0/12613` with
  `catalyst-radar market-bars template ...` as the first command.
- The TUI Run page showed the same first evidence step.

## Latest DB-Backed Manual Market Bar Full-Scan Fix

User clarification:

- They asked again why only a small ticker list was visible and said they want
  a full scan.
- The product now has three deliberately different ticker scopes:

  ```text
  Full scan rows:
    Human-review pages from the ranked scan. Live smoke currently shows 12,087
    previous useful scan rows from 2026-05-15 because the 2026-05-18 run failed
    before producing priced-in rows.

  Active market-bar universe:
    Every active security in the database that needs a fresh daily bar before a
    fresh full-market run can be trusted. Live smoke currently generated a
    12,613-row manual bar template for 2026-05-18.

  Source-fill chunks:
    Small provider-safe batches used to fill SEC/options/text/broker context.
    These chunks are not the scan universe.
  ```

Root cause fixed in this slice:

- The previous manual CSV helper still defaulted to
  `data/sample/securities.csv`. That was too easy to confuse with the actual
  full active database universe.
- Added first-class DB-backed manual market-bar operations:

  ```powershell
  catalyst-radar market-bars template --expected-as-of 2026-05-18 --out data\local\manual-bars-2026-05-18.csv
  catalyst-radar market-bars import --daily-bars <fresh-bars.csv> --expected-as-of 2026-05-18
  catalyst-radar market-bars import --daily-bars <fresh-bars.csv> --expected-as-of 2026-05-18 --execute
  ```

- `market-bars template` reads `MarketRepository.list_active_securities()`,
  writes one row per active DB ticker, and makes 0 external calls.
- `market-bars import` previews first, validates that every active ticker has a
  bar on `--expected-as-of`, refuses incomplete/stale imports, rejects invalid
  numeric OHLCV values, and only writes when `--execute` is present.
- Added API equivalents:

  ```text
  POST /api/radar/market-bars/template
  POST /api/radar/market-bars/import
  ```

- Dashboard/status/readme copy now points to the DB-backed CLI path instead of
  the legacy sample-securities wrapper.

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_provider_ingest_cli.py::test_market_bars_template_uses_database_active_universe tests\integration\test_provider_ingest_cli.py::test_market_bars_import_requires_expected_full_active_coverage tests\integration\test_provider_ingest_cli.py::test_market_bars_import_executes_without_securities_csv tests\integration\test_api_routes.py::test_post_radar_market_bars_template_and_import_use_database_universe -q
.\.venv\Scripts\python.exe -m pytest tests\integration\test_provider_ingest_cli.py::test_market_bars_import_rejects_blank_numeric_fields tests\integration\test_provider_ingest_cli.py::test_market_bars_import_executes_without_securities_csv -q
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_investment_readiness_payload_blocks_fixture_candidates tests\integration\test_dashboard_data.py::test_investment_readiness_payload_blocks_partial_latest_bar_coverage tests\integration\test_dashboard_data.py::test_radar_readiness_payload_summarizes_operator_decision_gate tests\integration\test_dashboard_data.py::test_readiness_checklist_payload_separates_blockers_from_expected_gates tests\integration\test_dashboard_data.py::test_radar_discovery_snapshot_labels_fixture_thin_run tests\integration\test_dashboard_data.py::test_radar_discovery_snapshot_flags_stale_bars_and_empty_packets tests\integration\test_dashboard_data.py::test_radar_discovery_snapshot_flags_incomplete_latest_bar_coverage tests\integration\test_local_scripts.py::test_readme_mentions_restart_script_for_local_dashboard tests\integration\test_local_scripts.py::test_market_radar_status_script_is_zero_external_call_sitrep -q
.\.venv\Scripts\python.exe -m pytest tests\integration\test_provider_ingest_cli.py tests\integration\test_local_scripts.py -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\market\manual_bars.py src\catalyst_radar\cli.py src\catalyst_radar\api\routes\radar.py src\catalyst_radar\dashboard\data.py tests\integration\test_provider_ingest_cli.py tests\integration\test_api_routes.py tests\integration\test_dashboard_data.py tests\integration\test_local_scripts.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-queue --full-scan --limit 3
.\.venv\Scripts\python.exe -m catalyst_radar.cli market-bars template --expected-as-of 2026-05-18 --out $env:TEMP\market-radar-manual-bars-smoke.csv
.\.venv\Scripts\python.exe -m catalyst_radar.cli dashboard-tui --once --page overview
```

Observed:

- Focused tests passed.
- Ruff passed.
- `git diff --check` passed.
- `priced-in-queue --full-scan --limit 3` made 0 external calls and showed
  `status=previous_scan`, `total=12087`, with
  `scan_selection=mode=previous_useful_scan`.
- DB-backed live template smoke made 0 external calls and wrote 12,613 data rows
  plus the CSV header for `expected_as_of=2026-05-18`.
- Dashboard overview made 0 external calls and showed the previous full-market
  priced-in scan page, with clear wording that the visible tickers are a page
  from the full scan, not a watchlist.

Next useful product slice:

- The fresh full-market scan is still blocked until fresh 2026-05-18 bars are
  provided for all 12,613 active DB securities or a live market provider is
  available.
- After importing complete fresh bars, run the plan-only smoke before any capped
  scheduler execution.

## Latest Full-Scan Fallback And Run-Guard Fix

User clarification:

- They asked why only a small ticker list was visible and reiterated that they
  want the full scan.
- There are two different ticker lists in the product:

  ```text
  Full scan rows: the ranked priced-in universe, currently 12,087 rows from the
  last useful scan.

  Source-fill chunk rows: the next provider-safe batch, often 5 tickers because
  SEC/Schwab/source refresh is capped and must be explicitly executed chunk by
  chunk.
  ```

- The source-fill chunk is not the scan universe.

Observed live zero-call state before the fix:

- A newer `2026-05-18` run failed at Polygon grouped daily with HTTP 403.
- Because that failed run had no feature scan rows, `priced-in-queue
  --full-scan` temporarily showed `total=0`, hiding the last useful full scan.
- That made the UI look like MarketRadar only knew about tiny ticker subsets.

Fix in this slice:

- `priced_in_queue_payload()` now falls back to the last coherent populated scan
  date when the latest run produced no priced-in rows because it failed or
  skipped before `feature_scan`.
- The fallback loads one scan date only, not a mixed "latest per ticker" set.
  Live smoke now reports:

  ```text
  priced_in_queue status=previous_scan count=5 total=12087
  scan_selection=mode=previous_useful_scan latest_run_as_of=2026-05-18 selected_as_of=2026-05-15 reason=latest_run_without_priced_in_rows
  headline=Latest run produced no priced-in rows; showing previous full scan, showing 1-5 of 12087 priced-in row(s).
  ```

- CLI output prints `scan_selection` when a previous useful scan is being shown.
  This makes stale/fallback state visible instead of silently pretending it is
  fresh.
- TUI overview copy now labels this as a `Previous full-market priced-in scan`
  and says rows come from the previous scan date, not the latest failed run.
- Source-batch planning now works again from that previous full scan. Live
  zero-call smoke showed:

  ```text
  catalyst_events gap_rows=12080 plannable=10479 total_batches=2096
  batch 1 tickers=AAL,AAMI,AAOI,AAON,AAP
  ```

- Scheduler `_event_ingest()` now skips immediately when `daily_bar_ingest`
  failed in the same run:

  ```text
  event_ingest skipped: blocked_by_failed_dependency:daily_bar_ingest
  ```

  This prevents wasting SEC source calls on a run that cannot produce a useful
  full-market scan.

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_jobs.py::test_daily_run_skips_sec_ingest_when_daily_market_bars_fail tests\integration\test_jobs.py::test_daily_run_polygon_provider_fails_closed_without_api_key tests\integration\test_jobs.py::test_daily_run_sec_event_provider_ingests_capped_submissions_with_guarded_http -q
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_queue_payload_keeps_previous_full_scan_after_failed_latest_run tests\integration\test_dashboard_data.py::test_priced_in_queue_payload_respects_latest_run_universe_scope -q
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_queue_cli_outputs_same_zero_call_signal tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_plans_sec_event_batches tests\integration\test_dashboard_data.py::test_priced_in_all_source_gap_batches_payload_summarizes_next_chunks -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\jobs\tasks.py src\catalyst_radar\cli.py tests\integration\test_dashboard_data.py tests\integration\test_jobs.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-queue --full-scan --limit 5
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-source-batches --source catalyst_events --limit 1
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-source-batches --source all
.\.venv\Scripts\python.exe -m catalyst_radar.cli dashboard-tui --once --page overview
```

Observed: focused pytest passed, ruff passed, `git diff --check` passed, and
all live CLI smokes were zero-call. The full scan is visible again as the last
useful scan, while the latest failed run remains visible as a blocker that must
be fixed before treating output as fresh.

## Latest SEC CIK Metadata Refresh Slice

The next full-scan blocker after clarifying chunk scope is CIK metadata.
`catalyst_events` currently has 12,080 full-scan gaps. Of those, 10,462 are
plannable SEC targets and 1,618 are blocked because the active security row does
not have a CIK.

Fix in this slice:

- Added a guarded SEC company-tickers refresh path:

  ```text
  catalyst-radar ingest-sec company-tickers
  POST /api/radar/sec/company-tickers
  ```

- Live mode uses the existing SEC safety boundary:

  ```text
  CATALYST_SEC_ENABLE_LIVE=1
  CATALYST_SEC_USER_AGENT=<SEC-compliant contact string>
  ```

- Fixture mode is supported for tests and makes zero external calls:

  ```text
  catalyst-radar ingest-sec company-tickers --fixture tests\fixtures\sec\company_tickers.json
  ```

- The refresh only updates active securities that are missing CIK metadata. It
  preserves existing CIKs, matches common share-class ticker separators such as
  `BRK.A` against SEC `BRK-A`, and stores:

  ```text
  cik
  sec_company_name
  cik_source=sec_company_tickers
  cik_updated_at
  ```

- `priced-in-source-batches --source catalyst_events` now points directly at
  the metadata fix when blocked rows exist:

  ```text
  diagnostic_next=Add CIK metadata ... with catalyst-radar ingest-sec company-tickers ...
  diagnostic_command=catalyst-radar ingest-sec company-tickers
  diagnostic_api=POST /api/radar/sec/company-tickers
  ```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_sec_cik_metadata.py tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_exposes_missing_cik_blockers tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_source_batches_cli_prints_blocked_source_samples tests\integration\test_api_routes.py::test_post_radar_sec_company_tickers_refreshes_cik_metadata -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\events\sec_cik.py src\catalyst_radar\cli.py src\catalyst_radar\api\routes\radar.py src\catalyst_radar\dashboard\data.py src\catalyst_radar\dashboard\tui.py tests\integration\test_sec_cik_metadata.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py tests\integration\test_api_routes.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli ingest-sec company-tickers --fixture tests\fixtures\sec\company_tickers.json
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-source-batches --source catalyst_events --limit 1
```

Observed: focused pytest passed, ruff passed after import ordering cleanup,
`git diff --check` passed, fixture CLI smoke made zero external calls, and
the live source-batch plan now exposes both the CIK refresh CLI command and API
endpoint.

## Latest Full-Scan Chunk Clarity Fix

User clarification:

- They asked why only a few tickers were shown and reiterated that they want a
  full scan.
- The live database already has the full priced-in scan:

  ```text
  priced-in-queue --full-scan --limit 10
  total=12087
  ```

- The confusing ticker list was not the scan universe. It was the first
  rate-limited provider chunk for source filling.
- For current SEC catalyst coverage, live zero-call planning shows:

  ```text
  catalyst_events gap_rows=12080 plannable=10462 batch_size=5 total_batches=2093
  blocked=1618 reason=missing_cik
  first_chunk=AAL,AAMI,AAOI,AAON,AAP
  last_chunk=ZVIA,ZYBT
  ```

Fix in this slice:

- Source-batch next action now says explicitly that the full scan is split into
  provider-safe chunks, and that the operator should review the full batch plan
  before running one chunk at a time.
- TUI command parsing now supports:

  ```text
  batch <source> all
  ```

  This summarizes the full chunk plan for a source without provider calls.

- TUI `batch <source>` now says:

  ```text
  Add `all` to summarize every chunk for this source.
  First safe chunk: ...
  ```

  This prevents the first provider chunk from reading like the whole scan.

- `batch <source> all` now reports that the full chunk plan was requested and
  that the TUI is summarizing it instead of printing every ticker.

The prior blocked-source diagnostic work in this branch remains in place:

- CLI source-batch output prints:

  ```text
  blocked_examples=AMDD,BOXX,CAFX,CLIP,IQMM reason=missing_cik
  diagnostic_next=Add CIK metadata for blocked tickers...
  ```

- Data payloads expose `diagnostic.next_action` for `missing_cik` and
  `missing_catalyst_events`.

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_batch_command_opens_full_scan_source_batch_plan tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_can_return_full_scan_plan tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_exposes_missing_cik_blockers tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_source_batches_cli_prints_blocked_source_samples -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\dashboard\tui.py src\catalyst_radar\cli.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-source-batches --source catalyst_events --limit 1
```

Observed: focused pytest passed, ruff passed, `git diff --check` passed, and
the live source-batch smoke made zero provider calls while showing the full
12,080-row catalyst gap, 10,462 eligible rows, 2,093 chunks, and the first
five-ticker chunk as only the next safe provider batch.

## Latest Overview Source-Coverage Hint Fix

After source recommendations were split into broad `coverage_first` and smaller
`decision_shortcut` lanes, the Ops page was clear but the default Insights page
still required the user to know to open Ops for the next data-layer move.

Fix in this slice:

- The overview guide/caption now includes a compact source hint:

  ```text
  Source coverage next: Coverage-first: catalyst_events. Decision shortcut: options (5 decision-ready row(s)).
  ```

- This keeps the first screen focused on the full scan while still exposing the
  smaller decision-ready shortcut.
- The hint is derived from the same `priced_in_source_workflow` payload already
  used by Ops, so it makes zero provider calls.

Live zero-call smoke after the fix:

```text
catalyst-radar dashboard-tui --once --page overview
Page: overview | View: Full scan | Answer: decision ready ready=true | ...
Full-market priced-in queue - showing rows 1-50 of 12087; decision 5 / blocked 7920 / monitor 4162
Source coverage next: Coverage-first: catalyst_events. Decision shortcut: options (5 decision-ready row(s)).
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_tui_once_can_show_full_scan_mode -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\tui.py tests\integration\test_dashboard_demo_seed_cli.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli dashboard-tui --once --page overview
```

Observed: focused pytest passed, ruff passed, `git diff --check` passed, and
the live overview showed the full-scan row count plus compact coverage-first and
decision-shortcut hints with zero provider calls.

## Latest Source Recommendation Split Fix

After the full-scan-primary UX fix, the live evidence map exposed a second
source-priority conflict:

- `priced-in-preflight --json` correctly said broad full-scan evidence should
  start with `catalyst_events`, because catalyst and local-text coverage was
  only `7/12087`.
- `priced-in-source-batches --source all` and the Ops Source Fill Workflow said
  to start with `options`, because options improved the five current
  decision-ready rows.
- Both recommendations were valid, but they answered different operator goals.
  The user's current goal is full-market scan quality first, so broad coverage
  must be the primary recommendation and the decision-ready subset must be a
  clearly labeled shortcut.

Fix in this slice:

- All-source source-batch overview now returns:

  ```text
  coverage_first_recommendation
  decision_shortcut_recommendation
  ```

- `next_action` now follows the broad full-scan coverage recommendation.
- CLI `priced-in-source-batches --source all` now prints both lanes:

  ```text
  next_action=Start full-scan coverage with catalyst_events; it has 12080 remaining gap row(s)...
  coverage_first=source=catalyst_events gaps=12080 calls=5 command=catalyst-radar priced-in-source-batches --source catalyst_events --execute-next
    why=Prioritizes broad evidence coverage across the whole scan.
  decision_shortcut=source=options decision=5 actionable=7 calls=1 command=catalyst-radar priced-in-source-batches --source options --execute-next
    examples=A,MSFT,AAAU,AAPL,AA
  ```

- The TUI Ops workflow now separates:

  ```text
  Coverage-first    : Review the run call plan and refresh event ingestion before trusting emotion.
  Decision shortcut : Start with options; it fills context for 5 decision-ready row(s)...
  ```

- The workflow table order now follows full-scan coverage/preflight order first,
  while still showing useful-row counts beside each source.
- The API all-source endpoint returns the same new fields through:

  ```text
  GET /api/radar/priced-in/source-batches?source=all
  ```

Live zero-call smoke after the fix:

```text
catalyst-radar priced-in-source-batches --source all
priced_in_source_batch_overview status=ready sources=6 ready_sources=3 blocked_sources=1 gap_rows=48329 external_calls=0
coverage_first=source=catalyst_events gaps=12080 calls=5 ...
decision_shortcut=source=options decision=5 actionable=7 calls=1 ...
```

```text
catalyst-radar dashboard-tui --once --page ops
Coverage-first    : Review the run call plan and refresh event ingestion before trusting emotion.
Decision shortcut : Start with options; it fills context for 5 decision-ready row(s)...
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_all_source_gap_batches_payload_summarizes_next_chunks tests\integration\test_dashboard_data.py::test_priced_in_all_source_gap_batches_prioritizes_decision_useful_gaps tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_snapshot_cli_outputs_dashboard_command_center_json tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_snapshot_ops_page_shows_priced_in_source_actions -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\dashboard\tui.py src\catalyst_radar\cli.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-source-batches --source all
.\.venv\Scripts\python.exe -m catalyst_radar.cli dashboard-tui --once --page ops
```

Observed: focused pytest passed, ruff passed after one line-length cleanup,
`git diff --check` passed, and live CLI/TUI smokes made zero provider calls
while separating broad full-scan coverage from the decision-ready shortcut.

## Latest Full-Scan Primary UX Fix

User clarification:

- The product goal is a full-market scan first: MarketRadar should analyze the
  whole active universe and then surface whether any stock's price has not
  matched market expectations.
- The five tickers (`A`, `MSFT`, `AAAU`, `AAPL`, `AA`) are useful as the current
  decision-ready subset, but they are not the scan universe.
- The previous answer path still made that subset feel primary because
  `priced-in-answer` returned:

  ```text
  next_command=catalyst-radar priced-in-queue --decision-ready --limit 50
  ```

Fix in this slice:

- `priced-in-answer` now keeps the decision-ready count in the answer, but its
  next command returns to the full ranked scan:

  ```text
  next_action=Review the full-market scan; decision-ready tickers are a filtered subset, not the scan universe.
  next_command=catalyst-radar priced-in-queue --full-scan --limit 50
  ```

- The TUI/plain dashboard header now separates scan scope from answer status:

  ```text
  View: Full scan | Answer: decision ready ready=true
  ```

  This prevents the header from implying the whole dashboard is narrowed to the
  five decision-ready rows.

- `GET /api/radar/priced-in?decision_ready=true` still exists as an explicit
  shortcut for the small filtered subset, but the default CLI/TUI path remains
  the full scan.

Live zero-call smoke after the fix:

```text
catalyst-radar priced-in-answer
priced_in_answer status=decision_ready ... total=12087 ... external_calls=0
scan_scope=Showing ranked rows 1-5 of 12087; the visible tickers are one page from the full scan, not the scan universe.
next_command=catalyst-radar priced-in-queue --full-scan --limit 50
```

```text
catalyst-radar dashboard-tui --once --page overview
Page: overview | View: Full scan | Answer: decision ready ready=true | ...
Full-market priced-in queue - showing rows 1-50 of 12087; decision 5 / blocked 7920 / monitor 4162
```

```text
catalyst-radar priced-in-queue --full-scan --limit 10
priced_in_queue status=ready count=10 total=12087 offset=0 external_calls=0
headline=Latest full scan ranked 12087 priced-in row(s); showing 1-10 of 12087.
```

```text
catalyst-radar priced-in-queue --decision-ready --limit 10
priced_in_queue status=ready count=5 total=5 offset=0 external_calls=0
```

Operator meaning:

- Full scan means `priced-in-queue --full-scan` or the default Insights view.
- Decision-ready means a useful subset from that full scan.
- Source-fill batch tickers are still provider chunks, not the ticker universe.

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_answer_opens_full_scan_queue_when_decision_ready tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_tui_once_can_show_full_scan_mode tests\integration\test_api_routes.py::test_get_radar_priced_in_queue_returns_cli_ready_rows -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\dashboard\tui.py src\catalyst_radar\api\routes\radar.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py tests\integration\test_api_routes.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-answer
.\.venv\Scripts\python.exe -m catalyst_radar.cli dashboard-tui --once --page overview
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-queue --full-scan --limit 10
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-queue --decision-ready --limit 10
```

Observed: focused pytest passed, ruff passed, `git diff --check` passed, and
the live CLI/TUI smokes made zero provider calls while clearly showing the
12,087-row full scan as the primary view.

## Latest Decision-Ready Shortcut Fix

After the full-scan labeling and source-batch post-check fixes, the next UX
gap was that the dashboard answer said:

```text
5 decision-ready not-priced-in row(s)
```

but the default full-scan table immediately mixed those rows with thousands of
blocked rows. The full scan must remain available, but the operator also needs
one direct way to see the rows that actually answer the priced-in question.

Fix in this slice:

- CLI `priced-in-queue` now supports:

  ```powershell
  catalyst-radar priced-in-queue --decision-ready
  ```

- This is a shortcut for:

  ```powershell
  catalyst-radar priced-in-queue --mismatches --usefulness decision_useful
  ```

- TUI now supports:

  ```text
  ready
  D
  Click SCAN -> Decision-ready
  ```

- The TUI decision-ready filtered view is explicitly labeled:

  ```text
  Decision-ready not-priced-in rows - showing rows 1-5 of 5; scan 12087; decision 5
  ```

- Switching back to `full`, `mismatches`, or `scan all` clears the
  decision-ready usefulness filter so the operator does not stay accidentally
  narrowed.
- `docs/dashboard-feature-inventory.md` now lists the decision-ready CLI/TUI
  entry points and the source-batch post-execution delta.

Live zero-call smoke after the fix:

```text
catalyst-radar priced-in-queue --decision-ready --limit 10
priced_in_queue status=ready count=5 total=5 offset=0 external_calls=0
scan_scope=scanned=12087 requested=n/a filter=actionable ranked_after_filter=5 visible_page=5
usefulness_counts=decision_useful:5
tickers=A,MSFT,AAAU,AAPL,AA
```

```text
catalyst-radar dashboard-tui --once --scan-mode actionable --usefulness decision_useful --page overview
Decision-ready not-priced-in rows - showing rows 1-5 of 5; scan 12087; decision 5
This page shows rows 1-5: 5 decision-ready not-priced-in row(s) from 12087 latest-scan row(s).
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_scan_commands_page_full_scan_rows tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_tui_once_can_show_full_scan_mode tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_queue_cli_outputs_same_zero_call_signal -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\cli.py src\catalyst_radar\dashboard\tui.py tests\integration\test_dashboard_demo_seed_cli.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli dashboard-tui --once --scan-mode actionable --usefulness decision_useful --page overview
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-queue --decision-ready --limit 10
```

Observed: focused pytest passed, ruff passed, `git diff --check` passed, live
CLI and TUI decision-ready views returned the 5 decision-useful rows from the
12,087-row full scan with zero provider calls.

## Latest Source Batch Post-Execution Check Fix

After the full-scan/source-chunk wording fix, the remaining operator gap was:

- `batch <source> execute` ran exactly one guarded source-fill chunk.
- The result ended with:

  ```text
  Refresh to see updated full-scan coverage.
  ```

- That forced the operator to manually refresh/re-plan to learn whether the
  chunk actually improved the full-scan priced-in answer.

Fix in this slice:

- `execute_priced_in_source_batch()` now performs one zero-call post-execution
  re-plan after a successful chunk.
- The execution payload can include:

  ```text
  post_execution.schema_version=priced-in-source-batch-post-execution-v1
  status=complete|improved|unchanged
  before_gap_rows / after_gap_rows / gap_rows_resolved
  before_plannable_rows / after_plannable_rows / plannable_rows_resolved
  before_batch_count / after_batch_count
  review_rows_command / all_batches_command
  next_action
  external_calls_made=0
  ```

- CLI `priced-in-source-batches --source <source> --execute-next` now prints:

  ```text
  post_execution=status=... gap_rows=before->after resolved=...
  post_next=...
  post_plan=...
  ```

- TUI `batch <source> execute` now summarizes the post-check directly instead
  of telling the operator to refresh blindly.
- The API execute-next response carries the same structured `post_execution`
  block.

Important operator meaning:

- Execution still runs at most one guarded chunk.
- The post-check is only a local re-plan and reports `external_calls_made=0`.
- If status is `improved` or `complete`, the next useful action is to review the
  updated source-batch plan before running another chunk.
- If status is `unchanged`, do not keep hammering the provider; inspect the
  updated plan/dashboard first.

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_batch_execute_runs_one_guarded_local_chunk tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_source_batches_execute_next_cli_runs_one_batch tests\integration\test_api_routes.py::test_post_radar_priced_in_source_batch_execute_next_runs_one_chunk -q
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_plans_safe_sync_batches tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_can_return_full_scan_plan tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_prioritizes_decision_useful_rows tests\integration\test_dashboard_data.py::test_priced_in_all_source_gap_batches_payload_summarizes_next_chunks tests\integration\test_api_routes.py::test_get_radar_priced_in_source_batches_returns_zero_call_plan tests\integration\test_api_routes.py::test_get_radar_priced_in_source_batches_can_return_all_source_overview -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\source_batches.py src\catalyst_radar\cli.py tests\integration\test_dashboard_demo_seed_cli.py tests\integration\test_api_routes.py
git diff --check
```

Observed: focused executor/API/TUI tests passed, surrounding source-batch
planning/API tests passed, ruff passed, and `git diff --check` passed. No live
provider execution was run for validation.

## Latest Full-Scan Versus First-Batch Clarity Fix

User confusion:

- The live source-fill recommendation showed only:

  ```text
  A, MSFT, AAAU, AAPL, AA
  ```

- That looked like MarketRadar was scanning only five tickers.
- In reality, the priced-in scan was already full-market:

  ```text
  full_scan active=12613 scanned=12087 ranked=12087
  ```

- The five tickers were only the first rate-limited provider sync chunk for the
  `options` source gap.

Fix in this slice:

- `priced_in_source_gap_batches_payload()` now includes an explicit
  `scan_scope` block:

  ```text
  mode=full_scan
  full_scan_gap_rows=<all matching ranked rows with this source gap>
  plannable_rows=<rows eligible for this source executor>
  planned_batches=<total source-fill chunks>
  returned_batches=<chunks returned by this CLI/API call>
  returned_tickers=<ticker count shown in this page of the batch plan>
  tickers_are_batch_sample=<true when returned tickers are only a chunk>
  ```

- CLI `priced-in-source-batches` now prints this scan-scope line plus a plain
  `scope_note`.
- TUI `batch <source>` now says:

  ```text
  Showing batch X-Y of N (... ticker(s)); these are not the whole ticker list.
  ```

  when the displayed tickers are only a chunk.

- TUI `batch all` and the Ops Source Fill Workflow now explain:

  ```text
  Full scan = the whole ranked universe.
  Source-fill tickers = the next rate-limited provider chunk, not the ticker universe.
  ```

Live zero-call smoke after the fix:

```text
priced_in_source_batches source=options status=ready gap_rows=12087 plannable=12087 ... batch_size=5 batches=1 total_batches=2418 ... external_calls=0
scan_scope=mode=full_scan gap_rows=12087 plannable=12087 returned_batches=1 planned_batches=2418 returned_tickers=5 batch_sample=true
scope_note=The full scan covers every matching ranked row. The tickers shown here are only the returned rate-limited source-fill batch(es); use all_batches_command to list the complete full-scan batch plan.
batch ... tickers=A,MSFT,AAAU,AAPL,AA ...
```

Important operator meaning:

- `priced-in-queue --full-scan --all --json` exports every ranked full-scan row.
- `priced-in-source-batches --source options --all --json` lists every
  rate-limited `options` fill chunk for the full scan.
- `priced-in-source-batches --source options --execute-next` executes only one
  explicit provider chunk. There is intentionally no accidental “call Schwab
  for all 12k rows now” button.

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_plans_safe_sync_batches tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_can_return_full_scan_plan tests\integration\test_dashboard_data.py::test_priced_in_all_source_gap_batches_payload_summarizes_next_chunks tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_batch_command_opens_full_scan_source_batch_plan tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_queue_cli_outputs_same_zero_call_signal -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\dashboard\tui.py src\catalyst_radar\cli.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-source-batches --source options --limit 1
.\.venv\Scripts\python.exe -m catalyst_radar.cli dashboard-tui --once --page ops
```

Observed: focused pytest passed, ruff passed, `git diff --check` passed, the
live CLI source-batch plan made zero provider calls and reported the full-scan
scope, and the live Ops dashboard now includes the full-scan/source-chunk
legend.

## Latest Useful-First Source Batch Ordering Fix

After the dashboard and all-source overview started recommending `options`
first, the executable `options` batch still used the original ranked queue
order. In the live scan, that first batch was:

```text
A, MSFT, AAA, AAAU, AAPL
```

That included blocked ticker `AAA` and omitted decision-ready ticker `AA`, even
though the all-source priority message correctly said the five decision-ready
examples were:

```text
A, MSFT, AAAU, AAPL, AA
```

Root cause:

- `priced_in_source_gap_batches_payload()` planned source batches in queue rank
  order after filtering to the source gap.
- Queue rank is useful for browsing, but source execution should fill the rows
  that most improve the current priced-in answer first.

Fix in this slice:

- Source batch planning now sorts plannable rows by:

  1. `decision_useful`
  2. `research_useful`
  3. actionable mismatch
  4. monitor-only
  5. blocked / other

- Within each usefulness tier, rows remain ordered by absolute
  emotion-reaction gap, then ticker.
- The behavior applies to read-only Schwab batches, SEC catalyst-event batches,
  and local text batches after source-specific eligibility filtering.

Live options batch smoke after the fix:

```text
priced_in_source_batches source=options status=ready gap_rows=12087 plannable=12087 external_calls=0
batch calls row_start row_end tickers command
1 1 1 5 A,MSFT,AAAU,AAPL,AA catalyst-radar schwab-market-sync --ticker A --ticker MSFT --ticker AAAU --ticker AAPL --ticker AA
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_prioritizes_decision_useful_rows tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_plans_safe_sync_batches tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_can_return_full_scan_plan tests\integration\test_dashboard_data.py::test_priced_in_all_source_gap_batches_payload_summarizes_next_chunks -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py tests\integration\test_dashboard_data.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-source-batches --source options --limit 1
```

Observed: focused pytest passed, ruff passed, `git diff --check` passed, and
the live first options batch now contains the five decision-ready tickers.

## Latest Dashboard Snapshot Performance Fix

After the dashboard source workflow was corrected, live Ops dashboard snapshots
still took roughly 96 seconds before the performance work and about 82 seconds
after the duplicate workflow-preflight cleanup. A timing pass showed the main
remaining duplicate work was readiness:

- `priced_in_queue_payload()` already builds the full-scan priced-in queue and
  embeds a `priced_in_preflight` payload.
- `dashboard_snapshot_payload()` separately rebuilt preflight before this
  slice.
- `radar_readiness_payload()` also reloaded current candidate rows, rebuilt
  discovery, and called `candidate_delta_payload()` without passing the already
  loaded candidate rows.

Fix in this slice:

- `dashboard_snapshot_payload()` now reuses `priced_in_queue["preflight"]`
  instead of calling `priced_in_preflight_payload()` a second time.
- `radar_readiness_payload()` accepts optional already-loaded
  `radar_run_summary`, `candidate_rows`, `broker_summary`, `ops_health`, and
  `discovery_snapshot`.
- `radar_readiness_payload()` now passes the resolved candidate rows into
  `candidate_delta_payload()`, avoiding another current-row load.
- `dashboard_snapshot_payload()` passes the already-loaded dashboard context
  into `radar_readiness_payload()`.

Live timing:

```text
before this performance cleanup: dashboard-snapshot --page ops --json ~= 95.98s
after preflight reuse only:       dashboard-snapshot --page ops --json ~= 82.45s
after readiness reuse too:        dashboard-snapshot --page ops --json ~= 66.35s
```

Live smoke after the fix:

```text
Start with options; it fills context for 5 decision-ready row(s) in the visible ranked page. Type batch options to inspect the full-scan plan. Example: A, MSFT, AAAU, AAPL, AA.
radar-readiness-v1
priced-in-preflight-v1
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_snapshot_reuses_priced_in_queue_preflight tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_snapshot_cli_outputs_dashboard_command_center_json tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_snapshot_ops_page_shows_priced_in_source_actions tests\integration\test_dashboard_data.py::test_radar_readiness_candidate_delta_treats_candidates_without_run_as_context -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\dashboard\tui.py tests\integration\test_dashboard_demo_seed_cli.py tests\integration\test_dashboard_data.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli dashboard-snapshot --page ops --json
```

Observed: focused pytest passed, ruff passed, `git diff --check` passed, live
Ops snapshot still returns `radar-readiness-v1` and `priced-in-preflight-v1`,
and the dashboard continues to recommend `options` first.

## Latest Dashboard Source Workflow Priority Fix

After the useful source-gap priority fix, the CLI `priced-in-source-batches
--source all` recommended `options` first, but the dashboard Ops page still
used the older preflight evidence order and said to refresh catalyst events
first.

Root cause:

- The TUI `priced_in_source_workflow` payload was derived only from
  `priced_in_preflight.evidence_plan`.
- That plan is useful for broad prerequisite coverage, but it does not know
  which source gap helps the currently visible ranked priced-in rows first.
- Running the expensive all-source source-batch planner during every dashboard
  render would make the dashboard slower, so the fix could not simply call the
  all-source planner from `dashboard_snapshot_payload()`.

Fix in this slice:

- `_priced_in_source_workflow_payload()` now accepts the already-loaded
  `priced_in_queue`.
- It computes lightweight priority counts over the visible ranked page:

  ```text
  decision_useful_gap_rows
  research_useful_gap_rows
  actionable_gap_rows
  priority_sample_tickers
  ```

- The source workflow now sorts steps by useful priority before falling back to
  the preflight/source order.
- The workflow keeps `priority_scope=visible_priced_in_rows` so the operator
  knows this is a dashboard guidance shortcut, while `batch all` remains the
  full-scan source-batch plan.
- The Ops rendered workflow table now includes a compact "Useful rows" column.

Live dashboard smoke after the fix:

```text
Start with options; it fills context for 5 decision-ready row(s) in the visible ranked page. Type batch options to inspect the full-scan plan. Example: A, MSFT, AAAU, AAPL, AA.
catalyst-radar priced-in-source-batches --source options --all --json
[(1, 'options', 5, 0, 7, ['A', 'MSFT', 'AAAU', 'AAPL', 'AA']), (2, 'broker_context', 0, 0, 2, [])]
```

The live TUI Ops smoke showed:

```text
Source Fill Workflow
Next action: Start with options; it fills context for 5 decision-ready row(s) in the visible ranked page.
1 | options | ... | decision ... | ...
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_snapshot_cli_outputs_dashboard_command_center_json tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_snapshot_ops_page_shows_priced_in_source_actions tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_batch_command_opens_full_scan_source_batch_plan -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\tui.py tests\integration\test_dashboard_demo_seed_cli.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli dashboard-snapshot --page ops --json
.\.venv\Scripts\python.exe -m catalyst_radar.cli dashboard-tui --once --page ops
```

Observed: focused pytest passed, ruff passed, `git diff --check` passed, and
the live dashboard snapshot/TUI now recommend `options` first.

## Latest Useful Source-Gap Priority Fix

After full-scan scope was clear, the next usability problem was source-fill
priority:

- The full-scan source overview had the right raw counts, but its first
  suggested runnable source came from source order.
- In the live 12k-row scan, that meant `catalyst_events` was suggested first
  even though the current decision-ready rows already had catalyst/local text
  and mostly needed optional `options` context.
- This was technically broad-market, but not human-useful enough for the
  question "which missing evidence helps the current priced-in answer first?"

Fix in this slice:

- `priced_in_all_source_gap_batches_payload()` now computes priority counts
  from the full ranked queue:

  ```text
  decision_useful_gap_rows
  research_useful_gap_rows
  actionable_gap_rows
  priority_sample_tickers
  ```

- All-source `next_action` now chooses ready sources by:

  1. Decision-useful gaps.
  2. Research-useful gaps.
  3. General actionable mismatch gaps.
  4. Original source order only when no useful/actionable priority exists.

- CLI all-source output now includes the priority counts and priority example
  tickers.
- The TUI `batch all` message now includes the same suggested-first wording and
  uses the priority fields when choosing `First executable`.

Live all-active smoke after the fix:

```text
next_action=Start with options; it fills context for 5 decision-ready row(s). Inspect first_batch, then run execute_next_command only if the provider budget is intentional. Example: A, MSFT, AAAU, AAPL, AA.
source status gap_rows decision research actionable plannable batches first_calls next_command
catalyst_events ready 12080 0 0 0 10462 2093 5 catalyst-radar priced-in-source-batches --source catalyst_events --execute-next
options ready 12087 5 0 7 12087 2418 1 catalyst-radar priced-in-source-batches --source options --execute-next
priority_examples=A,MSFT,AAAU,AAPL,AA
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_batch_command_opens_full_scan_source_batch_plan tests\integration\test_dashboard_data.py::test_priced_in_all_source_gap_batches_payload_summarizes_next_chunks tests\integration\test_dashboard_data.py::test_priced_in_all_source_gap_batches_prioritizes_decision_useful_gaps tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_plans_safe_sync_batches tests\integration\test_api_routes.py::test_get_radar_priced_in_source_batches_can_return_all_source_overview -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\cli.py src\catalyst_radar\dashboard\tui.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py tests\integration\test_api_routes.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-source-batches --source all --limit 1
```

Observed: focused pytest passed, ruff passed, `git diff --check` passed, and
the live all-source CLI smoke now recommends `options` first for the five
decision-ready rows instead of defaulting to broad `catalyst_events`.

## Latest Full-Scan Scope Clarity Fix

The user asked again: "Why only these tickers? I want full scan."

Current live evidence:

- The latest priced-in scan is all-active/full-scan scoped, not a selected
  watchlist.
- `priced-in-answer` reports `total=12087`, `mismatches=7`,
  `decision_ready_rows=5`, and `blocked=7920`.
- The top tickers shown by `priced-in-answer` are only rows `1-5` from the
  ranked page, not the scan universe.
- The TUI overview shows rows `1-50` from `12087` latest-scan rows and can page
  deeper with `next`, `prev`, `offset <row>`, or `limit <rows>`.

Root cause:

- The data layer already had a full-scan queue, but the answer surface centered
  the small visible sample.
- The CLI printed `scan_scope`, but it did not have a direct full-scan summary
  line that a human could read quickly.
- The TUI compact caption said the table was paged, but did not plainly say
  "these tickers are only the current page."

Fix in this slice:

- `priced_in_answer_payload()` now includes:

  ```text
  full_scan.schema_version=priced-in-full-scan-summary-v1
  full_scan.mode=full_scan
  full_scan.active_securities=<active security count>
  full_scan.scanned_rows=<current scan rows>
  full_scan.ranked_rows=<ranked result rows>
  full_scan.visible_tickers_are_sample=<true when page is smaller than result set>
  full_scan.review_command=<current page command>
  full_scan.full_export_command=catalyst-radar priced-in-queue --full-scan --all --json
  ```

- `priced-in-answer` CLI now prints a one-line full-scan summary plus a sample
  explanation, review command, and export command.
- The TUI overview guide now says the visible tickers are only the current page.
- The TUI compact overview caption now says:

  ```text
  These tickers are only the current page; the table is paged for human review, not reduced to a watchlist.
  ```

Live smoke after the fix:

```text
priced_in_answer status=decision_ready decision_ready=true investment_decision_ready=false total=12087 mismatches=7 research=0 blocked=7920 external_calls=0
scan_scope=Showing ranked rows 1-5 of 12087; the visible tickers are one page from the full scan, not the scan universe.
full_scan=mode=full_scan active=12613 scanned=12087 ranked=12087 visible=1-5 sample=true
sample_explanation=The tickers below are rows 1-5 from the current ranked page, not the full scan universe of 12087 row(s).
review_full_scan=catalyst-radar priced-in-queue --full-scan --limit 5 --offset 0
export_full_scan=catalyst-radar priced-in-queue --full-scan --all --json
```

TUI smoke showed:

```text
Full-market priced-in queue - showing rows 1-50 of 12087; decision 5 / blocked 7920 / monitor 4162
This page shows rows 1-50: 50 visible rows from 12087 latest-scan rows. These tickers are only the current page; the table is paged for human review, not reduced to a watchlist.
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_answer_payload_summarizes_current_scan tests\integration\test_dashboard_data.py::test_priced_in_answer_opens_full_scan_queue_when_decision_ready tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_answer_cli_outputs_current_scan_answer tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_tui_once_can_show_full_scan_mode -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\cli.py src\catalyst_radar\dashboard\tui.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-answer
.\.venv\Scripts\python.exe -m catalyst_radar.cli dashboard-tui --once --scan-mode all --page overview
```

Observed: focused pytest passed, ruff passed, `git diff --check` passed, the
live `priced-in-answer` smoke reported `full_scan=mode=full_scan active=12613
scanned=12087 ranked=12087 visible=1-5 sample=true`, and the live TUI smoke
labeled the visible tickers as only the current page.

## Latest All-Source Batch Performance Fix

After the full-active 12k-row scan, the all-source source-fill overview exposed
a performance bug:

- `priced-in-source-batches --source all --limit 1 --json` timed out in an
  earlier smoke and left a background reader process alive.
- Even one source, `priced-in-source-batches --source catalyst_events --limit 1
  --json`, took about 43 seconds.
- Root cause: `priced_in_all_source_gap_batches_payload()` called
  `priced_in_source_gap_batches_payload()` once per source, and each source
  rebuilt the full priced-in queue from the DB.

Fix in this slice:

- `priced_in_all_source_gap_batches_payload()` now builds the full priced-in
  queue once.
- It passes that resolved queue to each per-source planner.
- `priced_in_source_gap_batches_payload()` can now accept a precomputed queue
  and filter rows in memory for the requested source.
- Regression test asserts the all-source overview calls `priced_in_queue_payload`
  exactly once.

Live all-active smoke after the fix:

```text
elapsed=48.11 status=ready gap_rows=48329 ready=3 blocked=1 sources=6
market_bars       no_gaps      0
catalyst_events   ready    12080  plannable=10462 batches=2093
local_text        blocked  12080  plannable=0
options           ready    12087  plannable=12087 batches=2418
theme_peer_sector no_gaps      0
broker_context    ready    12082  plannable=12082 batches=2417
```

This is still bounded by one full 12k-row queue build, but it no longer repeats
that queue build six times or leaves a timed-out helper process under the
normal timeout used here.

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_all_source_gap_batches_payload_summarizes_next_chunks tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_plans_safe_sync_batches tests\integration\test_api_routes.py::test_get_radar_priced_in_source_batches_can_return_all_source_overview -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py tests\integration\test_dashboard_data.py tests\integration\test_api_routes.py
git diff --check
```

Observed: focused pytest passed, ruff passed, `git diff --check` passed, and
the live all-source CLI smoke completed in about 48 seconds.

## Latest Narrow Artifact Command Fix

After the all-active scan, `priced-in-answer` reported:

```text
status=research_only
total=12087
mismatches=7
research=5
decision=0
next=catalyst-radar build-packets --as-of 2026-05-15 --min-state ResearchOnly
```

The research-useful mismatch queue was only five rows:

```text
A, MSFT, AAAU, AAPL, AA
missing_for_decision=['candidate_packet', 'decision_card']
```

Root cause:

- `_priced_in_local_artifact_command()` explicitly discarded the sample tickers
  from `decision_gap_counts`.
- The generated command built all `ResearchOnly` packets for the scan date
  instead of the relevant priced-in mismatch rows.
- Running that broad command against the all-active local DB took too long and
  held the SQLite DB lock. A stale background `priced-in-source-batches` helper
  from a timed-out smoke was also found and stopped before retrying.

Fix in this slice:

- The local artifact command now includes de-duplicated ticker args from the
  decision-gap sample:

  ```powershell
  catalyst-radar build-packets --as-of 2026-05-15 --ticker MSFT --min-state ResearchOnly
  catalyst-radar build-decision-cards --as-of 2026-05-15 --ticker MSFT --ticker AAPL --ticker AA --ticker A --ticker AAA --ticker AAAU --min-state ResearchOnly
  ```

- The command remains bounded to the first local artifact batch exposed in the
  answer payload, avoiding accidental broad state-slice builds from the
  dashboard/API/CLI recommendation.

Live state after the partial packet build completed enough local artifacts:

```text
priced_in_answer status=decision_ready decision_ready=true investment_decision_ready=false total=12087 mismatches=7 research=0 blocked=7920 external_calls=0
decision_readiness=status=ready actionable=7 decision_ready=5 summary=5 not-priced-in row(s) are decision-ready.
next_command=catalyst-radar priced-in-queue --mismatches --usefulness decision_useful --limit 50
decision-useful rows: A, MSFT, AAAU, AAPL, AA
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_answer_prefers_local_artifact_gap_before_options tests\integration\test_dashboard_data.py::test_priced_in_answer_opens_full_scan_queue_when_decision_ready tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_answer_cli_outputs_current_scan_answer -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py tests\integration\test_dashboard_data.py
git diff --check
```

Observed: focused pytest passed, ruff passed, and `git diff --check` passed.

## Latest Full-Active Scan Scope Correction

The user pushed back again: "Why only these tickers? I want full scan."

Root cause:

- The latest priced-in answer was based on the most recent radar run.
- That run used `--universe liquid-us`, which scanned 2,429 liquidity-filtered
  securities.
- The local database actually has 12,613 active securities, with 12,087
  successfully scanned from stored Polygon bars after a no-provider-call
  all-active scan.
- Calling a selected-universe run "full market" was misleading.

Product behavior changed in this slice:

- A named-universe run that covers materially less than the active local market
  now reports `scan_status=selected_universe`.
- `priced-in-answer` is blocked when the latest scan is selected-universe
  scoped, even if that smaller run has decision-useful rows. The row count still
  remains visible under `counts.decision_ready_rows`.
- The answer scan scope now says exactly what happened, for example:

  ```text
  Showing rows 1-5 of 2429 from universe=liquid-us; the latest run did not scan all 12613 active securities.
  ```

- The top-level next action/command now points at the all-active run:

  ```powershell
  catalyst-radar run-daily --as-of <LATEST_TRADING_DATE> --available-at <UTC-now> --provider polygon --json
  ```

- `priced-in-preflight` now includes a `scan_scope` row when the latest run is
  selected-universe scoped.
- `scripts/run-full-market-scan.ps1` now defaults to all-active scanning. It
  keeps selected-universe scans behind explicit `-UseUniverse`.
- The script also tolerates `run-daily` returning `partial_success` after
  `feature_scan` succeeds, so a useful all-active scan is not discarded just
  because optional provider/downstream layers stayed gated.
- The TUI overview title/caption now labels selected-universe queues as
  selected-universe output, not full-market output.

Live local evidence after running a stored-data all-active scan with scheduled
provider ingest disabled for that invocation:

```text
feature_scan status=success requested_count=12613 normalized_count=12087 scan_scope=active_securities
priced_in_answer status=research_only decision_ready=false total=12087 mismatches=7 research=5 blocked=7920 external_calls=0
scan_scope=Showing ranked rows 1-5 of 12087; the visible tickers are one page from the full scan, not the scan universe.
next_command=catalyst-radar build-packets --as-of 2026-05-15 --min-state ResearchOnly
```

Dashboard smoke:

```text
Full-market priced-in queue - showing rows 1-50 of 12087; research 5 / blocked 7920 / monitor 4162
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_scan_status_marks_small_named_universe_as_selected tests\integration\test_dashboard_data.py::test_priced_in_scan_status_accepts_named_universe_when_it_covers_active_scope tests\integration\test_dashboard_data.py::test_priced_in_preflight_payload_reports_exact_next_steps tests\integration\test_dashboard_data.py::test_priced_in_preflight_warns_when_latest_run_is_selected_universe tests\integration\test_dashboard_data.py::test_priced_in_answer_blocks_selected_universe_even_with_ready_rows tests\integration\test_dashboard_data.py::test_priced_in_answer_opens_full_scan_queue_when_decision_ready tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_answer_cli_outputs_current_scan_answer tests\integration\test_local_scripts.py::test_run_full_market_scan_script_is_plan_first_and_execute_gated -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\dashboard\tui.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py tests\integration\test_local_scripts.py
git diff --check
```

Observed: focused pytest passed, ruff passed, and `git diff --check` passed.

## Latest Dashboard Source-Fill Workflow

The all-source batch overview made the workflow scriptable, but the TUI Ops
page still required the operator to know `batch all`. This slice surfaces the
same source-fill priority directly in the dashboard payload and Ops page without
adding another expensive all-source batch scan to every render.

Changes in this slice:

- `dashboard_snapshot_payload()` now includes:

  ```text
  priced_in_source_workflow.schema_version=priced-in-source-workflow-v1
  priced_in_source_workflow.overview_command=catalyst-radar priced-in-source-batches --source all
  priced_in_source_workflow.external_calls_made=0
  ```

- The workflow is derived from the existing zero-call `priced_in_preflight`
  evidence plan, so it reuses already-computed source dependency order.
- The Ops page now renders a `Source Fill Workflow` section with:
  - status;
  - next action;
  - all-source plan command;
  - ordered source steps with dependencies and plan commands.
- The Ops page explicitly tells the operator that `batch all` is plan-only and
  `batch <source> execute` runs exactly one guarded chunk.

Live zero-provider-call smoke:

```text
Source Fill Workflow
All-source plan          : catalyst-radar priced-in-source-batches --source all
batch all shows this source map without provider calls; batch <source> execute runs exactly one guarded chunk.
```

JSON smoke:

```text
priced-in-source-workflow-v1 attention 5 catalyst-radar priced-in-source-batches --source all 0
[(1, 'catalyst_events', 'attention'), (2, 'local_text', 'attention'), (3, 'options', 'attention'), (4, 'broker_context', 'attention')]
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_snapshot_cli_outputs_dashboard_command_center_json tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_snapshot_ops_page_shows_priced_in_source_actions tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_batch_command_opens_full_scan_source_batch_plan -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\tui.py tests\integration\test_dashboard_demo_seed_cli.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli dashboard-snapshot --page ops | Select-String -Pattern "Source Fill Workflow|All-source plan|priced-in-source-batches --source all|batch all|catalyst_events|local_text|options|broker_context"
.\.venv\Scripts\python.exe -m catalyst_radar.cli dashboard-snapshot --page ops --json | .\.venv\Scripts\python.exe -c "import json,sys; p=json.load(sys.stdin); w=p['priced_in_source_workflow']; print(w['schema_version'], w['status'], w['step_count'], w['overview_command'], w['external_calls_made']); print([(s['priority'], s['source'], s['status']) for s in w['steps'][:4]])"
```

Observed: focused pytest passed, ruff passed, `git diff --check` passed, and
live Ops-page smoke shows the source-fill workflow without provider calls.

## Latest All-Source Batch Overview

The full-scan queue is now broad enough to answer the current priced-in question,
but source coverage still shows large gaps:

- `catalyst_events`: 2,425 / 2,429 missing.
- `local_text`: 2,425 / 2,429 missing, blocked until event text exists.
- `options`: 2,429 / 2,429 missing.
- `broker_context`: 2,425 / 2,429 missing.

The operator previously had to inspect one source at a time. This slice adds a
plan-only overview across all priced-in source classes:

```powershell
catalyst-radar priced-in-source-batches --source all
catalyst-radar priced-in-source-batches --source all --json
```

API and TUI parity:

```text
GET /api/radar/priced-in/source-batches?source=all
batch all
```

Safety boundary:

- `--source all` makes 0 provider calls.
- `batch all` makes 0 provider calls.
- `--source all --execute-next` is rejected; the operator must choose exactly
  one source before execution.
- Bulk all-source execution was intentionally not added.

Live zero-provider-call smoke after this slice:

```text
priced_in_source_batch_overview status=ready sources=6 ready_sources=3 blocked_sources=1 gap_rows=9704 external_calls=0
headline=9704 source gap row(s) remain across 6 source class(es); 3 source(s) have a runnable next chunk and 1 source(s) are blocked.
next_action=Start with catalyst_events; inspect all_batches_command, then run execute_next_command only if the provider budget is intentional.
boundary=Plan only. This overview makes no provider calls and never executes every source. Pick one source and run its execute_next_command when the call budget matches your intent.
catalyst_events ready 2425 2425 485 5 catalyst-radar priced-in-source-batches --source catalyst_events --execute-next
local_text blocked 2425 0 0 0 n/a
options ready 2429 2429 486 1 catalyst-radar priced-in-source-batches --source options --execute-next
broker_context ready 2425 2425 485 1 catalyst-radar priced-in-source-batches --source broker_context --execute-next
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_all_source_gap_batches_payload_summarizes_next_chunks tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_batch_command_opens_full_scan_source_batch_plan tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_queue_cli_outputs_same_zero_call_signal tests\integration\test_api_routes.py::test_get_radar_priced_in_source_batches_can_return_all_source_overview -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\cli.py src\catalyst_radar\api\routes\radar.py src\catalyst_radar\dashboard\tui.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py tests\integration\test_api_routes.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-source-batches --source all --limit 1
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-source-batches --source all --limit 1 --json
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-source-batches --source all --execute-next
```

Observed: focused pytest passed, ruff passed, `git diff --check` passed, live
CLI overview reported 6 source classes with 3 runnable next chunks, and the
bulk execute attempt was rejected as plan-only.

## Latest Named-Universe Full-Scan Answer Correction

The user pushed back again: "Why only these tickers? I want full scan." The
right product answer is:

- MarketRadar should scan the full selected universe, currently `liquid-us` in
  the live local DB.
- The dashboard/CLI can only show a page or a next safe batch at a time.
- Those visible tickers are not the scan scope. They are a review window or a
  rate-limited executor chunk.

Root cause found in this slice:

- The latest local run was a named-universe scan over 2,429 rows.
- `_priced_in_scan_status()` still compared `scanned_securities` against raw
  `active_security_count` from the securities table.
- Raw active securities included many instruments outside the selected
  `liquid-us` universe, so a completed universe scan was mislabeled
  `partial_scan`.
- Because `partial_scan` maps to answer `blocked`, the top-level answer looked
  less ready than the data actually was.

Changes in this slice:

- `_priced_in_scan_status()` now detects `discovery.run.universe`.
- For named-universe scans, the denominator is `requested_securities` or
  `scanned_securities`, not raw active securities.
- Raw active securities remain the fallback denominator only when no named
  universe was requested.
- When the priced-in answer is `decision_ready`, top-level `next_command` now
  opens the full actionable mismatch queue:

  ```powershell
  catalyst-radar priced-in-queue --mismatches --usefulness decision_useful --limit 50
  ```

  It no longer jumps directly to a single ticker's decision card as the primary
  next step. Individual cards remain available from the queue rows.

Live zero-provider-call smoke after this correction:

```text
priced_in_answer status=decision_ready decision_ready=true investment_decision_ready=false total=2429 mismatches=4 research=0 blocked=58 external_calls=0
scan_scope=Showing ranked rows 1-5 of 2429; the visible tickers are one page from the full scan, not the scan universe.
full_scan_export=catalyst-radar priced-in-queue --full-scan --all --json
decision_readiness=status=ready actionable=4 decision_ready=4 summary=4 not-priced-in row(s) are decision-ready.
next_action=Review all decision-ready mismatch rows from the full scan.
next_command=catalyst-radar priced-in-queue --mismatches --usefulness decision_useful --limit 50
```

Follow-up queue smoke:

```text
priced_in_queue status=ready count=4 total=4 offset=0 external_calls=0
scan_scope=scanned=2429 requested=n/a filter=actionable ranked_after_filter=4 visible_page=4
headline=Latest full scan found 4 actionable mismatch row(s); showing 1-4 of 4.
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_scan_status_uses_named_universe_denominator tests\integration\test_dashboard_data.py::test_priced_in_answer_payload_summarizes_current_scan tests\integration\test_dashboard_data.py::test_priced_in_answer_opens_full_scan_queue_when_decision_ready tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_answer_cli_outputs_current_scan_answer -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py
git diff --check
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-answer | Select-String -Pattern "priced_in_answer|scan_scope|full_scan_export|decision_readiness|next_action|next_command"
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-queue --mismatches --usefulness decision_useful --limit 50 | Select-String -Pattern "priced_in_queue|headline|count=|ticker status|external_calls|scan_scope"
```

Observed: focused pytest passed, ruff passed, `git diff --check` passed, and
live local DB smoke now reports the scan as `ready` / `decision_ready` over
2,429 scanned universe rows.

## Latest Priced-In Answer Next-Command Alignment

After the source-batch CLI/API parity work, the live `priced-in-answer` output
still had a confusing contradiction:

- `decision_readiness.recommended_gap` correctly said `candidate_packet`.
- Top-level `next_action` / `next_command` still came from the broad preflight
  source-coverage plan and pointed to catalyst-event filling.

That made the answer tell the operator to start with Candidate Packets while
also printing a catalyst-event source-batch command as the primary next command.

Changes in this slice:

- `priced_in_answer_payload()` now computes `decision_readiness` before choosing
  the answer-level next step.
- `_priced_in_answer_next_step()` now prefers
  `decision_readiness.recommended_gap.next_action` and `.command` for
  blocked/research-only priced-in answers.
- Broad source coverage remains visible under `source_coverage` and
  `trust_blockers`, but it no longer overrides the local decision artifact step
  when the current actionable mismatch rows need Candidate Packets or Decision
  Cards first.

Live smoke after this correction:

```text
recommended_gap=candidate_packet count=4 command=catalyst-radar build-packets --as-of 2026-05-15 --min-state ResearchOnly
next_action=Build Candidate Packets for research-useful mismatch rows.
next_command=catalyst-radar build-packets --as-of 2026-05-15 --min-state ResearchOnly
```

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_answer_prefers_local_artifact_gap_before_options tests\integration\test_dashboard_data.py::test_priced_in_answer_payload_summarizes_current_scan tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_answer_cli_outputs_current_scan_answer -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py tests\integration\test_dashboard_data.py
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-answer | Select-String -Pattern "recommended_gap|next_command|next_action"
```

Observed: focused tests passed, ruff passed, and the live smoke now shows the
same Candidate Packet command for both `recommended_gap` and `next_command`.

## Latest CLI/API Source-Batch Execution Parity

The prior slice added `batch <source> execute` to the TUI so the operator can
fill exactly one source chunk from the current full scan. This slice makes the
same operation scriptable and API-accessible.

Changes in this slice:

- Added `src/catalyst_radar/dashboard/source_batches.py`, a shared executor for
  one next source-fill chunk from `priced_in_source_gap_batches_payload()`.
- The shared executor preserves the existing source boundaries:
  - `local_text` runs stored-event text intelligence and makes 0 external
    calls.
  - `catalyst_events` runs the existing SEC submissions batch executor and
    preserves SEC live/user-agent checks.
  - `options` / `broker_context` run the read-only Schwab market-context sync
    through the same token and rate-limit guards used by the broker route.
- The TUI now calls the shared executor instead of keeping its own private
  execution logic.
- CLI now supports:

  ```powershell
  catalyst-radar priced-in-source-batches --source <source> --execute-next
  ```

  Without `--execute-next`, the command remains plan-only and makes 0 provider
  calls. `--execute-next` cannot be combined with `--all`.
- API now supports:

  ```text
  POST /api/radar/priced-in/source-batches/execute-next
  ```

  with body fields `source`, optional `available_at`, `status`, `usefulness`,
  `decision_gap`, and `min_gap`.
- The new route is explicitly allowlisted in the security-boundary test because
  its path intentionally includes the word `execute` and can use read-only
  Schwab for broker-context/options source fill.
- README and `docs/dashboard-feature-inventory.md` now document the TUI,
  CLI, and API one-chunk execution surfaces.

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_batch_command_opens_full_scan_source_batch_plan tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_batch_execute_runs_one_guarded_local_chunk tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_source_batches_execute_next_cli_runs_one_batch tests\integration\test_api_routes.py::test_get_radar_priced_in_source_batches_returns_zero_call_plan tests\integration\test_api_routes.py::test_post_radar_priced_in_source_batch_execute_next_runs_one_chunk tests\integration\test_security_boundaries.py::test_openapi_routes_are_allowlisted_and_broker_routes_are_explicit -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\source_batches.py src\catalyst_radar\dashboard\tui.py src\catalyst_radar\cli.py src\catalyst_radar\api\routes\radar.py tests\integration\test_dashboard_demo_seed_cli.py tests\integration\test_api_routes.py tests\integration\test_security_boundaries.py
git diff --check
```

Observed: focused tests passed, ruff passed, and `git diff --check` passed.
A temp-database CLI smoke with `seed-dashboard-demo` followed by
`priced-in-source-batches --source local_text --execute-next` returned
`status=no_action`, `external_calls=0`, and the expected "No batch action is
needed for this source" message.

## Latest TUI Full-Scan Batch Clarification

The user asked again: "Why only these tickers? I want full scan." The live
database now shows the important distinction:

- The current priced-in queue is a full ranked universe page: 2,429 rows in the
  latest local smoke.
- The 5 tickers shown by `priced-in-source-batches` or the TUI `batch <source>`
  command are only the next safe executor chunk for a weak source, not the scan
  universe.
- Full evidence fill is therefore an iterative source-fill workflow over all
  planned chunks, not one accidental "call every provider for every ticker"
  action.

Changes in this slice:

- TUI help now documents both `batch <source>` and
  `batch <source> execute`.
- `batch <source>` remains plan-only and zero-call. Its message now says:
  "This is a full-scan plan, not a watchlist" and labels the listed tickers as
  the "next safe chunk only."
- `batch <source> execute` and `batch execute <source>` now run exactly one
  guarded source-fill chunk:
  - `local_text` calls the local text pipeline over stored event text and makes
    0 external calls.
  - `catalyst_events` calls the existing SEC submissions batch executor and
    preserves SEC live/user-agent guards.
  - `options` and `broker_context` call the existing read-only Schwab market
    sync route, preserving the Schwab connection/rate-limit/token guards.
- The ops page wording now says examples are sample tickers only and tells the
  operator to use `batch <source> execute` only for the next guarded chunk.
- README and `docs/dashboard-feature-inventory.md` now explain the distinction
  between the full ranked scan, the full source-fill plan, and one provider
  chunk.

Validation so far:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_batch_command_opens_full_scan_source_batch_plan tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_batch_execute_runs_one_guarded_local_chunk -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\tui.py tests\integration\test_dashboard_demo_seed_cli.py
```

Observed: focused tests passed, ruff passed, `git diff --check` passed, and a
static `dashboard-tui --once --page help | Select-String batch` smoke passed
with a larger timeout. The static smoke takes about 70 seconds on the current
large local dashboard payload.

## Latest Full-Scan Universe Scope Correction

The user pushed back again on limited visible tickers: "Why only these tickers?
I want full scan." The important product rule is now explicit:

- The small visible ticker list is only the human review window.
- A real full scan must run against a named point-in-time liquid universe,
  normally `liquid-us`.
- The daily/scheduled path must not silently fall back to every active security
  in the raw securities table when the operator intended a full market equity
  scan. Raw active securities can include warrants, units, odd share classes,
  and other instruments that are not the intended stock universe.

Root cause found in this slice:

- `scan --universe <name>` already honored universe snapshots.
- `run-daily` accepted scheduler scope fields but the feature-scan step did not
  load the named universe snapshot. If no explicit ticker list was supplied, it
  scanned all active securities.
- `scripts/run-full-market-scan.ps1` claimed the full-market sequence included a
  universe build, but the execute path did not actually call `build-universe`
  before `run-daily`.
- The zero-call preflight command hints did not make the universe/provider pair
  explicit enough.

Changes in this slice:

- `DailyRunSpec` / scheduler CLI now support `--provider` and `--universe` on
  `run-daily`.
- `run_daily()` now keeps a `ProviderRepository` in the daily context so the
  feature-scan step can resolve universe snapshots.
- `_feature_scan()` now scopes work in this order:
  1. explicit `--ticker` list;
  2. named `--universe` snapshot;
  3. raw active securities only when neither ticker nor universe was requested.
- If `run-daily --universe <name>` cannot find a point-in-time snapshot, the
  feature scan fails closed with `reason=universe_not_found` and downstream
  scoring/policy steps stay blocked.
- Successful universe-scoped daily scans report `scan_scope`, `universe`,
  `universe_snapshot_id`, and `universe_member_count` in the feature-scan
  payload.
- Dashboard priced-in queue reads now respect the latest run's named universe.
  If an older same-date all-active scan exists in the database, those older
  out-of-universe rows no longer leak back into the visible "full scan" queue.
- `build-universe` now defaults to the configured scheduled market provider
  (`CATALYST_DAILY_MARKET_PROVIDER`) before falling back to the older
  `CATALYST_MARKET_PROVIDER`.
- `priced-in-preflight` command hints now include
  `build-universe --provider <provider>` and
  `run-daily --provider <provider> --universe <name>`.
- `scripts/run-full-market-scan.ps1` now plans and executes:
  1. `ingest-polygon tickers --max-pages <n>`;
  2. `ingest-polygon grouped-daily --date <as_of>`;
  3. `build-universe --as-of <as_of> --available-at <cutoff> --name <universe> --provider polygon`;
  4. `run-daily --provider polygon --universe <universe> --json`.
- The script temporarily sets `CATALYST_DAILY_MARKET_PROVIDER=off` only for the
  `run-daily` call after it has already ingested grouped bars explicitly. This
  prevents a duplicate grouped-daily provider call while preserving the complete
  daily radar pipeline. The original environment value is restored in `finally`.

Expected operator behavior after this correction:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run-full-market-scan.ps1
```

Plan-only output should show zero external calls and include this shape:

```text
catalyst-radar ingest-polygon tickers --max-pages <n>
catalyst-radar ingest-polygon grouped-daily --date <LATEST_TRADING_DATE>
catalyst-radar build-universe --as-of <LATEST_TRADING_DATE> --available-at <UTC-now> --name liquid-us --provider polygon
catalyst-radar run-daily --as-of <LATEST_TRADING_DATE> --available-at <UTC-now> --provider polygon --universe liquid-us --json
```

`-Execute` is the credentialed path. It may call Polygon/Massive for ticker
reference and grouped daily bars. It still does not enable Schwab order
submission or real LLM execution.

Validation run in this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_jobs.py::test_daily_run_feature_scan_uses_universe_snapshot tests\integration\test_jobs.py::test_daily_run_feature_scan_fails_closed_when_universe_missing tests\integration\test_jobs.py::test_scheduler_config_passes_scan_scope_to_daily_spec tests\integration\test_scan_universe_filter.py::test_build_universe_defaults_to_daily_market_provider tests\integration\test_local_scripts.py::test_run_full_market_scan_script_is_plan_first_and_execute_gated -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\jobs\tasks.py src\catalyst_radar\cli.py src\catalyst_radar\dashboard\data.py tests\integration\test_jobs.py tests\integration\test_scan_universe_filter.py tests\integration\test_local_scripts.py
git diff --check
```

Observed: focused pytest passed, ruff passed, and `git diff --check` passed.
Zero-call script plan printed the full sequence with `build-universe
--available-at <UTC-now>` and `run-daily --provider polygon --universe
liquid-us`. A local no-provider-call smoke built `liquid-us` from stored Polygon
bars with 2,429 members, then `run-daily --provider polygon --universe
liquid-us` completed the feature scan over 2,429 rows. The overall daily run
remained `partial_success` because degraded-mode and source-coverage gates are
still blocking downstream research steps, not because the full scan scope
failed.

## Latest Priced-In vs Trade-Readiness Boundary Correction

The current full-scan answer can be ready for human priced-in review while the
system is still not safe for manual trading. The API/CLI had one misleading
field: `priced-in-answer` derived `can_make_investment_decision` from the
priced-in answer's `decision_ready` flag. That blurred the product boundary and
made a research answer look like trade approval.

Changes in this slice:

- `priced_in_answer_payload()` now keeps `decision_ready` /
  `priced_in_answer_ready` for the emotion-vs-reaction answer only.
- `can_make_investment_decision` and `manual_investment_decision_ready` stay
  `false` in the priced-in answer payload. Trade readiness remains governed by
  `GET /api/radar/readiness` and the `manual_buy_review` gate.
- Text-mode `priced-in-answer` now prints both `decision_ready=<bool>` and
  `investment_decision_ready=false`, plus an explicit investment boundary line.
- Static and interactive TUI headers now separate priced-in answer status from
  trade status, so the dashboard can say "priced-in decision_ready" while still
  showing "trade safe false."
- README and the radar-run runbook document the boundary.

Expected live smoke shape after this correction:

```text
priced_in_answer status=decision_ready decision_ready=true investment_decision_ready=false total=12087 ...
investment_boundary=Priced-in answer readiness is not trade approval...
```

## Latest Optional-Context Readiness Correction

The previous full-scan correction exposed a deeper product issue: the scanner
was treating missing options/broker context as a blocker for answering the core
question, even when market bars, catalyst events, local text, Candidate Packet,
and Decision Card were already present. That made the answer stay
`research_only` until broad options coverage existed, which is not useful for
ordinary equity priced-in analysis.

Changes in this slice:

- `options`, `broker_context`, and `theme_peer_sector` are now optional context
  gaps for the priced-in answer.
- Blocking decision gaps remain focused on local/actionable artifacts:
  Candidate Packet and Decision Card, after core market/catalyst/text evidence
  is present.
- Row usefulness now reports `optional_context_gaps` separately from
  `missing_for_decision`.
- Text-mode `candidate-detail` prints optional context beside usefulness, e.g.:

  ```text
  usefulness=decision_useful decision_ready=true next=Review the priced-in evidence and optional source gaps. optional_context=options
  ```

- `priced-in-answer` no longer reports broad optional source gaps as
  `trust_blockers` after the answer is decision-ready. Source coverage still
  shows options/broker gaps for follow-up.
- README, dashboard feature inventory, and radar-run runbook now distinguish
  the priced-in answer from investment/manual-buy readiness.

Live zero-provider-call smoke after this correction:

```text
status decision_ready
decision_ready True
counts {'actionable_mismatch_rows': 7, 'blocked_rows': 7920, 'decision_ready_rows': 5, 'research_lead_rows': 0, 'total_rows': 12087, 'visible_rows': 5}
trust_blockers []
top [('A', 'decision_useful', True, None, ['options']), ('MSFT', 'decision_useful', True, None, ['options']), ('AAA', 'blocked', False, None, ['options', 'broker_context']), ('AAAU', 'decision_useful', True, None, ['options']), ('AAPL', 'decision_useful', True, None, ['options'])]
```

Candidate detail smoke:

```text
candidate_detail ticker=A status=bullish_not_priced_in blocked=false
usefulness=decision_useful decision_ready=true next=Review the priced-in evidence and optional source gaps. optional_context=options
source_actions:
- options status=missing ... command=catalyst-radar schwab-market-sync --ticker A example_tickers=A
```

Interpretation:

- The latest local scan still covers the full available universe (`12087`
  ranked rows in this smoke). The 5 tickers in `top` are only the default
  answer display window for human review, not the scan universe.
- To inspect/export every scanned ticker, use
  `catalyst-radar priced-in-queue --full-scan --all --json`. To page through it
  interactively, use the TUI Insights page, `next` / `prev`, or
  `priced-in-queue --full-scan --limit <n> --offset <n>`.
- MarketRadar can now answer the current full-scan priced-in question with zero
  provider calls while still showing optional context gaps.
- This does not mean automated trading or Schwab order submission is available.
  Manual buy/investment readiness remains governed by the separate investment
  readiness gate.

Validation for this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_queue_payload_paginates_ranked_rows tests\integration\test_dashboard_data.py::test_priced_in_answer_payload_summarizes_current_scan tests\integration\test_dashboard_data.py::test_priced_in_queue_payload_filters_decision_gaps tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_queue_cli_outputs_same_zero_call_signal tests\integration\test_dashboard_demo_seed_cli.py::test_candidate_detail_cli_outputs_priced_in_evidence_brief tests\integration\test_api_routes.py::test_get_radar_priced_in_queue_returns_cli_ready_rows tests\unit\test_agent_sdk_orchestrator.py::test_redacted_operator_snapshot_allowlists_dashboard_fields -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\cli.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py
git diff --check
```

Observed: focused pytest passed, ruff passed, and `git diff --check` passed.

## Latest Full-Scan Recommendation Correction

The user pushed back on ticker-limited commands: "Why only these tickers? I want
full scan." Root cause: the scan itself was full-universe, but the
decision-readiness recommendation layer displayed optimized sample/exact-ticker
repair commands. That made the product look like it was only scanning or acting
on the visible ticker page.

Changes in this slice:

- Source-gap recommendations now use the full batch-plan command:

  ```text
  catalyst-radar priced-in-source-batches --source options --all --json
  ```

  This remains zero-call. It lists every executor chunk for the full current
  gap; individual batch execution remains explicit and rate-limited.

- Local artifact recommendations now use full scan-date commands instead of
  ticker-sliced commands:

  ```text
  catalyst-radar build-packets --as-of 2026-05-15 --min-state ResearchOnly
  catalyst-radar build-decision-cards --as-of 2026-05-15 --min-state ResearchOnly
  ```

- `load_radar_run_candidate_rows(..., include_post_run_artifacts=True)` was
  added for current dashboard/API views. Historical cutoff behavior remains the
  default, but the current dashboard can now show local Candidate Packets and
  Decision Cards built after the radar scan.
- The TUI snapshot and `GET /api/radar/candidates` use the current-artifact
  mode when no explicit `available_at` cutoff is requested.

Live zero-provider-call smoke after this correction:

```text
priced_in_answer status=research_only decision_ready=false total=12087 mismatches=7 research=5 blocked=7920 external_calls=0
scan_scope=Showing ranked rows 1-5 of 12087; the visible tickers are one page from the full scan, not the scan universe.
recommended_gap=options count=7 command=catalyst-radar priced-in-source-batches --source options --all --json
```

Full source-batch plan smoke:

```text
status ready total_gap_rows 12087 plannable 12087 batch_count 2418 count 2418 all_batches True external_calls 0
all_batches_command catalyst-radar priced-in-source-batches --source options --all --json
first_batch_size 5
```

Important interpretation:

- `priced-in-answer` no longer reports `research_only` only because options
  coverage is missing. Options remain visible as optional context.
- The `--all --json` command is a plan/export over the full current gap, not
  provider execution.
- A listed Schwab options batch is still the explicit read-only executor step
  and should not be run blindly across 2418 batches.

Validation for this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_radar_run_rows_can_include_post_run_local_artifacts tests\integration\test_dashboard_data.py::test_priced_in_answer_prefers_local_artifact_gap_before_options tests\integration\test_dashboard_data.py::test_priced_in_queue_payload_paginates_ranked_rows tests\integration\test_dashboard_data.py::test_priced_in_queue_source_actions_use_full_scan_batch_plan_for_broad_gaps tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_queue_cli_outputs_same_zero_call_signal tests\integration\test_api_routes.py::test_get_candidates_uses_latest_radar_run_scope -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\dashboard\tui.py src\catalyst_radar\api\routes\radar.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py tests\integration\test_api_routes.py
git diff --check
```

Observed: focused pytest passed, ruff passed, and `git diff --check` passed.

## Latest Decision-Readiness Gap Summary

The full scan is broad, but the priced-in answer still says `research_only`.
The missing product surface was a scan-level explanation for why
`decision_ready_rows=0`. Source coverage alone was too broad because it mixed
the entire 12k-row universe with the smaller actionable-mismatch set.

Changes in this slice:

- `priced_in_queue_payload()` now emits `decision_gap_counts`.
- `decision_gap_counts` is scoped to actionable mismatch rows, not every neutral
  or blocked scan row.
- `priced_in_answer_payload()` now emits `decision_readiness` with:
  - actionable mismatch row count;
  - decision-ready row count;
  - top decision gaps;
  - recommended first gap;
  - concrete command for the recommended gap.
- `priced-in-answer` CLI now prints the decision readiness summary and
  recommended gap command.
- The TUI overview now shows `Decision readiness:` before the row table so the
  operator does not have to infer why rows are not decision-ready.
- The dry-run agent brief receives the same `decision_readiness` object and now
  includes the recommended blocker in the priced-in insight.

Live zero-provider-call smoke:

```text
priced_in_answer status=research_only decision_ready=false total=12087 mismatches=7 research=5 blocked=7920 external_calls=0
decision_readiness=status=blocked actionable=7 decision_ready=0 summary=0 of 7 actionable mismatch row(s) are decision-ready; start with options (7 row(s)).
recommended_gap=options count=7 command=catalyst-radar priced-in-source-batches --source options --batch-limit 5
```

TUI once smoke now shows, before ticker rows:

```text
Decision readiness: 0 of 7 actionable mismatch row(s) are decision-ready; start with options (7 row(s)). Command: catalyst-radar priced-in-source-batches --source options --batch-limit 5
```

Agent brief smoke now includes:

```text
Priced-in answer is research_only; decision_ready=false; ...; blocker=options; next=...
```

Validation for this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_queue_payload_surfaces_ranked_gap_rows tests\integration\test_dashboard_data.py::test_priced_in_answer_payload_summarizes_current_scan tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_answer_cli_outputs_current_scan_answer tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_tui_once_can_show_full_scan_mode tests\unit\test_agent_sdk_orchestrator.py::test_redacted_operator_snapshot_allowlists_dashboard_fields -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\dashboard\tui.py src\catalyst_radar\cli.py src\catalyst_radar\agents\sdk_orchestrator.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py tests\unit\test_agent_sdk_orchestrator.py
git diff --check
```

Observed: focused pytest passed, ruff passed, `git diff --check` passed.

### Priority Correction

Follow-up inspection showed the first decision-readiness summary recommended
`options` before local artifact work. That was too aggressive because current
Schwab option-chain context can be explicit/read-only but is still live context,
while Candidate Packet and Decision Card work is local and prerequisite to
human decision review.

The recommendation order is now:

```text
market_bars -> catalyst_events -> local_text -> candidate_packet -> decision_card -> options -> broker_context -> theme_peer_sector
```

Live smoke after the priority correction:

```text
recommended_gap={'gap': 'candidate_packet', 'count': 5, 'command': 'catalyst-radar build-packets --as-of 2026-05-15 --min-state AddToWatchlist', ...}
```

The candidate-packet and decision-card recommendations now use executable local
artifact commands when the scan date is known. They fall back to the filtered
queue command only if no scan date is available.

### Local Artifact Command Correction

Follow-up: `build-packets` and `build-decision-cards` accept repeated
`--ticker` arguments for targeted debugging, but decision-readiness
recommendations intentionally use full scan-date commands so the product does
not look ticker-limited.

Live smoke now prints:

```text
recommended_gap=candidate_packet count=5 command=catalyst-radar build-packets --as-of 2026-05-15 --min-state ResearchOnly
```

Important correction: actionable mismatch rows may still be in `ResearchOnly`,
so local packet/card commands use `--min-state ResearchOnly`. The previous
`AddToWatchlist` floor could return `built candidate_packets=0` even when the
priced-in answer correctly reported missing packets.

After running the corrected command locally:

```text
built candidate_packets=5
candidate_packet_gap_total 0 rows []
```

The next blocker became `decision_card`. The dashboard now keeps sample tickers
as examples, but the recommended command is full-scan-date by default.

## Latest Full-Scan Scope UX

The live backend is scanning the broad local universe. A zero-provider-call
check showed:

```text
status=ready
headline=Latest full scan ranked 12087 priced-in row(s); showing 1-5 of 12087.
scan.scanned_securities=12087
scan.requested_securities=12104
freshness.active_security_count=12613
rows=A,MSFT,AAA,AAAU,AAPL
```

The problem was presentation: top rows and first pages looked like the only
tickers being scanned. The change in this slice makes the answer explicit:

- `priced_in_answer_payload()` now emits `scan_scope`.
- `scan_scope` states whether the current view is `full_scan` or
  `filtered_scan`, which rows are visible, total row count, whether more pages
  exist, and the reason the visible tickers are only a page.
- `scan_scope` includes:

  ```text
  current_page_command
  next_page_command
  current_filter_export_command
  full_scan_export_command
  ```

- The CLI `priced-in-answer` now prints the scan-scope explanation and export
  commands.
- The TUI overview guide now says the visible tickers are one page from the
  scan scope, and `export full` prints:

  ```powershell
  catalyst-radar priced-in-queue --full-scan --all --json
  ```

Live zero-provider-call smoke:

```text
priced_in_answer status=research_only decision_ready=false total=12087 mismatches=7 research=5 blocked=7920 external_calls=0
scan_scope=Showing ranked rows 1-5 of 12087; the visible tickers are one page from the full scan, not the scan universe.
full_scan_export=catalyst-radar priced-in-queue --full-scan --all --json
next_page=catalyst-radar priced-in-queue --full-scan --limit 5 --offset 5
```

Validation for this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_answer_payload_summarizes_current_scan tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_scan_commands_page_full_scan_rows tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_tui_once_can_show_full_scan_mode tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_queue_cli_outputs_same_zero_call_signal -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\dashboard\tui.py src\catalyst_radar\cli.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py
```

Observed: focused pytest passed and ruff passed.

## Latest Priced-In Answer Decision Flag

The live `priced-in-answer` payload had `can_make_investment_decision`, while
the agent snapshot and dashboard language used `decision_ready`. That made the
same concept appear under different names across CLI/API/dashboard/agents.

Changes in this slice:

- `priced_in_answer_payload()` now emits both:

  ```text
  decision_ready
  can_make_investment_decision
  ```

- Both fields are driven by the same `decision_ready_count > 0` value.
- This keeps backward compatibility while giving agents and dashboards a stable
  direct readiness flag.

Live zero-provider-call smoke:

```text
priced-in-answer-v1 research_only decision_ready=False can_make_investment_decision=False external_calls_made=0
```

Validation for this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_answer_payload_summarizes_current_scan tests\integration\test_api_routes.py::test_get_radar_priced_in_answer_returns_current_scan_answer tests\unit\test_agent_sdk_orchestrator.py::test_redacted_operator_snapshot_allowlists_dashboard_fields -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py tests\integration\test_dashboard_data.py tests\integration\test_api_routes.py
git diff --check
```

Observed: focused pytest passed, ruff passed, `git diff --check` passed.

## Latest TUI Agent Brief Page

The CLI/API had a dry-run multi-agent brief, but the terminal dashboard still
had no page for it. That meant the operator had to leave the TUI to see the
agent summary of the priced-in answer and safety checks.

Changes in this slice:

- Added an `agent` TUI page.
- Added navigation aliases:

  ```text
  10
  agent
  agents
  brief
  Ctrl+A
  ```

- `dashboard_snapshot_payload()` now includes `agent_brief`, built with
  `run_market_radar_agents(..., real=False)`.
- The page shows:
  - specialist agent summaries,
  - priced-in answer insight,
  - next actions,
  - safety checks,
  - OpenAI/market/broker call counts.
- The page stays dry-run and makes no hidden provider, broker, shell,
  filesystem, web, or OpenAI calls.

Live zero-provider-call smoke:

```text
dashboard-tui --once --page agent
Page: agent | Status: research_only | Decision safe: False | External calls made: 0
Agent Brief
Mode: dry_run | Status: dry_run | Calls: openai=0, market=0, broker=0
Insight: Priced-in answer is research_only...
```

Validation for this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_snapshot_cli_outputs_dashboard_command_center_json tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_agent_page_shows_agent_brief tests\integration\test_dashboard_demo_seed_cli.py::test_modern_dashboard_tui_supports_mouse_navigation -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\tui.py tests\integration\test_dashboard_demo_seed_cli.py
git diff --check
```

Observed: focused pytest passed, ruff passed, `git diff --check` passed.

## Latest Agent Brief API

The CLI had `agent-brief`, but the API did not expose the same multi-agent
operator brief. That left the CLI/API surface uneven for the current goal.

Changes in this slice:

- Added read-only API:

  ```text
  GET /api/agents/brief
  ```

- The endpoint builds the same dashboard snapshot used by the CLI and runs
  `run_market_radar_agents(..., real=False)`.
- The API endpoint is viewer-readable and always dry-run. It makes no hidden
  OpenAI, Polygon/Massive, SEC, Schwab, broker, shell, filesystem, or web calls.
- It supports the same useful filters as the CLI brief path: ticker,
  available-at, priced-in status/usefulness/source gap/decision gap, scan
  limit/offset, telemetry limit, and operator goal.
- The route is in the API allowlist and the dashboard feature inventory.

Validation for this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_api_routes.py::test_get_agent_brief_returns_zero_call_market_radar_brief tests\integration\test_security_boundaries.py::test_openapi_routes_are_allowlisted_and_broker_routes_are_explicit -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\api\routes\agents.py tests\integration\test_api_routes.py tests\integration\test_security_boundaries.py
git diff --check
```

## Latest Agent Priced-In Answer Context

The dashboard and CLI already expose the direct answer to:

```text
Has price fully matched market expectations?
```

But the redacted agent snapshot only carried the priced-in queue, source
coverage, and evidence plan. That meant the multi-agent layer could infer the
answer, but did not receive the same explicit answer object the human dashboard
uses.

Changes in this slice:

- `redacted_operator_snapshot()` now includes `priced_in.answer`.
- The allowlisted answer context includes only safe fields:
  schema/status, decision readiness, question, answer, headline, next action,
  next command, counts, trust blockers, and zero-call count.
- Deterministic `agent-brief` now adds a direct priced-in answer insight before
  lower-level scan/evidence-plan insights.
- Agent next actions now include the priced-in answer's next action and command.
- The existing secret stripping still removes unsafe nested payload fields from
  answer objects before any model input.

Live zero-provider-call smoke:

```text
agent-brief status=dry_run external_calls={'broker': 0, 'market_data': 0, 'openai': 0}
insight=Priced-in answer is research_only; decision_ready=false; Not fully priced for 5 research lead(s), but none are decision-ready yet.; next=Review the run call plan and refresh event ingestion before trusting emotion..
next_action=catalyst-radar priced-in-source-batches --source catalyst_events --batch-limit 5
```

Validation for this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\unit\test_agent_sdk_orchestrator.py tests\integration\test_dashboard_demo_seed_cli.py::test_agent_brief_cli_outputs_zero_call_dry_run -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\agents\sdk_orchestrator.py tests\unit\test_agent_sdk_orchestrator.py
git diff --check
```

Observed: focused pytest passed, ruff passed, `git diff --check` passed.

## Latest Full-Scan Batch Plan Control

The user asked again: "Why only these tickers? I want full scan." The
important correction is that a provider executor must still run in safe chunks,
but the planner must let the operator see every chunk in the current full-scan
source gap.

Changes in this slice:

- Added CLI full-plan mode:

  ```powershell
  catalyst-radar priced-in-source-batches --source <source> --all --json
  ```

- Added API full-plan mode:

  ```text
  GET /api/radar/priced-in/source-batches?source=<source>&all_batches=true
  ```

- `priced_in_source_gap_batches_payload()` now supports `all_batches=True`.
  It resets the batch offset to `0`, returns every planned batch for the
  current filtered full-scan gap, sets `has_more=false`, and keeps
  `external_calls_made=0`.
- The source-batch payload now includes `all_batches_command` and
  `all_batches_api` so the TUI can show the full chunk list instead of making a
  five-ticker first chunk look like the universe.
- The TUI batch command now says `First chunk only` and shows the full chunk
  list command. It also no longer accidentally uses the next-page command as
  the displayed first executable chunk.

Validation for this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_can_return_full_scan_plan tests\integration\test_api_routes.py::test_get_radar_priced_in_source_batches_returns_zero_call_plan tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_queue_cli_outputs_same_zero_call_signal tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_batch_command_opens_full_scan_source_batch_plan -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\cli.py src\catalyst_radar\api\routes\radar.py src\catalyst_radar\dashboard\tui.py tests\integration\test_dashboard_data.py tests\integration\test_api_routes.py tests\integration\test_dashboard_demo_seed_cli.py
```

Observed: focused pytest passed and ruff passed.

Live zero-provider-call smoke:

```text
catalyst-radar priced-in-source-batches --source catalyst_events --all --json
status=ready total_gap_rows=12080 plannable_gap_rows=10462 batch_count=2093 count=2093 all_batches=True external_calls_made=0
all_batches_command=catalyst-radar priced-in-source-batches --source catalyst_events --all --json
first_batch_example=BRK.A,NVR,ABLVW
```

## Latest Local Text Batch API

The current full-scan answer is research-only because catalyst-event coverage is
thin and local text depends on event text. SEC source batches now have CLI/API
execution, but local text batches only had the CLI command:

```text
catalyst-radar run-textint --as-of <DATE> --ticker ...
```

Changes in this slice:

- Added API:

  ```text
  POST /api/radar/text/features-batch
  {"as_of":"2026-05-15","available_at":"2026-05-18T16:00:00+00:00","tickers":["MSFT","AAPL"]}
  ```

- The API route is analyst-only, caps batches at 50 unique tickers, rejects
  empty ticker lists, and makes no external provider calls. It runs the existing
  local `run_text_pipeline()` over stored event rows.
- `priced-in-source-batches --source local_text` now advertises:
  - CLI executor: `catalyst-radar run-textint --as-of ... --ticker ...`
  - API executor: `POST /api/radar/text/features-batch`
- This does not unblock live local text by itself. The live DB currently still
  needs catalyst event batches first; once events exist for a ticker, local text
  batches have both CLI and API execution paths.

Validation for this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_plans_local_text_batches tests\integration\test_api_routes.py::test_post_radar_text_features_batch_runs_local_text_pipeline tests\integration\test_api_routes.py::test_post_radar_text_features_batch_rejects_empty_tickers tests\integration\test_security_boundaries.py::test_openapi_routes_are_allowlisted_and_broker_routes_are_explicit -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\api\routes\radar.py src\catalyst_radar\dashboard\data.py tests\integration\test_api_routes.py tests\integration\test_dashboard_data.py tests\integration\test_security_boundaries.py
```

Observed: focused pytest passed and ruff passed.

## Latest Priced-In Answer Surface

The product gap after the SEC batch API was not another connector. The system
could show full-scan queue rows and preflight blockers, but it still forced the
operator to infer the answer to the core question:

```text
Has price fully matched market expectations?
```

Changes in this slice:

- Added `priced_in_answer_payload()` as a thin zero-call aggregator over the
  existing priced-in queue and preflight evidence plan.
- Added CLI:

  ```powershell
  catalyst-radar priced-in-answer
  catalyst-radar priced-in-answer --json
  ```

- Added API:

  ```text
  GET /api/radar/priced-in/answer
  ```

- Added `priced_in_answer` to `dashboard-snapshot --json`.
- The TUI overview guide and first insight row now surface the current answer
  before the operator has to inspect the full table.

Live zero-provider-call smoke:

```text
priced_in_answer status=research_only decision_ready=false total=12087 mismatches=7 research=5 blocked=7920 external_calls=0
question=Has price fully matched market expectations?
answer=Not fully priced for 5 research lead(s), but none are decision-ready yet.
headline=5 research-useful not-priced-in lead(s), 7 actionable mismatch row(s), 12087 scanned row(s).
next_action=Review the run call plan and refresh event ingestion before trusting emotion.
next_command=catalyst-radar priced-in-source-batches --source catalyst_events --batch-limit 5
source_coverage=market_bars 12087/12087; catalyst_events 7/12087 (12080 missing); local_text 7/12087 (12080 missing); options 0/12087 (12087 missing); theme_peer_sector 12087/12087; broker_context 5/12087 (12082 missing)
```

Interpretation: the scan is broad enough to produce research leads, but the
current answer is not decision-ready. The first useful next step remains filling
catalyst-event source coverage. Browsing and rendering this answer makes zero
Polygon/Massive, SEC, Schwab, OpenAI, or broker calls.

Validation for this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_answer_payload_summarizes_current_scan tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_snapshot_cli_outputs_dashboard_command_center_json tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_answer_cli_outputs_current_scan_answer tests\integration\test_api_routes.py::test_get_radar_priced_in_answer_returns_current_scan_answer tests\integration\test_security_boundaries.py::test_openapi_routes_are_allowlisted_and_broker_routes_are_explicit -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\cli.py src\catalyst_radar\api\routes\radar.py src\catalyst_radar\dashboard\tui.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py tests\integration\test_api_routes.py tests\integration\test_security_boundaries.py
```

Observed: focused pytest passed and ruff passed.

## Latest SEC Source-Batch API Executor

The user asked again: "Why only these tickers? I want full scan." The important
product distinction is:

- The priced-in scan is already full-market in the local ranked universe.
- Source-fill actions are intentionally split into small provider batches.
- The five tickers shown in a batch are batch 1, not the whole scan.

Current live zero-provider-call planner proof:

```text
status ready batch_count 2093 external 0
required 5 breakdown {'catalyst_events': 5} plan live_calls_planned
api POST /api/radar/sec/submissions-batch payload {'targets': [{'cik': '0001067983', 'ticker': 'BRK.A'}, {'cik': '0000906163', 'ticker': 'NVR'}, {'cik': '0001957489', 'ticker': 'ABLVW'}, {'cik': '0002033770', 'ticker': 'DAICW'}, {'cik': '0001889823', 'ticker': 'DFSCW'}]}
catalyst-radar ingest-sec submissions-batch --target BRK.A:0001067983 --target NVR:0000906163 --target ABLVW:0001957489 --target DAICW:0002033770 --target DFSCW:0001889823
```

This means there are 2,093 SEC event source-fill batches in the current full
scan. The first batch happens to contain `BRK.A`, `NVR`, `ABLVW`, `DAICW`, and
`DFSCW` because those are the first plannable source-gap rows after ranking and
CIK filtering. The operator should not read that list as a watchlist or as the
entire universe.

Changes in this slice:

- Added shared SEC ingest helpers in `src/catalyst_radar/events/sec_ingest.py`.
  The CLI and API now call the same `ingest_sec_submissions_batch()` function.
- Kept the CLI command:

  ```text
  catalyst-radar ingest-sec submissions-batch --target TICKER:CIK ...
  ```

- Added an API executor for the same batch:

  ```text
  POST /api/radar/sec/submissions-batch
  {"targets":[{"ticker":"MSFT","cik":"0000789019"}]}
  ```

- The API route requires the analyst role, rejects empty target lists, caps
  target count with `CATALYST_SEC_DAILY_MAX_TICKERS`, never accepts fixture
  paths, and still fails closed unless SEC live mode and SEC user-agent are
  configured.
- `priced-in-source-batches --source catalyst_events` now advertises both:
  - CLI executor: `catalyst-radar ingest-sec submissions-batch ...`
  - API executor: `POST /api/radar/sec/submissions-batch`
- The batch planner remains zero-call. Only executing a batch makes SEC calls.

Validation for this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_plans_sec_event_batches tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_avoids_market_call_for_sec_batches tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_batch_command_opens_full_scan_source_batch_plan tests\integration\test_sec_ipo_cli.py tests\integration\test_api_routes.py::test_get_radar_priced_in_source_batches_returns_zero_call_plan tests\integration\test_api_routes.py::test_post_radar_sec_submissions_batch_calls_capped_sec_executor tests\integration\test_api_routes.py::test_post_radar_sec_submissions_batch_rejects_too_many_targets tests\integration\test_api_routes.py::test_post_radar_sec_submissions_batch_rejects_empty_targets tests\integration\test_api_routes.py::test_post_radar_sec_submissions_batch_rejects_blank_target_fields tests\integration\test_security_boundaries.py::test_openapi_routes_are_allowlisted_and_broker_routes_are_explicit -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\events\sec_ingest.py src\catalyst_radar\api\routes\radar.py src\catalyst_radar\cli.py src\catalyst_radar\dashboard\data.py src\catalyst_radar\dashboard\tui.py tests\integration\test_api_routes.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py tests\integration\test_security_boundaries.py tests\integration\test_sec_ipo_cli.py
git diff --check
```

Observed: focused pytest passed, ruff passed, `git diff --check` passed.

## Latest SEC-Only Catalyst Source Batches

The previous source-batch slice fixed budget visibility, but exposed a deeper
workflow problem: a catalyst-event source batch used `run-daily`. In Polygon
mode that would repeat the grouped-daily market-data request for every SEC
event batch. With the current live full-scan plan, that meant 2,093 event
batches could imply 2,093 duplicate Polygon market calls if an operator tried
to fill the source gap batch-by-batch.

This slice changes catalyst-event source batches to fill the source directly:

```text
catalyst-radar ingest-sec submissions-batch --target TICKER:CIK ...
```

Changes in this slice:

- Added `ingest-sec submissions-batch --target TICKER:CIK` as a small wrapper
  around the existing SEC submissions ingest. It loops over explicit CIK-backed
  targets and prints one aggregate result line.
- Catalyst-event source batches now include `targets[]` with public ticker/CIK
  pairs.
- Catalyst-event source-batch commands now call `ingest-sec submissions-batch`
  instead of `run-daily`.
- Catalyst-event source batches no longer advertise `POST /api/radar/runs`,
  because there is no direct API executor for SEC-only batch ingest yet. The
  API still exposes the zero-call plan and executable CLI command.
- Catalyst-event source-batch budgets now count SEC calls only:

  ```text
  external_calls_required = target_count
  external_call_breakdown = {"catalyst_events": target_count}
  ```

- The `calls` column and call breakdown from PR #256 remain useful for Schwab,
  local text, and SEC-only catalyst batches.

Current live zero-provider-call smoke:

```powershell
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-source-batches --source catalyst_events --batch-limit 1 --json |
  .\.venv\Scripts\python.exe -c "import json,sys; p=json.load(sys.stdin); b=p['batches'][0]; print('status', p['status'], 'batch_count', p['batch_count'], 'external', p['external_calls_made']); print('required', b['external_calls_required'], 'breakdown', b['external_call_breakdown'], 'plan', b['call_plan_status']); print('api', b['api'], 'payload', b['api_payload']); print(b['command'])"
```

Observed:

```text
status ready batch_count 2093 external 0
required 5 breakdown {'catalyst_events': 5} plan live_calls_planned
api None payload None
catalyst-radar ingest-sec submissions-batch --target BRK.A:0001067983 --target NVR:0000906163 --target ABLVW:0001957489 --target DAICW:0002033770 --target DFSCW:0001889823
```

Human CLI smoke:

```powershell
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-source-batches --source catalyst_events --batch-limit 1 |
  Select-String -Pattern 'priced_in_source_batches|batch calls|calls=|call_plan=|ingest-sec|run-daily'
```

Observed:

```text
priced_in_source_batches source=catalyst_events status=ready gap_rows=12080 plannable=10462 planned_at=2026-05-18T13:59:52+00:00 batch_size=5 batches=1 total_batches=2093 batch_offset=0 external_calls=0
batch calls row_start row_end tickers command
1 5 1 5 BRK.A,NVR,ABLVW,DAICW,DFSCW catalyst-radar ingest-sec submissions-batch --target BRK.A:0001067983 --target NVR:0000906163 --target ABLVW:0001957489 --target DAICW:0002033770 --target DFSCW:0001889823
  calls=catalyst_events:5
  call_plan=live_calls_planned
```

Validation:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_plans_sec_event_batches tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_avoids_market_call_for_sec_batches tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_batch_command_opens_full_scan_source_batch_plan tests\integration\test_sec_ipo_cli.py::test_ingest_sec_submissions_batch_persists_events tests\integration\test_sec_ipo_cli.py::test_ingest_sec_submissions_batch_requires_ticker_cik_targets -q
.\.venv\Scripts\python.exe -m pytest tests\integration\test_sec_ipo_cli.py -q
.\.venv\Scripts\python.exe -m pytest tests\integration\test_api_routes.py::test_get_radar_priced_in_source_batches_returns_zero_call_plan -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\cli.py src\catalyst_radar\dashboard\data.py tests\integration\test_dashboard_data.py tests\integration\test_sec_ipo_cli.py
git diff --check
```

All passed.

## Latest Full-Scan Source Action Wording

The user asked again: "Why only these tickers? I want full scan." The live
backend already had the full scan:

```text
priced_in_queue status=ready count=3 total=12087 offset=0 external_calls=0
scan_scope=scanned=12087 requested=12104 filter=all ranked_after_filter=12087 visible_page=3
```

Root cause of the confusion: source coverage actions for broad gaps showed a
five-ticker provider command as the primary `command=...`, so the UI/CLI made a
safe sample batch look like the entire scan universe.

Change in this slice:

- For batchable source gaps larger than the displayed example ticker set,
  `source_coverage.actions[].command` now points to the full-scan batch planner:

  ```text
  catalyst-radar priced-in-source-batches --source <source> --batch-limit 5
  ```

- The source action `api` now points to the corresponding zero-call batch-plan
  API:

  ```text
  GET /api/radar/priced-in/source-batches?source=<source>
  ```

- Direct five-ticker Schwab commands are still available, but only as
  `sample_command`, with `sample_api_payload` next to it. They are examples for
  a safe executable batch, not the full scan.
- CLI source-action output now labels examples as `example_tickers=...`, not
  `examples=...`.
- Options diagnostics now say `Example tickers: ...` instead of wording that
  could imply those names are the complete scope.
- Small scans are unchanged: when all gap rows fit inside the example ticker
  list, the direct command can still be the exact full set.

Current live zero-provider-call smoke:

```powershell
.\.venv\Scripts\python.exe -m catalyst_radar.cli priced-in-queue --status all --limit 3 |
  Select-String -Pattern 'priced_in_queue|scan_scope|source_coverage|source_actions:|^- options|^- catalyst_events|^- local_text|^- broker_context|  sample_scope|  batch_plan|  sample_command|example_tickers'
```

Observed:

```text
priced_in_queue status=ready count=3 total=12087 offset=0 external_calls=0
scan_scope=scanned=12087 requested=12104 filter=all ranked_after_filter=12087 visible_page=3
source_coverage=market_bars 12087/12087; catalyst_events 7/12087 (12080 missing); local_text 7/12087 (12080 missing); options 0/12087 (12087 missing); theme_peer_sector 12087/12087; broker_context 5/12087 (12082 missing)
- catalyst_events ... command=catalyst-radar priced-in-source-batches --source catalyst_events --batch-limit 5 example_tickers=BRK.A,NVR,ABLVW,DAICW,DFSCW
  batch_plan=catalyst-radar priced-in-source-batches --source catalyst_events --batch-limit 5
- local_text ... command=catalyst-radar priced-in-source-batches --source local_text --batch-limit 5 example_tickers=BRK.A,NVR,ABLVW,DAICW,DFSCW
  batch_plan=catalyst-radar priced-in-source-batches --source local_text --batch-limit 5
- options ... command=catalyst-radar priced-in-source-batches --source options --batch-limit 5 example_tickers=A,MSFT,AAA,AAAU,AAPL
  batch_plan=catalyst-radar priced-in-source-batches --source options --batch-limit 5
  sample_command=catalyst-radar schwab-market-sync --ticker A --ticker MSFT --ticker AAA --ticker AAAU --ticker AAPL
- broker_context ... command=catalyst-radar priced-in-source-batches --source broker_context --batch-limit 5 example_tickers=AAA,AAAC,BRK.A,NVR,ABLVW
  batch_plan=catalyst-radar priced-in-source-batches --source broker_context --batch-limit 5
  sample_command=catalyst-radar schwab-market-sync --ticker AAA --ticker AAAC --ticker BRK.A --ticker NVR --ticker ABLVW
```

Validation:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_queue_payload_paginates_ranked_rows tests\integration\test_dashboard_data.py::test_priced_in_queue_source_actions_use_full_scan_batch_plan_for_broad_gaps tests\integration\test_dashboard_data.py::test_priced_in_queue_payload_diagnoses_options_after_scan_date tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_queue_cli_outputs_same_zero_call_signal -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\cli.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py
```

Both passed.

## Latest Executable Source-Batch Timestamps

The full-scan event source batch planner previously emitted:

```text
--available-at <UTC-now>
"available_at": "<UTC-now>"
```

That explained intent, but it was not directly executable from CLI/API.

Changes in this slice:

- `priced-in-source-batches --source catalyst_events` now includes a concrete
  UTC `planned_at` timestamp.
- Event batch CLI commands use that concrete timestamp in `--available-at`.
- Event batch API payloads use the same concrete timestamp in `available_at`.
- Human CLI output prints `planned_at=...`.
- Planning still makes zero provider calls.

Live zero-provider-call verification:

```powershell
.\.venv\Scripts\catalyst-radar.exe priced-in-source-batches --source catalyst_events --batch-limit 1 --json |
  .\.venv\Scripts\python.exe -c "import json,sys,datetime; p=json.load(sys.stdin); b=p['batches'][0]; print(p['planned_at']); print(b['command']); print(b['api_payload']); datetime.datetime.fromisoformat(b['api_payload']['available_at']); print('<UTC-now>' in b['command'], p['external_calls_made'])"
```

Observed:

```text
2026-05-18T13:20:38+00:00
catalyst-radar run-daily --as-of 2026-05-15 --available-at 2026-05-18T13:20:38+00:00 --ticker BRK.A --ticker NVR --ticker ABLVW --ticker DAICW --ticker DFSCW --json
{'as_of': '2026-05-15', 'available_at': '2026-05-18T13:20:38+00:00', 'dry_run_alerts': True, 'run_llm': False, 'tickers': ['BRK.A', 'NVR', 'ABLVW', 'DAICW', 'DFSCW']}
False 0
```

Human CLI smoke:

```powershell
.\.venv\Scripts\catalyst-radar.exe priced-in-source-batches --source catalyst_events --batch-limit 1 |
  Select-String -Pattern 'planned_at|run-daily|<UTC-now>'
```

Observed `planned_at=...` and a concrete `run-daily --available-at ...` command;
`<UTC-now>` was absent.

Validation for this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_plans_sec_event_batches tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_plans_safe_sync_batches tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_batch_command_opens_full_scan_source_batch_plan -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\cli.py tests\integration\test_dashboard_data.py
git diff --check
```

All passed.

## Latest Agent Evidence Plan Context

The CLI/API/dashboard gained an ordered priced-in evidence plan, but the
agent-safe snapshot still only exposed source coverage and top rows. That meant
the deterministic and real Agents SDK path could miss the operator sequence that
the human dashboard now shows.

Changes in this slice:

- The redacted operator snapshot now includes:

  ```text
  priced_in.evidence_plan
  ```

- The evidence plan context is allowlisted and limited to safe fields:
  schema/status/headline/next action/next command/external-call count and up to
  eight steps.
- Deterministic `agent-brief` now emits a priced-in evidence-plan insight.
- `agent-brief` next actions include the plan's first action and first command.
- The default agent brief remains zero-call. It does not call OpenAI, market
  data, Schwab, shell, filesystem, or order endpoints.

Live zero-provider-call verification:

```powershell
.\.venv\Scripts\catalyst-radar.exe agent-brief --json |
  .\.venv\Scripts\python.exe -c "import json,sys; p=json.load(sys.stdin); print([x for x in p['insights'] if 'evidence plan' in x.lower()][0]); print([a for a in p['next_actions'] if 'priced-in-source-batches' in a][:2]); print(p['external_calls_made'])"
```

Observed:

```text
Priced-in evidence plan is attention; steps=5; next=Review the run call plan and refresh event ingestion before trusting emotion.; command=catalyst-radar priced-in-source-batches --source catalyst_events --batch-limit 5.
['catalyst-radar priced-in-source-batches --source catalyst_events --batch-limit 5']
{'broker': 0, 'market_data': 0, 'openai': 0}
```

Validation for this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\unit\test_agent_sdk_orchestrator.py tests\integration\test_dashboard_demo_seed_cli.py::test_agent_brief_cli_outputs_zero_call_dry_run -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\agents\sdk_orchestrator.py tests\unit\test_agent_sdk_orchestrator.py
git diff --check
```

All passed.

## Latest Priced-In Evidence Plan

The source-gap commands were available, but the user still had to infer the
operator sequence from separate preflight rows. That was not useful enough for
the goal of moving from a full-market scan to a trustworthy priced-in read.

Changes in this slice:

- `priced_in_preflight_payload()` now includes:

  ```text
  evidence_plan.schema_version = priced-in-evidence-plan-v1
  evidence_plan.status
  evidence_plan.headline
  evidence_plan.next_action
  evidence_plan.next_command
  evidence_plan.steps[]
  ```

- `catalyst-radar priced-in-preflight` now prints the evidence plan after the
  raw preflight rows.
- `GET /api/radar/priced-in/preflight` exposes the same `evidence_plan`.
- The dashboard Run page now includes a `Priced-in Evidence Plan` section.
- The plan keeps provider calls at zero while planning. It is a read-only
  sequencing artifact, not an executor.

The ordering is intentionally practical:

1. hard blockers first, if any;
2. `catalyst_events`;
3. `local_text`, which depends on `catalyst_events`;
4. `options`;
5. `broker_context`;
6. softer market-bar attention rows.

Live zero-provider-call verification:

```powershell
.\.venv\Scripts\catalyst-radar.exe priced-in-preflight --json |
  .\.venv\Scripts\python.exe -c "import json,sys; ep=json.load(sys.stdin)['evidence_plan']; print(ep['status'], ep['next_action'], ep['next_command']); print([s['area'] for s in ep['steps'][:5]])"
```

Observed:

```text
attention Review the run call plan and refresh event ingestion before trusting emotion. catalyst-radar priced-in-source-batches --source catalyst_events --batch-limit 5
['catalyst_events', 'local_text', 'options', 'broker_context', 'market_bars']
```

API verification through FastAPI `TestClient`:

```text
200
priced-in-evidence-plan-v1 attention 5 0
catalyst_events catalyst-radar priced-in-source-batches --source catalyst_events --batch-limit 5
```

Dashboard verification:

```powershell
.\.venv\Scripts\catalyst-radar.exe dashboard-tui --once --page run |
  Select-String -Pattern 'Priced-in Evidence Plan|Next evidence step|catalyst_events|local_text|options|broker_context|market_bars'
```

Validation for this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_preflight_payload_reports_exact_next_steps tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_preflight_cli_outputs_zero_call_plan tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_run_page_shows_priced_in_evidence_plan tests\integration\test_api_routes.py::test_get_radar_priced_in_preflight_returns_zero_call_steps -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\dashboard\tui.py src\catalyst_radar\cli.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py tests\integration\test_api_routes.py
git diff --check
```

All passed.

## Latest Preflight Source Dependency Order

After the dashboard source-batch command landed, a live `priced-in-preflight`
check showed a confusing dependency order:

- `local_text` had a batch planner command;
- but on the live DB, `local_text` is blocked until catalyst event text exists;
- the row still said to run text intelligence directly.

Change in this slice:

- When both `catalyst_events` and `local_text` have priced-in source gaps,
  preflight now tells the operator:

  ```text
  Fill catalyst_events first, then run local_text batches for rows with event text.
  ```

The command remains:

```powershell
catalyst-radar priced-in-source-batches --source local_text --batch-limit 5
```

That is intentional. The batch planner is still the right tool, but the preflight
row now explains the dependency before the user expects local text to work.

Live zero-provider-call verification:

```powershell
.\.venv\Scripts\catalyst-radar.exe priced-in-preflight |
  Select-String -Pattern '^local_text|^catalyst_events'
```

Observed `local_text` next action:

```text
Fill catalyst_events first, then run local_text batches for rows with event text.
```

Validation for this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_preflight_payload_reports_exact_next_steps tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_preflight_cli_outputs_zero_call_plan -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py tests\integration\test_dashboard_data.py
git diff --check
```

All passed.

## Latest Dashboard Source-Batch Command

The CLI/API now proves full-scan source-gap batch planning, but the dashboard
still made the Ops source-gap table feel like a small ticker list because it
showed only examples plus a truncated batch-plan command.

Changes in this slice:

- The Ops source-gap table now states that examples are sample tickers only.
- The TUI help page now documents:

  ```text
  batch <source>
  ```

- The interactive TUI command parser accepts:

  ```text
  batch catalyst_events
  batch local_text
  batch options
  batch broker_context
  ```

- The command opens Ops and prints a one-line source-batch summary with:
  - source status;
  - full-scan gap rows;
  - plannable rows;
  - total batch count;
  - first runnable batch command, when available.

This keeps the dashboard human-sized while making the full-scan scope explicit.
The source command is read-only while planning; it does not call Polygon/Massive,
SEC, Schwab, OpenAI, or broker order endpoints.

Live zero-provider-call verification:

```powershell
.\.venv\Scripts\catalyst-radar.exe dashboard-tui --once --page ops --scan-limit 3 |
  Select-String -Pattern 'Priced-in Source Gaps|Examples are sample tickers|batch <source>|catalyst_events|local_text|options|broker_context'
.\.venv\Scripts\catalyst-radar.exe dashboard-tui --once --page help |
  Select-String -Pattern 'batch <source>|source-gap'
```

Observed:

```text
Examples are sample tickers only. Type `batch <source>` to show the first full-scan batch command and total batch count for that source.
batch <source> | Show first runnable source-gap batch and total batch count.
```

Validation for this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_snapshot_ops_page_shows_priced_in_source_actions tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_batch_command_opens_full_scan_source_batch_plan tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_scan_commands_page_full_scan_rows -q
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_demo_seed_cli.py tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_plans_safe_sync_batches tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_plans_sec_event_batches tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_plans_local_text_batches tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_marks_text_rows_blocked_without_events -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\tui.py tests\integration\test_dashboard_demo_seed_cli.py
git diff --check
```

All passed.

## Latest Full-Scan Event/Text Batch Planning

The user asked again: "Why only these tickers? I want full scan."

The answer is now explicit in CLI/API behavior:

- `priced-in-queue --full-scan --all --json` exports every current ranked row.
- `priced-in-source-batches` plans across every matching source-gap row, then
  shows only the requested batch/page for safe execution.
- The visible tickers in a batch are not the universe. They are batch `N` of the
  full-scan gap set.

Changes in this slice:

- `catalyst-radar run-daily` now accepts repeatable `--ticker`, so the batch
  planner can emit scoped SEC/event-ingest runs:

  ```powershell
  catalyst-radar run-daily --as-of 2026-05-15 --available-at <UTC-now> --ticker BRK.A --ticker NVR --json
  ```

- `catalyst-radar run-textint` now accepts repeatable `--ticker`, so local text
  can be rerun for a full-scan batch of tickers after event text exists.
- `priced-in-source-batches --source catalyst_events` now plans all eligible
  event-gap rows, capped by `CATALYST_SEC_DAILY_MAX_TICKERS`.
- `priced-in-source-batches --source local_text` now plans all eligible text-gap
  rows with no provider calls, but blocks clearly when catalyst event text is
  missing.
- Batch payloads now include:

  ```text
  total_gap_rows
  plannable_gap_rows
  unplannable_gap_rows
  diagnostic.status / reason / blocked_reason / sample_blocked_tickers
  ```

Live zero-provider-call verification:

```powershell
.\.venv\Scripts\catalyst-radar.exe priced-in-queue --full-scan --all --json |
  .\.venv\Scripts\python.exe -c "import json,sys; p=json.load(sys.stdin); print(p['count'], p['total_count'], p['has_more'], p['filters']['status'], p['filters']['limit'], p['external_calls_made'])"
```

Observed:

```text
12087 12087 False all 1000000 0
```

```powershell
.\.venv\Scripts\catalyst-radar.exe priced-in-source-batches --source catalyst_events --batch-limit 2 --json |
  .\.venv\Scripts\python.exe -c "import json,sys; p=json.load(sys.stdin); print(p['status'], p['total_gap_rows'], p['plannable_gap_rows'], p['batch_count'], p['count'], p['diagnostic']['status'], p['external_calls_made']); print(p['batches'][0]['command'] if p['batches'] else 'no-batch')"
```

Observed:

```text
ready 12080 10462 2093 2 eligible 0
catalyst-radar run-daily --as-of 2026-05-15 --available-at <UTC-now> --ticker BRK.A --ticker NVR --ticker ABLVW --ticker DAICW --ticker DFSCW --json
```

```powershell
.\.venv\Scripts\catalyst-radar.exe priced-in-source-batches --source local_text --batch-limit 2 --json |
  .\.venv\Scripts\python.exe -c "import json,sys; p=json.load(sys.stdin); print(p['status'], p['total_gap_rows'], p['plannable_gap_rows'], p['batch_count'], p['count'], p['diagnostic']['status'], p['external_calls_made']); print(p['batches'][0]['command'] if p['batches'] else 'no-batch')"
```

Observed:

```text
blocked 12080 0 0 0 blocked 0
no-batch
```

That local-text result is expected: text analysis is a local processing step,
but it needs catalyst event text first. The useful next action is to fill
eligible `catalyst_events` batches first, then rerun `local_text` batches.

Validation for this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_plans_safe_sync_batches tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_plans_sec_event_batches tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_marks_text_rows_blocked_without_events tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_plans_local_text_batches tests\integration\test_dashboard_data.py::test_priced_in_preflight_payload_reports_exact_next_steps tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_queue_cli_outputs_same_zero_call_signal tests\integration\test_api_routes.py::test_get_radar_priced_in_source_batches_returns_zero_call_plan tests\integration\test_security_boundaries.py::test_openapi_routes_are_allowlisted_and_broker_routes_are_explicit tests\integration\test_text_pipeline.py::test_textint_cli_processes_events_and_prints_features tests\integration\test_jobs.py::test_cli_run_daily_json_smoke -q
.\.venv\Scripts\python.exe -m pytest tests\integration\test_text_pipeline.py::test_textint_cli_processes_events_and_prints_features tests\integration\test_jobs.py::test_cli_run_daily_json_smoke tests\integration\test_jobs.py::test_cli_run_daily_rejects_unsupported_real_llm_and_delivery tests\integration\test_jobs.py::test_scheduler_config_passes_scan_scope_to_daily_spec -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\cli.py tests\integration\test_dashboard_data.py tests\integration\test_text_pipeline.py tests\integration\test_jobs.py
git diff --check
```

All passed so far. Before creating the PR, rerun `git diff --check` after any
last edits and include the PR/merge result here.

## Latest Full-Scan Insights Table

The user asked again: "Why only these tickers? I want full scan."

Live verification showed the backend is already scanning the full latest ranked
universe:

```text
priced_in_queue status=ready count=5 total=12087 offset=0 external_calls=0
scan_scope=scanned=12087 requested=12104 filter=all ranked_after_filter=12087 visible_page=5
headline=Latest full scan ranked 12087 priced-in row(s); showing 1-5 of 12087.
```

And the automation/export path returns every ranked row:

```text
12087 12087 False all 1000000 0
```

The remaining problem was dashboard UX. The Insights table mixed full-scan
ticker rows with summary rows like `UNIVERSE`, `DATA`, alerts, readiness, and
run-plan shortcuts. That made the first visible ticker page feel like a small
watchlist even though it was only page 1 of the full scan.

Changes in this slice:

- The Insights page now renders a dedicated ranked scan table only:

  ```text
  # | Ticker | Signal | Gap | Data gaps | Why now | Next action
  ```

- The overview caption now says the ticker table is paged for human review, not
  reduced to a watchlist.
- The caption also names the full export command:

  ```powershell
  catalyst-radar priced-in-queue --full-scan --all --json
  ```

- Opening a row from Insights now opens that full-scan ticker row directly,
  with a response like:

  ```text
  Opened full-scan row 1 for ACME. Review evidence before any action.
  ```

Live zero-provider-call verification:

```powershell
.\.venv\Scripts\catalyst-radar.exe dashboard-tui --once --page overview --scan-limit 5
.\.venv\Scripts\catalyst-radar.exe priced-in-queue --status all --limit 5
.\.venv\Scripts\catalyst-radar.exe priced-in-queue --full-scan --all --json |
  .\.venv\Scripts\python.exe -c "import json,sys; p=json.load(sys.stdin); print(p['count'], p['total_count'], p['has_more'], p['filters']['status'], p['filters']['limit'], p['external_calls_made'])"
```

Validation for this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_demo_seed_cli.py -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\tui.py tests\integration\test_dashboard_demo_seed_cli.py
git diff --check
```

All passed.

## Latest Preflight Remediation Command Priority

After aligning preflight with source coverage, the rows correctly showed source
gaps but non-batchable sources still pointed to full-scan review commands before
their remediation commands. That was not useful enough.

Change in this slice:

- Preflight source-gap command priority is now:
  1. batch planner command, when available;
  2. source remediation command;
  3. full-scan review command.

Live zero-provider-call verification:

```powershell
.\.venv\Scripts\catalyst-radar.exe priced-in-preflight --json |
  .\.venv\Scripts\python.exe -c "import json,sys; p=json.load(sys.stdin); [print(r['area'], r.get('command')) for r in p['rows'] if r['area'] in {'catalyst_events','local_text','options','broker_context'}]"
```

Observed:

```text
catalyst_events catalyst-radar dashboard-tui --once --page run
broker_context catalyst-radar priced-in-source-batches --source broker_context --batch-limit 5
local_text catalyst-radar run-textint --as-of <LATEST_TRADING_DATE>
options catalyst-radar priced-in-source-batches --source options --batch-limit 5
```

Validation for this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_preflight_payload_reports_exact_next_steps tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_preflight_cli_outputs_zero_call_plan -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py tests\integration\test_dashboard_data.py
git diff --check
```

All passed.

## Latest Preflight Source-Coverage Alignment

`priced-in-preflight` had a misleading read: it reported catalyst events and
broker context as ready/not configured from provider settings, while the actual
priced-in source coverage had large gaps. That could make the dashboard sound
safer than the scan really was.

Changes in this slice:

- `priced_in_preflight_payload()` now includes and consumes priced-in source
  coverage.
- Preflight rows now surface source gaps for:
  - `catalyst_events`
  - `local_text`
  - `options`
  - `broker_context`
- Batchable sources use the batch planner command in preflight:

  ```text
  catalyst-radar priced-in-source-batches --source options --batch-limit 5
  catalyst-radar priced-in-source-batches --source broker_context --batch-limit 5
  ```

Live zero-provider-call verification:

```powershell
.\.venv\Scripts\catalyst-radar.exe priced-in-preflight --json |
  .\.venv\Scripts\python.exe -c "import json,sys; p=json.load(sys.stdin); print(p['status'], p['headline']); [print(r['area'], r['status'], r['finding'], r.get('command')) for r in p['rows'] if r['area'] in {'catalyst_events','local_text','options','broker_context'}]"
```

Observed:

```text
attention 5 prerequisite(s) need attention before trusting output.
catalyst_events attention Priced-in source coverage is 7/12087 (0.1%); gap rows=12080. catalyst-radar priced-in-queue --full-scan --source-gap catalyst_events --limit 50
broker_context attention Priced-in source coverage is 5/12087 (0.0%); gap rows=12082. catalyst-radar priced-in-source-batches --source broker_context --batch-limit 5
local_text attention Priced-in source coverage is 7/12087 (0.1%); gap rows=12080. catalyst-radar priced-in-queue --full-scan --source-gap local_text --limit 50
options attention Priced-in source coverage is 0/12087 (0.0%); gap rows=12087. catalyst-radar priced-in-source-batches --source options --batch-limit 5
```

Validation for this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_preflight_payload_reports_exact_next_steps tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_preflight_cli_outputs_zero_call_plan tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_snapshot_cli_outputs_dashboard_command_center_json -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py tests\integration\test_dashboard_data.py
git diff --check
```

All passed.

## Latest Agent Brief Priced-In Context

The deterministic and real Agents SDK path used an allowlisted, redacted
operator snapshot, but that snapshot did not include the full-market priced-in
queue. That meant the agent brief could reason over readiness, candidates,
alerts, broker state, and call plan, but not the core current goal: "which
stocks look not fully priced in by market emotion versus price reaction?"

Changes in this slice:

- The agent-safe redacted snapshot now includes a `priced_in` section with:
  - queue status/headline/next action/counts;
  - scan totals;
  - filters;
  - status/usefulness counts;
  - weak source coverage and source batch/export commands;
  - top priced-in rows with ticker, status, direction, gap, score, usefulness,
    and redacted source summaries.
- The deterministic dry-run agent brief now emits a priced-in scan insight.
- The same redacted snapshot is the input to real OpenAI Agents SDK mode, so
  real mode sees the full-scan context without receiving raw provider payloads,
  secrets, or tool access.

Live zero-provider-call verification:

```powershell
.\.venv\Scripts\catalyst-radar.exe agent-brief --json |
  .\.venv\Scripts\python.exe -c "import json,sys; p=json.load(sys.stdin); print(p['mode'], p['status'], p['external_calls_made']); print([x for x in p['insights'] if 'Priced-in' in x][0])"
```

Observed:

```text
dry_run dry_run {'broker': 0, 'market_data': 0, 'openai': 0}
Priced-in scan is ready: Latest full scan ranked 12087 priced-in row(s); showing 1-50 of 12087.; visible rows=50, total rows=12087, weak sources=options, broker_context, catalyst_events.
```

Validation for this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\unit\test_agent_sdk_orchestrator.py tests\integration\test_dashboard_demo_seed_cli.py::test_agent_brief_cli_outputs_zero_call_dry_run -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\agents\sdk_orchestrator.py tests\unit\test_agent_sdk_orchestrator.py
git diff --check
```

All passed.

## Latest API Full-Scan Export Parity

CLI had `priced-in-queue --full-scan --all --json`, but the API equivalent
still capped `GET /api/radar/priced-in` at 200 rows. That was a CLI/API parity
gap for E2E testing and external dashboard consumers.

Changes in this slice:

- `GET /api/radar/priced-in` now accepts:

  ```text
  all_rows=true
  ```

- With `all_rows=true`, the API uses the same explicit full-export behavior as
  CLI `--all`: `limit=1000000`, `offset=0`, and no provider calls while reading.

Live zero-provider-call verification through FastAPI `TestClient`:

```powershell
@'
from fastapi.testclient import TestClient
from apps.api.main import create_app
client = TestClient(create_app())
response = client.get('/api/radar/priced-in?all_rows=true')
print(response.status_code)
p=response.json()
print(p['count'], p['total_count'], p['has_more'], p['filters']['limit'], p['filters']['offset'])
'@ | .\.venv\Scripts\python.exe -
```

Observed:

```text
200
12087 12087 False 1000000 0
```

Validation for this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_api_routes.py::test_get_radar_priced_in_queue_returns_cli_ready_rows -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\api\routes\radar.py tests\integration\test_api_routes.py
git diff --check
```

All passed.

## Latest Full-Scan Source Batch Planner

The scanner already ranks the full market, but the missing source actions still
had a practical gap: they showed five example tickers and a giant full export,
without a safe plan for filling the source gap across the full scan. That made
full scan feel fake for options and broker context.

Changes in this slice:

- Added a zero-provider-call CLI/API batch planner:

  ```powershell
  .\.venv\Scripts\catalyst-radar.exe priced-in-source-batches --source options --batch-limit 2 --json
  GET /api/radar/priced-in/source-batches?source=options&batch_limit=2
  ```

- The planner returns every matching full-scan source-gap row as Schwab-safe
  batches capped by `SCHWAB_MARKET_SYNC_MAX_TICKERS` / config
  `schwab_market_sync_max_tickers`.
- Source action rows now include:

  ```text
  batch_plan=catalyst-radar priced-in-source-batches --source options --batch-limit 5
  ```

- The Ops dashboard source-gap table now shows `Batch plan` instead of implying
  the five example tickers are the whole sync target.
- The planner is read-only. It produces explicit commands; it does not call
  Schwab, Polygon, SEC, OpenAI, or any provider while planning.

Live zero-provider-call verification:

```powershell
.\.venv\Scripts\catalyst-radar.exe priced-in-source-batches --source options --batch-limit 2 --json |
  .\.venv\Scripts\python.exe -c "import json,sys; p=json.load(sys.stdin); print(p['source'], p['total_gap_rows'], p['batch_count'], p['count'], p['batch_size'], p['batches'][0]['tickers'], p['batches'][0]['command'], p['has_more'])"
.\.venv\Scripts\catalyst-radar.exe dashboard-tui --once --page ops --scan-limit 3 |
  Select-String -Pattern 'Priced-in Source Gaps|Batch plan|priced-in-source-batches|options|broker_context'
```

Observed:

```text
options 12087 2418 2 5 ['A', 'MSFT', 'AAA', 'AAAU', 'AAPL'] catalyst-radar schwab-market-sync --ticker A --ticker MSFT --ticker AAA --ticker AAAU --ticker AAPL True
Ops table shows Batch plan commands for options and broker_context.
```

Validation for this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_queue_payload_paginates_ranked_rows tests\integration\test_dashboard_data.py::test_priced_in_source_gap_batches_payload_plans_safe_sync_batches tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_queue_cli_outputs_same_zero_call_signal tests\integration\test_api_routes.py::test_get_radar_priced_in_source_batches_returns_zero_call_plan tests\integration\test_security_boundaries.py::test_openapi_routes_are_allowlisted_and_broker_routes_are_explicit -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\dashboard\tui.py src\catalyst_radar\cli.py src\catalyst_radar\api\routes\radar.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py tests\integration\test_api_routes.py tests\integration\test_security_boundaries.py
git diff --check
```

All passed.

## Latest Full-Scan Export Clarification

The user asked again: "Why only these tickers? I want full scan."

Live local evidence:

- The priced-in backend is already backed by the latest full ranked universe:

  ```text
  priced-in count=12087 total=12087 has_more=false offset=0 limit=1000000
  ```

- The handful of tickers shown in source-gap actions are examples and safety-capped
  Schwab batch suggestions, not the scan universe.
- The dashboard remains paged because rendering 12k rows in a TUI is not useful
  for human eyes.

Changes in this slice:

- `priced-in-queue` now accepts:

  ```powershell
  --all
  ```

  Use it with `--full-scan --json` to return every ranked row matching the
  current filters in one CLI/API-test-friendly payload:

  ```powershell
  .\.venv\Scripts\catalyst-radar.exe priced-in-queue --full-scan --all --json
  ```

- Source-gap actions now include both:

  ```text
  full_scan_review=catalyst-radar priced-in-queue --full-scan --source-gap <source> --limit 50
  full_scan_export=catalyst-radar priced-in-queue --full-scan --source-gap <source> --all --json
  ```

  This keeps the TUI human-sized while giving tests/automation an exact full-scan
  replacement UI path.

- The API route allowlist test was stale. It now explicitly includes the existing
  read-only priced-in and ops telemetry routes:

  ```text
  GET /api/radar/priced-in
  GET /api/radar/priced-in/preflight
  GET /api/ops/telemetry
  GET /api/ops/telemetry/coverage
  GET /api/ops/telemetry/raw
  ```

Live zero-provider-call verification:

```powershell
.\.venv\Scripts\catalyst-radar.exe priced-in-queue --full-scan --all --json |
  .\.venv\Scripts\python.exe -c "import json,sys; p=json.load(sys.stdin); print(p['count'], p['total_count'], p['has_more'], p['filters']['offset'], p['filters']['limit'])"
.\.venv\Scripts\catalyst-radar.exe priced-in-queue --full-scan --source-gap options --limit 2 |
  Select-String -Pattern 'priced_in_queue|scan_scope|source_actions|full_scan_export|ticker status|^A |^MSFT|more='
```

Observed:

```text
12087 12087 False 0 1000000
priced_in_queue status=ready count=2 total=12087 offset=0 external_calls=0
scan_scope=scanned=12087 requested=12104 filter=all ranked_after_filter=12087 visible_page=2
full_scan_export=catalyst-radar priced-in-queue --full-scan --source-gap options --all --json
more=catalyst-radar priced-in-queue --source-gap options --limit 2 --offset 2
```

Validation for this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_queue_cli_outputs_same_zero_call_signal tests\integration\test_security_boundaries.py::test_openapi_routes_are_allowlisted_and_broker_routes_are_explicit -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\cli.py src\catalyst_radar\dashboard\data.py tests\integration\test_dashboard_demo_seed_cli.py tests\integration\test_security_boundaries.py
git diff --check
```

All passed.

## Latest Dashboard Source-Gap Filter

The full scan is visible and pageable, but the next practical obstacle is
evidence coverage: current live local source coverage still has large gaps for
options, broker context, catalyst events, and local text. The raw
`priced-in-queue` command and API already supported `source_gap`, but the
human dashboard and agent-brief context did not. That made it too hard to ask:
"show me full-scan rows missing options/text/events/broker context."

Changes in this slice:

- Dashboard filters now carry `priced_in_source_gap`.
- `dashboard-snapshot`, `dashboard-tui`, and `agent-brief` accept:

  ```powershell
  --source-gap <source>
  ```

  Repeat or comma-separate values. Aliases include `text -> local_text`,
  `events -> catalyst_events`, `broker/schwab -> broker_context`, and
  `options_flow -> options`.

- The TUI command box now supports:

  ```text
  source-gap options
  source-gap text,events
  source-gap all
  data-gap broker
  ```

- The overview title/caption now names active source-gap filters:

  ```text
  Full-market priced-in queue - showing rows 1-3 of 12087; ...; source gaps options
  Active source gap filter: source gaps options.
  ```

Live zero-provider-call verification:

```powershell
.\.venv\Scripts\catalyst-radar.exe dashboard-snapshot --page overview --source-gap options --scan-limit 3 --json |
  .\.venv\Scripts\python.exe -c "import json,sys; p=json.load(sys.stdin); print(p['controls']['priced_in_source_gap'], p['priced_in_queue']['filters']['source_gap'], p['priced_in_queue']['count'], p['priced_in_queue']['total_count'])"
.\.venv\Scripts\catalyst-radar.exe dashboard-tui --once --page overview --source-gap options --scan-limit 3
```

Observed:

```text
['options'] ['options'] 3 12087
dashboard title: Full-market priced-in queue - showing rows 1-3 of 12087; ...; source gaps options
caption: Active source gap filter: source gaps options.
```

Validation for this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_snapshot_cli_outputs_dashboard_command_center_json tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_tui_once_can_show_full_scan_mode tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_scan_commands_page_full_scan_rows tests\integration\test_dashboard_demo_seed_cli.py::test_agent_brief_cli_outputs_zero_call_dry_run -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\tui.py src\catalyst_radar\cli.py tests\integration\test_dashboard_demo_seed_cli.py
git diff --check
```

All passed.

## Latest Full-Scan Paging Clarification

The latest user complaint was still: "Why only these tickers? I want full
scan." The root issue was no longer the scan backend. The full-scan queue was
already backed by the latest full universe (`12087` ranked rows in the local
Schwab-backed DB), but the dashboard only rendered the first page and did not
make paging obvious enough.

Changes in this slice:

- Dashboard filters now carry `priced_in_limit` and `priced_in_offset`.
- `dashboard-snapshot`, `dashboard-tui`, and `agent-brief` accept:

  ```powershell
  --scan-limit <1-200>
  --scan-offset <zero-based-row-offset>
  ```

- The TUI command box now supports:

  ```text
  next
  prev
  offset <1-based-row>
  limit <1-200>
  ```

- The overview title and caption now say row ranges, for example:

  ```text
  Full-market priced-in queue - showing rows 6-10 of 12087
  This page shows rows 6-10: 5 visible rows from 12087 latest-scan rows.
  ```

- The full-scan coverage row now opens Ops coverage instead of the legacy
  candidate-state table. The ranked full-scan rows stay on Insights. This
  avoids implying that the `Candidates` page is the whole market scan.

Live zero-provider-call verification:

```powershell
.\.venv\Scripts\catalyst-radar.exe dashboard-tui --once --page overview --scan-limit 5 --scan-offset 5
.\.venv\Scripts\catalyst-radar.exe dashboard-snapshot --page overview --scan-limit 5 --scan-offset 5 --json |
  .\.venv\Scripts\python.exe -c "import json,sys; p=json.load(sys.stdin); print(p['controls']['priced_in_limit'], p['controls']['priced_in_offset'], p['priced_in_queue']['filters']['limit'], p['priced_in_queue']['filters']['offset'], p['priced_in_queue']['count'], p['priced_in_queue']['total_count'])"
.\.venv\Scripts\catalyst-radar.exe priced-in-queue --full-scan --limit 5 --offset 5
```

Observed:

```text
dashboard title: Full-market priced-in queue - showing rows 6-10 of 12087
snapshot controls/filter/counts: 5 5 5 5 5 12087
priced-in queue headline: Latest full scan ranked 12087 priced-in row(s); showing 6-10 of 12087.
```

Validation for this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_snapshot_cli_outputs_dashboard_command_center_json tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_snapshot_cli_outputs_human_readable_zero_call_summary tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_tui_once_can_show_full_scan_mode tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_scan_commands_page_full_scan_rows tests\integration\test_dashboard_demo_seed_cli.py::test_modern_dashboard_tui_supports_mouse_navigation -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\tui.py src\catalyst_radar\cli.py tests\integration\test_dashboard_demo_seed_cli.py
git diff --check
```

All passed.

## Latest Run Freshness Cutoff Fix

The live full-scan queue had a contradictory freshness read:

- preflight said run-as-of bars covered `12090/12613` securities for
  `2026-05-15`;
- the same queue payload's `scan.freshness` said
  `latest_daily_bar_date=2026-05-08` and
  `latest_bars_older_than_as_of=true`.

Root cause: `radar_discovery_snapshot_payload()` loaded ops/database health at
the run `decision_available_at` timestamp. In the real run, grouped bars and
candidate artifacts were written seconds after that timestamp, so database
health was looking too early. The discovery snapshot already had an
`artifact_cutoff` (`finished_at` when present, else decision cutoff), and now
uses that cutoff for `load_ops_health()`.

This is important for the actual goal: the priced-in read compares market
emotion to price reaction, so the dashboard must not claim stale bars when the
latest run actually wrote valid run-as-of bars.

Live zero-provider-call verification after the fix:

```powershell
.\.venv\Scripts\catalyst-radar.exe priced-in-queue --full-scan --limit 1 --json |
  .\.venv\Scripts\python.exe -c "import json,sys; p=json.load(sys.stdin); print(p['scan']['freshness'])"
```

Observed:

```text
latest_daily_bar_date: 2026-05-15
latest_bars_older_than_as_of: False
active_security_with_as_of_bar_count: 12090
missing_as_of_daily_bar_count: 523
```

Regression coverage added:

```text
test_radar_discovery_snapshot_uses_finished_at_for_run_bar_freshness
```

Validation for this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_radar_discovery_snapshot_uses_finished_at_for_run_bar_freshness tests\integration\test_dashboard_data.py::test_radar_discovery_snapshot_flags_stale_bars_and_empty_packets tests\integration\test_dashboard_data.py::test_radar_discovery_snapshot_flags_incomplete_latest_bar_coverage tests\integration\test_dashboard_data.py::test_priced_in_queue_payload_paginates_ranked_rows tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_queue_cli_outputs_same_zero_call_signal -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py
git diff --check
```

All passed.

## Latest Full-Scan vs Example-Ticker Clarification

The user asked: "Why only these tickers? I want full scan."

The answer is now visible in the CLI/API/TUI instead of living only in this
handoff:

- The priced-in queue is still backed by the full latest scan. Current live
  local smoke:

  ```text
  scan_scope=scanned=12087 requested=12104 filter=all ranked_after_filter=12087 visible_page=3
  headline=Latest full scan ranked 12087 priced-in row(s); showing 1-3 of 12087.
  ```

- `priced-in-queue --full-scan` is now an explicit alias for the full ranked
  scan view.
- `priced-in-queue --mismatches` / `--actionable` is now an explicit alias for
  the actionable mismatch filter from that same full scan.
- Source-gap ticker lists are no longer printed as ambiguous `sample=...`.
  They now print as `examples=...`, include `gap_rows=<n>`, and include a
  `sample_scope=...` explanation.
- Every non-ready source action now includes a no-provider-call review command:

  ```text
  full_scan_review=catalyst-radar priced-in-queue --full-scan --source-gap options --limit 50
  ```

  This is for paging through full-scan source gaps. It is not a provider sync.

- The TUI Ops page now labels those rows as `Gap rows` and `Examples`, not
  `Sample`, so the table reads as full-scan coverage plus example tickers.
- The Schwab example batch was capped to 5 tickers so the generated
  `catalyst-radar schwab-market-sync ...` command matches the default
  Schwab market-sync safety cap instead of producing a too-large command.

Current live zero-provider-call smokes:

```powershell
.\.venv\Scripts\catalyst-radar.exe priced-in-queue --full-scan --limit 3
.\.venv\Scripts\catalyst-radar.exe priced-in-queue --mismatches --limit 3
.\.venv\Scripts\catalyst-radar.exe dashboard-snapshot --page ops
```

Observed:

```text
full scan:
scan_scope=scanned=12087 requested=12104 filter=all ranked_after_filter=12087 visible_page=3
source_actions:
- options ... gap_rows=12087 ... examples=A,MSFT,AAA,AAAU,AAPL
  sample_scope=These are the first 5 of 12087 missing/stale row(s) in the current filtered scan; use full_scan_gap_review_command to page through the full scan.
  full_scan_review=catalyst-radar priced-in-queue --full-scan --source-gap options --limit 50

mismatches:
scan_scope=scanned=12087 requested=12104 filter=actionable ranked_after_filter=7 visible_page=3
source_actions:
- options ... gap_rows=7 ... examples=A,MSFT,AAA,AAAU,AAPL
  sample_scope=These are the first 5 of 7 missing/stale row(s) in the current filtered scan; use full_scan_gap_review_command to page through the full scan.
```

Also added an options-gap diagnostic to the priced-in source coverage payload.
This is still zero-call and only inspects stored `option_features`. It explains
why options remain a gap:

```text
diagnostic=missing=12087; newer_than_scan=5; after_cutoff=0; no_stored_options=12082; eligible_but_missing=0
```

This matters because current Schwab option chains should not be silently used as
Friday score input. The system can now distinguish "we have no stored options"
from "we have options, but only after this scan date."

Validation for this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_queue_payload_paginates_ranked_rows tests\integration\test_dashboard_data.py::test_priced_in_queue_payload_diagnoses_options_after_scan_date tests\integration\test_dashboard_data.py::test_priced_in_queue_payload_filters_source_gaps tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_snapshot_cli_outputs_dashboard_command_center_json tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_snapshot_ops_page_shows_priced_in_source_actions tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_queue_cli_outputs_same_zero_call_signal tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_tui_once_can_show_full_scan_mode -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\dashboard\tui.py src\catalyst_radar\cli.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py
git diff --check
```

All passed.

## Latest Point-In-Time Options Guidance

The options source-gap wording is now explicit about point-in-time boundaries.
The previous text could imply that a current Schwab option-chain sync would fill
the options score for an older scan date. The new guidance says:

```text
Use point-in-time options for the scan date; for a current scan, sync Schwab
option-chain context, then rerun.
```

The source-action boundary also states that current option chains must not be
used as score input for older scan dates. This affects the CLI/API source
coverage payload, candidate detail source actions, and the dashboard evidence
gap row because all three read the same guidance helpers.

This does not change scoring. It prevents the next action from sending the
operator into a live Schwab sync that cannot legitimately repair a prior
point-in-time scan.

## Latest Dashboard Evidence-Gap First Row

The Insights dashboard already had a `DATA / Source coverage` row, but it was
rendered after all candidate rows and hidden by the 20-row terminal limit. That
made the dashboard look like a ticker list instead of a market-insight control
surface.

The dashboard now shows evidence coverage immediately after the full-scan row:

```text
UNIVERSE | Full scan coverage | ...
DATA     | Evidence gaps      | bar coverage ...; options missing ...
```

The evidence row uses the existing priced-in source coverage payload and points
the next action at the weakest source in `weak_sources`, so the current real
dashboard points at options instead of burying that gap on Ops:

```text
DATA | Evidence gaps | ... | Sync Schwab option-chain context ...
```

This is a dashboard-only clarity slice. It makes 0 provider calls and does not
change scoring or point-in-time option semantics.

Validation:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_tui_once_can_show_full_scan_mode -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\tui.py tests\integration\test_dashboard_demo_seed_cli.py
.\.venv\Scripts\catalyst-radar.exe dashboard-tui --once --page overview
```

All passed. The real TUI smoke showed `DATA | Evidence gaps` as row 2.

## Latest Full-Scan Queue Clarification + Stored Schwab Context

The latest user confusion was again: "Why only these tickers? I want full scan."
The answer is now both product-visible and technically truer:

- `priced-in-queue --status all` is the full ranked scan view. It reported
  `scanned=12087 requested=12104 ranked_after_filter=12087 visible_page=5`.
- `priced-in-queue --status actionable` is only the actionable mismatch filter
  from that same full scan. It reported
  `scanned=12087 requested=12104 ranked_after_filter=7 visible_page=7`.
- Source-action sample tickers are not the scan universe. They are the first
  missing-source examples for the current filtered queue so the operator can
  run a safe batch action without dumping 12k tickers into Schwab.
- The queue now accepts `--available-at <ISO>` so a fresh local candidate cutoff
  can be inspected directly after read-only context sync or a rerun.
- The API equivalent accepts `available_at=...` on
  `GET /api/radar/priced-in`.
- The TUI dashboard passes its `available-at` filter into the same queue helper.
- Stored Schwab market snapshots are now used by both the priced-in queue and
  ticker detail. The old queue path used persisted scan source fields and could
  still say `broker_context` was missing even after `schwab-market-sync`
  succeeded.
- The market-context extraction bug was that `market_context` is a list, but
  several dashboard paths were reading it through a mapping-only helper. Those
  paths now use a list-aware accessor.

Current real zero-provider-call smokes after this change:

```powershell
.\.venv\Scripts\catalyst-radar.exe priced-in-queue --status all --limit 5
.\.venv\Scripts\catalyst-radar.exe priced-in-queue --status actionable --limit 20
.\.venv\Scripts\catalyst-radar.exe priced-in-preflight --json
.\.venv\Scripts\catalyst-radar.exe priced-in-queue --available-at 2026-05-18T09:02:26+00:00 --usefulness research_useful --source-gap broker_context --limit 10
```

Observed:

```text
status all:
scan_scope=scanned=12087 requested=12104 filter=all ranked_after_filter=12087 visible_page=5
source_coverage=market_bars 12087/12087; ... broker_context 5/12087 (12082 missing)

status actionable:
scan_scope=scanned=12087 requested=12104 filter=actionable ranked_after_filter=7 visible_page=7
source_coverage=market_bars 7/7; catalyst_events 7/7; local_text 7/7; options 0/7 (7 missing); theme_peer_sector 7/7; broker_context 5/7 (2 missing)

broker_context gap with fresh cutoff:
count=0 total=0
```

The remaining actionable source gap is options. Do not blindly mark options
ready from the 2026-05-18 Schwab sync for the 2026-05-15 scan, because
`option_features` are point-in-time and filtered by scan `as_of`. Current
Schwab option-chain context can be shown as broker context, but using it as
Friday score input would be lookahead unless modeled as a separate current
supplemental signal.

Validation for this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_api_routes.py::test_get_radar_priced_in_queue_returns_cli_ready_rows tests\integration\test_dashboard_data.py::test_priced_in_queue_payload_paginates_ranked_rows tests\integration\test_dashboard_data.py::test_priced_in_queue_payload_uses_stored_schwab_market_context tests\integration\test_dashboard_data.py::test_load_ticker_detail_uses_stored_schwab_market_context tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_queue_cli_outputs_same_zero_call_signal -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\api\routes\radar.py src\catalyst_radar\cli.py src\catalyst_radar\dashboard\data.py src\catalyst_radar\dashboard\tui.py tests\integration\test_api_routes.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py
```

Both passed.

## Latest Executable Source-Gap Batch Actions

The source-gap guidance no longer leaves the operator with only
`--ticker <TICKER>` placeholders. Queue-level and candidate-level source actions
now include sample tickers from the current ranked gap set. For Schwab-backed
gaps (`options` and `broker_context`), the command is a directly runnable
read-only market-sync batch.

Current real local zero-provider-call smoke:

```powershell
.\.venv\Scripts\catalyst-radar.exe priced-in-queue --status actionable --limit 20
```

prints:

```text
external_calls=0
scan_scope=scanned=12087 requested=12104 filter=actionable ranked_after_filter=7 visible_page=7
source_actions:
- options ... command=catalyst-radar schwab-market-sync --ticker A --ticker MSFT --ticker AAA --ticker AAAU --ticker AAPL --ticker AA --ticker AAAC sample=A,MSFT,AAA,AAAU,AAPL,AA,AAAC
- broker_context ... command=catalyst-radar schwab-market-sync --ticker A --ticker MSFT --ticker AAA --ticker AAAU --ticker AAPL --ticker AA --ticker AAAC sample=A,MSFT,AAA,AAAU,AAPL,AA,AAAC
```

The Ops dashboard also shows a `Sample` column beside the command, so source-gap
actions are visibly tied to real rows from the current filtered scan instead of
generic placeholders.

Validation for this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_queue_payload_paginates_ranked_rows tests\integration\test_dashboard_data.py::test_priced_in_queue_payload_supports_actionable_status_alias tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_snapshot_cli_outputs_dashboard_command_center_json tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_snapshot_ops_page_shows_priced_in_source_actions tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_queue_cli_outputs_same_zero_call_signal tests\integration\test_dashboard_demo_seed_cli.py::test_candidate_detail_cli_outputs_priced_in_evidence_brief -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\cli.py src\catalyst_radar\dashboard\data.py src\catalyst_radar\dashboard\tui.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py
git diff --check
.\.venv\Scripts\catalyst-radar.exe priced-in-queue --status actionable --limit 20
.\.venv\Scripts\catalyst-radar.exe dashboard-snapshot --page ops
```

## Latest Full-Scan Clarity + Schwab Market CLI

The user asked again: "Why only these tickers? I want full scan." The current
implementation now makes the answer harder to miss:

- The local backing scan is broad: `scanned=12087 requested=12104`.
- `priced-in-queue --status all` is the full ranked scan view. It is paged for
  human use; it does not dump all 12k rows on one screen.
- `priced-in-queue --status actionable` is a filtered mismatch queue from the
  same full scan.
- Source coverage is now computed across the filtered scan result, not just the
  visible page. In full mode the CLI now reports coverage like
  `market_bars 12087/12087`, not `12/12`.
- The filtered/actionable headline now says:
  `Latest full scan found 7 actionable mismatch row(s); showing 1-7 of 7.`

Current real local zero-provider-call evidence:

```powershell
.\.venv\Scripts\catalyst-radar.exe priced-in-queue --status all --limit 12
.\.venv\Scripts\catalyst-radar.exe priced-in-queue --status actionable --limit 20
```

observed:

```text
scan_scope=scanned=12087 requested=12104 filter=all ranked_after_filter=12087 visible_page=12
source_coverage=market_bars 12087/12087; ... options 0/12087 ... broker_context 0/12087 ...

scan_scope=scanned=12087 requested=12104 filter=actionable ranked_after_filter=7 visible_page=7
headline=Latest full scan found 7 actionable mismatch row(s); showing 1-7 of 7.
```

Schwab market context now has a first-class CLI replacement for the raw curl:

```powershell
catalyst-radar schwab-market-sync --ticker <TICKER>
```

The CLI calls the same read-only, rate-limited implementation as
`POST /api/brokers/schwab/market-sync`, and promotes Schwab option-chain
snapshots into `option_features` when options are included. Do not run it during
tests unless an explicit live Schwab call is intended.

Validation for this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_broker_api_routes.py::test_schwab_market_sync_cli_uses_read_only_market_sync tests\integration\test_broker_api_routes.py::test_schwab_market_sync_returns_429_on_repeated_attempt_without_second_schwab_call tests\integration\test_dashboard_data.py::test_priced_in_queue_payload_surfaces_ranked_gap_rows tests\integration\test_dashboard_data.py::test_priced_in_queue_payload_paginates_ranked_rows tests\integration\test_dashboard_data.py::test_priced_in_queue_payload_supports_actionable_status_alias tests\integration\test_dashboard_data.py::test_priced_in_queue_payload_filters_decision_gaps tests\integration\test_dashboard_data.py::test_priced_in_queue_candidate_packet_gap_uses_artifacts tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_snapshot_cli_outputs_dashboard_command_center_json tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_snapshot_ops_page_shows_priced_in_source_actions tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_queue_cli_outputs_same_zero_call_signal -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\cli.py src\catalyst_radar\dashboard\data.py tests\integration\test_broker_api_routes.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py
git diff --check
.\.venv\Scripts\catalyst-radar.exe schwab-market-sync --help
.\.venv\Scripts\catalyst-radar.exe priced-in-queue --status all --limit 12
.\.venv\Scripts\catalyst-radar.exe priced-in-queue --status actionable --limit 20
```

## Latest Broker-Context Guidance Fix

The priced-in queue's `broker_context` source is based on stored Schwab market
snapshots, not the portfolio-only Schwab sync. The source-gap action now points
to the correct read-only market-sync endpoint:

```text
command=curl.exe --insecure --fail --silent --show-error --request POST https://127.0.0.1:8443/api/brokers/schwab/market-sync --header "Content-Type: application/json" --data '{"tickers":["<TICKER>"],"include_history":true,"include_options":true}'
api=POST /api/brokers/schwab/market-sync
```

This endpoint is explicit, read-only, rate-limited, and does not submit orders.
Because PR #230 promotes Schwab option-chain snapshots into `option_features`,
this one market-sync path can now help close both `broker_context` and
`options` source gaps after a rerun.

Current local source-gap smoke:

```powershell
catalyst-radar priced-in-queue --usefulness research_useful --source-gap broker_context --limit 3
```

prints:

```text
broker_context status=missing ... command=.../api/brokers/schwab/market-sync ... "include_options":true
```

Verification:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_demo_seed_cli.py::test_dashboard_snapshot_cli_outputs_dashboard_command_center_json tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_queue_cli_outputs_same_zero_call_signal tests\integration\test_dashboard_demo_seed_cli.py::test_candidate_detail_cli_outputs_priced_in_evidence_brief tests\integration\test_dashboard_data.py::test_load_ticker_detail_returns_candidate_packet_card_events_and_validation -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\dashboard\tui.py tests\integration\test_dashboard_demo_seed_cli.py tests\integration\test_dashboard_data.py
```

## Latest Decision Artifact Command Hints

The priced-in queue now turns missing decision artifacts into executable CLI
next steps. This matters because the current research-useful queue has
actionable signal evidence, but not every row has the Candidate Packet or
Decision Card needed for a human decision review.

Current local smoke for rows missing Candidate Packets:

```powershell
catalyst-radar priced-in-queue --usefulness research_useful --decision-gap candidate_packet --limit 4
```

prints rows such as:

```text
MSFT ... Build a Candidate Packet before Decision Card review. command=catalyst-radar build-packets --as-of 2026-05-15 --ticker MSFT --min-state AddToWatchlist
```

Current local smoke for rows missing Decision Cards:

```powershell
catalyst-radar priced-in-queue --usefulness research_useful --decision-gap decision_card --limit 4
```

prints:

```text
A ... Build or refresh the Decision Card before decision review. command=catalyst-radar build-decision-cards --as-of 2026-05-15 --ticker A --min-state AddToWatchlist
```

`build-decision-cards` now accepts `--min-state`, defaulting to `Warning` for
the old behavior. Use `--min-state AddToWatchlist` only when intentionally
building decision artifacts for research-useful watchlist rows.

Verification for this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_candidate_packets_cli.py tests\integration\test_dashboard_data.py::test_priced_in_queue_payload_filters_decision_gaps tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_queue_cli_outputs_same_zero_call_signal -q
.\.venv\Scripts\python.exe -m ruff check src tests
git diff --check
```

## Latest Schwab Options Evidence Bridge

The current actionable priced-in queue still has an options evidence gap. The
gap is now actionable instead of a dead end:

- Schwab `/api/brokers/schwab/market-sync` already fetches quote, history, and
  option-chain context when explicitly called.
- The market-sync API now promotes aggregate Schwab option-chain metrics into
  `option_features` via provider `schwab_option_chain`.
- The zero-call CLI path can promote already stored Schwab market snapshots:

```powershell
catalyst-radar ingest-options --from-schwab-market --ticker A
```

Current local smoke:

```text
ingested provider=schwab_option_chain raw=0 normalized=0 option_features=0 rejected=0
```

That means no stored Schwab market snapshot exists for `A` yet; the command made
no Schwab, Polygon, SEC, OpenAI, or broker-order call. To populate it for real,
the operator must explicitly run the rate-limited Schwab market-sync endpoint
for a small ticker batch, then rerun the scan so option features can enter the
priced-in score.

The priced-in source-gap guidance now says:

```text
Sync Schwab option-chain context or ingest an options fixture, then rerun the scan.
command=catalyst-radar ingest-options --from-schwab-market --ticker <TICKER>
api=POST /api/brokers/schwab/market-sync
```

Verification for this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_options_ingest.py tests\integration\test_broker_interactive_workflows.py tests\integration\test_broker_api_routes.py tests\integration\test_dashboard_demo_seed_cli.py tests\integration\test_api_routes.py -q
.\.venv\Scripts\python.exe -m ruff check src tests
git diff --check
.\.venv\Scripts\catalyst-radar.exe ingest-options --from-schwab-market --ticker A
.\.venv\Scripts\catalyst-radar.exe priced-in-queue --status actionable --source-gap options --limit 5
```

## Latest Full-Scan Scope Clarification

The user asked again: "Why only these tickers? I want full scan." The current
local answer is:

- Full scan is running over the local latest universe.
- The queue/table is a ranked, paged view of that scan, not a claim that only
  the visible tickers were scanned.
- `status=actionable` intentionally narrows the full scan to bullish/bearish
  not-priced-in mismatches.

Current real local evidence:

```powershell
catalyst-radar priced-in-queue --status all --limit 10
```

prints:

```text
priced_in_queue status=ready count=10 total=12087 offset=0 external_calls=0
scan_scope=scanned=12087 requested=12104 filter=all ranked_after_filter=12087 visible_page=10
```

The first visible tickers are `A,MSFT,AAA,AAAU,AAPL,AA,AAAC,BRK.A,NVR,ABLVW`
because they are the top ranked page, not because the scanner only covered
those tickers.

The actionable filter:

```powershell
catalyst-radar priced-in-queue --status actionable --limit 20
```

prints:

```text
scan_scope=scanned=12087 requested=12104 filter=actionable ranked_after_filter=7 visible_page=7
```

and returns the short mismatch list `A,MSFT,AAA,AAAU,AAPL,AA,AAAC`.

This slice also fixes a correctness gap in decision-gap filtering:
`priced_in_queue_payload` now loads candidate rows with artifacts, so
`--decision-gap candidate_packet` reflects real Candidate Packets. After a
local zero-provider-call packet build for `A`, the candidate-packet gap no
longer lists `A`; the current real local gap rows are `MSFT,AAAU,AAPL,AA`.

Regression coverage:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\integration\test_dashboard_data.py::test_priced_in_queue_payload_filters_decision_gaps tests\integration\test_dashboard_data.py::test_priced_in_queue_candidate_packet_gap_uses_artifacts tests\integration\test_dashboard_demo_seed_cli.py::test_priced_in_queue_cli_outputs_same_zero_call_signal -q
.\.venv\Scripts\python.exe -m ruff check src\catalyst_radar\cli.py src\catalyst_radar\dashboard\data.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py
```

Both passed before the broader validation run.

## Latest Candidate-Packet Decision Gap

The top research-useful mismatches can be priced-in useful before they have a
Candidate Packet. That made the old "missing Decision Card" guidance one step
too late. The usefulness verdict now adds `candidate_packet` to
`missing_for_decision` when no packet exists, and the row next action says:

```text
Build a Candidate Packet before Decision Card review.
```

Updated decision-gap workflow:

```powershell
catalyst-radar priced-in-queue --usefulness research_useful --decision-gap candidate_packet
catalyst-radar priced-in-queue --usefulness research_useful --decision-gap candidate_packet,decision_card
```

Decision-gap aliases include `packet`, `candidate-packet`, and
`candidate_packets` for `candidate_packet`.

Current local evidence before this fix: top research-useful rows `A`, `MSFT`,
`AAAU`, `AAPL`, and `AA` had candidate states but no candidate packets and no
decision cards. Running `candidate-packet --ticker A --as-of 2026-05-15 --json`
returned "candidate packet not found", so the dashboard needed to expose packet
generation as the first missing decision artifact.

## Latest Decision-Gap Filter

The priced-in queue now separates source availability gaps from decision
readiness gaps. This answers "which research-useful rows still cannot become
decision-useful, and why?" without opening every ticker.

New CLI/API affordances:

```powershell
catalyst-radar priced-in-queue --usefulness research_useful --decision-gap decision_card
catalyst-radar priced-in-queue --status actionable --decision-gap decision_card,options --json
catalyst-radar dashboard-snapshot --usefulness research_useful --decision-gap decision_card --json
```

API equivalent:

```text
GET /api/radar/priced-in?usefulness=research_useful&decision_gap=decision_card
GET /api/radar/priced-in?status=actionable&decision_gap=decision_card,options
```

Supported decision-gap names currently include:

- `candidate_packet`
- `decision_card`
- `options`
- `broker_context`

Aliases include `packet`/`candidate-packet` for `candidate_packet`,
`card`/`decision-cards` for `decision_card`, and `broker`/`schwab`/`portfolio`
for `broker_context`. Multiple decision gaps are ANDed.
`decision_card,options` means the row is missing both.

The TUI command box also supports:

```text
decision-gap decision_card
usefulness research_useful
decision-gap all
```

This is zero-call filtering over stored queue rows. It does not build a
Decision Card, sync Schwab, call Polygon/Massive, call SEC, call OpenAI, or
submit orders.

Dashboard snapshots and the modern TUI also accept `--usefulness`, so the
human surface can narrow from full scan to "research-useful rows missing a
Decision Card" without dropping to the standalone queue command.

## Latest Full-Scan Default

The dashboard now starts from the product goal: full-market scan first, then
filters for narrower action queues.

What changed:

- `dashboard-tui`, `dashboard-snapshot`, and `agent-brief` default to
  `--scan-mode all`.
- `DashboardFilters()` defaults to `priced_in_status="all"`.
- Blank/invalid scan-mode normalization now falls back to `all`.
- The existing `M` key, sidebar `SCAN` controls, and commands still switch to
  `Mismatches` when the operator wants only bullish/bearish not-priced-in rows.

Current operator meaning:

- The first Insights screen should show the first ranked page from the whole
  latest priced-in scan.
- The small mismatch queue is a deliberate filter, not the default dashboard.
- Use `--scan-mode mismatches` only when intentionally narrowing to rows where
  market emotion appears ahead of price reaction.

Zero-call checks for this default:

```powershell
catalyst-radar dashboard-snapshot --json
catalyst-radar dashboard-tui --once --page overview
catalyst-radar dashboard-tui --once --scan-mode mismatches --page overview
```

Current local smoke after the change:

- Default snapshot: `control=all`, `queue_status=all`,
  `total=12087`, `returned=50`, `scan=12087`.
- Default TUI overview title:
  `Full-market priced-in queue - showing 50 of 12087; research 5 / blocked 7920 / monitor 4162`.
- Explicit mismatch snapshot: `control=actionable`, `queue_status=actionable`,
  `total=7`, `returned=7`, `scan=12087`.

## Latest Source-Gap Filter

The priced-in queue can now answer "which useful rows are missing which data
layer?" directly.

New CLI/API affordances:

```powershell
catalyst-radar priced-in-queue --status actionable --source-gap options --limit 3
catalyst-radar priced-in-queue --status actionable --source-gap options,broker_context --usefulness research_useful --json
```

API equivalent:

```text
GET /api/radar/priced-in?status=actionable&source_gap=options
GET /api/radar/priced-in?status=actionable&source_gap=options,broker_context&usefulness=research_useful
```

Supported source-gap names are the same priced-in source classes:

- `market_bars`
- `catalyst_events`
- `local_text`
- `options`
- `theme_peer_sector`
- `broker_context`

Semantics:

- `source_gap` matches rows where the named source is missing or stale.
- Multiple source gaps are ANDed. `options,broker_context` means both are
  unavailable for the row.
- The human CLI `more=` continuation preserves `--status`, `--usefulness`,
  `--source-gap`, and `--min-gap`.

Current real local smoke:

```powershell
catalyst-radar priced-in-queue --status actionable --source-gap options --limit 3
```

reported `total=7`, `count=3`, and
`usefulness_counts=blocked:2,research_useful:5`. The first rows were A, MSFT,
and AAA, all missing options.

```powershell
catalyst-radar priced-in-queue --status actionable --source-gap options,broker_context --usefulness research_useful --limit 10 --json
```

reported `source_gap=options,broker_context`, `usefulness=research_useful`,
`total=5`, and `usefulness_counts={"research_useful":5}`.

Verification run for this slice:

```powershell
python -m pytest tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py tests\integration\test_api_routes.py -q
python -m ruff check src\catalyst_radar\dashboard\data.py src\catalyst_radar\api\routes\radar.py src\catalyst_radar\cli.py tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py tests\integration\test_api_routes.py
git diff --check
```

All three passed locally before commit.

## Latest Usefulness Filter

The priced-in queue usefulness verdict is now queryable instead of only visible
row-by-row.

New CLI/API affordances:

```powershell
catalyst-radar priced-in-queue --status actionable --usefulness research_useful --limit 3
catalyst-radar priced-in-queue --status actionable --usefulness blocked --limit 3
catalyst-radar priced-in-queue --status actionable --usefulness useful --json
```

API equivalent:

```text
GET /api/radar/priced-in?status=actionable&usefulness=research_useful
GET /api/radar/priced-in?status=actionable&usefulness=blocked
```

Supported usefulness filters:

- `useful`: `research_useful` or `decision_useful`
- `research_useful`
- `decision_useful`
- `blocked`
- `monitor_only`
- `not_useful`

The payload now includes:

- `filters.usefulness`
- `usefulness_counts`

The human CLI prints `usefulness_counts=...`, and the `more=` continuation
command preserves `--status`, `--usefulness`, and `--min-gap` filters.

Current real local smoke:

```powershell
catalyst-radar priced-in-queue --status actionable --usefulness research_useful --limit 3
```

reported `total=5`, `count=3`, and `usefulness_counts=research_useful:5`.
The first rows were A, MSFT, and AAAU. The `more=` command preserved
`--status actionable --usefulness research_useful`.

```powershell
catalyst-radar priced-in-queue --status actionable --usefulness blocked --limit 3
```

reported `total=2`, `count=2`, and `usefulness_counts=blocked:2`.
The rows were AAA and AAAC.

```powershell
catalyst-radar dashboard-tui --once --page overview
```

now titles the default insight page as:

```text
Mismatches from full scan - showing 7 of 7; scan 12087; research 5 / blocked 2
```

This is the current best operator split:

- 5 research-useful mismatches need evidence review and missing decision inputs.
- 2 blocked mismatches need policy/portfolio blockers cleared first.

Verification run for this slice:

```powershell
python -m pytest tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py tests\integration\test_api_routes.py -q
python -m ruff check src tests
git diff --check
```

All three passed locally before commit.

## Latest Full-Scan UI Correction

The latest confusion was "why only these tickers?" The answer is now encoded in
the dashboard itself:

- the scan can be broad;
- the default Insights page shows the smaller actionable mismatch queue;
- `Full Scan` mode shows the first ranked page from the whole scanned universe.

Current local state from fresh smoke:

```powershell
catalyst-radar dashboard-snapshot --scan-mode all --page overview --json
```

reported:

- `controls.priced_in_status=all`
- `priced_in_queue.total_count=12087`
- `priced_in_queue.returned_count=50`
- `priced_in_queue.scan.scanned_candidate_states=12087`
- `priced_in_queue.status_counts.bullish_not_priced_in=7`

The paired mismatch-mode smoke:

```powershell
catalyst-radar dashboard-snapshot --scan-mode mismatches --page overview --json
```

reported:

- `controls.priced_in_status=actionable`
- `priced_in_queue.total_count=7`
- `priced_in_queue.returned_count=7`
- `priced_in_queue.scan.scanned_candidate_states=12087`
- `priced_in_queue.status_counts={"bullish_not_priced_in":7}`

The TUI now has explicit `SCAN` sidebar controls:

- `M  Mismatches only`: only bullish/bearish not-priced-in rows.
- `ALL Full scan rows`: first ranked page of all scanned rows, including
  neutral and blocked rows.

Keyboard and command affordances:

- Press `M` in the modern TUI to toggle Mismatches/Full Scan.
- Type `full`, `all`, or `scan all` in the command box for full scan rows.
- Type `mismatches`, `mismatch`, `actionable`, or `scan actionable` for the
  short action queue.

Non-interactive checks:

```powershell
catalyst-radar dashboard-tui --once --scan-mode all --page overview
catalyst-radar dashboard-snapshot --scan-mode all --page overview --json
```

Important semantics:

- A full scan does not mean "show 12k rows on the first screen." It means the
  backing scan covered the broad local universe, and the UI can page/filter the
  resulting ranked queue.
- The actionable mismatch queue is intentionally small; it is the set of names
  where market emotion appears ahead of price reaction.
- The current real local full-scan page begins with the universe coverage row,
  then A, MSFT, AAA, AAAU, AAPL, AA, AAAC, BRK.A, NVR, and ABLVW because those
  are the top ranked rows after sorting, not because the scanner only looked at
  those tickers.

Verification run for this slice:

```powershell
python -m pytest tests\integration\test_dashboard_data.py tests\integration\test_dashboard_demo_seed_cli.py tests\integration\test_api_routes.py -q
python -m ruff check src tests
git diff --check
```

All three passed locally before commit.

## Latest Usefulness Verdict

The priced-in queue and candidate detail now carry an explicit usefulness
verdict. This is the operator-facing answer to "can I use this signal right
now?":

- `research_useful`: core emotion-versus-reaction evidence is present, but at
  least one decision-supporting input is missing.
- `decision_useful`: no source gaps and a Decision Card is available; still
  requires human review and real order submission remains disabled.
- `blocked`: the priced-in mismatch exists but policy/portfolio blockers must
  be cleared first.
- `monitor_only`: no bullish or bearish not-priced-in mismatch is visible.
- `not_useful`: core source data such as market bars, catalyst events, or local
  text is missing or stale.

The verdict is in:

- `priced-in-queue` row field: `usefulness`
- `priced_in_evidence_brief.usefulness`
- CLI output for `priced-in-queue`
- CLI output for `candidate-detail <ticker>`
- TUI candidate detail row: `Usefulness`

Current local smoke:

```powershell
catalyst-radar priced-in-queue --status actionable --limit 3
catalyst-radar candidate-detail A
catalyst-radar dashboard-tui --once --page candidate:A
```

Observed state: `A` and `MSFT` are `research_useful`; `AAA` is `blocked`.
Candidate `A` is not `decision_ready` because options, broker context, and a
Decision Card are missing. This is intentional: the scanner can surface a
research-useful priced-in mismatch without pretending it is trade-ready.

## Latest Candidate Source-Gap Detail

Candidate detail now carries the same source-action contract as the priced-in
queue. `priced_in_evidence_brief.source_actions` is a per-ticker list covering
`market_bars`, `catalyst_events`, `local_text`, `options`,
`theme_peer_sector`, and `broker_context`.

Use these zero-call checks:

```powershell
catalyst-radar candidate-detail A
catalyst-radar candidate-detail A --json
catalyst-radar dashboard-tui --once --page candidate:A
```

The human CLI output now prints non-ready `source_actions:` directly under the
candidate's data coverage. The TUI candidate page shows a `Source gaps` row.
For the current local full-market queue, candidate `A` shows:

- signal: `bullish_not_priced_in`
- available sources: market bars, catalyst events, local text, theme/peer/sector
- missing sources: options, broker context
- options action: treat options as absent until an options feed or local fixture
  is ingested
- broker action: sync read-only Schwab context before sizing or portfolio
  review

This is intentionally candidate-specific. The queue-level `source_coverage`
answers "what is weak across the visible queue?" Candidate detail answers "what
is weak for this ticker before I act?"

## Latest Source-Coverage Action Plan

The broad scan now makes missing "all kinds of data" explicit. The
`source_coverage` payload on `priced-in-queue` includes `actions` for every
priced-in source class:

- `market_bars`
- `catalyst_events`
- `local_text`
- `options`
- `theme_peer_sector`
- `broker_context`

Each action row reports `source`, `status`, coverage counts, `coverage_pct`,
`meaning`, `next_action`, `command`, `api`, and `external_call_boundary`. The
CLI prints non-ready source actions under `source_actions:` and the Ops page
shows them in a `Priced-in Source Gaps` table. This keeps the broad-market
queue honest: it can say "these are the actionable mismatches we found" and
"these sources are still missing before treating the evidence as complete."

Current local zero-call smoke against the full-market queue:

```powershell
catalyst-radar priced-in-queue --status actionable --limit 3
catalyst-radar dashboard-tui --once --page ops
```

Observed state:

- Queue: `count=3`, `total=7`, `external_calls=0`, `has_more=true`.
- Source coverage: market bars, catalyst events, local text, and
  theme/peer/sector context are present for the visible queue.
- Weak sources: `options`, `broker_context`.
- Options action: ingest a local options fixture or add an options feed before
  treating options as a supporting signal.
- Broker action: sync read-only Schwab context only before sizing or portfolio
  review; it is not part of signal discovery.

Important distinction: missing broker context should not block discovery of a
priced-in mismatch. It only blocks sizing/exposure confidence. Missing options
means options confirmation is absent, not that the market-bar/event/text signal
is invalid.

## Latest Candidate Detail Correction

The full-market scan queue is intentionally paged. The dashboard should not
pretend the visible rows are the whole market. The current local queue is broad:
`priced-in-queue --status actionable --limit 3 --json` reported
`scanned_candidate_states=12087`, `total_count=7`, `returned_count=3`, and
`has_more=true`. That means the scan covered the broad local universe, then the
operator surface narrowed it to the ranked actionable mismatch queue.

Use this zero-call sequence when the user asks "why only these tickers?":

```powershell
catalyst-radar priced-in-preflight --json
catalyst-radar priced-in-queue --status actionable --limit 50 --offset 0 --json
catalyst-radar priced-in-queue --status all --limit 50 --offset 50 --json
```

`priced-in-preflight` explains whether the latest run is broad enough.
`priced-in-queue` is the paged queue. `status=actionable` shows only bullish or
bearish not-priced-in mismatches. `status=all` inspects the full priced-in scan
slice by slice.

Candidate detail now has a concise evidence brief shared by API, CLI, and TUI:

```powershell
catalyst-radar candidate-detail A
catalyst-radar candidate-detail A --json
catalyst-radar dashboard-tui --once --page candidate:A
```

The API payload from `/api/radar/candidates/{ticker}` includes
`priced_in_evidence_brief` with:

- priced-in status, direction, emotion score, reaction score, gap, and
  priced-in score;
- the "why now" explanation;
- top catalyst/source/source URL;
- source coverage summary;
- blockers, if any;
- top evidence rows;
- the single next operator step.

The TUI candidate detail page now uses the same brief instead of a generic
field dump. It should answer: what signal did the full scan find, why might it
not be priced in, what evidence supports it, what data is missing, and what the
operator should do next. Rendering and detail inspection remain zero-call.

## Latest Queue UX Correction

After the full-market scheduled scan fix, the next source of confusion was the
display layer: CLI/API/TUI surfaces showed a ranked slice of candidates, but
did not make it obvious that the backing scan covered the full local universe.
The priced-in queue now treats this as a first-class pagination contract:

```powershell
catalyst-radar priced-in-queue --limit 50 --offset 0 --json
catalyst-radar priced-in-queue --limit 50 --offset 50
```

The CLI/API payload reports `total_count`, `returned_count`, `offset`,
`has_more`, and `filters.offset` in addition to `count`. `count` is the number
returned on the current page, not the whole scan. The dashboard overview now
uses the same queue metadata and titles the operator surface as, for example,
`Full-market priced-in queue - showing 50 of 12087`. The first coverage row
uses the scheduled run's scan yield (`scanned_candidate_states=12087`) instead
of the old 200-row dashboard display cap.

The CLI/API queue is the exact pagination surface. The TUI overview remains the
human first screen: it shows a fast visible slice and points deeper inspection
to `priced-in-queue --limit/--offset` or `GET /api/radar/priced-in?...&offset=`.

This is still zero-call browsing. Pagination, filtering, TUI rendering, and
JSON export read the local database only.

The latest dashboard focus correction keeps CLI/API full pagination intact, but
changes the TUI overview to default to actionable priced-in mismatches only:

```powershell
catalyst-radar priced-in-queue --status actionable --limit 20 --json
catalyst-radar priced-in-queue --status all --limit 50 --offset 50
```

`status=actionable` is an alias for `bullish_not_priced_in` and
`bearish_not_priced_in`. The TUI overview title now reads like
`Actionable mismatches - showing 7 of 7; scan 12087`, so the first screen is the
human action queue while still proving the broad scan size. If no actionable
mismatch exists, the TUI should not backfill neutral rows; it should say there
are no actionable not-priced-in mismatches and point to `--status all` for full
inspection.

Rows with a not-priced-in signal but a blocked policy state are still shown in
the actionable queue, but they are labeled as `Blocked mismatch`. The CLI also
prints a `blocked` column. The next action for those rows is to clear blockers
before treating the mismatch as actionable; do not present them as ready ideas.

## Latest Correction

The broad Polygon/Massive seed proved that the local database can hold the
full-market scan scope: ticker seeding reached more than 12k active securities
and raw scanning produced more than 12k candidate rows. The remaining bug was
not "Polygon only returned three tickers"; it was that
`scripts\run-full-market-scan.ps1` finished through the raw `scan` command.
The dashboard, `priced-in-queue`, and readiness panels intentionally read the
latest scheduled `run-daily` job metadata, so they kept showing the previous
three-ticker scheduled run.

The full-market helper should therefore seed Polygon/Massive tickers and then
call:

```powershell
catalyst-radar run-daily --as-of <LATEST_TRADING_DATE> --available-at <UTC-now> --json
```

That path ingests the grouped daily bars for the selected trading date, runs the
full active-security feature scan, records scheduler/job telemetry, and refreshes
the dashboard/API queue from the same run. The raw `scan` command remains useful
for local diagnostics, but it is not the operator path for a dashboard-visible
full scan.

Two runtime details matter for broad scans:

- Live data fetched inside a scheduled run must be visible to later steps in
  that same run. Use the run context's post-ingest available-at cutoff for
  feature scanning, packet building, decision cards, and alert planning.
- SQLite may briefly reject heartbeat writes during large candidate-state
  batches. Treat transient `database is locked` heartbeat errors as retryable;
  still fail closed if the heartbeat update returns `false` because the lock was
  actually lost.

Latest verified local full-market state after the fix:

- Active securities: `12613`.
- As-of bar coverage for `2026-05-15`: `12090/12613`; this is broad enough for
  research and should be shown as attention, not a hard block.
- Feature scan: `12087` scanned candidates.
- Candidate states: `12087`.
- Candidate packets: `7920`.
- Planned alerts: `3876`.
- `priced-in-queue` status: `ready`, with top rows including `A`, `MSFT`,
  `AAAU`, `AAPL`, and `AA` as `bullish_not_priced_in`.

## Current Objective

The corrected product goal is: **scan the whole available market to find stocks
where market emotion has not fully priced into the stock price yet**. Everything
else is supporting infrastructure for that goal.

The dashboard is still important, but it is not the product by itself. It must
act as the human control surface for a broad-market priced-in mismatch scanner:

- First prove whether the latest run is actually broad-market, by showing active
  universe size, requested/scanned securities, fresh bar coverage, and candidate
  count.
- Then rank the useful subset by "emotion versus price reaction": emotion score,
  reaction score, emotion-minus-reaction gap, priced-in status, reason, and next
  action.
- Never make a tiny fixture universe look like a full-market scan.
- Never hide provider/broker/OpenAI calls behind navigation, filtering, or row
  opening. Browsing remains zero-call.

The previous near-term operational work was to get out of local/demo-only mode
without forcing a Polygon/Massive API key. The user has now added a key and
switched the local market provider to Polygon, but Polygon must remain optional
for future setup paths.

The user confirmed:

- They initially did not have a Polygon API key, then added one in `.env.local`.
- Polygon.io has rebranded to Massive.com; keep code/provider names as `polygon` for now, but call the provider "Polygon/Massive" in user-facing guidance where clarity matters.
- They were confused by `CATALYST_SEC_USER_AGENT`.
- They filled `CATALYST_SEC_USER_AGENT` in `.env.local`.
- Polygon should still be treated as optional unless the operator explicitly selects it with `CATALYST_DAILY_MARKET_PROVIDER=polygon`.

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

`CATALYST_POLYGON_API_KEY` must remain optional unless the operator explicitly switches `CATALYST_DAILY_MARKET_PROVIDER=polygon`. On 2026-05-17, the local `.env.local` had `CATALYST_DAILY_MARKET_PROVIDER=polygon`, `CATALYST_DAILY_PROVIDER=polygon`, and a configured non-placeholder Polygon/Massive key. Do not print the key.

## Polygon/Massive Verification

Live Polygon/Massive grouped-daily ingest was verified on 2026-05-17 after the user updated `.env.local`:

```powershell
$env:PYTHONPATH='src'
py -m catalyst_radar.cli ingest-polygon grouped-daily --date 2026-05-15
```

Final successful output:

```text
ingested provider=polygon raw=12104 normalized=12104 securities=0 daily_bars=12104 holdings=0 events=0 rejected=0
```

The first live attempt proved the key worked, but exposed two small product bugs:

- Polygon/Massive raw records include both `T` and `t`. That is valid JSON, but PowerShell's `ConvertFrom-Json` treats those as duplicate keys and broke `scripts\market-radar-status.ps1` after incidents included raw provider payloads.
- Some live grouped-daily records omit `vw`. The connector required it and degraded provider health with 84 rejected records.

Current fixes in the working tree:

- `src/catalyst_radar/ops/health.py` makes operator health payload keys safe for case-insensitive PowerShell JSON consumers without changing raw provider storage.
- `src/catalyst_radar/connectors/polygon.py` no longer requires `vw` in grouped-daily raw records; when missing, normalized `vwap` falls back to `close` and metadata marks `vwap_fallback=close`.
- Regression tests were added in `tests/integration/test_ops_health.py` and `tests/integration/test_polygon_ingest_cli.py`.

Verification already run:

```powershell
$env:PYTHONPATH='src'
py -m pytest tests\integration\test_polygon_ingest_cli.py tests\integration\test_ops_health.py -q
py -m ruff check src\catalyst_radar\connectors\polygon.py src\catalyst_radar\ops\health.py tests\integration\test_polygon_ingest_cli.py tests\integration\test_ops_health.py
py -m catalyst_radar.cli dashboard-snapshot --json | ConvertFrom-Json > $null
powershell -ExecutionPolicy Bypass -File scripts\market-radar-status.ps1
py -m catalyst_radar.cli provider-health --provider polygon
```

Final status after restart:

- Polygon provider health: `healthy`.
- Latest daily bar: `2026-05-15`.
- Active market coverage: `active=8`, `with_bars=8`, `with_latest_bar=8`.
- Readiness still says `research_only` because the latest radar run `as_of` is `2026-05-16`, a Saturday, and the freshness gate currently expects bars on the exact run `as_of`. Do not treat this as a Polygon failure. The next useful slice should either run the radar for the latest trading day or teach freshness to use the previous trading session for weekend/non-trading-day runs.

## Definition Of Useful

Keep the usefulness bar explicit and small:

- **Research-useful** means a capped run scans the active local universe, proves
  how much of that universe was actually covered, computes priced-in mismatch
  fields for candidates, uses clearly labeled sources, surfaces candidate
  research/briefs, shows the single next operator action, and makes no hidden
  external calls.
- **Decision-useful** means research-useful plus fresh market bars for the run
  `as_of`, live catalyst input, no blocking run/readiness rows, a Decision Card
  for a manual-review candidate, fresh read-only portfolio context, and order
  submission still disabled.
- **Not useful enough to act** includes stale bars, fixture/CSV market data that
  is older than the run date, a thin universe, missing live credentials, blocked
  run steps, or any unclear provider-call budget.

The current active slice adds deterministic priced-in mismatch scoring directly
to scan output. It does not add a new database table, new provider source, or new
agent loop. It uses existing market reaction fields, local text/event/options
scores, and portfolio/data-staleness gates. The intended metadata shape on each
candidate is:

- `priced_in.status`: `bullish_not_priced_in`, `bearish_not_priced_in`,
  `fully_priced`, `overextended_hype`, `conflicted`, `stale`, `blocked`, or
  `neutral`.
- `priced_in.emotion_score`: source/event/text/options/theme strength.
- `priced_in.reaction_score`: direction-aware price, relative strength, volume,
  and extension reaction.
- `priced_in.emotion_reaction_gap`: emotion minus reaction; positive means the
  catalyst may not be fully priced.
- `priced_in.priced_in_score`: rough percent priced, where low means underpriced
  and high means fully/over-priced.
- `priced_in.reason` and `priced_in.next_step`: the human-readable dashboard
  explanation and operator move.

Full-market scan boundary:

- `catalyst-radar scan --as-of <date>` scans every active security in the local
  database, excluding explicit benchmark ETFs, when no `--universe` filter is
  provided.
- `catalyst-radar scan --as-of <date> --universe <name>` scans only that named
  universe snapshot.
- Polygon/Massive grouped-daily ingest adds bars, not securities. If the active
  local universe is tiny, run ticker-reference ingest first:

```powershell
catalyst-radar ingest-polygon tickers
catalyst-radar ingest-polygon grouped-daily --date <LATEST_TRADING_DATE>
catalyst-radar build-universe --as-of <LATEST_TRADING_DATE>
catalyst-radar scan --as-of <LATEST_TRADING_DATE>
```

The TUI overview now needs to make this explicit: first row is scan coverage,
then the priced-in candidate queue. If active security count is tiny, the first
row should say "Universe too small" instead of implying full-market insight.

The current CLI/API follow-up adds a scriptable version of that same queue:

```powershell
catalyst-radar priced-in-preflight --json
catalyst-radar priced-in-queue --json
catalyst-radar priced-in-queue --status bullish_not_priced_in --min-gap 20 --limit 20
```

API equivalent:

```text
GET /api/radar/priced-in/preflight
GET /api/radar/priced-in?limit=50&status=bullish_not_priced_in&min_gap=20
```

`priced-in-preflight` is the zero-call answer to "why only these tickers?" It
reports `priced-in-preflight-v1`, `external_calls_made=0`, current scan status,
and exact commands/API routes for ticker seeding, daily-bar ingest,
universe build, call-plan review, scan execution, and queue review. The
preflight follows the configured market provider, so Polygon/Massive mode
returns Polygon commands and CSV mode returns CSV ingest commands. In
Polygon/Massive mode it also reports the configured ticker-reference page cap;
with `CATALYST_POLYGON_TICKERS_MAX_PAGES=1`, ticker seeding is deliberately
capped and should not be described as the whole market. If grouped-daily bars
for a broad market date are already in the database, preflight estimates the
needed ticker-reference page count from the latest daily-bar ticker count. In
the current local database, that exposes the real blocker: latest bars contain
about 12k tickers, active securities are still 8, and the estimated ticker
reference seed is about 13 pages. The active follow-up adds
`CATALYST_POLYGON_TICKER_PAGE_DELAY_SECONDS`, a connector-level delay between
Polygon/Massive ticker-reference pages. Leave it `0` on plans that allow fast
pagination; set it before a large seed on rate-limited plans so the full scan
path is explicit instead of accidentally hammering the provider.

The active script follow-up adds `scripts\run-full-market-scan.ps1`. It is
plan-only unless `-Execute` is provided. Plan mode calls only local
`priced-in-preflight --json`, prints the selected as-of date, estimated ticker
pages, page delay, and exact command sequence, then exits with
`External calls made: 0`. Execute mode sets
`CATALYST_POLYGON_TICKERS_MAX_PAGES` and
`CATALYST_POLYGON_TICKER_PAGE_DELAY_SECONDS` only in that PowerShell process,
then runs ticker seed, grouped-daily ingest, universe build, scan, and
`priced-in-queue --json`.

This payload is `priced-in-queue-v1`, reports `external_calls_made=0`, includes
the full-scan boundary (`universe_too_small`, `partial_scan`, or `ready`), and
returns ranked rows with ticker, status, direction, emotion score, reaction
score, gap, priced-in score, state, setup, source, data-source coverage, reason,
and next step. Source coverage is explicit: `available`, `stale`, and `missing`
groups over market bars, catalyst events, local text, options, theme/peer/sector
context, and broker context. This keeps the CLI/API and dashboard aligned around
the same useful thing instead of creating a second interpretation.
The active follow-up adds queue-level `source_coverage` to the same payload and
dashboard snapshot. It counts available/stale/missing source classes across the
visible priced-in queue and exposes `weak_sources` in priority order, so the
overview can tell the operator whether the next blocker is stale bars, missing
catalyst events, missing local text, options, theme/peer/sector context, or
broker context.

Current state is **research-only**. The required run path and SEC catalyst path
work, but daily bars are still local CSV and stale (`latest_bar=2026-05-08` vs.
latest run `as_of=2026-05-16`), and the universe is intentionally tiny. The
next small product slice should make the CSV/manual market refresh path obvious,
not add a large new market-data framework.

The current small slice adds that operator path:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\refresh-csv-market-data.ps1 -TemplateOut data\local\manual-bars-2026-05-16.csv -ExpectedAsOf 2026-05-16
powershell -ExecutionPolicy Bypass -File scripts\refresh-csv-market-data.ps1 -DailyBars <fresh-bars.csv> -ExpectedAsOf 2026-05-16
powershell -ExecutionPolicy Bypass -File scripts\refresh-csv-market-data.ps1 -DailyBars <fresh-bars.csv> -ExpectedAsOf 2026-05-16 -Execute
```

The first command writes an ignored local template for active tickers. Fill
`open`, `high`, `low`, `close`, `volume`, and `vwap`, then use the second
command as preview-only. Preview now reports all missing or invalid bar fields
it finds before any import, and it refuses a file that is missing expected-as-of
bars for active tickers. `-Execute` wraps the existing `ingest-csv` CLI, records
provider health through the existing CSV provider path, and makes zero Polygon,
SEC, Schwab, or OpenAI calls.

The dashboard/API readiness wording now surfaces the same command from
`operator_next_step`, stale market-data blockers, and candidate readiness gates
instead of leaving the user with a generic "refresh CSV bars" instruction.
The terminal sitrep also prints a template command that writes to ignored
`data\local\manual-bars-<date>.csv`, plus active market-bar coverage from
`/api/ops/health`. The latest local polish makes the preview failure more
useful: an unfilled template reports every missing field it sees, then exits
before any database write or provider call. The active local polish adds
latest-date coverage to the same health/readiness path, so a partial import
cannot make the product look decision-ready just because one ticker has a fresh
bar. The active follow-up also lists the first missing latest-bar tickers in
status/readiness output, capped for readability. The active local slice adds
run-`as_of` coverage to status/readiness as well, so the operator sees the
exact active ticker gap for the date the next import must satisfy.

The current TUI slice adds `catalyst-radar dashboard-tui` as the terminal
replacement surface for the command center, plus `dashboard-snapshot --json`
for functional E2E assertions. The default interactive TUI uses Textual for a
modern Windows Terminal-compatible interface with sidebar mouse navigation,
status cards, selectable insight/candidate/alert rows, a command input,
keyboard shortcuts, and a footer. The latest visual polish fixes the cropped
sidebar button bug by using one-line clickable nav rows, groups the sidebar
into `LEARN`, `CORE`, `REVIEW`, `OPERATE`, and `SYSTEM`, surfaces
candidate/alert/IPO counts in navigation, tightens the metric grid so all four
cards fit, and uses a darker "ops console" style with clearer status values.
The current usability polish makes `1 Insights` the default operator surface:
it is a market-insight action queue where each row shows scope/ticker, signal,
why it matters now, and the next action. Candidate rows open candidate detail,
alert rows open alert detail, blocker rows open Readiness/Ops, and refresh rows
open the guarded Run plan. `0 Tutorial` remains available, but it is no longer
the default landing page. The TUI keeps shortcuts visible in a compact `KEYS` /
`MOUSE` guide, supports `Ctrl+N` / `Ctrl+P` and sidebar `Up` / `Down`
navigation, and shows side-by-side `NEXT ACTION` and `LAST RESPONSE` cards so
operator intent is distinct from dashboard feedback.
`dashboard-tui --once` still uses the plain text renderer for deterministic
smoke tests and low-fi logs. The TUI exposes the same useful dashboard data
families: readiness, latest run, discovery snapshot, candidate rows, alerts,
IPO/S-1 rows, themes, validation, costs, broker context, ops health, telemetry,
telemetry coverage, live activation, call planning, and a feature inventory.
Navigation/filtering/export are zero provider-call. The TUI also supports
guarded manual radar runs (`run execute` after viewing the call plan), local
opportunity actions, trigger creation/evaluation, blocked order-preview tickets,
and alert feedback. Real order submission remains disabled.

The current local bootstrap slice adds a repo-owned PowerShell launcher:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\install-dashboard-profile.ps1 -ProfilePath $PROFILE
radar
```

`scripts\install-dashboard-profile.ps1` writes a small idempotent block to the
user's PowerShell profile. The profile function calls
`scripts\run-dashboard-tui.ps1`, which creates `.venv` if needed, installs the
editable `catalyst-radar` command when `pyproject.toml` changes, fast-forwards
clean `main` to `origin/main`, and runs `dashboard-tui` without setting
`PYTHONPATH` or mutating the caller's shell environment. The user can pass TUI
arguments directly, for example `radar --once --page tutorial`. Use
`radar --no-update` to skip Git update and `radar --force-install` to refresh
the editable install.

Agent review is not connected to GitHub Copilot. Runtime code has no Copilot
SDK dependency or source reference, and `tests/unit/test_agent_provider_boundary.py`
guards that boundary. The current real-provider path is the official `openai`
Python SDK `responses.create(...)` call through `OpenAIResponsesClient`, gated
behind `CATALYST_ENABLE_PREMIUM_LLM=true`, `CATALYST_LLM_PROVIDER=openai`, and
`OPENAI_API_KEY`. Dry-run and fake review modes do not call OpenAI.

The current Agents SDK slice adds `catalyst-radar agent-brief` as the
multi-agent operator brain. Default mode is deterministic and zero-call; it
builds a structured brief from the same redacted dashboard snapshot using four
roles: Data Sentinel, Catalyst Analyst, Risk Officer, and Operator. Real SDK
mode is explicit-only:

```powershell
catalyst-radar agent-brief --real --json
```

It fails closed unless all gates are set:
`CATALYST_ENABLE_AGENT_SDK=true`, `CATALYST_ENABLE_PREMIUM_LLM=true`,
`CATALYST_LLM_PROVIDER=openai`, `CATALYST_AGENT_SDK_MODEL=<model>`, and
`OPENAI_API_KEY=<secret>`. The real-mode SDK surface uses `openai-agents` with
specialist agents exposed to a manager agent, but grants no Polygon/Massive,
SEC, Schwab, shell, filesystem, web, or order-submission tools. Provider and
broker actions remain separate human-triggered workflows.

Verification for this slice:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\unit -q
.\.venv\Scripts\python.exe -m pytest tests\integration\test_llm_cli.py tests\integration\test_dashboard_demo_seed_cli.py -q
.\.venv\Scripts\python.exe -m ruff check src tests
.\.venv\Scripts\python.exe -c "from agents import Agent, Runner; a=Agent(name='x'); print(hasattr(a, 'as_tool'), hasattr(Runner, 'run_sync'))"
.\.venv\Scripts\catalyst-radar.exe agent-brief --json
.\.venv\Scripts\catalyst-radar.exe agent-brief --real --json
.\.venv\Scripts\catalyst-radar.exe dashboard-tui --once --page overview
```

Expected results: unit tests pass, the two integration files pass, ruff passes,
Agents SDK imports with `as_tool` and `run_sync`, default `agent-brief` reports
`mode=dry_run` with zero OpenAI/market/broker calls, `--real` exits 2 with
`mode=blocked` until gates are set, and the TUI smoke renders the insight page.
The full `pytest -q` run was attempted but exceeded the 300-second tool timeout,
so do not count it as a passing full-suite run.

## Current Repository State

The SEC-only activation work was merged to `main` through PR #176 using rebase
merge. CIK target coverage for the CSV SEC smoke was later merged through PR
#179. SEC-only market freshness wording was merged through PR #181. This
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

Files changed by PR #179:

- `data/sample/securities.csv`
- `scripts/run-first-live-smoke.ps1`
- `src/catalyst_radar/connectors/csv_market.py`
- `src/catalyst_radar/connectors/market_data.py`
- `tests/integration/test_csv_ingest.py`
- `tests/integration/test_dashboard_data.py`
- `tests/integration/test_dry_run_csv_provider.py`
- `tests/integration/test_jobs.py`
- `tests/integration/test_local_scripts.py`

Files changed by PR #181:

- `src/catalyst_radar/dashboard/data.py`
- `tests/integration/test_dashboard_data.py`

Files changed by the manual CSV market-refresh slice:

- `scripts/refresh-csv-market-data.ps1`
- `scripts/market-radar-status.ps1`
- `tests/integration/test_local_scripts.py`
- `README.md`
- `handoff.md`

Files changed by the API readiness refresh-command slice:

- `src/catalyst_radar/dashboard/data.py`
- `tests/integration/test_dashboard_data.py`
- `handoff.md`

Files changed by the dashboard CLI snapshot slice:

- `src/catalyst_radar/cli.py`
- `src/catalyst_radar/dashboard/tui.py`
- `tests/integration/test_dashboard_demo_seed_cli.py`
- `docs/dashboard-feature-inventory.md`
- `README.md`
- `handoff.md`

Files changed by the Agents SDK operator slice:

- `pyproject.toml`
- `.env.example`
- `src/catalyst_radar/agents/sdk_orchestrator.py`
- `src/catalyst_radar/cli.py`
- `src/catalyst_radar/core/config.py`
- `tests/unit/test_agent_sdk_orchestrator.py`
- `tests/unit/test_agent_provider_boundary.py`
- `tests/unit/test_config.py`
- `tests/integration/test_dashboard_demo_seed_cli.py`
- `README.md`
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

## What Changed In PR #179

The old `no_sec_cik_targets` blocker is cleared for the local SEC-only path:

- `data/sample/securities.csv`
  - Now includes a `cik` column.
  - Keeps the original fixture tickers.
  - Adds a tiny real watchlist with AAPL and MSFT CIKs.

- `src/catalyst_radar/connectors/csv_market.py`
  - Preserves optional `cik`, `cik_str`, and `central_index_key` columns as
    `Security.metadata`.

- `src/catalyst_radar/connectors/market_data.py`
  - Carries the same optional CIK metadata through provider-style CSV ingest,
    which is the path used by scheduled daily runs.

- `scripts/run-first-live-smoke.ps1`
  - Fetches latest-run summary counts when the execute API returns the scheduler
    envelope, so `required=7/7` does not print as blanks.

## What Changed In PR #181

The dashboard/status wording now reflects the actual no-Polygon path:

- Market data remains local CSV/fixture-backed, so investment readiness remains
  `research_only`.
- The next action now says: use SEC-only results for research only; refresh CSV
  bars or configure a live market provider before acting.
- Stale-bar blockers now name CSV refresh explicitly instead of only saying to
  configure a live provider.

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

At that point, the old Polygon-key blocker was gone, but the next usefulness
blocker was product data shape: active local securities did not expose CIK
metadata, so the SEC live adapter had no submission targets. PR #179 fixed that
for the local SEC-only smoke path.

After PR #181 was merged, services were restarted from `main`.
`scripts\market-radar-status.ps1` reported:

```text
API: ok; version=0.1.0
Readiness: research_only; investable=False; next=Use SEC-only results for research only; refresh CSV bars or configure a live market provider before acting.
Latest run: success; required=7/7; action_needed=0; optional_gates=4; audit_rows=4
Live activation: ready; missing=0
Call plan: live_calls_planned; will_call_external=True; max_external_calls=2
Portfolio context: ready; Schwab read-only portfolio context is connected and fresh.
Telemetry: ready; events=25; attention=0; guarded=0
Telemetry coverage: ready; required_ready=3/3; missing_required=0
External calls made: 0
```

One read-only Schwab portfolio sync was run after PR #181 because status showed
the broker context was stale. It returned:

```text
status=connected
account_count=1
balance_count=1
position_count=0
open_order_count=0
order_submission_available=False
```

Post-merge plan-only smoke from `main` reported:

```text
Live activation: ready
Radar call plan: live_calls_planned; max_external_calls=2
Plan only: no provider calls were made.
Polygon universe seeding will be skipped unless the market provider is polygon.
Execute budget: polygon_universe_seed_pages=0; radar_external_calls_max=2
External calls made: 0
```

The capped execute smoke was run once after PR #179. It made no Polygon, Schwab,
or OpenAI calls. It made two SEC submissions calls for the CIK-backed CSV
targets. `/api/radar/runs/latest` then reported:

```text
status=success
required_step_count=7
required_completed_count=7
run_path_status=complete
event_ingest.status=success
event_ingest.provider=sec
event_ingest.target_count=2
event_ingest.event_count=2000
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

For the terminal dashboard replacement and CLI-based functional E2E checks:

```powershell
catalyst-radar dashboard-tui
catalyst-radar dashboard-tui --once --page features
catalyst-radar dashboard-snapshot --json
catalyst-radar dashboard-snapshot --ticker ACME --available-at 2026-05-10T21:06:00Z
```

The TUI reads the local database through dashboard data helpers, redacts
restricted provider payloads, and makes 0 Polygon, SEC, Schwab, or OpenAI calls
while rendering, navigating, filtering, refreshing, or exporting JSON. The
explicit `run execute` command starts one capped scheduler cycle only after the
call plan is visible. Broker/operator write commands save local rows only and
do not enable real order submission.

## Provider-Call Safety Rules

Do not make provider calls until:

1. Local services have restarted after `.env.local` changes.
2. `scripts/check-live-activation.ps1` is clean or only reports non-blocking optional Polygon state.
3. `scripts/run-first-live-smoke.ps1` plan-only mode reports the intended call budget.

Expected first useful call budget without Polygon:

- Polygon universe seed: `0`
- Polygon market data: `0`
- SEC submissions: `2` with the current sample CSV, capped by
  `CATALYST_SEC_DAILY_MAX_TICKERS`
- Schwab: `0`
- OpenAI: `0`

If the call plan is blocked, do not use `-Execute`. Inspect `/api/radar/runs/call-plan` first.

## Known Product Limitations After This Change

This change makes live SEC catalyst ingestion usable without Polygon, but it does not make the product fully investable.

Remaining limitations:

- Market data stays local CSV until another live market source is configured.
- Investment readiness should remain `research_only` if market data is fixture/stale.
- SEC polling now has AAPL/MSFT CIK-backed local targets, but this is only a
  tiny watchlist, not broad discovery.
- Polygon remains the existing broad-market live data adapter, but the user does not currently have a key.
- OpenAI/LLM review remains disabled/dry-run by design.
- Schwab is read-only context only; order submission remains disabled.

## Next Useful Product Slice

CIK target coverage, operator wording, the manual CSV import wrapper, and the
API/dashboard refresh-command wording are done. The next change should stay
small and focus on making the operator's manual bar refresh verifiable after
import without assuming Polygon:

- Use `scripts\refresh-csv-market-data.ps1` with a fresh daily-bar CSV, rerun
  `scripts\market-radar-status.ps1`, then run the plan-only smoke before any
  capped cycle.
- Treat market bars as fresh only when every active ticker has a bar on the
  latest/as-of date; partial fresh imports should remain research-only.
- If this still leaves the product research-only, inspect the remaining blocker
  in `Market freshness`, `Usefulness`, and `operator_next_step` before adding
  new data-provider code.
- Keep Polygon optional unless the user explicitly gets a key.
- If touching Schwab again, keep it read-only; the latest sync is fresh and
  order submission remains unavailable.

Relevant code paths:

```text
src\catalyst_radar\dashboard\data.py
src\catalyst_radar\cli.py
src\catalyst_radar\dashboard\tui.py
data\sample\securities.csv
docs\dashboard-feature-inventory.md
scripts\market-radar-status.ps1
scripts\run-first-live-smoke.ps1
tests\integration\test_dashboard_demo_seed_cli.py
```

## How To Resume If Interrupted

1. Check branch and worktree:

   ```powershell
   git status --short --branch
   ```

2. Re-run focused tests:

   ```powershell
   py -m pytest tests\integration\test_dashboard_demo_seed_cli.py -q
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

PR #176, `Make first live activation SEC-only`, PR #179,
`Add CSV CIK targets for SEC smoke`, and PR #181,
`Clarify SEC-only market freshness status`, have already been merged. Later
docs-only handoff cleanup PRs may exist. The next product PR should address a
lightweight CSV/manual market freshness path, unless the user redirects.

## Do Not Do

- Do not ask the user for a Polygon key again unless they explicitly choose Polygon.
- Do not paste `.env.local` contents into chat.
- Do not run `scripts/run-first-live-smoke.ps1 -Execute` if plan-only mode is blocked or unexpectedly high.
- Do not mark the active goal complete; the product is improving but not fully useful/investable yet.
- Do not update the checked-in PR ledger just to include a just-merged ledger PR, because that creates a self-referential loop.

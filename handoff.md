# MarketRadar Handoff

Last updated: 2026-05-19 06:25:26 +08:00

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

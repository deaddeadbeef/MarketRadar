# Dashboard Feature Inventory

Last updated: 2026-05-19

This inventory tracks the current Market Radar dashboard features and where the
terminal dashboard exposes them. The TUI uses the same dashboard data helpers as
the Streamlit command center and makes 0 Polygon, SEC, Schwab, or OpenAI calls
while rendering, navigating, filtering, or exporting its local JSON snapshot.

## Terminal Entry Points

```powershell
catalyst-radar dashboard-tui
catalyst-radar dashboard-tui --once --page tutorial
catalyst-radar dashboard-tui --once --page overview
catalyst-radar dashboard-tui --once --page features
catalyst-radar priced-in-answer
catalyst-radar priced-in-queue --stocks-only --json
catalyst-radar priced-in-queue --decision-ready
catalyst-radar priced-in-preflight --json
catalyst-radar priced-in-queue --json
catalyst-radar priced-in-source-batches --source all
catalyst-radar priced-in-source-batches --source catalyst_events --all --json
catalyst-radar dashboard-snapshot --json
powershell -ExecutionPolicy Bypass -File scripts/run-full-market-scan.ps1
```

Inside `dashboard-tui`, use page numbers or names to navigate, `open <#|ticker>`
from the candidates page, `open <#|alert-id>` from the alerts page, `ticker
<SYMBOL|all>`, `available-at <ISO|latest>`, `alert-status <status|all>`,
`alert-route <route|all>`, `refresh`, `json`, `run`, `run execute`,
`clear-filters`, `help`, and `q`. The default TUI entry page is `0 Tutorial`;
press `1` or use `--page overview` to open the full ranked Insights scan. Type
`ready` or press `D` in the TUI only when you intentionally want the
decision-useful subset from that full scan; the API equivalent is
`GET /api/radar/priced-in?decision_ready=true`; type `stocks` to focus common
stock/ADR rows, and type `full` to return to the whole ranked universe.
`run execute` starts one capped radar cycle
through the existing scheduler only after the call plan has been shown. The TUI
also supports source-fill planning and execution: `batch <source>` is zero-call
and shows the full-scan plan plus the next safe chunk, while
`batch all` summarizes every priced-in source gap without calling providers.
`batch <source> execute` runs only that one guarded chunk. The tickers shown in
one chunk are not the scan universe. `batch all` and
`priced-in-source-batches --source all` separate `coverage_first` from
`decision_shortcut`, so broad full-scan evidence coverage does not get confused
with the smaller current answer subset. CLI/API parity is available through
`priced-in-source-batches --source all`,
`priced-in-source-batches --source <source> --execute-next` and
`POST /api/radar/priced-in/source-batches/execute-next`.
The TUI also supports low-risk operator writes: `action <ticker> <action> [notes]`,
`trigger <ticker> <type> <op> <threshold> [notes]`, `eval-triggers [ticker]`,
`ticket <ticker> <buy|sell> <entry> <stop> [risk_pct] [notes]`, and
`feedback <alert-id|#> <label> [notes]`.

## Current Features

| Area | Current dashboard feature | TUI page | Operational use |
| --- | --- | --- | --- |
| Readiness | Investment readiness, usefulness score, and operator next step | `overview`, `readiness` | Know whether output is research-only or decision-useful. |
| Full scan coverage | Active security count, requested/scanned securities, candidate count, latest/run bar coverage, stocks-only bar coverage, zero-call preflight blockers, and selected-universe warnings | `overview`, `ops`, `run`, `priced-in-preflight`, `/api/radar/priced-in/preflight` | Confirm the queue came from a real all-active pass rather than a tiny fixture or `liquid-us` selected universe, distinguish a stocks-only coverage gap from fund/wrapper gaps, and get the exact next command when it did not. |
| Current priced-in answer | One zero-call answer to whether price has matched market expectations, with research/answer-ready counts, optional context gaps, next command, stock-only filtering, and explicit trade-readiness boundary | `overview`, `priced-in-answer`, `priced-in-queue --full-scan`, `priced-in-queue --stocks-only`, `priced-in-queue --decision-ready`, `/api/radar/priced-in`, `/api/radar/priced-in?stocks_only=true`, `/api/radar/priced-in?decision_ready=true`, `/api/radar/priced-in/answer`, `dashboard-snapshot --json` | Know whether the current scan can answer the priced-in question, focus actual common-stock/ADR rows when the task is “any stock,” while keeping trade approval tied to the separate readiness/manual-buy-review gate. |
| Priced-in mismatch | Emotion score, price-reaction score, emotion-minus-reaction gap, status, reason, queue-level and row-level source coverage, next step | `overview`, `candidates`, `candidate:<ticker>`, `priced-in-queue`, `/api/radar/priced-in` | Find stocks where market emotion appears ahead of or behind price reaction, distinguish blocking local evidence gaps from optional options/broker context, and see whether each source is contributing. |
| Source fill workflow | Prioritized zero-call workflow from priced-in preflight evidence-plan steps, including dependencies, next source action, the all-source plan command, and a compact overview hint for coverage-first versus decision-shortcut work | `overview`, `ops`, `dashboard-snapshot --page ops --json`, `batch all` in TUI | Know the next data layer to inspect before running provider chunks; keep action distinct from response. |
| All-source batch overview | Plan-only source-gap map across market bars, catalyst events, local text, options, theme/peer context, and broker context, with first executable chunk per source, a broad `coverage_first` recommendation, a smaller `decision_shortcut`, and zero provider calls | `priced-in-source-batches --source all`, `/api/radar/priced-in/source-batches?source=all`, `batch all` in TUI | Decide which data layer to fill next before spending provider calls; avoid confusing whole-market coverage work with the current top-answer subset. |
| Full-scan source batch plan | Every source-fill chunk for the current filtered full scan, with batch count, next safe chunk, all-batches command, API equivalent, and zero provider calls while planning | `priced-in-source-batches --source <source> --all --json`, `/api/radar/priced-in/source-batches?source=<source>&all_batches=true`, `batch <source>` in TUI | Avoid mistaking the first safe provider chunk for the whole scan; inspect the whole full-scan fill plan before running any explicit batch. |
| Guarded source batch execution | One explicit source-fill chunk at a time, using the existing SEC, local text, or read-only Schwab executors and provider caps; successful chunks return a zero-call post-execution source-gap delta | `batch <source> execute` in TUI, `priced-in-source-batches --source <source> --execute-next`, `POST /api/radar/priced-in/source-batches/execute-next`, `/api/radar/sec/submissions-batch`, `/api/radar/text/features-batch`, `/api/brokers/schwab/market-sync` | Fill evidence for the full scan without adding an accidental all-market live-call button, then see whether the updated plan improved before repeating. |
| Local text source fill | Deterministic narrative feature batches over stored event text | `priced-in-source-batches --source local_text`, `/api/radar/text/features-batch` | Turn ingested catalyst events into local emotion and theme evidence without external calls. |
| Market data | Run as-of coverage, latest bar coverage, stale-bar blockers | `overview`, `ops` | Verify fresh bars before relying on real market data. |
| Radar run | Latest run path, required steps, optional gates, call plan | `overview`, `run` | Check what will call external providers before executing a cycle. |
| Candidates | Candidate queue, decision labels, research gaps, card readiness | `candidates`, `candidate:<ticker>` | Work the research shortlist and manual-review queue. |
| Alerts | Alert rows, route/status filters, suppression context | `alerts`, `alert:<id>` | Review planned and dry-run alert output before delivery. |
| IPO/S-1 | SEC S-1 analysis rows, terms, risk flags, source links | `ipo` | Inspect live SEC catalyst evidence. |
| Themes | Theme aggregation over candidate rows | `themes` | Spot clustered catalysts and repeated setup types. |
| Validation | Validation run, useful-alert rate, false positives | `validation` | Track whether the radar is producing useful output. |
| Costs | LLM budget ledger summary and cost per useful alert | `costs` | Keep optional agentic review bounded. |
| Agent brief | Redacted multi-agent operator brief with the current priced-in answer, evidence plan, safety checks, and zero provider calls in dry-run mode | `agent` TUI page, `agent-brief`, `agent-brief --json`, `/api/agents/brief` | Let the agent layer summarize whether price has matched expectations without leaking secrets or making hidden market, broker, shell, or OpenAI calls unless real mode is explicitly gated. |
| Broker | Read-only Schwab connection, balances, positions, order kill switch | `broker` | Use portfolio context without enabling real order submission. |
| Ops | Provider health, database counts, jobs, degraded mode | `ops` | Diagnose stale data and provider failures. |
| Telemetry | Audit tape and coverage over required operational events | `telemetry` | Verify operational evidence before trusting status. |

## Remaining Replacement Gaps

The TUI replaces the web dashboard for operations, navigation, filtering,
drill-in review, JSON evidence export, guarded manual radar runs, opportunity
action saves, trigger management, trigger evaluation, blocked order-preview
ticket creation, and alert feedback. The `overview` page should be read as a
priced-in mismatch queue, not as a complete ticker table: the first row states
whether the active local universe is large enough to count as a full-market
scan, and candidate rows are the ranked useful subset. If the universe is tiny,
`priced-in-preflight --json` and `/api/radar/priced-in/preflight` provide the
zero-call checklist for seeding tickers, ingesting bars, reviewing the call
plan, running the all-active scan, and reviewing the queue. In
Polygon/Massive mode that checklist includes the configured ticker-reference
page cap and, when latest grouped-daily bars are available, the estimated page
count needed to seed a comparable ticker universe. The checklist also surfaces
`CATALYST_POLYGON_TICKER_PAGE_DELAY_SECONDS`, which paces paginated ticker
reference requests for rate-limited plans. These live-provider actions stay
behind existing explicit guarded commands or API routes because they can call
external services and require credentials:

When the latest run is universe-scoped, the priced-in queue is scoped to that
same universe snapshot so older same-date rows outside the latest run's universe
are not mixed in. The queue is labeled as selected-universe output until the
next run scans all active securities without `--universe`.

- Trigger optional universe seeding only when Polygon is configured; use
  `scripts/run-full-market-scan.ps1` for the plan-first full-market sequence.
- Refresh Schwab market context for a selected candidate.
- Run candidate agent-review dry runs.

Keep using the TUI as the primary operational surface, and use the existing
guarded CLI/API/script paths for the credentialed live-provider actions above.

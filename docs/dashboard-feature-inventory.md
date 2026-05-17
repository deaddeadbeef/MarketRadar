# Dashboard Feature Inventory

Last updated: 2026-05-17

This inventory tracks the current Market Radar dashboard features and where the
terminal dashboard exposes them. The TUI uses the same dashboard data helpers as
the Streamlit command center and makes 0 Polygon, SEC, Schwab, or OpenAI calls
while rendering, navigating, filtering, or exporting its local JSON snapshot.

## Terminal Entry Points

```powershell
catalyst-radar dashboard-tui
catalyst-radar dashboard-tui --once --page overview
catalyst-radar dashboard-tui --once --page features
catalyst-radar dashboard-snapshot --json
```

Inside `dashboard-tui`, use page numbers or names to navigate, `open <#|ticker>`
from the candidates page, `open <#|alert-id>` from the alerts page, `ticker
<SYMBOL|all>`, `available-at <ISO|latest>`, `alert-status <status|all>`,
`alert-route <route|all>`, `refresh`, `json`, `run`, `run execute`,
`clear-filters`, `help`, and `q`. `run execute` starts one capped radar cycle
through the existing scheduler only after the call plan has been shown. The TUI
also supports low-risk operator writes: `action <ticker> <action> [notes]`,
`trigger <ticker> <type> <op> <threshold> [notes]`, `eval-triggers [ticker]`,
`ticket <ticker> <buy|sell> <entry> <stop> [risk_pct] [notes]`, and
`feedback <alert-id|#> <label> [notes]`.

## Current Features

| Area | Current dashboard feature | TUI page | Operational use |
| --- | --- | --- | --- |
| Readiness | Investment readiness, usefulness score, and operator next step | `overview`, `readiness` | Know whether output is research-only or decision-useful. |
| Market data | Run as-of coverage, latest bar coverage, stale-bar blockers | `overview`, `ops` | Verify fresh bars before relying on real market data. |
| Radar run | Latest run path, required steps, optional gates, call plan | `overview`, `run` | Check what will call external providers before executing a cycle. |
| Candidates | Candidate queue, decision labels, research gaps, card readiness | `candidates`, `candidate:<ticker>` | Work the research shortlist and manual-review queue. |
| Alerts | Alert rows, route/status filters, suppression context | `alerts`, `alert:<id>` | Review planned and dry-run alert output before delivery. |
| IPO/S-1 | SEC S-1 analysis rows, terms, risk flags, source links | `ipo` | Inspect live SEC catalyst evidence. |
| Themes | Theme aggregation over candidate rows | `themes` | Spot clustered catalysts and repeated setup types. |
| Validation | Validation run, useful-alert rate, false positives | `validation` | Track whether the radar is producing useful output. |
| Costs | LLM budget ledger summary and cost per useful alert | `costs` | Keep optional agentic review bounded. |
| Broker | Read-only Schwab connection, balances, positions, order kill switch | `broker` | Use portfolio context without enabling real order submission. |
| Ops | Provider health, database counts, jobs, degraded mode | `ops` | Diagnose stale data and provider failures. |
| Telemetry | Audit tape and coverage over required operational events | `telemetry` | Verify operational evidence before trusting status. |

## Remaining Replacement Gaps

The TUI replaces the web dashboard for operations, navigation, filtering,
drill-in review, JSON evidence export, guarded manual radar runs, opportunity
action saves, trigger management, trigger evaluation, blocked order-preview
ticket creation, and alert feedback. These live-provider actions stay behind
existing explicit guarded commands or API routes because they can call external
services and require credentials:

- Trigger optional universe seeding only when Polygon is configured.
- Refresh Schwab market context for a selected candidate.
- Run candidate agent-review dry runs.

Keep using the TUI as the primary operational surface, and use the existing
guarded CLI/API/script paths for the credentialed live-provider actions above.

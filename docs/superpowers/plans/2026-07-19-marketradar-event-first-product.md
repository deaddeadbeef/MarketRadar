# MarketRadar Product Plan: Event-First Discovery

> **For agentic workers:** After approval, implement phase-by-phase using isolated worktrees. Prefer subagent-driven execution for independent tasks. Preserve zero-hidden-call browsing and decision-support-only boundaries.

**Goal:** Help a human find public equities whose **price has not yet fully discovered** a **world event** — ranked research leads with evidence, not autonomous trades.

**Architecture:** Keep MarketRadar’s deterministic priced-in/score/policy core. Re-center the product object on **world events** (X/Grok daily feed → JSON → optional event-store fan-out). Map events to primary and second-order tickers, join local reaction/priced-in features for those names only, apply a trust ladder, and prove value via shadow/value ledger.

**Tech Stack:** Python `catalyst_radar` (scan, priced-in, policy, validation), SQLite/Postgres, FastAPI, Tauri desktop + Rust TUI, Grok scheduled tasks + X search as the event bus. No in-process X OAuth in P0–P1.

## Global Constraints

- Decision support only: never auto-trade; highest automated state remains Eligible for Manual Buy Review.
- `investment_advice: false` on all discovery surfaces.
- Browse / snapshot / discovery-brief default paths: `external_calls_made=0`, `db_writes_made=0` unless explicit `--execute` / `--confirm-external-call`.
- Social/X-only leads stay `research_only` until primary/regulatory confirmation.
- Isolated branch/worktree; never commit on `main`; rebase-merge PRs.
- Do not block discovery on full-market SEC residual fill (5k+ gaps). Discovery runs on **mapped tickers**.
- Reuse existing validation stack; do not invent a second proof system.
- Preserve fail-closed policy for capital-adjacent states.

---

## 1. Product vision

### One-sentence product

**MarketRadar is an event-first discovery radar:** it turns world narratives into a short list of equities where emotion/catalyst strength is ahead of price reaction, then forces human confirmation before any capital decision.

### Who it is for

- Single operator / small research desk.
- Wants *asymmetric attention*, not another full-market scanner or broker bot.

### Success definition (measurable)

| Horizon | Metric | Target |
|--------|--------|--------|
| Weekly | Fresh world-event brief available | ≥5 weekdays with `generated_at` &lt; 24h |
| Weekly | Discoveries with real reaction join (not `unknown`) | ≥50% of top-20 rows |
| Monthly | Value-ledger claimable labels on discovery | At least enough evidence for `value-report` ≠ pure insufficient |
| Monthly | Attributable decision-support value | Track toward **$40/month** (existing product gate) |
| Always | Safety | 0 accidental broker orders; social-only never reaches buy-review |

### Explicit non-goals

- Autonomous execution or “AI picks that print money.”
- Becoming a news terminal or Twitter client.
- Blocking usefulness on complete SEC coverage of 12k tickers.
- Treating meme velocity as alpha without causal mapping.

---

## 2. Current state (as of PR #1114)

| Layer | Status |
|-------|--------|
| Priced-in gap engine | Mature (`scoring/priced_in.py`, full scan) |
| Score + policy | Mature; fail-closed action states |
| World-events JSON + mapper + `discovery-brief` | **Shipped P0** |
| Dashboard `event_discovery` + Tauri **World Events** | **Shipped P0** |
| Grok daily task `market-radar-daily-discovery` | Created (ops path still thin) |
| Full-market scan / bars | Often ready; product still blocked on SEC catalyst gaps for *trusted full answer* |
| Discovery → event store write | Missing |
| Discovery → value ledger | Missing |
| Quiet-tape scan for mapped set only | Missing |
| Case file (X + SEC join) | Missing |

**Key insight:** The old product inverted the loop (fix data → maybe discover). The new product inverts back: **event → map → reaction check → confirm → optional paper**.

---

## 3. Target user journey (north star UX)

```text
Morning
  1. Open MarketRadar → World Events (default landing later)
  2. See 3–8 world events from last 24h (X/Grok)
  3. Open Discovery Queue sorted by emotion_reaction_gap
  4. Click ticker → Case File: event sources, themes, 5d/20d reaction, RS, SEC confirmations
  5. Label useful / noisy / too-late (value ledger)
  6. Optionally paper-trade only after primary confirmation + policy allows

Never
  - Hidden provider/OpenAI/broker calls while browsing
  - “Buy” button that submits orders
```

### Information hierarchy (desktop)

1. **World Events** — what happened  
2. **Discovery Queue** — who may be under-discovered  
3. **Case File** — evidence + invalidation  
4. **Proof** — did past discoveries pay attention value?  
5. **Ops** — data repair (secondary, not default)

---

## 4. System architecture (target)

```text
┌─────────────────────────────────────────────────────────────┐
│  Event bus (Grok Build)                                      │
│  X semantic/keyword search → cluster → world-events-v1 JSON  │
│  Write: data/local/world_events.json                         │
└───────────────────────────┬─────────────────────────────────┘
                            │ zero-call read
┌───────────────────────────▼─────────────────────────────────┐
│  Discovery Service (Python)                                  │
│  load → map themes/tickers → optional event-store fan-out    │
│  join local bars/priced-in for mapped tickers only           │
│  trust ladder → discovery-brief-v1                           │
└───────────────────────────┬─────────────────────────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        ▼                   ▼                   ▼
   Tauri World Events   CLI discovery-brief   Value ledger
   Case File            API / snapshot        Shadow outcomes
                            │
                            ▼ (only when confirmed)
                     Existing scan/score/policy
                     Decision cards (sparse LLM)
```

### Trust ladder (product law)

| Evidence | Max usefulness | Max action state |
|----------|----------------|------------------|
| Social/X only | `research_only` | ResearchOnly |
| + reputable news or multi-source corroboration | `watch` | AddToWatchlist |
| + SEC/primary + quiet tape + liquidity + no hard blocks | `manual_review_candidate` | EligibleForManualBuyReview only via existing policy |
| Conflict / stale / hard block | `blocked` | Blocked |

Discovery **never** bypasses `evaluate_policy`.

---

## 5. Phased roadmap

### Phase 0 — Spine (DONE on `feat/event-first-discovery`)

- [x] Design doc + plan  
- [x] `world-events-v1` + sample pilot  
- [x] `discovery-brief` CLI  
- [x] Dashboard `event_discovery`  
- [x] Tauri World Events page  
- [x] Unit tests + PR #1114  
- [x] Grok daily task created  

### Phase 1 — Daily product loop (NEXT — critical path)

**Outcome:** Every market morning, operator can run one command / open one page and see *fresh* events with *real* reaction joins for mapped names.

| Task | Deliverable |
|------|-------------|
| P1.1 Freshness contract | Brief reports `events_age_hours`, `stale` if &gt; 36h; UI banner |
| P1.2 Local path ops | Document + script: copy Grok JSON → `data/local/world_events.json`; `discovery-ingest --validate` |
| P1.3 Mapped-ticker reaction join | For each discovery ticker, load bars/candidate row; if missing, mark `join_status=missing_scan` and emit **targeted** next command: scan/import for that set only |
| P1.4 Quiet-tape filter | Prefer rows with high emotion and low reaction / low ret_5d when join exists; demote mega-caps already fully reacted |
| P1.5 World-event → CanonicalEvent fan-out (opt-in) | `discovery-ingest --execute` writes per-ticker SOCIAL events with dedupe `world:{id}:{ticker}` |
| P1.6 README product path | Event-first quickstart above residual-bar ops |

### Phase 2 — Confirmation + case file

**Outcome:** Operator can prove or kill a lead without leaving the app.

| Task | Deliverable |
|------|-------------|
| P2.1 Case file page | From discovery row → ticker case: event sources, themes, gap, 5d/20d/RS, linked SEC events if any |
| P2.2 Confirmation status | `unconfirmed` / `corroborated` / `primary_confirmed` from event store quality |
| P2.3 Raise usefulness only with confirmation | Social alone never leaves research_only |
| P2.4 Deep link to existing candidate detail | Reuse candidate packet / decision card when present |

### Phase 3 — Proof loop (value)

**Outcome:** Know whether discovery is worth time.

| Task | Deliverable |
|------|-------------|
| P3.1 Value-ledger artifact type `discovery_row` | Or use `manual_note` with structured payload `event_id`, `gap`, `usefulness` |
| P3.2 Label from UI/CLI | useful / noisy / too-late / false-positive / good-research |
| P3.3 Forward outcomes | Reuse `value-outcomes` 5/10/20/60d on labeled discovery tickers |
| P3.4 Monthly discovery section in value-report | Hit rate of high-gap leads vs baselines |
| P3.5 Optional shadow tagging | Shadow run can record which discovery IDs were open that day |

### Phase 4 — Product polish + default UX

**Outcome:** Event-first is the default product; ops is secondary.

| Task | Deliverable |
|------|-------------|
| P4.1 Default desktop landing = World Events | Overview becomes secondary “workbench” |
| P4.2 Demote full-market trust gate from home | Still available under Ops/Evidence Gaps |
| P4.3 Theme ontology expansion | Curated second-order maps; optional sparse LLM mapper behind budget |
| P4.4 Alerts | Digest only for `watch`+ after confirmation; dry-run default |
| P4.5 Kill complexity | Hide residual-repair hero copy from primary path |

### Phase 5 — Optional live enrichment (later)

- Live news wire connector (not only fixtures)  
- Options/context for discovery subset only  
- Sparse LLM skeptic on top N confirmed candidates (existing agent budget gates)  
- Still no autonomous trading  

---

## 6. Implementation plan — Phase 1 (concrete)

Work continues on `feat/event-first-discovery` or stacked PR after merge of #1114.

### Task 1: Freshness + join status in discovery brief

**Files:**
- Modify: `src/catalyst_radar/discovery/brief.py`
- Modify: `src/catalyst_radar/discovery/models.py` (if needed)
- Test: `tests/unit/test_discovery_brief.py`
- Modify: `apps/radar-desktop/frontend/app.js` (`renderWorldEvents` banner)

**Produces:**
- Brief fields: `events_age_hours: float | null`, `freshness_status: fresh|stale|unknown`
- Discovery row fields: `join_status: joined|missing_scan|no_db`, `ret_5d_pct`, `reaction_score` when available

**Steps:**
1. Add unit tests for stale events (`generated_at` &gt; 36h → `freshness_status=stale`).
2. Implement age computation in `build_discovery_brief`.
3. When engine present, set `join_status` from priced-in index hit/miss; attach `ret_5d` from candidate metadata/features if present.
4. UI: show amber banner when stale; show “N of M joined” metric.
5. Commit: `feat: discovery freshness and join status`

### Task 2: Validate/import CLI for local world events

**Files:**
- Create: `src/catalyst_radar/discovery/ingest.py`
- Modify: `src/catalyst_radar/cli.py` — `discovery-ingest` with `--validate-only` / `--execute`
- Test: `tests/unit/test_discovery_ingest.py`
- Create: `scripts/import-world-events.ps1` (copy/validate helper)

**Produces:**
- `discovery-ingest --events PATH --validate-only --json` → schema errors, event count, 0 writes  
- `discovery-ingest --events PATH --execute --json` → copy or write to `data/local/world_events.json`, optional fan-out later  

**Steps:**
1. Test validate rejects bad schema.
2. Implement validate + write local path (no event-store yet).
3. Wire CLI; human + JSON output with call/write counters.
4. Commit: `feat: discovery-ingest validate and local import`

### Task 3: Optional CanonicalEvent fan-out

**Files:**
- Modify: `src/catalyst_radar/discovery/ingest.py`
- Modify: `src/catalyst_radar/events/dedupe.py` or use existing helpers
- Test: `tests/integration/test_discovery_event_fanout.py`

**Produces:**
- `--fanout-events --execute` inserts per-ticker SOCIAL events  
- Dedupe key: `world:{event_id}:{ticker}`  
- Preview default: planned write count, 0 writes without `--execute`

**Steps:**
1. Failing integration test: empty DB → execute fan-out → list_events_for_ticker returns row.
2. Implement fan-out with low materiality floor for social.
3. Ensure scan can read them without elevating policy past ResearchOnly solely from social.
4. Commit: `feat: fan out world events into event store`

### Task 4: Targeted scan next-command for missing joins

**Files:**
- Modify: `src/catalyst_radar/discovery/brief.py`
- Modify: `apps/radar-desktop/frontend/app.js`

**Produces:**
- When many `missing_scan`, `canonical_next_command` suggests scanning/importing **mapped ticker set**, not full SEC batch.
- UI lists sample missing tickers.

### Task 5: Product README path

**Files:**
- Modify: `README.md` (top “Event-first quickstart” section)
- Modify: `docs/designs/2026-07-19-event-first-discovery.md` status → multi-phase

---

## 7. Implementation plan — Phase 2–3 (summary tasks)

### Phase 2 Task A: Case file payload

**Files:** Create `src/catalyst_radar/discovery/case_file.py`; wire CLI `discovery-case --ticker X`; snapshot `event_discovery.case_file` optional; Tauri renderer section.

**Produces:** Single-ticker packet: events, sources, gap, market reaction, linked SEC events, confirmation status, next step.

### Phase 3 Task A: Ledger integration

**Files:** Extend `validation/value_ledger.py` ALLOWED artifact types with `discovery_row`; CLI `value-ledger record --artifact-type discovery_row --artifact-id evt:TICKER`; UI label buttons later.

**Produces:** Labeled discoveries feed monthly value report.

---

## 8. What we keep vs de-emphasize

| Keep / invest | De-emphasize on primary path |
|---------------|------------------------------|
| Priced-in gap, score pillars, policy hard blocks | Full-universe SEC gap fill as readiness hero |
| Shadow / value ledger / paper | Schwab order submission |
| Tauri shell + zero-call snapshot | Streamlit parity wars |
| Sparse budgeted LLM for confirmed candidates | LLM-owned scores |
| Grok/X as event bus | Building full X client inside MarketRadar |

---

## 9. Risks and mitigations

| Risk | Mitigation |
|------|------------|
| Social misinformation | Research-only until primary; source_quality caps |
| Theme over-mapping | Cap primary tickers; secondary discount; human labels |
| Emotion inflation if fan-out too aggressive | Social materiality floors; policy still fail-closed |
| Operator treats score as buy signal | Copy + flags + no order path |
| Stale Grok task | Freshness banner + ops check |
| Dual mental models (ops vs discovery) | Default landing World Events; Ops nested |

---

## 10. Suggested execution order after plan approval

1. Merge or continue PR #1114 (P0).  
2. Implement Phase 1 Tasks 1–5 on stacked branch `feat/discovery-daily-loop`.  
3. Run live for 5 trading days with Grok daily task → label manually.  
4. Only then Phase 2 case file + Phase 3 ledger automation.  
5. Phase 4 UX default switch after proof loop has any data.

**Execution mode recommendation:** Inline for Phase 1 Task 1–2 (tight coupling), then subagent-driven for fan-out + UI polish.

---

## 11. Acceptance criteria for “product useful”

The product is **useful for the goal** when all of the following are true:

1. Fresh world-events file exists most weekdays.  
2. Top discovery rows show **joined** reaction for a majority of names.  
3. Operator can open Case File and see *why* and *what would invalidate*.  
4. At least some discoveries are labeled; monthly report is not permanently empty for discovery.  
5. Zero accidental provider/broker side effects during normal use.  
6. Social-only never appears as investment-ready.

Until then: **safe research cockpit**, not “investable product.”

---

## 12. Open decisions (defaults recommended)

| Decision | Recommended default |
|----------|---------------------|
| Default landing page | World Events (Phase 4; keep Overview until Phase 1 proven) |
| Event bus | Grok scheduled task + file (not X API keys in-app) |
| Universe for reaction | Mapped tickers only, not full active universe |
| Fan-out social to event store | Opt-in execute, off by default in daily loop |
| Value target | Keep $40/month decision-support target |

No blocking questions required to start Phase 1 with these defaults.

# MarketRadar agent contract

This repository's **only supported product** is event-first discovery.

Authoritative docs: `docs/designs/2026-08-15-marketradar-product-spec.md`,
`docs/designs/2026-08-19-catalyst-signals.md`, `docs/PRODUCT_SCOPE.md`,
`docs/DEPRECATION.md`. Grok `event_id` contract is spec §15.1.

The **human receive surface is the CLI** (`catalyst-radar`, prefer JSON).
Tauri World Events, radar-tui, and Streamlit are deprecated. Do not open a GUI.

## Do

- Work on isolated branches/worktrees. Never commit on `main`.
- Add features only in `src/catalyst_radar/discovery/` plus supporting join/bar fill.
- Capture **pending binaries across domains** (policy, energy, semis, health,
  macro, legal — not a biotech desk). Do not fill the weekday dump with
  “stock +100% today” posts. Lesson: `docs/designs/2026-08-19-catalyst-signals.md`.
  Standing mission: `docs/missions/pending-binaries.md`.
  Grok Build: one skill `.grok/skills/market-radar/SKILL.md` with params
  `hunt|brief|ready|bars`. Slash: `/market-radar hunt`. Headless:
  `grok -p "/market-radar hunt"`. Daily scheduled prompt runs `/market-radar hunt`.
- Keep browse/snapshot paths at zero hidden provider, broker, and LLM calls.
- Keep social/X-only leads at `research_only` until SEC/EDGAR/PRIMARY/REGULATORY confirmation.
- Run product tests before handoff:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/test_discovery_*.py tests/unit/test_product_scope.py -q
```

```powershell
$env:PYTHONPATH='src'
.\.venv\Scripts\python.exe -m pytest tests\unit\test_discovery_*.py tests\unit\test_product_scope.py -q
```

## Do not

- Treat `assert-trial-ready`, `assert-shadow-ready`, or `assert-investable-readiness` as the ship gate. Use `assert-discovery-ready` / `catalyst-radar ready`.
- Expand the trading workbench, broker, IPO, alerts, agent cockpit, or Streamlit surfaces.
- Launch `radar-desktop`, `radar-tui`, or `scripts/open-market-radar.*`.
- Join discovery through `dashboard.data.load_candidate_rows`.
- Block discovery on full-universe SEC residual fill or grouped-daily of 12k names.
- Follow `handoff.md` or `docs/legacy/` as the current product contract.
- Fake a buy call or submit broker orders.

## Product loop

**Skill-triggered, headless:** Grok `market-radar` skill mines X and public
sources, classifies pending binaries, writes `x-posts-v1`, converts, optionally
fills bars. The human reads `catalyst-radar brief` JSON. Labels stay human.
Ops may copy JSON into Notion **outside this repo**.

Interactive: `/market-radar` (hunt if stale, else brief). Daily: `/market-radar hunt`
or `grok -p "/market-radar hunt"`.

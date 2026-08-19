# MarketRadar agent contract

This repository's **only supported product** is event-first discovery.

Authoritative docs: `docs/designs/2026-08-15-marketradar-product-spec.md`,
`docs/designs/2026-08-19-catalyst-signals.md`, `docs/PRODUCT_SCOPE.md`,
`docs/DEPRECATION.md`. Grok `event_id` contract is spec §15.1.

## Do

- Work on isolated branches/worktrees. Never commit on `main`.
- Add features only in `src/catalyst_radar/discovery/` plus supporting join/bar fill.
- Capture **pending binaries across domains** (policy, energy, semis, health,
  macro, legal — not a biotech desk). Do not fill the weekday dump with
  “stock +100% today” posts. Lesson: `docs/designs/2026-08-19-catalyst-signals.md`.
  Standing mission: `docs/missions/pending-binaries.md`.
- Keep browse/snapshot paths at zero hidden provider, broker, and LLM calls.
- Keep social/X-only leads at `research_only` until SEC/EDGAR/PRIMARY/REGULATORY confirmation.
- Run product tests before handoff:

```powershell
$env:PYTHONPATH='src'
.\.venv\Scripts\python.exe -m pytest tests\unit\test_discovery_*.py tests\unit\test_product_scope.py -q
```

## Do not

- Treat `assert-trial-ready`, `assert-shadow-ready`, or `assert-investable-readiness` as the ship gate. Use `assert-discovery-ready`.
- Expand the trading workbench, broker, IPO, alerts, agent cockpit, or Streamlit surfaces.
- Join discovery through `dashboard.data.load_candidate_rows`.
- Block discovery on full-universe SEC residual fill or grouped-daily of 12k names.
- Follow `handoff.md` or `docs/legacy/` as the current product contract.

## Product loop

World events JSON → `discovery-brief` → event-time bar join → case file → proof labels.

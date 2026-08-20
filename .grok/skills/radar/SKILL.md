---
name: radar
description: >
  Skill-triggered MarketRadar engine: mine X and the public web for pending
  binaries across domains, analyze, install the briefing file. The desktop UI
  only displays. Use for MarketRadar, radar, briefing, world events, X mining,
  /radar, /radar-hunt, /radar-brief, /radar-ready. Not a trading desk.
argument-hint: "hunt | brief | ready | bars"
---

# MarketRadar engine (Grok skill)

You **are** the product loop. The Tauri World Events window is the **receive surface** only.

Read: `docs/designs/2026-08-19-catalyst-signals.md`, `docs/missions/pending-binaries.md`, `.grok/skills/radar/references/hunt.md`.

Default when the user opens this repo or asks about the market without a subcommand: **hunt if the local file is missing/stale, then brief.** Then tell them to open World Events and press **R**.

## Split

| Layer | Does |
|-------|------|
| This skill | X + web mining, classify A/B vs X, write `x-posts-v1`, convert, optional bars |
| `scripts/radar-grok.ps1` | Deterministic convert / brief JSON / ready / bars |
| Desktop | Renders `data/local/world_events.json`. No mining. |

## Laws

- Research only. No investment advice. No broker orders.
- Social/X-only stays `research_only` on the briefing queue.
- Type A/B **across domains** (policy, energy, semis, health, macro, legal). Not an FDA desk.
- Type X (gap-up “+100% today”) is never a hero card.
- Polygon mapped `/v2/aggs` only, event tickers + SPY, only with explicit confirm.

## Hunt (heavy lifting)

Follow `references/hunt.md`. Then:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/radar-grok.ps1 convert -Execute
# optional, only if the user asked for prices:
powershell -ExecutionPolicy Bypass -File scripts/radar-grok.ps1 bars -ConfirmExternalCall -Execute
powershell -ExecutionPolicy Bypass -File scripts/radar-grok.ps1 brief
```

Repo root = workspace. Python via `.venv\Scripts\python.exe`.

## Other intents

| Intent | Action |
|--------|--------|
| brief | `radar-grok.ps1 brief` + plain English |
| ready | `radar-grok.ps1 ready` |
| bars | `radar-grok.ps1 bars` only if asked |
| open | World Events is the surface; press **R** |

Headless: `grok -p "/radar-hunt" --cwd <repo>`

No buy list.

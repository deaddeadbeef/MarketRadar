---
name: market-radar
description: >
  One MarketRadar skill with params: hunt, brief, ready, bars. Mines X and the
  public web for pending binaries across domains, converts into world-events,
  and prints a CLI JSON briefing. Headless only. Use for /market-radar,
  MarketRadar, briefing, world events, X mining. Not a trading desk. Not a GUI.
  Scheduled daily run uses param hunt.
argument-hint: "hunt | brief | ready | bars"
---

# /market-radar [hunt | brief | ready | bars]

You **are** the product loop. The **receive surface is the CLI** (`catalyst-radar`
JSON). Ops may file that JSON into Notion **outside this repo**. Do not open a
GUI. Do not tell anyone to refresh a desktop window. Do not launch radar-desktop
or the TUI.

Read: `docs/designs/2026-08-19-catalyst-signals.md`, `docs/missions/pending-binaries.md`, `references/hunt.md`.

Parse the user argument (or scheduled prompt) as **param**:

| Param | Do |
|-------|----|
| `hunt` (scheduled default) | Follow `references/hunt.md`. Write `data/local/inbox/x_posts_YYYY-MM-DD.json`. `catalyst-radar convert --posts data/local/inbox/x_posts_YYYY-MM-DD.json --execute`. Then `brief`. |
| `brief` | `catalyst-radar brief` and summarize the JSON in plain English. |
| `ready` | `catalyst-radar ready`. |
| `bars` | `catalyst-radar bars --public --confirm-external-call --execute` only if the user asked. |
| *(empty, interactive)* | If `data/local/world_events.json` missing or older than 24h → `hunt`. Else `brief`. |

```text
/market-radar hunt
/market-radar brief
/market-radar ready
grok -p "/market-radar hunt" --cwd <repo>
```

Headless daily loop:

```text
grok -p "/market-radar hunt" --cwd <repo>
catalyst-radar brief
catalyst-radar ready
catalyst-radar product-scope
```

On this checkout, `PYTHONPATH=src` and `.venv/bin/python -m catalyst_radar.cli …`
are equivalent to `catalyst-radar` if the console script is not on PATH.
Windows helper: `scripts/radar-grok.ps1 hunt|brief|convert|ready|bars|status`.
Cross-platform helper: `python scripts/radar_grok.py …`.

## Laws

- Research only. No investment advice. No broker orders. Never fake a buy call.
- Social/X-only stays `research_only` on the briefing queue.
- Type A/B **across domains**. Not an FDA desk. Type X gap-up posts are never hero cards.
- Polygon mapped `/v2/aggs` only, event tickers + SPY, explicit confirm.
- Do not revive Streamlit, Tauri World Events, or the old workbench.

Repo root = workspace. After `hunt`, print the brief JSON. No buy list. No GUI.

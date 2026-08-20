---
name: market-radar
description: >
  One MarketRadar skill with params: hunt, brief, ready, bars. Mines X and the
  public web for pending binaries across domains, installs the briefing, or
  shows it. Desktop only displays. Use for /market-radar, MarketRadar, briefing,
  world events, X mining. Not a trading desk. Scheduled daily run uses param hunt.
argument-hint: "hunt | brief | ready | bars"
---

# /market-radar [hunt | brief | ready | bars]

You **are** the product loop. World Events is the **receive surface** (press **R**).

Read: `docs/designs/2026-08-19-catalyst-signals.md`, `docs/missions/pending-binaries.md`, `references/hunt.md`.

Parse the user argument (or scheduled prompt) as **param**:

| Param | Do |
|-------|----|
| `hunt` (scheduled default) | Follow `references/hunt.md`. Write `data/local/inbox/x_posts_YYYY-MM-DD.json`. `scripts/radar-grok.ps1 convert -Execute`. Then `brief`. |
| `brief` | `scripts/radar-grok.ps1 brief` and summarize in plain English. |
| `ready` | `scripts/radar-grok.ps1 ready`. |
| `bars` | `scripts/radar-grok.ps1 bars -ConfirmExternalCall -Execute` only if the user asked. |
| *(empty, interactive)* | If `data/local/world_events.json` missing or older than 24h → `hunt`. Else `brief`. |

```text
/market-radar hunt
/market-radar brief
/market-radar ready
grok -p "/market-radar hunt" --cwd <repo>
```

## Laws

- Research only. No investment advice. No broker orders.
- Social/X-only stays `research_only` on the briefing queue.
- Type A/B **across domains**. Not an FDA desk. Type X gap-up posts are never hero cards.
- Polygon mapped `/v2/aggs` only, event tickers + SPY, explicit confirm.

Repo root = workspace. After `hunt`, tell the user to press **R**. No buy list.

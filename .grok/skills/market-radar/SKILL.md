---
name: market-radar
description: >
  One MarketRadar skill with params: hunt, brief, ready, bars, open. Mines X and
  the public web for pending binaries across domains, installs the briefing, and
  opens World Events. Desktop is the receive surface. Use for /market-radar,
  MarketRadar, briefing, world events, X mining, open the UI. Not a trading
  desk. Scheduled daily run uses param hunt.
argument-hint: "hunt | brief | ready | bars | open"
---

# /market-radar [hunt | brief | ready | bars | open]

You **are** the product loop. World Events is the **receive surface**. Hunt and
brief do not stop in chat: install, summarize, then **open the desktop**.

Repo source of truth: `.grok/skills/market-radar/` in MarketRadar. Grok reads the
installed copy under `%USERPROFILE%\.grok\skills\market-radar\` after
`scripts/install-market-radar-skill.ps1`.

Live install: `C:\Users\fpan1\MarketRadar` (always write inbox and
`world_events.json` here, even if git HEAD is not `main`).

Read: `docs/designs/2026-08-19-catalyst-signals.md`, `docs/missions/pending-binaries.md`, `references/hunt.md`.

Do **not** recursively explore `.worktrees` looking for the product.

Parse the user argument (or scheduled prompt) as **param**:

| Param | Do |
|-------|----|
| `hunt` (scheduled default) | Follow `references/hunt.md`. Write `data/local/inbox/x_posts_YYYY-MM-DD.json`. Convert, then `brief` (brief already opens World Events). |
| `brief` | Run brief, summarize in plain English, then **open**. |
| `open` | Open World Events only. If already running, tell the user to press **R**. |
| `ready` | Run assert-discovery-ready. |
| `bars` | Polygon mapped bars only if the user asked, with confirm + execute. |
| *(empty, interactive)* | If `data/local/world_events.json` missing or older than 24h → `hunt`. Else `brief`. |

## How to run scripts

Cwd and data = live install.

```powershell
$live = "C:\Users\fpan1\MarketRadar"
$grokSkill = Join-Path $env:USERPROFILE ".grok\skills\market-radar"
if ($env:GROK_HOME) { $grokSkill = Join-Path $env:GROK_HOME "skills\market-radar" }
$py = Join-Path $live ".venv\Scripts\python.exe"
$env:PYTHONPATH = Join-Path $live "src"
Set-Location $live

$radar = Join-Path $live "scripts\radar-grok.ps1"
# convert:
if (Test-Path $radar) { & $radar convert -Execute } else { & $py (Join-Path $live "scripts\radar_grok.py") convert --posts (Join-Path $live "data\local\inbox\x_posts_$((Get-Date).ToString('yyyy-MM-dd')).json") --execute }
# brief / ready:
if (Test-Path $radar) { & $radar brief } else { & $py (Join-Path $live "scripts\radar_grok.py") brief }
if (Test-Path $radar) { & $radar ready } else { & $py (Join-Path $live "scripts\radar_grok.py") ready }
```

Open World Events (after `hunt` / `brief`, or for param `open`):

```powershell
$open = Join-Path $grokSkill "scripts\open-market-radar.ps1"
if (-not (Test-Path $open)) { $open = Join-Path $live "scripts\open-market-radar.ps1" }
& $open
```

If `status=already_running`, tell the user to press **R**. If `status=launched`, World Events is open. Do not spawn a second desktop.

```text
/market-radar hunt
/market-radar brief
/market-radar open
/market-radar ready
grok -p "/market-radar hunt" --cwd C:\Users\fpan1\MarketRadar
```

## Laws

- Research only. No investment advice. No broker orders.
- Social/X-only stays `research_only` on the briefing queue.
- Type A/B **across domains**. Not an FDA desk. Type X gap-up posts are never hero cards.
- Polygon mapped `/v2/aggs` only, event tickers + SPY, explicit confirm.

After `hunt` or `brief`, open World Events. No buy list.

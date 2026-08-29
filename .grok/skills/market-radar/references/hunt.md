# Radar hunt playbook

Mine **pending binaries** (dated or windowed) with listed names and a quiet-enough tape. Cover **at least three domains**. Biotech is one domain, not the product.

## Search (every run)

Prefer sources in this order. Query **windows**, not movers.

1. **Official calendars / IR / regulator / court** hosts (company IR, FDA/EMA calendars, Fed/BLS/BEA, court dockets, OPEC, exchange notices).
2. **Reputable news** that cites a dated window.
3. **X** keyword/semantic search — last, for discovery tips and social corroboration.

| Domain | Hunt for |
|--------|----------|
| Policy / rates | FOMC, Jackson Hole, named court or tariff **date** |
| Energy / shipping | OPEC+ meeting date, chokepoint / escort news with a window, official inventory print |
| Semis / trade | Named earnings **date**, export-control deadline, official share-print |
| Health | PDUFA date, Phase 2/3 “data expected”, medical-meeting follow-up **before** a violent move |
| Macro | CPI / payrolls **still ahead** |
| Corporate / legal | Close date, ruling date |

Skip: “JUST IN +130%”, options sympathy, theme chatter with no date, already-printed binaries whose tape already exploded.

## Cap = distinct stories, not posts

- Cap is **up to 16 distinct stories** (`event_id` / dated window). Never pad with type X. Empty slots stay empty.
- Two URLs on the same `event_id` (official + x.com mirror) are **clustering**, not a second story. Do not spend a story slot on an X mirror of a story that already has a primary/regulator URL unless slots remain after new dated binaries are filled.
- Spend remaining slots on **new** dated binaries across at least three domains — not duplicate mirrors of the same eight names.
- Source class comes from the **URL host** (see product host classification), not from `provider=x` or the dump filename.
- Do **not** drop a still-open dated primary (e.g. an open company-IR PDUFA) just because a noisy dump preferred a new name.

## Write

`data/local/inbox/x_posts_YYYY-MM-DD.json` as `x-posts-v1`:

- required `event_id` (same id = one story)
- `published_at`, `title` or `text`, tickers and/or themes
- prefer official/primary URL on the post when available; X URL may cluster on the same `event_id`
- `investment_advice` is not a field on posts; keep copy research-only

Then convert:

```text
catalyst-radar convert --posts data/local/inbox/x_posts_YYYY-MM-DD.json --execute
```

Equivalent: `python scripts/radar_grok.py convert --posts data/local/inbox/x_posts_YYYY-MM-DD.json --execute`.

Then print the briefing with `catalyst-radar brief`. Do not open a GUI.

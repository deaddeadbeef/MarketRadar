# Radar hunt playbook

Mine **pending binaries** (dated or windowed) with listed names and a quiet-enough tape. Cover **at least three domains**. Biotech is one domain, not the product.

## Search (every run)

Use web search and X keyword/semantic search. Query **windows**, not movers.

| Domain | Hunt for |
|--------|----------|
| Policy / rates | FOMC, Jackson Hole, named court or tariff **date** |
| Energy / shipping | OPEC+ meeting date, chokepoint / escort news with a window, official inventory print |
| Semis / trade | Named earnings **date**, export-control deadline, official share-print |
| Health | PDUFA date, Phase 2/3 “data expected”, medical-meeting follow-up **before** a violent move |
| Macro | CPI / payrolls **still ahead** |
| Corporate / legal | Close date, ruling date |

Skip: “JUST IN +130%”, options sympathy, theme chatter with no date, already-printed binaries whose tape already exploded.

## Write

`data/local/inbox/x_posts_YYYY-MM-DD.json` as `x-posts-v1`:

- required `event_id` (same id = one story)
- `published_at`, `title` or `text`, tickers and/or themes
- cap 8 stories; never pad
- `investment_advice` is not a field on posts; keep copy research-only

Then convert with `scripts/radar-grok.ps1 convert -Execute`.

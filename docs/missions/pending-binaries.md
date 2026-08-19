# Mission: pending binaries across domains

**Status:** standing  
**Started:** 2026-08-19  
**Cadence:** daily (scheduled); not a biotech desk  

MarketRadar hunts **pending, dated or windowed binaries** that map to listed names while the tape is still quiet. Biotech (MRNA 2026) is one domain. It is not the product.

Authoritative taxonomy: `docs/designs/2026-08-19-catalyst-signals.md`.  
Product laws: `docs/designs/2026-08-15-marketradar-product-spec.md`.

This is research triage, not investment advice.

---

## Object

Type **A** (pending binary) and type **B** (confirmatory update ahead of a binary).  
Type **X** (gap-up posts, “stock +100% today”, theme chatter with no window) is forbidden as the hero card.

## Domains (rotate every run; do not collapse to one)

Each daily dump should cover **at least three** of:

| Domain | Example binaries (shape, not a shopping list) |
|--------|-----------------------------------------------|
| Policy / rates | FOMC, hike odds, named court or tariff ruling date |
| Energy / shipping | OPEC+ meeting, Hormuz/strait risk, official inventory print |
| Semis / trade | Export-control date, China share print, named earnings setup with a window |
| Health / science | PDUFA, Phase 2/3 window, medical-meeting follow-up **before** the tape explodes |
| Macro prints | CPI, payrolls, when the print is still ahead or just out and not fully digested |
| Corporate / legal | Known close date, ruling, product-approval clock outside FDA |

Partner / second-order names (type **D**) ride with the same `event_id`.

## Output each run

1. Write `data/local/inbox/x_posts_YYYY-MM-DD.json` as `x-posts-v1` (required `event_id`, tickers or themes, `published_at`).
2. Convert with `discovery-from-posts --execute` into `data/local/world_events.json`.
3. Optional: `discovery-bars --polygon --confirm-external-call --execute` for **event tickers + SPY only**.
4. Cap **8** stories. Never pad with type X.
5. Leave `investment_advice: false`. Do not chase names that already made the violent move today.

## Stop conditions

- Do not submit broker orders.
- Do not treat social-only as a buy list.
- Do not replace the dump with a single-sector calendar (FDA-only, semis-only, oil-only).

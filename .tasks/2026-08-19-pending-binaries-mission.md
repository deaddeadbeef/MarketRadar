# Task: standing pending-binaries mission

**Branch:** `docs/pending-binaries-mission`  
**Status:** standing / daily  
**Contract:** `docs/missions/pending-binaries.md`

## Goal

Keep MarketRadar’s weekday file filled with pending binaries **across domains**, not a biotech calendar and not yesterday’s gap-ups.

## Acceptance (each daily run)

- Dump covers ≥3 domains from the mission table.
- Stories are type A/B; type X gap-up posts are not hero cards.
- `event_id` set; convert into `data/local/world_events.json`.
- Research only; `investment_advice` false.

## Validation

- `discovery-from-posts` succeeds.
- Briefing story count ≤ 8.
- No domain is the only domain two days in a row if others have live binaries.

## Out of scope

- Investment advice, broker orders, FDA-only crawler, chasing MRNA after the +130% day as a new lead.

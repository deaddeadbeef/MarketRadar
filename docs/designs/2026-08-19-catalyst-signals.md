# What MarketRadar should capture

**Date:** 2026-08-19  
**Status:** Product lesson (authoritative for the Grok/operator feed)  
**Trigger:** Moderna (`MRNA`) doubled on 19 Aug 2026 after a Phase 3 melanoma-vaccine readout. The useful radar window was months earlier. The installed briefing never had the story.

This is research triage language, not investment advice.

---

## The lesson in one sentence

Capture a **pending, dated (or windowed) binary that maps to listed names while the tape is still quiet**. Do **not** treat the day-of gap-up as the signal.

---

## Teaching case: intismeran / MRNA

| When | What was public | Tape | Radar object? |
|------|-----------------|------|----------------|
| Jan 2026 | 5-year Phase 2b KEYNOTE-942: combo vs Keytruda still cut recurrence/death risk ~49%. Phase 3 INTerpath-001 fully enrolled. CEO: readout expected in 2026. | Not the +130% day | **Yes.** Windowed binary + primary sources + names (`MRNA`, partner `MRK`). |
| 1–2 Jun 2026 (ASCO) | Same 5-year data presented and published. Street notes called it confidence into 2026 Phase 3. | Still not the explosion (later close still ~$63) | **Yes.** Confirmatory science that raises the odds of a later binary. |
| 18 Aug 2026 close | No Phase 3 print yet | **$62.96** | Last quiet close before the binary. |
| 19 Aug 2026 midday | First Phase 3 win for a personalized mRNA cancer vaccine (INTerpath-001, with Merck). | **+$110% to +135%** toward ~$146 | **Too late.** This is the tape answering. X “JUST IN +130%” is not a discovery lead. |

MarketRadar’s live file on that morning was still the **13 Aug** memory/oil/tariff dump. No `MRNA`. No oncology theme. The mapper could not have second-ordered Moderna even if someone had tagged `biotech`.

The miss was not “we needed a faster quote.” It was “we never ingested the **pending binary**.”

---

## Signal types to capture (in order)

A weekday dump is doing its job when most cards look like **type A or B**, not type X.

### A — Pending binary (highest value)

A public event with a **date or a bounded window**, where the outcome can reprice listed names.

Examples:

- PDUFA / FDA decision date  
- Phase 2/3 readout, interim analysis, “fully enrolled, data expected YYYY”  
- Scheduled earnings only when tied to a named product/binary (not every print)  
- Known court/regulatory ruling date  

Must have: **names** (or a theme that maps names) and a **window**.  
Nice to have: ClinicalTrials.gov / IR / FDA calendar URL (helps the trust ladder later).

### B — Confirmatory science ahead of a binary

A primary-source update that **does not settle** the binary but **changes how seriously** to take the window.

Example: ASCO 5-year Phase 2b follow-up while Phase 3 is still pending.

These stay `research_only` on the briefing queue if the source is social. They are still worth a card so a human can go read the IR.

### C — First-order shock that is still under-reacted

A same-day primary event **before** a violent move is obvious (pre-market PR, after-hours 8-K, embargo lift). Rare in a once-a-day file. Still valid if the tape is not yet the story.

### D — Partner / second-order names on the same binary

If the story is a combo trial, map **both** sides (`MRNA` and `MRK`), not only the loud cashtag.

Theme expansion (oncology, `mrna`, `biotech_catalyst`) exists so a correctly tagged event can name partners. Theme-only names without a join still do not steal the newbie eight.

### X — Do not capture as the product signal

- Intraday “stock +100%” / “best day ever” posts  
- Options-flow sympathy after the gap  
- Recycled COVID-vaccine culture war with no new binary  
- Theme chatter (memory, oil, “biotech is hot”) with **no window and no primary hook**  
- Full-universe residual scans hoping a catalyst falls out  

Type X is what X is full of at 10:57 on the day MRNA doubled. Ingesting that would make the briefing a **rear-view mirror**.

---

## Feed contract add-on (Grok / operator)

The weekday `x-posts-v1` task must **search for type A/B**, not only trending cashtags in semis and energy.

Minimum query classes each weekday:

1. PDUFA / FDA decision dates in the next 90 days  
2. Phase 2/3 or pivotal “readout / top-line / fully enrolled”  
3. Major medical-meeting data (ASCO, ESMO, AHA, AASLD, etc.) tied to a ticker  
4. IR / 8-K / ClinicalTrials.gov style primary hooks when they appear on X  

Each post still needs `event_id`, tickers or themes, and `published_at`. Prefer linking the company PR or trial page when it exists.

Teaching fixture for the June (type B) object: `data/sample/x_posts_2026-06-02_asco_intismeran.json`.

---

## How the briefing should have read (June, not 19 Aug)

> **Story:** Moderna/Merck personalized melanoma vaccine — Phase 3 window in 2026 after 5-year Phase 2b follow-up.  
> **Names:** Moderna, Merck.  
> **Tape (then):** has not made a once-in-a-decade move; still a research card.  
> **Usefulness:** `research_only` until primary confirmation.  
> **Next action:** Read the IR / trial page. Not a buy list.

On 19 Aug the same card, if refreshed after the print, should say the tape **already moved**. That is honesty, not a new lead.

# Phase 8 Review: Candidate Packets And Decision Cards

Date: 2026-05-10

## Outcome

Phase 8 is ready for fixture-backed real-world testing. Catalyst Radar now persists deterministic candidate packets and decision cards, exposes packet/card availability in dashboard data, and provides CLI workflows to build and inspect review artifacts.

The implementation remains deterministic. It does not call LLMs, route orders, or make buy/sell decisions. Decision cards are manual-review decision-support artifacts only.

## Verification

Focused verification:

```text
python -m pytest tests/unit/test_candidate_packet_builder.py tests/golden/test_candidate_packets_replay.py tests/unit/test_decision_card_builder.py tests/integration/test_candidate_packet_repository.py tests/integration/test_candidate_packets_cli.py tests/integration/test_scan_pipeline.py
33 passed in 3.21s
```

Full tests:

```text
python -m pytest
280 passed in 37.41s
```

Lint:

```text
python -m ruff check src tests apps
All checks passed!
```

## Fixture Smoke

The smoke used an isolated SQLite database with `PYTHONPATH=src`:

```text
initialized database
ingested securities=6 daily_bars=36 holdings=1
ingested provider=news_fixture raw=2 normalized=2 securities=0 daily_bars=0 holdings=0 events=2 rejected=0
ingested provider=sec raw=2 normalized=2 securities=0 daily_bars=0 holdings=0 events=2 rejected=0
ingested provider=earnings_fixture raw=1 normalized=1 securities=0 daily_bars=0 holdings=0 events=1 rejected=0
ingested provider=options_fixture raw=1 normalized=1 option_features=1 rejected=0
processed text_features=1 snippets=5
scanned candidates=3
built candidate_packets=3
built decision_cards=3
AAA packet state=Blocked supporting=4 disconfirming=4 conflicts=0 supporting_top=Strong persisted market pillars [signal_features:AAA:2026-05-10T21:00:00+00:00:market-v1:pillar_scores] disconfirming_top=Hard policy block [candidate_states:99b22fd4-130e-4f26-a2ba-31ab5f36bbf7:hard_blocks]
AAA decision_card state=Blocked next_review_at=2026-05-17T21:00:00+00:00 supporting_top=Strong persisted market pillars [signal_features:AAA:2026-05-10T21:00:00+00:00:market-v1:pillar_scores] disconfirming_top=Hard policy block [candidate_states:99b22fd4-130e-4f26-a2ba-31ab5f36bbf7:hard_blocks]
```

## Review Findings And Fixes

Initial code/spec review found these issues:

- Packet and card IDs did not include `available_at`, so a later rebuild could overwrite earlier point-in-time history.
- Decision cards could duplicate evidence when built from a real `CandidatePacket`.
- Missing or future `signal_features` rows could be accepted by packet build input selection.
- Invalid persisted action states were silently coerced to `NoAction`.
- `ThesisWeakening`, `ExitInvalidateReview`, and `Blocked` packets did not have the same two-sided evidence guarantee as `Warning` and `EligibleForManualBuyReview`.
- Real Phase 8 packet score fields used `scores.pillars` and `score_delta_5d`, but card generation looked for older names.
- CLI human inspect output and dashboard data did not expose top source-linked evidence.
- Dashboard packet/card joins could duplicate candidate rows after repeated builds with multiple point-in-time versions.
- Streamlit dashboard home did not render top packet evidence even after the data layer exposed it.
- This review note was missing.

Fixes made:

- Packet and decision card IDs now include `available_at`, preserving multiple point-in-time versions.
- Repository tests cover early-visible plus later-rebuilt packet/card history.
- Decision card evidence is deduped before top evidence selection.
- Candidate input selection now inner joins `signal_features`, filters `candidate_states.created_at <= available_at`, and excludes signal payloads whose embedded availability is missing or future.
- Reconstructed invalid action states now raise.
- All states in the default `Warning`-and-above build path require supporting and disconfirming evidence.
- Decision cards read both `pillars` and `pillar_scores`, and both `score_delta_5d` and prior score-delta names.
- CLI inspect output includes source/computed-feature links for top supporting and disconfirming evidence.
- Dashboard rows expose top supporting and disconfirming evidence summaries.
- Dashboard data selects the latest packet/card version per candidate state.
- Streamlit home renders top supporting and disconfirming evidence columns.

Post-fix verification:

```text
python -m pytest
280 passed in 37.41s
```

```text
python -m ruff check src tests apps
All checks passed!
```

## Residual Risks

- Decision cards are deterministic research cards. LLM skeptic review and schema-validated LLM card writing remain future budget/router phases.
- Packet evidence is only as good as the persisted scan payloads, selected events, snippets, and feature rows available at build time.
- The fixture smoke produced blocked review cards, not a buy-review-eligible card; eligible-card completeness is covered by unit tests.
- `signal_features` still stores availability inside JSON payload rather than as top-level columns. The repository fails closed for missing/future payload availability, but a future schema phase should promote this into indexed columns.

# Phase 17: IPO S-1 Detection And Offering Analysis

## Objective

Add IPO detection and public S-1 download support to Market Radar, then produce a deterministic offering analysis that can be reviewed from the CLI and stored as source-grounded event evidence.

This phase is deliberately fixture-first and live-gated. It should not require an OpenAI API key. Public SEC document retrieval must use the existing SEC live-enable and User-Agent guardrails.

## Success Criteria

- Detect SEC `S-1` and `S-1/A` filings from the existing SEC submissions feed.
- Download or fixture-load the public primary S-1 document.
- Store the raw document text and source URL in provider raw records.
- Normalize detected S-1 filings into canonical financing events with IPO-specific payload fields.
- Produce deterministic offering analysis including proposed ticker, exchange, share count, price range, estimated gross proceeds, underwriters, use of proceeds, and risk flags.
- Expose a CLI flow for ingesting IPO S-1 filings and reviewing the stored analysis.
- Add fixtures and tests that prove the flow works without network access.
- Preserve existing SEC submissions behavior.
- Keep live SEC access gated behind `CATALYST_SEC_ENABLE_LIVE=1` and `CATALYST_SEC_USER_AGENT`.

## Non-Goals

- No LLM analysis in this phase.
- No broker integration or trade execution.
- No claim that the analysis is investment advice.
- No new database schema unless the existing raw, normalized, and event payload storage cannot support the feature.
- No broad IPO calendar product yet; this is filing detection plus analysis for specific CIK/ticker requests.

## Implementation Plan

### Task 1: Stabilize Baseline Alert Test

Context: this branch is based on `main`, which has a date-sensitive alert integration test. The test builds alerts with a fixed `2026-05-10` availability date and expects the default `alerts-list` clock to be earlier than that date. On and after `2026-05-10`, the test fails.

Edit:

- `tests/integration/test_alerts_cli.py`

Requirements:

- In `test_alerts_list_default_hides_future_alerts`, compute a future `available_at` relative to `datetime.now(UTC)`.
- Use that future timestamp for `build-alerts`.
- Keep the rest of the fixture data unchanged.

Verification:

```powershell
python -m pytest tests\integration\test_alerts_cli.py::test_alerts_list_default_hides_future_alerts -q
```

### Task 2: Add Deterministic S-1 Offering Analyzer

Add:

- `src/catalyst_radar/ipo/__init__.py`
- `src/catalyst_radar/ipo/s1.py`
- `tests/unit/test_ipo_s1_analysis.py`

Requirements:

- `is_ipo_registration_form(form_type: str) -> bool` returns true for `S-1` and `S-1/A`.
- `strip_sec_html(document: str) -> str` removes scripts, styles, tags, entities, and repeated whitespace.
- `analyze_s1_offering(...) -> dict[str, object]` returns stable JSON-compatible output with:
  - `analysis_version`
  - `company_name`
  - `form_type`
  - `source_url`
  - `proposed_ticker`
  - `exchange`
  - `shares_offered`
  - `price_range_low`
  - `price_range_high`
  - `price_range_midpoint`
  - `estimated_gross_proceeds`
  - `underwriters`
  - `use_of_proceeds_summary`
  - `risk_flags`
  - `sections_found`
- Missing fields should be `None` or empty collections, not exceptions.
- Extraction should be conservative and deterministic. It can use regular expressions, but the output must remain source-grounded and auditable.

Verification:

```powershell
python -m pytest tests\unit\test_ipo_s1_analysis.py -q
```

### Task 3: Extend SEC Connector With `ipo-s1`

Edit:

- `src/catalyst_radar/connectors/sec.py`
- `tests/unit/test_event_connectors.py`

Requirements:

- Preserve current `submissions` behavior.
- Add support for `ConnectorRequest.endpoint == "ipo-s1"`.
- Filter SEC recent filings to `S-1` and `S-1/A`.
- Build the public filing URL from CIK, accession number, and primary document.
- Load primary document text from a document fixture when provided.
- Download primary document text through the existing HTTP transport when live retrieval is enabled.
- Raw provider payloads for IPO S-1 filings must include:
  - SEC submissions metadata
  - `document_url`
  - `document_text`
  - `document_downloaded`
  - `downloaded_at`
- Normalized records must become `ConnectorRecordKind.EVENT` payloads with:
  - `event_type = financing`
  - `source_category = sec`
  - `form_type`
  - `filing_date`
  - `primary_document`
  - `document_url`
  - `ipo_analysis`
  - text fields suitable for local text intelligence, such as `summary` and/or `body`
- Classification reasons should identify the S-1 as an IPO registration statement.

Verification:

```powershell
python -m pytest tests\unit\test_event_connectors.py::test_sec_ipo_s1_downloads_document_and_normalizes_offer_analysis -q
```

### Task 4: Add IPO S-1 CLI Workflow

Edit:

- `src/catalyst_radar/cli.py`
- `tests/integration/test_sec_ipo_cli.py`

Requirements:

- Add:

```text
ingest-sec ipo-s1 --ticker TICKER --cik CIK [--fixture PATH] [--document-fixture PATH]
```

- Reuse existing SEC live guardrails:
  - fixture mode should work offline.
  - live mode requires `CATALYST_SEC_ENABLE_LIVE=1`.
  - live mode requires `CATALYST_SEC_USER_AGENT`.
- Persist raw, normalized, and canonical event records through the existing provider ingest pipeline.
- Add an analysis review command:

```text
ipo-s1-analysis --ticker TICKER [--as-of YYYY-MM-DD] [--available-at ISO_TS] [--json]
```

- The review command should read stored canonical events with `payload.ipo_analysis` and print the latest analysis for the ticker.
- JSON output should be stable for tests.

Verification:

```powershell
python -m pytest tests\integration\test_sec_ipo_cli.py -q
```

### Task 5: Fixtures

Add:

- `tests/fixtures/sec/submissions_acme_s1.json`
- `tests/fixtures/sec/acme_s1.htm`

Requirements:

- The submissions fixture must include at least one `S-1` filing and one non-IPO filing to prove filtering.
- The S-1 fixture must include extractable text for:
  - company name
  - proposed Nasdaq ticker
  - shares offered
  - price range
  - underwriters
  - use of proceeds
  - risk flags including losses, customer concentration, emerging growth company, and dual-class/control language.

### Task 6: Final Verification And Audit

Run:

```powershell
python -m pytest tests\unit\test_ipo_s1_analysis.py tests\unit\test_event_connectors.py::test_sec_ipo_s1_downloads_document_and_normalizes_offer_analysis tests\integration\test_sec_ipo_cli.py tests\integration\test_alerts_cli.py::test_alerts_list_default_hides_future_alerts -q
python -m pytest
python -m ruff check src tests apps
git diff --check
```

Completion audit must map the active objective to concrete evidence:

- IPO detection exists.
- Public S-1 fixture/live document retrieval exists.
- Offering analysis exists and is deterministic.
- CLI ingestion and review exist.
- Tests cover the flow.
- Existing SEC submissions behavior is preserved by existing tests.


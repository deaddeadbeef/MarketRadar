# Phase 3 Full Universe and Real Market Data Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a real market-data path and point-in-time universe builder so Catalyst Radar can scan a liquid U.S. equity universe from provider data without premium LLM calls.

**Architecture:** Keep the Phase 2 provider foundation intact. Add one live provider adapter behind the existing connector protocol, persist raw and normalized provider records, promote valid daily bars into the domain tables, snapshot eligible universe membership, and let the scanner consume a named universe snapshot. All live network behavior must be token-gated, fixture-tested, and fail closed when timestamps, coverage, or provider health are not trustworthy.

**Tech Stack:** Python 3.11, standard-library HTTP (`urllib.request`) with injectable transports, SQLAlchemy Core, pandas, SQLite-compatible local storage, PostgreSQL-compatible migration SQL, pytest, ruff.

---

## Starting Point

Current branch baseline after Phase 2:

```text
main @ 29903b7 merge: integrate phase 2 production data foundation
```

Verified Phase 2 capabilities:

- CSV ingest runs through `CsvMarketDataConnector`.
- Raw and normalized provider records persist separately.
- Provider health, job runs, data-quality incidents, universe snapshots, and universe members exist.
- Missing or naive provider timestamps fail closed.
- Scans respect `available_at`.
- `python -m pytest` reports `96 passed`.
- `python -m ruff check src tests apps` reports `All checks passed!`.

Baseline smoke command:

```powershell
Remove-Item data\local\catalyst_radar.db -ErrorAction SilentlyContinue
$env:CATALYST_DATABASE_URL='sqlite:///data/local/catalyst_radar.db'
python -m catalyst_radar.cli init-db
python -m catalyst_radar.cli ingest-csv --securities data/sample/securities.csv --daily-bars data/sample/daily_bars.csv --holdings data/sample/holdings.csv
python -m catalyst_radar.cli provider-health --provider csv
python -m catalyst_radar.cli scan --as-of 2026-05-08
```

Expected output:

```text
initialized database
ingested securities=6 daily_bars=36 holdings=1
provider=csv status=healthy
scanned candidates=3
```

## Provider Selection Gate

Provider research checked on 2026-05-10 from official provider pages:

| Provider | Current fit | Coverage and access | Adjustment and corporate-action notes | Cost and limits | Decision |
| --- | --- | --- | --- | --- | --- |
| Polygon.io | Best first adapter for full-universe daily bars. | Grouped Daily endpoint returns OHLC, volume, and VWAP for all U.S. stocks on one date. Pricing page lists free Stocks Basic with all U.S. stock tickers, end-of-day data, reference data, corporate actions, 2 years history, and 5 API calls/minute. Starter is listed at $29/month with unlimited API calls and 5 years history. | Grouped Daily has `adjusted=true` by default for split-adjusted bars. Corporate-action endpoints include splits and dividends. | Free tier can backfill slowly; paid Starter can backfill faster. | Implement first. It gives the cleanest path to nightly full-market EOD scans without thousands of symbol requests. |
| EODHD | Strong alternate for low-cost historical EOD and exchange bulk updates. | Historical EOD docs list 150,000+ tickers, 51,000+ U.S. stocks/ETFs/funds, demo tickers, free plan with 20 calls/day and one year EOD depth, paid EOD plan starting at $19.99/month, and bulk daily exchange endpoint. | EOD docs state OHLC are raw while `adjusted_close` is adjusted for splits and dividends. That requires deliberate normalization before using high/low/ATR features. | Paid EOD plan lists 100,000 calls/day and 1000 requests/minute. Bulk endpoint consumes 100 calls per exchange request. | Keep as documented fallback. Do not implement first because adjusted OHLC handling is more ambiguous for the current feature engine. |
| Tiingo | Useful alternate for high-quality EOD once symbol universe and request budget are clear. | Tiingo official KB recommends initial per-ticker historical loads, then `daily/prices` for latest daily prices. | KB says to refresh full history when `splitFactor != 1` or `divCash > 0`, which is good but makes full-universe refresh orchestration more involved. | Public current pricing and limits were less direct in the quick official-doc pass. | Do not implement in this phase. Revisit after Polygon and EODHD seams exist. |
| Alpha Vantage | Poor fit for full-universe scans. | Docs include daily adjusted data and listing status, but daily adjusted is marked premium. | Provides adjusted close and corporate-action fields in the daily adjusted endpoint. | Premium page says free standard limit is 25 requests/day and premium has no daily limits. | Do not implement for universe scanning. Use only as a future validation cross-check if needed. |

Official source links to keep in the implementation notes:

- Polygon pricing: `https://polygon.io/pricing`
- Polygon Grouped Daily endpoint: `https://polygon.io/docs/stocks/get_v2_aggs_grouped_locale_us_market_stocks__date`
- Polygon splits endpoint: `https://polygon.io/docs/rest/stocks/corporate-actions/splits`
- Polygon stocks overview: `https://polygon.io/docs/stocks/getting-started`
- EODHD historical EOD docs: `https://eodhd.com/financial-apis/api-for-historical-data-and-volumes`
- EODHD bulk docs: `https://eodhd.com/knowledgebase/bulk-download-api/`
- EODHD pricing: `https://eodhd.com/pricing`
- Tiingo EOD ingestion KB: `https://www.tiingo.com/kb/article/the-fastest-method-to-ingest-tiingo-end-of-day-stock-api-data/`
- Alpha Vantage docs: `https://www.alphavantage.co/documentation/`
- Alpha Vantage premium page: `https://www.alphavantage.co/premium/`

Decision for this phase:

- Implement a Polygon adapter first.
- Require `CATALYST_POLYGON_API_KEY` for live provider calls.
- Never commit API keys.
- Make all tests use local fixtures and fake transports.
- Keep a clear seam so an EODHD adapter can be added in a later phase without changing scanner code.

## Non-Goals

- Do not buy, subscribe to, or sign up for a paid provider plan.
- Do not commit provider credentials.
- Do not add SEC/news/earnings/event ingestion.
- Do not add options data.
- Do not add OpenAI or any LLM usage.
- Do not add broker integration or trade execution.
- Do not replace the dashboard beyond small read-model helpers required for provider health or universe visibility.
- Do not introduce async workers, Celery, Prefect, or scheduled jobs in this phase.

## Phase Exit Criteria

- `CATALYST_POLYGON_API_KEY` and provider options are config-driven and documented.
- A fixture-backed `PolygonMarketDataConnector` implements `fetch`, `normalize`, `healthcheck`, and `estimate_cost`.
- Polygon grouped daily payloads persist as raw provider records and normalized daily-bar records with strict `source_ts`, `fetched_at`, `available_at`, `license_tag`, and `retention_policy`.
- Polygon ticker/reference payloads can normalize enough `Security` records to scan, with explicit metadata fallbacks marked in payload metadata.
- Live ingest commands fail with a clear non-zero error when the provider key is absent.
- Fixture ingest commands run without network access.
- Universe filters exclude inactive, sub-threshold, stale, low-liquidity, unsupported, and metadata-insufficient tickers.
- Universe snapshots and members are written through the existing Phase 2 tables.
- `scan --universe <name>` uses the latest point-in-time universe snapshot available at the scan timestamp.
- The original CSV smoke flow remains unchanged.
- Full verification passes: tests, ruff, CSV smoke, Polygon fixture ingest smoke, universe build smoke, and universe scan smoke.

## File Structure

Files to create:

- `src/catalyst_radar/connectors/http.py`: small HTTP response, transport protocol, `urllib` transport, fake transport, JSON parsing, retry classification, URL redaction.
- `src/catalyst_radar/connectors/polygon.py`: Polygon request builders, adapter, normalization rules, timestamp policy, provider-specific rejected payloads.
- `src/catalyst_radar/connectors/provider_ingest.py`: shared provider ingest orchestration currently embedded in `cli.py`, so CSV and Polygon use one job/health/incident path.
- `src/catalyst_radar/universe/__init__.py`: public exports.
- `src/catalyst_radar/universe/filters.py`: filter config, exclusion reasons, deterministic member ranking.
- `src/catalyst_radar/universe/builder.py`: point-in-time universe snapshot builder from stored securities and daily bars.
- `tests/fixtures/polygon/grouped_daily_2026-05-08.json`: grouped-daily bars for benchmark ETFs and mixed eligible/ineligible equities.
- `tests/fixtures/polygon/tickers_page_1.json`: ticker/reference payload with active, inactive, ETF, ADR, and metadata-missing examples.
- `tests/fixtures/polygon/tickers_page_2.json`: second page fixture proving pagination.
- `tests/unit/test_http_client.py`: redaction, JSON parsing, fake transport, and retry classification tests.
- `tests/unit/test_polygon_connector.py`: request building, timestamp policy, normalization, rejection, and cost tests.
- `tests/unit/test_universe_filters.py`: deterministic filter reason and rank tests.
- `tests/integration/test_polygon_ingest_cli.py`: fixture ingest persistence and fail-closed token tests.
- `tests/integration/test_universe_builder.py`: snapshot persistence and replay tests.
- `tests/integration/test_scan_universe_filter.py`: `scan --universe` behavior.
- `tests/integration/test_real_market_indexes.py`: schema smoke proving full-universe query indexes exist.
- `tests/golden/test_market_scan_golden.py`: deterministic fixture-scale scan ordering and blocks.

Files to modify:

- `.env.example`: document local DB, Polygon key, provider base URL, timeout, and universe thresholds.
- `pyproject.toml`: keep dependencies unchanged unless implementation proves a standard-library HTTP client is insufficient.
- `src/catalyst_radar/core/config.py`: add provider, Polygon, HTTP, universe, and scan-batch config fields.
- `src/catalyst_radar/connectors/__init__.py`: export the new adapter and HTTP helpers.
- `src/catalyst_radar/connectors/provider_registry.py`: register Polygon and allow token-gated construction.
- `src/catalyst_radar/storage/provider_repositories.py`: add latest universe lookup and richer member reads.
- `src/catalyst_radar/storage/repositories.py`: add active-security lookup by ticker set and daily-bar coverage helpers.
- `sql/migrations/003_real_market_data_indexes.sql`: add read-path indexes for provider ingest, daily bars, securities, universe snapshots, universe members, job runs, and incidents.
- `src/catalyst_radar/pipeline/scan.py`: add optional universe member filtering and scan counters.
- `src/catalyst_radar/cli.py`: add `ingest-polygon`, `build-universe`, and `scan --universe`; move shared ingest code into `provider_ingest.py`.
- `README.md`: add local real-data workflow and provider credential rules.
- `docs/phase-3-review.md`: record implementation decisions, provider tradeoffs, verification output, and unresolved provider limitations.

## Task 1: Provider Config and Credential Guardrails

**Objective:** Add config needed for Polygon live calls, fixture mode, universe thresholds, and safe secret handling.

**Files:**

- Modify: `.env.example`
- Modify: `src/catalyst_radar/core/config.py`
- Create: `tests/unit/test_provider_config.py`
- Modify: `README.md`

- [ ] **Step 1: Write config tests**

Create `tests/unit/test_provider_config.py` with:

```python
from catalyst_radar.core.config import AppConfig


def test_polygon_config_reads_env_without_requiring_key() -> None:
    config = AppConfig.from_env(
        {
            "CATALYST_MARKET_PROVIDER": "polygon",
            "CATALYST_POLYGON_API_KEY": "secret-key",
            "CATALYST_POLYGON_BASE_URL": "https://example.test",
            "CATALYST_HTTP_TIMEOUT_SECONDS": "7.5",
            "CATALYST_PROVIDER_AVAILABILITY_POLICY": "next_session_11_utc",
            "CATALYST_UNIVERSE_NAME": "liquid-us",
            "CATALYST_UNIVERSE_MIN_PRICE": "10",
            "CATALYST_UNIVERSE_MIN_AVG_DOLLAR_VOLUME": "25000000",
            "CATALYST_UNIVERSE_REQUIRE_SECTOR": "true",
        }
    )

    assert config.market_provider == "polygon"
    assert config.polygon_api_key == "secret-key"
    assert config.polygon_base_url == "https://example.test"
    assert config.http_timeout_seconds == 7.5
    assert config.provider_availability_policy == "next_session_11_utc"
    assert config.universe_name == "liquid-us"
    assert config.universe_min_price == 10
    assert config.universe_min_avg_dollar_volume == 25_000_000
    assert config.universe_require_sector is True


def test_invalid_provider_timeout_fails_fast() -> None:
    try:
        AppConfig.from_env({"CATALYST_HTTP_TIMEOUT_SECONDS": "0"})
    except ValueError as exc:
        assert "CATALYST_HTTP_TIMEOUT_SECONDS" in str(exc)
    else:
        raise AssertionError("expected invalid timeout to fail")
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
python -m pytest tests/unit/test_provider_config.py -q
```

Expected: fails because the new config fields do not exist.

- [ ] **Step 3: Add config fields**

Update `AppConfig` with these fields and parsing:

```python
market_provider: str = "csv"
polygon_api_key: str | None = None
polygon_base_url: str = "https://api.polygon.io"
http_timeout_seconds: float = 10.0
provider_availability_policy: str = "live_fetch"
universe_name: str = "liquid-us"
universe_min_price: float = 5.0
universe_min_avg_dollar_volume: float = 10_000_000.0
universe_require_sector: bool = False
universe_include_etfs: bool = False
universe_include_adrs: bool = True
scan_batch_size: int = 500
```

Add helper validation:

```python
def _positive_float(env: Mapping[str, str], key: str, default: float) -> float:
    value = _float(env, key, default)
    if value <= 0:
        raise ValueError(f"{key} must be greater than zero")
    return value
```

- [ ] **Step 4: Document env vars**

Create or update `.env.example` with:

```text
CATALYST_ENV=local
CATALYST_DATABASE_URL=sqlite:///data/local/catalyst_radar.db
CATALYST_MARKET_PROVIDER=polygon
CATALYST_POLYGON_API_KEY=
CATALYST_POLYGON_BASE_URL=https://api.polygon.io
CATALYST_HTTP_TIMEOUT_SECONDS=10
CATALYST_PROVIDER_AVAILABILITY_POLICY=live_fetch
CATALYST_UNIVERSE_NAME=liquid-us
CATALYST_UNIVERSE_MIN_PRICE=5
CATALYST_UNIVERSE_MIN_AVG_DOLLAR_VOLUME=10000000
CATALYST_UNIVERSE_REQUIRE_SECTOR=false
CATALYST_UNIVERSE_INCLUDE_ETFS=false
CATALYST_UNIVERSE_INCLUDE_ADRS=true
CATALYST_SCAN_BATCH_SIZE=500
```

- [ ] **Step 5: Verify and commit**

Run:

```powershell
python -m pytest tests/unit/test_provider_config.py -q
python -m ruff check src tests apps
git add .env.example src/catalyst_radar/core/config.py tests/unit/test_provider_config.py README.md
git commit -m "feat: add provider configuration guardrails"
```

## Task 2: HTTP Transport and Fixture Client

**Objective:** Add a tiny HTTP layer that supports live provider calls, fake fixture responses, safe URL redaction, and deterministic tests.

**Files:**

- Create: `src/catalyst_radar/connectors/http.py`
- Create: `tests/unit/test_http_client.py`

- [ ] **Step 1: Write HTTP tests**

Create `tests/unit/test_http_client.py` with:

```python
from catalyst_radar.connectors.http import (
    FakeHttpTransport,
    HttpResponse,
    JsonHttpClient,
    redact_url,
)


def test_redact_url_hides_provider_tokens() -> None:
    url = "https://api.polygon.io/v2/aggs?apiKey=secret&token=abc&symbol=AAPL"

    assert redact_url(url) == (
        "https://api.polygon.io/v2/aggs?apiKey=REDACTED&token=REDACTED&symbol=AAPL"
    )


def test_json_client_uses_fake_transport() -> None:
    transport = FakeHttpTransport(
        {
            "https://example.test/data": HttpResponse(
                status_code=200,
                url="https://example.test/data",
                headers={"content-type": "application/json"},
                body=b'{"status":"OK","results":[{"T":"AAPL"}]}',
            )
        }
    )
    client = JsonHttpClient(transport=transport, timeout_seconds=3)

    payload = client.get_json("https://example.test/data")

    assert payload["status"] == "OK"
    assert transport.requests == ["https://example.test/data"]


def test_json_client_raises_redacted_error() -> None:
    transport = FakeHttpTransport(
        {
            "https://example.test/data?apiKey=secret": HttpResponse(
                status_code=429,
                url="https://example.test/data?apiKey=secret",
                headers={},
                body=b'{"error":"rate limited"}',
            )
        }
    )
    client = JsonHttpClient(transport=transport, timeout_seconds=3)

    try:
        client.get_json("https://example.test/data?apiKey=secret")
    except RuntimeError as exc:
        assert "apiKey=REDACTED" in str(exc)
        assert "secret" not in str(exc)
    else:
        raise AssertionError("expected HTTP error")
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
python -m pytest tests/unit/test_http_client.py -q
```

Expected: fails because `catalyst_radar.connectors.http` does not exist.

- [ ] **Step 3: Implement HTTP helpers**

Create `src/catalyst_radar/connectors/http.py` with:

```python
from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen


_SECRET_QUERY_KEYS = {"apikey", "api_key", "api-token", "api_token", "token"}


@dataclass(frozen=True)
class HttpResponse:
    status_code: int
    url: str
    headers: Mapping[str, str]
    body: bytes


class HttpTransport(Protocol):
    def get(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        timeout_seconds: float,
    ) -> HttpResponse:
        raise NotImplementedError


class UrlLibHttpTransport:
    def get(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        timeout_seconds: float,
    ) -> HttpResponse:
        request = Request(url, headers=dict(headers), method="GET")
        with urlopen(request, timeout=timeout_seconds) as response:
            return HttpResponse(
                status_code=int(response.status),
                url=url,
                headers=dict(response.headers.items()),
                body=response.read(),
            )


class FakeHttpTransport:
    def __init__(self, responses: Mapping[str, HttpResponse]) -> None:
        self._responses = dict(responses)
        self.requests: list[str] = []

    def get(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        timeout_seconds: float,
    ) -> HttpResponse:
        self.requests.append(url)
        if url not in self._responses:
            raise RuntimeError(f"missing fake HTTP response for {redact_url(url)}")
        return self._responses[url]


class JsonHttpClient:
    def __init__(self, transport: HttpTransport, timeout_seconds: float) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be greater than zero")
        self.transport = transport
        self.timeout_seconds = timeout_seconds

    def get_json(self, url: str, headers: Mapping[str, str] | None = None) -> Any:
        response = self.transport.get(
            url,
            headers=headers or {},
            timeout_seconds=self.timeout_seconds,
        )
        if response.status_code < 200 or response.status_code >= 300:
            raise RuntimeError(
                f"HTTP {response.status_code} from {redact_url(response.url)}"
            )
        try:
            return json.loads(response.body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"invalid JSON from {redact_url(response.url)}") from exc


def redact_url(url: str) -> str:
    parts = urlsplit(url)
    query = []
    for key, value in parse_qsl(parts.query, keep_blank_values=True):
        query.append((key, "REDACTED" if key.lower() in _SECRET_QUERY_KEYS else value))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))
```

- [ ] **Step 4: Verify and commit**

Run:

```powershell
python -m pytest tests/unit/test_http_client.py -q
python -m ruff check src tests apps
git add src/catalyst_radar/connectors/http.py tests/unit/test_http_client.py
git commit -m "feat: add provider HTTP transport seam"
```

## Task 3: Polygon Connector

**Objective:** Implement a fixture-testable Polygon connector for grouped daily bars and ticker/reference payloads.

**Files:**

- Create: `src/catalyst_radar/connectors/polygon.py`
- Modify: `src/catalyst_radar/connectors/__init__.py`
- Create: `tests/unit/test_polygon_connector.py`
- Create: `tests/fixtures/polygon/grouped_daily_2026-05-08.json`
- Create: `tests/fixtures/polygon/tickers_page_1.json`
- Create: `tests/fixtures/polygon/tickers_page_2.json`

- [ ] **Step 1: Add fixture payloads**

Create `tests/fixtures/polygon/grouped_daily_2026-05-08.json` with symbols covering:

```json
{
  "status": "OK",
  "adjusted": true,
  "queryCount": 7,
  "resultsCount": 7,
  "results": [
    {"T": "SPY", "v": 80000000, "vw": 580.0, "o": 577.0, "c": 582.0, "h": 583.0, "l": 575.0, "t": 1778198400000, "n": 1000000},
    {"T": "XLK", "v": 15000000, "vw": 230.0, "o": 228.0, "c": 231.0, "h": 232.0, "l": 227.0, "t": 1778198400000, "n": 200000},
    {"T": "XLI", "v": 9000000, "vw": 130.0, "o": 129.0, "c": 131.0, "h": 132.0, "l": 128.0, "t": 1778198400000, "n": 140000},
    {"T": "AAPL", "v": 65000000, "vw": 210.0, "o": 205.0, "c": 214.0, "h": 215.0, "l": 204.0, "t": 1778198400000, "n": 900000},
    {"T": "MSFT", "v": 28000000, "vw": 450.0, "o": 444.0, "c": 455.0, "h": 456.0, "l": 443.0, "t": 1778198400000, "n": 500000},
    {"T": "THIN", "v": 10000, "vw": 2.0, "o": 2.0, "c": 2.1, "h": 2.2, "l": 1.9, "t": 1778198400000, "n": 100},
    {"T": "BADTS", "v": 10000, "vw": 10.0, "o": 10.0, "c": 10.1, "h": 10.2, "l": 9.9, "n": 100}
  ]
}
```

Create ticker fixtures with `ticker`, `name`, `market`, `locale`, `primary_exchange`, `type`, `active`, `currency_name`, `cik`, and `composite_figi` fields. Include `next_url` on page 1 and no `next_url` on page 2.

- [ ] **Step 2: Write connector tests**

Create `tests/unit/test_polygon_connector.py` with tests for:

```python
from datetime import UTC, datetime
from pathlib import Path

from catalyst_radar.connectors.base import ConnectorRecordKind, ConnectorRequest
from catalyst_radar.connectors.http import FakeHttpTransport, HttpResponse, JsonHttpClient
from catalyst_radar.connectors.polygon import PolygonEndpoint, PolygonMarketDataConnector


def _client_for_fixture(url: str, fixture_path: str) -> JsonHttpClient:
    body = Path(fixture_path).read_bytes()
    transport = FakeHttpTransport(
        {
            url: HttpResponse(
                status_code=200,
                url=url,
                headers={"content-type": "application/json"},
                body=body,
            )
        }
    )
    return JsonHttpClient(transport=transport, timeout_seconds=3)


def test_grouped_daily_normalizes_adjusted_daily_bars() -> None:
    url = (
        "https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/"
        "2026-05-08?adjusted=true&include_otc=false&apiKey=fixture-key"
    )
    connector = PolygonMarketDataConnector(
        api_key="fixture-key",
        client=_client_for_fixture(url, "tests/fixtures/polygon/grouped_daily_2026-05-08.json"),
    )
    request = ConnectorRequest(
        provider="polygon",
        endpoint=PolygonEndpoint.GROUPED_DAILY.value,
        params={"date": "2026-05-08", "adjusted": True, "include_otc": False},
        requested_at=datetime(2026, 5, 9, 12, tzinfo=UTC),
    )
    raw = connector.fetch(request)
    normalized = connector.normalize(raw)

    assert normalized[0].kind == ConnectorRecordKind.DAILY_BAR
    assert normalized[0].identity == "AAPL:2026-05-08"
    assert normalized[0].payload["adjusted"] is True
    assert normalized[0].source_ts.tzinfo is not None
    assert normalized[0].available_at.tzinfo is not None


def test_grouped_daily_rejects_missing_timestamp() -> None:
    url = (
        "https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/"
        "2026-05-08?adjusted=true&include_otc=false&apiKey=fixture-key"
    )
    connector = PolygonMarketDataConnector(
        api_key="fixture-key",
        client=_client_for_fixture(url, "tests/fixtures/polygon/grouped_daily_2026-05-08.json"),
    )
    request = ConnectorRequest(
        provider="polygon",
        endpoint=PolygonEndpoint.GROUPED_DAILY.value,
        params={"date": "2026-05-08", "adjusted": True, "include_otc": False},
        requested_at=datetime(2026, 5, 9, 12, tzinfo=UTC),
    )
    connector.fetch(request)

    assert connector.rejected_payloads[0].affected_tickers == ("BADTS",)
    assert connector.rejected_payloads[0].fail_closed_action == "reject-payload"


def test_ticker_pages_follow_next_url_without_leaking_key() -> None:
    first_url = "https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&limit=1000&apiKey=fixture-key"
    second_url = "https://api.polygon.io/v3/reference/tickers?cursor=page-2&apiKey=fixture-key"
    transport = FakeHttpTransport(
        {
            first_url: HttpResponse(200, first_url, {}, Path("tests/fixtures/polygon/tickers_page_1.json").read_bytes()),
            second_url: HttpResponse(200, second_url, {}, Path("tests/fixtures/polygon/tickers_page_2.json").read_bytes()),
        }
    )
    connector = PolygonMarketDataConnector(
        api_key="fixture-key",
        client=JsonHttpClient(transport=transport, timeout_seconds=3),
    )
    request = ConnectorRequest(
        provider="polygon",
        endpoint=PolygonEndpoint.TICKERS.value,
        params={"market": "stocks", "active": True, "limit": 1000},
        requested_at=datetime(2026, 5, 9, 12, tzinfo=UTC),
    )
    raw_records = connector.fetch(request)

    assert len(raw_records) == 2
    assert "apiKey=" not in raw_records[0].request_hash
```

Use `FakeHttpTransport`, `HttpResponse`, and local JSON fixture bytes. Do not call the network.

- [ ] **Step 3: Run tests to verify they fail**

Run:

```powershell
python -m pytest tests/unit/test_polygon_connector.py -q
```

Expected: fails because the connector does not exist.

- [ ] **Step 4: Implement Polygon connector**

Create `src/catalyst_radar/connectors/polygon.py` with these public pieces:

```python
POLYGON_PROVIDER_NAME = "polygon"
POLYGON_LICENSE_TAG = "polygon-market-data"
POLYGON_RETENTION_POLICY = "retain-per-provider-license"


class PolygonEndpoint(StrEnum):
    GROUPED_DAILY = "polygon_grouped_daily"
    TICKERS = "polygon_tickers"
```

Implement `PolygonMarketDataConnector` with constructor:

```python
def __init__(
    self,
    *,
    api_key: str | None,
    client: JsonHttpClient,
    base_url: str = "https://api.polygon.io",
    provider: str = POLYGON_PROVIDER_NAME,
    availability_policy: str = "live_fetch",
) -> None:
    self.api_key = api_key
    self.client = client
    self.base_url = base_url.rstrip("/")
    self.provider = provider
    self.availability_policy = availability_policy
```

Required behavior:

- `healthcheck()` returns DOWN with reason `missing CATALYST_POLYGON_API_KEY` when `api_key` is blank.
- `estimate_cost()` returns request count based on one grouped-daily request per date or one ticker page per expected page.
- `fetch()` supports:
  - `endpoint="polygon_grouped_daily"` with params `{"date": "YYYY-MM-DD", "adjusted": true, "include_otc": false}`.
  - `endpoint="polygon_tickers"` with params `{"market": "stocks", "active": true, "limit": 1000}` and pagination through provider `next_url`.
- Request hashes must use redacted URL and params, never the raw key.
- Payload hashes must be deterministic with sorted JSON keys.
- `source_ts` for grouped daily bars must be parsed from provider `t` milliseconds as UTC.
- `available_at` must be `fetched_at` when `availability_policy == "live_fetch"`.
- `available_at` must be next calendar day at `11:00 UTC` when `availability_policy == "next_session_11_utc"`.
- Missing `T`, `t`, OHLC, volume, or non-adjusted grouped payload rows are rejected.
- Ticker payloads normalize to `Security` payloads with:
  - `ticker`
  - `name`
  - `exchange`
  - `sector="Unknown"` when no sector exists
  - `industry="Unknown"` when no industry exists
  - `market_cap=0.0` when absent
  - `avg_dollar_volume_20d=0.0` until universe builder computes it
  - `has_options=False` until options coverage exists
  - `is_active`
  - `updated_at`
  - `metadata_source="polygon_reference"`

- [ ] **Step 5: Verify and commit**

Run:

```powershell
python -m pytest tests/unit/test_polygon_connector.py -q
python -m ruff check src tests apps
git add src/catalyst_radar/connectors/polygon.py src/catalyst_radar/connectors/__init__.py tests/unit/test_polygon_connector.py tests/fixtures/polygon
git commit -m "feat: add polygon market data connector"
```

## Task 4: Shared Provider Ingest Orchestration

**Objective:** Move common ingest job, health, raw persistence, normalization, domain promotion, and incident handling out of `cli.py` so CSV and Polygon share the same fail-closed path.

**Files:**

- Create: `src/catalyst_radar/connectors/provider_ingest.py`
- Modify: `src/catalyst_radar/cli.py`
- Modify: `tests/integration/test_provider_ingest_cli.py`
- Create: `tests/integration/test_polygon_ingest_cli.py`

- [ ] **Step 1: Write integration tests**

Add tests that prove:

```python
def test_polygon_ingest_requires_api_key(tmp_path: Path) -> None:
    result = run_cli(
        ["ingest-polygon", "grouped-daily", "--date", "2026-05-08"],
        env={"CATALYST_POLYGON_API_KEY": ""},
    )
    assert result.exit_code == 1
    assert "missing CATALYST_POLYGON_API_KEY" in result.stderr


def test_polygon_fixture_ingest_persists_raw_normalized_and_daily_bars(tmp_path: Path) -> None:
    result = run_cli(
        [
            "ingest-polygon",
            "grouped-daily",
            "--date",
            "2026-05-08",
            "--fixture",
            "tests/fixtures/polygon/grouped_daily_2026-05-08.json",
        ],
        env={"CATALYST_POLYGON_API_KEY": "fixture-key"},
    )
    assert result.exit_code == 0
    assert "ingested provider=polygon raw=" in result.stdout
```

Use existing CLI test helpers if present; if not present, build a local helper that calls `catalyst_radar.cli.main(argv)` while setting `CATALYST_DATABASE_URL` to a temp SQLite file.

- [ ] **Step 2: Run tests to verify they fail**

Run:

```powershell
python -m pytest tests/integration/test_polygon_ingest_cli.py -q
```

Expected: fails because `ingest-polygon` is not implemented.

- [ ] **Step 3: Extract shared ingest function**

Create `ProviderIngestResult` and `ingest_provider_records()` in `provider_ingest.py`:

```python
@dataclass(frozen=True)
class ProviderIngestResult:
    provider: str
    job_id: str
    requested_count: int
    raw_count: int
    normalized_count: int
    security_count: int
    daily_bar_count: int
    holding_count: int
    rejected_count: int
```

Function signature:

```python
def ingest_provider_records(
    *,
    connector: MarketDataConnector,
    request: ConnectorRequest,
    market_repo: MarketRepository,
    provider_repo: ProviderRepository,
    job_type: str,
    metadata: Mapping[str, Any],
) -> ProviderIngestResult:
    raise NotImplementedError("implemented in Task 4")
```

The function must:

- Save health first.
- Start a job before fetch.
- Abort on DOWN health.
- Save raw records before normalization.
- Record connector `rejected_payloads` when the connector exposes that attribute.
- Abort on critical rejected payloads.
- Save normalized provider records.
- Promote normalized securities, daily bars, and holdings into domain tables.
- Finish job as SUCCESS, PARTIAL_SUCCESS, or FAILED.
- Record a critical incident for unexpected exceptions.

- [ ] **Step 4: Add CLI commands**

Update `build_parser()` with:

```python
polygon = subparsers.add_parser("ingest-polygon")
polygon_sub = polygon.add_subparsers(dest="polygon_command", required=True)
grouped = polygon_sub.add_parser("grouped-daily")
grouped.add_argument("--date", type=date.fromisoformat, required=True)
grouped.add_argument("--fixture", type=Path)
tickers = polygon_sub.add_parser("tickers")
tickers.add_argument("--fixture", type=Path)
```

Fixture mode must construct a fake HTTP transport using fixture content. Live mode must construct `UrlLibHttpTransport` and require `config.polygon_api_key`.

- [ ] **Step 5: Preserve CSV output**

Update the existing CSV path to call `ingest_provider_records()` but keep the exact old success message:

```text
ingested securities=6 daily_bars=36 holdings=1
```

- [ ] **Step 6: Verify and commit**

Run:

```powershell
python -m pytest tests/integration/test_provider_ingest_cli.py tests/integration/test_polygon_ingest_cli.py -q
python -m pytest tests/integration/test_csv_ingest.py tests/integration/test_provider_availability_gates.py -q
python -m ruff check src tests apps
git add src/catalyst_radar/connectors/provider_ingest.py src/catalyst_radar/cli.py tests/integration/test_provider_ingest_cli.py tests/integration/test_polygon_ingest_cli.py
git commit -m "feat: share provider ingest orchestration"
```

## Task 5: Universe Filters and Snapshot Builder

**Objective:** Build deterministic point-in-time universe snapshots from stored securities and available daily bars.

**Files:**

- Create: `src/catalyst_radar/universe/__init__.py`
- Create: `src/catalyst_radar/universe/filters.py`
- Create: `src/catalyst_radar/universe/builder.py`
- Modify: `src/catalyst_radar/storage/provider_repositories.py`
- Modify: `src/catalyst_radar/storage/repositories.py`
- Create: `tests/unit/test_universe_filters.py`
- Create: `tests/integration/test_universe_builder.py`

- [ ] **Step 1: Write filter tests**

Create tests for:

```python
def test_universe_filter_accepts_liquid_active_common_stock() -> None:
    security = make_security("AAPL", market_cap=3_000_000_000_000, sector="Technology")
    bars = make_daily_bars("AAPL", close=214, volume=65_000_000, sessions=20)
    decision = evaluate_universe_member(
        security,
        bars,
        UniverseFilterConfig(min_price=5, min_avg_dollar_volume=10_000_000),
    )

    assert decision.included is True
    assert decision.reason == "eligible"


def test_universe_filter_excludes_low_liquidity_and_missing_sector() -> None:
    security = make_security("THIN", market_cap=500_000_000, sector="Unknown")
    bars = make_daily_bars("THIN", close=2.1, volume=10_000, sessions=20)
    decision = evaluate_universe_member(
        security,
        bars,
        UniverseFilterConfig(
            min_price=5,
            min_avg_dollar_volume=10_000_000,
            require_sector=True,
        ),
    )

    assert decision.included is False
    assert "low_avg_dollar_volume" in decision.exclusion_reasons
    assert "missing_sector" in decision.exclusion_reasons
```

- [ ] **Step 2: Write builder integration test**

Create a temp database with securities and 20 daily bars. Then assert:

```python
snapshot = builder.build(as_of=date(2026, 5, 8), available_at=as_of_dt)
assert snapshot.member_count == 2
assert provider_repo.list_universe_members(snapshot.id) == ["AAPL", "MSFT"]
```

- [ ] **Step 3: Run tests to verify they fail**

Run:

```powershell
python -m pytest tests/unit/test_universe_filters.py tests/integration/test_universe_builder.py -q
```

Expected: fails because the universe package does not exist.

- [ ] **Step 4: Implement filters**

Create:

```python
@dataclass(frozen=True)
class UniverseFilterConfig:
    min_price: float
    min_avg_dollar_volume: float
    require_sector: bool = False
    include_etfs: bool = False
    include_adrs: bool = True


@dataclass(frozen=True)
class UniverseDecision:
    ticker: str
    included: bool
    reason: str
    rank: int | None
    avg_dollar_volume_20d: float
    latest_close: float
    exclusion_reasons: tuple[str, ...]
```

Implement `evaluate_universe_member(security, bars, config)` so it adds exact exclusion reason strings:

- `inactive`
- `missing_bars`
- `stale_bars`
- `low_price`
- `low_avg_dollar_volume`
- `missing_sector`
- `etf_excluded`
- `adr_excluded`

- [ ] **Step 5: Implement builder**

Create `UniverseBuilder`:

```python
class UniverseBuilder:
    def __init__(
        self,
        *,
        market_repo: MarketRepository,
        provider_repo: ProviderRepository,
        config: UniverseFilterConfig,
        name: str,
        provider: str,
    ) -> None:
        self.market_repo = market_repo
        self.provider_repo = provider_repo
        self.config = config
        self.name = name
        self.provider = provider

    def build(self, *, as_of: date, available_at: datetime) -> UniverseSnapshotResult:
        raise NotImplementedError("implemented in Task 5")
```

The builder must:

- Read active securities.
- Read 20 available daily bars per ticker.
- Apply filters.
- Rank included members by descending 20-day average dollar volume, then ticker.
- Save a snapshot with included members and metadata containing `eligible_count`, `excluded_count`, and reason counts.
- Store excluded reason counts in snapshot metadata, not `universe_members`.

- [ ] **Step 6: Add repository helpers**

Add:

```python
def latest_universe_snapshot(
    self,
    *,
    name: str,
    as_of: datetime,
    available_at: datetime,
) -> UniverseSnapshotRecord | None:
    raise NotImplementedError("implemented in Task 5")

def list_universe_member_rows(self, snapshot_id: str) -> list[UniverseMemberRecord]:
    raise NotImplementedError("implemented in Task 5")
```

Add `MarketRepository.list_active_securities_by_tickers(tickers: Collection[str])`.

- [ ] **Step 7: Verify and commit**

Run:

```powershell
python -m pytest tests/unit/test_universe_filters.py tests/integration/test_universe_builder.py -q
python -m ruff check src tests apps
git add src/catalyst_radar/universe src/catalyst_radar/storage/provider_repositories.py src/catalyst_radar/storage/repositories.py tests/unit/test_universe_filters.py tests/integration/test_universe_builder.py
git commit -m "feat: build point-in-time universe snapshots"
```

## Task 6: Build-Universe CLI and Universe-Aware Scan

**Objective:** Expose universe construction through CLI and make scans optionally consume a named snapshot.

**Files:**

- Modify: `src/catalyst_radar/cli.py`
- Modify: `src/catalyst_radar/pipeline/scan.py`
- Create: `tests/integration/test_scan_universe_filter.py`

- [ ] **Step 1: Write scan integration test**

Create `tests/integration/test_scan_universe_filter.py`:

```python
def test_scan_with_universe_uses_snapshot_members_only(tmp_path: Path) -> None:
    env = make_temp_database_env(tmp_path)
    seed_two_eligible_securities(env)
    save_universe_snapshot(env, name="liquid-us", members=["AAPL"])
    result = run_cli(["scan", "--as-of", "2026-05-08", "--universe", "liquid-us"], env=env)
    assert result.exit_code == 0
    assert "scanned candidates=1" in result.stdout
```

Set up two eligible securities in domain tables but save a universe snapshot containing only one ticker.

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
python -m pytest tests/integration/test_scan_universe_filter.py -q
```

Expected: fails because `scan --universe` is not implemented.

- [ ] **Step 3: Add `build-universe` command**

Add parser:

```python
build_universe = subparsers.add_parser("build-universe")
build_universe.add_argument("--name", default=config.universe_name)
build_universe.add_argument("--provider", default=config.market_provider)
build_universe.add_argument("--as-of", type=date.fromisoformat, required=True)
```

Output format:

```text
built universe=liquid-us members=1234 excluded=456
```

- [ ] **Step 4: Add scanner universe parameter**

Change signature:

```python
def run_scan(
    repo: MarketRepository,
    as_of: date,
    *,
    universe_tickers: set[str] | None = None,
) -> list[ScanResult]:
    raise NotImplementedError("implemented in Task 6")
```

Filter active securities with `universe_tickers` before feature computation.

- [ ] **Step 5: Add `scan --universe` lookup**

When `--universe` is present:

- Compute `as_of_dt` the same way `run_scan` does.
- Use `ProviderRepository.latest_universe_snapshot(name=args.universe, as_of=as_of_dt, available_at=as_of_dt)`.
- Exit non-zero with `universe not found: <name>` when no snapshot is available.
- Pass member tickers to `run_scan`.

- [ ] **Step 6: Verify and commit**

Run:

```powershell
python -m pytest tests/integration/test_scan_universe_filter.py -q
python -m pytest tests/integration/test_scan_pipeline.py -q
python -m ruff check src tests apps
git add src/catalyst_radar/cli.py src/catalyst_radar/pipeline/scan.py tests/integration/test_scan_universe_filter.py
git commit -m "feat: scan named universe snapshots"
```

## Task 7: Real-Market Indexes and Retention Metadata

**Objective:** Add the read-path indexes needed for universe-scale scans and make provider retention policy auditable.

**Files:**

- Create: `sql/migrations/003_real_market_data_indexes.sql`
- Modify: `src/catalyst_radar/storage/schema.py`
- Create: `tests/integration/test_real_market_indexes.py`
- Modify: `docs/phase-3-review.md`

- [ ] **Step 1: Write schema index test**

Create `tests/integration/test_real_market_indexes.py` with:

```python
from sqlalchemy import inspect

from catalyst_radar.storage.db import create_schema, engine_from_url


def test_real_market_indexes_exist(tmp_path) -> None:
    engine = engine_from_url(f"sqlite:///{tmp_path / 'market.db'}")
    create_schema(engine)

    inspector = inspect(engine)
    daily_bar_indexes = {index["name"] for index in inspector.get_indexes("daily_bars")}
    security_indexes = {index["name"] for index in inspector.get_indexes("securities")}
    universe_snapshot_indexes = {
        index["name"] for index in inspector.get_indexes("universe_snapshots")
    }
    universe_member_indexes = {
        index["name"] for index in inspector.get_indexes("universe_members")
    }

    assert "ix_daily_bars_ticker_date_available_at" in daily_bar_indexes
    assert "ix_securities_active_ticker" in security_indexes
    assert "ix_universe_snapshots_name_asof_available_at" in universe_snapshot_indexes
    assert "ix_universe_members_snapshot_rank_ticker" in universe_member_indexes
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
python -m pytest tests/integration/test_real_market_indexes.py -q
```

Expected: fails because the indexes are not declared.

- [ ] **Step 3: Add SQLAlchemy indexes**

Modify `src/catalyst_radar/storage/schema.py` and add indexes:

```python
Index("ix_daily_bars_ticker_date_available_at", daily_bars.c.ticker, daily_bars.c.date, daily_bars.c.available_at)
Index("ix_securities_active_ticker", securities.c.is_active, securities.c.ticker)
Index("ix_raw_provider_provider_kind_source", raw_provider_records.c.provider, raw_provider_records.c.kind, raw_provider_records.c.source_ts)
Index("ix_normalized_provider_identity_available", normalized_provider_records.c.provider, normalized_provider_records.c.kind, normalized_provider_records.c.identity, normalized_provider_records.c.available_at)
Index("ix_provider_health_provider_checked", provider_health.c.provider, provider_health.c.checked_at)
Index("ix_job_runs_provider_started", job_runs.c.provider, job_runs.c.started_at)
Index("ix_incidents_provider_detected", data_quality_incidents.c.provider, data_quality_incidents.c.detected_at)
Index("ix_universe_snapshots_name_asof_available_at", universe_snapshots.c.name, universe_snapshots.c.as_of, universe_snapshots.c.available_at)
Index("ix_universe_members_snapshot_rank_ticker", universe_members.c.snapshot_id, universe_members.c.rank, universe_members.c.ticker)
```

- [ ] **Step 4: Add migration SQL**

Create `sql/migrations/003_real_market_data_indexes.sql` with `CREATE INDEX IF NOT EXISTS` statements matching the SQLAlchemy index names. Keep SQL portable for PostgreSQL and SQLite syntax where possible.

- [ ] **Step 5: Record retention decision**

In `docs/phase-3-review.md`, record:

```text
Provider raw payloads are retained under the stored `retention_policy` value and are intended for local audit/replay only. Before any commercial use or redistribution, provider license terms must be reviewed and retention policy enforcement must be implemented as a scheduled cleanup.
```

- [ ] **Step 6: Verify and commit**

Run:

```powershell
python -m pytest tests/integration/test_real_market_indexes.py -q
python -m ruff check src tests apps
git add src/catalyst_radar/storage/schema.py sql/migrations/003_real_market_data_indexes.sql tests/integration/test_real_market_indexes.py docs/phase-3-review.md
git commit -m "feat: add real-market data indexes"
```

## Task 8: Fixture-Scale Provider Smoke and Golden Scan

**Objective:** Prove that provider fixture ingest, universe build, and universe scan form a deterministic no-network workflow.

**Files:**

- Create: `tests/golden/test_market_scan_golden.py`
- Modify: `tests/fixtures/polygon/grouped_daily_2026-05-08.json`
- Modify: `tests/fixtures/polygon/tickers_page_1.json`
- Modify: `tests/fixtures/polygon/tickers_page_2.json`
- Modify: `docs/phase-3-review.md`

- [ ] **Step 1: Expand fixtures**

Add enough dates or generated fixture rows to produce 60 sessions for:

- `SPY`
- `XLK`
- `XLI`
- `AAPL`
- `MSFT`
- `THIN`
- `STALE`

Keep the fixture small enough for fast tests. Use deterministic prices that make `AAPL` strongest, `MSFT` acceptable, `THIN` excluded by liquidity, and `STALE` excluded by stale bars.

- [ ] **Step 2: Write golden test**

The test must:

```python
def test_polygon_fixture_universe_scan_is_deterministic(tmp_path: Path) -> None:
    env = make_temp_database_env(tmp_path)
    run_cli(["init-db"], env=env)
    run_cli(
        ["ingest-polygon", "tickers", "--fixture", "tests/fixtures/polygon/tickers_page_1.json"],
        env=env,
    )
    run_cli(
        [
            "ingest-polygon",
            "grouped-daily",
            "--date",
            "2026-05-08",
            "--fixture",
            "tests/fixtures/polygon/grouped_daily_2026-05-08.json",
        ],
        env=env,
    )
    run_cli(["build-universe", "--name", "liquid-us", "--provider", "polygon", "--as-of", "2026-05-08"], env=env)
    results = run_scan_from_database(env, as_of="2026-05-08", universe="liquid-us")

    assert [result.ticker for result in results] == ["AAPL", "MSFT"]
    assert results[0].candidate.final_score > results[1].candidate.final_score
```

- [ ] **Step 3: Run golden test to verify it fails**

Run:

```powershell
python -m pytest tests/golden/test_market_scan_golden.py -q
```

Expected: fails until fixture ingest and universe scan are complete.

- [ ] **Step 4: Add review document**

Create `docs/phase-3-review.md` with sections:

```markdown
# Phase 3 Review

## Provider Decision

## Implemented Capabilities

## Known Provider Limitations

## Verification

## Real-World Testing Preconditions
```

Include the exact provider sources listed in this plan and the final verification output.

- [ ] **Step 5: Verify and commit**

Run:

```powershell
python -m pytest tests/golden/test_market_scan_golden.py -q
python -m ruff check src tests apps
git add tests/golden/test_market_scan_golden.py tests/fixtures/polygon docs/phase-3-review.md
git commit -m "test: add polygon universe golden scan"
```

## Task 9: Full Phase Verification

**Objective:** Re-run all tests and smoke flows on the Phase 3 branch before merge.

- [ ] **Step 1: Run full test suite**

Run:

```powershell
python -m pytest
```

Expected: all tests pass.

- [ ] **Step 2: Run ruff**

Run:

```powershell
python -m ruff check src tests apps
```

Expected:

```text
All checks passed!
```

- [ ] **Step 3: Run unchanged CSV smoke**

Run:

```powershell
Remove-Item data\local\catalyst_radar.db -ErrorAction SilentlyContinue
$env:CATALYST_DATABASE_URL='sqlite:///data/local/catalyst_radar.db'
python -m catalyst_radar.cli init-db
python -m catalyst_radar.cli ingest-csv --securities data/sample/securities.csv --daily-bars data/sample/daily_bars.csv --holdings data/sample/holdings.csv
python -m catalyst_radar.cli provider-health --provider csv
python -m catalyst_radar.cli scan --as-of 2026-05-08
```

Expected output:

```text
initialized database
ingested securities=6 daily_bars=36 holdings=1
provider=csv status=healthy
scanned candidates=3
```

- [ ] **Step 4: Run Polygon fixture smoke**

Run:

```powershell
Remove-Item data\local\catalyst_radar.db -ErrorAction SilentlyContinue
$env:CATALYST_DATABASE_URL='sqlite:///data/local/catalyst_radar.db'
$env:CATALYST_POLYGON_API_KEY='fixture-key'
$env:CATALYST_MARKET_PROVIDER='polygon'
python -m catalyst_radar.cli init-db
python -m catalyst_radar.cli ingest-polygon tickers --fixture tests/fixtures/polygon/tickers_page_1.json
python -m catalyst_radar.cli ingest-polygon grouped-daily --date 2026-05-08 --fixture tests/fixtures/polygon/grouped_daily_2026-05-08.json
python -m catalyst_radar.cli provider-health --provider polygon
python -m catalyst_radar.cli build-universe --name liquid-us --provider polygon --as-of 2026-05-08
python -m catalyst_radar.cli scan --as-of 2026-05-08 --universe liquid-us
```

Expected output pattern:

```text
initialized database
ingested provider=polygon raw=<n> normalized=<n> securities=<n> daily_bars=0 rejected=<n>
ingested provider=polygon raw=<n> normalized=<n> securities=0 daily_bars=<n> rejected=<n>
provider=polygon status=healthy
built universe=liquid-us members=<n> excluded=<n>
scanned candidates=<n>
```

- [ ] **Step 5: Run live-provider preflight when a key exists**

Run only if `CATALYST_POLYGON_API_KEY` is set to a real key:

```powershell
python -m catalyst_radar.cli ingest-polygon grouped-daily --date 2026-05-08
python -m catalyst_radar.cli provider-health --provider polygon
```

Expected:

```text
ingested provider=polygon raw=<n> normalized=<n> securities=0 daily_bars=<n> rejected=0
provider=polygon status=healthy
```

If no real key is present, record in `docs/phase-3-review.md`:

```text
Live Polygon smoke skipped: CATALYST_POLYGON_API_KEY is not set.
```

- [ ] **Step 6: Merge readiness review**

Request two subagent reviews:

- Reviewer A: code correctness, timestamp policy, provider token safety, fail-closed behavior.
- Reviewer B: product correctness, provider-selection realism, universe filter effectiveness, regression risk.

Fix high and medium findings before merge.

- [ ] **Step 7: Commit final review updates**

Run:

```powershell
git add docs/phase-3-review.md
git commit -m "docs: record phase 3 verification"
```

## Implementation Risks and Required Safeguards

- Provider terms and pricing can change. Keep provider facts in `docs/phase-3-review.md` with source URLs and date checked.
- Polygon grouped bars are split-adjusted by default, not dividend-total-return series. Do not present the score as a total-return model.
- Polygon reference ticker payloads may not contain sector, industry, options availability, or market cap. Store explicit fallbacks and let universe filters exclude metadata-insufficient names when `CATALYST_UNIVERSE_REQUIRE_SECTOR=true`.
- Free API limits can make historical backfills slow. The implementation must make one live request per grouped-daily date and no per-symbol daily-bar calls.
- Fixture tests must not hide token leaks. Test redacted errors and request hashes.
- `available_at` must remain separate from provider bar timestamp. Live ingests use actual fetch time; synthetic backfill availability must be opt-in and visible in metadata.
- Universe snapshots must be point-in-time. `scan --universe` may only use a snapshot whose `available_at` is less than or equal to the scan timestamp.

## Completion Definition for This Phase

Phase 3 is complete when the branch contains:

- Provider config and credential guardrails.
- HTTP transport seam.
- Polygon connector.
- Shared ingest orchestration.
- Universe filters and builder.
- CLI commands for Polygon ingest, universe build, and universe scan.
- Golden fixture scan.
- Review document with provider sources and verification output.
- Passing full test suite and ruff.
- Passing CSV smoke.
- Passing Polygon fixture smoke.
- Live smoke result recorded when a key is available, or explicit skip recorded when no key is available.
